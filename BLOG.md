> **Draft.** Not yet published anywhere. Numbers were measured
> 2026-09-26 to 09-28 on a shared laptop, and are called out
> as such below. See `docs/specs/2026-09-26-rustyq-bench-writeup.md` for
> the full methodology and reproduction commands.

# I built a Postgres-backed job queue in Rust to replace Celery

Most Python services I've worked on eventually grow a Celery deployment,
and Celery eventually becomes its own maintenance burden: a broker to run
(Redis or RabbitMQ), worker processes that need their own memory budget and
their own deploy story, and a retry/visibility model that's separate from
whatever already keeps your application data durable. Meanwhile, the thing
actually doing the durable bookkeeping in most of these systems — Postgres
— was sitting right there the whole time, capable of `FOR UPDATE SKIP
LOCKED`, `LISTEN`/`NOTIFY`, and transactional writes, for free.

So I built rustyq: a job queue where Postgres is the only infrastructure
dependency, the claim/dispatch/retry loop is a small Rust binary, and the
thing Python code keeps is a thin client that POSTs to an HTTP API. The
handler code moves from Python decorators into a Rust `Handler` registry
keyed by job "kind" — the price of a worker that runs jobs concurrently as
async tasks in one process instead of 16 forked interpreters.

## Why `FOR UPDATE SKIP LOCKED`

The core claim query is one statement:

```sql
UPDATE jobs SET state='running', locked_at=now(), locked_by=$1, attempts=attempts+1
WHERE id IN (
  SELECT id FROM jobs
  WHERE state='queued' AND run_at <= now() AND queue = ANY($2)
  ORDER BY priority DESC, run_at
  FOR UPDATE SKIP LOCKED
  LIMIT $3
)
RETURNING ...
```

`SKIP LOCKED` is what makes this safe with an arbitrary number of worker
processes hammering the same table: instead of blocking on a row another
transaction already has locked (or worse, deadlocking), a competing claim
just skips it and grabs the next available row. No advisory locks, no
external coordination, no leader election — Postgres's own row
locking is the queue's mutual exclusion mechanism. Batch it (claim up to N
rows in one round trip instead of looping `claim_one`) and you have a
respectable job queue core in a few hundred lines of SQL and Rust.

It is not free, though, and the first real bug I hit proved it. The
original dispatch index led with `queue`, matching the shape of the
original design ("index the columns you filter on, in filter order"). But
the claim query filters `queue = ANY($2)` — an array parameter, because a
worker can drain more than one queue — and Postgres cannot use an
array-membership check as a leading equality condition the same way it uses
`queue = $1`. The planner fell back to a sequential scan of every queued
row plus a full sort, on every single claim. At 20,000 queued rows that was
82 milliseconds per claim, and it got worse as the backlog grew — a
textbook O(n²) drain. Re-ordering the index to lead with the `ORDER BY`
columns instead (`(priority DESC, run_at) WHERE state='queued'`) let the
planner walk the index and stop at `LIMIT`, dropping claim latency to 0.8
milliseconds. The lesson wasn't "SKIP LOCKED is slow" — it was "the index
has to match the query's access pattern, not just its filter clause," which
is an easy thing to get backwards when you design the schema before you've
written the query that will actually run against it under load.

## LISTEN/NOTIFY, and why it still needs a poll fallback

Claiming efficiently doesn't help if a worker doesn't know there's anything
to claim. rustyq's enqueue path does the `INSERT` and a `pg_notify` in one
statement, so the notification is delivered at commit and a listening
worker wakes up immediately — no polling delay for the common case. But
every worker also falls back to a plain one-second poll, running
concurrently with the `LISTEN` wait. That redundancy is deliberate:
`PgListener` connections can drop and need to reconnect, a notify can in
principle be missed, and a queue that silently stalls because a single
long-lived connection died quietly is a worse failure mode than a worst-case
one-second dispatch delay. LISTEN/NOTIFY buys you low latency in the
common case; the poll is the fallback that makes the low-latency path safe
to trust.

## The chaos-test lesson: lock timeouts and fencing

The zero-loss guarantee is the whole point of a durable queue, so I wrote a
chaos test: boot four workers, kill two of them with SIGKILL mid-drain (no
graceful shutdown — their claimed rows just sit there, `running`, with a
lock nobody will ever release), restart them, and assert every job ends up
`done`, with none stuck, none dead and none lost.

It passed — 20,000 out of 20,000 done, zero dead, zero stuck, 59 jobs
re-run — but it also
surfaced the actual hard problem in this kind of system, which isn't the
kill, it's the recovery race. The reaper reclaims a `running` job once its
lock is older than `lock_timeout`, on the assumption that its worker is
dead. But "the worker is dead" and "the worker is just slow because
Postgres is stalling" look identical from the reaper's point of view. On
the loaded laptop I ran this on, Postgres itself stalled for up to 18
seconds at one point, and I'd set a 10-second lock timeout to make the test
converge quickly — so the reaper reclaimed jobs whose original worker was
still alive and would eventually try to finalize them too.

That collision is exactly why every finalize (done, dead or retry) in rustyq
is fenced on `(locked_by, attempts)`, not just `locked_by`. Every claim —
including the re-claim of a job the reaper put back — increments `attempts`, so it doubles as
a fencing token. When the original, merely-slow worker finally got around
to finalizing a job that had already been reclaimed, its `UPDATE ... WHERE
attempts = $original_attempts` matched zero rows, because the reclaim had
already bumped the counter. The finalize was skipped and logged instead of
silently overwriting the new owner's result. The job simply ran twice and
finished once, correctly — which is exactly the "at least once" contract a
job queue promises, working as designed under the exact conditions that
break the naive version of this design.

The operational corollary is that `lock_timeout` is a knob with a real
tradeoff: too short, and you get unnecessary duplicate work every time the
database has a slow moment; too long, and a genuinely dead worker's jobs
sit stuck longer before recovery. The shipped default is 300 seconds — 30
times longer than the value I used to make the chaos test converge in a
reasonable amount of time — specifically so it comfortably exceeds worse
stalls than the 18-second one I happened to observe.

## Numbers, with the caveats attached

I ran the same in-Docker comparison against Celery 5.4 (Redis broker,
prefork pool, 16 processes per worker, 4 workers) three times on 20,000
trivial jobs. Celery scored 279, 1,104 and 168 jobs/s; rustyq scored 386,
394 and 694 jobs/s with durable commits (849 in the third run with
`synchronous_commit=off`, roughly Redis's durability). Depending on the run, Celery was 2.8x
faster or rustyq was 4.1x faster. That spread is the shared laptop, not
either queue, so I'm not quoting a ratio. What I'd stand behind without
hedging is memory: rustyq's worker containers ran at 2.3–9.4 MiB of RSS
against Celery's roughly 340 MiB per worker in every run (35–150x), because
the gap comes from an architectural fact (one async Rust process versus 16 forked CPython
interpreters), not from noise.

The first quiet-hardware hint came from CI: the same drain bench on a
GitHub-hosted 4-vCPU runner with a native Postgres did 4,900 jobs/s and a
p99 enqueue-to-pickup of 4.3 ms, against 798 jobs/s and hundreds of
milliseconds on my loaded laptop. One run on shared CI isn't a benchmark,
but it says where the laptop numbers went. The honest summary is that the
Celery throughput comparison still needs a controlled re-run before I'd put
a ratio on a slide,
and the zero-loss chaos result has passed once so far (five back-to-back
runs are still to do). The memory gap is real and measured, and both can be
re-run with the scripts in this repository.

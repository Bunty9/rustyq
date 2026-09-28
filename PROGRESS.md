# PROGRESS — rustyq

> Per-sprint tracker. Template adapted from `project-plan.md` § 7,
> customised for P1 (rustyq) bench targets and the Phase B sequencing in
> `backend-cloud-roadmap.md` § 2 (weeks 7–10). Phase numbers match
> `docs/plans/2026-06-02-rustyq-execution-plan.md`.

## Sprint — Phase 1 scaffold

- [x] Workspace `Cargo.toml` with all members + pinned stack deps
- [x] `crates/core` — `Job`, `JobState`, `Worker`, `claim_one`, `finalize`
- [x] `crates/server` — axum binary, `POST /jobs`
- [x] `crates/worker` — daemon binary, Ctrl-C cancels via `CancellationToken`
- [x] `crates/pybind` — PyO3 + maturin `pyproject.toml`
- [x] `crates/client` — async Rust client wrapping reqwest
- [x] `migrations/0001_init.sql` — `jobs` table + dispatch / locked indexes
- [x] `Dockerfile` — cargo-chef multi-stage + distroless
- [x] `docker-compose.yml` — postgres + server + 2 workers
- [x] `fly.toml` — region `sin`, Neon-attached
- [x] `.github/workflows/ci.yml` — fmt + clippy + nextest + deny + bench
- [x] `deny.toml`, `rust-toolchain.toml`, `.gitignore`
- [x] `README.md`, design spec, phase plan
- [x] `cargo check --workspace` passes locally (commit `c8695f3` "phase 1 scaffold verified end-to-end")
- [x] `docker compose up` boots Postgres + server + 2 workers
- [x] `POST /jobs` returns 200 + UUID

## Sprint — Phase 2: real dispatch + observability

- [x] `run_job` dispatches by `kind` to a registered async handler
      (`crates/core/src/handler.rs` — `Handler` trait + `Registry`)
- [x] Built-in handlers: `noop`, `sleep`, `fail_once`
      (`crates/worker/src/handlers.rs`)
- [x] Integration tests cover dispatch, retry, dead-letter (TDD)
      (`crates/core/tests/{dispatch,retry,dead}.rs` against
      `TEST_DATABASE_URL`)
- [x] CI runs Postgres service container + serial nextest
- [x] `GET /jobs/{id}` status endpoint with `JobStatus` JSON shape
      (`crates/server/src/api.rs`) + 2 TDD tests
- [x] Prometheus `/metrics` endpoint on the server
      (`crates/server/src/metrics.rs` — `OnceLock` recorder, six
      `rustyq_*` series with HELP descriptions); worker-side series served
      by each worker's own listener (`RUSTYQ_METRICS_BIND`, default :9091)
- [x] `Worker::claim_batch(n)` — single `UPDATE…RETURNING LIMIT n` per
      drain cycle (`crates/core/src/lib.rs`) + TDD tests
      (`crates/core/tests/batch.rs`)
- [x] Local throughput benchmark harness (`bench/local/throughput.sh`,
      since removed — superseded by `crates/core/benches/drain.rs` and
      `KILL=0 scripts/chaos.sh`)
- [x] `tracing-opentelemetry` OTLP exporter behind a flag — landed in
      Phase 3 (see below), not Phase 2 as originally sequenced
- [x] Switch back to `sqlx::query!` macros + `sqlx prepare` in CI —
      landed in Phase 3 (see below)
- [x] Criterion-style bench harness in `crates/core/benches/` — landed in
      Phase 4 as a plain `harness = false` binary (`benches/drain.rs`),
      not Criterion (a 20k–100k job drain is one long measurement, not a
      micro-benchmark Criterion iterates)
- [x] Worker drain loop: stop breaking out of the inner batch loop when
      `sem.available_permits() == 0` — now waits on `acquire_owned().await`
      instead, so the worker stays pegged when there is more queued work
      (`crates/core/src/lib.rs::Worker::run`, the `if n == 0` branch).
      Landed in Phase 4/5 (commit `38deb58`).
- [ ] Scope `rustyq_new` LISTEN/NOTIFY channel per database/schema so
      tests can run in parallel without cross-talk (current workaround:
      `--test-threads=1`). **Not done** — see Remaining / blocked.

## Sprint — Phase 3: tracing/OTel + sqlx prepare in CI

- [x] `crates/core/src/telemetry.rs` — `init(service)` wires `tracing-subscriber`
      + a `tracing-opentelemetry` OTLP/gRPC layer when
      `OTEL_EXPORTER_OTLP_ENDPOINT` is set; falls back to a JSON `fmt`
      layer only, never fails process startup. `shutdown()` force-flushes
      the `BatchSpanProcessor` before the Tokio runtime is torn down.
- [x] `Worker::run`'s spawned job task wrapped in
      `tracing::info_span!("run_job", job.id, job.kind, job.queue,
      job.attempts, job.delay_ms)` (`crates/core/src/lib.rs`)
- [x] Server request spans via `tower_http::trace::TraceLayer`
      (`crates/server/src/api.rs::router`)
- [x] `docker-compose.yml` `jaeger` service behind `--profile otel` for
      local trace viewing
- [x] `claim_batch`, `finalize`, `finalize_done_batch`, `reap_stale`,
      enqueue, and status all rewritten with `sqlx::query!`/`query_as!`
      macros; `.sqlx/*.json` committed
- [x] `.github/workflows/ci.yml`: `test` job builds with `SQLX_OFFLINE=true`;
      separate `sqlx-check` job applies migrations to a fresh Postgres and
      runs `cargo sqlx prepare --workspace --check -- --tests`
- [ ] Verify a full trace (enqueue → claim → run → finalize under one
      `trace_id`) end-to-end against a local Jaeger — **not run this
      session** (stretch goal in the original phase plan; OTLP export
      itself is implemented and unit-tested, but no live Jaeger capture
      was taken)

## Sprint — Phase 4: benches + perf targets

- [x] `crates/core/benches/drain.rs` — in-process bench: bulk-insert
      `BENCH_JOBS` noop jobs, spin `BENCH_WORKERS` × `BENCH_CONCURRENCY`,
      time drain to `jobs/s`; second phase measures enqueue → handler-start
      latency (p50/p99/p99.9) with idle workers on LISTEN. Runs in a
      throwaway Postgres schema, skips cleanly when no `BENCH_DATABASE_URL`/
      `TEST_DATABASE_URL` is set.
- [x] Found and fixed the O(n²) claim-query regression: the old
      `(queue, state, priority DESC, run_at)` index was unusable against
      `queue = ANY($2)`, forcing a seq scan + full sort per claim
      (measured 82 ms/claim at 20k queued rows). New index
      `(priority DESC, run_at) WHERE state='queued'`
      (`migrations/0002_dispatch_index.sql`) dropped that to 0.8 ms/claim.
- [x] Batched success finalize — one `UPDATE ... FROM UNNEST(...)` per
      batch of up to 512 done jobs instead of one `UPDATE` per job
      (`finalize_done_batch`, `crates/core/src/lib.rs`).
- [x] `.github/workflows/ci.yml` `bench` job: non-blocking smoke run of
      `cargo bench -p rustyq-core --bench drain` with a low
      `BENCH_MIN_JOBS_PER_SEC=300` floor (catches order-of-magnitude
      regressions on noisy shared runners, not small drifts).
- [x] Measured numbers recorded — see Bench numbers table below.
- [ ] Hit the ≥ 5,000 jobs/s and p99 < 50 ms design targets — **not met**
      on the dev laptop this session (best measured: 1,579 jobs/s drain,
      p99 188–917 ms dispatch latency). The host was heavily loaded
      (load average 25–50) and Postgres ran inside Docker Desktop's VM;
      the raw `pgbench -N -c16 -T10` ceiling on this box was only 438 tps.
      Needs re-measurement on quiet, bare-metal-ish hardware before this
      item can be marked done — see Remaining / blocked.

## Sprint — Phase 5: chaos + Celery comparison harness

- [x] `scripts/chaos.sh [JOBS] [WORKERS]` — boots the compose stack with
      workers scaled to 0, bulk-inserts sleep jobs, scales up workers,
      `docker kill -s KILL`s half of them once ~25% are done, restarts
      them, and asserts every job ends `done` with 0 `dead` and none stuck
      in `queued`/`running`.
- [x] Reaper: `reap_stale` (`crates/core/src/lib.rs`) — periodic task on
      its own tokio task inside `Worker::run`, requeues `running` jobs
      whose `locked_at` predates `lock_timeout`, or marks them `dead` if
      `attempts >= max_attempts`. Covered by
      `crates/core/tests/reap.rs`.
- [x] Fencing on `(locked_by, attempts)` for every finalize path (single
      and batched) so a reaped-and-reclaimed job's stale finalize is a
      no-op instead of clobbering the new owner —
      `crates/core/tests/fenced_finalize.rs`.
- [x] Chaos run passed with zero job loss (see Bench numbers /
      README § Chaos testing for the exact numbers and the lock-timeout
      lesson learned from this run).
- [x] `bench/celery/` — Celery 5.4 + Redis broker baseline rig
      (`docker-compose.yml`, `tasks.py`, `enqueue.py`, `run.sh`), same
      "clock starts at first completed job" methodology as rustyq's bench.
- [x] In-Docker Celery-vs-rustyq comparison run recorded (see Bench
      numbers table) — 3 runs, inconclusive on throughput; fairness caveats
      (host noise, Redis vs. durable Postgres) in the bench write-up.
- [x] `docs/specs/2026-09-26-rustyq-bench-writeup.md` — methodology,
      numbers, `EXPLAIN` timings, chaos results, reproduction commands.
- [ ] 5 consecutive clean chaos runs — **only 1 run performed** this
      session; re-run before treating this as fully proven.

## Sprint — Phase 7 (local parts): Python client + e2e

- [x] `crates/pybind/src/lib.rs` — PyO3 `Client` class: `enqueue()` takes
      a dict/list/str/number/None payload (serialised via Python's own
      `json.dumps`, not passed through as a string), `status()`; non-2xx
      responses raise `RuntimeError`, unknown job id raises `KeyError`;
      HTTP calls run under `py.allow_threads` so the GIL is released.
- [x] `examples/python/celery_drop_in.py` — before/after Celery → rustyq
      call-site snippet.
- [x] `examples/python/test_client.py` — live pytest suite against a
      running `rustyq-server` (skipped unless `RUSTYQ_URL` is set):
      enqueue returns a UUID, status of a fresh job, `KeyError` on unknown
      id, priority/delay_secs accepted (`delay_secs` checked via `run_at`).
- [ ] TestPyPI publish (`maturin publish --repository testpypi`) —
      **blocked**, needs a TestPyPI token. See Remaining / blocked.
- [ ] CI job running `maturin develop` + `pytest examples/python` against
      a Compose backend — **not added**.

## Sprint — Phase 8 (partial, this session): docs

- [x] README rewritten to match shipped reality: architecture diagram
      (no gRPC, no per-queue caps), Features & semantics, Configuration
      tables, HTTP API table, measured Benchmarks section, Chaos testing
      section, Running tests section, Deploy (Fly) section marked
      "config ready, not yet deployed".
- [x] `docs/specs/2026-09-26-rustyq-bench-writeup.md` — full methodology
      write-up.
- [x] `BLOG.md` draft — "I built a Postgres-backed job queue in Rust to
      replace Celery".
- [ ] Fly badge, PyPI badge, demo URL, Grafana screenshot — **not done**,
      blocked on Phase 6/7 (deploy + publish) landing first.
- [ ] Cross-post plan, tag `v0.1.0`, publish `rustyq-client` to
      crates.io — **not done**.

## Bench numbers (as of 2026-09-26; Celery runs 09-26 to 09-28)

Host: 8 vCPU / 39 GB Linux laptop, Postgres 16 inside Docker Desktop's VM,
heavily shared (load average 25–50 from unrelated builds during most runs).
Raw ceiling on this box: `pgbench -N -c16 -T10` = 438 tps (1,239 tps with
`synchronous_commit=off`). **Absolute numbers below are pessimistic for
that reason — treat relative deltas (before/after a fix) as the reliable
signal; even rustyq/Celery on the same box swung too much for a ratio.** Full methodology and reproduction
commands: `docs/specs/2026-09-26-rustyq-bench-writeup.md`.

| metric                                                          | target          | current            | as-of      |
|------------------------------------------------------------------|-----------------|--------------------|------------|
| Drain throughput, 20k jobs, 4 workers × 16 concurrency           | >= 5,000 jobs/s | 798 jobs/s         | 2026-09-26 |
| Drain throughput, 4 workers × 64 concurrency                     | >= 5,000 jobs/s | 1,579 jobs/s       | 2026-09-26 |
| p99 enqueue -> first worker pickup (trickle, idle workers)       | < 50 ms         | 188–917 ms         | 2026-09-26 |
| Chaos: SIGKILL 2 of 4 workers mid-drain, jobs lost               | 0               | 0 (20,000/20,000 done, 0 dead, 59 re-run) | 2026-09-26 |
| Memory, rustyq worker container (in-Docker Celery comparison)   | < 2 MB/job      | 2.3–9.4 MiB **per worker process** | 2026-09-26 |
| Throughput vs Celery (same Docker VM, 20k jobs, 4×16)            | >= 3-5x         | inconclusive: 3 runs, Celery 168–1,104 vs rustyq 386–694 jobs/s (host noise) | 2026-09-28 |
| Memory vs Celery (same comparison; Celery ~340 MiB/worker)      | much lower      | ~35–150x lower (2.3–9.4 MiB) | 2026-09-28 |

\* The original 1,100 jobs/s baseline (2026-06-16, the since-removed `bench/local/throughput.sh`)
measured the curl/HTTP POST ceiling, not the worker's drain capacity — it is
superseded by the in-process `crates/core/benches/drain.rs` numbers above,
which isolate the claim/dispatch/finalize loop from HTTP overhead.

## Remaining / blocked

Needs credentials or quiet hardware, not available this session:

- **Fly.io deploy + Neon attach** (Phase 6) — `fly.toml` is ready
  (`--migrate` on the server process group, `kill_timeout` 40s > worker
  `shutdown_grace` 30s, `/healthz` check), but no Fly account / Neon
  database was available to actually deploy.
- **TestPyPI publish** (Phase 7) — `maturin build`/`publish` needs a
  TestPyPI token.
- **crates.io publish of `rustyq-client`** (Phase 8) — needs a crates.io
  token.
- **Grafana screenshot** (Phase 6) — depends on the Fly deploy above.
- **Re-measure drain throughput and dispatch p99 on quiet hardware** —
  this session's numbers were taken on a laptop at load average 25–50
  with Postgres inside Docker Desktop's VM; the 5,000 jobs/s and p99 <
  50 ms targets need a clean re-run before being called met or missed for
  real.
- **Scope the `rustyq_new` LISTEN/NOTIFY channel per database/schema** so
  the test suite can drop `--test-threads=1` and run in parallel.
- **5 consecutive clean chaos runs** — only one run was performed this
  session.
- **Live OTLP trace capture** — the exporter is implemented and unit
  tested, but no end-to-end trace was captured against a running Jaeger
  this session.

## Blog topics surfacing

- The O(n²) claim-query index bug (`queue = ANY($2)` couldn't use the old
  `(queue, ...)` -leading index; seq scan + full sort per claim).
- The chaos-run lock-timeout lesson: a 10s `RUSTYQ_LOCK_TIMEOUT_SECS`
  against an 18s Postgres stall caused the reaper to requeue live jobs;
  the `(locked_by, attempts)` finalize fence made the eventual stale
  finalize a no-op instead of a double-finalize. Default is 300s for a
  reason.
- `fail_once`'s original in-process `HashMap` bug (grew forever, and
  failed again if the retry landed on a different worker) — fixed by
  keying on `job.attempts` instead of worker-local state.
- The Debian trixie/glibc mismatch between the unpinned `cargo-chef`
  builder image and the `distroless/cc-debian12` runtime
  (`GLIBC_2.38 not found`), fixed by pinning both to trixie/debian13.
- `Dockerfile` `CMD` vs `ENTRYPOINT`: fly.toml `[processes]` replaces
  `CMD`, not `ENTRYPOINT` — an `ENTRYPOINT` would have turned the worker
  process into `rustyq-server /usr/local/bin/rustyq-worker`.

---
title: rustyq Execution Plan — Scaffold → P1 Exit
status: draft
date: 2026-06-02
related:
    - ../specs/2026-05-28-rustyq-design.md
    - 2026-05-28-rustyq-phase-1-scaffold.md
    - ../../../projects-l3-l4.md
    - ../../../project-plan.md
---

# rustyq Execution Plan — Scaffold → P1 Exit

> Drives current scaffold to full P1 exit per `projects-l3-l4.md` § P1 and
> `project-plan.md` § 3 (weeks 7–10, 50 hr budget). Each phase has a
> deliverable list, exit criterion, and rough hour estimate. Phases run
> sequentially; never start Phase N+1 with Phase N exit criteria red.

## Current state (snapshot)

- `cargo check --workspace` — **green** (one warning: invalid manifest key
  `workspace.dev-dependencies` in root `Cargo.toml`; fix in Phase 2).
- Scaffold files all landed; single commit `63239f7`.
- `crates/core/src/lib.rs` — `Job`, `JobState`, `Worker::run`, `claim_one`,
  `finalize` all real. `run_job` is `Ok(())` stub.
- `crates/server/src/api.rs` — `POST /jobs` real (runtime sqlx, no macros).
- `crates/{worker,client,pybind}` — real, end-to-end wired.
- `docker compose up` + `curl POST /jobs` — **not yet verified**.

## Phase budget

| Phase | Theme                              | Hours | Cumulative |
|-------|------------------------------------|-------|------------|
| 1.1   | Close scaffold verification        | 2     | 2          |
| 2     | Real dispatch + status + metrics   | 10    | 12         |
| 3     | Tracing/OTel + sqlx prepare in CI  | 6     | 18         |
| 4     | Criterion benches + perf targets   | 10    | 28         |
| 5     | Chaos + Celery comparison harness  | 8     | 36         |
| 6     | Fly.io deploy + Neon attach        | 6     | 42         |
| 7     | TestPyPI publish + Python e2e      | 4     | 46         |
| 8     | README polish + blog draft         | 4     | 50         |

Total: 50 hr. Matches `project-plan.md` § 3 P1 budget.

---

## Phase 1.1 — Close scaffold verification (2 hr)

**Goal:** flip the three unchecked items at the bottom of
`PROGRESS.md § Sprint — Phase 1 scaffold`.

- [ ] `cargo check --workspace` — already green; re-run after Phase 2
      manifest fix to confirm.
- [ ] `docker compose up --build -d` boots `postgres` healthy, server
      listening, both workers subscribed to `rustyq_new`.
- [ ] `curl -X POST http://localhost:8080/jobs -d '{...}'` returns 200 +
      `{"id":"01..."}`; row appears in `jobs` with `state='queued'`; within
      ~1 s the worker claims it and transitions to `state='done'`
      (via the `Ok(())` stub `run_job`).
- [ ] Commit: "phase 1 scaffold verified end-to-end".

**Exit:** PROGRESS sprint 1 fully checked. `docker compose down -v` clean.

---

## Phase 2 — Real dispatch + status + metrics (10 hr)

**Goal:** make rustyq actually *do* something on the worker side, expose
job status, and ship a Prometheus endpoint. This is the phase that turns
the scaffold into a usable queue.

### 2.1 Manifest fix (15 min)

- [ ] Root `Cargo.toml`: remove the invalid `[workspace.dev-dependencies]`
      table (Cargo has no such key — dev-deps belong to member crates).
      Move `testcontainers`, `wiremock`, `proptest`, `divan`, `criterion`
      into a `[workspace.dependencies]` block as workspace-shared and
      have each member crate opt in via `[dev-dependencies]` blocks.
- [ ] Re-run `cargo check --workspace` — no warnings.

### 2.2 Handler registry (`crates/core/src/handler.rs`) (3 hr)

- [ ] Define
      ```rust
      pub type HandlerFut = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
      pub trait Handler: Send + Sync + 'static {
          fn call(&self, job: &Job) -> HandlerFut;
      }
      ```
- [ ] `Registry { map: HashMap<String, Arc<dyn Handler>> }` with
      `register(kind, handler)` and `dispatch(job) -> HandlerFut`.
- [ ] Replace stub `run_job` with `registry.dispatch(job)`; thread an
      `Arc<Registry>` into `Worker::new` and store it in `Worker`.
- [ ] Built-in handlers (in worker `main.rs`): `noop`, `sleep` (reads
      `payload.ms`), `fail_once` (errors first attempt, succeeds second —
      proves the retry loop works end-to-end).

### 2.3 GET /jobs/{id} status (1 hr)

- [ ] `crates/server/src/api.rs`: add
      `GET /jobs/:id -> Json<JobStatus>` returning `id, state, attempts,
      last_error, run_at, locked_by`.
- [ ] Bench note in README: status reads bypass dispatcher.

### 2.4 Prometheus /metrics (3 hr)

- [ ] Add `metrics-exporter-prometheus` install in `crates/server/src/main.rs`.
      Single recorder per process.
- [ ] Counters/histograms in `core`:
      - `rustyq_jobs_enqueued_total{queue,kind}`
      - `rustyq_jobs_claimed_total{queue,worker}`
      - `rustyq_jobs_finished_total{queue,state}`  (`done|failed|dead`)
      - `rustyq_job_run_duration_seconds{queue,kind}` (histogram, buckets
        `[0.001, 0.01, 0.1, 1, 10]`)
      - `rustyq_dispatch_latency_seconds{queue}` (created_at → claimed_at)
- [ ] Server `/metrics` route returns `recorder.render()`.

### 2.5 Worker pool sizing review (45 min)

- [ ] Replace the busy-loop `while sem.available_permits() > 0` in
      `Worker::run` with batched claim (LIMIT N matching available permits)
      so under heavy load we issue one UPDATE per cycle rather than N.
      Defer if it complicates the chaos invariant — document the choice
      either way in `docs/specs/`.

### 2.6 Unit tests (2 hr)

- [ ] `crates/core/tests/dispatch.rs` — testcontainers Postgres,
      enqueue → claim → finalize done. Use `testcontainers-modules`.
- [ ] `crates/core/tests/retry.rs` — `fail_once` handler, two attempts,
      state ends `done`, `attempts=2`, `last_error` populated then cleared.
- [ ] `crates/core/tests/dead.rs` — handler always errors, hits
      `max_attempts`, ends `state='dead'`.

**Exit:**
- `cargo nextest run --workspace` green with the three new tests.
- `curl localhost:8080/metrics | grep rustyq_jobs_` shows counters
  incrementing during `docker compose` smoke.
- `PROGRESS.md` "Next sprint — Phase 2" all items checked except OTel
  (Phase 3) and Criterion (Phase 4).

---

## Phase 3 — Tracing/OTel + sqlx prepare in CI (6 hr)

**Goal:** turn on the second half of the observability stack and switch
back to compile-time-checked sqlx.

### 3.1 tracing-opentelemetry exporter (3 hr)

- [ ] `crates/core/src/telemetry.rs` — `init(otlp_endpoint: Option<&str>)`
      that wires `tracing-subscriber` + `tracing-opentelemetry` with the
      tonic OTLP exporter. No-op when env var unset.
- [ ] Wrap `Worker::run`'s spawned job task in
      `tracing::info_span!("run_job", job.id, job.kind, job.queue)` and
      record `attempts` + `delay_secs` as span fields.
- [ ] Server: span per request via `tower-http::trace::TraceLayer`.

### 3.2 Switch to `sqlx::query!` macros + offline prepare (2.5 hr)

- [ ] Rewrite `claim_one`, `finalize`, `enqueue`, status endpoint with
      `query!` / `query_as!` macros.
- [ ] `cargo install sqlx-cli --features postgres,rustls`. Run
      `cargo sqlx prepare --workspace` against a local compose Postgres.
      Commit `crates/*/.sqlx/*.json`.
- [ ] `.github/workflows/ci.yml`: add `SQLX_OFFLINE=true` to the test
      job. New job `sqlx-prepare-check` runs
      `cargo sqlx prepare --workspace --check`.

### 3.3 Verify (30 min)

- [ ] Run a local Jaeger or Grafana Tempo via Compose extension; confirm
      spans flow end-to-end (enqueue → claim → run → finalize all under
      one trace_id when client passes a `traceparent` header — stretch).

**Exit:**
- CI green with `SQLX_OFFLINE=true`.
- Local Compose with `OTEL_EXPORTER_OTLP_ENDPOINT` set exports spans.
- Manifest no longer carries the design-spec phase-1 caveat.

---

## Phase 4 — Criterion benches + perf targets (10 hr)

**Goal:** prove the bench targets in `projects-l3-l4.md` § P1.

### 4.1 Bench harness (3 hr)

- [ ] `crates/core/benches/enqueue_drain.rs` — Criterion bench. Pre-loads
      100k jobs, spins N workers, measures wall-clock to drain → records
      jobs/sec.
- [ ] `crates/core/benches/dispatch_latency.rs` — instrument enqueue → first
      claim time; p50/p99/p99.9. Compare LISTEN/NOTIFY vs 1 s poll
      fallback (toggle via env).
- [ ] `crates/core/benches/contention.rs` — N workers (1..16) on one
      queue; measure claim-rate scaling.

### 4.2 Hit the targets (5 hr)

Targets from spec:
- Throughput ≥ **5k jobs/s** drain on 4-vCPU.
- p99 dispatch latency **< 50 ms**.
- Memory per in-flight job **< 2 MB**.

Likely wins if numbers fall short:
- Batched claim (already noted Phase 2.5) — one UPDATE returning N rows.
- `tokio::sync::Notify` instead of re-acquiring semaphore permit per spin.
- `LISTEN` payload carrying queue name so workers can skip wakeups for
  queues they don't drain.
- Connection pool sizing: `concurrency + 2` is current floor — bench at
  `concurrency * 2`.

### 4.3 CI regression check (1 hr)

- [ ] `.github/workflows/ci.yml` bench job: switch from `--no-run` to
      a small smoke bench (100 jobs / 1 worker) that prints `jobs/sec`
      and fails the job if it drops below a checked-in floor. Full
      Criterion comparison remains a manual run (CI runners are too noisy).

### 4.4 Record numbers (1 hr)

- [ ] Update `PROGRESS.md` "Bench numbers" table with measured values
      and as-of date.
- [ ] README bench table swaps "pending" → real numbers.

**Exit:**
- 4-vCPU Hetzner / fly.io machine drains 100k jobs in ≤ 20 s.
- p99 dispatch < 50 ms confirmed against a 10 k enqueue burst.
- `cargo bench --workspace` runs locally.

---

## Phase 5 — Chaos + Celery comparison harness (8 hr)

**Goal:** validate zero-loss invariant + ship the headline "3–5× faster
than Celery" claim with reproducible numbers.

### 5.1 Chaos test (3 hr)

- [ ] `tests/chaos.sh` — orchestrates Compose: enqueue 100 k jobs across
      4 workers, `docker kill --signal=KILL` 2 random workers mid-drain,
      restart them, verify final state: every job in `done` or with
      attempts ≥ 1 (i.e., at-least-once).
- [ ] Invariant assertion: `SELECT count(*) FROM jobs WHERE state IN
      ('queued','running')` is 0 at end. No row stuck in `running`.
- [ ] Add reaper: server task that sweeps rows with
      `state='running' AND locked_at < now() - INTERVAL '5 min'` and
      flips them back to `queued`. Lifetime-of-claim is the chaos guard.

### 5.2 Celery comparison rig (4 hr)

- [ ] `bench/celery/` — minimal Python harness: Celery 5 + redis-results
      + postgres-broker (closest apples-to-apples vs rustyq's PG). Same
      4-vCPU host, same job count, same trivial handler (sleep 1 ms).
- [ ] `bench/rustyq/` — equivalent harness using `rustyq.Client`.
- [ ] Record drain time, p99 dispatch, memory peak. README bench table
      gains a "vs Celery" column.

### 5.3 Write up (1 hr)

- [ ] `docs/specs/2026-XX-XX-rustyq-bench-writeup.md` — methodology +
      numbers + caveats (Celery brokered through PG vs Redis, etc.).

**Exit:**
- Chaos test passes 5 consecutive runs.
- Celery comparison shows ≥ 3× throughput and ≥ 5× lower RSS.
- README "Bench" section reflects measured numbers, not targets.

---

## Phase 6 — Fly.io deploy + Neon attach (6 hr)

**Goal:** public demo URL hitting a real managed Postgres.

- [ ] Neon free-tier project + branch; export `DATABASE_URL`.
- [ ] `flyctl launch` from existing `fly.toml`; one machine in `sin`.
- [ ] Second app `rustyq-worker` reusing the same image with override
      `[processes]` entry: `worker = "/usr/local/bin/rustyq-worker"`.
- [ ] Run migrations from CI step (`sqlx migrate run`) or one-off
      `flyctl ssh console`.
- [ ] Health probe: Fly `[[http_service.checks]]` → `GET /metrics`.
- [ ] Add Fly badge + demo URL to README header.
- [ ] Grafana Cloud free tier scraping Fly `/metrics` — screenshot
      one dashboard into `docs/img/`.

**Exit:**
- Public `https://<app>.fly.dev/jobs` accepts an enqueue.
- Public `<app>.fly.dev/metrics` returns counters.
- Grafana panel screenshot committed.

---

## Phase 7 — TestPyPI publish + Python e2e (4 hr)

**Goal:** the *bridge story*. Python user does `pip install rustyq`,
points client at Fly URL, enqueues — done.

- [ ] `crates/pybind/pyproject.toml` — verify `[tool.maturin]` keys
      (`module-name = "rustyq"`, `python-source` if needed).
- [ ] `maturin build --release --strip` produces wheels for `linux x86_64`
      and `manylinux2014`.
- [ ] `maturin publish --repository testpypi -u __token__` (TestPyPI token
      first; PyPI is post-week-22 polish).
- [ ] `examples/python/celery_drop_in.py` — a 30-line "before / after"
      Celery → rustyq snippet.
- [ ] CI: add a job that runs `maturin develop` + `pytest
      examples/python` against a Compose backend (smoke; not on every push).

**Exit:**
- `pip install -i https://test.pypi.org/simple/ rustyq` works on a clean
  3.12 venv.
- README "Quick start (Python client)" tested end-to-end.

---

## Phase 8 — README polish + blog draft (4 hr)

**Goal:** Phase B P1 exit per `project-plan.md` § 3.

- [ ] README: real benchmark table, demo URL, Fly badge, PyPI badge,
      Grafana screenshot, "what hiring panel sees" snippet.
- [ ] `BLOG.md` draft (≥ 800 words) — "I built a Postgres-backed job
      queue in Rust to replace Celery". Cover:
      - Why `FOR UPDATE SKIP LOCKED` is the magic.
      - The LISTEN/NOTIFY + poll fallback tradeoff.
      - One specific hard bug encountered during chaos test.
      - Numbers vs Celery.
- [ ] Cross-post plan: dev.to, r/rust, This Week in Rust (PR to the repo).
- [ ] Tag `v0.1.0` on GitHub. Publish `rustyq-client` (the Rust async
      client) to crates.io.

**Exit (= P1 exit per `project-plan.md` § 3 Phase B P1):**
- Public repo with: README + arch diagram + measured bench table +
  Docker + Fly.io demo URL + GHA CI green.
- TestPyPI wheel installable.
- Blog draft ready for Phase D HN run (week 45+).

---

## Risk register (P1-specific)

| Risk                                          | Trigger                            | Mitigation                                            |
|-----------------------------------------------|------------------------------------|-------------------------------------------------------|
| sqlx macro vs runtime drift bites Phase 3     | `query!` rewrite breaks types      | Land 3.2 behind a feature flag; keep runtime fallback |
| 5 k jobs/s target misses on Fly free machine  | Drain < 3 k/s after Phase 4 tuning | Move bench harness to local 4-vCPU box; document     |
| testcontainers flake in CI                    | Postgres container start > 30 s    | Pin postgres:16-alpine; allow retry                  |
| Celery comparison disputed (broker mismatch)  | "Redis is faster" comments         | Run both PG-brokered and Redis-brokered Celery       |
| Maturin manylinux build complexity            | Wheel rejected by TestPyPI         | `maturin/maturin-action` GHA does the work          |
| Fly + Neon free tier rate limit during chaos  | 502s during 100 k enqueue burst    | Chaos test runs against local Compose only          |

---

## Out of scope (deferred to L4 stretch — see `projects-l3-l4.md` § P1 stretch)

- Driftdb (P5) drop-in storage backend.
- Work-stealing across nodes + gossip.
- Per-tenant fairness scheduler.
- gRPC enqueue surface (`tonic` dep is present but unused in P1).
- `argon2` API key hashing (no auth in P1 — Fly app is private demo).

---

## Definition of done (this plan)

When all 8 phases close:

1. `PROGRESS.md` sprint sections are all green or rolled into the next
   phase's plan.
2. `project-plan.md` § 3 P1 checklist is fully ticked.
3. Repo passes the Phase B exit criteria in `project-plan.md` § 3.
4. Move on to P2 (`ferryman`) with no dangling P1 work.

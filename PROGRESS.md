# PROGRESS — rustyq

> Per-sprint tracker. Template adapted from `project-plan.md` § 7,
> customised for P1 (rustyq) bench targets and the Phase B sequencing in
> `backend-cloud-roadmap.md` § 2 (weeks 7–10).

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
- [ ] `cargo check --workspace` passes locally (verified at end of scaffold)
- [ ] `docker compose up` boots Postgres + server + 2 workers
- [ ] `POST /jobs` returns 200 + UUID

## Next sprint — Phase 2: real dispatch + observability

- [x] `run_job` dispatches by `kind` to a registered async handler
      (`crates/core/src/handler.rs` — `Handler` trait + `Registry`)
- [x] Built-in handlers: `noop`, `sleep`, `fail_once`
      (`crates/worker/src/handlers.rs`)
- [x] Integration tests cover dispatch, retry, dead-letter (TDD)
      (`crates/core/tests/{dispatch,retry,dead}.rs` against
      `TEST_DATABASE_URL`)
- [x] CI runs Postgres service container + serial nextest
- [ ] Prometheus `/metrics` endpoint on the server (`metrics-exporter-prometheus`)
- [ ] `tracing-opentelemetry` OTLP exporter behind a flag
- [ ] Switch back to `sqlx::query!` macros + `sqlx prepare` in CI
- [ ] `GET /jobs/{id}` status endpoint
- [ ] Criterion bench harness in `crates/core/benches/`
- [ ] Scope `rustyq_new` LISTEN/NOTIFY channel per database/schema so
      tests can run in parallel without cross-talk (current workaround:
      `--test-threads=1`).

## Done

(none yet — scaffold landing is the first commit)

## Blocked

- (none)

## Bench numbers (targets per `projects-l3-l4.md` § P1; updated weekly)

| metric                                              | target          | current | as-of      |
|-----------------------------------------------------|-----------------|---------|------------|
| Throughput (drain rate, 4 vCPU, 100k enqueued)      | >= 5,000 jobs/s |         |            |
| p99 enqueue -> first worker pickup                  | < 50 ms         |         |            |
| Chaos: kill -9 2 of 4 workers, jobs lost            | 0               |         |            |
| Memory per in-flight job                            | < 2 MB          |         |            |
| Throughput vs Celery (same Postgres + hardware)     | >= 3-5x         |         |            |

## Blog topics surfacing

- (none yet)

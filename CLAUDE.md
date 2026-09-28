# rustyq — notes for Claude

Postgres-backed durable job queue in Rust (at-least-once) with a PyO3 Python
client. Workspace crates: `core` (Job, Worker loop, claim/finalize/reap),
`server` (axum HTTP API), `worker` (daemon + built-in handlers), `client`
(async Rust client), `pybind` (Python wheel via maturin).

## Commands

```bash
export PATH=$HOME/.cargo/bin:$PATH
eval "$(./scripts/test-pg.sh up | tail -1)"        # Postgres on :55432, exports TEST_DATABASE_URL
SQLX_OFFLINE=true cargo test --workspace -- --test-threads=1
cargo fmt --all && SQLX_OFFLINE=true cargo clippy --workspace --all-targets -- -D warnings
BENCH_DATABASE_URL=$TEST_DATABASE_URL cargo bench -p rustyq-core --bench drain   # drain + latency
scripts/chaos.sh 20000 4            # compose stack, SIGKILL half the workers, assert zero loss
KILL=0 MS=0 scripts/chaos.sh        # same harness as a plain in-Docker drain benchmark
bench/celery/run.sh 20000 4         # Celery baseline (separate compose project)
./scripts/test-pg.sh down
```

`cargo nextest` is not installed locally; CI uses it.

## Rules that bite

- **`sqlx::query!` macros + offline metadata.** After adding or changing any
  `query!`/`query_as!`, regenerate `.sqlx/` and commit it (CI runs
  `cargo sqlx prepare --workspace --check -- --tests`):
  ```bash
  docker exec rustyq-test-pg psql -U postgres -c 'DROP DATABASE IF EXISTS rustyq_prepare' -c 'CREATE DATABASE rustyq_prepare'
  for f in migrations/*.sql; do docker exec -i rustyq-test-pg psql -U postgres -d rustyq_prepare < "$f"; done
  DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/rustyq_prepare cargo sqlx prepare --workspace -- --tests
  ```
- **Migrations are append-only.** Never edit an existing file in
  `migrations/` (sqlx checksums; initdb'd volumes keep old objects). Add
  `000N_*.sql` and also append it to the `concat!(include_str!(..))` lists in
  `crates/core/tests/common/mod.rs`, `crates/server/tests/common/mod.rs` and
  `crates/core/benches/drain.rs`.
- **Tests need `--test-threads=1`**: every test binary shares the global
  `rustyq_new` NOTIFY channel (schemas are isolated, the channel is not).
- **Timing assertions must be generous.** The dev box is shared and often
  at load 25–50; bounds should catch order-of-magnitude regressions
  (e.g. the old 1 s idle per refill), not 2x drifts.
- **Finalize fencing.** Every finalize UPDATE (done/dead/requeue, single and
  batched) is fenced on
  `state='running' AND locked_by=$worker AND attempts=$attempts`; `attempts`
  is the fencing token (claim increments it). Keep that on any new
  transition, or a reaped-and-reclaimed job gets clobbered.
- **Lock timeout vs stalls.** `RUSTYQ_LOCK_TIMEOUT_SECS` (default 300) must
  exceed worst-case handler time plus DB stall, or the reaper requeues live
  jobs (safe thanks to fencing, but they re-run).
- **Dockerfile**: builder (`cargo-chef:latest-rust-1-trixie`) and runtime
  (`distroless/cc-debian13`) must share a Debian release (glibc). Use `CMD`,
  not `ENTRYPOINT` — fly.toml `[processes]` replace CMD.
- **Metrics are per process.** `metrics::` calls in `core` run inside
  `rustyq-worker`, which exports them on its own listener
  (`RUSTYQ_METRICS_BIND`, default :9091); the server's `/metrics` only has
  enqueue counts. A new metric must be emitted in the process that serves it.
- Host port 8080 is usually taken here: `RUSTYQ_HTTP_PORT=18080 docker compose up`.

## Git

Commits are authored by `Bunty9 <Bunty9@users.noreply.github.com>` only. No
`Co-Authored-By`, "Generated with", or any AI attribution in commits, PRs or
changelogs (see `~/.claude/CLAUDE.md`). Commit subjects follow
`phase N: <what>`.

## Status

See `PROGRESS.md` (sprint tracker, bench table, remaining/blocked work) and
`docs/plans/2026-06-02-rustyq-execution-plan.md` (phase plan). Deploy (Fly +
Neon), TestPyPI and crates.io publishing need credentials and are not done.

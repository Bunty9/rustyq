---
title: rustyq Phase 1 — Scaffold + Compile
status: draft
date: 2026-05-28
related:
    - ../specs/2026-05-28-rustyq-design.md
    - ../../../projects-l3-l4.md
    - ../../../backend-cloud-roadmap.md
---

# rustyq Phase 1 — Scaffold + Compile

> **Goal:** lay down every workspace member, the schema, the container
> story, and CI so that `cargo check --workspace` is green and
> `docker compose up` boots Postgres + the HTTP server + 2 workers
> end-to-end. No real job handlers yet; `run_job` is a `Ok(())` stub.

**Spec source:** [`../specs/2026-05-28-rustyq-design.md`](../specs/2026-05-28-rustyq-design.md).

## File inventory (checklist)

- [x] `Cargo.toml` — workspace root with all members and workspace deps
      pinned from `backend-cloud-roadmap.md` § 3.
- [x] `rust-toolchain.toml` — `channel = "stable"` + clippy + rustfmt.
- [x] `deny.toml` — minimal `cargo-deny` config (advisories deny, license
      allowlist for MIT/Apache/BSD/ISC/MPL/Unicode/CC0).
- [x] `.gitignore` — Rust + `.env` + `target/` + `*.cwasm` + `dist/` +
      `.venv/`.
- [x] `migrations/0001_init.sql` — `jobs` table + dispatch / locked indexes.
- [x] `crates/core/Cargo.toml` + `crates/core/src/lib.rs` — `Job`,
      `JobState`, `Worker`, `claim_one`, `finalize`.
- [x] `crates/server/Cargo.toml` + `crates/server/src/main.rs` +
      `crates/server/src/api.rs` — axum router with `POST /jobs`.
- [x] `crates/worker/Cargo.toml` + `crates/worker/src/main.rs` — boots a
      `Worker` and cancels on Ctrl-C.
- [x] `crates/pybind/Cargo.toml` + `crates/pybind/src/lib.rs` +
      `crates/pybind/pyproject.toml` — PyO3 client + maturin build config.
- [x] `crates/client/Cargo.toml` + `crates/client/src/lib.rs` — async Rust
      client wrapping `reqwest`.
- [x] `Dockerfile` — cargo-chef multi-stage, distroless final, both bins.
- [x] `docker-compose.yml` — postgres + rustyq-server + 2× rustyq-worker.
- [x] `fly.toml` — single machine, primary region `sin`, Neon-attached.
- [x] `.github/workflows/ci.yml` — matrix on stable + beta; runs `cargo
      fmt --check`, `cargo clippy -- -D warnings`, `cargo nextest run`,
      `cargo deny check`, and `cargo bench --no-run` (non-blocking).
- [x] `README.md` — problem, ASCII architecture, stack table, quick-start,
      bench targets, license.
- [x] `docs/specs/2026-05-28-rustyq-design.md` — full P1 design spec.
- [x] `docs/plans/2026-05-28-rustyq-phase-1-scaffold.md` — this plan.
- [x] `PROGRESS.md` — per-sprint tracker, P1 bench targets recorded.

## Exit criteria

1. **`cargo check --workspace` passes** from the project root with no
   `DATABASE_URL` available (offline check; `sqlx::query!` macros are
   avoided in Phase 1 — switch to them once `sqlx prepare` runs in CI).
2. **`docker compose up`** boots `postgres` (healthy), `rustyq-server`
   (listening on `:8080`), and both workers (subscribed to `rustyq_new`).
3. **`POST /jobs` returns HTTP 200** with a JSON body `{"id": "<uuid>"}`,
   and the corresponding row appears in `jobs` with `state='queued'`.

## Out of scope (deferred to later phases)

- Real `run_job` dispatch by `kind` → registered handler.
- Prometheus `/metrics` endpoint and tracing-OTel exporter wiring.
- Exponential-backoff retry observability (counters, dead-letter inspection).
- PyPI / TestPyPI publish workflow.
- Fly.io deploy + Neon attach demo.
- `cargo bench` real Criterion harness (CI step exists but no targets yet).
- Switch back to `sqlx::query!` macros + `sqlx prepare` artifact in CI.

## Verification recipe

```bash
cd rustyq
cargo check --workspace           # exit-criterion 1
docker compose up --build -d      # exit-criterion 2
sleep 5
curl -sS -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue":"default","kind":"send_email","payload":{"to":"a@b"}}'
# exit-criterion 3: response is {"id":"..."}, HTTP 200
docker compose down -v
```

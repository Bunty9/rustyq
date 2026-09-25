# syntax=docker/dockerfile:1.7

# ---- Stage 1: chef base (cargo-chef for layer-cacheable builds) -------------
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

# ---- Stage 2: planner (compute the recipe of dependencies) ------------------
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- Stage 3: builder (cook deps, then build the actual workspace) ----------
FROM chef AS builder
# Disable LTO and split codegen across many units inside the container build.
# `release` profile uses lto="fat" + codegen-units=1 (best runtime, worst
# build memory) which has been killing Docker Desktop's build VM. These
# env vars override the profile settings at compile time and lower the
# memory ceiling to comfortably under 2 GB. Runtime overhead is small for
# this workload; revisit when the host build env is stable.
ENV CARGO_PROFILE_RELEASE_LTO=false \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
    SQLX_OFFLINE=true
COPY --from=planner /app/recipe.json recipe.json
# Cook only the dependency graph — this layer is cached as long as Cargo.{toml,lock}
# do not change.
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
# Build only the bin crates we ship.
RUN cargo build --release \
    --bin rustyq-server \
    --bin rustyq-worker

# ---- Stage 4: distroless runtime --------------------------------------------
FROM gcr.io/distroless/cc-debian12 AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/rustyq-server /usr/local/bin/rustyq-server
COPY --from=builder /app/target/release/rustyq-worker /usr/local/bin/rustyq-worker
# Default to the server. CMD, not ENTRYPOINT: fly.toml [processes] replace
# CMD, so an ENTRYPOINT would turn the worker process into
# `rustyq-server /usr/local/bin/rustyq-worker`.
USER nonroot:nonroot
EXPOSE 8080
CMD ["/usr/local/bin/rustyq-server"]

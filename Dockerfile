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
# Default to the server. Override CMD in compose / fly.toml for workers.
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/rustyq-server"]

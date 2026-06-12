#!/usr/bin/env bash
# Bring up (or down) a local Postgres for `cargo test`. Idempotent.
#
#   ./scripts/test-pg.sh up    # start `rustyq-test-pg` on host port 55432
#   ./scripts/test-pg.sh down  # stop and remove
#   ./scripts/test-pg.sh url   # print TEST_DATABASE_URL for shell export
set -euo pipefail

NAME=rustyq-test-pg
PORT=55432
IMAGE=postgres:16-alpine
URL="postgres://postgres:postgres@127.0.0.1:${PORT}/postgres"

cmd="${1:-up}"

case "$cmd" in
  up)
    if docker ps --format '{{.Names}}' | grep -q "^${NAME}\$"; then
      echo "${NAME} already running"
    else
      docker rm -f "${NAME}" >/dev/null 2>&1 || true
      docker run -d --name "${NAME}" -p "${PORT}:5432" \
        -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres \
        "${IMAGE}" >/dev/null
      echo "started ${NAME} on :${PORT}"
    fi
    # Wait for ready.
    for i in $(seq 1 20); do
      if docker exec "${NAME}" pg_isready -U postgres >/dev/null 2>&1; then
        echo "ready"
        echo "export TEST_DATABASE_URL=${URL}"
        exit 0
      fi
      sleep 0.5
    done
    echo "FAILED: ${NAME} never became ready"
    exit 1
    ;;
  down)
    docker rm -f "${NAME}" >/dev/null 2>&1 || true
    echo "removed ${NAME}"
    ;;
  url)
    echo "${URL}"
    ;;
  *)
    echo "usage: $0 {up|down|url}" >&2
    exit 2
    ;;
esac

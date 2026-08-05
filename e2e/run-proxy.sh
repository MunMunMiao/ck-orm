#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="${CK_ORM_PROXY_E2E_PROJECT:-ck-orm-proxy-e2e}"
COMPOSE_ARGS=(-p "$PROJECT_NAME" -f "$ROOT_DIR/e2e/docker-compose.yml" -f "$ROOT_DIR/e2e/docker-compose.proxy.yml")
CERT_DIR="$ROOT_DIR/e2e/certs"

cleanup() {
  if [[ "${KEEP_CK_ORM_E2E:-0}" == "1" ]]; then
    return
  fi
  docker compose "${COMPOSE_ARGS[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -f "$CERT_DIR/server.key" "$CERT_DIR/server.crt"
}

trap cleanup EXIT

mkdir -p "$CERT_DIR"
openssl req -x509 -newkey rsa:2048 -sha256 -days 1 -nodes \
  -keyout "$CERT_DIR/server.key" -out "$CERT_DIR/server.crt" \
  -subj "/CN=clickhouse" >/dev/null 2>&1
chmod 600 "$CERT_DIR/server.key"

docker compose "${COMPOSE_ARGS[@]}" up -d --build --wait --wait-timeout 60 clickhouse proxy-topology
docker compose "${COMPOSE_ARGS[@]}" run --rm --build seed
docker compose "${COMPOSE_ARGS[@]}" run --rm --no-deps e2e \
  bun test e2e/write-paths.e2e.test.ts \
  -t "writes array rows through insertJsonEachRow|uploads a production-sized async user scope" \
  --timeout 30000

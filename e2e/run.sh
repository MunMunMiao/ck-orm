#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/e2e/docker-compose.yml"
PROJECT_NAME="${CK_ORM_E2E_PROJECT:-ck-orm-e2e}"
CLICKHOUSE_USER="${CK_ORM_E2E_USER:-e2e}"
CLICKHOUSE_PASSWORD="${CK_ORM_E2E_PASSWORD:-e2e_password}"

cleanup() {
  if [[ "${KEEP_CK_ORM_E2E:-0}" == "1" ]]; then
    return
  fi
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" up -d --build clickhouse

healthy=0
for _ in $(seq 1 60); do
  if docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T clickhouse \
    clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT 1" >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep 1
done

if [[ "$healthy" != "1" ]]; then
  echo "ClickHouse did not become healthy in time" >&2
  exit 1
fi

docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" run --rm --build seed
docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" run --rm --no-deps --build e2e bun test e2e/dataset-smoke.e2e.test.ts
docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" run --rm --no-deps --build e2e

# `ck-orm` E2E

This directory contains the real `ck-orm` end-to-end suite. It is not a unit test directory.

`bun run test:orm` only runs the ORM unit tests under `src/` and does not execute these E2E tests. The E2E suite connects to a real ClickHouse instance only when the `CLICKHOUSE_E2E_*` environment variables are present or when you run the bundled E2E script.

The suite does the following:

1. Starts a temporary ClickHouse 26.3 LTS instance with Docker Compose
2. Recreates the database and seeds a large deterministic dataset programmatically
3. Covers the public runtime API, schema DSL, and complex SQL scenarios in `ck-orm`
4. Runs the full E2E suite inside an `oven/bun:latest` container
5. Cleans up containers and volumes automatically after validation

Repo-local E2E files import the public API through [`./ck-orm.ts`](./ck-orm.ts), so the suite can run inside this repository without depending on Bun package self-resolution. Direct imports from `../src/*` are only kept for internal test-only helpers that are intentionally not part of the public API.

## One-command execution

Run from the project root:

```bash
bun run test:e2e
```

Or directly:

```bash
bash ./e2e/run.sh
```

## Environment model

This repository uses two different environment-variable groups on purpose:

- `CK_ORM_E2E_*`: controls the local Docker/Compose harness in [`run.sh`](./run.sh)
  - `CK_ORM_E2E_PROJECT`
  - `CK_ORM_E2E_USER`
  - `CK_ORM_E2E_PASSWORD`
  - `KEEP_CK_ORM_E2E`
- `CLICKHOUSE_E2E_*`: tells the test code how to connect to ClickHouse
  - `CLICKHOUSE_E2E_URL`
  - `CLICKHOUSE_E2E_DATABASE`
  - `CLICKHOUSE_E2E_USERNAME`
  - `CLICKHOUSE_E2E_PASSWORD`

When you run `bash ./e2e/run.sh`, the harness variables control container startup and the script injects the `CLICKHOUSE_E2E_*` values into the `seed` and `e2e` containers for you.

When you point the E2E suite at an already-running ClickHouse instance, you usually only need the `CLICKHOUSE_E2E_*` variables.

## Standard execution flow

Each E2E run follows the same sequence:

1. `docker compose` starts a temporary ClickHouse instance
2. The `seed` service runs `e2e/seed.ts`
   - drops the old database
   - recreates `ck_orm_e2e`
   - creates the schema round-trip tables and scenario tables
   - generates deterministic test data programmatically
   - keeps the scenario dataset at a fixed six-figure scale
3. `dataset-smoke.e2e.test.ts` runs first
   - if row counts, key distributions, or deterministic invariants fail, the rest of the suite stops immediately
4. If smoke passes, the full `bun test e2e` run starts
   - this includes happy-path coverage, advanced analytics scenarios, and `error-contracts.e2e.test.ts`
5. `run.sh` reports success or failure from the final exit code
6. `docker compose down -v --remove-orphans` runs by default

## Coverage areas

The current E2E suite covers:

- dataset smoke
  - counts, distributions, invariants, CDC physical rows vs logical rows
- schema round-trip
  - all column factories
  - `chTable`
  - `alias`
  - aliased table interpolation in `` sql`...` ``
- basic API
  - `sql('...')`
  - `` sql`...` ``
  - `sql.raw`
  - `sql.join`
  - `sql.identifier`
  - `decodeRow`
  - `createSessionId`
- query and builder behavior
  - `and`, `or`, `not`
  - `eq`, `ne`, `gt`, `gte`, `lt`, `lte`
  - `like`, `notLike`, `ilike`, `notIlike`
  - `escapeLike`
  - `between`
  - `inArray`, `notInArray`
  - `exists`, `notExists`
  - `Predicate[]` with variadic `.where(...predicates)` / `.count(...predicates)`
  - `asc`, `desc`
  - `expr`
  - `select`, `insert`
  - `from`, `innerJoin`, `leftJoin`
  - `where`, `groupBy`, `having`
  - `orderBy`, `limit`, `offset`, `limitBy`
  - `with`, `$with`, `as`, `final`, `iterator`
- count coverage
  - `db.count(table)`
  - `db.count(table, ...predicates)`
  - `db.count(subqueryOrCte)`
  - `db.count(...).as('alias')`
  - session-scoped counts over temporary tables and `FINAL` subqueries
- function coverage
  - all `fn.*`
  - `tableFn.call('numbers', ...)`
- SQL injection coverage by context
  - foundations
    - classic payloads in builder equality filters
    - classic payloads in raw template literals
    - Unicode line separators passed as parameter values
  - value contexts
    - `inArray(...)`
    - `like`, `notLike`, `ilike`, `notIlike`
    - `escapeLike(...)` for literal `%` and `_`
  - identifier contexts
    - `sql.identifier('...')`
    - `sql.identifier({ table, column, as })`
    - `alias(...)`
    - temporary table names
    - `fn.withParams(...)` function names
    - `tableFn.call(...)` function names
  - raw SQL contexts
    - `execute(string)`
    - `command(string)`
    - `sql.raw(...)` fragments that would create stacked statements
    - no-mutation checks after rejected stacked-query attempts
  - transport and trusted-only boundaries
    - `query_params` key validation
    - reserved `orm_param*` prefix rejection
    - `query_id` / `session_id` validation
    - per-request `session_timeout` and continued-session `session_check`
    - `createTemporaryTable(name, definition)` single-statement boundary
    - `sql.join(...)` separator validation
- advanced ClickHouse SQL
  - scalar `WITH`
  - `ARRAY JOIN`
  - window functions
  - `ASOF JOIN`
  - `INSERT INTO ... SELECT`
  - multi-CTE report queries
- session and temporary tables
  - `runInSession()`
  - `registerTempTable()`
  - `createTemporaryTable()`
  - session cleanup
- observability
  - `logger`
  - `tracing`
  - `instrumentation`
- error contracts
  - invalid SQL syntax
  - missing tables
  - accessing a temporary table after the session ends
  - `insertJsonEachRow()` type mismatch
  - validating `ExceptionBeforeStart` and `ExceptionWhileProcessing` through `system.query_log`
- transport behavior
  - POST-only fetch runtime
  - response compression

See [api-matrix.md](./api-matrix.md) for the API-to-test coverage map.

## Dataset notes

- large scenario tables are generated programmatically and are not stored in the repository
- fixed dataset sizes currently include:
  - `users`: 5,000
  - `pets`: 8,000
  - `web_events`: 100,000
  - `reward_events`: 24,000 physical rows
  - `trade_fills`: 20,000
  - `quote_snapshots`: 40,000
- experimental settings required by `JSON`, `Dynamic`, `QBit`, and related types are provided by the default E2E client settings

## Debugging

If you want to keep containers and the database around for inspection after a failure:

```bash
KEEP_CK_ORM_E2E=1 bash ./e2e/run.sh
```

After keeping the environment, you can run:

```bash
docker compose -f ./e2e/docker-compose.yml ps
docker compose -f ./e2e/docker-compose.yml logs clickhouse
docker compose -f ./e2e/docker-compose.yml run --rm e2e
```

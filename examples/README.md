# `examples/`

This directory is the fastest way to learn how `ck-orm` is meant to be read and used inside an application.

## Reading order

If this is your first time in the repository, start with:

1. [`schema-and-types.ts`](./schema-and-types.ts)
2. [`basic-select.ts`](./basic-select.ts)
3. [`params-and-insert.ts`](./params-and-insert.ts)
4. [`raw-sql.ts`](./raw-sql.ts)
5. [`json-array-functions.ts`](./json-array-functions.ts)
6. [`session-temp-table.ts`](./session-temp-table.ts)
7. [`cte-and-subquery.ts`](./cte-and-subquery.ts)

After that, move on to the larger scenario examples:

- [`advanced-compiled-query.ts`](./advanced-compiled-query.ts)
- [`activity-monthly-export.ts`](./activity-monthly-export.ts)
- [`count-and-errors.ts`](./count-and-errors.ts)
- [`cross-system-order-enrichment.ts`](./cross-system-order-enrichment.ts)
- [`fulfillment-order-lifecycle.ts`](./fulfillment-order-lifecycle.ts)
- [`joins-and-settings.ts`](./joins-and-settings.ts)
- [`large-scope-session.ts`](./large-scope-session.ts)
- [`runtime-observability.ts`](./runtime-observability.ts)

## API map

| Need | File |
| --- | --- |
| Define schemas, table options, DDL metadata, inferred model types | [`schema-and-types.ts`](./schema-and-types.ts) |
| Build common `select` queries with filters, grouping, ordering, `FINAL` | [`basic-select.ts`](./basic-select.ts) |
| Insert rows and use raw `query_params` | [`params-and-insert.ts`](./params-and-insert.ts) |
| Use `csql`, identifiers, raw execution, and table functions | [`raw-sql.ts`](./raw-sql.ts) |
| Use `jsonExtract`, `hasAny`, array helpers, `arrayJoin`, `arrayZip`, `tupleElement` | [`json-array-functions.ts`](./json-array-functions.ts) |
| Compose CTEs, subqueries, and joins | [`cte-and-subquery.ts`](./cte-and-subquery.ts) |
| Choose left join null semantics and override settings | [`joins-and-settings.ts`](./joins-and-settings.ts) |
| Use `runInSession` and structured temporary tables | [`session-temp-table.ts`](./session-temp-table.ts) |
| Handle large filter scopes with temporary tables and streaming | [`large-scope-session.ts`](./large-scope-session.ts) |
| Use runtime methods, endpoint helpers, logger, instrumentation | [`runtime-observability.ts`](./runtime-observability.ts) |
| Integrate a precompiled query with `executeCompiled`, `iteratorCompiled`, `ck.decodeRow`, `ck.createSessionId` | [`advanced-compiled-query.ts`](./advanced-compiled-query.ts) |
| Pick a count mode and handle `ck-orm` errors | [`count-and-errors.ts`](./count-and-errors.ts) |
| Combine rows from two independent ClickHouse clients | [`cross-system-order-enrichment.ts`](./cross-system-order-enrichment.ts) |
| Model a multi-CTE lifecycle/report query | [`fulfillment-order-lifecycle.ts`](./fulfillment-order-lifecycle.ts) |

## Why examples import `./ck-orm`

[`ck-orm.ts`](./ck-orm.ts) is a repo-local shim that re-exports the package root API from `src/public_api.ts`.

Examples import from `./ck-orm` so they look the same as published-package usage without depending on Bun package self-resolution during local repository development.

If you are reading the code to understand the real package boundary, treat `./ck-orm` as “the package root”.

## About `schema/*`

[`./schema/`](./schema) contains example-only schemas that keep the runnable examples readable without mixing example imports with unit-test fixtures.

## Running examples

These examples are type-checkable teaching examples. They are designed to be read first and adapted second. Most files create a client with placeholder local connection settings such as `http://127.0.0.1:8123` and `"<password>"`.

To run one against your own ClickHouse database, first create the tables described in [`schema/`](./schema), update the connection settings in the example, then call one of the exported `run*Example()` helpers or import a `build*Example()` helper into your own scratch file.

Files with `build*Example()` return a query or insert builder for inspection and composition. Files with `run*Example()` execute the query explicitly with `.execute()`, `execute(...)`, `stream(...)`, or another runtime method.

To type-check the examples without connecting to ClickHouse:

```bash
bun run type-check:examples
```

# `examples/`

This directory is the fastest way to learn how `ck-orm` is meant to be read and used inside an application.

## Reading order

If this is your first time in the repository, start with:

1. [`basic-select.ts`](./basic-select.ts)
2. [`params-and-insert.ts`](./params-and-insert.ts)
3. [`raw-sql.ts`](./raw-sql.ts)
4. [`session-temp-table.ts`](./session-temp-table.ts)
5. [`cte-and-subquery.ts`](./cte-and-subquery.ts)

After that, move on to the larger scenario examples:

- [`activity-monthly-export.ts`](./activity-monthly-export.ts)
- [`cross-system-order-enrichment.ts`](./cross-system-order-enrichment.ts)
- [`fulfillment-order-lifecycle.ts`](./fulfillment-order-lifecycle.ts)
- [`joins-and-settings.ts`](./joins-and-settings.ts)
- [`large-scope-session.ts`](./large-scope-session.ts)

## Why examples import `./ck-orm`

[`ck-orm.ts`](./ck-orm.ts) is a repo-local shim that re-exports the package root API from `src/public_api.ts`.

Examples import from `./ck-orm` so they look the same as published-package usage without depending on Bun package self-resolution during local repository development.

If you are reading the code to understand the real package boundary, treat `./ck-orm` as “the package root”.

## About `schema/*`

[`./schema/`](./schema) contains example-only schemas that keep the runnable examples readable without mixing example imports with unit-test fixtures.

## Running examples

These examples are designed to be read first and adapted second. Most files create a client with placeholder local connection settings such as `http://127.0.0.1:8123` and `"<password>"`.

To run one, update the connection settings in the example and call the exported helper that starts with `run*Example()`, or import the builder helper into your own scratch file.

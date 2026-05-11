# NewJSON events

> Demonstrates: **ClickHouse 24.x+ JSON type** with typed paths, SKIP rules,
> and the ck-orm path-access DSL.
> Source: <https://clickhouse.com/docs/sql-reference/data-types/newjson>

## What this example tests

A typical event-ingestion table whose `payload` is half-structured: a few
hot paths are known (and worth storing as typed sub-columns), the rest is
truly dynamic. ClickHouse 24.x's `JSON` type does exactly that without
giving up the "drop in any object" ergonomics.

## ck-orm features exercised

- `ckType.json<T>(name, config)` parameterized DDL — `max_dynamic_paths`,
  `max_dynamic_types`, `typeHints`, `skip`, `skipRegexp` in one call.
- **typeHints route through ck-orm column factories** —
  `user_id: ckType.uint64()` makes `payload.user_id` decode to a lossless
  string just like a top-level `uint64` column would.
- Path-access methods on the column: `.path("user_id")`,
  `.path("session.tier")`, `.castPath(...)`, `.subobject("session")`.
- Compile-time validation: `Paths<T>` rejects typos in the path argument.
- Runtime guard: top-level non-object inserts fail with a column-scoped
  client-side error before they reach ClickHouse.

## Key queries (in `index.ts`)

- `buildNewjsonRevenueQuery(userId)` — select typed path values
  (`payload.user_id`, `payload.action`, `payload.session.tier`) and filter
  on a typed path in WHERE.
- `buildNewjsonSubobjectQuery()` — pull `payload.^session` as a JSON
  sub-object, useful when downstream code wants the whole nested object.

## Why ClickHouse's new JSON

Most events have 80% shared structure (user, action, timestamps) and 20%
event-specific extras. Storing the whole payload as a `String` blob makes
the hot 80% expensive to read; modeling every field as its own column
makes the 20% impossible to evolve. NewJSON's "typed paths become real
sub-columns, the rest is dynamic" model captures both.

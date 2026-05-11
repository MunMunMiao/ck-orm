# Uber schema-agnostic logs

> Replicates: **Uber's schema-agnostic logging platform**
> Source: <https://www.uber.com/en-NL/blog/logging/>

## What this example tests

Uber stores every log line as parallel KV arrays (`string_keys` + `string_values`,
`number_keys` + `number_values`). A handful of high-frequency fields
(`request_id`, `user_id`, `trip_id`) are surfaced as **materialized columns**
extracted via `string_values[indexOf(string_keys, 'foo')]` — ClickHouse
transparently uses the materialized column whenever the user queries it.

## ck-orm features exercised

- `ckType.array(ckType.lowCardinality(ckType.string()))` for the KV key arrays
- `ckType.array(ckType.float64()).codec(ckSql\`Gorilla, ZSTD(1)\`)` for KV number values
- **`.materialized(ckSql\`string_values[indexOf(string_keys, 'request_id')]\`)`** —
  the hot lookup path Uber added on top of the schema-agnostic format
- Physical column names with leading underscores (`_source`, `_namespace`)

## Key queries (in `index.ts`)

- `buildUberLookupByRequestId(requestId)` — point-query a single `request_id`
  using the materialized column, which makes the bloom-filter skip-index
  (added in production via `ckSql`) viable.

## Why ClickHouse

The parallel-array KV pattern keeps file count bounded (vs. a column per
attribute) while still letting Uber observability promote hot fields to first-
class columns without rewriting historical data — the materialized column
expression is evaluated lazily on read.

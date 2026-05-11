# Cloudflare HTTP analytics

> Replicates: **Cloudflare's 6M requests/sec HTTP analytics**
> Source: <https://blog.cloudflare.com/http-analytics-for-6m-requests-per-second-using-clickhouse/>

## What this example tests

Cloudflare summarises 6M+ requests/sec into a minute-bucketed
`SummingMergeTree` table. The engine itself sums the counter columns
during background merges, so the dashboards never need to scan raw
request logs — they read the already-aggregated minute table.

## ck-orm features exercised

- **`engine: ckSql\`SummingMergeTree((${table.requests}, ${table.bytes}, ${table.cached_requests}, ${table.ssl_requests}))\``**
  — engine expression with the list of counter columns
- `settings: { index_granularity: 32 }` — Cloudflare uses small granularity for
  the minute table because the aggregate row count is small but read-heavy
- `LowCardinality(String)` for the country / content_type / threat_type axes

## Key queries (in `index.ts`)

- `buildCloudflareTrafficByCountryExample(zoneId)` — total requests + bytes
  per country, the bread-and-butter Cloudflare zone dashboard row.

## Why ClickHouse

The original pipeline used Citus + Postgres and hit a wall at ~6M req/s.
ClickHouse SummingMergeTree + Materialized Views ingested the same data with
zero pre-aggregation logic, and `index_granularity=32` on the small minute-
level table dropped report latency from seconds to milliseconds.

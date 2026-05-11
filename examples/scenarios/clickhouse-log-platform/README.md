# ClickHouse internal logging platform

> Replicates: **ClickHouse Inc.'s own 19 PiB OpenTelemetry log store**
> Source: <https://clickhouse.com/blog/building-a-logging-platform-with-clickhouse-and-saving-millions-over-datadog>

## What this example tests

A wide OTel `log` table indexed by `(pod_name, timestamp)`, partitioned by
event date, with a 180-day TTL on `event_time`. The original production
schema runs on `SharedMergeTree` (ClickHouse Cloud); we substitute
`MergeTree` so the schema is portable to a single-node ClickHouse.

## ck-orm features exercised

- `ckType.lowCardinality(ckType.string())` for high-cardinality service /
  region / pod labels — dictionary encoded, typical 14–30× compression
- `ckType.map(ckType.string(), ckType.string())` for OTel resource / log
  attributes (no DDL needed when fields evolve)
- `.codec(ckSql\`Delta(8), ZSTD(1)\`)` on the high-precision timestamp
- Table-level `partitionBy: table.event_date`, `orderBy: [pod_name, timestamp]`,
  `ttl: ckSql\`event_time + toIntervalDay(180)\``

## Key queries (in `index.ts`)

- `buildClickhouseLogErrorsByService()` — top services by error count from
  `SeverityText IN ('ERROR', 'CRITICAL', 'FATAL')`, the canonical "what
  service is on fire right now?" dashboard query.

## Why ClickHouse

37 trillion rows / 19 PiB on a single SharedMergeTree-backed table,
LowCardinality compression keeps the storage bill low, and tokenbf_v1 full-text
skip indexes (omitted here for portability) make `hasToken(body, 'OOM')`
sub-second. The platform replaced Datadog and saved Anthropic-scale dollars.

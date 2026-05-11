# PostHog product analytics

> Replicates: **PostHog `sharded_events`**
> Source: <https://posthog.com/handbook/engineering/clickhouse/schema/sharded-events>

## What this example tests

PostHog stores every front-end event in a single `ReplacingMergeTree` table.
The schema also extracts hot JSON properties (`$session_id`, `$window_id`) as
materialized columns for cheap session-stitched analytics.

## ck-orm features exercised

- `ckType.uuid()` event ids
- `ckType.string().codec(ckSql\`ZSTD(3)\`)` — `properties` is raw JSON, ZSTD(3)
  is the PostHog default
- **`engine: "ReplacingMergeTree"` + `versionColumn: table._timestamp`** —
  idempotent Kafka ingest
- `sampleBy: ckSql\`cityHash64(distinct_id)\`` — `SAMPLE 0.1` accelerates ad-hoc
  funnel queries by 10×
- Materialized columns from JSON: `replaceRegexpAll(JSONExtractRaw(...), '^"|"$', '')`

## Key queries (in `index.ts`)

- `buildPosthogFunnelExample(teamId, distinctId)` — `windowFunnel(86400)(...)`
  computing how many steps of `signed_up → checkout → payment` a user reached.

## Why ClickHouse

ReplacingMergeTree solves at-least-once Kafka ingest without bespoke dedup.
The `windowFunnel` aggregate ships with ClickHouse (no UDF needed), and
SAMPLE BY lets PostHog answer "how many users hit step 3" on a billion-row
event table in interactive time.

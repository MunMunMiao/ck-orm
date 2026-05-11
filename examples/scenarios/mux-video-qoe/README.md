# Mux video QoE views

> Replicates: **Mux's CollapsingMergeTree video-views table**
> Source: <https://www.mux.com/blog/from-russia-with-love-how-clickhouse-saved-our-data>

## What this example tests

Mux stores every video playback "view" with ~100 dimensions (browser, CDN,
country) and ~100 quality metrics (rebuffer count, startup time, bitrate).
Because a view is mutated as the playback continues, the schema uses
`CollapsingMergeTree(sign)` — write `Sign=-1` for the old state plus
`Sign=+1` for the new state, and let the background merge cancel them.

## ck-orm features exercised

- **`engine: ckSql\`CollapsingMergeTree(\${table.sign})\``** — custom engine
  expression that references a column from the schema
- `ckType.enum8({ vod: 1, live: 2, dvr: 3 })` — type-safe streaming kind, with
  the literal union inferred automatically via ck-orm's const-generic enum
- `ckType.nullable(ckType.dateTime())` + `ckType.nullable(ckType.uint16())` —
  optional end-time / error-type columns

## Key queries (in `index.ts`)

- `buildMuxCdnQualityExample()` — sum the sign-weighted rebuffer counts per
  CDN. Critical for picking the best CDN for a region.

## Why ClickHouse

CollapsingMergeTree replaces the painful "delete + reinsert" pattern with a
two-row write. The columnar storage lets Mux read just 5 of 200 columns for a
typical QoE report, keeping P99 dashboard latency under a second.

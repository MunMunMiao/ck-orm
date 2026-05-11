# ML feature store

> Replicates: **Online + offline ML feature store on ClickHouse**
> Source: <https://clickhouse.com/blog/modeling-machine-learning-data-in-clickhouse>

## What this example tests

Raw user behaviour events feed both online inference (sub-10ms feature
lookups) and offline training (point-in-time correct historical features
via `ASOF JOIN`). This example captures the raw event table that backs the
feature engineering pipeline.

## ck-orm features exercised

- `ckType.lowCardinality(ckType.string())` — domain / referer category
- `ckType.uint32()` IP address column packed into one integer
- `.codec(ckSql\`DoubleDelta, ZSTD(1)\`)` on the contiguous event timestamps
- ORDER BY `(domain, user_id, event_ts)` so domain-wide aggregations only
  scan the relevant prefix of the sort key

## Key queries (in `index.ts`)

- `buildMlFeatureWindowExample()` — per-domain unique IPs + bounce rate over
  the configured window. This is exactly the shape of "feature served at
  inference time" — one row per entity, two/three derived columns.

## Why ClickHouse

The same table backs both the online serving path (sub-10ms `SELECT` for one
domain's last-hour features) and the offline training path (full-history
`ASOF JOIN` to attach point-in-time-correct features to labels). One store,
no Feast / Redis split.

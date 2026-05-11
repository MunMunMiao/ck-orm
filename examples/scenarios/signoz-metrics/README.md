# SigNoz Prometheus-compatible metrics

> Replicates: **SigNoz `samples_v4` time series store**
> Source: <https://signoz.io/docs/userguide/write-a-metrics-clickhouse-query/>

## What this example tests

A Prometheus-compatible metric samples table. Every sample is keyed by
`(env, temporality, metric_name, fingerprint)`, where `fingerprint` is a
UInt64 hash of the full label set — so the on-disk row stays small even when
the labels are high-cardinality.

## ck-orm features exercised

- `ckType.uint64().codec(ckSql\`Delta(8), ZSTD(1)\`)` — fingerprint with Delta
- `ckType.int64().codec(ckSql\`DoubleDelta, ZSTD(1)\`)` — monotonically increasing
  unix-millis column compresses to nearly nothing
- **`ckType.float64().codec(ckSql\`Gorilla, ZSTD(1)\`)`** — Gorilla codec was
  designed for time-series floats (Facebook 2015) and easily reaches 8× over LZ4
- `.default("'default'")` for an env DSL DEFAULT clause

## Key queries (in `index.ts`)

- `buildSignozMetricAverageByFingerprint(metricName)` — average value per
  fingerprint, the building block of any PromQL `avg() by (...)` query.

## Why ClickHouse

A single SigNoz node handles ~1M samples/sec ingest. Gorilla + DoubleDelta
typically gets ClickHouse below 1.5 bytes per sample on disk; ReplacingMergeTree
on the separate `time_series_v4` label-set table makes a UPSERT model possible
without any external coordination.

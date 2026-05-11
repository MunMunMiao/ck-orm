# SigNoz distributed traces

> Replicates: **SigNoz `signoz_index_v3` table**
> Source: <https://signoz.io/docs/userguide/writing-clickhouse-traces-query/>

## What this example tests

A trace-span schema modelled on SigNoz's APM backend: every span carries an
hour-bucketed `ts_bucket_start`, a `resource_fingerprint` for cheap label-set
deduplication, and three typed attribute maps (string / number / bool).

## ck-orm features exercised

- `ckType.fixedString({ length: 32 })` — `trace_id` is fixed-width hex
- `ckType.map(string, float64)` / `ckType.map(string, bool)` — typed attribute maps
- `ckType.bool().codec(ckSql\`T64, ZSTD(1)\`)` — boolean flag with T64 codec
- Table `orderBy: [ts_bucket_start, resource_fingerprint, has_error, name, timestamp]`
  shrinks the row scan window from full table → just rows in a one-hour bucket

## Key queries (in `index.ts`)

- `buildSignozErrorRateByService()` — `countIf(has_error)` over a `service_name`
  group-by, the foundation of an APM error-rate panel.

## Why ClickHouse

SigNoz needs to render millions of spans per service in under a second.
`ts_bucket_start` slicing limits the scan to one-hour partitions, T64 codec on
`duration_nano` compresses the wide-range UInt64 column 5×+, and the per-attr
Maps avoid hundreds of nullable columns.

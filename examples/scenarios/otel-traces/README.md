# OpenTelemetry Collector ClickHouse exporter

> Replicates: **OTel Collector `clickhouseexporter` trace table**
> Source: <https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse>

## What this example tests

The OpenTelemetry Collector's reference trace schema, also used by HyperDX and
ClickStack. We keep the parallel array shape for `Events.*` (timestamp + name)
that ClickHouse uses instead of a side table.

## ck-orm features exercised

- `ckType.array(ckType.dateTime64({ precision: 9, timezone: "UTC" }))` — array
  of event timestamps, no `JOIN` needed
- `ckType.array(ckType.lowCardinality(ckType.string()))` — array of LowCardinality
  event names; ClickHouse dictionary-encodes the inner strings
- `Map<String, String>` for `ResourceAttributes` and `SpanAttributes`

## Key queries (in `index.ts`)

- `buildOtelTraceById(traceId)` — assemble the full span tree for one trace,
  ordered by timestamp. Powers the flamegraph UI.

## Why ClickHouse

The Collector emits hundreds of thousands of spans/sec. ClickHouse's array
columns let one row carry an entire span's events (no extra table to join), and
the bloom-filter skip-index on `TraceId` (omitted here) makes single-trace
lookups sub-millisecond on a wide table.

# EMQ MQTT industrial IoT telemetry

> Replicates: **EMQ NeuronEX edge gateway → ClickHouse Cloud pipeline**
> Source: <https://clickhouse.com/blog/emq-ai-assisted-analytics>

## What this example tests

Each row is one OPC-UA / Modbus / MQTT telemetry reading from a factory
sensor — temperature, vibration, output-count, etc. EMQ supports 100M
concurrent device connections, with the telemetry flowing into ClickHouse
Cloud at hundreds of thousands of samples/sec.

## ck-orm features exercised

- `ckType.nullable(ckType.float64()).codec(ckSql\`Gorilla, ZSTD(1)\`)` —
  optional float metric with Gorilla codec (3-5× compression on continuous
  sensor readings)
- `ckType.nullable(ckType.int64()).codec(ckSql\`ZSTD(9)\`)` /
  `ckType.nullable(ckType.string())` — int / string variants for the same
  `metric_name` column
- `ckType.uint8().default("192")` — OPC-UA quality code with the standard
  "Good" default
- `ckType.map(ckType.string(), ckType.string())` — flexible tag bag
- Compound partition `(plant_id, toYYYYMMDD(ts))` for per-plant data locality

## Key queries (in `index.ts`)

- `buildIotAnomaliesExample(thresholdC)` — devices where `temperature` crossed
  a threshold; the basis for the "factory alerts" widget.

## Why ClickHouse

Gorilla codec on the float column hits 3-5× compression. The Map tag column
lets the schema absorb new sensor types without DDL changes. EMQ's blog
reports 10s dashboard refresh from raw 100M-sample-per-sec ingestion.

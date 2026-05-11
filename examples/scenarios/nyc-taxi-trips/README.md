# NYC TLC taxi trips

> Replicates: **NYC Taxi & Limousine Commission dataset**
> Source: <https://clickhouse.com/docs/getting-started/example-datasets/nyc-taxi>

## What this example tests

The canonical ClickHouse geo + time-series demo dataset — 3+ billion trip
records since 2009, also used by Lyft / Uber for OLAP benchmarking. Powers
demand-forecast modeling and pricing analysis tutorials worldwide.

## ck-orm features exercised

- `ckType.float32()` lat/long geo columns
- **`ckType.decimal({ precision: 8, scale: 2 })`** for every fare / tip / toll
  column — no Float drift on cents
- `ckType.enum8({ CSH: 1, CRE: 2, NOC: 3, DIS: 4, UNK: 5 })` payment type
- `ckType.lowCardinality(ckType.string())` for neighbourhood names
- `.codec(ckSql\`DoubleDelta, ZSTD(1)\`)` on the contiguous pickup_datetime /
  dropoff_datetime columns

## Key queries (in `index.ts`)

- `buildNycPaymentBreakdownExample()` — trips + avg fare + tip % per payment
  type, a classic ClickHouse benchmark query.

## Why ClickHouse

The 3B-row aggregation that ClickHouse closes in < 1 second was the demo
Yandex used to show off the database in 2016, and it remains a common
"how-fast-is-it" pitch. The Decimal columns preserve the cents-level fare
data the TLC ships in CSVs.

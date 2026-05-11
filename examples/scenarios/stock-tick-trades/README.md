# Stock tick (OHLCV) store

> Replicates: **Quant-style 160B-row tick store for Polygon.io / Alpaca data**
> Source: <https://rafalkwasny.com/clickhouse-tick-store>

## What this example tests

Per-trade ticks ingested at hundreds of thousands of msgs/sec. The schema is
the foundation for OHLCV (open-high-low-close-volume) aggregation, VWAP,
spread analysis, etc.

## ck-orm features exercised

- **`ckType.decimal({ precision: 18, scale: 6 })`** price + **`Decimal(38, 8)`**
  size — no Float64 drift on cents / share fractions
- `ckType.array(ckType.fixedString({ length: 2 }))` — Polygon trade
  condition codes
- `ckType.enum8({ NYSE: 1, AMEX: 2, NASDAQ: 3 })` — type-safe tape
- `.codec(ckSql\`DoubleDelta, ZSTD(1)\`)` on the nanosecond timestamps; combined
  with the `received_ts` `ReplacingMergeTree` version column, the table
  absorbs out-of-order quote feeds without dedupe logic

## Key queries (in `index.ts`)

- `buildStockVwapExample()` — VWAP per symbol = `sum(price * size) / sum(size)`,
  the bedrock of execution-quality analysis.

## Why ClickHouse

Two-second OHLCV aggregation over 160B rows; columnar storage means the
typical "ticker symbol over time" query reads <1% of the dataset. Decimal
types preserve sub-cent pricing precision that Float64 would corrupt.

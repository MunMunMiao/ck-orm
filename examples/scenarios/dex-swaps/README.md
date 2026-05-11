# Cross-chain DEX swaps

> Replicates: **Coinhall omnichain DEX analytics platform**
> Source: <https://clickhouse.com/blog/trade-secrets-how-coinhall-uses-clickhouse-to-power-its-blockchain-data-platform>

## What this example tests

Every Uniswap-style swap event from 23 different blockchains in one table.
Coinhall powers a real-time omnichain price feed and arbitrage explorer on
top of this schema (~200K analytics queries/day, 150 TB scanned).

## ck-orm features exercised

- **`ckType.decimal({ precision: 38, scale: 18 })`** — preserves full wei
  precision (~10¹⁸ units, no float drift)
- `ckType.fixedString({ length: 66 })` — Ethereum / Cosmos tx hash
- Compound partitioning `(chain_id, toYYYYMM(block_ts))` so each chain has
  its own partition tree
- `engine: "ReplacingMergeTree"` + `versionColumn: block_height` — handles
  re-orgs by overriding orphaned blocks

## Key queries (in `index.ts`)

- `buildDexLatestPriceExample(tokenIn, tokenOut)` — latest cross-chain price
  for a pair using `argMax(price, block_ts)` per chain.

## Why ClickHouse

Coinhall's ASOF JOIN-based arbitrage queries run 400× faster on ClickHouse
than the previous Snowflake stack, and the cost dropped 40×. Wei-precision
`Decimal(38, 18)` keeps every gwei accurate, which Float would lose.

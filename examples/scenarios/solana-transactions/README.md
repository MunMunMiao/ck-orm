# Solana on-chain transactions

> Replicates: **CryptoHouse — public Solana transaction warehouse**
> Source: <https://clickhouse.com/blog/announcing-cryptohouse-free-blockchain-analytics>

## What this example tests

A full Solana transaction record, including pre/post account balances and a
**Nested** column for token-balance changes. CryptoHouse ingests 3-4K Solana
TPS into a 468 TiB (uncompressed) table backed by `ReplacingMergeTree`.

## ck-orm features exercised

- `ckType.fixedString({ length: 88 })` Solana tx signature, **`FixedString(44)`** account hashes
- `ckType.array(ckType.string())` / `ckType.array(ckType.uint64())` for the
  per-account hash + balance lists
- **`ckType.nested({ account_index, mint, owner, amount_pre, amount_post, decimals })`** —
  one row carries every token-balance change in the transaction, no JOIN
- `engine: "ReplacingMergeTree"` + `versionColumn: block_slot` — Goldsky's
  at-least-once stream → idempotent ingest

## Key queries (in `index.ts`)

- `buildSolanaTopComputeExample(limit)` — top transactions by
  `compute_units_used`, the basic "which programs are expensive" report.

## Why ClickHouse

CryptoHouse keeps 100+ TiB of compressed Solana data free for the public to
query. ReplacingMergeTree handles Solana fork rollbacks transparently;
Nested columns make per-token-balance-change analytics straightforward.

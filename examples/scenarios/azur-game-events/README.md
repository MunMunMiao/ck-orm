# Azur Games mobile game telemetry

> Replicates: **Azur Games' game analytics on ClickHouse Cloud / AWS**
> Source: <https://aws.amazon.com/blogs/gametech/azur-games-migrates-all-game-analytics-data-to-clickhouse-cloud-on-aws/>

## What this example tests

Azur Games (8B+ install hyper-casual mobile games) writes every session start,
level complete, purchase, and match event here. The migration from a
self-hosted ClickHouse cluster to ClickHouse Cloud saved 60% admin time and
absorbed seasonal traffic spikes (Black Friday) on demand.

## ck-orm features exercised

- `ckType.enum8({ ios: 1, android: 2, pc: 3, console: 4 })` — type-safe platform
- **`ckType.lowCardinality(ckType.fixedString({ length: 2 }))`** — ISO-2
  country code, double-compressed
- `ckType.nullable(ckType.bool())` / `ckType.nullable(ckType.decimal(...))` —
  optional outcome / revenue fields
- `ckType.map(ckType.string(), ckType.string())` — per-version event property bag
- Compound partition `(game_id, toYYYYMM(event_ts))` for per-game locality

## Key queries (in `index.ts`)

- `buildGameArpuExample()` — ARPU (average revenue per user) per A/B variant.
- `buildGameLevelPassRateExample(gameId)` — wins vs. attempts per level.

## Why ClickHouse

Mobile games swing 100× between dev and global launch. ClickHouse Cloud
scales automatically; the Map column accepts new game-specific event fields
without `ALTER TABLE`; AggregatingMergeTree-backed retention dashboards keep
the BI cost flat as install count grows.

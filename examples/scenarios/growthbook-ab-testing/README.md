# GrowthBook A/B testing

> Replicates: **GrowthBook + ClickHouse A/B testing data model**
> Source: <https://clickhouse.com/blog/how-growthbook-and-clickhouse-make-enterprise-grade-ab-testing-easy>

## What this example tests

Two tables drive every experiment:

- `growthbook_exposures` — every user/variation assignment event
- `growthbook_conversions` — every downstream "did the metric fire" event

GrowthBook runs Bayesian / frequentist statistics directly on these tables,
so query response time matters: a typical experiment results page joins
millions of exposures against millions of conversions.

## ck-orm features exercised

- Two-table example with a `leftJoin` on `user_id`
- `ckType.map(ckType.string(), ckType.string())` — flexible user-attribute
  bag (`country`, `plan`, …) so analysts can stratify on demand
- `ckType.nullable(ckType.decimal({ precision: 18, scale: 4 }))` — revenue is
  optional on most events

## Key queries (in `index.ts`)

- `buildGrowthbookConversionExample(experimentId)` — per-variation exposed
  user count vs. converted user count.

## Why ClickHouse

No ETL: GrowthBook runs hypothesis tests directly against raw exposure /
conversion rows. ClickHouse's join + aggregation throughput keeps experiment
result pages responsive even at 10M+ users per experiment, and Map columns
let teams strafify on any attribute without ALTER TABLE.

# Yandex.Metrica / ClickBench hits

> Replicates: **Yandex.Metrica `hits` — the wide table that created ClickHouse**
> Source: <https://clickhouse.com/blog/evolution-of-data-structures-in-yandexmetrica>

## What this example tests

A wide (28+ column) web clickstream `hits` table. ClickBench uses a near-
identical subset of these columns as the canonical OLAP benchmark for
ClickHouse vs. competitors. Queries typically `WHERE CounterID = X` then
group-by some attribute.

## ck-orm features exercised

- 28+ columns demonstrating ck-orm's columnar typing without overhead
- `LowCardinality(String)` for high-cardinality categorical fields (browser,
  campaign, region)
- `sampleBy: ckSql\`intHash32(user_id)\`` — Yandex's well-known sampling trick

## Key queries (in `index.ts`)

- `buildMetricaTopCampaignsExample(counterId)` — top UTM campaigns by hit
  count, with `uniqExact(user_id)` for unique visitors.

## Why ClickHouse

Yandex Metrica was the original use case: 18.3 trillion non-aggregated rows,
sub-second interactive reports. The wide table's columnar storage means
typical "top-N by dimension" queries read only the relevant 1–2 columns,
delivering 100× speedups over row stores like Postgres.

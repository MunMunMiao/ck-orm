# Real-time bidding (RTB) ad tracking

> Replicates: **DSP-style ad impressions + clicks tables**
> Source: <https://oneuptime.com/blog/post/2026-03-31-clickhouse-track-ad-impressions-and-clicks/view>

## What this example tests

The two core tables every DSP needs:

- `rtb_ad_impressions` — bid / win pricing, placement, device, geo
- `rtb_ad_clicks` — references the impression via `impression_id`

CPM, CTR, win-rate, ROAS, etc. all derive from these two tables.

## ck-orm features exercised

- `ckType.decimal({ precision: 10, scale: 6 })` — `bid_price` / `win_price`
  in CPM dollars, no float drift
- Materialized `event_date` column derived from `toDate(event_time)`
- Two-table example with `leftJoin` on `impression_id`
- 180-day TTL on the impressions table (GDPR + cost control)

## Key queries (in `index.ts`)

- `buildRtbCtrExample()` — clicks ÷ impressions per campaign, the classic
  RTB CTR report.

## Why ClickHouse

Single-node throughput easily ingests tens of billions of impressions per day,
and a single SQL groupBy returns campaign-level CTR / ROAS in milliseconds —
no Spark / batch ETL needed. Decimal types preserve sub-cent bid precision
that Float64 would corrupt.

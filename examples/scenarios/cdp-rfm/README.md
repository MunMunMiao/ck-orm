# Customer Data Platform (CDP) — RFM segmentation

> Replicates: **E-commerce CDP behavioural events + orders**
> Source: <https://github.com/RafaelAdao/cdp-clickhouse>

## What this example tests

Two tables that anchor every CDP:

- `cdp_user_events` — page views, searches, add-to-cart, checkout, purchase
- `cdp_orders` — final paid orders, with item product-id array

RFM (recency / frequency / monetary) is computed by joining + aggregating
these two tables. The `items: Array(UInt32)` column lets co-purchase analysis
run with a single `arrayJoin`, no junction table.

## ck-orm features exercised

- `ckType.map(ckType.string(), ckType.string())` for flexible event properties
- **`ckType.array(ckType.uint32())`** — order item-id list as a single column
- `engine: "ReplacingMergeTree"` + `versionColumn: updated_at` for the orders
  table — idempotent updates from order-status webhooks
- 2-year TTL on the user events table

## Key queries (in `index.ts`)

- `buildCdpTotalSpentExample()` — total revenue per user across paid orders.
- `buildCdpUserActivityExample(userId)` — event-type counts for one user.

## Why ClickHouse

CDP queries are heavy on group-by + join. ClickHouse's columnar joins +
arrayJoin/`has(items, X)` predicates make co-purchase / segment queries
sub-second even at 100M+ orders. ReplacingMergeTree absorbs the steady
trickle of order-status updates without surprise UPSERT lock contention.

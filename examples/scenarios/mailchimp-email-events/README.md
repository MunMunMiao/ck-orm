# Mailchimp-style email campaign events

> Replicates: **Mailchimp / SendGrid transactional event stream**
> Source: <https://mailchimp.com/developer/transactional/docs/activity-reports/>

## What this example tests

Every email campaign engagement type — sent / delivered / bounced / opened /
clicked / unsubscribed / complained — written as a row. Dashboards count
each event type per campaign to draw the engagement funnel.

## ck-orm features exercised

- **`ckType.enum8({ sent: 1, delivered: 2, bounced: 3, ... })`** — strict
  enum at the storage level, type-narrowed in ck-orm via const-generic enum
- `ckType.uuid()` event ids
- Materialized `event_date` column derived from `toDate(event_time)`
- 365-day TTL on the events table

## Key queries (in `index.ts`)

- `buildEmailFunnelExample(campaignId)` — sent / delivered / opened /
  clicked / bounced counts in one round-trip, perfect for the campaign
  detail page.

## Why ClickHouse

Mailchimp-scale fleets (~100M+ events/day per tenant) need a permanent home
that can stream-aggregate without an OLAP cluster. ClickHouse's columnar
storage with Enum8 + LowCardinality keeps storage cost an order of magnitude
below Snowflake, and the funnel report query is sub-second on raw events.

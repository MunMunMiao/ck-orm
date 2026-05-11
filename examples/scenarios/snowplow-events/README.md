# Snowplow canonical events

> Replicates: **Snowplow Analytics canonical event schema**
> Source: <https://docs.snowplow.io/docs/fundamentals/canonical-event/>

## What this example tests

Snowplow's industry-standard behavioural-event schema (used by Snowplow OSS,
many CDPs, and Mailchimp's behavioural events). Every event carries device-
time, collector-time, page URL, referrer chain, geo, browser, and two ZSTD-
compressed JSON columns for arbitrary context.

## ck-orm features exercised

- `ckType.uuid()` for `event_id`
- `.codec(ckSql\`ZSTD(3)\`)` on the JSON `unstruct_event`, `contexts`,
  `derived_contexts` columns
- `ReplacingMergeTree` keyed on `(app_id, event, collector_tstamp, event_id)`
  with `collector_tstamp` as the version column — at-least-once Kafka dedupe

## Key queries (in `index.ts`)

- `buildSnowplowTrafficSourcesExample()` — count page views + unique visitors
  per referrer medium (search / direct / social / email).

## Why ClickHouse

The Snowplow ClickHouse Sink (Kafka → ClickHouse) replaces multi-day BigQuery
loaders with second-level latency. ReplacingMergeTree + version column lets
the loader be idempotent without an external offset store, and a 180-day TTL
satisfies the GDPR retention requirement.

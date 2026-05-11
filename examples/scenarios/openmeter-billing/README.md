# OpenMeter SaaS usage metering

> Replicates: **OpenMeter usage-based billing platform**
> Source: <https://clickhouse.com/blog/openmeter-real-time-usage-based-billing-powered-by-clickhouse-cloud>

## What this example tests

Every usage event for SaaS / AI-API billing (Stripe / Twilio / OpenAI style):
one row per API call, token batch, gigabyte stored, etc. Idempotency comes
from `idempotency_key`, which lets a Kafka loader retry without double-
charging.

## ck-orm features exercised

- **`ckType.uuid().default("generateUUIDv4()")`** — auto-generated event id
- `ckType.map(ckType.string(), ckType.string())` — per-meter dimension tags
  (`model='gpt-4'`, `region='us-east-1'`)
- Compound `orderBy: [customer_id, meter_slug, event_ts]` — every per-customer
  billing query hits a tight slice of the sort key

## Key queries (in `index.ts`)

- `buildSaasUsageRollupExample()` — usage + event count per customer + meter,
  the data behind a billing-page invoice preview.

## Why ClickHouse

OpenMeter ingests millions of meter events per second on a single cluster and
rolls them up into customer-month aggregates with Materialized Views (the
e2e suite simulates the raw event ingest; the rollup view is left as a
production exercise). At-least-once Kafka delivery is handled by the
`idempotency_key` column + `argMax`-style queries.

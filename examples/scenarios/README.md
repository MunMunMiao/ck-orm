# `examples/scenarios/`

**23 production ClickHouse schemas, replicated with ck-orm.**

Each subdirectory is a self-contained example that replicates a real-world
ClickHouse deployment described in a public blog post, GitHub repository, or
conference talk. Every example contains:

- **`README.md`** — what the example covers, the original source, why
  ClickHouse was chosen, and which ck-orm features it exercises.
- **`index.ts`** — the runnable code: schema re-export plus
  `build*Example()` / `run*Example()` helpers that build typed queries against
  the schema.

The 23 schemas themselves live in [`examples/schema/scenarios.ts`](../schema/scenarios.ts)
so they can be shared with the e2e suite (which seeds and tests each one
against a real ClickHouse server). Each example re-exports its own table from
that central file — the README in each folder points back to the original
source URL so you can study the full production schema.

## Index

### Observability — logs / traces / metrics

| Example | Source | ClickHouse features |
| --- | --- | --- |
| [`clickhouse-log-platform`](./clickhouse-log-platform/) | [ClickHouse: 19 PiB logging](https://clickhouse.com/blog/building-a-logging-platform-with-clickhouse-and-saving-millions-over-datadog) | `LowCardinality`, `Map`, `Codec(ZSTD/Delta)`, table TTL |
| [`signoz-traces`](./signoz-traces/) | [SigNoz APM](https://signoz.io/docs/userguide/writing-clickhouse-traces-query/) | `FixedString`, `Map<String, Float64/Bool>`, `T64`/`DoubleDelta` codec |
| [`otel-traces`](./otel-traces/) | [OpenTelemetry Collector exporter](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse) | `Array(DateTime64)`, `Array(LowCardinality)`, MergeTree TTL |
| [`signoz-metrics`](./signoz-metrics/) | [SigNoz metrics](https://signoz.io/docs/userguide/write-a-metrics-clickhouse-query/) | `Gorilla` / `DoubleDelta` codecs, fingerprint pattern |
| [`highlight-logs`](./highlight-logs/) | [Highlight.io](https://highlight.io/blog/how-we-built-logging-with-clickhouse) | OTel-style log schema, partitioned + TTL |
| [`uber-schema-agnostic-logs`](./uber-schema-agnostic-logs/) | [Uber engineering](https://www.uber.com/en-NL/blog/logging/) | Parallel-array KV pattern, **materialized columns** |

### Product analytics — events / sessions / video

| Example | Source | ClickHouse features |
| --- | --- | --- |
| [`posthog-events`](./posthog-events/) | [PostHog `sharded_events`](https://posthog.com/handbook/engineering/clickhouse/schema/sharded-events) | `ReplacingMergeTree`, `SAMPLE BY`, materialized columns from JSON |
| [`metrica-hits`](./metrica-hits/) | [Yandex.Metrica / ClickBench](https://clickhouse.com/blog/evolution-of-data-structures-in-yandexmetrica) | Wide table (28+ cols), `SAMPLE BY intHash32`, `LowCardinality` |
| [`mux-video-qoe`](./mux-video-qoe/) | [Mux ClickHouse story](https://www.mux.com/blog/from-russia-with-love-how-clickhouse-saved-our-data) | `CollapsingMergeTree(sign)`, `Enum8`, `Nullable` |
| [`snowplow-events`](./snowplow-events/) | [Snowplow canonical event](https://docs.snowplow.io/docs/fundamentals/canonical-event/) | `ReplacingMergeTree(collector_tstamp)`, ZSTD compressed JSON strings |

### Marketing & e-commerce

| Example | Source | ClickHouse features |
| --- | --- | --- |
| [`cloudflare-http-requests`](./cloudflare-http-requests/) | [Cloudflare HTTP analytics](https://blog.cloudflare.com/http-analytics-for-6m-requests-per-second-using-clickhouse/) | `SummingMergeTree((requests, bytes, ...))`, `index_granularity=32` |
| [`growthbook-ab-testing`](./growthbook-ab-testing/) | [GrowthBook + ClickHouse](https://clickhouse.com/blog/how-growthbook-and-clickhouse-make-enterprise-grade-ab-testing-easy) | Two-table join, `Map<String, String>` attributes |
| [`rtb-ad-tracking`](./rtb-ad-tracking/) | [RTB ad tracking](https://oneuptime.com/blog/post/2026-03-31-clickhouse-track-ad-impressions-and-clicks/view) | Impressions + clicks join, `Decimal(10, 6)` for bid/win price |
| [`mailchimp-email-events`](./mailchimp-email-events/) | [Mailchimp activity reports](https://mailchimp.com/developer/transactional/docs/activity-reports/) | `Enum8` event type, materialized `event_date` column |
| [`cdp-rfm`](./cdp-rfm/) | [CDP ClickHouse demo](https://github.com/RafaelAdao/cdp-clickhouse) | `Map<String, String>` flexible attributes, `Array(UInt32)` items, `ReplacingMergeTree` orders |

### Finance / crypto / time-series

| Example | Source | ClickHouse features |
| --- | --- | --- |
| [`stock-tick-trades`](./stock-tick-trades/) | [Quant ClickHouse tick store](https://rafalkwasny.com/clickhouse-tick-store) | `Decimal(18,6)` price, `Decimal(38,8)` size, `DoubleDelta` nanosecond timestamps |
| [`dex-swaps`](./dex-swaps/) | [Coinhall DEX analytics](https://clickhouse.com/blog/trade-secrets-how-coinhall-uses-clickhouse-to-power-its-blockchain-data-platform) | `Decimal(38, 18)` wei precision, `FixedString(66)` tx hash, multi-chain partitions |
| [`solana-transactions`](./solana-transactions/) | [CryptoHouse Solana](https://clickhouse.com/blog/announcing-cryptohouse-free-blockchain-analytics) | `FixedString(44/88)`, `Array(UInt64)` balances, **`Nested(...)`** token balance changes |
| [`nyc-taxi-trips`](./nyc-taxi-trips/) | [NYC Taxi dataset](https://clickhouse.com/docs/getting-started/example-datasets/nyc-taxi) | Geo columns + `Decimal(8,2)` fares, `Enum8` payment type, `LowCardinality(neighborhood)` |

### IoT / Gaming / SaaS / ML

| Example | Source | ClickHouse features |
| --- | --- | --- |
| [`emq-iot-telemetry`](./emq-iot-telemetry/) | [EMQ MQTT + ClickHouse](https://clickhouse.com/blog/emq-ai-assisted-analytics) | `Gorilla` codec floats, `Nullable<Float64/Int64/String>` union values, `Map` tags |
| [`azur-game-events`](./azur-game-events/) | [Azur Games on AWS](https://aws.amazon.com/blogs/gametech/azur-games-migrates-all-game-analytics-data-to-clickhouse-cloud-on-aws/) | `LowCardinality(FixedString(2))` country code, `Map`-flex properties |
| [`openmeter-billing`](./openmeter-billing/) | [OpenMeter usage billing](https://clickhouse.com/blog/openmeter-real-time-usage-based-billing-powered-by-clickhouse-cloud) | `UUID default generateUUIDv4()`, `Map` properties, idempotency key |
| [`ml-feature-store`](./ml-feature-store/) | [ML feature engineering on CH](https://clickhouse.com/blog/modeling-machine-learning-data-in-clickhouse) | Raw event store as basis for online + offline features |

### NewJSON

| Example | Source | ClickHouse features |
| --- | --- | --- |
| [`newjson-events`](./newjson-events/) | [ClickHouse NewJSON docs](https://clickhouse.com/docs/sql-reference/data-types/newjson) | Parameterized `JSON(max_dynamic_paths, typeHints, SKIP, SKIP REGEXP)`, path-access DSL (`payload.path("user.id")`, `castPath`, `subobject`) |

## Running an example

Every example exposes a `build*Example()` helper that returns a typed ck-orm
query builder. To execute against a real ClickHouse instance, pair it with a
configured client:

```ts
import { clickhouseClient } from "../../ck-orm";
import { buildClickhouseLogErrorsByService } from "./clickhouse-log-platform";

const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "scenarios",
  username: "default",
  password: "",
});

const rows = await buildClickhouseLogErrorsByService().select(db).execute();
```

The e2e suite under [`/e2e/scenarios-*.e2e.test.ts`](../../e2e/) seeds each
scenario with sample data and exercises the same query helpers against
ClickHouse, so you can crib both the schema and the test as a starting point.

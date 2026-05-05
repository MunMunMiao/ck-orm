# ck-orm

<p align="center">
  <img src="bunner.png" alt="ck-orm - TypeScript ORM for ClickHouse" width="100%" />
</p>

<p align="center">
  <a href="https://npmjs.com/package/ck-orm"><img src="https://img.shields.io/npm/v/ck-orm?style=flat-square&labelColor=%23151A1F&color=%23151A1F" alt="npm package"></a>
  <a href="https://npmjs.com/package/ck-orm"><img src="https://img.shields.io/npm/dm/ck-orm?style=flat-square&labelColor=%23151A1F&color=%23151A1F" alt="monthly downloads"></a>
  <a href="https://github.com/MunMunMiao/ck-orm/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/MunMunMiao/ck-orm/ci.yml?branch=main&style=flat-square&labelColor=%23151A1F&color=%23151A1F" alt="build status"></a>
  <a href="https://github.com/MunMunMiao/ck-orm/blob/main/LICENSE"><img src="https://img.shields.io/github/license/MunMunMiao/ck-orm?style=flat-square&labelColor=%23151A1F&color=%23151A1F" alt="license"></a>
</p>

<p align="center">
  <a href="https://deepwiki.com/MunMunMiao/ck-orm"><img src="https://img.shields.io/badge/Ask_DeepWiki-151A1F?style=flat-square&logo=data%3Aimage%2Fsvg%2Bxml%3Bbase64%2CPHN2ZyB4bWxucz0naHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmcnIHZpZXdCb3g9JzAgMCAzMiAzMic%2BPGcgZmlsbD0nI0Y4QzAwMCc%2BPGNpcmNsZSBjeD0nNi41JyBjeT0nMTYnIHI9JzUuMjUnLz48Y2lyY2xlIGN4PScxMS41JyBjeT0nOScgcj0nNScgb3BhY2l0eT0nLjg0Jy8%2BPGNpcmNsZSBjeD0nMjAnIGN5PSc3LjUnIHI9JzUnIG9wYWNpdHk9Jy43MicvPjxjaXJjbGUgY3g9JzI1LjUnIGN5PScxMy41JyByPSc1JyBvcGFjaXR5PScuODgnLz48Y2lyY2xlIGN4PScxNC41JyBjeT0nMTYuNScgcj0nNS4yNScvPjxjaXJjbGUgY3g9JzknIGN5PScyNCcgcj0nNScgb3BhY2l0eT0nLjc4Jy8%2BPGNpcmNsZSBjeD0nMTguNScgY3k9JzIzLjUnIHI9JzUnIG9wYWNpdHk9Jy44MicvPjxjaXJjbGUgY3g9JzI0JyBjeT0nMTknIHI9JzUnIG9wYWNpdHk9Jy43NCcvPjwvZz48L3N2Zz4%3D" alt="Ask DeepWiki"></a>
</p>

<p align="center">
  A typed ClickHouse query layer for modern JavaScript runtimes.
</p>

It gives you:

- a schema DSL for ClickHouse tables and columns
- a typed query builder for the common path
- raw SQL when ClickHouse-specific syntax is the better tool
- session helpers for temporary-table workflows
- observability hooks for logging, tracing, and custom instrumentation

The design goal is straightforward: make everyday ClickHouse access easier to structure without hiding the parts that make ClickHouse different.

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Examples](#examples)
- [Mental model](#mental-model)
- [Schema DSL](#schema-dsl)
- [Client configuration](#client-configuration)
- [Query builder](#query-builder)
- [Join null semantics](#join-null-semantics)
- [Writes](#writes)
- [Raw SQL](#raw-sql)
- [Functions and table functions](#functions-and-table-functions)
- [Sessions and temporary tables](#sessions-and-temporary-tables)
- [Runtime methods](#runtime-methods)
- [Observability](#observability)
- [Error model](#error-model)
- [Security](#security)

## Installation

```bash
bun add ck-orm
```

```bash
npm install ck-orm
```

## Quick start

The examples below use a single table so the main flow stays easy to follow.

### 1. Define a schema

```ts
import { ckTable, ckType } from "ck-orm";

export const orderRewardLog = ckTable(
  "order_reward_log",
  {
    id: ckType.int32(),
    userId: ckType.string("user_id"),
    campaignId: ckType.int32("campaign_id"),
    orderId: ckType.int64("order_id"),
    rewardPoints: ckType.decimal("reward_points", { precision: 20, scale: 5 }),
    status: ckType.int16(),
    createdAt: ckType.int32("created_at"),
    peerdbSyncedAt: ckType.dateTime64("_peerdb_synced_at", { precision: 9 }),
    peerdbIsDeleted: ckType.uint8("_peerdb_is_deleted"),
    peerdbVersion: ckType.uint64("_peerdb_version"),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.userId, table.createdAt, table.id],
    versionColumn: table.peerdbVersion,
  }),
);

export const commerceSchema = {
  orderRewardLog,
};
```

### 2. Create a client

```ts
import { clickhouseClient } from "ck-orm";
import { commerceSchema } from "./schema";

export const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "demo_store",
  username: "default",
  password: "<password>",
  schema: commerceSchema,
  clickhouse_settings: {
    allow_experimental_correlated_subqueries: 1,
    max_execution_time: 10,
  },
});
```

### 3. Query data

```ts
import { ck, fn } from "ck-orm";
import { db } from "./db";
import { orderRewardLog } from "./schema";

const query = db
  .select({
    userId: orderRewardLog.userId,
    totalRewardPoints: fn.sum(orderRewardLog.rewardPoints).as(
      "total_reward_points",
    ),
  })
  .from(orderRewardLog)
  .where(ck.eq(orderRewardLog.status, 1))
  .groupBy(orderRewardLog.userId)
  .orderBy(ck.desc(fn.sum(orderRewardLog.rewardPoints)))
  .limit(20);

const rows = await query;
```

Builder queries are thenable, so `await query` executes the query directly.

`fn.sum` over a `Decimal` column auto-casts to `Decimal(P, S)` and returns `string` to keep precision intact — see [Decimal precision in expressions](#decimal-precision-in-expressions).

## Examples

The README is the reference path. The [`examples/`](./examples) directory is the copy-and-adapt path.

Start here by task:

| Task | Example |
| --- | --- |
| Basic builder query, filters, aggregate, `FINAL` | [`examples/basic-select.ts`](./examples/basic-select.ts) |
| Schema options, column name mapping, column metadata, inferred row/insert types | [`examples/schema-and-types.ts`](./examples/schema-and-types.ts) |
| Inserts, raw `query_params`, direct value binding | [`examples/params-and-insert.ts`](./examples/params-and-insert.ts) |
| Raw SQL templates, identifiers, table functions | [`examples/raw-sql.ts`](./examples/raw-sql.ts) |
| JSON extraction, array helpers, `arrayJoin(arrayZip(...))`, tuple scopes | [`examples/json-array-functions.ts`](./examples/json-array-functions.ts) |
| CTEs, subqueries, joins | [`examples/cte-and-subquery.ts`](./examples/cte-and-subquery.ts) |
| Left join null semantics and `withSettings()` | [`examples/joins-and-settings.ts`](./examples/joins-and-settings.ts) |
| Session temporary tables | [`examples/session-temp-table.ts`](./examples/session-temp-table.ts) |
| Large filter scopes with session temp tables and streaming export | [`examples/large-scope-session.ts`](./examples/large-scope-session.ts) |
| Runtime methods, logger, instrumentation, endpoint helpers | [`examples/runtime-observability.ts`](./examples/runtime-observability.ts) |
| Advanced compiled-query integration | [`examples/advanced-compiled-query.ts`](./examples/advanced-compiled-query.ts) |
| Count modes and error guards | [`examples/count-and-errors.ts`](./examples/count-and-errors.ts) |
| Cross-system enrichment with two ClickHouse clients | [`examples/cross-system-order-enrichment.ts`](./examples/cross-system-order-enrichment.ts) |
| Multi-CTE analytical lifecycle query | [`examples/fulfillment-order-lifecycle.ts`](./examples/fulfillment-order-lifecycle.ts) |

Public API coverage by guide:

| API family | README section | Example files |
| --- | --- | --- |
| `ckType`, `ckTable`, `ckAlias`, model inference | [Schema DSL](#schema-dsl) | [`schema-and-types.ts`](./examples/schema-and-types.ts), [`schema/commerce.ts`](./examples/schema/commerce.ts), [`schema/fulfillment.ts`](./examples/schema/fulfillment.ts) |
| `clickhouseClient`, connection config, settings | [Client configuration](#client-configuration) | [`basic-select.ts`](./examples/basic-select.ts), [`runtime-observability.ts`](./examples/runtime-observability.ts) |
| `select`, joins, filters, grouping, ordering, `limitBy`, CTEs | [Query builder](#query-builder) | [`basic-select.ts`](./examples/basic-select.ts), [`cte-and-subquery.ts`](./examples/cte-and-subquery.ts), [`fulfillment-order-lifecycle.ts`](./examples/fulfillment-order-lifecycle.ts) |
| `count`, `count().toSafe()`, `count().toMixed()` | [`db.count()`](#dbcount) | [`count-and-errors.ts`](./examples/count-and-errors.ts) |
| `insert`, `insertJsonEachRow` | [Writes](#writes) | [`params-and-insert.ts`](./examples/params-and-insert.ts), [`large-scope-session.ts`](./examples/large-scope-session.ts) |
| `ckSql`, `ck.expr`, `query_params`, identifiers | [Raw SQL](#raw-sql) | [`raw-sql.ts`](./examples/raw-sql.ts), [`params-and-insert.ts`](./examples/params-and-insert.ts) |
| `fn.*` scalar, aggregate, JSON, array, tuple, table helpers | [Functions and table functions](#functions-and-table-functions) | [`json-array-functions.ts`](./examples/json-array-functions.ts), [`raw-sql.ts`](./examples/raw-sql.ts), [`activity-monthly-export.ts`](./examples/activity-monthly-export.ts) |
| `runInSession`, temporary tables, session concurrency | [Sessions and temporary tables](#sessions-and-temporary-tables) | [`session-temp-table.ts`](./examples/session-temp-table.ts), [`large-scope-session.ts`](./examples/large-scope-session.ts) |
| `execute`, `stream`, `command`, `ping`, `replicasStatus`, `withSettings` | [Runtime methods](#runtime-methods) | [`runtime-observability.ts`](./examples/runtime-observability.ts), [`raw-sql.ts`](./examples/raw-sql.ts) |
| `executeCompiled`, `iteratorCompiled`, `ck.decodeRow`, `ck.createSessionId`, `CompiledQuery` | [Runtime methods](#runtime-methods) | [`advanced-compiled-query.ts`](./examples/advanced-compiled-query.ts) |
| logger, tracing, instrumentation | [Observability](#observability) | [`runtime-observability.ts`](./examples/runtime-observability.ts) |
| error guards and error fields | [Error model](#error-model) | [`count-and-errors.ts`](./examples/count-and-errors.ts) |

## Mental model

Use `ck-orm` with these boundaries in mind:

- `ckType.*` defines schema column types
- `ckTable(...)` defines table schemas
- `ck.*` is the query-helper namespace
- `fn.*` is the SQL function-helper namespace
- schema describes tables and columns, not the database name
- the database connection lives on `clickhouseClient(...)`
- builder queries are the default path, raw SQL is the escape hatch
- `runInSession()` is a ClickHouse session helper, not a transaction
- `leftJoin()` uses SQL-style null semantics by default
- large or high-risk numeric results are decoded conservatively rather than silently coerced

`ck-orm` is not trying to be:

- a migration framework
- a schema sync tool
- a transaction-oriented ORM with a unit-of-work abstraction
- a fake OLTP abstraction layered over ClickHouse

## Schema DSL

### `ckTable()`

Use `ckTable(name, columns, options?)` to define a table.

Examples in this section assume:

```ts
import { ckTable, ckType, ckSql } from "ck-orm";
```

```ts
const orderRewardLog = ckTable("order_reward_log", {
  id: ckType.int32(),
  userId: ckType.string("user_id"),
  orderId: ckType.int64("order_id"),
  rewardPoints: ckType.decimal("reward_points", { precision: 20, scale: 5 }),
  createdAt: ckType.int32("created_at"),
  peerdbVersion: ckType.uint64("_peerdb_version"),
});
```

The third argument can be a plain object or a factory function:

```ts
const orderRewardLog = ckTable(
  "order_reward_log",
  {
    id: ckType.int32(),
    userId: ckType.string("user_id"),
    orderId: ckType.int64("order_id"),
    rewardPoints: ckType.decimal("reward_points", { precision: 20, scale: 5 }),
    createdAt: ckType.int32("created_at"),
    peerdbVersion: ckType.uint64("_peerdb_version"),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.userId, table.createdAt, table.id],
    versionColumn: table.peerdbVersion,
  }),
);
```

Public table options:

- `engine`
- `partitionBy`
- `primaryKey`
- `orderBy`
- `sampleBy`
- `ttl`
- `settings`
- `comment`
- `versionColumn`

Column definitions can also carry DDL metadata directly:

- `.default(expr)`
- `.materialized(expr)`
- `.aliasExpr(expr)`
- `.comment(text)`
- `.codec(expr)`
- `.ttl(expr)`

Schema metadata has two jobs in `ck-orm`:

- it drives typed queries, insert validation, and result decoding
- it can render structured DDL for session temporary tables

It does not automatically migrate or synchronize production ClickHouse tables. Keep production DDL in your migration tool, and keep `ckTable(...)` aligned with the schema your application reads and writes.

Example:

```ts
const orderRewardLog = ckTable(
  "order_reward_log",
  {
    id: ckType.int32(),
    createdAt: ckType.dateTime("created_at"),
    shardDay: ckType.date("shard_day").materialized(ckSql`toDate(created_at)`),
    note: ckType.string().default(ckSql`'pending'`),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    partitionBy: ckSql`toYYYYMM(created_at)`,
    orderBy: [table.id],
    versionColumn: table.createdAt,
  }),
);
```

### Type inference

Every table exposes:

- `table.$inferSelect`
- `table.$inferInsert`

```ts
type RewardLogRow = typeof orderRewardLog.$inferSelect;
type RewardLogInsert = typeof orderRewardLog.$inferInsert;

const orderId: RewardLogRow["orderId"] = "900001";
const peerdbVersion: RewardLogInsert["peerdbVersion"] = "1";
```

For generic helpers, use:

- `InferSelectModel<TTable>`
- `InferInsertModel<TTable>`
- `InferSelectSchema<TSchema>`
- `InferInsertSchema<TSchema>`

```ts
import type {
  InferInsertModel, InferSelectModel, InferSelectSchema } from "ck-orm";

type RewardLogRow = InferSelectModel<typeof orderRewardLog>;
type RewardLogInsert = InferInsertModel<typeof orderRewardLog>;
type CommerceRows = InferSelectSchema<typeof commerceSchema>;
```

### `ckAlias()`

Use `ckAlias()` when the same table needs to appear more than once in a query.

```ts
import { fn, ck, ckAlias } from "ck-orm";

const rewardLog = ckAlias(orderRewardLog, "reward_log");
```

Columns returned by `ckAlias()` are rebound automatically to the alias.

### Column names

The schema object key is the logical key used by TypeScript rows, decoded query results, and insert values. By default, that same key is also the ClickHouse column name:

```ts
const rewardLog = ckTable("reward_log", {
  rewardPoints: ckType.decimal({ precision: 20, scale: 5 }),
});
```

When the database column uses a different name, pass that physical column name as the first argument:

```ts
const rewardLog = ckTable("reward_log", {
  userId: ckType.string("user_id"),
  rewardPoints: ckType.decimal("reward_points", { precision: 20, scale: 5 }),
  createdAt: ckType.dateTime64("created_at", { precision: 9 }),
});

await db.insert(rewardLog).values({
  userId: "u_100",
  rewardPoints: "12.50000",
  createdAt: new Date(),
});
```

SQL, DDL, filters, ordering, grouping, and write column lists use the physical names (`user_id`, `reward_points`, `created_at`). Inferred models, default select results, explicit projection keys, and insert values use the schema object keys (`userId`, `rewardPoints`, `createdAt`).

Every public `ckType` builder supports an outer physical column name. Builders without extra configuration accept `name?`; builders with type configuration keep the optional physical column name first and put the type configuration in an object:

```ts
const typedColumns = ckTable("typed_columns", {
  id: ckType.int32("id"),
  code: ckType.fixedString("code", { length: 8 }),
  amount: ckType.decimal("amount", { precision: 20, scale: 5 }),
  tags: ckType.array("tags", ckType.string()),
  attrs: ckType.map("attrs", ckType.string(), ckType.string()),
  embedding: ckType.qbit("embedding", ckType.float32(), { dimensions: 8 }),
});
```

`aggregateFunction` and `simpleAggregateFunction` have one extra wrinkle: in their natural ClickHouse-shaped form, the first string is the aggregate function name, not the column name:

```ts
ckType.aggregateFunction("sum", ckType.uint64());
ckType.simpleAggregateFunction("sum", ckType.uint64());
```

Use object config when you also need a physical column name:

```ts
const aggregateStateColumns = ckTable("aggregate_state_columns", {
  rewardSumState: ckType.aggregateFunction("reward_sum_state", {
    name: "sum",
    args: [ckType.decimal({ precision: 20, scale: 5 })],
  }),
  rewardSum: ckType.simpleAggregateFunction("reward_sum", {
    name: "sum",
    value: ckType.decimal({ precision: 20, scale: 5 }),
  }),
});
```

`nested("items", shape)` names the outer `Nested(...)` column. Nested field names are the keys of `shape`; inner column `configuredName` values are not used for the nested field names:

```ts
const orderLines = ckTable("order_lines", {
  items: ckType.nested("items", {
    productSku: ckType.string(),
    quantity: ckType.float64(),
  }),
});
```

### Column builders

Use `ckType.*` for schema column builders. The schema DSL covers the common ClickHouse type families:

- integers: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`
- floating point and decimal: `float32`, `float64`, `bfloat16`, `decimal`
- scalar types: `bool`, `string`, `fixedString`, `uuid`, `ipv4`, `ipv6`
- time types: `date`, `date32`, `dateTime`, `dateTime64`, `time`, `time64`
- enums and special types: `enum8`, `enum16`, `json`, `dynamic`, `qbit`
- containers: `nullable`, `array`, `tuple`, `map`, `nested`, `variant`, `lowCardinality`
- aggregate types: `aggregateFunction`, `simpleAggregateFunction`
- geometry types: `point`, `ring`, `lineString`, `multiLineString`, `polygon`, `multiPolygon`

`int64` and `uint64` default to TypeScript `string` in schema-driven reads, writes, and inferred models so 64-bit values stay exact across the ClickHouse JSON wire format and JavaScript runtimes. When you explicitly want `bigint`, opt in with your own decoder such as `mapWith((value) => BigInt(String(value)))`.

ClickHouse does not support `Nullable(Array(...))`, `Nullable(Map(...))`, or `Nullable(Tuple(...))`. `ck-orm` rejects those shapes at schema-definition time. Put `nullable(...)` inside the composite type instead, for example `ckType.array(ckType.nullable(ckType.string()))`.

Builders with type configuration use object config, with the optional physical column name first:

```ts
ckType.decimal({ precision: 20, scale: 5 });
ckType.decimal("reward_points", { precision: 20, scale: 5 });
ckType.fixedString({ length: 8 });
ckType.dateTime64("created_at", { precision: 9, timezone: "UTC" });
ckType.qbit("embedding", ckType.float32(), { dimensions: 8 });
```

### Column Type Cookbook

Common schema shapes and their TypeScript values:

| ClickHouse shape | Schema | TypeScript value shape |
| --- | --- | --- |
| enum | `ckType.enum8<"new" | "paid">({ new: 1, paid: 2 })` | `"new" | "paid"` |
| low-cardinality string | `ckType.lowCardinality(ckType.string())` | `string` |
| nullable decimal | `ckType.nullable(ckType.decimal({ precision: 18, scale: 5 }))` | `string | null` |
| array | `ckType.array(ckType.string())` | `string[]` |
| nullable array item | `ckType.array(ckType.nullable(ckType.string()))` | `(string | null)[]` |
| tuple | `ckType.tuple(ckType.string(), ckType.int32())` | `[string, number]` |
| map | `ckType.map(ckType.string(), ckType.string())` | `Record<string, string>` |
| nested object array | `ckType.nested({ sku: ckType.string(), quantity: ckType.float64() })` | `{ sku: string; quantity: number }[]` |
| variant | `ckType.variant(ckType.string(), ckType.int32())` | `string | number` |
| JSON | `ckType.json<{ risk?: { score?: number } }>()` | `{ risk?: { score?: number } }` |

`ckType.map(...)` currently supports `String` keys only and maps them to a JavaScript record, so it does not model ClickHouse's duplicate-key `Map(K, V)` edge case.

Insert rows use the same inferred shape as `typeof table.$inferInsert`, except columns with ClickHouse defaults or generated expressions can be omitted when you call `insert(table).values(...)`. `MATERIALIZED` and `ALIAS` columns are never written in the generated `INSERT` column list; passing them explicitly is rejected.

## Client configuration

Create a client with `clickhouseClient()`:

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:<password>@127.0.0.1:8123/demo_store",
  schema: commerceSchema,
});
```

### Connection modes

`clickhouseClient()` supports two mutually exclusive connection styles.

#### `databaseUrl`

Use `databaseUrl` when you want a single connection string:

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:secret@127.0.0.1:8123/demo_store",
  schema: commerceSchema,
});
```

When `databaseUrl` is present, do not also pass:

- `host`
- `database`
- `username`
- `password`
- `pathname`

#### Structured connection fields

Use explicit fields when you want each part configured separately:

```ts
const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "demo_store",
  username: "default",
  password: "<password>",
  schema: commerceSchema,
});
```

Structured mode defaults:

- `host`: `http://localhost:8123`
- `database`: `default`
- `username`: `default`
- `password`: `""`

### Common fields

Most projects only need a small subset of client fields:

| Field | Purpose |
| --- | --- |
| `schema` | Application schema |
| `request_timeout` | Default request timeout in milliseconds |
| `clickhouse_settings` | Default ClickHouse session/query settings |
| `application` | Set the ClickHouse application name |

### Advanced fields

Use these only when you actually need the corresponding behavior:

| Field | Purpose |
| --- | --- |
| `http_headers` | Additional default headers |
| `role` | Default ClickHouse role or roles |
| `session_id` | Default session id |
| `session_max_concurrent_requests` | Maximum in-flight requests allowed per `session_id` within this client (default `1`) |
| `compression.response` | Request compressed responses |
| `logger` / `logLevel` | Logger integration |
| `tracing` | OpenTelemetry integration |
| `instrumentation` | Custom query lifecycle hooks |

Session lifetime controls are intentionally request-scoped. Pass `session_timeout` to a single query or to `runInSession(...)`. Use `session_check` when you are continuing an existing `session_id`, not when bootstrapping a brand-new session.
`session_max_concurrent_requests` is different: it is a client-level guard that throttles overlapping requests that target the same `session_id`.
Real ClickHouse sessions are still server-locked, so increasing `session_max_concurrent_requests` above `1` can surface `SESSION_IS_LOCKED` instead of giving you true same-session parallelism.
Keep the default `1` unless you intentionally want to remove local serialization and are prepared to handle server-side session-lock failures.

### ClickHouse settings

`clickhouse_settings` is only for ClickHouse session/query settings, the same kind of keys documented in ClickHouse's [Session Settings](https://clickhouse.com/docs/operations/settings/settings) and accepted by the HTTP API as query parameters. It is separate from ck-orm client configuration such as `host`, `database`, `request_timeout`, `http_headers`, and `session_max_concurrent_requests`.

Do not put HTTP transport fields such as `query`, `database`, `session_id`, `role`, or `param_*` in `clickhouse_settings`; ck-orm rejects those keys because they collide with the request envelope and named-parameter channel.

Official setting keys have TypeScript completion, and arbitrary keys remain valid for newer ClickHouse versions or deployment-specific settings:

```ts
import { clickhouseClient, type ClickHouseSettings } from "ck-orm";

const reportSettings: ClickHouseSettings = {
  allow_experimental_correlated_subqueries: 1,
  max_threads: 4,
  setting_added_by_future_clickhouse: "enabled",
};

const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "demo_store",
  username: "default",
  password: "<password>",
  schema: commerceSchema,
  request_timeout: 30_000,
  clickhouse_settings: reportSettings,
});
```

`ck-orm` also keeps JSON parse/stringify hooks internal to the fetch transport. The public client config does not expose a `json` override.

### Authentication

`ck-orm` uses basic authentication for database connections.

- in `databaseUrl` mode, credentials may be embedded in the URL
- in structured mode, use `username` and `password`
- if no credentials are provided, the default is `default` with an empty password

## Query builder

The snippets below assume `db`, `orderRewardLog`, and the referenced helpers imported from `ck-orm`.

### `select()`

Explicit selection gives you an explicitly shaped result:

```ts
const rows = await db
  .select({
    userId: orderRewardLog.userId,
    rewardPoints: orderRewardLog.rewardPoints,
  })
  .from(orderRewardLog)
  .limit(10);
```

Projection objects are built from public `Selection` values or columns. In practice that means table columns, `fn.*(...)`, and `ck.expr(...)` outputs all compose the same way inside `select({ ... })`.

Implicit selection returns the full table model when there are no joins:

```ts
const rows = await db.select().from(orderRewardLog).limit(10);
```

With joins, implicit selection groups fields by source and returns nested objects.

### `from()`, `innerJoin()`, `leftJoin()`

```ts
import { ckAlias, ck } from "ck-orm";

const rewardLog = ckAlias(orderRewardLog, "reward_log");
const matchedRewardLog = ckAlias(orderRewardLog, "matched_reward_log");

const rows = await db
  .select({
    userId: rewardLog.userId,
    rewardEventId: rewardLog.id,
    matchedRewardEventId: matchedRewardLog.id,
  })
  .from(rewardLog)
  .leftJoin(
    matchedRewardLog,
    ck.eq(rewardLog.userId, matchedRewardLog.userId),
  );
```

### `where()` and condition helpers

Public condition helpers:

- `ck.and`
- `ck.or`
- `ck.not`
- `ck.eq`
- `ck.ne`
- `ck.gt`
- `ck.gte`
- `ck.lt`
- `ck.lte`
- `ck.between`
- `ck.has`
- `ck.hasAll`
- `ck.hasAny`
- `ck.contains`
- `ck.startsWith`
- `ck.endsWith`
- `ck.containsIgnoreCase`
- `ck.startsWithIgnoreCase`
- `ck.endsWithIgnoreCase`
- `ck.like`
- `ck.notLike`
- `ck.ilike`
- `ck.notIlike`
- `ck.inArray`
- `ck.notInArray`
- `ck.exists`
- `ck.notExists`

`.where(...predicates)` is a variadic `AND` entrypoint. It ignores `undefined`, so you can either pass multiple predicates directly or build grouped predicates with `ck.and(...)` and `ck.or(...)`.

```ts
import { ck } from "ck-orm";

const query = db
  .select({
    userId: orderRewardLog.userId,
    rewardPoints: orderRewardLog.rewardPoints,
  })
  .from(orderRewardLog)
  .where(
    ck.eq(orderRewardLog.status, 1),
    ck.inArray(orderRewardLog.campaignId, [10, 20, 30]),
    ck.between(orderRewardLog.createdAt, 1710000000, 1719999999),
  );
```

`ck.and(...)` skips `undefined`, which makes inline dynamic filters easy to assemble:

```ts
import { ck } from "ck-orm";

const query = db
  .select({
    id: orderRewardLog.id,
    status: orderRewardLog.status,
  })
  .from(orderRewardLog)
  .where(
    ck.and(
      minId !== undefined ? ck.gt(orderRewardLog.id, minId) : undefined,
      status !== undefined
        ? ck.or(ck.eq(orderRewardLog.status, status), ck.eq(orderRewardLog.status, 9))
        : undefined,
    ),
  );
```

For larger runtime-built filters, prefer `Predicate[]` plus variadic `.where(...predicates)`:

```ts
import { ck, type Predicate } from "ck-orm";

const predicates: Predicate[] = [];

if (minId !== undefined) {
  predicates.push(ck.gt(orderRewardLog.id, minId));
}

if (status !== undefined) {
  predicates.push(ck.or(ck.eq(orderRewardLog.status, status), ck.eq(orderRewardLog.status, 9)));
}

const query = db
  .select({
    id: orderRewardLog.id,
    status: orderRewardLog.status,
  })
  .from(orderRewardLog)
  .where(...predicates);
```

`Predicate` is the public name for reusable boolean SQL clauses. You can use the same predicate objects in `where`, `having`, join `on` clauses, and boolean-aware helpers such as `ck.exists(...)`.

`Selection` is the public name for reusable computed builder values such as `fn.sum(...)`, `fn.toString(...)`, and `ck.expr(ckSql...)`. Use `.as(...)` to alias them and `.mapWith(...)` to override decoding. `Order` is the clause object returned by `ck.asc(...)` and `ck.desc(...)`.

`ck.has(...)`, `ck.hasAll(...)`, and `ck.hasAny(...)` map directly to the native ClickHouse functions and keep ClickHouse's array, map, and JSON semantics.

`where(...)` is variadic, while `having(...)` takes a single predicate. For multi-clause `having`, compose the predicate first with `ck.and(...)` or `ck.or(...)`.

`ck.contains(...)`, `ck.startsWith(...)`, `ck.endsWith(...)` and their `*IgnoreCase` variants treat the input as literal text. They parameterize the value and escape LIKE wildcard characters (`%`, `_`, `\`) internally.

Use `ck.like(...)` / `ck.ilike(...)` only when you intentionally want full pattern semantics. Those APIs still parameterize values for SQL safety, but `%` and `_` keep their wildcard meaning because LIKE is a pattern language.

Literal-text search example:

```ts
import { ck } from "ck-orm";

const rows = await db
  .select({
    userId: orderRewardLog.userId,
  })
  .from(orderRewardLog)
  .where(ck.contains(orderRewardLog.userId, "user_100%"));
```

Advanced pattern example:

```ts
import { ck } from "ck-orm";

const rows = await db
  .select({
    userId: orderRewardLog.userId,
  })
  .from(orderRewardLog)
  .where(ck.like(orderRewardLog.userId, "user_%"));
```

### `groupBy()`, `having()`, `orderBy()`, `limit()`, `offset()`

```ts
import { ck, fn } from "ck-orm";

const totalRewardPoints = fn.sum(orderRewardLog.rewardPoints).as(
  "total_reward_points",
);

const query = db
  .select({
    userId: orderRewardLog.userId,
    totalRewardPoints,
  })
  .from(orderRewardLog)
  .groupBy(orderRewardLog.userId)
  .having(ck.gt(fn.sum(orderRewardLog.rewardPoints), "100.00000"))
  .orderBy(ck.desc(orderRewardLog.createdAt))
  .limit(20)
  .offset(0);
```

`groupBy()` and `limitBy([...])` accept columns and computed `Selection` values from helpers like `fn.*(...)` or `ck.expr(...)`.

`orderBy()` accepts:

- `ck.desc(selection)`
- `ck.asc(selection)`
- a column directly

### `final()`

Append table-level `FINAL` to a table query:

```ts
const query = db.select().from(orderRewardLog).final();
```

For simple unaliased table reads, `ck-orm` emits `FROM table FINAL`. When the root table is aliased or the query joins additional sources, `ck-orm` wraps the finalized table in a subquery and keeps the alias on the outer source. This avoids ClickHouse analyzer edge cases around `FINAL`, table aliases, joins, and lambda expressions while preserving the same builder API.

`final()` only applies to a table root source. If you need `FINAL` inside a CTE, subquery, or table-function flow, place `.final()` on the table-backed query before calling `.as(...)`.

### `limitBy()`

Use ClickHouse `LIMIT ... BY ...`:

```ts
import { ck } from "ck-orm";

const query = db
  .select({
    userId: orderRewardLog.userId,
    createdAt: orderRewardLog.createdAt,
  })
  .from(orderRewardLog)
  .orderBy(ck.desc(orderRewardLog.createdAt))
  .limitBy([orderRewardLog.userId], 1);
```

### Execution modes

Builder queries can be executed in three ways:

```ts
const query = db.select().from(orderRewardLog).limit(10);

const rows = await query;
const sameRows = await query.execute();

for await (const row of query.iterator()) {
  console.log(row);
}
```

Use `.execute()` in application examples when you want the execution point to be visually obvious. Direct `await query` is supported for Drizzle-style ergonomics and is useful once the team is familiar with builder queries being thenable.

`query.iterator()` uses the same session-aware concurrency rules as `db.stream()`: if the query targets a `session_id`, the slot stays occupied until iteration finishes or the iterator is closed early.

### Subqueries and CTEs

Use `.as("alias")` to turn a builder into a subquery:

```ts
const latestRewardEvent = db
  .select({
    userId: orderRewardLog.userId,
    createdAt: orderRewardLog.createdAt,
  })
  .from(orderRewardLog)
  .orderBy(ck.desc(orderRewardLog.createdAt))
  .limit(10)
  .as("latest_reward_event");
```

Use `$with()` and `with()` for CTEs:

```ts
import { ck, fn } from "ck-orm";

const rankedUsers = db.$with("ranked_users").as(
  db
    .select({
      userId: orderRewardLog.userId,
      totalRewardPoints: fn.sum(orderRewardLog.rewardPoints).as(
        "total_reward_points",
      ),
    })
    .from(orderRewardLog)
    .groupBy(orderRewardLog.userId),
);

const rows = await db
  .with(rankedUsers)
  .select({
    userId: rankedUsers.userId,
    totalRewardPoints: rankedUsers.totalRewardPoints,
  })
  .from(rankedUsers);
```

### `db.count()`

Use `db.count(source, ...predicates)` for a Drizzle-style count helper. It follows the same predicate semantics as `.where(...predicates)`: multiple predicates are combined with `AND`, and `undefined` values are ignored.

```ts
import { ck } from "ck-orm";

const total = await db.count(
  orderRewardLog,
  ck.eq(orderRewardLog.status, 1),
  ck.gt(orderRewardLog.id, 1000),
);
```

For more complex result sets, count a subquery or CTE:

```ts
const activeUsers = db
  .select({
    userId: orderRewardLog.userId,
  })
  .from(orderRewardLog)
  .where(ck.eq(orderRewardLog.status, 1))
  .as("active_users");

const total = await db.count(activeUsers);
```

`db.count(...)` defaults to the convenient unsafe path: it renders `toFloat64(count())` and decodes to `number`, so very large counts can lose JavaScript integer precision. Use the chainable modes when the return shape matters:

```ts
const approximateTotal = await db.count(activeUsers); // number
const exactTotal = await db.count(activeUsers).toSafe(); // string
const wireTotal = await db.count(activeUsers).toMixed(); // number | string
```

`.toSafe()` renders `toString(count())` and is intended for exact reads. If you use a safe count as a SQL expression, it has `String` semantics; use the default/`.toUnsafe()` or `.toMixed()` for numeric SQL comparisons. `.toMixed()` renders `toUInt64(count())` and preserves the driver/wire shape; with ck-orm's default lossless 64-bit JSON settings, real ClickHouse responses usually arrive as `string`.

## Join null semantics

`leftJoin()` defaults to SQL-style null semantics by automatically applying `join_use_nulls = 1`.

That means:

- the right side of a default left join is inferred as nullable
- if you explicitly disable `join_use_nulls`, the inferred types change as well

To align with ClickHouse default join behavior:

```ts
const rawDefaultDb = db.withSettings({
  join_use_nulls: 0,
});
```

The forced `join_use_nulls = 1` setting is preserved when a joined query is reused as a subquery, CTE, `ck.exists(...)`, or `ck.inArray(...)` source, so builder types stay aligned with runtime behavior.

## Writes

### `insert(table).values(...)`

Use the builder when you want typed inserts that follow the table schema:

```ts
await db.insert(orderRewardLog).values({
  id: 1,
  userId: "user_100",
  campaignId: 10,
  orderId: "900001",
  rewardPoints: "42.50000",
  status: 1,
  createdAt: 1710000000,
  peerdbSyncedAt: new Date("2026-04-21T00:00:00.000Z"),
  peerdbIsDeleted: 0,
  peerdbVersion: "1",
});
```

Insert rows must use keys from the table schema. Unknown keys are rejected early. Omitted columns continue to use `DEFAULT`.

### `insertJsonEachRow()`

Use `insertJsonEachRow()` when you already have object rows or an async row stream:

```ts
await db.insertJsonEachRow("tmp_scope", [
  { user_id: "user_100" },
  { user_id: "user_200" },
]);
```

It accepts:

- a string table name
- a table object created by `ckTable()`
- a regular array
- an `AsyncIterable`

An empty regular array is treated as a client-side no-op and still reports a successful insert lifecycle with `rowCount: 0` to instrumentation hooks. ClickHouse controls unknown-field behavior; pass settings such as `input_format_skip_unknown_fields: 1` when you want the server to ignore extra JSON fields.

## Raw SQL

`ck-orm` includes its own SQL template API. Use it when builder syntax would be less direct than the SQL you already want to write.

### `` ckSql`...` ``

```ts
import { ckSql, fn } from "ck-orm";

const rows = await db.execute(ckSql`
  select
    ${orderRewardLog.userId},
    ${fn.sum(orderRewardLog.rewardPoints)} as total_reward_points
  from ${orderRewardLog}
  where ${orderRewardLog.id} > ${10}
  group by ${orderRewardLog.userId}
`);
```

### `ckSql.join()` and `ckSql.identifier()`

```ts
import { ckSql } from "ck-orm";

const fields = ckSql.join(
  [ckSql.identifier("user_id"), ckSql.identifier("reward_points")],
  ", ",
);

const rows = await db.execute(
  ckSql`select ${fields} from ${ckSql.identifier("order_reward_log")}`,
);
```

### Raw SQL with `query_params`

```ts
import { ckSql } from "ck-orm";

const rows = await db.execute(
  ckSql`select user_id, reward_points from order_reward_log where user_id = {user_id:String} limit {limit:Int64}`,
  {
    query_params: {
      user_id: "user_100",
      limit: 10,
    },
  },
);
```

Parameter transport is chosen automatically. You do not need to configure multipart handling for `query_params`.

`query_params` keys that start with `orm_param` are rejected. That prefix is reserved for parameters generated internally by `` ckSql`...` ``.

The value formatter supports primitive values, `Date`, `NaN`, `Infinity`, arrays, objects, and `Map` values for ClickHouse typed placeholders such as `{ids:Array(UInt64)}` or `{attrs:Map(String, String)}`. Use ClickHouse's `Identifier` placeholder type when the parameter is a table or column name:

```ts
const rows = await db.execute(
  ckSql`
    select {selected_column:Identifier}
    from {target_table:Identifier}
    where id = {id:Int32}
  `,
  {
    query_params: {
      selected_column: "name",
      target_table: "users",
      id: 1,
    },
  },
);
```

### `ck.expr()`

Use `ck.expr()` to wrap a SQL fragment as a reusable `Selection`:

```ts
import { ck, ckSql } from "ck-orm";

const query = db.select({
  constantOne: ck.expr(ckSql`1`).as("constant_one"),
});
```

### Raw query formats

Raw eager queries only support `JSON` output:

```ts
const rows = await db.execute(ckSql`select 1`, {
  format: "JSON",
});
```

Raw streaming queries only support `JSONEachRow` output:

```ts
for await (const row of db.stream(ckSql`select 1`, {
  format: "JSONEachRow",
})) {
  console.log(row);
}
```

### Decimal precision in expressions

```ts
import { ckSql, ckTable, ckType, fn } from "ck-orm";

const ledger = ckTable("ledger", {
  amount: ckType.decimal({ precision: 18, scale: 5 }),
});

// fn.sum / sumIf / min / max auto-cast to Decimal(P, S) and decode as string.
db.select({ total: fn.sum(ledger.amount) }).from(ledger);
// → CAST(sum(`ledger`.`amount`) AS Decimal(38, 5))   row.total: string

// Explicit casts.
fn.toDecimal128(ledger.amount, 5);     // toDecimal32 / 64 / 128 / 256
ckSql.decimal(ckSql`sum(a) - sum(b)`, 20, 5);
ledger.amount.cast(20, 2);             // column shortcut
```

- `sum` / `sumIf` widen P to ≥ 38; `min` / `max` keep the column's P. Auto-cast also fires through `nullable(decimal(...))` and `lowCardinality(decimal(...))`.
- `avg` is **not** auto-cast — ClickHouse computes `avg(Decimal)` over Float64, so `fn.avg` returns `Selection<number>`. For exact Decimal averages, use `ckSql.decimal(ckSql\`sum(x) / count(x)\`, P, S)`.
- `column.cast(P, S)` casts the column, not the aggregate — using it bare inside `GROUP BY` raises `NOT_AN_AGGREGATE`. Use `fn.sum(column)` or wrap the aggregate.
- Inserts reject non-string/number objects (e.g. raw `decimal.js` instances) — pass `.toFixed(scale)`:

```ts
db.insert(ledger).values({ amount: new Decimal("1.23").toFixed(5) });   // ✅
db.insert(ledger).values({ amount: new Decimal("1.23") as never });     // ❌ throws
```

## Functions and table functions

### `fn`

Generic, conversion, aggregate, JSON, tuple, and table-related helpers include:

- `fn.call()`
- `fn.withParams()`
- `fn.toString()`
- `fn.toDate()`
- `fn.toDateTime()`
- `fn.toDecimal32()` / `fn.toDecimal64()` / `fn.toDecimal128()` / `fn.toDecimal256()`
- `fn.toStartOfMonth()`
- `fn.count()` / `fn.countIf()` — default `Selection<number>` wrapped as `toFloat64(count(...))`. Chain `.toSafe()` for `Selection<string>` (`toString(count(...))`), `.toMixed()` for `Selection<number | string>` (`toUInt64(count(...))`), or `.toUnsafe()` to revert to the default. Mirrors `db.count`. Decoders enforce non-negative integers and reject `NaN`, negatives, booleans, etc. — see [`fn.count` / `fn.uniqExact` modes](#fncount--fnuniqexact-modes) for examples.
- `fn.sum()` / `fn.sumIf()` / `fn.min()` / `fn.max()` — auto-cast to `Decimal(P, S)` for Decimal columns; see [Decimal precision in expressions](#decimal-precision-in-expressions)
- `fn.avg()` — `Selection<number>` (Float64), matching ClickHouse's native `avg(Decimal)` behavior
- `fn.uniqExact()` — same three chainable modes as `fn.count()`: default `Selection<number>` wrapped as `toFloat64(uniqExact(...))`, `.toSafe()` for `Selection<string>` (`toString(uniqExact(...))`), `.toMixed()` for `Selection<number | string>` (`toUInt64(uniqExact(...))`), `.toUnsafe()` to revert. Decoders are the same non-negative integer guard. See [`fn.count` / `fn.uniqExact` modes](#fncount--fnuniqexact-modes).
- `fn.coalesce()`
- `fn.jsonExtract()`
- `fn.tuple()`
- `fn.arrayJoin()`
- `fn.tupleElement()`
- `fn.not()`

Array helper names mirror the canonical headings in the ClickHouse [Array functions](https://clickhouse.com/docs/sql-reference/functions/array-functions) reference. Alias-only names stay available through `fn.call(...)` instead of expanding the public API twice.

- `fn.array()`
- `fn.arrayAUCPR()`
- `fn.arrayAll()`
- `fn.arrayAutocorrelation()`
- `fn.arrayAvg()`
- `fn.arrayCompact()`
- `fn.arrayConcat()`
- `fn.arrayCount()`
- `fn.arrayCumSum()`
- `fn.arrayCumSumNonNegative()`
- `fn.arrayDifference()`
- `fn.arrayDistinct()`
- `fn.arrayDotProduct()`
- `fn.arrayElement()`
- `fn.arrayElementOrNull()`
- `fn.arrayEnumerate()`
- `fn.arrayEnumerateDense()`
- `fn.arrayEnumerateDenseRanked()`
- `fn.arrayEnumerateUniq()`
- `fn.arrayEnumerateUniqRanked()`
- `fn.arrayExcept()`
- `fn.arrayExists()`
- `fn.arrayFill()`
- `fn.arrayFilter()`
- `fn.arrayFirst()`
- `fn.arrayFirstIndex()`
- `fn.arrayFirstOrNull()`
- `fn.arrayFlatten()`
- `fn.arrayFold()`
- `fn.arrayIntersect()`
- `fn.arrayJaccardIndex()`
- `fn.arrayLast()`
- `fn.arrayLastIndex()`
- `fn.arrayLastOrNull()`
- `fn.arrayLevenshteinDistance()`
- `fn.arrayLevenshteinDistanceWeighted()`
- `fn.arrayMap()`
- `fn.arrayMax()`
- `fn.arrayMin()`
- `fn.arrayNormalizedGini()`
- `fn.arrayPartialReverseSort()`
- `fn.arrayPartialShuffle()`
- `fn.arrayPartialSort()`
- `fn.arrayPopBack()`
- `fn.arrayPopFront()`
- `fn.arrayProduct()`
- `fn.arrayPushBack()`
- `fn.arrayPushFront()`
- `fn.arrayROCAUC()`
- `fn.arrayRandomSample()`
- `fn.arrayReduce()`
- `fn.arrayReduceInRanges()`
- `fn.arrayRemove()`
- `fn.arrayResize()`
- `fn.arrayReverse()`
- `fn.arrayReverseFill()`
- `fn.arrayReverseSort()`
- `fn.arrayReverseSplit()`
- `fn.arrayRotateLeft()`
- `fn.arrayRotateRight()`
- `fn.arrayShiftLeft()`
- `fn.arrayShiftRight()`
- `fn.arrayShingles()`
- `fn.arrayShuffle()`
- `fn.arraySimilarity()`
- `fn.arraySlice()`
- `fn.arraySort()`
- `fn.arraySplit()`
- `fn.arraySum()`
- `fn.arraySymmetricDifference()`
- `fn.arrayTranspose()`
- `fn.arrayUnion()`
- `fn.arrayUniq()`
- `fn.arrayWithConstant()`
- `fn.arrayZip()`
- `fn.arrayZipUnaligned()`
- `fn.countEqual()`
- `fn.empty()`
- `fn.emptyArrayDate()`
- `fn.emptyArrayDateTime()`
- `fn.emptyArrayFloat32()`
- `fn.emptyArrayFloat64()`
- `fn.emptyArrayInt16()`
- `fn.emptyArrayInt32()`
- `fn.emptyArrayInt64()`
- `fn.emptyArrayInt8()`
- `fn.emptyArrayString()`
- `fn.emptyArrayToSingle()`
- `fn.emptyArrayUInt16()`
- `fn.emptyArrayUInt32()`
- `fn.emptyArrayUInt64()`
- `fn.emptyArrayUInt8()`
- `fn.has()`
- `fn.hasAll()`
- `fn.hasAny()`
- `fn.hasSubstr()`
- `fn.indexOf()`
- `fn.indexOfAssumeSorted()`
- `fn.kql_array_sort_asc()`
- `fn.kql_array_sort_desc()`
- `fn.length()`
- `fn.notEmpty()`
- `fn.range()`
- `fn.replicate()`
- `fn.reverse()`

`fn.call(name, ...)` and `fn.withParams(name, ...)` validate `name` as a SQL identifier before compilation. `ck.has(...)`, `ck.hasAll(...)`, `ck.hasAny(...)`, and `ck.hasSubstr(...)` are where-friendly predicate shortcuts for the same ClickHouse functions.

`fn.jsonExtract(json, returnType, ...path)` only accepts `ckType.*` return types, so the ClickHouse return type and the TypeScript decoder stay together:

The JSON and array snippets below use the richer example schema from [`examples/schema/commerce.ts`](./examples/schema/commerce.ts), where `orderRewardLog.metadata` is a `JSON` column and `orderRewardLog.tags` is `Array(String)`.

```ts
import { ckType, ck, fn } from "ck-orm";

const regulatory = fn.jsonExtract(
  orderRewardLog.metadata,
  ckType.array(ckType.string()),
  "regulatory",
);

const riskScore = fn.jsonExtract(
  orderRewardLog.metadata,
  ckType.nullable(ckType.float64()),
  "risk",
  "score",
);

const filtered = db
  .select({
    orderId: orderRewardLog.orderId,
    regulatoryRegions: regulatory.as("regulatory_regions"),
    riskScore: riskScore.as("risk_score"),
  })
  .from(orderRewardLog)
  .where(ck.hasAny(regulatory, ["AU", "EU"]), ck.gte(riskScore, 80));
```

Path segments are ClickHouse JSON path arguments. Use string keys for object fields and number or bigint indexes for array positions:

```ts
// Reads metadata.orders[1].ticket from a JSON document such as:
// { "orders": [{ "ticket": "900001" }, { "ticket": "900002" }] }
const firstTicket = fn.jsonExtract(
  orderRewardLog.metadata,
  ckType.int64(),
  "orders",
  1,
  "ticket",
);
```

Higher-order array functions follow ClickHouse's parameter order. Use `ckSql` for the lambda, interpolate outer schema fields, and leave lambda-local variables as bare SQL names. Functions whose lambda controls the element shape, such as `fn.arrayMap(...)`, `fn.arrayFilter(...)`, and `fn.range(...)`, return `Selection<unknown[]>` by default; chain `.mapWith(...)` when your query needs a narrower decoded element type.

```ts
import { ck, ckSql, fn, type Predicate, type Selection } from "ck-orm";

const buildRangePredicate = (params: {
  timeColumn: Selection<Date>;
  scopeTable: {
    alwaysAccess: Selection<number>;
    startTsList: Selection<Date[]>;
    endTsList: Selection<Date[]>;
  };
}): Predicate => {
  const rangeMatched = fn.arrayExists(
    ckSql`
      (start_ts, end_ts) ->
        ${params.timeColumn} >= start_ts
        AND ${params.timeColumn} < end_ts
    `,
    params.scopeTable.startTsList,
    params.scopeTable.endTsList,
  );

  return ck.or(ck.eq(params.scopeTable.alwaysAccess, 1), rangeMatched);
};
```

In that lambda, `${params.timeColumn}` and `params.scopeTable.startTsList` are schema-backed expressions. `start_ts` and `end_ts` are lambda-local variables created by ClickHouse from the array elements.

`fn.arrayJoin(array)` maps to ClickHouse `arrayJoin` and expands one input row into one row per array element. Pair it with `fn.arrayZip(...)` and `fn.tupleElement(...)` when two arrays must stay positionally matched:

```ts
import { ck, fn } from "ck-orm";

const orderIds = ["900001", "900002"];
const userIds = ["user_100", "user_200"];

const targetPairs = db.$with("target_pairs").as(
  db.select({
    pair: fn.arrayJoin(fn.arrayZip(orderIds, userIds)).as("pair"),
  }),
);

const targetOrders = db.$with("target_orders").as(
  db
    .with(targetPairs)
    .select({
      orderId: fn.tupleElement<string>(targetPairs.pair, 1).as("order_id"),
      userId: fn.tupleElement<string>(targetPairs.pair, 2).as("user_id"),
    })
    .from(targetPairs),
);

const scopedPairs = db
  .select({
    pair: fn.tuple(targetOrders.orderId, targetOrders.userId).as("pair"),
  })
  .from(targetOrders)
  .as("scoped_pairs");

const query = db
  .with(targetPairs, targetOrders)
  .select({
    orderId: orderRewardLog.orderId,
    userId: orderRewardLog.userId,
  })
  .from(orderRewardLog)
  .where(
    ck.inArray(fn.tuple(orderRewardLog.orderId, orderRewardLog.userId), scopedPairs),
  );
```

The important part is the semantics: `arrayZip(orderIds, userIds)` preserves positional pairs, `arrayJoin(...)` expands them into rows, and `tupleElement(..., 1/2)` reads each side of the pair.

Use tuple membership for compound keys. Two independent `IN` predicates produce a cross product and can match pairs that were never requested:

```ts
// Avoid this when orderId and userId must stay paired.
const wrong = ck.and(
  ck.inArray(orderRewardLog.orderId, orderIds),
  ck.inArray(orderRewardLog.userId, userIds),
);
```

For pair semantics, compare one tuple against a tuple-producing subquery:

```ts
const right = ck.inArray(
  fn.tuple(orderRewardLog.orderId, orderRewardLog.userId),
  scopedPairs,
);
```

Array helpers return typed `Selection` values, so they compose in projections, filters, groups, and joins:

```ts
import { ck, fn } from "ck-orm";

const query = db
  .select({
    firstTag: fn.arrayElement<string>(orderRewardLog.tags, 1).as("first_tag"),
    maybeSecondTag: fn.arrayElementOrNull<string>(orderRewardLog.tags, 2).as(
      "maybe_second_tag",
    ),
    topTags: fn.arraySlice<string>(orderRewardLog.tags, 1, 3).as("top_tags"),
    tagCount: fn.length(orderRewardLog.tags).as("tag_count"),
    hasTags: fn.notEmpty(orderRewardLog.tags).as("has_tags"),
  })
  .from(orderRewardLog)
  .where(ck.hasAny(orderRewardLog.tags, ["vip", "reward"]));
```

### `fn.count` / `fn.uniqExact` modes

`fn.count()`, `fn.countIf(predicate)` and `fn.uniqExact(expression)` are aggregate counterparts to `db.count(...)` and follow the same three modes — useful when the count appears mid-query (in `select`, `having`, group-bys, sub-queries) rather than as a top-level scalar. They share the same SQL wrappers and decoders, so the choice maps to the same trade-offs:

```ts
import { ck, fn } from "ck-orm";

const summary = db
  .select({
    userId: orderRewardLog.userId,
    // default — Selection<number>, renders toFloat64(count(...))
    orderCount: fn.count(orderRewardLog.orderId).as("order_count"),
    // exact — Selection<string>, renders toString(count(...))
    auditedOrderCount: fn.count(orderRewardLog.orderId).toSafe().as("audited_order_count"),
    // wire-shape — Selection<number | string>, renders toUInt64(count(...))
    rawOrderCount: fn.count(orderRewardLog.orderId).toMixed().as("raw_order_count"),
    // countIf shares the same chainable modes
    paidOrderCount: fn.countIf(ck.eq(orderRewardLog.status, 1)).as("paid_order_count"),
    // uniqExact is symmetric with count — same modes, same decoders
    distinctCampaigns: fn.uniqExact(orderRewardLog.campaignId).as("distinct_campaigns"),
    distinctCampaignsExact: fn.uniqExact(orderRewardLog.campaignId).toSafe().as("distinct_campaigns_exact"),
  })
  .from(orderRewardLog)
  .groupBy(orderRewardLog.userId)
  // mode chooses the SQL semantics used in HAVING, ORDER BY, and other comparisons
  .having(ck.gt(fn.count(orderRewardLog.orderId), 0));
```

The decoders reject anything that is not a non-negative integer — booleans, `NaN`, decimals, and negative numbers throw `Failed to decode count() result: ...` — so corrupt server output fails fast instead of silently producing `0`.

Other aggregates intentionally diverge from this modes API — their precision considerations are not the same as `count` / `uniqExact`:

- `fn.sum()` / `fn.sumIf()` already pick a safe decoder per input column (string for integer/Decimal columns, number for floating-point columns) and auto-widen Decimal precision; chain `.mapWith(Number)` if you specifically want a `number`.
- `fn.avg()` always returns `Selection<number>` because ClickHouse runs `avg(Decimal)` through Float64 internally — wrapping it as a string would lie about the runtime path.
- `fn.min()` / `fn.max()` preserve the input column's TS type (so `min(int64Col)` is `Selection<string>`, matching the column's read shape).

### `fn.table`

```ts
import { fn } from "ck-orm";

const numbers = fn.table.call("numbers", 10).as("n");

const query = db
  .select({
    total: fn.count(),
  })
  .from(numbers);
```

## Sessions and temporary tables

ClickHouse does not provide traditional transactions. For scoped analytics and large-filter workflows, session-bound temporary tables are often the right primitive.

Inside a session callback, `ck-orm` gives you:

- `runInSession()`
- `registerTempTable()`
- `createTemporaryTable()`
- `createTemporaryTableRaw()`

### `runInSession()`

```ts
import { ckTable, ckType, ckSql } from "ck-orm";

const tmpScope = ckTable("tmp_scope", {
  user_id: ckType.string(),
});

await db.runInSession(async (session) => {
  // Temporary tables live only inside this Session and are cleaned up automatically.
  await session.createTemporaryTable(tmpScope);
  await session.insertJsonEachRow(tmpScope, [
    { user_id: "user_100" },
    { user_id: "user_200" },
  ]);

  return session.execute(ckSql`
    SELECT user_id
    FROM order_reward_log
    WHERE user_id IN (SELECT user_id FROM tmp_scope)
  `);
});
```

Use `registerTempTable(name)` when the temporary table is created by a command you control but you still want `ck-orm` to drop it during session cleanup:

```ts
await db.runInSession(async (session) => {
  await session.command(ckSql`
    CREATE TEMPORARY TABLE tmp_external_scope
    (
      user_id String
    )
    ENGINE = Memory
  `);

  session.registerTempTable("tmp_external_scope");

  await session.insertJsonEachRow("tmp_external_scope", [
    { user_id: "user_100" },
  ]);
});
```

Use `createTemporaryTableRaw(name, definition)` only for trusted, developer-controlled table definitions that cannot be expressed with `ckTable(...)`:

```ts
await db.runInSession(async (session) => {
  await session.createTemporaryTableRaw(
    "tmp_raw_scope",
    `
      (
        user_id String,
        created_at DateTime64(3, 'UTC')
      )
      ENGINE = Memory
    `,
  );
});
```

The `definition` argument starts after the table name. Do not include `CREATE TEMPORARY TABLE` or the table name; ck-orm renders those and validates the identifier separately.

### Session behavior

- requests targeting the same `session_id` share a session concurrency controller; the client default is `session_max_concurrent_requests = 1`
- real ClickHouse sessions are exclusive on the server side, so raising `session_max_concurrent_requests` above `1` disables local serialization but can still fail with `SESSION_IS_LOCKED`
- `createTemporaryTable()` automatically registers the table for cleanup
- `createTemporaryTable()` consumes schema objects; temporary-table lifecycle stays on `Session`, not on the schema itself
- `createTemporaryTableRaw()` is the trusted-only raw SQL escape hatch and rejects multi-statement definitions
- `runInSession()` drops registered temporary tables when the callback finishes
- nested `runInSession()` calls always create a new child session
- nested calls may not reuse any active ancestor `session_id`
- nested child sessions do not share temp tables or same-session concurrency slots with their parent because they always use a different `session_id`

### Session concurrency contract

The practical rules are:

- the same explicit `session_id` is serialized by default
- the client default `session_id` participates in the same limiter
- child clients created by `withSettings()` share the same limiter as their parent when they target the same `session_id`
- different `session_id` values do not block each other
- `stream()` and builder `iterator()` hold the same-session slot until the iterator finishes or is closed
- nested `runInSession()` calls create a fresh child `session_id`, so they do not share the parent's same-session slot

Recommended usage:

- keep `session_max_concurrent_requests = 1` for temporary-table workflows and any continued-session logic
- if you need true parallelism, use different `session_id` values instead of raising the same-session limit
- only raise `session_max_concurrent_requests` when you explicitly want to remove local backpressure and can tolerate `SESSION_IS_LOCKED`

## Runtime methods

Beyond the builder, the client also exposes lower-level runtime methods.

Common per-query fields include:

- `clickhouse_settings`
- `query_params`
- `query_id`
- `session_id`
- `session_timeout`
- `session_check`
- `role`
- `auth`
- `abort_signal`
- `http_headers`
- `ignore_error_response`

`session_timeout` and `session_check` live here on purpose: they describe a specific request or a specific session block, not a global client default.
`query_id`, `session_id`, and `session_timeout` are request/session options; keep them outside `clickhouse_settings`.

`session_check` does not bootstrap a new ClickHouse session. Use it when you need ClickHouse to verify that an explicit `session_id` already exists.
Any request that carries a `session_id` participates in the same per-session limiter, including raw `execute()`, `command()`, `stream()`, builder `.execute()`, and builder `.iterator()`.

Per-request `clickhouse_settings` only override ClickHouse settings for that request:

```ts
await db.execute(ckSql`SELECT 1`, {
  query_id: "debug-query",
  session_id: "debug-session",
  session_timeout: 60,
  clickhouse_settings: {
    max_threads: 2,
    readonly: 1,
  },
});
```

### `execute()`

Execute a raw query and return `Record<string, unknown>[]`:

```ts
const rows = await db.execute(ckSql`SELECT 1 AS one`);
```

### `stream()`

Stream raw query results:

```ts
for await (const row of db.stream(ckSql`SELECT number FROM numbers(10)`)) {
  console.log(row);
}
```

If `stream()` targets a `session_id`, the same-session slot is released only after the async iterator completes or you close it early by breaking out of the loop.

### `command()`

Execute a command that does not return a row set:

```ts
await db.command(ckSql`SYSTEM FLUSH LOGS`);
```

### `withSettings()`

Create a child client with additional default settings:

```ts
const reportDb = db.withSettings({
  max_execution_time: 60,
  join_use_nulls: 0,
});
```

`withSettings()` only changes default ClickHouse session/query settings. The returned client keeps the same schema, transport, auth, timeout, and session concurrency controller as the parent client.

Session lifecycle options stay separate from ClickHouse settings in `runInSession()`:

```ts
await db.runInSession(
  async (session) => {
    await session.execute(ckSql`SELECT 1`);

    await session
      .withSettings({
        max_threads: 2,
      })
      .execute(ckSql`SELECT 2`);
  },
  {
    session_id: "report-session",
    session_timeout: 120,
    clickhouse_settings: {
      allow_experimental_correlated_subqueries: 1,
    },
  },
);
```

### Compiled query integration

Most applications should use the builder directly. Use `executeCompiled()` and `iteratorCompiled()` when you are integrating another layer that already produces a `CompiledQuery`.

```ts
import { ck, type CompiledQuery } from "ck-orm";

const oneQuery = {
  kind: "compiled-query",
  mode: "query",
  statement: "SELECT 1 AS one",
  params: {},
  selection: [
    {
      key: "one",
      sqlAlias: "one",
      path: ["one"],
      decoder: (value: unknown) => Number(value),
    },
  ],
} satisfies CompiledQuery<{ one: number }>;

const rows = await db.executeCompiled(oneQuery, {
  session_id: ck.createSessionId(),
});

const decoded = ck.decodeRow<{ one: number }>({ one: "1" }, oneQuery.selection);
```

### `ping()` and `replicasStatus()`

```ts
await db.ping();
await db.replicasStatus();
```

## Observability

`ck-orm` provides three built-in observability layers:

- `logger`
- `tracing`
- `instrumentation`

### Logger

```ts
import type { ClickHouseORMLogger } from "ck-orm";

const logger: ClickHouseORMLogger = {
  trace(message, fields) {
    console.debug(message, fields);
  },
  debug(message, fields) {
    console.debug(message, fields);
  },
  info(message, fields) {
    console.info(message, fields);
  },
  warn(message, fields) {
    console.warn(message, fields);
  },
  error(message, fields) {
    console.error(message, fields);
  },
};

const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123/demo_store",
  schema: commerceSchema,
  logger,
  logLevel: "info",
});
```

### Tracing

```ts
import { clickhouseClient } from "ck-orm";
import { trace } from "@opentelemetry/api";
import { commerceSchema } from "./schema";

const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123/demo_store",
  schema: commerceSchema,
  tracing: {
    tracer: trace.getTracer("ck-orm-example"),
    includeStatement: false,
    includeRowCount: true,
  },
});
```

Tracing derives the database name, server address, server port, and request timeout from the `clickhouseClient(...)` configuration. Pass service-level labels through `tracing.attributes`; keys starting with `db.` are ignored so application code cannot overwrite built-in database attributes.

`includeStatement: false` is the safer setting for shared tracing backends. The library default is `true`; turn it off when table names, column names, or query shape should not leave the service boundary. Bound values are not embedded in the compacted statement, but SQL shape can still be operationally sensitive.

### Custom instrumentation

```ts
import type { ClickHouseORMInstrumentation } from "ck-orm";

const instrumentation: ClickHouseORMInstrumentation = {
  onQueryStart(event) {
    console.log("start", event.operation, event.statement);
  },
  onQuerySuccess(event) {
    console.log("success", event.durationMs, event.rowCount);
  },
  onQueryError(event) {
    console.error("error", event.error);
  },
};

const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123/demo_store",
  schema: commerceSchema,
  instrumentation: [instrumentation],
});
```

Public event types:

- `ClickHouseORMQueryEvent`
- `ClickHouseORMQueryResultEvent`
- `ClickHouseORMQueryErrorEvent`
- `ClickHouseORMQueryStatistics`
- `ClickHouseORMTracingOptions`
- `ClickHouseORMLogLevel`

Built-in event fields include `databaseName`, `serverAddress`, `serverPort`, `requestTimeoutMs`, `statementHash`, `querySummary`, `queryId`, `sessionId`, and `tableName` when ck-orm can identify one table without guessing from raw SQL. Eager JSON queries also include ClickHouse response statistics when the server returns them: `serverElapsedMs`, `readRows`, `readBytes`, `resultRows`, and `rowsBeforeLimitAtLeast`.

## Error model

Public error types (type-only exports):

- `ClickHouseORMError`
- `DecodeError`

Runtime error checks:

- `isClickHouseORMError(error)`
- `isDecodeError(error)`

`ClickHouseORMError` preserves as much context as possible, including:

- `kind`
- `executionState`
- `queryId`
- `sessionId`
- `httpStatus`
- `clickhouseCode`
- `clickhouseName`
- `responseText`
- `requestTimeoutMs`

Current public `kind` values:

- `client_validation`
- `request_failed`
- `decode`
- `timeout`
- `aborted`
- `session`

Use the full error context for server-side logs, then return a small public error to application callers:

```ts
import { isClickHouseORMError, isDecodeError } from "ck-orm";

const toPublicQueryError = (error: unknown) => {
  if (isDecodeError(error)) {
    console.error("ClickHouse decode failed", {
      path: error.path,
      causeValue: error.causeValue,
    });

    return { message: "The query result could not be decoded." };
  }

  if (isClickHouseORMError(error)) {
    console.error("ClickHouse request failed", {
      kind: error.kind,
      executionState: error.executionState,
      queryId: error.queryId,
      sessionId: error.sessionId,
      httpStatus: error.httpStatus,
      clickhouseCode: error.clickhouseCode,
      clickhouseName: error.clickhouseName,
      requestTimeoutMs: error.requestTimeoutMs,
    });

    if (error.kind === "timeout") return { message: "The query timed out." };
    if (error.kind === "aborted") return { message: "The query was cancelled." };
  }

  return { message: "The query failed." };
};
```

Do not return `responseText` or the raw error `message` to untrusted clients. ClickHouse can include SQL fragments and object names in error text.

## Security

`ck-orm` is designed to make unsafe SQL construction difficult by default.

The built-in protections include:

- identifiers passed to `ckSql.identifier()` are validated and quoted
- function names used by `fn.call(...)`, `fn.withParams(...)`, `ckType.aggregateFunction(...)`, `ckType.simpleAggregateFunction(...)`, and `ckType.nested({...})` keys are validated
- values interpolated into `` ckSql`...` `` become ClickHouse named parameters rather than raw SQL text
- `query_params` keys that start with `orm_param` are rejected because that prefix is reserved for internal SQL-template parameters
- only a single top-level statement is allowed per request
- authorization headers derived from connection config cannot be overridden by user-supplied `http_headers`
- structured connection config rejects credentials embedded in `host`
- tracing destination data is normalized before it is attached to spans

### Trusted-only APIs

`ck-orm` does not expose a general-purpose public raw SQL string escape hatch. The remaining trusted-only API is only available inside a `runInSession()` callback:

- `session.createTemporaryTableRaw(name, definition)`

`fn.call(name, ...)` and `fn.withParams(name, ...)` validate `name`, but you should still treat dynamically chosen function names as developer-controlled input rather than end-user input.

`ckSql.identifier(...)` validates on the compile/execute boundary. Constructing the fragment is cheap; the error appears when the fragment is compiled into a query.

### Tracing data exposure

`tracing.includeStatement` defaults to `true`, so compacted SQL text is attached to spans as `db.statement` and `db.query.text`.

Bound values are stored separately as ClickHouse named parameters and are not included in the statement text, but table names, column names, and query shape still appear in tracing output. If your trace backend is shared or off-host, disable `includeStatement` or filter `db.statement` / `db.query.text` in your collector.

### Error responses

`ClickHouseORMError.responseText` and the error `message` may contain raw text from the ClickHouse server, including SQL fragments and database object names.

Do not forward those directly to untrusted clients. Log them server-side and expose a generic message instead.

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

When you change public behavior, update these files together so the docs do not drift away from the tested contract:

- [`README.md`](./README.md)
- [`examples/README.md`](./examples/README.md) and any touched example files
- [`src/runtime.typecheck.ts`](./src/runtime.typecheck.ts)
- [`e2e/api-matrix.md`](./e2e/api-matrix.md)

## Quick start

The examples below use a single table so the main flow stays easy to follow.

### 1. Define a schema

```ts
import { chTable, chType, csql } from "ck-orm";

export const orderRewardLog = chTable(
  "order_reward_log",
  {
    id: chType.int32(),
    user_id: chType.string(),
    campaign_id: chType.int32(),
    order_id: chType.int64(),
    reward_points: chType.decimal(20, 5),
    status: chType.int16(),
    created_at: chType.int32(),
    _peerdb_synced_at: chType.dateTime64(9),
    _peerdb_is_deleted: chType.uint8(),
    _peerdb_version: chType.uint64(),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.user_id, table.created_at, table.id],
    versionColumn: table._peerdb_version,
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
  host: "http://127.0.0.1:8123", database: "demo_store", username: "default", password: "<password>", schema: commerceSchema, clickhouse_settings: {
    max_execution_time: 10, }, });
```

### 3. Query data

```ts
import { ck, fn } from "ck-orm";
import { db } from "./db";
import { orderRewardLog } from "./schema";

const query = db
  .select({
    userId: orderRewardLog.user_id, totalRewardPoints: fn.sum(orderRewardLog.reward_points).as(
      "total_reward_points", ), })
  .from(orderRewardLog)
  .where(ck.eq(orderRewardLog.status, 1))
  .groupBy(orderRewardLog.user_id)
  .orderBy(ck.desc(fn.sum(orderRewardLog.reward_points)))
  .limit(20);

const rows = await query;
```

Builder queries are thenable, so `await query` executes the query directly.

## Mental model

Use `ck-orm` with these boundaries in mind:

- `chType.*` defines schema column types
- `chTable(...)` defines table schemas
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

### `chTable()`

Use `chTable(name, columns, options?)` to define a table.

Examples in this section assume:

```ts
import { chTable, chType, ck } from "ck-orm";
```

```ts
const orderRewardLog = chTable("order_reward_log", {
  id: chType.int32(), user_id: chType.string(), reward_points: chType.decimal(20, 5), });
```

The third argument can be a plain object or a factory function:

```ts
const orderRewardLog = chTable(
  "order_reward_log", {
    id: chType.int32(), user_id: chType.string(), reward_points: chType.decimal(20, 5), }, (table) => ({
    engine: "ReplacingMergeTree", orderBy: [table.user_id, table.id], }), );
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

Example:

```ts
const orderRewardLog = chTable(
  "order_reward_log", {
    id: chType.int32(), created_at: chType.dateTime(), shard_day: chType.date().materialized(csql`toDate(created_at)`), note: chType.string().default(csql`'pending'`), }, (table) => ({
    engine: "ReplacingMergeTree", partitionBy: csql`toYYYYMM(created_at)`, orderBy: [table.id], versionColumn: table.created_at, }), );
```

### Type inference

Every table exposes:

- `table.$inferSelect`
- `table.$inferInsert`

```ts
type RewardLogRow = typeof orderRewardLog.$inferSelect;
type RewardLogInsert = typeof orderRewardLog.$inferInsert;
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

### `alias()`

Use `alias()` when the same table needs to appear more than once in a query.

```ts
import { fn, ck, alias } from "ck-orm";

const rewardLog = alias(orderRewardLog, "reward_log");
```

Aliased columns are rebound automatically to the alias.

### Column builders

Use `chType.*` for schema column builders. The schema DSL covers the common ClickHouse type families:

- integers: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`
- floating point and decimal: `float32`, `float64`, `bfloat16`, `decimal`
- scalar types: `bool`, `string`, `fixedString`, `uuid`, `ipv4`, `ipv6`
- time types: `date`, `date32`, `dateTime`, `dateTime64`, `time`, `time64`
- enums and special types: `enum8`, `enum16`, `json`, `dynamic`, `qbit`
- containers: `nullable`, `array`, `tuple`, `map`, `nested`, `variant`, `lowCardinality`
- aggregate types: `aggregateFunction`, `simpleAggregateFunction`
- geometry types: `point`, `ring`, `lineString`, `multiLineString`, `polygon`, `multiPolygon`

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
| `clickhouse_settings` | Default ClickHouse settings |
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
    userId: orderRewardLog.user_id,
    rewardPoints: orderRewardLog.reward_points,
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
import { alias, ck } from "ck-orm";

const rewardLog = alias(orderRewardLog, "reward_log");
const matchedRewardLog = alias(orderRewardLog, "matched_reward_log");

const rows = await db
  .select({
    userId: rewardLog.user_id,
    rewardEventId: rewardLog.id,
    matchedRewardEventId: matchedRewardLog.id,
  })
  .from(rewardLog)
  .leftJoin(
    matchedRewardLog,
    ck.eq(rewardLog.user_id, matchedRewardLog.user_id),
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
    userId: orderRewardLog.user_id,
    rewardPoints: orderRewardLog.reward_points,
  })
  .from(orderRewardLog)
  .where(
    ck.eq(orderRewardLog.status, 1),
    ck.inArray(orderRewardLog.campaign_id, [10, 20, 30]),
    ck.between(orderRewardLog.created_at, 1710000000, 1719999999),
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

`Selection` is the public name for reusable computed builder values such as `fn.sum(...)`, `fn.toString(...)`, and `ck.expr(csql...)`. Use `.as(...)` to alias them and `.mapWith(...)` to override decoding. `Order` is the clause object returned by `ck.asc(...)` and `ck.desc(...)`.

`ck.has(...)`, `ck.hasAll(...)`, and `ck.hasAny(...)` map directly to the native ClickHouse functions and keep ClickHouse's array, map, and JSON semantics.

`where(...)` is variadic, while `having(...)` takes a single predicate. For multi-clause `having`, compose the predicate first with `ck.and(...)` or `ck.or(...)`.

`ck.contains(...)`, `ck.startsWith(...)`, `ck.endsWith(...)` and their `*IgnoreCase` variants treat the input as literal text. They parameterize the value and escape LIKE wildcard characters (`%`, `_`, `\`) internally.

Use `ck.like(...)` / `ck.ilike(...)` only when you intentionally want full pattern semantics. Those APIs still parameterize values for SQL safety, but `%` and `_` keep their wildcard meaning because LIKE is a pattern language.

Literal-text search example:

```ts
import { ck } from "ck-orm";

const rows = await db
  .select({
    userId: orderRewardLog.user_id,
  })
  .from(orderRewardLog)
  .where(ck.contains(orderRewardLog.user_id, "user_100%"));
```

Advanced pattern example:

```ts
import { ck } from "ck-orm";

const rows = await db
  .select({
    userId: orderRewardLog.user_id,
  })
  .from(orderRewardLog)
  .where(ck.like(orderRewardLog.user_id, "user_%"));
```

### `groupBy()`, `having()`, `orderBy()`, `limit()`, `offset()`

```ts
import { ck, fn } from "ck-orm";

const totalRewardPoints = fn.sum(orderRewardLog.reward_points).as(
  "total_reward_points",
);

const query = db
  .select({
    userId: orderRewardLog.user_id,
    totalRewardPoints,
  })
  .from(orderRewardLog)
  .groupBy(orderRewardLog.user_id)
  .having(ck.gt(fn.sum(orderRewardLog.reward_points), "100.00000"))
  .orderBy(ck.desc(orderRewardLog.created_at))
  .limit(20)
  .offset(0);
```

`groupBy()` and `limitBy([...])` accept columns and computed `Selection` values from helpers like `fn.*(...)` or `ck.expr(...)`.

`orderBy()` accepts:

- `ck.desc(selection)`
- `ck.asc(selection)`
- a column directly

### `final()`

Append `FINAL` to a table query:

```ts
const query = db.select().from(orderRewardLog).final();
```

### `limitBy()`

Use ClickHouse `LIMIT ... BY ...`:

```ts
import { ck } from "ck-orm";

const query = db
  .select({
    userId: orderRewardLog.user_id,
    createdAt: orderRewardLog.created_at,
  })
  .from(orderRewardLog)
  .orderBy(ck.desc(orderRewardLog.created_at))
  .limitBy([orderRewardLog.user_id], 1);
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

`query.iterator()` uses the same session-aware concurrency rules as `db.stream()`: if the query targets a `session_id`, the slot stays occupied until iteration finishes or the iterator is closed early.

### Subqueries and CTEs

Use `.as("alias")` to turn a builder into a subquery:

```ts
const latestRewardEvent = db
  .select({
    userId: orderRewardLog.user_id,
    createdAt: orderRewardLog.created_at,
  })
  .from(orderRewardLog)
  .orderBy(ck.desc(orderRewardLog.created_at))
  .limit(10)
  .as("latest_reward_event");
```

Use `$with()` and `with()` for CTEs:

```ts
import { ck, fn } from "ck-orm";

const rankedUsers = db.$with("ranked_users").as(
  db
    .select({
      userId: orderRewardLog.user_id,
      totalRewardPoints: fn.sum(orderRewardLog.reward_points).as(
        "total_reward_points",
      ),
    })
    .from(orderRewardLog)
    .groupBy(orderRewardLog.user_id),
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
    userId: orderRewardLog.user_id,
  })
  .from(orderRewardLog)
  .where(ck.eq(orderRewardLog.status, 1))
  .as("active_users");

const total = await db.count(activeUsers);
```

`db.count(...)` decodes to `number` for convenience. If you need explicit aggregate control or exact handling for very large counts, use `fn.count()`.

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
  user_id: "user_100",
  campaign_id: 10,
  order_id: 900001,
  reward_points: "42.50000",
  status: 1,
  created_at: 1710000000,
  _peerdb_synced_at: new Date("2026-04-21T00:00:00.000Z"),
  _peerdb_is_deleted: 0,
  _peerdb_version: 1n,
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
- a table object created by `chTable()`
- a regular array
- an `AsyncIterable`

## Raw SQL

`ck-orm` includes its own SQL template API. Use it when builder syntax would be less direct than the SQL you already want to write.

### `` csql`...` ``

```ts
import { csql, fn } from "ck-orm";

const rows = await db.execute(csql`
  select
    ${orderRewardLog.user_id},
    ${fn.sum(orderRewardLog.reward_points)} as total_reward_points
  from ${orderRewardLog}
  where ${orderRewardLog.id} > ${10}
  group by ${orderRewardLog.user_id}
`);
```

### `csql.join()` and `csql.identifier()`

```ts
import { csql } from "ck-orm";

const fields = csql.join(
  [csql.identifier("user_id"), csql.identifier("reward_points")],
  ", ",
);

const rows = await db.execute(
  csql`select ${fields} from ${csql.identifier("order_reward_log")}`,
);
```

### Raw SQL with `query_params`

```ts
import { ck } from "ck-orm";

const rows = await db.execute(
  csql`select user_id, reward_points from order_reward_log where user_id = {user_id:String} limit {limit:Int64}`,
  {
    query_params: {
      user_id: "user_100",
      limit: 10,
    },
  },
);
```

Parameter transport is chosen automatically. You do not need to configure multipart handling for `query_params`.

`query_params` keys that start with `orm_param` are rejected. That prefix is reserved for parameters generated internally by `` csql`...` ``.

### `ck.expr()`

Use `ck.expr()` to wrap a SQL fragment as a reusable `Selection`:

```ts
import { ck, csql } from "ck-orm";

const query = db.select({
  constantOne: ck.expr(csql`1`).as("constant_one"),
});
```

### Raw query formats

Raw eager queries only support `JSON` output:

```ts
const rows = await db.execute(csql`select 1`, {
  format: "JSON",
});
```

Raw streaming queries only support `JSONEachRow` output:

```ts
for await (const row of db.stream(csql`select 1`, {
  format: "JSONEachRow",
})) {
  console.log(row);
}
```

## Functions and table functions

### `fn`

Common helpers include:

- `fn.call()`
- `fn.withParams()`
- `fn.toString()`
- `fn.toDate()`
- `fn.toDateTime()`
- `fn.toStartOfMonth()`
- `fn.count()`
- `fn.countIf()`
- `fn.sum()`
- `fn.sumIf()`
- `fn.avg()`
- `fn.min()`
- `fn.max()`
- `fn.uniqExact()`
- `fn.coalesce()`
- `fn.tuple()`
- `fn.arrayZip()`
- `fn.not()`

`fn.call(name, ...)` and `fn.withParams(name, ...)` validate `name` as a SQL identifier before compilation.

```ts
import { ck, fn } from "ck-orm";

const query = db
  .select({
    month: fn.toStartOfMonth(orderRewardLog.created_at).as("month"),
    totalRewardPoints: fn.sum(orderRewardLog.reward_points).as(
      "total_reward_points",
    ),
  })
  .from(orderRewardLog);
```

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
import { chTable, chType } from "ck-orm";

const tmpScope = chTable("tmp_scope", {
  user_id: chType.string(),
});

await db.runInSession(async (session) => {
  // Temporary tables live only inside this Session and are cleaned up automatically.
  await session.createTemporaryTable(tmpScope);
  await session.insertJsonEachRow(tmpScope, [
    { user_id: "user_100" },
    { user_id: "user_200" },
  ]);

  return session.execute(csql`
    SELECT user_id
    FROM order_reward_log
    WHERE user_id IN (SELECT user_id FROM tmp_scope)
  `);
});
```

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

`session_check` does not bootstrap a new ClickHouse session. Use it when you need ClickHouse to verify that an explicit `session_id` already exists.
Any request that carries a `session_id` participates in the same per-session limiter, including raw `execute()`, `command()`, `stream()`, builder `.execute()`, and builder `.iterator()`.

### `execute()`

Execute a raw query and return `Record<string, unknown>[]`:

```ts
const rows = await db.execute(csql`SELECT 1 AS one`);
```

### `stream()`

Stream raw query results:

```ts
for await (const row of db.stream(csql`SELECT number FROM numbers(10)`)) {
  console.log(row);
}
```

If `stream()` targets a `session_id`, the same-session slot is released only after the async iterator completes or you close it early by breaking out of the loop.

### `command()`

Execute a command that does not return a row set:

```ts
await db.command(csql`SYSTEM FLUSH LOGS`);
```

### `withSettings()`

Create a child client with additional default settings:

```ts
const reportDb = db.withSettings({
  max_execution_time: 60,
  join_use_nulls: 0,
});
```

The returned client keeps the same schema, transport, and session concurrency controller as the parent client.

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
import type { ClickHouseOrmLogger } from "ck-orm";

const logger: ClickHouseOrmLogger = {
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
import { commerceSchema } from "./schema";

const tracer = myObservabilityStack.getTracer("ck-orm-example");

const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123/demo_store", schema: commerceSchema, tracing: {
    tracer, dbName: "demo_store", }, });
```

### Custom instrumentation

```ts
import type { ClickHouseOrmInstrumentation } from "ck-orm";

const instrumentation: ClickHouseOrmInstrumentation = {
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

- `ClickHouseOrmQueryEvent`
- `ClickHouseOrmQueryResultEvent`
- `ClickHouseOrmQueryErrorEvent`
- `ClickHouseOrmTracingOptions`
- `ClickHouseOrmLogLevel`

## Error model

Public error types (type-only exports):

- `ClickHouseOrmError`
- `DecodeError`

Runtime error checks:

- `isClickHouseOrmError(error)`
- `isDecodeError(error)`

`ClickHouseOrmError` preserves as much context as possible, including:

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

## Security

`ck-orm` is designed to make unsafe SQL construction difficult by default.

The built-in protections include:

- identifiers passed to `csql.identifier()` are validated and quoted
- function names used by `fn.call(...)`, `fn.withParams(...)`, `chType.aggregateFunction(...)`, `chType.simpleAggregateFunction(...)`, and `chType.nested({...})` keys are validated
- values interpolated into `` csql`...` `` become ClickHouse named parameters rather than raw SQL text
- `query_params` keys that start with `orm_param` are rejected because that prefix is reserved for internal SQL-template parameters
- only a single top-level statement is allowed per request
- authorization headers derived from connection config cannot be overridden by user-supplied `http_headers`
- structured connection config rejects credentials embedded in `host`
- tracing destination data is normalized before it is attached to spans

### Trusted-only APIs

`ck-orm` does not expose a general-purpose public raw SQL string escape hatch. The remaining trusted-only API is:

- `db.createTemporaryTableRaw(name, definition)`

`fn.call(name, ...)` and `fn.withParams(name, ...)` validate `name`, but you should still treat dynamically chosen function names as developer-controlled input rather than end-user input.

`csql.identifier(...)` validates on the compile/execute boundary. Constructing the fragment is cheap; the error appears when the fragment is compiled into a query.

### Tracing data exposure

`tracing.includeStatement` defaults to `true`, so compacted SQL text is attached to spans as `db.statement`.

Bound values are stored separately as ClickHouse named parameters and are not included in the statement text, but table names, column names, and query shape still appear in tracing output. If your trace backend is shared or off-host, disable `includeStatement` or filter `db.statement` in your collector.

### Error responses

`ClickHouseOrmError.responseText` and the error `message` may contain raw text from the ClickHouse server, including SQL fragments and database object names.

Do not forward those directly to untrusted clients. Log them server-side and expose a generic message instead.

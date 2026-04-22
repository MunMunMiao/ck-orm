# ck-orm

<p align="center">
  <img src="bunner.png" alt="ck-orm - TypeScript ORM for ClickHouse" width="100%" />
</p>

`ck-orm` is a typed ClickHouse query layer for modern JavaScript runtimes.

It gives you:

- a schema DSL for ClickHouse tables and columns
- a typed query builder for the common path
- raw SQL when ClickHouse-specific syntax is the better tool
- session helpers for temporary-table workflows
- observability hooks for logging, tracing, and custom instrumentation

The design goal is straightforward: make everyday ClickHouse access easier to structure without hiding the parts that make ClickHouse different.

## Contents

- [Installation](#installation)
- [Runtime requirements](#runtime-requirements)
- [Repository map](#repository-map)
- [Local verification and workflows](#local-verification-and-workflows)
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

`@opentelemetry/api` is a peer dependency and is automatically installed by modern package managers (bun, npm 7+, pnpm). If you use a manager that does not auto-install peer dependencies, add it explicitly: `npm install @opentelemetry/api`.

## Runtime requirements

`ck-orm` targets modern runtimes and expects the environment to provide:

- `fetch`
- `Request`
- `Response`
- `URL`
- `TextEncoder`
- `ReadableStream`
- `FormData`
- `crypto` from Web Crypto

It does not depend on `node:crypto`, `Buffer`, or the official ClickHouse SDK.

## Repository map

If you are reading the repository for the first time, use this path:

1. Package root: [`src/index.ts`](./src/index.ts) -> [`src/public_api.ts`](./src/public_api.ts)
2. Learning path: [`examples/README.md`](./examples/README.md)
3. Real ClickHouse contract tests: [`e2e/README.md`](./e2e/README.md)

Key directories and files:

- [`src/public_api.ts`](./src/public_api.ts): the package-root export boundary
- [`src/runtime.ts`](./src/runtime.ts): the public runtime facade
- [`src/query.ts`](./src/query.ts): the main query-builder implementation
- [`examples/`](./examples): beginner-friendly usage examples
- [`e2e/`](./e2e): the real ClickHouse end-to-end suite
- [`src/runtime.typecheck.ts`](./src/runtime.typecheck.ts): a type-only contract file checked by `tsc --noEmit`

Recommended first read:

1. This README
2. [`examples/basic-select.ts`](./examples/basic-select.ts)
3. [`examples/session-temp-table.ts`](./examples/session-temp-table.ts)
4. [`e2e/api-matrix.md`](./e2e/api-matrix.md)

## Local verification and workflows

For local repository checks, use the scripts in [`package.json`](./package.json):

- `bun run build`
- `bun run lint`
- `bun run type-check`
- `bun run test:orm`
- `bun run test:coverage`
- `bun run test:e2e`

`bun run test:coverage` inherits the repository-wide Bun threshold, which currently requires 100% lines and 100% functions.

Repository workflows currently live in:

- [`.github/workflows/ci.yml`](./.github/workflows/ci.yml): lint, coverage, and type-check
- [`.github/workflows/publish.yml`](./.github/workflows/publish.yml): manual npm publish workflow

## Quick start

The examples below use a single table so the main flow stays easy to follow.

### 1. Define a schema

```ts
import {
  chTable,
  dateTime64,
  decimal,
  int16,
  int32,
  int64,
  string,
  uint8,
  uint64,
} from "ck-orm";

export const orderRewardLog = chTable(
  "order_reward_log",
  {
    id: int32(),
    user_id: string(),
    campaign_id: int32(),
    order_id: int64(),
    reward_points: decimal(20, 5),
    status: int16(),
    created_at: int32(),
    _peerdb_synced_at: dateTime64(9),
    _peerdb_is_deleted: uint8(),
    _peerdb_version: uint64(),
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
  host: "http://127.0.0.1:8123",
  database: "demo_store",
  username: "default",
  password: "<password>",
  schema: commerceSchema,
  clickhouse_settings: {
    max_execution_time: 10,
  },
});
```

### 3. Query data

```ts
import { desc, eq, fn } from "ck-orm";
import { db } from "./db";
import { orderRewardLog } from "./schema";

const query = db
  .select({
    userId: orderRewardLog.user_id,
    totalRewardPoints: fn.sum(orderRewardLog.reward_points).as(
      "total_reward_points",
    ),
  })
  .from(orderRewardLog)
  .where(eq(orderRewardLog.status, 1))
  .groupBy(orderRewardLog.user_id)
  .orderBy(desc(fn.sum(orderRewardLog.reward_points)))
  .limit(20);

const rows = await query;
```

Builder queries are thenable, so `await query` executes the query directly.

## Mental model

Use `ck-orm` with these boundaries in mind:

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

```ts
const orderRewardLog = chTable("order_reward_log", {
  id: int32(),
  user_id: string(),
  reward_points: decimal(20, 5),
});
```

The third argument can be a plain object or a factory function:

```ts
const orderRewardLog = chTable(
  "order_reward_log",
  {
    id: int32(),
    user_id: string(),
    reward_points: decimal(20, 5),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.user_id, table.id],
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

Example:

```ts
const orderRewardLog = chTable(
  "order_reward_log",
  {
    id: int32(),
    created_at: dateTime(),
    shard_day: date().materialized(sql`toDate(created_at)`),
    note: string().default(sql`'pending'`),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    partitionBy: sql`toYYYYMM(created_at)`,
    orderBy: [table.id],
    versionColumn: table.created_at,
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
```

For generic helpers, use:

- `InferSelectModel<TTable>`
- `InferInsertModel<TTable>`
- `InferSelectSchema<TSchema>`
- `InferInsertSchema<TSchema>`

```ts
import type {
  InferInsertModel,
  InferSelectModel,
  InferSelectSchema,
} from "ck-orm";

type RewardLogRow = InferSelectModel<typeof orderRewardLog>;
type RewardLogInsert = InferInsertModel<typeof orderRewardLog>;
type CommerceRows = InferSelectSchema<typeof commerceSchema>;
```

### `alias()`

Use `alias()` when the same table needs to appear more than once in a query.

```ts
import { alias } from "ck-orm";

const rewardLog = alias(orderRewardLog, "reward_log");
```

Aliased columns are rebound automatically to the alias.

### Column builders

The schema DSL covers the common ClickHouse type families:

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
| `compression.response` | Request compressed responses |
| `logger` / `logLevel` | Logger integration |
| `tracing` | OpenTelemetry integration |
| `instrumentation` | Custom query lifecycle hooks |

Session lifetime controls are intentionally request-scoped. Pass `session_timeout` to a single query or to `runInSession(...)`. Use `session_check` when you are continuing an existing `session_id`, not when bootstrapping a brand-new session.

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

Projection objects are built from public `Selection` values or columns. In practice that means table columns, `fn.*(...)`, and `expr(...)` outputs all compose the same way inside `select({ ... })`.

Implicit selection returns the full table model when there are no joins:

```ts
const rows = await db.select().from(orderRewardLog).limit(10);
```

With joins, implicit selection groups fields by source and returns nested objects.

### `from()`, `innerJoin()`, `leftJoin()`

```ts
import { alias, eq } from "ck-orm";

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
    eq(rewardLog.user_id, matchedRewardLog.user_id),
  );
```

### `where()` and condition helpers

Public condition helpers:

- `and`
- `or`
- `not`
- `eq`
- `ne`
- `gt`
- `gte`
- `lt`
- `lte`
- `between`
- `has`
- `hasAll`
- `hasAny`
- `like`
- `notLike`
- `ilike`
- `notIlike`
- `escapeLike`
- `inArray`
- `notInArray`
- `exists`
- `notExists`

`.where(...predicates)` is a variadic `AND` entrypoint. It ignores `undefined`, so you can either pass multiple predicates directly or build grouped predicates with `and(...)` and `or(...)`.

```ts
import { between, eq, inArray } from "ck-orm";

const query = db
  .select({
    userId: orderRewardLog.user_id,
    rewardPoints: orderRewardLog.reward_points,
  })
  .from(orderRewardLog)
  .where(
    eq(orderRewardLog.status, 1),
    inArray(orderRewardLog.campaign_id, [10, 20, 30]),
    between(orderRewardLog.created_at, 1710000000, 1719999999),
  );
```

`and(...)` skips `undefined`, which makes inline dynamic filters easy to assemble:

```ts
import { and, eq, gt, or } from "ck-orm";

const query = db
  .select({
    id: orderRewardLog.id,
    status: orderRewardLog.status,
  })
  .from(orderRewardLog)
  .where(
    and(
      minId !== undefined ? gt(orderRewardLog.id, minId) : undefined,
      status !== undefined
        ? or(eq(orderRewardLog.status, status), eq(orderRewardLog.status, 9))
        : undefined,
    ),
  );
```

For larger runtime-built filters, prefer `Predicate[]` plus variadic `.where(...predicates)`:

```ts
import { eq, gt, or, type Predicate } from "ck-orm";

const predicates: Predicate[] = [];

if (minId !== undefined) {
  predicates.push(gt(orderRewardLog.id, minId));
}

if (status !== undefined) {
  predicates.push(or(eq(orderRewardLog.status, status), eq(orderRewardLog.status, 9)));
}

const query = db
  .select({
    id: orderRewardLog.id,
    status: orderRewardLog.status,
  })
  .from(orderRewardLog)
  .where(...predicates);
```

`Predicate` is the public name for reusable boolean SQL clauses. You can use the same predicate objects in `where`, `having`, join `on` clauses, and boolean-aware helpers such as `exists(...)`.

`Selection` is the public name for reusable computed builder values such as `fn.sum(...)`, `fn.toString(...)`, and `expr(sql...)`. Use `.as(...)` to alias them and `.mapWith(...)` to override decoding. `Order` is the clause object returned by `asc(...)` and `desc(...)`.

`has(...)`, `hasAll(...)`, and `hasAny(...)` map directly to the native ClickHouse functions and keep ClickHouse's array, map, and JSON semantics.

`where(...)` is variadic, while `having(...)` takes a single predicate. For multi-clause `having`, compose the predicate first with `and(...)` or `or(...)`.

When you need to match literal `%` or `_` characters, escape the pattern before passing it to `like(...)`:

```ts
import { escapeLike, like } from "ck-orm";

const rows = await db
  .select({
    userId: orderRewardLog.user_id,
  })
  .from(orderRewardLog)
  .where(like(orderRewardLog.user_id, escapeLike("user_100%")));
```

### `groupBy()`, `having()`, `orderBy()`, `limit()`, `offset()`

```ts
import { desc, fn, gt } from "ck-orm";

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
  .having(gt(fn.sum(orderRewardLog.reward_points), "100.00000"))
  .orderBy(desc(orderRewardLog.created_at))
  .limit(20)
  .offset(0);
```

`groupBy()` and `limitBy([...])` accept columns and computed `Selection` values from helpers like `fn.*(...)` or `expr(...)`.

`orderBy()` accepts:

- `desc(selection)`
- `asc(selection)`
- a column directly

### `final()`

Append `FINAL` to a table query:

```ts
const query = db.select().from(orderRewardLog).final();
```

### `limitBy()`

Use ClickHouse `LIMIT ... BY ...`:

```ts
import { desc } from "ck-orm";

const query = db
  .select({
    userId: orderRewardLog.user_id,
    createdAt: orderRewardLog.created_at,
  })
  .from(orderRewardLog)
  .orderBy(desc(orderRewardLog.created_at))
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

### Subqueries and CTEs

Use `.as("alias")` to turn a builder into a subquery:

```ts
const latestRewardEvent = db
  .select({
    userId: orderRewardLog.user_id,
    createdAt: orderRewardLog.created_at,
  })
  .from(orderRewardLog)
  .orderBy(desc(orderRewardLog.created_at))
  .limit(10)
  .as("latest_reward_event");
```

Use `$with()` and `with()` for CTEs:

```ts
import { fn } from "ck-orm";

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
import { eq, gt } from "ck-orm";

const total = await db.count(
  orderRewardLog,
  eq(orderRewardLog.status, 1),
  gt(orderRewardLog.id, 1000),
);
```

For more complex result sets, count a subquery or CTE:

```ts
const activeUsers = db
  .select({
    userId: orderRewardLog.user_id,
  })
  .from(orderRewardLog)
  .where(eq(orderRewardLog.status, 1))
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

The forced `join_use_nulls = 1` setting is preserved when a joined query is reused as a subquery, CTE, `exists(...)`, or `inArray(...)` source, so builder types stay aligned with runtime behavior.

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

### `sql\`...\``

```ts
import { fn, sql } from "ck-orm";

const rows = await db.execute(sql`
  select
    ${orderRewardLog.user_id},
    ${fn.sum(orderRewardLog.reward_points)} as total_reward_points
  from ${orderRewardLog}
  where ${orderRewardLog.id} > ${10}
  group by ${orderRewardLog.user_id}
`);
```

### Plain strings and `sql("...")`

```ts
import { sql } from "ck-orm";

const rows = await db.execute("SELECT 1 AS one");
const sameRows = await db.execute(sql("SELECT 1 AS one"));
```

### `sql.raw()`, `sql.join()`, `sql.identifier()`

```ts
const fields = sql.join(
  [sql.identifier("user_id"), sql.identifier("reward_points")],
  ", ",
);

const rows = await db.execute(
  sql`${sql.raw("select ")}${fields}${sql.raw(" from ")}${sql.identifier("order_reward_log")}`,
);
```

### Raw SQL with `query_params`

```ts
import { sql } from "ck-orm";

const rows = await db.execute(
  sql`select user_id, reward_points from order_reward_log where user_id = {user_id:String} limit {limit:Int64}`,
  {
    query_params: {
      user_id: "user_100",
      limit: 10,
    },
  },
);
```

Parameter transport is chosen automatically. You do not need to configure multipart handling for `query_params`.

`query_params` keys that start with `orm_param` are rejected. That prefix is reserved for parameters generated internally by `sql\`...\``.

### `expr()`

Use `expr()` to wrap a raw SQL fragment as a reusable `Selection`:

```ts
import { expr, sql } from "ck-orm";

const query = db.select({
  constantOne: expr(sql.raw("1")).as("constant_one"),
});
```

### Raw query formats

Raw eager queries only support `JSON` output:

```ts
const rows = await db.execute("select 1", {
  format: "JSON",
});
```

Raw streaming queries only support `JSONEachRow` output:

```ts
for await (const row of db.stream("select 1", {
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
import { fn } from "ck-orm";

const query = db
  .select({
    month: fn.toStartOfMonth(orderRewardLog.created_at).as("month"),
    totalRewardPoints: fn.sum(orderRewardLog.reward_points).as(
      "total_reward_points",
    ),
  })
  .from(orderRewardLog);
```

### `tableFn`

```ts
import { fn, tableFn } from "ck-orm";

const numbers = tableFn.call("numbers", 10).as("n");

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
const tmpScope = chTable("tmp_scope", {
  user_id: string(),
});

await db.runInSession(async (sessionDb) => {
  // Temporary tables live only inside this Session and are cleaned up automatically.
  await sessionDb.createTemporaryTable(tmpScope);
  await sessionDb.insertJsonEachRow(tmpScope, [
    { user_id: "user_100" },
    { user_id: "user_200" },
  ]);

  return sessionDb.execute(`
    SELECT user_id
    FROM order_reward_log
    WHERE user_id IN (SELECT user_id FROM tmp_scope)
  `);
});
```

### Session behavior

- queries inside a session are serialized to protect temporary table lifecycle
- `createTemporaryTable()` automatically registers the table for cleanup
- `createTemporaryTable()` consumes schema objects; temporary-table lifecycle stays on `Session`, not on the schema itself
- `createTemporaryTableRaw()` is the trusted-only raw SQL escape hatch and rejects multi-statement definitions
- `runInSession()` drops registered temporary tables when the callback finishes
- nested `runInSession()` calls may reuse the same session
- nested calls may not switch to a different `session_id`

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

### `execute()`

Execute a raw query and return `Record<string, unknown>[]`:

```ts
const rows = await db.execute("SELECT 1 AS one");
```

### `stream()`

Stream raw query results:

```ts
for await (const row of db.stream("SELECT number FROM numbers(10)")) {
  console.log(row);
}
```

### `command()`

Execute a command that does not return a row set:

```ts
await db.command("SYSTEM FLUSH LOGS");
```

### `withSettings()`

Create a child client with additional default settings:

```ts
const reportDb = db.withSettings({
  max_execution_time: 60,
  join_use_nulls: 0,
});
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
import { trace } from "@opentelemetry/api";

const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123/demo_store",
  schema: commerceSchema,
  tracing: {
    tracer: trace.getTracer("ck-orm-example"),
    dbName: "demo_store",
  },
});
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

Public error types:

- `ClickHouseOrmError`
- `DecodeError`

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

- identifiers passed to `sql.identifier()` are validated and quoted
- function names used by `fn.call(...)`, `fn.withParams(...)`, `aggregateFunction(...)`, `simpleAggregateFunction(...)`, and `nested({...})` keys are validated
- values interpolated into `sql\`...\`` become ClickHouse named parameters rather than raw SQL text
- `query_params` keys that start with `orm_param` are rejected because that prefix is reserved for internal SQL-template parameters
- only a single top-level statement is allowed per request
- authorization headers derived from connection config cannot be overridden by user-supplied `http_headers`
- structured connection config rejects credentials embedded in `host`
- tracing destination data is normalized before it is attached to spans

### Unsafe APIs

Treat the following APIs like `eval`: only pass developer-controlled strings, never user-derived input.

- `sql.raw(value)`
- `db.createTemporaryTableRaw(name, definition)`

`fn.call(name, ...)` and `fn.withParams(name, ...)` validate `name`, but you should still treat dynamically chosen function names as developer-controlled input rather than end-user input.

`sql.identifier(...)` validates on the compile/execute boundary. Constructing the fragment is cheap; the error appears when the fragment is compiled into a query.

### Tracing data exposure

`tracing.includeStatement` defaults to `true`, so compacted SQL text is attached to spans as `db.statement`.

Bound values are stored separately as ClickHouse named parameters and are not included in the statement text, but table names, column names, and query shape still appear in tracing output. If your trace backend is shared or off-host, disable `includeStatement` or filter `db.statement` in your collector.

### Error responses

`ClickHouseOrmError.responseText` and the error `message` may contain raw text from the ClickHouse server, including SQL fragments and database object names.

Do not forward those directly to untrusted clients. Log them server-side and expose a generic message instead.

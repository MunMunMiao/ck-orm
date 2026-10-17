## 安装
<!-- src:e86aaf8a -->

```bash [bun]
bun add ck-orm
```

```bash [npm]
npm install ck-orm
```

```bash [pnpm]
pnpm add ck-orm
```

```bash [yarn]
yarn add ck-orm
```

ck-orm 没有必需依赖。OpenTelemetry 追踪是单独的入口 `ck-orm/otel`，`@opentelemetry/api` 是可选的对等依赖项，只有这个入口会用到它。`ck-orm` 主入口从不导入 OpenTelemetry，所以不做追踪的应用、浏览器端的打包产物和边缘 worker 既不用安装它，也不会把它打包进去。要追踪查询，请按[OpenTelemetry 插件](#opentelemetry-plugin)里的步骤设置。

## 快速上手
<!-- src:847057c0 -->

下面的示例只用一张表，方便看清主流程。

### 1. 定义模式

```ts
import { ckTable, ckType } from "ck-orm";

export const probeTelemetry = ckTable(
  "probe_telemetry",
  {
    id: ckType.int32(),
    probeId: ckType.string("probe_id"),
    missionId: ckType.int32("mission_id"),
    sampleId: ckType.int64("sample_id"),
    signalStrength: ckType.decimal("signal_strength", { precision: 20, scale: 5 }),
    status: ckType.int16(),
    createdAt: ckType.int32("created_at"),
    deletedAt: ckType.nullable(
      "deleted_at",
      ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    ),
    ingestedAt: ckType.dateTime64("_ingested_at", { precision: 9 }),
    isDeleted: ckType.uint8("_is_deleted"),
    ingestVersion: ckType.uint64("_ingest_version"),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.probeId, table.createdAt, table.id],
    versionColumn: table.ingestVersion,
  }),
);
```

### 2. 创建客户端

```ts
import { clickhouseClient } from "ck-orm";

export const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  username: "default",
  password: "<password>",
  clickhouse_settings: {
    allow_experimental_correlated_subqueries: 1,
    max_execution_time: 10,
  },
});
```

### 3. 查询数据

```ts
import { ck, fn } from "ck-orm";
import { db } from "./db";
import { probeTelemetry } from "./schema";

const query = db
  .select({
    probeId: probeTelemetry.probeId,
    totalSignalStrength: fn.sum(probeTelemetry.signalStrength).as(
      "total_signal_strength",
    ),
  })
  .from(probeTelemetry)
  .where(ck.eq(probeTelemetry.status, 1))
  .groupBy(probeTelemetry.probeId)
  .orderBy(ck.desc(fn.sum(probeTelemetry.signalStrength)))
  .limit(20);

const rows = await query;
```

构建器查询是 thenable，所以 `await query` 会直接执行查询。

对 `Decimal` 列调用 `fn.sum` 时，结果会自动转成 `Decimal(P, S)`，并返回 `string`，这样精度才不会丢。详情见[表达式中的 Decimal 精度](#decimal-precision-in-expressions)。

### 4. 观测查询

`plugins` 可以挂到 ck-orm 发给 ClickHouse 的每个请求上。SDK 运行起来之后，`ck-orm/otel` 的 `otelTracing()` 会把每个请求变成一个 OpenTelemetry span；设置步骤见[OpenTelemetry 插件](#opentelemetry-plugin)，自己写钩子见[编写插件](#writing-plugins)。

```ts
import { clickhouseClient } from "ck-orm";
import { otelTracing } from "ck-orm/otel";

export const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  plugins: [otelTracing()],
});
```

## 示例
<!-- src:c8f5c20b -->

[示例页面](https://ck-orm.lyz.cloud/examples/)展示可运行的查询，并列出每条查询发给 ClickHouse 的确切 SQL。每个示例都是 [`examples/`](../examples) 里的独立项目，各自带测试。可运行的 ClickHouse 测试都在 [`e2e/`](../e2e) 里，其中 [`e2e/scenarios.ts`](../e2e/scenarios.ts) 还包含贴近生产环境的场景模式。

## 心智模型
<!-- src:a6fa96d2 -->

用 `ck-orm` 时，先记住这些边界：

- `ckType.*` 定义模式中的列类型。
- `ckTable(...)` 定义表的模式。
- `ck.*` 是查询辅助函数的命名空间。
- `fn.*` 是 SQL 函数辅助函数的命名空间。
- 模式描述的是表和列，不是数据库名。
- 数据库连接挂在 `clickhouseClient(...)` 上。
- 默认走查询构建器；需要 ClickHouse 专用表达式时，再用原始 SQL。
- `runInSession()` 是 ClickHouse 的会话辅助函数，不是事务。
- `plugins` 观察 ck-orm 发出的每个请求；追踪是按需启用的 `ck-orm/otel` 插件，不在主入口里。
- `leftJoin()` 和 `asofLeftJoin()` 默认使用 SQL 风格的 NULL 语义。
- 大数或高风险数值结果要谨慎解码，不要静默强制转换。

`ck-orm` 专注于带类型的 ClickHouse 访问：表定义、查询组合、写入辅助函数、会话辅助函数和可观测性。`createTable()`、`createMaterializedView()` 和 `dropTable()` 各自运行一条 DDL 语句；模式迁移（比较模式、`ALTER`、版本管理）仍交给应用使用的迁移工具处理。

## 模式 DSL
<!-- src:5149bff0 -->

### `ckTable()`

用 `ckTable(name, columns, options?)` 定义表。

下面的示例都使用这段导入：

```ts
import { ckTable, ckType, ckSql } from "ck-orm";
```

```ts
const probeTelemetry = ckTable("probe_telemetry", {
  id: ckType.int32(),
  probeId: ckType.string("probe_id"),
  sampleId: ckType.int64("sample_id"),
  signalStrength: ckType.decimal("signal_strength", { precision: 20, scale: 5 }),
  createdAt: ckType.int32("created_at"),
  deletedAt: ckType.nullable(
    "deleted_at",
    ckType.dateTime64({ precision: 3, timezone: "UTC" }),
  ),
  ingestVersion: ckType.uint64("_ingest_version"),
});
```

第三个参数可以是普通对象，也可以是工厂函数：

```ts
const probeTelemetry = ckTable(
  "probe_telemetry",
  {
    id: ckType.int32(),
    probeId: ckType.string("probe_id"),
    sampleId: ckType.int64("sample_id"),
    signalStrength: ckType.decimal("signal_strength", { precision: 20, scale: 5 }),
    createdAt: ckType.int32("created_at"),
    deletedAt: ckType.nullable(
      "deleted_at",
      ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    ),
    ingestVersion: ckType.uint64("_ingest_version"),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.probeId, table.createdAt, table.id],
    versionColumn: table.ingestVersion,
  }),
);
```

可配置的表选项：

- `engine`
- `partitionBy`
- `primaryKey`
- `orderBy`
- `sampleBy`
- `ttl`
- `settings`
- `comment`
- `versionColumn`

列定义也可以直接带有 DDL 元数据：

- `.default(expr)`
- `.materialized(expr)`
- `.aliasExpr(expr)`
- `.comment(text)`
- `.codec(expr)`
- `.ttl(expr)`

在 `ck-orm` 中，模式元数据有两个用途：

- 支撑带类型的查询、插入验证和结果解码
- 为 `createTable()` 和 `createTemporaryTable()` 渲染 `CREATE TABLE` 语句

它不会迁移或同步已有的 ClickHouse 表：`createTable()` 只创建表（见[表和物化视图](#tables-and-materialized-views)）。模式迁移留在迁移工具里，`ckTable(...)` 要与应用实际读写的模式保持一致。

示例：

```ts
const probeTelemetry = ckTable(
  "probe_telemetry",
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

### 类型推断

每张表都提供：

- `table.$inferSelect`
- `table.$inferInsert`

```ts
type TelemetryRow = typeof probeTelemetry.$inferSelect;
type TelemetryInsert = typeof probeTelemetry.$inferInsert;

const sampleId: TelemetryRow["sampleId"] = "900001";
const ingestVersion: TelemetryInsert["ingestVersion"] = "1";
```

写泛型辅助函数时，使用：

- `InferSelectModel<TTable>`
- `InferInsertModel<TTable>`
- `InferSelectSchema<TSchema>`
- `InferInsertSchema<TSchema>`

```ts
import type { InferInsertModel, InferSelectModel, InferSelectSchema } from "ck-orm";

type TelemetryRow = InferSelectModel<typeof probeTelemetry>;
type TelemetryInsert = InferInsertModel<typeof probeTelemetry>;

const telemetryTables = {
  probeTelemetry,
};

type TelemetryRows = InferSelectSchema<typeof telemetryTables>;
```

传给 `InferSelectSchema` 和 `InferInsertSchema` 的模式对象只是普通 TypeScript 对象的组合。这样既能复用模型类型，也不会和客户端配置混在一起。

### 用 `$type<T>()` 细化列类型

每个列构建器都提供可链式调用的 `$type<T>()` 方法。它只改列的 TypeScript 类型，不改变任何运行时行为。默认推断出的类型在你的业务里太宽泛时，就用它收紧：

```ts
import { ckTable, ckType } from "ck-orm";

type UserId = number & { readonly __brand: "UserId" };

const users = ckTable("users", {
  id: ckType.int32().$type<UserId>(),
  role: ckType.string().$type<"guest" | "user" | "admin">(),
  preferences: ckType.json().$type<{ theme: "light" | "dark"; betaFeatures: readonly string[] }>(),
});

type UserRow = typeof users.$inferSelect;
// {
//   id: UserId;
//   role: "guest" | "user" | "admin";
//   preferences: { theme: "light" | "dark"; betaFeatures: readonly string[] };
// }
```

`$type<T>()` 可以和其他列修饰符按任意顺序链式调用：`ckType.string().$type<Role>().default(ckSql`'guest'`).comment("…")`。运行时解码器、编码器、SQL 类型和 DDL 输出都不受影响；`$type` 只是编译期断言。

`enum8` 和 `enum16` 已经会从值对象推断出字面量联合类型。只有需要给这个联合类型换上自定义类型，或让它与外部枚举类型对齐时，才用 `$type`：

```ts
// Inferred automatically — no `as const`, no explicit type arguments
const status = ckType.enum8({ active: 1, paused: 2, banned: 3 });
//    ↑ Enum8<"active" | "paused" | "banned">

// Re-brand on top of the inferred keys
type UserStatus = "active" | "paused" | "banned";
const userStatus = ckType.enum8({ active: 1, paused: 2, banned: 3 }).$type<UserStatus>();
```

> **注意事项：** `$type<T>()` 不验证运行时值。如果数据库行里有声明类型之外的值（比如模式迁移前留下的旧数据），TypeScript 仍会把它当成匹配值来收窄类型，带穷尽检查的 `switch` 也会静默漏掉这种情况。需要在读取时校验值，就在投影中使用 `mapWith`；需要在每次解码和编码时都校验，就用下面的 `$validator()`。

### 用 `$type<{ select, insert }>()` 区分插入和查询类型

ClickHouse 的列有时允许插入比查询结果更宽的类型。例如，`DateTime` 列插入时同时接受 `string` 和 `Date`，但解码后始终返回 `Date`。给 `$type` 传一个对象，就能区分这两种类型：

```ts
const events = ckTable("events", {
  id: ckType.int32(),
  occurredAt: ckType.dateTime().$type<{ select: Date; insert: string | Date }>(),
});

type EventSelect = typeof events.$inferSelect;
// { id: number; occurredAt: Date }

type EventInsert = typeof events.$inferInsert;
// { id: number; occurredAt: string | Date }
```

只传 `$type<{ select: T }>()`（不写 `insert`）等价于只带一个泛型参数的 `$type<T>()`。

### 插入模型规则：DEFAULT、MATERIALIZED、ALIAS

`$inferInsert` 会根据模式中的列结构推导，而不是直接从 `$inferSelect` 复制：

| DDL 修饰符 | 对插入模型的影响 |
| --- | --- |
| `.default(expr)` | 列会变成可选；不传这一列，ClickHouse 会补上默认值 |
| `.materialized(expr)` | 这一列不会出现在插入模型里 |
| `.aliasExpr(expr)` | 这一列不会出现在插入模型里 |

```ts
const users = ckTable("users", {
  id: ckType.int32(),
  name: ckType.string().default(ckSql`'anonymous'`),
  computedAt: ckType.dateTime().materialized(ckSql`now()`),
  derivedTag: ckType.string().aliasExpr(ckSql`upper(name)`),
});

// id is required; name is optional (has default); computedAt/derivedTag are
// not part of $inferInsert at all.
db.insert(users).values({ id: 1 });
db.insert(users).values({ id: 2, name: "alice" });
```

### 用 `$validator(schema)` 做运行时验证

如果 `$type` 不够用——数据库里可能留着旧数据、你要接收来自不受信任上游的 JSON 结构，或者想让插入 API 把字符串强制转换成 `Date`——就改成链式调用 `$validator(schema)`。任何满足 [Standard Schema v1 规范](https://github.com/standard-schema/standard-schema) 的对象都可以：Zod 3.23+、Valibot 1+、ArkType、Effect Schema、TypeBox。ck-orm 不依赖其中任何库。

```ts
import { z } from "zod";

const ProfileSchema = z.object({
  avatarUrl: z.string().url(),
  bio: z.string().optional(),
});

const users = ckTable("users", {
  id: ckType.int32(),
  profile: ckType.json().$validator(ProfileSchema),
});

// On select, the decoded JSON is run through the schema; on failure ck-orm
// throws a DecodeError with the schema's issue messages.
const rows = await db.select().from(users).execute();
//    rows[0].profile is typed as z.infer<typeof ProfileSchema>

// On insert, the value is validated before being encoded; on failure ck-orm
// throws a client_validation error.
await db.insert(users).values({ id: 1, profile: { avatarUrl: "https://…" } });
```

模式转换也会生效（例如 `z.string().transform((v) => new Date(v))`）：插入时列接收模式的 *输入* 类型，查询时暴露模式的 *输出* 类型。`$validator` 只支持同步的模式对象；异步模式会在解码或编码时抛出错误。

`$type` 与 `$validator` 的对比：

| | `$type<T>()` | `$validator(schema)` |
| --- | --- | --- |
| 运行时开销 | 零 | 每行每列同步校验一次 |
| 类型安全 | 编译期断言 | 从模式推断 |
| 运行时验证 | 否 | 是；不匹配时会抛出错误 |
| 输入与输出类型不同 | 用 `$type<{ select, insert }>()` | 用模式转换 |
| 外部依赖项 | 无 | 无（模式库由你选择） |

### JSON 列类型

ck-orm 面向 ClickHouse 24.x+ 的 **新版 JSON 数据类型**
（[文档](https://clickhouse.com/docs/sql-reference/data-types/newjson)）：这种类型会把 JSON 对象拆成一组带类型的子列，而不是把整个值存成一个不透明字符串。先记住两条基本规则：

- ClickHouse 的 `JSON` 数据类型**只接受顶层对象**。顶层的数组、字符串和数字都会被直接拒绝。ck-orm 会在编译期通过 `T extends Record<string, unknown>` 强制同样的约束，并在每次插入/查询时用一次廉价的 O(1) 检查在运行时再挡一遍。
- 带类型路径（`a.b UInt32`）会在 ClickHouse 中变成真正的子列；ck-orm 会让这些路径走对应 ck-orm 列工厂的 `mapFromDriverValue` / `mapToDriverValue`，因此声明成 `ckType.uint64()` 的路径会像顶层 `uint64` 列一样，无损解码成 `string`。

最简单的写法和其他 ck-orm 列工厂一样：

```ts
import { ckTable, ckType } from "ck-orm";

const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json("payload"),
});
// DDL:  `payload` JSON
// $inferSelect.payload: { [key: string]: unknown }
```

传入泛型后，`$inferSelect` / `$inferInsert` 就会给出实际结构：

```ts
type EventPayload = { user_id: number; event: string; meta?: { ip: string } };

const userEvents = ckTable("user_events", {
  id: ckType.uint64(),
  payload: ckType.json<EventPayload>("payload"),
});
```

如果想把泛型与其他列构建器调用放在一起，也可以用可链式调用的等价写法 `ckType.json<T>().$type<T2>()`。

#### DDL 参数

第二个工厂参数可以设置 NewJSON 的 4 个 DDL 参数：

```ts
const userEvents = ckTable("user_events", {
  id: ckType.uint64(),
  payload: ckType.json<{
    user_id: string;
    created_at: Date;
    user: { name: string; tier: number };
  }>("payload", {
    maxDynamicPaths: 256,
    maxDynamicTypes: 8,
    typeHints: {
      user_id: ckType.uint64(),
      created_at: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
      "user.tier": ckType.uint32(),
    },
    skip: ["debug", "internal"],
    skipRegexp: ["^_tmp"],
  }),
});

// DDL:
//   `payload` JSON(
//     max_dynamic_paths=256,
//     max_dynamic_types=8,
//     created_at DateTime64(3, 'UTC'),
//     user.tier UInt32,
//     user_id UInt64,
//     SKIP debug,
//     SKIP internal,
//     SKIP REGEXP '^_tmp'
//   )
```

`typeHints` 的键会按 `Paths<T>` 做类型检查；像 `{ "user.idd": ... }` 这样的拼写错误会在编译期失败。生成的 DDL 始终按稳定的字典序输出，因此不管配置怎么写，外部模式对比工具看到的输出都逐字节一致。

#### typeHints 会影响运行时解码器

ClickHouse 的 64 位无损策略会让 ck-orm 把 `UInt64` 解码成字符串。如果在 TypeScript 类型结构 *和* `typeHints` 条目中都声明了 `user_id`，两者必须保持一致；对 `uint64` 来说，泛型里要写 `user_id: string`。如果插入时接受的类型和查询时解码出的类型不同，就用 `$type<{ select, insert }>()`：

```ts
ckType.json("payload", { typeHints: { user_id: ckType.uint64() } })
  .$type<{
    select: { user_id: string };
    insert: { user_id: string | number | bigint };
  }>();
```

#### 路径访问

每个 JSON 列都有 5 个可链式调用的路径方法，它们会原样渲染 ClickHouse 的路径访问语法：

| 方法 | SQL | 含义 |
| --- | --- | --- |
| `col.path("a.b")` | `col.a.b` | 读取路径；静态类型是 `PathValue<T, "a.b">`（见下面关于 `typeHints` 的备注） |
| `col.castPath("a.b", ckType.uint64())` | `col.a.b.:UInt64` | 通过另一个 ck-orm 列工厂强制解码路径；静态类型来自转换列的 `TData` |
| `col.subobject("a")` | `col.^a` | 把嵌套对象作为 `JSON` 值读取 |
| `col.merged("a")` | `col.@a` | 读取合并后的共享数据视图 |
| `col.arrayPath("a")` | `col.a[]` | 展开数组类型的路径 |

> `col.path(P)` 的静态类型始终是 `PathValue<T, P>`；它的类型根据泛型 `T` 推导，不是从 `typeHints` 来的。`typeHints` 只改变运行时解码器；静态类型与 typeHint 必须手动保持同步（上文说明了 `uint64 → string` 的无损策略）。两者不一致时，改用 `castPath(P, ckType.X())`；它的返回类型来自转换列。

```ts
import { gt } from "ck-orm";

const recentVip = await db
  .select({
    id: userEvents.id,
    uid: userEvents.payload.path("user_id"),     // SQL: payload.user_id   type: string
    when: userEvents.payload.path("created_at"), // SQL: payload.created_at type: Date
    tier: userEvents.payload.path("user.tier"),  // SQL: payload.user.tier  type: number
  })
  .from(userEvents)
  .where(gt(userEvents.payload.path("user.tier"), 5))
  .execute();
```

`Paths<T>` 会在编译期验证路径字面量；当 `"a.zzz"` 不属于 `T` 时，`payload.path("a.zzz")` 无法通过类型检查。

路径方法会在运行时由每个链式修饰符重新挂载（包括 `$type`、`$validator`、`.default()`、`.materialized()`、`.bind()` 等），所以底层调用始终可用。**静态类型会跟踪除 `$validator` 外的每个链式环节**：`$validator` 的契约以 `StandardSchemaV1<unknown, unknown>` 泛型为基础；如果直接收窄到 `JsonShape`，就会破坏 TypeScript 强制执行的 LSP 风格子类型规则，所以经过这一环后，静态类型会退回 `Column<T>`。方法本身仍然可以调用；只是想继续链式调用时，要补一次类型断言：

```ts
const events = ckTable("events", {
  payload: ckType.json<{ a: number }>("payload")
    .$type<{ a: number; b: string }>()
    .default(ckSql`'{}'`),
});

// Type and runtime both happy — JsonColumn shape preserved through
// `$type` + `.default()`.
await db.select({ b: events.payload.path("b") }).from(events).execute();
```

```ts
const validated = ckTable("validated", {
  payload: ckType.json<{ a: number }>("payload").$validator(someSchema),
});
// Runtime works, but the static type lost `.path` after $validator —
// cast to JsonColumn (or call through the validator's TData) to use it:
const col = validated.payload as unknown as JsonColumn<{ a: number }, "validated">;
await db.select({ a: col.path("a") }).from(validated).execute();
```

#### `fn.json*`：动态路径调用点

如果路径字符串要到运行时才拼出来（或者要读取 `T` 中未声明的路径），就用 `fn.*` 命名空间：

```ts
import { fn } from "ck-orm";

const userPath = userInput as string;
await db.select({
  raw:    fn.jsonPath<string>(userEvents.payload, userPath),
  casted: fn.jsonCast(userEvents.payload, userPath, ckType.string()),
  shape:  fn.dynamicType(fn.jsonPath(userEvents.payload, userPath)),
});
```

`fn.jsonCast` 是 `castPath` 的动态版本；`fn.jsonSubobject`、`fn.jsonMerged` 和 `fn.jsonArray` 分别对应同名列方法；`fn.dynamicType` 暴露 ClickHouse 的 `dynamicType()` 函数，用来查看 Dynamic 类型路径在运行时的实际类型。

#### 运行时验证

每一行都会经过两层处理，**顺序如下**：

1. **ck-orm 内置的 JSON 防护**——`null` / `undefined` 原样通过，所以 `nullable()` JSON 列可以正常工作；其他非对象输入（数组、字符串、数字、布尔值）都会被直接拒绝，并给出与该列关联的 `client_validation` / `decode` 错误，例如 `JSON column expects a plain object, got array`。`typeHints` 中的带类型路径会在写入和读取时经过对应列工厂的编码器/解码器。始终启用。
2. **可选的 `$validator(schema)`**——如果存在，插入时它会在内置编码器*之前*执行，查询时会在内置解码器*之后*执行。需要做结构级验证时就用它：

   ```ts
   import { z } from "zod";

   const PayloadSchema = z.object({
     user_id: z.string().min(1),
     event: z.enum(["login", "logout", "click"]),
   });

   const events = ckTable("events", {
     payload: ckType
       .json("payload", { typeHints: { user_id: ckType.uint64() } })
       .$validator(PayloadSchema),
   });
   ```

ck-orm 有意**不递归**检查 JSON 值，也不会判断 `T["a"]` 是否真的是 `number` 等类型。需要这种检查时就用 `$validator`；JSON 防护只负责拦截那些很难从 ClickHouse 自身错误信息反查到具体列的情况。顶层标量参数会直接报错，并在错误中带上所属列的列名：

```ts
await db.insert(events).values({ payload: ["wrong"] as never });
// throws: JSON column expects a plain object, got array
```

#### 插入时的默认值

JSON 列和其他列一样遵循 `$inferInsert` 规则。`.default()` 会让字段变为可选；`.materialized()` / `.aliasExpr()` 会把它从插入模型中彻底移除：

```ts
const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json<{ note: string }>("payload").default(ckSql`'{}'`),
});

await db.insert(auditLogs).values({ id: "1" }); // payload filled by CH DEFAULT
```

### `ckAlias()`

当同一个表需要在查询中出现多次时，就使用 `ckAlias()`。

```ts
import { fn, ck, ckAlias } from "ck-orm";

const telemetry = ckAlias(probeTelemetry, "telemetry");
```

`ckAlias()` 返回的列会自动重新绑定到别名。

### 列名

模式对象的键就是逻辑键。TypeScript 中的行、解码后的查询结果和插入值都用它。默认情况下，这个键也作为 ClickHouse 列名：

```ts
const telemetry = ckTable("telemetry", {
  signalStrength: ckType.decimal({ precision: 20, scale: 5 }),
});
```

如果数据库列用了别的名字，就把物理列名作为第一个参数传入：

```ts
const telemetry = ckTable("telemetry", {
  probeId: ckType.string("probe_id"),
  signalStrength: ckType.decimal("signal_strength", { precision: 20, scale: 5 }),
  createdAt: ckType.dateTime64("created_at", { precision: 9 }),
});

await db.insert(telemetry).values({
  probeId: "probe_alpha",
  signalStrength: "12.50000",
  createdAt: new Date(),
});
```

SQL、DDL、过滤条件、排序、分组和写入时的列列表都用物理名（`probe_id`、`signal_strength`、`created_at`）。推断出的模型、默认查询结果、显式投影键和插入值则用模式对象键（`probeId`、`signalStrength`、`createdAt`）。

每个公开的 `ckType` 构建器都支持外层的可选物理列名。不需要额外配置的构建器直接接受 `name?`；需要类型配置的构建器把可选物理列名放在前面，类型配置放进对象：

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

`aggregateFunction` 和 `simpleAggregateFunction` 有 ClickHouse 专用的调用形式：第一个字符串是聚合函数名，不是列名：

```ts
ckType.aggregateFunction("sum", ckType.uint64());
ckType.aggregateFunction("quantile(0.5)", ckType.float64());
ckType.simpleAggregateFunction("sum", ckType.uint64());
```

`AggregateFunction` 接受带字面量参数的 ClickHouse 聚合函数名，例如 `quantile(0.5)` 或 `topK(10)`。`simpleAggregateFunction` 的规则更严格，只接受不带字面量参数的普通聚合函数名。

如果还要指定物理列名，就使用对象配置：

```ts
const aggregateStateColumns = ckTable("aggregate_state_columns", {
  signalSumState: ckType.aggregateFunction("signal_sum_state", {
    name: "sum",
    args: [ckType.decimal({ precision: 20, scale: 5 })],
  }),
  signalSum: ckType.simpleAggregateFunction("signal_sum", {
    name: "sum",
    value: ckType.decimal({ precision: 20, scale: 5 }),
  }),
});
```

`nested("items", shape)` 指定外层 `Nested(...)` 列的名字。嵌套字段名取 `shape` 的键；内部列的 `configuredName` 值不会用作嵌套字段名：

```ts
const sensorPackets = ckTable("sensor_packets", {
  items: ckType.nested("items", {
    componentId: ckType.string(),
    quantity: ckType.float64(),
  }),
});
```

### 列构建器

用 `ckType.*` 构建模式中的列。模式 DSL 覆盖这些常见的 ClickHouse 类型族：

- 整数：`int8`、`int16`、`int32`、`int64`、`uint8`、`uint16`、`uint32`、`uint64`
- 浮点数和 Decimal：`float32`、`float64`、`bfloat16`、`decimal`
- 标量类型：`bool`、`string`、`fixedString`、`uuid`、`ipv4`、`ipv6`
- 时间类型：`date`、`date32`、`dateTime`、`dateTime64`、`time`、`time64`
- 枚举和特殊类型：`enum8`、`enum16`、`json`、`dynamic`、`qbit`
- 容器：`nullable`、`array`、`tuple`、`map`、`nested`、`variant`、`lowCardinality`
- 聚合类型：`aggregateFunction`、`simpleAggregateFunction`
- 几何类型：`point`、`ring`、`lineString`、`multiLineString`、`polygon`、`multiPolygon`

模式驱动的读取、写入和推断模型都把 `int64`、`uint64` 默认映射为 TypeScript `string`。这样无论经过 ClickHouse JSON 传输格式还是 JavaScript 运行时，64 位值都能保持精确。确实需要 `bigint` 时，可以传入自己的解码器完成转换，例如 `mapWith((value) => BigInt(String(value)))`。

ClickHouse 不支持 `Nullable(Array(...))`、`Nullable(Map(...))` 或 `Nullable(Tuple(...))`。`ck-orm` 会在模式定义时直接拒绝这些结构。把 `nullable(...)` 放进复合类型内部，例如 `ckType.array(ckType.nullable(ckType.string()))`。

带类型配置的构建器使用配置对象；如果要指定物理列名，就把它放在配置对象前面：

```ts
ckType.decimal({ precision: 20, scale: 5 });
ckType.decimal("signal_strength", { precision: 20, scale: 5 });
ckType.fixedString({ length: 8 });
ckType.dateTime64("created_at", { precision: 9, timezone: "UTC" });
ckType.qbit("embedding", ckType.float32(), { dimensions: 8 });
```

`Date` / `Date32` 的写入值和模式驱动的谓词值会把 JavaScript `Date` 按 UTC 日历日期发送（`2026-06-15T23:00:00Z` 会变成 `2026-06-15`）。要存别的日期，就传入 `YYYY-MM-DD` 字符串；字符串和数字会原样发送。

在 `DateTime` / `DateTime64` 谓词值里绑定 JavaScript `Date` 时，它会按 UTC 时刻处理，所以列、会话或服务器时区都不能改变这个时刻。字符串不转换，由 ClickHouse 按列时区读取。在 32 位 `DateTime` 位置，如果该参数中的 Date 都是整秒，就绑定为 `DateTime('UTC')`；只要有一个带毫秒，就绑定为 `DateTime64(3, 'UTC')`。`DateTime64` 位置总是绑定为 `DateTime64(3, 'UTC')`。`gte` 这类标量谓词仍会比较精确时刻。`DateTime` 列上的 `inArray` / `notInArray` 要求其中的 Date 都是整秒；带毫秒的 Date 会被 ClickHouse 拒绝（`TYPE_MISMATCH`）。这是 ClickHouse 本身的行为，而且这种值本来也不可能相等。`insert().values()` 会按列精度把每个 `Date` 发送为整数时间刻度。见“写入”章节。

### 列类型速查

常见模式结构，以及对应的 TypeScript 值：

| ClickHouse 结构 | 模式 | TypeScript 值结构 |
| --- | --- | --- |
| 枚举 | `ckType.enum8({ idle: 1, active: 2 })` | `"idle" | "active"`（从键推断） |
| 低基数字符串 | `ckType.lowCardinality(ckType.string())` | `string` |
| 可为 NULL 的 Decimal 类型 | `ckType.nullable(ckType.decimal({ precision: 18, scale: 5 }))` | `string | null` |
| 数组 | `ckType.array(ckType.string())` | `string[]` |
| 可为 NULL 的数组元素 | `ckType.array(ckType.nullable(ckType.string()))` | `(string | null)[]` |
| 元组 | `ckType.tuple(ckType.string(), ckType.int32())` | `[string, number]` |
| 映射 | `ckType.map(ckType.string(), ckType.string())` | `Record<string, string>` |
| 嵌套对象数组 | `ckType.nested({ component: ckType.string(), quantity: ckType.float64() })` | `{ component: string; quantity: number }[]` |
| 变体类型 | `ckType.variant(ckType.string(), ckType.int32())` | `string | number` |
| JSON | `ckType.json<{ risk?: { score?: number } }>()` | `{ risk?: { score?: number } }` |

`ckType.map(...)` 目前只支持 `String` 键，并把它们映射为 JavaScript 对象，因此无法表达 ClickHouse `Map(K, V)` 中重复键的边界情况。

插入行的推断结构和 `typeof table.$inferInsert` 一致；调用 `insert(table).values(...)` 时，可以省略带 ClickHouse 默认值或生成表达式的列。生成的 `INSERT` 列列表永远不会包含 `MATERIALIZED` 和 `ALIAS` 列；显式传入这些列会被拒绝。

## 客户端配置
<!-- src:26d96c74 -->

用 `clickhouseClient()` 创建客户端：

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:<password>@127.0.0.1:8123/telemetry_lab",
});
```

### 连接模式

`clickhouseClient()` 支持两种互斥的连接模式。

#### `databaseUrl`

如果只想用一个连接字符串，就使用 `databaseUrl`：

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:secret@127.0.0.1:8123/telemetry_lab",
});
```

使用 `databaseUrl` 时，不要再传入以下字段：

- `host`
- `database`
- `username`
- `password`
- `pathname`

#### 结构化连接字段

如果想分别配置各个部分，就显式填写这些字段：

```ts
const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  username: "default",
  password: "<password>",
});
```

结构化模式的默认值：

- `host`: `http://localhost:8123`
- `database`: `default`
- `username`: `default`
- `password`: `""`

### 常用字段

大多数项目只需要配置少数几个客户端字段：

| 字段 | 用途 |
| --- | --- |
| `request_timeout` | 每个请求可选的实际耗时上限，单位为毫秒。省略时，ck-orm 不会强制设置客户端超时——请求持续多久由底层 fetch、平台、服务器和 `abort_signal` 决定。对于长时间运行的流式查询，不要设置它，改用 `abort_signal` 控制实际耗时预算（例如 `AbortSignal.timeout(30_000)`）。 |
| `clickhouse_settings` | 默认的 ClickHouse 会话/查询设置 |
| `application` | 设置 ClickHouse 应用名称 |

表定义不依赖连接配置。用 `ckTable(...)` 定义表，在组装查询的地方导入表对象，再直接把它们传给 `.from(...)`、`.insert(...)` 和临时表辅助函数。

### 高级字段

下面这些字段用来控制更高级的运行时行为：

| 字段 | 用途 |
| --- | --- |
| `fetch` | 每个 HTTP 请求都会调用 `(request: Request) => Response \| Promise<Response>`。它可以是任意 fetch 实现、服务器处理函数，或 [`examples/decimal-sum/main.test.ts`](../examples/decimal-sum/main.test.ts) 这样的测试替身。默认使用平台 `fetch` |
| `http_headers` | 额外的默认请求头 |
| `role` | 默认 ClickHouse 角色（一个或多个） |
| `session_id` | 默认会话 ID |
| `session_max_concurrent_requests` | 此客户端内每个 `session_id` 允许同时在途的最大请求数（默认 `1`） |
| `compression.response` | 默认值为 `false` 时，流式请求会发送 `enable_http_compression = 0`，让行在 ClickHouse 生成后立即到达。非流式请求沿用服务器默认值（ClickHouse 26.3+ 会在客户端接受压缩时压缩响应）。设为 `true` 会压缩所有响应；稀疏流可能要等压缩块填满后才有数据返回 |
| `logger` / `logLevel` | 日志记录器集成 |
| `plugins` | 查询生命周期插件，例如来自 `ck-orm/otel` 的 `otelTracing()` |

这些会话生命周期控制项特意设计为请求级别。把 `session_timeout` 传给单个查询或 `runInSession(...)`。继续使用现有 `session_id` 时才用 `session_check`；初始化全新会话时不要用。
`session_max_concurrent_requests` 不一样：它是客户端级的限制机制，会限制发往同一 `session_id` 的并发请求。
ClickHouse 会话本身仍由服务器加锁，因此把 `session_max_concurrent_requests` 提高到 `1` 以上，可能触发 `SESSION_IS_LOCKED`，并不能获得真正的同会话并行执行。
除非你明确要取消本地串行化，并准备处理服务器端的会话锁错误，否则保留默认值 `1`。

### ClickHouse 设置

`clickhouse_settings` 只放 ClickHouse 的会话/查询设置，也就是 ClickHouse [会话设置](https://clickhouse.com/docs/operations/settings/settings) 文档中列出的、HTTP API 也接受为查询参数的键。它与 ck-orm 客户端配置（例如 `host`、`database`、`request_timeout`、`http_headers` 和 `session_max_concurrent_requests`）是彼此独立的两套配置。

不要把 `query`、`database`、`session_id`、`role` 和 `param_*` 等 HTTP 传输字段放进 `clickhouse_settings`；ck-orm 会单独验证这些键，因为它们属于请求外层或具名参数通道。

官方设置键有 TypeScript 自动补全；对于较新的 ClickHouse 版本或部署专用设置，也可以使用未列出的键：

```ts
import { clickhouseClient, type ClickHouseSettings } from "ck-orm";

const labSettings: ClickHouseSettings = {
  allow_experimental_correlated_subqueries: 1,
  max_threads: 4,
  setting_added_by_future_clickhouse: "enabled",
};

const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  username: "default",
  password: "<password>",
  request_timeout: 30_000,
  clickhouse_settings: labSettings,
});
```

JSON 的解析和序列化由 fetch 传输层负责。公开的客户端配置会让所有运行时保持一致，不会暴露 `json` 覆盖项。

ck-orm 会在每个请求中强制设置一小组传输契约参数：`http_write_exception_in_output_format = 0`、`output_format_json_quote_64bit_integers = 1`、`output_format_json_quote_decimals = 1` 和 `date_time_output_format = "iso"`。如果调用方设置与这些值冲突，ck-orm 会记录一次警告，并使用强制值发送；`max_threads` 等工作负载/性能设置仍由调用方控制。

### 认证

`ck-orm` 的数据库连接使用基本认证。

- 在 `databaseUrl` 模式下，可以把凭据嵌入 URL
- 在结构化模式下，使用 `username` 和 `password`
- 未提供凭据时，默认用户名为 `default`，密码为空

## 查询构建器
<!-- src:fc28245d -->

下面的代码片段都假定 `db`、`probeTelemetry` 和文中用到的辅助函数已从 `ck-orm` 导入。

### `select()`

显式选择会得到结构明确的结果：

```ts
const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
    signalStrength: probeTelemetry.signalStrength,
  })
  .from(probeTelemetry)
  .limit(10);
```

投影对象由对外公开的 `Selection` 值或列组成。实际上，表列、`fn.*(...)` 和 `ck.expr(...)` 的输出，都可以在 `select({ ... })` 里用同一种方式组合。

没有连接时，隐式选择返回完整的表模型：

```ts
const rows = await db.select().from(probeTelemetry).limit(10);
```

有连接时，隐式选择按来源给字段分组，并返回嵌套对象。左连接未匹配上的组会变成 `null`。对于 `Array` / `Map` 列，ClickHouse 返回 `[]` / `{}` 而不是 NULL，所以空容器在这个检查中也算缺失。匹配到的行如果标量字段全为 NULL，且数组和映射为空，就无法和未匹配的组区分开来，也会变成 `null`。

### `from()`、`innerJoin()`、`leftJoin()`

```ts
import { ckAlias, ck } from "ck-orm";

const telemetry = ckAlias(probeTelemetry, "telemetry");
const matchedTelemetry = ckAlias(probeTelemetry, "matched_telemetry");

const rows = await db
  .select({
    probeId: telemetry.probeId,
    telemetrySampleId: telemetry.id,
    matchedTelemetrySampleId: matchedTelemetry.id,
  })
  .from(telemetry)
  .leftJoin(
    matchedTelemetry,
    ck.eq(telemetry.probeId, matchedTelemetry.probeId),
  );
```

### `asofJoin()`、`asofLeftJoin()`

`asofJoin()` 和 `asofLeftJoin()` 会渲染 ClickHouse 的 `ASOF JOIN` 和 `LEFT ASOF JOIN`。对左侧的每一行，它们只连接右侧值最接近的那一行，而不是所有匹配的行。条件用 `ck.and(...)` 把一个或多个相等条件，以及左右两列之间恰好一个 `ck.gt`、`ck.gte`、`ck.lt` 或 `ck.lte` 组合在一起：

```ts
import { ck, ckTable, ckType } from "ck-orm";

const trades = ckTable("trades", {
  tradeId: ckType.uint32("trade_id"),
  symbol: ckType.string(),
  tradeTime: ckType.dateTime64("trade_time", { precision: 3, timezone: "UTC" }),
});

const quotes = ckTable("quotes", {
  symbol: ckType.string(),
  quoteTime: ckType.dateTime64("quote_time", { precision: 3, timezone: "UTC" }),
  price: ckType.decimal({ precision: 20, scale: 5 }),
});

// Each trade gets the latest quote of its symbol at or before the trade time.
const rows = await db
  .select({
    tradeId: trades.tradeId,
    price: quotes.price,
  })
  .from(trades)
  .asofLeftJoin(
    quotes,
    ck.and(
      ck.eq(trades.symbol, quotes.symbol),
      ck.gte(trades.tradeTime, quotes.quoteTime),
    ),
  );
// rows: { tradeId: number; price: string | null }[]
```

- `ck.gte(left, right)` 选取不晚于左侧值的最新右侧行，`ck.gt` 选取严格早于左侧值的最新一行。`ck.lte` 和 `ck.lt` 分别选取不早于、或严格晚于左侧值的最早右侧行。
- `asofLeftJoin()` 遵循 [`leftJoin()` 的 NULL 语义](#join-null-semantics)：它会发送 `join_use_nulls = 1`，并把右侧类型标成可为 NULL，所以没有匹配的左侧行得到 `null`。`asofJoin()` 会丢掉这些行，和 `innerJoin()` 一样。
- 用来找最接近值的列必须是整数、浮点数、`Date`、`DateTime` 或 `Decimal`，比较的两边都必须是列；和值比较不算。额外的相等过滤可以写，例如 `ck.eq(quotes.symbol, "BTCUSD")`。
- ck-orm 不检查这个条件。没有不等比较、出现第二个不等比较，或写了 `ck.or(...)` 时，ClickHouse 会在读取任何数据之前拒绝；错误是 `ClickHouseORMError`，`kind` 为 `"request_failed"`，`clickhouseName` 为 `"INVALID_JOIN_ON_EXPRESSION"`。没有相等条件，以及最接近匹配列是其他类型，同样会被拒绝；缺少相等条件时的错误名在不同 ClickHouse 版本之间不一样。
- ASOF 连接使用默认的 `join_algorithm`，也可以用 `hash`、`parallel_hash` 或 `full_sorting_merge`。`partial_merge`、`grace_hash` 和 `direct` 会以 `NOT_IMPLEMENTED` 失败。

### `where()` 和条件辅助函数

公开的条件辅助函数：

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
- `ck.isNull`
- `ck.isNotNull`
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

`.where(...predicates)` 接受可变参数，并按 `AND` 组合。它会忽略 `undefined` 谓词对象，因此你既可以直接传入多个谓词，也可以先用 `ck.and(...)` 和 `ck.or(...)` 组合出分组谓词。

```ts
import { ck } from "ck-orm";

const query = db
  .select({
    probeId: probeTelemetry.probeId,
    signalStrength: probeTelemetry.signalStrength,
  })
  .from(probeTelemetry)
  .where(
    ck.eq(probeTelemetry.status, 1),
    ck.inArray(probeTelemetry.missionId, [10, 20, 30]),
    ck.between(probeTelemetry.createdAt, 1710000000, 1719999999),
  );
```

值列表会作为一个 `Array(T)` 查询参数发送（`col in {p:Array(T)}`）。列表序列化后超过约 40,000 个字符，就会拆成多个参数，并用 `arrayConcat(...)` 连接。主键和跳数索引的裁剪不受影响。ClickHouse 26.3+ 的每个 HTTP 请求最多接受 1,000 个查询参数，25.8 的分析器会拒绝合并后大小超过约 1 MB 的列表；更大的集合改用临时表或外部数据。包含 SQL 表达式，或混合了无类型值的列表，仍然按元素各用一个参数。

`ck.and(...)` 会跳过 `undefined`，因此可以直接在查询链里按需组装动态过滤条件：

```ts
import { ck } from "ck-orm";

const query = db
  .select({
    id: probeTelemetry.id,
    status: probeTelemetry.status,
  })
  .from(probeTelemetry)
  .where(
    ck.and(
      minId !== undefined ? ck.gt(probeTelemetry.id, minId) : undefined,
      status !== undefined
        ? ck.or(ck.eq(probeTelemetry.status, status), ck.eq(probeTelemetry.status, 9))
        : undefined,
    ),
  );
```

`undefined` 只会在谓词层跳过。谓词值要求严格：`ck.eq(column, undefined)` 会直接抛错，不会静默放宽查询范围。裸 `null`、`false`、`true`、`0` 和 `""` 不是谓词；要表达这些值，请改用 SQL 表达式。`null` 仍是 Nullable 列和写入时的有效数据值，但过滤 NULL 必须显式写出来：

```ts
db.select()
  .from(probeTelemetry)
  .where(ck.isNull(probeTelemetry.deletedAt));
```

布尔列仍然既可以作为普通值使用，也可以直接作为 SQL 表达式：

```ts
const healthChecks = ckTable("health_checks", {
  id: ckType.int32(),
  isPassing: ckType.bool("is_passing"),
});

db.select().from(healthChecks).where(ck.eq(healthChecks.isPassing, false));
db.select().from(healthChecks).where(healthChecks.isPassing);
```

不要把裸 `null` 放进普通比较或 `IN` 列表。需要表达 NULL 逻辑时，显式组合：

```ts
ck.or(
  ck.inArray(probeTelemetry.deletedAt, [new Date("2026-04-21T00:00:00.000Z")]),
  ck.isNull(probeTelemetry.deletedAt),
);
```

运行时要组装较多过滤条件时，优先用 `Predicate[]`，再把它展开传给可变参数的 `.where(...predicates)`：

```ts
import { ck, type Predicate } from "ck-orm";

const predicates: Predicate[] = [];

if (minId !== undefined) {
  predicates.push(ck.gt(probeTelemetry.id, minId));
}

if (status !== undefined) {
  predicates.push(ck.or(ck.eq(probeTelemetry.status, status), ck.eq(probeTelemetry.status, 9)));
}

const query = db
  .select({
    id: probeTelemetry.id,
    status: probeTelemetry.status,
  })
  .from(probeTelemetry)
  .where(...predicates);
```

`Predicate` 是可复用布尔 SQL 子句的对外类型名。同一组谓词对象可以用于 `where`、`having`、连接中的 `on` 子句，以及 `ck.exists(...)` 这类支持布尔值的辅助函数。

构建器产生的可复用计算值统一用对外公开的 `Selection` 表示，例如 `fn.sum(...)`、`fn.toString(...)` 和 `ck.expr(ckSql...)`。用 `.as(...)` 给它们设置别名，用 `.mapWith(...)` 覆盖解码方式。`Order` 是 `ck.asc(...)` 和 `ck.desc(...)` 返回的子句对象。

`ck.has(...)`、`ck.hasAll(...)` 和 `ck.hasAny(...)` 直接对应 ClickHouse 的原生函数，并保留 ClickHouse 的数组、映射和 JSON 语义。左侧是数组列时，普通 JavaScript 值会先按该列编码，所以 `Array(Date)` 过滤条件会先使用该列的 Date 编码，再绑定成查询参数。

`where(...)` 接受可变参数，而 `having(...)` 只接受一个谓词。`having(...)` 需要多个子句时，先用 `ck.and(...)` 或 `ck.or(...)` 组合谓词。

`ck.contains(...)`、`ck.startsWith(...)`、`ck.endsWith(...)` 以及对应的 `*IgnoreCase` 变体都会把输入当作字面文本。它们会把值作为参数传入，并在内部转义 LIKE 通配符（`%`、`_`、`\`）。

只有确实需要完整的模式匹配语义时，才使用 `ck.like(...)` / `ck.ilike(...)`。这些 API 仍会把值作为参数传入，保证 SQL 安全；但因为 LIKE 是模式语言，`%` 和 `_` 仍保留通配符含义。

字面文本搜索示例：

```ts
import { ck } from "ck-orm";

const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
  })
  .from(probeTelemetry)
  .where(ck.contains(probeTelemetry.probeId, "probe_alpha%"));
```

高级模式匹配示例：

```ts
import { ck } from "ck-orm";

const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
  })
  .from(probeTelemetry)
  .where(ck.like(probeTelemetry.probeId, "probe_%"));
```

### `groupBy()`、`having()`、`orderBy()`、`limit()`、`offset()`

```ts
import { ck, fn } from "ck-orm";

const totalSignalStrength = fn.sum(probeTelemetry.signalStrength).as(
  "total_signal_strength",
);

const query = db
  .select({
    probeId: probeTelemetry.probeId,
    totalSignalStrength,
  })
  .from(probeTelemetry)
  .groupBy(probeTelemetry.probeId)
  .having(ck.gt(fn.sum(probeTelemetry.signalStrength), "100.00000"))
  .orderBy(ck.desc(probeTelemetry.createdAt))
  .limit(20)
  .offset(0);
```

`groupBy()` 和 `limitBy([...])` 接受列和由 `fn.*(...)`、`ck.expr(...)` 等辅助函数生成的计算 `Selection` 值。

`limit()`、`offset()` 和 `limitBy(..., limit)` 直接传入的值必须是非负安全整数。只有确实需要 ClickHouse 常量表达式时，才使用 `ckSql`。

`orderBy()` 接受：

- `ck.desc(selection)`
- `ck.asc(selection)`
- 直接传入列

### `final()`

给表查询加上表级 `FINAL`：

```ts
const query = db.select().from(probeTelemetry).final();
```

读取没有别名的简单表时，`ck-orm` 会输出 `FROM table FINAL`。如果根表有别名，或者查询还连接了其他来源，`ck-orm` 会把应用了 FINAL 的表包进子查询，并在外层保留这个别名。这样可以避开 ClickHouse 分析器处理 `FINAL`、表别名、连接和 lambda 表达式时的边界情况，同时不改变构建器 API。

`final()` 只对表查询的根表生效。如果需要在 CTE、子查询或表函数流程内部使用 `FINAL`，在调用 `.as(...)` 前先对基于表的查询调用 `.final()`。

### `limitBy()`

使用 ClickHouse 的 `LIMIT ... BY ...`：

```ts
import { ck } from "ck-orm";

const query = db
  .select({
    probeId: probeTelemetry.probeId,
    createdAt: probeTelemetry.createdAt,
  })
  .from(probeTelemetry)
  .orderBy(ck.desc(probeTelemetry.createdAt))
  .limitBy([probeTelemetry.probeId], 1);
```

### 执行方式

构建器查询有三种执行方式：

```ts
const query = db.select().from(probeTelemetry).limit(10);

const rows = await query;
const sameRows = await query.execute();

for await (const row of query.iterator()) {
  console.log(row);
}
```

在应用示例里，如果想让执行位置一眼可见，就使用 `.execute()`。直接写 `await query` 也可以，符合 Drizzle 风格；等团队熟悉把构建器查询当作 thenable 使用后，这样写起来更顺手。

`query.iterator()` 使用和 `db.stream()` 一样的会话相关并发规则：如果查询针对某个 `session_id`，这个槽位会一直被占用，直到迭代完成或迭代器提前关闭。

### 用 `toSQL()` 查看 SQL

`toSQL()` 只编译构建器，不会发送任何请求。请直接在构建器上调用它，因为对构建器 await 会执行查询。`select()` 构建器、`insert().values()` 和 `insert().fromSelect()` 都支持它。

```ts
const query = db
  .select({ id: probeTelemetry.id, status: probeTelemetry.status })
  .from(probeTelemetry)
  .where(
    ck.and(
      ck.eq(probeTelemetry.probeId, "probe_alpha"),
      ck.gte(probeTelemetry.createdAt, 1710000000),
    ),
  )
  .limit(10);

const { sql, params } = query.toSQL();
// params: { orm_param1: "probe_alpha", orm_param2: 1710000000 }
```

返回结果包含这些字段：

- `sql` 是单行语句，每个值的位置都是 `{orm_param1:String}` 这样的占位符。其中不含 `FORMAT` 子句，这个子句在发送查询时才会加上。
- `params` 保存占位符对应的原始 JavaScript 值。查询执行时，这些值会单独编码，与 SQL 分开发送，所以绝不会内联到 `sql` 里。
- `forcedSettings` 列出构建器强制加到请求上的设置，例如 `leftJoin()` 的 `join_use_nulls: 1`。没有这类设置时，不会出现这个字段。
- `body` 只有 `insert().values()` 才有：它是 Values 文本形式的行数据，而 `sql` 里是 `INSERT INTO t (cols) FORMAT Values` 语句。其他情况下没有这个字段。

返回的是副本，修改它不会影响构建器。

传入 `{ pretty: true }` 可以把 `sql` 和 `body` 排成多行，方便写日志和调试：

```ts
console.log(query.toSQL({ pretty: true }).sql);
```

```sql
select
  `probe_telemetry`.`id` as `id`,
  `probe_telemetry`.`status` as `status`
from `probe_telemetry`
where (
  `probe_telemetry`.`probe_id` = {orm_param1:String}
  and `probe_telemetry`.`created_at` >= {orm_param2:Int32}
)
limit 10
```

美化输出只改变空白。每个子句另起一行，`select` 的每一项占一行，子查询和 CTE 会缩进，`and` 或 `or` 组里的条件也是一个条件一行。函数参数仍在原来那一行，引号内的文本不会被改动，含有 SQL 注释的语句则原样返回。`insert().values()` 的请求体里，每行数据单独占一行。美化输出不会影响实际发送的查询。

### 子查询和 CTE

用 `.as("alias")` 给构建器指定名称，生成子查询：

```ts
const latestTelemetrySample = db
  .select({
    probeId: probeTelemetry.probeId,
    createdAt: probeTelemetry.createdAt,
  })
  .from(probeTelemetry)
  .orderBy(ck.desc(probeTelemetry.createdAt))
  .limit(10)
  .as("latest_telemetry_sample");
```

在子查询里，`.as("…")` 是**可选**的。不调用时，框架会在编译时自动分配别名（`__sub_1`、`__sub_2`、……）。
裸构建器可以直接传给 `from()`、`innerJoin()`、`leftJoin()`、
`inArray()`、`notInArray()`、`exists()` 和 `notExists()`。

```ts
// Bare builder, variable-bound — column refs are accessible.
const signalTotals = db
  .select({
    probeId: probeTelemetry.probeId,
    totalSignalStrength: fn.sum(probeTelemetry.signalStrength).as("total"),
  })
  .from(probeTelemetry)
  .groupBy(probeTelemetry.probeId);

await db
  .select({
    probeId: signalTotals.probeId,
    totalSignalStrength: signalTotals.totalSignalStrength,
  })
  .from(signalTotals);

// Bare builder inline — use the callback form of innerJoin/leftJoin so the
// `on` condition can reference its columns via the `joined` parameter.
await db
  .select({ probeId: probeTelemetry.probeId })
  .from(probeTelemetry)
  .innerJoin(
    db
      .select({
        peerProbeId: probeTelemetry.probeId,
        peerCount: fn.count().as("peer_count"),
      })
      .from(probeTelemetry)
      .groupBy(probeTelemetry.probeId),
    (joined) => ck.eq(probeTelemetry.probeId, joined.peerProbeId),
  );

// inArray accepts a bare builder directly.
await db
  .select({ probeId: probeTelemetry.probeId })
  .from(probeTelemetry)
  .where(
    ck.inArray(
      probeTelemetry.probeId,
      db.select({ activeProbeId: probeTelemetry.probeId }).from(probeTelemetry),
    ),
  );
```

#### 风格指南：选 `.as(name)` 还是裸构建器？

| 场景 | 推荐写法 |
|---|---|
| 一次性内联子查询，不需要复用 | 裸构建器 + `innerJoin/leftJoin` 回调，或在 `inArray/exists` 中直接使用裸构建器 |
| 子查询的列引用在多个位置使用（SELECT / WHERE / 一个 JOIN） | 绑定到 `const` 后直接传入裸构建器——列引用会随链式调用传递 |
| 自连接（同一个逻辑子查询，两个 SQL 别名） | 显式调用 `.as("a")` 和 `.as("b")`——同一个实例作为来源使用两次时，查询编译成 SQL 会抛出错误 |
| 需要稳定、可读或带类型的别名（生产环境中的 `query_log` 查询、`EXPLAIN`、慢日志、跨模块辅助函数、字面量 `TAlias` 来源键） | 调用 `.as("descriptive_name")` |

对结构不同的查询，自动别名在多次编译之间**不稳定**；但在同一次编译中，以及对同一构建器树重复编译时，它都保持稳定——`snapshot` 风格的测试不需要归一化。

#### 选择键冲突

与 SelectBuilder 方法名冲突的选择键（`from`、
`where`、`select`、`as`、`innerJoin`、`leftJoin`、`asofJoin`、`asofLeftJoin`、`groupBy`、`having`、
`orderBy`、`limit`、`offset`、`final`、`limitBy`、`execute`、`iterator`、
`then`、`catch`、`finally`、`buildSelectionItems`）在裸构建器上仍然保留方法类型。
`sub.from` 始终返回构建器方法，绝不会返回列引用——即使 `from` 出现在选择中也是如此。
把这些键重命名（例如 `from` → `fromAddress`），或者用 `.as("name")` 包装子查询，
让列访问走专用的 `Subquery` 对象。

用 `$with()` 和 `with()` 管理 CTE：

```ts
import { ck, fn } from "ck-orm";

const rankedProbes = db.$with("ranked_probes").as(
  db
    .select({
      probeId: probeTelemetry.probeId,
      totalSignalStrength: fn.sum(probeTelemetry.signalStrength).as(
        "total_signal_strength",
      ),
    })
    .from(probeTelemetry)
    .groupBy(probeTelemetry.probeId),
);

const rows = await db
  .with(rankedProbes)
  .select({
    probeId: rankedProbes.probeId,
    totalSignalStrength: rankedProbes.totalSignalStrength,
  })
  .from(rankedProbes);
```

不传名称的 `$with()` 会创建匿名 CTE。匿名 CTE 在每个查询里只能作为来源引用一次；如果要把同一个结果连接两次，就定义两个 CTE，每个连接各用一个（调用两次 `db.$with().as(...)`，或者定义两个不同名称的具名 CTE）。

### `db.count()`

使用 `db.count(source, ...predicates)` 这个 Drizzle 风格的计数辅助函数。它和 `.where(...predicates)` 使用相同的谓词规则：多个谓词会用 `AND` 组合，`undefined` 谓词对象会被跳过。传给 `ck.eq(...)` 等辅助函数的值为 `undefined` 时，仍会抛出错误。

```ts
import { ck } from "ck-orm";

const total = await db.count(
  probeTelemetry,
  ck.eq(probeTelemetry.status, 1),
  ck.gt(probeTelemetry.id, 1000),
);
```

结果集较复杂时，可以统计子查询或 CTE：

```ts
const activeProbes = db
  .select({
    probeId: probeTelemetry.probeId,
  })
  .from(probeTelemetry)
  .where(ck.eq(probeTelemetry.status, 1))
  .as("active_probes");

const total = await db.count(activeProbes);
```

`db.count(...)` 默认使用数值计数模式：会生成 `toFloat64(count())`，并解码为 `number`。超大的计数可能超出 JavaScript 整数精度；如果需要精确结果，或要保留驱动返回的类型，请使用可链式调用的模式：

```ts
const approximateTotal = await db.count(activeProbes); // number
const exactTotal = await db.count(activeProbes).toSafe(); // string
const wireTotal = await db.count(activeProbes).toMixed(); // number | string
```

`.toSafe()` 会生成 `toString(count())`，用于精确读取。如果把安全计数当作 SQL 表达式使用，它按 `String` 语义处理；需要做数值 SQL 比较时，使用默认模式、`.toUnsafe()` 或 `.toMixed()`。`.toMixed()` 会生成 `toUInt64(count())`，并保留驱动返回的类型；在 ck-orm 默认启用无损 64 位 JSON 的设置下，ClickHouse 实际返回的结果通常是 `string`。

## 连接的 NULL 语义
<!-- src:59e7a4e2 -->

`leftJoin()` 和 `asofLeftJoin()` 默认使用 SQL 风格的 NULL 语义，并自动启用 `join_use_nulls = 1`。

也就是说：

- 默认左连接的右侧类型会推断为可为 NULL
- 如果显式关闭 `join_use_nulls`，推断出的类型也会改变

要与 ClickHouse 的默认连接行为保持一致，传入字面量 `0`。这个字面量会参与类型推断，所以不需要 `as const`。比如从变量取值时，只要它的类型是普通的 `number`，结果类型仍会带上 `| null`。

```ts
const rawDefaultDb = db.withSettings({
  join_use_nulls: 0,
});
```

对裸构建器（没有调用 `.as()` 的子查询）或匿名 CTE 做左连接时，这些来源中的每个列引用都会推断为 `| null`，包括 `FROM` 一侧和内连接裸构建器中的引用。这些引用共用同一个 `string` 类型的来源键，因此类型无法区分被左连接的是哪个来源。运行时仍只有左连接进来的那一侧会返回 NULL。

```ts
const rows = await db
  .select({ uid: allUsers.uid, spent: totals.spent })
  .from(allUsers)
  .leftJoin(totals, ck.eq(allUsers.uid, totals.uid));
// uid and spent are both number | null
```

给 `FROM` 一侧或内连接的子查询加上 `.as("name")`。这样，这些引用会保留各自的来源键，类型也保持为非 NULL；被左连接的一侧仍然是 `| null`。

当连接查询被复用为子查询、CTE、`ck.exists(...)` 或 `ck.inArray(...)` 的来源时，强制启用的 `join_use_nulls = 1` 设置会继续生效，因此构建器类型始终与运行时行为一致。

计算表达式在运行时遵循 ClickHouse 的 NULL 传播规则：ClickHouse 返回 NULL 时（输入是 NULL，或左连接的行没有匹配），值就是 `null`。这些表达式的 TypeScript 类型就是你声明的 `TData`（或辅助函数的固定类型），不会自动放宽为可空类型；因此输入可能为 NULL 时，需要声明 `| null`（例如 `fn.plus<number | null>(...)`），或用 `fn.coalesce(...)` 包装输入。

## 写入
<!-- src:5d78750d -->

### `insert(table).values(...)`

想让插入数据遵循表模式、有类型检查时，使用构建器：

```ts
await db.insert(probeTelemetry).values({
  id: 1,
  probeId: "probe_alpha",
  missionId: 10,
  sampleId: "900001",
  signalStrength: "42.50000",
  status: 1,
  createdAt: 1710000000,
  deletedAt: null,
  ingestedAt: new Date("2026-04-21T00:00:00.000Z"),
  isDeleted: 0,
  ingestVersion: "1",
});
```

这些行会作为一个 `INSERT INTO t (cols) FORMAT Values` 请求发送。语句不带查询参数；它们会作为请求体中的 Values 字面文本发送，因此 1,000 个参数和每个值 128 KiB 的 HTTP 限制不适用。省略的列只会在该行使用自己的 `DEFAULT`。插入时会关闭表达式求值（`input_format_values_interpret_expressions = 0` 和 `input_format_values_deduce_templates_of_expressions = 0`），所以每个单元格都只是数据。需要计算值时，使用 `insert().fromSelect()`。JavaScript `Date` 会按它表示的准确时刻发送，并按列精度截断；`DateTime` 列只保留整秒。`DateTime` 或 `DateTime64` 列中的数字会按 Unix 秒解析。字符串会按列的时区解析。

插入行必须使用表模式中的键。未知键会尽早被拒绝。

### `insert(table).fromSelect(select)`

用 `.fromSelect(...)` 编译出一条 `INSERT INTO target (cols) SELECT ...` 语句，让 ClickHouse 在服务端物化 SELECT 结果。行不会经过 Node 进程，适合对大型源表只筛选一次，再在 `runInSession` 中的多个下游查询里复用这个物化后的数据子集。

```ts
await db.runInSession(async (session) => {
  const tmpScopedDeals = ckTable("tmp_scoped_deals", {
    dealTicket: ckType.int64("deal_ticket"),
    login: ckType.int64(),
    volume: ckType.float64(),
  });

  await session.createTemporaryTable(tmpScopedDeals);

  await session.insert(tmpScopedDeals).fromSelect(
    session
      .select({
        dealTicket: mtDeal.dealTicket,
        login: mtDeal.login,
        volume: mtDeal.volume,
      })
      .from(mtDeal)
      .where(ck.inArray(mtDeal.entry, [1, 2, 3])),
  );

  // Subsequent count / aggregation queries scan the temp table, not mt_deal:
  const totalRows = await session.count(tmpScopedDeals);
  const byLogin = await session
    .select({ login: tmpScopedDeals.login, volume: fn.sum(tmpScopedDeals.volume) })
    .from(tmpScopedDeals)
    .groupBy(tmpScopedDeals.login);
});
```

规则：

- SELECT 必须投影目标表的所有必需列；用 `.default(...)` 声明的列可以省略。缺少必需列会在编译时（TypeScript）报错，运行时也会再次被拒绝。
- INSERT 列列表按 SELECT 投影键的顺序生成，因此即使 ClickHouse 按位置对齐列，对调用方来说仍是 `name-aligned` 语义。投影键的顺序不会改变哪个目标列接收哪个值，实际匹配的是键名。
- 未知投影键（不在目标表上的键）和逐列类型不匹配，都会在查询发送前触发 TypeScript 错误。
- 同一条插入链上，`.values()` 和 `.fromSelect()` 互斥；构建器类型会收窄，让另一个方法从自动补全中消失，运行时也会拒绝这次调用。
- 编译后的传输请求是普通的 ClickHouse 命令；查询事件中包含 `mode: "insert"`，而 `tableName` 设置为目标表。
- `.fromSelect()` 接受 `SelectBuilder`（`session.select({...}).from(...)`），不接受子查询包装器。只有需要在另一个 SELECT 中把结果用作 CTE/子查询时，才使用 `.as("name")`；直接传给 `.fromSelect()` 时不要用它。

#### 嵌套列

`ckType.nested(...)` 列与 `.fromSelect()` 有 3 种用法：

1. **从投影中省略嵌套键**。这是默认模式，符合 ck-orm 的运行时契约：缺失的嵌套值会编码为 SQL `DEFAULT`，ClickHouse 会为每个字段填充空的并行数组。TS 层在 `$inferInsert` 中将嵌套列视为 `optional`，与此保持一致。
2. **直接投影嵌套列引用**，例如 `select({ events: src.events })`。ck-orm 会把内部 SELECT 包装进子查询，并将外层投影改写为按字段逐一访问的点路径（`__ck_inner.events.name`、`__ck_inner.events.score`，……）。来源的嵌套结构必须包含目标嵌套列要求的每个字段。
3. **把计算表达式投影到嵌套列**会被编译器直接拒绝——单个 SQL 表达式无法展开为平行数组字段。要写入行值，请使用 `.values(...)` 或 `.insertJsonEachRow(...)`。

如果希望 TS 层强制要求始终提供嵌套数据（`Pattern D`，即不允许空嵌套数组的业务契约），请启用 `ckType.nested({...}).requiredOnInsert()`。这个链式调用返回一个列，其 `$inferInsert` 结构会把 `events` 列为必需；`.fromSelect()` 会要求使用嵌套列引用投影模式，或改用按行写入的方式。

> **JSON 传输格式备注**：`.values()` 会把 JSON 列作为带引号的 `JSON.stringify(...)` 文本写入 Values 请求体。ClickHouse 会把这个字面值解析为 JSON 列值。`.fromSelect()` 完全在服务端运行，因此 JSON 值不会经过 JS 进程。两条路径最终存储的数据相同。

### `insertJsonEachRow()`

如果已经有对象行或异步行流，就使用 `insertJsonEachRow()`：

```ts
await db.insertJsonEachRow("tmp_scope", [
  { probe_id: "probe_alpha" },
  { probe_id: "probe_beta" },
]);
```

它接受：

- 字符串表名
- `ckTable()` 创建的表对象
- 普通数组
- 一个 `AsyncIterable`

空的普通数组会在客户端被视为无操作，但插件钩子仍会收到一次成功的插入生命周期报告，其中包含 `rowCount: 0`。未知字段由 ClickHouse 处理；如果想让服务器忽略额外的 JSON 字段，就传入 `input_format_skip_unknown_fields: 1` 之类的设置。嵌套列会以扁平化后的 `column.field` 数组发送（ClickHouse 默认 `flatten_nested = 1`），所以 `{ lines: [{ sku: "a" }] }` 在 `insert().values()` 和 `insertJsonEachRow()` 中都能正常工作。

如果提供了表模式，`insertJsonEachRow()` 会把 `DateTime`/`DateTime64` 列中的 JavaScript `Date` 发送为与 `insert().values()` 完全相同的整数刻度值，并让 ClickHouse 默认的 `date_time_input_format = basic` 解析器继续处理字符串，因此两者解析日期时间字符串的方式一致。你自行在 `DateTime64` 列传入的 JSON 数字，仍会被 `JSONEachRow` 按刻度读取，被 `.values()` 按 Unix 秒读取。

使用裸表名的 `insertJsonEachRow("table_name", rows)` 没有列类型，因此 `Date` 会以带 `Z` 的 ISO 8601 发送，ck-orm 会为该请求设置 `date_time_input_format = best_effort`。对于使用模式表的行，如果某个键不在模式中，处理方式也一样：ck-orm 会原样传递它，因此其中的 `Date` 需要在调用中设置 `clickhouse_settings: { date_time_input_format: "best_effort" }`。在 ClickHouse 25.8 上，`best_effort` 解析器会误读 1970 年之前且带有亚秒位数的时刻，并拒绝 `12:34:56` 这类 `Time` 字符串；由模式指定类型的列可以避开这两个问题。

## 原始 SQL
<!-- src:f47b381e -->

`ck-orm` 自带 SQL 模板 API。如果直接写 SQL 比用构建器更顺手，就用它。

### `` ckSql`...` ``

```ts
import { ckSql, fn } from "ck-orm";

const rows = await db.execute(ckSql`
  select
    ${probeTelemetry.probeId},
    ${fn.sum(probeTelemetry.signalStrength)} as total_signal_strength
  from ${probeTelemetry}
  where ${probeTelemetry.id} > ${10}
  group by ${probeTelemetry.probeId}
`);
```

### `ckSql.join()` 和 `ckSql.identifier()`

```ts
import { ckSql } from "ck-orm";

const fields = ckSql.join(
  [ckSql.identifier("probe_id"), ckSql.identifier("signal_strength")],
  ", ",
);

const rows = await db.execute(
  ckSql`select ${fields} from ${ckSql.identifier("probe_telemetry")}`,
);
```

### 带 `query_params` 的原始 SQL

```ts
import { ckSql } from "ck-orm";

const rows = await db.execute(
  ckSql`select probe_id, signal_strength from probe_telemetry where probe_id = {probe_id:String} limit {limit:Int64}`,
  {
    query_params: {
      probe_id: "probe_alpha",
      limit: 10,
    },
  },
);
```

系统会自动选择参数的传输方式。你不需要为 `query_params` 配置 multipart 处理。

以 `orm_param` 开头的 `query_params` 键会被拒绝。这个前缀专供 `` ckSql`...` `` 内部生成的参数使用。

值格式化器支持基本类型、`Date`、`NaN`、`Infinity`、数组、对象和 `Map` 值，可用于 `{ids:Array(UInt64)}` 或 `{attrs:Map(String, String)}` 这类 ClickHouse 的带类型占位符。通过 `query_params` 传入的值本身不带列类型，因此数组始终按 Array 文本发送。这里不支持 `Tuple(...)` 占位符。数组中的 64 位整数必须是 `bigint`；字符串会加引号；元素类型为数值类型时，字符串会被拒绝。`Date` 会以 UTC 日期时间文本发送，ClickHouse 会按占位符类型的时区读取它；如果不想让会话或服务器时区改变这个时间，请声明为 `{at:DateTime64(3, 'UTC')}`；`` ckSql`...${date}` `` 插值会替你完成这一步。如果参数表示表名或列名，请使用 ClickHouse 的 `Identifier` 占位符类型：

```ts
const rows = await db.execute(
  ckSql`
    select {selected_column:Identifier}
    from {target_table:Identifier}
    where id = {id:Int32}
  `,
  {
    query_params: {
      selected_column: "probe_id",
      target_table: "probe_telemetry",
      id: 1,
    },
  },
);
```

### `ck.expr()`

用 `ck.expr()` 把 SQL 片段封装成可复用的 `Selection`：

```ts
import { ck, ckSql } from "ck-orm";

const query = db.select({
  constantOne: ck.expr(ckSql`1`).as("constant_one"),
});
```

片段带类型时，`ck.expr()` 会保留该类型。无类型的片段会有意保持为 `unknown`；如果你在模式之外已经知道结果类型，请传入泛型或解码器：

```ts
const status = ck.expr(ckSql<string>`'closed'`); // Selection<string>
const enabled = ck.expr<boolean>(ckSql`1`, {
  decoder: (value) => Number(value) === 1,
});
```

### 原始查询格式

原始的非流式查询只支持 `JSON` 输出：

```ts
const rows = await db.execute(ckSql`select 1`, {
  format: "JSON",
});
```

原始流式查询只支持 `JSONEachRow` 输出。在默认的 `compression.response: false` 配置下，流会以未压缩形式发送，所以 ClickHouse 生成一行，客户端就能收到一行：

```ts
for await (const row of db.stream(ckSql`select 1`, {
  format: "JSONEachRow",
})) {
  console.log(row);
}
```

### 表达式中的 Decimal 精度

```ts
import { ckSql, ckTable, ckType, fn } from "ck-orm";

const measurementLog = ckTable("measurement_log", {
  value: ckType.decimal({ precision: 18, scale: 5 }),
});

// fn.sum / sumIf / min / max auto-cast to Decimal(P, S) and decode as string.
db.select({ total: fn.sum(measurementLog.value) }).from(measurementLog);
// → CAST(sum(`measurement_log`.`value`) AS Decimal(38, 5))   row.total: string

// Explicit casts.
fn.toDecimal128(measurementLog.value, 5); // toDecimal32 / 64 / 128 / 256
ckSql.decimal(ckSql`sum(a) - sum(b)`, 20, 5);
measurementLog.value.cast(20, 2); // column shortcut
```

- `sum` / `sumIf` 会把 P 扩大到 ≥ 38；`min` / `max` 保留列的 P。对于模式定义的 Decimal 列，`fn.sum()` 和 `fn.sumIf()` 是 `Selection<string>`；Float 列是 `Selection<number>`。自动类型转换同样会作用于 `nullable(decimal(...))` 和 `lowCardinality(decimal(...))`。
- `maxIf(value, condition)` 遵循与 `max` 相同的 Decimal 规则：Decimal 结果是 `Selection<string>`，并保留声明的 `Decimal(P, S)` 结构。
- `avg` **不会自动转换**；ClickHouse 会在 Float64 上计算 `avg(Decimal)`，所以 `fn.avg` 返回 `Selection<number>`。需要精确的 Decimal 平均值时，请使用 `ckSql.decimal(ckSql\`sum(x) / count(x)\`, P, S)`。
- `fn.divideDecimal(left, right, resultScale?)` 直接对应 ClickHouse 的 `divideDecimal`。它返回 `Selection<string>`，因此不会丢失精度。提供 `resultScale` 时，它必须是 `0` 到 `76` 的字面整数，并以内联形式输出；构建器不会检查版本，也不会模拟 ClickHouse 函数。
- `column.cast(P, S)` 转换的是列，不是聚合结果；在 `GROUP BY` 中直接把它用作裸表达式会抛出 `NOT_AN_AGGREGATE`。请使用 `fn.sum(column)`，或把聚合结果包装起来。
- 插入会拒绝既不是字符串也不是数字的对象（例如原始 `decimal.js` 实例），请传入 `.toFixed(scale)`：

```ts
db.insert(measurementLog).values({ value: new Decimal("1.23").toFixed(5) }); // ✅
db.insert(measurementLog).values({ value: new Decimal("1.23") as never }); // ❌ throws
```

## 窗口表达式
<!-- src:6d0b3f76 -->

用 `fn.over(...)` 包装 `fn.*` 函数表达式。可选的窗口规范中，`partitionBy` 只能传入查询构建器的选择项，`orderBy` 只能传入普通的 `ck.asc(...)` / `ck.desc(...)` 值：

```ts
import { ck, ckTable, ckType, fn } from "ck-orm";

const events = ckTable("events", {
  id: ckType.int32(),
  accountId: ckType.int32("account_id"),
  amount: ckType.decimal({ precision: 18, scale: 2 }),
});

const rows = await db
  .select({
    id: events.id,
    rankInAccount: fn
      .over(fn.rowNumber(), {
        partitionBy: [events.accountId],
        orderBy: [ck.asc(events.id)],
      })
      .as("rank_in_account"),
    exactRankInAccount: fn
      .over(fn.rowNumber().toSafe(), {
        partitionBy: [events.accountId],
        orderBy: [ck.asc(events.id)],
      })
      .as("exact_rank_in_account"),
    accountTotal: fn
      .over(fn.sum(events.amount), { partitionBy: [events.accountId] })
      .as("account_total"),
  })
  .from(events);
```

`fn.rowNumber()` 默认通过 `toFloat64` 得到不安全的 `Selection<number>`。需要精确的 `Selection<string>` 时，在 `fn.over(...)` 前调用 `.toSafe()`；需要原生 `UInt64` 传输结构（`Selection<number | string>`）时调用 `.toMixed()`；`.toUnsafe()` 明确对应默认行为。构建器有意不添加 JavaScript 的安全整数检查：ClickHouse 的 `toFloat64` 转换可能把超过 `Number.MAX_SAFE_INTEGER` 的值四舍五入；`.toSafe()` 保留精确的十进制字符串，`.toMixed()` 保留驱动的原生结果结构。Decimal 聚合会保留常规的字符串解码器，并编译成 `CAST(sum(...) OVER (...) AS Decimal(...))`，因此 `OVER` 会留在聚合内部，不会移到转换外面。`fn.over` 有意不接受裸列或原始 SQL 片段；目前这个接口面只支持 `PARTITION BY` 和 `ORDER BY`，窗口帧、命名窗口或其他 ClickHouse 专用子句请使用 `ckSql`。

## 函数和表函数
<!-- src:1e16409b -->

### `fn`

通用、转换、聚合、JSON、元组和表相关辅助函数包括：

- `fn.call()`
- `fn.withParams()`
- `fn.replaceRegexpAll()`：带类型的 ClickHouse RE2 替换；见[正则替换](#regex-replacement)
- 类型转换辅助函数与 ClickHouse 的类型转换页面一致：`fn.cast()`、`fn.date()`、`fn.accurateCast*()`、`fn.reinterpret*()`、`fn.parseDateTime*()`、`fn.formatDateTime()`、`fn.formatRow*()`、`fn.toString*()`、`fn.toBool()`、`fn.toInt*()`、`fn.toUInt*()`、`fn.toFloat*()`、`fn.toBFloat16*()`、`fn.toDecimal*()`、`fn.toDecimalString()`、`fn.toFixedString()`、`fn.toDate*()`、`fn.toDateTime*()`、`fn.toTime*()`、`fn.toTime64*()`、`fn.toInterval*()`、`fn.toLowCardinality()`、`fn.toNullable()` 和 `fn.toUUID*()`。
- 64 位及更宽的整数转换（`toInt64/128/256*`、`toUInt64/128/256*`、Unix 时间戳的 64 位辅助函数）解码为 `string`；更窄的整数和浮点转换解码为 `number`。
- 只能传入字面量的 ClickHouse 参数会在需要时验证并内联：Decimal 小数位数、DateTime64/Time64 精度、FixedString 长度、目标类型字符串和间隔单位。
- `fn.toStartOfMonth()`
- `fn.toUnixTimestamp()` / `fn.toUnixTimestamp64Second()` / `fn.toUnixTimestamp64Milli()` / `fn.toUnixTimestamp64Micro()` / `fn.toUnixTimestamp64Nano()`
- `fn.fromUnixTimestamp()` / `fn.fromUnixTimestamp64Second()` / `fn.fromUnixTimestamp64Milli()` / `fn.fromUnixTimestamp64Micro()` / `fn.fromUnixTimestamp64Nano()`
- `fn.count()` / `fn.countIf()`：默认是 `Selection<number>`，包装为 `toFloat64(count(...))`。链式调用 `.toSafe()` 得到 `Selection<string>`（`toString(count(...))`），`.toMixed()` 得到 `Selection<number | string>`（`toUInt64(count(...))`），`.toUnsafe()` 恢复默认值。行为与 `db.count` 一致。解码器强制要求非负整数，会拒绝 `NaN`、负数、布尔值等；示例见[`fn.count` / `fn.uniqExact` 的模式](#fncount--fnuniqexact-modes)。
- `fn.sum()` / `fn.sumIf()` / `fn.min()` / `fn.max()` / `fn.maxIf()`：Decimal 列会自动转换为 `Decimal(P, S)`；见[表达式中的 Decimal 精度](#decimal-precision-in-expressions)。
- `fn.over()` / `fn.rowNumber()`：把窗口函数与 `PARTITION BY`、`ORDER BY` 组合起来；见[窗口表达式](#window-expressions)。
- `fn.divideDecimal()`：精确执行 Decimal 除法；如果指定了结果小数位数，就以内联形式输出；见[表达式中的 Decimal 精度](#decimal-precision-in-expressions)。
- `fn.argMax()` / `fn.argMin()`：返回 `val` 最大或最小的那一行中的 `arg` 值。返回类型与 `arg` 列一致（不额外套 Decimal CAST；ClickHouse 会保留该列声明的类型）。
- `fn.avg()`：`Selection<number>`（Float64），与 ClickHouse 原生 `avg(Decimal)` 行为一致
- `fn.uniqExact()`：和 `fn.count()` 一样支持三种链式调用模式：默认是 `Selection<number>`，包装为 `toFloat64(uniqExact(...))`；`.toSafe()` 得到 `Selection<string>`（`toString(uniqExact(...))`），`.toMixed()` 得到 `Selection<number | string>`（`toUInt64(uniqExact(...))`），`.toUnsafe()` 恢复默认模式。解码器同样只接受非负整数。见[`fn.count` / `fn.uniqExact` 的模式](#fncount--fnuniqexact-modes)。
- `fn.coalesce()`
- `fn.if()` / `fn.multiIf()`：条件函数。两者都返回 `Selection<unknown>`，因为 ClickHouse 会根据所有值分支确定服务端结果类型。
- `fn.greatest()` / `fn.least()` / `fn.nullIf()`：比较函数；`nullIf` 会自动把 sqlType 包装为 `Nullable(...)`，但不会对已经可为 NULL 的输入二次包装。
- `fn.positionCaseInsensitive()` / `fn.positionCaseInsensitiveUTF8()`：大小写不敏感 / UTF-8 子串搜索。返回 `Selection<string>`（ClickHouse `UInt64`，解码为字符串以保留 64 位精度，与 `fn.indexOf()` 一致）。可选的 `startPos` 必须是 `UInt*` 类型：传入 UInt 列或 `fn.toUInt64(n)` 处理字面量；裸数字字面量会变成 `Int64`，ClickHouse 会拒绝。
- `fn.jsonExtract()`
- `fn.tuple()`
- `fn.arrayJoin()`
- `fn.tupleElement()`
- `fn.not()`

### 条件分支类型

条件结果的泛型已经移除：`fn.if<TData>(...)` 和 `fn.multiIf<TData>(...)` 现在无法编译。如果上下文中有已知的 `Float64` 或 `UInt64` 锚点，兼容的安全整数字面量分支会根据这个锚点确定类型；`UInt64` 字面量还必须非负。类型不同的表达式和原始 SQL 永远不会自动转换，所以每个值分支都要显式转换。`.mapWith(...)` 只定义 JavaScript 解码器，不改变 SQL 类型。例如，和 `fn.over(fn.rowNumber().toMixed())` 搭配的非负安全整数回退分支会按 `UInt64` 编译。`fn.if(...)` 和 `fn.multiIf(...)` 是标量，不能直接传给 `fn.over(...)`；值分支仍可以是窗口选择值。

无法证明所有值分支共享同一个 SQL 类型时，结果元数据会保持未知，解码改为直接透传。因此已有结果可能从 `number` 变成传输层的 `string`。迁移时，把每个值分支都显式转换为同一个 SQL 类型，再按需调用 `.mapWith(...)`。自动结果元数据有意只覆盖精确的 `Bool`、`Int64`、`UInt64`、`Float64` 和 `String` 锚点。其他类型即使分支元数据相同也会直接透传，因为 ClickHouse 可能归一化相同的存储类型（例如 `LowCardinality(String)` 会变成 `String`）。

```ts
// Before: the generic asserted a JS type but did not change SQL types.
fn.if<number>(condition, valueA, valueB);

// After: unify every value branch's SQL type, then define the decoder.
fn
  .if(condition, fn.toUInt64(valueA), fn.toUInt64(valueB))
  .mapWith((value) => String(value));

// Known anchors contextually type compatible safe-integer literals.
fn.if(condition, fn.toFloat64(valueA), 0);
fn.if(condition, fn.over(fn.rowNumber().toMixed()), 6); // fallback compiles as UInt64
```

`fn.coalesce(...)` 不再根据上下文把回退值推断为 `Float32`、`BFloat16`、`Decimal`，也不会把目标范围无法证明安全的 `bigint` 作为推断结果。这些回退值需要显式转换：

```ts
fn.coalesce(amount, fn.toDecimal64("0.00", 2));
```

### 正则替换

`fn.replaceRegexpAll(haystack, pattern, replacement)` 会生成 ClickHouse 的 [`replaceRegexpAll`](https://clickhouse.com/docs/sql-reference/functions/string-replace-functions#replaceregexpall) 调用。它返回 `Selection<string>`；如果带类型的输入可以为 NULL，则返回 `Selection<string | null>`。RE2 反斜杠和捕获组替换都要用 `String.raw`：

```ts
const usDate = fn.replaceRegexpAll(
  orders.dateText,
  String.raw`^(\d{4})-(\d{2})-(\d{2})$`,
  String.raw`\2/\3/\1`,
);
// Selection<string>
```

参数会作为查询参数绑定。模式使用 ClickHouse RE2 语法，替换支持 `\0`–`\9`，无效模式仍由 ClickHouse 报错。无类型的 `ckSql` 输入按可为 NULL 处理；确定片段不会为 NULL 时，写成 `ckSql<string>`。

### 带类型的元组聚合

`fn.tuple()` 会保留每一项的类型。传入一个值的 `groupArray` 形式会保留这个元组类型和解码器；一元 `fn.arrayReverseSort()` 也会保留相同的数组元素类型：

```ts
import { ckTable, ckType, fn } from "ck-orm";

const historyOrder = ckTable("history_order", {
  closedAt: ckType.dateTime64({ precision: 3 }),
  active: ckType.bool(),
  id: ckType.int32(),
});
const pageRow = fn.tuple(historyOrder.closedAt, historyOrder.active, historyOrder.id);
const items = fn.arrayReverseSort(fn.withParams("groupArray", [21], pageRow));
// Selection<[Date, boolean, number][]>
```

原始或动态场景仍可使用现有的显式泛型写法。

数组辅助函数的命名与 ClickHouse [数组函数](https://clickhouse.com/docs/sql-reference/functions/array-functions) 参考文档中的标准标题一致。只作为别名存在的函数名仍可通过 `fn.call(...)` 使用，避免重复扩展公共 API。

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

`fn.call(name, ...)` 和 `fn.withParams(name, ...)` 会在编译前把 `name` 验证为 SQL 标识符。`ck.has(...)`、`ck.hasAll(...)`、`ck.hasAny(...)` 和 `ck.hasSubstr(...)` 是这些 ClickHouse 函数的谓词快捷方式，适合直接放进 WHERE 条件。

`fn.jsonExtract(json, returnType, ...path)` 只接受 `ckType.*` 作为返回类型，因此 ClickHouse 返回类型和 TypeScript 解码器始终配套：

下面的 JSON 和数组示例都假定，虚构的 `probeTelemetry` 模式中还定义了 `metadata: JSON` 和 `tags: Array(String)` 两列。

```ts
import { ckType, ck, fn } from "ck-orm";

const alertCodes = fn.jsonExtract(
  probeTelemetry.metadata,
  ckType.array(ckType.string()),
  "alerts",
);

const riskScore = fn.jsonExtract(
  probeTelemetry.metadata,
  ckType.nullable(ckType.float64()),
  "risk",
  "score",
);

const filtered = db
  .select({
    sampleId: probeTelemetry.sampleId,
    alertCodes: alertCodes.as("alert_codes"),
    riskScore: riskScore.as("risk_score"),
  })
  .from(probeTelemetry)
  .where(ck.hasAny(alertCodes, ["battery", "thermal"]), ck.gte(riskScore, 80));
```

这些路径片段会作为 ClickHouse 的 JSON 路径参数传入。对象字段用字符串键，数组位置用 number 或 bigint 索引：

```ts
// Reads metadata.samples[1].id from a JSON document such as:
// { "samples": [{ "id": "900001" }, { "id": "900002" }] }
const secondSampleId = fn.jsonExtract(
  probeTelemetry.metadata,
  ckType.int64(),
  "samples",
  1,
  "id",
);
```

高阶数组函数的参数顺序遵循 ClickHouse。用 `ckSql` 写 lambda，把外部模式中的字段插值进去；lambda 内部变量则保留为裸 SQL 名称。对于元素结构由 lambda 控制的函数（例如 `fn.arrayMap(...)`、`fn.arrayFilter(...)` 和 `fn.range(...)`），默认返回 `Selection<unknown[]>`；如果查询需要更具体的解码元素类型，就链式调用 `.mapWith(...)`。

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

在这个 lambda 中，`${params.timeColumn}` 和 `params.scopeTable.startTsList` 都是来自模式的表达式。`start_ts` 和 `end_ts` 则是 ClickHouse 根据数组元素创建的 lambda 内局部变量。

`fn.arrayJoin(array)` 对应 ClickHouse 的 `arrayJoin`，会把一行输入展开为每个数组元素各占一行。两个数组需要保持位置对应时，把它和 `fn.arrayZip(...)`、`fn.tupleElement(...)` 搭配使用：

```ts
import { ck, fn } from "ck-orm";

const sampleIds = ["900001", "900002"];
const probeIds = ["probe_alpha", "probe_beta"];

const targetSamples = db.$with("target_samples").as(
  db.select({
    pair: fn.arrayJoin(fn.arrayZip(sampleIds, probeIds)).as("pair"),
  }),
);

const targetTelemetry = db.$with("target_telemetry").as(
  db
    .with(targetSamples)
    .select({
      sampleId: fn.tupleElement<string>(targetSamples.pair, 1).as("sample_id"),
      probeId: fn.tupleElement<string>(targetSamples.pair, 2).as("probe_id"),
    })
    .from(targetSamples),
);

const scopedSamples = db
  .select({
    pair: fn.tuple(targetTelemetry.sampleId, targetTelemetry.probeId).as("pair"),
  })
  .from(targetTelemetry)
  .as("scoped_samples");

const query = db
  .with(targetSamples, targetTelemetry)
  .select({
    sampleId: probeTelemetry.sampleId,
    probeId: probeTelemetry.probeId,
  })
  .from(probeTelemetry)
  .where(
    ck.inArray(fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId), scopedSamples),
  );
```

关键在于这几个函数的语义：`arrayZip(sampleIds, probeIds)` 保持位置配对，`arrayJoin(...)` 把配对结果展开成多行，`tupleElement(..., 1/2)` 分别读取这对值的两边。

复合键要用元组做成员判断。两个独立的 `IN` 谓词会产生笛卡尔积，可能匹配到从未请求过的组合：

```ts
// Avoid this when sampleId and probeId must stay paired.
const wrong = ck.and(
  ck.inArray(probeTelemetry.sampleId, sampleIds),
  ck.inArray(probeTelemetry.probeId, probeIds),
);
```

需要保持成对关系时，可以把一个元组与生成元组的子查询比较，或者与一组配对值比较。如果元素类型已知，`fn.tuple` 会保留每个元素的类型，因此整个列表会作为一个 `Array(Tuple(...))` 参数绑定：

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  [
    ["900001", "probe_alpha"],
    ["900002", "probe_beta"],
  ],
);
```

原始 SQL 片段没有元素类型。把元组传给 `ck.inArray` 或 `ck.eq` 前，先用 `fn.cast(fn.tuple(...), "Tuple(Int32, String)")` 封装它。

如果右侧是生成元组的子查询，同样的左侧表达式可以直接使用，不需要绑定参数：

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  scopedSamples,
);
```

数组辅助函数返回带类型的 `Selection` 值，所以可以直接组合到投影、过滤条件、GROUP BY 和连接中：

```ts
import { ck, fn } from "ck-orm";

const query = db
  .select({
    firstTag: fn.arrayElement<string>(probeTelemetry.tags, 1).as("first_tag"),
    maybeSecondTag: fn.arrayElementOrNull<string>(probeTelemetry.tags, 2).as(
      "maybe_second_tag",
    ),
    topTags: fn.arraySlice<string>(probeTelemetry.tags, 1, 3).as("top_tags"),
    tagCount: fn.length(probeTelemetry.tags).as("tag_count"),
    hasTags: fn.notEmpty(probeTelemetry.tags).as("has_tags"),
  })
  .from(probeTelemetry)
  .where(ck.hasAny(probeTelemetry.tags, ["solar", "critical"]));
```

### `fn.count` / `fn.uniqExact` 的模式

`fn.count()`、`fn.countIf(predicate)` 和 `fn.uniqExact(expression)` 是 `db.count(...)` 的聚合形式，也遵循同样的三种模式。当计数出现在查询内部（`select`、`having`、GROUP BY 或子查询里），而不是作为顶层标量时，这很有用。它们共用相同的 SQL 包装和解码器，所以三种模式的取舍也一样：

```ts
import { ck, fn } from "ck-orm";

const summary = db
  .select({
    probeId: probeTelemetry.probeId,
    // default — Selection<number>, renders toFloat64(count(...))
    sampleCount: fn.count(probeTelemetry.sampleId).as("sample_count"),
    // exact — Selection<string>, renders toString(count(...))
    auditedSampleCount: fn.count(probeTelemetry.sampleId).toSafe().as("audited_sample_count"),
    // wire-shape — Selection<number | string>, renders toUInt64(count(...))
    rawSampleCount: fn.count(probeTelemetry.sampleId).toMixed().as("raw_sample_count"),
    // countIf shares the same chainable modes
    healthySampleCount: fn.countIf(ck.eq(probeTelemetry.status, 1)).as("healthy_sample_count"),
    // uniqExact is symmetric with count — same modes, same decoders
    distinctMissions: fn.uniqExact(probeTelemetry.missionId).as("distinct_missions"),
    distinctMissionsExact: fn.uniqExact(probeTelemetry.missionId).toSafe().as("distinct_missions_exact"),
  })
  .from(probeTelemetry)
  .groupBy(probeTelemetry.probeId)
  // mode chooses the SQL semantics used in HAVING, ORDER BY, and other comparisons
  .having(ck.gt(fn.count(probeTelemetry.sampleId), 0));
```

解码器会拒绝任何不是非负整数的值：布尔值、`NaN`、小数和负数都会抛出 `Failed to decode count() result: ...`。这样，服务端输出损坏时会立即失败，不会静默产出 `0`。

其他聚合函数刻意不走这套模式 API，因为它们对精度的处理和 `count` / `uniqExact` 不一样：

- `fn.sum()` / `fn.sumIf()` 已经会按输入列选择安全的解码器（整数/Decimal 列使用 string，浮点列使用 number），并自动扩大 Decimal 精度；确实需要 `number` 时，链式调用 `.mapWith(Number)`。
- `fn.avg()` 始终返回 `Selection<number>`，因为 ClickHouse 内部通过 Float64 执行 `avg(Decimal)`；把它包装成 string 会误导人对实际运行路径的判断。
- `fn.min()` / `fn.max()` 保留输入列的 TS 类型（所以 `min(int64Col)` 是 `Selection<string>`，和该列读出来的类型一致）。

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

## 表和物化视图
<!-- src:03694c38 -->

`createTable()`、`createMaterializedView()` 和 `dropTable()` 各自发送一条由模式对象生成的 DDL 语句。它们不比较模式、不执行 `ALTER`，也不记录版本；已有表的迁移留在迁移工具里。客户端和会话上都可以调用。它们创建的表和视图是持久的，`runInSession()` 不会删除它们。

### `createTable()`

`createTable(table, options?)` 发送由 `ckTable(...)` 定义渲染出的 `CREATE TABLE` 语句：先是带 DDL 元数据的列，然后是 `ENGINE`、`PARTITION BY`、`PRIMARY KEY`、`ORDER BY`、`SAMPLE BY`、`TTL`、`SETTINGS` 和 `COMMENT`。

```ts
import { ckTable, ckType } from "ck-orm";

const usageEvents = ckTable(
  "usage_events",
  {
    tenantId: ckType.string("tenant_id"),
    occurredAt: ckType.dateTime64("occurred_at", { precision: 3, timezone: "UTC" }),
    units: ckType.uint64(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.tenantId, table.occurredAt],
  }),
);

await db.createTable(usageEvents);
```

- 默认情况下，已有同名表时语句会失败，错误名是 `TABLE_ALREADY_EXISTS`
- `mode: "if_not_exists"` 渲染 `CREATE TABLE IF NOT EXISTS`，已有表保持原样，即使列定义不同也不改
- `mode: "or_replace"` 渲染 `CREATE OR REPLACE TABLE`，会替换已有表并丢掉其中的数据
- 没有 `engine` 的表会使用 `MergeTree`，并带上 `ORDER BY tuple()`
- 来自 `ckAlias(...)` 的别名表会被拒绝

### `createMaterializedView()`

`createMaterializedView(name, { to, as, mode? })` 创建一个由插入触发的物化视图，写入已有表 `to`。`as` 是 `select()` 构建器，规则与 `insert(to).fromSelect(...)` 相同：必须投影 `to` 的每一个必需列；未知键或类型不匹配是 TypeScript 错误。

```ts
import { ckTable, ckType, fn } from "ck-orm";

const usageMinutes = ckTable(
  "usage_minutes",
  {
    tenantId: ckType.string("tenant_id"),
    minute: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
    unitsState: ckType.aggregateFunction("units_state", {
      name: "sum",
      args: [ckType.uint64()],
    }),
  },
  (table) => ({
    engine: "AggregatingMergeTree",
    orderBy: [table.tenantId, table.minute],
  }),
);

await db.createTable(usageMinutes);

const minute = fn.call<Date>("toStartOfMinute", usageEvents.occurredAt);
await db.createMaterializedView("usage_minutes_mv", {
  to: usageMinutes,
  as: db
    .select({
      tenantId: usageEvents.tenantId,
      minute,
      unitsState: fn.call("sumState", usageEvents.units),
    })
    .from(usageEvents)
    .groupBy(usageEvents.tenantId, minute),
});
```

- ClickHouse 按列名把物化视图的输出写入 `to`，所以 ck-orm 把 select 包成 `select ... as <column> from (...)`，让每个键落到 `to` 中对应的列
- `as` 里的查询参数在创建视图时被替换，因此保存下来的视图持有这些值
- select 强制的设置，例如 `leftJoin()` 的 `join_use_nulls = 1`，会写进视图的 `SETTINGS` 子句，因为请求设置不会随视图保存
- 视图只能看到创建之后插入的行；要回填，用同一个 select 运行 `db.insert(to).fromSelect(...)`
- `mode: "if_not_exists"` 渲染 `IF NOT EXISTS`。ClickHouse 25.8 和 26.3 没有 `CREATE OR REPLACE MATERIALIZED VIEW`，所以没有 `or_replace` 模式
- `to` 是必需的：自带 `ENGINE` 的视图、`POPULATE` 和可刷新视图都不支持；这些情况用 `ckSql` 配合 `command()`

### `dropTable()`

`dropTable(table, options?)` 删除一张表或一个视图。可以传入模式对象，也可以传入名称：

```ts
await db.dropTable("usage_minutes_mv", { sync: true });
await db.dropTable(usageMinutes, { ifExists: true, sync: true });
```

- 默认渲染 `DROP TABLE name`；没有这个名字的对象时，以 `UNKNOWN_TABLE` 失败
- `ifExists: true` 渲染 `IF EXISTS`
- `sync: true` 渲染 `SYNC`：调用在 ClickHouse 删掉数据之后才返回，而不是把删除留给 `Atomic` 数据库引擎的延迟清理
- 删除物化视图后，它的 `to` 表以及已经写入的行都还在
- 来自 `ckAlias(...)` 的别名表会被拒绝，也不会渲染 `ON CLUSTER`；在集群上，使用迁移工具，或用 `ckSql` 配合 `command()`

## 会话与临时表
<!-- src:8f031227 -->

ClickHouse 不提供传统事务。对于限定范围的分析和需要大范围过滤的工作流，会话级临时表往往是更合适的做法。

在会话回调里，`ck-orm` 提供：

- `runInSession()`
- `registerTempTable()`
- `createTemporaryTable()`
- `createTemporaryTableRaw()`

### `runInSession()`

```ts
import { ckTable, ckType, ckSql } from "ck-orm";

const tmpScope = ckTable("tmp_scope", {
  probe_id: ckType.string(),
});

await db.runInSession(async (session) => {
  // Temporary tables live only inside this Session and are cleaned up automatically.
  await session.createTemporaryTable(tmpScope);
  await session.insertJsonEachRow(tmpScope, [
    { probe_id: "probe_alpha" },
    { probe_id: "probe_beta" },
  ]);

  return session.execute(ckSql`
    SELECT probe_id
    FROM probe_telemetry
    WHERE probe_id IN (SELECT probe_id FROM tmp_scope)
  `);
});
```

如果临时表是由你控制的命令创建的，但你仍希望 `ck-orm` 在会话清理时把它删掉，就用 `registerTempTable(name)`：

```ts
await db.runInSession(async (session) => {
  await session.command(ckSql`
    CREATE TEMPORARY TABLE tmp_external_scope
    (
      probe_id String
    )
    ENGINE = Memory
  `);

  session.registerTempTable("tmp_external_scope");

  await session.insertJsonEachRow("tmp_external_scope", [
    { probe_id: "probe_alpha" },
  ]);
});
```

只有表定义来自可信来源、由开发者控制，且无法用 `ckTable(...)` 表达时，才使用 `createTemporaryTableRaw(name, definition)`：

```ts
await db.runInSession(async (session) => {
  await session.createTemporaryTableRaw(
    "tmp_raw_scope",
    `
      (
        probe_id String,
        created_at DateTime64(3, 'UTC')
      )
      ENGINE = Memory
    `,
  );
});
```

`definition` 参数只需要填写表名后面的内容。不要包含 `CREATE TEMPORARY TABLE` 或表名；ck-orm 会生成这两部分，并单独验证标识符。

### 会话行为

- 使用同一个 `session_id` 的请求共用一个会话并发控制器；客户端默认值为 `session_max_concurrent_requests = 1`
- ClickHouse 的真实会话在服务端是独占的，所以把 `session_max_concurrent_requests` 调到 `1` 以上会关闭本地串行化，但仍可能因 `SESSION_IS_LOCKED` 失败
- `createTemporaryTable()` 会自动登记该表，以便清理
- `createTemporaryTable()` 会接收模式对象；临时表的生命周期由 `Session` 管理，不属于模式本身
- `createTemporaryTableRaw()` 是只接受可信输入的原始 SQL API，用来定义临时表；它会拒绝包含多条语句的定义
- `runInSession()` 会在每个请求（包括清理请求）中，把解析后的 `session_id` 作为 `X-ClickHouse-Replica-Tag` 发送；这个由运行时管理的请求头不能通过 `http_headers` 覆盖
- ClickHouse Cloud 的[副本感知路由](https://clickhouse.com/docs/products/cloud/features/infrastructure/replica-aware-routing)需要单独启用，而且只能尽力而为：扩缩容可能重新映射标签；跨域浏览器部署还必须在 CORS 中允许 `X-ClickHouse-Replica-Tag`
- `runInSession()` 会在回调结束后删除已登记的临时表
- 嵌套的 `runInSession()` 调用总会创建新的子会话
- 嵌套调用不能复用任何仍处于活动状态的祖先 `session_id`
- 嵌套子会话始终使用不同的 `session_id`，因此不会与父会话共享临时表，也不会共享同会话的并发槽

### 会话并发契约

实际规则如下：

- 使用同一个显式 `session_id` 的请求默认会串行执行
- 客户端默认的 `session_id` 也进入同一个限流器
- 由 `withSettings()` 创建的子客户端在目标 `session_id` 相同时，与父客户端共用同一个限流器
- 不同的 `session_id` 不会互相阻塞
- `stream()` 和构建器的 `iterator()` 会一直占用同会话的并发槽，直到迭代器结束。提前停止（`break`、`return` 或发生错误）时，会在会话外发送 `KILL QUERY` 取消服务端查询（使用相同凭据和副本标签），然后等待 ClickHouse 释放会话，最多等待 `request_timeout`（默认 30 秒）
- 嵌套的 `runInSession()` 调用会创建全新的子 `session_id`，因此不会与父会话共享并发槽

中止请求或触发 `request_timeout` 都只会关闭连接。ClickHouse 会继续执行查询，直到查询完成或发现连接断开。因此 `INSERT ... SELECT` 仍可能执行完成，同一会话的下一个请求则可能返回 `SESSION_IS_LOCKED`。要让服务端先停止，可以把 `clickhouse_settings` 中的 `max_execution_time` 设为低于 `request_timeout`；也可以在会话外执行 `KILL QUERY WHERE query_id = '...'`；或者封装 `fetch`，在遇到 373 时对请求体可以重新发送的请求做有限次数的重试。包括 `SESSION_IS_LOCKED` 在内的清理失败，会通过 `onCleanupError` 或 `SessionError` 报告。

推荐用法：

- 临时表流程和任何需要延续会话的逻辑都保持 `session_max_concurrent_requests = 1`
- 需要真正并行时，使用不同的 `session_id`，不要调高同会话上限
- 只有明确要去掉本地背压，并且能接受 `SESSION_IS_LOCKED` 时，才调高 `session_max_concurrent_requests`

## 运行时方法
<!-- src:1d0d331a -->

除了查询构建器，客户端还提供更底层的运行时方法。

每个查询常用的字段包括：

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

`session_timeout` 和 `session_check` 特意放在这里：它们描述的是某个请求或会话块，而不是客户端的全局默认值。
`query_id`、`session_id` 和 `session_timeout` 是请求/会话选项；不要把它们放进 `clickhouse_settings`。

`session_check` 不会启动新的 ClickHouse 会话。需要让 ClickHouse 检查显式 `session_id` 是否已存在时，才使用它。
任何携带 `session_id` 的请求都会参与同一个会话级限流器，包括原始 `execute()`、`command()`、`stream()`，以及查询构建器的 `.execute()` 和 `.iterator()`。

每次请求传入的 `clickhouse_settings` 只会覆盖该请求的 ClickHouse 设置：

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

执行原始查询并返回 `Record<string, unknown>[]`：

```ts
const rows = await db.execute(ckSql`SELECT 1 AS one`);
```

### `stream()`

流式获取原始查询结果：

```ts
for await (const row of db.stream(ckSql`SELECT number FROM numbers(10)`)) {
  console.log(row);
}
```

如果 `stream()` 使用了 `session_id`，同会话的并发槽只有在异步迭代器完成后，或你通过提前跳出循环来关闭它后，才会释放。

### `command()`

执行不返回结果集的命令：

```ts
await db.command(ckSql`SYSTEM FLUSH LOGS`);
```

### `withSettings()`

创建一个带额外默认设置的子客户端：

```ts
const reportDb = db.withSettings({
  max_execution_time: 60,
  join_use_nulls: 0,
});
```

`withSettings()` 只会修改默认的 ClickHouse 会话/查询设置。返回的客户端与父客户端共用传输、认证、超时和会话并发控制器。
`join_use_nulls: 0` 字面量会被推断，因此子客户端的左连接类型不含 null；如果值只被标注为普通的 `number`，结果仍会带有 `| null`。

`runInSession()` 中的会话生命周期选项仍与 ClickHouse 设置分开：

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
    session_id: "probe-session",
    session_timeout: 120,
    clickhouse_settings: {
      allow_experimental_correlated_subqueries: 1,
    },
  },
);
```

`runInSession()` 会把自己的 `clickhouse_settings`、`http_headers` 和 `query_params` 与父客户端的默认值合并，两边设置了同一个键时以会话的值为准。和 `withSettings()` 一样，会话设置中的 `join_use_nulls: 0` 或 `join_use_nulls: 1` 字面量也决定会话里左连接的类型是否含 null。

### 编译后查询集成

大多数应用直接使用查询构建器即可。只有在接入已经生成 `CompiledQuery` 的其他层时，才使用 `executeCompiled()` 和 `iteratorCompiled()`。

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

### `ping()` 和 `replicasStatus()`

```ts
await db.ping();
await db.replicasStatus();
```

## 可观测性
<!-- src:45091b93 -->

`ck-orm` 提供两层可观测性能力：

- `logger` 和 `logLevel`：内置日志记录
- `plugins`：查询生命周期钩子；OpenTelemetry 追踪以 `ck-orm/otel` 插件的形式提供

### 日志记录器

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
  databaseUrl: "http://127.0.0.1:8123/telemetry_lab",
  logger,
  logLevel: "info",
});
```

### OpenTelemetry 插件

`ck-orm/otel` 的 `otelTracing()` 会为 ck-orm 发给 ClickHouse 的每个请求记录一个客户端 span。它放在单独的入口里：`ck-orm` 主入口从不导入 OpenTelemetry，所以不做追踪的应用、浏览器端的打包产物和边缘 worker 不会为它付出任何代价。`@opentelemetry/api` 是可选的对等依赖项，只有 `ck-orm/otel` 会导入它。

#### 设置

ck-orm 通过 OpenTelemetry API 创建 span，而 API 本身什么都不会记录：SDK 注册 tracer provider 之前，所有 span 都会被丢弃。ck-orm 不会启动 SDK，也不会选择导出器，这两件事由你的应用在启动时做一次。

先安装 API、SDK 和导出器。下面用的是 Node.js SDK 加 OTLP/HTTP 导出器；任何能注册 tracer provider 的 SDK 都可以，包括浏览器端的 SDK。

```bash [bun]
bun add @opentelemetry/api @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-http
```

```bash [npm]
npm install @opentelemetry/api @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-http
```

```bash [pnpm]
pnpm add @opentelemetry/api @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-http
```

```bash [yarn]
yarn add @opentelemetry/api @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-http
```

在第一次查询之前启动 SDK，比如放进入口文件最先导入的模块里：

```ts
// instrumentation.ts
import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";

export const sdk = new NodeSDK({
  serviceName: "billing-worker",
  traceExporter: new OTLPTraceExporter(),
});

sdk.start();
```

导出器默认发送到 `http://localhost:4318/v1/traces`；设置标准的 `OTEL_EXPORTER_OTLP_ENDPOINT` 环境变量或传入 `url` 可以改掉它。SDK 还会注册上下文管理器和 W3C 传播器，span 的父子关系与 `propagateTraceContext` 都依赖它们。运行时间很短的进程退出前必须 `await sdk.shutdown()`，否则最后几个 span 会丢失。

然后把插件加到客户端上：

```ts
import { clickhouseClient } from "ck-orm";
import { otelTracing } from "ck-orm/otel";

const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123/telemetry_lab",
  plugins: [
    otelTracing({
      includeStatement: false,
      includeRowCount: true,
    }),
  ],
});
```

不接 Collector 也能看到 span：[OpenTelemetry 追踪示例](https://ck-orm.lyz.cloud/examples/#otel-tracing)用内存导出器，并把插件记录的内容打印出来。

#### Span

span 的类型是 `CLIENT`。返回行的查询，span 名称是 `clickhouse QUERY`；其他请求叫 `clickhouse <OPERATION>`，例如 `clickhouse INSERT` 或 `clickhouse CREATE`。查询开始时处于活动状态的 span 会成为它的父 span，比如框架的 OpenTelemetry 插桩为每个 HTTP 请求开的那个 span。

除非传入 `tracer`，插件会使用 `trace.getTracer("ck-orm")`。它会根据 `clickhouseClient(...)` 的配置得到数据库名、服务器地址、服务器端口和请求超时。
服务级标签通过 `attributes` 传入；以 `db.` 开头的键会被忽略，避免应用代码覆盖内置的数据库属性。

`includeStatement: false` 对共享追踪后端更安全。默认值是 `true`；如果表名、列名或查询结构不应离开服务边界，就关闭它。
绑定值不会嵌入精简后的 SQL 语句，但 SQL 结构在运维上仍可能敏感。

失败 span 的状态始终是 `ERROR`。默认情况下，插件会保留封闭集合里的失败分类（`error.type`、`ck_orm.error.kind`、`ck_orm.execution_state`，以及能映射到的 ClickHouse 错误码或已分配的 HTTP 状态），并且不会把原始错误的 message、堆栈、响应体和 cause 写进导出的 span。调用方拿到的错误对象不变。`includeErrorDiagnostics: true` 会恢复当前的原始诊断导出——status 的 message、对原始错误调用 `recordException`，以及现有的错误属性——而且与 `includeStatement` 无关。只有构造 `otelTracing()` 时读到一次的、值为布尔值 `true` 的自有数据属性，才会打开这个开关。这不是全面的脱敏保证。内置日志记录器和 `includeStatement: true` 仍会带上调用方提供的内容。

#### 服务端 span

设置 `propagateTraceContext: true`，每个请求都会带上该 span 的 W3C 追踪上下文（`traceparent`、`tracestate`）。这样 ClickHouse 会为这次查询记录自己的 span，例如 `HTTPHandler`、`query` 和各个流水线阶段，并把它们挂在 ck-orm 的 span 之下。插件只转发这两个请求头，绝不会转发全局传播器产生的 `baggage` 或厂商专有请求头，而且只对已采样的 span 转发：ClickHouse 对收到的每个追踪上下文都会记录服务端 span，不管它有没有被采样。插件通过全局注册的传播器注入，OpenTelemetry SDK 默认将其设为 W3C 追踪上下文；没有注册传播器时，不会发送任何内容。

```ts
const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123/telemetry_lab",
  plugins: [otelTracing({ propagateTraceContext: true })],
});
```

ClickHouse 把这些 span 写入 `system.opentelemetry_span_log`，不会自行导出；要导出到追踪后端，请参阅 [Tracing ClickHouse with OpenTelemetry](https://clickhouse.com/docs/operations/opentelemetry)。服务端 span 带有 `clickhouse.query_id`，它与 ck-orm span 上的 `db.query.id` 一致；失败通过 `clickhouse.exception_code` 报告，而不是 span 状态。

如果你在 `http_headers` 中传入自己的 `traceparent`，它会覆盖插件的；请同时传入与之匹配的 `tracestate`，否则插件的 `tracestate` 仍会一起发出。

在浏览器中，除非 ClickHouse 允许 `traceparent` 请求头，否则带上它的请求会通不过 CORS 预检；又因为只有已采样的请求才带这个头，失败看起来是随机的。默认的 `http_options_response` 不允许它，所以只有在 `config.d` 文件中加上它之后，才能在浏览器里启用传播：

```xml
<clickhouse>
  <http_options_response replace="replace">
    <header><name>Access-Control-Allow-Origin</name><value>*</value></header>
    <header><name>Access-Control-Allow-Headers</name><value>origin, x-requested-with, x-clickhouse-format, x-clickhouse-user, x-clickhouse-key, Authorization, traceparent, tracestate</value></header>
    <header><name>Access-Control-Allow-Methods</name><value>POST, GET, OPTIONS</value></header>
    <header><name>Access-Control-Max-Age</name><value>86400</value></header>
  </http_options_response>
</clickhouse>
```

### 编写插件

插件是一个带 `name` 的对象，最多可以有三个钩子：

```ts
import { clickhouseClient, type ClickHouseORMPlugin } from "ck-orm";

const slowQueries = (thresholdMs: number): ClickHouseORMPlugin => ({
  name: "slow-queries",
  onQuerySuccess(event) {
    if (event.durationMs >= thresholdMs) {
      console.warn("slow query", event.querySummary, event.durationMs);
    }
  },
});

const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123/telemetry_lab",
  plugins: [slowQueries(500)],
});
```

`onQueryStart` 可以为当前请求返回请求头：

```ts
const requestSource: ClickHouseORMPlugin = {
  name: "request-source",
  onQueryStart() {
    return { http_headers: { "x-request-source": "billing-worker" } };
  },
};
```

[查询插件示例](https://ck-orm.lyz.cloud/examples/#query-plugins)运行了一个插件：它在 `onQueryStart` 中返回一个请求头，并在 `onQuerySuccess` 和 `onQueryError` 中记录每条语句，输出按语句实际执行的顺序排列。

插件遵循以下规则：

- ck-orm 发给 ClickHouse 的每个请求都会运行钩子：查询、流式读取、命令、插入、DDL 辅助函数、`ping()`、`replicasStatus()`，以及 `runInSession()` 中的临时表清理。只有取消被放弃的流并等待其会话解锁的请求（`KILL QUERY` 及其后的轮询）不会运行钩子。
- 每个 `onQueryStart` 之后，都会有且只有一个带相同 `executionId` 的 `onQuerySuccess` 或 `onQueryError`；对于流，这发生在迭代器结束或被关闭时。调用方提前停止的流算作成功，并报告到那时为止已读取的行数。按请求保存的状态请以 `executionId` 作为键。
- `onQueryStart` 按注册顺序运行，并且全部完成后才会发送请求。`onQuerySuccess` 和 `onQueryError` 按相反顺序运行。内置的日志记录器始终在最外层运行。
- 后面的插件设置的请求头会覆盖前面的。显式传入的 `http_headers`（无论是客户端级还是请求级）会覆盖插件的请求头。插件不能覆盖由连接凭据生成的 `Authorization` 请求头，也不能覆盖会话发送的 `X-ClickHouse-Replica-Tag` 请求头。
- 抛出异常的钩子会连同插件名一起写入日志，不影响查询，也不影响其他插件。抛出异常的 `onQueryStart`，或返回了无效请求头的 `onQueryStart`，它的请求头会被丢弃。
- 事件是只读的。插件不能修改 SQL、参数、设置或结果。
- 钩子在查询路径上会被 await。网络导出之类的慢操作请先缓冲，不要在钩子里 await。

公开的可观测性类型：

- `ClickHouseORMPlugin`
- `ClickHouseORMQueryStartResult`
- `ClickHouseORMQueryEvent`
- `ClickHouseORMQueryResultEvent`
- `ClickHouseORMQueryErrorEvent`
- `ClickHouseORMQueryStatistics`
- `ClickHouseORMTracingOptions`（来自 `ck-orm/otel`）
- `ClickHouseORMLogLevel`

内置事件字段包括 `databaseName`、`serverAddress`、`serverPort`、`requestTimeoutMs`、`statementHash`、`querySummary`、`queryId`、`sessionId`；如果 ck-orm 无需从原始 SQL 猜测就能识别出单个表，还会包括 `tableName`。
非流式 JSON 查询在服务器返回统计信息时，也会包含 ClickHouse 响应统计：`serverElapsedMs`、`readRows`、`readBytes`、`resultRows` 和 `rowsBeforeLimitAtLeast`。

## 错误模型
<!-- src:b4b93db0 -->

公开错误类型（只导出类型）：

- `ClickHouseORMError`
- `DecodeError`

运行时错误检查：

- `isClickHouseORMError(error)`
- `isDecodeError(error)`

`ClickHouseORMError` 会尽量保留上下文，包括：

- `kind`
- `executionState`
- `queryId`
- `sessionId`
- `httpStatus`
- `clickhouseCode`
- `clickhouseName`
- `responseText`
- `requestTimeoutMs`

当前公开的 `kind` 值：

- `client_validation`
- `request_failed`
- `decode`
- `timeout`
- `aborted`
- `session`

先把完整的错误上下文记录到服务端日志中，再向应用调用方返回精简的公开错误：

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

不要把 `responseText` 或原始错误 `message` 返回给不受信任的客户端。ClickHouse 的错误文本可能包含 SQL 片段和对象名。

## 安全
<!-- src:b6d029f6 -->

`ck-orm` 默认就让构造不安全的 SQL 变得很难。

内置保护包括：

- 传给 `ckSql.identifier()` 的标识符会先验证，再加引号。
- 会验证 `fn.call(...)`、`fn.withParams(...)`、`ckType.aggregateFunction(...)` 和 `ckType.simpleAggregateFunction(...)` 使用的函数名；`AggregateFunction` 的参数必须是字符串或数字字面量。
- 传给 `ckType.nested({...})` 的嵌套字段键会按 ClickHouse 标识符规则验证。
- 插值到 `` ckSql`...` `` 的值会变成 ClickHouse 具名参数，不会拼进原始 SQL 文本。
- 以 `orm_param` 开头的 `query_params` 键会被拒绝，因为这个前缀保留给内部 SQL 模板参数使用。
- 每个请求只允许一条顶层语句。
- 根据连接配置生成的授权请求头不能被调用方传入的 `http_headers` 覆盖。
- 结构化连接配置会拒绝在 `host` 中嵌入凭据。
- 追踪目标信息会在附加到 span 之前规范化。

### 可信场景专用 API

当可信的临时表 DDL 无法用 `ckTable(...)` 表达时，`ck-orm` 会在 `runInSession()` 中提供一个限定作用域的原始 SQL API：

- `session.createTemporaryTableRaw(name, definition)`

`fn.call(name, ...)` 和 `fn.withParams(name, ...)` 会验证 `name`，但仍要把动态选择的函数名当成开发者控制的输入，而不是不受信任的运行时输入。

`ckSql.identifier(...)` 会等到编译/执行边界才验证。构造这个 SQL 片段的成本很低；片段编译进查询时才会报错。

### 追踪数据暴露

`otelTracing()` 会把压缩空白后的 SQL 文本作为 `db.statement` 和 `db.query.text` 附加到 span 上，因为 `includeStatement` 默认是 `true`。

绑定值会单独存储为 ClickHouse 具名参数，不包含在语句文本中；但表名、列名和查询结构仍会出现在追踪输出中。
如果追踪后端是共享的，或者部署在主机之外，请关闭 `includeStatement`，或者在收集器中过滤 `db.statement` / `db.query.text`。

失败时，默认的追踪导出会保留封闭集合分类，并省略原始错误文本。`includeErrorDiagnostics: true` 会明确重新导出原始诊断，且与 `includeStatement` 无关。这个开关、`includeStatement: true` 以及内置日志记录器，都不是全面的脱敏控制。

ClickHouse 服务端 span 也会把语句记录为 `system.opentelemetry_span_log` 中的 `db.statement`。除非你导出这张表，否则它只留在 ClickHouse 服务器上。

### 错误响应

`ClickHouseORMError.responseText` 和错误对象的 `message` 可能包含 ClickHouse 服务器返回的原始文本，包括 SQL 片段和数据库对象名。

不要把这些细节直接转发给不受信任的客户端。在服务端记录它们，改为返回一条通用错误消息。

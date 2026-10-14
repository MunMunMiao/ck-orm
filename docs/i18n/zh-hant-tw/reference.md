## 安裝
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

ck-orm 沒有必要的依賴。OpenTelemetry 追蹤是獨立的進入點 `ck-orm/otel`，`@opentelemetry/api` 是選用的 peer dependency，只有這個進入點會用到。`ck-orm` 的主進入點從不匯入 OpenTelemetry，所以不追蹤的應用程式、瀏覽器打包產物或邊緣 worker 既不用安裝 OpenTelemetry，也不會把它打包進去。要追蹤查詢，請依照 [OpenTelemetry 外掛程式](#opentelemetry-plugin)的設定步驟操作。

## 快速上手
<!-- src:847057c0 -->

下面的範例只用一張資料表，主要流程會比較容易看懂。

### 1. 定義 schema

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

### 2. 建立用戶端

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

### 3. 查詢資料

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

建構器查詢是具有 then 方法的物件，所以 `await query` 會直接執行查詢。

`fn.sum` 對 `Decimal` 欄位會自動轉型成 `Decimal(P, S)`，並回傳 `string` 以完整保留精確度。請見 [運算式中的 Decimal 精確度](#decimal-precision-in-expressions)。

### 4. 觀察查詢

`plugins` 能介入 ck-orm 送給 ClickHouse 的每個請求。只要有 SDK 在執行，`ck-orm/otel` 的 `otelTracing()` 就會把每個請求變成一個 OpenTelemetry span；設定步驟見 [OpenTelemetry 外掛程式](#opentelemetry-plugin)，自己寫 hook 則見[撰寫外掛程式](#writing-plugins)。

```ts
import { clickhouseClient } from "ck-orm";
import { otelTracing } from "ck-orm/otel";

export const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  plugins: [otelTracing()],
});
```

## 範例
<!-- src:c8f5c20b -->

[範例頁面](https://ck-orm.lyz.cloud/examples/) 會展示可執行的查詢，並列出每個查詢實際傳給 ClickHouse 的完整 SQL。每個範例都是 [`examples/`](../examples) 中的專案，也有各自的測試。ClickHouse 的可執行測試放在 [`e2e/`](../e2e)，其中 [`e2e/scenarios.ts`](../e2e/scenarios.ts) 放了貼近正式環境的情境 schema。

## 心智模型
<!-- src:a6fa96d2 -->

使用 `ck-orm` 時，記得把下面這些界線分清楚：

- `ckType.*` 定義 schema 的欄位型別
- `ckTable(...)` 定義資料表的 schema
- `ck.*` 是查詢輔助工具的命名空間
- `fn.*` 是 SQL 函式輔助工具的命名空間
- schema 描述資料表和欄位，不描述資料庫名稱
- `clickhouseClient(...)` 負責資料庫連線
- 建構器查詢是預設方式；遇到 ClickHouse 專用運算式時，也可以使用原始 SQL
- `runInSession()` 是 ClickHouse 的工作階段輔助工具，不是交易
- `plugins` 可觀察 ck-orm 送出的每個請求；追蹤是需要自行加入的 `ck-orm/otel` 外掛程式，不在主進入點裡
- `leftJoin()` 和 `asofLeftJoin()` 預設採用 SQL 風格的 NULL 語意
- 數值很大或風險較高的結果會採保守方式解碼，不會默默強制轉型

`ck-orm` 專注在有型別的 ClickHouse 存取：資料表定義、查詢組合、寫入輔助工具、工作階段輔助工具和可觀察性。`createTable()`、`createMaterializedView()` 和 `dropTable()` 各自執行一條 DDL 陳述式；schema 遷移（比較 schema、`ALTER`、版本控管）仍由應用程式採用的遷移工具負責。

## Schema DSL 用法
<!-- src:5149bff0 -->

### `ckTable()`

用 `ckTable(name, columns, options?)` 定義資料表。

本節的範例都假設已匯入以下內容：

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

第三個引數可以是一般物件，也可以是工廠函式：

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

公開的資料表選項：

- `engine`
- `partitionBy`
- `primaryKey`
- `orderBy`
- `sampleBy`
- `ttl`
- `settings`
- `comment`
- `versionColumn`

欄位定義也可以直接帶有 DDL 中繼資料：

- `.default(expr)`
- `.materialized(expr)`
- `.aliasExpr(expr)`
- `.comment(text)`
- `.codec(expr)`
- `.ttl(expr)`

`ck-orm` 的 schema 中繼資料有兩項用途：

- 用來驅動有型別的查詢、插入驗證和結果解碼
- 為 `createTable()` 和 `createTemporaryTable()` 渲染 `CREATE TABLE` 陳述式

ck-orm 不會遷移或同步既有的 ClickHouse 資料表：`createTable()` 只建立資料表（見 [資料表與具體化檢視](#tables-and-materialized-views)）。schema 遷移留在遷移工具中，並讓 `ckTable(...)` 與應用程式實際讀寫的 schema 保持一致。

範例：

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

### 型別推論

每張資料表都提供：

- `table.$inferSelect`
- `table.$inferInsert`

```ts
type TelemetryRow = typeof probeTelemetry.$inferSelect;
type TelemetryInsert = typeof probeTelemetry.$inferInsert;

const sampleId: TelemetryRow["sampleId"] = "900001";
const ingestVersion: TelemetryInsert["ingestVersion"] = "1";
```

寫泛型輔助工具時，請使用：

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

`InferSelectSchema` 和 `InferInsertSchema` 接受的 schema 物件只是單純的 TypeScript 分組。它們很適合定義共用的模型型別，而且和用戶端設定是分開的。

### 用 `$type<T>()` 細化欄位型別

每個欄位建構器都提供可鏈式的 `$type<T>()` 方法，它會覆寫欄位的 TypeScript 型別，但不改變任何執行階段行為。如果預設推斷出的型別在你的領域裡太寬，就可以用它把型別收窄：

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

`$type<T>()` 可以和其他欄位修飾詞以任意順序鏈式組合：`ckType.string().$type<Role>().default(ckSql`'guest'`).comment("…")`。執行階段的解碼器、編碼器、SQL 型別與 DDL 輸出都不受影響；`$type` 只在編譯期做型別斷言。

`enum8` 和 `enum16` 已經會從值物件推斷字面值聯集，所以只有要把聯集重新指定成另一個型別，或對齊外部 enum 型別時，才需要 `$type`：

```ts
// Inferred automatically — no `as const`, no explicit type arguments
const status = ckType.enum8({ active: 1, paused: 2, banned: 3 });
//    ↑ Enum8<"active" | "paused" | "banned">

// Re-brand on top of the inferred keys
type UserStatus = "active" | "paused" | "banned";
const userStatus = ckType.enum8({ active: 1, paused: 2, banned: 3 }).$type<UserStatus>();
```

> **注意事項：** `$type<T>()` 不會驗證執行階段的值。如果資料庫資料列含有宣告型別以外的值（例如 schema 遷移前留下的舊資料），TypeScript 仍會把它當成符合型別來收窄；涵蓋所有分支的 `switch` 陳述式也會默默漏掉這個情況。要在讀取時強制檢查值，請在投影中使用 `mapWith`；每次解碼和編碼都要檢查，就用下面的 `$validator()`。

### 用 `$type<{ select, insert }>()` 分開插入與選取型別

ClickHouse 欄位有時候在插入時接受比選取回傳值更寬的型別；例如 `DateTime` 欄位插入時接受 `string` 和 `Date`，但解碼後一定回傳 `Date`。把物件傳給 `$type`，就能分開這兩種型別：

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

不寫 `insert` 的 `$type<{ select: T }>()`，等同只有一個泛型的 `$type<T>()`。

### 插入模型規則：DEFAULT、MATERIALIZED、ALIAS

`$inferInsert` 是根據 schema 中各欄位的資料形狀推導出來的，不是直接從 `$inferSelect` 複製：

| DDL 修飾詞 | 對插入模型的影響 |
| --- | --- |
| `.default(expr)` | 欄位變成選用；省略時由 ClickHouse 填入值 |
| `.materialized(expr)` | 欄位會完全從插入模型移除 |
| `.aliasExpr(expr)` | 欄位會完全從插入模型移除 |

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

### 用 `$validator(schema)` 做執行階段驗證

`$type` 不夠用時，例如資料庫可能有舊資料、要接受不受信任的上游送來的 JSON 資料形狀，或希望插入 API 把字串轉成 `Date`，請改為串接 `$validator(schema)`。任何符合 [Standard Schema v1 規格](https://github.com/standard-schema/standard-schema) 的物件都可以：Zod 3.23+、Valibot 1+、ArkType、Effect Schema、TypeBox。ck-orm 不依賴這些函式庫中的任何一個。

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

Schema 轉換（例如 `z.string().transform((v) => new Date(v))`）會照常生效；插入時欄位接受 schema 的*輸入*型別，選取時則提供*輸出*型別。`$validator` 只支援同步 schema；非同步 schema 會在解碼或編碼時拋出錯誤。

`$type` 和 `$validator` 的差異：

| | `$type<T>()` | `$validator(schema)` |
| --- | --- | --- |
| 執行階段成本 | 無 | 每列每個欄位執行一次同步 schema 驗證 |
| 型別安全 | 編譯期斷言 | 由 schema 推斷 |
| 執行階段驗證 | 沒有 | 有；不相符時拋出錯誤 |
| 輸入與輸出型別不同 | 透過 `$type<{ select, insert }>()` | 透過 schema 轉換 |
| 外部依賴 | 沒有 | 沒有（schema 函式庫由你選擇） |

### JSON 欄位型別

ck-orm 支援 ClickHouse 24.x 以上的 **新版 JSON 資料型別**（[文件](https://clickhouse.com/docs/sql-reference/data-types/newjson)）。它會在儲存層把 JSON 物件拆成有型別的子欄位，不會把整個值存成不透明字串。先掌握兩條基本規則：

- ClickHouse 的 `JSON` 資料型別只接受 **頂層物件**。頂層陣列、字串和數字都不接受。ck-orm 在編譯時用 `T extends Record<string, unknown>`、在執行階段用每次插入／選取的 O(1) 廉價檢查，執行同一套規則。
- 有型別的路徑（`a.b UInt32`）在 ClickHouse 內會成為真正的子欄位；ck-orm 會讓這些路徑走對應 ck-orm 欄位工廠的 `mapFromDriverValue` / `mapToDriverValue`，所以宣告成 `ckType.uint64()` 的路徑會像頂層 `uint64` 欄位一樣，解碼成不失真的 `string`。

最簡單的寫法和其他 ck-orm 欄位工廠一樣：

```ts
import { ckTable, ckType } from "ck-orm";

const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json("payload"),
});
// DDL:  `payload` JSON
// $inferSelect.payload: { [key: string]: unknown }
```

加上泛型，`$inferSelect` / `$inferInsert` 就會帶出具體的資料形狀：

```ts
type EventPayload = { user_id: number; event: string; meta?: { ip: string } };

const userEvents = ckTable("user_events", {
  id: ckType.uint64(),
  payload: ckType.json<EventPayload>("payload"),
});
```

如果想把泛型和其他欄位建構器的呼叫寫在一起，可以用鏈式寫法 `ckType.json<T>().$type<T2>()`。

#### 參數化 DDL

工廠的第二個引數有四個 NewJSON DDL 設定：

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

`typeHints` 的鍵會依 `Paths<T>` 做型別檢查；像 `{ "user.idd": ... }` 這種拼錯的鍵會讓編譯失敗。產出的 DDL 一律按穩定的字典順序排列；不管設定怎麼寫，外部 schema 差異比對工具都會看到位元組完全相同的輸出。

#### typeHints 會影響執行階段解碼器

ClickHouse 的 64 位元不失真策略會讓 ck-orm 把 `UInt64` 解碼成字串。如果你同時在 TypeScript 的資料形狀 *和* `typeHints` 項目宣告 `user_id`，兩邊必須保持同步；以 `uint64` 來說，泛型要寫 `user_id: string`。如果插入時接受的輸入和選取時解碼出的型別不同，就使用 `$type<{ select, insert }>()`：

```ts
ckType.json("payload", { typeHints: { user_id: ckType.uint64() } })
  .$type<{
    select: { user_id: string };
    insert: { user_id: string | number | bigint };
  }>();
```

#### 路徑存取

每個 JSON 欄位都有五個可鏈式的路徑方法，會原樣產生 ClickHouse 的路徑存取語法：

| 方法 | SQL | 意義 |
| --- | --- | --- |
| `col.path("a.b")` | `col.a.b` | 讀取指定路徑；靜態型別是 `PathValue<T, "a.b">`（見下方 `typeHints` 的注意事項） |
| `col.castPath("a.b", ckType.uint64())` | `col.a.b.:UInt64` | 用另一個 ck-orm 欄位工廠強制解碼指定路徑；靜態型別取自轉型用欄位的 `TData` |
| `col.subobject("a")` | `col.^a` | 把巢狀物件讀成 `JSON` 值 |
| `col.merged("a")` | `col.@a` | 讀取合併後的共用資料檢視 |
| `col.arrayPath("a")` | `col.a[]` | 展開陣列型別的路徑 |

> `col.path(P)` 的靜態型別固定是 `PathValue<T, P>`；它由泛型 `T` 推得，不是由 `typeHints` 決定。`typeHints` 只會改變執行階段解碼器；靜態型別和 typeHint 要手動保持同步（上一節說明了 `uint64 → string` 的不失真策略）。如果兩者不一致，改用 `castPath(P, ckType.X())`；它的回傳型別由轉型用欄位提供。

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

`Paths<T>` 會在編譯時驗證路徑字面值；若 `"a.zzz"` 不屬於 `T`，`payload.path("a.zzz")` 就過不了型別檢查。

每個鏈式修飾詞（`$type`、`$validator`、`.default()`、`.materialized()`、`.bind()`、……）都會在執行階段重新掛上路徑方法，所以底層呼叫始終可用。**除了 `$validator` 之外，靜態型別會沿著每個鏈節保留這些方法**：`$validator` 的契約是泛型 `StandardSchemaV1<unknown, unknown>`；在 TypeScript 強制的 LSP 風格子型別規則下，這個契約無法安全收窄成 `JsonShape`，所以走過這個鏈節後，靜態型別會退回 `Column<T>`。方法本身還是可以呼叫；如果要繼續鏈式呼叫，做一次型別斷言就好：

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

#### `fn.json*`：動態路徑呼叫點

如果路徑字串在執行階段才建立（或要讀取 `T` 中沒宣告的路徑），就使用 `fn.*` 命名空間：

```ts
import { fn } from "ck-orm";

const userPath = userInput as string;
await db.select({
  raw:    fn.jsonPath<string>(userEvents.payload, userPath),
  casted: fn.jsonCast(userEvents.payload, userPath, ckType.string()),
  shape:  fn.dynamicType(fn.jsonPath(userEvents.payload, userPath)),
});
```

`fn.jsonCast` 是 `castPath` 的動態版本；`fn.jsonSubobject`、`fn.jsonMerged` 和 `fn.jsonArray` 對應各自的欄位方法；`fn.dynamicType` 提供 ClickHouse 的 `dynamicType()` 函式，用來查看 Dynamic 型別路徑在執行階段的實際種類。

#### 執行階段驗證

每個資料列都會跑兩層檢查，**順序如下**：

1. **ck-orm 內建的 JSON 防護**：`null` / `undefined` 會原樣通過，讓 `nullable()` JSON 欄位照常運作；其他非物件輸入（陣列、字串、數字、布林值）一律不接受，並產生該欄位的 `client_validation` / `decode` 錯誤，例如 `JSON column expects a plain object, got array`。`typeHints` 的有型別路徑在寫入和讀取時都會經過對應欄位工廠的編碼器／解碼器。這層永遠啟用。
2. **選用的 `$validator(schema)`**：啟用後，插入時會在內建編碼器*之前*執行，選取時則在內建解碼器*之後*執行。用它做資料形狀層級的驗證：

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

ck-orm **刻意不會**遞迴進入 JSON 值，檢查 `T["a"]` 是否真的是 `number` 等型別。需要這種檢查時，請使用 `$validator`；JSON 防護只攔下 ClickHouse 錯誤訊息不容易對應回欄位的情況。頂層純量輸入會直接報錯，錯誤中會明確帶上欄位名稱：

```ts
await db.insert(events).values({ payload: ["wrong"] as never });
// throws: JSON column expects a plain object, got array
```

#### 插入時的預設值

JSON 欄位和其他欄位一樣，遵守相同的 `$inferInsert` 規則。`.default()` 會讓欄位變成選用；`.materialized()` / `.aliasExpr()` 會把它完全從插入模型移除：

```ts
const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json<{ note: string }>("payload").default(ckSql`'{}'`),
});

await db.insert(auditLogs).values({ id: "1" }); // payload filled by CH DEFAULT
```

### 用 `ckAlias()` 建立別名

當同一張資料表需要在查詢中出現多次時，就使用 `ckAlias()`。

```ts
import { fn, ck, ckAlias } from "ck-orm";

const telemetry = ckAlias(probeTelemetry, "telemetry");
```

`ckAlias()` 回傳的欄位會自動重新繫結到別名。

### 欄位名稱

schema 物件的鍵是邏輯鍵；TypeScript 的資料列、解碼後的查詢結果和插入值都用它。預設也會把它當成 ClickHouse 欄位名稱：

```ts
const telemetry = ckTable("telemetry", {
  signalStrength: ckType.decimal({ precision: 20, scale: 5 }),
});
```

如果資料庫欄位使用不同名稱，請把實際欄位名稱當成第一個引數傳入：

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

SQL、DDL、篩選條件、排序、分組和寫入欄位清單用實際欄位名稱（`probe_id`、`signal_strength`、`created_at`）。推斷模型、預設選取結果、明確指定的投影鍵和插入值則用 schema 物件鍵（`probeId`、`signalStrength`、`createdAt`）。

每個公開的 `ckType` 建構器都支援在最外層指定實際欄位名稱。沒有額外設定的建構器接受 `name?`；有型別設定的建構器則把選用的實際欄位名稱放在前面，型別設定放進物件：

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

`aggregateFunction` 和 `simpleAggregateFunction` 有 ClickHouse 專用的呼叫格式：第一個字串是彙總函式名稱，不是欄位名稱：

```ts
ckType.aggregateFunction("sum", ckType.uint64());
ckType.aggregateFunction("quantile(0.5)", ckType.float64());
ckType.simpleAggregateFunction("sum", ckType.uint64());
```

`AggregateFunction` 接受帶字面值參數的 ClickHouse 彙總名稱，例如 `quantile(0.5)` 或 `topK(10)`。`simpleAggregateFunction` 則維持較嚴格的規則，只使用一般彙總函式名稱。

如果同時需要實際欄位名稱，請使用物件設定：

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

`nested("items", shape)` 會替外層 `Nested(...)` 欄位命名。巢狀欄位名稱直接取 `shape` 的鍵，不用內層欄位的 `configuredName`：

```ts
const sensorPackets = ckTable("sensor_packets", {
  items: ckType.nested("items", {
    componentId: ckType.string(),
    quantity: ckType.float64(),
  }),
});
```

### 欄位建構器

schema 的欄位建構器是 `ckType.*`。schema DSL 涵蓋常見的 ClickHouse 型別分類：

- 整數：`int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`
- 浮點數與 Decimal：`float32`, `float64`, `bfloat16`, `decimal`
- 純量型別：`bool`, `string`, `fixedString`, `uuid`, `ipv4`, `ipv6`
- 時間型別：`date`, `date32`, `dateTime`, `dateTime64`, `time`, `time64`
- enum 與特殊型別：`enum8`, `enum16`, `json`, `dynamic`, `qbit`
- 容器：`nullable`, `array`, `tuple`, `map`, `nested`, `variant`, `lowCardinality`
- 彙總型別：`aggregateFunction`, `simpleAggregateFunction`
- 幾何型別：`point`, `ring`, `lineString`, `multiLineString`, `polygon`, `multiPolygon`

`int64` 和 `uint64` 在 schema 驅動的讀取、寫入與推斷模型中，預設使用 TypeScript `string`；這樣 64 位元值在 ClickHouse JSON 傳輸格式和 JavaScript 執行階段之間往返時仍能保持精確。如果要使用 `bigint`，就用自己的解碼器轉換，例如 `mapWith((value) => BigInt(String(value)))`。

ClickHouse 不支援 `Nullable(Array(...))`、`Nullable(Map(...))` 或 `Nullable(Tuple(...))`。`ck-orm` 會在 schema 定義時拒絕這些資料形狀。請把 `nullable(...)` 放進複合型別中，例如 `ckType.array(ckType.nullable(ckType.string()))`。

有型別設定的建構器使用物件設定，選用的實際欄位名稱放在前面：

```ts
ckType.decimal({ precision: 20, scale: 5 });
ckType.decimal("signal_strength", { precision: 20, scale: 5 });
ckType.fixedString({ length: 8 });
ckType.dateTime64("created_at", { precision: 9, timezone: "UTC" });
ckType.qbit("embedding", ckType.float32(), { dimensions: 8 });
```

`Date` / `Date32` 寫入和 schema 驅動的述詞值，都會把 JavaScript `Date` 以 UTC 日曆日送出（`2026-06-15T23:00:00Z` 會變成 `2026-06-15`）。如果要存別的日期，請傳入 `YYYY-MM-DD` 字串；字串和數字會原樣送出。

`DateTime` / `DateTime64` 的述詞值會把 JavaScript `Date` 繫結成 UTC 時間點，所以欄位、工作階段或伺服器時區都不會改變這個時間點。字串則由 ClickHouse 按欄位時區原樣解讀。在 32 位元 `DateTime` 位置，參數內的所有 Date 都是整秒時，會繫結成 `DateTime('UTC')`；只要其中任何 Date 含毫秒，就會繫結成 `DateTime64(3, 'UTC')`。`DateTime64` 位置一定繫結 `DateTime64(3, 'UTC')`。`gte` 等純量述詞仍會以精確時間點比較。`inArray` / `notInArray` 用在 `DateTime` 欄位時，Date 必須都是整秒；含毫秒的 Date 會被 ClickHouse 拒絕（`TYPE_MISMATCH`）。這是原生行為，而且這種含毫秒的值本來就不會和整秒值相等。`insert().values()` 會依欄位精確度把每個 `Date` 轉成整數時間刻度送出。請見〈寫入〉。

### 欄位型別速查表

常見的 schema 資料形狀與對應的 TypeScript 值：

| ClickHouse 資料形狀 | Schema | TypeScript 值的資料形狀 |
| --- | --- | --- |
| enum | `ckType.enum8({ idle: 1, active: 2 })` | `"idle" | "active"`（由鍵推斷） |
| 低基數字串 | `ckType.lowCardinality(ckType.string())` | `string` |
| 可為 NULL 的 Decimal | `ckType.nullable(ckType.decimal({ precision: 18, scale: 5 }))` | `string | null` |
| 陣列 | `ckType.array(ckType.string())` | `string[]` |
| 可為 NULL 的陣列元素 | `ckType.array(ckType.nullable(ckType.string()))` | `(string | null)[]` |
| tuple | `ckType.tuple(ckType.string(), ckType.int32())` | `[string, number]` |
| map | `ckType.map(ckType.string(), ckType.string())` | `Record<string, string>` |
| 巢狀物件陣列 | `ckType.nested({ component: ckType.string(), quantity: ckType.float64() })` | `{ component: string; quantity: number }[]` |
| variant | `ckType.variant(ckType.string(), ckType.int32())` | `string | number` |
| JSON | `ckType.json<{ risk?: { score?: number } }>()` | `{ risk?: { score?: number } }` |

`ckType.map(...)` 目前只支援 `String` 鍵，並對應成 JavaScript 物件，因此無法呈現 ClickHouse `Map(K, V)` 重複鍵的邊界情況。

新增資料列時，推斷出的資料形狀與 `typeof table.$inferInsert` 相同；不過呼叫 `insert(table).values(...)` 時，有 ClickHouse 預設值或產生的運算式的欄位可以省略。產生的 `INSERT` 欄位清單一定不含 `MATERIALIZED` 和 `ALIAS` 欄位；明確傳入時會直接拒絕。

## 用戶端設定
<!-- src:26d96c74 -->

用 `clickhouseClient()` 建立用戶端：

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:<password>@127.0.0.1:8123/telemetry_lab",
});
```

### 連線模式

`clickhouseClient()` 支援兩種互斥的連線方式。

#### `databaseUrl`

如果想用單一連線字串，可以使用 `databaseUrl`：

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:secret@127.0.0.1:8123/telemetry_lab",
});
```

設定了 `databaseUrl` 後，不要再傳入以下欄位：

- `host`
- `database`
- `username`
- `password`
- `pathname`

#### 結構化連線欄位

如果想分別設定各個部分，請使用明確欄位：

```ts
const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  username: "default",
  password: "<password>",
});
```

結構化模式的預設值：

- `host`：`http://localhost:8123`
- `database`：`default`
- `username`：`default`
- `password`：`""`

### 常用欄位

多數專案只需要其中幾個用戶端欄位：

| 欄位 | 用途 |
| --- | --- |
| `request_timeout` | 選用；單次請求的實際時間上限，單位是毫秒。省略時，ck-orm 不會在用戶端強制任何逾時；請求生命週期由底層 fetch、平台、伺服器或 `abort_signal` 控制。長時間執行的串流查詢不要設定這個欄位，改用 `abort_signal` 加上實際時間預算（例如 `AbortSignal.timeout(30_000)`）。 |
| `clickhouse_settings` | ClickHouse 工作階段／查詢設定的預設值 |
| `application` | 設定 ClickHouse 應用程式名稱 |

資料表定義與連線設定彼此獨立。用 `ckTable(...)` 定義資料表，在組合查詢的地方匯入這些資料表物件，再直接傳給 `.from(...)`、`.insert(...)` 和暫存資料表輔助工具。

### 進階欄位

這些欄位用來控制進階執行階段行為：

| 欄位 | 用途 |
| --- | --- |
| `fetch` | 每個 HTTP 請求都會呼叫 `(request: Request) => Response \| Promise<Response>`。可以傳入任何 fetch 實作、伺服器處理函式，或像 [`examples/decimal-sum/main.test.ts`](../examples/decimal-sum/main.test.ts) 中的測試替身。預設使用平台的 `fetch` |
| `http_headers` | 額外的預設標頭 |
| `role` | 預設的 ClickHouse 角色，可以是單一角色或多個角色 |
| `session_id` | 預設工作階段識別碼 |
| `session_max_concurrent_requests` | 這個用戶端內同一 `session_id` 最多可同時處理的請求數（預設 `1`） |
| `compression.response` | 預設 `false` 時，串流請求會送出 `enable_http_compression = 0`，讓資料列依 ClickHouse 產出的進度逐批抵達。非串流請求則保留伺服器預設值（ClickHouse 26.3 以上版本在用戶端接受壓縮時會壓縮回應）。`true` 會壓縮每個回應；遇到稀疏串流時，可能要等壓縮區塊填滿才會送出來。 |
| `logger` / `logLevel` | 記錄器整合 |
| `plugins` | 查詢生命週期外掛程式，例如來自 `ck-orm/otel` 的 `otelTracing()` |

工作階段生命週期控制刻意限定在請求範圍。把 `session_timeout` 傳給單一查詢或 `runInSession(...)`。要延續既有 `session_id` 時才使用 `session_check`；建立全新工作階段時不要用。
`session_max_concurrent_requests` 的作用不同：它是用戶端層級的保護，會限制目標為同一 `session_id` 的重疊請求。
ClickHouse 的實際工作階段仍由伺服器鎖定，所以把 `session_max_concurrent_requests` 調到 `1` 以上，可能只會遇到 `SESSION_IS_LOCKED`，並不會得到真正的工作階段內並行。
除非你有意移除本機端的序列化，而且準備好處理伺服器端工作階段鎖定失敗，否則請保留預設值 `1`。

### ClickHouse 設定

`clickhouse_settings` 只用來放 ClickHouse 的工作階段／查詢設定，也就是 ClickHouse 的[工作階段設定](https://clickhouse.com/docs/operations/settings/settings)列出的那種設定鍵；HTTP API 會把它們當成查詢參數接受。它與 ck-orm 的 `host`、`database`、`request_timeout`、`http_headers` 和 `session_max_concurrent_requests` 等用戶端設定分開處理。

請把 `query`、`database`、`session_id`、`role` 和 `param_*` 等 HTTP 傳輸欄位留在 `clickhouse_settings` 外面；ck-orm 會另外驗證這些設定鍵，因為它們屬於請求封裝和具名參數通道。

官方設定鍵有 TypeScript 自動補全；其他設定鍵仍然有效，可用於較新的 ClickHouse 版本或部署專屬設定：

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

JSON 的解析與字串化由 fetch 傳輸層負責。公開的用戶端設定刻意讓這項行為在不同執行環境保持一致，不會另外提供 `json` 覆寫選項。

ck-orm 會在每個請求強制套用一組固定的傳輸契約設定：`http_write_exception_in_output_format = 0`、`output_format_json_quote_64bit_integers = 1`、`output_format_json_quote_decimals = 1` 和 `date_time_output_format = "iso"`。如果呼叫端設定與這些值衝突，ck-orm 只會記錄一次警告，並送出強制值；`max_threads` 等工作負載與效能設定仍由呼叫端控制。

### 身分驗證

`ck-orm` 資料庫連線使用基本驗證。

- 在 `databaseUrl` 模式，可以把認證資訊內嵌在網址中
- 在結構化模式，使用 `username` 和 `password`
- 如果沒有提供認證資訊，預設使用者是 `default`，密碼為空字串

## 查詢建構器
<!-- src:fc28245d -->

下面的程式碼片段假設 `db`、`probeTelemetry`，以及片段中用到的輔助工具，都已從 `ck-orm` 匯入。

### `select()`

明確指定選取項目，回傳的資料形狀也會跟著確定：

```ts
const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
    signalStrength: probeTelemetry.signalStrength,
  })
  .from(probeTelemetry)
  .limit(10);
```

投影物件由公開的 `Selection` 值或欄位組成。實際上，資料表欄位、`fn.*(...)` 與 `ck.expr(...)` 的輸出，在 `select({ ... })` 裡都用同樣方式組合。

沒有聯結時，隱含選取會回傳完整的資料表模型：

```ts
const rows = await db.select().from(probeTelemetry).limit(10);
```

有聯結時，隱含選取會依來源分組欄位，並回傳巢狀物件。左聯結後沒有配對到的群組會變成 `null`。ClickHouse 對 `Array`／`Map` 欄位會回傳 `[]`／`{}` 而不是 NULL，所以空容器在這個檢查中也算缺失。有配對到的資料列如果純量欄位全是 NULL，而且陣列和 map 都是空的，也無法與此區分，同樣會變成 `null`。

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

`asofJoin()` 和 `asofLeftJoin()` 會渲染 ClickHouse 的 `ASOF JOIN` 與 `LEFT ASOF JOIN`。對左側的每一資料列，它們只聯結右側值最接近的那一筆資料列，而不是每一筆符合的結果。條件用 `ck.and(...)` 組合一個或多個相等條件，以及左右欄位之間恰好一個 `ck.gt`、`ck.gte`、`ck.lt` 或 `ck.lte`：

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

- `ck.gte(left, right)` 選取不晚於左側值的最新右側資料列，`ck.gt` 選取嚴格早於左側值的最新一筆資料列。`ck.lte` 和 `ck.lt` 分別選取不早於、或嚴格晚於左側值的最早右側資料列。
- `asofLeftJoin()` 遵循 [`leftJoin()` 的 NULL 語意](#join-null-semantics)：它會送出 `join_use_nulls = 1`，並把右側型別標成可為 NULL，所以沒有符合項目的左側資料列會得到 `null`。`asofJoin()` 會丟掉這些資料列，和 `innerJoin()` 一樣。
- 用來找最接近值的欄位必須是整數、浮點數、`Date`、`DateTime` 或 `Decimal`，比較的兩邊都必須是欄位；和值比較不算。可以再加上相等過濾，例如 `ck.eq(quotes.symbol, "BTCUSD")`。
- ck-orm 不檢查這個條件。沒有不等比較、出現第二個不等比較，或寫了 `ck.or(...)` 時，ClickHouse 會在讀取任何資料之前拒絕；錯誤是 `ClickHouseORMError`，`kind` 為 `"request_failed"`，`clickhouseName` 為 `"INVALID_JOIN_ON_EXPRESSION"`。沒有相等條件，以及最接近匹配欄位是其他型別，同樣會被拒絕；缺少相等條件時的錯誤名稱在不同 ClickHouse 版本之間不一樣。
- ASOF 聯結使用預設的 `join_algorithm`，也可以用 `hash`、`parallel_hash` 或 `full_sorting_merge`。`partial_merge`、`grace_hash` 和 `direct` 會以 `NOT_IMPLEMENTED` 失敗。

### `where()` 與條件輔助工具

公開的條件輔助工具：

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

`.where(...predicates)` 是接受可變參數的 `AND` 入口。它會略過 `undefined` 述詞物件，所以你可以直接傳入多個述詞，也可以用 `ck.and(...)` 和 `ck.or(...)` 建立群組述詞。

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

值清單會以單一 `Array(T)` 查詢參數送出（`col in {p:Array(T)}`）。清單文字長度超過約 40,000 個字元時，會拆成多個參數，再用 `arrayConcat(...)` 串接。主鍵與跳過索引的裁剪不受影響。ClickHouse 26.3 及以後版本，每個 HTTP 請求最多接受 1,000 個查詢參數；25.8 版的分析器會拒絕合併後超過約 1 MB 的清單。清單更大時，請改用暫存資料表或外部資料。清單若包含 SQL 運算式，或包含混合型別的無型別值，仍然是一個元素使用一個參數。

`ck.and(...)` 會略過 `undefined`，所以動態篩選條件可以直接組在程式碼裡：

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

`undefined` 只會在述詞層級略過。述詞的值會嚴格驗證：`ck.eq(column, undefined)` 會拋出錯誤，不會默默把查詢放寬。直接把 `null`、`false`、`true`、`0` 或 `""` 當成述詞都行不通；請改用 SQL 運算式。在 Nullable 欄位和寫入資料時，`null` 仍是有效的資料值，但 NULL 篩選必須明確指定：

```ts
db.select()
  .from(probeTelemetry)
  .where(ck.isNull(probeTelemetry.deletedAt));
```

布林欄位可以當一般值用，也可以直接當 SQL 運算式用：

```ts
const healthChecks = ckTable("health_checks", {
  id: ckType.int32(),
  isPassing: ckType.bool("is_passing"),
});

db.select().from(healthChecks).where(ck.eq(healthChecks.isPassing, false));
db.select().from(healthChecks).where(healthChecks.isPassing);
```

不要把單獨的 `null` 放進一般比較或 `IN` 清單。請明確組出 NULL 邏輯：

```ts
ck.or(
  ck.inArray(probeTelemetry.deletedAt, [new Date("2026-04-21T00:00:00.000Z")]),
  ck.isNull(probeTelemetry.deletedAt),
);
```

如果要在執行階段組出較大的篩選條件，建議使用 `Predicate[]` 搭配接受可變參數的 `.where(...predicates)`：

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

可重複使用的布林 SQL 子句，在公開 API 中叫 `Predicate`。同一個述詞物件可以用在 `where`、`having`、聯結的 `on` 子句，以及 `ck.exists(...)` 這類會接收布林條件的輔助工具中。

可重複使用的建構器計算值，在公開 API 中稱為 `Selection`，例如 `fn.sum(...)`、`fn.toString(...)` 和 `ck.expr(ckSql...)`。用 `.as(...)` 為它們建立別名，用 `.mapWith(...)` 覆寫解碼方式。`Order` 是 `ck.asc(...)` 和 `ck.desc(...)` 回傳的子句物件。

`ck.has(...)`、`ck.hasAll(...)` 和 `ck.hasAny(...)` 會直接對應 ClickHouse 原生函式，保留 ClickHouse 的陣列、map 和 JSON 語意。左側是陣列欄位時，一般 JavaScript 值會先透過該欄位編碼，所以 `Array(Date)` 的篩選條件會先經過欄位的 Date 編碼，再成為查詢參數。

`where(...)` 接受可變參數，`having(...)` 則只接受單一述詞。要在 `having` 放多個條件時，請先用 `ck.and(...)` 或 `ck.or(...)` 把述詞組合起來。

`ck.contains(...)`、`ck.startsWith(...)`、`ck.endsWith(...)` 和它們的 `*IgnoreCase` 變體會把輸入當成字面文字處理。它們會將值參數化，並在內部跳脫 LIKE 萬用字元（`%`、`_`、`\`）。

只有真的需要完整的模式比對語意時，才使用 `ck.like(...)`／`ck.ilike(...)`。這些 API 仍會將值參數化，確保 SQL 安全；但 `%` 和 `_` 會保留萬用字元的意義，因為 LIKE 是模式語言。

字面文字搜尋範例：

```ts
import { ck } from "ck-orm";

const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
  })
  .from(probeTelemetry)
  .where(ck.contains(probeTelemetry.probeId, "probe_alpha%"));
```

進階模式範例：

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

`groupBy()` 和 `limitBy([...])` 接受欄位，也接受 `fn.*(...)`、`ck.expr(...)` 等輔助工具計算出來的 `Selection` 值。

`limit()`、`offset()` 和 `limitBy(..., limit)` 傳入的原始值必須是非負的安全整數。只有在你刻意需要 ClickHouse 常數運算式時，才用 `ckSql`。

`orderBy()` 接受：

- `ck.desc(selection)`
- `ck.asc(selection)`
- 直接傳入欄位

### `final()`

在資料表查詢加上資料表層級的 `FINAL`：

```ts
const query = db.select().from(probeTelemetry).final();
```

如果只是讀取沒有別名的資料表，`ck-orm` 會產生 `FROM table FINAL`。根資料表有別名，或查詢聯結其他來源時，`ck-orm` 會把已加上 FINAL 的資料表包進子查詢，並讓外層來源沿用這個別名。這樣能避開 ClickHouse 分析器處理 `FINAL`、資料表別名、聯結和 lambda 運算式時的邊界案例，建構器 API 也保持不變。

`final()` 只適用於以資料表為根的來源。如果 CTE、子查詢或資料表函式流程內部需要 `FINAL`，請先把 `.final()` 加在以資料表為來源的查詢上，再呼叫 `.as(...)`。

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

### 執行模式

建構器查詢可以用三種方式執行：

```ts
const query = db.select().from(probeTelemetry).limit(10);

const rows = await query;
const sameRows = await query.execute();

for await (const row of query.iterator()) {
  console.log(row);
}
```

在應用程式範例中，如果想讓執行點一眼就看得出來，就使用 `.execute()`。為了保留 Drizzle 風格的操作體驗，也支援直接 `await query`；等團隊熟悉建構器查詢具有 then 方法後，這種寫法就很方便。

`query.iterator()` 遵循與 `db.stream()` 相同的工作階段並行規則：查詢若指定 `session_id`，這個並行槽位會一直被佔用，直到迭代完成或迭代器提前關閉。

### 用 `toSQL()` 檢視 SQL

`toSQL()` 只編譯建構器，不會送出任何東西。請直接對建構器呼叫它，因為 await 建構器就會執行查詢。`select()` 建構器、`insert().values()` 和 `insert().fromSelect()` 都可以使用。

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

回傳的結果有這幾個欄位：

- `sql` 是壓成一行的陳述式，每個值的位置都換成 `{orm_param1:String}` 這類預留位置。裡面不含 `FORMAT` 子句，這個子句是查詢送出時才加上的。
- `params` 存放這些預留位置對應的原始 JavaScript 值。查詢執行時，這些值會經過編碼，與 SQL 分開送出，所以值絕不會內嵌到 `sql` 裡。
- `forcedSettings` 列出建構器強制加在請求上的設定，例如 `leftJoin()` 會加上 `join_use_nulls: 1`。沒有強制設定時，不會有這個欄位。
- `body` 只有 `insert().values()` 才有：它是以 Values 文字表示的資料列，而 `sql` 裡是 `INSERT INTO t (cols) FORMAT Values` 陳述式。其他情況不會有這個欄位。

回傳的是一份複本，所以改動它不會影響建構器。

要記錄或除錯時，可以傳入 `{ pretty: true }`，把 `sql` 和 `body` 排成多行：

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

美化輸出只會改變空白。每個子句各自另起一行，`select` 的項目各佔一行，子查詢和 CTE 會縮排，`and` 或 `or` 群組裡的條件也各佔一行。函式的參數維持在同一行，加了引號的文字絕不會更動，含有 SQL 註解的陳述式則會原樣回傳。`insert().values()` 的主體裡，每個資料列各佔一行。美化輸出不會影響實際送出的查詢。

### 子查詢與 CTE

用 `.as("alias")` 把建構器轉成有明確名稱的子查詢：

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

`.as("…")` 對子查詢是**選用**的；不呼叫它時，框架會在編譯時自動指派別名(`__sub_1`、`__sub_2`……)。
未包裝的建構器可以直接用在 `from()`、`innerJoin()`、`leftJoin()`、
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

#### `.as(name)` 與未包裝形式怎麼選

| 情境 | 建議寫法 |
|---|---|
| 一次性的行內子查詢，不需要重複使用 | 未包裝的建構器，搭配回呼函式形式的 `innerJoin/leftJoin`；或直接在 `inArray/exists` 中使用未包裝形式 |
| 子查詢的欄位參照在多處使用 (SELECT / WHERE / 單一 JOIN) | 指定給 `const`，再以未包裝形式傳入；欄位參照會沿著鏈式方法傳遞 |
| 自我聯結 (同一個邏輯子查詢，但使用兩個 SQL 別名) | 明確呼叫 `.as("a")` 和 `.as("b")`；同一個實例若被當成來源使用兩次，查詢編譯成 SQL 時會拋出錯誤 |
| 需要穩定、易讀或有型別的別名 (正式環境 `query_log` 搜尋、`EXPLAIN`、慢查詢記錄、跨模組輔助工具、字面值 `TAlias` 來源鍵) | 呼叫 `.as("descriptive_name")` |

自動別名在不同結構的查詢之間**不會**保持一致；但在同一次編譯內，
以及同一建構器樹重複編譯時會保持一致，所以 `snapshot` 風格測試不需要正規化。

#### 選取鍵衝突

如果選取鍵與 SelectBuilder 的方法名稱(`from`、`where`、`select`、`as`、
`innerJoin`、`leftJoin`、`asofJoin`、`asofLeftJoin`、`groupBy`、`having`、`orderBy`、`limit`、`offset`、
`final`、`limitBy`、`execute`、`iterator`、`then`、`catch`、`finally`、
`buildSelectionItems`)撞名，它們在未包裝的建構器上會保留方法型別。
`sub.from` 一定回傳建構器方法，絕不會回傳欄位參照，即使 `from` 出現在選取中也一樣。
請重新命名這類選取鍵 (例如把 `from` 改成 `fromAddress`)，或用 `.as("name")` 包裝子查詢，
讓欄位改由專屬的 `Subquery` 物件存取。

建立 CTE 時使用 `$with()` 和 `with()`：

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

不指定名稱的 `$with()` 會建立匿名 CTE。匿名 CTE 在同一份查詢中只能當成來源使用一次；如果要兩次聯結同一個結果，請定義兩個 CTE，每次聯結各用一個 (呼叫兩次 `db.$with().as(...)`，或建立兩個不同名稱的具名 CTE)。

### `db.count()`

使用 `db.count(source, ...predicates)` 這個 Drizzle 風格的計數輔助工具。它的述詞語意與 `.where(...predicates)` 相同：多個述詞會用 `AND` 組合，`undefined` 述詞物件會略過。`ck.eq(...)` 等輔助工具內部的值若是 `undefined`，仍會拋出錯誤。

```ts
import { ck } from "ck-orm";

const total = await db.count(
  probeTelemetry,
  ck.eq(probeTelemetry.status, 1),
  ck.gt(probeTelemetry.id, 1000),
);
```

如果結果集更複雜，就對子查詢或 CTE 計數：

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

`db.count(...)` 預設使用數值計數模式：會產生 `toFloat64(count())`，並解碼成 `number`。極大的計數可能超出 JavaScript 整數可精確表示的範圍，所以需要精確值或保留傳輸資料形狀時，請使用可鏈式模式：

```ts
const approximateTotal = await db.count(activeProbes); // number
const exactTotal = await db.count(activeProbes).toSafe(); // string
const wireTotal = await db.count(activeProbes).toMixed(); // number | string
```

`.toSafe()` 會產生 `toString(count())`，適合需要精確讀取的場合。如果把安全計數模式的結果當成 SQL 運算式使用，它會有 `String` 語意；SQL 數值比較請使用預設模式、`.toUnsafe()` 或 `.toMixed()`。`.toMixed()` 會產生 `toUInt64(count())`，並保留驅動程式／傳輸資料形狀；搭配 ck-orm 預設不失真的 64 位元 JSON 設定，實際的 ClickHouse 回應通常會以 `string` 傳來。

## 聯結的 NULL 語意
<!-- src:59e7a4e2 -->

`leftJoin()` 和 `asofLeftJoin()` 預設採用 SQL 風格的 NULL 語意，會自動套用 `join_use_nulls = 1`。

也就是說：

- 預設左聯結的右側會推斷為可為 NULL
- 如果明確停用 `join_use_nulls`，推斷出的型別也會跟著改變

如果要符合 ClickHouse 預設的聯結行為，請傳入字面值 `0`。這個字面值會自動推斷型別，所以不需要 `as const`。若值的型別只是 `number`，例如來自變數，結果仍會加上 `| null`。

```ts
const rawDefaultDb = db.withSettings({
  join_use_nulls: 0,
});
```

左聯結未包裝的建構器（沒有 `.as()` 的子查詢）或匿名 CTE 時，這些來源的每個欄位參照都會標成 `| null`，包括 `FROM` 側或內聯結未包裝建構器的欄位參照。這些參照共用同一個寬鬆的 `string` 來源鍵，所以型別無法分辨哪個來源被左聯結。執行階段仍只會在左聯結的那一側回傳 NULL。

```ts
const rows = await db
  .select({ uid: allUsers.uid, spent: totals.spent })
  .from(allUsers)
  .leftJoin(totals, ck.eq(allUsers.uid, totals.uid));
// uid and spent are both number | null
```

請用 `.as("name")` 為 `FROM` 側或內聯結的子查詢命名。這樣它的欄位參照會保留自己的來源鍵並維持非 NULL；左聯結的來源仍是 `| null`。

聯結查詢再次用作子查詢、CTE、`ck.exists(...)` 或 `ck.inArray(...)` 的來源時，會保留強制設定 `join_use_nulls = 1`，因此建構器型別會持續符合執行階段行為。

計算出的運算式在執行階段遵循 ClickHouse 的 NULL 傳播：ClickHouse 回傳 NULL（輸入是 NULL，或左聯結找不到配對資料列）時，值就是 `null`。它們的 TypeScript 型別是你宣告的 `TData`（或輔助工具固定的型別），不會自動擴寬；所以輸入可能為 NULL 時，請宣告 `| null`（例如 `fn.plus<number | null>(...)`），或用 `fn.coalesce(...)` 包裝輸入。

## 寫入
<!-- src:5d78750d -->

### `insert(table).values(...)`

如果要依資料表 schema 插入有型別的資料，請使用建構器：

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

所有資料列會用單一 `INSERT INTO t (cols) FORMAT Values` 請求送出。這個陳述式沒有查詢參數；資料列是請求主體中的 Values 字面值文字，因此不受 HTTP 的 1,000 個參數上限，以及每個值 128 KiB 上限的限制。省略的欄位只會在該資料列套用自己的 `DEFAULT`。這筆插入會停用運算式求值（`input_format_values_interpret_expressions = 0` 和 `input_format_values_deduce_templates_of_expressions = 0`），所以每個儲存格都只會當成資料。需要計算值時，請使用 `insert().fromSelect()`。JavaScript `Date` 會以確切時點送出，並截斷到欄位精確度；`DateTime` 欄位保留整秒。`DateTime` 或 `DateTime64` 欄位中的數字會解讀成 Unix 秒。字串會依欄位時區解讀。

插入資料列的鍵必須來自資料表 schema；ck-orm 會在送出前就拒絕未知的鍵。

### `insert(table).fromSelect(select)`

用 `.fromSelect(...)` 編譯單一 `INSERT INTO target (cols) SELECT ...` 陳述式，讓 ClickHouse 在伺服器端實體化 SELECT。資料列不會經過 Node 程序；很適合先把大型來源資料表需要的部分篩出來一次，再在 `runInSession` 內讓多個後續查詢重用這份已實體化的資料。

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

規則：

- SELECT 必須在投影中包含目標資料表的每個必要欄位；用 `.default(...)` 宣告的欄位可以省略。缺少必要欄位時，編譯階段（TypeScript）會先拒絕，執行階段還會再擋一次。
- INSERT 欄位清單會依 SELECT 投影的鍵順序產生，所以即使 ClickHouse 按位置對齊欄位，呼叫端看到的仍是 `name-aligned` 語意。投影鍵的順序不會影響哪個目標欄位收到哪個值；實際比對的是鍵本身。
- 未知的投影鍵（不在目標資料表上）或欄位型別不相符時，查詢送出前就會出現 TypeScript 錯誤。
- 同一條插入鏈上，`.values()` 與 `.fromSelect()` 互斥。建構器型別會收窄，讓未使用的方法不出現在自動完成中，執行階段也會拒絕呼叫。
- 編譯後的傳輸請求就是一般的 ClickHouse 指令；查詢事件會帶有 `mode: "insert"`，並把 `tableName` 設為目標資料表。
- `.fromSelect()` 接受 `SelectBuilder`（`session.select({...}).from(...)`），不接受子查詢包裝。只有需要把結果當成另一個 SELECT 內的 CTE／子查詢時才使用 `.as("name")`；交給 `.fromSelect()` 時不要使用。

#### nested 欄位

`ckType.nested(...)` 欄位與 `.fromSelect()` 搭配時有三種模式：

1. **在投影中省略 nested 鍵。** 這是預設模式，也符合 ck-orm 的執行階段契約：缺少 nested 值時會編碼成 SQL `DEFAULT`，ClickHouse 會為每個欄位填入空的平行陣列。TS 層會把 `$inferInsert` 中的 nested 欄位視為 `optional`，以反映這個行為。
2. **投影 nested 欄位的直接參照**，例如 `select({ events: src.events })`。ck-orm 會把內部 SELECT 包進子查詢，並將外層投影改寫成逐欄位的點路徑存取（`__ck_inner.events.name`、`__ck_inner.events.score`、……）。來源的 nested 資料形狀必須包含目標 nested 欄位要求的每個欄位。
3. **把計算運算式投影到 nested 欄位**，編譯階段會直接拒絕。單一 SQL 運算式無法展開成平行陣列欄位。要以資料列值插入時，請使用 `.values(...)` 或 `.insertJsonEachRow(...)`。

如果想讓 TS 層強制一定要提供 nested 資料（`Pattern D`，也就是不允許空 nested 陣列的業務契約），請啟用 `ckType.nested({...}).requiredOnInsert()`。這條鏈會回傳一個欄位，其 `$inferInsert` 資料形狀會把 `events` 列為必要；`.fromSelect()` 必須使用 nested 欄位參照投影模式，或改用逐列傳入值的方式。

> **JSON 傳輸格式注意事項**：`.values()` 會把 JSON 欄位轉成加引號的 `JSON.stringify(...)` 文字，放進 Values 主體。ClickHouse 會把這個字面值解析後存入 JSON 欄位。`.fromSelect()` 完全在伺服器端執行，所以 JSON 值不會在 JS 程序中往返。兩條路徑儲存的資料完全相同。

### `insertJsonEachRow()`

如果已經有物件資料列或非同步資料列串流，請使用 `insertJsonEachRow()`：

```ts
await db.insertJsonEachRow("tmp_scope", [
  { probe_id: "probe_alpha" },
  { probe_id: "probe_beta" },
]);
```

它接受：

- 字串形式的資料表名稱
- 由 `ckTable()` 建立的資料表物件
- 一般陣列
- `AsyncIterable`

空的一般陣列會在用戶端直接略過，但仍會以 `rowCount: 0` 向外掛程式 hook 回報成功的插入生命週期。未知欄位的行為由 ClickHouse 控制；如果要讓伺服器忽略多餘的 JSON 欄位，請傳入 `input_format_skip_unknown_fields: 1` 這類設定。nested 欄位會以攤平的 `column.field` 陣列送出（ClickHouse 預設 `flatten_nested = 1`），所以 `{ lines: [{ sku: "a" }] }` 在 `insert().values()` 與 `insertJsonEachRow()` 都能使用。

有資料表 schema 時，`insertJsonEachRow()` 會把 `DateTime`/`DateTime64` 欄位中的 JavaScript `Date` 送成與 `insert().values()` 完全相同的整數 tick 值，字串則仍交由 ClickHouse 預設的 `date_time_input_format = basic` 解析器處理，因此兩者讀取日期時間字串的方式相同。你自己傳到 `DateTime64` 欄位的 JSON 數字，在 `JSONEachRow` 仍會當成 tick 讀取，在 `.values()` 則會當成 Unix 秒讀取。

`insertJsonEachRow("table_name", rows)` 只傳入資料表名稱時沒有欄位型別，因此 `Date` 會以帶 `Z` 的 ISO 8601 送出，ck-orm 也會為該請求設定 `date_time_input_format = best_effort`。透過 schema 資料表物件傳入時，資料列中不在 schema 裡的鍵也適用相同規則：ck-orm 會原封不動地傳遞它們，所以其中的 `Date` 在呼叫時需要加上 `clickhouse_settings: { date_time_input_format: "best_effort" }`。在 ClickHouse 25.8 上，`best_effort` 解析器會誤讀帶秒以下小數位的 1970 年以前時點，也會拒絕 `12:34:56` 這類 `Time` 字串；使用帶 schema 型別資訊的欄位可以避開這兩個問題。

## 原始 SQL
<!-- src:f47b381e -->

`ck-orm` 有自己的 SQL 範本 API。如果建構器寫法反而不如你本來就想寫的 SQL 直接，就用它。

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

### `ckSql.join()` 與 `ckSql.identifier()`

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

### 使用 `query_params` 的原始 SQL

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

ck-orm 會自動選擇參數的傳輸方式，不需要為 `query_params` 設定 multipart 處理。

ck-orm 會拒絕以 `orm_param` 開頭的 `query_params` 鍵。這個前綴保留給 `` ckSql`...` `` 內部產生的參數。

值格式器支援基本型別值、`Date`、`NaN`、`Infinity`、陣列、物件和 `Map` 值，可以套用到 ClickHouse 的型別化預留位置，例如 `{ids:Array(UInt64)}` 或 `{attrs:Map(String, String)}`。透過 `query_params` 傳入的值本身不帶欄位型別，所以陣列一律以 Array 文字送出。這裡不支援 `Tuple(...)` 預留位置。陣列中的 64 位元整數必須是 `bigint`；字串會加上引號；元素型別為數值時，加了引號的字串會被拒絕。`Date` 會輸出成 UTC 牆鐘時間文字，ClickHouse 會依預留位置型別的時區解讀；因此請宣告成 `{at:DateTime64(3, 'UTC')}`，避免工作階段或伺服器時區造成偏移；`` ckSql`...${date}` `` 的插值會自動幫你處理。參數是資料表或欄位名稱時，請使用 ClickHouse 的 `Identifier` 預留位置型別：

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

用 `ck.expr()` 把 SQL 片段包裝成可重用的 `Selection`：

```ts
import { ck, ckSql } from "ck-orm";

const query = db.select({
  constantOne: ck.expr(ckSql`1`).as("constant_one"),
});
```

片段有型別時，`ck.expr()` 會保留該型別。無型別的片段會刻意保持 `unknown`；如果 schema 之外已經知道結果型別，就傳入泛型或解碼器：

```ts
const status = ck.expr(ckSql<string>`'closed'`); // Selection<string>
const enabled = ck.expr<boolean>(ckSql`1`, {
  decoder: (value) => Number(value) === 1,
});
```

### 原始查詢格式

原始的非串流查詢只支援 `JSON` 輸出：

```ts
const rows = await db.execute(ckSql`select 1`, {
  format: "JSON",
});
```

原始的串流查詢只支援 `JSONEachRow` 輸出。預設 `compression.response: false` 時，串流會以未壓縮方式送出，資料列就能隨 ClickHouse 產出即時抵達：

```ts
for await (const row of db.stream(ckSql`select 1`, {
  format: "JSONEachRow",
})) {
  console.log(row);
}
```

### 運算式中的 Decimal 精確度

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

- `sum` / `sumIf` 會把 P 擴大到 ≥ 38；`min` / `max` 保留欄位的 P。schema 中 Decimal 欄位的 `fn.sum()` 與 `fn.sumIf()` 是 `Selection<string>`；Float 欄位是 `Selection<number>`。自動轉型也會在 `nullable(decimal(...))` 與 `lowCardinality(decimal(...))` 內生效。
- `maxIf(value, condition)` 與 `max` 遵循相同的 Decimal 規則：Decimal 結果是 `Selection<string>`，並保留宣告的 `Decimal(P, S)` 資料形狀。
- `avg` **不會自動轉型**：ClickHouse 會在 Float64 上計算 `avg(Decimal)`，所以 `fn.avg` 回傳 `Selection<number>`。需要精確的 Decimal 平均值時，請使用 `ckSql.decimal(ckSql\`sum(x) / count(x)\`, P, S)`。
- `fn.divideDecimal(left, right, resultScale?)` 直接對應 ClickHouse 的 `divideDecimal`。它回傳 `Selection<string>`，所以不會失去精確度。提供 `resultScale` 時，必須是 `0` 到 `76` 的整數字面值，並會直接內嵌輸出；建構器不會檢查版本，也不會模擬 ClickHouse 函式。
- `column.cast(P, S)` 轉型的是欄位，不是彙總；在 `GROUP BY` 內直接使用會產生 `NOT_AN_AGGREGATE`。請使用 `fn.sum(column)`，或包裝整個彙總。
- 插入會拒絕不是字串或數值的物件（例如未轉換的 `decimal.js` 實例）；請傳入 `.toFixed(scale)`：

```ts
db.insert(measurementLog).values({ value: new Decimal("1.23").toFixed(5) }); // ✅
db.insert(measurementLog).values({ value: new Decimal("1.23") as never }); // ❌ throws
```

## 視窗運算式
<!-- src:6d0b3f76 -->

用 `fn.over(...)` 包裝 `fn.*` 函式運算式。選用的設定物件中，`partitionBy` 只接受建構器選取，`orderBy` 只接受一般的 `ck.asc(...)` / `ck.desc(...)` 值：

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

`fn.rowNumber()` 預設透過 `toFloat64` 產生不安全的 `Selection<number>`。若要精確的 `Selection<string>`，請在 `fn.over(...)` 前呼叫 `.toSafe()`；若要原生 `UInt64` 傳輸資料形狀（`Selection<number | string>`），請呼叫 `.toMixed()`；`.toUnsafe()` 是明確的預設值。建構器刻意不加入 JavaScript 安全整數檢查：ClickHouse 的 `toFloat64` 轉換可能把高於 `Number.MAX_SAFE_INTEGER` 的值四捨五入；`.toSafe()` 會保留精確的十進位字串，`.toMixed()` 會保留驅動程式的原生結果資料形狀。Decimal 彙總會保留原本的字串解碼器，並編譯成 `CAST(sum(...) OVER (...) AS Decimal(...))`，所以 `OVER` 會留在彙總內，而不是放在轉型外側。`fn.over` 刻意不接受未包裝的欄位或原始 SQL 片段；第一版介面只涵蓋 `PARTITION BY` 與 `ORDER BY`。視窗框架、具名視窗或其他 ClickHouse 專用子句請使用 `ckSql`。

## 函式與資料表函式
<!-- src:1e16409b -->

### `fn`

泛用、轉換、彙總、JSON、tuple 與資料表相關的輔助工具包括：

- `fn.call()`
- `fn.withParams()`
- `fn.replaceRegexpAll()`：有型別的 ClickHouse RE2 取代；請參閱 [正規表示式取代](#regex-replacement)。
- 型別轉換輔助工具對應 ClickHouse 的型別轉換頁面：`fn.cast()`、`fn.date()`、`fn.accurateCast*()`、`fn.reinterpret*()`、`fn.parseDateTime*()`、`fn.formatDateTime()`、`fn.formatRow*()`、`fn.toString*()`、`fn.toBool()`、`fn.toInt*()`、`fn.toUInt*()`、`fn.toFloat*()`、`fn.toBFloat16*()`、`fn.toDecimal*()`、`fn.toDecimalString()`、`fn.toFixedString()`、`fn.toDate*()`、`fn.toDateTime*()`、`fn.toTime*()`、`fn.toTime64*()`、`fn.toInterval*()`、`fn.toLowCardinality()`、`fn.toNullable()` 和 `fn.toUUID*()`。
- 64 位元以上的整數轉換（`toInt64/128/256*`、`toUInt64/128/256*`、Unix 時間戳記 64 位元輔助工具）會解碼成 `string`；位元數較少的整數和浮點數轉換則解碼成 `number`。
- 只能接受字面值的 ClickHouse 參數會在需要時驗證並直接內嵌：Decimal 的小數位數、DateTime64/Time64 的精確度、FixedString 的長度、目標型別字串和間隔單位。
- `fn.toStartOfMonth()`
- `fn.toUnixTimestamp()` / `fn.toUnixTimestamp64Second()` / `fn.toUnixTimestamp64Milli()` / `fn.toUnixTimestamp64Micro()` / `fn.toUnixTimestamp64Nano()`
- `fn.fromUnixTimestamp()` / `fn.fromUnixTimestamp64Second()` / `fn.fromUnixTimestamp64Milli()` / `fn.fromUnixTimestamp64Micro()` / `fn.fromUnixTimestamp64Nano()`
- `fn.count()` / `fn.countIf()`：預設的 `Selection<number>` 會包成 `toFloat64(count(...))`。如果要 `Selection<string>`，請串接 `.toSafe()`（`toString(count(...))`）；如果要 `Selection<number | string>`，請串接 `.toMixed()`（`toUInt64(count(...))`）；也可以使用 `.toUnsafe()` 還原預設行為。這和 `db.count` 一致。解碼器會要求非負整數，並拒絕 `NaN`、負數、布林值等結果；範例請參閱 [`fn.count` / `fn.uniqExact` 模式](#fncount--fnuniqexact-modes)。
- `fn.sum()` / `fn.sumIf()` / `fn.min()` / `fn.max()` / `fn.maxIf()`：對 Decimal 欄位自動轉型為 `Decimal(P, S)`；請參閱 [運算式中的 Decimal 精確度](#decimal-precision-in-expressions)。
- `fn.over()` / `fn.rowNumber()`：使用 `PARTITION BY` 和 `ORDER BY` 組合視窗函式；請參閱 [視窗運算式](#window-expressions)。
- `fn.divideDecimal()`：做精確的 Decimal 除法，也可以直接內嵌結果小數位數；請參閱 [運算式中的 Decimal 精確度](#decimal-precision-in-expressions)。
- `fn.argMax()` / `fn.argMin()`：在 `val` 最大／最小的資料列中回傳 `arg` 值。回傳型別和 `arg` 欄位一致（不包裝 Decimal CAST；ClickHouse 會保留欄位宣告的型別）。
- `fn.avg()`：`Selection<number>`（Float64），符合 ClickHouse 原生 `avg(Decimal)` 行為。
- `fn.uniqExact()`：和 `fn.count()` 一樣有三種可鏈式模式：預設的 `Selection<number>` 會包成 `toFloat64(uniqExact(...))`；使用 `.toSafe()` 可取得 `Selection<string>`（`toString(uniqExact(...))`），使用 `.toMixed()` 可取得 `Selection<number | string>`（`toUInt64(uniqExact(...))`），使用 `.toUnsafe()` 還原預設行為。解碼器採用相同的非負整數檢查。請參閱 [`fn.count` / `fn.uniqExact` 模式](#fncount--fnuniqexact-modes)。
- `fn.coalesce()`
- `fn.if()` / `fn.multiIf()`：條件式。兩者會回傳 `Selection<unknown>`，因為 ClickHouse 會根據所有值分支決定伺服器結果型別。
- `fn.greatest()` / `fn.least()` / `fn.nullIf()`：比較函式；`nullIf` 會自動將 sqlType 包裝成 `Nullable(...)`，已經是可為 NULL 的輸入則不會重複包裝。
- `fn.positionCaseInsensitive()` / `fn.positionCaseInsensitiveUTF8()`：不分大小寫／UTF-8 的子字串搜尋。會回傳 `Selection<string>`（ClickHouse 的 `UInt64` 會解碼成字串，以保留 64 位元精確度，和 `fn.indexOf()` 一致）。選用的 `startPos` 必須是 `UInt*` 型別；請傳入 UInt 欄位，如果是字面值則使用 `fn.toUInt64(n)`。直接寫數字字面值會變成 `Int64`，ClickHouse 會拒絕。
- `fn.jsonExtract()`
- `fn.tuple()`
- `fn.arrayJoin()`
- `fn.tupleElement()`
- `fn.not()`

### 條件分支型別

條件結果的泛型已移除：`fn.if<TData>(...)` 和
`fn.multiIf<TData>(...)` 不再能編譯。已知的 `Float64` 或 `UInt64` 錨點會依上下文替相容的安全整數字面值分支推導型別；`UInt64` 字面值也必須是非負數。型別不一致的運算式與原始 SQL 絕不會自動轉型，因此請明確轉換每個值分支。`.mapWith(...)`
只定義 JavaScript 解碼器，不會改變 SQL 型別。具體來說，放在
搭配 `fn.over(fn.rowNumber().toMixed())` 的非負安全整數備援值會編譯成 `UInt64`。
`fn.if(...)` 和 `fn.multiIf(...)` 是純量，不能直接傳給
`fn.over(...)`；但搭配的值分支仍然可以是視窗選取。

如果無法證明所有值分支都使用同一 SQL 型別，結果中繼資料會維持未知，解碼會直接保留傳輸值。現有結果可能因此從 `number` 變成傳輸用 `string`。要遷移時，請先將每個值分支明確轉換成同一 SQL 型別，再視需要套用 `.mapWith(...)`。
自動產生的結果中繼資料刻意只限於確切的 `Bool`、`Int64`、`UInt64`、`Float64` 和 `String` 錨點。即使分支中繼資料相符，其他型別仍會直接保留傳輸值，因為 ClickHouse 可能會正規化相同的儲存型別（例如 `LowCardinality(String)` 會變成 `String`）。

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

`fn.coalesce(...)` 不會再根據上下文把 `Float32`、`BFloat16`、`Decimal` 或目標範圍無法證明安全的 `bigint` 備援值推導成對應型別。請明確轉換這些備援值：

```ts
fn.coalesce(amount, fn.toDecimal64("0.00", 2));
```

### 正規表示式取代

`fn.replaceRegexpAll(haystack, pattern, replacement)` 會對應到 ClickHouse 的
[`replaceRegexpAll`](https://clickhouse.com/docs/sql-reference/functions/string-replace-functions#replaceregexpall)。
它會回傳 `Selection<string>`；如果輸入有型別且可能是 null，則回傳 `Selection<string | null>`。請用 `String.raw` 處理 RE2 反斜線和擷取群組的取代內容：

```ts
const usDate = fn.replaceRegexpAll(
  orders.dateText,
  String.raw`^(\d{4})-(\d{2})-(\d{2})$`,
  String.raw`\2/\3/\1`,
);
// Selection<string>
```

參數會繫結成查詢參數。模式使用 ClickHouse RE2 語法，取代內容支援 `\0`–`\9`；無效的模式仍會由 ClickHouse 回報錯誤。無型別的 `ckSql` 輸入會保守地視為可能是 null；如果確定片段不會是 null，請寫成 `ckSql<string>`。

### 有型別的 tuple 彙總

`fn.tuple()` 會保留每個項目的型別。單一值的 `groupArray` 形式會保留該 tuple 型別和解碼器，單一引數的 `fn.arrayReverseSort()` 也會保留相同的陣列項目型別：

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

現有的明確泛型寫法仍可用於原始或動態情況。

陣列輔助工具的名稱會比照 ClickHouse 的[陣列函式](https://clickhouse.com/docs/sql-reference/functions/array-functions)參考資料中的標準標題。只有別名的函式仍可透過 `fn.call(...)` 使用，不需要把公開 API 擴成兩套。

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

`fn.call(name, ...)` 和 `fn.withParams(name, ...)` 會在編譯前驗證 `name` 是否為 SQL 識別字。`ck.has(...)`、`ck.hasAll(...)`、`ck.hasAny(...)` 和 `ck.hasSubstr(...)` 是可直接放進 WHERE 的述詞簡寫，對應相同的 ClickHouse 函式。

`fn.jsonExtract(json, returnType, ...path)` 只接受 `ckType.*` 回傳型別，因此 ClickHouse 回傳型別和 TypeScript 解碼器會維持一致：

下面的 JSON 和陣列片段假設虛構的 `probeTelemetry` schema 還有 `metadata: JSON` 和 `tags: Array(String)` 欄位。

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

路徑區段是 ClickHouse 的 JSON 路徑引數。物件欄位用字串索引鍵，陣列位置用 number 或 bigint 索引：

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

高階陣列函式遵循 ClickHouse 的參數順序。lambda 要用 `ckSql`，把外部 schema 欄位插值進去；lambda 區域變數則直接寫成 SQL 名稱。對於元素資料形狀由 lambda 決定的函式（例如 `fn.arrayMap(...)`、`fn.arrayFilter(...)` 和 `fn.range(...)`），預設會回傳 `Selection<unknown[]>`；如果查詢需要更精確的解碼元素型別，請串接 `.mapWith(...)`。

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

在這個 lambda 裡，`${params.timeColumn}` 和 `params.scopeTable.startTsList` 都是來自 schema 的運算式。`start_ts` 和 `end_ts` 則是 ClickHouse 依陣列元素建立的 lambda 區域變數。

`fn.arrayJoin(array)` 對應 ClickHouse `arrayJoin`，會把一筆輸入資料列展開成每個陣列元素各一列。兩個陣列如果必須保持位置配對，請搭配 `fn.arrayZip(...)` 和 `fn.tupleElement(...)`：

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

重點是這段的語意：`arrayZip(sampleIds, probeIds)` 會保留原本的位置配對，`arrayJoin(...)` 會把它們展開成資料列，而 `tupleElement(..., 1/2)` 會讀取每個配對的兩個值。

複合索引鍵請用 tuple 做整組比對。兩個獨立的 `IN` 述詞會產生笛卡兒積，可能比對到從未要求的配對：

```ts
// Avoid this when sampleId and probeId must stay paired.
const wrong = ck.and(
  ck.inArray(probeTelemetry.sampleId, sampleIds),
  ck.inArray(probeTelemetry.probeId, probeIds),
);
```

需要保留配對語意時，請把一個 tuple 和會產生 tuple 的子查詢或配對清單比較。`fn.tuple` 在型別已知時會帶上每個元素的型別，因此清單會繫結成一個 `Array(Tuple(...))` 參數：

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  [
    ["900001", "probe_alpha"],
    ["900002", "probe_beta"],
  ],
);
```

原始 SQL 片段沒有元素型別。使用 `ck.inArray` 或 `ck.eq` 前，先用 `fn.cast(fn.tuple(...), "Tuple(Int32, String)")` 包裝 tuple。

如果右側是會產生 tuple 的子查詢，左側照樣可以直接使用，不需要參數：

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  scopedSamples,
);
```

陣列輔助工具會回傳有型別的 `Selection` 值，所以可以組合進投影、篩選、分組和聯結：

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

### `fn.count` / `fn.uniqExact` 模式

`fn.count()`、`fn.countIf(predicate)` 和 `fn.uniqExact(expression)` 是 `db.count(...)` 對應的彙總版本，三種模式也相同；當計數出現在查詢中段（`select`、`having`、GROUP BY、子查詢），而不是頂層純量時，就很適合使用它們。它們共用相同的 SQL 包裝器和解碼器，因此選擇模式時的取捨也相同：

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

解碼器只接受非負整數；布林值、`NaN`、小數和負數都會拋出 `Failed to decode count() result: ...`，因此伺服器回傳損壞的資料時會立即失敗，不會默默產生 `0`。

其他彙總函式刻意不共用這套模式 API，因為它們在精確度方面的考量和 `count` / `uniqExact` 不同：

- `fn.sum()` / `fn.sumIf()` 會依輸入欄位選擇安全的解碼器（整數／Decimal 欄位使用字串，浮點欄位使用數字），並自動擴大 Decimal 精確度；如果確實需要 `number`，請串接 `.mapWith(Number)`。
- `fn.avg()` 一律回傳 `Selection<number>`，因為 ClickHouse 會在內部透過 Float64 執行 `avg(Decimal)`；把它包成字串會和實際執行路徑不符。
- `fn.min()` / `fn.max()` 會保留輸入欄位的 TS 型別（因此 `min(int64Col)` 是 `Selection<string>`，符合欄位讀取時的資料形狀）。

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

## 資料表與具體化檢視
<!-- src:03694c38 -->

`createTable()`、`createMaterializedView()` 和 `dropTable()` 各自送出一條由 schema 物件產生的 DDL 陳述式。它們不比較 schema、不執行 `ALTER`，也不記錄版本；既有資料表的遷移留在遷移工具裡。客戶端和工作階段上都可以呼叫。它們建立的資料表和檢視是持久的，`runInSession()` 不會刪除它們。

### `createTable()`

`createTable(table, options?)` 送出由 `ckTable(...)` 定義渲染出的 `CREATE TABLE` 陳述式：先是帶有 DDL 中繼資料的欄位，然後是 `ENGINE`、`PARTITION BY`、`PRIMARY KEY`、`ORDER BY`、`SAMPLE BY`、`TTL`、`SETTINGS` 和 `COMMENT`。

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

- 預設情況下，已有同名資料表時陳述式會失敗，錯誤名稱是 `TABLE_ALREADY_EXISTS`
- `mode: "if_not_exists"` 渲染 `CREATE TABLE IF NOT EXISTS`，既有資料表保持原樣，即使欄位定義不同也不改
- `mode: "or_replace"` 渲染 `CREATE OR REPLACE TABLE`，會取代既有資料表並捨棄其中的資料
- 沒有 `engine` 的資料表會使用 `MergeTree`，並帶上 `ORDER BY tuple()`
- 來自 `ckAlias(...)` 的別名資料表會被拒絕

### `createMaterializedView()`

`createMaterializedView(name, { to, as, mode? })` 建立一個由插入觸發的具體化檢視，寫入既有資料表 `to`。`as` 是 `select()` 建構器，規則與 `insert(to).fromSelect(...)` 相同：必須投影 `to` 的每一個必要欄位；未知鍵或型別不符是 TypeScript 錯誤。

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

- ClickHouse 按欄位名稱把具體化檢視的輸出寫入 `to`，所以 ck-orm 把 select 包成 `select ... as <column> from (...)`，讓每個鍵落到 `to` 中對應的欄位
- `as` 裡的查詢參數在建立檢視時被替換，因此存下來的檢視持有這些值
- select 強制的設定，例如 `leftJoin()` 的 `join_use_nulls = 1`，會寫進檢視的 `SETTINGS` 子句，因為請求設定不會隨檢視保存
- 檢視只能看到建立之後插入的資料列；要回填，用同一個 select 執行 `db.insert(to).fromSelect(...)`
- `mode: "if_not_exists"` 渲染 `IF NOT EXISTS`。ClickHouse 25.8 和 26.3 沒有 `CREATE OR REPLACE MATERIALIZED VIEW`，所以沒有 `or_replace` 模式
- `to` 是必要的：自帶 `ENGINE` 的檢視、`POPULATE` 和可重新整理的檢視都不支援；這些情況用 `ckSql` 配合 `command()`

### `dropTable()`

`dropTable(table, options?)` 刪除一張資料表或一個檢視。可以傳入 schema 物件，也可以傳入名稱：

```ts
await db.dropTable("usage_minutes_mv", { sync: true });
await db.dropTable(usageMinutes, { ifExists: true, sync: true });
```

- 預設渲染 `DROP TABLE name`；沒有這個名稱的物件時，以 `UNKNOWN_TABLE` 失敗
- `ifExists: true` 渲染 `IF EXISTS`
- `sync: true` 渲染 `SYNC`：呼叫在 ClickHouse 刪掉資料之後才回傳，而不是把刪除留給 `Atomic` 資料庫引擎的延遲清理
- 刪除具體化檢視後，它的 `to` 資料表以及已經寫入的資料列都還在
- 來自 `ckAlias(...)` 的別名資料表會被拒絕，也不會渲染 `ON CLUSTER`；在叢集上，使用遷移工具，或用 `ckSql` 配合 `command()`

## 工作階段與暫存資料表
<!-- src:8f031227 -->

ClickHouse 不提供傳統交易。針對特定範圍的分析和大量篩選，通常適合使用繫結到工作階段的暫存資料表。

在工作階段回呼中，`ck-orm` 提供：

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

如果你用自己控制的指令建立暫存資料表，但仍希望 `ck-orm` 在工作階段清理時刪除它，請使用 `registerTempTable(name)`：

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

只有在資料表定義可信、由開發者控制，而且無法用 `ckTable(...)` 表達時，才使用 `createTemporaryTableRaw(name, definition)`：

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

`definition` 參數從資料表名稱後面開始。不要包含 `CREATE TEMPORARY TABLE` 或資料表名稱；ck-orm 會產生這些內容，並另外驗證識別字。

### 工作階段行為

- 指向相同 `session_id` 的請求共用同一個工作階段並行控制器；用戶端預設值是 `session_max_concurrent_requests = 1`
- 實際的 ClickHouse 工作階段在伺服器端是獨佔的，因此把 `session_max_concurrent_requests` 調高到 `1` 以上會停用本機端的序列化，但仍可能因 `SESSION_IS_LOCKED` 而失敗
- `createTemporaryTable()` 會自動註冊資料表，供清理時使用
- `createTemporaryTable()` 會消耗 schema 物件；暫存資料表的生命週期由 `Session` 管理，不是由 schema 本身管理
- `createTemporaryTableRaw()` 是處理暫存資料表定義的原始 SQL API，只接受可信任的輸入，也會拒絕包含多個陳述式的定義
- `runInSession()` 會自動在每個請求（包括清理請求）帶上解析出的 `session_id` 作為 `X-ClickHouse-Replica-Tag`；這個由執行階段管理的標頭無法透過 `http_headers` 覆寫
- ClickHouse Cloud 的[副本感知路由](https://clickhouse.com/docs/products/cloud/features/infrastructure/replica-aware-routing)必須另外啟用，而且只保證盡力而為；擴展規模時可能重新對應標籤，跨來源的瀏覽器部署則必須在 CORS 中允許 `X-ClickHouse-Replica-Tag`
- `runInSession()` 會在回呼結束後刪除已註冊的暫存資料表
- 巢狀 `runInSession()` 呼叫一律建立新的子工作階段
- 巢狀呼叫不能重用任何仍在使用中的祖先 `session_id`
- 巢狀子工作階段不會與父工作階段共用暫存資料表或同一工作階段的並行名額，因為兩者一定使用不同的 `session_id`

### 工作階段並行契約

實際上，規則如下：

- 相同的明確 `session_id` 預設會讓請求依序執行
- 用戶端預設的 `session_id` 也會受同一個限制器控管
- `withSettings()` 建立的子用戶端若指向相同的 `session_id`，會與父用戶端共用同一個限制器
- 不同的 `session_id` 之間不會互相阻塞
- `stream()` 和建構器 `iterator()` 會一直佔用同一工作階段的並行名額，直到迭代器完成。提早停止（`break`、`return` 或發生錯誤）時，會在工作階段外送出 `KILL QUERY` 取消伺服器端查詢（使用相同的認證資訊和副本標籤），然後等待 ClickHouse 釋放工作階段，最多等到 `request_timeout`（預設 30 s）
- 巢狀 `runInSession()` 呼叫會建立全新的子 `session_id`，所以不會共用父工作階段的並行名額

中止請求或觸發 `request_timeout` 只會關閉連線。ClickHouse 會繼續執行查詢，直到查詢完成或注意到連線中斷，因此 `INSERT ... SELECT` 可能仍會完成，而同一工作階段的下一個請求可能因 `SESSION_IS_LOCKED` 而失敗。請把 `clickhouse_settings` 中的 `max_execution_time` 設成小於 `request_timeout`，讓伺服器先停止查詢；也可以在工作階段外執行 `KILL QUERY WHERE query_id = '...'`，或包裝 `fetch`，對於請求內容可以重新送出的請求，在有限次數內重試 373。清理失敗（包括 `SESSION_IS_LOCKED`）會透過 `onCleanupError` 或 `SessionError` 回報。

建議用法：

- 暫存資料表流程和任何需要延續工作階段的邏輯，都維持 `session_max_concurrent_requests = 1`
- 需要真正並行時，請使用不同的 `session_id`，不要調高同一工作階段的並行上限
- 只有在你明確想移除本機端的背壓，而且能接受 `SESSION_IS_LOCKED` 時，才調高 `session_max_concurrent_requests`

## 執行階段方法
<!-- src:1d0d331a -->

除了建構器，用戶端也提供低階的執行階段方法。

查詢可用的常見欄位包括：

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

`session_timeout` 和 `session_check` 會放在這裡，是因為它們描述特定請求或特定工作階段區塊，不是全域的用戶端預設值。
`query_id`、`session_id` 和 `session_timeout` 是請求／工作階段選項；不要放進 `clickhouse_settings`。

`session_check` 不會建立新的 ClickHouse 工作階段。只有需要 ClickHouse 確認明確指定的 `session_id` 已經存在時，才使用它。
任何帶有 `session_id` 的請求都會受同一個工作階段限制器控管，包括原始的 `execute()`、`command()`、`stream()`、建構器 `.execute()` 和 `.iterator()`。

每個請求的 `clickhouse_settings` 只會覆寫該請求的 ClickHouse 設定：

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

執行原始查詢並回傳 `Record<string, unknown>[]`：

```ts
const rows = await db.execute(ckSql`SELECT 1 AS one`);
```

### `stream()`

以串流方式讀取原始查詢結果：

```ts
for await (const row of db.stream(ckSql`SELECT number FROM numbers(10)`)) {
  console.log(row);
}
```

如果 `stream()` 使用 `session_id`，同一工作階段的並行名額要等非同步迭代器完成，或你提早跳出迴圈把它關閉後，才會釋放。

### `command()`

執行不回傳結果集的命令：

```ts
await db.command(ckSql`SYSTEM FLUSH LOGS`);
```

### `withSettings()`

建立帶有額外預設設定的子用戶端：

```ts
const reportDb = db.withSettings({
  max_execution_time: 60,
  join_use_nulls: 0,
});
```

`withSettings()` 只會變更 ClickHouse 工作階段／查詢的預設設定。回傳的用戶端沿用父用戶端的傳輸、認證資訊、逾時與工作階段並行控制器。`join_use_nulls: 0` 這個字面值會被推斷成字面值型別，因此子用戶端的左聯結結果型別不含 null；如果值只標成一般的 `number`，結果仍會帶 `| null`。

在 `runInSession()` 中，工作階段生命週期的選項與 ClickHouse 設定分開處理：

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

`runInSession()` 會把自己的 `clickhouse_settings`、`http_headers` 和 `query_params` 與父用戶端的預設值合併；兩邊設定了同一個鍵時，以工作階段的值為準。和 `withSettings()` 一樣，工作階段設定中的 `join_use_nulls: 0` 或 `join_use_nulls: 1` 字面值也會決定工作階段裡左聯結的結果型別是否含 null。

### 整合編譯後的查詢

多數應用程式直接使用建構器就好。只有在要整合另一個已經產生 `CompiledQuery` 的層時，才使用 `executeCompiled()` 和 `iteratorCompiled()`。

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

## 可觀察性
<!-- src:11c06560 -->

`ck-orm` 提供兩層可觀察性功能：

- `logger` 和 `logLevel`：內建記錄
- `plugins`：查詢生命週期 hook；OpenTelemetry 追蹤以 `ck-orm/otel` 外掛程式的形式提供

### 記錄器

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

### OpenTelemetry 外掛程式

`ck-orm/otel` 的 `otelTracing()` 會為 ck-orm 送給 ClickHouse 的每個請求記錄一個用戶端 span（跨度）。它放在獨立的進入點：`ck-orm` 的主進入點從不匯入 OpenTelemetry，所以不追蹤的應用程式、瀏覽器打包產物和邊緣 worker 不會多出任何成本。`@opentelemetry/api` 是選用的 peer dependency，只有 `ck-orm/otel` 會匯入它。

#### 設定步驟

ck-orm 透過 OpenTelemetry API 建立 span，但光有 API 不會記錄任何東西：在 SDK 註冊 tracer provider 之前，所有 span 都會被丟棄。ck-orm 不會啟動 SDK，也不會替你選匯出器，這兩件事都由你的應用程式在啟動時做一次。

請安裝 API、SDK 和匯出器。下面用的是 Node.js SDK 搭配 OTLP/HTTP 匯出器；只要能註冊 tracer provider，任何 SDK 都可以，瀏覽器版也一樣。

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

請在第一次查詢之前啟動 SDK，例如放在應用程式進入點最先匯入的模組裡：

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

匯出器預設會送到 `http://localhost:4318/v1/traces`；要改的話，設定標準的 `OTEL_EXPORTER_OTLP_ENDPOINT` 環境變數，或傳入 `url`。SDK 也會註冊脈絡管理器和 W3C 傳播器，span 的父子關係以及 `propagateTraceContext` 都要靠它們。執行時間很短的程序在結束前必須先 `await sdk.shutdown()`，否則最後的 span 會遺失。

接著把外掛程式加到用戶端：

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

不架設收集器也想看到 span 的話，可以參考 [OpenTelemetry 追蹤範例](https://ck-orm.lyz.cloud/examples/#otel-tracing)，它用記憶體內的匯出器，並印出外掛程式記錄到的內容。

#### Span

span 的種類是 `CLIENT`。會回傳資料列的查詢，span 名稱是 `clickhouse QUERY`；其他請求一律叫 `clickhouse <OPERATION>`，例如 `clickhouse INSERT` 或 `clickhouse CREATE`。查詢開始時哪個 span 正在作用中，新 span 就成為它的子 span，例如框架的 OpenTelemetry 檢測替每個 HTTP 請求開啟的 span。

除非傳入 `tracer`，否則外掛程式會使用 `trace.getTracer("ck-orm")`。它會從 `clickhouseClient(...)` 設定取得資料庫名稱、伺服器位址、伺服器連接埠和請求逾時。服務層級的標籤請透過 `attributes` 傳入；以 `db.` 開頭的鍵會被忽略，因此應用程式無法覆寫內建的資料庫屬性。

共用的追蹤後端建議使用較安全的 `includeStatement: false`。預設值是 `true`；如果不希望資料表名稱、欄位名稱或查詢的資料形狀離開服務邊界，請將它關閉。繫結值不會嵌入精簡後的陳述式，但 SQL 的資料形狀在營運上仍可能屬於敏感資訊。

#### 伺服器端 span

設定 `propagateTraceContext: true`，就會在每個請求中附上該 span 的 W3C 追蹤脈絡（`traceparent`、`tracestate`）。ClickHouse 隨後會替查詢記錄自己的 span，例如 `HTTPHandler`、`query` 和管線階段，並把它們記為 ck-orm span 的子 span。外掛程式只轉送這兩個標頭，絕不轉送全域傳播器產生的 `baggage` 或廠商標頭，而且只對有取樣的 span 轉送：ClickHouse 對收到的每個追蹤脈絡都會記錄伺服器 span，不論有沒有取樣。外掛程式透過全域註冊的傳播器注入，OpenTelemetry SDK 預設將其設為 W3C 追蹤脈絡；沒有註冊傳播器時，不會送出任何內容。

```ts
const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123/telemetry_lab",
  plugins: [otelTracing({ propagateTraceContext: true })],
});
```

ClickHouse 會把這些 span 寫入 `system.opentelemetry_span_log`，不會匯出；如何把它們匯出到追蹤後端，請參閱[使用 OpenTelemetry 追蹤 ClickHouse](https://clickhouse.com/docs/operations/opentelemetry)。伺服器 span 帶有 `clickhouse.query_id`，與 ck-orm span 上的 `db.query.id` 相符，並透過 `clickhouse.exception_code` 而不是 span 狀態回報失敗。

如果你在 `http_headers` 中傳入自己的 `traceparent`，它會覆寫外掛程式的；請一併傳入對應的 `tracestate`，否則外掛程式的 `tracestate` 會隨它一起送出。

在瀏覽器中，除非 ClickHouse 允許 `traceparent` 標頭，否則帶有它的請求會通不過 CORS 預檢；又因為只有取樣的請求才會帶上它，失敗看起來會像隨機發生。預設的 `http_options_response` 不允許這個標頭，所以請先在 `config.d` 檔案中加入它，再於瀏覽器中啟用傳播：

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

### 撰寫外掛程式

外掛程式是一個物件，包含 `name` 和最多三個 hook：

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

`onQueryStart` 可以為該請求回傳標頭：

```ts
const requestSource: ClickHouseORMPlugin = {
  name: "request-source",
  onQueryStart() {
    return { http_headers: { "x-request-source": "billing-worker" } };
  },
};
```

[查詢外掛程式範例](https://ck-orm.lyz.cloud/examples/#query-plugins)會執行一個外掛程式：它從 `onQueryStart` 回傳標頭，並在 `onQuerySuccess` 和 `onQueryError` 中依實際執行的順序記錄每條陳述式。

外掛程式遵循這些規則：

- hook 會在 ck-orm 送給 ClickHouse 的每個請求上執行：查詢、串流、命令、插入、DDL 輔助工具、`ping()`、`replicasStatus()`，以及 `runInSession()` 中的暫存資料表清理。只有取消被放棄的串流並等待其工作階段解除鎖定的請求（`KILL QUERY` 及其後的輪詢）不會執行 hook。
- 每個 `onQueryStart` 之後，都會恰好接著一次帶有相同 `executionId` 的 `onQuerySuccess` 或 `onQueryError`；對串流而言，這發生在迭代器完成或被關閉時。呼叫端提前停止的串流算作成功，並回報到目前為止已讀取的資料列。請依 `executionId` 管理每個請求的狀態。
- `onQueryStart` hook 依註冊順序執行，而且全部完成後才會送出請求。`onQuerySuccess` 和 `onQueryError` 以相反順序執行。內建記錄器永遠在最外層執行。
- 後面的外掛程式的標頭會覆寫前面的。明確指定的 `http_headers`（無論是用戶端層級或單一請求）會覆寫外掛程式的標頭。外掛程式無法覆寫由連線認證資訊建立的 `Authorization` 標頭，也無法覆寫工作階段送出的 `X-ClickHouse-Replica-Tag` 標頭。
- 拋出例外的 hook 會連同外掛程式名稱一起記錄，不影響查詢或其他外掛程式。拋出例外、或回傳無效標頭的 `onQueryStart`，它的標頭會被捨棄。
- 事件是唯讀的。外掛程式無法更改 SQL、參數、設定或結果。
- hook 在查詢路徑上會被等待。像網路匯出這類緩慢的工作，請先緩衝，不要在 hook 中等待它完成。

公開的可觀察性型別：

- `ClickHouseORMPlugin`
- `ClickHouseORMQueryStartResult`
- `ClickHouseORMQueryEvent`
- `ClickHouseORMQueryResultEvent`
- `ClickHouseORMQueryErrorEvent`
- `ClickHouseORMQueryStatistics`
- `ClickHouseORMTracingOptions`（來自 `ck-orm/otel`）
- `ClickHouseORMLogLevel`

內建事件欄位包含 `databaseName`、`serverAddress`、`serverPort`、`requestTimeoutMs`、`statementHash`、`querySummary`、`queryId`、`sessionId`；如果 ck-orm 能在不猜測原始 SQL 的情況下辨識出單一資料表，也會包含 `tableName`。非串流 JSON 查詢在伺服器回傳統計資料時，還會包含 ClickHouse 回應統計：`serverElapsedMs`、`readRows`、`readBytes`、`resultRows` 和 `rowsBeforeLimitAtLeast`。

## 錯誤模型
<!-- src:b4b93db0 -->

公開的錯誤型別（只匯出型別）：

- `ClickHouseORMError`
- `DecodeError`

執行階段錯誤檢查：

- `isClickHouseORMError(error)`
- `isDecodeError(error)`

`ClickHouseORMError` 會盡可能保留最多的脈絡，包括：

- `kind`
- `executionState`
- `queryId`
- `sessionId`
- `httpStatus`
- `clickhouseCode`
- `clickhouseName`
- `responseText`
- `requestTimeoutMs`

目前公開的 `kind` 值：

- `client_validation`
- `request_failed`
- `decode`
- `timeout`
- `aborted`
- `session`

伺服器端記錄時要保留完整錯誤脈絡，再回傳精簡的公開錯誤給應用程式呼叫端：

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

請避免把 `responseText` 或原始錯誤 `message` 回傳給不受信任的用戶端。ClickHouse 的錯誤文字可能包含 SQL 片段和物件名稱。

## 安全性
<!-- src:062d0032 -->

`ck-orm` 預設讓你很難組出不安全的 SQL。

內建防護包括：

- 傳入 `ckSql.identifier()` 的識別字會先驗證，再加上引號
- 會驗證 `fn.call(...)`、`fn.withParams(...)`、`ckType.aggregateFunction(...)` 和 `ckType.simpleAggregateFunction(...)` 使用的函式名稱；`AggregateFunction` 參數必須是字串或數值字面值
- 傳入 `ckType.nested({...})` 的巢狀欄位鍵會按 ClickHouse 識別字規則驗證
- 插值到 `` ckSql`...` `` 的值會變成 ClickHouse 具名參數，不會直接成為原始 SQL 文字
- 以 `orm_param` 開頭的 `query_params` 鍵會拒絕，因為這個前置詞保留給內部 SQL 範本參數
- 每個請求只允許一個頂層陳述式
- 連線設定衍生的授權標頭不能由呼叫端提供的 `http_headers` 覆寫
- 結構化連線設定不接受內嵌在 `host` 的認證資訊
- 追蹤目的端資料會先正規化，再附加到 span

### 只供可信內容使用的 API

對於無法用 `ckTable(...)` 表示、而且內容可信的暫存資料表 DDL，`ck-orm` 在 `runInSession()` 內提供作用域受限的原始 SQL API：

- `session.createTemporaryTableRaw(name, definition)`

`fn.call(name, ...)` 和 `fn.withParams(name, ...)` 會驗證 `name`，但動態選擇的函式名稱仍應視為開發者可控的輸入，而不是不受信任的執行階段輸入。

`ckSql.identifier(...)` 會在編譯／執行邊界驗證。建立 SQL 片段的成本很低；錯誤會在片段編譯進查詢時出現。

### 追蹤資料的暴露範圍

`otelTracing()` 會把壓縮後的 SQL 文字以 `db.statement` 和 `db.query.text` 附加到 span，因為 `includeStatement` 預設是 `true`。

繫結值會另外存成 ClickHouse 具名參數，不會放進陳述式文字；但資料表名稱、欄位名稱和查詢形狀仍會出現在追蹤輸出中。如果追蹤後端是共用的，或不在服務主機上，請停用 `includeStatement`，或在收集器中過濾 `db.statement` / `db.query.text`。

ClickHouse 伺服器 span 也會在 `system.opentelemetry_span_log` 中把陳述式記為 `db.statement`。除非你匯出，否則該資料表只會留在 ClickHouse 伺服器上。

### 錯誤回應

`ClickHouseORMError.responseText` 和錯誤 `message` 可能包含 ClickHouse 伺服器的原始文字，其中包括 SQL 片段和資料庫物件名稱。

不要把這些細節直接轉傳給不受信任的用戶端。請在伺服器端記錄，改提供通用訊息。

## 安裝
<!-- src:8558838e -->

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
yarn add ck-orm @opentelemetry/api
```

ck-orm 有一個 peer dependency：`@opentelemetry/api`。Bun、npm 和 pnpm 會自動一併安裝它；Yarn 不會，所以 Yarn 指令會明確加入這個套件。

## 快速開始
<!-- src:d7ba4246 -->

以下範例只用一個資料表，主要流程會比較容易理解。

### 1. 定義模式

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

### 2. 建立客戶端

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

查詢建構器產生的查詢是 thenable，所以 `await query` 會直接執行查詢。

`fn.sum` 處理 `Decimal` 欄位時，會自動轉為 `Decimal(P, S)`，再返回 `string`，以免損失精確度。見 [運算式中的 Decimal 精確度](#decimal-precision-in-expressions)。

## 範例
<!-- src:c8f5c20b -->

[範例頁面](https://ck-orm.lyz.cloud/examples/) 會展示可執行的查詢，以及每個查詢實際送往 ClickHouse 的 SQL。每個範例都是 [`examples/`](../examples) 內的獨立項目，各自有測試。可執行的 ClickHouse 測試覆蓋範圍放在 [`e2e/`](../e2e)，其中包括 [`e2e/scenarios.ts`](../e2e/scenarios.ts) 內貼近生產環境的場景模式。

## 心智模型
<!-- src:a26a2ce8 -->

使用 `ck-orm` 時，請記住以下界線：

- `ckType.*` 定義模式的欄位類型
- `ckTable(...)` 定義資料表模式
- `ck.*` 是查詢輔助工具命名空間
- `fn.*` 是 SQL 函數輔助工具命名空間
- 模式描述的是資料表和欄位，不是數據庫名稱
- 數據庫連線放在 `clickhouseClient(...)` 上
- 建構器查詢是預設路徑；遇到 ClickHouse 專用運算式時，可使用原始 SQL
- `runInSession()` 是 ClickHouse 的工作階段輔助工具，不是交易
- `leftJoin()` 和 `asofLeftJoin()` 預設使用 SQL 風格的 NULL 語義
- 數值較大或風險較高的結果會以保守方式解碼，而不是靜默強制轉換

`ck-orm` 專注於具類型的 ClickHouse 存取：資料表定義、查詢組合、寫入輔助工具、工作階段輔助工具和可觀測性。`createTable()`、`createMaterializedView()` 和 `dropTable()` 各自執行一條 DDL 語句；模式遷移（比較模式、`ALTER`、版本管理）仍由應用程式使用的遷移工具處理。

## 模式 DSL
<!-- src:5149bff0 -->

### `ckTable()`

用 `ckTable(name, columns, options?)` 定義資料表。

以下範例都假設你已載入：

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

第三個參數可以直接傳普通物件，也可以傳工廠函數：

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

模式中繼資料在 `ck-orm` 中負責兩件事：

- 驅動具類型的查詢、插入驗證和結果解碼
- 為 `createTable()` 和 `createTemporaryTable()` 渲染 `CREATE TABLE` 語句

它不會遷移或同步既有的 ClickHouse 資料表：`createTable()` 只建立資料表（見 [資料表與物化視圖](#tables-and-materialized-views)）。模式遷移留在遷移工具裡，也要讓 `ckTable(...)` 與應用程式實際讀寫的模式保持一致。

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

### 類型推斷

每個資料表都提供：

- `table.$inferSelect`
- `table.$inferInsert`

```ts
type TelemetryRow = typeof probeTelemetry.$inferSelect;
type TelemetryInsert = typeof probeTelemetry.$inferInsert;

const sampleId: TelemetryRow["sampleId"] = "900001";
const ingestVersion: TelemetryInsert["ingestVersion"] = "1";
```

如果要寫泛型輔助工具，就用：

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

配合 `InferSelectSchema` 和 `InferInsertSchema` 使用的模式物件只是普通的 TypeScript 物件分組。它們可以用來共用模型類型，而且與客戶端配置分開。

### 用 `$type<T>()` 細化欄位類型

每個欄位建構器都有可鏈式的 `$type<T>()` 方法，只會覆寫欄位的 TypeScript 類型，不會改變任何執行階段行為。如果預設推斷類型太寬，超出你的領域，就用它：

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

`$type<T>()` 可以按任何順序與其他欄位修飾符鏈式組合：`ckType.string().$type<Role>().default(ckSql`'guest'`).comment("…")`。執行階段解碼器、編碼器、SQL 類型和 DDL 輸出都不受影響；`$type` 只是編譯時期的類型斷言。

`enum8` 和 `enum16` 會直接從傳入的鍵值物件推斷字面值聯集，所以只有要換成另一個聯集類型，或讓它對齊外部 enum 類型時，才需要用 `$type`：

```ts
// Inferred automatically — no `as const`, no explicit type arguments
const status = ckType.enum8({ active: 1, paused: 2, banned: 3 });
//    ↑ Enum8<"active" | "paused" | "banned">

// Re-brand on top of the inferred keys
type UserStatus = "active" | "paused" | "banned";
const userStatus = ckType.enum8({ active: 1, paused: 2, banned: 3 }).$type<UserStatus>();
```

> **注意事項：** `$type<T>()` 不會驗證執行階段值。如果數據庫資料列含有宣告的類型以外的值（例如模式遷移前留下的舊資料），TypeScript 仍會當它符合宣告的類型，照樣收窄類型。這時窮舉的 `switch` 語句也會悄悄漏掉這個情況。如果要在讀取時確保值符合類型，可以在投影中用 `mapWith`；如果每次解碼和編碼都要驗證，就用下面的 `$validator()`。

### 用 `$type<{ select, insert }>()` 區分插入與選取類型

ClickHouse 欄位在插入時接受的類型，有時會比選取時返回值的類型更寬。例如 `DateTime` 欄位在插入時同時接受 `string` 和 `Date`，但解碼後一定返回 `Date`。把物件傳給 `$type`，就可以分開這兩種類型：

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

只傳 `$type<{ select: T }>()`（不含 `insert`），就等同於單一泛型的 `$type<T>()` 寫法。

### 插入模型規則：DEFAULT、MATERIALIZED、ALIAS

`$inferInsert` 是按模式的欄位結構推導，而不是直接照抄 `$inferSelect`：

| DDL 修飾符 | 對插入模型的影響 |
| --- | --- |
| `.default(expr)` | 欄位變成可選；省略它時，ClickHouse 會填入值 |
| `.materialized(expr)` | 插入模型會完全移除這個欄位 |
| `.aliasExpr(expr)` | 插入模型會完全移除這個欄位 |

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

### 用 `$validator(schema)` 驗證執行階段值

如果 `$type` 不夠用，就改用 `$validator(schema)`，例如數據庫可能有舊資料、你要接受來自不受信任上游系統的 JSON 結構，或想讓 insert API 把字串強制轉成 `Date`。任何符合 [Standard Schema v1 規格](https://github.com/standard-schema/standard-schema) 的物件都可以用：Zod 3.23+、Valibot 1+、ArkType、Effect Schema、TypeBox。ck-orm 不依賴上述任何程式庫。

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

ck-orm 會配合模式轉換（例如 `z.string().transform((v) => new Date(v))`）：插入時欄位接受模式的*輸入*類型，選取類型則是*輸出*類型。`$validator` 只支援同步模式；非同步模式會在解碼或編碼時擲回錯誤。

`$type` 與 `$validator` 的比較：

| | `$type<T>()` | `$validator(schema)` |
| --- | --- | --- |
| 執行階段成本 | 零 | 每個資料列的每個欄位都會同步執行一次模式驗證 |
| 類型安全 | 編譯時期斷言 | 由模式推斷 |
| 執行階段驗證 | 否 | 是；不相符時擲回錯誤 |
| 輸入和輸出類型不同 | 用 `$type<{ select, insert }>()` | 用模式轉換 |
| 外部依賴 | 無 | 無（模式程式庫由你選擇） |

### JSON 欄位類型

ck-orm 支援 ClickHouse 24.x 以上的**新版 JSON 數據類型**（[說明文件](https://clickhouse.com/docs/sql-reference/data-types/newjson)）。這種類型會把 JSON 物件按類型拆成子欄位存放，不會把整個值塞進不透明字串。一開始先記住兩項基本規則：

- ClickHouse 的 `JSON` 數據類型**只接受頂層物件**，頂層陣列、字串和數字一律會被拒絕。ck-orm 會在編譯時用 `T extends Record<string, unknown>` 執行同一規則，每次插入或查詢時也會做一次成本很低的 O(1) 執行階段檢查。
- 具類型路徑（`a.b UInt32`）在 ClickHouse 內會變成真正的子欄位；ck-orm 會把這些路徑交給對應的 ck-orm 欄位工廠，用 `mapFromDriverValue` / `mapToDriverValue` 處理。因此，宣告成 `ckType.uint64()` 的路徑會和頂層 `uint64` 欄位一樣，解碼成無損的 `string`。

最簡單的寫法和其他 ck-orm 欄位工廠完全一致：

```ts
import { ckTable, ckType } from "ck-orm";

const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json("payload"),
});
// DDL:  `payload` JSON
// $inferSelect.payload: { [key: string]: unknown }
```

加上泛型後，`$inferSelect` / `$inferInsert` 就會顯示實際結構：

```ts
type EventPayload = { user_id: number; event: string; meta?: { ip: string } };

const userEvents = ckTable("user_events", {
  id: ckType.uint64(),
  payload: ckType.json<EventPayload>("payload"),
});
```

如果你想把泛型和其他欄位建構器呼叫放在一起，可以改用可鏈式的 `ckType.json<T>().$type<T2>()`。

#### 參數化 DDL

第二個工廠參數可以用來設定 NewJSON DDL 的四個選項：

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

`typeHints` 的鍵會用 `Paths<T>` 做類型檢查，像 `{ "user.idd": ... }` 這種拼錯的鍵會令編譯失敗。輸出的 DDL 永遠按穩定的字典序產生，所以無論配置怎樣寫，外部模式比較工具看到的輸出都逐位元組一致。

#### typeHints 會影響執行階段解碼器

因為 ClickHouse 對 64 位元值採用無損策略，`UInt64` 在 ck-orm 中會解碼成字串。如果 `user_id` 同時宣告在 TS 結構和 `typeHints` 項目內，兩邊必須保持一致；以 `uint64` 為例，泛型要寫 `user_id: string`。如果插入時接受的輸入和查詢時解碼出的類型不同，就用 `$type<{ select, insert }>()`：

```ts
ckType.json("payload", { typeHints: { user_id: ckType.uint64() } })
  .$type<{
    select: { user_id: string };
    insert: { user_id: string | number | bigint };
  }>();
```

#### 路徑存取

每個 JSON 欄位都有五個可鏈式的路徑方法，會照原樣輸出 ClickHouse 的路徑存取語法：

| 方法 | SQL | 含義 |
| --- | --- | --- |
| `col.path("a.b")` | `col.a.b` | 讀取路徑；靜態類型是 `PathValue<T, "a.b">`（請參閱下面關於 `typeHints` 的備註） |
| `col.castPath("a.b", ckType.uint64())` | `col.a.b.:UInt64` | 用另一個 ck-orm 欄位工廠強制解碼這條路徑；靜態類型取自強制解碼欄位的 `TData` |
| `col.subobject("a")` | `col.^a` | 把嵌套物件讀成 `JSON` 值 |
| `col.merged("a")` | `col.@a` | 讀取合併後的共享資料視圖 |
| `col.arrayPath("a")` | `col.a[]` | 展開陣列類型的路徑 |

> `col.path(P)` 的靜態類型永遠是 `PathValue<T, P>`，因為它來自泛型 `T`，而不是來自 `typeHints`。`typeHints` 只會改變執行階段解碼器；靜態類型和類型提示要自己手動保持一致（上一節說明了 `uint64 → string` 的無損策略）。如果兩者不一致，改用 `castPath(P, ckType.X())`；它的返回類型取自強制解碼用的欄位。

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

`Paths<T>` 會在編譯時驗證路徑字面值；如果 `"a.zzz"` 不屬於 `T`，`payload.path("a.zzz")` 就過不了類型檢查。

每個鏈式修飾符（`$type`、`$validator`、`.default()`、`.materialized()`、`.bind()` 等）都會在執行階段重新附加路徑方法，所以底層呼叫永遠有效。**靜態類型會一直保留這些方法，只有 `$validator` 例外**：`$validator` 的契約以 `StandardSchemaV1<unknown, unknown>` 為泛型；在 TypeScript 強制的 LSP 風格子類型規則下，無法安全收窄成 `JsonShape`，所以經過這個環節後，靜態類型會退回 `Column<T>`。方法本身仍可呼叫；如果還要把它接回鏈式呼叫，就使用類型斷言：

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

#### `fn.json*`：動態路徑呼叫

當路徑字串是在執行階段建立（或者你要讀取 `T` 沒有宣告的路徑），就使用 `fn.*` 命名空間：

```ts
import { fn } from "ck-orm";

const userPath = userInput as string;
await db.select({
  raw:    fn.jsonPath<string>(userEvents.payload, userPath),
  casted: fn.jsonCast(userEvents.payload, userPath, ckType.string()),
  shape:  fn.dynamicType(fn.jsonPath(userEvents.payload, userPath)),
});
```

`fn.jsonCast` 是 `castPath` 的動態版本；`fn.jsonSubobject`、`fn.jsonMerged` 和 `fn.jsonArray` 分別對應各欄位方法；`fn.dynamicType` 公開 ClickHouse 的 `dynamicType()` 函數，可以用來檢查 Dynamic 類型路徑在執行階段的實際類型。

#### 執行階段驗證

每一個資料列都會按**這個順序**經過兩層驗證：

1. **ck-orm 內建的 JSON 防護**——`null` / `undefined` 會原樣通過，所以 `nullable()` JSON 欄位可以正常使用；其他非物件輸入（陣列、字串、數字、布爾值）一律會被拒絕，並產生限定欄位範圍的 `client_validation` / `decode` 錯誤，例如 `JSON column expects a plain object, got array`。`typeHints` 中宣告的具類型路徑，寫入和讀出時都會交給對應欄位工廠的編碼器／解碼器處理。這一層永遠啟用。
2. **可選的 `$validator(schema)`**——設定後，插入時會在內建編碼器之前執行，查詢時則在內建解碼器之後執行。可以用它做結構層面的驗證：

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

ck-orm **刻意**不會遞迴進入 JSON 值，檢查 `T["a"]` 是否真的是 `number` 等。需要這種驗證就用 `$validator`；JSON 防護的職責，只是在 ClickHouse 自己的錯誤訊息難以對應回欄位時先攔下問題。頂層標量輸入會直接報錯，錯誤訊息也會帶上欄位名稱：

```ts
await db.insert(events).values({ payload: ["wrong"] as never });
// throws: JSON column expects a plain object, got array
```

#### 插入預設值

JSON 欄位和其他欄位一樣，都遵守 `$inferInsert` 規則。`.default()` 會讓欄位變成可選；`.materialized()` / `.aliasExpr()` 會把它完全從插入模型移除。

```ts
const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json<{ note: string }>("payload").default(ckSql`'{}'`),
});

await db.insert(auditLogs).values({ id: "1" }); // payload filled by CH DEFAULT
```

### `ckAlias()`

當同一個資料表要在查詢中出現多次，就使用 `ckAlias()`。

```ts
import { fn, ck, ckAlias } from "ck-orm";

const telemetry = ckAlias(probeTelemetry, "telemetry");
```

`ckAlias()` 返回的欄位會自動重新繫結到別名。

### 欄位名稱

模式物件的鍵是 TypeScript 資料列、解碼後的查詢結果和插入值共用的邏輯鍵。預設情況下，這個鍵也會當作 ClickHouse 欄位名稱：

```ts
const telemetry = ckTable("telemetry", {
  signalStrength: ckType.decimal({ precision: 20, scale: 5 }),
});
```

如果數據庫欄位用了不同名稱，就把那個實體欄位名稱傳入第一個參數：

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

SQL、DDL、篩選條件、排序、分組和寫入欄位清單會使用實體欄位名稱（`probe_id`、`signal_strength`、`created_at`）。推斷模型、預設 SELECT 結果、明確投影鍵和插入值則使用模式物件鍵（`probeId`、`signalStrength`、`createdAt`）。

每個公開的 `ckType` 建構器都支援在最外層指定實體欄位名稱。不需額外配置的建構器可以傳入 `name?`；有類型配置的建構器則把可選的實體欄位名稱放在第一個參數，再把類型配置放進物件：

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

`aggregateFunction` 和 `simpleAggregateFunction` 使用 ClickHouse 專用的呼叫格式：第一個字串是聚合函數名稱，不是欄位名稱：

```ts
ckType.aggregateFunction("sum", ckType.uint64());
ckType.aggregateFunction("quantile(0.5)", ckType.float64());
ckType.simpleAggregateFunction("sum", ckType.uint64());
```

`AggregateFunction` 接受帶字面值參數的 ClickHouse 聚合名稱，例如 `quantile(0.5)` 或 `topK(10)`。`simpleAggregateFunction` 的限制較嚴，只接受不含字面值參數的聚合函數名稱。

同時需要實體欄位名稱時，請使用物件配置：

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

`nested("items", shape)` 會指定外層 `Nested(...)` 欄位的名稱。嵌套欄位名稱來自 `shape` 的鍵；內部欄位的 `configuredName` 值不會用作嵌套欄位名稱：

```ts
const sensorPackets = ckTable("sensor_packets", {
  items: ckType.nested("items", {
    componentId: ckType.string(),
    quantity: ckType.float64(),
  }),
});
```

### 欄位建構器

`ckType.*` 是模式欄位建構器。模式 DSL 涵蓋常見的 ClickHouse 類型系列：

- 整數：`int8`、`int16`、`int32`、`int64`、`uint8`、`uint16`、`uint32`、`uint64`
- 浮點數和小數：`float32`、`float64`、`bfloat16`、`decimal`
- 標量類型：`bool`、`string`、`fixedString`、`uuid`、`ipv4`、`ipv6`
- 時間類型：`date`、`date32`、`dateTime`、`dateTime64`、`time`、`time64`
- 列舉和特殊類型：`enum8`、`enum16`、`json`、`dynamic`、`qbit`
- 容器：`nullable`、`array`、`tuple`、`map`、`nested`、`variant`、`lowCardinality`
- 聚合類型：`aggregateFunction`、`simpleAggregateFunction`
- 幾何類型：`point`、`ring`、`lineString`、`multiLineString`、`polygon`、`multiPolygon`

在模式驅動的讀取、寫入和推斷模型中，`int64` 和 `uint64` 預設會用 TypeScript `string`，所以 64 位元值在 ClickHouse JSON 傳輸格式和 JavaScript 執行階段之間傳遞時仍能保持精確。如果你想明確改用 `bigint`，可以自行加入解碼器，例如 `mapWith((value) => BigInt(String(value)))`。

ClickHouse 不支援 `Nullable(Array(...))`、`Nullable(Map(...))` 或 `Nullable(Tuple(...))`。`ck-orm` 會在定義模式時拒絕這些結構。請把 `nullable(...)` 改放進複合類型內，例如 `ckType.array(ckType.nullable(ckType.string()))`。

有類型配置的建構器使用物件配置，並把可選的實體欄位名稱放在第一個參數：

```ts
ckType.decimal({ precision: 20, scale: 5 });
ckType.decimal("signal_strength", { precision: 20, scale: 5 });
ckType.fixedString({ length: 8 });
ckType.dateTime64("created_at", { precision: 9, timezone: "UTC" });
ckType.qbit("embedding", ckType.float32(), { dimensions: 8 });
```

`Date` / `Date32` 的寫入值和模式驅動的判斷條件值，會把 JavaScript `Date` 轉成 UTC 日曆日送出（`2026-06-15T23:00:00Z` 會變成 `2026-06-15`）。如果要儲存不同的日期，請傳入 `YYYY-MM-DD` 字串；字串和數字會原樣送出。

`DateTime` / `DateTime64` 的判斷條件值會把 JavaScript `Date` 繫結成 UTC 時間點，因此欄位、工作階段或伺服器的時區都不會改變這個時間點。字串則由 ClickHouse 按欄位時區原樣讀取。在 32 位元 `DateTime` 位置，如果參數內所有 Date 值都是整秒，就會繫結為 `DateTime('UTC')`；只要其中任何 Date 值帶有毫秒，就會繫結為 `DateTime64(3, 'UTC')`。`DateTime64` 位置一律繫結 `DateTime64(3, 'UTC')`。`gte` 這類標量判斷條件仍會比較精確時間點。`DateTime` 欄位的 `inArray` / `notInArray` 需要整秒的 Date；帶毫秒的 Date 在這裡會被 ClickHouse 拒絕（`TYPE_MISMATCH`）。這是原生行為，而且這類值本來就不可能相等。`insert().values()` 會按欄位精確度，把每個 `Date` 以整數時間刻度送出。請參閱「寫入」。

### 欄位類型速查表

常見模式結構及對應的 TypeScript 值：

| ClickHouse 結構 | 模式 | TypeScript 值結構 |
| --- | --- | --- |
| 列舉 | `ckType.enum8({ idle: 1, active: 2 })` | `"idle" | "active"`（由鍵推斷） |
| 低基數字串 | `ckType.lowCardinality(ckType.string())` | `string` |
| 可為 NULL 的小數 | `ckType.nullable(ckType.decimal({ precision: 18, scale: 5 }))` | `string | null` |
| 陣列 | `ckType.array(ckType.string())` | `string[]` |
| 可為 NULL 的陣列元素 | `ckType.array(ckType.nullable(ckType.string()))` | `(string | null)[]` |
| 元組 | `ckType.tuple(ckType.string(), ckType.int32())` | `[string, number]` |
| 對映 | `ckType.map(ckType.string(), ckType.string())` | `Record<string, string>` |
| 嵌套物件陣列 | `ckType.nested({ component: ckType.string(), quantity: ckType.float64() })` | `{ component: string; quantity: number }[]` |
| 變體 | `ckType.variant(ckType.string(), ckType.int32())` | `string | number` |
| JSON | `ckType.json<{ risk?: { score?: number } }>()` | `{ risk?: { score?: number } }` |

`ckType.map(...)` 目前只支援 `String` 鍵，並會把它們轉成 JavaScript 鍵值物件，因此無法反映 ClickHouse `Map(K, V)` 的重複鍵特例。

插入的資料列會使用與 `typeof table.$inferInsert` 相同的推斷結構；不過，呼叫 `insert(table).values(...)` 時，可以省略有 ClickHouse 預設值或產生運算式的欄位。產生的 `INSERT` 欄位清單永遠不會包含 `MATERIALIZED` 和 `ALIAS` 欄位；即使明確傳入，也會被拒絕。

## 客戶端配置
<!-- src:0d752985 -->

用 `clickhouseClient()` 建立客戶端：

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:<password>@127.0.0.1:8123/telemetry_lab",
});
```

### 連線模式

`clickhouseClient()` 支援兩種互相排斥的連線方式。

#### `databaseUrl`

想用單一連線字串時，就用 `databaseUrl`：

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:secret@127.0.0.1:8123/telemetry_lab",
});
```

設定 `databaseUrl` 後，不要再傳入：

- `host`
- `database`
- `username`
- `password`
- `pathname`

#### 結構化連線欄位

想分開設定各部分時，就直接傳入各個欄位：

```ts
const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  username: "default",
  password: "<password>",
});
```

結構化模式的預設值：

- `host`: `http://localhost:8123`
- `database`: `default`
- `username`: `default`
- `password`: `""`

### 常用欄位

大多數軟件項目只會用到一小部分客戶端欄位：

| 欄位 | 用途 |
| --- | --- |
| `request_timeout` | 每次請求的實際耗時上限（可選），單位是毫秒。省略時，ck-orm 不會在客戶端套用逾時限制；請求的生命週期由底層擷取機制、平台、伺服器和 `abort_signal` 控制。長時間執行的串流查詢不要設定這項，改用 `abort_signal` 設定整體耗時上限（例如 `AbortSignal.timeout(30_000)`）。 |
| `clickhouse_settings` | 預設的 ClickHouse 工作階段／查詢設定 |
| `application` | 設定 ClickHouse 應用程式名稱 |

資料表定義與連線配置分開。用 `ckTable(...)` 定義資料表，在組合查詢時匯入這些資料表物件，然後直接傳入 `.from(...)`、`.insert(...)` 和暫存資料表輔助工具。

### 進階欄位

這些欄位用來控制較進階的執行階段行為：

| 欄位 | 用途 |
| --- | --- |
| `fetch` | 每次 HTTP 請求都會呼叫 `(request: Request) => Response \| Promise<Response>`：它可以是任何 fetch 實作、伺服器處理函數，或像 [`examples/decimal-sum/main.test.ts`](../examples/decimal-sum/main.test.ts) 一樣的測試替身。預設使用平台的 `fetch` 實作 |
| `http_headers` | 額外的預設標頭 |
| `role` | 預設的 ClickHouse 角色，可設一個或多個 |
| `session_id` | 預設工作階段 ID |
| `session_max_concurrent_requests` | 每個 `session_id` 在這個客戶端內可同時執行的請求數上限（預設 `1`） |
| `compression.response` | 預設為 `false`，串流請求會送出 `enable_http_compression = 0`，讓資料列隨 ClickHouse 產出而送達。非串流請求會沿用伺服器預設值（ClickHouse 26.3+ 會在客戶端接受壓縮時壓縮回應）。設為 `true` 會壓縮所有回應；如果串流很稀疏，可能要等壓縮區塊填滿才收到資料 |
| `logger` / `logLevel` | 記錄器整合 |
| `tracing` | OpenTelemetry 整合 |
| `instrumentation` | 自訂查詢生命週期 hook |

工作階段生命週期的控制刻意只在請求層級生效。把 `session_timeout` 傳入單一查詢或 `runInSession(...)`。只有繼續使用現有 `session_id` 時才用 `session_check`；建立全新工作階段時不要用。
`session_max_concurrent_requests` 則不同：這是客戶端層級的保護機制，會限制指向同一個 `session_id` 的並行請求數量。
真正的 ClickHouse 工作階段仍由伺服器鎖定，所以把 `session_max_concurrent_requests` 調到 `1` 以上，可能只會出現 `SESSION_IS_LOCKED`，而不是讓同一工作階段真正並行處理請求。
除非你有意移除本地請求序列化，而且準備處理伺服器端的工作階段鎖定失敗，否則保留預設值 `1`。

### ClickHouse 設定

`clickhouse_settings` 只用來設定 ClickHouse 工作階段／查詢設定。這些設定鍵列在 ClickHouse [工作階段設定](https://clickhouse.com/docs/operations/settings/settings) 中，HTTP API 也接受它們作為查詢參數。它和 `host`、`database`、`request_timeout`、`http_headers`、`session_max_concurrent_requests` 等 ck-orm 客戶端配置分開。

`query`、`database`、`session_id`、`role` 和 `param_*` 這些 HTTP 傳輸欄位要留在 `clickhouse_settings` 外面；ck-orm 會分別驗證它們，因為它們屬於請求封裝和具名參數通道。

官方設定鍵支援 TypeScript 自動補全；較新 ClickHouse 版本新增的設定鍵，以及部署專屬設定使用的任意設定鍵，也仍然有效：

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

JSON 解析／序列化由 fetch 傳輸層處理。公開的客戶端配置會讓這項行為在不同執行階段保持一致，不會另外提供 `json` 覆寫選項。

ck-orm 會在每次請求中強制套用一組精簡的傳輸契約設定：`http_write_exception_in_output_format = 0`、`output_format_json_quote_64bit_integers = 1`、`output_format_json_quote_decimals = 1`，以及 `date_time_output_format = "iso"`。如果呼叫端設定與這些值衝突，ck-orm 只會記錄一次警告，然後送出強制值；`max_threads` 這類工作負載／性能設定仍由呼叫端控制。

### 身份驗證

`ck-orm` 的數據庫連線使用基本身份驗證。

- `databaseUrl` 模式可以把登入憑證內嵌在網址中
- 結構化模式使用 `username` 和 `password`
- 未提供登入憑證時，預設用戶名稱是 `default`，密碼留空

## 查詢建構器
<!-- src:fc28245d -->

以下片段假設 `db`、`probeTelemetry` 以及文中使用的輔助工具都已從 `ck-orm` 匯入。

### `select()`

明確指定欄位會返回結構明確的結果：

```ts
const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
    signalStrength: probeTelemetry.signalStrength,
  })
  .from(probeTelemetry)
  .limit(10);
```

投影物件由公開的 `Selection` 值或欄位組成。實際上，資料表欄位、`fn.*(...)` 和 `ck.expr(...)` 的輸出，都可以在 `select({ ... })` 中用同樣方式組合。

沒有 JOIN 時，隱式選取會返回完整的資料表模型：

```ts
const rows = await db.select().from(probeTelemetry).limit(10);
```

有 JOIN 時，隱式選取會按來源分組欄位，並返回嵌套物件。LEFT JOIN 找不到配對時，該組會變成 `null`。ClickHouse 的 `Array` / `Map` 欄位會返回 `[]` / `{}`，而不是 NULL；因此在這個判斷中，空容器會視為沒有資料。即使資料列已配對，但所有標量欄位都是 NULL，陣列和對映又都是空的，結果仍無法和未配對區分，同樣會變成 `null`。

### `from()`, `innerJoin()`, `leftJoin()`

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

### `asofJoin()`, `asofLeftJoin()`

`asofJoin()` 和 `asofLeftJoin()` 會渲染 ClickHouse 的 `ASOF JOIN` 和 `LEFT ASOF JOIN`。對左側每一筆資料列，它們只聯結右側值最接近的那一筆資料列，而不是每一筆符合的結果。條件用 `ck.and(...)` 組合一個或多個相等條件，以及左右欄位之間恰好一個 `ck.gt`、`ck.gte`、`ck.lt` 或 `ck.lte`：

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
- `asofLeftJoin()` 遵循 [`leftJoin()` 的 NULL 語義](#join-null-semantics)：它會送出 `join_use_nulls = 1`，並把右側類型標成可為 NULL，所以沒有符合項目的左側資料列會得到 `null`。`asofJoin()` 會丟掉這些資料列，和 `innerJoin()` 一樣。
- 用來找最接近值的欄位必須是整數、浮點數、`Date`、`DateTime` 或 `Decimal`，比較的兩邊都必須是欄位；和值比較不算。可以再加上相等過濾，例如 `ck.eq(quotes.symbol, "BTCUSD")`。
- ck-orm 不檢查這個條件。沒有不等比較、出現第二個不等比較，或寫了 `ck.or(...)` 時，ClickHouse 會在讀取任何資料之前拒絕；錯誤是 `ClickHouseORMError`，`kind` 為 `"request_failed"`，`clickhouseName` 為 `"INVALID_JOIN_ON_EXPRESSION"`。沒有相等條件，以及最接近匹配欄位是其他類型，同樣會被拒絕；缺少相等條件時的錯誤名稱在不同 ClickHouse 版本之間不一樣。
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

`.where(...predicates)` 接受可變參數，並以 `AND` 組合條件。它會略過值為 `undefined` 的判斷條件物件，所以你可以直接傳入多個條件，也可以用 `ck.and(...)` 和 `ck.or(...)` 建立分組條件。

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

值清單會以單一 `Array(T)` 查詢參數送出（`col in {p:Array(T)}`）。文字長度超過約 40,000 個字符的清單會拆成多個參數，再用 `arrayConcat(...)` 合併。這樣拆分不會影響主鍵和 skip-index 的裁剪。ClickHouse 26.3+ 每個 HTTP 請求最多接受 1,000 個查詢參數；25.8 的分析器會拒絕合併後超過約 1 MB 的清單。資料集更大時，請改用暫存資料表或外部資料。清單只要含有 SQL 運算式，或是混合類型的無類型值，仍會逐元素使用一個參數。

`ck.and(...)` 會略過 `undefined`，所以動態篩選條件可以直接組合：

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

`undefined` 只會在判斷條件層級被略過。判斷條件的值會嚴格檢查：`ck.eq(column, undefined)` 會擲回錯誤，而不是悄悄放寬查詢條件。單獨的 `null`、`false`、`true`、`0` 和 `""` 都不是判斷條件；請改用 SQL 運算式。`null` 仍是 Nullable 欄位和寫入的有效資料值，但篩選 NULL 必須明確寫出：

```ts
db.select()
  .from(probeTelemetry)
  .where(ck.isNull(probeTelemetry.deletedAt));
```

布爾欄位仍是一般值，也可以直接用作 SQL 運算式：

```ts
const healthChecks = ckTable("health_checks", {
  id: ckType.int32(),
  isPassing: ckType.bool("is_passing"),
});

db.select().from(healthChecks).where(ck.eq(healthChecks.isPassing, false));
db.select().from(healthChecks).where(healthChecks.isPassing);
```

不要把單獨的 `null` 放進一般比較或 `IN` 清單。NULL 邏輯要明確組合：

```ts
ck.or(
  ck.inArray(probeTelemetry.deletedAt, [new Date("2026-04-21T00:00:00.000Z")]),
  ck.isNull(probeTelemetry.deletedAt),
);
```

如果要在執行階段組合較大的篩選條件，建議使用 `Predicate[]` 配合可變參數的 `.where(...predicates)`：

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

`Predicate` 是可重用布爾 SQL 子句的公開名稱。同一個判斷條件物件可以用在 `where`、`having`、JOIN 的 `on` 子句，以及 `ck.exists(...)` 這類處理布爾值的輔助工具中。

`Selection` 是查詢建構器中可重用計算值的公開名稱，例如 `fn.sum(...)`、`fn.toString(...)` 和 `ck.expr(ckSql...)`。用 `.as(...)` 建立別名，用 `.mapWith(...)` 覆寫解碼。`Order` 是 `ck.asc(...)` 和 `ck.desc(...)` 返回的子句物件。

`ck.has(...)`、`ck.hasAll(...)` 和 `ck.hasAny(...)` 會直接對應原生 ClickHouse 函數，並保留 ClickHouse 的陣列、對映和 JSON 語義。左側是陣列欄位時，一般 JavaScript 值會先按該欄位編碼，所以 `Array(Date)` 篩選會先套用欄位的 Date 編碼，再變成查詢參數。

`where(...)` 接受可變參數，`having(...)` 只接受一個判斷條件。要在 `having` 中組合多個條件，請先用 `ck.and(...)` 或 `ck.or(...)` 組合判斷條件。

`ck.contains(...)`、`ck.startsWith(...)`、`ck.endsWith(...)` 及其 `*IgnoreCase` 變體會按字面處理輸入。它們會把值參數化，並在內部逸出 LIKE 萬用字元（`%`、`_`、`\`）。

只有在確實需要完整模式語義時，才使用 `ck.like(...)` / `ck.ilike(...)`。這些 API 仍會把值參數化，以維持 SQL 保安；但 LIKE 是模式語言，所以 `%` 和 `_` 會保留萬用字元含義。

按字面搜尋範例：

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

### `groupBy()`, `having()`, `orderBy()`, `limit()`, `offset()`

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

`groupBy()` 和 `limitBy([...])` 接受欄位，以及由 `fn.*(...)` 或 `ck.expr(...)` 這類輔助工具產生的計算 `Selection` 值。

`limit()`、`offset()` 和 `limitBy(..., limit)` 直接傳入的值必須是非負安全整數。只有確實需要 ClickHouse 常數運算式時，才使用 `ckSql`。

`orderBy()` 接受：

- `ck.desc(selection)`
- `ck.asc(selection)`
- 直接傳入欄位

### `final()`

為資料表查詢加上資料表層級的 `FINAL`：

```ts
const query = db.select().from(probeTelemetry).final();
```

對沒有別名的簡單資料表讀取，`ck-orm` 會產生 `FROM table FINAL`。如果根資料表有別名，或者查詢 JOIN 了其他來源，`ck-orm` 就會把套用 FINAL 處理後的資料表包在子查詢內，並在外層來源保留別名。這樣可以避開 ClickHouse 分析器處理 `FINAL`、資料表別名、JOIN 和 lambda 運算式時的邊界情況，同時保留原有的建構器 API。

`final()` 只套用到資料表根來源。如果 CTE、子查詢或資料表函數流程需要在內部使用 `FINAL`，請先在以資料表為來源的查詢上呼叫 `.final()`，再呼叫 `.as(...)`。

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

查詢建構器產生的查詢有 3 種執行方式：

```ts
const query = db.select().from(probeTelemetry).limit(10);

const rows = await query;
const sameRows = await query.execute();

for await (const row of query.iterator()) {
  console.log(row);
}
```

在應用程式範例中，如果想讓執行點一眼看得出來，就使用 `.execute()`。直接寫 `await query` 也可以，這是 Drizzle 風格的便利用法；團隊熟悉查詢建構器產生的查詢是 thenable 後，這種寫法就很實用。

`query.iterator()` 的工作階段並行規則跟 `db.stream()` 一樣：查詢使用 `session_id` 時，就會一直佔用並行槽位，直到迭代完成，或提早關閉迭代器。

### 用 `toSQL()` 查看 SQL

`toSQL()` 只會編譯建構器，不會送出任何東西。請直接在建構器上呼叫它，因為 await 建構器會直接執行查詢。`select()`、`insert().values()` 和 `insert().fromSelect()` 產生的建構器都可以用。

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

返回的結果有以下欄位：

- `sql` 是單行的 SQL 語句，每個值的位置都是一個佔位符，例如 `{orm_param1:String}`。它不含 `FORMAT` 子句，因為這個子句要等查詢送出時才會加上。
- `params` 存放佔位符對應的原始 JavaScript 值。查詢執行時，這些值會單獨編碼，與 SQL 分開送出，所以絕不會被內嵌進 `sql`。
- `forcedSettings` 列出建構器強制套用到請求上的設定，例如 `leftJoin()` 會帶來 `join_use_nulls: 1`。沒有這類設定時，這個欄位不會出現。
- `body` 只有 `insert().values()` 才有：它是以 Values 文字表示的資料列，而 `sql` 則是 `INSERT INTO t (cols) FORMAT Values` 語句。其他情況下沒有這個欄位。

返回的結果是副本，修改它不會影響建構器。

傳入 `{ pretty: true }`，`sql` 和 `body` 就會分成多行排版，方便記錄和除錯：

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

美化輸出只會改變空白。每個子句各佔一行，`select` 的項目逐行排列，子查詢和 CTE 會縮排，`and` 或 `or` 組合內的條件也是每個一行。函數的參數仍留在同一行，引號內的文字完全不會動，含有 SQL 註解的語句則原樣返回。`insert().values()` 的主體裡，每個資料列各佔一行。美化輸出完全不影響實際送出的查詢。

### 子查詢與 CTE

用 `.as("alias")` 把建構器轉成有明確別名的子查詢：

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

子查詢的 `.as("…")` **可以省略**。如果省略這個呼叫，框架會在編譯時自動指派別名（`__sub_1`、`__sub_2`……）。未加別名的建構器可以直接用在 `from()`、`innerJoin()`、`leftJoin()`、`inArray()`、`notInArray()`、`exists()` 和 `notExists()`。

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

#### 寫法建議：何時用 `.as(name)`，何時直接用建構器

| 情況 | 建議寫法 |
|---|---|
| 一次性行內子查詢，不需要重用 | 直接傳入建構器，配合 `innerJoin/leftJoin` 的回呼函數形式；或直接傳給 `inArray/exists` |
| 子查詢的欄位參照會在多處使用（SELECT / WHERE / 單一 JOIN） | 繫結到 `const`，再直接傳入，不加別名；欄位參照會在鏈式方法中一路保留 |
| 自我 JOIN（同一個邏輯子查詢、2 個 SQL 別名） | 明確呼叫 `.as("a")` 和 `.as("b")`；如果同一個實例重複作為 2 個來源，查詢編譯成 SQL 時會擲回錯誤 |
| 需要穩定、易讀或有類型的別名（例如生產環境的 `query_log` 搜尋、`EXPLAIN`、慢查詢記錄、跨模組輔助工具，或字面值 `TAlias` 來源鍵） | 呼叫 `.as("descriptive_name")` |

結構不同的查詢，各次編譯所得的自動別名**不穩定**；但在同一次編譯內，以及同一建構器樹重複編譯時，別名會保持穩定。`snapshot` 測試不需要正規化。

#### 選取鍵與方法名稱衝突

如果選取鍵和 SelectBuilder 的方法名稱撞名（`from`、`where`、`select`、`as`、`innerJoin`、`leftJoin`、`asofJoin`、`asofLeftJoin`、`groupBy`、`having`、`orderBy`、`limit`、`offset`、`final`、`limitBy`、`execute`、`iterator`、`then`、`catch`、`finally`、`buildSelectionItems`），未加別名的建構器會保留方法類型。`sub.from` 永遠返回建構器方法，不會返回欄位參照；即使選取內容包含 `from` 也一樣。這類鍵請改名（例如 `from` → `fromAddress`），或用 `.as("name")` 包裝子查詢，讓欄位存取改由專用的 `Subquery` 物件處理。

使用 `$with()` 和 `with()` 建立 CTE：

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

呼叫 `$with()` 時不指定名稱，就會建立匿名 CTE。每個查詢只能把匿名 CTE 用作來源 1 次；如果要把同一個結果 JOIN 2 次，請為每次 JOIN 各定義 1 個 CTE（呼叫 2 次 `db.$with().as(...)`，或建立 2 個不同名稱的具名 CTE）。

### `db.count()`

`db.count(source, ...predicates)` 是 Drizzle 風格的計數輔助工具。它採用與 `.where(...predicates)` 相同的判斷條件語義：多個判斷條件會用 `AND` 組合，`undefined` 的判斷條件物件會略過。不過，`ck.eq(...)` 這類輔助工具收到 `undefined` 作為值時，仍會擲回錯誤。

```ts
import { ck } from "ck-orm";

const total = await db.count(
  probeTelemetry,
  ck.eq(probeTelemetry.status, 1),
  ck.gt(probeTelemetry.id, 1000),
);
```

結果集較複雜時，可以改為計算子查詢或 CTE 的筆數：

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

`db.count(...)` 預設使用數值計數模式：它會產生 `toFloat64(count())`，再解碼成 `number`。計數值很大時，可能超出 JavaScript 整數精確度；如果需要精確值，或要保留傳輸結構，請使用可鏈式模式：

```ts
const approximateTotal = await db.count(activeProbes); // number
const exactTotal = await db.count(activeProbes).toSafe(); // string
const wireTotal = await db.count(activeProbes).toMixed(); // number | string
```

`.toSafe()` 會產生 `toString(count())`，主要用於讀取精確值。如果把這個安全計數值用作 SQL 運算式，它會採用 `String` 語義；要在 SQL 中作數值比較，請使用預設模式、`.toUnsafe()` 或 `.toMixed()`。`.toMixed()` 會產生 `toUInt64(count())`，並保留驅動程式和傳輸結構；依照 ck-orm 預設的無損 64 位元 JSON 設定，實際 ClickHouse 回應通常會以 `string` 返回。

## JOIN 的 NULL 語義
<!-- src:59e7a4e2 -->

`leftJoin()` 和 `asofLeftJoin()` 預設會自動套用 `join_use_nulls = 1`，因此使用 SQL 風格的 NULL 語義。

具體來說：

- 預設 LEFT JOIN 的右側會推斷成可為 NULL
- 如果明確停用 `join_use_nulls`，推斷出的類型也會隨之改變

想跟 ClickHouse 預設 JOIN 行為一致，就傳入字面值 `0`。這個字面值的類型會自動推斷，所以不需要 `as const`。例如從變量取得的值，即使類型是普通的 `number`，結果仍會加上 `| null`。

```ts
const rawDefaultDb = db.withSettings({
  join_use_nulls: 0,
});
```

對不具名的建構器（即沒有 `.as()` 的子查詢）或匿名 CTE 做 LEFT JOIN 時，這些來源的每個欄位參照都會推斷成 `| null`，就連 `FROM` 一側以及已做 INNER JOIN 的不具名建構器內的欄位參照也一樣。這些參照共用一個較寬的 `string` 來源鍵，所以類型系統看不出哪個來源是 LEFT JOIN 的對象。執行階段仍然只會對 LEFT JOIN 的一側返回 NULL。

```ts
const rows = await db
  .select({ uid: allUsers.uid, spent: totals.spent })
  .from(allUsers)
  .leftJoin(totals, ck.eq(allUsers.uid, totals.uid));
// uid and spent are both number | null
```

請用 `.as("name")` 為 `FROM` 一側或已做 INNER JOIN 的子查詢命名。這樣它的欄位參照會保留自己的來源鍵並維持非 NULL，LEFT JOIN 的一側則維持 `| null`。

JOIN 過的查詢再用作子查詢、CTE、`ck.exists(...)` 或 `ck.inArray(...)` 的來源時，強制設定的 `join_use_nulls = 1` 會保留，所以建構器類型仍會配合執行階段行為。

計算運算式在執行階段按 ClickHouse 的規則傳播 NULL：ClickHouse 返回 NULL（輸入是 NULL，或 LEFT JOIN 找不到配對資料列）時，值就是 `null`。它們的 TypeScript 類型是你宣告的 `TData`（或輔助工具的固定類型），不會自動擴大類型；輸入可能為 NULL 時，就宣告 `| null`（例如 `fn.plus<number | null>(...)`），或用 `fn.coalesce(...)` 包裝輸入。

## 寫入
<!-- src:d7555d6e -->

### `insert(table).values(...)`

想按資料表模式插入具類型資料，就用建構器：

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

所有資料列會用一個 `INSERT INTO t (cols) FORMAT Values` 請求送出。這個 SQL 語句沒有查詢參數；資料列會以 Values 格式的文字直接放在請求主體內，所以不受 HTTP 的 1,000 個參數上限和每個值 128 KiB 上限影響。某個欄位若略過，只會在該資料列套用它的 `DEFAULT`。這次插入會停用運算式求值（`input_format_values_interpret_expressions = 0` 和 `input_format_values_deduce_templates_of_expressions = 0`），所以每個值都只會當作資料。要插入計算所得的值，請用 `insert().fromSelect()`。JavaScript `Date` 會按那個精確時間點送出，再按欄位精確度截斷；`DateTime` 欄位保留整秒。`DateTime` 或 `DateTime64` 欄位收到數值時，會當作 Unix 秒數解讀；收到字串時，則按欄位時區解讀。

插入資料列必須使用資料表模式中的鍵。遇到未知的鍵時，會立即拒絕這次插入。

### `insert(table).fromSelect(select)`

用 `.fromSelect(...)` 把查詢編譯成一條 `INSERT INTO target (cols) SELECT ...` SQL 語句，讓 ClickHouse 在伺服器端把 SELECT 結果物化。資料列不會經過 Node 進程。這種寫法很適合先從大型來源資料表篩出所需範圍，再在 `runInSession` 內重用物化後的資料片段，供多個下游查詢使用。

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

- SELECT 必須投影目標資料表的所有必填欄位；以 `.default(...)` 宣告的欄位可以略過。缺少必填欄位時，編譯階段（TypeScript）會先報錯，執行階段也會再次拒絕。
- INSERT 的欄位清單會按 SELECT 投影鍵的順序產生，所以即使 ClickHouse 按位置對齊欄位，對呼叫端來說仍是 `name-aligned` 語義。投影鍵的順序不會影響哪個值寫入哪個目標欄位，因為實際比對的是鍵本身。
- 未知的投影鍵（目標資料表上沒有這個鍵）以及個別欄位的類型不符，都會在送出查詢前先由 TypeScript 報錯。
- 同一條插入鏈不能同時使用 `.values()` 和 `.fromSelect()`。建構器類型會收窄，未使用的方法不會在自動完成中出現，執行階段也會拒絕這次呼叫。
- 已編譯的傳輸請求就是普通的 ClickHouse 指令：查詢事件中的 `mode: "insert"`，`tableName` 則設為目標資料表。
- `.fromSelect()` 接受 `SelectBuilder`（`session.select({...}).from(...)`），不接受包裝過的子查詢。只有要把結果放進另一個 SELECT 內當作 CTE 或子查詢時，才需要用 `.as("name")`；交給 `.fromSelect()` 時不需要。

#### 嵌套欄位

`ckType.nested(...)` 欄位與 `.fromSelect()` 互動時有三種模式：

1. **略過投影中的嵌套鍵**。這是預設模式，也符合 ck-orm 的執行階段契約：缺少嵌套值時會編碼成 SQL `DEFAULT`，ClickHouse 會為嵌套結構的每個欄位填入空的平行陣列。TS 層會把 `$inferInsert` 中的嵌套欄位視為 `optional`，反映這個行為。
2. **投影直接參照嵌套欄位**，例如 `select({ events: src.events })`。ck-orm 會把內層 SELECT 包進子查詢，再把外層投影改寫成按欄位沿點號路徑存取（`__ck_inner.events.name`、`__ck_inner.events.score`……）。來源嵌套結構必須包含目標嵌套結構所需的每個欄位。
3. **把計算運算式投影到嵌套欄位**會在編譯階段被拒絕，因為單一 SQL 運算式無法展開成平行陣列的各個欄位。如果要逐列插入值，請用 `.values(...)` 或 `.insertJsonEachRow(...)`。

如果要由 TS 層強制每筆資料都提供嵌套資料（`Pattern D`，即不允許空嵌套陣列的業務契約），可以啟用 `ckType.nested({...}).requiredOnInsert()`。這個鏈式呼叫會返回一個欄位，它的 `$inferInsert` 結構會把 `events` 列為必填；這時 `.fromSelect()` 必須使用嵌套欄位參照投影模式，或改用逐列方式。

> **JSON 傳輸格式備註**：`.values()` 會把 JSON 欄位轉成加上引號的 `JSON.stringify(...)` 文字，直接放在 Values 主體內送出；ClickHouse 再把這個字面值解析成 JSON 欄位值。`.fromSelect()` 完全在伺服器端執行，所以 JSON 值不會在 JS 進程中來回轉換。兩種方式最後儲存的資料完全相同。

### `insertJsonEachRow()`

如果手上已經是物件資料列或非同步資料列串流，就用 `insertJsonEachRow()`：

```ts
await db.insertJsonEachRow("tmp_scope", [
  { probe_id: "probe_alpha" },
  { probe_id: "probe_beta" },
]);
```

它接受：

- 字串形式的資料表名稱
- 用 `ckTable()` 建立的資料表物件
- 一般陣列
- `AsyncIterable`

空的一般陣列會視為客戶端的空操作，但仍會向插裝 hook 回報一次成功的插入生命週期，並附上 `rowCount: 0`。未知欄位的處理方式由 ClickHouse 控制；想讓伺服器略過額外的 JSON 欄位時，請傳入 `input_format_skip_unknown_fields: 1` 之類的設定。嵌套欄位會按展平後的 `column.field` 陣列送出（ClickHouse `flatten_nested = 1`，即預設值），所以 `{ lines: [{ sku: "a" }] }` 在 `insert().values()` 和 `insertJsonEachRow()` 中都可以使用。

有資料表模式時，`insertJsonEachRow()` 會把 `DateTime`／`DateTime64` 欄位中的 JavaScript `Date` 以與 `insert().values()` 相同的精確整數時間刻度送出，並交由 ClickHouse 預設的 `date_time_input_format = basic` 解析器處理字串，所以兩者會以相同方式解讀日期時間字串。你自己傳到 `DateTime64` 欄位的 JSON 數值，在 `JSONEachRow` 中仍會按時間刻度解讀，在 `.values()` 中則按 Unix 秒數解讀。

直接用資料表名稱字串呼叫 `insertJsonEachRow("table_name", rows)` 時沒有欄位類型，所以 `Date` 會以帶 `Z` 的 ISO 8601 文字送出，ck-orm 也會為這次請求設定 `date_time_input_format = best_effort`。使用有模式的資料表時，資料列中不在模式內的鍵也一樣：ck-orm 會原樣傳遞，所以要處理這些鍵的 `Date`，就得在該次呼叫加入 `clickhouse_settings: { date_time_input_format: "best_effort" }`。在 ClickHouse 25.8 上，`best_effort` 解析器會錯誤解讀 1970 年以前、帶有小數秒的時間點，又會拒絕 `12:34:56` 這類 `Time` 字串；由模式指定類型的欄位就能避開這兩個問題。

## 原始 SQL
<!-- src:f47b381e -->

`ck-orm` 提供自己的 SQL 模板 API。當直接寫想要的 SQL 比用建構器語法更直接時，就用它。

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

### 配合 `query_params` 使用原始 SQL

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

ck-orm 會自動選擇參數傳輸方式。你不需要為 `query_params` 另外設定 multipart 處理。

以 `orm_param` 開頭的 `query_params` 鍵會被拒絕。這個前綴保留給 `` ckSql`...` `` 內部產生的參數。

值格式化器支援基本類型值、`Date`、`NaN`、`Infinity`、陣列、物件和 `Map` 值，可用於 `{ids:Array(UInt64)}` 或 `{attrs:Map(String, String)}` 這類 ClickHouse 的具類型佔位符。透過 `query_params` 傳入的值沒有自己的欄位類型，所以陣列一律以 Array 文字送出。這裡不支援 `Tuple(...)` 佔位符。陣列內的 64 位元整數必須是 `bigint`；字串雖然會加上引號，但如果元素類型是數值，仍會被拒絕。`Date` 會以 UTC 時間文字送出，ClickHouse 會按佔位符類型的時區解讀；如果不想讓工作階段或伺服器時區造成時間偏移，請把它宣告成 `{at:DateTime64(3, 'UTC')}`。用 `` ckSql`...${date}` `` 插值時，會自動完成相同的處理。如果參數代表資料表或欄位名稱，請使用 ClickHouse 的 `Identifier` 佔位符類型：

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

片段有類型時，`ck.expr()` 會保留原類型。片段沒有類型時，會刻意維持 `unknown`；如果結果類型在模式之外已知，就傳入泛型或解碼器：

```ts
const status = ck.expr(ckSql<string>`'closed'`); // Selection<string>
const enabled = ck.expr<boolean>(ckSql`1`, {
  decoder: (value) => Number(value) === 1,
});
```

### 原始查詢格式

非串流的原始查詢只支援 `JSON` 輸出：

```ts
const rows = await db.execute(ckSql`select 1`, {
  format: "JSON",
});
```

原始串流查詢只支援 `JSONEachRow` 輸出。使用預設的 `compression.response: false` 時，串流不會壓縮；每列都會隨 ClickHouse 產出而送達：

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

- `sum` / `sumIf` 會把 P 擴大到 ≥ 38；`min` / `max` 則保留欄位的 P。由模式定義的 Decimal 欄位使用 `fn.sum()` 和 `fn.sumIf()` 時會得到 `Selection<string>`；Float 欄位則得到 `Selection<number>`。不論經過 `nullable(decimal(...))` 還是 `lowCardinality(decimal(...))` 包裝，都會自動轉型。
- `maxIf(value, condition)` 與 `max` 遵循相同的 Decimal 規則：Decimal 結果會是 `Selection<string>`，並保留宣告的 `Decimal(P, S)` 結構。
- `avg` **不**會自動轉型。ClickHouse 會以 Float64 計算 `avg(Decimal)`，所以 `fn.avg` 會返回 `Selection<number>`。需要精確的 Decimal 平均值時，請用 `ckSql.decimal(ckSql\`sum(x) / count(x)\`, P, S)`。
- `fn.divideDecimal(left, right, resultScale?)` 直接對應 ClickHouse 的 `divideDecimal`。它會返回 `Selection<string>`，所以不會損失精確度。提供 `resultScale` 時，值必須是 `0` 到 `76` 的字面整數，並會直接內嵌到 SQL；建構器不會檢查 ClickHouse 版本，也不會模擬這個 ClickHouse 函數。
- `column.cast(P, S)` 轉型的是欄位，不是聚合結果。直接把它放在 `GROUP BY` 內會擲回 `NOT_AN_AGGREGATE`。請使用 `fn.sum(column)`，或包裝整個聚合運算式。
- 插入不接受既不是字串也不是數值的物件（例如原始 `decimal.js` 實例），請改傳 `.toFixed(scale)`：

```ts
db.insert(measurementLog).values({ value: new Decimal("1.23").toFixed(5) }); // ✅
db.insert(measurementLog).values({ value: new Decimal("1.23") as never }); // ❌ throws
```

## 視窗運算式
<!-- src:6d0b3f76 -->

用 `fn.over(...)` 包裝 `fn.*` 函數運算式。可選設定中，`partitionBy` 只接受建構器的選取項目，`orderBy` 則接受一般的 `ck.asc(...)`／`ck.desc(...)` 值：

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

`fn.rowNumber()` 預設會用 `toFloat64` 產生不安全的 `Selection<number>`。要在 `fn.over(...)` 前呼叫 `.toSafe()`，取得精確的 `Selection<string>`；或呼叫 `.toMixed()`，保留原生 `UInt64` 傳輸結構（`Selection<number | string>`）。`.toUnsafe()` 就是明確指定這個預設行為。建構器刻意不加入 JavaScript 安全整數檢查：ClickHouse 的 `toFloat64` 轉換可能把大於 `Number.MAX_SAFE_INTEGER` 的值捨入；`.toSafe()` 會保留精確的十進制字串，`.toMixed()` 則保留驅動程式的原生結果結構。Decimal 聚合會保留一般的字串解碼器，並編譯成 `CAST(sum(...) OVER (...) AS Decimal(...))`，所以 `OVER` 會留在聚合運算式內，不會放到 CAST 外面。`fn.over` 刻意不接受單獨的欄位或原始 SQL 片段；目前這個對外界面只涵蓋 `PARTITION BY` 和 `ORDER BY`。要使用視窗框架、具名視窗或其他 ClickHouse 專用子句，請改用 `ckSql`。

## 函數與資料表函數
<!-- src:1e16409b -->

### `fn`

以下輔助工具涵蓋泛型、轉換、聚合、JSON、元組和資料表相關功能：

- `fn.call()`
- `fn.withParams()`
- `fn.replaceRegexpAll()` — 具類型的 ClickHouse RE2 替換；見[正則表達式替換](#regex-replacement)
- 類型轉換輔助工具對應 ClickHouse 的類型轉換頁面：`fn.cast()`、`fn.date()`、`fn.accurateCast*()`、`fn.reinterpret*()`、`fn.parseDateTime*()`、`fn.formatDateTime()`、`fn.formatRow*()`、`fn.toString*()`、`fn.toBool()`、`fn.toInt*()`、`fn.toUInt*()`、`fn.toFloat*()`、`fn.toBFloat16*()`、`fn.toDecimal*()`、`fn.toDecimalString()`、`fn.toFixedString()`、`fn.toDate*()`、`fn.toDateTime*()`、`fn.toTime*()`、`fn.toTime64*()`、`fn.toInterval*()`、`fn.toLowCardinality()`、`fn.toNullable()` 和 `fn.toUUID*()`。
- 64 位元或更寬的整數轉換（`toInt64/128/256*`、`toUInt64/128/256*`、Unix 64 位元時間戳記輔助工具）會以 `string` 解碼；較窄的整數和浮點數轉換則以 `number` 解碼。
- 只能用字面值的 ClickHouse 參數會先驗證，需要時再直接內嵌：Decimal 的小數位數、DateTime64/Time64 精確度、FixedString 長度、目標類型字串和時間間隔單位。
- `fn.toStartOfMonth()`
- `fn.toUnixTimestamp()` / `fn.toUnixTimestamp64Second()` / `fn.toUnixTimestamp64Milli()` / `fn.toUnixTimestamp64Micro()` / `fn.toUnixTimestamp64Nano()`
- `fn.fromUnixTimestamp()` / `fn.fromUnixTimestamp64Second()` / `fn.fromUnixTimestamp64Milli()` / `fn.fromUnixTimestamp64Micro()` / `fn.fromUnixTimestamp64Nano()`
- `fn.count()` / `fn.countIf()` — 預設會返回 `Selection<number>`，SQL 會包裝成 `toFloat64(count(...))`。呼叫 `.toSafe()` 可得到 `Selection<string>`（`toString(count(...))`），呼叫 `.toMixed()` 可得到 `Selection<number | string>`（`toUInt64(count(...))`），也可以用 `.toUnsafe()` 回到預設模式。行為跟 `db.count` 一樣。解碼器只接受非負整數，並拒絕 `NaN`、負數、布爾值等；範例見 [`fn.count` / `fn.uniqExact` 模式](#fncount--fnuniqexact-modes)。
- `fn.sum()` / `fn.sumIf()` / `fn.min()` / `fn.max()` / `fn.maxIf()` — Decimal 欄位會自動轉型為 `Decimal(P, S)`；見[運算式中的 Decimal 精確度](#decimal-precision-in-expressions)
- `fn.over()` / `fn.rowNumber()` — 可用 `PARTITION BY` 和 `ORDER BY` 組合視窗函數；見[視窗運算式](#window-expressions)
- `fn.divideDecimal()` — 精確 Decimal 除法；如有指定結果小數位數，就會直接內嵌；見[運算式中的 Decimal 精確度](#decimal-precision-in-expressions)
- `fn.argMax()` / `fn.argMin()` — 返回 `val` 最大／最小那一列的 `arg` 值。返回類型跟 `arg` 欄位一致（不會加上 Decimal CAST 包裝；ClickHouse 會保留欄位宣告的類型）。
- `fn.avg()` — `Selection<number>`（Float64），與 ClickHouse 原生 `avg(Decimal)` 行為一致
- `fn.uniqExact()` — 與 `fn.count()` 一樣提供 3 種鏈式模式：預設 `Selection<number>`，SQL 會包裝成 `toFloat64(uniqExact(...))`；呼叫 `.toSafe()` 可得到 `Selection<string>`（`toString(uniqExact(...))`）；呼叫 `.toMixed()` 可得到 `Selection<number | string>`（`toUInt64(uniqExact(...))`）；用 `.toUnsafe()` 回到預設模式。解碼器同樣只接受非負整數。見 [`fn.count` / `fn.uniqExact` 模式](#fncount--fnuniqexact-modes)。
- `fn.coalesce()`
- `fn.if()` / `fn.multiIf()` — 條件函數。兩者都會返回 `Selection<unknown>`，因為 ClickHouse 會根據所有值分支決定伺服器端的結果類型。
- `fn.greatest()` / `fn.least()` / `fn.nullIf()` — 比較函數；`nullIf` 會自動把 sqlType 包裝成 `Nullable(...)`，本身已可為 NULL 的輸入不會重複包裝。
- `fn.positionCaseInsensitive()` / `fn.positionCaseInsensitiveUTF8()` — 不區分大小寫／UTF-8 的子字串搜尋。返回 `Selection<string>`（ClickHouse `UInt64`，為保留 64 位元精確度而以字串解碼，與 `fn.indexOf()` 一致）。可選的 `startPos` 必須是 `UInt*` 類型；請傳入 UInt 欄位，字面值則用 `fn.toUInt64(n)`；單獨的數值字面值會變成 `Int64`，ClickHouse 會拒絕。
- `fn.jsonExtract()`
- `fn.tuple()`
- `fn.arrayJoin()`
- `fn.tupleElement()`
- `fn.not()`

### 條件分支類型

條件函數的結果泛型已移除：`fn.if<TData>(...)` 和
`fn.multiIf<TData>(...)` 不再能編譯。如果其中一個分支的 SQL 類型已知是 `Float64` 或 `UInt64`，
兼容的安全整數字面值分支就會按上下文推斷成同一類型；`UInt64` 字面值也必須是非負數。類型不一致的運算式和原始 SQL
永遠不會自動轉型，所以每個值分支都要明確轉型。`.mapWith(...)`
只會定義 JavaScript 解碼器，不會改變 SQL 類型。尤其是非負安全整數備用值與
`fn.over(fn.rowNumber().toMixed())` 並列時，會編譯成 `UInt64`。
`fn.if(...)` 和 `fn.multiIf(...)` 是標量函數，不能直接傳入
`fn.over(...)`；值分支仍然可以是視窗選取項目。

如果無法證明所有值分支都是同一個 SQL 類型，結果類型資訊會維持未知，解碼會直接傳遞原值。
因此現有結果可能由 `number` 變成傳輸層的 `string`。遷移時，請把每個值分支
明確轉成同一個 SQL 類型，再按需要套用 `.mapWith(...)`。
自動推斷結果類型刻意只限於精確的 `Bool`、`Int64`、
`UInt64`、`Float64` 和 `String` 這些基準類型。其他類型即使分支類型資訊一致，也仍會原樣
傳遞，因為 ClickHouse 可能把相同儲存類型正規化（例如 `LowCardinality(String)` 會變成 `String`）。

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

`fn.coalesce(...)` 不再按上下文把備用值推斷成 `Float32`、`BFloat16`、
`Decimal`，或目標範圍無法證明安全的 `bigint`。請把這些備用值明確轉換：

```ts
fn.coalesce(amount, fn.toDecimal64("0.00", 2));
```

### 正則表達式替換

`fn.replaceRegexpAll(haystack, pattern, replacement)` 會產生 ClickHouse 的
[`replaceRegexpAll`](https://clickhouse.com/docs/sql-reference/functions/string-replace-functions#replaceregexpall) 呼叫。
它會返回 `Selection<string>`；如果輸入具類型而且可為 NULL，則會返回 `Selection<string | null>`。
要處理 RE2 的反斜線和捕獲群組替換，請使用 `String.raw`：

```ts
const usDate = fn.replaceRegexpAll(
  orders.dateText,
  String.raw`^(\d{4})-(\d{2})-(\d{2})$`,
  String.raw`\2/\3/\1`,
);
// Selection<string>
```

這些參數會繫結成查詢參數。模式使用 ClickHouse RE2 語法，替換文字支援 `\0`–`\9`，
無效模式仍會由 ClickHouse 報錯。無類型的 `ckSql` 輸入會保守地當作可為 NULL；
如果確定片段不是 NULL，請寫 `ckSql<string>`。

### 具類型的元組聚合

`fn.tuple()` 會保留每個元素的類型。只有一個值的 `groupArray` 寫法會保留該元組的類型和
解碼器；只有一個參數的 `fn.arrayReverseSort()` 也會保留相同的陣列元素類型：

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

原有的明確指定泛型寫法仍可用來處理原始或動態情況。

陣列輔助工具的名稱沿用 ClickHouse [陣列函數](https://clickhouse.com/docs/sql-reference/functions/array-functions)參考資料中的標準標題。只提供別名的函數名稱仍可透過 `fn.call(...)` 使用，不用在公開 API 中重複加入它們。

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

`fn.call(name, ...)` 和 `fn.withParams(name, ...)` 會在編譯前驗證 `name` 是否為 SQL 識別碼。`ck.has(...)`、`ck.hasAll(...)`、`ck.hasAny(...)` 和 `ck.hasSubstr(...)` 是適合放在 WHERE 子句中的判斷條件簡寫，對應 ClickHouse 的同一組函數。

`fn.jsonExtract(json, returnType, ...path)` 只接受 `ckType.*` 返回類型，讓 ClickHouse 的返回類型和 TypeScript 解碼器保持一致：

以下 JSON 和陣列片段假設虛構的 `probeTelemetry` 模式還有 `metadata: JSON` 和 `tags: Array(String)` 欄位。

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

路徑片段是 ClickHouse JSON 路徑參數。物件欄位使用字串鍵，陣列位置則使用 number 或 bigint 索引：

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

高階陣列函數沿用 ClickHouse 的參數順序。用 `ckSql` 寫 lambda，把外層模式欄位插值進去，lambda 內部變量則保留為不加引號的 SQL 名稱。由 lambda 控制元素結構的函數（例如 `fn.arrayMap(...)`、`fn.arrayFilter(...)` 和 `fn.range(...)`）預設會返回 `Selection<unknown[]>`；如果查詢需要更具體的解碼後元素類型，請接上 `.mapWith(...)`。

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

在這個 lambda 中，`${params.timeColumn}` 和 `params.scopeTable.startTsList` 是來自模式的運算式。`start_ts` 和 `end_ts` 是 ClickHouse 根據陣列元素建立的 lambda 內部變量。

`fn.arrayJoin(array)` 對應 ClickHouse `arrayJoin`，會把一列輸入資料展開成每個陣列元素各一列。兩個陣列必須按位置配對時，請配合 `fn.arrayZip(...)` 和 `fn.tupleElement(...)` 使用：

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

關鍵在於：`arrayZip(sampleIds, probeIds)` 會保留按位置配對的值，`arrayJoin(...)` 會把這些配對展開成資料列，而 `tupleElement(..., 1/2)` 會讀取每個配對的兩邊。

複合鍵要用元組做成員判斷。兩個獨立的 `IN` 判斷條件會產生笛卡兒積，可能會配對到從未要求的組合：

```ts
// Avoid this when sampleId and probeId must stay paired.
const wrong = ck.and(
  ck.inArray(probeTelemetry.sampleId, sampleIds),
  ck.inArray(probeTelemetry.probeId, probeIds),
);
```

要按配對關係比較，就把一個元組與會產生元組的子查詢或配對清單比較。如果元素類型已知，`fn.tuple` 會帶有各元素的類型，所以清單會繫結成一個 `Array(Tuple(...))` 參數：

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  [
    ["900001", "probe_alpha"],
    ["900002", "probe_beta"],
  ],
);
```

原始 SQL 片段沒有元素類型。請先用 `fn.cast(fn.tuple(...), "Tuple(Int32, String)")` 包裝元組，再放進 `ck.inArray` 或 `ck.eq`。

如果右側是會產生元組的子查詢，左側可以沿用同一個運算式，而且不用傳入參數：

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  scopedSamples,
);
```

陣列輔助工具會返回具類型的 `Selection` 值，所以可以在投影、篩選、分組和 JOIN 中組合使用：

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

`fn.count()`、`fn.countIf(predicate)` 和 `fn.uniqExact(expression)` 是 `db.count(...)` 在聚合查詢中的對應函數，同樣提供 3 種模式。當計數放在查詢內部（`select`、`having`、GROUP BY 或子查詢），而不是查詢最外層的標量時，這些函數就很實用。它們共用相同的 SQL 包裝和解碼器，所以 3 種模式的取捨也一樣：

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

解碼器會拒絕任何不是非負整數的值；布爾值、`NaN`、小數和負數都會擲回 `Failed to decode count() result: ...`，所以伺服器返回損壞資料時會立即失敗，不會默默產生 `0`。

其他聚合函數刻意不採用這套模式 API；它們對精確度的考量和 `count` / `uniqExact` 不同：

- `fn.sum()` / `fn.sumIf()` 會按輸入欄位選用安全的解碼器（整數和 Decimal 欄位用字串，浮點數欄位用數值），並自動提高 Decimal 精確度；如果明確需要 `number`，可以鏈式呼叫 `.mapWith(Number)`。
- `fn.avg()` 永遠返回 `Selection<number>`，因為 ClickHouse 內部以 Float64 執行 `avg(Decimal)`；把它包裝成字串會錯誤反映實際執行路徑。
- `fn.min()` / `fn.max()` 保留輸入欄位的 TS 類型（因此 `min(int64Col)` 是 `Selection<string>`，與欄位的讀取結構一致）。

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

## 資料表與物化視圖
<!-- src:03694c38 -->

`createTable()`、`createMaterializedView()` 和 `dropTable()` 各自送出一條由模式物件產生的 DDL 語句。它們不比較模式、不執行 `ALTER`，也不記錄版本；既有資料表的遷移留在遷移工具裡。客戶端和工作階段上都可以呼叫。它們建立的資料表和視圖是持久的，`runInSession()` 不會刪除它們。

### `createTable()`

`createTable(table, options?)` 送出由 `ckTable(...)` 定義渲染出的 `CREATE TABLE` 語句：先是帶有 DDL 元資料的欄位，然後是 `ENGINE`、`PARTITION BY`、`PRIMARY KEY`、`ORDER BY`、`SAMPLE BY`、`TTL`、`SETTINGS` 和 `COMMENT`。

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

- 預設情況下，已有同名資料表時語句會失敗，錯誤名稱是 `TABLE_ALREADY_EXISTS`
- `mode: "if_not_exists"` 渲染 `CREATE TABLE IF NOT EXISTS`，既有資料表保持原樣，即使欄位定義不同也不改
- `mode: "or_replace"` 渲染 `CREATE OR REPLACE TABLE`，會取代既有資料表並捨棄其中的資料
- 沒有 `engine` 的資料表會使用 `MergeTree`，並帶上 `ORDER BY tuple()`
- 來自 `ckAlias(...)` 的別名資料表會被拒絕

### `createMaterializedView()`

`createMaterializedView(name, { to, as, mode? })` 建立一個由插入觸發的物化視圖，寫入既有資料表 `to`。`as` 是 `select()` 建構器，規則與 `insert(to).fromSelect(...)` 相同：必須投影 `to` 的每一個必填欄位；未知鍵或類型不符是 TypeScript 錯誤。

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

- ClickHouse 按欄位名稱把物化視圖的輸出寫入 `to`，所以 ck-orm 把 select 包成 `select ... as <column> from (...)`，讓每個鍵落到 `to` 中對應的欄位
- `as` 裡的查詢參數在建立視圖時被替換，因此存下來的視圖持有這些值
- select 強制的設定，例如 `leftJoin()` 的 `join_use_nulls = 1`，會寫進視圖的 `SETTINGS` 子句，因為請求設定不會隨視圖保存
- 視圖只能看到建立之後插入的資料列；要回填，用同一個 select 執行 `db.insert(to).fromSelect(...)`
- `mode: "if_not_exists"` 渲染 `IF NOT EXISTS`。ClickHouse 25.8 和 26.3 沒有 `CREATE OR REPLACE MATERIALIZED VIEW`，所以沒有 `or_replace` 模式
- `to` 是必填的：自帶 `ENGINE` 的視圖、`POPULATE` 和可刷新的視圖都不支援；這些情況用 `ckSql` 配合 `command()`

### `dropTable()`

`dropTable(table, options?)` 刪除一張資料表或一個視圖。可以傳入模式物件，也可以傳入名稱：

```ts
await db.dropTable("usage_minutes_mv", { sync: true });
await db.dropTable(usageMinutes, { ifExists: true, sync: true });
```

- 預設渲染 `DROP TABLE name`；沒有這個名稱的物件時，以 `UNKNOWN_TABLE` 失敗
- `ifExists: true` 渲染 `IF EXISTS`
- `sync: true` 渲染 `SYNC`：呼叫在 ClickHouse 刪掉資料之後才返回，而不是把刪除留給 `Atomic` 數據庫引擎的延遲清理
- 刪除物化視圖後，它的 `to` 資料表以及已經寫入的資料列都還在
- 來自 `ckAlias(...)` 的別名資料表會被拒絕，也不會渲染 `ON CLUSTER`；在叢集上，使用遷移工具，或用 `ckSql` 配合 `command()`

## 工作階段與暫存資料表
<!-- src:8f031227 -->

ClickHouse 不提供傳統交易。做特定範圍的分析或大型篩選工作時，綁定工作階段的暫存資料表通常是合適的基本工具。

在工作階段回呼函數內，`ck-orm` 提供以下方法：

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

如果你用自行控制的指令建立暫存資料表，但仍想讓 `ck-orm` 在工作階段清理時刪除它，請使用 `registerTempTable(name)`：

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

只有在資料表定義由開發者控制而且可信，並且無法用 `ckTable(...)` 表達時，才使用 `createTemporaryTableRaw(name, definition)`：

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

`definition` 參數從資料表名稱後面開始。不要加入 `CREATE TEMPORARY TABLE` 或資料表名稱；ck-orm 會負責產生這些內容，並另外驗證識別碼。

### 工作階段行為

- 指向同一個 `session_id` 的請求會共用工作階段並行控制器；客戶端預設值為 `session_max_concurrent_requests = 1`
- ClickHouse 的實際工作階段在伺服器端是互斥的，所以把 `session_max_concurrent_requests` 調高至 `1` 以上只會停用本地序列化，仍可能出現 `SESSION_IS_LOCKED`
- `createTemporaryTable()` 會自動登記資料表，供清理時使用
- `createTemporaryTable()` 會接收模式物件；暫存資料表的生命週期由 `Session` 管理，不屬於模式本身
- `createTemporaryTableRaw()` 是只接受可信資料表定義的原始 SQL API，並會拒絕多語句定義
- `runInSession()` 會在每項請求（包括清理操作）自動把解析後的 `session_id` 放在 `X-ClickHouse-Replica-Tag` 標頭中傳送；這個標頭由執行階段管理，不能透過 `http_headers` 覆寫
- ClickHouse Cloud 的[副本感知路由](https://clickhouse.com/docs/products/cloud/features/infrastructure/replica-aware-routing)必須另外啟用，而且效果只屬盡力而為；擴容可能會重新映射標記，跨來源瀏覽器部署也必須在 CORS 設定中允許 `X-ClickHouse-Replica-Tag`
- `runInSession()` 會在回呼結束後刪除已登記的暫存資料表
- 嵌套 `runInSession()` 呼叫永遠會建立新的子工作階段
- 嵌套呼叫不得重用任何仍在使用的祖先 `session_id`
- 嵌套子工作階段不會與父層共用暫存資料表或同一工作階段的並行請求額度，因為它們永遠使用不同的 `session_id`

### 工作階段並行契約

實務規則如下：

- 使用同一個明確 `session_id` 的請求，預設會序列化執行
- 客戶端預設的 `session_id` 也會使用同一個工作階段並行控制器
- `withSettings()` 建立的子客戶端如果指向同一個 `session_id`，會與父客戶端共用同一個工作階段並行控制器
- 不同的 `session_id` 不會互相阻塞
- `stream()` 和建構器 `iterator()` 會一直佔用同一工作階段的額度，直到迭代器完成。提早停止（`break`、`return` 或發生錯誤）時，會在工作階段外送出 `KILL QUERY` 取消伺服器端查詢（使用相同登入憑證和副本標記），然後等待 ClickHouse 釋放工作階段，最多等待 `request_timeout`（預設 30 秒）
- 嵌套 `runInSession()` 呼叫會建立全新的子 `session_id`，所以不會與父層共用同一工作階段的額度

中止請求或 `request_timeout` 觸發時，只會關閉連線。ClickHouse 會繼續執行查詢，直到查詢完成或察覺連線中斷；所以 `INSERT ... SELECT` 仍可能完成，而同一工作階段的下一個請求也可能因 `SESSION_IS_LOCKED` 失敗。你可以把 `clickhouse_settings` 中的 `max_execution_time` 設為低於 `request_timeout`，讓伺服器先停止；也可以在工作階段外執行 `KILL QUERY WHERE query_id = '...'`，或包裝 `fetch`，對請求主體可以重新傳送的請求有限次數重試錯誤碼 373。清理失敗（包括 `SESSION_IS_LOCKED`）會透過 `onCleanupError` 或 `SessionError` 回報。

建議用法：

- 暫存資料表工作流程和任何需要持續使用同一工作階段的邏輯，都保留 `session_max_concurrent_requests = 1`
- 如果需要真正並行，請使用不同的 `session_id`，不要調高同一工作階段的上限
- 只有你明確要移除本地背壓，而且可以接受 `SESSION_IS_LOCKED` 時，才調高 `session_max_concurrent_requests`

## 執行階段方法
<!-- src:3e31db64 -->

除了建構器，客戶端也會公開較低階的執行階段方法。

每次查詢常用的欄位包括：

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

`session_timeout` 和 `session_check` 刻意放在這裡：它們描述特定請求或特定工作階段區塊，不是客戶端的全域預設值。
`query_id`、`session_id` 和 `session_timeout` 是請求或工作階段選項，請放在 `clickhouse_settings` 外面。

`session_check` 不會建立新的 ClickHouse 工作階段。如果要讓 ClickHouse 確認明確的 `session_id` 已經存在，就使用它。
任何帶有 `session_id` 的請求，都會受同一個工作階段並行控制器限制，包括原始的 `execute()`、`command()`、`stream()`、建構器的 `.execute()`，以及建構器的 `.iterator()`。

每次請求傳入的 `clickhouse_settings` 只會覆寫該次請求的 ClickHouse 設定：

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

執行原始查詢，返回 `Record<string, unknown>[]`：

```ts
const rows = await db.execute(ckSql`SELECT 1 AS one`);
```

### `stream()`

以串流方式返回原始查詢結果：

```ts
for await (const row of db.stream(ckSql`SELECT number FROM numbers(10)`)) {
  console.log(row);
}
```

如果 `stream()` 指向某個 `session_id`，同一工作階段的額度會在非同步迭代器完成後釋放；如果提早跳出迴圈，關閉迭代器也會釋放額度。

### `command()`

執行不會返回結果集的指令：

```ts
await db.command(ckSql`SYSTEM FLUSH LOGS`);
```

### `withSettings()`

建立帶有額外預設設定的子客戶端：

```ts
const reportDb = db.withSettings({
  max_execution_time: 60,
  join_use_nulls: 0,
});
```

`withSettings()` 只會改變 ClickHouse 工作階段和查詢的預設設定。返回的客戶端會沿用父客戶端的傳輸、身分驗證、逾時和工作階段並行控制器。`join_use_nulls: 0` 字面值會被推斷，因此子客戶端的 LEFT JOIN 不會帶有 NULL；如果值的類型只是一般的 `number`，結果仍然會得到 `| null`。

在 `runInSession()` 中，工作階段生命週期的選項仍與 ClickHouse 設定分開：

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

### 整合已編譯查詢

大多數應用程式應直接使用建構器。如果要整合其他已經產生 `CompiledQuery` 的層，才使用 `executeCompiled()` 和 `iteratorCompiled()`。

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

## 可觀測性
<!-- src:8021e8d9 -->

`ck-orm` 內建 3 層可觀測性：

- `logger`
- `tracing`
- `instrumentation`

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

### 追蹤

```ts
import { clickhouseClient } from "ck-orm";
import { trace } from "@opentelemetry/api";

const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123/telemetry_lab",
  tracing: {
    tracer: trace.getTracer("ck-orm-example"),
    includeStatement: false,
    includeRowCount: true,
  },
});
```

追蹤會從 `clickhouseClient(...)` 的配置取得數據庫名稱、伺服器地址、伺服器連接埠和請求逾時。把服務層級標籤傳入 `tracing.attributes`；以 `db.` 開頭的鍵會被忽略，讓應用程式碼無法覆寫內建的數據庫屬性。

共用追蹤後端時，`includeStatement: false` 是較安全的設定。函式庫預設值為 `true`；如果不希望資料表名稱、欄位名稱或查詢結構離開服務邊界，就把它關掉。繫結值不會嵌入精簡後的 SQL 語句，但 SQL 結構本身在實際運作中仍可能屬於敏感資訊。

### 自訂插裝

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
  databaseUrl: "http://127.0.0.1:8123/telemetry_lab",
  instrumentation: [instrumentation],
});
```

公開的事件類型：

- `ClickHouseORMQueryEvent`
- `ClickHouseORMQueryResultEvent`
- `ClickHouseORMQueryErrorEvent`
- `ClickHouseORMQueryStatistics`
- `ClickHouseORMTracingOptions`
- `ClickHouseORMLogLevel`

內建事件欄位包括 `databaseName`、`serverAddress`、`serverPort`、`requestTimeoutMs`、`statementHash`、`querySummary`、`queryId` 和 `sessionId`；如果 ck-orm 不用從原始 SQL 猜測就能識別單一資料表，也會包括 `tableName`。非串流 JSON 查詢在伺服器返回統計資料時，還會包括 ClickHouse 回應統計資料：`serverElapsedMs`、`readRows`、`readBytes`、`resultRows` 和 `rowsBeforeLimitAtLeast`。

## 錯誤模型
<!-- src:b4b93db0 -->

公開的錯誤類型（只作類型匯出）：

- `ClickHouseORMError`
- `DecodeError`

執行階段錯誤檢查：

- `isClickHouseORMError(error)`
- `isDecodeError(error)`

`ClickHouseORMError` 會盡量保留錯誤脈絡，包括：

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

伺服器端日誌要保留完整錯誤脈絡，再向應用程式呼叫端返回精簡的公開錯誤：

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

不要把 `responseText` 或原始錯誤 `message` 返回不受信任的客戶端。ClickHouse 的錯誤文字可能包含 SQL 片段和物件名稱。

## 保安
<!-- src:c2dbb125 -->

`ck-orm` 的預設設計會刻意令不安全的 SQL 難以建立。

內建保護包括：

- 傳入 `ckSql.identifier()` 的識別碼會先驗證，再加引號
- `fn.call(...)`、`fn.withParams(...)`、`ckType.aggregateFunction(...)` 和 `ckType.simpleAggregateFunction(...)` 使用的函數名稱會先驗證；`AggregateFunction` 參數必須是字串或數值字面值
- 傳入 `ckType.nested({...})` 的嵌套欄位鍵會按 ClickHouse 識別碼驗證
- 插入 `` ckSql`...` `` 的值會成為 ClickHouse 具名參數，而不是原始 SQL 文字
- 以 `orm_param` 開頭的 `query_params` 鍵會被拒絕，因為這個前綴保留給內部 SQL 模板參數
- 每個請求只允許一個頂層語句
- 由連線配置產生的授權標頭不能被呼叫端提供的 `http_headers` 覆寫
- 結構化連線配置會拒絕內嵌在 `host` 的登入憑證
- 追蹤目的地資料會先標準化，再附加到 span

### 只供可信內容使用的 API

對於可信、但無法用 `ckTable(...)` 表達的暫存資料表 DDL，`ck-orm` 會在 `runInSession()` 內公開一個限定範圍的原始 SQL API：

- `session.createTemporaryTableRaw(name, definition)`

`fn.call(name, ...)` 和 `fn.withParams(name, ...)` 會驗證 `name`，但動態選取的函數名稱仍應視為開發者控制的輸入，而不是不受信任的執行階段輸入。

`ckSql.identifier(...)` 會在編譯／執行邊界驗證。建立 SQL 片段的成本很低；片段編譯成查詢時才會出現錯誤。

### 追蹤資料暴露

`tracing.includeStatement` 預設為 `true`，所以精簡後的 SQL 文字會以 `db.statement` 和 `db.query.text` 附加到 span。

繫結值會另外以 ClickHouse 具名參數儲存，不會放進語句文字；不過資料表名稱、欄位名稱和查詢結構仍會出現在追蹤輸出。如果你的追蹤後端是共用的，或部署在主機以外，請停用 `includeStatement`，或在收集器中過濾 `db.statement` / `db.query.text`。

### 錯誤回應

`ClickHouseORMError.responseText` 和錯誤 `message` 可能包含來自 ClickHouse 伺服器的原始文字，包括 SQL 片段和數據庫物件名稱。

不要把這些細節直接轉發給不受信任的客戶端。請在伺服器端記錄它們，再提供一般訊息。

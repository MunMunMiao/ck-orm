## インストール
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

ck-ormのピア依存関係は`@opentelemetry/api`1つです。Bun、npm、pnpmでは自動で入り、Yarnではコマンドに明示的に追加します。

## クイックスタート
<!-- src:d7ba4246 -->

以下の例では、流れを追いやすくするため、1つのテーブルだけを使います。

### 1. スキーマを定義する

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

### 2. クライアントを作成する

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

### 3. データをクエリする

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

ビルダーのクエリはthenableなので、`await query`だけでそのまま実行できます。

`Decimal`カラムに対する`fn.sum`は`Decimal(P, S)`へ自動的にキャストし、精度を保つために`string`を返します。[式でのDecimal精度](#decimal-precision-in-expressions)を参照してください。

## 例
<!-- src:c8f5c20b -->

[使用例ページ](https://ck-orm.lyz.cloud/examples/)では、実行可能なクエリと、それぞれがClickHouseへ送信する正確なSQLを確認できます。各例は[`examples/`](../examples)内に個別のプロジェクトとして置かれ、それぞれにテストがあります。実際にClickHouseで実行するテストは[`e2e/`](../e2e)にあります。[`e2e/scenarios.ts`](../e2e/scenarios.ts)には、本番環境と同じ構成のシナリオスキーマも含まれます。

## メンタルモデル
<!-- src:f7815577 -->

`ck-orm`は、次の点を押さえて使います。

- `ckType.*`でスキーマのカラム型を定義します。
- `ckTable(...)`でテーブルのスキーマを定義します。
- `ck.*`はクエリヘルパーの名前空間です。
- `fn.*`はSQL関数ヘルパーの名前空間です。
- スキーマはテーブルとカラムを表しますが、データベース名は表しません。
- データベース接続は`clickhouseClient(...)`で扱います。
- ビルダーのクエリが基本の使い方で、ClickHouse固有の式には生SQLも使えます。
- `runInSession()`はClickHouseのセッションヘルパーであり、トランザクションではありません。
- `leftJoin()`はデフォルトでSQLと同じNULLの扱いを使います。
- 桁数が大きい、または精度を失うおそれのある数値結果は、暗黙に変換せず、精度を保ってデコードします。

`ck-orm`は、型付きのClickHouseアクセスを中心に、テーブル定義、クエリの組み立て、書き込みヘルパー、セッションヘルパー、オブザーバビリティを扱います。本番環境のDDLとスキーマのマイグレーション処理は、従来どおりアプリケーション側のマイグレーションツールで扱います。

## スキーマDSL
<!-- src:6809e7a9 -->

### `ckTable()`

テーブルは`ckTable(name, columns, options?)`で定義します。

このセクションの例では、次のimportを前提にします。

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

第3引数には、プレーンオブジェクトかファクトリ関数を渡せます。

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

公開されているテーブルオプションは次のとおりです。

- `engine`
- `partitionBy`
- `primaryKey`
- `orderBy`
- `sampleBy`
- `ttl`
- `settings`
- `comment`
- `versionColumn`

カラム定義には、DDLメタデータを直接持たせることもできます。

- `.default(expr)`
- `.materialized(expr)`
- `.aliasExpr(expr)`
- `.comment(text)`
- `.codec(expr)`
- `.ttl(expr)`

スキーマメタデータには、`ck-orm`で2つの役割があります。

- 型付きクエリ、挿入時の検証、結果のデコードを支えます。
- セッション用一時テーブルの構造化DDLを生成できます。

ck-ormは、本番環境のClickHouseテーブルを自動でマイグレーションも同期もしません。本番環境のDDLはマイグレーションツールで管理し、`ckTable(...)`はアプリケーションが読み書きするスキーマと一致させてください。

例を示します。

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

### 型推論

すべてのテーブルは、次の2つを公開します。

- `table.$inferSelect`
- `table.$inferInsert`

```ts
type TelemetryRow = typeof probeTelemetry.$inferSelect;
type TelemetryInsert = typeof probeTelemetry.$inferInsert;

const sampleId: TelemetryRow["sampleId"] = "900001";
const ingestVersion: TelemetryInsert["ingestVersion"] = "1";
```

ジェネリックヘルパーでは、次の型を使います。

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

`InferSelectSchema`と`InferInsertSchema`に渡すスキーマオブジェクトは、ただのTypeScriptの型の集まりです。モデル型を共有するときに便利で、クライアント設定とは独立しています。

### `$type<T>()`でカラム型を絞り込む

すべてのカラムビルダーには、チェーン可能な`$type<T>()`メソッドがあります。ランタイムの動作を変えずに、カラムのTypeScript型を上書きします。デフォルトで推論される型が、扱う値の範囲に対して広すぎるときに使います。

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

`$type<T>()`は、ほかのどのカラム修飾子とも、順序を問わずチェーンできます。たとえば`ckType.string().$type<Role>().default(ckSql`'guest'`).comment("…")`のようにします。ランタイムのデコーダー、エンコーダー、SQL型、DDL出力には影響しません。`$type`はコンパイル時の型アサーションだけです。

`enum8`と`enum16`では、値オブジェクトからリテラルのユニオンが自動で推論されます。そのため、`$type`が必要なのは、そのユニオンにブランドを付けるか、外部の列挙型に合わせるときだけです。

```ts
// Inferred automatically — no `as const`, no explicit type arguments
const status = ckType.enum8({ active: 1, paused: 2, banned: 3 });
//    ↑ Enum8<"active" | "paused" | "banned">

// Re-brand on top of the inferred keys
type UserStatus = "active" | "paused" | "banned";
const userStatus = ckType.enum8({ active: 1, paused: 2, banned: 3 }).$type<UserStatus>();
```

> **注意点**：`$type<T>()`はランタイムの値を検証しません。データベースの行に宣言した型に収まらない値がある場合（たとえば、スキーマのマイグレーションより前の古いデータ）でも、TypeScriptは一致するものとして型を絞り込みます。網羅的な`switch`でも、そのケースを黙って取りこぼします。読み取り時に値を検証するなら、プロジェクションで`mapWith`を使います。デコードとエンコードのたびに検証するなら、後述の`$validator()`を使います。

### `$type<{ select, insert }>()`で挿入時と取得時の型を分ける

ClickHouseのカラムには、取得時より挿入時に広い型を受け付けるものがあります。たとえば`DateTime`カラムは、挿入時に`string`と`Date`の両方を受け付けますが、デコード後は常に`Date`を返します。`$type`へオブジェクトを渡すと、この2つの型を分けられます。

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

`$type<{ select: T }>()`（`insert`を指定しない場合）は、ジェネリックを1つだけ取る`$type<T>()`と同じです。

### 挿入モデルの規則：DEFAULT、MATERIALIZED、ALIAS

`$inferInsert`は、`$inferSelect`をそのままコピーしたのではなく、スキーマのカラムの形から導かれます。

| DDL修飾子 | 挿入モデルへの影響 |
| --- | --- |
| `.default(expr)` | カラムがオプショナルになり、省略するとClickHouseが値を埋めます |
| `.materialized(expr)` | カラムは挿入モデルから完全に外れます |
| `.aliasExpr(expr)` | カラムは挿入モデルから完全に外れます |

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

### `$validator(schema)`による実行時検証

`$type`だけでは足りない場合があります。データベースに古いデータが残っている、信頼できない上流からJSONの形を受け取る、挿入APIで文字列を`Date`へ変換したい、といったケースでは、代わりに`$validator(schema)`をチェーンします。[Standard Schema v1仕様](https://github.com/standard-schema/standard-schema)を満たすオブジェクトなら、どれでも使えます。Zod 3.23+、Valibot 1+、ArkType、Effect Schema、TypeBoxも使えます。ck-ormは、これらのライブラリのどれにも依存しません。

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

スキーマの変換（たとえば`z.string().transform((v) => new Date(v))`）も反映されます。カラムは、挿入時にスキーマの*入力*型を受け付け、取得時には*出力*型を返します。`$validator`は同期スキーマ専用で、非同期のスキーマはデコード・エンコード時に例外を投げます。

`$type`と`$validator`の比較：

| | `$type<T>()` | `$validator(schema)` |
| --- | --- | --- |
| 実行時のコスト | ゼロ | 1行につき各カラムで同期スキーマ検証を1回実行 |
| 型安全 | コンパイル時の型アサーション | スキーマから推論 |
| 実行時検証 | なし | あり（一致しないと例外を投げます） |
| 入力型と出力型を分ける場合 | `$type<{ select, insert }>()`を使う | スキーマ変換を使う |
| 外部依存 | なし | なし（スキーマライブラリは自由に選べます） |

### JSONカラム型

ck-ormが対象にするのは、ClickHouse 24.x以降の**新しいJSONデータ型**（[ドキュメント](https://clickhouse.com/docs/sql-reference/data-types/newjson)）です。こちらは、JSONオブジェクトを型付きサブカラムへ物理的に分けて、値を不透明な文字列として保存しません。最初に、次の2つを押さえてください。

- ClickHouseの`JSON`データ型が受け付けるのは**トップレベルのオブジェクトだけ**です。トップレベルの配列、文字列、数値は受け付けません。ck-ormも同じ制約を、コンパイル時の`T extends Record<string, unknown>`と、各INSERT・SELECTで行う軽量なO(1)チェックで適用します。
- 型付きパス（`a.b UInt32`）は、ClickHouse内で実際のサブカラムになります。ck-ormはこれらのパスを対応するck-ormカラムファクトリの`mapFromDriverValue` / `mapToDriverValue`に通します。そのため、`ckType.uint64()`と宣言したパスは、トップレベルの`uint64`カラムと同様に、精度を保つ`string`へデコードされます。

いちばん簡単な形は、ほかのck-ormカラムファクトリと同じです。

```ts
import { ckTable, ckType } from "ck-orm";

const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json("payload"),
});
// DDL:  `payload` JSON
// $inferSelect.payload: { [key: string]: unknown }
```

ジェネリック型を渡すと、実際の形が`$inferSelect` / `$inferInsert`に現れます。

```ts
type EventPayload = { user_id: number; event: string; meta?: { ip: string } };

const userEvents = ckTable("user_events", {
  id: ckType.uint64(),
  payload: ckType.json<EventPayload>("payload"),
});
```

ジェネリック型をほかのカラムビルダー呼び出しと同じ場所に置きたいなら、チェーン可能な`ckType.json<T>().$type<T2>()`で書けます。

#### パラメーター付きDDL

ファクトリの第2引数で、NewJSONのDDL設定4つを指定できます。

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

`typeHints`のキーは`Paths<T>`に対して型チェックされます。`{ "user.idd": ... }`のようなキー名のタイプミスは、コンパイル時にエラーになります。生成されるDDLは常に一定の辞書順で出力されます。そのため、設定の記述方法が違っても、外部のスキーマ差分ツールから見た出力はバイト単位で同一です。

#### `typeHints`がランタイムのデコードに与える影響

ClickHouseは64ビット値の精度を保つ方針なので、ck-ormでは`UInt64`を文字列へデコードします。TypeScript上の形と`typeHints`の項目の*両方*で`user_id`を宣言する場合は、2つを揃えてください。`uint64`なら、ジェネリックに`user_id: string`と書きます。挿入時に受け付ける値と、取得時にデコードされる型が異なる場合は、`$type<{ select, insert }>()`を使います。

```ts
ckType.json("payload", { typeHints: { user_id: ckType.uint64() } })
  .$type<{
    select: { user_id: string };
    insert: { user_id: string | number | bigint };
  }>();
```

#### パスアクセス

すべてのJSONカラムには、ClickHouseのパスアクセス構文をそのまま生成するチェーン可能なパスメソッドが5つあります。

| メソッド | SQL | 意味 |
| --- | --- | --- |
| `col.path("a.b")` | `col.a.b` | パスを読みます。静的な型は`PathValue<T, "a.b">`です（後述の`typeHints`の注記を参照） |
| `col.castPath("a.b", ckType.uint64())` | `col.a.b.:UInt64` | 別のck-ormカラムファクトリを使って、パスを強制的にデコードします。静的な型は、キャスト先カラムの`TData`から決まります |
| `col.subobject("a")` | `col.^a` | ネストしたオブジェクトを`JSON`値として読みます |
| `col.merged("a")` | `col.@a` | マージ済み共有データビューを読みます |
| `col.arrayPath("a")` | `col.a[]` | 配列型のパスを展開します |

> `col.path(P)`の型は常に`PathValue<T, P>`です。型はジェネリックの`T`から決まり、`typeHints`からは決まりません。`typeHints`が変えるのはランタイムのデコーダーだけです。静的な型とtypeHintは手動で揃えてください（精度を保つ`uint64 → string`の方針は前述のとおりです）。両者が食い違う場合は、代わりに`castPath(P, ckType.X())`を使ってください。戻り値の型は、キャスト先カラムから決まります。

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

`Paths<T>`は、パスのリテラルをコンパイル時に検証します。`"a.zzz"`が`T`にない場合、`payload.path("a.zzz")`は型エラーになります。

パスメソッドは、`$type`、`$validator`、`.default()`、`.materialized()`、`.bind()`などのチェーン修飾子を通過するたびに、ランタイムで付け直されます。そのため、どのリンクでも呼び出せます。**静的な型は、`$validator`を除く各チェーンの段階でパスメソッドを追跡します**。ただし、`$validator`の契約は`StandardSchemaV1<unknown, unknown>`に対するジェネリックです。TypeScriptが守るLSP風のサブタイプ規則を壊さずに`JsonShape`まで安全に絞れないため、このリンクの後では静的な型が`Column<T>`に戻ります。メソッド自体はまだ呼べるので、先でチェーンを続ける必要があるときは型アサーションを使ってください。

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

#### `fn.json*`：動的なパスの呼び出し

パス文字列をランタイムで組み立てる場合や、`T`に宣言されていないパスを読みたい場合は、`fn.*`の名前空間を使います。

```ts
import { fn } from "ck-orm";

const userPath = userInput as string;
await db.select({
  raw:    fn.jsonPath<string>(userEvents.payload, userPath),
  casted: fn.jsonCast(userEvents.payload, userPath, ckType.string()),
  shape:  fn.dynamicType(fn.jsonPath(userEvents.payload, userPath)),
});
```

`fn.jsonCast`は`castPath`の動的な版です。`fn.jsonSubobject`、`fn.jsonMerged`、`fn.jsonArray`は、対応するカラムメソッドと同じ動きをします。`fn.dynamicType`は、Dynamic型パスのランタイム上の種類を調べるClickHouseの`dynamicType()`関数を提供します。

#### 実行時検証

各行では、**次の順序**で2つのレイヤーが動きます。

1. **ck-ormの組み込みJSONガード**。`null` / `undefined`は変更せずに通すため、`nullable()`のJSONカラムもそのまま扱えます。`null` / `undefined`を除くオブジェクト以外の入力（配列、文字列、数値、真偽値）はすべて受け付けず、`JSON column expects a plain object, got array`のようなカラム単位の`client_validation` / `decode`エラーになります。`typeHints`の型付きパスは、入出力の際に対応するカラムファクトリのエンコーダー・デコーダーに通します。このガードは常に有効です。
2. **オプションの`$validator(schema)`**。指定すると、挿入時は組み込みエンコーダーの*前*、取得時は組み込みデコーダーの*後*に実行します。オブジェクト全体の形を検証するときはこれを使います。

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

ck-ormは意図的にJSON値を再帰的に**調べず**、`T["a"]`が本当に`number`かどうかなどは確認しません。それが必要な場合は`$validator`を使ってください。JSONガードの役割は、ClickHouse自身のエラーメッセージをカラムへ対応づけにくいケースを拾うことだけです。トップレベルのスカラー引数では、カラム名を含むエラーが投げられます。

```ts
await db.insert(events).values({ payload: ["wrong"] as never });
// throws: JSON column expects a plain object, got array
```

#### 挿入時のデフォルト値

JSONカラムも、ほかのカラムと同じ`$inferInsert`の規則に従います。`.default()`はフィールドをオプショナルにし、`.materialized()` / `.aliasExpr()`は挿入モデルから完全に外します。

```ts
const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json<{ note: string }>("payload").default(ckSql`'{}'`),
});

await db.insert(auditLogs).values({ id: "1" }); // payload filled by CH DEFAULT
```

### `ckAlias()`

1つのクエリで同じテーブルを複数回使う必要があるときは、`ckAlias()`を使います。

```ts
import { fn, ck, ckAlias } from "ck-orm";

const telemetry = ckAlias(probeTelemetry, "telemetry");
```

`ckAlias()`が返すカラムは、自動的にそのエイリアス用に結び直されます。

### カラム名

スキーマオブジェクトのキーは、TypeScript上の行、デコード後のクエリ結果、挿入値で共通する論理キーです。デフォルトでは、同じキーをClickHouseのカラム名にも使います。

```ts
const telemetry = ckTable("telemetry", {
  signalStrength: ckType.decimal({ precision: 20, scale: 5 }),
});
```

データベース側のカラム名が異なる場合は、その物理カラム名を第1引数に渡します。

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

SQL、DDL、フィルター、並び替え、グループ化、書き込み時のカラムリストでは物理名（`probe_id`、`signal_strength`、`created_at`）を使います。型推論で得られるモデル、デフォルトの取得結果、明示的なプロジェクションのキー、挿入値では、スキーマオブジェクトのキー（`probeId`、`signalStrength`、`createdAt`）を使います。

どの公開`ckType`ビルダーでも、型設定の外側に物理カラム名を指定できます。追加設定のないビルダーは`name?`を受け付けます。型設定を持つビルダーでは、オプショナルな物理カラム名を先に置き、型設定をオブジェクトに渡します。

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

`aggregateFunction`と`simpleAggregateFunction`にはClickHouse固有の呼び出し形式があります。最初の文字列は、カラム名ではなく集約関数名です。

```ts
ckType.aggregateFunction("sum", ckType.uint64());
ckType.aggregateFunction("quantile(0.5)", ckType.float64());
ckType.simpleAggregateFunction("sum", ckType.uint64());
```

`AggregateFunction`は`quantile(0.5)`や`topK(10)`のように、リテラルのパラメーターを含むClickHouseの集約関数名を受け付けます。`simpleAggregateFunction`はより厳密で、リテラルのパラメーターを含まない集約関数名を使います。

物理カラム名も指定する場合は、オブジェクト設定を使います。

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

`nested("items", shape)`は、外側の`Nested(...)`カラムに名前を付けます。ネスト内のフィールド名は`shape`のキーです。ネスト内のフィールド名には、内側のカラムの`configuredName`の値を使いません。

```ts
const sensorPackets = ckTable("sensor_packets", {
  items: ckType.nested("items", {
    componentId: ckType.string(),
    quantity: ckType.float64(),
  }),
});
```

### カラムビルダー

スキーマのカラムビルダーには`ckType.*`を使います。スキーマDSLは、ClickHouseでよく使う型ファミリーをカバーします。

- 整数：`int8`、`int16`、`int32`、`int64`、`uint8`、`uint16`、`uint32`、`uint64`
- 浮動小数点数とDecimal：`float32`、`float64`、`bfloat16`、`decimal`
- スカラー型：`bool`、`string`、`fixedString`、`uuid`、`ipv4`、`ipv6`
- 日時・時刻型：`date`、`date32`、`dateTime`、`dateTime64`、`time`、`time64`
- 列挙型と特殊型：`enum8`、`enum16`、`json`、`dynamic`、`qbit`
- コンテナー型：`nullable`、`array`、`tuple`、`map`、`nested`、`variant`、`lowCardinality`
- 集約型：`aggregateFunction`、`simpleAggregateFunction`
- ジオメトリ型：`point`、`ring`、`lineString`、`multiLineString`、`polygon`、`multiPolygon`

`int64`と`uint64`は、スキーマを基にした読み取り・書き込みと推論モデルでは、デフォルトでTypeScriptの`string`です。ClickHouseのJSONワイヤ形式とJavaScriptランタイムをまたいでも、64ビット値を正確に保つためです。明示的に`bigint`を使う場合は、`mapWith((value) => BigInt(String(value)))`のようなデコーダーを自分で指定して選びます。

ClickHouseは`Nullable(Array(...))`、`Nullable(Map(...))`、`Nullable(Tuple(...))`をサポートしません。`ck-orm`は、スキーマを定義する時点ではこれらの形を受け付けません。代わりに`nullable(...)`を複合型の内側に置いてください。たとえば`ckType.array(ckType.nullable(ckType.string()))`です。

型設定を持つビルダーではオブジェクト設定を使い、オプショナルな物理カラム名を先に置きます。

```ts
ckType.decimal({ precision: 20, scale: 5 });
ckType.decimal("signal_strength", { precision: 20, scale: 5 });
ckType.fixedString({ length: 8 });
ckType.dateTime64("created_at", { precision: 9, timezone: "UTC" });
ckType.qbit("embedding", ckType.float32(), { dimensions: 8 });
```

`Date`/`Date32`への書き込みと、スキーマに従う述語値では、JavaScriptの`Date`をUTCの暦日として送信します（`2026-06-15T23:00:00Z`は`2026-06-15`に変換されます）。別の日を保存するには`YYYY-MM-DD`文字列を渡します。文字列と数値はそのまま送信します。

`DateTime`/`DateTime64`の述語値では、JavaScriptの`Date`をUTCの時点としてバインドするため、カラム、セッション、サーバーのタイムゾーンによって、その日時はずれません。文字列は、書かれたとおりの日時として、カラムのタイムゾーンでClickHouseが解釈します。`DateTime`の32ビット位置では、そのパラメーター内のDateがすべて秒ちょうどなら、`DateTime('UTC')`としてバインドされます。パラメーター内のDateにミリ秒成分が1つでもあれば、そのパラメーターは`DateTime64(3, 'UTC')`としてバインドされます。`DateTime64`の位置は、常に`DateTime64(3, 'UTC')`としてバインドされます。`gte`などのスカラー述語では、正確な時点のまま比較します。`DateTime`カラムの`inArray`/`notInArray`では、Dateは秒ちょうどである必要があります。ClickHouseは、そこでミリ秒成分を含むDateを`TYPE_MISMATCH`として受け付けません。これはClickHouse本来の動作です。そもそも、そのような値が等しくなることはありません。`insert().values()`は、各`Date`をカラムの精度に合わせた整数ティックとして送信します。書き込みのセクションを参照してください。

### カラム型の実例集

よく使うスキーマの形と、対応するTypeScriptの値は次のとおりです。

| ClickHouseの型構成 | スキーマ | TypeScriptの値の形 |
| --- | --- | --- |
| 列挙型 | `ckType.enum8({ idle: 1, active: 2 })` | `"idle" | "active"`（キーから推論） |
| 低カーディナリティの文字列 | `ckType.lowCardinality(ckType.string())` | `string` |
| NULL許容のDecimal | `ckType.nullable(ckType.decimal({ precision: 18, scale: 5 }))` | `string | null` |
| 配列 | `ckType.array(ckType.string())` | `string[]` |
| NULL許容の配列要素 | `ckType.array(ckType.nullable(ckType.string()))` | `(string | null)[]` |
| タプル | `ckType.tuple(ckType.string(), ckType.int32())` | `[string, number]` |
| マップ | `ckType.map(ckType.string(), ckType.string())` | `Record<string, string>` |
| ネストしたオブジェクトの配列 | `ckType.nested({ component: ckType.string(), quantity: ckType.float64() })` | `{ component: string; quantity: number }[]` |
| バリアント型 | `ckType.variant(ckType.string(), ckType.int32())` | `string | number` |
| JSON | `ckType.json<{ risk?: { score?: number } }>()` | `{ risk?: { score?: number } }` |

`ckType.map(...)`は現在、`String`キーだけをサポートし、マップ全体をJavaScriptのレコードに対応付けます。そのため、ClickHouseの`Map(K, V)`で起こり得る重複キーのエッジケースは表現できません。

挿入する行の形は、`typeof table.$inferInsert`から推論される形と同じです。ただし、ClickHouseのデフォルト値や生成式を持つカラムは、`insert(table).values(...)`を呼び出すときに省略できます。生成される`INSERT`のカラムリストには、`MATERIALIZED`カラムと`ALIAS`カラムは含まれません。これらを明示的に渡すと、受け付けられません。

## クライアント設定
<!-- src:0d752985 -->

`clickhouseClient()`でクライアントを作成します。

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:<password>@127.0.0.1:8123/telemetry_lab",
});
```

### 接続モード

`clickhouseClient()`は、互いに排他的な2つの接続方法をサポートします。

#### `databaseUrl`

1つの接続文字列にまとめたいときは、`databaseUrl`を使います。

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:secret@127.0.0.1:8123/telemetry_lab",
});
```

`databaseUrl`を指定したときは、次のフィールドを一緒に渡さないでください。

- `host`
- `database`
- `username`
- `password`
- `pathname`

#### 接続情報の個別指定

各接続情報を個別に設定するときは、フィールドを明示します。

```ts
const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  username: "default",
  password: "<password>",
});
```

構造化モードのデフォルト値は次のとおりです。

- `host`: `http://localhost:8123`
- `database`: `default`
- `username`: `default`
- `password`: `""`

### 共通フィールド

多くのプロジェクトで必要なクライアントフィールドは一部だけです。

| フィールド | 用途 |
| --- | --- |
| `request_timeout` | リクエストごとに指定できる実時間ベースのタイムアウト（ミリ秒）。省略すると、ck-ormはクライアント側のタイムアウトを設定しません。リクエストの寿命は、下層のfetch / platform / server / `abort_signal`が制御します。長く実行されるストリーミングクエリでは、ここを未設定にし、代わりに`abort_signal`で実時間の上限を設定してください（例：`AbortSignal.timeout(30_000)`）。 |
| `clickhouse_settings` | ClickHouseのセッション・クエリ設定のデフォルト値 |
| `application` | ClickHouseアプリケーション名の設定 |

テーブル定義は接続設定とは独立しています。`ckTable(...)`でテーブルを定義し、クエリを組み立てる場所でそのテーブルオブジェクトをインポートして、`.from(...)`、`.insert(...)`、一時テーブル用ヘルパーに直接渡します。

### 詳細フィールド

高度なランタイム動作には、次のフィールドを使います。

| フィールド | 用途 |
| --- | --- |
| `fetch` | すべてのHTTPリクエストで`(request: Request) => Response \| Promise<Response>`を呼び出します。fetch実装、サーバーハンドラー、[`examples/decimal-sum/main.test.ts`](../examples/decimal-sum/main.test.ts)のテスト用疑似実装のいずれでも使えます。デフォルトはプラットフォームの`fetch`です。どのHTTPリクエストでも呼び出されるハンドラーとして利用できます。 |
| `http_headers` | 追加するデフォルトヘッダー |
| `role` | デフォルトのClickHouseロール |
| `session_id` | デフォルトのセッションID |
| `session_max_concurrent_requests` | このクライアント内で同じ`session_id`に対して同時に実行できるリクエスト数の上限（デフォルトは`1`） |
| `compression.response` | デフォルトの`false`では、ストリーミングリクエストは`enable_http_compression = 0`を送り、行はClickHouseが生成した順に届きます。通常のリクエストはサーバーのデフォルトを維持します（ClickHouse 26.3以降では、クライアントが対応する場合にレスポンスを圧縮します）。`true`はすべてのレスポンスを圧縮します。データが少ないストリームでは、圧縮ブロックが埋まるまで届くのが遅れることがあります。 |
| `logger` / `logLevel` | ロガー連携 |
| `tracing` | OpenTelemetry連携 |
| `instrumentation` | カスタムクエリライフサイクルフック |

セッションの寿命を決める設定は、意図的にリクエスト単位にしています。`session_timeout`は単一クエリか`runInSession(...)`に渡します。`session_check`は、新しいセッションを開始するときではなく、既存の`session_id`を引き継ぐときに使ってください。
`session_max_concurrent_requests`は異なります。同じ`session_id`を対象にした重複リクエストを絞り込む、クライアント単位のガードです。
実際のClickHouseセッションはサーバー側でロックされたままなので、`session_max_concurrent_requests`を`1`より大きくすると、同じセッションでの本当の並行実行ではなく`SESSION_IS_LOCKED`が返る可能性があります。
ローカルでの直列化を意図的に外し、サーバー側のセッションロック失敗を処理する準備がある場合を除き、デフォルトの`1`を使ってください。

### ClickHouse設定

`clickhouse_settings`はClickHouseのセッション・クエリ設定だけに使います。ClickHouseの[セッション設定](https://clickhouse.com/docs/operations/settings/settings)で説明されているのと同じ種類のキーで、HTTP APIではクエリパラメーターとして受け付けます。`host`、`database`、`request_timeout`、`http_headers`、`session_max_concurrent_requests`などのck-ormクライアント設定とは別です。

`query`、`database`、`session_id`、`role`、`param_*`などのHTTPトランスポートのフィールドは、`clickhouse_settings`の外に置いてください。これらのキーはリクエストの組み立てと名前付きパラメーターの経路に属するため、ck-ormが個別に検証します。

公式の設定キーはTypeScriptの補完に対応します。新しいClickHouseバージョンや導入環境固有の設定では、任意のキーも有効です。

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

JSONのparse/stringifyの動作はfetchトランスポートが管理します。公開クライアント設定では、実行環境をまたいで同じ動作を維持し、`json`の上書きは公開しません。

ck-ormは、すべてのリクエストでワイヤ契約に必要な少数の設定を強制します。具体的には、`http_write_exception_in_output_format = 0`、`output_format_json_quote_64bit_integers = 1`、`output_format_json_quote_decimals = 1`、`date_time_output_format = "iso"`です。呼び出し側の設定がこれらの値と衝突する場合、ck-ormは警告を1回だけログに記録し、強制した値を送信します。`max_threads`などのワークロード・パフォーマンス設定は、引き続き呼び出し側が制御します。

### 認証

`ck-orm`はデータベース接続にBasic認証を使います。

- `databaseUrl`モードでは、URLに資格情報を埋め込めます
- 構造化モードでは、`username`と`password`を使います
- 資格情報を指定しない場合、デフォルトはパスワードが空の`default`です

## クエリビルダー
<!-- src:5152d6ff -->

以下のコード例では、`db`、`probeTelemetry`、参照するヘルパーは`ck-orm`からインポート済みとします。

### `select()`

選択項目を明示すると、結果の形も明確になります。

```ts
const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
    signalStrength: probeTelemetry.signalStrength,
  })
  .from(probeTelemetry)
  .limit(10);
```

プロジェクションオブジェクトは、公開されている`Selection`の値やカラムを使って組み立てます。つまり、テーブルのカラムも、`fn.*(...)`や`ck.expr(...)`の出力も、`select({ ... })`の中では同じように組み合わせられます。

結合がなければ、暗黙の選択でテーブルモデル全体が返ります。

```ts
const rows = await db.select().from(probeTelemetry).limit(10);
```

結合がある場合、暗黙の選択はソースごとにフィールドをまとめ、ネストしたオブジェクトを返します。左結合で一致しなかったグループは`null`になります。ClickHouseは`Array` / `Map`カラムではNULLの代わりに`[]` / `{}`を返すため、この判定では空の配列やマップも値がないものとして扱います。スカラー値がすべてNULLで、配列とマップも空の一致行は、見分けがつかないため同じく`null`になります。

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

### `where()`と条件ヘルパー

公開されている条件ヘルパーは次のとおりです。

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

`.where(...predicates)`は、可変長引数を`AND`で結合します。`undefined`の述語オブジェクトは無視されるため、複数の述語をそのまま渡したり、`ck.and(...)`や`ck.or(...)`でグループ化した述語を作ったりできます。

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

値のリストは、1つの`Array(T)`クエリパラメーターとして送信されます（`col in {p:Array(T)}`）。リストのテキストが約40,000文字を超える場合は、`arrayConcat(...)`でつないだ複数のパラメーターに分割します。プライマリキーとスキップインデックスによるデータの絞り込みには影響しません。ClickHouse 26.3以降は、HTTPリクエストごとにクエリパラメーターを最大1,000個まで受け付けます。また、25.8のアナライザーは、合計サイズが約1 MBを超えるリストを受け付けません。もっと大きな集合には一時テーブルか外部データを使ってください。SQL式を含むリストや、型指定のない値で型が混在するリストでは、引き続き要素ごとに1つのパラメーターを使います。

`ck.and(...)`は`undefined`を無視するので、コード内で動的なフィルターを組み立てやすくなります。

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

`undefined`が無視されるのは、述語そのものの場合だけです。述語の値は厳密に扱われ、`ck.eq(column, undefined)`はクエリの範囲を暗黙に広げず、例外を投げます。単独の`null`、`false`、`true`、`0`、`""`は述語ではありません。代わりにSQL式を使ってください。`null`はNULL許容カラムへの書き込みに使える有効な値ですが、NULLで絞り込むには条件を明示する必要があります。

```ts
db.select()
  .from(probeTelemetry)
  .where(ck.isNull(probeTelemetry.deletedAt));
```

真偽値カラムは、通常の値としてもSQL式としても扱えます。

```ts
const healthChecks = ckTable("health_checks", {
  id: ckType.int32(),
  isPassing: ckType.bool("is_passing"),
});

db.select().from(healthChecks).where(ck.eq(healthChecks.isPassing, false));
db.select().from(healthChecks).where(healthChecks.isPassing);
```

単独の`null`を通常の比較や`IN`リストの中に入れないでください。NULLの条件は明示的に組み立ててください。

```ts
ck.or(
  ck.inArray(probeTelemetry.deletedAt, [new Date("2026-04-21T00:00:00.000Z")]),
  ck.isNull(probeTelemetry.deletedAt),
);
```

実行時に組み立てるフィルターが大きい場合は、`Predicate[]`と可変長引数の`.where(...predicates)`を組み合わせるのがおすすめです。

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

`Predicate`は、再利用できる真偽値のSQL句を表す公開名です。同じ述語オブジェクトを、`where`、`having`、結合の`on`句、`ck.exists(...)`のような真偽値対応ヘルパーで使えます。

`Selection`は、`fn.sum(...)`、`fn.toString(...)`、`ck.expr(ckSql...)`など、ビルダーで計算した値を再利用するための公開名です。`.as(...)`でエイリアスを付け、`.mapWith(...)`でデコード方法を上書きできます。`Order`は、`ck.asc(...)`と`ck.desc(...)`が返す順序指定のオブジェクトです。

`ck.has(...)`、`ck.hasAll(...)`、`ck.hasAny(...)`はネイティブのClickHouse関数に直接対応し、ClickHouse本来の配列、マップ、JSONの扱いをそのまま保ちます。左辺が配列カラムの場合、通常のJavaScript値はいったんそのカラムでエンコードされます。そのため、`Array(Date)`のフィルターでは、クエリパラメーターにする前に、そのカラムのDateエンコードが使われます。

`where(...)`は可変長引数を受け取りますが、`having(...)`は述語を1つだけ受け取ります。`having`で複数の条件を使う場合は、先に`ck.and(...)`か`ck.or(...)`で1つの述語にまとめます。

`ck.contains(...)`、`ck.startsWith(...)`、`ck.endsWith(...)`と、それぞれの`*IgnoreCase`版は、入力をリテラル文字列として扱います。値はパラメーター化し、LIKEのワイルドカード文字（`%`、`_`、`\`）は内部でエスケープします。

パターンとして解釈したい場合だけ、`ck.like(...)` / `ck.ilike(...)`を使ってください。これらのAPIでもSQLの安全性のために値をパラメーター化しますが、LIKEはパターンを扱うため、`%`と`_`はワイルドカードとして機能します。

リテラル文字列の検索例：

```ts
import { ck } from "ck-orm";

const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
  })
  .from(probeTelemetry)
  .where(ck.contains(probeTelemetry.probeId, "probe_alpha%"));
```

パターン検索の応用例：

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

`groupBy()`と`limitBy([...])`には、カラムや`fn.*(...)`、`ck.expr(...)`などのヘルパーが返す計算済みの`Selection`値を渡せます。

`limit()`、`offset()`、`limitBy(..., limit)`には、0以上の安全な整数を指定してください。ClickHouseの定数式を意図的に使うときだけ`ckSql`を使ってください。

`orderBy()`には次のいずれかを渡せます。

- `ck.desc(selection)`
- `ck.asc(selection)`
- カラムを直接渡す

### `final()`

テーブルクエリに、テーブルレベルの`FINAL`を付けます。

```ts
const query = db.select().from(probeTelemetry).final();
```

エイリアスなしで単純にテーブルを読む場合、`ck-orm`は`FROM table FINAL`を出力します。ルートテーブルにエイリアスがある場合や、クエリに別のソースを結合する場合は、`ck-orm`は最終化済みのテーブルをサブクエリで包み、外側のソースにエイリアスを残します。これにより、`FINAL`、テーブルエイリアス、結合、ラムダ式を組み合わせたときのClickHouseアナライザーの境界ケースを避けながら、同じビルダーAPIをそのまま使えます。

`final()`はテーブルのルートソースにだけ適用されます。CTE、サブクエリ、テーブル関数の処理の中で`FINAL`が必要なら、`.as(...)`を呼ぶ前に、テーブルを元にしたクエリへ`.final()`を付けてください。

### `limitBy()`

ClickHouseの`LIMIT ... BY ...`を使います。

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

### 実行モード

ビルダーのクエリは、3通りの方法で実行できます。

```ts
const query = db.select().from(probeTelemetry).limit(10);

const rows = await query;
const sameRows = await query.execute();

for await (const row of query.iterator()) {
  console.log(row);
}
```

アプリケーション例で実行箇所を目で追いやすくしたいときは、`.execute()`を使います。直接`await query`する書き方も、Drizzle風の使い勝手のためにサポートしています。チームがビルダーのクエリをthenableとして扱うことに慣れれば、この書き方も便利です。

`query.iterator()`には`db.stream()`と同じ、セッションを考慮した並行性ルールが適用されます。クエリが`session_id`を対象にする場合、反復処理が終わるか、イテレーターを途中で閉じるまで、そのスロットは空きません。

### `toSQL()`でSQLを確認する

`toSQL()`は、ビルダーを何も送信せずにコンパイルします。ビルダーをawaitすると実行されてしまうので、awaitする前のビルダーそのものに対して呼び出してください。`select()`のビルダーのほか、`insert().values()`と`insert().fromSelect()`でも使えます。

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

戻り値には次のフィールドがあります。

- `sql`は1行にまとめたステートメントで、値が入る位置には`{orm_param1:String}`のようなプレースホルダーが入ります。`FORMAT`句は含まれず、クエリを送信するときに追加されます。
- `params`は、プレースホルダーに対応する生のJavaScript値を保持します。クエリの実行時には、SQLとは別にエンコードして送信されるので、`sql`に埋め込まれることはありません。
- `forcedSettings`は、ビルダーがリクエストに強制する設定の一覧です。たとえば`leftJoin()`の`join_use_nulls: 1`があります。強制する設定がなければ、このフィールドはありません。
- `body`は`insert().values()`でのみ設定されます。行をValues形式のテキストにしたもので、`sql`には`INSERT INTO t (cols) FORMAT Values`のステートメントが入ります。それ以外では、このフィールドはありません。

戻り値はコピーなので、変更してもビルダーには影響しません。

ログやデバッグ向けに`sql`と`body`を複数行にしたいときは、`{ pretty: true }`を渡します。

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

整形で変わるのは空白だけです。句ごとに改行し、`select`の項目は1行に1つずつ並べ、サブクエリとCTEはインデントします。`and`や`or`のグループに含まれる条件も、1行に1つずつです。関数の引数は同じ行のままで、クォートされたテキストには一切手を加えません。SQLコメントを含むステートメントは、整形せずそのまま返します。`insert().values()`のボディでは、挿入する行も1行に1つずつ並べます。整形が、実際に送信されるクエリに影響することはありません。

### サブクエリとCTE

`.as("alias")`を使うと、ビルダーを名前付きのサブクエリにできます。

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

サブクエリの`.as("…")`は**省略できます**。呼ばなければ、フレームワークがコンパイル時に自動エイリアス（`__sub_1`、`__sub_2`、…）を割り当てます。
エイリアスなしのビルダーは、`from()`、`innerJoin()`、`leftJoin()`、`inArray()`、`notInArray()`、`exists()`、`notExists()`で直接使えます。

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

#### `.as(name)`とエイリアスなしの使い分け

| 状況 | 推奨される形 |
|---|---|
| 再利用しない、使い捨てのインラインサブクエリ | エイリアスなしのビルダーを使い、`innerJoin/leftJoin`ではコールバック形式にします。`inArray/exists`にはエイリアスなしのビルダーをそのまま渡します |
| サブクエリのカラム参照を複数の場所で使う（SELECT/WHERE/1つのJOIN） | `const`に入れて、エイリアスなしで渡します。チェーンメソッドの先でもカラム参照を使えます |
| 自己結合（同じ論理サブクエリを2つのSQLエイリアスで使う） | `.as("a")`と`.as("b")`を明示的に呼びます。同じインスタンスをソースとして2回使うと、SQLにコンパイルするときに例外を投げます |
| 安定した、読みやすい、または型付きのエイリアスが必要（本番環境の`query_log`検索、`EXPLAIN`、スローログ、モジュールをまたぐヘルパー、リテラルな`TAlias`ソースキー） | `.as("descriptive_name")`を呼びます |

構造の異なるクエリ間では、自動エイリアスは**安定しません**。ただし、1回のコンパイル中と、同じビルダーツリーを繰り返しコンパイルしたときは安定します。そのため、`snapshot`形式のテストでも正規化は不要です。

#### Selectionキーの競合

SelectBuilderのメソッド名（`from`,
`where`, `select`, `as`, `innerJoin`, `leftJoin`, `groupBy`, `having`,
`orderBy`, `limit`, `offset`, `final`, `limitBy`, `execute`, `iterator`,
`then`, `catch`, `finally`, `buildSelectionItems`）と衝突するSelectionキーは、
エイリアスなしのビルダーではメソッドの型を保ちます。`sub.from`は、選択に
選択リストに`from`が含まれていても、常にビルダーのメソッドを返し、カラム参照にはなりません。
そのようなキーは名前を変えるか（例：`from` → `fromAddress`）、サブクエリを`.as("name")`で
包み、専用の`Subquery`オブジェクト経由でカラムにアクセスしてください。

CTEには`$with()`と`with()`を使います。

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

名前を指定せずに`$with()`を呼ぶと、無名CTEを作ります。無名CTEは、1つのクエリにつき1回だけソースとして使えます。同じ結果を2回結合するなら、結合ごとに1つずつCTEを2つ定義してください（`db.$with().as(...)`を2回呼ぶか、それぞれ別の名前を付けたCTEを2つ作ります）。

### `db.count()`

Drizzle風のカウントには`db.count(source, ...predicates)`を使います。述語の扱いは`.where(...predicates)`と同じです。複数の述語を`AND`で結合し、`undefined`の述語オブジェクトはスキップします。ただし、`ck.eq(...)`などのヘルパーに値として`undefined`を渡すと、例外を投げます。

```ts
import { ck } from "ck-orm";

const total = await db.count(
  probeTelemetry,
  ck.eq(probeTelemetry.status, 1),
  ck.gt(probeTelemetry.id, 1000),
);
```

もっと複雑な結果セットでは、サブクエリやCTEをカウントします。

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

`db.count(...)`はデフォルトで数値を返すカウントモードです。`toFloat64(count())`を出力し、`number`にデコードします。非常に大きなカウントはJavaScriptの整数精度を超えることがあるため、正確な値や通信時の形式を保ちたい場合は、チェーン可能なモードを使ってください。

```ts
const approximateTotal = await db.count(activeProbes); // number
const exactTotal = await db.count(activeProbes).toSafe(); // string
const wireTotal = await db.count(activeProbes).toMixed(); // number | string
```

`.toSafe()`は`toString(count())`を出力し、正確な値を読むためのものです。安全なカウント値をSQL式として使うと、`String`として扱われます。SQLで数値比較をする場合は、デフォルトの動作、`.toUnsafe()`、または`.toMixed()`を使ってください。`.toMixed()`は`toUInt64(count())`を出力し、ドライバーから返る値の形式と通信時の形式を保ちます。ck-ormのデフォルトJSON設定では、64ビット整数の精度を保つため、実際のClickHouseレスポンスは通常`string`で届きます。

## 結合時のNULLの扱い
<!-- src:5554058b -->

`leftJoin()`は`join_use_nulls = 1`を自動で適用し、デフォルトではSQLと同じNULLの扱いを使います。

つまり、次のようになります。

- デフォルトの左結合では、右側がNULL許容として推論されます。
- `join_use_nulls`を明示的に無効にすると、推論される型も変わります。

ClickHouseのデフォルトの結合動作に合わせるには、リテラルの`0`を渡します。リテラルは型推論されるため、`as const`は不要です。変数から渡すなど、単なる`number`型の値でも、結果には`| null`が付きます。

```ts
const rawDefaultDb = db.withSettings({
  join_use_nulls: 0,
});
```

エイリアスなしのビルダー（`.as()`を呼ばないサブクエリ）または無名CTEを左結合すると、`FROM`側や内側結合したエイリアスなしのビルダーの参照を含め、それらのソースに由来するすべてのカラム参照の型に`| null`が付きます。これらの参照は、リテラル名を特定できない共通の`string`型ソースキーを共有するため、型だけではどのソースを左結合したかを区別できません。実行時は引き続き、左結合した側だけがNULLを返します。

```ts
const rows = await db
  .select({ uid: allUsers.uid, spent: totals.spent })
  .from(allUsers)
  .leftJoin(totals, ck.eq(allUsers.uid, totals.uid));
// uid and spent are both number | null
```

`FROM`側または内側結合したサブクエリには、`.as("name")`で名前を付けてください。こうすると、その参照はそれぞれのソースキーを保ち、NULL非許容のままになり、左結合した側だけが`| null`のままです。

結合クエリをサブクエリ、CTE、`ck.exists(...)`、`ck.inArray(...)`のソースとして再利用しても、強制される`join_use_nulls = 1`設定は保持されます。そのため、ビルダーの型は実行時の動作とずれません。

計算式は実行時にClickHouseのNULL伝播に従います。ClickHouseがNULLを返すと（入力がNULLの場合、または左結合した行が一致しない場合）、値は`null`です。そのTypeScriptの型は、宣言した`TData`（またはヘルパーの固定型）のままで、自動的にNULL許容型には広げられません。入力がNULLになる可能性がある場合は、`| null`を宣言する（例：`fn.plus<number | null>(...)`）か、入力を`fn.coalesce(...)`で包んでください。

## 書き込み
<!-- src:d7555d6e -->

### `insert(table).values(...)`

テーブルスキーマに沿う型付きINSERTを作るときは、ビルダーを使ってください。

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

行は、`INSERT INTO t (cols) FORMAT Values`リクエストとして1回で送信されます。ステートメントにはクエリパラメーターがありません。行はリクエストボディ内のValues形式のリテラルテキストなので、1,000パラメーターと値1つあたり128 KiBというHTTP制限は適用されません。省略したカラムには、その行だけ`DEFAULT`が使われます。このINSERTでは式の評価が無効になるため（`input_format_values_interpret_expressions = 0`と`input_format_values_deduce_templates_of_expressions = 0`）、各セルはデータとして扱われます。計算した値には`insert().fromSelect()`を使ってください。JavaScriptの`Date`は、その瞬間をカラムの精度まで切り詰めて送信します。`DateTime`カラムでは秒単位まで保ちます。`DateTime`または`DateTime64`カラムに数値を渡すとUnix秒として扱います。文字列はカラムのタイムゾーンで読み込まれます。

INSERTする行のキーは、テーブルスキーマにあるキーを使ってください。不明なキーは、送信前に受け付けません。

### `insert(table).fromSelect(select)`

`.fromSelect(...)`を使うと、`INSERT INTO target (cols) SELECT ...`ステートメントを1つにコンパイルし、SELECTのマテリアライズをClickHouseサーバー側で実行できます。行がNodeプロセスを通ることはありません。大きなソーステーブルを一度だけ対象範囲へ絞り込み、そのマテリアライズ結果を`runInSession`内の複数の後続クエリで再利用する用途に適しています。

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

ルール：

- SELECTでは、対象テーブルの必須カラムをすべてプロジェクションする必要があります。`.default(...)`で宣言したカラムは省略できます。必須カラムが不足している場合は、コンパイル時（TypeScript）と実行時の両方で受け付けません。
- INSERTのカラムリストは、SELECTのプロジェクションキーの順に生成されます。ClickHouseは位置でカラムを対応付けますが、呼び出し側には`name-aligned`の意味で見えます。どの値を対象カラムのどこに入れるかには、プロジェクションキーの順序は関係ありません。キー同士が対応付けられます。
- 対象テーブルにない不明なプロジェクションキーや、カラムごとの型不一致は、クエリを送る前にTypeScriptエラーとして現れます。
- 1つのinsertチェーンで`.values()`と`.fromSelect()`は同時に使えません。ビルダーの型が絞られるため、使わないメソッドは補完に出ず、実行時にも呼び出しを拒否します。
- コンパイル済みのワイヤーリクエストは、通常のClickHouseコマンドです。クエリイベントでは`mode: "insert"`、`tableName`には対象テーブルが設定されます。
- `.fromSelect()`が受け付けるのは`SelectBuilder`（`session.select({...}).from(...)`）で、サブクエリラッパーではありません。別のSELECT内で結果をCTEやサブクエリとして使いたいときだけ`.as("name")`を使い、`.fromSelect()`へ渡すときは使わないでください。

#### ネストしたカラム

`ckType.nested(...)`のカラムは、`.fromSelect()`とは次の3つのモードで組み合わせます。

1. **ネストのキーをプロジェクションから省略**します。これはデフォルトモードで、ck-ormの実行時の契約を反映しています。ネスト値がない場合はSQLの`DEFAULT`としてエンコードされ、ClickHouseが各フィールドの空の並列配列を埋めます。TSレイヤーでも、この動きに合わせて`$inferInsert`のネストカラムを`optional`として扱います。
2. **ネストカラム参照を直接プロジェクション**します。たとえば、`select({ events: src.events })`のように指定します。ck-ormは内側のSELECTをサブクエリで包み、外側のプロジェクションをフィールドごとのドットパスアクセス（`__ck_inner.events.name`、`__ck_inner.events.score`、…）に書き換えます。ソースのネスト構造には、対象のネストが期待するすべてのフィールドが必要です。
3. **ネストカラムへ計算式をプロジェクション**する操作は、コンパイル時に受け付けません。1つのSQL式を並列配列フィールドへ展開できないためです。行の値には`.values(...)`か`.insertJsonEachRow(...)`を使ってください。

TSレイヤーにネストデータを常に渡すルールを適用したい場合（`Pattern D`、空のネスト配列を許さないビジネス契約）は、`ckType.nested({...}).requiredOnInsert()`を有効にします。このチェーンは、`$inferInsert`の形で`events`を必須として示すカラムを返します。また、`.fromSelect()`では、ネストカラム参照のプロジェクションモードか、行単位の代替手段が必要です。

> **JSONのワイヤー形式について**：`.values()`は、JSONカラムをValuesボディ内で、クォートした`JSON.stringify(...)`テキストとして送信します。ClickHouseはそのリテラルをJSONカラムとして解析します。`.fromSelect()`はすべてサーバー側で実行するため、JSON値がJSプロセスを往復することはありません。どちらの経路でも、保存データは同じです。

### `insertJsonEachRow()`

オブジェクトの行を持っている場合や、非同期の行ストリームを使う場合は、`insertJsonEachRow()`を使ってください。

```ts
await db.insertJsonEachRow("tmp_scope", [
  { probe_id: "probe_alpha" },
  { probe_id: "probe_beta" },
]);
```

受け付けるものは次のとおりです。

- 文字列のテーブル名
- `ckTable()`で作ったテーブルオブジェクト
- 通常の配列
- `AsyncIterable`

空の通常配列は、クライアント側で何も実行しない扱いになります。それでも計装フックには、INSERTが成功し`rowCount: 0`だったライフサイクルが報告されます。不明なフィールドの扱いはClickHouseが決めます。サーバーに余分なJSONフィールドを無視させたいときは、`input_format_skip_unknown_fields: 1`などの設定を渡してください。ネストカラムは、フラット化された`column.field`配列（ClickHouseの`flatten_nested = 1`、デフォルト）として送信されます。そのため、`{ lines: [{ sku: "a" }] }`は`insert().values()`と`insertJsonEachRow()`のどちらでも使えます。

テーブルスキーマを使う場合、`insertJsonEachRow()`は、`DateTime`/`DateTime64`カラムのJavaScriptの`Date`を`insert().values()`と同じ正確な整数tickとして送信します。文字列の解釈は、ClickHouseのデフォルトパーサー`date_time_input_format = basic`に任せます。このため、日時の文字列はどちらでも同じ方法で読み込まれます。自分で`DateTime64`カラムへJSONの数値を渡した場合は、`JSONEachRow`ではtick、`.values()`ではUnix秒として読み込まれます。

`insertJsonEachRow("table_name", rows)`にテーブル名だけを渡すと、カラム型がありません。そのため、`Date`は`Z`付きのISO 8601で送信され、ck-ormはそのリクエストに`date_time_input_format = best_effort`を設定します。スキーマを持つテーブルの行に、スキーマ外のキーがある場合も同じです。ck-ormはそのキーを変更せずに渡すため、そこに`Date`を含める場合は、呼び出しに`clickhouse_settings: { date_time_input_format: "best_effort" }`が必要です。ClickHouse 25.8では、`best_effort`パーサーが小数秒つきの1970年より前の時刻を誤って読み、`12:34:56`のような`Time`文字列を受け付けません。スキーマで型を指定したカラムなら、この2つを避けられます。

## 生SQL
<!-- src:f47b381e -->

`ck-orm`にはSQLテンプレートAPIが用意されています。すでに書きたいSQLがあり、ビルダー構文にするとかえって遠回りなときに使ってください。

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

### `ckSql.join()`と`ckSql.identifier()`

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

### `query_params`を使う生SQL

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

パラメーターの送信方法は自動で選ばれます。`query_params`でmultipart処理を設定する必要はありません。

`orm_param`で始まる`query_params`キーは受け付けません。このプレフィックスは、`` ckSql`...` ``が内部で生成するパラメーター用に予約されています。

値フォーマッターは、プリミティブ値、`Date`、`NaN`、`Infinity`、配列、オブジェクト、`Map`値に対応します。`{ids:Array(UInt64)}`や`{attrs:Map(String, String)}`のようなClickHouseの型付きプレースホルダーでも使えます。`query_params`で渡す値にはそれ自身のカラム型がないため、配列は常にArrayのテキストとして送信されます。ここでは`Tuple(...)`プレースホルダーはサポートされません。配列内の64ビット整数は`bigint`にしてください。文字列はクォートされますが、数値の要素型では受け付けません。`Date`はUTCでの時刻テキストとして送信され、ClickHouseはプレースホルダー型のタイムゾーンで読みます。セッションやサーバーのタイムゾーンで時刻がずれないように、`{at:DateTime64(3, 'UTC')}`のように宣言してください。`` ckSql`...${date}` ``の補間では、これを自動で行います。パラメーターがテーブル名やカラム名の場合は、ClickHouseの`Identifier`プレースホルダー型を使ってください。

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

SQLフラグメントを再利用できる`Selection`として包むには、`ck.expr()`を使います。

```ts
import { ck, ckSql } from "ck-orm";

const query = db.select({
  constantOne: ck.expr(ckSql`1`).as("constant_one"),
});
```

フラグメントに型がある場合、`ck.expr()`はその型を保ちます。型なしのフラグメントは意図的に`unknown`のままです。スキーマの外で結果の型が分かるときは、ジェネリックかデコーダーを渡してください。

```ts
const status = ck.expr(ckSql<string>`'closed'`); // Selection<string>
const enabled = ck.expr<boolean>(ckSql`1`, {
  decoder: (value) => Number(value) === 1,
});
```

### 生クエリの出力形式

即時実行する生クエリがサポートする出力は`JSON`だけです。

```ts
const rows = await db.execute(ckSql`select 1`, {
  format: "JSON",
});
```

ストリーミングする生クエリがサポートする出力は`JSONEachRow`だけです。デフォルトの`compression.response: false`では、圧縮なしでストリームを送ります。そのため、ClickHouseが行を生成した順に1行ずつ届きます。

```ts
for await (const row of db.stream(ckSql`select 1`, {
  format: "JSONEachRow",
})) {
  console.log(row);
}
```

### 式でのDecimal精度

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

- `sum` / `sumIf`はPを38以上に広げ、`min` / `max`はカラムのPを保ちます。スキーマのDecimalカラムでは、`fn.sum()`と`fn.sumIf()`は`Selection<string>`です。Floatカラムは`Selection<number>`です。`nullable(decimal(...))`や`lowCardinality(decimal(...))`を通じても自動キャストが適用されます。
- `maxIf(value, condition)`は`max`と同じDecimal規則に従います。Decimal結果は、宣言した`Decimal(P, S)`の形を持つ`Selection<string>`です。
- `avg`では**自動キャストしません**。ClickHouseは`avg(Decimal)`をFloat64上で計算するため、`fn.avg`は`Selection<number>`を返します。正確なDecimal平均には、`ckSql.decimal(ckSql\`sum(x) / count(x)\`, P, S)`を使ってください。
- `fn.divideDecimal(left, right, resultScale?)`は、ClickHouseの`divideDecimal`に直接対応します。精度を失わないよう`Selection<string>`を返します。指定する場合、`resultScale`は`0`から`76`までのリテラル整数でなければならず、インラインで出力されます。ビルダーは、ClickHouse関数のバージョン確認も、関数のエミュレーションもしません。
- `column.cast(P, S)`がキャストするのはカラムで、集約ではありません。`GROUP BY`の中でそのまま使うと、`NOT_AN_AGGREGATE`エラーになります。`fn.sum(column)`を使うか、集約を包んでください。
- INSERTでは文字列・数値以外のオブジェクト（例：`decimal.js`のインスタンスをそのまま渡す場合）を受け付けません。`.toFixed(scale)`を渡してください。

```ts
db.insert(measurementLog).values({ value: new Decimal("1.23").toFixed(5) }); // ✅
db.insert(measurementLog).values({ value: new Decimal("1.23") as never }); // ❌ throws
```

## ウィンドウ式
<!-- src:6d0b3f76 -->

`fn.*`の関数式を、`fn.over(...)`で包みます。仕様は省略できますが、`partitionBy`にはビルダーの選択値だけ、`orderBy`には通常の`ck.asc(...)` / `ck.desc(...)`値だけを指定できます。

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

`fn.rowNumber()`は、デフォルトでは`toFloat64`を経由する、精度を保たない`Selection<number>`です。正確な`Selection<string>`にするには、`fn.over(...)`の前に`.toSafe()`を呼びます。ネイティブの`UInt64`トランスポート形（`Selection<number | string>`）にするには`.toMixed()`を使います。`.toUnsafe()`は明示的なデフォルトです。ビルダーは、JavaScriptの安全な整数チェックを意図的に追加していません。ClickHouseの`toFloat64`変換では、`Number.MAX_SAFE_INTEGER`を超える値が丸められることがあるためです。`.toSafe()`は正確な10進文字列を保ち、`.toMixed()`はドライバーのネイティブな結果の形を保ちます。Decimal集約は通常の文字列デコーダーを保ち、`CAST(sum(...) OVER (...) AS Decimal(...))`としてコンパイルされます。そのため、`OVER`はキャストの外ではなく集約の中に残ります。`fn.over`は、意図的にカラム単体や生SQLフラグメントを受け付けません。また、この最初のAPI公開範囲で対応するのは`PARTITION BY`と`ORDER BY`だけです。フレーム、名前付きウィンドウ、その他のClickHouse固有句には`ckSql`を使ってください。

## 関数とテーブル関数
<!-- src:1e16409b -->

### `fn`

ジェネリック、型変換、集約、JSON、タプル、テーブル関連のヘルパーを使えます。

- `fn.call()`
- `fn.withParams()`
- `fn.replaceRegexpAll()`は、型付きのClickHouse RE2置換です。詳しくは[正規表現置換](#regex-replacement)を参照してください。
- 型変換ヘルパーはClickHouseの型変換ページにある変換関数に対応しています。`fn.cast()`、`fn.date()`、`fn.accurateCast*()`、`fn.reinterpret*()`、`fn.parseDateTime*()`、`fn.formatDateTime()`、`fn.formatRow*()`、`fn.toString*()`、`fn.toBool()`、`fn.toInt*()`、`fn.toUInt*()`、`fn.toFloat*()`、`fn.toBFloat16*()`、`fn.toDecimal*()`、`fn.toDecimalString()`、`fn.toFixedString()`、`fn.toDate*()`、`fn.toDateTime*()`、`fn.toTime*()`、`fn.toTime64*()`、`fn.toInterval*()`、`fn.toLowCardinality()`、`fn.toNullable()`、`fn.toUUID*()`を使えます。
- 64ビット以上の整数変換（`toInt64/128/256*`、`toUInt64/128/256*`、Unixタイムスタンプ64ヘルパー）は`string`としてデコードし、64ビット未満の整数変換と浮動小数点変換は`number`としてデコードします。
- リテラルだけを受け取るClickHouse引数は、必要に応じて検証してインライン化します。対象はDecimalのスケール、DateTime64/Time64の精度、FixedStringの長さ、変換先の型文字列、間隔の単位です。
- `fn.toStartOfMonth()`
- `fn.toUnixTimestamp()` / `fn.toUnixTimestamp64Second()` / `fn.toUnixTimestamp64Milli()` / `fn.toUnixTimestamp64Micro()` / `fn.toUnixTimestamp64Nano()`
- `fn.fromUnixTimestamp()` / `fn.fromUnixTimestamp64Second()` / `fn.fromUnixTimestamp64Milli()` / `fn.fromUnixTimestamp64Micro()` / `fn.fromUnixTimestamp64Nano()`
- `fn.count()` / `fn.countIf()`は、デフォルトでは`toFloat64(count(...))`で包んだ`Selection<number>`です。`Selection<string>`（`toString(count(...))`）にするには`.toSafe()`、`Selection<number | string>`（`toUInt64(count(...))`）にするには`.toMixed()`をチェーンします。`.toUnsafe()`でデフォルトに戻せます。`db.count`と同じ動作です。デコーダーは0以上の整数だけを受け付け、`NaN`、負数、真偽値などを拒否します。例については[`fn.count` / `fn.uniqExact`のモード](#fncount--fnuniqexact-modes)を参照してください。
- `fn.sum()` / `fn.sumIf()` / `fn.min()` / `fn.max()` / `fn.maxIf()`は、Decimalカラムでは`Decimal(P, S)`へ自動的にキャストします。[式でのDecimal精度](#decimal-precision-in-expressions)を参照してください。
- `fn.over()` / `fn.rowNumber()`は、`PARTITION BY`と`ORDER BY`でウィンドウ関数を組み合わせます。[ウィンドウ式](#window-expressions)を参照してください。
- `fn.divideDecimal()`は、結果スケールを必要に応じてインライン指定できる、正確なDecimal除算です。[式でのDecimal精度](#decimal-precision-in-expressions)を参照してください。
- `fn.argMax()` / `fn.argMin()`は、`val`が最大または最小になる行の`arg`の値を返します。戻り値の型は`arg`カラムの型になります。DecimalのCASTで包むことはなく、ClickHouseがカラムの宣言型を保ちます。
- `fn.avg()`は`Selection<number>`（Float64）を返し、ClickHouseの`avg(Decimal)`の動作に合わせています。
- `fn.uniqExact()`は`fn.count()`と同じ3つのチェーン可能なモードを持ちます。デフォルトは`toFloat64(uniqExact(...))`で包んだ`Selection<number>`です。`Selection<string>`（`toString(uniqExact(...))`）には`.toSafe()`、`Selection<number | string>`（`toUInt64(uniqExact(...))`）には`.toMixed()`を使い、`.toUnsafe()`でデフォルトに戻せます。デコーダーも、同じように0以上の整数かどうかを確認します。[`fn.count` / `fn.uniqExact`のモード](#fncount--fnuniqexact-modes)を参照してください。
- `fn.coalesce()`
- `fn.if()` / `fn.multiIf()`は条件分岐です。ClickHouseがすべての値の分岐からサーバー側の結果型を決めるため、どちらも`Selection<unknown>`を返します。
- `fn.greatest()` / `fn.least()` / `fn.nullIf()`は値を比較します。`nullIf`はsqlTypeを自動的に`Nullable(...)`で包みます。NULL許容の入力なら、二重には包みません。
- `fn.positionCaseInsensitive()` / `fn.positionCaseInsensitiveUTF8()`は、大文字と小文字を区別しない部分文字列検索と、そのUTF-8版です。64ビットの精度を保つため、ClickHouseの`UInt64`を文字列としてデコードした`Selection<string>`を返します（`fn.indexOf()`と同じです）。`startPos`を指定する場合は`UInt*`型にしてください。UInt型のカラムか、リテラルなら`fn.toUInt64(n)`を渡します。数値リテラルをそのまま書くと`Int64`になり、ClickHouseは受け付けません。
- `fn.jsonExtract()`
- `fn.tuple()`
- `fn.arrayJoin()`
- `fn.tupleElement()`
- `fn.not()`

### 条件分岐の型

条件分岐の結果をジェネリック型で指定する機能は削除されました。そのため、`fn.if<TData>(...)`と`fn.multiIf<TData>(...)`はコンパイルできなくなりました。既知の`Float64`または`UInt64`のアンカーがあれば、互換性のある安全な整数リテラルの分岐は文脈に応じて型付けされます。`UInt64`のリテラルは0以上である必要があります。型が混在する式と生SQLは自動的にキャストされないため、値の各分岐を明示的に変換してください。`.mapWith(...)`が定義するのはJavaScriptデコーダーだけで、SQL型は変わりません。特に、`fn.over(fn.rowNumber().toMixed())`の隣に0以上の安全な整数リテラルを置くと、`UInt64`としてコンパイルされます。`fn.if(...)`と`fn.multiIf(...)`はスカラーなので、`fn.over(...)`には直接渡せません。ただし、値の分岐にはウィンドウ関数の結果を指定できます。

すべての値の分岐で同じSQL型を確定できない場合、結果メタデータは不明のままで、デコードでは値をそのまま渡します。そのため、既存の結果が`number`からトランスポート上の`string`へ変わる可能性があります。移行するには、値の各分岐を1つのSQL型に明示的に変換し、必要に応じて`.mapWith(...)`を適用します。自動で決まる結果メタデータは、正確な`Bool`、`Int64`、`UInt64`、`Float64`、`String`のアンカーだけに限定しています。分岐のメタデータが一致していても、他の型は変換せずそのまま渡します。ClickHouseが同じ格納型を正規化することがあるためです（たとえば、`LowCardinality(String)`は`String`になります）。

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

`fn.coalesce(...)`では、フォールバックを`Float32`、`BFloat16`、`Decimal`、または対象範囲の安全性を確認できない`bigint`として文脈から型推論しなくなりました。これらのフォールバックは明示的に変換してください。

```ts
fn.coalesce(amount, fn.toDecimal64("0.00", 2));
```

### 正規表現置換

`fn.replaceRegexpAll(haystack, pattern, replacement)`はClickHouseの[`replaceRegexpAll`](https://clickhouse.com/docs/sql-reference/functions/string-replace-functions#replaceregexpall)としてコンパイルされます。戻り値は`Selection<string>`です。型付きの入力がnullになり得る場合は`Selection<string | null>`になります。RE2のバックスラッシュとキャプチャ参照には`String.raw`を使ってください。

```ts
const usDate = fn.replaceRegexpAll(
  orders.dateText,
  String.raw`^(\d{4})-(\d{2})-(\d{2})$`,
  String.raw`\2/\3/\1`,
);
// Selection<string>
```

引数をクエリパラメーターとしてバインドします。パターンにはClickHouse RE2構文を使い、置換文字列では`\0`から`\9`までを使えます。不正なパターンはClickHouseのエラーのままです。型なしの`ckSql`入力は、安全側に倒してNULL許容として扱います。フラグメントがNULLにならないと分かっている場合は`ckSql<string>`と書いてください。

### 型付きタプルの集約

`fn.tuple()`は各要素の型を保ちます。引数1つの`groupArray`形式では、そのタプル型とデコーダーも保ちます。引数を1つだけ取る`fn.arrayReverseSort()`も、配列の要素型をそのまま保ちます。

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

生SQLや動的な処理では、ジェネリック型を明示する従来の形式も引き続き使えます。

配列ヘルパーの名前は、ClickHouseの[配列関数](https://clickhouse.com/docs/sql-reference/functions/array-functions)リファレンスにある正式な見出しにそろえています。エイリアスだけの名前も、公開APIに同じものを2つ追加せず、`fn.call(...)`から使えます。

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

コンパイル前に、`fn.call(name, ...)`と`fn.withParams(name, ...)`は`name`をSQL識別子として検証します。`ck.has(...)`、`ck.hasAll(...)`、`ck.hasAny(...)`、`ck.hasSubstr(...)`は、同じClickHouse関数をWHERE句で使いやすくした述語の短縮形です。

`fn.jsonExtract(json, returnType, ...path)`で指定できる戻り値の型は`ckType.*`だけです。ClickHouseの戻り値の型とTypeScriptデコーダーの対応関係を保てます。

次のJSONと配列を扱う例では、架空の`probeTelemetry`スキーマに`metadata: JSON`と`tags: Array(String)`のカラムもあるものとします。

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

パスの各要素はClickHouseのJSONパス引数です。オブジェクトのフィールドには文字列キーを、配列の位置にはnumberまたはbigintのインデックスを使ってください。

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

高階配列関数は、ClickHouseのパラメーターの順序に従います。ラムダ式には`ckSql`を使い、外側のスキーマのフィールドを補間し、ラムダ内のローカル変数は、補間せずSQLの名前としてそのまま書いてください。ラムダが要素の形を決める`fn.arrayMap(...)`、`fn.arrayFilter(...)`、`fn.range(...)`などの関数は、デフォルトで`Selection<unknown[]>`を返します。クエリでデコード後の要素型を絞る場合は、`.mapWith(...)`をチェーンしてください。

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

このラムダでは、`${params.timeColumn}`と`params.scopeTable.startTsList`はスキーマに基づく式です。`start_ts`と`end_ts`は、ClickHouseが配列の要素から作るラムダ内のローカル変数です。

`fn.arrayJoin(array)`はClickHouseの`arrayJoin`に対応し、入力の1行を配列の要素ごとに1行へ展開します。2つの配列の要素を同じ位置で対応させたい場合は、`fn.arrayZip(...)`と`fn.tupleElement(...)`を組み合わせてください。

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

動きはこうです。`arrayZip(sampleIds, probeIds)`は同じ位置の値を組にしたまま保ち、`arrayJoin(...)`は組を行に展開し、`tupleElement(..., 1/2)`は組の各要素を読み取ります。

複合キーでは、タプルの所属判定を使ってください。独立した`IN`述語を2つ書くと直積になり、要求していない組にも一致することがあります。

```ts
// Avoid this when sampleId and probeId must stay paired.
const wrong = ck.and(
  ck.inArray(probeTelemetry.sampleId, sampleIds),
  ck.inArray(probeTelemetry.probeId, probeIds),
);
```

組として比較する場合は、1つのタプルを、タプルを返すサブクエリまたは組のリストと比較します。`fn.tuple`は各要素の型が分かればその型を保持するため、リスト全体が1つの`Array(Tuple(...))`パラメーターとしてバインドされます。

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  [
    ["900001", "probe_alpha"],
    ["900002", "probe_beta"],
  ],
);
```

生SQLフラグメントには要素型がありません。`ck.inArray`や`ck.eq`で使う前に、タプルを`fn.cast(fn.tuple(...), "Tuple(Int32, String)")`で包んでください。

タプルを返すサブクエリなら、同じ左辺をパラメーターなしで使えます。

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  scopedSamples,
);
```

配列ヘルパーは型付きの`Selection`値を返すため、プロジェクション、フィルター、GROUP BY、結合に組み込んで使えます。

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

### `fn.count` / `fn.uniqExact`のモード

`fn.count()`、`fn.countIf(predicate)`、`fn.uniqExact(expression)`は`db.count(...)`に相当する集約関数で、同じ3つのモードを使えます。トップレベルのスカラーではなく、クエリの途中（`select`、`having`、GROUP BY、サブクエリ）で集計結果を使う場合に便利です。SQLラッパーとデコーダーも共通なので、モードごとの使い分けも同じです。

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

デコーダーは0以上の整数以外を拒否します。真偽値、`NaN`、小数、負数では`Failed to decode count() result: ...`の例外を投げるため、壊れたサーバー出力を黙って`0`に変えず、すぐに失敗します。

他の集約関数は、意図的にこのモードAPIとは別の形になっています。`count` / `uniqExact`とは精度の扱いが異なるためです。

- `fn.sum()` / `fn.sumIf()`は入力カラムごとに安全なデコーダーを選びます。整数またはDecimalのカラムではstring、浮動小数点数のカラムではnumberを使います。Decimalの精度も自動的に広げます。`number`が必要な場合だけ`.mapWith(Number)`をチェーンしてください。
- `fn.avg()`は、ClickHouseが`avg(Decimal)`を内部でFloat64経由で計算するため、常に`Selection<number>`を返します。文字列として扱うと、実際の処理経路を正しく表せません。
- `fn.min()` / `fn.max()`は入力カラムのTypeScriptの型を保ちます。そのため、`min(int64Col)`は`Selection<string>`で、カラムの読み取り時の形と一致します。

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

## セッションと一時テーブル
<!-- src:8f031227 -->

ClickHouseには従来のトランザクションがありません。範囲を絞った分析や大規模な絞り込み処理では、セッションに紐づく一時テーブルが適した手段になることがよくあります。

セッションのコールバック内では、`ck-orm`の次のAPIを使えます。

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

自分で管理するコマンドで一時テーブルを作り、セッションのクリーンアップ時には`ck-orm`に削除させたい場合は、`registerTempTable(name)`を使ってください。

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

`ckTable(...)`では表現できない、開発者が管理する信頼できるテーブル定義にだけ`createTemporaryTableRaw(name, definition)`を使ってください。

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

`definition`引数はテーブル名に続く部分から指定します。`CREATE TEMPORARY TABLE`とテーブル名は含めないでください。ck-ormがその2つを生成し、識別子は別途検証します。

### セッションの動作

- 同じ`session_id`を対象とするリクエストは、セッション単位の並行実行数制御を共有します。クライアントのデフォルト値は`session_max_concurrent_requests = 1`です。
- ClickHouseのセッションはサーバー側で排他されます。そのため、`session_max_concurrent_requests`を`1`より大きくするとローカルの直列化は無効になりますが、`SESSION_IS_LOCKED`で失敗する可能性は残ります。
- `createTemporaryTable()`はテーブルを自動的にクリーンアップ対象に登録します。
- `createTemporaryTable()`はスキーマオブジェクトを使います。一時テーブルのライフサイクルはスキーマではなく`Session`が管理します。
- `createTemporaryTableRaw()`は、一時テーブル定義用の生SQL APIです。信頼できる入力にだけ使ってください。複数文の定義は受け付けません。
- `runInSession()`は確定した`session_id`を、クリーンアップを含むすべてのリクエストで`X-ClickHouse-Replica-Tag`として送ります。このヘッダーはランタイムが管理するため、`http_headers`では上書きできません。
- ClickHouse Cloudの[レプリカ対応ルーティング](https://clickhouse.com/docs/products/cloud/features/infrastructure/replica-aware-routing)は別途有効にする必要があり、動作はベストエフォートです。スケールするとタグが再割り当てされることがあり、ブラウザーからクロスオリジンで利用する構成ではCORSで`X-ClickHouse-Replica-Tag`を許可する必要があります。
- `runInSession()`は、コールバックが終わると登録済みの一時テーブルを削除します。
- `runInSession()`のネストした呼び出しでは、常に新しい子セッションを作ります。
- ネストした呼び出しでは、アクティブな祖先の`session_id`を再利用できません。
- ネストした子セッションは常に別の`session_id`を使うため、親セッションとは一時テーブルも同一セッションの並行実行枠も共有しません。

### セッションの並行実行に関する契約

具体的なルールは次のとおりです。

- 同じ`session_id`を明示的に指定したリクエストは、デフォルトで直列に処理されます。
- クライアントのデフォルト`session_id`も、同じ制限の対象です。
- `withSettings()`で作る子クライアントは、同じ`session_id`を使う場合、親クライアントと同じ制限を共有します。
- 異なる`session_id`は互いにブロックしません。
- `stream()`とビルダーの`iterator()`は、イテレーターが完了するまで同一セッションの並行実行枠を保持します。途中で（`break`、`return`、またはエラーによって）終了すると、同じ資格情報とレプリカタグでセッション外から`KILL QUERY`を送り、サーバー側のクエリをキャンセルします。その後、ClickHouseがセッションを解放するまで待ちます。待機時間は`request_timeout`（デフォルト30秒）が上限です。
- `runInSession()`のネストした呼び出しは新しい子`session_id`を作るため、親セッションとは同じセッションの並行実行枠を共有しません。

リクエストを中断したり、`request_timeout`が発生したりしても、閉じるのは接続だけです。ClickHouseはクエリが完了するか切断を検知するまで実行を続けるため、`INSERT ... SELECT`が完了することがあり、同じセッションの次のリクエストが`SESSION_IS_LOCKED`で失敗する場合があります。サーバーを先に停止するには、`clickhouse_settings`の`max_execution_time`を`request_timeout`より小さく設定します。セッション外で`KILL QUERY WHERE query_id = '...'`を実行することもできます。リクエストボディを再送できる場合は、`fetch`をラップしてエラー373を回数上限付きでリトライすることもできます。クリーンアップの失敗（`SESSION_IS_LOCKED`を含む）は、`onCleanupError`または`SessionError`で通知されます。

推奨する使い方は次のとおりです。

- 一時テーブルを使う処理と、継続してセッションを使う処理では、`session_max_concurrent_requests = 1`を維持してください。
- 本当に並行実行する必要がある場合は、同じセッションの上限を上げず、異なる`session_id`を使ってください。
- ローカルのバックプレッシャーを無効にしたく、`SESSION_IS_LOCKED`も許容できる場合にだけ、`session_max_concurrent_requests`を上げてください。

## ランタイムメソッド
<!-- src:3e31db64 -->

ビルダーだけでなく、クライアントには低レベルのランタイムメソッドもあります。

クエリごとの共通フィールドは次のとおりです。

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

`session_timeout`と`session_check`をここに置いているのは意図的です。これらは特定のリクエストやセッションブロックに対する設定で、クライアント全体のデフォルトではありません。
`query_id`、`session_id`、`session_timeout`はリクエストやセッションに関するオプションです。`clickhouse_settings`の外側に置いてください。

`session_check`は新しいClickHouseセッションを作成しません。明示的に指定した`session_id`が既に存在することをClickHouseに確認させたいときに使ってください。
`session_id`を含むリクエストはすべて、セッションごとの並行実行数制限に従います。生の`execute()`、`command()`、`stream()`、ビルダーの`.execute()`、`.iterator()`も含まれます。

各リクエストの`clickhouse_settings`は、そのリクエストに限ってClickHouse設定を上書きします。

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

生のクエリを実行し、`Record<string, unknown>[]`を返します。

```ts
const rows = await db.execute(ckSql`SELECT 1 AS one`);
```

### `stream()`

生のクエリ結果をストリーミングします。

```ts
for await (const row of db.stream(ckSql`SELECT number FROM numbers(10)`)) {
  console.log(row);
}
```

`stream()`が`session_id`を使う場合、同一セッションの並行実行枠は、非同期イテレーターが完了するか、ループを抜けて早期に閉じるまで解放されません。

### `command()`

行セットを返さないコマンドを実行します。

```ts
await db.command(ckSql`SYSTEM FLUSH LOGS`);
```

### `withSettings()`

追加のデフォルト設定を持つ子クライアントを作成します。

```ts
const reportDb = db.withSettings({
  max_execution_time: 60,
  join_use_nulls: 0,
});
```

`withSettings()`が変更するのは、ClickHouseのセッションとクエリ設定のデフォルトだけです。返されるクライアントは、親クライアントと同じトランスポート、資格情報、タイムアウト、セッションの並行実行数コントローラーを引き継ぎます。リテラルの`join_use_nulls: 0`は型推論されるため、子クライアントの左結合の結果型にNULLは含まれません。単純な`number`型の値では、`| null`が残ります。

`runInSession()`では、セッションのライフサイクルオプションとClickHouse設定を分けて指定します。

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

### コンパイル済みクエリとの連携

多くのアプリケーションでは、ビルダーを直接使うのが基本です。別のレイヤーがすでに`CompiledQuery`を生成していて、そのレイヤーと連携する場合は、`executeCompiled()`と`iteratorCompiled()`を使います。

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

### `ping()`と`replicasStatus()`

```ts
await db.ping();
await db.replicasStatus();
```

## オブザーバビリティ
<!-- src:8021e8d9 -->

`ck-orm`は、組み込みのオブザーバビリティレイヤーを3つ備えています。

- `logger`
- `tracing`
- `instrumentation`

### ロガー

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

### トレーシング

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

トレーシングは、`clickhouseClient(...)`の設定からデータベース名、サーバーアドレス、サーバーポート、リクエストのタイムアウトを取得します。サービス単位のラベルは`tracing.attributes`で渡します。`db.`で始まるキーは無視するため、アプリケーションコードでは組み込みのデータベース属性を上書きできません。

複数のサービスで共有するトレーシングバックエンドでは、`includeStatement: false`のほうが安全です。ライブラリのデフォルトは`true`です。テーブル名、カラム名、クエリの形をサービスの境界から外に出したくない場合は、オフにしてください。バインドされた値は空白を詰めたSQLに埋め込まれませんが、SQLの形自体も運用上は機微な情報になる可能性があります。

### カスタム計装

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

公開イベント型は次のとおりです。

- `ClickHouseORMQueryEvent`
- `ClickHouseORMQueryResultEvent`
- `ClickHouseORMQueryErrorEvent`
- `ClickHouseORMQueryStatistics`
- `ClickHouseORMTracingOptions`
- `ClickHouseORMLogLevel`

組み込みのイベントフィールドには`databaseName`、`serverAddress`、`serverPort`、`requestTimeoutMs`、`statementHash`、`querySummary`、`queryId`、`sessionId`が含まれます。ck-ormが生SQLから推測せずに1つのテーブルを特定できる場合は、`tableName`も含まれます。JSONレスポンスをまとめて受け取るクエリでは、サーバーが返す場合にClickHouseのレスポンス統計も含まれます。含まれる統計は`serverElapsedMs`、`readRows`、`readBytes`、`resultRows`、`rowsBeforeLimitAtLeast`です。

## エラーの扱い
<!-- src:b4b93db0 -->

公開エラー型（型としてのみエクスポート）は次のとおりです。

- `ClickHouseORMError`
- `DecodeError`

実行時に使えるエラー判定関数は次のとおりです。

- `isClickHouseORMError(error)`
- `isDecodeError(error)`

`ClickHouseORMError`は、次の情報を含め、できるだけ多くのコンテキストを保持します。

- `kind`
- `executionState`
- `queryId`
- `sessionId`
- `httpStatus`
- `clickhouseCode`
- `clickhouseName`
- `responseText`
- `requestTimeoutMs`

現在公開されている`kind`の値は次のとおりです。

- `client_validation`
- `request_failed`
- `decode`
- `timeout`
- `aborted`
- `session`

サーバー側のログにはエラーのコンテキストをすべて使い、アプリケーションの呼び出し元へは、内容を絞った公開エラーを返してください。

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

信頼できないクライアントには、`responseText`や生のエラー`message`を返さないでください。ClickHouseのエラーテキストには、SQLの断片やオブジェクト名が含まれることがあります。

## セキュリティ
<!-- src:c2dbb125 -->

`ck-orm`は、デフォルトで安全でないSQLを組み立てにくくする設計です。

組み込みの保護機能は次のとおりです。

- `ckSql.identifier()`に渡す識別子は検証され、クォートされます。
- `fn.call(...)`、`fn.withParams(...)`、`ckType.aggregateFunction(...)`、`ckType.simpleAggregateFunction(...)`で使う関数名は検証されます。`AggregateFunction`のパラメーターは、文字列リテラルか数値リテラルでなければなりません。
- `ckType.nested({...})`に渡すネストしたフィールドキーは、ClickHouseの識別子として検証されます。
- `` ckSql`...` ``に埋め込む値は、生SQLのテキストではなくClickHouseの名前付きパラメーターになります。
- `orm_param`で始まる`query_params`キーは受け付けません。このプレフィックスは内部のSQLテンプレートパラメーター用に予約されているためです。
- リクエストごとに使えるトップレベルステートメントは1つだけです。
- 接続設定から生成するAuthorizationヘッダーは、呼び出し元が渡す`http_headers`で上書きできません。
- 構造化した接続設定では、`host`に埋め込まれた資格情報を受け付けません。
- トレーシングの送信先情報は、スパンに付与する前に正規化されます。

### 信頼できる入力だけに使うAPI

`ckTable(...)`で表現できない一時テーブルDDLを信頼できる入力だけで書く場合、`ck-orm`は`runInSession()`の中だけで生SQL APIを公開します。

- `session.createTemporaryTableRaw(name, definition)`

`fn.call(name, ...)`と`fn.withParams(name, ...)`は`name`を検証します。ただし、動的に選ぶ関数名でも、信頼できない実行時入力ではなく、開発者が制御する入力として扱ってください。

`ckSql.identifier(...)`は、コンパイルと実行の境界で検証します。フラグメントの作成そのものは軽量ですが、エラーが出るのは、フラグメントをクエリにコンパイルするときです。

### トレーシングで公開されるデータ

`tracing.includeStatement`のデフォルトは`true`です。空白を詰めたSQLテキストは、`db.statement`と`db.query.text`としてスパンに付与されます。

バインドされた値はClickHouseの名前付きパラメーターとして別に保存され、SQLテキストには含まれません。ただし、テーブル名、カラム名、クエリの形はトレーシングの出力に残ります。トレースバックエンドを共有する場合やホスト外に置いている場合は、`includeStatement`を無効にするか、コレクターで`db.statement`と`db.query.text`をフィルターしてください。

### エラーレスポンス

`ClickHouseORMError.responseText`とエラーの`message`には、ClickHouseサーバーから返る生のテキストが含まれることがあります。SQLの断片やデータベースオブジェクト名も含まれ得ます。

その詳細を、信頼できないクライアントへそのまま転送しないでください。サーバー側のログに記録し、代わりに汎用のメッセージを返してください。

## 설치
<!-- src:03d420fc -->

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

ck-orm에는 필수 종속성이 없습니다. `@opentelemetry/api`는 `ck-orm/otel` 플러그인에서만 사용하는 선택적 피어 종속성이므로, 이 플러그인을 사용할 때 ck-orm과 함께 설치하세요.

## 빠른 시작
<!-- src:d7ba4246 -->

아래 예제는 흐름을 따라가기 쉽도록 테이블 하나만 사용합니다.

### 1. 스키마 정의

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

### 2. 클라이언트 생성

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

### 3. 데이터 조회

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

빌더 쿼리는 thenable이라서 `await query`만 하면 바로 실행됩니다.

`fn.sum`은 `Decimal` 컬럼을 `Decimal(P, S)`로 자동으로 형변환하고, 정밀도를 유지하기 위해 `string`을 반환합니다. 자세한 내용은 [표현식의 Decimal 정밀도](#decimal-precision-in-expressions)를 참고하세요.

## 예제
<!-- src:c8f5c20b -->

[예제 페이지](https://ck-orm.lyz.cloud/examples/)는 실행 가능한 쿼리와 각 쿼리가 ClickHouse에 보내는 SQL을 그대로 보여 줍니다. 각 예제는 [`examples/`](../examples)에 있는 프로젝트이며, 각각 자체 테스트를 포함합니다. 실제 ClickHouse를 대상으로 하는 테스트는 [`e2e/`](../e2e)에 있고, 운영 환경과 비슷한 시나리오 스키마는 [`e2e/scenarios.ts`](../e2e/scenarios.ts)에 있습니다.

## 핵심 개념
<!-- src:a26a2ce8 -->

다음 경계를 염두에 두고 `ck-orm`을 사용하세요:

- `ckType.*`는 스키마의 컬럼 타입을 정의합니다.
- `ckTable(...)`는 테이블 스키마를 정의합니다.
- `ck.*`는 쿼리 헬퍼 네임스페이스입니다.
- `fn.*`는 SQL 함수 헬퍼 네임스페이스입니다.
- 스키마는 데이터베이스 이름이 아니라 테이블과 컬럼을 설명합니다.
- 데이터베이스 연결은 `clickhouseClient(...)`에서 설정합니다.
- 기본적으로 빌더 쿼리를 사용하고, ClickHouse 전용 표현식이 필요할 때는 원시 SQL도 사용할 수 있습니다.
- `runInSession()`은 ClickHouse 세션 헬퍼이며 트랜잭션이 아닙니다.
- `leftJoin()`과 `asofLeftJoin()`은 기본적으로 SQL 스타일의 NULL 처리 규칙을 사용합니다.
- 값이 크거나 손실 위험이 큰 숫자 결과는 암묵적으로 형변환하지 않고 보수적으로 디코딩합니다.

`ck-orm`은 타입 기반으로 ClickHouse를 다루는 데 초점을 둡니다. 테이블 정의, 쿼리 조합, 쓰기 헬퍼, 세션 헬퍼, 옵저버빌리티를 다룹니다. `createTable()`, `createMaterializedView()`, `dropTable()`은 각각 DDL 문 하나를 실행합니다. 스키마 마이그레이션(스키마 비교, `ALTER`, 버전 관리)은 애플리케이션의 마이그레이션 도구에 그대로 둡니다.

## 스키마 DSL
<!-- src:5149bff0 -->

### `ckTable()`

테이블은 `ckTable(name, columns, options?)`로 정의하세요.

이 섹션의 예제는 다음 import 구문을 사용합니다.

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

세 번째 인자로는 일반 객체나 팩토리 함수를 넘길 수 있습니다.

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

사용할 수 있는 테이블 옵션은 다음과 같습니다.

- `engine`
- `partitionBy`
- `primaryKey`
- `orderBy`
- `sampleBy`
- `ttl`
- `settings`
- `comment`
- `versionColumn`

컬럼 정의에 DDL 메타데이터를 직접 지정할 수도 있습니다.

- `.default(expr)`
- `.materialized(expr)`
- `.aliasExpr(expr)`
- `.comment(text)`
- `.codec(expr)`
- `.ttl(expr)`

스키마 메타데이터는 `ck-orm`에서 두 가지 역할을 합니다.

- 타입 정보를 바탕으로 쿼리를 만들고, 삽입을 검증하고, 결과를 디코딩합니다.
- `createTable()`과 `createTemporaryTable()`의 `CREATE TABLE` 문을 렌더링합니다.

이 메타데이터는 기존 ClickHouse 테이블을 마이그레이션하거나 동기화하지 않습니다. `createTable()`은 테이블을 만들기만 합니다([테이블과 구체화된 뷰](#tables-and-materialized-views) 참고). 스키마 마이그레이션은 마이그레이션 도구에 두고, `ckTable(...)`을 애플리케이션이 읽고 쓰는 스키마와 일치시키세요.

예제는 다음과 같습니다.

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

### 타입 추론

모든 테이블은 다음 속성을 노출합니다.

- `table.$inferSelect`
- `table.$inferInsert`

```ts
type TelemetryRow = typeof probeTelemetry.$inferSelect;
type TelemetryInsert = typeof probeTelemetry.$inferInsert;

const sampleId: TelemetryRow["sampleId"] = "900001";
const ingestVersion: TelemetryInsert["ingestVersion"] = "1";
```

제네릭 헬퍼에 사용할 타입은 다음과 같습니다.

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

`InferSelectSchema`와 `InferInsertSchema`에 사용하는 스키마 객체는 일반적인 TypeScript 객체 묶음입니다. 공유 모델 타입을 만들 때 유용하고, 클라이언트 구성과는 분리되어 있습니다.

### `$type<T>()`로 컬럼 타입 보정하기

모든 컬럼 빌더는 체이너블 `$type<T>()` 메서드를 제공합니다. 이 메서드는 런타임 동작을 바꾸지 않으면서 컬럼의 TypeScript 타입만 재정의합니다. 기본으로 추론된 타입이 도메인에서 필요한 범위보다 넓을 때 사용하세요.

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

`$type<T>()` 메서드는 다른 모든 컬럼 수정자와 어떤 순서로든 연결할 수 있습니다. 예를 들어 `ckType.string().$type<Role>().default(ckSql`'guest'`).comment("…")`처럼 사용합니다. 런타임 디코더와 인코더, SQL 타입, DDL 출력에는 아무 영향이 없습니다. `$type`은 컴파일 시점에만 적용되는 타입 단언입니다.

`enum8`과 `enum16`은 값 객체에서 리터럴 유니온을 이미 추론하므로, 유니온에 다른 브랜드를 붙이거나 외부 열거형 타입에 맞출 때만 `$type`을 사용하면 됩니다.

```ts
// Inferred automatically — no `as const`, no explicit type arguments
const status = ckType.enum8({ active: 1, paused: 2, banned: 3 });
//    ↑ Enum8<"active" | "paused" | "banned">

// Re-brand on top of the inferred keys
type UserStatus = "active" | "paused" | "banned";
const userStatus = ckType.enum8({ active: 1, paused: 2, banned: 3 }).$type<UserStatus>();
```

> **주의 사항:** `$type<T>()`은 런타임 값을 검증하지 않습니다. 데이터베이스 행에 타입 선언과 맞지 않는 값이 있어도(예: 스키마 마이그레이션 전의 오래된 데이터) TypeScript는 값이 타입에 맞는다고 보고 계속 타입을 좁힙니다. 모든 경우를 다루는 `switch` 문도 해당 값을 조용히 놓칩니다. 읽을 때 값의 타입을 강제하려면 프로젝션에서 `mapWith`를 사용하고, 디코딩과 인코딩마다 검증하려면 아래 `$validator()`를 사용하세요.

### `$type<{ select, insert }>()`로 삽입 타입과 조회 타입 다르게 지정하기

ClickHouse 컬럼은 삽입할 때 조회 결과로 반환되는 값보다 더 넓은 타입을 받기도 합니다. 예를 들어 `DateTime` 컬럼은 삽입할 때 `string`과 `Date`를 모두 받지만 디코딩 후에는 항상 `Date`를 반환합니다. `$type`에 객체를 넘기면 두 타입을 다르게 지정할 수 있습니다.

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

`$type<{ select: T }>()`에 `insert`를 지정하지 않으면 단일 제네릭 `$type<T>()` 형식과 같습니다.

### 삽입 모델 규칙: DEFAULT, MATERIALIZED, ALIAS

`$inferInsert`는 `$inferSelect`를 그대로 복사하지 않고 스키마의 컬럼 형태에서 파생됩니다.

| DDL 수정자 | 삽입 모델에 미치는 영향 |
| --- | --- |
| `.default(expr)` | 컬럼을 생략할 수 있습니다. 생략하면 ClickHouse가 값을 채웁니다. |
| `.materialized(expr)` | 삽입 모델에서 컬럼을 완전히 뺍니다. |
| `.aliasExpr(expr)` | 삽입 모델에서 컬럼을 완전히 뺍니다. |

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

### `$validator(schema)`로 런타임 유효성 검사

`$type`만으로 부족한 경우도 있습니다. 데이터베이스에 오래된 데이터가 남아 있거나, 신뢰할 수 없는 업스트림에서 JSON 구조를 받거나, 삽입 API에서 문자열을 `Date`로 강제 변환하려는 경우입니다. 이때는 `$validator(schema)`를 메서드 체인으로 연결하세요. [Standard Schema v1 사양](https://github.com/standard-schema/standard-schema)을 따르는 객체라면 무엇이든 사용할 수 있습니다. Zod 3.23+, Valibot 1+, ArkType, Effect Schema, TypeBox도 사용할 수 있습니다. ck-orm은 이 라이브러리들에 의존하지 않습니다.

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

스키마 변환도 그대로 적용됩니다(예: `z.string().transform((v) => new Date(v))`). 컬럼은 삽입할 때 스키마의 *입력* 타입을 받고, 조회할 때 *출력* 타입을 노출합니다. `$validator`는 동기 방식만 지원하며 비동기 스키마를 사용하면 디코딩/인코딩 시점에 예외가 발생합니다.

`$type`과 `$validator` 비교:

| | `$type<T>()` | `$validator(schema)` |
| --- | --- | --- |
| 런타임 비용 | 0 | 행과 컬럼마다 동기 스키마 검증 1회 |
| 타입 안전성 | 컴파일 시점의 타입 단언 | 스키마에서 추론 |
| 런타임 유효성 검사 | 아니요 | 예. 일치하지 않으면 예외가 발생합니다 |
| 입력 ≠ 출력 타입 | `$type<{ select, insert }>()`로 지정 | 스키마 변환으로 지정 |
| 외부 종속성 | 없음 | 없음(스키마 라이브러리는 자유롭게 선택) |

### JSON 컬럼 타입

ck-orm은 ClickHouse 24.x 이상에 도입된 **새 JSON 데이터 타입**을 기준으로 동작합니다
([문서](https://clickhouse.com/docs/sql-reference/data-types/newjson)). 이 타입은 값을 불투명한 문자열로
저장하지 않고 JSON 객체를 타입이 지정된 하위 컬럼으로 물리적으로 나눕니다. 기억할 규칙은 두 가지입니다.

- ClickHouse의 `JSON` 데이터 타입은 **최상위 객체만 허용**합니다. 최상위 배열, 문자열, 숫자는 허용하지 않습니다.
  ck-orm도 컴파일 시에는 `T extends Record<string, unknown>`, 런타임에는 삽입/조회할 때마다 간단한 O(1)
  검사로 같은 제약을 적용합니다.
- 타입이 지정된 경로(`a.b UInt32`)는 ClickHouse 안에서 실제 하위 컬럼이 됩니다. ck-orm은 해당 경로를 대응하는
  ck-orm 컬럼 팩토리의 `mapFromDriverValue` / `mapToDriverValue`로 처리합니다. 따라서 `ckType.uint64()`로 선언한
  경로도 무손실 `string`으로 디코딩되어 최상위 `uint64` 컬럼과 같은 방식으로 동작합니다.

가장 단순하게는 다른 ck-orm 컬럼 팩토리와 같은 방식으로 정의합니다.

```ts
import { ckTable, ckType } from "ck-orm";

const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json("payload"),
});
// DDL:  `payload` JSON
// $inferSelect.payload: { [key: string]: unknown }
```

제네릭을 지정하면 실제 형태가 `$inferSelect` / `$inferInsert`에 반영됩니다.

```ts
type EventPayload = { user_id: number; event: string; meta?: { ip: string } };

const userEvents = ckTable("user_events", {
  id: ckType.uint64(),
  payload: ckType.json<EventPayload>("payload"),
});
```

다른 컬럼 빌더 호출과 함께 제네릭을 지정하고 싶다면 체이너블 형태인
`ckType.json<T>().$type<T2>()`를 사용하면 됩니다.

#### DDL 옵션 설정

팩토리의 두 번째 인자로 NewJSON DDL 옵션 네 가지를 지정할 수 있습니다.

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

`typeHints`의 키가 `Paths<T>`에 맞는지 컴파일 시 검사합니다. `{ "user.idd": ... }` 같은 오타가 있으면 컴파일 시점에
오류가 납니다. 생성된 DDL은 항상 일관된 사전순으로 출력됩니다. 설정을 어떻게 작성해도 외부 스키마 비교 도구가
보는 결과는 바이트 단위로 동일합니다.

#### typeHints와 런타임 디코더

ClickHouse가 64비트 값을 무손실로 처리하므로 ck-orm은 `UInt64`를 문자열로 디코딩합니다. TS 타입과
`user_id`를 TS 타입과 `typeHints` 항목에 모두 선언했다면 두 곳의 타입을 맞추세요. `uint64`라면 제네릭에서도
`user_id: string`으로 선언해야 합니다. 삽입할 때 허용하는 입력 타입과 조회할 때 디코딩되는 타입이 다르면
`$type<{ select, insert }>()`를 사용하세요.

```ts
ckType.json("payload", { typeHints: { user_id: ckType.uint64() } })
  .$type<{
    select: { user_id: string };
    insert: { user_id: string | number | bigint };
  }>();
```

#### 경로 접근

모든 JSON 컬럼은 ClickHouse 경로 접근 구문을 그대로 출력하는 체이너블 경로 메서드 다섯 개를 제공합니다.

| 메서드 | SQL | 의미 |
| --- | --- | --- |
| `col.path("a.b")` | `col.a.b` | 경로를 읽습니다. 정적 타입은 `PathValue<T, "a.b">`입니다(아래 `typeHints` 참고). |
| `col.castPath("a.b", ckType.uint64())` | `col.a.b.:UInt64` | 다른 ck-orm 컬럼 팩토리로 경로를 강제로 디코딩합니다. 정적 타입은 캐스팅할 때 넘긴 컬럼의 `TData`를 사용합니다. |
| `col.subobject("a")` | `col.^a` | 중첩 객체를 `JSON` 값으로 읽습니다. |
| `col.merged("a")` | `col.@a` | 공유 데이터를 병합한 뷰를 읽습니다. |
| `col.arrayPath("a")` | `col.a[]` | 배열 타입 경로를 전개합니다. |

> `col.path(P)`의 타입은 항상 `PathValue<T, P>`입니다. 제네릭 `T`에서 파생되며 `typeHints`에서 파생되지 않습니다.
> `typeHints`는 런타임 디코더만 바꾸므로 정적 타입과 타입 힌트를 직접 맞춰야
> 합니다(위 섹션에서 `uint64 → string` 무손실 정책을 설명했습니다). 둘이 다르면
> `castPath(P, ckType.X())`를 사용하세요. 반환 타입은 캐스팅에 사용한 컬럼에서 가져옵니다.

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

`Paths<T>`는 경로 리터럴을 컴파일 시점에 검사합니다. `payload.path("a.zzz")`는
`"a.zzz"`가 `T`에 포함되지 않으면 타입 검사에 실패합니다.

각 체인 수정자(`$type`, `$validator`, `.default()`, `.materialized()`, `.bind()` 등)가 런타임에 경로
메서드를 다시 연결하므로 실제 호출은 항상 동작합니다. 정적 타입은 **`$validator`를 제외한 모든 체인 연결에서
경로 메서드를 계속 추적합니다.** `$validator`의 계약은 `StandardSchemaV1<unknown, unknown>`에 대해 제네릭으로
정의되어 있어, TypeScript가 적용하는 LSP식 하위 타입 규칙을 어기지 않고 `JsonShape`로 안전하게 좁힐 수 없습니다.
따라서 이 메서드를 한 번 연결하면 정적 타입은 다시 `Column<T>`로 돌아갑니다. 메서드 자체는 계속 호출할 수
있습니다. 이후에도 체이닝하려면 타입 단언을 사용하세요.

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

#### `fn.json*` — 동적 경로 읽기

경로 문자열을 런타임에 만들거나 `T`에 선언하지 않은 경로를 읽어야 한다면 `fn.*` 네임스페이스를 사용하세요.

```ts
import { fn } from "ck-orm";

const userPath = userInput as string;
await db.select({
  raw:    fn.jsonPath<string>(userEvents.payload, userPath),
  casted: fn.jsonCast(userEvents.payload, userPath, ckType.string()),
  shape:  fn.dynamicType(fn.jsonPath(userEvents.payload, userPath)),
});
```

`fn.jsonCast`은 `castPath`의 동적 버전입니다. `fn.jsonSubobject`, `fn.jsonMerged`, `fn.jsonArray`는
각각 대응하는 컬럼 메서드와 같은 역할을 합니다. `fn.dynamicType`은 Dynamic 타입 경로의 런타임 타입을 확인할 수
있도록 ClickHouse의 `dynamicType()` 함수를 제공합니다.

#### 런타임 유효성 검사

모든 행에서 두 계층이 **다음 순서로** 실행됩니다.

1. **ck-orm의 기본 JSON 가드** — `null` / `undefined`는 그대로 통과시키므로 `nullable()` JSON 컬럼이
   자연스럽게 동작합니다. 객체가 아닌 나머지 입력(배열, 문자열, 숫자, 불리언)은
   해당 컬럼을 가리키는 `client_validation` / `decode` 오류(예: `JSON column expects a plain object, got array`)를
   내면서 거부됩니다. `typeHints`로 타입을 지정한 경로는 입출력할 때 해당 컬럼 팩토리의 인코더·디코더를
   거칩니다. 이 가드는 항상 적용됩니다.
2. **선택적 `$validator(schema)`** — 있으면 삽입할 때 기본 인코더 *전에*, 조회할 때 기본 디코더 *후에*
   실행합니다. 데이터 형태를 검증할 때 사용하세요.

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

ck-orm은 `T["a"]`가 실제로 `number`인지 등을 확인하려고 JSON 값을 **의도적으로 재귀 탐색하지 않습니다**.
그런 검사가 필요하면 `$validator`를 사용하세요. JSON 가드는 ClickHouse 자체 오류 메시지만으로 어느 컬럼의 문제인지
연결하기 어려운 경우만 잡습니다. 최상위 스칼라 인자는 해당 컬럼 이름을 포함한 명확한 오류를 내며 실패합니다.

```ts
await db.insert(events).values({ payload: ["wrong"] as never });
// throws: JSON column expects a plain object, got array
```

#### 삽입 기본값

JSON 컬럼도 다른 모든 컬럼과 같은 `$inferInsert` 규칙을 따릅니다. `.default()`는 필드를 선택적으로 만들고,
`.materialized()`와 `.aliasExpr()`는 해당 필드를 삽입 모델에서 완전히 제거합니다.

```ts
const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json<{ note: string }>("payload").default(ckSql`'{}'`),
});

await db.insert(auditLogs).values({ id: "1" }); // payload filled by CH DEFAULT
```

### `ckAlias()`

같은 테이블이 쿼리에 두 번 이상 등장해야 할 때 `ckAlias()`를 사용하세요.

```ts
import { fn, ck, ckAlias } from "ck-orm";

const telemetry = ckAlias(probeTelemetry, "telemetry");
```

`ckAlias()`가 반환하는 컬럼은 별칭에 맞게 자동으로 다시 바인딩됩니다.

### 컬럼 이름

스키마 객체 키는 TypeScript 행, 디코딩한 쿼리 결과, 삽입 값에서 사용하는 논리 키입니다. 기본적으로 이
키가 ClickHouse 컬럼 이름이기도 합니다.

```ts
const telemetry = ckTable("telemetry", {
  signalStrength: ckType.decimal({ precision: 20, scale: 5 }),
});
```

데이터베이스에서 다른 컬럼 이름을 사용하면 물리적 컬럼 이름을 첫 번째 인자로 넘기세요.

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

SQL, DDL, 필터, 정렬, 그룹화, 쓰기 작업의 컬럼 목록에는 물리적 이름(`probe_id`, `signal_strength`, `created_at`)을
사용합니다. 추론 모델, 기본 조회 결과, 명시적 프로젝션 키, 삽입 값은 스키마 객체
키(`probeId`, `signalStrength`, `createdAt`)를 사용합니다.

모든 공개 `ckType` 빌더는 바깥 컬럼의 물리적 이름을 지정할 수 있습니다. 추가 구성이 없는 빌더는 `name?`를 받고,
타입 구성이 있는 빌더는 선택적 물리적 컬럼 이름을 첫 자리에 두고 타입 구성은 객체에 넣습니다.

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

`aggregateFunction`와 `simpleAggregateFunction`은 ClickHouse 전용 호출 방식을 사용합니다. 첫 번째
문자열은 컬럼 이름이 아니라 집계 함수 이름입니다.

```ts
ckType.aggregateFunction("sum", ckType.uint64());
ckType.aggregateFunction("quantile(0.5)", ckType.float64());
ckType.simpleAggregateFunction("sum", ckType.uint64());
```

`AggregateFunction`는 `quantile(0.5)`나 `topK(10)`처럼 리터럴 매개변수가 포함된 ClickHouse 집계 함수 이름을
받습니다. `simpleAggregateFunction`는 더 엄격해서 일반 집계 함수 이름만 받습니다.

물리적 컬럼 이름도 지정해야 하면 객체 설정을 사용하세요.

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

`nested("items", shape)`는 바깥 `Nested(...)` 컬럼의 이름을 지정합니다. 중첩 필드 이름은 `shape`의 키이며,
중첩 필드 이름에는 내부 컬럼의 `configuredName` 값을 사용하지 않습니다.

```ts
const sensorPackets = ckTable("sensor_packets", {
  items: ckType.nested("items", {
    componentId: ckType.string(),
    quantity: ckType.float64(),
  }),
});
```

### 컬럼 빌더

스키마 컬럼 빌더에는 `ckType.*`를 사용하세요. 스키마 DSL에서는 자주 쓰는 ClickHouse 타입 계열을 지원합니다.

- 정수: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`
- 부동 소수점과 Decimal: `float32`, `float64`, `bfloat16`, `decimal`
- 스칼라 타입: `bool`, `string`, `fixedString`, `uuid`, `ipv4`, `ipv6`
- 시간 타입: `date`, `date32`, `dateTime`, `dateTime64`, `time`, `time64`
- 열거형과 특수 타입: `enum8`, `enum16`, `json`, `dynamic`, `qbit`
- 컨테이너: `nullable`, `array`, `tuple`, `map`, `nested`, `variant`, `lowCardinality`
- 집계 타입: `aggregateFunction`, `simpleAggregateFunction`
- 지오메트리 타입: `point`, `ring`, `lineString`, `multiLineString`, `polygon`, `multiPolygon`

`int64`와 `uint64`는 스키마 기반 읽기와 쓰기, 추론 모델에서 기본적으로 TypeScript `string`으로 처리됩니다.
그래서 ClickHouse JSON 전송 형식과 JavaScript 런타임 사이에서도 64비트 값의 정확도가 유지됩니다. 명시적으로
`bigint`가 필요하면 `mapWith((value) => BigInt(String(value)))`처럼 직접 디코더를 지정하세요.

ClickHouse는 `Nullable(Array(...))`, `Nullable(Map(...))`, `Nullable(Tuple(...))`를 지원하지 않습니다.
`ck-orm`은 스키마 정의 시점에 이런 형태를 거부합니다. 대신 복합 타입 안에 `nullable(...)`을 넣으세요. 예를
들어 `ckType.array(ckType.nullable(ckType.string()))`처럼 쓰면 됩니다.

타입 설정을 함께 받는 빌더는 객체 설정을 사용하며, 선택적 물리적 컬럼 이름을 첫 번째 인자로 받습니다.

```ts
ckType.decimal({ precision: 20, scale: 5 });
ckType.decimal("signal_strength", { precision: 20, scale: 5 });
ckType.fixedString({ length: 8 });
ckType.dateTime64("created_at", { precision: 9, timezone: "UTC" });
ckType.qbit("embedding", ckType.float32(), { dimensions: 8 });
```

`Date` / `Date32` 쓰기와 스키마 기반 조건식 값은 JavaScript `Date`를 UTC 기준 날짜로
보냅니다(`2026-06-15T23:00:00Z`는 `2026-06-15`가 됩니다). 다른 날짜를 저장하려면 `YYYY-MM-DD` 문자열을
넘기세요. 문자열과 숫자는 그대로 보냅니다.

`DateTime` / `DateTime64` 조건식 값은 JavaScript `Date`를 UTC 기준 시점으로 바인딩하므로 컬럼, 세션, 서버
시간대가 이 값을 바꾸지 못합니다. 문자열은 적힌 값 그대로, 컬럼 시간대를 기준으로 ClickHouse가 읽습니다. 32비트
`DateTime` 위치에서는 매개변수에 들어 있는 Date가 모두 정수 초 단위일 때 `DateTime('UTC')`로 바인딩합니다. 매개변수에
밀리초가 있는 Date가 하나라도 있으면 `DateTime64(3, 'UTC')`로 바인딩합니다. `DateTime64` 위치는 항상
`DateTime64(3, 'UTC')`로 바인딩합니다. `gte` 같은 스칼라 조건식은 여전히 정확한 시점을 비교합니다.
`DateTime` 컬럼의 `inArray` / `notInArray`에는 정수 초 단위인 Date만 사용할 수 있습니다. 여기에 밀리초가 있는 Date를
넣으면 ClickHouse가 거부합니다(`TYPE_MISMATCH`). 이는 ClickHouse 자체 동작이며, 그런 값은 어차피 일치할 수
없습니다. `insert().values()`는 각 `Date`를 컬럼 정밀도에 맞춘 정수 틱 값으로 보냅니다. 자세한 내용은 쓰기 섹션을
참고하세요.

### 컬럼 타입 레시피

자주 쓰는 스키마 형태와 TypeScript 값은 다음과 같습니다.

| ClickHouse 형태 | 스키마 | TypeScript 값 형태 |
| --- | --- | --- |
| 열거형 | `ckType.enum8({ idle: 1, active: 2 })` | `"idle" | "active"` (키에서 추론) |
| 카디널리티가 낮은 문자열 | `ckType.lowCardinality(ckType.string())` | `string` |
| NULL 허용 Decimal | `ckType.nullable(ckType.decimal({ precision: 18, scale: 5 }))` | `string | null` |
| 배열 | `ckType.array(ckType.string())` | `string[]` |
| NULL을 허용하는 배열 항목 | `ckType.array(ckType.nullable(ckType.string()))` | `(string | null)[]` |
| 튜플 | `ckType.tuple(ckType.string(), ckType.int32())` | `[string, number]` |
| 맵 | `ckType.map(ckType.string(), ckType.string())` | `Record<string, string>` |
| 중첩 객체 배열 | `ckType.nested({ component: ckType.string(), quantity: ckType.float64() })` | `{ component: string; quantity: number }[]` |
| Variant | `ckType.variant(ckType.string(), ckType.int32())` | `string | number` |
| JSON | `ckType.json<{ risk?: { score?: number } }>()` | `{ risk?: { score?: number } }` |

`ckType.map(...)`은 현재 `String` 키만 지원하며 이를 JavaScript 레코드로 매핑합니다. 따라서 ClickHouse
`Map(K, V)`에서 키가 중복되는 경우까지는 표현하지 않습니다.

삽입 행은 `typeof table.$inferInsert`와 같은 형태로 추론됩니다. 단, ClickHouse 기본값이나 생성
표현식이 있는 컬럼은 `insert(table).values(...)`를 호출할 때 생략할 수 있습니다. 생성된 `INSERT` 컬럼
목록에는 `MATERIALIZED`와 `ALIAS` 컬럼을 넣지 않습니다. 명시적으로 넘겨도 허용되지 않습니다.

## 클라이언트 구성
<!-- src:26d96c74 -->

클라이언트는 `clickhouseClient()`로 만듭니다.

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:<password>@127.0.0.1:8123/telemetry_lab",
});
```

### 연결 모드

`clickhouseClient()`는 연결 방식 두 가지를 지원하며, 둘 중 하나만 사용할 수 있습니다.

#### `databaseUrl`

연결 문자열 하나로 설정하려면 `databaseUrl`을 사용하세요:

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:secret@127.0.0.1:8123/telemetry_lab",
});
```

`databaseUrl`을 지정하면 다음 값은 함께 넘기지 마세요:

- `host`
- `database`
- `username`
- `password`
- `pathname`

#### 연결 필드를 따로 지정하기

각 항목을 따로 설정하려면 필드를 직접 지정하세요:

```ts
const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  username: "default",
  password: "<password>",
});
```

구조화 모드의 기본값은 다음과 같습니다:

- `host`: `http://localhost:8123`
- `database`: `default`
- `username`: `default`
- `password`: `""`

### 공통 필드

대부분의 프로젝트에서는 클라이언트 필드 몇 가지만 사용합니다:

| 필드 | 용도 |
| --- | --- |
| `request_timeout` | 요청별 실제 경과 시간 제한을 밀리초로 지정하는 선택적 필드입니다. 생략하면 ck-orm은 클라이언트 쪽 시간 제한을 적용하지 않습니다. 요청이 언제 끝나는지는 fetch, 플랫폼, 서버, `abort_signal`이 결정합니다. 오래 실행되는 스트리밍 쿼리에서는 이 값을 설정하지 말고 `abort_signal`(예: `AbortSignal.timeout(30_000)`)로 실행 시간 한도를 적용하세요. |
| `clickhouse_settings` | 기본 ClickHouse 세션 및 쿼리 설정 |
| `application` | ClickHouse 애플리케이션 이름 설정 |

테이블 정의는 연결 구성과 독립적입니다. `ckTable(...)`로 테이블을 정의한 다음 쿼리를 조합하는 곳에서 테이블 객체를 가져와 `.from(...)`, `.insert(...)`, 임시 테이블 헬퍼에 그대로 넘기세요.

### 고급 필드

고급 런타임 동작에는 다음 필드를 사용하세요:

| 필드 | 용도 |
| --- | --- |
| `fetch` | `(request: Request) => Response \| Promise<Response>`는 모든 HTTP 요청마다 호출됩니다. 임의의 fetch 구현, 서버 핸들러, 또는 [`examples/decimal-sum/main.test.ts`](../examples/decimal-sum/main.test.ts)에 있는 테스트용 가짜 구현을 지정할 수 있습니다. 기본값은 플랫폼의 `fetch`입니다. |
| `http_headers` | 추가 기본 헤더 |
| `role` | 기본 ClickHouse 역할 또는 역할 목록 |
| `session_id` | 기본 세션 ID |
| `session_max_concurrent_requests` | 이 클라이언트에서 같은 `session_id`에 허용되는 최대 동시 요청 수(기본값 `1`) |
| `compression.response` | 기본값인 `false`에서는 스트리밍 요청이 `enable_http_compression = 0`을 보내므로 행이 ClickHouse에서 생성되는 대로 도착합니다. 스트리밍이 아닌 요청은 서버 기본값을 그대로 사용합니다(ClickHouse 26.3 이상에서는 클라이언트가 허용하면 응답을 압축합니다). `true`로 설정하면 모든 응답을 압축하므로, 데이터가 드문 스트림은 압축 블록이 채워질 때까지 지연될 수 있습니다. |
| `logger` / `logLevel` | 로거 연동 |
| `plugins` | `ck-orm/otel`의 `otelTracing()` 같은 쿼리 라이프사이클 플러그인 |

세션 수명을 제어하는 옵션은 의도적으로 요청 범위로만 동작합니다. `session_timeout`은 개별 쿼리 또는 `runInSession(...)`에 넘기세요. `session_check`는 기존 `session_id`를 이어 쓸 때 사용하고, 새 세션을 시작할 때는 사용하지 마세요.
`session_max_concurrent_requests`는 다르게 동작합니다. 클라이언트 수준에서 같은 `session_id`를 대상으로 겹치는 요청 수를 제한합니다.
실제 ClickHouse 세션은 여전히 서버에서 잠기므로, `session_max_concurrent_requests`를 `1`보다 크게 하면 같은 세션에서 실제로 병렬 실행되는 대신 `SESSION_IS_LOCKED` 오류가 나타날 수 있습니다.
로컬 직렬화를 의도적으로 없애고 서버 쪽 세션 잠김 실패를 처리할 준비가 되어 있을 때만 값을 올리고, 그 외에는 기본값 `1`을 유지하세요.

### ClickHouse 설정

`clickhouse_settings`에는 ClickHouse 세션 및 쿼리 설정만 넣습니다. ClickHouse의 [세션 설정](https://clickhouse.com/docs/operations/settings/settings)에 문서화되어 있고 HTTP API가 쿼리 매개변수로 받는 종류의 키입니다. `host`, `database`, `request_timeout`, `http_headers`, `session_max_concurrent_requests` 같은 ck-orm 클라이언트 구성과는 별개입니다.

`query`, `database`, `session_id`, `role`, `param_*` 같은 HTTP 전송 계층 필드는 `clickhouse_settings` 밖에 두세요. 이런 키는 요청 형식과 이름 있는 매개변수 채널에 속하므로 ck-orm이 따로 검증합니다.

공식 설정 키는 TypeScript 자동 완성을 지원하며, 임의의 키도 최신 ClickHouse 버전이나 배포 환경별 설정에 사용할 수 있습니다:

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

JSON parse/stringify 동작은 fetch 전송 계층에서 관리합니다. 클라이언트의 공개 구성은 런타임에 따라 이 동작이 달라지지 않도록 일관되게 유지하며, `json` 오버라이드는 노출하지 않습니다.

ck-orm은 모든 요청에 전송 계약에 필요한 최소 설정 집합을 강제합니다: `http_write_exception_in_output_format = 0`, `output_format_json_quote_64bit_integers = 1`, `output_format_json_quote_decimals = 1`, `date_time_output_format = "iso"`. 호출자 설정이 이 값과 충돌하면 ck-orm은 경고를 한 번 남기고 강제 값을 보냅니다. `max_threads` 같은 부하/성능 설정은 여전히 호출자가 제어합니다.

### 인증

`ck-orm`은 데이터베이스 연결에 Basic 인증을 사용합니다.

- `databaseUrl` 모드에서는 URL에 자격 증명을 넣을 수 있습니다.
- 구조화 모드에서는 `username`과 `password`를 사용합니다.
- 자격 증명을 지정하지 않으면 사용자 이름은 `default`이고 비밀번호는 빈 문자열입니다.

## 쿼리 빌더
<!-- src:fc28245d -->

아래 코드 조각에서는 `db`, `probeTelemetry`와 코드에 나오는 헬퍼를 `ck-orm`에서 가져왔다고 가정합니다.

### `select()`

선택 목록을 명시하면 결과 형태도 명확하게 정해집니다:

```ts
const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
    signalStrength: probeTelemetry.signalStrength,
  })
  .from(probeTelemetry)
  .limit(10);
```

프로젝션 객체는 공개 `Selection` 값이나 컬럼을 조합해 만듭니다. 실제로 테이블 컬럼, `fn.*(...)`, `ck.expr(...)` 출력은 모두 `select({ ... })` 안에서 같은 방식으로 조합할 수 있습니다.

조인이 없으면 암시적 선택이 전체 테이블 모델을 반환합니다:

```ts
const rows = await db.select().from(probeTelemetry).limit(10);
```

조인이 있으면 암시적 선택은 필드를 소스별로 묶어 중첩 객체로 반환합니다. 왼쪽 조인에서 대응하는 행이 없는 그룹은 `null`이 됩니다. ClickHouse는 `Array` / `Map` 컬럼에서 NULL 대신 `[]` / `{}` 값을 반환하므로 빈 컨테이너도 이 검사에서는 누락된 것으로 간주합니다. 조인된 행에서도 스칼라 필드가 모두 NULL이고 배열과 맵이 비어 있으면 매칭되지 않은 경우와 구분할 수 없어 역시 `null`이 됩니다.

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

`asofJoin()`과 `asofLeftJoin()`은 ClickHouse의 `ASOF JOIN`과 `LEFT ASOF JOIN`을 렌더링합니다. 왼쪽의 각 행에 대해 일치하는 행을 모두 조인하는 대신, 값이 가장 가까운 오른쪽 행 하나만 조인합니다. 조건은 `ck.and(...)`로 하나 이상의 동등 조건과, 왼쪽 컬럼과 오른쪽 컬럼 사이의 `ck.gt`, `ck.gte`, `ck.lt`, `ck.lte` 중 정확히 하나를 묶습니다.

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

- `ck.gte(left, right)`는 왼쪽 값 이전 또는 그 시점의 가장 최근 오른쪽 행을 고르고, `ck.gt`는 왼쪽 값보다 엄격히 이전인 가장 최근 행을 고릅니다. `ck.lte`와 `ck.lt`는 왼쪽 값 이후 또는 그 시점, 또는 엄격히 이후인 가장 이른 오른쪽 행을 고릅니다.
- `asofLeftJoin()`은 [`leftJoin()`의 NULL 처리 규칙](#join-null-semantics)을 따릅니다. `join_use_nulls = 1`을 보내고 오른쪽을 NULL 허용으로 타입을 정하므로, 일치하는 행이 없는 왼쪽 행은 `null`이 됩니다. `asofJoin()`은 `innerJoin()`처럼 그런 행을 버립니다.
- 가장 가까운 값을 찾는 컬럼은 정수, 부동소수점, `Date`, `DateTime`, `Decimal` 중 하나여야 하고, 비교의 양쪽은 모두 컬럼을 가리켜야 합니다. 값과의 비교는 여기에 포함되지 않습니다. `ck.eq(quotes.symbol, "BTCUSD")` 같은 추가 동등 필터는 허용됩니다.
- ck-orm은 이 조건을 검사하지 않습니다. 부등 비교가 없거나, 부등 비교가 두 개이거나, `ck.or(...)`가 있으면 ClickHouse는 데이터를 읽기 전에 거부합니다. 오류는 `ClickHouseORMError`이며 `kind`는 `"request_failed"`, `clickhouseName`은 `"INVALID_JOIN_ON_EXPRESSION"`입니다. 동등 조건이 없는 경우와 가장 가까운 값의 컬럼이 다른 타입인 경우도 거부됩니다. 동등 조건이 없을 때의 오류 이름은 ClickHouse 버전마다 다릅니다.
- ASOF 조인은 기본 `join_algorithm`과 `hash`, `parallel_hash`, `full_sorting_merge`로 실행됩니다. `partial_merge`, `grace_hash`, `direct`는 `NOT_IMPLEMENTED`로 실패합니다.

### `where()`와 조건 헬퍼

공개 조건 헬퍼:

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

`.where(...predicates)`는 조건을 가변 인수로 받아 `AND`로 묶는 API입니다. 조건 객체가 `undefined`이면 무시하므로 조건을 여러 개 직접 넘기거나 `ck.and(...)`나 `ck.or(...)`로 그룹을 만들 수 있습니다.

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

값 목록은 `Array(T)` 쿼리 매개변수 하나로 보냅니다(`col in {p:Array(T)}`). 텍스트가 약 40,000자를 넘는 목록은 여러 매개변수로 나눈 뒤 `arrayConcat(...)`로 연결합니다. 기본 키와 skip index를 이용한 가지치기에는 영향을 주지 않습니다. ClickHouse 26.3 이상은 HTTP 요청 하나당 쿼리 매개변수를 최대 1,000개까지만 허용하고, 25.8의 분석기는 합친 목록이 약 1 MB를 넘으면 거부합니다. 더 큰 집합에는 임시 테이블이나 외부 데이터를 사용하세요. SQL 표현식이 들어 있거나 타입이 지정되지 않은 값들이 서로 다른 타입으로 섞여 있어도 목록은 요소마다 매개변수 하나를 사용합니다.

`ck.and(...)`는 `undefined`를 건너뛰므로 동적 필터를 인라인으로 쉽게 조합할 수 있습니다:

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

`undefined`는 조건 객체 수준에서만 건너뜁니다. 조건 값은 엄격한 규칙을 따릅니다. `ck.eq(column, undefined)`는 쿼리 범위를 암묵적으로 넓히지 않고 예외를 발생시킵니다. `null`, `false`, `true`, `0`, `""`을 조건으로 그대로 쓸 수는 없습니다. SQL 표현식을 사용하세요. `null`은 NULL 허용 컬럼과 쓰기 작업에서 여전히 유효한 데이터 값으로 취급되지만, NULL 필터링은 명시적으로 작성해야 합니다:

```ts
db.select()
  .from(probeTelemetry)
  .where(ck.isNull(probeTelemetry.deletedAt));
```

불리언 컬럼도 일반 값이자 SQL 표현식입니다:

```ts
const healthChecks = ckTable("health_checks", {
  id: ckType.int32(),
  isPassing: ckType.bool("is_passing"),
});

db.select().from(healthChecks).where(ck.eq(healthChecks.isPassing, false));
db.select().from(healthChecks).where(healthChecks.isPassing);
```

일반 비교나 `IN` 목록에 `null`을 그대로 넣지 마세요. NULL 관련 조건은 명시적으로 조합하세요:

```ts
ck.or(
  ck.inArray(probeTelemetry.deletedAt, [new Date("2026-04-21T00:00:00.000Z")]),
  ck.isNull(probeTelemetry.deletedAt),
);
```

런타임에 만드는 필터가 커지면 `Predicate[]`에 조건을 모아 가변 인수를 받는 `.where(...predicates)`에 넘기는 편이 좋습니다:

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

`Predicate`는 재사용 가능한 불리언 SQL 조건식을 나타내는 공개 타입입니다. 같은 조건 객체를 `where`, `having`, 조인의 `on` 절, 그리고 `ck.exists(...)`처럼 불리언 조건을 처리하는 헬퍼에서 사용할 수 있습니다.

`Selection`은 `fn.sum(...)`, `fn.toString(...)`, `ck.expr(ckSql...)` 같은 계산 결과를 담은 빌더 값을 재사용할 때 쓰는 공개 이름입니다. `.as(...)`로 별칭을 지정하고 디코딩 방식을 재정의하려면 `.mapWith(...)`을 사용하세요. `Order`는 `ck.asc(...)`와 `ck.desc(...)`가 반환하는 절 객체입니다.

`ck.has(...)`, `ck.hasAll(...)`, `ck.hasAny(...)`는 ClickHouse 네이티브 함수에 직접 대응하며 ClickHouse의 배열, 맵, JSON 처리 규칙을 유지합니다. 왼쪽이 배열 컬럼이면 일반 JavaScript 값을 먼저 그 컬럼을 기준으로 인코딩합니다. 따라서 `Array(Date)` 필터는 쿼리 매개변수가 되기 전에 컬럼의 Date 인코딩을 거칩니다.

`where(...)`는 가변 인수를 받지만 `having(...)`는 조건 하나만 받습니다. 여러 조건을 넣는 `having`은 `ck.and(...)`나 `ck.or(...)`로 먼저 조건을 구성하세요.

`ck.contains(...)`, `ck.startsWith(...)`, `ck.endsWith(...)`과 이들의 `*IgnoreCase` 변형은 입력을 리터럴 텍스트로 처리합니다. 값은 매개변수로 처리하고, LIKE의 와일드카드 문자(`%`, `_`, `\`)는 내부에서 이스케이프합니다.

`ck.like(...)` / `ck.ilike(...)`는 패턴 문법을 그대로 적용하려는 경우에만 사용하세요. 두 API 모두 SQL 보안을 위해 값을 매개변수로 처리하지만 LIKE는 패턴 언어이므로 `%`와 `_`의 와일드카드 의미는 유지됩니다.

리터럴 텍스트 검색 예제:

```ts
import { ck } from "ck-orm";

const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
  })
  .from(probeTelemetry)
  .where(ck.contains(probeTelemetry.probeId, "probe_alpha%"));
```

고급 패턴 예제:

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

`groupBy()`와 `limitBy([...])`에는 컬럼과 `fn.*(...)`, `ck.expr(...)` 같은 헬퍼로 만든 계산된 `Selection` 값을 넘길 수 있습니다.

`limit()`, `offset()`, `limitBy(..., limit)`에 직접 넘기는 값은 음이 아닌 안전한 정수여야 합니다. ClickHouse 상수 표현식이 꼭 필요할 때만 `ckSql`을 사용하세요.

`orderBy()`에는 다음 중 하나를 넘길 수 있습니다.

- `ck.desc(selection)`
- `ck.asc(selection)`
- 컬럼을 직접 전달

### `final()`

테이블 쿼리에 테이블 단위 `FINAL`을 추가하세요.

```ts
const query = db.select().from(probeTelemetry).final();
```

별칭 없는 단순 테이블 조회에서는 ck-orm이 `FROM table FINAL`을 생성합니다. 루트 테이블에 별칭이 있거나 쿼리에 다른 소스를 조인하면 ck-orm은 `FINAL`을 적용한 테이블을 서브쿼리로 감싸고 바깥쪽 소스에는 별칭을 유지합니다. 이렇게 하면 빌더 API는 그대로 유지하면서 `FINAL`, 테이블 별칭, 조인, 람다 표현식이 얽힐 때 ClickHouse 분석기에서 생기는 예외 상황을 피할 수 있습니다.

`final()`은 테이블 루트 소스에만 적용됩니다. CTE나 서브쿼리, 테이블 함수 안에서 `FINAL`이 필요하면 `.as(...)`를 호출하기 전에 테이블 기반 쿼리에 `.final()`을 붙이세요.

### `limitBy()`

ClickHouse의 `LIMIT ... BY ...`를 사용하세요.

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

### 실행 모드

빌더 쿼리는 세 가지 방식으로 실행할 수 있습니다.

```ts
const query = db.select().from(probeTelemetry).limit(10);

const rows = await query;
const sameRows = await query.execute();

for await (const row of query.iterator()) {
  console.log(row);
}
```

애플리케이션 예제에서 실행 지점을 분명하게 보여 주려면 `.execute()`를 사용하세요. Drizzle 스타일의 편의성을 위해 `await query`도 지원합니다. 빌더 쿼리가 thenable이라는 데 익숙해진 뒤에는 이 방식이 편리합니다.

`query.iterator()`도 `db.stream()`과 같은 세션 기반 동시성 규칙을 따릅니다. 쿼리에 `session_id`가 지정되면 반복이 끝나거나 이터레이터를 일찍 닫을 때까지 요청 슬롯을 점유합니다.

### `toSQL()`로 SQL 확인하기

`toSQL()`은 빌더를 컴파일할 뿐 아무것도 전송하지 않습니다. 빌더를 await하면 실행되므로 await하지 말고 빌더에서 바로 호출하세요. `select()` 빌더와 `insert().values()`, `insert().fromSelect()`에서 사용할 수 있습니다.

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

결과에는 다음 필드가 있습니다.

- `sql`은 한 줄로 된 SQL 문이며, 값이 들어갈 자리마다 `{orm_param1:String}` 같은 자리 표시자가 들어 있습니다. `FORMAT` 절은 없습니다. 이 절은 쿼리를 전송할 때 붙습니다.
- `params`에는 자리 표시자에 채울 JavaScript 값이 인코딩하기 전 그대로 들어 있습니다. 쿼리를 실행하면 ck-orm이 이 값을 인코딩해 SQL과 별도로 전송하므로 `sql`에 직접 들어가는 일은 없습니다.
- `forcedSettings`는 빌더가 요청에 강제하는 설정 목록입니다. 예를 들어 `leftJoin()`은 `join_use_nulls: 1`을 강제합니다. 강제하는 설정이 없으면 이 필드는 없습니다.
- `body`는 `insert().values()`에만 있습니다. 행을 Values 텍스트로 담고, 이때 `sql`에는 `INSERT INTO t (cols) FORMAT Values` 문이 들어 있습니다. 다른 경우에는 없습니다.

반환값은 복사본이므로 수정해도 빌더에는 영향이 없습니다.

로그나 디버깅용으로 `sql`과 `body`를 여러 줄로 나눠 보려면 `{ pretty: true }`를 넘기세요.

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

보기 좋게 출력해도 바뀌는 것은 공백뿐입니다. 절마다 새 줄에서 시작하고, `select` 항목은 한 줄에 하나씩 나오며, 서브쿼리와 CTE는 들여씁니다. `and`나 `or` 그룹의 조건도 한 줄에 하나씩 나옵니다. 함수 인수는 같은 줄에 그대로 두고, 따옴표로 감싼 텍스트는 절대 건드리지 않습니다. SQL 주석이 들어 있는 SQL 문은 손대지 않고 그대로 반환합니다. `insert().values()`의 본문에서는 행마다 한 줄을 씁니다. 보기 좋게 출력해도 실제로 전송되는 쿼리에 영향을 주는 일은 없습니다.

### 서브쿼리와 CTE

빌더에 이름을 지정해 서브쿼리로 만들려면 `.as("alias")`를 사용하세요.

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

`.as("…")`는 서브쿼리에서는 **선택 사항입니다**. 호출하지 않으면
프레임워크가 컴파일 시점에 자동 별칭(`__sub_1`, `__sub_2`, …)을 지정합니다.
별칭 없는 빌더는 `from()`, `innerJoin()`, `leftJoin()`,
`inArray()`, `notInArray()`, `exists()`, `notExists()`에 바로 사용할 수 있습니다.

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

#### 스타일 가이드: `.as(name)`과 별칭 없는 빌더를 사용할 때

| 상황 | 권장 형태 |
|---|---|
| 재사용할 필요가 없는 일회성 인라인 서브쿼리 | 별칭 없는 빌더 + 콜백 형태의 `innerJoin/leftJoin`, 또는 `inArray/exists`에 별칭 없는 빌더 사용 |
| 서브쿼리의 컬럼 참조를 여러 위치(SELECT / WHERE / 조인 한 곳)에서 사용 | `const`에 바인딩한 뒤 별칭 없이 넘기기(참조는 메서드 체인을 따라 전달됩니다) |
| 셀프 조인(같은 논리 서브쿼리에 SQL 별칭 두 개) | `.as("a")`와 `.as("b")`를 명시적으로 호출하기. 같은 인스턴스를 소스로 두 번 재사용하면 SQL로 컴파일할 때 예외가 발생합니다 |
| 안정적이거나 읽기 쉽거나 타입이 지정된 별칭이 필요할 때(운영 환경의 `query_log` 검색, `EXPLAIN`, 슬로우 로그, 모듈 간 헬퍼, 리터럴 `TAlias` 소스 키) | `.as("descriptive_name")` 호출 |

구조가 다른 쿼리를 컴파일하면 자동 별칭은 **항상 같다고 보장되지 않지만**,
한 번의 컴파일 안에서는 안정적이고 같은 빌더 트리를 반복 컴파일할 때도 유지됩니다.
따라서 `snapshot` 스타일 테스트는 정규화할 필요가 없습니다.

#### Selection 키 충돌

SelectBuilder 메서드 이름인 `from`, `where`, `select`, `as`, `innerJoin`, `leftJoin`, `asofJoin`, `asofLeftJoin`, `groupBy`, `having`, `orderBy`, `limit`, `offset`, `final`, `limitBy`, `execute`, `iterator`, `then`, `catch`, `finally`, `buildSelectionItems`와 충돌하는 Selection 키는 별칭 없는 빌더에서 메서드 타입을 유지합니다. `sub.from`은 `from`이 선택 목록에 있어도 항상 빌더 메서드를 반환하며 컬럼 참조는 반환하지 않습니다. 이런 키는 이름을 바꾸거나(`from` → `fromAddress`), 서브쿼리를 `.as("name")`으로 감싸서 컬럼 접근이 전용 `Subquery` 객체를 거치게 하세요.

CTE에는 `$with()`와 `with()`를 사용하세요.

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

이름 없이 `$with()`를 호출하면 이름 없는 CTE를 만듭니다. 이름 없는 CTE는 쿼리 하나에서 소스로 한 번만 사용할 수 있습니다. 같은 결과를 두 번 조인하려면 조인할 때마다 CTE를 하나씩 정의하세요. 즉, `db.$with().as(...)`를 두 번 호출하거나 이름이 서로 다른 CTE 두 개를 만드세요.

### `db.count()`

`db.count(source, ...predicates)`는 Drizzle 스타일의 개수 조회 헬퍼입니다. `.where(...predicates)`와 같은 조건 처리 규칙을 따릅니다. 여러 조건은 `AND`로 결합하고 `undefined`인 조건 객체는 건너뜁니다. `ck.eq(...)` 같은 헬퍼 안의 값에 `undefined`를 넣으면 여전히 예외가 발생합니다.

```ts
import { ck } from "ck-orm";

const total = await db.count(
  probeTelemetry,
  ck.eq(probeTelemetry.status, 1),
  ck.gt(probeTelemetry.id, 1000),
);
```

복잡한 결과 집합은 서브쿼리나 CTE로 개수를 세세요.

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

`db.count(...)`의 기본 모드는 숫자를 반환합니다. `toFloat64(count())`를 생성하고 `number`로 디코딩합니다. 개수가 매우 크면 JavaScript에서 정수로 정확하게 표현할 수 있는 범위를 넘을 수 있습니다. 정확한 값이나 전송 형태가 중요하면 체이너블 모드를 사용하세요.

```ts
const approximateTotal = await db.count(activeProbes); // number
const exactTotal = await db.count(activeProbes).toSafe(); // string
const wireTotal = await db.count(activeProbes).toMixed(); // number | string
```

`.toSafe()`는 `toString(count())`을 생성하며 정확한 값을 읽을 때 사용합니다. `.toSafe()` 결과를 SQL 표현식으로 사용하면 `String` 처리 규칙을 따릅니다. SQL에서 숫자로 비교할 때는 기본 모드, `.toUnsafe()` 또는 `.toMixed()`를 사용하세요. `.toMixed()`는 `toUInt64(count())`를 생성하고 드라이버/전송 형태를 유지합니다. ck-orm의 기본 무손실 64비트 JSON 설정에서는 실제 ClickHouse 응답이 보통 `string`으로 도착합니다.

## 조인 NULL 처리 규칙
<!-- src:59e7a4e2 -->

`leftJoin()`과 `asofLeftJoin()`은 기본적으로 `join_use_nulls = 1`을 자동으로 적용해 SQL 스타일의 NULL 처리 규칙을 따릅니다.

이렇게 됩니다:

- 기본 왼쪽 조인에서는 오른쪽이 NULL 허용으로 추론됩니다.
- `join_use_nulls`를 명시적으로 끄면 추론 타입도 함께 바뀝니다.

ClickHouse 기본 조인 동작과 맞추려면 리터럴 `0`을 넘기세요. 리터럴로 추론하므로 `as const`는 필요 없습니다. 변수에서 오는 것처럼 일반 `number` 타입 값은 여전히 `| null`이 됩니다.

```ts
const rawDefaultDb = db.withSettings({
  join_use_nulls: 0,
});
```

이름 없는 빌더(`.as()` 없는 서브쿼리) 또는 이름 없는 CTE를 왼쪽 조인하면 해당 소스의 모든 컬럼 참조 타입에 `| null`이 붙습니다. `FROM` 쪽이나 안쪽 조인한 이름 없는 빌더의 참조도 포함합니다. 이 참조들은 넓은 `string` 소스 키 하나를 공유하므로, 타입만으로 어느 소스를 왼쪽 조인했는지 구분할 수 없습니다. 런타임에서는 여전히 왼쪽 조인된 쪽에만 NULL이 반환됩니다.

```ts
const rows = await db
  .select({ uid: allUsers.uid, spent: totals.spent })
  .from(allUsers)
  .leftJoin(totals, ck.eq(allUsers.uid, totals.uid));
// uid and spent are both number | null
```

`FROM` 쪽이나 안쪽 조인한 서브쿼리에는 `.as("name")`으로 이름을 붙이세요. 그러면 해당 참조는 각자의 소스 키를 유지하고 NULL을 허용하지 않는 타입으로 남으며, 왼쪽 조인된 쪽은 `| null`을 유지합니다.

조인한 쿼리를 서브쿼리, CTE, `ck.exists(...)`, `ck.inArray(...)` 소스로 재사용해도 `join_use_nulls = 1`을 강제로 적용한 설정이 유지되므로, 빌더 타입과 런타임 동작이 계속 일치합니다.

계산 표현식은 런타임에서 ClickHouse의 NULL 전파를 따릅니다. ClickHouse가 NULL을 반환하면(NULL 입력이거나 왼쪽 조인에서 짝이 없는 행) 값은 `null`입니다. TypeScript 타입은 선언한 `TData`(또는 헬퍼의 고정 타입)이며 자동으로 넓어지지 않습니다. 입력이 NULL일 수 있으면 `| null`을 선언하거나(`fn.plus<number | null>(...)`) 입력을 `fn.coalesce(...)`로 감싸세요.

## 쓰기
<!-- src:5d78750d -->

### `insert(table).values(...)`

테이블 스키마에 맞춰 타입을 검사하는 삽입이 필요하면 빌더를 사용하세요.

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

행은 `INSERT INTO t (cols) FORMAT Values` 요청 하나로 전송됩니다. 이 문에는 쿼리 매개변수가 없고 행은 요청 본문의 Values 리터럴 텍스트이므로 HTTP의 쿼리 매개변수 1,000개 제한과 값당 128 KiB 제한이 적용되지 않습니다. 생략한 컬럼에는 해당 행에만 `DEFAULT`가 적용됩니다. 이 삽입에서는 표현식 평가가 꺼져 있으므로(`input_format_values_interpret_expressions = 0` 및 `input_format_values_deduce_templates_of_expressions = 0`) 각 셀은 데이터로 처리됩니다. 계산한 값은 `insert().fromSelect()`로 넣으세요. JavaScript `Date`는 정확한 시각을 컬럼 정밀도에 맞춰 잘라서 보냅니다. `DateTime` 컬럼은 초 단위만 유지합니다. `DateTime` 또는 `DateTime64` 컬럼에 넣은 숫자는 Unix 초로 해석됩니다. 문자열은 컬럼 시간대 기준으로 읽습니다.

삽입할 행의 키는 테이블 스키마에 있는 키여야 합니다. 알 수 없는 키는 쿼리를 보내기 전에 거부됩니다.

### `insert(table).fromSelect(select)`

`.fromSelect(...)`를 사용하면 `INSERT INTO target (cols) SELECT ...` 문 하나로 컴파일하고, ClickHouse가 SELECT 결과를 서버에서 구체화합니다. 행이 Node 프로세스를 거치지 않으므로 큰 원본 테이블에서 필요한 범위를 한 번만 좁힌 뒤 그 구체화한 결과를 `runInSession` 안의 여러 후속 쿼리에서 재사용할 때 적합합니다.

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

규칙:

- SELECT는 대상 테이블의 필수 컬럼을 모두 프로젝션해야 합니다. `.default(...)`로 선언한 컬럼은 생략할 수 있습니다. 빠진 필수 컬럼은 컴파일 시점(TypeScript)과 런타임에서 모두 거부됩니다.
- INSERT 컬럼 목록은 SELECT 프로젝션 키 순서로 생성됩니다. ClickHouse는 컬럼을 위치로 맞추지만, 호출자에게는 `name-aligned` 방식으로 동작합니다. 어떤 값이 어느 대상 컬럼에 들어갈지는 프로젝션 키 순서가 아니라 키 이름으로 결정됩니다.
- 대상 테이블에 없는 알 수 없는 프로젝션 키와 컬럼별 타입 불일치는 쿼리를 보내기 전에 TypeScript 오류로 드러납니다.
- 하나의 삽입 메서드 체인에서 `.values()`와 `.fromSelect()`는 함께 쓸 수 없습니다. 빌더 타입이 좁아져서 사용하지 않는 메서드가 자동 완성에서 사라지고, 런타임에서도 해당 호출을 거부합니다.
- 컴파일된 전송 요청은 일반 ClickHouse 명령입니다. 쿼리 이벤트에는 `mode: "insert"`가 들어가고 `tableName`은 대상 테이블로 설정됩니다.
- `.fromSelect()`는 서브쿼리 래퍼가 아니라 `SelectBuilder`(`session.select({...}).from(...)`)를 받습니다. 결과가 다른 SELECT 안에서 CTE/서브쿼리로 필요할 때만 `.as("name")`을 사용하세요. `.fromSelect()`에 넘길 때는 필요 없습니다.

#### 중첩 컬럼

`ckType.nested(...)` 컬럼은 `.fromSelect()`와 다음 세 가지 방식으로 사용할 수 있습니다.

1. **프로젝션에서 중첩 키를 생략합니다.** 기본 모드이며 ck-orm의 런타임 계약을 반영합니다. 중첩 값이 없을 때는 SQL `DEFAULT`로 인코딩하며, ClickHouse가 모든 필드에 빈 병렬 배열을 채웁니다. TS 계층은 이 동작에 맞춰 `$inferInsert`에서 중첩 컬럼을 `optional`로 취급합니다.
2. **중첩 컬럼 참조를 직접 프로젝션합니다.** 예를 들어 `select({ events: src.events })`처럼 씁니다. ck-orm은 내부 SELECT를 서브쿼리로 감싸고 바깥 프로젝션을 필드별 점 경로 접근(`__ck_inner.events.name`, `__ck_inner.events.score`, …)으로 바꿉니다. 소스의 중첩 구조에는 대상 중첩 컬럼에 필요한 모든 필드가 있어야 합니다.
3. **계산 표현식을 중첩 컬럼으로 프로젝션하면** 컴파일 시점에 거부됩니다. SQL 표현식 하나를 병렬 배열 필드로 쪼갤 수 없습니다. 행 값은 `.values(...)` 또는 `.insertJsonEachRow(...)`를 사용하세요.

중첩 데이터를 항상 넘기도록 TS 계층에서 강제하려면(`Pattern D`: 빈 중첩 배열을 허용하지 않는 비즈니스 계약) `ckType.nested({...}).requiredOnInsert()`를 사용하세요. 이 메서드 체인은 `$inferInsert` 형태에서 `events`를 필수로 표시한 컬럼을 반환하고, `.fromSelect()`에서는 중첩 컬럼 참조 프로젝션 모드와 행 단위 대안 중 하나가 필요합니다.

> **JSON 전송 형식 참고**: `.values()`는 JSON 컬럼을 Values 본문에서 따옴표로 감싼 `JSON.stringify(...)` 텍스트로 보냅니다. ClickHouse는 그 리터럴을 JSON 컬럼으로 파싱합니다. `.fromSelect()`는 완전히 서버에서 실행하므로 JSON 값이 JS 프로세스를 왕복하지 않습니다. 두 경로 모두 저장되는 데이터는 동일합니다.

### `insertJsonEachRow()`

객체 형태의 행이나 비동기 행 스트림이 이미 있다면 `insertJsonEachRow()`를 사용하세요.

```ts
await db.insertJsonEachRow("tmp_scope", [
  { probe_id: "probe_alpha" },
  { probe_id: "probe_beta" },
]);
```

다음 값을 받을 수 있습니다:

- 문자열 테이블 이름
- `ckTable()`으로 만든 테이블 객체
- 일반 배열
- `AsyncIterable`

빈 일반 배열은 클라이언트에서 아무 작업도 하지 않는 것으로 처리되지만, 플러그인 훅에는 `rowCount: 0`과 함께 성공한 삽입 라이프사이클을 계속 보고합니다. 알 수 없는 필드의 동작은 ClickHouse가 결정합니다. 서버가 추가 JSON 필드를 무시하게 하려면 `input_format_skip_unknown_fields: 1` 같은 설정을 넘기세요. 중첩 컬럼은 평탄화된 `column.field` 배열로 전송됩니다(ClickHouse `flatten_nested = 1`이 기본값). 따라서 `{ lines: [{ sku: "a" }] }`는 `insert().values()`와 `insertJsonEachRow()`에서 모두 동작합니다.

테이블 스키마가 있으면 `insertJsonEachRow()`는 `DateTime`/`DateTime64` 컬럼의 JavaScript `Date`를 `insert().values()`와 똑같은 정수 틱으로 보냅니다. 문자열은 ClickHouse 기본 `date_time_input_format = basic` 파서에 맡기므로 두 방식이 날짜/시간 문자열을 같은 방식으로 읽습니다. 직접 넘긴 JSON 숫자를 `DateTime64` 컬럼에 넣으면 `JSONEachRow`에서는 여전히 틱으로 읽고 `.values()`에서는 Unix 초로 읽습니다.

테이블 이름만 있는 `insertJsonEachRow("table_name", rows)`에는 컬럼 타입이 없습니다. 따라서 `Date`는 `Z`가 포함된 ISO 8601로 보내고, ck-orm은 해당 요청에 `date_time_input_format = best_effort`를 설정합니다. 스키마가 있는 테이블의 행에서 스키마에 없는 키도 같은 규칙을 따릅니다. ck-orm은 이런 필드를 그대로 통과시키므로 해당 키의 `Date`에는 호출 시 `clickhouse_settings: { date_time_input_format: "best_effort" }`가 필요합니다. ClickHouse 25.8에서는 `best_effort` 파서가 소수 초가 포함된 1970년 이전 시각을 잘못 읽고 `12:34:56` 같은 `Time` 문자열을 거부합니다. 스키마로 타입이 지정된 컬럼을 사용하면 이 두 문제를 모두 피할 수 있습니다.

## 원시 SQL
<!-- src:f47b381e -->

`ck-orm`에는 자체 SQL 템플릿 API가 있습니다. 이미 작성하려는 SQL을 빌더 문법으로 표현하는 것보다 직접 쓰는 편이 더 간단할 때 사용하세요.

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

### `ckSql.join()`과 `ckSql.identifier()`

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

### `query_params`를 사용하는 원시 SQL

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

매개변수 전송 방식은 자동으로 선택됩니다. `query_params`의 멀티파트 처리는 따로 설정할 필요가 없습니다.

`query_params`에서 `orm_param`으로 시작하는 키는 거부됩니다. 이 접두사는 `` ckSql`...` ``가 내부적으로 생성하는 매개변수용으로 예약되어 있습니다.

값 포매터는 원시 값, `Date`, `NaN`, `Infinity`, 배열, 객체, `Map` 값을 지원하며 `{ids:Array(UInt64)}` 또는 `{attrs:Map(String, String)}` 같은 ClickHouse 타입 자리 표시자에 사용할 수 있습니다. `query_params`로 넘긴 값에는 자체 컬럼 타입이 없으므로 배열은 항상 Array 텍스트로 전송됩니다. `Tuple(...)` 자리 표시자는 여기서 지원하지 않습니다. 배열 안의 64비트 정수는 `bigint`여야 하며, 숫자 요소 타입에 문자열을 넘기면 따옴표로 감싼 뒤 거부됩니다. `Date`는 UTC 기준 시각 텍스트로 전송되고 ClickHouse는 이를 자리 표시자 타입의 시간대 기준으로 읽으므로, 세션 또는 서버 시간대에 따라 값이 바뀌지 않게 하려면 `{at:DateTime64(3, 'UTC')}`로 선언하세요. `` ckSql`...${date}` `` 보간은 이 과정을 자동으로 처리합니다. 매개변수가 테이블 이름이나 컬럼 이름이면 ClickHouse의 `Identifier` 자리 표시자 타입을 사용하세요.

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

`ck.expr()`로 SQL 조각을 재사용 가능한 `Selection`으로 감싸세요.

```ts
import { ck, ckSql } from "ck-orm";

const query = db.select({
  constantOne: ck.expr(ckSql`1`).as("constant_one"),
});
```

조각에 타입이 있으면 `ck.expr()`는 그 타입을 유지합니다. 타입이 없는 조각은 의도적으로 `unknown`으로 남습니다. 스키마에 없는 결과 타입을 알고 있다면 제네릭이나 디코더를 넘기세요.

```ts
const status = ck.expr(ckSql<string>`'closed'`); // Selection<string>
const enabled = ck.expr<boolean>(ckSql`1`, {
  decoder: (value) => Number(value) === 1,
});
```

### 원시 쿼리 형식

원시 쿼리를 한 번에 실행할 때는 `JSON` 출력만 지원합니다.

```ts
const rows = await db.execute(ckSql`select 1`, {
  format: "JSON",
});
```

원시 스트리밍 쿼리는 `JSONEachRow` 출력만 지원합니다. 기본 `compression.response: false`에서는 스트림을 압축하지 않으므로 각 행이 ClickHouse에서 생성되는 대로 도착합니다.

```ts
for await (const row of db.stream(ckSql`select 1`, {
  format: "JSONEachRow",
})) {
  console.log(row);
}
```

### 표현식의 Decimal 정밀도

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

- `sum` / `sumIf`는 P를 38 이상으로 넓히고 `min` / `max`는 컬럼의 P를 유지합니다. 스키마 Decimal 컬럼에서 `fn.sum()`과 `fn.sumIf()`는 `Selection<string>`이고, Float 컬럼은 `Selection<number>`입니다. 자동 캐스트는 `nullable(decimal(...))`과 `lowCardinality(decimal(...))`을 거쳐도 동작합니다.
- `maxIf(value, condition)`은 `max`와 같은 Decimal 규칙을 따릅니다. Decimal 결과는 선언한 `Decimal(P, S)` 형태의 `Selection<string>`입니다.
- `avg`는 자동 캐스트 대상이 **아닙니다**. ClickHouse는 `avg(Decimal)`을 Float64로 계산하므로 `fn.avg`는 `Selection<number>`를 반환합니다. 정확한 Decimal 평균에는 `ckSql.decimal(ckSql\`sum(x) / count(x)\`, P, S)`를 사용하세요.
- `fn.divideDecimal(left, right, resultScale?)`은 ClickHouse `divideDecimal`에 직접 대응합니다. 정밀도를 잃지 않도록 `Selection<string>`을 반환합니다. `resultScale`을 넘기면 `0`에서 `76` 사이의 리터럴 정수여야 하며 인라인으로 출력됩니다. 빌더는 ClickHouse 함수의 버전을 확인하거나 모방하지 않습니다.
- `column.cast(P, S)`는 집계가 아니라 컬럼을 캐스트합니다. `GROUP BY` 안에서 집계 없이 그대로 쓰면 `NOT_AN_AGGREGATE` 예외가 발생합니다. `fn.sum(column)`을 사용하거나 집계를 감싸세요.
- 삽입 시 문자열이나 숫자가 아닌 객체(예: 원시 `decimal.js` 인스턴스)는 거부됩니다. `.toFixed(scale)`로 변환해 넘기세요.

```ts
db.insert(measurementLog).values({ value: new Decimal("1.23").toFixed(5) }); // ✅
db.insert(measurementLog).values({ value: new Decimal("1.23") as never }); // ❌ throws
```

## 윈도 표현식
<!-- src:6d0b3f76 -->

`fn.*` 함수 표현식은 `fn.over(...)`로 감싸세요. 선택적 옵션 객체에서 `partitionBy`에는 빌더 Selection 값만, `orderBy`에는 일반 `ck.asc(...)` / `ck.desc(...)` 값만 지정할 수 있습니다.

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

`fn.rowNumber()`의 기본값은 `toFloat64`를 사용하는 안전하지 않은 `Selection<number>`입니다. 정확한 `Selection<string>`이 필요하면 `fn.over(...)` 전에 `.toSafe()`를 호출하고, 네이티브 `UInt64` 전송 형태(`Selection<number | string>`)가 필요하면 `.toMixed()`를 호출하세요. `.toUnsafe()`는 기본 동작을 명시적으로 선택합니다. 빌더는 의도적으로 JavaScript 안전 정수 검사를 추가하지 않습니다. ClickHouse의 `toFloat64` 변환은 `Number.MAX_SAFE_INTEGER`보다 큰 값을 반올림할 수 있습니다. `.toSafe()`는 정확한 10진수 문자열을 유지하고 `.toMixed()`는 드라이버의 네이티브 결과 형태를 유지합니다. Decimal 집계는 기존 문자열 디코더를 유지하며 `CAST(sum(...) OVER (...) AS Decimal(...))`로 컴파일됩니다. 따라서 `OVER`는 캐스트 바깥이 아니라 집계 함수 안에 남습니다. `fn.over`는 단일 컬럼이나 원시 SQL 조각을 의도적으로 받지 않습니다. 이 API는 우선 `PARTITION BY`와 `ORDER BY`만 지원합니다. 프레임, 이름 있는 윈도, 그 밖의 ClickHouse 전용 절에는 `ckSql`을 사용하세요.

## 함수와 테이블 함수
<!-- src:1e16409b -->

### `fn`

범용, 변환, 집계, JSON, 튜플, 테이블 관련 헬퍼를 제공합니다.

- `fn.call()`
- `fn.withParams()`
- `fn.replaceRegexpAll()` — 타입이 지정된 ClickHouse RE2 치환을 제공합니다. 자세한 내용은 [정규식 치환](#regex-replacement)에서 확인하세요.
- 타입 변환 헬퍼는 ClickHouse의 타입 변환 페이지를 따릅니다: `fn.cast()`, `fn.date()`, `fn.accurateCast*()`, `fn.reinterpret*()`, `fn.parseDateTime*()`, `fn.formatDateTime()`, `fn.formatRow*()`, `fn.toString*()`, `fn.toBool()`, `fn.toInt*()`, `fn.toUInt*()`, `fn.toFloat*()`, `fn.toBFloat16*()`, `fn.toDecimal*()`, `fn.toDecimalString()`, `fn.toFixedString()`, `fn.toDate*()`, `fn.toDateTime*()`, `fn.toTime*()`, `fn.toTime64*()`, `fn.toInterval*()`, `fn.toLowCardinality()`, `fn.toNullable()`, `fn.toUUID*()`.
- 64비트 이상 정수 변환(`toInt64/128/256*`, `toUInt64/128/256*`, Unix 타임스탬프 64 헬퍼)은 `string`으로 디코딩합니다. 그보다 비트 폭이 작은 정수와 부동 소수점 변환은 `number`로 디코딩합니다.
- 리터럴만 허용하는 ClickHouse 인수는 검증한 뒤 필요한 곳에 인라인합니다. 여기에는 Decimal 스케일, DateTime64/Time64 정밀도, FixedString 길이, 대상 타입 문자열, 간격 단위가 포함됩니다.
- `fn.toStartOfMonth()`
- `fn.toUnixTimestamp()` / `fn.toUnixTimestamp64Second()` / `fn.toUnixTimestamp64Milli()` / `fn.toUnixTimestamp64Micro()` / `fn.toUnixTimestamp64Nano()`
- `fn.fromUnixTimestamp()` / `fn.fromUnixTimestamp64Second()` / `fn.fromUnixTimestamp64Milli()` / `fn.fromUnixTimestamp64Micro()` / `fn.fromUnixTimestamp64Nano()`
- `fn.count()` / `fn.countIf()` — 기본 `Selection<number>`은 `toFloat64(count(...))`로 감싼 형태입니다. `.toSafe()`를 연결하면 `Selection<string>`(`toString(count(...))`)이 되고, `.toMixed()`를 연결하면 `Selection<number | string>`(`toUInt64(count(...))`)이 됩니다. `.toUnsafe()`로 기본 모드로 돌아갑니다. `db.count`와 같은 동작입니다. 디코더는 음이 아닌 정수만 허용하며 `NaN`, 음수, 불리언 등은 거부합니다. 예제는 [`fn.count` / `fn.uniqExact` 모드](#fncount--fnuniqexact-modes)에서 확인하세요.
- `fn.sum()` / `fn.sumIf()` / `fn.min()` / `fn.max()` / `fn.maxIf()` — Decimal 컬럼은 자동으로 `Decimal(P, S)`로 형변환합니다. 자세한 내용은 [표현식의 Decimal 정밀도](#decimal-precision-in-expressions)에서 확인하세요.
- `fn.over()` / `fn.rowNumber()` — `PARTITION BY`와 `ORDER BY`로 윈도 함수를 조합합니다. 자세한 내용은 [윈도 표현식](#window-expressions)에서 확인하세요.
- `fn.divideDecimal()` — 정확한 Decimal 나눗셈을 제공하며 결과 스케일을 인라인으로 지정할 수도 있습니다. 자세한 내용은 [표현식의 Decimal 정밀도](#decimal-precision-in-expressions)에서 확인하세요.
- `fn.argMax()` / `fn.argMin()` — `val`이 최대 또는 최소인 행에서 `arg` 값을 반환합니다. 반환 타입은 `arg` 컬럼을 그대로 따르며 Decimal CAST로 감싸지 않습니다. ClickHouse는 컬럼에 선언된 타입을 유지합니다.
- `fn.avg()` — `Selection<number>`(Float64)를 반환하며 ClickHouse 네이티브 `avg(Decimal)` 동작과 같습니다.
- `fn.uniqExact()` — `fn.count()`와 같은 세 가지 체이너블 모드가 있습니다. 기본 `Selection<number>`은 `toFloat64(uniqExact(...))`로 감싼 형태이고, `.toSafe()`는 `Selection<string>`(`toString(uniqExact(...))`), `.toMixed()`는 `Selection<number | string>`(`toUInt64(uniqExact(...))`)을 반환합니다. `.toUnsafe()`로 기본 모드로 돌아갑니다. 디코더의 음이 아닌 정수 검사도 같습니다. 자세한 내용은 [`fn.count` / `fn.uniqExact` 모드](#fncount--fnuniqexact-modes)에서 확인하세요.
- `fn.coalesce()`
- `fn.if()` / `fn.multiIf()` — 조건부 함수입니다. ClickHouse가 모든 값 분기에서 서버 결과 타입을 결정하므로 둘 다 `Selection<unknown>`을 반환합니다.
- `fn.greatest()` / `fn.least()` / `fn.nullIf()` — 비교 함수입니다. `nullIf`는 이미 NULL을 허용하는 입력은 다시 감싸지 않고, sqlType을 `Nullable(...)`로 자동으로 감쌉니다.
- `fn.positionCaseInsensitive()` / `fn.positionCaseInsensitiveUTF8()` — 대소문자를 구분하지 않는 부분 문자열 검색과 UTF-8 부분 문자열 검색을 제공합니다. `Selection<string>`을 반환하며, ClickHouse의 `UInt64` 결과를 64비트 정밀도를 보존하도록 문자열로 디코딩합니다(`fn.indexOf()`와 같은 동작입니다). 선택적 `startPos`는 `UInt*` 타입이어야 합니다. UInt 컬럼을 넘기거나 리터럴을 `fn.toUInt64(n)`으로 변환하세요. 숫자 리터럴을 그대로 쓰면 `Int64`가 되어 ClickHouse가 거부합니다.
- `fn.jsonExtract()`
- `fn.tuple()`
- `fn.arrayJoin()`
- `fn.tupleElement()`
- `fn.not()`

### 조건 분기 타입

조건 결과용 제네릭을 제거했습니다. 따라서 `fn.if<TData>(...)`와
`fn.multiIf<TData>(...)`는 더는 컴파일되지 않습니다. 알려진 `Float64` 또는 `UInt64` 앵커는
호환되는 안전 정수 리터럴 분기에 문맥상 타입을 부여하며, `UInt64` 리터럴은
음이 아니어야 합니다. 서로 다른 타입의 표현식과 원시 SQL은 자동으로 형변환하지 않으므로
모든 값 분기를 명시적으로 변환하세요. `.mapWith(...)`은 JavaScript 디코더만
정의하고 SQL 타입은 바꾸지 않습니다. 특히
`fn.over(fn.rowNumber().toMixed())` 옆의 음이 아닌 안전 정수 폴백은 `UInt64`로
컴파일됩니다. `fn.if(...)`와 `fn.multiIf(...)`는 스칼라라서 `fn.over(...)`에 직접
넘길 수 없습니다. 다만 값 분기에는 윈도 선택을 넣을 수 있습니다.

모든 값 분기에서 하나의 SQL 타입을 확인할 수 없으면 결과 메타데이터는 unknown으로
남고 디코딩은 값을 그대로 통과시킵니다. 따라서 기존 결과가 `number`에서 전송
형태인 `string`으로 바뀔 수 있습니다. 모든 값 분기를 하나의 SQL 타입으로 명시적으로
변환한 뒤 필요한 경우 `.mapWith(...)`을 적용해 마이그레이션하세요. 자동 결과
메타데이터는 정확한 `Bool`, `Int64`, `UInt64`, `Float64`, `String` 앵커에만 적용합니다.
분기 메타데이터가 같아도 다른 타입은 그대로 전달합니다. ClickHouse가
동일한 스토리지 타입을 정규화할 수 있기 때문입니다(예:
`LowCardinality(String)`가 `String`이 됨).

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

`fn.coalesce(...)`는 더는 폴백을 `Float32`, `BFloat16`, `Decimal` 또는 대상 범위가
안전하다고 입증할 수 없는 `bigint`로 문맥상 타입 지정하지 않습니다. 해당 폴백은
명시적으로 변환하세요.

```ts
fn.coalesce(amount, fn.toDecimal64("0.00", 2));
```

### 정규식 치환

`fn.replaceRegexpAll(haystack, pattern, replacement)`는 ClickHouse의
[`replaceRegexpAll`](https://clickhouse.com/docs/sql-reference/functions/string-replace-functions#replaceregexpall) 호출로
변환됩니다. `Selection<string>`을 반환하며, 타입이 지정된 입력이 NULL을 허용하면
`Selection<string | null>`을 반환합니다. RE2의 백슬래시와 캡처 그룹 치환에는 `String.raw`를
사용하세요.

```ts
const usDate = fn.replaceRegexpAll(
  orders.dateText,
  String.raw`^(\d{4})-(\d{2})-(\d{2})$`,
  String.raw`\2/\3/\1`,
);
// Selection<string>
```

인수는 모두 쿼리 매개변수로 바인딩됩니다. 패턴에는 ClickHouse RE2 문법을 사용합니다. 치환 문자열은
`\0`–`\9`를 지원하며, 잘못된 패턴은 ClickHouse 오류로 그대로 남습니다. 타입이 지정되지
않은 `ckSql` 입력은 보수적으로 NULL 허용으로 처리합니다. 조각이 NULL이 아니라고 확실하면
`ckSql<string>`으로 작성하세요.

### 타입이 지정된 튜플 집계

`fn.tuple()`은 각 요소의 타입을 유지합니다. `groupArray`에 값 하나만 넘기는 형태는 해당
튜플 타입과 디코더를 유지하고, 인자를 하나 받는 `fn.arrayReverseSort()`도 같은 배열 요소
타입을 유지합니다.

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

원시 SQL이나 동적인 경우에도 기존의 명시적 제네릭 형태를 계속 사용할 수 있습니다.

배열 헬퍼 이름은 ClickHouse [배열 함수](https://clickhouse.com/docs/sql-reference/functions/array-functions) 레퍼런스의 정식 제목을 따릅니다. 별칭만 있는 이름은 공개 API에 중복으로 추가하지 않고도 `fn.call(...)`에서 계속 사용할 수 있습니다.

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

`fn.call(name, ...)`과 `fn.withParams(name, ...)`는 컴파일 전에 `name`을 SQL 식별자로 검증합니다. `ck.has(...)`, `ck.hasAll(...)`, `ck.hasAny(...)`, `ck.hasSubstr(...)`는 같은 ClickHouse 함수를 WHERE 절에서 바로 쓸 수 있는 조건식 단축형입니다.

`fn.jsonExtract(json, returnType, ...path)`은 반환 타입으로 `ckType.*`만 받습니다. 따라서 ClickHouse 반환 타입과 TypeScript 디코더가 함께 유지됩니다.

아래 JSON과 배열 예제에서는 가상의 `probeTelemetry` 스키마에 `metadata: JSON`과 `tags: Array(String)` 컬럼도 있다고 가정합니다.

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

경로의 각 부분은 ClickHouse JSON 경로 인수입니다. 객체 필드에는 문자열 키를, 배열 위치에는 number 또는 bigint 인덱스를 사용하세요.

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

고차 배열 함수는 ClickHouse의 매개변수 순서를 따릅니다. 람다는 `ckSql`로 작성하고 바깥쪽 스키마 필드는 보간하며, 람다 내부 변수는 SQL 이름 그대로 둡니다. `fn.arrayMap(...)`, `fn.arrayFilter(...)`, `fn.range(...)`처럼 람다가 요소의 형태를 결정하는 함수는 기본적으로 `Selection<unknown[]>`을 반환합니다. 쿼리에서 요소를 더 구체적인 타입으로 디코딩해야 하면 `.mapWith(...)`를 이어서 호출하세요.

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

해당 람다에서 `${params.timeColumn}`과 `params.scopeTable.startTsList`은 스키마에 연결된 표현식입니다. `start_ts`와 `end_ts`는 ClickHouse가 배열 요소에서 만드는 람다 내부 변수입니다.

`fn.arrayJoin(array)`는 ClickHouse `arrayJoin`에 대응하며 입력 행 하나를 배열 요소마다 한 행씩 펼칩니다. 두 배열의 같은 위치에 있는 값들을 짝으로 유지해야 하면 `fn.arrayZip(...)`과 `fn.tupleElement(...)`를 함께 사용하세요.

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

중요한 것은 처리 규칙입니다. `arrayZip(sampleIds, probeIds)`는 위치별 쌍을 유지하고, `arrayJoin(...)`은 그 쌍을 행으로 펼치며, `tupleElement(..., 1/2)`는 쌍의 각 값을 읽습니다.

복합 키에는 튜플 포함 검사를 사용하세요. 독립적인 `IN` 조건식 두 개는 곱집합을 만들어 요청하지 않은 쌍까지 일치할 수 있습니다.

```ts
// Avoid this when sampleId and probeId must stay paired.
const wrong = ck.and(
  ck.inArray(probeTelemetry.sampleId, sampleIds),
  ck.inArray(probeTelemetry.probeId, probeIds),
);
```

쌍 처리 규칙에서는 튜플 하나를 튜플을 반환하는 서브쿼리 또는 쌍 목록과 비교하세요. `fn.tuple`은 각 요소 타입을 알고 있으면 그대로 전달하므로 목록은 `Array(Tuple(...))` 매개변수 하나로 바인딩됩니다.

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  [
    ["900001", "probe_alpha"],
    ["900002", "probe_beta"],
  ],
);
```

원시 SQL 조각에는 요소 타입이 없습니다. `ck.inArray`나 `ck.eq`에 사용하기 전에 `fn.cast(fn.tuple(...), "Tuple(Int32, String)")`로 튜플을 감싸세요.

튜플을 반환하는 서브쿼리에서는 같은 왼쪽을 매개변수 없이 사용할 수 있습니다.

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  scopedSamples,
);
```

배열 헬퍼는 타입이 지정된 `Selection` 값을 반환하므로 프로젝션, 필터, 그룹 및 조인에서 조합할 수 있습니다.

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

### `fn.count` / `fn.uniqExact` 모드

`fn.count()`, `fn.countIf(predicate)`, `fn.uniqExact(expression)`은 `db.count(...)`에 대응하는 집계 함수이며 같은 세 가지 모드를 따릅니다. 최상위 스칼라가 아니라 쿼리 중간(`select`, `having`, GROUP BY 절, 서브쿼리)에 카운트를 넣을 때 유용합니다. SQL 래퍼와 디코더를 공유하므로 모드별 장단점도 같습니다.

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

디코더는 음이 아닌 정수가 아닌 값을 거부합니다. 불리언, `NaN`, 소수, 음수는 `Failed to decode count() result: ...` 예외를 발생시키므로, 손상된 서버 출력은 `0`으로 조용히 바뀌지 않고 디코딩 단계에서 즉시 실패합니다.

다른 집계 함수는 `count` / `uniqExact`와 정밀도 특성이 다르므로 이 모드 API와 의도적으로 다르게 동작합니다.

- `fn.sum()` / `fn.sumIf()`는 이미 입력 컬럼별로 안전한 디코더를 선택합니다(정수와 Decimal 컬럼은 문자열, 부동 소수점 컬럼은 number). Decimal 정밀도도 자동으로 넓힙니다. `number`가 꼭 필요하면 `.mapWith(Number)`을 체인하세요.
- `fn.avg()`는 ClickHouse가 `avg(Decimal)`을 내부적으로 Float64로 처리하므로 항상 `Selection<number>`을 반환합니다. 문자열로 감싸면 실제 런타임 경로를 잘못 설명하게 됩니다.
- `fn.min()` / `fn.max()`는 입력 컬럼의 TS 타입을 유지합니다. 따라서 `min(int64Col)`은 컬럼에서 읽는 형태와 같은 `Selection<string>`입니다.

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

## 테이블과 구체화된 뷰
<!-- src:03694c38 -->

`createTable()`, `createMaterializedView()`, `dropTable()`은 각각 스키마 객체로 만든 DDL 문을 하나씩 보냅니다. 스키마를 비교하거나 `ALTER`를 실행하거나 버전을 기록하지 않습니다. 기존 테이블의 마이그레이션은 마이그레이션 도구에 두세요. 클라이언트와 세션에서 모두 사용할 수 있습니다. 이들이 만드는 테이블과 뷰는 지속되며, `runInSession()`은 삭제하지 않습니다.

### `createTable()`

`createTable(table, options?)`은 `ckTable(...)` 정의에서 렌더링한 `CREATE TABLE` 문을 보냅니다. DDL 메타데이터가 있는 컬럼 다음에 `ENGINE`, `PARTITION BY`, `PRIMARY KEY`, `ORDER BY`, `SAMPLE BY`, `TTL`, `SETTINGS`, `COMMENT`가 옵니다.

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

- 기본적으로 같은 이름의 테이블이 있으면 문이 실패하며, 오류 이름은 `TABLE_ALREADY_EXISTS`입니다.
- `mode: "if_not_exists"`는 `CREATE TABLE IF NOT EXISTS`를 렌더링하고, 컬럼이 달라도 기존 테이블을 그대로 둡니다.
- `mode: "or_replace"`는 `CREATE OR REPLACE TABLE`을 렌더링하며, 기존 테이블을 바꾸고 그 데이터를 버립니다.
- `engine`이 없는 테이블은 `MergeTree`가 되고 `ORDER BY tuple()`이 붙습니다.
- `ckAlias(...)`로 만든 별칭 테이블은 거부됩니다.

### `createMaterializedView()`

`createMaterializedView(name, { to, as, mode? })`는 삽입이 일어날 때 기존 테이블 `to`에 쓰는 구체화된 뷰를 만듭니다. `as`는 `insert(to).fromSelect(...)`와 같은 규칙의 select 빌더입니다. `to`의 필수 컬럼을 모두 프로젝션해야 하며, 알 수 없는 키나 타입 불일치는 TypeScript 오류입니다.

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

- ClickHouse는 구체화된 뷰의 출력을 컬럼 이름으로 `to`에 쓰므로, ck-orm은 select를 `select ... as <column> from (...)`로 감싸 각 키가 `to`의 해당 컬럼에 들어가게 합니다.
- `as`의 쿼리 파라미터는 뷰를 만들 때 치환되므로, 저장된 뷰는 그 값을 갖습니다.
- select가 강제하는 설정, 예를 들어 `leftJoin()`의 `join_use_nulls = 1`은 뷰의 `SETTINGS` 절에 들어갑니다. 요청 설정은 뷰와 함께 저장되지 않기 때문입니다.
- 뷰는 자신이 생긴 뒤에 삽입된 행만 봅니다. 이전 행을 채우려면 같은 select로 `db.insert(to).fromSelect(...)`를 실행하세요.
- `mode: "if_not_exists"`는 `IF NOT EXISTS`를 렌더링합니다. ClickHouse 25.8과 26.3에는 `CREATE OR REPLACE MATERIALIZED VIEW`가 없으므로 `or_replace` 모드는 없습니다.
- `to`는 필수입니다. 자체 `ENGINE`이 있는 뷰, `POPULATE`, 새로 고칠 수 있는 뷰는 지원하지 않습니다. 그런 경우는 `ckSql`과 `command()`를 사용하세요.

### `dropTable()`

`dropTable(table, options?)`은 스키마 객체 또는 이름으로 테이블이나 뷰를 삭제합니다.

```ts
await db.dropTable("usage_minutes_mv", { sync: true });
await db.dropTable(usageMinutes, { ifExists: true, sync: true });
```

- 기본적으로 `DROP TABLE name`을 렌더링하며, 그 이름의 객체가 없으면 `UNKNOWN_TABLE`로 실패합니다.
- `ifExists: true`는 `IF EXISTS`를 렌더링합니다.
- `sync: true`는 `SYNC`를 렌더링합니다. 호출은 `Atomic` 데이터베이스 엔진의 지연된 정리에 맡기지 않고, ClickHouse가 데이터를 삭제한 뒤에 반환합니다.
- 구체화된 뷰를 삭제해도 그 `to` 테이블과 이미 쓰인 행은 남습니다.
- `ckAlias(...)`로 만든 별칭 테이블은 거부되며, `ON CLUSTER`도 렌더링되지 않습니다. 클러스터에서는 마이그레이션 도구를 쓰거나 `ckSql`과 `command()`를 사용하세요.

## 세션과 임시 테이블
<!-- src:8f031227 -->

ClickHouse는 일반적인 트랜잭션을 제공하지 않습니다. 범위를 제한한 분석이나 큰 필터를 처리할 때는 세션 단위 임시 테이블이 적합한 기본 수단인 경우가 많습니다.

세션 콜백에서는 `ck-orm`이 다음 기능을 제공합니다.

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

직접 제어하는 명령으로 임시 테이블을 만들고, 세션을 정리할 때는 `ck-orm`이 테이블을 삭제하게 하려면 `registerTempTable(name)`을 사용하세요.

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

개발자가 직접 관리하고 `ckTable(...)`로 표현할 수 없는 신뢰할 수 있는 테이블 정의에만 `createTemporaryTableRaw(name, definition)`을 사용하세요.

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

`definition`은 테이블 이름 다음부터 작성합니다. `CREATE TEMPORARY TABLE`이나 테이블 이름은 넣지 마세요. 이 두 부분은 ck-orm이 렌더링하고, 식별자는 따로 검증합니다.

### 세션 동작

- 같은 `session_id`를 사용하는 요청은 세션 동시성 제한기를 공유합니다. 클라이언트 기본값은 `session_max_concurrent_requests = 1`입니다.
- 실제 ClickHouse 세션은 서버에서 한 번에 하나만 사용할 수 있습니다. 따라서 `session_max_concurrent_requests`를 `1`보다 높이면 로컬 직렬화는 해제되지만 `SESSION_IS_LOCKED` 오류가 발생할 수 있습니다.
- `createTemporaryTable()`은 정리 대상으로 테이블을 자동 등록합니다.
- `createTemporaryTable()`은 스키마 객체를 입력으로 사용합니다. 임시 테이블 라이프사이클은 스키마 자체가 아니라 `Session`에서 관리합니다.
- `createTemporaryTableRaw()`는 신뢰할 수 있는 정의에만 사용할 수 있는 임시 테이블용 원시 SQL API이며, 여러 문을 포함한 정의는 거부합니다.
- `runInSession()`은 정리 요청을 포함한 모든 요청의 `X-ClickHouse-Replica-Tag` 헤더에 해석한 `session_id`를 자동으로 넣습니다. 이 헤더는 런타임이 관리하므로 `http_headers`로 덮어쓸 수 없습니다.
- ClickHouse Cloud [복제본 인식 라우팅](https://clickhouse.com/docs/products/cloud/features/infrastructure/replica-aware-routing)은 별도로 활성화해야 하며 최선을 다해 동작할 뿐 결과를 보장하지 않습니다. 스케일링에 따라 태그가 다시 매핑될 수 있고, 브라우저에서 다른 출처로 배포한다면 CORS에서 `X-ClickHouse-Replica-Tag`를 허용해야 합니다.
- `runInSession()`은 콜백이 끝나면 등록된 임시 테이블을 제거합니다.
- 중첩된 `runInSession()` 호출은 항상 새로운 하위 세션을 만듭니다.
- 중첩 호출은 활성 상태인 상위 `session_id`를 재사용할 수 없습니다.
- 중첩된 하위 세션은 항상 다른 `session_id`를 사용하므로 부모 세션의 임시 테이블이나 같은 세션 동시성 슬롯을 공유하지 않습니다.

### 세션 동시성 계약

실제로는 다음 규칙을 따르면 됩니다.

- 같은 `session_id`를 명시적으로 지정한 요청은 기본적으로 한 번에 하나씩 처리됩니다.
- 클라이언트의 기본 `session_id`를 사용하는 요청도 같은 동시성 제한기를 거칩니다.
- `withSettings()`로 만든 하위 클라이언트가 부모와 같은 `session_id`를 사용하면 같은 동시성 제한기를 공유합니다.
- 다른 `session_id` 값은 서로 차단하지 않습니다.
- `stream()`과 빌더 `iterator()`는 이터레이터가 끝날 때까지 같은 세션 슬롯을 점유합니다. 일찍 중단하면(`break`, `return` 또는 오류) 세션 밖에서 같은 자격 증명과 복제본 태그로 `KILL QUERY`를 보내 서버 쿼리를 취소한 다음, ClickHouse가 세션을 해제할 때까지 최대 `request_timeout`(기본 30s) 동안 기다립니다.
- 중첩된 `runInSession()` 호출은 새로운 하위 `session_id`를 만듭니다. 따라서 부모의 같은 세션 슬롯을 공유하지 않습니다.

요청을 중단하거나 `request_timeout`이 만료되어도 클라이언트는 연결만 닫습니다. ClickHouse는 쿼리가 끝나거나 연결이 끊긴 것을 감지할 때까지 계속 실행하므로 `INSERT ... SELECT`가 완료될 수 있고, 같은 세션의 다음 요청은 `SESSION_IS_LOCKED` 오류로 실패할 수 있습니다. 서버가 먼저 멈추게 하려면 `clickhouse_settings`에서 `max_execution_time`을 `request_timeout`보다 작게 설정하세요. 또는 세션 밖에서 `KILL QUERY WHERE query_id = '...'`를 실행하거나, 본문을 다시 보낼 수 있는 요청에 한해 `fetch`를 감싸 373 오류를 정해진 횟수만큼 재시도하세요. `SESSION_IS_LOCKED`를 포함한 정리 실패는 `onCleanupError` 또는 `SessionError`로 보고됩니다.

다음처럼 사용하세요.

- 임시 테이블 작업 흐름과 세션을 계속 유지해야 하는 로직에서는 `session_max_concurrent_requests = 1`을 유지하세요.
- 실제로 병렬 처리가 필요하면 같은 세션 제한을 높이지 말고 다른 `session_id` 값을 사용하세요.
- 로컬 백프레셔를 없애려는 의도가 분명하고 `SESSION_IS_LOCKED`를 감수할 수 있을 때만 `session_max_concurrent_requests`를 높이세요.

## 런타임 메서드
<!-- src:3e31db64 -->

빌더 외에도 클라이언트는 저수준 런타임 메서드를 제공합니다.

각 쿼리에 공통으로 적용할 수 있는 필드는 다음과 같습니다.

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

`session_timeout`과 `session_check`는 특정 요청이나 특정 세션 블록에만 적용됩니다. 전역 클라이언트 기본값이 아니므로 이곳에서 설명합니다.
`query_id`, `session_id`, `session_timeout`은 요청/세션 옵션이므로 `clickhouse_settings` 밖에 두세요.

`session_check`는 새 ClickHouse 세션을 시작하지 않습니다. 명시적인 `session_id`가 이미 존재하는지 ClickHouse에 확인해야 할 때 사용하세요.
`session_id`가 있는 모든 요청은 원시 `execute()`, `command()`, `stream()`, 빌더 `.execute()`, 빌더 `.iterator()`를 포함해 세션별 동시성 제한기에 참여합니다.

요청별 `clickhouse_settings`는 해당 요청에서만 ClickHouse 설정을 재정의합니다.

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

원시 쿼리를 실행하고 `Record<string, unknown>[]`을 반환합니다.

```ts
const rows = await db.execute(ckSql`SELECT 1 AS one`);
```

### `stream()`

원시 쿼리 결과를 스트리밍합니다.

```ts
for await (const row of db.stream(ckSql`SELECT number FROM numbers(10)`)) {
  console.log(row);
}
```

`stream()`이 `session_id`를 사용하면 비동기 이터레이터가 끝나거나 루프에서 일찍 빠져나갈 때까지 같은 세션 슬롯을 점유합니다.

### `command()`

결과 행을 반환하지 않는 명령을 실행합니다.

```ts
await db.command(ckSql`SYSTEM FLUSH LOGS`);
```

### `withSettings()`

추가 기본 설정이 적용된 하위 클라이언트를 만듭니다.

```ts
const reportDb = db.withSettings({
  max_execution_time: 60,
  join_use_nulls: 0,
});
```

`withSettings()`는 기본 ClickHouse 세션/쿼리 설정만 바꿉니다. 반환된 클라이언트는 부모 클라이언트와 동일한 전송 계층, 인증, 시간 초과, 세션 동시성 제한기를 그대로 사용합니다. 리터럴 `join_use_nulls: 0`을 사용하면 하위 클라이언트의 왼쪽 조인 결과가 NULL을 허용하지 않는 타입으로 추론됩니다. 값을 일반 `number` 타입으로 지정하면 여전히 `| null`이 됩니다.

세션 라이프사이클 옵션은 `runInSession()`에서 ClickHouse 설정과 분리해 관리합니다.

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

### 컴파일된 쿼리 연동

대부분의 애플리케이션에서는 빌더를 직접 사용하면 됩니다. 이미 `CompiledQuery`를 만드는 다른 계층과 연동할 때만 `executeCompiled()`와 `iteratorCompiled()`를 사용하세요.

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

### `ping()`과 `replicasStatus()`

```ts
await db.ping();
await db.replicasStatus();
```

## 옵저버빌리티
<!-- src:37125a8e -->

`ck-orm`은 옵저버빌리티 계층 두 가지를 제공합니다.

- 내장 로깅을 위한 `logger`와 `logLevel`
- 쿼리 라이프사이클 훅을 위한 `plugins`. OpenTelemetry 트레이싱은 `ck-orm/otel` 플러그인으로 제공됩니다

### 로거

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

### OpenTelemetry 플러그인

`@opentelemetry/api`를 설치한 다음 플러그인을 추가하세요.

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

`tracer`를 넘기지 않으면 플러그인은 `trace.getTracer("ck-orm")`를 사용합니다. 데이터베이스 이름, 서버 주소, 서버 포트, 요청 시간 초과는 `clickhouseClient(...)` 구성에서 가져옵니다. 서비스 수준 레이블은 `attributes`로 넘기세요. `db.`로 시작하는 키는 무시하므로 애플리케이션 코드가 내장 데이터베이스 속성을 덮어쓸 수 없습니다.

`includeStatement: false`는 공유 트레이싱 백엔드에 더 안전한 설정입니다. 기본값은 `true`입니다. 테이블 이름, 컬럼 이름 또는 쿼리 형태가 서비스 경계 밖으로 나가면 안 될 때 끄세요. 바인딩된 값은 공백을 정리한 SQL 문에 포함되지 않지만 SQL 형태는 여전히 운영상 민감할 수 있습니다.

#### 서버 측 스팬

`propagateTraceContext: true`를 설정하면 스팬의 W3C 트레이스 컨텍스트(`traceparent`, `tracestate`)를 각 요청과 함께 보냅니다. 그러면 ClickHouse가 쿼리에 대한 자체 스팬(`HTTPHandler`, `query`, 파이프라인 단계 등)을 ck-orm 스팬의 자식으로 기록합니다. 플러그인은 이 두 헤더만 전달하며, 전역 propagator가 만드는 `baggage`나 벤더 헤더는 전달하지 않습니다. 또한 샘플링된 스팬에만 헤더를 붙입니다. ClickHouse는 샘플링 여부와 관계없이 받은 모든 트레이스 컨텍스트에 대해 서버 스팬을 기록하기 때문입니다. 플러그인은 전역으로 등록된 propagator를 통해 주입하며, OpenTelemetry SDK는 이를 기본적으로 W3C 트레이스 컨텍스트로 설정합니다. propagator가 등록되어 있지 않으면 아무것도 전송되지 않습니다.

```ts
const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123/telemetry_lab",
  plugins: [otelTracing({ propagateTraceContext: true })],
});
```

ClickHouse는 이 스팬을 `system.opentelemetry_span_log`에 기록할 뿐 내보내지는 않습니다. 트레이싱 백엔드로 내보내는 방법은 [Tracing ClickHouse with OpenTelemetry](https://clickhouse.com/docs/operations/opentelemetry)를 참고하세요. 서버 스팬에는 ck-orm 스팬의 `db.query.id`와 일치하는 `clickhouse.query_id`가 들어 있으며, 실패는 스팬 상태가 아니라 `clickhouse.exception_code`로 보고합니다.

`http_headers`에 직접 `traceparent`를 넘기면 플러그인의 값보다 우선합니다. 이때 대응하는 `tracestate`도 함께 넘기세요. 그렇지 않으면 플러그인의 `tracestate`가 함께 전송됩니다.

브라우저에서는 ClickHouse가 `traceparent` 헤더를 허용하지 않으면 `traceparent` 때문에 요청이 CORS 사전 요청(preflight)에서 실패합니다. 샘플링된 요청에만 이 헤더가 붙으므로 실패가 무작위로 보입니다. 기본 `http_options_response`는 이 헤더를 허용하지 않으므로, `config.d` 파일에 추가한 뒤에만 브라우저에서 전파를 활성화하세요.

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

### 플러그인 작성

플러그인은 `name`과 최대 세 개의 훅을 가진 객체입니다.

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

`onQueryStart`는 해당 요청에 사용할 헤더를 반환할 수 있습니다.

```ts
const requestSource: ClickHouseORMPlugin = {
  name: "request-source",
  onQueryStart() {
    return { http_headers: { "x-request-source": "billing-worker" } };
  },
};
```

플러그인에는 다음 규칙이 적용됩니다.

- 훅은 ck-orm이 ClickHouse로 보내는 모든 요청(쿼리, 스트림, 명령, 삽입, DDL 헬퍼, `ping()`, `replicasStatus()`, `runInSession()`의 임시 테이블 정리)에서 실행됩니다. 중단된 스트림을 취소하고 해당 세션이 잠금 해제되기를 기다리는 요청(`KILL QUERY`와 그 이후의 폴링)에서만 훅이 실행되지 않습니다.
- 모든 `onQueryStart`에는 같은 `executionId`를 가진 `onQuerySuccess` 또는 `onQueryError`가 정확히 하나씩 뒤따릅니다. 스트림의 경우 이터레이터가 끝나거나 닫힐 때 뒤따릅니다. 호출자가 중간에 멈춘 스트림은 성공으로 처리되며 그때까지 읽은 행 수를 보고합니다. 요청별 상태는 `executionId`를 키로 관리하세요.
- `onQueryStart` 훅은 등록 순서대로 실행되며, 요청이 전송되기 전에 모두 끝납니다. `onQuerySuccess`와 `onQueryError`는 역순으로 실행됩니다. 내장 로거는 항상 가장 바깥에서 실행됩니다.
- 나중에 등록한 플러그인의 헤더가 앞선 플러그인의 헤더를 덮어씁니다. 클라이언트 수준이든 요청별이든 명시적인 `http_headers`는 플러그인 헤더를 덮어씁니다. 플러그인은 연결 자격 증명으로 만든 `Authorization` 헤더와 세션이 보내는 `X-ClickHouse-Replica-Tag` 헤더를 덮어쓸 수 없습니다.
- 예외를 던지는 훅은 플러그인 이름과 함께 로그에 기록되며 쿼리나 다른 플러그인에는 영향을 주지 않습니다. 예외를 던지거나 잘못된 헤더를 반환한 `onQueryStart`의 헤더는 버려집니다.
- 이벤트는 읽기 전용입니다. 플러그인은 SQL, 매개변수, 설정, 결과를 바꿀 수 없습니다.
- 훅은 쿼리 경로에서 await됩니다. 네트워크 내보내기처럼 느린 작업은 훅에서 await하지 말고 버퍼링하세요.

공개 옵저버빌리티 타입은 다음과 같습니다.

- `ClickHouseORMPlugin`
- `ClickHouseORMQueryStartResult`
- `ClickHouseORMQueryEvent`
- `ClickHouseORMQueryResultEvent`
- `ClickHouseORMQueryErrorEvent`
- `ClickHouseORMQueryStatistics`
- `ClickHouseORMTracingOptions` (`ck-orm/otel`에서 제공)
- `ClickHouseORMLogLevel`

내장 이벤트 필드에는 `databaseName`, `serverAddress`, `serverPort`, `requestTimeoutMs`, `statementHash`, `querySummary`, `queryId`, `sessionId`가 포함됩니다. ck-orm이 원시 SQL을 보고 추측하지 않아도 테이블 하나를 식별할 수 있을 때는 `tableName`도 포함됩니다. JSON 결과를 한 번에 받는 쿼리에는 서버가 통계를 반환하면 `serverElapsedMs`, `readRows`, `readBytes`, `resultRows`, `rowsBeforeLimitAtLeast` 같은 ClickHouse 응답 통계도 포함됩니다.

## 오류 모델
<!-- src:b4b93db0 -->

공개 오류 타입(타입 전용 export):

- `ClickHouseORMError`
- `DecodeError`

런타임 오류 검사:

- `isClickHouseORMError(error)`
- `isDecodeError(error)`

`ClickHouseORMError`는 가능한 한 많은 관련 정보를 보존하며 다음 필드를 포함합니다:

- `kind`
- `executionState`
- `queryId`
- `sessionId`
- `httpStatus`
- `clickhouseCode`
- `clickhouseName`
- `responseText`
- `requestTimeoutMs`

현재 공개 `kind` 값:

- `client_validation`
- `request_failed`
- `decode`
- `timeout`
- `aborted`
- `session`

전체 오류 정보는 서버 로그에 남기고, 애플리케이션 호출자에게는 간단한 공개 오류만 반환하세요:

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

신뢰할 수 없는 클라이언트에 `responseText`나 오류의 원시 `message`를 반환하지 마세요. ClickHouse 오류 텍스트에는 SQL 조각과 객체 이름이 들어갈 수 있습니다.

## 보안
<!-- src:062d0032 -->

`ck-orm`은 기본적으로 안전하지 않은 SQL을 만들기 어렵게 설계했습니다.

내장 보호 기능은 다음과 같습니다:

- `ckSql.identifier()`에 넘긴 식별자는 검증하고 인용합니다
- `fn.call(...)`, `fn.withParams(...)`, `ckType.aggregateFunction(...)`, `ckType.simpleAggregateFunction(...)`에서 사용하는 함수 이름을 검증합니다. `AggregateFunction` 매개변수는 문자열 또는 숫자 리터럴이어야 합니다
- `ckType.nested({...})`에 넘긴 중첩 필드 키는 ClickHouse 식별자인지 검증합니다
- `` ckSql`...` ``에 보간한 값은 원시 SQL 텍스트 대신 ClickHouse 이름 있는 매개변수가 됩니다
- `orm_param`으로 시작하는 `query_params` 키는 내부 SQL 템플릿 매개변수에 예약된 접두사이므로 거부됩니다
- 각 요청에는 최상위 문 하나만 허용합니다
- 연결 구성에서 만든 인증 헤더는 호출자가 넘긴 `http_headers`로 재정의할 수 없습니다
- 구조화된 연결 구성은 `host`에 포함된 자격 증명을 거부합니다
- 트레이싱에 포함할 연결 정보는 스팬에 추가하기 전에 정규화합니다

### 신뢰할 수 있는 코드 전용 API

`ckTable(...)`로 표현할 수 없고 신뢰할 수 있는 코드에서만 실행해야 하는 임시 테이블 DDL을 위해, `ck-orm`은 `runInSession()` 안에서만 사용할 수 있는 원시 SQL API를 제공합니다:

- `session.createTemporaryTableRaw(name, definition)`

`fn.call(name, ...)`과 `fn.withParams(name, ...)`은 `name`을 검증하지만, 동적으로 선택한 함수 이름은 신뢰할 수 없는 런타임 입력이 아니라 개발자가 제어하는 입력으로 다루세요.

`ckSql.identifier(...)`는 컴파일/실행 경계에서 검증합니다. 조각을 만드는 작업은 가볍고, 오류는 조각을 쿼리로 컴파일할 때 나타납니다.

### 트레이싱 데이터 노출

`includeStatement`의 기본값이 `true`이므로 `otelTracing()`은 공백을 정리한 SQL 텍스트를 스팬의 `db.statement`와 `db.query.text`에 첨부합니다.

바인딩된 값은 ClickHouse 이름 있는 매개변수로 따로 저장되므로 SQL 텍스트에는 포함되지 않습니다. 그래도 테이블 이름, 컬럼 이름, 쿼리 형태는 트레이싱 출력에 나타납니다. 트레이싱 백엔드를 공유하거나 외부 호스트에서 운영한다면 `includeStatement`를 끄거나 컬렉터에서 `db.statement` / `db.query.text`를 필터링하세요.

ClickHouse 서버 스팬도 `system.opentelemetry_span_log`에 SQL 문을 `db.statement`로 기록합니다. 이 테이블은 내보내지 않는 한 ClickHouse 서버에 머뭅니다.

### 오류 응답

`ClickHouseORMError.responseText`와 오류의 `message`에는 SQL 조각과 데이터베이스 객체 이름을 포함한 ClickHouse 서버의 원시 텍스트가 들어갈 수 있습니다.

신뢰할 수 없는 클라이언트에 그 세부 정보를 그대로 전달하지 마세요. 서버 쪽 로그에 남기고 대신 일반 메시지를 노출하세요.

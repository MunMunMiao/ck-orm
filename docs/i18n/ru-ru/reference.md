## Установка
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

У ck-orm нет обязательных зависимостей. Трассировка OpenTelemetry вынесена в отдельную точку входа `ck-orm/otel`, а `@opentelemetry/api` — необязательная peer-зависимость, которую использует только она. Основная точка входа `ck-orm` никогда не импортирует OpenTelemetry, поэтому, если трассировка не нужна, приложение, браузерный бандл или edge-воркер не устанавливают и не включают в сборку ничего из OpenTelemetry. Чтобы трассировать запросы, выполните настройку из раздела [Плагин OpenTelemetry](#opentelemetry-plugin).

## Быстрый старт
<!-- src:847057c0 -->

Ниже всё построено на одной таблице — так проще увидеть основной сценарий.

### 1. Определите схему

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

### 2. Создайте клиент

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

### 3. Запросите данные

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

Запросы билдера — thenable, поэтому `await query` сразу выполняет запрос.

`fn.sum` для столбца `Decimal` автоматически приводит результат к `Decimal(P, S)` и возвращает `string`, чтобы сохранить точность — см. [Точность Decimal в выражениях](#decimal-precision-in-expressions).

### 4. Наблюдайте за запросами

`plugins` подключаются к каждому запросу, который ck-orm отправляет в ClickHouse. `otelTracing()` из `ck-orm/otel` превращает каждый запрос в спан OpenTelemetry, как только запущен SDK. Настройка описана в разделе [Плагин OpenTelemetry](#opentelemetry-plugin), а о собственных хуках рассказывает раздел [Написание плагинов](#writing-plugins).

```ts
import { clickhouseClient } from "ck-orm";
import { otelTracing } from "ck-orm/otel";

export const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  plugins: [otelTracing()],
});
```

## Примеры
<!-- src:c8f5c20b -->

На [странице примеров](https://ck-orm.lyz.cloud/examples/) показаны запускаемые запросы и точный SQL, который каждый из них отправляет в ClickHouse. Каждый пример — отдельный проект в [`examples/`](../examples) со своим тестом. Запускаемые проверки ClickHouse находятся в [`e2e/`](../e2e), в том числе похожие на продакшен схемы сценариев в [`e2e/scenarios.ts`](../e2e/scenarios.ts).

## Как устроено
<!-- src:a6fa96d2 -->

При работе с `ck-orm` учитывайте следующие границы:

- `ckType.*` задаёт типы столбцов схемы.
- `ckTable(...)` задаёт схемы таблиц.
- `ck.*` — пространство имён вспомогательных функций для запросов.
- `fn.*` — пространство имён вспомогательных функций SQL.
- Схема описывает таблицы и столбцы, но не имя базы данных.
- Подключение к базе данных задаётся в `clickhouseClient(...)`.
- Запросы билдера используйте по умолчанию; для выражений, специфичных для ClickHouse, доступен сырой SQL.
- `runInSession()` — вспомогательная функция для сессий ClickHouse, а не транзакция.
- `plugins` наблюдают за каждым запросом, который отправляет ck-orm; трассировка включается по желанию через плагин `ck-orm/otel`, который не входит в основную точку входа
- `leftJoin()` и `asofLeftJoin()` по умолчанию используют семантику NULL в стиле SQL.
- Крупные или потенциально опасные числовые результаты декодируются осторожно, а не неявно приводятся к другому типу.

`ck-orm` предназначен для типизированной работы с ClickHouse: определения таблиц, составления запросов, вспомогательных функций записи и сессий, а также наблюдаемости. `createTable()`, `createMaterializedView()` и `dropTable()` выполняют по одному оператору DDL; миграции схемы (сравнение схем, `ALTER`, версионирование) остаются в инструментах миграции, которые использует приложение.

## DSL для схем
<!-- src:5149bff0 -->

### `ckTable()`

Используйте `ckTable(name, columns, options?)` для описания таблицы.

Во всех примерах этого раздела используется такой импорт:

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

Передайте в третьем аргументе обычный объект или функцию-фабрику:

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

Публичные опции таблицы:

- `engine`
- `partitionBy`
- `primaryKey`
- `orderBy`
- `sampleBy`
- `ttl`
- `settings`
- `comment`
- `versionColumn`

В определениях столбцов можно также напрямую указать метаданные DDL:

- `.default(expr)`
- `.materialized(expr)`
- `.aliasExpr(expr)`
- `.comment(text)`
- `.codec(expr)`
- `.ttl(expr)`

Метаданные схемы в `ck-orm` нужны для двух задач:

- используются для типизированных запросов, проверки данных при вставке и декодирования результатов;
- формируют оператор `CREATE TABLE` для `createTable()` и `createTemporaryTable()`.

Они не мигрируют и не синхронизируют существующие таблицы ClickHouse: `createTable()` только создаёт таблицу (см. [Таблицы и материализованные представления](#tables-and-materialized-views)). Оставляйте миграции схемы в инструменте миграций и следите, чтобы `ckTable(...)` совпадала со схемой, из которой приложение читает данные и в которую их записывает.

Пример:

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

### Вывод типов

У каждой таблицы есть:

- `table.$inferSelect`
- `table.$inferInsert`

```ts
type TelemetryRow = typeof probeTelemetry.$inferSelect;
type TelemetryInsert = typeof probeTelemetry.$inferInsert;

const sampleId: TelemetryRow["sampleId"] = "900001";
const ingestVersion: TelemetryInsert["ingestVersion"] = "1";
```

Для обобщённых вспомогательных функций используйте:

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

Объекты схемы, которые передают в `InferSelectSchema` и `InferInsertSchema`, — это обычные объекты TypeScript, объединяющие таблицы. Они нужны для общих типов моделей и остаются отдельными от конфигурации клиента.

### Уточнение типов столбцов через `$type<T>()`

У каждого билдера столбца есть метод `$type<T>()`, который можно вызывать в цепочке. Он переопределяет тип столбца в TypeScript и не меняет поведение во время выполнения. Используйте его, если выведенный тип слишком широкий для вашей предметной области:

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

`$type<T>()` можно сочетать с любым другим модификатором столбца в любом порядке: `ckType.string().$type<Role>().default(ckSql`'guest'`).comment("…")`. Декодировщик и кодировщик во время выполнения, SQL-тип и сгенерированный DDL не меняются: `$type` — только утверждение типа на этапе компиляции.

`enum8` и `enum16` уже выводят объединение литералов из объекта значений, поэтому `$type` нужен только, чтобы задать объединению собственный бренд или согласовать его с внешним типом перечисления:

```ts
// Inferred automatically — no `as const`, no explicit type arguments
const status = ckType.enum8({ active: 1, paused: 2, banned: 3 });
//    ↑ Enum8<"active" | "paused" | "banned">

// Re-brand on top of the inferred keys
type UserStatus = "active" | "paused" | "banned";
const userStatus = ckType.enum8({ active: 1, paused: 2, banned: 3 }).$type<UserStatus>();
```

> **Оговорка:** `$type<T>()` не проверяет значения во время выполнения. Если в строке базы данных окажется значение, не соответствующее объявленному типу (например, устаревшие данные, записанные до миграции схемы), TypeScript всё равно сузит тип так, будто значение соответствует объявленному типу. Исчерпывающий `switch` молча пропустит такой случай. Если значение нужно проверять при чтении, используйте `mapWith` в проекции. Для проверки при каждом декодировании и кодировании используйте описанный ниже `$validator()`.

### Разные типы при вставке и выборке с `$type<{ select, insert }>()`

Иногда при вставке столбец ClickHouse принимает значение более широкого типа, чем возвращает при выборке. Например, для столбца `DateTime` при вставке можно передать `string` или `Date`, а после декодирования он всегда возвращает `Date`. Передайте объект в `$type`, чтобы задать разные типы:

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

Если в `$type<{ select: T }>()` нет `insert`, это то же самое, что форма `$type<T>()` с одним обобщённым параметром.

### Правила модели вставки: DEFAULT, MATERIALIZED, ALIAS

`$inferInsert` выводится из формы столбцов в схеме, а не просто копируется из `$inferSelect`:

| Модификатор DDL | Что меняется в модели вставки |
| --- | --- |
| `.default(expr)` | Столбец становится необязательным: если его не передать, значение подставит ClickHouse |
| `.materialized(expr)` | Столбец полностью исключается из модели вставки |
| `.aliasExpr(expr)` | Столбец полностью исключается из модели вставки |

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

### Проверка во время выполнения через `$validator(schema)`

Если `$type` недостаточно, используйте `$validator(schema)`: в базе могут остаться устаревшие данные, вы принимаете JSON-структуры от непроверенного внешнего источника или хотите, чтобы API для вставки преобразовывал строку в `Date`. Подойдёт любой объект, соответствующий [спецификации Standard Schema v1](https://github.com/standard-schema/standard-schema): Zod 3.23+, Valibot 1+, ArkType, Effect Schema, TypeBox. ck-orm не зависит ни от одной из этих библиотек.

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

Преобразования схемы (например, `z.string().transform((v) => new Date(v))`) учитываются: при вставке столбец принимает *входной тип* схемы, а при выборке возвращает *выходной тип*. `$validator` поддерживает только синхронные схемы: при декодировании или кодировании асинхронная схема выбрасывает ошибку.

Сравнение `$type` и `$validator`:

| | `$type<T>()` | `$validator(schema)` |
| --- | --- | --- |
| Нагрузка во время выполнения | Нет | Одна синхронная проверка схемы для каждого столбца в каждой строке |
| Безопасность типов | Утверждение типа во время компиляции | Выводится из схемы |
| Проверка во время выполнения | Нет | Да — при несовпадении выбрасывается ошибка |
| Разные типы на входе и выходе | Через `$type<{ select, insert }>()` | Через преобразования схемы |
| Внешняя зависимость | Нет | Нет — библиотеку схемы выбираете вы |

### Тип столбца JSON

`ck-orm` использует **новый тип данных JSON** в ClickHouse 24.x+
([документация](https://clickhouse.com/docs/sql-reference/data-types/newjson)) —
этот тип физически разбивает JSON-объект на типизированные подстолбцы, а не хранит
значение в виде строки без структуры. Сразу учтите два правила:

- Тип данных `JSON` в ClickHouse принимает **только объекты верхнего уровня**.
  Массивы, строки и числа верхнего уровня отклоняются. `ck-orm` проверяет это на
  этапе компиляции с помощью `T extends Record<string, unknown>`, а при каждой
  вставке и выборке — выполняет быструю проверку за O(1).
- Типизированные пути (`a.b UInt32`) становятся настоящими подстолбцами в
  ClickHouse. `ck-orm` пропускает эти пути через `mapFromDriverValue` /
  `mapToDriverValue` соответствующей фабрики столбца `ck-orm`, поэтому путь,
  объявленный как `ckType.uint64()`, декодируется в `string` без потери
  точности — так же, как столбец `uint64` верхнего уровня.

Самый простой вариант устроен так же, как остальные фабрики столбцов `ck-orm`:

```ts
import { ckTable, ckType } from "ck-orm";

const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json("payload"),
});
// DDL:  `payload` JSON
// $inferSelect.payload: { [key: string]: unknown }
```

Укажите обобщённый тип, чтобы `$inferSelect` / `$inferInsert`
отражали фактическую форму:

```ts
type EventPayload = { user_id: number; event: string; meta?: { ip: string } };

const userEvents = ckTable("user_events", {
  id: ckType.uint64(),
  payload: ckType.json<EventPayload>("payload"),
});
```

Если удобнее держать обобщённый тип рядом с другими вызовами билдера столбца,
используйте вариант с цепочкой вызовов `ckType.json<T>().$type<T2>()`.

#### Параметризованный DDL

Второй аргумент фабрики принимает четыре настройки DDL для NewJSON:

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

Ключи `typeHints` проверяются на соответствие `Paths<T>`: опечатки вроде
`{ "user.idd": ... }` приводят к ошибке во время компиляции. DDL всегда
генерируется в стабильном лексикографическом порядке, поэтому
внешние инструменты сравнения схем получают побайтово одинаковый результат
независимо от порядка ключей в конфигурации.

#### Как typeHints влияют на декодировщик во время выполнения

Из-за политики ClickHouse, сохраняющей точность 64-битных значений, в `ck-orm`
`UInt64` декодируется в `string`. Если `user_id` указан и в типе TypeScript, и в
записи `typeHints`, следите, чтобы типы совпадали. Для `uint64` в обобщённом типе
нужно написать `user_id: string`. Используйте `$type<{ select, insert }>()`, если
тип входных данных при вставке отличается от типа, который декодируется при
выборке:

```ts
ckType.json("payload", { typeHints: { user_id: ckType.uint64() } })
  .$type<{
    select: { user_id: string };
    insert: { user_id: string | number | bigint };
  }>();
```

#### Доступ к путям

У каждого столбца JSON есть пять методов доступа к путям, поддерживающих цепочку
вызовов. Они без изменений формируют синтаксис доступа к путям ClickHouse:

| Метод | SQL | Назначение |
| --- | --- | --- |
| `col.path("a.b")` | `col.a.b` | Читает путь; статический тип — `PathValue<T, "a.b">` (см. примечание о `typeHints` ниже) |
| `col.castPath("a.b", ckType.uint64())` | `col.a.b.:UInt64` | Принудительно декодирует путь через другую фабрику столбца ck-orm; статический тип задаётся `TData` столбца, используемого для приведения |
| `col.subobject("a")` | `col.^a` | Читает вложенный объект как значение `JSON` |
| `col.merged("a")` | `col.@a` | Читает объединённое представление общих данных |
| `col.arrayPath("a")` | `col.a[]` | Разворачивает путь типа массива |

> `col.path(P)` всегда имеет статический тип `PathValue<T, P>`: он выводится из
> обобщённого типа `T`, а не из `typeHints`. `typeHints` меняет только
> декодировщик во время выполнения; статический тип и typeHint нужно согласовать
> вручную (выше описана политика декодирования `uint64 → string` без потери точности). Если они
> расходятся, используйте `castPath(P, ckType.X())` — тип результата определяется
> приводимым столбцом.

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

`Paths<T>` проверяет литерал пути во время компиляции: `payload.path("a.zzz")`
не проходит проверку типов, если `"a.zzz"` не входит в `T`.

Каждый модификатор цепочки (`$type`, `$validator`, `.default()`,
`.materialized()`, `.bind()`, …) снова добавляет методы путей во время
выполнения, поэтому исходный вызов всегда работает. **Статический тип сохраняет
эти методы на каждом звене цепочки, кроме `$validator`**: контракт `$validator`
параметризован типом `StandardSchemaV1<unknown, unknown>`; безопасно сузить его
до `JsonShape` нельзя, поскольку это нарушит правила подтипов в духе LSP, которые
TypeScript требует соблюдать. После этого звена статический тип снова сводится к
`Column<T>`. Сам метод по-прежнему доступен: если нужно продолжить цепочку,
используйте утверждение типа:

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

#### `fn.json*` — вызовы с динамическим путём

Если строка пути формируется во время выполнения или нужно прочитать путь, не
объявленный в `T`, используйте пространство имён `fn.*`:

```ts
import { fn } from "ck-orm";

const userPath = userInput as string;
await db.select({
  raw:    fn.jsonPath<string>(userEvents.payload, userPath),
  casted: fn.jsonCast(userEvents.payload, userPath, ckType.string()),
  shape:  fn.dynamicType(fn.jsonPath(userEvents.payload, userPath)),
});
```

`fn.jsonCast` — динамический эквивалент `castPath`; `fn.jsonSubobject`,
`fn.jsonMerged` и `fn.jsonArray` повторяют соответствующие методы столбца;
`fn.dynamicType` предоставляет функцию ClickHouse `dynamicType()` для определения
фактического типа пути с типом Dynamic во время выполнения.

#### Проверка во время выполнения

Для каждой строки выполняются два слоя проверки **в таком порядке**:

1. **Встроенная JSON-проверка в `ck-orm`** — `null` / `undefined` проходят без
   изменений, поэтому столбцы JSON с `nullable()` работают корректно. Любой
   другой ввод, который не является объектом (массив, строка, число или
   логическое значение), отклоняется с привязанной к столбцу ошибкой
   `client_validation` / `decode`, например `JSON column expects a plain object, got array`.
   Типизированные пути из `typeHints` на входе и выходе проходят через
   кодировщик и декодировщик соответствующей фабрики столбца. Этот слой работает
   всегда.
2. **Необязательный `$validator(schema)`** — если задан, при вставке он
   срабатывает *до* встроенного кодировщика, а при выборке — *после*
   встроенного декодировщика. Используйте его для проверки формы:

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

`ck-orm` намеренно **не** проверяет содержимое JSON рекурсивно, чтобы выяснить,
действительно ли `T["a"]` имеет тип `number`, и так далее. Если такая проверка
нужна, используйте `$validator`: встроенная JSON-проверка ловит только случаи,
когда сообщение об ошибке ClickHouse трудно связать со столбцом. Скалярные
аргументы верхнего уровня вызывают явную ошибку с именем столбца:

```ts
await db.insert(events).values({ payload: ["wrong"] as never });
// throws: JSON column expects a plain object, got array
```

#### Значения по умолчанию при вставке

Столбцы JSON подчиняются тем же правилам `$inferInsert`, что и остальные
столбцы. `.default()` делает поле необязательным; `.materialized()` /
`.aliasExpr()` полностью удаляют его из модели вставки:

```ts
const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json<{ note: string }>("payload").default(ckSql`'{}'`),
});

await db.insert(auditLogs).values({ id: "1" }); // payload filled by CH DEFAULT
```

### `ckAlias()`

Вызовите `ckAlias()`, если одну и ту же таблицу нужно использовать в запросе
несколько раз.

```ts
import { fn, ck, ckAlias } from "ck-orm";

const telemetry = ckAlias(probeTelemetry, "telemetry");
```

Столбцы, которые возвращает `ckAlias()`, автоматически привязываются к этому
псевдониму.

### Имена столбцов

Ключ объекта схемы — это логический ключ, который используется в строках TypeScript,
декодированных результатах запросов и значениях вставки. По умолчанию этот ключ
одновременно служит именем столбца ClickHouse:

```ts
const telemetry = ckTable("telemetry", {
  signalStrength: ckType.decimal({ precision: 20, scale: 5 }),
});
```

Если имя столбца в базе данных отличается, передайте это физическое имя первым
аргументом:

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

Физические имена используются в SQL, DDL, фильтрах, сортировке, группировке и
списках столбцов при записи (`probe_id`, `signal_strength`, `created_at`).
Выведенные модели, результаты выборки по умолчанию, явные ключи проекции и
значения вставки используют ключи объекта схемы (`probeId`, `signalStrength`,
`createdAt`).

У каждого публичного билдера `ckType` можно указать физическое имя столбца.
Билдеры без дополнительной конфигурации принимают `name?`; у билдеров с
конфигурацией типа необязательное физическое имя столбца указывается первым, а
конфигурация типа — объектом:

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

У `aggregateFunction` и `simpleAggregateFunction` особая для ClickHouse форма
вызова: первый строковый аргумент — имя агрегатной функции, а не имя столбца:

```ts
ckType.aggregateFunction("sum", ckType.uint64());
ckType.aggregateFunction("quantile(0.5)", ckType.float64());
ckType.simpleAggregateFunction("sum", ckType.uint64());
```

`AggregateFunction` принимает имена агрегатных функций ClickHouse с параметрами,
заданными литералами, например `quantile(0.5)` или `topK(10)`. `simpleAggregateFunction`
строже и использует обычное имя агрегатной функции.

Если нужно указать и физическое имя столбца, используйте объектную конфигурацию:

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

`nested("items", shape)` задаёт имя внешнего столбца `Nested(...)`. Имена
вложенных полей берутся из ключей `shape`; значения `configuredName` внутренних
столбцов на эти имена не влияют:

```ts
const sensorPackets = ckTable("sensor_packets", {
  items: ckType.nested("items", {
    componentId: ckType.string(),
    quantity: ckType.float64(),
  }),
});
```

### Билдеры столбцов

Для построения столбцов схемы используйте `ckType.*`. DSL схемы поддерживает
основные семейства типов ClickHouse:

- целые числа: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`
- числа с плавающей точкой и Decimal: `float32`, `float64`, `bfloat16`, `decimal`
- скалярные типы: `bool`, `string`, `fixedString`, `uuid`, `ipv4`, `ipv6`
- типы времени: `date`, `date32`, `dateTime`, `dateTime64`, `time`, `time64`
- перечисления и специальные типы: `enum8`, `enum16`, `json`, `dynamic`, `qbit`
- контейнеры: `nullable`, `array`, `tuple`, `map`, `nested`, `variant`, `lowCardinality`
- агрегатные типы: `aggregateFunction`, `simpleAggregateFunction`
- геометрические типы: `point`, `ring`, `lineString`, `multiLineString`, `polygon`, `multiPolygon`

По умолчанию `int64` и `uint64` получают тип `string` в TypeScript при чтении и
записи через схему, а также в моделях, выведенных из схемы. Так 64-битные значения
сохраняют точность и в формате JSON при обмене с ClickHouse, и в средах
выполнения JavaScript. Если нужен именно `bigint`, используйте собственный декодировщик,
например `mapWith((value) => BigInt(String(value)))`.

ClickHouse не поддерживает `Nullable(Array(...))`, `Nullable(Map(...))` или
`Nullable(Tuple(...))`. `ck-orm` отклоняет такие формы при определении схемы.
Вместо этого поместите `nullable(...)` внутрь составного типа, например
`ckType.array(ckType.nullable(ckType.string()))`.

В билдерах с конфигурацией типа параметры передаются объектом; необязательное
физическое имя столбца указывается первым:

```ts
ckType.decimal({ precision: 20, scale: 5 });
ckType.decimal("signal_strength", { precision: 20, scale: 5 });
ckType.fixedString({ length: 8 });
ckType.dateTime64("created_at", { precision: 9, timezone: "UTC" });
ckType.qbit("embedding", ckType.float32(), { dimensions: 8 });
```

При записи в столбцы `Date` / `Date32` и при передаче значений предикатов, сформированных
по схеме, JavaScript `Date` отправляется как календарный день в UTC:
`2026-06-15T23:00:00Z` становится `2026-06-15`. Чтобы сохранить другой день,
передайте строку `YYYY-MM-DD`; строки и числа отправляются без изменений.

При передаче значений предикатов `DateTime` / `DateTime64` JavaScript `Date`
передаётся как момент времени в UTC, поэтому часовой пояс столбца, сессии или
сервера не может его сместить. ClickHouse читает строку в часовом поясе столбца в
том виде, в каком она записана. В позиции 32-битного `DateTime` параметр, в котором все `Date`
содержат только целые секунды, привязывается как `DateTime('UTC')`; если хотя бы
один `Date` в параметре содержит миллисекунды, тот же параметр привязывается как
`DateTime64(3, 'UTC')`. В позициях типа `DateTime64` всегда используется
`DateTime64(3, 'UTC')`. Скалярные предикаты вроде `gte` по-прежнему сравнивают
точный момент времени. В `inArray` / `notInArray` для столбца `DateTime` нужны
`Date` только с целыми секундами; `Date` с миллисекундами ClickHouse отклоняет
(`TYPE_MISMATCH`). Это штатное поведение ClickHouse: такое значение всё равно не
могло бы быть равно значению столбца. `insert().values()` передаёт каждый `Date`
целым числом тиков с точностью столбца. См. раздел «Запись данных».

### Примеры типов столбцов

Типовые варианты схемы и соответствующие им значения TypeScript:

| Форма в ClickHouse | Схема | Форма значения в TypeScript |
| --- | --- | --- |
| перечисление | `ckType.enum8({ idle: 1, active: 2 })` | `"idle" | "active"` (выводится из ключей) |
| строка с низкой кардинальностью | `ckType.lowCardinality(ckType.string())` | `string` |
| Decimal, допускающий NULL | `ckType.nullable(ckType.decimal({ precision: 18, scale: 5 }))` | `string | null` |
| массив | `ckType.array(ckType.string())` | `string[]` |
| элемент массива, допускающий NULL | `ckType.array(ckType.nullable(ckType.string()))` | `(string | null)[]` |
| кортеж | `ckType.tuple(ckType.string(), ckType.int32())` | `[string, number]` |
| тип Map | `ckType.map(ckType.string(), ckType.string())` | `Record<string, string>` |
| массив вложенных объектов | `ckType.nested({ component: ckType.string(), quantity: ckType.float64() })` | `{ component: string; quantity: number }[]` |
| вариант | `ckType.variant(ckType.string(), ckType.int32())` | `string | number` |
| JSON | `ckType.json<{ risk?: { score?: number } }>()` | `{ risk?: { score?: number } }` |

`ckType.map(...)` сейчас поддерживает только ключи `String` и представляет такой
тип в виде объекта JavaScript, поэтому не моделирует граничный случай ClickHouse
с повторяющимися ключами `Map(K, V)`.

Строки вставки имеют ту же выведенную форму, что и
`typeof table.$inferInsert`. Исключение составляют столбцы со значениями по
умолчанию ClickHouse и столбцы со сгенерированными выражениями: при вызове
`insert(table).values(...)` такие столбцы можно не передавать. Столбцы
`MATERIALIZED` и `ALIAS` никогда не попадают в сгенерированный список столбцов
`INSERT`; явная передача таких столбцов отклоняется.

## Конфигурация клиента
<!-- src:26d96c74 -->

Создайте клиент с помощью `clickhouseClient()`:

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:<password>@127.0.0.1:8123/telemetry_lab",
});
```

### Режимы подключения

`clickhouseClient()` поддерживает два взаимоисключающих способа задать подключение.

#### `databaseUrl`

Используйте `databaseUrl`, если удобно передать одну строку подключения:

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:secret@127.0.0.1:8123/telemetry_lab",
});
```

Когда указан `databaseUrl`, не передавайте одновременно:

- `host`
- `database`
- `username`
- `password`
- `pathname`

#### Структурированные поля подключения

Используйте отдельные поля, если каждую часть подключения нужно настроить отдельно:

```ts
const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  username: "default",
  password: "<password>",
});
```

Значения по умолчанию для структурированного режима:

- `host`: `http://localhost:8123`
- `database`: `default`
- `username`: `default`
- `password`: `""`

### Основные поля

Большинству проектов нужен лишь небольшой набор полей клиента:

| Поле | Назначение |
| --- | --- |
| `request_timeout` | Необязательный таймаут для каждого запроса в миллисекундах, отсчитываемый по реальному времени. Если поле не указано, ck-orm не задаёт таймаут на стороне клиента: время жизни запроса контролируют fetch / платформа / сервер / `abort_signal`. Для долгих потоковых запросов не задавайте это поле и задайте лимит реального времени через `abort_signal` (например, `AbortSignal.timeout(30_000)`). |
| `clickhouse_settings` | Настройки сессий и запросов ClickHouse по умолчанию |
| `application` | Имя приложения в ClickHouse |

Определения таблиц не зависят от конфигурации подключения. Задайте таблицы через `ckTable(...)`, импортируйте их объекты туда, где составляете запросы, и передавайте напрямую в `.from(...)`, `.insert(...)` и вспомогательные функции для временных таблиц.

### Дополнительные поля

Используйте эти поля, чтобы настроить поведение во время выполнения:

| Поле | Назначение |
| --- | --- |
| `fetch` | `(request: Request) => Response \| Promise<Response>` вызывается для каждого HTTP-запроса: подойдёт любая реализация fetch, серверный обработчик или тестовая заглушка, например из [`examples/decimal-sum/main.test.ts`](../examples/decimal-sum/main.test.ts). По умолчанию используется платформенный `fetch` |
| `http_headers` | Дополнительные заголовки по умолчанию |
| `role` | Роль или роли ClickHouse по умолчанию |
| `session_id` | Идентификатор сессии по умолчанию |
| `session_max_concurrent_requests` | Максимум одновременно выполняющихся запросов для одного `session_id` в этом клиенте (по умолчанию `1`) |
| `compression.response` | Со значением `false` по умолчанию потоковые запросы отправляют `enable_http_compression = 0`, поэтому строки приходят по мере появления в ClickHouse. Непотоковые запросы сохраняют настройку сервера по умолчанию (начиная с ClickHouse 26.3 сервер сжимает ответ, если клиент это поддерживает). Значение `true` сжимает каждый ответ, из-за чего поток с редкими данными может задержаться до заполнения блока сжатия |
| `logger` / `logLevel` | Интеграция с логгером |
| `plugins` | Плагины жизненного цикла запроса, например `otelTracing()` из `ck-orm/otel` |

Параметры времени жизни сессии намеренно задаются для каждого запроса. Передавайте `session_timeout` отдельному запросу или `runInSession(...)`. Используйте `session_check`, когда продолжаете существующую `session_id`, а не когда создаёте новую сессию с нуля.
`session_max_concurrent_requests` работает иначе: это ограничение на уровне клиента, которое ограничивает число одновременно выполняющихся запросов к одной `session_id`.
Настоящие сессии ClickHouse по-прежнему блокируются на сервере, поэтому увеличение `session_max_concurrent_requests` выше `1` может привести к `SESSION_IS_LOCKED`, а не к настоящему параллелизму в одной сессии.
Меняйте значение по умолчанию `1` только если сознательно хотите снять локальную сериализацию и готовы обрабатывать ошибки блокировки сессии на сервере.

### Настройки ClickHouse

`clickhouse_settings` предназначен только для настроек сессий и запросов ClickHouse — тех же ключей, которые описаны в [настройках сессии](https://clickhouse.com/docs/operations/settings/settings) ClickHouse и передаются в HTTP API в виде параметров запроса. Он отделён от конфигурации клиента ck-orm, например `host`, `database`, `request_timeout`, `http_headers` и `session_max_concurrent_requests`.

Поля HTTP-транспорта, например `query`, `database`, `session_id`, `role` и `param_*`, держите вне `clickhouse_settings`: ck-orm проверяет эти ключи отдельно, потому что они относятся к обёртке запроса и каналу именованных параметров.

Официальные ключи настроек поддерживают автодополнение TypeScript, а произвольные ключи можно использовать в новых версиях ClickHouse и в настройках конкретного развёртывания:

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

Поведение разбора и сериализации JSON задаёт транспорт fetch. Публичная конфигурация клиента обеспечивает одинаковое поведение в разных средах выполнения и не предоставляет возможности переопределить `json`.

ck-orm принудительно задаёт на каждом запросе небольшой набор настроек контракта передачи данных: `http_write_exception_in_output_format = 0`, `output_format_json_quote_64bit_integers = 1`, `output_format_json_quote_decimals = 1` и `date_time_output_format = "iso"`. Если переданные настройки конфликтуют с этими значениями, ck-orm один раз записывает предупреждение в лог и отправляет принудительное значение; настройки нагрузки и производительности, такие как `max_threads`, остаются на усмотрение вызывающего кода.

### Аутентификация

`ck-orm` использует Basic-аутентификацию для подключений к базе данных.

- в режиме `databaseUrl` учётные данные можно встроить в URL
- в структурированном режиме используйте `username` и `password`
- если учётные данные не переданы, по умолчанию используется `default` с пустым паролем
## Построитель запросов
<!-- src:fc28245d -->

Ниже `db`, `probeTelemetry` и упомянутые вспомогательные функции уже импортированы из `ck-orm`.

### `select()`

Явно выбранные поля задают форму результата:

```ts
const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
    signalStrength: probeTelemetry.signalStrength,
  })
  .from(probeTelemetry)
  .limit(10);
```

Объекты проекции можно собирать из публичных значений `Selection` или столбцов. На практике столбцы таблиц, результаты `fn.*(...)` и `ck.expr(...)` одинаково комбинируются внутри `select({ ... })`.

Если соединений нет, неявный выбор возвращает полную модель таблицы:

```ts
const rows = await db.select().from(probeTelemetry).limit(10);
```

При соединениях неявный выбор группирует поля по источнику и возвращает вложенные объекты. Если для левого соединения соответствующей строки нет, вложенный объект этой группы становится `null`. ClickHouse возвращает `[]` / `{}` вместо NULL для столбцов `Array` / `Map`, поэтому в этой проверке пустые контейнеры считаются отсутствующими. Совпавшую строку, у которой все скалярные поля равны NULL, а массивы и Map пусты, невозможно отличить от отсутствующей группы, поэтому соответствующий объект тоже становится `null`.

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

`asofJoin()` и `asofLeftJoin()` формируют ClickHouse `ASOF JOIN` и `LEFT ASOF JOIN`. Для каждой левой строки они присоединяют одну правую строку с ближайшим значением, а не все подходящие строки. Условие через `ck.and(...)` сочетает одно или несколько условий равенства и ровно одно сравнение `ck.gt`, `ck.gte`, `ck.lt` или `ck.lte` между левым и правым столбцом:

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

- `ck.gte(left, right)` выбирает последнюю правую строку не позже левого значения, а `ck.gt` — последнюю строго раньше него. `ck.lte` и `ck.lt` выбирают самую раннюю правую строку не раньше левого значения или строго позже него.
- `asofLeftJoin()` следует [семантике NULL у `leftJoin()`](#join-null-semantics): отправляет `join_use_nulls = 1` и помечает правую сторону как допускающую NULL, поэтому левая строка без совпадения получает `null`. `asofJoin()` отбрасывает такие строки, как `innerJoin()`.
- Столбец ближайшего совпадения должен иметь целый, вещественный тип, `Date`, `DateTime` или `Decimal`, и обе стороны сравнения должны ссылаться на столбцы; сравнение со значением не считается. Дополнительные фильтры равенства, например `ck.eq(quotes.symbol, "BTCUSD")`, допустимы.
- ck-orm не проверяет условие. ClickHouse отклоняет условие без неравенства, со вторым неравенством или с `ck.or(...)` до чтения данных; ошибка — `ClickHouseORMError` с `kind: "request_failed"` и `clickhouseName: "INVALID_JOIN_ON_EXPRESSION"`. Условие без равенства и столбец ближайшего совпадения другого типа тоже отклоняются, а имя ошибки при отсутствии равенства различается между версиями ClickHouse.
- ASOF-соединения выполняются с `join_algorithm` по умолчанию, а также с `hash`, `parallel_hash` или `full_sorting_merge`. `partial_merge`, `grace_hash` и `direct` завершаются ошибкой `NOT_IMPLEMENTED`.

### `where()` и вспомогательные функции условий

Публичные вспомогательные функции для условий:

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

`.where(...predicates)` принимает любое число предикатов и объединяет их через `AND`. Предикаты `undefined` пропускаются, поэтому можно передавать условия по одному или собирать группы предикатов с помощью `ck.and(...)` и `ck.or(...)`.

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

Список значений отправляется одним параметром запроса типа `Array(T)` (`col in {p:Array(T)}`). Если длина текста списка превышает примерно 40 000 символов, его разбивают на несколько параметров, соединённых через `arrayConcat(...)`. Фильтрация по первичному ключу и индексам пропуска при этом работает как раньше. Начиная с версии 26.3 ClickHouse принимает не более 1 000 параметров запроса в одном HTTP-запросе, а анализатор в версии 25.8 отклоняет объединённый список, если его размер превышает примерно 1 МБ. Для больших наборов используйте временную таблицу или внешние данные. Если список содержит SQL-выражения или нетипизированные значения разных типов, для каждого элемента по-прежнему используется отдельный параметр.

`ck.and(...)` пропускает `undefined`, поэтому динамические фильтры удобно собирать на месте:

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

`undefined` пропускается только на уровне предиката. Значения внутри предикатов проверяются строго: `ck.eq(column, undefined)` выбрасывает ошибку, а не молча расширяет запрос. Значения `null`, `false`, `true`, `0` и `""`, переданные сами по себе, не являются предикатами: вместо них используйте SQL-выражение. `null` остаётся допустимым значением данных для столбцов, допускающих NULL, и при записи в них, но фильтр по NULL нужно задавать явно:

```ts
db.select()
  .from(probeTelemetry)
  .where(ck.isNull(probeTelemetry.deletedAt));
```

Логические столбцы можно использовать и как обычные значения, и как SQL-выражения:

```ts
const healthChecks = ckTable("health_checks", {
  id: ckType.int32(),
  isPassing: ckType.bool("is_passing"),
});

db.select().from(healthChecks).where(ck.eq(healthChecks.isPassing, false));
db.select().from(healthChecks).where(healthChecks.isPassing);
```

Не подставляйте отдельный `null` в обычные сравнения и списки `IN`. Условия с NULL составляйте явно:

```ts
ck.or(
  ck.inArray(probeTelemetry.deletedAt, [new Date("2026-04-21T00:00:00.000Z")]),
  ck.isNull(probeTelemetry.deletedAt),
);
```

Если во время выполнения нужно собрать большой фильтр, используйте `Predicate[]` и `.where(...predicates)` с переменным числом аргументов:

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

`Predicate` — публичный тип для переиспользуемых логических условий SQL. Одни и те же предикаты можно использовать в `where`, `having`, условиях `on` соединений и вспомогательных функциях, которые работают с логическими значениями, например `ck.exists(...)`.

`Selection` — публичный тип для переиспользуемых вычисляемых значений билдера: `fn.sum(...)`, `fn.toString(...)` и `ck.expr(ckSql...)`. Задайте им псевдоним через `.as(...)`, а декодирование переопределите через `.mapWith(...)`. `Order` — объект сортировки, который возвращают `ck.asc(...)` и `ck.desc(...)`.

`ck.has(...)`, `ck.hasAll(...)` и `ck.hasAny(...)` напрямую соответствуют нативным функциям ClickHouse и сохраняют семантику массивов, Map и JSON в ClickHouse. Если слева стоит столбец с массивом, обычные значения JavaScript сначала проходят через кодировщик этого столбца. Поэтому в фильтрах `Array(Date)` до создания параметров запроса используется кодировщик этого столбца для типа Date.

`where(...)` принимает переменное число аргументов, а `having(...)` — один предикат. Если условий в `having` несколько, сначала объедините их с помощью `ck.and(...)` или `ck.or(...)`.

`ck.contains(...)`, `ck.startsWith(...)`, `ck.endsWith(...)` и их варианты `*IgnoreCase` воспринимают входной текст буквально. Значение передаётся как параметр, а символы подстановки LIKE (`%`, `_`, `\`) внутри него экранируются.

Используйте `ck.like(...)` / `ck.ilike(...)`, только если вам нужна именно работа с шаблонами. Эти API всё так же передают значения через параметры для безопасности SQL, но `%` и `_` остаются символами подстановки, поскольку LIKE работает с шаблонами.

Пример поиска по буквальному тексту:

```ts
import { ck } from "ck-orm";

const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
  })
  .from(probeTelemetry)
  .where(ck.contains(probeTelemetry.probeId, "probe_alpha%"));
```

Пример поиска по шаблону:

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

`groupBy()` и `limitBy([...])` принимают столбцы и вычисляемые значения `Selection`, возвращаемые вспомогательными функциями вроде `fn.*(...)` и `ck.expr(...)`.

Примитивные значения, переданные в `limit()`, `offset()` и `limitBy(..., limit)`, должны быть неотрицательными безопасными целыми числами. Используйте `ckSql` только если вам нужно явно задать константное выражение ClickHouse.

`orderBy()` принимает:

- `ck.desc(selection)`
- `ck.asc(selection)`
- сам столбец

### `final()`

Добавьте к запросу `FINAL` на уровне таблицы:

```ts
const query = db.select().from(probeTelemetry).final();
```

Для простого чтения таблицы без псевдонима `ck-orm` генерирует `FROM table FINAL`. Если корневой таблице задан псевдоним или запрос соединяет дополнительные источники, `ck-orm` оборачивает таблицу, к которой применён FINAL, в подзапрос, а псевдоним оставляет у внешнего источника. Так удаётся обойти особенности анализатора ClickHouse, связанные с `FINAL`, псевдонимами таблиц, соединениями и лямбда-выражениями, сохраняя тот же API билдера.

`final()` работает только с табличным источником в корне запроса. Если `FINAL` нужен внутри CTE, подзапроса или цепочки с табличной функцией, вызовите `.final()` у запроса к таблице до `.as(...)`.

### `limitBy()`

Используйте конструкцию ClickHouse `LIMIT ... BY ...`:

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

### Режимы выполнения

Запросы билдера можно выполнить тремя способами:

```ts
const query = db.select().from(probeTelemetry).limit(10);

const rows = await query;
const sameRows = await query.execute();

for await (const row of query.iterator()) {
  console.log(row);
}
```

В примерах приложения используйте `.execute()`, чтобы место выполнения было сразу видно. Прямой `await query` тоже поддерживается — это удобный вариант в стиле Drizzle, когда команда уже привыкла к тому, что запросы билдера — thenable.

`query.iterator()` использует те же правила параллелизма для сессий, что и `db.stream()`: если запрос привязан к `session_id`, слот остаётся занятым до завершения итерации или досрочного закрытия итератора.

### Просмотр SQL через `toSQL()`

`toSQL()` компилирует билдер и ничего не отправляет. Вызывайте его у самого билдера: если применить к билдеру await, запрос выполнится. Метод работает с билдерами `select()`, а также с `insert().values()` и `insert().fromSelect()`.

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

Результат содержит такие поля:

- `sql` — SQL-запрос в одну строку; на месте каждого значения стоит плейсхолдер вида `{orm_param1:String}`. Секции `FORMAT` в нём нет: её добавляют при отправке запроса.
- `params` — исходные JavaScript-значения для плейсхолдеров. Когда запрос выполняется, они кодируются и передаются отдельно от SQL, поэтому в `sql` значения никогда не подставляются.
- `forcedSettings` — настройки, которые билдер принудительно задаёт для запроса, например `join_use_nulls: 1` для `leftJoin()`. Если таких настроек нет, поля в результате тоже нет.
- `body` бывает только у `insert().values()`: это строки в текстовом формате Values, а в `sql` лежит запрос `INSERT INTO t (cols) FORMAT Values`. У остальных запросов этого поля нет.

Результат — копия, поэтому его изменение не влияет на билдер.

Передайте `{ pretty: true }`, чтобы `sql` и `body` раскладывались на несколько строк — это удобно для логов и отладки:

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

Многострочный вывод меняет только пробельные символы. Каждая секция запроса начинается с новой строки, элементы `select` идут по одному на строку, подзапросы и CTE выделены отступами, а условия группы `and` или `or` стоят по одному на строку. Аргументы функций остаются на своей строке, текст в кавычках не затрагивается, а запрос с SQL-комментариями возвращается как есть. В теле `insert().values()` каждая вставляемая строка таблицы занимает отдельную строку текста. На отправляемый запрос многострочный вывод никак не влияет.

### Подзапросы и CTE

Вызовите `.as("alias")`, чтобы превратить билдер в подзапрос с явным именем:

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

Для подзапросов `.as("…")` **необязателен**: без этого вызова фреймворк при компиляции назначит автоматический псевдоним (`__sub_1`, `__sub_2`, …). Билдер без явного имени можно напрямую использовать в `from()`, `innerJoin()`, `leftJoin()`, `inArray()`, `notInArray()`, `exists()` и `notExists()`.

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

#### Когда использовать `.as(name)`, а когда — билдер без имени

| Ситуация | Рекомендуемая форма |
|---|---|
| Одноразовый встроенный подзапрос, который не нужно переиспользовать | билдер без имени + `innerJoin/leftJoin` с функцией обратного вызова или билдер без имени в `inArray/exists` |
| Ссылки на столбцы подзапроса используются в нескольких местах (SELECT / WHERE / один JOIN) | сохраните подзапрос в `const` и передавайте без имени — ссылки на столбцы будут доступны в методах цепочки |
| Самосоединение (один логический подзапрос, два псевдонима в SQL) | задайте `.as("a")` и `.as("b")` явно — использование одного экземпляра в качестве источника дважды приводит к ошибке при компиляции запроса в SQL |
| Нужен стабильный, читаемый или типизированный псевдоним (поиск `query_log` в продакшене, `EXPLAIN`, логи медленных запросов, вспомогательные функции между модулями, литеральный ключ источника `TAlias`) | вызовите `.as("descriptive_name")` |

Для структурно разных запросов автоматический псевдоним **не** стабилен, но внутри одной компиляции и при повторных компиляциях того же дерева билдера остаётся стабильным. Поэтому для тестов в стиле `snapshot` нормализация не нужна.

#### Конфликты ключей проекции

Ключи проекции, совпадающие с именами методов SelectBuilder (`from`, `where`, `select`, `as`, `innerJoin`, `leftJoin`, `asofJoin`, `asofLeftJoin`, `groupBy`, `having`, `orderBy`, `limit`, `offset`, `final`, `limitBy`, `execute`, `iterator`, `then`, `catch`, `finally`, `buildSelectionItems`), у билдера без имени по-прежнему имеют тип метода. `sub.from` всегда возвращает метод билдера, а не ссылку на столбец, даже если `from` есть в проекции. Переименуйте такие ключи (например, `from` → `fromAddress`) или оберните подзапрос вызовом `.as("name")`, чтобы обращаться к столбцам через отдельный объект `Subquery`.

Для CTE используйте `$with()` и `with()`:

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

Вызов `$with()` без имени создаёт безымянный CTE. Безымянный CTE можно использовать как источник один раз в запросе; чтобы присоединить один и тот же результат дважды, определите два CTE — по одному на соединение (два вызова `db.$with().as(...)` или два именованных CTE с разными именами).

### `db.count()`

Используйте `db.count(source, ...predicates)` как вспомогательную функцию подсчёта в стиле Drizzle. Она использует ту же семантику предикатов, что и `.where(...predicates)`: несколько предикатов объединяются через `AND`, а предикаты, переданные как `undefined`, пропускаются. При этом `undefined` на уровне значения внутри таких вспомогательных функций, как `ck.eq(...)`, по-прежнему выбрасывает ошибку.

```ts
import { ck } from "ck-orm";

const total = await db.count(
  probeTelemetry,
  ck.eq(probeTelemetry.status, 1),
  ck.gt(probeTelemetry.id, 1000),
);
```

Для более сложных наборов результатов используйте подзапрос или CTE как источник для подсчёта:

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

По умолчанию `db.count(...)` использует числовой режим: генерирует `toFloat64(count())` и декодирует результат в `number`. Очень большие значения счётчика могут выйти за пределы точности целых чисел в JavaScript, поэтому, если важны точность или сохранение формы данных при передаче, используйте режимы, доступные в цепочке вызовов:

```ts
const approximateTotal = await db.count(activeProbes); // number
const exactTotal = await db.count(activeProbes).toSafe(); // string
const wireTotal = await db.count(activeProbes).toMixed(); // number | string
```

`.toSafe()` генерирует `toString(count())` и предназначен для чтения без потери точности. Если использовать безопасный режим подсчёта как SQL-выражение, он имеет семантику `String`; для числовых SQL-сравнений используйте режим по умолчанию, `.toUnsafe()` или `.toMixed()`. `.toMixed()` генерирует `toUInt64(count())` и сохраняет форму результата на уровне драйвера и транспорта. При стандартных для ck-orm настройках JSON, сохраняющих точность 64-битных значений, реальные ответы ClickHouse обычно приходят в виде `string`.

## Семантика NULL при соединениях
<!-- src:59e7a4e2 -->

По умолчанию `leftJoin()` и `asofLeftJoin()` используют семантику NULL в стиле SQL: они автоматически задают `join_use_nulls = 1`.

Это значит:

- правая сторона левого соединения по умолчанию получает тип, допускающий NULL
- если явно отключить `join_use_nulls`, изменятся и выведенные типы

Чтобы использовать поведение соединений ClickHouse по умолчанию, передайте литерал `0`. Тип литерала выводится, поэтому `as const` не нужен. Если значение имеет обычный тип `number`, например пришло из переменной, всё равно получится `| null`.

```ts
const rawDefaultDb = db.withSettings({
  join_use_nulls: 0,
});
```

При левом соединении с безымянным билдером (подзапросом без `.as()`) или безымянным CTE ссылки на каждый столбец из этих источников получают тип с `| null` — в том числе ссылки со стороны `FROM` и из безымянного билдера во внутреннем соединении. У таких ссылок один общий ключ источника типа `string`, поэтому типы не различают, какой именно источник оказался в левом соединении. Во время выполнения NULL по-прежнему возвращается только для присоединённой стороны.

```ts
const rows = await db
  .select({ uid: allUsers.uid, spent: totals.spent })
  .from(allUsers)
  .leftJoin(totals, ck.eq(allUsers.uid, totals.uid));
// uid and spent are both number | null
```

Назовите подзапрос со стороны `FROM` или во внутреннем соединении с помощью `.as("name")`. Тогда его ссылки сохранят собственный ключ источника и не будут допускать NULL, а сторона левого соединения сохранит `| null`.

Если повторно использовать соединённый запрос как подзапрос, CTE, источник для `ck.exists(...)` или `ck.inArray(...)`, принудительная настройка `join_use_nulls = 1` сохранится. Поэтому типы билдера по-прежнему соответствуют поведению во время выполнения.

При выполнении вычисляемые выражения подчиняются правилам распространения NULL в ClickHouse: если ClickHouse возвращает NULL (во входе был NULL или для строки, присоединённой через левое соединение, не нашлось пары), значение будет `null`. В TypeScript типом выражения остаётся объявленный вами `TData` (или фиксированный тип вспомогательной функции) и автоматически не расширяется. Поэтому укажите `| null` — например, `fn.plus<number | null>(...)`, — или оберните вход в `fn.coalesce(...)`, если он может быть NULL.

## Запись данных
<!-- src:5d78750d -->

### `insert(table).values(...)`

Используйте билдер, если нужны типизированные вставки по схеме таблицы:

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

Строки отправляются одним запросом `INSERT INTO t (cols) FORMAT Values`. В операторе нет параметров запроса: строки передаются в теле запроса как текстовый литерал Values, поэтому HTTP-лимиты на 1 000 параметров и 128 КиБ на значение не действуют. Для пропущенного столбца в этой строке используется его `DEFAULT`. Вычисление выражений для этой вставки отключено (`input_format_values_interpret_expressions = 0` и `input_format_values_deduce_templates_of_expressions = 0`), поэтому каждая ячейка передаётся как данные. Для вычисляемых значений используйте `insert().fromSelect()`. JavaScript `Date` передаётся как точный момент времени, обрезанный до точности столбца; в столбцах `DateTime` сохраняются целые секунды. Число в столбце `DateTime` или `DateTime64` трактуется как Unix-секунды. Строка читается в часовом поясе столбца.

Во вставляемых строках используйте ключи из схемы таблицы. Неизвестные ключи отклоняются ещё до отправки.

### `insert(table).fromSelect(select)`

Используйте `.fromSelect(...)`, чтобы скомпилировать один оператор `INSERT INTO target (cols) SELECT ...` и поручить ClickHouse материализовать SELECT на сервере. Строки не проходят через процесс Node — это удобно для однократного отбора нужного фрагмента большой исходной таблицы и повторного использования материализованного среза в нескольких последующих запросах внутри `runInSession`.

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

Правила:

- SELECT должен выбирать каждый обязательный столбец целевой таблицы; столбцы, объявленные через `.default(...)`, можно не указывать. Запрос с отсутствующими обязательными столбцами отклоняется во время компиляции (TypeScript) и ещё раз во время выполнения.
- Список столбцов INSERT создаётся в порядке ключей проекции SELECT, поэтому вызывающий код видит семантику `name-aligned`, хотя ClickHouse выравнивает столбцы по позициям. Порядок ключей проекции не влияет на то, в какой целевой столбец попадёт значение: сопоставляются сами ключи.
- Неизвестные ключи проекции (которых нет в целевой таблице) и несовпадения типов по столбцам всплывают как ошибки TypeScript до отправки запроса.
- `.values()` и `.fromSelect()` нельзя использовать в одной цепочке вызовов вставки: тип билдера сужается, поэтому неиспользуемый метод пропадает из автодополнения, а во время выполнения вызов тоже отклоняется.
- Скомпилированный сетевой запрос — обычная команда ClickHouse: в событии запроса указывается `mode: "insert"`, а `tableName` — целевая таблица.
- `.fromSelect()` принимает `SelectBuilder` (`session.select({...}).from(...)`), а не обёртку подзапроса. Используйте `.as("name")` только если результат нужен как CTE или подзапрос внутри другого SELECT, но не когда передаёте его в `.fromSelect()`.

#### Вложенные столбцы

Столбцы `ckType.nested(...)` работают с `.fromSelect()` в трёх режимах:

1. **Не включайте вложенный ключ в проекцию.** Это режим по умолчанию и отражает контракт времени выполнения ck-orm: отсутствующее вложенное значение кодируется как SQL `DEFAULT`, а ClickHouse заполняет каждое поле пустым параллельным массивом. Слой TS считает вложенные столбцы `optional` в `$inferInsert`, чтобы соответствовать этому поведению.
2. **Спроецируйте прямую ссылку на вложенный столбец**, например `select({ events: src.events })`. ck-orm оборачивает внутренний SELECT в подзапрос и переписывает внешнюю проекцию в доступ к каждому полю через точечный путь (`__ck_inner.events.name`, `__ck_inner.events.score`, …). Форма вложенного источника должна содержать все поля, предусмотренные во вложенной форме целевой таблицы.
3. **Попытка спроецировать вычисляемое выражение во вложенный столбец** отклоняется во время компиляции: одно SQL-выражение нельзя развернуть в поля параллельных массивов. Для значений строки используйте `.values(...)` или `.insertJsonEachRow(...)`.

Если вы хотите, чтобы слой TS требовал всегда передавать вложенные данные (`Pattern D` — бизнес-контракт, запрещающий пустые вложенные массивы), включите `ckType.nested({...}).requiredOnInsert()`. В форме `$inferInsert` таблицы с таким столбцом поле `events` становится обязательным, а `.fromSelect()` требует либо режим проекции прямой ссылки на вложенный столбец, либо построчную альтернативу.

> **Примечание о формате JSON при передаче**: `.values()` передаёт в теле Values JSON-столбец как текст `JSON.stringify(...)`, заключённый в кавычки. ClickHouse разбирает этот литерал в JSON-столбец. `.fromSelect()` целиком выполняется на сервере, поэтому значения JSON не проходят через процесс JS и обратно. Оба варианта сохраняют одинаковые данные.

### `insertJsonEachRow()`

Используйте `insertJsonEachRow()`, если у вас уже есть строки в виде объектов или асинхронный поток строк:

```ts
await db.insertJsonEachRow("tmp_scope", [
  { probe_id: "probe_alpha" },
  { probe_id: "probe_beta" },
]);
```

Метод принимает:

- строку с именем таблицы
- объект таблицы, созданный `ckTable()`
- обычный массив
- `AsyncIterable`

Пустой обычный массив ничего не делает на стороне клиента, но хуки плагинов всё равно получают событие успешного завершения вставки с `rowCount: 0`. Поведение при неизвестных полях контролирует ClickHouse; если сервер должен игнорировать лишние поля JSON, передайте настройки вроде `input_format_skip_unknown_fields: 1`. Вложенные столбцы отправляются как плоские массивы `column.field` (в ClickHouse по умолчанию `flatten_nested = 1`), поэтому `{ lines: [{ sku: "a" }] }` работает и с `insert().values()`, и с `insertJsonEachRow()`.

При наличии схемы таблицы `insertJsonEachRow()` передаёт JavaScript `Date` в столбец `DateTime`/`DateTime64` как то же целое значение в тиках, что и `insert().values()`, и поручает разбор строк стандартному парсеру ClickHouse `date_time_input_format = basic`. Поэтому оба способа одинаково разбирают строки даты и времени. JSON-число, которое вы сами передаёте в столбец `DateTime64`, по-прежнему читается как тики в `JSONEachRow` и как секунды Unix в `.values()`.

У `insertJsonEachRow("table_name", rows)` с обычным именем таблицы нет типов столбцов, поэтому `Date` отправляется в ISO 8601 с `Z`, а ck-orm задаёт `date_time_input_format = best_effort` для этого запроса. То же относится к ключам строки таблицы со схемой, которых нет в этой схеме: ck-orm передаёт значения по этим ключам без изменений, поэтому для `Date` в таком поле нужно передать `clickhouse_settings: { date_time_input_format: "best_effort" }` в этом вызове. В ClickHouse 25.8 парсер `best_effort` неправильно разбирает моменты времени до 1970 года с долями секунды и отклоняет строки `Time` вроде `12:34:56`; столбцы с типами из схемы позволяют избежать обеих проблем.

## Сырой SQL
<!-- src:f47b381e -->

У `ck-orm` есть собственный API для SQL-шаблонов. Используйте его, если так проще написать нужный SQL, чем собирать его билдером.

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

### `ckSql.join()` и `ckSql.identifier()`

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

### Сырой SQL с `query_params`

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

Транспорт параметров выбирается автоматически. Настраивать обработку multipart для `query_params` не нужно.

Ключи `query_params`, начинающиеся с `orm_param`, отклоняются. Этот префикс зарезервирован для параметров, которые `` ckSql`...` `` генерирует сам.

Форматтер значений поддерживает примитивные значения, `Date`, `NaN`, `Infinity`, массивы, объекты и значения типа `Map` для типизированных плейсхолдеров ClickHouse вроде `{ids:Array(UInt64)}` или `{attrs:Map(String, String)}`. Значения из `query_params` не имеют собственного типа столбца, поэтому массив всегда отправляется как текст в формате Array. Плейсхолдеры `Tuple(...)` здесь не поддерживаются. 64-битное целое число внутри массива должно иметь тип `bigint`; строка заключается в кавычки и отклоняется, если тип элемента числовой. `Date` передаётся как текст со временем UTC, который ClickHouse читает в часовом поясе типа плейсхолдера. Поэтому задайте для него тип `{at:DateTime64(3, 'UTC')}`, чтобы часовой пояс сессии или сервера не сдвинул значение; интерполяция `` ckSql`...${date}` `` сделает это за вас. Если параметр — имя таблицы или столбца, используйте тип плейсхолдера ClickHouse `Identifier`:

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

Используйте `ck.expr()`, чтобы превратить фрагмент SQL в повторно используемый `Selection`:

```ts
import { ck, ckSql } from "ck-orm";

const query = db.select({
  constantOne: ck.expr(ckSql`1`).as("constant_one"),
});
```

Если фрагмент типизирован, `ck.expr()` сохраняет этот тип. Нетипизированный фрагмент намеренно остаётся `unknown`; передайте обобщённый тип или декодировщик, если тип результата известен вне схемы:

```ts
const status = ck.expr(ckSql<string>`'closed'`); // Selection<string>
const enabled = ck.expr<boolean>(ckSql`1`, {
  decoder: (value) => Number(value) === 1,
});
```

### Форматы сырых запросов

Сырые запросы без потоковой выдачи поддерживают только вывод `JSON`:

```ts
const rows = await db.execute(ckSql`select 1`, {
  format: "JSON",
});
```

Сырые потоковые запросы поддерживают только вывод `JSONEachRow`. При значении по умолчанию `compression.response: false` поток передаётся без сжатия, поэтому каждая строка может приходить по мере того, как её создаёт ClickHouse:

```ts
for await (const row of db.stream(ckSql`select 1`, {
  format: "JSONEachRow",
})) {
  console.log(row);
}
```

### Точность Decimal в выражениях

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

- `sum` / `sumIf` расширяют P до ≥ 38; `min` / `max` сохраняют P столбца. Для Decimal-столбцов схемы `fn.sum()` и `fn.sumIf()` имеют тип `Selection<string>`, а столбцы Float — `Selection<number>`. Автоматическое приведение также срабатывает через `nullable(decimal(...))` и `lowCardinality(decimal(...))`.
- `maxIf(value, condition)` следует тому же правилу Decimal, что и `max`: результат Decimal имеет тип `Selection<string>` с объявленной формой `Decimal(P, S)`.
- `avg` **не приводится автоматически** — ClickHouse вычисляет `avg(Decimal)` через Float64, поэтому `fn.avg` возвращает `Selection<number>`. Для точных средних по Decimal используйте `ckSql.decimal(ckSql\`sum(x) / count(x)\`, P, S)`.
- `fn.divideDecimal(left, right, resultScale?)` напрямую соответствует `divideDecimal` ClickHouse. Он возвращает `Selection<string>`, поэтому точность не теряется. Если передан `resultScale`, это должен быть литерал целого числа от `0` до `76`; он подставляется прямо в SQL. Билдер не проверяет версию и не эмулирует функцию ClickHouse.
- `column.cast(P, S)` приводит сам столбец, а не агрегат. Если использовать его без обёртки внутри `GROUP BY`, возникнет `NOT_AN_AGGREGATE`. Используйте `fn.sum(column)` или оберните агрегат.
- Вставка отклоняет объекты, которые не являются строками или числами (например, необработанные экземпляры `decimal.js`) — передавайте `.toFixed(scale)`:

```ts
db.insert(measurementLog).values({ value: new Decimal("1.23").toFixed(5) }); // ✅
db.insert(measurementLog).values({ value: new Decimal("1.23") as never }); // ❌ throws
```

## Оконные выражения
<!-- src:6d0b3f76 -->

Оберните выражение функции `fn.*` вызовом `fn.over(...)`. В необязательной спецификации для `partitionBy` можно передавать только выборки билдера, а для `orderBy` — обычные значения `ck.asc(...)` / `ck.desc(...)`:

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

По умолчанию `fn.rowNumber()` возвращает небезопасный `Selection<number>` через `toFloat64`. Вызовите `.toSafe()` перед `fn.over(...)`, чтобы получить точную `Selection<string>`, или `.toMixed()`, чтобы сохранить нативную форму передачи `UInt64` (`Selection<number | string>`). `.toUnsafe()` явно выбирает режим по умолчанию. Билдер намеренно не проверяет, помещается ли число в безопасный диапазон JavaScript: преобразование ClickHouse через `toFloat64` может округлять значения больше `Number.MAX_SAFE_INTEGER`. `.toSafe()` сохраняет точную десятичную строку, а `.toMixed()` — нативную форму результата драйвера. Decimal-агрегаты сохраняют обычный строковый декодировщик и компилируются как `CAST(sum(...) OVER (...) AS Decimal(...))`, поэтому `OVER` остаётся внутри агрегата, а не снаружи приведения. `fn.over` намеренно не принимает отдельный столбец или фрагмент сырого SQL. Эта часть API поддерживает только `PARTITION BY` и `ORDER BY`; для рамок, именованных окон и других конструкций, специфичных для ClickHouse, используйте `ckSql`.

## Функции и табличные функции
<!-- src:1e16409b -->

### `fn`

Сюда входят вспомогательные функции общего назначения, функции преобразования типов и агрегирования, а также функции для работы с JSON, кортежами и таблицами:

- `fn.call()`
- `fn.withParams()`
- `fn.replaceRegexpAll()` — типизированная замена в ClickHouse с синтаксисом RE2; см. [Замена по регулярному выражению](#regex-replacement)
- Вспомогательные функции преобразования типов соответствуют разделу ClickHouse о преобразовании типов: `fn.cast()`, `fn.date()`, `fn.accurateCast*()`, `fn.reinterpret*()`, `fn.parseDateTime*()`, `fn.formatDateTime()`, `fn.formatRow*()`, `fn.toString*()`, `fn.toBool()`, `fn.toInt*()`, `fn.toUInt*()`, `fn.toFloat*()`, `fn.toBFloat16*()`, `fn.toDecimal*()`, `fn.toDecimalString()`, `fn.toFixedString()`, `fn.toDate*()`, `fn.toDateTime*()`, `fn.toTime*()`, `fn.toTime64*()`, `fn.toInterval*()`, `fn.toLowCardinality()`, `fn.toNullable()` и `fn.toUUID*()`.
- Преобразования 64-битных и более широких целых чисел (`toInt64/128/256*`, `toUInt64/128/256*`, функции для 64-битных временных меток Unix) декодируются в `string`; более узкие целые и числа с плавающей точкой — в `number`.
- Аргументы ClickHouse, которые можно задавать только литералами, проверяются и при необходимости подставляются прямо в SQL: масштаб Decimal, точность DateTime64/Time64, длина FixedString, строки целевых типов и единицы интервалов.
- `fn.toStartOfMonth()`
- `fn.toUnixTimestamp()` / `fn.toUnixTimestamp64Second()` / `fn.toUnixTimestamp64Milli()` / `fn.toUnixTimestamp64Micro()` / `fn.toUnixTimestamp64Nano()`
- `fn.fromUnixTimestamp()` / `fn.fromUnixTimestamp64Second()` / `fn.fromUnixTimestamp64Milli()` / `fn.fromUnixTimestamp64Micro()` / `fn.fromUnixTimestamp64Nano()`
- `fn.count()` / `fn.countIf()` — по умолчанию возвращают `Selection<number>` с обёрткой `toFloat64(count(...))`. Для `Selection<string>` вызовите `.toSafe()` (`toString(count(...))`), для `Selection<number | string>` — `.toMixed()` (`toUInt64(count(...))`), а `.toUnsafe()` возвращает режим по умолчанию. Поведение такое же, как у `db.count`. Декодировщики проверяют, что результат — неотрицательное целое число, и отклоняют `NaN`, отрицательные значения, логические значения и прочие недопустимые значения; примеры см. в разделе [Режимы `fn.count` / `fn.uniqExact`](#fncount--fnuniqexact-modes).
- `fn.sum()` / `fn.sumIf()` / `fn.min()` / `fn.max()` / `fn.maxIf()` — для столбцов типа Decimal автоматически приводят результат к `Decimal(P, S)`; см. [Точность Decimal в выражениях](#decimal-precision-in-expressions)
- `fn.over()` / `fn.rowNumber()` — композиция оконных функций с `PARTITION BY` и `ORDER BY`; см. [Оконные выражения](#window-expressions)
- `fn.divideDecimal()` — точное деление Decimal с необязательным масштабом результата, который можно указать прямо в вызове; см. [Точность Decimal в выражениях](#decimal-precision-in-expressions)
- `fn.argMax()` / `fn.argMin()` — возвращают значение `arg` из строки, в которой `val` принимает максимальное или минимальное значение. Тип результата совпадает с типом столбца `arg` (без обёртки CAST для Decimal: ClickHouse сохраняет объявленный тип столбца).
- `fn.avg()` — `Selection<number>` (Float64), что соответствует встроенному в ClickHouse поведению `avg(Decimal)`
- `fn.uniqExact()` — те же три режима, что и у `fn.count()`, которые можно выбирать цепочкой вызовов: по умолчанию `Selection<number>` с обёрткой `toFloat64(uniqExact(...))`, `.toSafe()` для `Selection<string>` (`toString(uniqExact(...))`), `.toMixed()` для `Selection<number | string>` (`toUInt64(uniqExact(...))`) и `.toUnsafe()` для возврата к режиму по умолчанию. Декодировщики используют ту же проверку: принимают только неотрицательные целые числа. См. [Режимы `fn.count` / `fn.uniqExact`](#fncount--fnuniqexact-modes).
- `fn.coalesce()`
- `fn.if()` / `fn.multiIf()` — условные выражения. Обе функции возвращают `Selection<unknown>`, потому что ClickHouse определяет тип результата на сервере с учётом всех ветвей значений.
- `fn.greatest()` / `fn.least()` / `fn.nullIf()` — сравнения; `nullIf` автоматически оборачивает sqlType в `Nullable(...)` и не оборачивает его повторно, если вход уже допускает NULL.
- `fn.positionCaseInsensitive()` / `fn.positionCaseInsensitiveUTF8()` — поиск подстроки без учёта регистра и с поддержкой UTF-8. Возвращают `Selection<string>` (ClickHouse `UInt64`, декодируется в строку, чтобы сохранить 64-битную точность — так же, как `fn.indexOf()`). Необязательный `startPos` должен иметь тип `UInt*`: передайте столбец UInt или `fn.toUInt64(n)` для литерала. Числовой литерал без приведения получает тип `Int64`, и ClickHouse его отклонит.
- `fn.jsonExtract()`
- `fn.tuple()`
- `fn.arrayJoin()`
- `fn.tupleElement()`
- `fn.not()`

### Типы ветвей условных выражений

Обобщённый параметр результата у `fn.if<TData>(...)` и
`fn.multiIf<TData>(...)` удалён: теперь эти вызовы не компилируются. При наличии
известного якоря типа `Float64` или `UInt64` совместимые ветви с литералами
безопасных целых чисел получают тип из контекста; литералы `UInt64` к тому же
должны быть неотрицательными.
Разнородные выражения и сырой SQL никогда не приводятся автоматически, поэтому
явно преобразуйте каждую ветвь значения. `.mapWith(...)` задаёт только
декодировщик JavaScript и не меняет тип SQL. В частности, резервная ветвь с
неотрицательным литералом безопасного целого числа рядом с
`fn.over(fn.rowNumber().toMixed())` получает тип `UInt64`.
`fn.if(...)` и `fn.multiIf(...)` — скалярные функции, поэтому напрямую передать
их в `fn.over(...)` нельзя; при этом ветвью значения всё ещё может быть оконная
выборка.

Если для всех ветвей значений нельзя доказать единый SQL-тип, метаданные результата
остаются неизвестными, а при декодировании значение передаётся как есть. Поэтому
уже существующие результаты могут смениться с `number` на транспортный `string`.
При миграции явно приведите каждую ветвь значения к одному SQL-типу, затем при
необходимости добавьте `.mapWith(...)`. Автоматические метаданные результата
намеренно ограничены якорями точных типов `Bool`, `Int64`, `UInt64`, `Float64` и
`String`. Остальные типы всё равно передаются как есть, даже если метаданные
ветвей совпадают: ClickHouse может нормализовать одинаковые типы хранения
(например, `LowCardinality(String)` превращается в `String`).

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

У `fn.coalesce(...)` резервные значения больше не типизируются по контексту как
`Float32`, `BFloat16`, `Decimal` или как `bigint`, если нельзя доказать, что его
целевой диапазон безопасен. Преобразуйте такие резервные значения явно:

```ts
fn.coalesce(amount, fn.toDecimal64("0.00", 2));
```

### Замена по регулярному выражению

`fn.replaceRegexpAll(haystack, pattern, replacement)` генерирует вызов функции
ClickHouse [`replaceRegexpAll`](https://clickhouse.com/docs/sql-reference/functions/string-replace-functions#replaceregexpall).
Она возвращает `Selection<string>`, а для типизированного входа, который может
быть null, — `Selection<string | null>`. Используйте `String.raw` для обратных
слешей в RE2 и подстановок захваченных групп:

```ts
const usDate = fn.replaceRegexpAll(
  orders.dateText,
  String.raw`^(\d{4})-(\d{2})-(\d{2})$`,
  String.raw`\2/\3/\1`,
);
// Selection<string>
```

Аргументы передаются как параметры запроса. Шаблоны используют синтаксис RE2 в
ClickHouse; замена поддерживает `\0`–`\9`, а некорректные шаблоны по-прежнему
приводят к ошибкам ClickHouse. Для нетипизированного входа `ckSql` консервативно
предполагается, что результат может быть null; если известно, что фрагмент не
может быть null, используйте `ckSql<string>`.

### Типизированная агрегация кортежей

`fn.tuple()` сохраняет тип каждого элемента. Вызов `groupArray` с одним аргументом
сохраняет тип кортежа и соответствующий декодировщик, а унарная `fn.arrayReverseSort()` —
тот же тип элемента массива:

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

Вариант с явным параметром типа по-прежнему доступен для сырых или динамических
значений.

Названия вспомогательных функций для массивов совпадают с каноническими заголовками
в справочнике ClickHouse [Функции массивов](https://clickhouse.com/docs/sql-reference/functions/array-functions).
Имена, которые существуют только как псевдонимы, остаются доступными через
`fn.call(...)`, поэтому не приходится раздувать публичный API.

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

`fn.call(name, ...)` и `fn.withParams(name, ...)` проверяют `name` как
SQL-идентификатор до компиляции. `ck.has(...)`, `ck.hasAll(...)`,
`ck.hasAny(...)` и `ck.hasSubstr(...)` — короткие предикаты для тех же функций
ClickHouse, удобные для условий WHERE.

`fn.jsonExtract(json, returnType, ...path)` принимает только типы результата
`ckType.*`, поэтому тип результата ClickHouse согласован с декодировщиком TypeScript:

В следующих примерах с JSON и массивами предполагается, что в вымышленной схеме
`probeTelemetry` также есть столбцы `metadata: JSON` и `tags: Array(String)`.

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

Сегменты пути передаются как аргументы пути JSON в ClickHouse. Для полей объекта используйте
строковые ключи, а для позиций в массиве — индексы типа number или bigint:

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

Функции массивов высшего порядка используют тот же порядок параметров, что и ClickHouse.
Передавайте лямбду через `ckSql`, подставляйте поля внешней схемы, а локальные
переменные лямбды оставляйте обычными SQL-именами. Функции, в которых лямбда
задаёт форму элемента, например `fn.arrayMap(...)`, `fn.arrayFilter(...)` и
`fn.range(...)`, по умолчанию возвращают `Selection<unknown[]>`; добавьте
`.mapWith(...)`, если запросу нужен более конкретный тип элемента после декодирования.

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

В этой лямбде `${params.timeColumn}` и `params.scopeTable.startTsList` —
выражения, привязанные к схеме. `start_ts` и `end_ts` — локальные переменные
лямбды, которые ClickHouse создаёт из элементов массива.

`fn.arrayJoin(array)` соответствует `arrayJoin` в ClickHouse и превращает одну
входную строку в набор строк — по одной на каждый элемент массива. Сочетайте его с
`fn.arrayZip(...)` и `fn.tupleElement(...)`, когда нужно сохранить соответствие
элементов двух массивов по позициям:

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

Здесь важна семантика: `arrayZip(sampleIds, probeIds)` сохраняет пары по позициям,
`arrayJoin(...)` разворачивает их в строки, а `tupleElement(..., 1/2)` извлекает
обе части пары.

Для составных ключей проверяйте вхождение кортежа. Два
независимых предиката `IN` дают декартово произведение и могут сопоставить пары,
которых в запросе не было:

```ts
// Avoid this when sampleId and probeId must stay paired.
const wrong = ck.and(
  ck.inArray(probeTelemetry.sampleId, sampleIds),
  ck.inArray(probeTelemetry.probeId, probeIds),
);
```

Чтобы сохранить семантику пар, сравнивайте один кортеж с подзапросом, возвращающим
кортежи, или со списком пар. `fn.tuple` сохраняет тип каждого элемента, если он
известен, поэтому список передаётся одним параметром типа `Array(Tuple(...))`:

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  [
    ["900001", "probe_alpha"],
    ["900002", "probe_beta"],
  ],
);
```

У фрагмента сырого SQL нет типа элемента. Оберните кортеж с помощью
`fn.cast(fn.tuple(...), "Tuple(Int32, String)")` перед использованием в
`ck.inArray` или `ck.eq`.

Если подзапрос возвращает кортежи, левая часть остаётся прежней — параметр не нужен:

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  scopedSamples,
);
```

Вспомогательные функции массивов возвращают типизированные значения `Selection`,
поэтому их можно комбинировать в проекциях, фильтрах, группировках и соединениях:

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

### Режимы `fn.count` / `fn.uniqExact`

`fn.count()`, `fn.countIf(predicate)` и `fn.uniqExact(expression)` — агрегатные
аналоги `db.count(...)` с теми же тремя режимами. Они нужны, когда подсчёт
выполняется внутри запроса — в `select`, `having`, группировках или подзапросах, —
а не как отдельный скаляр на верхнем уровне. SQL-обёртки и декодировщики у всех
режимов одинаковые, поэтому выбор режима связан с теми же компромиссами:

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

Декодировщики отклоняют всё, что не является неотрицательным целым числом:
логические значения, `NaN`, десятичные и отрицательные числа вызывают
`Failed to decode count() result: ...`. Поэтому повреждённый ответ сервера сразу
приводит к ошибке, а не молча превращается в `0`.

Другие агрегаты намеренно устроены иначе: требования к точности у них не такие,
как у `count` / `uniqExact`:

- `fn.sum()` / `fn.sumIf()` уже выбирают безопасный декодировщик для каждого входного столбца (строку для целых чисел и Decimal, число — для чисел с плавающей точкой) и автоматически расширяют точность Decimal; добавьте `.mapWith(Number)`, если нужен именно `number`.
- `fn.avg()` всегда возвращает `Selection<number>`, потому что внутри ClickHouse вычисляет `avg(Decimal)` через Float64; преобразование результата в строку исказило бы фактический путь выполнения.
- `fn.min()` / `fn.max()` сохраняют TS-тип входного столбца (поэтому `min(int64Col)` даёт `Selection<string>`, как и сам столбец при чтении).

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

## Таблицы и материализованные представления
<!-- src:03694c38 -->

`createTable()`, `createMaterializedView()` и `dropTable()` отправляют по одному оператору DDL, построенному из объектов схемы. Они не сравнивают схемы, не выполняют `ALTER` и не записывают версии; миграции существующих таблиц оставляйте в инструменте миграций. Методы доступны и на клиенте, и в сессии. Созданные ими таблицы и представления постоянны, и `runInSession()` их не удаляет.

### `createTable()`

`createTable(table, options?)` отправляет оператор `CREATE TABLE`, собранный из определения `ckTable(...)`: столбцы с метаданными DDL, затем `ENGINE`, `PARTITION BY`, `PRIMARY KEY`, `ORDER BY`, `SAMPLE BY`, `TTL`, `SETTINGS` и `COMMENT`.

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

- по умолчанию существующая таблица с тем же именем приводит к ошибке `TABLE_ALREADY_EXISTS`
- `mode: "if_not_exists"` формирует `CREATE TABLE IF NOT EXISTS` и оставляет существующую таблицу как есть, даже если столбцы отличаются
- `mode: "or_replace"` формирует `CREATE OR REPLACE TABLE`, заменяет существующую таблицу и отбрасывает её данные
- таблица без `engine` получает `MergeTree` с `ORDER BY tuple()`
- таблица-псевдоним из `ckAlias(...)` отклоняется

### `createMaterializedView()`

`createMaterializedView(name, { to, as, mode? })` создаёт материализованное представление, которое срабатывает при вставке и пишет в существующую таблицу `to`. `as` — построитель выборки с теми же правилами, что у `insert(to).fromSelect(...)`: он должен спроецировать каждый обязательный столбец `to`, а неизвестные ключи и несовпадение типов — ошибки TypeScript.

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

- ClickHouse записывает вывод материализованного представления в `to` по имени столбца, поэтому ck-orm оборачивает выборку как `select ... as <column> from (...)`, и каждый ключ попадает в свой столбец `to`
- параметры запроса в `as` подставляются при создании представления, поэтому сохранённое представление хранит их значения
- настройки, которые выборка задаёт принудительно, например `join_use_nulls = 1` из `leftJoin()`, попадают в предложение `SETTINGS` представления, потому что настройки запроса вместе с представлением не сохраняются
- представление видит только строки, вставленные после его создания; чтобы заполнить прошлые строки, выполните `db.insert(to).fromSelect(...)` с той же выборкой
- `mode: "if_not_exists"` формирует `IF NOT EXISTS`. В ClickHouse 25.8 и 26.3 нет `CREATE OR REPLACE MATERIALIZED VIEW`, поэтому режима `or_replace` нет
- `to` обязателен: представления с собственным `ENGINE`, `POPULATE` и обновляемые представления не поддерживаются; для них используйте `ckSql` с `command()`

### `dropTable()`

`dropTable(table, options?)` удаляет таблицу или представление по объекту схемы или по имени:

```ts
await db.dropTable("usage_minutes_mv", { sync: true });
await db.dropTable(usageMinutes, { ifExists: true, sync: true });
```

- по умолчанию формирует `DROP TABLE name` и завершается ошибкой `UNKNOWN_TABLE`, если объекта с таким именем нет
- `ifExists: true` формирует `IF EXISTS`
- `sync: true` формирует `SYNC`: вызов возвращается, когда ClickHouse удалил данные, а не оставляет это отложенной очистке движка базы `Atomic`
- удаление материализованного представления оставляет его таблицу `to` и уже записанные туда строки
- таблица-псевдоним из `ckAlias(...)` отклоняется, и `ON CLUSTER` не формируется; в кластере используйте инструмент миграций или `ckSql` с `command()`

## Сессии и временные таблицы
<!-- src:8f031227 -->

ClickHouse не поддерживает традиционные транзакции. Для аналитических задач с
ограниченным охватом и сценариев с большими наборами фильтров часто подходят
временные таблицы, привязанные к сессии.

Внутри функции обратного вызова сессии `ck-orm` доступны:

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

Вызовите `registerTempTable(name)`, если временную таблицу создаёт SQL-команда под вашим контролем,
но удалить её при очистке сессии должен `ck-orm`:

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

Используйте `createTemporaryTableRaw(name, definition)` только с доверенными
определениями таблиц, которые вы полностью контролируете и которые нельзя
описать через `ckTable(...)`:

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

Аргумент `definition` начинается после имени таблицы. Не добавляйте
`CREATE TEMPORARY TABLE` и имя таблицы: их подставляет ck-orm и отдельно
проверяет идентификатор.

### Поведение сессии

- запросы с одинаковым `session_id` используют общий контроллер параллелизма сессии; по умолчанию клиент задаёт `session_max_concurrent_requests = 1`
- сессии ClickHouse на сервере эксклюзивны, поэтому `session_max_concurrent_requests` выше `1` отключает локальную сериализацию, но запрос всё равно может завершиться ошибкой `SESSION_IS_LOCKED`
- `createTemporaryTable()` автоматически регистрирует таблицу для очистки
- `createTemporaryTable()` принимает объекты схемы; жизненным циклом временной таблицы управляет `Session`, а не сама схема
- `createTemporaryTableRaw()` — API сырого SQL только для доверенных определений временных таблиц; он отклоняет определения с несколькими операторами
- `runInSession()` автоматически отправляет полученный `session_id` как `X-ClickHouse-Replica-Tag` с каждым запросом, включая очистку; этот заголовок задаётся средой выполнения, и его нельзя переопределить через `http_headers`
- в ClickHouse Cloud [маршрутизацию с учётом реплик](https://clickhouse.com/docs/products/cloud/features/infrastructure/replica-aware-routing) нужно включить отдельно, и она работает без гарантии результата: при масштабировании тег может быть переназначен, а для браузерных приложений, работающих из другого источника, нужно разрешить `X-ClickHouse-Replica-Tag` в CORS
- `runInSession()` удаляет зарегистрированные временные таблицы после завершения функции обратного вызова
- вложенные вызовы `runInSession()` всегда создают новую дочернюю сессию
- вложенные вызовы не могут повторно использовать `session_id` активной сессии-предка
- дочерние сессии не имеют общих временных таблиц и слотов параллелизма с родительской сессией, поскольку всегда используют другой `session_id`

### Контракт параллелизма в сессии

На практике действуют такие правила:

- запросы с одинаковым явным `session_id` по умолчанию выполняются последовательно
- для клиентского `session_id` по умолчанию действует тот же ограничитель
- дочерние клиенты из `withSettings()` при совпадающем `session_id` используют тот же ограничитель, что и родительский клиент
- разные `session_id` не блокируют друг друга
- `stream()` и `iterator()` билдера занимают слот параллелизма текущей сессии, пока итератор не завершится. При досрочной остановке через `break` или `return` либо из-за ошибки серверный запрос отменяется командой `KILL QUERY`, которую отправляют вне сессии (с теми же учётными данными и тегом реплики). Затем клиент ждёт, пока ClickHouse освободит сессию, но не дольше `request_timeout` (по умолчанию 30 с)
- вложенные вызовы `runInSession()` создают новый дочерний `session_id`, поэтому не используют слот параллелизма родительской сессии

При отмене запроса или срабатывании `request_timeout` закрывается только соединение. ClickHouse продолжает выполнять запрос, пока он не завершится или сервер не заметит разрыв. Поэтому `INSERT ... SELECT` всё равно может завершиться, а следующий запрос в этой сессии может завершиться ошибкой `SESSION_IS_LOCKED`. Чтобы сервер остановил запрос первым, задайте `max_execution_time` в `clickhouse_settings` меньше `request_timeout`; выполните `KILL QUERY WHERE query_id = '...'` вне сессии; или оберните `fetch`, чтобы повторять запросы с кодом 373 ограниченное число раз, если тело можно отправить повторно. Ошибки очистки, включая `SESSION_IS_LOCKED`, передаются в `onCleanupError` или выбрасываются как `SessionError`.

Рекомендуемый сценарий:

- оставьте `session_max_concurrent_requests = 1` для сценариев с временными таблицами и логики, которая продолжает работу в той же сессии
- если нужен настоящий параллелизм, используйте разные `session_id`, а не увеличивайте лимит для одной сессии
- увеличивайте `session_max_concurrent_requests` только если намеренно снимаете локальное ограничение числа одновременных запросов и готовы к `SESSION_IS_LOCKED`

## Методы выполнения
<!-- src:1d0d331a -->

Помимо билдера, клиент предоставляет низкоуровневые методы выполнения.

У отдельного запроса есть такие общие поля:

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

`session_timeout` и `session_check` находятся здесь намеренно: они относятся к конкретному запросу или блоку конкретной сессии, а не к глобальным настройкам клиента по умолчанию.
`query_id`, `session_id` и `session_timeout` — это параметры запроса или сессии. Не включайте их в `clickhouse_settings`.

`session_check` не создаёт новую сессию ClickHouse. Используйте его, когда нужно проверить, что явно указанный `session_id` уже существует в ClickHouse.
Любой запрос с `session_id` подчиняется общему ограничителю для этой сессии, в том числе низкоуровневые `execute()`, `command()` и `stream()`, а также `.execute()` и `.iterator()` билдера.

`clickhouse_settings`, заданные для отдельного запроса, переопределяют настройки ClickHouse только для этого запроса:

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

Выполняет сырой запрос и возвращает `Record<string, unknown>[]`:

```ts
const rows = await db.execute(ckSql`SELECT 1 AS one`);
```

### `stream()`

Передаёт результаты сырого запроса потоком:

```ts
for await (const row of db.stream(ckSql`SELECT number FROM numbers(10)`)) {
  console.log(row);
}
```

Если `stream()` использует `session_id`, слот текущей сессии освобождается только после завершения асинхронного итератора или его досрочного закрытия выходом из цикла.

### `command()`

Выполняет команду, которая не возвращает набор строк:

```ts
await db.command(ckSql`SYSTEM FLUSH LOGS`);
```

### `withSettings()`

Создаёт дочерний клиент с дополнительными настройками по умолчанию:

```ts
const reportDb = db.withSettings({
  max_execution_time: 60,
  join_use_nulls: 0,
});
```

`withSettings()` меняет только настройки ClickHouse, заданные по умолчанию для сессий и запросов. Возвращённый клиент сохраняет транспорт, аутентификацию, таймаут и ограничитель параллелизма сессий родительского клиента. Литерал `join_use_nulls: 0` выводится как литеральный тип, поэтому тип левых соединений дочернего клиента не включает null; значение с типом `number` всё равно даёт тип `| null`.

В `runInSession()` параметры жизненного цикла сессии задаются отдельно от настроек ClickHouse:

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

`runInSession()` объединяет свои `clickhouse_settings`, `http_headers` и `query_params` со значениями по умолчанию родительского клиента; если один и тот же ключ задан в обоих местах, используется значение сессии. Как и в `withSettings()`, литерал `join_use_nulls: 0` или `join_use_nulls: 1` в настройках сессии также определяет, включает ли тип левых соединений сессии null.

### Интеграция со скомпилированными запросами

В большинстве приложений используйте билдер напрямую. Используйте `executeCompiled()` и `iteratorCompiled()`, если интегрируете другой слой, который уже создаёт `CompiledQuery`.

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

### `ping()` и `replicasStatus()`

```ts
await db.ping();
await db.replicasStatus();
```

## Наблюдаемость
<!-- src:11c06560 -->

В `ck-orm` есть два слоя наблюдаемости:

- `logger` и `logLevel` для встроенного логирования
- `plugins` для хуков жизненного цикла запроса; трассировка OpenTelemetry поставляется как плагин `ck-orm/otel`

### Логгер

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

### Плагин OpenTelemetry

`otelTracing()` из `ck-orm/otel` записывает один клиентский спан на каждый запрос, который ck-orm отправляет в ClickHouse. Плагин лежит в отдельной точке входа: основная точка входа `ck-orm` никогда не импортирует OpenTelemetry, поэтому приложения, браузерные бандлы и edge-воркеры, которым трассировка не нужна, не несут из-за неё накладных расходов. `@opentelemetry/api` — необязательная peer-зависимость, которую импортирует только `ck-orm/otel`.

#### Настройка

ck-orm создаёт спаны через OpenTelemetry API, но один API ничего не записывает: пока SDK не зарегистрирует провайдер трассировщика, все спаны отбрасываются. ck-orm не запускает SDK и не выбирает экспортер, поэтому и то и другое делает ваше приложение — один раз при старте.

Установите API, SDK и экспортер. Ниже — SDK для Node.js и экспортер OTLP/HTTP; подойдёт любой SDK, который регистрирует провайдер трассировщика, в том числе браузерный.

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

Запустите SDK до первого запроса, например в модуле, который ваша точка входа импортирует первым:

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

Экспортер отправляет данные на `http://localhost:4318/v1/traces`, если вы не задали стандартную переменную `OTEL_EXPORTER_OTLP_ENDPOINT` и не передали `url`. Кроме того, SDK регистрирует менеджер контекста и propagator W3C: от них зависят выбор родительского спана и `propagateTraceContext`. Если процесс короткоживущий, перед выходом обязательно вызовите `await sdk.shutdown()`, иначе последние спаны потеряются.

Затем добавьте плагин в клиент:

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

Чтобы увидеть спаны без коллектора, посмотрите [пример трассировки OpenTelemetry](https://ck-orm.lyz.cloud/examples/#otel-tracing): он использует экспортер в памяти и выводит то, что записал плагин.

#### Спаны

Это клиентские спаны (`CLIENT`). Запросы, которые возвращают строки, дают спан `clickhouse QUERY`, всё остальное — `clickhouse <OPERATION>`, например `clickhouse INSERT` или `clickhouse CREATE`. Спан становится дочерним для спана, который активен в момент начала запроса, например для того, который инструментирование OpenTelemetry вашего фреймворка открывает на каждый HTTP-запрос.

Если не передать `tracer`, плагин использует `trace.getTracer("ck-orm")`. Он берёт из конфигурации `clickhouseClient(...)` имя базы данных, адрес и порт сервера, а также таймаут запроса. Передавайте атрибуты уровня сервиса через `attributes`; ключи, начинающиеся с `db.`, игнорируются, поэтому код приложения не может перезаписать встроенные атрибуты базы данных.

Для общего бэкенда трассировки безопаснее использовать `includeStatement: false`. По умолчанию стоит `true`; отключите эту настройку, если имена таблиц, столбцов или форма запроса не должны покидать границы сервиса. Привязанные значения не попадают в компактную форму запроса, но даже форма SQL может содержать чувствительные сведения о работе сервиса.

#### Серверные спаны

Передайте `propagateTraceContext: true`, чтобы с каждым запросом отправлялся контекст трассировки W3C (`traceparent`, `tracestate`) текущего спана. Тогда ClickHouse записывает собственные спаны запроса, такие как `HTTPHandler`, `query` и стадии конвейера, как дочерние по отношению к спану ck-orm. Плагин пересылает только эти два заголовка, а `baggage` и вендорские заголовки вашего глобального propagator не отправляет никогда, и только для семплированных спанов (sampled): ClickHouse записывает серверные спаны для любого полученного контекста трассировки, независимо от семплирования. Плагин внедряет контекст через глобально зарегистрированный propagator, который OpenTelemetry SDK по умолчанию настраивает на контекст трассировки W3C; если propagator не зарегистрирован, ничего не отправляется.

```ts
const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123/telemetry_lab",
  plugins: [otelTracing({ propagateTraceContext: true })],
});
```

ClickHouse записывает эти спаны в `system.opentelemetry_span_log` и сам их не экспортирует; об экспорте в бэкенд трассировки см. [Tracing ClickHouse with OpenTelemetry](https://clickhouse.com/docs/operations/opentelemetry). Серверные спаны содержат `clickhouse.query_id`, который совпадает с `db.query.id` в спане ck-orm, а об ошибках сообщают через `clickhouse.exception_code`, а не через статус спана.

Если вы передадите собственный `traceparent` в `http_headers`, он переопределит заголовок плагина; передайте и соответствующий `tracestate`, иначе вместе с ним будет отправлен `tracestate` плагина.

В браузерах из-за `traceparent` предварительный запрос CORS (preflight) завершается ошибкой, если ClickHouse не разрешает этот заголовок, а поскольку его несут только семплированные запросы, сбои выглядят случайными. Значение `http_options_response` по умолчанию его не разрешает, поэтому включайте распространение в браузерах только после добавления заголовка в файл `config.d`:

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

### Написание плагинов

Плагин — это объект с полем `name` и не более чем тремя хуками:

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

`onQueryStart` может вернуть заголовки для этого запроса:

```ts
const requestSource: ClickHouseORMPlugin = {
  name: "request-source",
  onQueryStart() {
    return { http_headers: { "x-request-source": "billing-worker" } };
  },
};
```

[Пример плагинов запросов](https://ck-orm.lyz.cloud/examples/#query-plugins) запускает плагин: `onQueryStart` возвращает заголовок, а `onQuerySuccess` и `onQueryError` пишут в лог каждый оператор в порядке выполнения.

Плагины подчиняются следующим правилам:

- Хуки выполняются для каждого запроса, который ck-orm отправляет в ClickHouse, включая запросы, потоки, команды, вставки, вспомогательные функции DDL, `ping()`, `replicasStatus()` и очистку временных таблиц в `runInSession()`. Хуки не запускают только запросы, которые отменяют брошенный поток и ждут разблокировки его сессии (`KILL QUERY` и последующий опрос).
- За каждым `onQueryStart` следует ровно один `onQuerySuccess` или `onQueryError` с тем же `executionId`; для потока это происходит, когда итератор завершается или закрывается. Поток, который вызывающий код остановил досрочно, считается успешным и сообщает число строк, прочитанных к этому моменту. Храните состояние запроса по ключу `executionId`.
- Хуки `onQueryStart` выполняются в порядке регистрации, и все они завершаются до отправки запроса. `onQuerySuccess` и `onQueryError` выполняются в обратном порядке. Встроенный логгер всегда работает самым внешним.
- Заголовок более позднего плагина переопределяет заголовок более раннего. Явно заданные `http_headers` (на уровне клиента или отдельного запроса) переопределяют заголовки плагинов. Плагины не могут переопределить заголовок `Authorization`, построенный из учётных данных подключения, и заголовок `X-ClickHouse-Replica-Tag`, который отправляют сессии.
- Хук, выбросивший исключение, попадает в журнал вместе с именем плагина и не влияет ни на запрос, ни на другие плагины. Заголовки из `onQueryStart`, который выбросил исключение или вернул недопустимый заголовок, отбрасываются.
- События доступны только для чтения. Плагины не могут изменять SQL, параметры, настройки и результаты.
- Хуки ожидаются (await) на пути выполнения запроса. Медленную работу, например сетевой экспорт, буферизуйте, а не ожидайте в хуке.

Публичные типы наблюдаемости:

- `ClickHouseORMPlugin`
- `ClickHouseORMQueryStartResult`
- `ClickHouseORMQueryEvent`
- `ClickHouseORMQueryResultEvent`
- `ClickHouseORMQueryErrorEvent`
- `ClickHouseORMQueryStatistics`
- `ClickHouseORMTracingOptions` (из `ck-orm/otel`)
- `ClickHouseORMLogLevel`

Встроенные поля событий включают `databaseName`, `serverAddress`, `serverPort`, `requestTimeoutMs`, `statementHash`, `querySummary`, `queryId`, `sessionId` и `tableName`, когда ck-orm может определить единственную таблицу, не угадывая её по сырому SQL. JSON-запросы, выполняемые целиком, также содержат статистику ответа ClickHouse, если сервер её возвращает: `serverElapsedMs`, `readRows`, `readBytes`, `resultRows` и `rowsBeforeLimitAtLeast`.

## Модель ошибок
<!-- src:b4b93db0 -->

Публичные типы ошибок (экспортируются только как типы):

- `ClickHouseORMError`
- `DecodeError`

Функции для проверки ошибок во время выполнения:

- `isClickHouseORMError(error)`
- `isDecodeError(error)`

`ClickHouseORMError` сохраняет максимум доступного контекста, в том числе:

- `kind`
- `executionState`
- `queryId`
- `sessionId`
- `httpStatus`
- `clickhouseCode`
- `clickhouseName`
- `responseText`
- `requestTimeoutMs`

Текущие публичные значения `kind`:

- `client_validation`
- `request_failed`
- `decode`
- `timeout`
- `aborted`
- `session`

Полный контекст ошибки записывайте в логи на сервере, а вызывающему коду возвращайте короткую публичную ошибку:

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

Не возвращайте `responseText` или исходный текст ошибки `message` клиентам, которым нельзя доверять. В тексте ошибки ClickHouse могут быть фрагменты SQL и имена объектов.

## Безопасность
<!-- src:062d0032 -->

По умолчанию `ck-orm` устроен так, чтобы небезопасный SQL было трудно составлять.

Встроенная защита включает:

- идентификаторы, переданные в `ckSql.identifier()`, проверяются и заключаются в кавычки
- имена функций, используемые в `fn.call(...)`, `fn.withParams(...)`, `ckType.aggregateFunction(...)` и `ckType.simpleAggregateFunction(...)`, проверяются; параметры `AggregateFunction` должны быть строковыми или числовыми литералами
- ключи вложенных полей, переданные в `ckType.nested({...})`, проверяются как идентификаторы ClickHouse
- значения, подставляемые в `` ckSql`...` ``, становятся именованными параметрами ClickHouse, а не текстом сырого SQL
- ключи `query_params`, начинающиеся с `orm_param`, отклоняются: этот префикс зарезервирован для внутренних параметров SQL-шаблонов
- в каждом запросе разрешён только один оператор верхнего уровня
- заголовки авторизации из конфигурации подключения нельзя переопределить через переданный вызывающим кодом `http_headers`
- структурированная конфигурация подключения отклоняет учётные данные, встроенные в `host`
- адрес назначения для трассировки нормализуется перед добавлением в спаны

### API только для доверенного кода

Если доверенный DDL временной таблицы нельзя описать через `ckTable(...)`, `ck-orm` предоставляет API сырого SQL, доступный только внутри `runInSession()`:

- `session.createTemporaryTableRaw(name, definition)`

`fn.call(name, ...)` и `fn.withParams(name, ...)` проверяют `name`, но динамически выбранные имена функций всё равно считайте вводом под контролем разработчика, а не непроверенным вводом во время выполнения.

`ckSql.identifier(...)` выполняет проверку на границе компиляции и выполнения. Само создание фрагмента почти ничего не стоит; ошибка появляется, когда фрагмент компилируется в запрос.

### Какие данные раскрывает трассировка

`otelTracing()` добавляет компактный текст SQL к спанам как `db.statement` и `db.query.text`, потому что `includeStatement` по умолчанию равно `true`.

Привязанные значения хранятся отдельно как именованные параметры ClickHouse и не входят в текст запроса, но имена таблиц и столбцов, а также форма запроса всё равно попадают в вывод трассировки. Если бэкенд трассировки общий или находится вне хоста сервиса, отключите `includeStatement` или отфильтруйте `db.statement` / `db.query.text` в коллекторе.

Серверные спаны ClickHouse также записывают запрос как `db.statement` в `system.opentelemetry_span_log`. Эта таблица остаётся на сервере ClickHouse, пока вы её не экспортируете.

### Ответы с ошибками

`ClickHouseORMError.responseText` и исходное значение `message` могут содержать сырой текст от сервера ClickHouse, включая фрагменты SQL и имена объектов базы данных.

Не отправляйте эти данные клиентам, которым нельзя доверять. Записывайте их в лог на сервере и возвращайте вместо них общее сообщение.

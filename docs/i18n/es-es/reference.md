## Instalación
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

ck-orm tiene una única peer dependency: `@opentelemetry/api`. Bun, npm y pnpm la instalan automáticamente; Yarn no lo hace, así que el comando de Yarn la añade explícitamente.

## Inicio rápido
<!-- src:d7ba4246 -->

Los ejemplos siguientes usan una sola tabla para que sea fácil seguir el recorrido principal.

### 1. Define un esquema

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

### 2. Crea un cliente

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

### 3. Consulta datos

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

Las consultas del constructor son thenable: `await query` las ejecuta directamente.

`fn.sum` sobre una columna `Decimal` convierte automáticamente a `Decimal(P, S)` y devuelve `string` para conservar toda la precisión; consulta [Precisión de Decimal en expresiones](#decimal-precision-in-expressions).

## Ejemplos
<!-- src:c8f5c20b -->

La [página de ejemplos](https://ck-orm.lyz.cloud/examples/) muestra consultas que puedes ejecutar y el SQL exacto que cada una envía a ClickHouse. Cada ejemplo es un proyecto en [`examples/`](../examples) con su propia prueba. Las pruebas de ClickHouse que puedes ejecutar están en [`e2e/`](../e2e), donde también encontrarás los esquemas de escenarios parecidos a los de producción de [`e2e/scenarios.ts`](../e2e/scenarios.ts).

## Modelo mental
<!-- src:f7815577 -->

Ten en cuenta estos límites al usar `ck-orm`:

- `ckType.*` define los tipos de columna del esquema.
- `ckTable(...)` define los esquemas de las tablas.
- `ck.*` es el espacio de nombres de las funciones auxiliares de consulta.
- `fn.*` es el espacio de nombres de las funciones auxiliares de SQL.
- El esquema describe las tablas y las columnas, no el nombre de la base de datos.
- La conexión a la base de datos se configura en `clickhouseClient(...)`.
- Las consultas del constructor son la opción predeterminada; para expresiones específicas de ClickHouse puedes usar SQL sin formato.
- `runInSession()` es una función auxiliar de sesión de ClickHouse, no una transacción.
- `leftJoin()` usa la semántica SQL de NULL de forma predeterminada.
- Los resultados numéricos grandes o de alto riesgo se decodifican con cautela, en vez de convertirse silenciosamente.

`ck-orm` se centra en el acceso tipado a ClickHouse: definiciones de tablas, composición de consultas, funciones auxiliares de escritura y de sesión, y observabilidad. El DDL de producción y los flujos de migración de esquemas siguen gestionándose con las herramientas de migración que usa la aplicación.

## DSL de esquemas
<!-- src:6809e7a9 -->

### `ckTable()`

Usa `ckTable(name, columns, options?)` para definir una tabla.

Los ejemplos de esta sección parten de la siguiente importación:

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

El tercer argumento puede ser un objeto plano o una función de fábrica:

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

Opciones públicas de la tabla:

- `engine`
- `partitionBy`
- `primaryKey`
- `orderBy`
- `sampleBy`
- `ttl`
- `settings`
- `comment`
- `versionColumn`

Las definiciones de columnas también pueden incluir directamente metadatos de DDL:

- `.default(expr)`
- `.materialized(expr)`
- `.aliasExpr(expr)`
- `.comment(text)`
- `.codec(expr)`
- `.ttl(expr)`

Los metadatos del esquema cumplen dos funciones en `ck-orm`:

- sirven para construir consultas tipadas, validar inserciones y decodificar resultados
- pueden generar DDL estructurado para tablas temporales de sesión

Estos metadatos no migran ni sincronizan automáticamente las tablas de ClickHouse en producción. Mantén el DDL de producción en tu herramienta de migración y alinea `ckTable(...)` con el esquema que tu aplicación lee y escribe.

Ejemplo:

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

### Inferencia de tipos

Cada tabla expone:

- `table.$inferSelect`
- `table.$inferInsert`

```ts
type TelemetryRow = typeof probeTelemetry.$inferSelect;
type TelemetryInsert = typeof probeTelemetry.$inferInsert;

const sampleId: TelemetryRow["sampleId"] = "900001";
const ingestVersion: TelemetryInsert["ingestVersion"] = "1";
```

Para las funciones auxiliares genéricas, usa:

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

Los objetos de esquema que usas con `InferSelectSchema` e `InferInsertSchema` son solo agrupaciones de TypeScript. Sirven para compartir tipos de modelo y van aparte de la configuración del cliente.

### Ajustar los tipos de columna con `$type<T>()`

Cada constructor de columnas expone un método `$type<T>()` encadenable que sobrescribe el tipo TypeScript de la columna sin modificar su comportamiento en tiempo de ejecución. Úsalo cuando el tipo inferido por defecto sea demasiado amplio para tu dominio:

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

Puedes encadenar `$type<T>()` con cualquier otro modificador de columna, en cualquier orden: `ckType.string().$type<Role>().default(ckSql`'guest'`).comment("…")`. El decodificador y el codificador en tiempo de ejecución, el tipo SQL y la salida DDL no se ven afectados: `$type` solo es una aserción en tiempo de compilación.

`enum8` y `enum16` ya infieren la unión de literales a partir del objeto de valores, así que solo necesitas `$type` para asignarle una marca distinta a la unión o alinearla con un tipo de enumeración externo:

```ts
// Inferred automatically — no `as const`, no explicit type arguments
const status = ckType.enum8({ active: 1, paused: 2, banned: 3 });
//    ↑ Enum8<"active" | "paused" | "banned">

// Re-brand on top of the inferred keys
type UserStatus = "active" | "paused" | "banned";
const userStatus = ckType.enum8({ active: 1, paused: 2, banned: 3 }).$type<UserStatus>();
```

> **Salvedad:** `$type<T>()` no valida los valores en tiempo de ejecución. Si una fila de la base de datos contiene un valor que no pertenece al tipo declarado (p. ej., datos obsoletos de antes de una migración de esquema), TypeScript seguirá acotando el tipo como si fuera válido, y los `switch` exhaustivos omitirán ese caso sin avisar. Usa `mapWith` en una proyección si necesitas validar el valor al leerlo; para validarlo en cada decodificación y codificación, usa `$validator()` más abajo.

### Separar los tipos de inserción y selección con `$type<{ select, insert }>()`

A veces, las columnas de ClickHouse aceptan al insertar un tipo más amplio que el que devuelven al seleccionar. Por ejemplo, una columna `DateTime` acepta `string` y `Date` al insertar, pero siempre devuelve `Date` tras decodificarse. Pasa un objeto a `$type` para distinguir ambos tipos:

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

`$type<{ select: T }>()` (sin `insert`) equivale a la forma con un único genérico `$type<T>()`.

### Reglas del modelo de inserción: DEFAULT, MATERIALIZED, ALIAS

`$inferInsert` se obtiene de la forma de las columnas del esquema; no es una simple copia de `$inferSelect`:

| Modificador de DDL | Efecto en el modelo de inserción |
| --- | --- |
| `.default(expr)` | La columna se vuelve opcional: si la omites, ClickHouse rellena el valor |
| `.materialized(expr)` | La columna se elimina por completo del modelo de inserción |
| `.aliasExpr(expr)` | La columna se elimina por completo del modelo de inserción |

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

### Validación en tiempo de ejecución con `$validator(schema)`

Usa `$validator(schema)` cuando `$type` no sea suficiente. Puede que la base de datos contenga datos obsoletos, que aceptes formas JSON procedentes de fuentes no fiables o que quieras que la API de inserción convierta una cadena en una `Date`. Cualquier objeto que cumpla la [especificación de Standard Schema v1](https://github.com/standard-schema/standard-schema) es válido: Zod 3.23+, Valibot 1+, ArkType, Effect Schema o TypeBox. ck-orm no depende de ninguna de estas bibliotecas.

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

Las transformaciones del esquema (p. ej., `z.string().transform((v) => new Date(v))`) se respetan: la columna acepta el tipo de *entrada* del esquema al insertar y expone el tipo de *salida* al seleccionar. `$validator` solo funciona de forma síncrona: los esquemas asíncronos lanzan un error durante la decodificación o la codificación.

`$type` frente a `$validator`:

| | `$type<T>()` | `$validator(schema)` |
| --- | --- | --- |
| Coste en tiempo de ejecución | Cero | Una validación síncrona del esquema por cada fila y cada columna |
| Seguridad de tipos | Aserción en tiempo de compilación | Inferida a partir del esquema |
| Validación en tiempo de ejecución | No | Sí: lanza un error si el valor no coincide con el esquema |
| Tipos de entrada ≠ tipos de salida | Con `$type<{ select, insert }>()` | Con transformaciones del esquema |
| Dependencia externa | Ninguna | Ninguna (tú eliges la biblioteca de esquemas) |

### Tipo de columna JSON

ck-orm está pensado para el **nuevo tipo de datos JSON** de ClickHouse 24.x+
([docs](https://clickhouse.com/docs/sql-reference/data-types/newjson)): el que
divide físicamente un objeto JSON en subcolumnas tipadas, en vez de guardar el
valor como una cadena opaca. Dos reglas básicas que conviene tener claras desde
el principio:

- El tipo de datos `JSON` de ClickHouse acepta **solo objetos de nivel superior**.
  Rechaza las matrices, las cadenas y los números de nivel superior. ck-orm
  aplica la misma regla en tiempo de compilación mediante
  `T extends Record<string, unknown>` y en tiempo de ejecución con una
  comprobación O(1) de bajo coste al insertar y seleccionar.
- Las rutas tipadas (`a.b UInt32`) se convierten en subcolumnas reales dentro de
  ClickHouse; ck-orm usa `mapFromDriverValue` / `mapToDriverValue` de la fábrica
  de columnas correspondiente para que una ruta declarada como `ckType.uint64()`
  se decodifique como una `string` sin pérdida, igual que la columna `uint64` de
  nivel superior.

La forma más sencilla sigue el mismo patrón que el resto de fábricas de columnas
de ck-orm:

```ts
import { ckTable, ckType } from "ck-orm";

const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json("payload"),
});
// DDL:  `payload` JSON
// $inferSelect.payload: { [key: string]: unknown }
```

Pasa un tipo genérico para que `$inferSelect` / `$inferInsert` reflejen la forma
real:

```ts
type EventPayload = { user_id: number; event: string; meta?: { ip: string } };

const userEvents = ckTable("user_events", {
  id: ckType.uint64(),
  payload: ckType.json<EventPayload>("payload"),
});
```

`ckType.json<T>().$type<T2>()` es el equivalente encadenable si prefieres
mantener el tipo genérico junto a las demás llamadas al constructor de columnas.

#### DDL parametrizado

El segundo argumento de la fábrica acepta las cuatro opciones de DDL de NewJSON:

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

Las claves de `typeHints` se comprueban contra `Paths<T>`: un error tipográfico
como `{ "user.idd": ... }` provoca un error de tipos en tiempo de compilación.
El DDL generado siempre se emite en un orden lexicográfico estable para que las
herramientas externas de comparación de esquemas vean una salida idéntica byte a
byte, independientemente de cómo se haya escrito la configuración.

#### typeHints afectan al decodificador en tiempo de ejecución

La política de ClickHouse para valores de 64 bits sin pérdida hace que `UInt64`
se decodifique como una cadena en ck-orm. Si declaras `user_id` tanto en el tipo
de TypeScript *como* en una entrada de `typeHints`, mantén sincronizadas ambas
declaraciones: para `uint64`, eso significa escribir `user_id: string` en el tipo
genérico. Usa `$type<{ select, insert }>()` cuando la entrada admitida al insertar sea distinta
del tipo decodificado al seleccionar:

```ts
ckType.json("payload", { typeHints: { user_id: ckType.uint64() } })
  .$type<{
    select: { user_id: string };
    insert: { user_id: string | number | bigint };
  }>();
```

#### Acceso a rutas

Cada columna JSON lleva cinco métodos de ruta encadenables que reproducen
literalmente la sintaxis de acceso a rutas de ClickHouse:

| Método | SQL | Significado |
| --- | --- | --- |
| `col.path("a.b")` | `col.a.b` | Lee una ruta; el tipo estático es `PathValue<T, "a.b">` (consulta la nota sobre `typeHints` más abajo) |
| `col.castPath("a.b", ckType.uint64())` | `col.a.b.:UInt64` | Fuerza la decodificación de una ruta usando otra fábrica de columnas de ck-orm; el tipo estático procede del `TData` de la columna de conversión |
| `col.subobject("a")` | `col.^a` | Lee un objeto anidado como valor `JSON` |
| `col.merged("a")` | `col.@a` | Lee la vista combinada de los datos compartidos |
| `col.arrayPath("a")` | `col.a[]` | Expande una ruta cuyo tipo es una matriz |

> `col.path(P)` siempre tiene el tipo `PathValue<T, P>`: se calcula a partir del
> genérico `T`, no de `typeHints`. `typeHints` solo cambia el decodificador en
> tiempo de ejecución; tienes que mantener sincronizados el tipo estático y el
> typeHint (la sección anterior explica la política `uint64 → string` sin
> pérdida). Si no coinciden, usa en su lugar `castPath(P, ckType.X())`: su tipo
> de retorno lo determina la columna de conversión.

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

`Paths<T>` valida el literal de ruta en tiempo de compilación:
`payload.path("a.zzz")` no pasa la comprobación de tipos si `"a.zzz"` no forma
parte de `T`.

Los métodos de ruta se vuelven a asociar en tiempo de ejecución con cada
modificador del encadenamiento (`$type`, `$validator`, `.default()`,
`.materialized()`, `.bind()`, …), para que la llamada subyacente siempre
funcione. **El tipo estático mantiene esos métodos a través de todos los
eslabones de la cadena, salvo `$validator`**: el contrato de `$validator` es genérico sobre
`StandardSchemaV1<unknown, unknown>` y no se puede acotar de forma segura a
`JsonShape` sin infringir las reglas de subtipos de estilo LSP que impone
TypeScript, por lo que el tipo estático vuelve a `Column<T>` después de ese
único eslabón. Puedes seguir llamando al método; si quieres encadenarlo más,
basta con usar una aserción de tipos:

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

#### `fn.json*` — llamadas con rutas dinámicas

Cuando la cadena de la ruta se construye en tiempo de ejecución (o quieres leer
una ruta que no está declarada en `T`), usa el espacio de nombres `fn.*`:

```ts
import { fn } from "ck-orm";

const userPath = userInput as string;
await db.select({
  raw:    fn.jsonPath<string>(userEvents.payload, userPath),
  casted: fn.jsonCast(userEvents.payload, userPath, ckType.string()),
  shape:  fn.dynamicType(fn.jsonPath(userEvents.payload, userPath)),
});
```

`fn.jsonCast` es el equivalente dinámico de `castPath`; `fn.jsonSubobject`,
`fn.jsonMerged` y `fn.jsonArray` hacen lo mismo que sus métodos de columna
equivalentes; `fn.dynamicType` expone la función `dynamicType()` de ClickHouse
para inspeccionar el tipo en tiempo de ejecución de una ruta de tipo Dynamic.

#### Validación en tiempo de ejecución

Cada fila pasa por dos capas, **en este orden**:

1. **La protección JSON integrada en ck-orm**: `null` / `undefined` pasan sin
   cambios para que las columnas JSON `nullable()` funcionen con normalidad;
   cualquier otra entrada que no sea un objeto (matriz, cadena, número o
   booleano) se rechaza con un error `client_validation` / `decode` asociado a
   la columna, como `JSON column expects a plain object, got array`. Las rutas
   tipadas de `typeHints` pasan por el codificador y el decodificador de la
   fábrica de columnas correspondiente, tanto al entrar como al salir. Está
   siempre activa.
2. **`$validator(schema)` opcional**: si está presente, se ejecuta *antes* del
   codificador integrado al insertar y *después* del decodificador integrado al
   seleccionar. Úsalo para validar la forma:

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

ck-orm **no** inspecciona recursivamente el valor JSON para comprobar si `T["a"]`
es realmente un `number`, etc. Usa `$validator` si necesitas eso: la función de
la protección JSON solo sirve para detectar los casos en que sería difícil
asociar el propio mensaje de error de ClickHouse con una columna. Los argumentos
escalares de nivel superior fallan de forma explícita y el error queda asociado
a la columna:

```ts
await db.insert(events).values({ payload: ["wrong"] as never });
// throws: JSON column expects a plain object, got array
```

#### Valores predeterminados al insertar

Las columnas JSON siguen las mismas reglas de `$inferInsert` que el resto de
columnas. `.default()` vuelve opcional el campo; `.materialized()` /
`.aliasExpr()` lo eliminan por completo del modelo de inserción:

```ts
const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json<{ note: string }>("payload").default(ckSql`'{}'`),
});

await db.insert(auditLogs).values({ id: "1" }); // payload filled by CH DEFAULT
```

### `ckAlias()`

Usa `ckAlias()` si una consulta necesita la misma tabla más de una vez.

```ts
import { fn, ck, ckAlias } from "ck-orm";

const telemetry = ckAlias(probeTelemetry, "telemetry");
```

Las columnas que devuelve `ckAlias()` quedan vinculadas automáticamente al alias.

### Nombres de columna

La clave del objeto de esquema es la clave lógica que usan las filas de TypeScript, los resultados decodificados de las consultas y los valores que insertas. Por defecto, esa misma clave también es el nombre de la columna en ClickHouse:

```ts
const telemetry = ckTable("telemetry", {
  signalStrength: ckType.decimal({ precision: 20, scale: 5 }),
});
```

Si la columna de la base de datos tiene otro nombre, pasa ese nombre físico como primer argumento:

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

Los nombres físicos se usan en SQL y DDL, así como en los filtros, la ordenación, la agrupación y las listas de columnas de escritura (`probe_id`, `signal_strength`, `created_at`). Los modelos inferidos, los resultados predeterminados de las consultas, las claves de proyección explícitas y los valores que insertas usan las claves del objeto de esquema (`probeId`, `signalStrength`, `createdAt`).

Todos los constructores públicos de `ckType` admiten un nombre físico para la columna exterior. Los constructores sin configuración adicional aceptan `name?`; los que tienen configuración de tipo reciben primero el nombre físico opcional y después un objeto con la configuración del tipo:

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

`aggregateFunction` y `simpleAggregateFunction` siguen una forma de llamada propia de ClickHouse: la primera cadena es el nombre de la función de agregación, no el de la columna:

```ts
ckType.aggregateFunction("sum", ckType.uint64());
ckType.aggregateFunction("quantile(0.5)", ckType.float64());
ckType.simpleAggregateFunction("sum", ckType.uint64());
```

`AggregateFunction` admite nombres de funciones de agregación de ClickHouse con parámetros literales, como `quantile(0.5)` o `topK(10)`. `simpleAggregateFunction` es más estricto y usa un nombre de función de agregación sin parámetros.

Usa la configuración en forma de objeto si también necesitas un nombre físico de columna:

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

`nested("items", shape)` da nombre a la columna exterior `Nested(...)`. Las claves de `shape` son los nombres de los campos anidados; los valores de `configuredName` de las columnas internas no se usan para nombrarlos:

```ts
const sensorPackets = ckTable("sensor_packets", {
  items: ckType.nested("items", {
    componentId: ckType.string(),
    quantity: ckType.float64(),
  }),
});
```

### Constructores de columnas

Usa `ckType.*` para los constructores de columnas del esquema. El DSL de esquemas cubre las familias de tipos habituales de ClickHouse:

- enteros: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`
- coma flotante y decimal: `float32`, `float64`, `bfloat16`, `decimal`
- tipos escalares: `bool`, `string`, `fixedString`, `uuid`, `ipv4`, `ipv6`
- tipos de fecha y hora: `date`, `date32`, `dateTime`, `dateTime64`, `time`, `time64`
- enumeraciones y tipos especiales: `enum8`, `enum16`, `json`, `dynamic`, `qbit`
- contenedores: `nullable`, `array`, `tuple`, `map`, `nested`, `variant`, `lowCardinality`
- tipos de agregación: `aggregateFunction`, `simpleAggregateFunction`
- tipos geométricos: `point`, `ring`, `lineString`, `multiLineString`, `polygon`, `multiPolygon`

`int64` y `uint64` usan `string` de TypeScript por defecto en las lecturas y escrituras basadas en el esquema y en los modelos inferidos, para que los valores de 64 bits se mantengan exactos al pasar entre el formato JSON de transmisión de ClickHouse y los entornos de ejecución de JavaScript. Si quieres usar `bigint`, hazlo con tu propio decodificador, como `mapWith((value) => BigInt(String(value)))`.

ClickHouse no admite `Nullable(Array(...))`, `Nullable(Map(...))` ni `Nullable(Tuple(...))`. `ck-orm` rechaza esas formas al definir el esquema. En su lugar, pon `nullable(...)` dentro del tipo compuesto, por ejemplo `ckType.array(ckType.nullable(ckType.string()))`.

Los constructores con configuración de tipo reciben primero el nombre físico opcional de la columna y después un objeto de configuración:

```ts
ckType.decimal({ precision: 20, scale: 5 });
ckType.decimal("signal_strength", { precision: 20, scale: 5 });
ckType.fixedString({ length: 8 });
ckType.dateTime64("created_at", { precision: 9, timezone: "UTC" });
ckType.qbit("embedding", ckType.float32(), { dimensions: 8 });
```

En las escrituras de `Date` / `Date32` y en los valores de predicado derivados del esquema, una `Date` de JavaScript se envía como la fecha del calendario UTC que le corresponde (`2026-06-15T23:00:00Z` se convierte en `2026-06-15`). Para guardar otro día, pasa una cadena `YYYY-MM-DD`; las cadenas y los números se envían sin cambios.

Los valores de predicado de `DateTime` / `DateTime64` vinculan una `Date` de JavaScript a un instante UTC, así que ninguna zona horaria de la columna, la sesión o el servidor puede desplazarlo. ClickHouse lee una cadena tal como está escrita, usando la zona horaria de la columna. En una posición `DateTime` de 32 bits, si todas las instancias de Date del parámetro representan segundos exactos, el parámetro se vincula como `DateTime('UTC')`; si alguna instancia tiene milisegundos, se vincula como `DateTime64(3, 'UTC')`. Las posiciones `DateTime64` siempre se vinculan como `DateTime64(3, 'UTC')`. Los predicados escalares, como `gte`, siguen comparando el instante exacto. En una columna `DateTime`, `inArray` / `notInArray` necesitan valores de tipo Date que representen segundos exactos. Si uno lleva milisegundos, ClickHouse lo rechaza (`TYPE_MISMATCH`); es el comportamiento nativo de ClickHouse y, además, ese valor nunca podría coincidir. `insert().values()` envía cada `Date` como un número entero de unidades de tiempo con la precisión de la columna. Consulta «Escrituras».

### Guía práctica de tipos de columna

Formas habituales del esquema y sus valores de TypeScript:

| Forma de ClickHouse | Esquema | Forma del valor en TypeScript |
| --- | --- | --- |
| enumeración | `ckType.enum8({ idle: 1, active: 2 })` | `"idle" | "active"` (inferido de las claves) |
| cadena de baja cardinalidad | `ckType.lowCardinality(ckType.string())` | `string` |
| decimal que admite NULL | `ckType.nullable(ckType.decimal({ precision: 18, scale: 5 }))` | `string | null` |
| matriz | `ckType.array(ckType.string())` | `string[]` |
| elemento de matriz que admite NULL | `ckType.array(ckType.nullable(ckType.string()))` | `(string | null)[]` |
| tupla | `ckType.tuple(ckType.string(), ckType.int32())` | `[string, number]` |
| mapa | `ckType.map(ckType.string(), ckType.string())` | `Record<string, string>` |
| matriz de objetos anidados | `ckType.nested({ component: ckType.string(), quantity: ckType.float64() })` | `{ component: string; quantity: number }[]` |
| variante | `ckType.variant(ckType.string(), ckType.int32())` | `string | number` |
| JSON | `ckType.json<{ risk?: { score?: number } }>()` | `{ risk?: { score?: number } }` |

Actualmente, `ckType.map(...)` solo admite claves `String` y representa el mapa como un registro de JavaScript, por lo que no modela el caso límite de `Map(K, V)` con claves duplicadas de ClickHouse.

Las filas que insertas tienen la misma forma inferida que `typeof table.$inferInsert`. Al llamar a `insert(table).values(...)`, puedes omitir las columnas con valores predeterminados de ClickHouse o con expresiones generadas. Las columnas `MATERIALIZED` y `ALIAS` nunca se escriben en la lista de columnas del `INSERT` generado; si las pasas explícitamente, se rechaza la operación.

## Configuración del cliente
<!-- src:0d752985 -->

Crea un cliente con `clickhouseClient()`:

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:<password>@127.0.0.1:8123/telemetry_lab",
});
```

### Modos de conexión

`clickhouseClient()` admite dos formas de conexión que se excluyen mutuamente.

#### `databaseUrl`

Usa `databaseUrl` si quieres una sola cadena de conexión:

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:secret@127.0.0.1:8123/telemetry_lab",
});
```

Si indicas `databaseUrl`, no pases también:

- `host`
- `database`
- `username`
- `password`
- `pathname`

#### Campos de conexión estructurados

Usa campos explícitos si quieres configurar cada parte por separado:

```ts
const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  username: "default",
  password: "<password>",
});
```

Valores predeterminados del modo estructurado:

- `host`: `http://localhost:8123`
- `database`: `default`
- `username`: `default`
- `password`: `""`

### Campos comunes

La mayoría de proyectos solo necesitan unos pocos campos del cliente:

| Campo | Propósito |
| --- | --- |
| `request_timeout` | Tiempo de espera opcional por solicitud, en milisegundos, que mide el tiempo total transcurrido. Si lo omites, ck-orm no aplica ningún tiempo de espera en el cliente: la duración de la solicitud la controlan el fetch subyacente, la plataforma, el servidor o `abort_signal`. Para consultas en flujo de larga duración, déjalo sin configurar y aplica el límite de tiempo mediante `abort_signal` (p. ej., `AbortSignal.timeout(30_000)`). |
| `clickhouse_settings` | Ajustes predeterminados de sesión y de consulta de ClickHouse |
| `application` | Define el nombre de la aplicación de ClickHouse |

Las definiciones de tablas no dependen de la configuración de conexión. Define tablas con `ckTable(...)`, importa esos objetos de tabla al construir consultas y pásalos directamente a `.from(...)`, `.insert(...)` y a las funciones auxiliares para tablas temporales.

### Campos avanzados

Usa estos campos para controlar el comportamiento avanzado en tiempo de ejecución:

| Campo | Propósito |
| --- | --- |
| `fetch` | La función `(request: Request) => Response \| Promise<Response>` se llama para cada solicitud HTTP; puede ser cualquier implementación de fetch, un manejador de servidor o un doble de prueba como el de [`examples/decimal-sum/main.test.ts`](../examples/decimal-sum/main.test.ts). Por defecto, usa el `fetch` de la plataforma |
| `http_headers` | Cabeceras predeterminadas adicionales |
| `role` | Rol o roles predeterminados de ClickHouse |
| `session_id` | Identificador de sesión predeterminado |
| `session_max_concurrent_requests` | Máximo de solicitudes en curso permitido por cada `session_id` en este cliente (valor predeterminado: `1`) |
| `compression.response` | Con el valor predeterminado `false`, las solicitudes en flujo envían `enable_http_compression = 0` para que las filas lleguen a medida que ClickHouse las produce. Las solicitudes que devuelven el resultado de una vez mantienen el valor predeterminado del servidor (ClickHouse 26.3+ comprime una respuesta cuando el cliente la acepta). `true` comprime todas las respuestas, lo que puede retener un flujo con pocos datos hasta que se llene un bloque de compresión |
| `logger` / `logLevel` | Integración con el logger |
| `tracing` | Integración de OpenTelemetry para trazado |
| `instrumentation` | hooks personalizados para el ciclo de vida de las consultas |

Los controles de duración de la sesión se aplican deliberadamente por solicitud. Pasa `session_timeout` a una consulta concreta o a `runInSession(...)`. Usa `session_check` cuando continúes una sesión existente identificada por `session_id`, no para iniciar una sesión nueva desde cero.
La diferencia de `session_max_concurrent_requests` es que se trata de un límite del cliente que regula las solicitudes que se solapan y apuntan al mismo `session_id`.
El servidor sigue bloqueando las sesiones reales de ClickHouse, así que aumentar `session_max_concurrent_requests` por encima de `1` puede hacer que aparezca `SESSION_IS_LOCKED` en lugar de darte paralelismo real dentro de la misma sesión.
Mantén `1` como valor predeterminado salvo que quieras eliminar la serialización local a propósito y estés preparado para gestionar los errores de bloqueo de sesión del servidor.

### Ajustes de ClickHouse

`clickhouse_settings` solo sirve para configurar los ajustes de sesión y de consulta de ClickHouse: son las mismas claves que documenta [Configuración de sesión](https://clickhouse.com/docs/operations/settings/settings) de ClickHouse y que la API HTTP acepta como parámetros de consulta. Es independiente de la configuración del cliente de ck-orm, como `host`, `database`, `request_timeout`, `http_headers` y `session_max_concurrent_requests`.

Deja fuera de `clickhouse_settings` los campos de transporte HTTP como `query`, `database`, `session_id`, `role` y `param_*`; ck-orm valida esas claves por separado porque pertenecen a la envoltura de la solicitud y al canal de parámetros con nombre.

Las claves de configuración oficiales tienen autocompletado en TypeScript, y las claves arbitrarias siguen siendo válidas para versiones más recientes de ClickHouse o ajustes propios de un despliegue:

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

El transporte fetch analiza y serializa JSON. La configuración pública del cliente mantiene ese comportamiento igual en todos los entornos de ejecución, en vez de exponer un ajuste `json`.

ck-orm fija en cada solicitud este pequeño conjunto de ajustes del contrato de transmisión: `http_write_exception_in_output_format = 0`, `output_format_json_quote_64bit_integers = 1`, `output_format_json_quote_decimals = 1` y `date_time_output_format = "iso"`. Si los ajustes que pasas entran en conflicto con esos valores, ck-orm registra una advertencia una sola vez y envía el valor forzado; los ajustes de carga de trabajo y rendimiento, como `max_threads`, siguen bajo tu control.

### Autenticación

`ck-orm` usa autenticación básica para las conexiones a la base de datos.

- en el modo `databaseUrl`, puedes incluir las credenciales en la URL
- en modo estructurado, usa `username` y `password`
- si no pasas credenciales, el valor predeterminado es `default` con una contraseña vacía

## Constructor de consultas
<!-- src:e5211074 -->

Los fragmentos siguientes dan por hecho que has importado `db`, `probeTelemetry` y las funciones auxiliares indicadas desde `ck-orm`.

### `select()`

Con una selección explícita, el resultado tiene la forma que has definido:

```ts
const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
    signalStrength: probeTelemetry.signalStrength,
  })
  .from(probeTelemetry)
  .limit(10);
```

Los objetos de proyección se construyen a partir de valores públicos de `Selection` o de columnas. En la práctica, dentro de `select({ ... })` puedes combinar de la misma forma las columnas de las tablas, las salidas de `fn.*(...)` y las de `ck.expr(...)`.

Si no hay uniones, la selección implícita devuelve el modelo completo de la tabla:

```ts
const rows = await db.select().from(probeTelemetry).limit(10);
```

Cuando hay uniones, la selección implícita agrupa los campos por origen y devuelve objetos anidados. Si un grupo de una unión izquierda no encuentra coincidencia, se convierte en `null`. ClickHouse devuelve `[]` / `{}` en lugar de NULL para las columnas `Array` / `Map`, así que, para esta comprobación, los contenedores vacíos cuentan como ausentes. Una fila coincidente cuyos campos escalares sean todos NULL y cuyas matrices y mapas estén vacíos no se puede distinguir de una fila sin coincidencia, por lo que también se convierte en `null`.

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

### `where()` y funciones auxiliares para condiciones

Funciones auxiliares públicas para construir condiciones:

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

`.where(...predicates)` acepta cualquier número de predicados y los combina con `AND`. Ignora los predicados `undefined`, así que puedes pasar varios directamente o agruparlos con `ck.and(...)` y `ck.or(...)`.

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

Una lista de valores se envía como un único parámetro de consulta `Array(T)` (`col in {p:Array(T)}`). Las listas cuyo texto supera unos 40 000 caracteres se dividen en varios parámetros unidos con `arrayConcat(...)`. La poda mediante la clave primaria y los índices que permiten omitir bloques no se ve afectada. ClickHouse 26.3+ acepta como máximo 1000 parámetros de consulta por solicitud HTTP, y el analizador de 25.8 rechaza una lista combinada que supere aproximadamente 1 MB; para conjuntos mayores, usa una tabla temporal o datos externos. Una lista que contenga expresiones SQL o valores sin tipar de tipos distintos sigue usando un parámetro por elemento.

`ck.and(...)` omite `undefined`, así que puedes montar filtros dinámicos directamente:

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

Solo se omite `undefined` cuando aparece como predicado. Los valores de los predicados son estrictos: `ck.eq(column, undefined)` lanza un error en vez de ampliar la consulta en silencio. Los literales `null`, `false`, `true`, `0` y `""` no son predicados; usa una expresión SQL. `null` sigue siendo un valor de datos válido para las columnas que admiten NULL y para las escrituras, pero el filtrado por NULL debe ser explícito:

```ts
db.select()
  .from(probeTelemetry)
  .where(ck.isNull(probeTelemetry.deletedAt));
```

Las columnas booleanas también son valores y expresiones SQL normales:

```ts
const healthChecks = ckTable("health_checks", {
  id: ckType.int32(),
  isPassing: ckType.bool("is_passing"),
});

db.select().from(healthChecks).where(ck.eq(healthChecks.isPassing, false));
db.select().from(healthChecks).where(healthChecks.isPassing);
```

No pongas `null` a secas dentro de comparaciones normales ni listas `IN`. Compón la lógica de NULL de forma explícita:

```ts
ck.or(
  ck.inArray(probeTelemetry.deletedAt, [new Date("2026-04-21T00:00:00.000Z")]),
  ck.isNull(probeTelemetry.deletedAt),
);
```

Si construyes filtros grandes en tiempo de ejecución, usa `Predicate[]` con la variante variádica de `.where(...predicates)`:

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

`Predicate` es el nombre público de las cláusulas SQL booleanas reutilizables. Puedes usar los mismos objetos de predicado en `where`, `having`, las cláusulas `on` de las uniones y las funciones auxiliares que trabajan con booleanos, como `ck.exists(...)`.

`Selection` es el nombre público de los valores calculados reutilizables del constructor, como `fn.sum(...)`, `fn.toString(...)` y `ck.expr(ckSql...)`. Usa `.as(...)` para darles un alias y `.mapWith(...)` para sobrescribir la decodificación. `Order` es el objeto de cláusula que devuelven `ck.asc(...)` y `ck.desc(...)`.

`ck.has(...)`, `ck.hasAll(...)` y `ck.hasAny(...)` corresponden directamente a las funciones nativas de ClickHouse y mantienen la semántica de matrices, mapas y JSON de ClickHouse. Cuando el lado izquierdo es una columna de matriz, los valores normales de JavaScript se codifican primero usando esa columna; así, los filtros `Array(Date)` usan la codificación del tipo Date de la columna antes de que los valores se conviertan en parámetros de consulta.

`where(...)` acepta un número variable de predicados, mientras que `having(...)` acepta uno solo. Si `having` tiene varias cláusulas, compón primero el predicado con `ck.and(...)` o `ck.or(...)`.

`ck.contains(...)`, `ck.startsWith(...)`, `ck.endsWith(...)` y sus variantes `*IgnoreCase` tratan la entrada como texto literal. Parametrizan el valor y escapan internamente los caracteres comodín de LIKE (`%`, `_`, `\`).

Usa `ck.like(...)` / `ck.ilike(...)` solo cuando quieras que se aplique toda la semántica de patrones. Estas API siguen parametrizando los valores para que SQL siga siendo seguro, pero `%` y `_` conservan su significado de comodín porque LIKE es un lenguaje de patrones.

Búsqueda de texto literal:

```ts
import { ck } from "ck-orm";

const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
  })
  .from(probeTelemetry)
  .where(ck.contains(probeTelemetry.probeId, "probe_alpha%"));
```

Ejemplo de patrón avanzado:

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

`groupBy()` y `limitBy([...])` aceptan columnas y valores `Selection` calculados a partir de funciones auxiliares como `fn.*(...)` o `ck.expr(...)`.

Los valores primitivos de `limit()`, `offset()` y `limitBy(..., limit)` deben ser enteros seguros y no negativos. Usa `ckSql` solo si necesitas expresamente una expresión constante de ClickHouse.

`orderBy()` acepta:

- `ck.desc(selection)`
- `ck.asc(selection)`
- una columna directamente

### `final()`

Añade `FINAL` a la consulta de una tabla:

```ts
const query = db.select().from(probeTelemetry).final();
```

En una lectura sencilla de una tabla sin alias, `ck-orm` genera `FROM table FINAL`. Si la tabla raíz tiene alias o la consulta une fuentes adicionales, `ck-orm` envuelve la tabla finalizada en una subconsulta y conserva el alias en la fuente externa. Así evita los casos límite del analizador de ClickHouse con `FINAL`, los alias de tabla, las uniones y las expresiones lambda, sin cambiar la API del constructor.

Solo puedes aplicar `final()` a la fuente raíz de una tabla. Si necesitas `FINAL` dentro de un CTE, una subconsulta o una función de tabla, aplica `.final()` a la consulta basada en tabla antes de llamar a `.as(...)`.

### `limitBy()`

Usa el `LIMIT ... BY ...` de ClickHouse:

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

### Modos de ejecución

Hay tres formas de ejecutar las consultas del constructor:

```ts
const query = db.select().from(probeTelemetry).limit(10);

const rows = await query;
const sameRows = await query.execute();

for await (const row of query.iterator()) {
  console.log(row);
}
```

Usa `.execute()` en los ejemplos de aplicación cuando quieras dejar claro dónde se ejecuta la consulta. El `await query` directo también funciona, como en Drizzle, y resulta útil cuando el equipo ya está acostumbrado a que las consultas del constructor sean thenable.

`query.iterator()` usa las mismas reglas de concurrencia asociadas a la sesión que `db.stream()`: si la consulta usa un `session_id`, el cupo de concurrencia sigue ocupado hasta que termina la iteración o se cierra el iterador antes de tiempo.

### Subconsultas y CTE

Usa `.as("alias")` para convertir un constructor en una subconsulta con un alias explícito:

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

`.as("…")` es **opcional** en las subconsultas: si no lo llamas, el framework asigna un alias automático (`__sub_1`, `__sub_2`, …) al compilar. Puedes usar directamente un constructor sin alias en `from()`, `innerJoin()`, `leftJoin()`, `inArray()`, `notInArray()`, `exists()` y `notExists()`.

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

#### Guía de estilo: cuándo usar `.as(name)` y cuándo dejarlo sin alias

| Situación | Forma recomendada |
|---|---|
| Subconsulta en línea de un solo uso, sin necesidad de reutilizarla | constructor sin alias con una función de retorno de llamada en `innerJoin/leftJoin`, o constructor sin alias en `inArray/exists` |
| Referencias de columnas de la subconsulta usadas en varios sitios (SELECT / WHERE / una cláusula JOIN) | guárdala en una `const` y pásala sin alias: las referencias pasan por los métodos encadenados |
| Autounión (misma subconsulta lógica, dos alias SQL) | llama explícitamente a `.as("a")` y `.as("b")`: si reutilizas la misma instancia dos veces como fuente, la compilación de la consulta en SQL lanza un error |
| Necesitas un alias estable, legible o tipado (búsqueda de `query_log` en producción, `EXPLAIN`, registros de consultas lentas, funciones auxiliares entre módulos o una clave de origen `TAlias` literal) | usa `.as("descriptive_name")` |

El alias automático **no** se mantiene entre compilaciones de consultas con estructuras distintas, pero sí dentro de una misma compilación y al volver a compilar el mismo árbol del constructor; las pruebas al estilo `snapshot` no necesitan normalización.

#### Conflictos entre claves de selección y métodos

Las claves de selección cuyo nombre coincide con un método de SelectBuilder (`from`, `where`, `select`, `as`, `innerJoin`, `leftJoin`, `groupBy`, `having`, `orderBy`, `limit`, `offset`, `final`, `limitBy`, `execute`, `iterator`, `then`, `catch`, `finally`, `buildSelectionItems`) conservan el tipo del método en el constructor sin alias. `sub.from` siempre devuelve el método del constructor, nunca una referencia de columna, aunque `from` esté en la selección. Cambia el nombre de esas claves (p. ej., `from` → `fromAddress`) o envuelve la subconsulta con `.as("name")` para acceder a las columnas a través del objeto `Subquery` correspondiente.

Usa `$with()` y `with()` para trabajar con CTE:

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

`$with()` sin nombre crea un CTE anónimo. Puedes usar un CTE anónimo como fuente una vez por consulta; para unir el mismo resultado dos veces, define dos CTE, uno para cada unión (dos llamadas a `db.$with().as(...)`, o dos CTE con nombres distintos).

### `db.count()`

Usa `db.count(source, ...predicates)` como función auxiliar de recuento al estilo de Drizzle. Tiene la misma semántica de predicados que `.where(...predicates)`: combina varios predicados con `AND` y omite los objetos de predicado que sean `undefined`. Pasar `undefined` como valor a funciones auxiliares como `ck.eq(...)` sigue lanzando un error.

```ts
import { ck } from "ck-orm";

const total = await db.count(
  probeTelemetry,
  ck.eq(probeTelemetry.status, 1),
  ck.gt(probeTelemetry.id, 1000),
);
```

Para conjuntos de resultados más complejos, usa una subconsulta o un CTE como fuente del recuento:

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

De forma predeterminada, `db.count(...)` usa el modo de recuento numérico: genera `toFloat64(count())` y lo decodifica como `number`. Los recuentos muy grandes pueden superar la precisión de los enteros de JavaScript, así que usa los modos encadenables cuando necesites exactitud o conservar el formato de los datos que devuelve el controlador:

```ts
const approximateTotal = await db.count(activeProbes); // number
const exactTotal = await db.count(activeProbes).toSafe(); // string
const wireTotal = await db.count(activeProbes).toMixed(); // number | string
```

`.toSafe()` genera `toString(count())` y sirve para lecturas exactas. Si usas un recuento seguro como expresión SQL, tiene semántica `String`; usa el modo predeterminado, `.toUnsafe()` o `.toMixed()` para comparaciones SQL numéricas. `.toMixed()` genera `toUInt64(count())` y conserva el formato de la respuesta que devuelve el controlador; con los ajustes JSON predeterminados de ck-orm, que permiten conservar los enteros de 64 bits sin perder precisión, las respuestas de ClickHouse suelen llegar como `string`.

## Semántica de NULL en las uniones
<!-- src:5554058b -->

`leftJoin()` usa de forma predeterminada la semántica de NULL de SQL porque aplica automáticamente `join_use_nulls = 1`.

Eso significa:

- se infiere que el lado derecho de una unión izquierda predeterminada admite NULL
- si desactivas `join_use_nulls` de forma explícita, los tipos inferidos también cambian

Para igualar el comportamiento predeterminado de las uniones de ClickHouse, pasa el literal `0`. El literal se infiere, así que no hace falta `as const`. Un valor tipado como `number` a secas, por ejemplo el de una variable, todavía produce `| null`.

```ts
const rawDefaultDb = db.withSettings({
  join_use_nulls: 0,
});
```

Cuando haces una unión izquierda con un constructor sin alias (una subconsulta sin `.as()`) o con un CTE anónimo, todas las referencias de columna de esas fuentes reciben el tipo `| null`, incluidas las del lado `FROM` y las de un constructor sin alias que participa en una unión interna. Esas referencias comparten una sola clave de origen de tipo `string`, demasiado general para que el sistema de tipos distinga qué fuente se unió por la izquierda. En tiempo de ejecución, NULL solo se devuelve para el lado unido por la izquierda.

```ts
const rows = await db
  .select({ uid: allUsers.uid, spent: totals.spent })
  .from(allUsers)
  .leftJoin(totals, ck.eq(allUsers.uid, totals.uid));
// uid and spent are both number | null
```

Asigna un nombre con `.as("name")` a la subconsulta del lado `FROM` o a la que participa en una unión interna. Así, sus referencias conservan su propia clave de origen y no admiten NULL, mientras que el lado unido por la izquierda sigue siendo `| null`.

El ajuste forzado `join_use_nulls = 1` se mantiene cuando reutilizas una consulta con uniones como subconsulta, CTE o fuente de `ck.exists(...)` o `ck.inArray(...)`, por lo que los tipos del constructor siguen alineados con el comportamiento en tiempo de ejecución.

En tiempo de ejecución, las expresiones calculadas siguen la propagación de NULL de ClickHouse: cuando ClickHouse devuelve NULL (por una entrada NULL o por una fila de una unión izquierda sin coincidencia), el valor es `null`. Su tipo en TypeScript es el `TData` que declaras (o el tipo fijo de la función auxiliar) y no se amplía automáticamente, así que declara `| null` (por ejemplo, `fn.plus<number | null>(...)`) o envuelve la entrada con `fn.coalesce(...)` cuando una entrada puede ser NULL.

## Escrituras
<!-- src:d7555d6e -->

### `insert(table).values(...)`

Usa el constructor si quieres hacer inserciones tipadas que sigan el esquema de la tabla:

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

Las filas se envían en una sola solicitud `INSERT INTO t (cols) FORMAT Values`. La sentencia no tiene parámetros de consulta: las filas son texto literal en formato Values dentro del cuerpo de la solicitud, así que no se aplican los límites HTTP de 1000 parámetros ni de 128 KiB por valor. Si omites una columna, esa fila usa su `DEFAULT`. La evaluación de expresiones está desactivada para esta inserción (`input_format_values_interpret_expressions = 0` y `input_format_values_deduce_templates_of_expressions = 0`), así que cada celda contiene datos. Usa `insert().fromSelect()` para valores calculados. Una `Date` de JavaScript se envía como ese instante exacto, truncado a la precisión de la columna; las columnas `DateTime` conservan los segundos completos. Un número en una columna `DateTime` o `DateTime64` representa segundos Unix. Una cadena se interpreta según la zona horaria de la columna.

Las filas de la inserción deben usar las claves del esquema de la tabla. Las claves desconocidas se rechazan de inmediato.

### `insert(table).fromSelect(select)`

Usa `.fromSelect(...)` para compilar una única sentencia `INSERT INTO target (cols) SELECT ...` y hacer que ClickHouse materialice el SELECT en el servidor. Ninguna fila pasa por el proceso de Node: es ideal para filtrar una tabla de origen grande una sola vez y reutilizar el subconjunto materializado en varias consultas posteriores dentro de `runInSession`.

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

Reglas:

- El SELECT debe proyectar todas las columnas obligatorias de la tabla de destino; las columnas declaradas con `.default(...)` pueden omitirse. Si faltan columnas obligatorias, se rechazan tanto en tiempo de compilación (TypeScript) como en tiempo de ejecución.
- La lista de columnas del INSERT se genera en el orden de las claves de proyección del SELECT, así que desde el código que llama se comporta como `name-aligned`, aunque ClickHouse alinee las columnas por posición. El orden de las claves de proyección no determina qué valor recibe cada columna de destino: el emparejamiento se hace por las propias claves.
- Las claves de proyección desconocidas (que no están en la tabla de destino) y los errores de tipo por columna se detectan como errores de TypeScript antes de enviar la consulta.
- `.values()` y `.fromSelect()` se excluyen mutuamente en una misma cadena de inserción: el tipo del constructor se estrecha, el método que no uses desaparece del autocompletado y la llamada también se rechaza en tiempo de ejecución.
- La solicitud de red compilada es un comando directo de ClickHouse: el evento de consulta lleva `mode: "insert"` y `tableName` queda establecido en la tabla de destino.
- `.fromSelect()` acepta un `SelectBuilder` (`session.select({...}).from(...)`), no un envoltorio de subconsulta. Usa `.as("name")` solo si necesitas el resultado como CTE o subconsulta dentro de otro SELECT, no cuando se lo pases a `.fromSelect()`.

#### Columnas anidadas

Las columnas `ckType.nested(...)` funcionan con `.fromSelect()` en tres modos:

1. **Omite la clave anidada de la proyección.** Es el modo predeterminado y refleja el contrato en tiempo de ejecución de ck-orm: si falta el valor anidado, se codifica como SQL `DEFAULT` y ClickHouse rellena todos sus campos con matrices paralelas vacías. La capa TS trata las columnas anidadas como `optional` en `$inferInsert` para mantener el mismo comportamiento.
2. **Proyecta una referencia directa a una columna anidada**, p. ej. `select({ events: src.events })`. ck-orm envuelve el SELECT interno en una subconsulta y reescribe la proyección externa para acceder a cada campo mediante su ruta con puntos (`__ck_inner.events.name`, `__ck_inner.events.score`, …). La forma anidada de la fuente debe contener todos los campos que espera la columna anidada de destino.
3. **Proyectar una expresión calculada en una columna anidada** se rechaza en tiempo de compilación: una sola expresión SQL no puede repartirse entre los campos de las matrices paralelas. Usa `.values(...)` o `.insertJsonEachRow(...)` para los valores de las filas.

Si quieres que la capa TS exija que siempre se pasen los datos anidados (`Pattern D` — contrato de negocio que no permite matrices anidadas vacías), activa `ckType.nested({...}).requiredOnInsert()`. La cadena devuelve una columna cuya forma `$inferInsert` marca `events` como obligatoria y `.fromSelect()` exige que uses el modo de proyección de la referencia a la columna anidada o la alternativa por filas.

> **Nota sobre el formato JSON de transmisión**: `.values()` envía una columna JSON como texto entrecomillado generado por `JSON.stringify(...)` en el cuerpo Values. ClickHouse analiza ese literal y lo guarda en la columna JSON. `.fromSelect()` se ejecuta por completo en el servidor, así que los valores JSON nunca hacen el recorrido de ida y vuelta por el proceso JS. Ambas rutas guardan exactamente los mismos datos.

### `insertJsonEachRow()`

Usa `insertJsonEachRow()` cuando ya tengas filas en forma de objeto o un flujo asíncrono de filas:

```ts
await db.insertJsonEachRow("tmp_scope", [
  { probe_id: "probe_alpha" },
  { probe_id: "probe_beta" },
]);
```

Admite:

- una cadena con el nombre de la tabla
- un objeto de tabla creado con `ckTable()`
- una matriz normal
- un `AsyncIterable`

Una matriz normal vacía no hace nada en el cliente, pero aun así notifica a los hooks de instrumentación un ciclo de vida de inserción correcto con `rowCount: 0`. ClickHouse controla qué ocurre con los campos desconocidos; pasa ajustes como `input_format_skip_unknown_fields: 1` cuando quieras que el servidor ignore campos JSON adicionales. Las columnas anidadas se envían como matrices aplanadas de `column.field` (ClickHouse `flatten_nested = 1`, el valor predeterminado), así que `{ lines: [{ sku: "a" }] }` funciona tanto con `insert().values()` como con `insertJsonEachRow()`.

Con un esquema de tabla, `insertJsonEachRow()` envía una `Date` de JavaScript en una columna `DateTime`/`DateTime64` como exactamente los mismos ticks enteros que `insert().values()` y deja que el analizador predeterminado de ClickHouse `date_time_input_format = basic` se encargue de las cadenas; por eso ambas rutas interpretan igual las cadenas de fecha y hora. Un número JSON que pases directamente en una columna `DateTime64` se interpreta como ticks con `JSONEachRow` y como segundos Unix con `.values()`.

`insertJsonEachRow("table_name", rows)` con un nombre de tabla a secas no conoce los tipos de columna, así que una `Date` se envía como ISO 8601 con `Z` y ck-orm establece `date_time_input_format = best_effort` para esa solicitud. Lo mismo ocurre con las claves de una fila de una tabla con esquema que no aparezcan en el esquema: ck-orm las pasa sin tocar, así que una `Date` en una de ellas necesita `clickhouse_settings: { date_time_input_format: "best_effort" }` en la llamada. En ClickHouse 25.8, el analizador `best_effort` interpreta mal instantes anteriores a 1970 con fracciones de segundo y rechaza cadenas `Time` como `12:34:56`; las columnas tipadas por el esquema evitan ambos problemas.

## SQL sin formato
<!-- src:f47b381e -->

`ck-orm` incluye su propia API de plantillas SQL. Úsala cuando escribir una consulta con el constructor resulte menos directo que escribir el SQL que necesitas.

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

### `ckSql.join()` y `ckSql.identifier()`

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

### SQL sin formato con `query_params`

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

El transporte de parámetros se elige automáticamente. No tienes que configurar el manejo de multipart para `query_params`.

ck-orm rechaza las claves de `query_params` que empiezan por `orm_param`. Ese prefijo está reservado para los parámetros que genera internamente `` ckSql`...` ``.

El formateador de valores admite valores primitivos, `Date`, `NaN`, `Infinity`, matrices, objetos y valores `Map` para marcadores de posición con tipos de ClickHouse como `{ids:Array(UInt64)}` o `{attrs:Map(String, String)}`. Los valores que pasas mediante `query_params` no tienen un tipo de columna propio, así que una matriz siempre se envía como texto de tipo Array. Los marcadores `Tuple(...)` no se admiten ahí. Un entero de 64 bits dentro de una matriz debe ser un `bigint`; si pasas una cadena para un tipo de elemento numérico, se entrecomilla y se rechaza. Una `Date` se envía como texto de fecha y hora en UTC, y ClickHouse lo interpreta usando la zona horaria del tipo del marcador de posición, así que declárala como `{at:DateTime64(3, 'UTC')}` para evitar que la zona horaria de la sesión o del servidor la desplace; la interpolación de `` ckSql`...${date}` `` se encarga de eso. Usa el tipo de marcador de posición `Identifier` de ClickHouse cuando el parámetro sea el nombre de una tabla o de una columna:

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

Usa `ck.expr()` para envolver un fragmento SQL y reutilizarlo como un `Selection`:

```ts
import { ck, ckSql } from "ck-orm";

const query = db.select({
  constantOne: ck.expr(ckSql`1`).as("constant_one"),
});
```

Cuando el fragmento está tipado, `ck.expr()` conserva ese tipo. Un fragmento sin tipar
permanece deliberadamente en `unknown`; si sabes qué devuelve aunque el esquema no lo defina,
pasa un tipo genérico o un decodificador:

```ts
const status = ck.expr(ckSql<string>`'closed'`); // Selection<string>
const enabled = ck.expr<boolean>(ckSql`1`, {
  decoder: (value) => Number(value) === 1,
});
```

### Formatos para consultas sin formato

Las consultas sin formato que devuelven el resultado de una vez solo admiten salida `JSON`:

```ts
const rows = await db.execute(ckSql`select 1`, {
  format: "JSON",
});
```

Las consultas sin formato que procesan datos en flujo solo admiten salida `JSONEachRow`. Con el valor predeterminado `compression.response: false`, el flujo se envía sin comprimir para que cada fila llegue a medida que ClickHouse la produce:

```ts
for await (const row of db.stream(ckSql`select 1`, {
  format: "JSONEachRow",
})) {
  console.log(row);
}
```

### Precisión de Decimal en expresiones

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

- `sum` / `sumIf` amplían P hasta un mínimo de 38; `min` / `max` conservan la P de la columna. En las columnas Decimal del esquema, `fn.sum()` y `fn.sumIf()` son `Selection<string>`; las columnas de coma flotante son `Selection<number>`. La conversión automática también se aplica a `nullable(decimal(...))` y `lowCardinality(decimal(...))`.
- `maxIf(value, condition)` sigue la misma regla Decimal que `max`: un resultado Decimal es `Selection<string>` con su forma declarada `Decimal(P, S)`.
- `avg` **no** usa conversión automática: ClickHouse calcula `avg(Decimal)` sobre Float64, así que `fn.avg` devuelve `Selection<number>`. Para calcular promedios exactos con Decimal, usa `ckSql.decimal(ckSql\`sum(x) / count(x)\`, P, S)`.
- `fn.divideDecimal(left, right, resultScale?)` se traduce directamente a `divideDecimal` de ClickHouse. Devuelve `Selection<string>` para no perder precisión. Si se indica, `resultScale` debe ser un entero literal de `0` a `76` y se emite en la propia consulta; el constructor no comprueba la versión ni imita la función de ClickHouse.
- `column.cast(P, S)` convierte la columna, no el agregado: usarlo sin más dentro de `GROUP BY` provoca `NOT_AN_AGGREGATE`. Usa `fn.sum(column)` o envuelve el agregado.
- Las inserciones rechazan objetos que no sean cadenas ni números (p. ej., instancias sin convertir de `decimal.js`): pasa `.toFixed(scale)`:

```ts
db.insert(measurementLog).values({ value: new Decimal("1.23").toFixed(5) }); // ✅
db.insert(measurementLog).values({ value: new Decimal("1.23") as never }); // ❌ throws
```

## Expresiones de ventana
<!-- src:6d0b3f76 -->

Envuelve una expresión de función `fn.*` con `fn.over(...)`. La especificación opcional solo acepta selecciones del constructor para `partitionBy` y valores habituales de `ck.asc(...)` / `ck.desc(...)` para `orderBy`:

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

`fn.rowNumber()` devuelve de forma predeterminada un `Selection<number>` no seguro mediante `toFloat64`. Llama a `.toSafe()` antes de `fn.over(...)` para obtener un `Selection<string>` exacto, o a `.toMixed()` para la forma de transporte nativa de `UInt64` (`Selection<number | string>`); `.toUnsafe()` es el valor predeterminado explícito. El constructor no añade deliberadamente una comprobación de enteros seguros de JavaScript: la conversión `toFloat64` de ClickHouse puede redondear valores superiores a `Number.MAX_SAFE_INTEGER`; `.toSafe()` conserva una cadena decimal exacta y `.toMixed()` preserva la forma de resultado nativa del controlador. Los agregados Decimal mantienen su decodificador habitual, que devuelve cadenas, y se compilan como `CAST(sum(...) OVER (...) AS Decimal(...))`, por lo que `OVER` queda dentro del agregado, no fuera de la conversión. Por diseño, `fn.over` no acepta una columna a secas ni un fragmento SQL sin formato, y esta primera superficie de API solo cubre `PARTITION BY` y `ORDER BY`; usa `ckSql` para marcos de ventana, ventanas con nombre u otras cláusulas específicas de ClickHouse.

## Funciones y funciones de tabla
<!-- src:1e16409b -->

### `fn`

Incluye funciones auxiliares genéricas, de conversión y de agregado, además de funciones relacionadas con JSON, tuplas y tablas:

- `fn.call()`
- `fn.withParams()`
- `fn.replaceRegexpAll()` — reemplazo tipado de ClickHouse con RE2; consulta [Sustitución con expresiones regulares](#regex-replacement)
- Las funciones auxiliares de conversión de tipos siguen la lista de la página de conversión de tipos de ClickHouse: `fn.cast()`, `fn.date()`, `fn.accurateCast*()`, `fn.reinterpret*()`, `fn.parseDateTime*()`, `fn.formatDateTime()`, `fn.formatRow*()`, `fn.toString*()`, `fn.toBool()`, `fn.toInt*()`, `fn.toUInt*()`, `fn.toFloat*()`, `fn.toBFloat16*()`, `fn.toDecimal*()`, `fn.toDecimalString()`, `fn.toFixedString()`, `fn.toDate*()`, `fn.toDateTime*()`, `fn.toTime*()`, `fn.toTime64*()`, `fn.toInterval*()`, `fn.toLowCardinality()`, `fn.toNullable()` y `fn.toUUID*()`.
- Las conversiones a enteros de 64 bits o más (`toInt64/128/256*`, `toUInt64/128/256*` y las funciones auxiliares para marcas de tiempo Unix de 64 bits) se decodifican como `string`; las de enteros de menos bits y las de coma flotante, como `number`.
- Los argumentos de ClickHouse que tienen que ser literales se validan y se insertan directamente en la consulta cuando hace falta: la escala Decimal, la precisión de DateTime64/Time64, la longitud de FixedString, las cadenas de tipo de destino y las unidades de intervalo.
- `fn.toStartOfMonth()`
- `fn.toUnixTimestamp()` / `fn.toUnixTimestamp64Second()` / `fn.toUnixTimestamp64Milli()` / `fn.toUnixTimestamp64Micro()` / `fn.toUnixTimestamp64Nano()`
- `fn.fromUnixTimestamp()` / `fn.fromUnixTimestamp64Second()` / `fn.fromUnixTimestamp64Milli()` / `fn.fromUnixTimestamp64Micro()` / `fn.fromUnixTimestamp64Nano()`
- `fn.count()` / `fn.countIf()` — el valor predeterminado es `Selection<number>`, envuelto como `toFloat64(count(...))`. Encadena `.toSafe()` para obtener `Selection<string>` (`toString(count(...))`), `.toMixed()` para `Selection<number | string>` (`toUInt64(count(...))`) o `.toUnsafe()` para volver al valor predeterminado. Funciona igual que `db.count`. Los decodificadores exigen enteros no negativos y rechazan `NaN`, números negativos, booleanos, etc. — consulta [Modos de `fn.count` / `fn.uniqExact`](#fncount--fnuniqexact-modes) para ver ejemplos.
- `fn.sum()` / `fn.sumIf()` / `fn.min()` / `fn.max()` / `fn.maxIf()` — hacen una conversión automática a `Decimal(P, S)` para las columnas Decimal; consulta [Precisión de Decimal en expresiones](#decimal-precision-in-expressions)
- `fn.over()` / `fn.rowNumber()` — composición de funciones de ventana con `PARTITION BY` y `ORDER BY`; consulta [Expresiones de ventana](#window-expressions)
- `fn.divideDecimal()` — división Decimal exacta con una escala de resultado opcional escrita directamente en la consulta; consulta [Precisión de Decimal en expresiones](#decimal-precision-in-expressions)
- `fn.argMax()` / `fn.argMin()` — devuelven el valor `arg` de la fila donde `val` alcanza el máximo o el mínimo. El tipo devuelto sigue el de la columna `arg` (sin envolverlo en un CAST a Decimal; ClickHouse conserva el tipo declarado de la columna).
- `fn.avg()` — `Selection<number>` (Float64), igual que el comportamiento nativo de ClickHouse para `avg(Decimal)`
- `fn.uniqExact()` — los mismos tres modos encadenables que `fn.count()`: el valor predeterminado es `Selection<number>`, envuelto como `toFloat64(uniqExact(...))`; `.toSafe()` da `Selection<string>` (`toString(uniqExact(...))`), `.toMixed()` da `Selection<number | string>` (`toUInt64(uniqExact(...))`) y `.toUnsafe()` vuelve al valor predeterminado. Los decodificadores aplican la misma comprobación de enteros no negativos. Consulta [Modos de `fn.count` / `fn.uniqExact`](#fncount--fnuniqexact-modes).
- `fn.coalesce()`
- `fn.if()` / `fn.multiIf()` — condicionales. Ambos devuelven `Selection<unknown>` porque ClickHouse determina el tipo de resultado del servidor a partir de todas las ramas de valores.
- `fn.greatest()` / `fn.least()` / `fn.nullIf()` — comparaciones; `nullIf` envuelve automáticamente el sqlType como `Nullable(...)` sin envolver dos veces una entrada que ya admite NULL.
- `fn.positionCaseInsensitive()` / `fn.positionCaseInsensitiveUTF8()` — búsqueda de subcadenas sin distinguir mayúsculas de minúsculas y búsqueda de subcadenas en UTF-8. Devuelven `Selection<string>` (ClickHouse `UInt64`, decodificado como cadena para conservar la precisión de 64 bits, igual que `fn.indexOf()`). `startPos` es opcional y debe tener un tipo `UInt*`: pasa una columna UInt o `fn.toUInt64(n)` para literales; un literal numérico suelto se interpreta como `Int64` y ClickHouse lo rechazará.
- `fn.jsonExtract()`
- `fn.tuple()`
- `fn.arrayJoin()`
- `fn.tupleElement()`
- `fn.not()`

### Tipos de las ramas condicionales

El genérico del resultado condicional ya no está disponible: `fn.if<TData>(...)` y
`fn.multiIf<TData>(...)` ya no compilan. Un ancla conocida de tipo `Float64` o `UInt64`
asigna por contexto el tipo a las ramas compatibles con literales de enteros seguros; los
literales `UInt64` también deben ser no negativos. Las
expresiones heterogéneas y el SQL sin formato nunca se convierten
automáticamente, así que convierte explícitamente cada rama de valores.
`.mapWith(...)` solo define el decodificador de JavaScript; no cambia el tipo
SQL. En particular, un valor alternativo de entero seguro no negativo junto a
`fn.over(fn.rowNumber().toMixed())` compila como `UInt64`.
`fn.if(...)` y `fn.multiIf(...)` son escalares y no se pueden pasar directamente
a `fn.over(...)`; una rama de valores puede seguir siendo una selección de
ventana.

Cuando no hay forma de demostrar un único tipo SQL para todas las ramas de valores,
los metadatos del resultado siguen siendo desconocidos y la decodificación deja
el valor tal cual. Por eso, los resultados existentes pueden cambiar de un
`number` a un `string` de transporte. Para migrar, convierte explícitamente cada
rama de valores a un único tipo SQL y aplica después `.mapWith(...)` cuando haga
falta. Los metadatos de resultado automáticos se limitan deliberadamente a
anclas exactas `Bool`, `Int64`, `UInt64`, `Float64` y `String`. Los demás tipos
siguen pasando sin convertirse incluso cuando los metadatos de las ramas
coinciden, porque ClickHouse puede normalizar tipos de almacenamiento idénticos
(p. ej., `LowCardinality(String)` se convierte en `String`).

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

`fn.coalesce(...)` ya no asigna por contexto a los valores alternativos los tipos
`Float32`, `BFloat16` o `Decimal`, ni un `bigint` cuyo rango de destino no se pueda
demostrar que es seguro. Convierte esos valores alternativos explícitamente:

```ts
fn.coalesce(amount, fn.toDecimal64("0.00", 2));
```

### Sustitución con expresiones regulares

`fn.replaceRegexpAll(haystack, pattern, replacement)` genera la llamada a
[`replaceRegexpAll`](https://clickhouse.com/docs/sql-reference/functions/string-replace-functions#replaceregexpall)
de ClickHouse. Devuelve `Selection<string>`, o `Selection<string | null>` cuando
una entrada tipada admite NULL. Usa `String.raw` para las barras invertidas de
RE2 y las sustituciones por capturas:

```ts
const usDate = fn.replaceRegexpAll(
  orders.dateText,
  String.raw`^(\d{4})-(\d{2})-(\d{2})$`,
  String.raw`\2/\3/\1`,
);
// Selection<string>
```

Los argumentos se pasan como parámetros de consulta. Los patrones usan la
sintaxis RE2 de ClickHouse, la sustitución admite `\0`–`\9` y los patrones no
válidos siguen provocando errores de ClickHouse. Por precaución, una entrada
`ckSql` sin tipar se trata como un fragmento que admite NULL; escribe
`ckSql<string>` cuando sepas que el fragmento no admite NULL.

### Agregación de tuplas tipadas

`fn.tuple()` conserva el tipo de cada elemento. La variante de `groupArray` que
recibe un solo valor mantiene el tipo de esa tupla y el decodificador
correspondiente, y `fn.arrayReverseSort()` con un solo argumento mantiene el
mismo tipo de elemento de la matriz:

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

La variante existente con genérico explícito sigue disponible cuando trabajes con valores sin tipar o dinámicos.

Los nombres de las funciones auxiliares de matrices coinciden con los encabezados canónicos de la referencia de ClickHouse sobre [funciones de matrices](https://clickhouse.com/docs/sql-reference/functions/array-functions). Los nombres que solo existen como alias siguen disponibles mediante `fn.call(...)`, sin duplicar la API pública.

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

`fn.call(name, ...)` y `fn.withParams(name, ...)` validan `name` como un identificador SQL antes de compilar. `ck.has(...)`, `ck.hasAll(...)`, `ck.hasAny(...)` y `ck.hasSubstr(...)` son atajos de predicados para usar en la cláusula WHERE con esas mismas funciones de ClickHouse.

`fn.jsonExtract(json, returnType, ...path)` solo admite tipos de retorno `ckType.*`, así que el tipo de retorno de ClickHouse y el decodificador de TypeScript van siempre juntos:

Los fragmentos de JSON y de matrices que siguen suponen que el esquema ficticio `probeTelemetry` también incluye las columnas `metadata: JSON` y `tags: Array(String)`.

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

Cada segmento de la ruta es un argumento de ruta JSON de ClickHouse. Usa claves de cadena para los campos de los objetos e índices numéricos o bigint para las posiciones de una matriz:

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

Las funciones de matrices de orden superior respetan el orden de los parámetros de ClickHouse. Usa `ckSql` para la lambda, interpola los campos externos del esquema y deja las variables locales de la lambda como nombres SQL sin interpolar. Las funciones cuya lambda determina la forma del elemento, como `fn.arrayMap(...)`, `fn.arrayFilter(...)` y `fn.range(...)`, devuelven `Selection<unknown[]>` por defecto; encadena `.mapWith(...)` si tu consulta necesita un tipo de elemento decodificado más específico.

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

En esa lambda, `${params.timeColumn}` y `params.scopeTable.startTsList` son expresiones vinculadas al esquema. `start_ts` y `end_ts` son variables locales de la lambda que ClickHouse crea a partir de los elementos de la matriz.

`fn.arrayJoin(array)` corresponde a `arrayJoin` de ClickHouse y expande una fila de entrada en una fila por cada elemento de la matriz. Combínalo con `fn.arrayZip(...)` y `fn.tupleElement(...)` si necesitas mantener emparejados por posición los elementos de dos matrices:

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

La idea clave es la semántica: `arrayZip(sampleIds, probeIds)` mantiene los pares por posición, `arrayJoin(...)` los expande en filas y `tupleElement(..., 1/2)` lee cada elemento del par.

Para las claves compuestas, usa una comprobación de pertenencia con tuplas. Dos predicados `IN` independientes generan un producto cartesiano y pueden hacer coincidir pares que nadie pidió:

```ts
// Avoid this when sampleId and probeId must stay paired.
const wrong = ck.and(
  ck.inArray(probeTelemetry.sampleId, sampleIds),
  ck.inArray(probeTelemetry.probeId, probeIds),
);
```

Para conservar la relación entre los elementos del par, compara una tupla con una subconsulta que produzca tuplas o con una lista de pares. `fn.tuple` conserva el tipo de cada elemento cuando se conoce, así que la lista se pasa como un único parámetro `Array(Tuple(...))`:

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  [
    ["900001", "probe_alpha"],
    ["900002", "probe_beta"],
  ],
);
```

Un fragmento de SQL sin formato no tiene tipo de elemento. Envuelve la tupla con `fn.cast(fn.tuple(...), "Tuple(Int32, String)")` antes de usarla en `ck.inArray` o `ck.eq`.

Si la subconsulta produce tuplas, puedes usar el mismo lado izquierdo sin pasar un parámetro:

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  scopedSamples,
);
```

Las funciones auxiliares de matrices devuelven valores `Selection` tipados, así que puedes combinarlas en proyecciones, filtros, agrupaciones y uniones:

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

### Modos de `fn.count` / `fn.uniqExact`

`fn.count()`, `fn.countIf(predicate)` y `fn.uniqExact(expression)` son las funciones agregadas equivalentes a `db.count(...)` y siguen los mismos tres modos. Son útiles cuando el recuento aparece dentro de la consulta (en `select`, `having`, agrupaciones o subconsultas) y no como escalar de primer nivel. Comparten los mismos envoltorios SQL y decodificadores, así que elegir un modo u otro ofrece las mismas ventajas e inconvenientes:

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

Los decodificadores rechazan cualquier valor que no sea un entero no negativo —los booleanos, `NaN`, los decimales y los números negativos lanzan `Failed to decode count() result: ...`—, de modo que una salida corrupta del servidor falla enseguida en vez de producir `0` en silencio.

Los demás agregados no siguen deliberadamente esta API de modos: las cuestiones de precisión son distintas de las de `count` / `uniqExact`:

- `fn.sum()` / `fn.sumIf()` ya eligen un decodificador seguro para cada columna de entrada (cadena para las columnas de enteros y Decimal, número para las columnas de coma flotante) y amplían automáticamente la precisión de Decimal; encadena `.mapWith(Number)` si quieres específicamente un `number`.
- `fn.avg()` devuelve siempre `Selection<number>` porque ClickHouse calcula `avg(Decimal)` internamente con Float64; devolverlo como cadena sería engañoso, ya que no reflejaría la ruta de ejecución real.
- `fn.min()` / `fn.max()` conservan el tipo TS de la columna de entrada (por lo que `min(int64Col)` es `Selection<string>`, igual que el tipo que devuelve la columna al leerla).

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

## Sesiones y tablas temporales
<!-- src:8f031227 -->

ClickHouse no ofrece transacciones tradicionales. Para análisis acotados y flujos de trabajo con filtros amplios, las tablas temporales asociadas a una sesión suelen ser el mecanismo adecuado.

En la función de retorno de llamada de una sesión, `ck-orm` te ofrece:

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

Usa `registerTempTable(name)` cuando un comando que controlas cree la tabla temporal y, aun así, quieras que `ck-orm` la elimine al limpiar la sesión:

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

Usa `createTemporaryTableRaw(name, definition)` solo para definiciones de tablas de confianza que controles tú y que no puedas expresar con `ckTable(...)`:

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

El argumento `definition` empieza después del nombre de la tabla. No incluyas `CREATE TEMPORARY TABLE` ni el nombre de la tabla: ck-orm genera esas partes y valida el identificador por separado.

### Comportamiento de la sesión

- Las solicitudes que usan el mismo `session_id` comparten el controlador de concurrencia de esa sesión; el valor predeterminado del cliente es `session_max_concurrent_requests = 1`.
- En el servidor, las sesiones reales de ClickHouse son exclusivas, así que aumentar `session_max_concurrent_requests` por encima de `1` desactiva la serialización local, pero aun así puede aparecer `SESSION_IS_LOCKED`.
- `createTemporaryTable()` registra automáticamente la tabla para limpiarla.
- `createTemporaryTable()` consume objetos del esquema; `Session` gestiona el ciclo de vida de la tabla temporal, no el propio esquema.
- `createTemporaryTableRaw()` es la API de SQL sin formato reservada para definiciones de tablas temporales de confianza y rechaza las definiciones con varias sentencias.
- `runInSession()` envía automáticamente el `session_id` resuelto como `X-ClickHouse-Replica-Tag` en cada solicitud, incluida la de limpieza; la biblioteca controla este encabezado en tiempo de ejecución y no puedes sobrescribirlo mediante `http_headers`.
- El [enrutamiento consciente de las réplicas](https://clickhouse.com/docs/products/cloud/features/infrastructure/replica-aware-routing) de ClickHouse Cloud debe activarse por separado y no ofrece garantías: al escalar, ClickHouse puede reasignar una etiqueta, y los despliegues web entre distintos orígenes deben permitir `X-ClickHouse-Replica-Tag` en CORS.
- `runInSession()` elimina las tablas temporales registradas al terminar la función de retorno de llamada.
- Las llamadas anidadas a `runInSession()` siempre crean una nueva sesión hija.
- Las llamadas anidadas no pueden reutilizar ningún `session_id` activo de sus sesiones antecesoras.
- Las sesiones hijas anidadas no comparten las tablas temporales ni los cupos de concurrencia de la sesión principal, porque siempre usan un `session_id` distinto.

### Contrato de concurrencia de sesiones

En la práctica:

- Las solicitudes con el mismo `session_id` explícito se serializan de forma predeterminada.
- El `session_id` predeterminado del cliente también pasa por el mismo limitador.
- Los clientes secundarios creados por `withSettings()` comparten el mismo limitador que su cliente principal cuando apuntan al mismo `session_id`.
- Los distintos valores de `session_id` no se bloquean entre sí.
- `stream()` y el `iterator()` del constructor mantienen ocupado el cupo de la misma sesión hasta que termina el iterador. Si detienes la iteración antes de tiempo (`break`, `return` o un error), la biblioteca cancela la consulta del servidor con `KILL QUERY`, que se envía fuera de la sesión (con las mismas credenciales y la misma etiqueta de réplica), y espera a que ClickHouse libere la sesión, como máximo durante `request_timeout` (30 s de forma predeterminada).
- Las llamadas anidadas a `runInSession()` crean una sesión hija con un `session_id` nuevo, así que no comparten el cupo de la sesión principal.

Abortar una solicitud o dejar que se dispare `request_timeout` solo cierra la conexión. ClickHouse sigue ejecutando la consulta hasta que termina o detecta la desconexión, así que un `INSERT ... SELECT` aún puede completarse y la siguiente solicitud de la misma sesión puede fallar con `SESSION_IS_LOCKED`. Configura `max_execution_time` en `clickhouse_settings` por debajo de `request_timeout` para que el servidor se detenga primero; también puedes ejecutar `KILL QUERY WHERE query_id = '...'` fuera de la sesión o envolver `fetch` para reintentar un número limitado de veces las solicitudes que fallen con el error 373 y cuyo cuerpo se pueda volver a enviar. Los fallos de limpieza, incluido `SESSION_IS_LOCKED`, se notifican mediante `onCleanupError` o un `SessionError`.

Uso recomendado:

- Mantén `session_max_concurrent_requests = 1` para los flujos de trabajo con tablas temporales y cualquier lógica que mantenga una sesión abierta.
- Si necesitas paralelismo real, usa valores distintos de `session_id` en vez de subir el límite de la misma sesión.
- Aumenta `session_max_concurrent_requests` solo cuando quieras desactivar explícitamente el control local de concurrencia y puedas tolerar `SESSION_IS_LOCKED`.

## Métodos de ejecución
<!-- src:3e31db64 -->

Además del constructor, el cliente ofrece métodos de ejecución de bajo nivel.

Los campos habituales por consulta son:

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

`session_timeout` y `session_check` están aquí por algo: describen una solicitud concreta o un bloque de sesión concreto, no un valor predeterminado global del cliente.
`query_id`, `session_id` y `session_timeout` son opciones de la solicitud o de la sesión; déjalos fuera de `clickhouse_settings`.

`session_check` no inicia una sesión nueva de ClickHouse. Úsalo cuando necesites que ClickHouse compruebe que un `session_id` explícito ya existe.
Cualquier solicitud que incluya un `session_id` pasa por el mismo limitador de esa sesión, incluidos el `execute()` sin formato, `command()`, `stream()`, `.execute()` del constructor y `.iterator()` del constructor.

Los `clickhouse_settings` de cada solicitud solo sobrescriben los ajustes de ClickHouse para esa solicitud:

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

Ejecuta una consulta sin formato y devuelve `Record<string, unknown>[]`:

```ts
const rows = await db.execute(ckSql`SELECT 1 AS one`);
```

### `stream()`

Procesa los resultados de una consulta sin formato como un flujo:

```ts
for await (const row of db.stream(ckSql`SELECT number FROM numbers(10)`)) {
  console.log(row);
}
```

Si `stream()` usa un `session_id`, el cupo de la misma sesión solo se libera cuando termina el iterador asíncrono o cuando sales del bucle antes de tiempo y lo cierras.

### `command()`

Ejecuta un comando que no devuelve un conjunto de resultados:

```ts
await db.command(ckSql`SYSTEM FLUSH LOGS`);
```

### `withSettings()`

Crea un cliente secundario con ajustes predeterminados adicionales:

```ts
const reportDb = db.withSettings({
  max_execution_time: 60,
  join_use_nulls: 0,
});
```

`withSettings()` solo cambia los ajustes predeterminados de sesión y consulta de ClickHouse. El cliente devuelto conserva el mismo transporte, autenticación, tiempo de espera y controlador de concurrencia de sesiones que el cliente principal. El literal `join_use_nulls: 0` se infiere, así que los left joins del cliente secundario tienen un tipo no anulable; un valor tipado como `number` a secas sigue dando `| null`.

Las opciones del ciclo de vida de la sesión se mantienen separadas de los ajustes de ClickHouse en `runInSession()`:

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

### Integración con consultas compiladas

La mayoría de las aplicaciones debería usar el constructor directamente. Usa `executeCompiled()` e `iteratorCompiled()` cuando integres otra capa que ya produzca un `CompiledQuery`.

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

### `ping()` y `replicasStatus()`

```ts
await db.ping();
await db.replicasStatus();
```

## Observabilidad
<!-- src:8021e8d9 -->

`ck-orm` incluye tres capas de observabilidad integradas:

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
  databaseUrl: "http://127.0.0.1:8123/telemetry_lab",
  logger,
  logLevel: "info",
});
```

### Trazado

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

El trazado obtiene el nombre de la base de datos, la dirección del servidor, el puerto del servidor y el tiempo de espera de la solicitud a partir de la configuración de `clickhouseClient(...)`. Pasa las etiquetas de nivel de servicio mediante `tracing.attributes`; las claves que empiezan por `db.` se ignoran para que el código de la aplicación no pueda sobrescribir los atributos de base de datos integrados.

En sistemas de trazado compartidos, es más seguro usar `includeStatement: false`. La biblioteca usa `true` de forma predeterminada; desactiva esta opción cuando los nombres de las tablas o columnas, o la forma de la consulta, no deban salir del perímetro del servicio. Los valores vinculados no se incorporan a la sentencia compactada, pero la forma del SQL puede seguir resultando sensible desde el punto de vista operativo.

### Instrumentación personalizada

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

Tipos de eventos públicos:

- `ClickHouseORMQueryEvent`
- `ClickHouseORMQueryResultEvent`
- `ClickHouseORMQueryErrorEvent`
- `ClickHouseORMQueryStatistics`
- `ClickHouseORMTracingOptions`
- `ClickHouseORMLogLevel`

Los campos de evento integrados incluyen `databaseName`, `serverAddress`, `serverPort`, `requestTimeoutMs`, `statementHash`, `querySummary`, `queryId`, `sessionId` y `tableName` cuando ck-orm puede identificar una sola tabla sin adivinarla a partir del SQL sin formato. Las consultas JSON que cargan todos los resultados también incluyen las estadísticas de respuesta de ClickHouse cuando el servidor las devuelve: `serverElapsedMs`, `readRows`, `readBytes`, `resultRows` y `rowsBeforeLimitAtLeast`.

## Modelo de errores
<!-- src:b4b93db0 -->

Tipos de error públicos (se exportan solo como tipos):

- `ClickHouseORMError`
- `DecodeError`

Comprobaciones de errores en tiempo de ejecución:

- `isClickHouseORMError(error)`
- `isDecodeError(error)`

`ClickHouseORMError` conserva todo el contexto posible, incluidos:

- `kind`
- `executionState`
- `queryId`
- `sessionId`
- `httpStatus`
- `clickhouseCode`
- `clickhouseName`
- `responseText`
- `requestTimeoutMs`

Los valores públicos actuales de `kind` son:

- `client_validation`
- `request_failed`
- `decode`
- `timeout`
- `aborted`
- `session`

Usa todo el contexto del error en los registros del servidor y devuelve un error público breve al código que llama a la aplicación:

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

No devuelvas `responseText` ni el `message` sin procesar del error a clientes que no sean de confianza. ClickHouse puede incluir fragmentos SQL y nombres de objetos en el texto del error.

## Seguridad
<!-- src:c2dbb125 -->

`ck-orm` está diseñado para que, de forma predeterminada, sea difícil construir SQL inseguro.

Las protecciones integradas incluyen:

- los identificadores que se pasan a `ckSql.identifier()` se validan y entrecomillan
- los nombres de función que se pasan a `fn.call(...)`, `fn.withParams(...)`, `ckType.aggregateFunction(...)` y `ckType.simpleAggregateFunction(...)` se validan; los parámetros de `AggregateFunction` deben ser literales de cadena o numéricos
- las claves de campos anidados que se pasan a `ckType.nested({...})` se validan como identificadores de ClickHouse
- los valores interpolados en `` ckSql`...` `` se convierten en parámetros con nombre de ClickHouse en vez de texto SQL sin formato
- se rechazan las claves de `query_params` que empiezan por `orm_param`, porque ese prefijo está reservado a los parámetros internos de plantillas SQL
- solo se permite una sentencia de nivel superior por solicitud
- los encabezados de autorización derivados de la configuración de conexión no pueden anularse con los `http_headers` proporcionados por el código que hace la llamada
- la configuración de conexión estructurada rechaza las credenciales incluidas en `host`
- los datos de destino del trazado se normalizan antes de adjuntarlos a los spans

### API solo para código de confianza

Si necesitas DDL de tablas temporales que solo pueda ejecutar código de confianza y no puedes representarlo con `ckTable(...)`, `ck-orm` expone una API acotada de SQL sin formato dentro de `runInSession()`:

- `session.createTemporaryTableRaw(name, definition)`

`fn.call(name, ...)` y `fn.withParams(name, ...)` validan `name`, pero aun así debes tratar los nombres de función elegidos dinámicamente como entradas controladas por el desarrollador, no como entradas no fiables recibidas en tiempo de ejecución.

`ckSql.identifier(...)` valida en el límite entre compilación y ejecución. Crear el fragmento cuesta poco; el error aparece cuando se compila como parte de una consulta.

### Exposición de datos de trazado

`tracing.includeStatement` tiene `true` como valor predeterminado, por lo que el texto SQL compactado se adjunta a los spans como `db.statement` y `db.query.text`.

Los valores vinculados se guardan por separado como parámetros con nombre de ClickHouse y no aparecen en el texto de la sentencia, pero los nombres de tabla y columna y la forma de la consulta sí aparecen en la salida de trazado. Si el sistema de trazado es compartido o está fuera del host, desactiva `includeStatement` o filtra `db.statement` / `db.query.text` en el colector.

### Respuestas de error

`ClickHouseORMError.responseText` y el `message` del error pueden contener texto sin procesar del servidor ClickHouse, incluidos fragmentos SQL y nombres de objetos de la base de datos.

No reenvíes esos detalles directamente a clientes que no sean de confianza. Regístralos en el servidor y expón un mensaje genérico en su lugar.

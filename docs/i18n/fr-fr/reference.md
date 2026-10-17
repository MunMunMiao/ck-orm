## Installation de ck-orm
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

ck-orm n'a aucune dépendance obligatoire. Le traçage OpenTelemetry est un point d'entrée distinct, `ck-orm/otel`, et `@opentelemetry/api` est une dépendance entre pairs facultative que lui seul utilise. Le point d'entrée principal `ck-orm` n'importe jamais OpenTelemetry : une application, un bundle pour navigateur ou un worker edge qui ne trace pas n'installe ni n'embarque rien de tout cela. Pour tracer vos requêtes, suivez la mise en place décrite dans [Plugin OpenTelemetry](#opentelemetry-plugin).

## Prise en main
<!-- src:847057c0 -->

Les exemples ci-dessous utilisent une seule table pour que le déroulé principal reste facile à suivre.

### 1. Définir un schéma

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

### 2. Créer un client

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

### 3. Interroger les données

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

Les requêtes construites sont des semi-promesses : `await query` les exécute directement.

Sur une colonne `Decimal`, `fn.sum` convertit automatiquement le résultat en `Decimal(P, S)` et renvoie `string` pour préserver la précision — voir [Précision de Decimal dans les expressions](#decimal-precision-in-expressions).

### 4. Observer les requêtes

Les `plugins` se branchent sur chaque requête que ck-orm envoie à ClickHouse. `otelTracing()`, fourni par `ck-orm/otel`, transforme chaque requête en span OpenTelemetry dès qu'un SDK est démarré ; la mise en place est décrite dans [Plugin OpenTelemetry](#opentelemetry-plugin), et [Écrire des plugins](#writing-plugins) explique comment écrire vos propres hooks.

```ts
import { clickhouseClient } from "ck-orm";
import { otelTracing } from "ck-orm/otel";

export const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  plugins: [otelTracing()],
});
```

## Exemples
<!-- src:c8f5c20b -->

La [page d'exemples](https://ck-orm.lyz.cloud/examples/) présente des requêtes exécutables avec le SQL exact qu'elles envoient à ClickHouse. Chaque exemple est un projet dans [`examples/`](../examples), avec son propre test. Les tests ClickHouse exécutables sont dans [`e2e/`](../e2e), qui contient notamment des schémas de scénarios proches de la production dans [`e2e/scenarios.ts`](../e2e/scenarios.ts).

## Modèle mental
<!-- src:a6fa96d2 -->

Gardez ces limites en tête quand vous utilisez `ck-orm` :

- `ckType.*` définit les types de colonnes du schéma
- `ckTable(...)` définit les schémas de table
- `ck.*` est l'espace de noms des fonctions utilitaires de requête
- `fn.*` est l'espace de noms des fonctions utilitaires SQL
- le schéma décrit les tables et les colonnes, pas le nom de la base de données
- la connexion à la base de données passe par `clickhouseClient(...)`
- les requêtes construites sont le choix par défaut ; le SQL brut reste disponible pour les expressions propres à ClickHouse
- `runInSession()` est une fonction utilitaire de session ClickHouse, pas une transaction
- `plugins` observent chaque requête envoyée par ck-orm ; le traçage est le plugin `ck-orm/otel`, à activer explicitement et séparé du point d'entrée principal
- `leftJoin()` et `asofLeftJoin()` utilisent par défaut la sémantique SQL des valeurs nulles
- les résultats numériques volumineux ou à risque sont décodés avec prudence, sans conversion silencieuse

`ck-orm` se concentre sur l'accès typé à ClickHouse : définitions de tables, composition de requêtes, fonctions utilitaires d'écriture et de session ainsi que l'observabilité. `createTable()`, `createMaterializedView()` et `dropTable()` exécutent chacun une instruction DDL ; les migrations de schéma (comparaison des schémas, `ALTER`, gestion des versions) restent dans l'outil de migration utilisé par l'application.

## DSL de schéma
<!-- src:5149bff0 -->

### `ckTable()`

Utilisez `ckTable(name, columns, options?)` pour définir une table.

Les exemples de cette section supposent l'import suivant :

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

Le troisième argument peut être un objet simple ou une fonction de fabrique :

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

Options publiques de la table :

- `engine`
- `partitionBy`
- `primaryKey`
- `orderBy`
- `sampleBy`
- `ttl`
- `settings`
- `comment`
- `versionColumn`

Les définitions de colonne peuvent aussi inclure directement les métadonnées DDL :

- `.default(expr)`
- `.materialized(expr)`
- `.aliasExpr(expr)`
- `.comment(text)`
- `.codec(expr)`
- `.ttl(expr)`

Les métadonnées du schéma remplissent deux rôles dans `ck-orm` :

- elles pilotent les requêtes typées, la validation à l'insertion et le décodage des résultats
- elles produisent l'instruction `CREATE TABLE` de `createTable()` et de `createTemporaryTable()`

Elles ne migrent ni ne synchronisent les tables ClickHouse existantes : `createTable()` ne fait que créer une table (voir [Tables et vues matérialisées](#tables-and-materialized-views)). Gardez les migrations de schéma dans votre outil de migration et alignez `ckTable(...)` sur le schéma que votre application lit et écrit.

Exemple :

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

### Inférence de types

Chaque table expose :

- `table.$inferSelect`
- `table.$inferInsert`

```ts
type TelemetryRow = typeof probeTelemetry.$inferSelect;
type TelemetryInsert = typeof probeTelemetry.$inferInsert;

const sampleId: TelemetryRow["sampleId"] = "900001";
const ingestVersion: TelemetryInsert["ingestVersion"] = "1";
```

Pour les fonctions utilitaires génériques, utilisez :

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

Les objets de schéma utilisés avec `InferSelectSchema` et `InferInsertSchema` sont de simples regroupements TypeScript. Ils servent à partager les types de modèle et restent distincts de la configuration du client.

### Affiner les types de colonnes avec `$type<T>()`

Chaque constructeur de colonne expose une méthode chaînable `$type<T>()` qui remplace le type TypeScript de la colonne sans changer son comportement à l'exécution. Utilisez-la quand le type inféré par défaut est trop large pour votre domaine :

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

Vous pouvez chaîner `$type<T>()` avec tous les autres modificateurs de colonne, dans n'importe quel ordre : `ckType.string().$type<Role>().default(ckSql`'guest'`).comment("…")`. Le décodeur à l'exécution, l'encodeur, le type SQL et la sortie DDL restent inchangés : `$type` n'est qu'une assertion de type à la compilation.

`enum8` et `enum16` infèrent déjà l'union littérale à partir de l'objet de valeurs. Vous n'avez donc besoin de `$type` que pour lui attribuer une autre marque ou l'aligner sur un type d'énumération externe :

```ts
// Inferred automatically — no `as const`, no explicit type arguments
const status = ckType.enum8({ active: 1, paused: 2, banned: 3 });
//    ↑ Enum8<"active" | "paused" | "banned">

// Re-brand on top of the inferred keys
type UserStatus = "active" | "paused" | "banned";
const userStatus = ckType.enum8({ active: 1, paused: 2, banned: 3 }).$type<UserStatus>();
```

> **Point d'attention :** `$type<T>()` ne valide pas les valeurs à l'exécution. Si une ligne de la base contient une valeur qui ne fait pas partie du type déclaré (par exemple, des données obsolètes antérieures à une migration de schéma), TypeScript continuera à restreindre le type comme si cette valeur correspondait au type déclaré — un `switch` exhaustif ignorera silencieusement ce cas. Utilisez `mapWith` dans une projection si vous devez valider la valeur à la lecture, ou `$validator()` ci-dessous pour la valider à chaque décodage et encodage.

### Différencier les types d'insertion et de sélection avec `$type<{ select, insert }>()`

Les colonnes ClickHouse acceptent parfois un type plus large à l'insertion que la valeur renvoyée lors de la lecture — par exemple, une colonne `DateTime` accepte `string` et `Date` à l'insertion, mais renvoie toujours `Date` après décodage. Transmettez un objet à `$type` pour distinguer les deux cas :

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

La forme `$type<{ select: T }>()` (sans `insert`) équivaut à `$type<T>()`, qui n'utilise qu'un seul générique.

### Règles du modèle d'insertion : DEFAULT, MATERIALIZED, ALIAS

`$inferInsert` est dérivé de la structure des colonnes du schéma ; ce n'est pas une simple copie de `$inferSelect` :

| Modificateur DDL | Effet sur le modèle d'insertion |
| --- | --- |
| `.default(expr)` | La colonne devient facultative — si vous l'omettez, ClickHouse renseigne sa valeur |
| `.materialized(expr)` | La colonne est entièrement exclue du modèle d'insertion |
| `.aliasExpr(expr)` | La colonne est entièrement exclue du modèle d'insertion |

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

### Validation à l'exécution avec `$validator(schema)`

Quand `$type` ne suffit pas — la base peut contenir des données obsolètes, vous acceptez des structures JSON provenant de sources en amont non fiables ou vous voulez que l'API d'insertion convertisse une chaîne en `Date` — utilisez plutôt `$validator(schema)`. N'importe quel objet conforme à la [spécification Standard Schema v1](https://github.com/standard-schema/standard-schema) fonctionne : Zod 3.23+, Valibot 1+, ArkType, Effect Schema, TypeBox. ck-orm ne dépend d'aucune de ces bibliothèques.

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

Les transformations du schéma (par exemple `z.string().transform((v) => new Date(v))`) sont prises en compte : la colonne accepte le type d'*entrée* du schéma lors de l'insertion et expose le type de *sortie* lors de la lecture. `$validator` ne fonctionne qu'en synchrone : les schémas asynchrones lèvent une erreur au moment du décodage ou de l'encodage.

Comparaison entre `$type` et `$validator` :

| | `$type<T>()` | `$validator(schema)` |
| --- | --- | --- |
| Coût à l'exécution | Aucun | Une validation synchrone du schéma par ligne et par colonne |
| Sûreté du typage | Assertion de type à la compilation | Types inférés à partir du schéma |
| Validation à l'exécution | Non | Oui — lève une erreur en cas d'écart |
| Types d'entrée et de sortie différents | Via `$type<{ select, insert }>()` | Via les transformations du schéma |
| Dépendance externe | Aucune | Aucune (vous choisissez la bibliothèque de schémas) |

### Type de colonne JSON

ck-orm cible le **nouveau type de données JSON** de ClickHouse à partir de la version 24.x
([documentation](https://clickhouse.com/docs/sql-reference/data-types/newjson)) :
celui qui découpe physiquement un objet JSON en sous-colonnes typées au lieu de
stocker la valeur dans une chaîne de caractères opaque. Retenez d'abord deux
règles simples :

- Le type de données `JSON` de ClickHouse n'accepte **que les objets de premier
  niveau**. Les tableaux, les chaînes de caractères et les nombres de premier
  niveau sont rejetés.
  ck-orm applique la même règle à la compilation via
  `T extends Record<string, unknown>` et à l'exécution avec une vérification
  rapide en O(1) à chaque insertion ou sélection.
- Les chemins typés (`a.b UInt32`) deviennent de vraies sous-colonnes dans
  ClickHouse ; ck-orm fait passer ces chemins par `mapFromDriverValue` et
  `mapToDriverValue` de la fabrique de colonnes correspondante de ck-orm, de sorte
  qu'un chemin déclaré avec `ckType.uint64()` est décodé en `string` sans perte,
  exactement comme la colonne `uint64` de premier niveau.

La forme la plus simple reprend le modèle de toutes les autres fabriques de
colonnes de ck-orm :

```ts
import { ckTable, ckType } from "ck-orm";

const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json("payload"),
});
// DDL:  `payload` JSON
// $inferSelect.payload: { [key: string]: unknown }
```

Transmettez un générique pour faire apparaître la structure réelle dans `$inferSelect` /
`$inferInsert` :

```ts
type EventPayload = { user_id: number; event: string; meta?: { ip: string } };

const userEvents = ckTable("user_events", {
  id: ckType.uint64(),
  payload: ckType.json<EventPayload>("payload"),
});
```

`ckType.json<T>().$type<T2>()` est l'équivalent chaînable si vous préférez
garder le générique avec les autres appels au constructeur de colonnes.

#### DDL paramétré

Le deuxième argument de la fabrique accepte les quatre paramètres DDL de NewJSON :

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

Les clés de `typeHints` sont vérifiées par rapport à `Paths<T>` : les fautes de
frappe comme `{ "user.idd": ... }` ne passent pas la compilation. Le DDL produit
suit toujours le même ordre lexicographique, pour que les outils externes de
comparaison de schémas obtiennent une sortie identique à l'octet près, quelle que
soit la façon dont vous avez écrit la configuration.

#### Effet de typeHints sur le décodeur à l'exécution

Avec la politique sans perte de ClickHouse pour les entiers de 64 bits, `UInt64`
est décodé en chaîne de caractères par ck-orm. Si vous déclarez `user_id` à la
fois dans la structure TypeScript *et* comme entrée de `typeHints`, les deux
doivent correspondre : pour `uint64`, le générique doit donc contenir
`user_id: string`. Utilisez `$type<{ select, insert }>()` lorsque le type accepté
à l'insertion diffère du type décodé lors de la sélection :

```ts
ckType.json("payload", { typeHints: { user_id: ckType.uint64() } })
  .$type<{
    select: { user_id: string };
    insert: { user_id: string | number | bigint };
  }>();
```

#### Accès aux chemins

Chaque colonne JSON expose cinq méthodes chaînables qui reprennent telle quelle
la syntaxe d'accès aux chemins de ClickHouse :

| Méthode | SQL | Rôle |
| --- | --- | --- |
| `col.path("a.b")` | `col.a.b` | Lit un chemin ; le type statique est `PathValue<T, "a.b">` (voir la remarque sur `typeHints` ci-dessous) |
| `col.castPath("a.b", ckType.uint64())` | `col.a.b.:UInt64` | Force le décodage d'un chemin via une autre fabrique de colonnes de ck-orm ; le type statique provient du `TData` de la colonne de conversion |
| `col.subobject("a")` | `col.^a` | Lit un objet imbriqué comme une valeur `JSON` |
| `col.merged("a")` | `col.@a` | Lit la vue fusionnée des données partagées |
| `col.arrayPath("a")` | `col.a[]` | Développe un chemin de type tableau |

> `col.path(P)` est toujours typé `PathValue<T, P>` : il est déduit du
> générique `T`, pas de `typeHints`. `typeHints` ne modifie que le décodeur
> à l'exécution ; vous devez donc maintenir manuellement la cohérence entre le
> type statique et l'indication de type (la section précédente explique la
> politique sans perte `uint64 → string`). Si les deux ne correspondent pas,
> utilisez plutôt `castPath(P, ckType.X())` : son type de retour provient de
> la colonne de conversion.

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

`Paths<T>` valide le littéral de chemin à la compilation : `payload.path("a.zzz")`
ne passe pas la vérification de types si `"a.zzz"` ne fait pas partie de `T`.

Chaque modificateur de chaînage (`$type`, `$validator`, `.default()`,
`.materialized()`, `.bind()`, …) réattache les méthodes de chemin à l'exécution :
l'appel sous-jacent fonctionne donc toujours. **Le type statique conserve ces
méthodes à chaque étape du chaînage, sauf après `$validator`** : le contrat de
`$validator` est paramétré par `StandardSchemaV1<unknown, unknown>`, que
TypeScript ne peut pas restreindre en toute sécurité à `JsonShape` sans enfreindre
les règles de sous-typage de style LSP qu'il impose. Après ce maillon, le type
statique redevient donc `Column<T>`. La méthode reste appelable : si vous devez
poursuivre le chaînage, utilisez simplement une assertion de type :

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

#### `fn.json*` — appels avec un chemin dynamique

Si vous construisez le chemin sous forme de chaîne de caractères à l'exécution
(ou si vous voulez lire un chemin qui n'est pas déclaré dans `T`), utilisez
l'espace de noms `fn.*` :

```ts
import { fn } from "ck-orm";

const userPath = userInput as string;
await db.select({
  raw:    fn.jsonPath<string>(userEvents.payload, userPath),
  casted: fn.jsonCast(userEvents.payload, userPath, ckType.string()),
  shape:  fn.dynamicType(fn.jsonPath(userEvents.payload, userPath)),
});
```

`fn.jsonCast` est l'équivalent dynamique de `castPath` ; `fn.jsonSubobject`,
`fn.jsonMerged` et `fn.jsonArray` reprennent les méthodes correspondantes des
colonnes ; `fn.dynamicType` expose la fonction ClickHouse `dynamicType()` pour
connaître à l'exécution le type d'un chemin Dynamic.

#### Validation à l'exécution

Deux couches s'appliquent à chaque ligne, **dans cet ordre** :

1. **La garde JSON intégrée à ck-orm** — `null` / `undefined` passent tels
   quels pour que les colonnes JSON `nullable()` fonctionnent naturellement ;
   toute autre entrée qui n'est pas un objet (tableau, chaîne de caractères,
   nombre, booléen) est rejetée avec une erreur `client_validation` / `decode`
   associée à la colonne, comme `JSON column expects a plain object, got array`.
   Les chemins typés de `typeHints` passent par l'encodeur et le décodeur de
   leur fabrique de colonnes, à l'encodage comme au décodage. Cette couche est
   toujours active.
2. **Le `$validator(schema)` facultatif** — s'il est présent, il s'exécute
   *avant* l'encodeur intégré lors de l'insertion et *après* le décodeur intégré
   lors de la sélection. Utilisez-le pour valider la structure :

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

ck-orm **ne** parcourt **pas** récursivement la valeur JSON pour vérifier que
`T["a"]` est bien un `number`, etc. Utilisez `$validator` si vous en avez besoin :
le rôle de la garde JSON est seulement de détecter les cas où le message d'erreur
de ClickHouse lui-même serait difficile à relier à la colonne concernée. Les
arguments scalaires de premier niveau provoquent une erreur explicite qui indique
le nom de la colonne concernée :

```ts
await db.insert(events).values({ payload: ["wrong"] as never });
// throws: JSON column expects a plain object, got array
```

#### Valeurs par défaut à l'insertion

Les colonnes JSON suivent les mêmes règles de `$inferInsert` que toutes les
autres colonnes. `.default()` rend le champ facultatif ; `.materialized()` /
`.aliasExpr()` le suppriment complètement du modèle d'insertion :

```ts
const auditLogs = ckTable("audit_logs", {
  id: ckType.uint64(),
  payload: ckType.json<{ note: string }>("payload").default(ckSql`'{}'`),
});

await db.insert(auditLogs).values({ id: "1" }); // payload filled by CH DEFAULT
```

### `ckAlias()`

Utilisez `ckAlias()` quand la même table doit apparaître plusieurs fois dans une requête.

```ts
import { fn, ck, ckAlias } from "ck-orm";

const telemetry = ckAlias(probeTelemetry, "telemetry");
```

Les colonnes produites par `ckAlias()` sont automatiquement associées à l'alias.

### Noms des colonnes

La clé de l'objet de schéma est la clé logique utilisée dans les lignes TypeScript, les résultats de requête décodés et les valeurs d'insertion. Par défaut, cette même clé sert aussi de nom de colonne ClickHouse :

```ts
const telemetry = ckTable("telemetry", {
  signalStrength: ckType.decimal({ precision: 20, scale: 5 }),
});
```

Quand la colonne de la base porte un autre nom, transmettez ce nom de colonne physique en premier argument :

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

SQL, le DDL, les filtres, les tris, les regroupements et les listes de colonnes pour les écritures utilisent les noms physiques (`probe_id`, `signal_strength`, `created_at`). Les modèles inférés, les résultats de sélection par défaut, les clés de projection explicites et les valeurs d'insertion utilisent les clés de l'objet de schéma (`probeId`, `signalStrength`, `createdAt`).

Chaque constructeur public `ckType` accepte un nom de colonne physique en argument de premier niveau. Les constructeurs sans configuration supplémentaire acceptent `name?` ; ceux qui ont une configuration de type prennent le nom de colonne physique facultatif en premier, puis la configuration dans un objet :

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

`aggregateFunction` et `simpleAggregateFunction` ont une syntaxe d'appel propre à ClickHouse : la première chaîne correspond au nom de la fonction d'agrégation, pas à celui de la colonne :

```ts
ckType.aggregateFunction("sum", ckType.uint64());
ckType.aggregateFunction("quantile(0.5)", ckType.float64());
ckType.simpleAggregateFunction("sum", ckType.uint64());
```

`AggregateFunction` accepte les noms de fonctions d'agrégation ClickHouse avec des paramètres littéraux, comme `quantile(0.5)` ou `topK(10)`. `simpleAggregateFunction` reste plus strict et utilise un simple nom de fonction d'agrégation.

Utilisez un objet de configuration si vous avez aussi besoin d'un nom de colonne physique :

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

`nested("items", shape)` nomme la colonne `Nested(...)` de niveau supérieur. Les noms des champs imbriqués sont les clés de `shape` ; les valeurs `configuredName` des colonnes internes ne servent pas à nommer ces champs :

```ts
const sensorPackets = ckTable("sensor_packets", {
  items: ckType.nested("items", {
    componentId: ckType.string(),
    quantity: ckType.float64(),
  }),
});
```

### Constructeurs de colonnes

Utilisez `ckType.*` pour les constructeurs de colonnes du schéma. Le DSL de schéma couvre les familles de types ClickHouse les plus courantes :

- entiers : `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`
- types à virgule flottante et décimaux : `float32`, `float64`, `bfloat16`, `decimal`
- types scalaires : `bool`, `string`, `fixedString`, `uuid`, `ipv4`, `ipv6`
- types temporels : `date`, `date32`, `dateTime`, `dateTime64`, `time`, `time64`
- énumérations et types particuliers : `enum8`, `enum16`, `json`, `dynamic`, `qbit`
- conteneurs : `nullable`, `array`, `tuple`, `map`, `nested`, `variant`, `lowCardinality`
- types d'agrégation : `aggregateFunction`, `simpleAggregateFunction`
- types géométriques : `point`, `ring`, `lineString`, `multiLineString`, `polygon`, `multiPolygon`

Pour les lectures, les écritures et les modèles inférés à partir du schéma, `int64` et `uint64` correspondent par défaut à `string` en TypeScript, pour conserver l'exactitude des valeurs sur 64 bits dans le format JSON de ClickHouse comme dans les environnements d'exécution JavaScript. Si vous voulez explicitement `bigint`, utilisez votre propre décodeur, par exemple `mapWith((value) => BigInt(String(value)))`.

ClickHouse ne prend pas en charge `Nullable(Array(...))`, `Nullable(Map(...))` ni `Nullable(Tuple(...))`. `ck-orm` rejette ces structures au moment de définir le schéma. Placez plutôt `nullable(...)` à l'intérieur du type composite, par exemple `ckType.array(ckType.nullable(ckType.string()))`.

Les constructeurs qui prennent une configuration de type utilisent un objet de configuration ; le nom de colonne physique facultatif vient en premier :

```ts
ckType.decimal({ precision: 20, scale: 5 });
ckType.decimal("signal_strength", { precision: 20, scale: 5 });
ckType.fixedString({ length: 8 });
ckType.dateTime64("created_at", { precision: 9, timezone: "UTC" });
ckType.qbit("embedding", ckType.float32(), { dimensions: 8 });
```

Les écritures `Date` / `Date32` et les valeurs de prédicat issues du schéma envoient, pour un `Date` JavaScript, le jour calendaire correspondant en UTC (`2026-06-15T23:00:00Z` devient `2026-06-15`). Pour stocker un autre jour, transmettez une chaîne `YYYY-MM-DD` ; les chaînes et les nombres sont transmis tels quels.

Les valeurs de prédicat `DateTime` / `DateTime64` associent un `Date` JavaScript à un instant UTC ; le fuseau horaire de la colonne, de la session ou du serveur ne peut donc pas modifier cet instant. ClickHouse lit une chaîne dans le fuseau horaire de la colonne, telle qu'elle est écrite. Dans une position `DateTime` sur 32 bits, un paramètre dont toutes les valeurs de type Date sont exprimées à la seconde près est lié comme `DateTime('UTC')` ; si l'une des valeurs de type Date de ce paramètre contient des millisecondes, il est lié comme `DateTime64(3, 'UTC')`. Dans une position `DateTime64`, le paramètre est toujours lié comme `DateTime64(3, 'UTC')`. Les prédicats scalaires comme `gte` comparent toujours l'instant exact. Sur une colonne `DateTime`, `inArray` / `notInArray` exigent des valeurs de type Date à la seconde près ; ClickHouse rejette une valeur de type Date avec des millisecondes (`TYPE_MISMATCH`). C'est le comportement natif de ClickHouse ; une telle valeur ne pourrait de toute façon jamais être égale. `insert().values()` envoie chaque `Date` sous forme d'un entier représentant des unités de temps à la précision de la colonne. Voir Écritures.

### Exemples de types de colonnes

Structures de schéma courantes et valeurs correspondantes en TypeScript :

| Structure ClickHouse | Schéma | Structure des valeurs TypeScript |
| --- | --- | --- |
| énumération | `ckType.enum8({ idle: 1, active: 2 })` | `"idle" | "active"` (inférée à partir des clés) |
| chaîne de caractères à faible cardinalité | `ckType.lowCardinality(ckType.string())` | `string` |
| décimal qui accepte NULL | `ckType.nullable(ckType.decimal({ precision: 18, scale: 5 }))` | `string | null` |
| tableau | `ckType.array(ckType.string())` | `string[]` |
| élément de tableau qui accepte NULL | `ckType.array(ckType.nullable(ckType.string()))` | `(string | null)[]` |
| tuple | `ckType.tuple(ckType.string(), ckType.int32())` | `[string, number]` |
| table associative | `ckType.map(ckType.string(), ckType.string())` | `Record<string, string>` |
| tableau d'objets imbriqués | `ckType.nested({ component: ckType.string(), quantity: ckType.float64() })` | `{ component: string; quantity: number }[]` |
| variante | `ckType.variant(ckType.string(), ckType.int32())` | `string | number` |
| JSON | `ckType.json<{ risk?: { score?: number } }>()` | `{ risk?: { score?: number } }` |

`ckType.map(...)` n'accepte actuellement que les clés `String` et les représente par un objet JavaScript ; il ne modélise donc pas le cas limite des clés en double de `Map(K, V)` dans ClickHouse.

Les lignes à insérer utilisent la même structure inférée que `typeof table.$inferInsert`, mais vous pouvez omettre les colonnes qui ont une valeur par défaut ClickHouse ou une expression générée lorsque vous appelez `insert(table).values(...)`. Les colonnes `MATERIALIZED` et `ALIAS` n'apparaissent jamais dans la liste de colonnes `INSERT` générée ; si vous les transmettez malgré tout, l'opération est rejetée.

## Configuration du client
<!-- src:26d96c74 -->

Créez un client avec `clickhouseClient()` :

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:<password>@127.0.0.1:8123/telemetry_lab",
});
```

### Modes de connexion

`clickhouseClient()` prend en charge deux modes de connexion mutuellement exclusifs.

#### `databaseUrl`

Utilisez `databaseUrl` si vous voulez fournir une seule chaîne de connexion :

```ts
const db = clickhouseClient({
  databaseUrl: "http://default:secret@127.0.0.1:8123/telemetry_lab",
});
```

Si `databaseUrl` est présent, ne renseignez pas non plus :

- `host`
- `database`
- `username`
- `password`
- `pathname`

#### Champs de connexion structurés

Utilisez des champs explicites si vous voulez configurer chaque élément séparément :

```ts
const db = clickhouseClient({
  host: "http://127.0.0.1:8123",
  database: "telemetry_lab",
  username: "default",
  password: "<password>",
});
```

Valeurs par défaut du mode structuré :

- `host` : `http://localhost:8123`
- `database` : `default`
- `username` : `default`
- `password` : `""`

### Champs courants

La plupart des projets n'ont besoin que de quelques champs du client :

| Champ | Rôle |
| --- | --- |
| `request_timeout` | Délai maximal facultatif par requête, en millisecondes. Si vous ne le définissez pas, ck-orm n'applique aucun délai côté client : la durée de la requête dépend du fetch sous-jacent, de la plateforme, du serveur ou de `abort_signal`. Pour les requêtes en diffusion continue de longue durée, laissez ce champ vide et imposez plutôt une limite de temps via `abort_signal` (par exemple `AbortSignal.timeout(30_000)`). |
| `clickhouse_settings` | Paramètres par défaut de session et de requête ClickHouse |
| `application` | Définit le nom de l'application ClickHouse |

Les définitions de tables ne dépendent pas de la configuration de connexion. Définissez les tables avec `ckTable(...)`, importez ces objets là où vous composez les requêtes, puis transmettez-les directement à `.from(...)`, `.insert(...)` et aux fonctions utilitaires pour les tables temporaires.

### Champs avancés

Utilisez ces champs pour configurer les comportements avancés à l'exécution :

| Champ | Rôle |
| --- | --- |
| `fetch` | `(request: Request) => Response \| Promise<Response>` est appelée pour chaque requête HTTP : vous pouvez fournir n'importe quelle implémentation de fetch, un gestionnaire côté serveur ou un double de test comme celui de [`examples/decimal-sum/main.test.ts`](../examples/decimal-sum/main.test.ts). Par défaut, c'est le `fetch` de la plateforme |
| `http_headers` | En-têtes par défaut supplémentaires |
| `role` | Rôle ou rôles ClickHouse par défaut |
| `session_id` | Identifiant de session par défaut |
| `session_max_concurrent_requests` | Nombre maximal de requêtes simultanées autorisées pour une même `session_id` dans ce client (`1` par défaut) |
| `compression.response` | Avec la valeur par défaut `false`, les requêtes en diffusion continue envoient `enable_http_compression = 0` pour que les lignes arrivent au fil de leur production par ClickHouse. Les requêtes qui ne diffusent pas en continu conservent la valeur par défaut du serveur (ClickHouse 26.3+ compresse une réponse quand le client l'accepte). Avec `true`, toutes les réponses sont compressées, ce qui peut retenir un flux peu dense jusqu'à ce qu'un bloc de compression soit rempli |
| `logger` / `logLevel` | Intégration du logger |
| `plugins` | Plugins du cycle de vie des requêtes, comme `otelTracing()` de `ck-orm/otel` |

Les paramètres qui contrôlent la durée de vie des sessions sont volontairement limités à la requête. Transmettez `session_timeout` à une seule requête ou à `runInSession(...)`. Utilisez `session_check` lorsque vous poursuivez une `session_id` existante, et non pour démarrer une toute nouvelle session.
`session_max_concurrent_requests` est différent : c'est un garde-fou au niveau du client qui limite les requêtes simultanées ciblant la même `session_id`.
Les vraies sessions ClickHouse restent verrouillées côté serveur. Définir `session_max_concurrent_requests` à une valeur supérieure à `1` peut donc déclencher `SESSION_IS_LOCKED` au lieu d'offrir un véritable parallélisme dans la même session.
Gardez la valeur par défaut `1`, sauf si vous voulez vraiment supprimer la sérialisation locale et êtes prêt à gérer les échecs de verrouillage de session côté serveur.

### Paramètres ClickHouse

`clickhouse_settings` sert uniquement aux paramètres de session et de requête ClickHouse : ce sont les mêmes clés que celles documentées dans [Paramètres de session](https://clickhouse.com/docs/operations/settings/settings) et acceptées par l'API HTTP comme paramètres de requête. Cette option est distincte de la configuration du client ck-orm, notamment de `host`, `database`, `request_timeout`, `http_headers` et `session_max_concurrent_requests`.

Conservez les champs de transport HTTP comme `query`, `database`, `session_id`, `role` et `param_*` en dehors de `clickhouse_settings` ; ck-orm valide ces clés séparément, car elles appartiennent à l'enveloppe de requête et au canal des paramètres nommés.

TypeScript propose l'autocomplétion pour les clés officielles, tandis que les clés arbitraires restent valides pour les versions plus récentes de ClickHouse ou les paramètres propres à votre déploiement :

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

Le transport fetch gère l'analyse et la sérialisation JSON. La configuration publique du client garde ce comportement cohérent d'un environnement d'exécution à l'autre au lieu d'exposer une option `json` pour le remplacer.

ck-orm impose à chaque requête un petit ensemble de paramètres du contrat d'échange : `http_write_exception_in_output_format = 0`, `output_format_json_quote_64bit_integers = 1`, `output_format_json_quote_decimals = 1` et `date_time_output_format = "iso"`. Si les paramètres transmis par l'appelant entrent en conflit avec ces valeurs, ck-orm journalise un avertissement une seule fois et envoie la valeur imposée ; les paramètres de charge de travail ou de performance comme `max_threads` restent sous le contrôle de l'appelant.

### Authentification

`ck-orm` utilise l'authentification basique pour les connexions à la base de données.

- en mode `databaseUrl`, les identifiants d'authentification peuvent être inclus dans l'URL
- en mode structuré, utilisez `username` et `password`
- si aucun identifiant n'est fourni, la valeur par défaut est `default` avec un mot de passe vide

## Constructeur de requêtes
<!-- src:fc28245d -->

Les exemples ci-dessous supposent que `db`, `probeTelemetry` et les fonctions utilitaires citées sont importés depuis `ck-orm`.

### `select()`

Une sélection explicite vous renvoie un résultat dont la structure est clairement définie :

```ts
const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
    signalStrength: probeTelemetry.signalStrength,
  })
  .from(probeTelemetry)
  .limit(10);
```

Les objets de projection sont construits à partir de valeurs publiques de type `Selection` ou de colonnes. Concrètement, les colonnes de table, les résultats de `fn.*(...)` et de `ck.expr(...)` se combinent tous de la même façon dans `select({ ... })`.

La sélection implicite renvoie le modèle complet de la table quand il n'y a pas de jointure :

```ts
const rows = await db.select().from(probeTelemetry).limit(10);
```

Avec des jointures, la sélection implicite regroupe les champs par source et renvoie des objets imbriqués. Un groupe sans correspondance dans une jointure à gauche devient `null`. Pour les colonnes `Array` / `Map`, ClickHouse renvoie `[]` / `{}` au lieu de NULL ; pour cette vérification, un conteneur vide compte donc comme absent. Une ligne correspondante dont tous les champs scalaires valent NULL et dont les tableaux et les tables associatives sont vides ne se distingue pas d'un groupe sans correspondance ; elle devient elle aussi `null`.

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

`asofJoin()` et `asofLeftJoin()` produisent `ASOF JOIN` et `LEFT ASOF JOIN` de ClickHouse. Pour chaque ligne de gauche, ils joignent la seule ligne de droite dont la valeur est la plus proche, au lieu de toutes les lignes correspondantes. La condition combine, avec `ck.and(...)`, une ou plusieurs conditions d'égalité et exactement un `ck.gt`, `ck.gte`, `ck.lt` ou `ck.lte` entre une colonne de gauche et une colonne de droite :

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

- `ck.gte(left, right)` retient la dernière ligne de droite à la valeur de gauche ou avant, et `ck.gt` la dernière strictement avant. `ck.lte` et `ck.lt` retiennent la première ligne de droite à la valeur de gauche ou après, ou strictement après
- `asofLeftJoin()` suit la [sémantique des valeurs nulles de `leftJoin()`](#join-null-semantics) : il envoie `join_use_nulls = 1` et type le côté droit comme nullable, donc une ligne de gauche sans correspondance reçoit `null`. `asofJoin()` écarte ces lignes, comme `innerJoin()`
- la colonne de la correspondance la plus proche doit avoir un type entier, flottant, `Date`, `DateTime` ou `Decimal`, et les deux côtés de cette comparaison doivent désigner des colonnes ; une comparaison avec une valeur ne compte pas. Les filtres d'égalité supplémentaires, comme `ck.eq(quotes.symbol, "BTCUSD")`, sont autorisés
- ck-orm n'inspecte pas la condition. ClickHouse rejette une condition sans inégalité, avec une seconde inégalité ou avec `ck.or(...)` avant de lire des données ; l'erreur est un `ClickHouseORMError` avec `kind: "request_failed"` et `clickhouseName: "INVALID_JOIN_ON_EXPRESSION"`. Une condition sans égalité et une colonne de correspondance la plus proche d'un autre type sont rejetées aussi, et le nom d'erreur en l'absence d'égalité diffère selon les versions de ClickHouse
- les jointures ASOF s'exécutent avec le `join_algorithm` par défaut et avec `hash`, `parallel_hash` ou `full_sorting_merge`. `partial_merge`, `grace_hash` et `direct` échouent avec `NOT_IMPLEMENTED`

### `where()` et fonctions utilitaires pour les conditions

Fonctions utilitaires publiques pour les conditions :

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

`.where(...predicates)` prend un nombre variable de prédicats et les combine avec `AND`. Il ignore les prédicats qui valent `undefined`. Vous pouvez donc transmettre plusieurs prédicats directement ou les regrouper avec `ck.and(...)` et `ck.or(...)`.

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

Une liste de valeurs est envoyée dans un seul paramètre de requête `Array(T)` (`col in {p:Array(T)}`). Les listes dont le texte dépasse environ 40 000 caractères sont découpées en plusieurs paramètres réunis par `arrayConcat(...)`. L'élagage par clé primaire et par index de saut reste inchangé. ClickHouse 26.3+ accepte au maximum 1 000 paramètres de requête dans chaque requête HTTP, et l'analyseur de la version 25.8 rejette une liste combinée supérieure à environ 1 Mo ; pour des ensembles plus volumineux, utilisez une table temporaire ou des données externes. Une liste contenant des expressions SQL ou des valeurs non typées de types différents utilise toujours un paramètre par élément.

`ck.and(...)` ignore `undefined`, ce qui facilite la construction de filtres dynamiques directement dans la requête :

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

`undefined` n'est ignoré que dans les prédicats. Les valeurs passées aux prédicats sont contrôlées strictement : `ck.eq(column, undefined)` lève une erreur au lieu d'élargir silencieusement la requête. Pris seuls, `null`, `false`, `true`, `0` et `""` ne sont pas des prédicats ; utilisez plutôt une expression SQL. `null` reste une valeur de données valide pour les colonnes Nullable et les écritures, mais le filtrage sur NULL doit être explicite :

```ts
db.select()
  .from(probeTelemetry)
  .where(ck.isNull(probeTelemetry.deletedAt));
```

Les colonnes booléennes restent de simples valeurs et expressions SQL :

```ts
const healthChecks = ckTable("health_checks", {
  id: ckType.int32(),
  isPassing: ckType.bool("is_passing"),
});

db.select().from(healthChecks).where(ck.eq(healthChecks.isPassing, false));
db.select().from(healthChecks).where(healthChecks.isPassing);
```

Ne placez pas `null` seul dans des comparaisons ordinaires ni dans des listes `IN`. Composez explicitement la logique liée à NULL :

```ts
ck.or(
  ck.inArray(probeTelemetry.deletedAt, [new Date("2026-04-21T00:00:00.000Z")]),
  ck.isNull(probeTelemetry.deletedAt),
);
```

Pour les filtres plus volumineux construits à l'exécution, préférez `Predicate[]` et la forme variadique `.where(...predicates)` :

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

`Predicate` est le nom public des clauses SQL booléennes réutilisables. Vous pouvez réutiliser ces prédicats dans `where`, `having`, les clauses `on` des jointures et les fonctions utilitaires qui acceptent des conditions booléennes, comme `ck.exists(...)`.

`Selection` est le nom public des valeurs calculées réutilisables produites par le constructeur, comme `fn.sum(...)`, `fn.toString(...)` et `ck.expr(ckSql...)`. Utilisez `.as(...)` pour leur donner un alias et `.mapWith(...)` pour remplacer le décodage. `Order` est l'objet de clause que renvoient `ck.asc(...)` et `ck.desc(...)`.

`ck.has(...)`, `ck.hasAll(...)` et `ck.hasAny(...)` correspondent directement aux fonctions natives de ClickHouse et en conservent la sémantique pour les tableaux, les tables associatives et JSON. Quand le côté gauche est une colonne de tableau, les valeurs JavaScript simples passent d'abord par l'encodage de cette colonne : les filtres `Array(Date)` utilisent donc l'encodage Date de la colonne avant de devenir des paramètres de requête.

`where(...)` est variadique, alors que `having(...)` ne prend qu'un seul prédicat. Pour un `having` qui contient plusieurs clauses, composez d'abord le prédicat avec `ck.and(...)` ou `ck.or(...)`.

`ck.contains(...)`, `ck.startsWith(...)`, `ck.endsWith(...)` et leurs variantes `*IgnoreCase` traitent l'entrée comme du texte littéral. Elles paramètrent la valeur et échappent en interne les caractères génériques LIKE (`%`, `_`, `\`).

N'utilisez `ck.like(...)` / `ck.ilike(...)` que si vous avez besoin de la sémantique complète des motifs. Ces API paramètrent malgré tout les valeurs pour garantir la sécurité SQL, mais `%` et `_` gardent leur rôle de caractères génériques, car LIKE est un langage de motifs.

Exemple de recherche de texte littéral :

```ts
import { ck } from "ck-orm";

const rows = await db
  .select({
    probeId: probeTelemetry.probeId,
  })
  .from(probeTelemetry)
  .where(ck.contains(probeTelemetry.probeId, "probe_alpha%"));
```

Exemple de motif avancé :

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

`groupBy()` et `limitBy([...])` acceptent des colonnes et des valeurs `Selection` calculées par des fonctions utilitaires comme `fn.*(...)` ou `ck.expr(...)`.

Les valeurs primitives de `limit()`, `offset()` et `limitBy(..., limit)` doivent être des entiers sûrs non négatifs. Utilisez `ckSql` uniquement si vous avez réellement besoin d'une expression constante ClickHouse.

`orderBy()` accepte :

- `ck.desc(selection)`
- `ck.asc(selection)`
- une colonne directement

### `final()`

Ajoutez `FINAL` à une requête sur une table :

```ts
const query = db.select().from(probeTelemetry).final();
```

Pour une lecture simple d'une table sans alias, `ck-orm` émet `FROM table FINAL`. Quand la table racine porte un alias ou que la requête joint d'autres sources, `ck-orm` encapsule la table finalisée dans une sous-requête et conserve l'alias sur la source qui l'englobe. Cela évite les cas limites de l'analyseur ClickHouse liés à `FINAL`, aux alias de table, aux jointures et aux expressions lambda, tout en gardant la même API du constructeur.

`final()` ne s'applique qu'à une source racine de type table. Si vous avez besoin de `FINAL` dans une CTE, une sous-requête ou un flux basé sur une fonction de table, appelez `.final()` sur la requête qui cible la table avant d'appeler `.as(...)`.

### `limitBy()`

Utilisez `LIMIT ... BY ...` de ClickHouse :

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

### Modes d'exécution

Les requêtes construites peuvent s'exécuter de trois façons :

```ts
const query = db.select().from(probeTelemetry).limit(10);

const rows = await query;
const sameRows = await query.execute();

for await (const row of query.iterator()) {
  console.log(row);
}
```

Dans les exemples d'application, utilisez `.execute()` si vous voulez rendre l'exécution évidente au premier coup d'œil. Le `await query` direct est pris en charge pour garder une ergonomie proche de celle de Drizzle ; il est pratique une fois que l'équipe a l'habitude des requêtes du constructeur qui se comportent comme des semi-promesses.

`query.iterator()` suit les mêmes règles de concurrence liées aux sessions que `db.stream()` : si la requête vise une `session_id`, la place réservée à la requête reste occupée jusqu'à la fin de l'itération ou jusqu'à la fermeture anticipée de l'itérateur.

### Inspecter le SQL avec `toSQL()`

`toSQL()` compile un constructeur sans rien envoyer. Appelez-la sur le constructeur lui-même, car un await sur un constructeur l'exécute. Elle fonctionne avec les constructeurs `select()`, ainsi qu'avec `insert().values()` et `insert().fromSelect()`.

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

Le résultat contient ces champs :

- `sql` est l'instruction sur une seule ligne, avec un espace réservé comme `{orm_param1:String}` à la place de chaque valeur. Elle n'inclut pas la clause `FORMAT`, qui est ajoutée à l'envoi de la requête.
- `params` contient les valeurs JavaScript brutes des espaces réservés. Elles sont encodées et envoyées à part du SQL quand la requête s'exécute : elles ne sont donc jamais écrites dans `sql`.
- `forcedSettings` liste les paramètres que le constructeur impose à la requête, comme `join_use_nulls: 1` pour `leftJoin()`. Ce champ est absent s'il n'y en a aucun.
- `body` n'est défini que pour `insert().values()` : il contient les lignes en texte Values, tandis que `sql` contient l'instruction `INSERT INTO t (cols) FORMAT Values`. Il est absent dans tous les autres cas.

Le résultat est une copie : le modifier n'a aucun effet sur le constructeur.

Passez `{ pretty: true }` pour répartir `sql` et `body` sur plusieurs lignes, ce qui est pratique pour les journaux et le débogage :

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

La mise en forme ne modifie que les espaces blancs. Chaque clause démarre sur une nouvelle ligne, les éléments du `select` sont placés un par ligne, les sous-requêtes et les CTE sont indentées, et les conditions d'un groupe `and` ou `or` sont placées une par ligne. Les arguments d'une fonction restent sur leur ligne, le texte entre guillemets n'est jamais modifié, et une instruction qui contient des commentaires SQL est renvoyée telle quelle. Dans le corps d'un `insert().values()`, chaque ligne insérée occupe sa propre ligne de texte. La mise en forme n'a jamais d'effet sur la requête envoyée.

### Sous-requêtes et CTE

Utilisez `.as("alias")` pour transformer un constructeur en sous-requête explicitement nommée :

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

Pour les sous-requêtes, `.as("…")` est **facultatif** — si vous ne l'appelez pas, la bibliothèque attribue un alias automatique (`__sub_1`, `__sub_2`, …) au moment de la compilation. Vous pouvez utiliser directement un constructeur sans alias dans `from()`, `innerJoin()`, `leftJoin()`, `inArray()`, `notInArray()`, `exists()` et `notExists()`.

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

#### Guide de style — choisir entre `.as(name)` et un constructeur sans alias

| Situation | Forme recommandée |
|---|---|
| Sous-requête ponctuelle intégrée, sans besoin de la réutiliser | constructeur sans alias + `innerJoin/leftJoin` avec fonction de rappel, ou constructeur sans alias dans `inArray/exists` |
| Sous-requête dont les références de colonnes sont utilisées à plusieurs endroits (SELECT / WHERE / une jointure) | stockez-la dans une `const` et transmettez-la sans alias — ses références restent accessibles dans les méthodes de chaînage |
| Auto-jointure (même sous-requête logique, deux alias SQL) | appelez explicitement `.as("a")` et `.as("b")` — réutiliser la même instance deux fois comme source lève une erreur lors de la compilation SQL |
| Besoin d'un alias stable, lisible ou typé (recherche dans `query_log` en production, `EXPLAIN`, journaux des requêtes lentes, fonctions utilitaires partagées entre modules, clé source littérale `TAlias`) | appelez `.as("descriptive_name")` |

L'alias automatique n'est **pas** stable entre deux compilations de requêtes de structure différente, mais il reste stable dans une même compilation et entre plusieurs compilations du même arbre de constructeur — les tests de type `snapshot` n'ont donc pas besoin de normalisation.

#### Conflits de clés de sélection

Les clés de sélection qui portent le même nom qu'une méthode de SelectBuilder (`from`, `where`, `select`, `as`, `innerJoin`, `leftJoin`, `asofJoin`, `asofLeftJoin`, `groupBy`, `having`, `orderBy`, `limit`, `offset`, `final`, `limitBy`, `execute`, `iterator`, `then`, `catch`, `finally`, `buildSelectionItems`) conservent leur type de méthode sur le constructeur sans alias. `sub.from` renvoie toujours la méthode du constructeur, jamais une référence de colonne — même si `from` fait partie de la sélection. Renommez ces clés (par exemple, `from` → `fromAddress`) ou encapsulez la sous-requête avec `.as("name")` pour accéder aux colonnes via l'objet `Subquery` dédié.

Utilisez `$with()` et `with()` pour les CTE :

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

`$with()` sans nom construit une CTE anonyme. Une CTE anonyme ne peut servir de source qu'une seule fois par requête ; pour joindre deux fois le même résultat, définissez deux CTE, une par jointure (deux appels `db.$with().as(...)`, ou deux CTE nommées différemment).

### `db.count()`

Utilisez `db.count(source, ...predicates)` comme fonction utilitaire de comptage, à la manière de Drizzle. Elle applique les mêmes règles aux prédicats que `.where(...predicates)` : plusieurs prédicats sont combinés avec `AND` et les objets de prédicat `undefined` sont ignorés. Une valeur `undefined` passée à une fonction utilitaire comme `ck.eq(...)` lève toujours une erreur.

```ts
import { ck } from "ck-orm";

const total = await db.count(
  probeTelemetry,
  ck.eq(probeTelemetry.status, 1),
  ck.gt(probeTelemetry.id, 1000),
);
```

Pour les jeux de résultats plus complexes, comptez une sous-requête ou une CTE :

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

Par défaut, `db.count(...)` utilise le mode de comptage numérique : il produit `toFloat64(count())` et décode le résultat en `number`. Les très grands nombres peuvent dépasser la précision des entiers JavaScript ; si vous avez besoin d'un résultat exact ou de conserver le format transmis, utilisez les modes chaînables :

```ts
const approximateTotal = await db.count(activeProbes); // number
const exactTotal = await db.count(activeProbes).toSafe(); // string
const wireTotal = await db.count(activeProbes).toMixed(); // number | string
```

`.toSafe()` produit `toString(count())` et sert aux lectures exactes. Si vous utilisez ce résultat comme expression SQL, il suit la sémantique de `String` ; pour les comparaisons SQL numériques, utilisez le mode par défaut, `.toUnsafe()` ou `.toMixed()`. `.toMixed()` produit `toUInt64(count())` et préserve le format renvoyé par le pilote et transmis sur le réseau. Avec les paramètres JSON par défaut de ck-orm, qui préservent les entiers de 64 bits sans perte, les réponses effectivement renvoyées par ClickHouse prennent généralement la forme de `string`.

## Sémantique des valeurs nulles dans les jointures
<!-- src:59e7a4e2 -->

Par défaut, `leftJoin()` et `asofLeftJoin()` utilisent la sémantique SQL des valeurs nulles en appliquant automatiquement `join_use_nulls = 1`.

Cela signifie :

- le côté droit d'une jointure à gauche par défaut est inféré comme pouvant contenir NULL
- si vous désactivez explicitement `join_use_nulls`, les types inférés changent eux aussi

Pour retrouver le comportement par défaut des jointures de ClickHouse, transmettez le littéral `0`. Le littéral est inféré : `as const` n'est donc pas nécessaire. Une valeur typée simplement comme `number`, par exemple issue d'une variable, donne quand même `| null`.

```ts
const rawDefaultDb = db.withSettings({
  join_use_nulls: 0,
});
```

Avec une jointure à gauche sur un constructeur sans alias (une sous-requête sans `.as()`) ou sur une CTE anonyme, chaque référence de colonne provenant de ces sources est typée en `| null`, y compris les références du côté `FROM` ou d'un constructeur sans alias utilisé dans une jointure interne. Ces références partagent une seule clé de source `string`, assez large : les types ne peuvent donc pas indiquer quelle source a été jointe à gauche. À l'exécution, seul le côté joint à gauche renvoie NULL.

```ts
const rows = await db
  .select({ uid: allUsers.uid, spent: totals.spent })
  .from(allUsers)
  .leftJoin(totals, ck.eq(allUsers.uid, totals.uid));
// uid and spent are both number | null
```

Ajoutez `.as("name")` à la sous-requête du côté `FROM` ou utilisée dans une jointure interne. Ses références conservent alors leur propre clé de source et restent non nulles, tandis que le côté joint à gauche reste `| null`.

Le paramètre imposé `join_use_nulls = 1` est conservé lorsqu'une requête avec jointure est réutilisée comme sous-requête, CTE, source de `ck.exists(...)` ou de `ck.inArray(...)` : les types du constructeur restent ainsi alignés sur le comportement à l'exécution.

À l'exécution, les expressions calculées suivent la propagation de NULL de ClickHouse : quand ClickHouse renvoie NULL (une entrée NULL ou une ligne du côté joint à gauche sans correspondance), la valeur vaut `null`. Leur type TypeScript correspond au `TData` que vous déclarez (ou au type fixe de la fonction utilitaire) et ne s'élargit pas automatiquement ; déclarez `| null` (par exemple `fn.plus<number | null>(...)`) ou encapsulez l'entrée avec `fn.coalesce(...)` lorsqu'une entrée peut valoir NULL.

## Écritures
<!-- src:5d78750d -->

### `insert(table).values(...)`

Utilisez le constructeur pour des insertions typées conformes au schéma de la table :

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

Les lignes sont envoyées dans une seule requête `INSERT INTO t (cols) FORMAT Values`. Elle ne contient pas de paramètres de requête : les lignes sont du texte Values littéral dans le corps de la requête, donc les limites HTTP de 1 000 paramètres et de 128 KiB par valeur ne s'appliquent pas. Une colonne omise utilise sa valeur `DEFAULT` uniquement pour cette ligne. L'évaluation des expressions est désactivée pour cette insertion (`input_format_values_interpret_expressions = 0` et `input_format_values_deduce_templates_of_expressions = 0`) : chaque cellule est traitée comme une donnée. Utilisez `insert().fromSelect()` pour calculer des valeurs. Un `Date` JavaScript est envoyé à l'instant exact, tronqué à la précision de la colonne ; les colonnes `DateTime` conservent les secondes entières. Un nombre dans une colonne `DateTime` ou `DateTime64` représente des secondes Unix. Une chaîne de caractères est lue selon le fuseau horaire de la colonne.

Les lignes insérées doivent utiliser les clés du schéma de la table. Les clés inconnues sont rejetées dès le départ.

### `insert(table).fromSelect(select)`

Utilisez `.fromSelect(...)` pour compiler une seule instruction `INSERT INTO target (cols) SELECT ...` et laisser ClickHouse matérialiser le SELECT côté serveur. Aucune ligne ne passe par le processus Node. C'est idéal pour appliquer un filtre à une grande table source une seule fois, puis réutiliser la tranche matérialisée dans plusieurs requêtes en aval dans `runInSession`.

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

Règles :

- Le SELECT doit projeter chaque colonne obligatoire de la table cible ; les colonnes déclarées avec `.default(...)` peuvent être omises. Les colonnes obligatoires manquantes sont rejetées à la compilation (TypeScript), puis à nouveau à l'exécution.
- La liste des colonnes INSERT suit l'ordre des clés de la projection SELECT. Côté appelant, la sémantique est donc alignée sur les noms (`name-aligned`), même si ClickHouse aligne les colonnes par position. L'ordre des clés de projection ne détermine pas quelle colonne cible reçoit quelle valeur : ce sont les clés elles-mêmes qui font la correspondance.
- Les clés de projection inconnues (absentes de la table cible) et les incohérences de type par colonne sont signalées par TypeScript avant l'envoi de la requête.
- `.values()` et `.fromSelect()` sont mutuellement exclusives sur une même chaîne d'insertion : le type du constructeur se restreint, ce qui fait disparaître la méthode inutilisée de l'autocomplétion, et l'exécution rejette aussi l'appel.
- La requête compilée envoyée est une simple commande ClickHouse : l'événement de requête contient `mode: "insert"` et `tableName` est défini sur la table cible.
- `.fromSelect()` accepte un `SelectBuilder` (`session.select({...}).from(...)`), pas une sous-requête encapsulée. Utilisez `.as("name")` uniquement si vous avez besoin du résultat comme CTE ou sous-requête dans un autre SELECT, pas pour le transmettre à `.fromSelect()`.

#### Colonnes imbriquées

Les colonnes `ckType.nested(...)` fonctionnent avec `.fromSelect()` selon trois modes :

1. **Omettez la clé imbriquée de la projection.** C'est le mode par défaut, qui reflète le contrat d'exécution de ck-orm : une valeur imbriquée manquante est encodée comme `DEFAULT` SQL, et ClickHouse remplit des tableaux parallèles vides pour chaque champ. La couche TS traite les colonnes imbriquées comme `optional` dans `$inferInsert` pour refléter ce comportement.
2. **Projetez une référence directe de colonne imbriquée**, par exemple `select({ events: src.events })`. ck-orm encapsule le SELECT interne dans une sous-requête et réécrit la projection externe en accès à chaque champ par son chemin en notation pointée (`__ck_inner.events.name`, `__ck_inner.events.score`, …). La structure imbriquée source doit contenir chaque champ attendu par la structure imbriquée cible.
3. **La projection d'une expression calculée dans une colonne imbriquée** est rejetée à la compilation : une seule expression SQL ne peut pas se répartir entre les champs des tableaux parallèles. Utilisez `.values(...)` ou `.insertJsonEachRow(...)` pour les valeurs ligne par ligne.

Si vous voulez que la couche TS impose que les données imbriquées soient toujours fournies (`Pattern D` — contrat métier qui interdit les tableaux imbriqués vides), activez `ckType.nested({...}).requiredOnInsert()`. Le chaînage renvoie une colonne dont la structure `$inferInsert` liste `events` comme obligatoire, et `.fromSelect()` exige alors soit le mode de projection d'une référence de colonne imbriquée, soit l'alternative ligne par ligne.

> **Remarque sur le format d'échange JSON** : `.values()` envoie une colonne JSON sous forme de texte `JSON.stringify(...)` entre guillemets dans le corps Values. ClickHouse analyse ce littéral pour remplir la colonne JSON. `.fromSelect()` s'exécute entièrement côté serveur : les valeurs JSON ne transitent donc jamais par le processus JS. Les deux approches produisent les mêmes données stockées.

### `insertJsonEachRow()`

Utilisez `insertJsonEachRow()` si vous avez déjà des lignes sous forme d'objets ou un flux de lignes asynchrone :

```ts
await db.insertJsonEachRow("tmp_scope", [
  { probe_id: "probe_alpha" },
  { probe_id: "probe_beta" },
]);
```

Cette fonction accepte :

- un nom de table sous forme de chaîne
- un objet de table créé par `ckTable()`
- un tableau ordinaire
- un `AsyncIterable`

Un tableau ordinaire vide ne fait rien côté client, mais le cycle d'insertion signalé aux hooks des plugins reste réussi avec `rowCount: 0`. ClickHouse contrôle le comportement des champs inconnus ; transmettez des paramètres comme `input_format_skip_unknown_fields: 1` si vous voulez que le serveur ignore les champs JSON supplémentaires. Les colonnes imbriquées sont envoyées sous forme de tableaux `column.field` aplatis (ClickHouse `flatten_nested = 1`, la valeur par défaut) : `{ lines: [{ sku: "a" }] }` fonctionne donc avec `insert().values()` et `insertJsonEachRow()`.

Avec un schéma de table, `insertJsonEachRow()` envoie un `Date` JavaScript dans une colonne `DateTime`/`DateTime64` avec exactement les mêmes ticks entiers que `insert().values()`, et confie les chaînes à l'analyseur par défaut de ClickHouse, `date_time_input_format = basic` : les chaînes de date et d'heure sont donc interprétées de la même façon dans les deux cas. Un nombre JSON que vous transmettez vous-même dans une colonne `DateTime64` est toujours lu comme des ticks par `JSONEachRow` et comme des secondes Unix par `.values()`.

Quand vous passez seulement un nom de table, `insertJsonEachRow("table_name", rows)` ne connaît pas les types de colonnes : un `Date` est envoyé au format ISO 8601 avec `Z`, et ck-orm définit `date_time_input_format = best_effort` pour cette requête. La même règle s'applique aux clés d'une ligne issue d'une table avec schéma mais absentes de ce schéma : ck-orm les transmet telles quelles. Si l'une de ces valeurs est un `Date`, ajoutez donc `clickhouse_settings: { date_time_input_format: "best_effort" }` à l'appel. Sur ClickHouse 25.8, l'analyseur `best_effort` interprète mal les instants antérieurs à 1970 qui contiennent des fractions de seconde et rejette les chaînes `Time` comme `12:34:56` ; les colonnes typées par schéma évitent ces deux problèmes.

## SQL brut
<!-- src:f47b381e -->

`ck-orm` propose sa propre API de modèles SQL. Utilisez-la quand la syntaxe du constructeur vous ferait faire un détour par rapport au SQL à écrire.

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

### `ckSql.join()` et `ckSql.identifier()`

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

### SQL brut avec `query_params`

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

ck-orm choisit automatiquement le transport des paramètres. Vous n'avez pas besoin de configurer la gestion multipart pour `query_params`.

Les clés de `query_params` qui commencent par `orm_param` sont rejetées. Ce préfixe est réservé aux paramètres générés en interne par `` ckSql`...` ``.

Le formateur de valeurs accepte les valeurs primitives, `Date`, `NaN`, `Infinity`, les tableaux, les objets et les valeurs `Map` pour les espaces réservés typés de ClickHouse comme `{ids:Array(UInt64)}` ou `{attrs:Map(String, String)}`. Les valeurs transmises par `query_params` n'ont pas leur propre type de colonne : un tableau est donc toujours envoyé sous forme de texte Array. Les espaces réservés `Tuple(...)` ne sont pas pris en charge ici. Un entier de 64 bits dans un tableau doit être un `bigint` ; une chaîne est mise entre guillemets puis rejetée si le type d'élément est numérique. Un `Date` est envoyé sous forme de texte représentant l'heure UTC, que ClickHouse lit dans le fuseau horaire du type de l'espace réservé : déclarez-le comme `{at:DateTime64(3, 'UTC')}` pour éviter que le fuseau horaire de la session ou du serveur ne le décale ; l'interpolation `` ckSql`...${date}` `` s'en charge pour vous. Utilisez le type d'espace réservé `Identifier` de ClickHouse quand le paramètre est un nom de table ou de colonne :

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

Utilisez `ck.expr()` pour encapsuler un fragment SQL dans une `Selection` réutilisable :

```ts
import { ck, ckSql } from "ck-orm";

const query = db.select({
  constantOne: ck.expr(ckSql`1`).as("constant_one"),
});
```

Quand le fragment est typé, `ck.expr()` conserve ce type. Un fragment non typé reste volontairement `unknown` ; transmettez un type générique ou un décodeur si vous connaissez son résultat en dehors du schéma :

```ts
const status = ck.expr(ckSql<string>`'closed'`); // Selection<string>
const enabled = ck.expr<boolean>(ckSql`1`, {
  decoder: (value) => Number(value) === 1,
});
```

### Formats des requêtes brutes

Les requêtes brutes qui récupèrent le résultat d'un coup ne prennent en charge que la sortie `JSON` :

```ts
const rows = await db.execute(ckSql`select 1`, {
  format: "JSON",
});
```

Les requêtes brutes en diffusion continue ne prennent en charge que la sortie `JSONEachRow`. Avec la valeur par défaut `compression.response: false`, le flux est envoyé sans compression pour que chaque ligne arrive au fil de sa production par ClickHouse :

```ts
for await (const row of db.stream(ckSql`select 1`, {
  format: "JSONEachRow",
})) {
  console.log(row);
}
```

### Précision de Decimal dans les expressions

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

- `sum` / `sumIf` élargissent P à ≥ 38 ; `min` / `max` conservent le P de la colonne. Pour les colonnes Decimal du schéma, `fn.sum()` et `fn.sumIf()` sont des `Selection<string>` ; les colonnes Float sont des `Selection<number>`. La conversion automatique s'applique aussi via `nullable(decimal(...))` et `lowCardinality(decimal(...))`.
- `maxIf(value, condition)` suit, pour Decimal, la même règle que `max` : un résultat Decimal est une `Selection<string>` avec la structure `Decimal(P, S)` déclarée.
- `avg` n'est **pas** converti automatiquement — ClickHouse calcule `avg(Decimal)` sur Float64, donc `fn.avg` renvoie `Selection<number>`. Pour calculer une moyenne Decimal exacte, utilisez `ckSql.decimal(ckSql\`sum(x) / count(x)\`, P, S)`.
- `fn.divideDecimal(left, right, resultScale?)` correspond directement à `divideDecimal` de ClickHouse. Cette fonction renvoie `Selection<string>` pour ne perdre aucune précision. Si vous fournissez `resultScale`, sa valeur doit être un entier littéral de `0` à `76` et est insérée directement dans la requête ; le constructeur ne vérifie pas la version et n'émule pas la fonction ClickHouse.
- `column.cast(P, S)` convertit la colonne, pas l'agrégat : l'utiliser seul dans `GROUP BY` lève `NOT_AN_AGGREGATE`. Utilisez `fn.sum(column)` ou encapsulez l'agrégat.
- Les insertions rejettent les objets qui ne sont ni des chaînes ni des nombres (par exemple des instances brutes de `decimal.js`) — transmettez `.toFixed(scale)` :

```ts
db.insert(measurementLog).values({ value: new Decimal("1.23").toFixed(5) }); // ✅
db.insert(measurementLog).values({ value: new Decimal("1.23") as never }); // ❌ throws
```

## Expressions de fenêtre
<!-- src:6d0b3f76 -->

Encapsulez une expression de fonction `fn.*` avec `fn.over(...)`. La spécification facultative n'accepte que des sélections du constructeur pour `partitionBy` et les valeurs normales `ck.asc(...)` / `ck.desc(...)` pour `orderBy` :

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

Par défaut, `fn.rowNumber()` renvoie un `Selection<number>` non sûr via `toFloat64`. Appelez `.toSafe()` avant `fn.over(...)` pour obtenir un `Selection<string>` exact, ou `.toMixed()` pour conserver la forme de transport native `UInt64` (`Selection<number | string>`) ; `.toUnsafe()` correspond explicitement au comportement par défaut. Le constructeur n'effectue volontairement aucune vérification des entiers sûrs de JavaScript : la conversion `toFloat64` de ClickHouse peut arrondir les valeurs supérieures à `Number.MAX_SAFE_INTEGER` ; `.toSafe()` conserve une chaîne décimale exacte et `.toMixed()` préserve la forme native du résultat renvoyée par le pilote. Les agrégats Decimal conservent leur décodeur habituel des chaînes et sont compilés comme `CAST(sum(...) OVER (...) AS Decimal(...))` : `OVER` reste ainsi dans l'agrégat plutôt qu'après la conversion. `fn.over` n'accepte volontairement ni une colonne seule ni un fragment SQL brut ; cette première surface d'API ne couvre que `PARTITION BY` et `ORDER BY`. Utilisez `ckSql` pour les cadres de fenêtre, les fenêtres nommées ou les autres clauses propres à ClickHouse.

## Fonctions et fonctions de table
<!-- src:1e16409b -->

### `fn`

Les fonctions utilitaires génériques, de conversion et d'agrégation, ainsi que celles liées à JSON, aux tuples et aux tables, comprennent notamment :

- `fn.call()`
- `fn.withParams()`
- `fn.replaceRegexpAll()` — remplacement typé de ClickHouse basé sur RE2 ; voir [Remplacement par regex](#regex-replacement)
- Les fonctions de conversion de type reprennent les conversions de type documentées par ClickHouse : `fn.cast()`, `fn.date()`, `fn.accurateCast*()`, `fn.reinterpret*()`, `fn.parseDateTime*()`, `fn.formatDateTime()`, `fn.formatRow*()`, `fn.toString*()`, `fn.toBool()`, `fn.toInt*()`, `fn.toUInt*()`, `fn.toFloat*()`, `fn.toBFloat16*()`, `fn.toDecimal*()`, `fn.toDecimalString()`, `fn.toFixedString()`, `fn.toDate*()`, `fn.toDateTime*()`, `fn.toTime*()`, `fn.toTime64*()`, `fn.toInterval*()`, `fn.toLowCardinality()`, `fn.toNullable()` et `fn.toUUID*()`.
- Les conversions d'entiers de 64 bits ou plus (`toInt64/128/256*`, `toUInt64/128/256*`, les fonctions d'horodatage Unix sur 64 bits) sont décodées en `string` ; les conversions vers des entiers de taille inférieure ou vers des nombres à virgule flottante sont décodées en `number`.
- Les arguments ClickHouse qui doivent être des littéraux sont validés et insérés directement dans le SQL lorsque c'est nécessaire : échelle de Decimal, précision de DateTime64/Time64, longueur de FixedString, chaînes de type cible et unités d'intervalle.
- `fn.toStartOfMonth()`
- `fn.toUnixTimestamp()` / `fn.toUnixTimestamp64Second()` / `fn.toUnixTimestamp64Milli()` / `fn.toUnixTimestamp64Micro()` / `fn.toUnixTimestamp64Nano()`
- `fn.fromUnixTimestamp()` / `fn.fromUnixTimestamp64Second()` / `fn.fromUnixTimestamp64Milli()` / `fn.fromUnixTimestamp64Micro()` / `fn.fromUnixTimestamp64Nano()`
- `fn.count()` / `fn.countIf()` — par défaut, le résultat est un `Selection<number>` encapsulé dans `toFloat64(count(...))`. Chaînez `.toSafe()` pour obtenir `Selection<string>` (`toString(count(...))`), `.toMixed()` pour `Selection<number | string>` (`toUInt64(count(...))`) ou `.toUnsafe()` pour revenir au comportement par défaut. Ces fonctions fonctionnent comme `db.count`. Les décodeurs vérifient que la valeur est un entier non négatif et rejettent `NaN`, les nombres négatifs, les booléens, etc. — voir [modes de `fn.count` / `fn.uniqExact`](#fncount--fnuniqexact-modes) pour des exemples.
- `fn.sum()` / `fn.sumIf()` / `fn.min()` / `fn.max()` / `fn.maxIf()` — convertissent automatiquement le résultat en `Decimal(P, S)` pour les colonnes Decimal ; voir [Précision de Decimal dans les expressions](#decimal-precision-in-expressions)
- `fn.over()` / `fn.rowNumber()` — composition de fonctions de fenêtre avec `PARTITION BY` et `ORDER BY` ; voir [Expressions de fenêtre](#window-expressions)
- `fn.divideDecimal()` — division Decimal exacte avec une échelle de résultat facultative incluse directement dans le SQL ; voir [Précision de Decimal dans les expressions](#decimal-precision-in-expressions)
- `fn.argMax()` / `fn.argMin()` — renvoient la valeur de `arg` sur la ligne qui maximise ou minimise `val`. Le type de retour suit le type de la colonne `arg` (sans encapsuler le résultat avec un CAST en Decimal ; ClickHouse conserve le type déclaré de la colonne).
- `fn.avg()` — renvoie `Selection<number>` (Float64), comme `avg(Decimal)` dans ClickHouse
- `fn.uniqExact()` — mêmes trois modes chaînables que `fn.count()` : par défaut, le résultat est un `Selection<number>` encapsulé dans `toFloat64(uniqExact(...))`, `.toSafe()` donne `Selection<string>` (`toString(uniqExact(...))`), `.toMixed()` donne `Selection<number | string>` (`toUInt64(uniqExact(...))`) et `.toUnsafe()` revient au comportement par défaut. Les décodeurs appliquent la même vérification des entiers non négatifs. Voir [modes de `fn.count` / `fn.uniqExact`](#fncount--fnuniqexact-modes).
- `fn.coalesce()`
- `fn.if()` / `fn.multiIf()` — fonctions conditionnelles. Toutes deux renvoient `Selection<unknown>`, car ClickHouse détermine le type du résultat côté serveur à partir de toutes les branches de valeur.
- `fn.greatest()` / `fn.least()` / `fn.nullIf()` — comparaisons ; `nullIf` encapsule automatiquement le sqlType dans `Nullable(...)` sans encapsuler à nouveau une entrée qui accepte déjà NULL.
- `fn.positionCaseInsensitive()` / `fn.positionCaseInsensitiveUTF8()` — recherche de sous-chaîne insensible à la casse ou en UTF-8. Elles renvoient `Selection<string>` (ClickHouse utilise `UInt64`, décodé en chaîne de caractères pour préserver la précision sur 64 bits, comme `fn.indexOf()`). Le `startPos` facultatif doit être de type `UInt*` : transmettez une colonne UInt ou `fn.toUInt64(n)` pour les littéraux ; un littéral numérique seul devient `Int64` et ClickHouse le rejette.
- `fn.jsonExtract()`
- `fn.tuple()`
- `fn.arrayJoin()`
- `fn.tupleElement()`
- `fn.not()`

### Types des branches conditionnelles

Le générique du résultat conditionnel a été supprimé : `fn.if<TData>(...)` et
`fn.multiIf<TData>(...)` ne compilent plus. Une ancre connue de type `Float64` ou
`UInt64` attribue, selon le contexte, un type aux branches littérales compatibles
qui représentent des entiers sûrs ; les littéraux `UInt64` doivent aussi être non
négatifs. Les expressions hétérogènes et le SQL brut ne sont jamais convertis
automatiquement : convertissez explicitement chaque branche de valeur.
`.mapWith(...)` définit uniquement le décodeur JavaScript ; il ne change pas le
type SQL. En particulier, avec `fn.over(fn.rowNumber().toMixed())`, une branche
de repli qui contient un entier sûr non négatif est compilée en `UInt64`.
`fn.if(...)` et `fn.multiIf(...)` sont scalaires et ne peuvent pas
être transmises directement à `fn.over(...)` ; une branche de valeur peut
toutefois être une sélection de fenêtre.

Quand il est impossible de prouver qu'un même type SQL s'applique à toutes les
branches de valeur, les métadonnées du résultat restent inconnues et le décodage
laisse la valeur telle quelle. Les résultats existants peuvent donc passer de
`number` à un `string` provenant du transport. Pour migrer, convertissez
explicitement chaque branche de valeur vers un seul type SQL, puis appliquez
`.mapWith(...)` si nécessaire. Les
métadonnées automatiques du résultat se limitent volontairement aux ancres
exactes `Bool`, `Int64`, `UInt64`, `Float64` et `String`. Les autres types
restent transmis tels quels, même si les métadonnées des branches
correspondent, car ClickHouse peut normaliser des types de stockage identiques
(`LowCardinality(String)` devient par exemple `String`).

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

`fn.coalesce(...)` ne déduit plus, selon le contexte, un type `Float32`, `BFloat16`
ou `Decimal` pour les valeurs de repli, ni pour un `bigint` dont la plage cible ne
peut pas être prouvée sûre. Convertissez explicitement ces valeurs de repli :

```ts
fn.coalesce(amount, fn.toDecimal64("0.00", 2));
```

### Remplacement par regex

`fn.replaceRegexpAll(haystack, pattern, replacement)` génère l'appel à la fonction
ClickHouse [`replaceRegexpAll`](https://clickhouse.com/docs/sql-reference/functions/string-replace-functions#replaceregexpall).
Elle renvoie `Selection<string>`, ou `Selection<string | null>` si une entrée
typée peut valoir null. Utilisez `String.raw` pour les antislashs RE2 et les
substitutions basées sur les captures :

```ts
const usDate = fn.replaceRegexpAll(
  orders.dateText,
  String.raw`^(\d{4})-(\d{2})-(\d{2})$`,
  String.raw`\2/\3/\1`,
);
// Selection<string>
```

Les arguments sont liés en tant que paramètres de requête. Les motifs suivent
la syntaxe RE2 de ClickHouse, la chaîne de remplacement accepte `\0`–`\9` et un
motif invalide continue de produire une erreur ClickHouse. Par prudence, une
entrée `ckSql` non typée accepte NULL ; écrivez `ckSql<string>` si vous savez que
le fragment ne peut pas valoir null.

### Agrégation de tuples typés

`fn.tuple()` conserve le type de chaque élément. La forme de `groupArray` à une
seule valeur conserve ce type de tuple et son décodeur, tandis que l'appel
unaire de `fn.arrayReverseSort()` conserve le même type d'élément du tableau :

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

La forme existante avec générique explicite reste disponible pour le SQL brut et
les cas dynamiques.

Les noms des fonctions utilitaires de tableau reprennent les intitulés canoniques
de la référence ClickHouse [Fonctions de tableau](https://clickhouse.com/docs/sql-reference/functions/array-functions).
Les noms qui ne sont disponibles que comme alias restent accessibles via
`fn.call(...)`, sans dupliquer l'API publique.

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

`fn.call(name, ...)` et `fn.withParams(name, ...)` valident `name` comme identifiant SQL avant la compilation. `ck.has(...)`, `ck.hasAll(...)`, `ck.hasAny(...)` et `ck.hasSubstr(...)` sont des raccourcis de prédicat adaptés aux filtres pour ces mêmes fonctions ClickHouse.

`fn.jsonExtract(json, returnType, ...path)` n'accepte que des types de retour `ckType.*`, pour que le type de retour ClickHouse et le décodeur TypeScript restent associés :

Les extraits JSON et de tableaux ci-dessous supposent que le schéma fictif `probeTelemetry` contient aussi les colonnes `metadata: JSON` et `tags: Array(String)`.

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

Les segments de chemin correspondent aux arguments de chemin JSON de ClickHouse. Utilisez des clés de type chaîne de caractères pour les champs d'objet et des index de type number ou bigint pour les positions dans un tableau :

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

Les fonctions de tableau d'ordre supérieur suivent l'ordre des paramètres de ClickHouse. Utilisez `ckSql` pour la lambda, interpolez les champs du schéma externe et laissez les variables locales de la lambda comme de simples noms SQL. Les fonctions où la lambda détermine la structure de l'élément, comme `fn.arrayMap(...)`, `fn.arrayFilter(...)` et `fn.range(...)`, renvoient par défaut `Selection<unknown[]>` ; enchaînez `.mapWith(...)` si vous avez besoin d'un type d'élément décodé plus précis.

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

Dans cette lambda, `${params.timeColumn}` et `params.scopeTable.startTsList` sont des expressions issues du schéma. `start_ts` et `end_ts` sont des variables locales de la lambda, créées par ClickHouse à partir des éléments des tableaux.

`fn.arrayJoin(array)` correspond à `arrayJoin` de ClickHouse et produit une ligne par élément du tableau pour chaque ligne d'entrée. Associez-la à `fn.arrayZip(...)` et `fn.tupleElement(...)` quand deux tableaux doivent rester alignés par position :

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

À retenir : `arrayZip(sampleIds, probeIds)` associe les éléments par position, `arrayJoin(...)` développe ces paires en lignes et `tupleElement(..., 1/2)` récupère chacun des deux éléments.

Pour les clés composées, testez l'appartenance d'un tuple. Deux prédicats `IN` indépendants produisent un produit cartésien et peuvent faire correspondre des paires qui n'ont jamais été demandées :

```ts
// Avoid this when sampleId and probeId must stay paired.
const wrong = ck.and(
  ck.inArray(probeTelemetry.sampleId, sampleIds),
  ck.inArray(probeTelemetry.probeId, probeIds),
);
```

Pour respecter la sémantique des paires, comparez un tuple à une sous-requête qui renvoie des tuples ou à une liste de paires. `fn.tuple` transporte le type de chaque élément lorsqu'il est connu ; la liste est donc liée comme un seul paramètre `Array(Tuple(...))` :

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  [
    ["900001", "probe_alpha"],
    ["900002", "probe_beta"],
  ],
);
```

Un fragment SQL brut n'a pas de type d'élément. Encapsulez le tuple avec `fn.cast(fn.tuple(...), "Tuple(Int32, String)")` avant de l'utiliser dans `ck.inArray` ou `ck.eq`.

Avec une sous-requête qui renvoie des tuples, vous pouvez utiliser le même tuple à gauche sans paramètre :

```ts
const right = ck.inArray(
  fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId),
  scopedSamples,
);
```

Les fonctions utilitaires de tableau renvoient des valeurs `Selection` typées : vous pouvez donc les utiliser dans les projections, les filtres, les regroupements et les jointures :

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

### Modes de `fn.count` et `fn.uniqExact`

`fn.count()`, `fn.countIf(predicate)` et `fn.uniqExact(expression)` sont les variantes agrégées de `db.count(...)` et proposent les mêmes trois modes. Utilisez-les quand vous avez besoin d'un comptage au milieu d'une requête (dans `select`, `having`, les regroupements ou les sous-requêtes), plutôt que pour produire un scalaire de premier niveau. Elles partagent les mêmes encapsulations SQL et les mêmes décodeurs : chaque mode offre donc les mêmes compromis :

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

Les décodeurs refusent tout ce qui n'est pas un entier non négatif : les booléens, `NaN`, les nombres décimaux et les nombres négatifs déclenchent l'erreur `Failed to decode count() result: ...`. Si la sortie du serveur est corrompue, le décodage échoue donc immédiatement au lieu de produire silencieusement `0`.

Les autres agrégats s'écartent volontairement de cette API à trois modes : leurs contraintes de précision diffèrent de celles de `count` / `uniqExact` :

- `fn.sum()` / `fn.sumIf()` choisissent déjà un décodeur sûr selon la colonne d'entrée (string pour les colonnes entières ou Decimal, number pour les colonnes en virgule flottante) et augmentent automatiquement la précision de Decimal ; chaînez `.mapWith(Number)` si vous voulez explicitement un `number`.
- `fn.avg()` renvoie toujours `Selection<number>` parce que ClickHouse exécute `avg(Decimal)` via Float64 en interne : l'encapsuler dans une chaîne de caractères donnerait une image fausse du chemin d'exécution réel.
- `fn.min()` / `fn.max()` conservent le type TS de la colonne d'entrée (`min(int64Col)` est donc `Selection<string>`, comme le type utilisé pour lire la colonne).

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

## Tables et vues matérialisées
<!-- src:03694c38 -->

`createTable()`, `createMaterializedView()` et `dropTable()` envoient chacun une instruction DDL construite à partir d'objets de schéma. Ils ne comparent pas les schémas, n'exécutent pas `ALTER` et n'enregistrent pas de versions ; gardez les migrations des tables existantes dans votre outil de migration. Ils sont disponibles sur le client et sur une session. Les tables et vues qu'ils créent sont persistantes, et `runInSession()` ne les supprime pas.

### `createTable()`

`createTable(table, options?)` envoie l'instruction `CREATE TABLE` produite à partir d'une définition `ckTable(...)` : les colonnes avec leurs métadonnées DDL, puis `ENGINE`, `PARTITION BY`, `PRIMARY KEY`, `ORDER BY`, `SAMPLE BY`, `TTL`, `SETTINGS` et `COMMENT`.

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

- par défaut, une table existante du même nom fait échouer l'instruction avec `TABLE_ALREADY_EXISTS`
- `mode: "if_not_exists"` produit `CREATE TABLE IF NOT EXISTS` et laisse la table existante telle quelle, même si ses colonnes diffèrent
- `mode: "or_replace"` produit `CREATE OR REPLACE TABLE`, qui remplace une table existante et supprime ses données
- une table sans `engine` reçoit `MergeTree` avec `ORDER BY tuple()`
- une table alias issue de `ckAlias(...)` est rejetée

### `createMaterializedView()`

`createMaterializedView(name, { to, as, mode? })` crée une vue matérialisée déclenchée par les insertions et qui écrit dans la table existante `to`. `as` est un constructeur de sélection soumis aux mêmes règles que `insert(to).fromSelect(...)` : il doit projeter chaque colonne obligatoire de `to`, et les clés inconnues ou les incompatibilités de type sont des erreurs TypeScript.

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

- ClickHouse écrit la sortie d'une vue matérialisée dans `to` par nom de colonne, donc ck-orm enveloppe la sélection en `select ... as <column> from (...)` et chaque clé arrive dans sa colonne de `to`
- les paramètres de requête de `as` sont substitués à la création de la vue, donc la vue stockée conserve leurs valeurs
- les paramètres que la sélection force, comme `join_use_nulls = 1` issu de `leftJoin()`, vont dans une clause `SETTINGS` de la vue, parce que les paramètres ClickHouse envoyés avec la requête ne sont pas stockés avec la vue
- la vue ne voit que les lignes insérées après son existence ; pour rattraper l'historique, exécutez `db.insert(to).fromSelect(...)` avec la même sélection
- `mode: "if_not_exists"` produit `IF NOT EXISTS`. ClickHouse 25.8 et 26.3 n'ont pas `CREATE OR REPLACE MATERIALIZED VIEW`, donc il n'y a pas de mode `or_replace`
- `to` est obligatoire : les vues avec leur propre `ENGINE`, `POPULATE` et les vues rafraîchissables ne sont pas prises en charge ; utilisez `ckSql` avec `command()` pour celles-là

### `dropTable()`

`dropTable(table, options?)` supprime une table ou une vue, à partir de son objet de schéma ou de son nom :

```ts
await db.dropTable("usage_minutes_mv", { sync: true });
await db.dropTable(usageMinutes, { ifExists: true, sync: true });
```

- par défaut, il produit `DROP TABLE name` et échoue avec `UNKNOWN_TABLE` quand rien ne porte ce nom
- `ifExists: true` produit `IF EXISTS`
- `sync: true` produit `SYNC` : l'appel revient une fois que ClickHouse a supprimé les données, au lieu de laisser cela au nettoyage différé du moteur de base `Atomic`
- supprimer une vue matérialisée laisse sa table `to` et les lignes déjà écrites
- une table alias issue de `ckAlias(...)` est rejetée, et `ON CLUSTER` n'est pas produit ; sur un cluster, utilisez votre outil de migration ou `ckSql` avec `command()`

## Sessions et tables temporaires
<!-- src:8f031227 -->

ClickHouse ne propose pas de transactions traditionnelles. Pour les analyses ciblées et les traitements qui appliquent de gros filtres, les tables temporaires liées à une session sont souvent le bon outil.

Dans une fonction de rappel de session, utilisez ces méthodes de `ck-orm` :

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

Utilisez `registerTempTable(name)` quand une commande que vous contrôlez crée la table temporaire et que vous voulez tout de même que `ck-orm` la supprime lors du nettoyage de la session :

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

Utilisez `createTemporaryTableRaw(name, definition)` uniquement pour des définitions de table que vous contrôlez et auxquelles vous faites confiance, quand `ckTable(...)` ne permet pas de les exprimer :

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

L'argument `definition` commence juste après le nom de la table. N'y mettez ni `CREATE TEMPORARY TABLE` ni le nom de la table : ck-orm ajoute ces éléments et valide l'identifiant séparément.

### Comportement des sessions

- les requêtes qui ciblent la même `session_id` partagent un contrôleur de concurrence de session ; la valeur par défaut du client est `session_max_concurrent_requests = 1`
- les sessions ClickHouse elles-mêmes sont exclusives côté serveur : augmenter `session_max_concurrent_requests` au-delà de `1` désactive donc la sérialisation locale, mais vous pouvez toujours obtenir `SESSION_IS_LOCKED`
- `createTemporaryTable()` enregistre automatiquement la table pour le nettoyage
- `createTemporaryTable()` prend des objets de schéma en entrée ; le cycle de vie des tables temporaires reste sur `Session`, pas sur le schéma lui-même
- `createTemporaryTableRaw()` est l'API de SQL brut à utiliser uniquement pour les définitions de tables contrôlées par le développeur et rejette les définitions contenant plusieurs instructions
- `runInSession()` envoie automatiquement la `session_id` résolue dans `X-ClickHouse-Replica-Tag` pour chaque requête, nettoyage compris ; cet en-tête est géré par l'environnement d'exécution et ne peut pas être remplacé via `http_headers`
- Le [routage de ClickHouse Cloud tenant compte des réplicas](https://clickhouse.com/docs/products/cloud/features/infrastructure/replica-aware-routing) doit être activé séparément et fonctionne sans garantie ; une mise à l'échelle peut réattribuer un tag, et les déploiements navigateur avec des origines différentes doivent autoriser `X-ClickHouse-Replica-Tag` dans CORS
- `runInSession()` supprime les tables temporaires enregistrées quand la fonction de rappel se termine
- les appels `runInSession()` imbriqués créent toujours une nouvelle session enfant
- un appel imbriqué ne peut pas réutiliser la `session_id` d'une session parente active
- les sessions enfants imbriquées ne partagent avec leur parent ni les tables temporaires ni les créneaux de concurrence de la session, car elles utilisent toujours une `session_id` différente

### Contrat de concurrence des sessions

En pratique :

- les requêtes qui utilisent la même `session_id` explicite sont sérialisées par défaut
- la `session_id` par défaut du client est soumise au même limiteur
- les clients enfants créés par `withSettings()` partagent le même limiteur que leur parent lorsqu'ils ciblent la même `session_id`
- des valeurs de `session_id` différentes ne se bloquent pas mutuellement
- `stream()` et `iterator()` du constructeur gardent le créneau de la session jusqu'à la fin de l'itérateur. Si vous arrêtez l'itération trop tôt (`break`, `return` ou une erreur), `KILL QUERY` annule la requête côté serveur hors de la session (avec les mêmes identifiants d'authentification et le même tag de réplica), puis le client attend que ClickHouse libère la session pendant au plus `request_timeout` (30 s par défaut)
- les appels `runInSession()` imbriqués créent une nouvelle `session_id` enfant et ne partagent donc pas le créneau de la session parente

Une annulation de requête ou l'expiration de `request_timeout` ferme uniquement la connexion. ClickHouse continue d'exécuter la requête jusqu'à sa fin ou jusqu'à ce qu'il détecte la déconnexion ; une instruction `INSERT ... SELECT` peut donc quand même se terminer, et la requête suivante dans la même session peut échouer avec `SESSION_IS_LOCKED`. Réglez `max_execution_time` dans `clickhouse_settings` à une valeur inférieure à `request_timeout` pour que le serveur arrête d'abord la requête, lancez `KILL QUERY WHERE query_id = '...'` hors de la session, ou encapsulez `fetch` dans une logique de nouvelle tentative bornée sur le code 373 lorsque le corps de la requête peut être renvoyé. Les échecs de nettoyage, y compris `SESSION_IS_LOCKED`, passent par `onCleanupError` ou une `SessionError`.

Utilisation recommandée :

- gardez `session_max_concurrent_requests = 1` pour les parcours utilisant des tables temporaires et toute logique qui continue à utiliser une session
- si vous avez besoin d'un vrai parallélisme, utilisez des valeurs de `session_id` différentes au lieu d'augmenter la limite de la même session
- n'augmentez `session_max_concurrent_requests` que si vous voulez explicitement désactiver la sérialisation locale et pouvez accepter `SESSION_IS_LOCKED`

## Méthodes d'exécution
<!-- src:1d0d331a -->

En plus du constructeur, le client expose des méthodes d'exécution de bas niveau.

Les champs courants à fournir pour chaque requête sont :

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

`session_timeout` et `session_check` sont ici parce qu'ils décrivent une requête ou un bloc de session précis, pas une valeur par défaut globale du client.
`query_id`, `session_id` et `session_timeout` sont des options de requête ou de session : gardez-les hors de `clickhouse_settings`.

`session_check` ne démarre pas de nouvelle session ClickHouse. Utilisez-le pour demander à ClickHouse de vérifier qu'une `session_id` explicite existe déjà.
Toute requête qui transporte une `session_id` participe au même limiteur de session, y compris un `execute()` brut, `command()`, `stream()`, le `.execute()` du constructeur et son `.iterator()`.

Les `clickhouse_settings` propres à une requête ne remplacent les paramètres ClickHouse que le temps de cette requête :

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

Exécutez une requête brute et récupérez un tableau `Record<string, unknown>[]` :

```ts
const rows = await db.execute(ckSql`SELECT 1 AS one`);
```

### `stream()`

Diffusez en continu les résultats d'une requête brute :

```ts
for await (const row of db.stream(ckSql`SELECT number FROM numbers(10)`)) {
  console.log(row);
}
```

Si `stream()` cible une `session_id`, le créneau de la session n'est libéré qu'à la fin de l'itérateur asynchrone ou lorsque vous quittez prématurément la boucle.

### `command()`

Exécutez une commande qui ne renvoie pas de jeu de résultats :

```ts
await db.command(ckSql`SYSTEM FLUSH LOGS`);
```

### `withSettings()`

Créez un client enfant avec des paramètres par défaut supplémentaires :

```ts
const reportDb = db.withSettings({
  max_execution_time: 60,
  join_use_nulls: 0,
});
```

`withSettings()` ne modifie que les paramètres par défaut des requêtes et des sessions ClickHouse. Le client renvoyé conserve le même transport, les mêmes identifiants d'authentification, le même délai d'expiration et le même contrôleur de concurrence de session que le client parent. Un littéral `join_use_nulls: 0` est inféré : les jointures à gauche du client enfant restent donc non nulles ; une valeur typée comme un simple `number` produit quand même `| null`.

Les options de cycle de vie de la session restent séparées des paramètres ClickHouse dans `runInSession()` :

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

`runInSession()` fusionne ses `clickhouse_settings`, `http_headers` et `query_params` avec les valeurs par défaut du client parent ; si les deux définissent la même clé, c'est la valeur de la session qui l'emporte. Comme avec `withSettings()`, un littéral `join_use_nulls: 0` ou `join_use_nulls: 1` dans les paramètres de la session détermine aussi si les jointures à gauche de la session sont typées comme pouvant être nulles.

### Intégration des requêtes compilées

La plupart des applications ont intérêt à utiliser directement le constructeur. Utilisez `executeCompiled()` et `iteratorCompiled()` lorsque vous intégrez une autre couche qui produit déjà un `CompiledQuery`.

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

### `ping()` et `replicasStatus()`

```ts
await db.ping();
await db.replicasStatus();
```

## Observabilité
<!-- src:45091b93 -->

`ck-orm` propose deux couches d'observabilité :

- `logger` et `logLevel` pour la journalisation intégrée
- `plugins` pour les hooks du cycle de vie des requêtes ; le traçage OpenTelemetry est fourni par le plugin `ck-orm/otel`

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

### Plugin OpenTelemetry

`otelTracing()`, fourni par `ck-orm/otel`, enregistre un span client pour chaque requête que ck-orm envoie à ClickHouse. Il a son propre point d'entrée : le point d'entrée principal `ck-orm` n'importe jamais OpenTelemetry, donc les applications, les bundles pour navigateur et les workers edge qui ne tracent pas ne supportent aucun surcoût. `@opentelemetry/api` est une dépendance entre pairs facultative que seul `ck-orm/otel` importe.

#### Mise en place

ck-orm crée les spans via l'API OpenTelemetry, et l'API seule n'enregistre rien : tant qu'aucun SDK n'a enregistré de fournisseur de traceurs, tous les spans sont ignorés. ck-orm ne démarre aucun SDK et ne choisit aucun exportateur ; votre application fait les deux, une seule fois, au démarrage.

Installez l'API, un SDK et un exportateur. Cet exemple utilise le SDK Node.js avec l'exportateur OTLP/HTTP ; tout SDK qui enregistre un fournisseur de traceurs convient, y compris celui du navigateur.

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

Démarrez le SDK avant la première requête, par exemple dans un module que votre point d'entrée importe en premier :

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

L'exportateur envoie les spans à `http://localhost:4318/v1/traces`, sauf si vous définissez la variable standard `OTEL_EXPORTER_OTLP_ENDPOINT` ou transmettez `url`. Le SDK enregistre aussi le gestionnaire de contexte et le propagateur W3C dont dépendent la relation parent-enfant entre les spans et `propagateTraceContext`. Un processus de courte durée doit appeler `await sdk.shutdown()` avant de se terminer, sinon ses derniers spans sont perdus.

Ajoutez ensuite le plugin au client :

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

Pour voir les spans sans collecteur, l'[exemple de traçage OpenTelemetry](https://ck-orm.lyz.cloud/examples/#otel-tracing) utilise un exportateur en mémoire et affiche ce que le plugin a enregistré.

#### Spans

Les spans sont de type `CLIENT`. Ils s'appellent `clickhouse QUERY` pour les requêtes qui renvoient des lignes et `clickhouse <OPERATION>` pour tout le reste, par exemple `clickhouse INSERT` ou `clickhouse CREATE`. Un span devient l'enfant du span actif au moment où la requête démarre, par exemple celui que l'instrumentation OpenTelemetry de votre framework ouvre pour chaque requête HTTP.

Le plugin utilise `trace.getTracer("ck-orm")` sauf si vous transmettez `tracer`. Il déduit le nom de la base de données, l'adresse du serveur, son port et le délai d'expiration des requêtes de la configuration de `clickhouseClient(...)`. Transmettez les libellés propres au service via `attributes` ; les clés qui commencent par `db.` sont ignorées pour que le code applicatif ne puisse pas écraser les attributs de base de données intégrés.

Le paramètre `includeStatement: false` est plus sûr avec des systèmes de traçage partagés. La valeur par défaut est `true` ; désactivez-le si les noms de tables, les noms de colonnes ou la structure des requêtes ne doivent pas quitter le périmètre du service. Les valeurs liées ne sont pas intégrées à l'instruction compactée, mais la structure du SQL peut quand même être sensible en exploitation.

Les spans en échec utilisent toujours le statut `ERROR`. Par défaut, le plugin conserve la classification d'échec du jeu fermé (`error.type`, `ck_orm.error.kind`, `ck_orm.execution_state`, et un code ClickHouse associé ou un statut HTTP attribué lorsqu'il est présent) et omet le message, la pile, le corps et le cause d'origine dans le span exporté. L'objet d'erreur de l'appelant n'est pas modifié. `includeErrorDiagnostics: true` rétablit l'export actuel des diagnostics bruts — message de statut, `recordException` de l'erreur d'origine et attributs d'erreur existants — indépendamment de `includeStatement`. Seule une propriété data propre dont la valeur est le booléen `true`, lue une fois à la construction de `otelTracing()`, active cet opt-in. Ce n'est pas une garantie exhaustive de masquage des données sensibles. Le logger intégré et `includeStatement: true` transportent encore le contenu fourni par l'appelant.

#### Spans côté serveur

Définissez `propagateTraceContext: true` pour envoyer le contexte de trace W3C du span (`traceparent`, `tracestate`) avec chaque requête. ClickHouse enregistre alors ses propres spans pour la requête, comme `HTTPHandler`, `query` et les étapes du pipeline, en tant qu'enfants du span ck-orm. Le plugin ne transmet que ces deux en-têtes, jamais `baggage` ni les en-têtes propres à un fournisseur issus de votre propagateur global, et uniquement pour les spans échantillonnés : ClickHouse enregistre des spans côté serveur pour chaque contexte de trace reçu, échantillonné ou non. Le plugin injecte via le propagateur enregistré globalement, que le SDK OpenTelemetry configure par défaut sur le contexte de trace W3C ; sans propagateur enregistré, rien n'est envoyé.

```ts
const db = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123/telemetry_lab",
  plugins: [otelTracing({ propagateTraceContext: true })],
});
```

ClickHouse écrit ces spans dans `system.opentelemetry_span_log` et ne les exporte pas ; consultez [Tracing ClickHouse with OpenTelemetry](https://clickhouse.com/docs/operations/opentelemetry) pour les exporter vers un système de traçage. Les spans du serveur portent `clickhouse.query_id`, qui correspond à `db.query.id` sur le span ck-orm, et signalent les échecs par `clickhouse.exception_code` plutôt que par le statut du span.

Si vous transmettez votre propre `traceparent` dans `http_headers`, il remplace celui du plugin ; transmettez aussi le `tracestate` correspondant, sinon le `tracestate` du plugin est envoyé avec lui.

Dans les navigateurs, `traceparent` fait échouer la requête préliminaire CORS tant que ClickHouse n'autorise pas cet en-tête, et comme seules les requêtes échantillonnées le portent, les échecs semblent aléatoires. Le `http_options_response` par défaut ne l'autorise pas : activez donc la propagation dans les navigateurs seulement après l'avoir ajouté dans un fichier `config.d` :

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

### Écrire des plugins

Un plugin est un objet avec un `name` et jusqu'à trois hooks :

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

`onQueryStart` peut renvoyer des en-têtes pour cette requête :

```ts
const requestSource: ClickHouseORMPlugin = {
  name: "request-source",
  onQueryStart() {
    return { http_headers: { "x-request-source": "billing-worker" } };
  },
};
```

L'[exemple de plugins de requête](https://ck-orm.lyz.cloud/examples/#query-plugins) exécute un plugin qui renvoie un en-tête dans `onQueryStart` et journalise chaque instruction dans `onQuerySuccess` et `onQueryError`, dans l'ordre d'exécution.

Les plugins suivent ces règles :

- Les hooks s'exécutent pour chaque requête que ck-orm envoie à ClickHouse : requêtes, flux, commandes, insertions, fonctions utilitaires DDL, `ping()`, `replicasStatus()` et le nettoyage des tables temporaires dans `runInSession()`. Seules les requêtes qui annulent un flux abandonné et attendent le déverrouillage de sa session (`KILL QUERY` et l'interrogation qui suit) n'exécutent pas les hooks.
- Chaque `onQueryStart` est suivi d'exactement un `onQuerySuccess` ou un `onQueryError` avec le même `executionId` ; pour un flux, cela se produit quand l'itérateur se termine ou est fermé. Un flux que l'appelant arrête plus tôt compte comme un succès et indique les lignes lues jusque-là. Indexez l'état propre à chaque requête par `executionId`.
- Les hooks `onQueryStart` s'exécutent dans l'ordre d'enregistrement, et tous se terminent avant l'envoi de la requête. `onQuerySuccess` et `onQueryError` s'exécutent dans l'ordre inverse. Le logger intégré s'exécute toujours à l'extérieur de tous les plugins.
- L'en-tête d'un plugin ultérieur remplace celui d'un plugin antérieur. Les `http_headers` explicites, au niveau du client ou de la requête, remplacent les en-têtes des plugins. Les plugins ne peuvent pas remplacer l'en-tête `Authorization` construit à partir des identifiants de connexion ni l'en-tête `X-ClickHouse-Replica-Tag` que les sessions envoient.
- Un hook qui lève une exception est journalisé avec le nom du plugin et n'affecte ni la requête ni les autres plugins. Les en-têtes d'un `onQueryStart` qui lève une exception ou renvoie un en-tête invalide sont ignorés.
- Les événements sont en lecture seule. Les plugins ne peuvent modifier ni le SQL, ni les paramètres, ni les settings, ni les résultats.
- Les hooks sont attendus sur le chemin de la requête. Mettez en tampon les tâches lentes, comme l'export réseau, au lieu de les attendre dans un hook.

Types d'observabilité publics :

- `ClickHouseORMPlugin`
- `ClickHouseORMQueryStartResult`
- `ClickHouseORMQueryEvent`
- `ClickHouseORMQueryResultEvent`
- `ClickHouseORMQueryErrorEvent`
- `ClickHouseORMQueryStatistics`
- `ClickHouseORMTracingOptions` (de `ck-orm/otel`)
- `ClickHouseORMLogLevel`

Les champs d'événement disponibles par défaut incluent `databaseName`, `serverAddress`, `serverPort`, `requestTimeoutMs`, `statementHash`, `querySummary`, `queryId`, `sessionId` et `tableName` quand ck-orm peut identifier une seule table sans deviner à partir du SQL brut. Les requêtes JSON qui récupèrent tous les résultats d'un coup ajoutent aussi les statistiques de réponse ClickHouse lorsque le serveur les renvoie : `serverElapsedMs`, `readRows`, `readBytes`, `resultRows` et `rowsBeforeLimitAtLeast`.

## Modèle d'erreur
<!-- src:b4b93db0 -->

Types d'erreur publics (exports de types uniquement) :

- `ClickHouseORMError`
- `DecodeError`

Contrôles d'erreur à l'exécution :

- `isClickHouseORMError(error)`
- `isDecodeError(error)`

`ClickHouseORMError` conserve autant de contexte que possible, notamment :

- `kind`
- `executionState`
- `queryId`
- `sessionId`
- `httpStatus`
- `clickhouseCode`
- `clickhouseName`
- `responseText`
- `requestTimeoutMs`

Valeurs publiques actuelles de `kind` :

- `client_validation`
- `request_failed`
- `decode`
- `timeout`
- `aborted`
- `session`

Journalisez le contexte complet de l'erreur côté serveur, puis renvoyez aux appelants de l'application une erreur publique concise :

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

Ne renvoyez pas `responseText` ni le `message` d'erreur brut à des clients qui ne sont pas de confiance. ClickHouse peut inclure des fragments SQL et des noms d'objets dans le texte d'erreur.

## Sécurité
<!-- src:b6d029f6 -->

Par défaut, `ck-orm` rend difficile la construction de requêtes SQL non sécurisées.

Les protections intégrées sont les suivantes :

- les identifiants transmis à `ckSql.identifier()` sont validés et mis entre guillemets
- les noms de fonctions utilisés par `fn.call(...)`, `fn.withParams(...)`, `ckType.aggregateFunction(...)` et `ckType.simpleAggregateFunction(...)` sont validés ; les paramètres `AggregateFunction` doivent être des littéraux de chaîne de caractères ou des littéraux numériques
- les clés des champs imbriqués transmises à `ckType.nested({...})` sont validées comme identifiants ClickHouse
- les valeurs interpolées dans `` ckSql`...` `` deviennent des paramètres nommés ClickHouse plutôt que du SQL brut
- les clés de `query_params` qui commencent par `orm_param` sont rejetées, car ce préfixe est réservé aux paramètres internes des modèles SQL
- une seule instruction au niveau supérieur est autorisée par requête
- les en-têtes d'autorisation dérivés de la configuration de connexion ne peuvent pas être remplacés par les `http_headers` fournis par l'appelant
- la configuration de connexion structurée rejette les identifiants d'authentification intégrés dans `host`
- les données de destination du traçage sont normalisées avant d'être attachées aux spans

### API réservées au code de confiance

Pour le DDL de tables temporaires impossible à représenter avec `ckTable(...)`, `ck-orm` expose dans `runInSession()` une API de SQL brut à portée limitée, réservée au code de confiance :

- `session.createTemporaryTableRaw(name, definition)`

`fn.call(name, ...)` et `fn.withParams(name, ...)` valident `name`, mais traitez quand même les noms de fonctions choisis dynamiquement comme des entrées contrôlées par le développeur, pas comme des entrées non approuvées reçues à l'exécution.

`ckSql.identifier(...)` valide à la frontière entre compilation et exécution. Construire le fragment est peu coûteux ; l'erreur apparaît quand le fragment est compilé dans une requête.

### Données exposées par le traçage

`otelTracing()` attache le texte SQL compacté aux spans dans `db.statement` et `db.query.text`, car `includeStatement` vaut `true` par défaut.

Les valeurs liées sont stockées séparément comme paramètres nommés ClickHouse et ne figurent pas dans le texte de l'instruction ; les noms de tables, les noms de colonnes et la structure des requêtes apparaissent toutefois dans la sortie du traçage. Si votre service de traçage est partagé ou se trouve sur un autre hôte, désactivez `includeStatement` ou filtrez `db.statement` / `db.query.text` dans votre outil de collecte.

En cas d'échec, l'export de traçage par défaut conserve la classification du jeu fermé et omet le texte d'erreur d'origine. `includeErrorDiagnostics: true` réexporte explicitement les diagnostics bruts, indépendamment de `includeStatement`. Ni ce paramètre, ni `includeStatement: true`, ni le logger intégré ne constituent un contrôle exhaustif de masquage des données sensibles.

Les spans côté serveur de ClickHouse enregistrent aussi l'instruction dans `db.statement` dans `system.opentelemetry_span_log`. Cette table reste sur le serveur ClickHouse tant que vous ne l'exportez pas.

### Réponses d'erreur

`ClickHouseORMError.responseText` et le `message` d'erreur peuvent contenir du texte brut envoyé par le serveur ClickHouse, y compris des fragments SQL et des noms d'objets de la base de données.

Ne transmettez pas directement ces détails à des clients qui ne sont pas de confiance. Journalisez-les côté serveur et exposez plutôt un message générique.

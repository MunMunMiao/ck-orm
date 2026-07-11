import type * as RootApi from "../index";
import {
  type ClickHouseBaseQueryOptions,
  type ClickHouseSettings,
  ck,
  ckSql,
  ckTable,
  ckType,
  clickhouseClient,
  fn,
  type Order,
  type Paths,
  type PathValue,
  type Predicate,
  type Selection,
  type SQLFragment,
} from "../index";
import { activityLedger, activityMetricLog } from "./fixtures";
import type { DataOf, Equal, Expect } from "./helpers";

type SqlFragmentData<T> = T extends SQLFragment<infer TData> ? TData : never;
type SelectionData<T> = T extends Selection<infer TData> ? TData : never;

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/public_api_matrix",
});

const requestOptions: ClickHouseBaseQueryOptions = {
  query_id: "public_api_matrix_query",
  session_id: "public_api_matrix_session",
  session_timeout: 60,
  clickhouse_settings: {
    max_threads: 2,
    readonly: 1,
  },
};

const settings: ClickHouseSettings = {
  allow_experimental_correlated_subqueries: 1,
  setting_added_by_future_clickhouse: "on",
};

const settingsDb = db.withSettings(settings);

settingsDb.execute(ckSql`SELECT 1`, requestOptions);
db.execute(ckSql`SELECT 1`, { clickhouse_settings: settings });

const columnSmoke = {
  array: ckType.array(ckType.string()),
  dateTime64: ckType.dateTime64({ precision: 9, timezone: "UTC" }),
  decimal: ckType.decimal({ precision: 20, scale: 5 }),
  json: ckType.json<{ readonly id: number }>(),
  lowCardinality: ckType.lowCardinality(ckType.string()),
  map: ckType.map(ckType.string(), ckType.int32()),
  nested: ckType.nested({ id: ckType.int32(), name: ckType.string() }),
  nullable: ckType.nullable(ckType.string()),
  qbit: ckType.qbit(ckType.float32(), { dimensions: 8 }),
  tuple: ckType.tuple(ckType.int32(), ckType.string()),
  uint64: ckType.uint64(),
  variant: ckType.variant(ckType.string(), ckType.int32()),
} satisfies Readonly<Record<string, RootApi.Column>>;

type _ColumnSmokeData = Expect<
  Equal<
    { readonly [K in keyof typeof columnSmoke]: DataOf<(typeof columnSmoke)[K]> },
    {
      readonly array: string[];
      readonly dateTime64: Date;
      readonly decimal: string;
      readonly json: { readonly id: number };
      readonly lowCardinality: string;
      readonly map: Record<string, number>;
      readonly nested: { id: number; name: string }[];
      readonly nullable: string | null;
      readonly qbit: readonly number[];
      readonly tuple: readonly [number, string];
      readonly uint64: string;
      readonly variant: string | number;
    }
  >
>;

const functionSmoke = {
  arrayExists: fn.arrayExists(ckSql`x -> x > 1`, [1, 2]),
  arrayMap: fn.arrayMap<number>(ckSql`x -> x + 1`, [1, 2]),
  count: fn.count(),
  jsonExtract: fn.jsonExtract(activityMetricLog.payload, ckType.array(ckType.string()), "labels"),
  length: fn.length(["vip"]),
  range: fn.range(1, 5),
  sum: fn.sum(activityLedger.delta_value),
  toDateTime64: fn.toDateTime64(activityLedger.event_time, 3, "UTC"),
  tupleElement: fn.tupleElement<number>(fn.tuple(activityLedger.system_id, activityLedger.actor_id), 2),
  uniqExactSafe: fn.uniqExact(activityLedger.actor_id).toSafe(),
} satisfies Readonly<Record<string, Selection>>;

type _FunctionSmokeData = Expect<
  Equal<
    {
      readonly arrayExists: SelectionData<(typeof functionSmoke)["arrayExists"]>;
      readonly arrayMap: SelectionData<(typeof functionSmoke)["arrayMap"]>;
      readonly count: SelectionData<(typeof functionSmoke)["count"]>;
      readonly length: SelectionData<(typeof functionSmoke)["length"]>;
      readonly toDateTime64: SelectionData<(typeof functionSmoke)["toDateTime64"]>;
      readonly uniqExactSafe: SelectionData<(typeof functionSmoke)["uniqExactSafe"]>;
    },
    {
      readonly arrayExists: boolean;
      readonly arrayMap: unknown[];
      readonly count: number;
      readonly length: string;
      readonly toDateTime64: Date;
      readonly uniqExactSafe: string;
    }
  >
>;

const tableFunctionSmoke = {
  call: fn.table.call("numbers", 10),
} satisfies Readonly<Record<keyof typeof fn.table, unknown>>;

const ckSmoke = {
  and: ck.and(ck.eq(activityLedger.actor_id, 10001), ck.ne(activityLedger.event_phase, 9)),
  asc: ck.asc(activityLedger.actor_id),
  eq: ck.eq(activityLedger.actor_id, 10001),
  exists: ck.exists(db.select({ actor_id: activityLedger.actor_id }).from(activityLedger)),
  expr: ck.expr<boolean>(ckSql`1`, { decoder: (value) => Number(value) === 1, sqlType: "UInt8" }),
  fn: ck.fn,
} satisfies Readonly<Record<string, unknown>>;

const ckSqlSmoke = {
  identifier: ckSql.identifier({ table: "activity_ledger", column: "actor_id" }),
  join: ckSql.join([ckSql`select 1`, ckSql`select 2`], ckSql`, `),
  decimal: ckSql.decimal(ckSql`sum(${activityLedger.delta_value})`, 20, 5),
  tagged: ckSql`select ${activityLedger.actor_id}`,
} satisfies Readonly<Record<string, unknown>>;

const nameSelection: Selection<string> = fn.toString(activityLedger.system_id);
const idPredicate: Predicate = ck.eq(activityLedger.actor_id, 1);
const sortOrder: Order = ck.asc(nameSelection);

type HasCkType = "ckType" extends keyof typeof import("../index") ? true : false;
type HasRootInt32 = "int32" extends keyof typeof import("../index") ? true : false;
type _HasCkType = Expect<Equal<HasCkType, true>>;
type _HasNoRootInt32 = Expect<Equal<HasRootInt32, false>>;

type JsonShapeFixture = { readonly a: { readonly b: number; readonly c: string }; readonly d: boolean };

type _JsonPaths = Expect<Equal<Paths<JsonShapeFixture>, "a" | "a.b" | "a.c" | "d">>;
type _JsonPathValueLeaf = Expect<Equal<PathValue<JsonShapeFixture, "a.b">, number>>;
type _JsonPathValueObject = Expect<Equal<PathValue<JsonShapeFixture, "a">, { readonly b: number; readonly c: string }>>;
type _JsonPathValueUnknown = Expect<Equal<PathValue<JsonShapeFixture, "missing">, unknown>>;

const jsonHints = {
  typeHints: {
    "a.b": ckType.uint32(),
  },
} satisfies RootApi.JsonConfig<JsonShapeFixture>;

const jsonPathTable = ckTable("json_path_t", {
  payload: ckType.json<JsonShapeFixture>(),
});
const jsonPathLeaf = jsonPathTable.payload.path("a.b");
type _JsonPathLeafType = Expect<Equal<SqlFragmentData<typeof jsonPathLeaf>, number>>;

const jsonPathObject = jsonPathTable.payload.path("a");
type _JsonPathObjectType = Expect<
  Equal<SqlFragmentData<typeof jsonPathObject>, { readonly b: number; readonly c: string }>
>;

const jsonCastLeaf = jsonPathTable.payload.castPath("a.b", ckType.uint64());
type _JsonCastLeafType = Expect<Equal<SqlFragmentData<typeof jsonCastLeaf>, string>>;

const jsonIoTable = ckTable("json_io_t", {
  payload: ckType.json("payload", { typeHints: { uid: ckType.uint64() } }).$type<{
    readonly select: { readonly uid: string };
    readonly insert: { readonly uid: string | number | bigint };
  }>(),
});
type JsonIoSelect = typeof jsonIoTable.$inferSelect;
type JsonIoInsert = typeof jsonIoTable.$inferInsert;
type _JsonIoSelectUid = Expect<Equal<JsonIoSelect["payload"]["uid"], string>>;
type _JsonIoInsertUid = Expect<Equal<JsonIoInsert["payload"]["uid"], string | number | bigint>>;

const jsonDefaultTable = ckTable("json_default_t", {
  id: ckType.uint64(),
  p: ckType.json<{ readonly x: number }>("p").default(ckSql`'{}'`),
});
type JsonDefaultInsert = typeof jsonDefaultTable.$inferInsert;
type _JsonDefaultInsert = Expect<Equal<JsonDefaultInsert, { id: string; p?: { readonly x: number } }>>;

const jsonMaterializedTable = ckTable("json_materialized_t", {
  id: ckType.uint64(),
  p: ckType.json<{ readonly x: number }>("p").materialized(ckSql`'{}'`),
});
type JsonMaterializedInsert = typeof jsonMaterializedTable.$inferInsert;
type _JsonMaterializedInsert = Expect<Equal<JsonMaterializedInsert, { id: string }>>;

void tableFunctionSmoke;
void ckSmoke;
void ckSqlSmoke;
void idPredicate;
void jsonHints;
void jsonPathLeaf;
void jsonPathObject;
void jsonCastLeaf;
void sortOrder;

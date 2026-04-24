import type * as RootApi from "../index";
import {
  type ClickHouseBaseQueryOptions,
  type ClickHouseSettings,
  chType,
  ck,
  clickhouseClient,
  csql,
  fn,
  type Order,
  type Predicate,
  type Selection,
} from "../index";
import { activityLedger, activityMetricLog, typeScenarioSchema } from "./fixtures";
import type { DataOf, Equal, Expect } from "./helpers";

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/public_api_matrix",
  schema: typeScenarioSchema,
});

const settingsDb = clickhouseClient({
  databaseUrl: "http://localhost:8123/public_api_matrix",
  schema: typeScenarioSchema,
  clickhouse_settings: {
    allow_experimental_correlated_subqueries: 1,
    max_threads: 4,
    future_clickhouse_setting_from_typecheck: "enabled",
  },
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

const knownAndDynamicSettings: ClickHouseSettings = {
  allow_experimental_correlated_subqueries: 1,
  setting_added_by_future_clickhouse: "on",
};

const settingsChildDb = db.withSettings({
  allow_experimental_correlated_subqueries: 1,
  future_clickhouse_setting_from_child: true,
});

settingsDb.execute(csql`SELECT 1`, requestOptions);
settingsChildDb.execute(csql`SELECT 1`);
db.execute(csql`SELECT 1`, { clickhouse_settings: knownAndDynamicSettings });

const invalidObjectSettings: ClickHouseSettings = {
  // @ts-expect-error object values are not valid ClickHouse HTTP setting values.
  max_threads: { value: 4 },
};

const invalidArraySettings: ClickHouseSettings = {
  // @ts-expect-error array values are not valid ClickHouse HTTP setting values.
  setting_added_by_future_clickhouse: ["on"],
};

void invalidObjectSettings;
void invalidArraySettings;

const chTypeNameMatrix = {
  aggregateFunction: [
    chType.aggregateFunction("sum", chType.uint64()),
    chType.aggregateFunction("sum_state", { name: "sum", args: [chType.uint64()] }),
  ],
  array: [chType.array(chType.string()), chType.array("tag_names", chType.string())],
  bfloat16: [chType.bfloat16(), chType.bfloat16("score_bf16")],
  bool: [chType.bool(), chType.bool("is_active")],
  date: [chType.date(), chType.date("event_date")],
  date32: [chType.date32(), chType.date32("event_date32")],
  dateTime: [chType.dateTime(), chType.dateTime("created_at")],
  dateTime64: [
    chType.dateTime64({ precision: 9, timezone: "UTC" }),
    chType.dateTime64("created_at_64", { precision: 9, timezone: "UTC" }),
  ],
  decimal: [chType.decimal({ precision: 20, scale: 5 }), chType.decimal("metric_value", { precision: 20, scale: 5 })],
  dynamic: [chType.dynamic<{ label: string }>(), chType.dynamic<{ label: string }>("payload_dynamic")],
  enum8: [
    chType.enum8<"open" | "closed">({ open: 1, closed: 2 }),
    chType.enum8<"open" | "closed">("status_8", { open: 1, closed: 2 }),
  ],
  enum16: [
    chType.enum16<"small" | "large">({ small: 1, large: 1000 }),
    chType.enum16<"small" | "large">("status_16", { small: 1, large: 1000 }),
  ],
  fixedString: [chType.fixedString({ length: 8 }), chType.fixedString("code", { length: 8 })],
  float32: [chType.float32(), chType.float32("ratio_32")],
  float64: [chType.float64(), chType.float64("ratio_64")],
  int8: [chType.int8(), chType.int8("i8")],
  int16: [chType.int16(), chType.int16("i16")],
  int32: [chType.int32(), chType.int32("i32")],
  int64: [chType.int64(), chType.int64("i64")],
  ipv4: [chType.ipv4(), chType.ipv4("ip_v4")],
  ipv6: [chType.ipv6(), chType.ipv6("ip_v6")],
  json: [chType.json<{ id: number }>(), chType.json<{ id: number }>("payload_json")],
  lineString: [chType.lineString(), chType.lineString("line_value")],
  lowCardinality: [chType.lowCardinality(chType.string()), chType.lowCardinality("region", chType.string())],
  map: [chType.map(chType.string(), chType.int32()), chType.map("attrs", chType.string(), chType.int32())],
  multiLineString: [chType.multiLineString(), chType.multiLineString("multi_line_value")],
  multiPolygon: [chType.multiPolygon(), chType.multiPolygon("multi_polygon_value")],
  nested: [
    chType.nested({ id: chType.int32(), name: chType.string() }),
    chType.nested("profiles", { id: chType.int32(), name: chType.string() }),
  ],
  nullable: [chType.nullable(chType.string()), chType.nullable("optional_note", chType.string())],
  point: [chType.point(), chType.point("point_value")],
  polygon: [chType.polygon(), chType.polygon("polygon_value")],
  qbit: [
    chType.qbit(chType.float32(), { dimensions: 8 }),
    chType.qbit("embedding", chType.float32(), { dimensions: 8 }),
  ],
  ring: [chType.ring(), chType.ring("ring_value")],
  simpleAggregateFunction: [
    chType.simpleAggregateFunction("sum", chType.uint64()),
    chType.simpleAggregateFunction("sum_value", { name: "sum", value: chType.uint64() }),
  ],
  string: [chType.string(), chType.string("user_id")],
  time: [chType.time(), chType.time("event_time")],
  time64: [chType.time64({ precision: 6 }), chType.time64("event_time_64", { precision: 6 })],
  tuple: [chType.tuple(chType.int32(), chType.string()), chType.tuple("point_pair", chType.int32(), chType.string())],
  uint8: [chType.uint8(), chType.uint8("u8")],
  uint16: [chType.uint16(), chType.uint16("u16")],
  uint32: [chType.uint32(), chType.uint32("u32")],
  uint64: [chType.uint64(), chType.uint64("u64")],
  uuid: [chType.uuid(), chType.uuid("entity_uuid")],
  variant: [
    chType.variant(chType.string(), chType.int32()),
    chType.variant("variant_value", chType.string(), chType.int32()),
  ],
} satisfies { readonly [K in keyof typeof chType]: readonly [unknown, unknown] };

const columnTypeMatrix = {
  aggregateFunction: chType.aggregateFunction<number>("sum", chType.uint64()),
  array: chType.array(chType.string()),
  bfloat16: chType.bfloat16(),
  bool: chType.bool(),
  date: chType.date(),
  date32: chType.date32(),
  dateTime: chType.dateTime(),
  dateTime64: chType.dateTime64({ precision: 9 }),
  decimal: chType.decimal({ precision: 20, scale: 5 }),
  dynamic: chType.dynamic<{ label: string }>(),
  enum8: chType.enum8<"open" | "closed">({ open: 1, closed: 2 }),
  enum16: chType.enum16<"small" | "large">({ small: 1, large: 1000 }),
  fixedString: chType.fixedString({ length: 8 }),
  float32: chType.float32(),
  float64: chType.float64(),
  int8: chType.int8(),
  int16: chType.int16(),
  int32: chType.int32(),
  int64: chType.int64(),
  ipv4: chType.ipv4(),
  ipv6: chType.ipv6(),
  json: chType.json<{ id: number }>(),
  lineString: chType.lineString(),
  lowCardinality: chType.lowCardinality(chType.string()),
  map: chType.map(chType.string(), chType.int32()),
  multiLineString: chType.multiLineString(),
  multiPolygon: chType.multiPolygon(),
  nested: chType.nested({ id: chType.int32(), name: chType.string() }),
  nullable: chType.nullable(chType.string()),
  point: chType.point(),
  polygon: chType.polygon(),
  qbit: chType.qbit(chType.float32(), { dimensions: 8 }),
  ring: chType.ring(),
  simpleAggregateFunction: chType.simpleAggregateFunction<number>("sum", chType.uint64()),
  string: chType.string(),
  time: chType.time(),
  time64: chType.time64({ precision: 6 }),
  tuple: chType.tuple(chType.int32(), chType.string()),
  uint8: chType.uint8(),
  uint16: chType.uint16(),
  uint32: chType.uint32(),
  uint64: chType.uint64(),
  uuid: chType.uuid(),
  variant: chType.variant(chType.string(), chType.int32()),
} satisfies { readonly [K in keyof typeof chType]: RootApi.Column };

type _ChTypeDataMatrix = Expect<
  Equal<
    { readonly [K in keyof typeof columnTypeMatrix]: DataOf<(typeof columnTypeMatrix)[K]> },
    {
      readonly aggregateFunction: number;
      readonly array: string[];
      readonly bfloat16: number;
      readonly bool: boolean;
      readonly date: Date;
      readonly date32: Date;
      readonly dateTime: Date;
      readonly dateTime64: Date;
      readonly decimal: string;
      readonly dynamic: { label: string };
      readonly enum8: "open" | "closed";
      readonly enum16: "small" | "large";
      readonly fixedString: string;
      readonly float32: number;
      readonly float64: number;
      readonly int8: number;
      readonly int16: number;
      readonly int32: number;
      readonly int64: string;
      readonly ipv4: string;
      readonly ipv6: string;
      readonly json: { id: number };
      readonly lineString: readonly [number, number][];
      readonly lowCardinality: string;
      readonly map: Record<string, number>;
      readonly multiLineString: readonly [number, number][][];
      readonly multiPolygon: readonly [number, number][][][];
      readonly nested: { id: number; name: string }[];
      readonly nullable: string | null;
      readonly point: readonly [number, number];
      readonly polygon: readonly [number, number][][];
      readonly qbit: readonly number[];
      readonly ring: readonly [number, number][];
      readonly simpleAggregateFunction: number;
      readonly string: string;
      readonly time: Date;
      readonly time64: Date;
      readonly tuple: readonly [number, string];
      readonly uint8: number;
      readonly uint16: number;
      readonly uint32: number;
      readonly uint64: string;
      readonly uuid: string;
      readonly variant: string | number;
    }
  >
>;

const functionTypeMatrix = {
  array: fn.array<string>("vip", "pro"),
  arrayAUCPR: fn.arrayAUCPR([0.1, 0.4], [0, 1]),
  arrayAll: fn.arrayAll(csql`x -> x > 0`, [1, 2]),
  arrayAutocorrelation: fn.arrayAutocorrelation([1, 2, 3]),
  arrayAvg: fn.arrayAvg([1, 2, 3]),
  arrayCompact: fn.arrayCompact<string>(["vip", "vip", "pro"]),
  arrayConcat: fn.arrayConcat<string>(["vip"], ["pro"]),
  arrayCount: fn.arrayCount(csql`x -> x > 0`, [1, 2, 3]),
  arrayCumSum: fn.arrayCumSum<number>([1, 2, 3]),
  arrayCumSumNonNegative: fn.arrayCumSumNonNegative<number>([1, -3, 4]),
  arrayDifference: fn.arrayDifference<number>([1, 3, 6]),
  arrayDistinct: fn.arrayDistinct<string>(["vip", "vip"]),
  arrayDotProduct: fn.arrayDotProduct<number>([1, 2], [3, 4]),
  arrayElement: fn.arrayElement<string>(["vip"], 1),
  arrayElementOrNull: fn.arrayElementOrNull<string>(["vip"], 2),
  arrayEnumerate: fn.arrayEnumerate(["vip"]),
  arrayEnumerateDense: fn.arrayEnumerateDense(["vip"]),
  arrayEnumerateDenseRanked: fn.arrayEnumerateDenseRanked(1, [[1, 2]], 2),
  arrayEnumerateUniq: fn.arrayEnumerateUniq(["vip"], ["pro"]),
  arrayEnumerateUniqRanked: fn.arrayEnumerateUniqRanked(1, [[1, 2]], 2),
  arrayExcept: fn.arrayExcept<string>(["vip", "pro"], ["pro"]),
  arrayExists: fn.arrayExists(csql`x -> x > 1`, [1, 2]),
  arrayFill: fn.arrayFill<number>(csql`x -> x > 0`, [1, 0, 2]),
  arrayFilter: fn.arrayFilter<number>(csql`x -> x > 1`, [1, 2, 3]),
  arrayFirst: fn.arrayFirst<number>(csql`x -> x > 1`, [1, 2, 3]),
  arrayFirstIndex: fn.arrayFirstIndex(csql`x -> x > 1`, [1, 2, 3]),
  arrayFirstOrNull: fn.arrayFirstOrNull<number>(csql`x -> x > 9`, [1, 2, 3]),
  arrayFlatten: fn.arrayFlatten<string>([["vip"], ["pro"]]),
  arrayFold: fn.arrayFold<number>(csql`(acc, x) -> acc + x`, [1, 2], 0),
  arrayIntersect: fn.arrayIntersect<string>(["vip"], ["pro", "vip"]),
  arrayJaccardIndex: fn.arrayJaccardIndex(["vip"], ["vip", "pro"]),
  arrayJoin: fn.arrayJoin<number>([1, 2]),
  arrayLast: fn.arrayLast<number>(csql`x -> x > 1`, [1, 2, 3]),
  arrayLastIndex: fn.arrayLastIndex(csql`x -> x > 1`, [1, 2, 3]),
  arrayLastOrNull: fn.arrayLastOrNull<number>(csql`x -> x > 9`, [1, 2, 3]),
  arrayLevenshteinDistance: fn.arrayLevenshteinDistance(["A"], ["B"]),
  arrayLevenshteinDistanceWeighted: fn.arrayLevenshteinDistanceWeighted(["A"], ["B"], [1], [1]),
  arrayMap: fn.arrayMap<number>(csql`x -> x + 1`, [1, 2]),
  arrayMax: fn.arrayMax<number>([1, 2]),
  arrayMin: fn.arrayMin<number>([1, 2]),
  arrayNormalizedGini: fn.arrayNormalizedGini([0.9, 0.3], [1, 0]),
  arrayPartialReverseSort: fn.arrayPartialReverseSort<number>(2, [5, 1, 3]),
  arrayPartialShuffle: fn.arrayPartialShuffle<number>([1, 2, 3], 2),
  arrayPartialSort: fn.arrayPartialSort<number>(2, [5, 1, 3]),
  arrayPopBack: fn.arrayPopBack<string>(["vip", "pro"]),
  arrayPopFront: fn.arrayPopFront<string>(["vip", "pro"]),
  arrayProduct: fn.arrayProduct<number>([1, 2, 3]),
  arrayPushBack: fn.arrayPushBack<string>(["vip"], "pro"),
  arrayPushFront: fn.arrayPushFront<string>(["pro"], "vip"),
  arrayROCAUC: fn.arrayROCAUC([0.1, 0.9], [0, 1]),
  arrayRandomSample: fn.arrayRandomSample<string>(["vip", "pro"], 1),
  arrayReduce: fn.arrayReduce<string>("sum", [1, 2]),
  arrayReduceInRanges: fn.arrayReduceInRanges<string>("sum", [[1, 2]], [1, 2]),
  arrayRemove: fn.arrayRemove<string>(["vip", "pro"], "pro"),
  arrayResize: fn.arrayResize<string>(["vip"], 2, "pro"),
  arrayReverse: fn.arrayReverse<string>(["vip", "pro"]),
  arrayReverseFill: fn.arrayReverseFill<number>(csql`x -> x > 0`, [1, 0, 2]),
  arrayReverseSort: fn.arrayReverseSort<number>([2, 1]),
  arrayReverseSplit: fn.arrayReverseSplit<number>(csql`x -> x = 0`, [1, 0, 2]),
  arrayRotateLeft: fn.arrayRotateLeft<number>([1, 2, 3], 1),
  arrayRotateRight: fn.arrayRotateRight<number>([1, 2, 3], 1),
  arrayShiftLeft: fn.arrayShiftLeft<number>([1, 2, 3], 1, 0),
  arrayShiftRight: fn.arrayShiftRight<number>([1, 2, 3], 1, 0),
  arrayShingles: fn.arrayShingles<readonly string[]>(["a", "b", "c"], 2),
  arrayShuffle: fn.arrayShuffle<string>(["vip", "pro"]),
  arraySimilarity: fn.arraySimilarity(["A"], ["B"], [1], [1]),
  arraySlice: fn.arraySlice<string>(["vip", "pro"], 1, 1),
  arraySort: fn.arraySort<number>([2, 1]),
  arraySplit: fn.arraySplit<number>(csql`x -> x = 0`, [1, 0, 2]),
  arraySum: fn.arraySum<number>([1, 2]),
  arraySymmetricDifference: fn.arraySymmetricDifference<string>(["vip"], ["pro"]),
  arrayTranspose: fn.arrayTranspose<number>([
    [1, 2],
    [3, 4],
  ]),
  arrayUnion: fn.arrayUnion<string>(["vip"], ["pro"]),
  arrayUniq: fn.arrayUniq(["vip", "vip"]),
  arrayWithConstant: fn.arrayWithConstant<string>(2, "vip"),
  arrayZip: fn.arrayZip([1], ["vip"]),
  arrayZipUnaligned: fn.arrayZipUnaligned([1], ["vip"]),
  avg: fn.avg(activityLedger.actor_id),
  call: fn.call<number>("abs", activityLedger.actor_id),
  coalesce: fn.coalesce<string>(activityLedger.delta_value, "0"),
  count: fn.count(),
  countEqual: fn.countEqual(["vip", "vip"], "vip"),
  countIf: fn.countIf(ck.eq(activityLedger.event_phase, 1)),
  empty: fn.empty([]),
  emptyArrayDate: fn.emptyArrayDate(),
  emptyArrayDateTime: fn.emptyArrayDateTime(),
  emptyArrayFloat32: fn.emptyArrayFloat32(),
  emptyArrayFloat64: fn.emptyArrayFloat64(),
  emptyArrayInt16: fn.emptyArrayInt16(),
  emptyArrayInt32: fn.emptyArrayInt32(),
  emptyArrayInt64: fn.emptyArrayInt64(),
  emptyArrayInt8: fn.emptyArrayInt8(),
  emptyArrayString: fn.emptyArrayString(),
  emptyArrayToSingle: fn.emptyArrayToSingle<string>(fn.emptyArrayString()),
  emptyArrayUInt16: fn.emptyArrayUInt16(),
  emptyArrayUInt32: fn.emptyArrayUInt32(),
  emptyArrayUInt64: fn.emptyArrayUInt64(),
  emptyArrayUInt8: fn.emptyArrayUInt8(),
  has: fn.has(chTypeNameMatrix.array[0], "vip"),
  hasAll: fn.hasAll(chTypeNameMatrix.array[0], ["vip"]),
  hasAny: fn.hasAny(chTypeNameMatrix.array[0], ["vip"]),
  hasSubstr: fn.hasSubstr(chTypeNameMatrix.array[0], ["vip"]),
  indexOf: fn.indexOf(["vip"], "vip"),
  indexOfAssumeSorted: fn.indexOfAssumeSorted(["pro", "vip"], "vip"),
  jsonExtract: fn.jsonExtract(activityMetricLog.payload, chType.array(chType.string()), "labels"),
  kql_array_sort_asc: fn.kql_array_sort_asc<readonly [string[]]>(["pro", "vip"]),
  kql_array_sort_desc: fn.kql_array_sort_desc<readonly [string[]]>(["pro", "vip"]),
  length: fn.length(["vip"]),
  max: fn.max<string>(activityLedger.system_id),
  min: fn.min<number>(activityLedger.actor_id),
  not: fn.not(ck.eq(activityLedger.event_phase, 1)),
  notEmpty: fn.notEmpty(["vip"]),
  range: fn.range(1, 5),
  replicate: fn.replicate<string>("vip", [1, 2]),
  reverse: fn.reverse<string>(["vip", "pro"]),
  sum: fn.sum(activityLedger.delta_value),
  sumIf: fn.sumIf(activityLedger.delta_value, ck.eq(activityLedger.event_phase, 1)),
  toDate: fn.toDate(activityLedger.event_time),
  toDateTime: fn.toDateTime(activityLedger.event_time, "UTC"),
  toStartOfMonth: fn.toStartOfMonth(activityLedger.event_time),
  toString: fn.toString(activityLedger.actor_id),
  tuple: fn.tuple(activityLedger.system_id, activityLedger.actor_id),
  tupleElement: fn.tupleElement<number>(fn.tuple(activityLedger.system_id, activityLedger.actor_id), 2),
  uniqExact: fn.uniqExact(activityLedger.actor_id),
  withParams: fn.withParams<number>("quantile", [0.95], activityLedger.actor_id),
} satisfies Omit<{ readonly [K in keyof typeof fn]: unknown }, "table">;

const tableFunctionTypeMatrix = {
  call: fn.table.call("numbers", 10),
} satisfies { readonly [K in keyof typeof fn.table]: unknown };

type _FunctionDataMatrix = Expect<
  Equal<
    Pick<
      {
        readonly [K in keyof typeof functionTypeMatrix]: (typeof functionTypeMatrix)[K] extends Selection<infer TData>
          ? TData
          : never;
      },
      | "array"
      | "arrayAUCPR"
      | "arrayAll"
      | "arrayAutocorrelation"
      | "arrayAvg"
      | "arrayCount"
      | "arrayDotProduct"
      | "arrayEnumerate"
      | "arrayEnumerateDense"
      | "arrayExists"
      | "arrayFill"
      | "arrayFilter"
      | "arrayFirst"
      | "arrayFirstIndex"
      | "arrayFirstOrNull"
      | "arrayFold"
      | "arrayJaccardIndex"
      | "arrayLast"
      | "arrayLastIndex"
      | "arrayLastOrNull"
      | "arrayLevenshteinDistance"
      | "arrayMap"
      | "arrayMax"
      | "arrayMin"
      | "arrayPartialSort"
      | "arrayProduct"
      | "arrayROCAUC"
      | "arrayReduce"
      | "arrayReverseSplit"
      | "arraySimilarity"
      | "arraySum"
      | "arrayZip"
      | "count"
      | "emptyArrayDate"
      | "emptyArrayFloat64"
      | "emptyArrayInt64"
      | "emptyArrayString"
      | "has"
      | "hasAll"
      | "hasAny"
      | "hasSubstr"
      | "indexOf"
      | "kql_array_sort_asc"
      | "length"
      | "notEmpty"
      | "range"
      | "toDate"
      | "tupleElement"
    >,
    {
      readonly array: string[];
      readonly arrayAUCPR: number;
      readonly arrayAll: boolean;
      readonly arrayAutocorrelation: number[];
      readonly arrayAvg: number;
      readonly arrayCount: string;
      readonly arrayDotProduct: number;
      readonly arrayEnumerate: number[];
      readonly arrayEnumerateDense: number[];
      readonly arrayExists: boolean;
      readonly arrayFill: number[];
      readonly arrayFilter: number[];
      readonly arrayFirst: number;
      readonly arrayFirstIndex: number;
      readonly arrayFirstOrNull: number | null;
      readonly arrayFold: number;
      readonly arrayJaccardIndex: number;
      readonly arrayLast: number;
      readonly arrayLastIndex: number;
      readonly arrayLastOrNull: number | null;
      readonly arrayLevenshteinDistance: number;
      readonly arrayMap: number[];
      readonly arrayMax: number;
      readonly arrayMin: number;
      readonly arrayPartialSort: number[];
      readonly arrayProduct: number;
      readonly arrayROCAUC: number;
      readonly arrayReduce: string;
      readonly arrayReverseSplit: number[];
      readonly arraySimilarity: number;
      readonly arraySum: number;
      readonly arrayZip: unknown[];
      readonly count: string;
      readonly emptyArrayDate: Date[];
      readonly emptyArrayFloat64: number[];
      readonly emptyArrayInt64: string[];
      readonly emptyArrayString: string[];
      readonly has: boolean;
      readonly hasAll: boolean;
      readonly hasAny: boolean;
      readonly hasSubstr: boolean;
      readonly indexOf: string;
      readonly kql_array_sort_asc: readonly [string[]];
      readonly length: string;
      readonly notEmpty: boolean;
      readonly range: number[];
      readonly toDate: Date;
      readonly tupleElement: number;
    }
  >
>;

const ckApiMatrix = {
  and: ck.and(ck.eq(activityLedger.actor_id, 10001), ck.ne(activityLedger.event_phase, 9)),
  asc: ck.asc(activityLedger.actor_id),
  between: ck.between(activityLedger.actor_id, 1, 100),
  contains: ck.contains(activityLedger.system_id, "system"),
  containsIgnoreCase: ck.containsIgnoreCase(activityLedger.system_id, "SYSTEM"),
  createSessionId: ck.createSessionId(),
  decodeRow: ck.decodeRow,
  desc: ck.desc(activityLedger.actor_id),
  endsWith: ck.endsWith(activityLedger.system_id, "_a"),
  endsWithIgnoreCase: ck.endsWithIgnoreCase(activityLedger.system_id, "_A"),
  eq: ck.eq(activityLedger.actor_id, 10001),
  exists: ck.exists(db.select({ actor_id: activityLedger.actor_id }).from(activityLedger)),
  expr: ck.expr<boolean>(csql`1`, { decoder: (value) => Number(value) === 1, sqlType: "UInt8" }),
  fn: ck.fn,
  gt: ck.gt(activityLedger.actor_id, 0),
  gte: ck.gte(activityLedger.actor_id, 0),
  has: ck.has(chTypeNameMatrix.array[0], "vip"),
  hasAll: ck.hasAll(chTypeNameMatrix.array[0], ["vip"]),
  hasAny: ck.hasAny(chTypeNameMatrix.array[0], ["vip"]),
  hasSubstr: ck.hasSubstr(chTypeNameMatrix.array[0], ["vip"]),
  ilike: ck.ilike(activityLedger.system_id, "%system%"),
  inArray: ck.inArray(activityLedger.event_phase, [0, 1]),
  like: ck.like(activityLedger.system_id, "%system%"),
  lt: ck.lt(activityLedger.actor_id, 100),
  lte: ck.lte(activityLedger.actor_id, 100),
  ne: ck.ne(activityLedger.actor_id, 10002),
  not: ck.not(ck.eq(activityLedger.actor_id, 10001)),
  notExists: ck.notExists(db.select({ actor_id: activityLedger.actor_id }).from(activityLedger)),
  notIlike: ck.notIlike(activityLedger.system_id, "%test%"),
  notInArray: ck.notInArray(activityLedger.event_phase, [8, 9]),
  notLike: ck.notLike(activityLedger.system_id, "%test%"),
  or: ck.or(ck.eq(activityLedger.event_phase, 0), ck.eq(activityLedger.event_phase, 1)),
  startsWith: ck.startsWith(activityLedger.system_id, "system"),
  startsWithIgnoreCase: ck.startsWithIgnoreCase(activityLedger.system_id, "SYSTEM"),
} satisfies { readonly [K in keyof typeof ck]: unknown };

const csqlMatrix = {
  identifier: csql.identifier({ table: "activity_ledger", column: "actor_id" }),
  join: csql.join([csql`select 1`, csql`select 2`], csql`, `),
} satisfies { readonly [K in keyof typeof csql]: unknown };

const csqlTaggedTemplate = {
  tagged: csql`select ${activityLedger.actor_id}`,
};

const nameSelection: Selection<string> = fn.toString(activityLedger.system_id);
const idPredicate: Predicate = ck.eq(activityLedger.actor_id, 1);
const sortOrder: Order = ck.asc(nameSelection);

// @ts-expect-error decimal config must stay object-shaped.
chType.decimal(20, 5);
// @ts-expect-error decimal named config must stay object-shaped.
chType.decimal("metric_value", 20, 5);
// @ts-expect-error fixedString config must stay object-shaped.
chType.fixedString(8);
// @ts-expect-error fixedString named config must stay object-shaped.
chType.fixedString("code", 8);
// @ts-expect-error dateTime64 config must stay object-shaped.
chType.dateTime64(9);
// @ts-expect-error time64 config must stay object-shaped.
chType.time64(6);
// @ts-expect-error qbit dimensions must stay object-shaped.
chType.qbit(chType.float32(), 8);
// @ts-expect-error simpleAggregateFunction named form requires a value column.
chType.simpleAggregateFunction("sum_value", { name: "sum" });
// @ts-expect-error csql only supports tagged-template usage.
csql("select 1");
// @ts-expect-error jsonExtract return type must come from chType.
fn.jsonExtract(csql`payload`, "Array(String)");
// @ts-expect-error Selection should not expose compile.
nameSelection.compile;
// @ts-expect-error Selection should not expose decoder.
nameSelection.decoder;
// @ts-expect-error Selection should not expose sqlType.
nameSelection.sqlType;
// @ts-expect-error Selection should not expose sourceKey.
nameSelection.sourceKey;
// @ts-expect-error Predicate should not expose compile.
idPredicate.compile;
// @ts-expect-error Predicate should not expose decoder.
idPredicate.decoder;
// @ts-expect-error ck.sql should not be part of the public ck namespace.
ck.sql;
// @ts-expect-error escapeLike should not be part of the public ck namespace.
ck.escapeLike("literal");
// @ts-expect-error schema factory values should not stay root-exported.
RootApi.int32;
// @ts-expect-error SqlExpression should remain internal to the package root.
const hiddenSqlExpression: RootApi.SqlExpression | undefined = undefined;
// @ts-expect-error Grouping should remain internal to the package root.
const hiddenGrouping: RootApi.Grouping | undefined = undefined;

type HasChType = "chType" extends keyof typeof import("../index") ? true : false;
type HasRootInt32 = "int32" extends keyof typeof import("../index") ? true : false;
type _HasChType = Expect<Equal<HasChType, true>>;
type _HasNoRootInt32 = Expect<Equal<HasRootInt32, false>>;

void chTypeNameMatrix;
void columnTypeMatrix;
void functionTypeMatrix;
void tableFunctionTypeMatrix;
void ckApiMatrix;
void csqlMatrix;
void csqlTaggedTemplate;
void hiddenSqlExpression;
void hiddenGrouping;
void sortOrder;

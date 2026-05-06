import type * as RootApi from "../index";
import {
  type ClickHouseBaseQueryOptions,
  type ClickHouseSettings,
  ck,
  ckSql,
  ckType,
  clickhouseClient,
  fn,
  type Order,
  type Predicate,
  type Selection,
} from "../index";
import { activityLedger, activityMetricLog } from "./fixtures";
import type { DataOf, Equal, Expect } from "./helpers";

const db = clickhouseClient({
  databaseUrl: "http://localhost:8123/public_api_matrix",
});

const settingsDb = clickhouseClient({
  databaseUrl: "http://localhost:8123/public_api_matrix",
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

settingsDb.execute(ckSql`SELECT 1`, requestOptions);
settingsChildDb.execute(ckSql`SELECT 1`);
db.execute(ckSql`SELECT 1`, { clickhouse_settings: knownAndDynamicSettings });

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

const ckTypeNameMatrix = {
  aggregateFunction: [
    ckType.aggregateFunction("sum", ckType.uint64()),
    ckType.aggregateFunction("sum_state", { name: "sum", args: [ckType.uint64()] }),
  ],
  array: [ckType.array(ckType.string()), ckType.array("tag_names", ckType.string())],
  bfloat16: [ckType.bfloat16(), ckType.bfloat16("score_bf16")],
  bool: [ckType.bool(), ckType.bool("is_active")],
  date: [ckType.date(), ckType.date("event_date")],
  date32: [ckType.date32(), ckType.date32("event_date32")],
  dateTime: [ckType.dateTime(), ckType.dateTime("created_at")],
  dateTime64: [
    ckType.dateTime64({ precision: 9, timezone: "UTC" }),
    ckType.dateTime64("created_at_64", { precision: 9, timezone: "UTC" }),
  ],
  decimal: [ckType.decimal({ precision: 20, scale: 5 }), ckType.decimal("metric_value", { precision: 20, scale: 5 })],
  dynamic: [ckType.dynamic<{ label: string }>(), ckType.dynamic<{ label: string }>("payload_dynamic")],
  enum8: [
    ckType.enum8<"open" | "closed">({ open: 1, closed: 2 }),
    ckType.enum8<"open" | "closed">("status_8", { open: 1, closed: 2 }),
  ],
  enum16: [
    ckType.enum16<"small" | "large">({ small: 1, large: 1000 }),
    ckType.enum16<"small" | "large">("status_16", { small: 1, large: 1000 }),
  ],
  fixedString: [ckType.fixedString({ length: 8 }), ckType.fixedString("code", { length: 8 })],
  float32: [ckType.float32(), ckType.float32("ratio_32")],
  float64: [ckType.float64(), ckType.float64("ratio_64")],
  int8: [ckType.int8(), ckType.int8("i8")],
  int16: [ckType.int16(), ckType.int16("i16")],
  int32: [ckType.int32(), ckType.int32("i32")],
  int64: [ckType.int64(), ckType.int64("i64")],
  ipv4: [ckType.ipv4(), ckType.ipv4("ip_v4")],
  ipv6: [ckType.ipv6(), ckType.ipv6("ip_v6")],
  json: [ckType.json<{ id: number }>(), ckType.json<{ id: number }>("payload_json")],
  lineString: [ckType.lineString(), ckType.lineString("line_value")],
  lowCardinality: [ckType.lowCardinality(ckType.string()), ckType.lowCardinality("region", ckType.string())],
  map: [ckType.map(ckType.string(), ckType.int32()), ckType.map("attrs", ckType.string(), ckType.int32())],
  multiLineString: [ckType.multiLineString(), ckType.multiLineString("multi_line_value")],
  multiPolygon: [ckType.multiPolygon(), ckType.multiPolygon("multi_polygon_value")],
  nested: [
    ckType.nested({ id: ckType.int32(), name: ckType.string() }),
    ckType.nested("profiles", { id: ckType.int32(), name: ckType.string() }),
  ],
  nullable: [ckType.nullable(ckType.string()), ckType.nullable("optional_note", ckType.string())],
  point: [ckType.point(), ckType.point("point_value")],
  polygon: [ckType.polygon(), ckType.polygon("polygon_value")],
  qbit: [
    ckType.qbit(ckType.float32(), { dimensions: 8 }),
    ckType.qbit("embedding", ckType.float32(), { dimensions: 8 }),
  ],
  ring: [ckType.ring(), ckType.ring("ring_value")],
  simpleAggregateFunction: [
    ckType.simpleAggregateFunction("sum", ckType.uint64()),
    ckType.simpleAggregateFunction("sum_value", { name: "sum", value: ckType.uint64() }),
  ],
  string: [ckType.string(), ckType.string("user_id")],
  time: [ckType.time(), ckType.time("event_time")],
  time64: [ckType.time64({ precision: 6 }), ckType.time64("event_time_64", { precision: 6 })],
  tuple: [ckType.tuple(ckType.int32(), ckType.string()), ckType.tuple("point_pair", ckType.int32(), ckType.string())],
  uint8: [ckType.uint8(), ckType.uint8("u8")],
  uint16: [ckType.uint16(), ckType.uint16("u16")],
  uint32: [ckType.uint32(), ckType.uint32("u32")],
  uint64: [ckType.uint64(), ckType.uint64("u64")],
  uuid: [ckType.uuid(), ckType.uuid("entity_uuid")],
  variant: [
    ckType.variant(ckType.string(), ckType.int32()),
    ckType.variant("variant_value", ckType.string(), ckType.int32()),
  ],
} satisfies { readonly [K in keyof typeof ckType]: readonly [unknown, unknown] };

const columnTypeMatrix = {
  aggregateFunction: ckType.aggregateFunction<number>("sum", ckType.uint64()),
  array: ckType.array(ckType.string()),
  bfloat16: ckType.bfloat16(),
  bool: ckType.bool(),
  date: ckType.date(),
  date32: ckType.date32(),
  dateTime: ckType.dateTime(),
  dateTime64: ckType.dateTime64({ precision: 9 }),
  decimal: ckType.decimal({ precision: 20, scale: 5 }),
  dynamic: ckType.dynamic<{ label: string }>(),
  enum8: ckType.enum8<"open" | "closed">({ open: 1, closed: 2 }),
  enum16: ckType.enum16<"small" | "large">({ small: 1, large: 1000 }),
  fixedString: ckType.fixedString({ length: 8 }),
  float32: ckType.float32(),
  float64: ckType.float64(),
  int8: ckType.int8(),
  int16: ckType.int16(),
  int32: ckType.int32(),
  int64: ckType.int64(),
  ipv4: ckType.ipv4(),
  ipv6: ckType.ipv6(),
  json: ckType.json<{ id: number }>(),
  lineString: ckType.lineString(),
  lowCardinality: ckType.lowCardinality(ckType.string()),
  map: ckType.map(ckType.string(), ckType.int32()),
  multiLineString: ckType.multiLineString(),
  multiPolygon: ckType.multiPolygon(),
  nested: ckType.nested({ id: ckType.int32(), name: ckType.string() }),
  nullable: ckType.nullable(ckType.string()),
  point: ckType.point(),
  polygon: ckType.polygon(),
  qbit: ckType.qbit(ckType.float32(), { dimensions: 8 }),
  ring: ckType.ring(),
  simpleAggregateFunction: ckType.simpleAggregateFunction<number>("sum", ckType.uint64()),
  string: ckType.string(),
  time: ckType.time(),
  time64: ckType.time64({ precision: 6 }),
  tuple: ckType.tuple(ckType.int32(), ckType.string()),
  uint8: ckType.uint8(),
  uint16: ckType.uint16(),
  uint32: ckType.uint32(),
  uint64: ckType.uint64(),
  uuid: ckType.uuid(),
  variant: ckType.variant(ckType.string(), ckType.int32()),
} satisfies { readonly [K in keyof typeof ckType]: RootApi.Column };

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
      readonly time: string;
      readonly time64: string;
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

const conversionFunctionTypeMatrix = {
  accurateCast: fn.accurateCast<number>("1", "UInt8"),
  accurateCastOrDefault: fn.accurateCastOrDefault<number>("bad", "UInt8", 7),
  accurateCastOrNull: fn.accurateCastOrNull<number>("bad", "UInt8"),
  cast: fn.cast<number>("1", "UInt8"),
  date: fn.date(activityLedger.event_time),
  formatDateTime: fn.formatDateTime(activityLedger.event_time, "%Y-%m-%d", "UTC"),
  formatRow: fn.formatRow("JSONEachRow", activityLedger.actor_id),
  formatRowNoNewline: fn.formatRowNoNewline("JSONEachRow", activityLedger.actor_id),
  parseDateTime: fn.parseDateTime("2026-01-01 00:00:00", "%F %T", "UTC"),
  parseDateTime32BestEffort: fn.parseDateTime32BestEffort("2026-01-01 00:00:00", "UTC"),
  parseDateTime32BestEffortOrNull: fn.parseDateTime32BestEffortOrNull("bad", "UTC"),
  parseDateTime32BestEffortOrZero: fn.parseDateTime32BestEffortOrZero("bad", "UTC"),
  parseDateTime64: fn.parseDateTime64("2026-01-01 00:00:00.123", "%F %T.%f", "UTC"),
  parseDateTime64BestEffort: fn.parseDateTime64BestEffort("2026-01-01 00:00:00.123", 3, "UTC"),
  parseDateTime64BestEffortOrNull: fn.parseDateTime64BestEffortOrNull("bad", 3, "UTC"),
  parseDateTime64BestEffortOrZero: fn.parseDateTime64BestEffortOrZero("bad", 3, "UTC"),
  parseDateTime64BestEffortUS: fn.parseDateTime64BestEffortUS("01/02/2026 00:00:00.123", 3, "UTC"),
  parseDateTime64BestEffortUSOrNull: fn.parseDateTime64BestEffortUSOrNull("bad", 3, "UTC"),
  parseDateTime64BestEffortUSOrZero: fn.parseDateTime64BestEffortUSOrZero("bad", 3, "UTC"),
  parseDateTime64InJodaSyntax: fn.parseDateTime64InJodaSyntax(
    "2026-01-01 00:00:00.123",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "UTC",
  ),
  parseDateTime64InJodaSyntaxOrNull: fn.parseDateTime64InJodaSyntaxOrNull("bad", "yyyy-MM-dd HH:mm:ss.SSS", "UTC"),
  parseDateTime64InJodaSyntaxOrZero: fn.parseDateTime64InJodaSyntaxOrZero("bad", "yyyy-MM-dd HH:mm:ss.SSS", "UTC"),
  parseDateTime64OrNull: fn.parseDateTime64OrNull("bad", "%F %T", "UTC"),
  parseDateTime64OrZero: fn.parseDateTime64OrZero("bad", "%F %T", "UTC"),
  parseDateTimeBestEffort: fn.parseDateTimeBestEffort("2026-01-01 00:00:00", "UTC"),
  parseDateTimeBestEffortOrNull: fn.parseDateTimeBestEffortOrNull("bad", "UTC"),
  parseDateTimeBestEffortOrZero: fn.parseDateTimeBestEffortOrZero("bad", "UTC"),
  parseDateTimeBestEffortUS: fn.parseDateTimeBestEffortUS("01/02/2026 00:00:00", "UTC"),
  parseDateTimeBestEffortUSOrNull: fn.parseDateTimeBestEffortUSOrNull("bad", "UTC"),
  parseDateTimeBestEffortUSOrZero: fn.parseDateTimeBestEffortUSOrZero("bad", "UTC"),
  parseDateTimeInJodaSyntax: fn.parseDateTimeInJodaSyntax("2026-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "UTC"),
  parseDateTimeInJodaSyntaxOrNull: fn.parseDateTimeInJodaSyntaxOrNull("bad", "yyyy-MM-dd HH:mm:ss", "UTC"),
  parseDateTimeInJodaSyntaxOrZero: fn.parseDateTimeInJodaSyntaxOrZero("bad", "yyyy-MM-dd HH:mm:ss", "UTC"),
  parseDateTimeOrNull: fn.parseDateTimeOrNull("bad", "%F %T", "UTC"),
  parseDateTimeOrZero: fn.parseDateTimeOrZero("bad", "%F %T", "UTC"),
  reinterpret: fn.reinterpret<number>("a", "UInt8"),
  reinterpretAsDate: fn.reinterpretAsDate(0),
  reinterpretAsDateTime: fn.reinterpretAsDateTime(0),
  reinterpretAsFixedString: fn.reinterpretAsFixedString(0),
  reinterpretAsFloat32: fn.reinterpretAsFloat32("a"),
  reinterpretAsFloat64: fn.reinterpretAsFloat64("a"),
  reinterpretAsInt128: fn.reinterpretAsInt128("a"),
  reinterpretAsInt16: fn.reinterpretAsInt16("a"),
  reinterpretAsInt256: fn.reinterpretAsInt256("a"),
  reinterpretAsInt32: fn.reinterpretAsInt32("a"),
  reinterpretAsInt64: fn.reinterpretAsInt64("a"),
  reinterpretAsInt8: fn.reinterpretAsInt8("a"),
  reinterpretAsString: fn.reinterpretAsString(1),
  reinterpretAsUInt128: fn.reinterpretAsUInt128("a"),
  reinterpretAsUInt16: fn.reinterpretAsUInt16("a"),
  reinterpretAsUInt256: fn.reinterpretAsUInt256("a"),
  reinterpretAsUInt32: fn.reinterpretAsUInt32("a"),
  reinterpretAsUInt64: fn.reinterpretAsUInt64("a"),
  reinterpretAsUInt8: fn.reinterpretAsUInt8("a"),
  reinterpretAsUUID: fn.reinterpretAsUUID("a"),
  toBFloat16: fn.toBFloat16("1.5"),
  toBFloat16OrNull: fn.toBFloat16OrNull("bad"),
  toBFloat16OrZero: fn.toBFloat16OrZero("bad"),
  toBool: fn.toBool(1),
  toDate32OrDefault: fn.toDate32OrDefault("bad", "1970-01-01"),
  toDate32OrNull: fn.toDate32OrNull("bad"),
  toDate32OrZero: fn.toDate32OrZero("bad"),
  toDateOrDefault: fn.toDateOrDefault("bad", "1970-01-01"),
  toDateOrNull: fn.toDateOrNull("bad"),
  toDateOrZero: fn.toDateOrZero("bad"),
  toDateTime64OrDefault: fn.toDateTime64OrDefault("bad", 3, "UTC", "1970-01-01 00:00:00.000"),
  toDateTime64OrNull: fn.toDateTime64OrNull("bad", 3, "UTC"),
  toDateTime64OrZero: fn.toDateTime64OrZero("bad", 3, "UTC"),
  toDateTimeOrDefault: fn.toDateTimeOrDefault("bad", "UTC", "1970-01-01 00:00:00"),
  toDateTimeOrNull: fn.toDateTimeOrNull("bad", "UTC"),
  toDateTimeOrZero: fn.toDateTimeOrZero("bad", "UTC"),
  toDecimal128OrDefault: fn.toDecimal128OrDefault("bad", 5, "1.00000"),
  toDecimal128OrNull: fn.toDecimal128OrNull("bad", 5),
  toDecimal128OrZero: fn.toDecimal128OrZero("bad", 5),
  toDecimal256OrDefault: fn.toDecimal256OrDefault("bad", 5, "1.00000"),
  toDecimal256OrNull: fn.toDecimal256OrNull("bad", 5),
  toDecimal256OrZero: fn.toDecimal256OrZero("bad", 5),
  toDecimal32OrDefault: fn.toDecimal32OrDefault("bad", 2, "1.00"),
  toDecimal32OrNull: fn.toDecimal32OrNull("bad", 2),
  toDecimal32OrZero: fn.toDecimal32OrZero("bad", 2),
  toDecimal64OrDefault: fn.toDecimal64OrDefault("bad", 5, "1.00000"),
  toDecimal64OrNull: fn.toDecimal64OrNull("bad", 5),
  toDecimal64OrZero: fn.toDecimal64OrZero("bad", 5),
  toDecimalString: fn.toDecimalString("1.2345", 2),
  toFixedString: fn.toFixedString("abc", 8),
  toFloat32: fn.toFloat32("1.5"),
  toFloat32OrDefault: fn.toFloat32OrDefault("bad", 1.5),
  toFloat32OrNull: fn.toFloat32OrNull("bad"),
  toFloat32OrZero: fn.toFloat32OrZero("bad"),
  toFloat64: fn.toFloat64("1.5"),
  toFloat64OrDefault: fn.toFloat64OrDefault("bad", 1.5),
  toFloat64OrNull: fn.toFloat64OrNull("bad"),
  toFloat64OrZero: fn.toFloat64OrZero("bad"),
  toInt128: fn.toInt128("1"),
  toInt128OrDefault: fn.toInt128OrDefault("bad", 1),
  toInt128OrNull: fn.toInt128OrNull("bad"),
  toInt128OrZero: fn.toInt128OrZero("bad"),
  toInt16: fn.toInt16("1"),
  toInt16OrDefault: fn.toInt16OrDefault("bad", 1),
  toInt16OrNull: fn.toInt16OrNull("bad"),
  toInt16OrZero: fn.toInt16OrZero("bad"),
  toInt256: fn.toInt256("1"),
  toInt256OrDefault: fn.toInt256OrDefault("bad", 1),
  toInt256OrNull: fn.toInt256OrNull("bad"),
  toInt256OrZero: fn.toInt256OrZero("bad"),
  toInt32: fn.toInt32("1"),
  toInt32OrDefault: fn.toInt32OrDefault("bad", 1),
  toInt32OrNull: fn.toInt32OrNull("bad"),
  toInt32OrZero: fn.toInt32OrZero("bad"),
  toInt64: fn.toInt64("1"),
  toInt64OrDefault: fn.toInt64OrDefault("bad", 1),
  toInt64OrNull: fn.toInt64OrNull("bad"),
  toInt64OrZero: fn.toInt64OrZero("bad"),
  toInt8: fn.toInt8("1"),
  toInt8OrDefault: fn.toInt8OrDefault("bad", 1),
  toInt8OrNull: fn.toInt8OrNull("bad"),
  toInt8OrZero: fn.toInt8OrZero("bad"),
  toInterval: fn.toInterval(1, "day"),
  toIntervalDay: fn.toIntervalDay(1),
  toIntervalHour: fn.toIntervalHour(1),
  toIntervalMicrosecond: fn.toIntervalMicrosecond(1),
  toIntervalMillisecond: fn.toIntervalMillisecond(1),
  toIntervalMinute: fn.toIntervalMinute(1),
  toIntervalMonth: fn.toIntervalMonth(1),
  toIntervalNanosecond: fn.toIntervalNanosecond(1),
  toIntervalQuarter: fn.toIntervalQuarter(1),
  toIntervalSecond: fn.toIntervalSecond(1),
  toIntervalWeek: fn.toIntervalWeek(1),
  toIntervalYear: fn.toIntervalYear(1),
  toLowCardinality: fn.toLowCardinality<string>("vip"),
  toNullable: fn.toNullable<string>("vip"),
  toStringCutToZero: fn.toStringCutToZero("abc\u0000def"),
  toTime: fn.toTime("12:34:56"),
  toTime64: fn.toTime64("12:34:56.123", 6),
  toTime64OrNull: fn.toTime64OrNull("bad", 6),
  toTime64OrZero: fn.toTime64OrZero("bad", 6),
  toTimeOrNull: fn.toTimeOrNull("bad"),
  toTimeOrZero: fn.toTimeOrZero("bad"),
  toUInt128: fn.toUInt128("1"),
  toUInt128OrDefault: fn.toUInt128OrDefault("bad", 1),
  toUInt128OrNull: fn.toUInt128OrNull("bad"),
  toUInt128OrZero: fn.toUInt128OrZero("bad"),
  toUInt16: fn.toUInt16("1"),
  toUInt16OrDefault: fn.toUInt16OrDefault("bad", 1),
  toUInt16OrNull: fn.toUInt16OrNull("bad"),
  toUInt16OrZero: fn.toUInt16OrZero("bad"),
  toUInt256: fn.toUInt256("1"),
  toUInt256OrDefault: fn.toUInt256OrDefault("bad", 1),
  toUInt256OrNull: fn.toUInt256OrNull("bad"),
  toUInt256OrZero: fn.toUInt256OrZero("bad"),
  toUInt32: fn.toUInt32("1"),
  toUInt32OrDefault: fn.toUInt32OrDefault("bad", 1),
  toUInt32OrNull: fn.toUInt32OrNull("bad"),
  toUInt32OrZero: fn.toUInt32OrZero("bad"),
  toUInt64: fn.toUInt64("1"),
  toUInt64OrDefault: fn.toUInt64OrDefault("bad", 1),
  toUInt64OrNull: fn.toUInt64OrNull("bad"),
  toUInt64OrZero: fn.toUInt64OrZero("bad"),
  toUInt8: fn.toUInt8("1"),
  toUInt8OrDefault: fn.toUInt8OrDefault("bad", 1),
  toUInt8OrNull: fn.toUInt8OrNull("bad"),
  toUInt8OrZero: fn.toUInt8OrZero("bad"),
  toUUID: fn.toUUID("00000000-0000-0000-0000-000000000000"),
  toUUIDOrZero: fn.toUUIDOrZero("bad"),
} satisfies Record<string, Selection>;

const functionTypeMatrix = {
  ...conversionFunctionTypeMatrix,
  array: fn.array<string>("vip", "pro"),
  arrayAUCPR: fn.arrayAUCPR([0.1, 0.4], [0, 1]),
  arrayAll: fn.arrayAll(ckSql`x -> x > 0`, [1, 2]),
  arrayAutocorrelation: fn.arrayAutocorrelation([1, 2, 3]),
  arrayAvg: fn.arrayAvg([1, 2, 3]),
  arrayCompact: fn.arrayCompact<string>(["vip", "vip", "pro"]),
  arrayConcat: fn.arrayConcat<string>(["vip"], ["pro"]),
  arrayCount: fn.arrayCount(ckSql`x -> x > 0`, [1, 2, 3]),
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
  arrayExists: fn.arrayExists(ckSql`x -> x > 1`, [1, 2]),
  arrayFill: fn.arrayFill<number>(ckSql`x -> x > 0`, [1, 0, 2]),
  arrayFilter: fn.arrayFilter<number>(ckSql`x -> x > 1`, [1, 2, 3]),
  arrayFirst: fn.arrayFirst<number>(ckSql`x -> x > 1`, [1, 2, 3]),
  arrayFirstIndex: fn.arrayFirstIndex(ckSql`x -> x > 1`, [1, 2, 3]),
  arrayFirstOrNull: fn.arrayFirstOrNull<number>(ckSql`x -> x > 9`, [1, 2, 3]),
  arrayFlatten: fn.arrayFlatten<string>([["vip"], ["pro"]]),
  arrayFold: fn.arrayFold<number>(ckSql`(acc, x) -> acc + x`, [1, 2], 0),
  arrayIntersect: fn.arrayIntersect<string>(["vip"], ["pro", "vip"]),
  arrayJaccardIndex: fn.arrayJaccardIndex(["vip"], ["vip", "pro"]),
  arrayJoin: fn.arrayJoin<number>([1, 2]),
  arrayLast: fn.arrayLast<number>(ckSql`x -> x > 1`, [1, 2, 3]),
  arrayLastIndex: fn.arrayLastIndex(ckSql`x -> x > 1`, [1, 2, 3]),
  arrayLastOrNull: fn.arrayLastOrNull<number>(ckSql`x -> x > 9`, [1, 2, 3]),
  arrayLevenshteinDistance: fn.arrayLevenshteinDistance(["A"], ["B"]),
  arrayLevenshteinDistanceWeighted: fn.arrayLevenshteinDistanceWeighted(["A"], ["B"], [1], [1]),
  arrayMap: fn.arrayMap<number>(ckSql`x -> x + 1`, [1, 2]),
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
  arrayReverseFill: fn.arrayReverseFill<number>(ckSql`x -> x > 0`, [1, 0, 2]),
  arrayReverseSort: fn.arrayReverseSort<number>([2, 1]),
  arrayReverseSplit: fn.arrayReverseSplit<number>(ckSql`x -> x = 0`, [1, 0, 2]),
  arrayRotateLeft: fn.arrayRotateLeft<number>([1, 2, 3], 1),
  arrayRotateRight: fn.arrayRotateRight<number>([1, 2, 3], 1),
  arrayShiftLeft: fn.arrayShiftLeft<number>([1, 2, 3], 1, 0),
  arrayShiftRight: fn.arrayShiftRight<number>([1, 2, 3], 1, 0),
  arrayShingles: fn.arrayShingles<readonly string[]>(["a", "b", "c"], 2),
  arrayShuffle: fn.arrayShuffle<string>(["vip", "pro"]),
  arraySimilarity: fn.arraySimilarity(["A"], ["B"], [1], [1]),
  arraySlice: fn.arraySlice<string>(["vip", "pro"], 1, 1),
  arraySort: fn.arraySort<number>([2, 1]),
  arraySplit: fn.arraySplit<number>(ckSql`x -> x = 0`, [1, 0, 2]),
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
  fromUnixTimestamp: fn.fromUnixTimestamp(1735689600),
  fromUnixTimestamp64Micro: fn.fromUnixTimestamp64Micro("1735689600000000", "UTC"),
  fromUnixTimestamp64Milli: fn.fromUnixTimestamp64Milli("1735689600000", "UTC"),
  fromUnixTimestamp64Nano: fn.fromUnixTimestamp64Nano("1735689600000000000", "UTC"),
  fromUnixTimestamp64Second: fn.fromUnixTimestamp64Second("1735689600", "UTC"),
  has: fn.has(ckTypeNameMatrix.array[0], "vip"),
  hasAll: fn.hasAll(ckTypeNameMatrix.array[0], ["vip"]),
  hasAny: fn.hasAny(ckTypeNameMatrix.array[0], ["vip"]),
  hasSubstr: fn.hasSubstr(ckTypeNameMatrix.array[0], ["vip"]),
  indexOf: fn.indexOf(["vip"], "vip"),
  indexOfAssumeSorted: fn.indexOfAssumeSorted(["pro", "vip"], "vip"),
  jsonExtract: fn.jsonExtract(activityMetricLog.payload, ckType.array(ckType.string()), "labels"),
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
  toDate32: fn.toDate32(activityLedger.event_time),
  toDateTime: fn.toDateTime(activityLedger.event_time, "UTC"),
  toDateTime32: fn.toDateTime32(activityLedger.event_time, "UTC"),
  toDateTime64: fn.toDateTime64(activityLedger.event_time, 3, "UTC"),
  toDecimal32: fn.toDecimal32(activityLedger.delta_value, 4),
  toDecimal64: fn.toDecimal64(activityLedger.delta_value, 5),
  toDecimal128: fn.toDecimal128(activityLedger.delta_value, 5),
  toDecimal256: fn.toDecimal256(activityLedger.delta_value, 5),
  toStartOfMonth: fn.toStartOfMonth(activityLedger.event_time),
  toString: fn.toString(activityLedger.actor_id),
  toUnixTimestamp: fn.toUnixTimestamp(activityLedger.event_time, "UTC"),
  toUnixTimestamp64Micro: fn.toUnixTimestamp64Micro(activityLedger.event_time),
  toUnixTimestamp64Milli: fn.toUnixTimestamp64Milli(activityLedger.event_time),
  toUnixTimestamp64Nano: fn.toUnixTimestamp64Nano(activityLedger.event_time),
  toUnixTimestamp64Second: fn.toUnixTimestamp64Second(activityLedger.event_time),
  tuple: fn.tuple(activityLedger.system_id, activityLedger.actor_id),
  tupleElement: fn.tupleElement<number>(fn.tuple(activityLedger.system_id, activityLedger.actor_id), 2),
  uniqExact: fn.uniqExact(activityLedger.actor_id),
  withParams: fn.withParams<number>("quantile", [0.95], activityLedger.actor_id),
} satisfies Omit<{ readonly [K in keyof typeof fn]: unknown }, "table">;

const uniqExactModeMatrix = {
  unsafe: fn.uniqExact(activityLedger.actor_id),
  safe: fn.uniqExact(activityLedger.actor_id).toSafe(),
  mixed: fn.uniqExact(activityLedger.actor_id).toMixed(),
  unsafeAfterSafe: fn.uniqExact(activityLedger.actor_id).toSafe().toUnsafe(),
};

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
      readonly arrayFilter: unknown[];
      readonly arrayFirst: number;
      readonly arrayFirstIndex: number;
      readonly arrayFirstOrNull: number | null;
      readonly arrayFold: number;
      readonly arrayJaccardIndex: number;
      readonly arrayLast: number;
      readonly arrayLastIndex: number;
      readonly arrayLastOrNull: number | null;
      readonly arrayLevenshteinDistance: number;
      readonly arrayMap: unknown[];
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
      readonly count: number;
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
      readonly range: unknown[];
      readonly toDate: Date;
      readonly tupleElement: number;
    }
  >
>;

type _UniqExactModeDataMatrix = Expect<
  Equal<
    {
      readonly [K in keyof typeof uniqExactModeMatrix]: (typeof uniqExactModeMatrix)[K] extends Selection<infer TData>
        ? TData
        : never;
    },
    {
      readonly unsafe: number;
      readonly safe: string;
      readonly mixed: number | string;
      readonly unsafeAfterSafe: number;
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
  expr: ck.expr<boolean>(ckSql`1`, { decoder: (value) => Number(value) === 1, sqlType: "UInt8" }),
  fn: ck.fn,
  gt: ck.gt(activityLedger.actor_id, 0),
  gte: ck.gte(activityLedger.actor_id, 0),
  has: ck.has(ckTypeNameMatrix.array[0], "vip"),
  hasAll: ck.hasAll(ckTypeNameMatrix.array[0], ["vip"]),
  hasAny: ck.hasAny(ckTypeNameMatrix.array[0], ["vip"]),
  hasSubstr: ck.hasSubstr(ckTypeNameMatrix.array[0], ["vip"]),
  ilike: ck.ilike(activityLedger.system_id, "%system%"),
  inArray: ck.inArray(activityLedger.event_phase, [0, 1]),
  isNotNull: ck.isNotNull(activityLedger.system_id),
  isNull: ck.isNull(activityLedger.system_id),
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

const limitValueMatrix = [
  db.select({ actor_id: activityLedger.actor_id }).from(activityLedger).limit(10),
  db.select({ actor_id: activityLedger.actor_id }).from(activityLedger).offset(0n),
  db.select({ actor_id: activityLedger.actor_id }).from(activityLedger).limit(ckSql`toUInt64(10)`),
  db.select({ actor_id: activityLedger.actor_id }).from(activityLedger).limitBy([activityLedger.actor_id], ckSql`10`),
];

// @ts-expect-error LIMIT values must not accept arbitrary column or function selections.
db.select({ actor_id: activityLedger.actor_id }).from(activityLedger).limit(activityLedger.actor_id);

db.select({ actor_id: activityLedger.actor_id })
  .from(activityLedger)
  // @ts-expect-error LIMIT BY values must not accept arbitrary `Selection` wrappers.
  .limitBy([activityLedger.actor_id], ck.expr(ckSql`10`));

void limitValueMatrix;

const ckSqlMatrix = {
  identifier: ckSql.identifier({ table: "activity_ledger", column: "actor_id" }),
  join: ckSql.join([ckSql`select 1`, ckSql`select 2`], ckSql`, `),
  decimal: ckSql.decimal(ckSql`sum(${activityLedger.delta_value})`, 20, 5),
} satisfies { readonly [K in keyof typeof ckSql]: unknown };

const ckSqlTaggedTemplate = {
  tagged: ckSql`select ${activityLedger.actor_id}`,
};

const nameSelection: Selection<string> = fn.toString(activityLedger.system_id);
const idPredicate: Predicate = ck.eq(activityLedger.actor_id, 1);
const sortOrder: Order = ck.asc(nameSelection);

// @ts-expect-error decimal config must stay object-shaped.
ckType.decimal(20, 5);
// @ts-expect-error decimal named config must stay object-shaped.
ckType.decimal("metric_value", 20, 5);
// @ts-expect-error fixedString config must stay object-shaped.
ckType.fixedString(8);
// @ts-expect-error fixedString named config must stay object-shaped.
ckType.fixedString("code", 8);
// @ts-expect-error dateTime64 config must stay object-shaped.
ckType.dateTime64(9);
// @ts-expect-error time64 config must stay object-shaped.
ckType.time64(6);
// @ts-expect-error qbit dimensions must stay object-shaped.
ckType.qbit(ckType.float32(), 8);
// @ts-expect-error simpleAggregateFunction named form requires a value column.
ckType.simpleAggregateFunction("sum_value", { name: "sum" });
// @ts-expect-error ckSql only supports tagged-template usage.
ckSql("select 1");
// @ts-expect-error jsonExtract return type must come from ckType.
fn.jsonExtract(ckSql`payload`, "Array(String)");
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

type HasCkType = "ckType" extends keyof typeof import("../index") ? true : false;
type HasRootInt32 = "int32" extends keyof typeof import("../index") ? true : false;
type _HasChType = Expect<Equal<HasCkType, true>>;
type _HasNoRootInt32 = Expect<Equal<HasRootInt32, false>>;

void ckTypeNameMatrix;
void columnTypeMatrix;
void functionTypeMatrix;
void tableFunctionTypeMatrix;
void ckApiMatrix;
void ckSqlMatrix;
void ckSqlTaggedTemplate;
void hiddenSqlExpression;
void hiddenGrouping;
void sortOrder;

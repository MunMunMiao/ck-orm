import { describe, expect, it } from "bun:test";
import type * as RootApi from "ck-orm";
import * as publicApi from "./index";
import { compileSql } from "./sql";

const expectedRootRuntimeKeys = [
  "ckAlias",
  "ckTable",
  "ckType",
  "ck",
  "clickhouseClient",
  "ckSql",
  "fn",
  "isClickHouseORMError",
  "isDecodeError",
] as const;

const expectedCkKeys = [
  "and",
  "asc",
  "between",
  "contains",
  "containsIgnoreCase",
  "createSessionId",
  "decodeRow",
  "desc",
  "endsWith",
  "endsWithIgnoreCase",
  "eq",
  "exists",
  "expr",
  "fn",
  "gt",
  "gte",
  "has",
  "hasAll",
  "hasAny",
  "hasSubstr",
  "ilike",
  "inArray",
  "isNotNull",
  "isNull",
  "like",
  "lt",
  "lte",
  "ne",
  "not",
  "notExists",
  "notIlike",
  "notInArray",
  "notLike",
  "or",
  "startsWith",
  "startsWithIgnoreCase",
] as const;

const expectedChTypeKeys = [
  "aggregateFunction",
  "array",
  "bfloat16",
  "bool",
  "date",
  "date32",
  "dateTime",
  "dateTime64",
  "decimal",
  "dynamic",
  "enum16",
  "enum8",
  "fixedString",
  "float32",
  "float64",
  "int16",
  "int32",
  "int64",
  "int8",
  "ipv4",
  "ipv6",
  "json",
  "lineString",
  "lowCardinality",
  "map",
  "multiLineString",
  "multiPolygon",
  "nested",
  "nullable",
  "point",
  "polygon",
  "qbit",
  "ring",
  "simpleAggregateFunction",
  "string",
  "time",
  "time64",
  "tuple",
  "uint16",
  "uint32",
  "uint64",
  "uint8",
  "uuid",
  "variant",
] as const;

const expectedFnKeys = [
  "accurateCast",
  "accurateCastOrDefault",
  "accurateCastOrNull",
  "array",
  "arrayAUCPR",
  "arrayAll",
  "arrayAutocorrelation",
  "arrayAvg",
  "arrayCompact",
  "arrayConcat",
  "arrayCount",
  "arrayCumSum",
  "arrayCumSumNonNegative",
  "arrayDifference",
  "arrayDistinct",
  "arrayDotProduct",
  "arrayElement",
  "arrayElementOrNull",
  "arrayEnumerate",
  "arrayEnumerateDense",
  "arrayEnumerateDenseRanked",
  "arrayEnumerateUniq",
  "arrayEnumerateUniqRanked",
  "arrayExcept",
  "arrayExists",
  "arrayFill",
  "arrayFilter",
  "arrayFirst",
  "arrayFirstIndex",
  "arrayFirstOrNull",
  "arrayFlatten",
  "arrayFold",
  "arrayIntersect",
  "arrayJaccardIndex",
  "arrayJoin",
  "arrayLast",
  "arrayLastIndex",
  "arrayLastOrNull",
  "arrayLevenshteinDistance",
  "arrayLevenshteinDistanceWeighted",
  "arrayMap",
  "arrayMax",
  "arrayMin",
  "arrayNormalizedGini",
  "arrayPartialReverseSort",
  "arrayPartialShuffle",
  "arrayPartialSort",
  "arrayPopBack",
  "arrayPopFront",
  "arrayProduct",
  "arrayPushBack",
  "arrayPushFront",
  "arrayROCAUC",
  "arrayRandomSample",
  "arrayReduce",
  "arrayReduceInRanges",
  "arrayRemove",
  "arrayResize",
  "arrayReverse",
  "arrayReverseFill",
  "arrayReverseSort",
  "arrayReverseSplit",
  "arrayRotateLeft",
  "arrayRotateRight",
  "arrayShiftLeft",
  "arrayShiftRight",
  "arrayShingles",
  "arrayShuffle",
  "arraySimilarity",
  "arraySlice",
  "arraySort",
  "arraySplit",
  "arraySum",
  "arraySymmetricDifference",
  "arrayTranspose",
  "arrayUnion",
  "arrayUniq",
  "arrayWithConstant",
  "arrayZip",
  "arrayZipUnaligned",
  "avg",
  "call",
  "cast",
  "coalesce",
  "count",
  "countEqual",
  "countIf",
  "date",
  "empty",
  "emptyArrayDate",
  "emptyArrayDateTime",
  "emptyArrayFloat32",
  "emptyArrayFloat64",
  "emptyArrayInt16",
  "emptyArrayInt32",
  "emptyArrayInt64",
  "emptyArrayInt8",
  "emptyArrayString",
  "emptyArrayToSingle",
  "emptyArrayUInt16",
  "emptyArrayUInt32",
  "emptyArrayUInt64",
  "emptyArrayUInt8",
  "formatDateTime",
  "formatRow",
  "formatRowNoNewline",
  "fromUnixTimestamp",
  "fromUnixTimestamp64Micro",
  "fromUnixTimestamp64Milli",
  "fromUnixTimestamp64Nano",
  "fromUnixTimestamp64Second",
  "has",
  "hasAll",
  "hasAny",
  "hasSubstr",
  "indexOf",
  "indexOfAssumeSorted",
  "jsonExtract",
  "kql_array_sort_asc",
  "kql_array_sort_desc",
  "length",
  "max",
  "min",
  "not",
  "notEmpty",
  "parseDateTime",
  "parseDateTime32BestEffort",
  "parseDateTime32BestEffortOrNull",
  "parseDateTime32BestEffortOrZero",
  "parseDateTime64",
  "parseDateTime64BestEffort",
  "parseDateTime64BestEffortOrNull",
  "parseDateTime64BestEffortOrZero",
  "parseDateTime64BestEffortUS",
  "parseDateTime64BestEffortUSOrNull",
  "parseDateTime64BestEffortUSOrZero",
  "parseDateTime64InJodaSyntax",
  "parseDateTime64InJodaSyntaxOrNull",
  "parseDateTime64InJodaSyntaxOrZero",
  "parseDateTime64OrNull",
  "parseDateTime64OrZero",
  "parseDateTimeBestEffort",
  "parseDateTimeBestEffortOrNull",
  "parseDateTimeBestEffortOrZero",
  "parseDateTimeBestEffortUS",
  "parseDateTimeBestEffortUSOrNull",
  "parseDateTimeBestEffortUSOrZero",
  "parseDateTimeInJodaSyntax",
  "parseDateTimeInJodaSyntaxOrNull",
  "parseDateTimeInJodaSyntaxOrZero",
  "parseDateTimeOrNull",
  "parseDateTimeOrZero",
  "range",
  "reinterpret",
  "reinterpretAsDate",
  "reinterpretAsDateTime",
  "reinterpretAsFixedString",
  "reinterpretAsFloat32",
  "reinterpretAsFloat64",
  "reinterpretAsInt128",
  "reinterpretAsInt16",
  "reinterpretAsInt256",
  "reinterpretAsInt32",
  "reinterpretAsInt64",
  "reinterpretAsInt8",
  "reinterpretAsString",
  "reinterpretAsUInt128",
  "reinterpretAsUInt16",
  "reinterpretAsUInt256",
  "reinterpretAsUInt32",
  "reinterpretAsUInt64",
  "reinterpretAsUInt8",
  "reinterpretAsUUID",
  "replicate",
  "reverse",
  "sum",
  "sumIf",
  "table",
  "toBFloat16",
  "toBFloat16OrNull",
  "toBFloat16OrZero",
  "toBool",
  "toDate",
  "toDate32",
  "toDate32OrDefault",
  "toDate32OrNull",
  "toDate32OrZero",
  "toDateOrDefault",
  "toDateOrNull",
  "toDateOrZero",
  "toDateTime",
  "toDateTime32",
  "toDateTime64",
  "toDateTime64OrDefault",
  "toDateTime64OrNull",
  "toDateTime64OrZero",
  "toDateTimeOrDefault",
  "toDateTimeOrNull",
  "toDateTimeOrZero",
  "toDecimal128",
  "toDecimal128OrDefault",
  "toDecimal128OrNull",
  "toDecimal128OrZero",
  "toDecimal256",
  "toDecimal256OrDefault",
  "toDecimal256OrNull",
  "toDecimal256OrZero",
  "toDecimal32",
  "toDecimal32OrDefault",
  "toDecimal32OrNull",
  "toDecimal32OrZero",
  "toDecimal64",
  "toDecimal64OrDefault",
  "toDecimal64OrNull",
  "toDecimal64OrZero",
  "toDecimalString",
  "toFixedString",
  "toFloat32",
  "toFloat32OrDefault",
  "toFloat32OrNull",
  "toFloat32OrZero",
  "toFloat64",
  "toFloat64OrDefault",
  "toFloat64OrNull",
  "toFloat64OrZero",
  "toInt128",
  "toInt128OrDefault",
  "toInt128OrNull",
  "toInt128OrZero",
  "toInt16",
  "toInt16OrDefault",
  "toInt16OrNull",
  "toInt16OrZero",
  "toInt256",
  "toInt256OrDefault",
  "toInt256OrNull",
  "toInt256OrZero",
  "toInt32",
  "toInt32OrDefault",
  "toInt32OrNull",
  "toInt32OrZero",
  "toInt64",
  "toInt64OrDefault",
  "toInt64OrNull",
  "toInt64OrZero",
  "toInt8",
  "toInt8OrDefault",
  "toInt8OrNull",
  "toInt8OrZero",
  "toInterval",
  "toIntervalDay",
  "toIntervalHour",
  "toIntervalMicrosecond",
  "toIntervalMillisecond",
  "toIntervalMinute",
  "toIntervalMonth",
  "toIntervalNanosecond",
  "toIntervalQuarter",
  "toIntervalSecond",
  "toIntervalWeek",
  "toIntervalYear",
  "toLowCardinality",
  "toNullable",
  "toStartOfMonth",
  "toString",
  "toStringCutToZero",
  "toTime",
  "toTime64",
  "toTime64OrNull",
  "toTime64OrZero",
  "toTimeOrNull",
  "toTimeOrZero",
  "toUInt128",
  "toUInt128OrDefault",
  "toUInt128OrNull",
  "toUInt128OrZero",
  "toUInt16",
  "toUInt16OrDefault",
  "toUInt16OrNull",
  "toUInt16OrZero",
  "toUInt256",
  "toUInt256OrDefault",
  "toUInt256OrNull",
  "toUInt256OrZero",
  "toUInt32",
  "toUInt32OrDefault",
  "toUInt32OrNull",
  "toUInt32OrZero",
  "toUInt64",
  "toUInt64OrDefault",
  "toUInt64OrNull",
  "toUInt64OrZero",
  "toUInt8",
  "toUInt8OrDefault",
  "toUInt8OrNull",
  "toUInt8OrZero",
  "toUUID",
  "toUUIDOrZero",
  "toUnixTimestamp",
  "toUnixTimestamp64Micro",
  "toUnixTimestamp64Milli",
  "toUnixTimestamp64Nano",
  "toUnixTimestamp64Second",
  "tuple",
  "tupleElement",
  "uniqExact",
  "withParams",
] as const;

describe("ck-orm public api", function describePublicApi() {
  it("keeps runtime public namespace keys explicit", function testPublicNamespaceSurface() {
    expect(Object.keys(publicApi).sort()).toEqual([...expectedRootRuntimeKeys].sort());
    expect(Object.keys(publicApi.ck).sort()).toEqual([...expectedCkKeys].sort());
    expect(Object.keys(publicApi.ckSql).sort()).toEqual(["decimal", "identifier", "join"]);
    expect(Object.keys(publicApi.ckType).sort()).toEqual([...expectedChTypeKeys].sort());
    expect(Object.keys(publicApi.fn).sort()).toEqual([...expectedFnKeys].sort());
    expect(Object.keys(publicApi.fn.table).sort()).toEqual(["call"]);
  });

  it("keeps internal runtime helpers out of the package root", function testPrivateRuntimeHelpers() {
    expect("renderTableIdentifier" in publicApi).toBe(false);
    // @ts-expect-error ClickHouseTableEngine should stay private to the schema module
    expectType<RootApi.ClickHouseTableEngine | undefined>(undefined);
    // @ts-expect-error TableOptions should stay private to the schema module
    expectType<RootApi.TableOptions | undefined>(undefined);
  });

  it("keeps core schema and query builders available from the package root", function testPublicBuilders() {
    expect("ck" in publicApi).toBe(true);
    expect("ckSql" in publicApi).toBe(true);
    expect("ckType" in publicApi).toBe(true);
    expect("ckTable" in publicApi).toBe(true);
    expect("clickhouseClient" in publicApi).toBe(true);
    expect("Grouping" in publicApi).toBe(false);
    expect("Order" in publicApi).toBe(false);
    expect("Predicate" in publicApi).toBe(false);
    expect("Selection" in publicApi).toBe(false);
    expect("SqlExpression" in publicApi).toBe(false);
    expect("makeWhereCondition" in publicApi).toBe(false);
    expect("sql" in publicApi).toBe(false);
    expect("eq" in publicApi).toBe(false);
    expect("desc" in publicApi).toBe(false);
    expect("int32" in publicApi).toBe(false);
    expect("string" in publicApi).toBe(false);
    expect("decimal" in publicApi).toBe(false);
    expect("array" in publicApi).toBe(false);
    expect("tableFn" in publicApi).toBe(false);
    expect(typeof publicApi.ckType.int32).toBe("function");
    expect(typeof publicApi.ckType.string).toBe("function");
    expect(publicApi.ck.fn).toBe(publicApi.fn);
    expect(typeof publicApi.ck.eq).toBe("function");
    expect(typeof publicApi.ck.desc).toBe("function");
    expect(typeof publicApi.ck.contains).toBe("function");
    expect(typeof publicApi.ck.startsWith).toBe("function");
    expect(typeof publicApi.ck.endsWith).toBe("function");
    expect(typeof publicApi.ck.containsIgnoreCase).toBe("function");
    expect(typeof publicApi.ck.startsWithIgnoreCase).toBe("function");
    expect(typeof publicApi.ck.endsWithIgnoreCase).toBe("function");
    expect("escapeLike" in publicApi.ck).toBe(false);
    expect("sql" in publicApi.ck).toBe(false);
    expect(typeof publicApi.ckSql).toBe("function");
    expect(typeof publicApi.ckSql.identifier).toBe("function");
    expect(typeof publicApi.ckSql.join).toBe("function");
    expect("raw" in publicApi.ckSql).toBe(false);
    expect(typeof publicApi.fn.table.call).toBe("function");
  });

  it("rejects direct ckSql function calls at runtime", function testCkSqlDirectCallGuard() {
    expect(() => (publicApi.ckSql as unknown as (query: string) => unknown)("select 1")).toThrow(
      '[ck-orm] ckSql only supports tagged-template usage. Use ckSql`...` instead of ckSql("...").',
    );
  });

  it("keeps public ckSql helpers callable from the package root", function testPublicCkSqlHelpers() {
    const built = compileSql(publicApi.ckSql.join([publicApi.ckSql.identifier("events"), publicApi.ckSql`final`], " "));

    expect(built.query).toBe("`events` final");
    expect(compileSql(publicApi.ckSql.decimal(publicApi.ckSql`sum(amount)`, 18, 2)).query).toBe(
      "CAST(sum(amount) AS Decimal(18, 2))",
    );
  });

  it("keeps advanced root-exported types aligned with public_api.ts", function testRootExportedTypes() {
    expectType<RootApi.AnyColumn | undefined>(undefined);
    expectType<RootApi.Column | undefined>(undefined);
    expectType<RootApi.AnyTable | undefined>(undefined);
    expectType<RootApi.Table | undefined>(undefined);
    expectType<RootApi.ClickHouseBaseQueryOptions | undefined>(undefined);
    expectType<RootApi.ClickHouseClientConfig | undefined>(undefined);
    expectType<RootApi.ClickHouseEndpointOptions | undefined>(undefined);
    expectType<RootApi.ClickHouseKnownSettingName | undefined>(undefined);
    expectType<RootApi.ClickHouseKnownSettings | undefined>(undefined);
    expectType<RootApi.ClickHouseORMClient | undefined>(undefined);
    expectType<RootApi.ClickHouseORMExecutionState | undefined>(undefined);
    expectType<RootApi.ClickHouseORMInstrumentation | undefined>(undefined);
    expectType<RootApi.ClickHouseORMLogLevel | undefined>(undefined);
    expectType<RootApi.ClickHouseORMLogger | undefined>(undefined);
    expectType<RootApi.ClickHouseORMQueryErrorEvent | undefined>(undefined);
    expectType<RootApi.ClickHouseORMQueryEvent | undefined>(undefined);
    expectType<RootApi.ClickHouseORMQueryResultEvent | undefined>(undefined);
    expectType<RootApi.ClickHouseORMTracingOptions | undefined>(undefined);
    expectType<RootApi.ClickHouseORMQueryStatistics | undefined>(undefined);
    expectType<RootApi.ClickHouseQueryOptions | undefined>(undefined);
    expectType<RootApi.ClickHouseSettings | undefined>(undefined);
    expectType<RootApi.ClickHouseSettingValue | undefined>(undefined);
    expectType<RootApi.ClickHouseStreamOptions | undefined>(undefined);
    expectType<RootApi.CompiledQuery | undefined>(undefined);
    expectType<RootApi.CompiledQueryMetadata | undefined>(undefined);
    expectType<RootApi.CreateTemporaryTableOptions | undefined>(undefined);
    expectType<RootApi.InferInsertModel<RootApi.AnyTable> | undefined>(undefined);
    expectType<RootApi.InferInsertSchema<Record<string, RootApi.AnyTable>> | undefined>(undefined);
    expectType<RootApi.InferSelectModel<RootApi.AnyTable> | undefined>(undefined);
    expectType<RootApi.InferSelectSchema<Record<string, RootApi.AnyTable>> | undefined>(undefined);
    expectType<RootApi.JsonPathSegment | undefined>(undefined);
    expectType<RootApi.Order | undefined>(undefined);
    expectType<RootApi.Predicate | undefined>(undefined);
    expectType<RootApi.Selection | undefined>(undefined);
    expectType<RootApi.Session | undefined>(undefined);
    expectType<RootApi.SQLFragment | undefined>(undefined);
    // @ts-expect-error Grouping should remain private to clause internals
    expectType<RootApi.Grouping | undefined>(undefined);
  });

  it("keeps error guards and compatibility exports available from the package root", function testRootErrorExports() {
    expect("ClickHouseORMError" in publicApi).toBe(false);
    expect("DecodeError" in publicApi).toBe(false);
    expect("isClickHouseORMError" in publicApi).toBe(true);
    expect("isDecodeError" in publicApi).toBe(true);

    expectType<RootApi.ClickHouseORMError | undefined>(undefined);
    expectType<RootApi.DecodeError | undefined>(undefined);
  });
});

function expectType<TValue>(_value: TValue) {}

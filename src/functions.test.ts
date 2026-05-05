import { describe, expect, it } from "bun:test";
import {
  array as arrayColumn,
  bfloat16,
  dateTime,
  decimal,
  float32,
  float64,
  int32,
  int64,
  lowCardinality,
  nullable,
  string,
  uint64,
} from "./columns";
import { fn, tableFn } from "./functions";
import { compileSql, sql } from "./sql";

const officialArrayFunctionNames = [
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
  "countEqual",
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
  "has",
  "hasAll",
  "hasAny",
  "hasSubstr",
  "indexOf",
  "indexOfAssumeSorted",
  "kql_array_sort_asc",
  "kql_array_sort_desc",
  "length",
  "notEmpty",
  "range",
  "replicate",
  "reverse",
] as const;

const createContext = () => ({
  params: {} as Record<string, unknown>,
  nextParamIndex: 0,
});

const compileExpression = (expression: { compile(ctx: ReturnType<typeof createContext>): unknown }) => {
  const ctx = createContext();
  const built = compileSql(sql`${expression.compile(ctx)}`, ctx);
  return {
    query: built.query,
    params: {
      ...ctx.params,
      ...built.params,
    },
  };
};

describe("ck-orm functions", function describeClickHouseORMFunctions() {
  it("exposes ClickHouse official array function headings on fn", function testOfficialArrayFunctionSurface() {
    for (const name of officialArrayFunctionNames) {
      expect(fn).toHaveProperty(name);
    }
  });

  it("compiles common function helpers and parameterized functions", function testCompileFunctions() {
    const callBuilt = compileExpression(fn.call("toString", int32().bind({ name: "id", tableName: "orders" })));
    expect(callBuilt.query).toContain("toString(`orders`.`id`)");

    const toStringBuilt = compileExpression(fn.toString(int32().bind({ name: "id", tableName: "orders" })));
    expect(toStringBuilt.query).toContain("toString(`orders`.`id`)");

    const toDateBuilt = compileExpression(fn.toDate(string().bind({ name: "created_at", tableName: "orders" })));
    expect(toDateBuilt.query).toContain("toDate(`orders`.`created_at`)");

    const toDate32Built = compileExpression(fn.toDate32(string().bind({ name: "created_at", tableName: "orders" })));
    expect(toDate32Built.query).toContain("toDate32(`orders`.`created_at`)");

    const toDateTimeBuilt = compileExpression(
      fn.toDateTime(string().bind({ name: "created_at", tableName: "orders" }), "Asia/Shanghai"),
    );
    expect(toDateTimeBuilt.query).toContain("toDateTime(`orders`.`created_at`, {orm_param1:String})");
    expect(toDateTimeBuilt.params).toEqual({
      orm_param1: "Asia/Shanghai",
    });

    const toDateTime32Built = compileExpression(
      fn.toDateTime32(string().bind({ name: "created_at", tableName: "orders" }), "Asia/Shanghai"),
    );
    expect(toDateTime32Built.query).toContain("toDateTime32(`orders`.`created_at`, {orm_param1:String})");
    expect(toDateTime32Built.params).toEqual({
      orm_param1: "Asia/Shanghai",
    });

    const toDateTime64Built = compileExpression(
      fn.toDateTime64(string().bind({ name: "created_at", tableName: "orders" }), 3, "UTC"),
    );
    expect(toDateTime64Built.query).toContain("toDateTime64(`orders`.`created_at`, 3, {orm_param1:String})");
    expect(toDateTime64Built.params).toEqual({
      orm_param1: "UTC",
    });
    expect(() => fn.toDateTime64(string(), 10)).toThrow(
      "toDateTime64 scale must be an integer between 0 and 9, got 10",
    );

    const toStartOfMonthBuilt = compileExpression(
      fn.toStartOfMonth(string().bind({ name: "created_at", tableName: "orders" })),
    );
    expect(toStartOfMonthBuilt.query).toContain("toStartOfMonth(`orders`.`created_at`)");

    const toUnixTimestampBuilt = compileExpression(
      fn.toUnixTimestamp(string().bind({ name: "created_at", tableName: "orders" }), "UTC"),
    );
    expect(toUnixTimestampBuilt.query).toContain("toUnixTimestamp(`orders`.`created_at`, {orm_param1:String})");
    expect(toUnixTimestampBuilt.params).toEqual({
      orm_param1: "UTC",
    });

    const toUnixTimestamp64MilliBuilt = compileExpression(
      fn.toUnixTimestamp64Milli(string().bind({ name: "created_at", tableName: "orders" })),
    );
    expect(toUnixTimestamp64MilliBuilt.query).toContain("toUnixTimestamp64Milli(`orders`.`created_at`)");
    const toUnixTimestamp64SecondBuilt = compileExpression(
      fn.toUnixTimestamp64Second(string().bind({ name: "created_at", tableName: "orders" })),
    );
    expect(toUnixTimestamp64SecondBuilt.query).toContain("toUnixTimestamp64Second(`orders`.`created_at`)");
    const toUnixTimestamp64MicroBuilt = compileExpression(
      fn.toUnixTimestamp64Micro(string().bind({ name: "created_at", tableName: "orders" })),
    );
    expect(toUnixTimestamp64MicroBuilt.query).toContain("toUnixTimestamp64Micro(`orders`.`created_at`)");
    const toUnixTimestamp64NanoBuilt = compileExpression(
      fn.toUnixTimestamp64Nano(string().bind({ name: "created_at", tableName: "orders" })),
    );
    expect(toUnixTimestamp64NanoBuilt.query).toContain("toUnixTimestamp64Nano(`orders`.`created_at`)");

    const fromUnixTimestampBuilt = compileExpression(
      fn.fromUnixTimestamp(int32().bind({ name: "created_at_epoch", tableName: "orders" })),
    );
    expect(fromUnixTimestampBuilt.query).toContain("fromUnixTimestamp(`orders`.`created_at_epoch`)");

    const formattedFromUnixTimestampBuilt = compileExpression(
      fn.fromUnixTimestamp(int32().bind({ name: "created_at_epoch", tableName: "orders" }), "%F %T", "UTC"),
    );
    expect(formattedFromUnixTimestampBuilt.query).toContain(
      "fromUnixTimestamp(`orders`.`created_at_epoch`, {orm_param1:String}, {orm_param2:String})",
    );
    expect(formattedFromUnixTimestampBuilt.params).toEqual({
      orm_param1: "%F %T",
      orm_param2: "UTC",
    });

    const fromUnixTimestamp64MilliBuilt = compileExpression(
      fn.fromUnixTimestamp64Milli(int64().bind({ name: "created_at_ms", tableName: "orders" }), "UTC"),
    );
    expect(fromUnixTimestamp64MilliBuilt.query).toContain(
      "fromUnixTimestamp64Milli(`orders`.`created_at_ms`, {orm_param1:String})",
    );
    const fromUnixTimestamp64SecondBuilt = compileExpression(
      fn.fromUnixTimestamp64Second(int64().bind({ name: "created_at_s", tableName: "orders" }), "UTC"),
    );
    expect(fromUnixTimestamp64SecondBuilt.query).toContain(
      "fromUnixTimestamp64Second(`orders`.`created_at_s`, {orm_param1:String})",
    );
    const fromUnixTimestamp64MicroBuilt = compileExpression(
      fn.fromUnixTimestamp64Micro(int64().bind({ name: "created_at_us", tableName: "orders" }), "UTC"),
    );
    expect(fromUnixTimestamp64MicroBuilt.query).toContain(
      "fromUnixTimestamp64Micro(`orders`.`created_at_us`, {orm_param1:String})",
    );
    const fromUnixTimestamp64NanoBuilt = compileExpression(
      fn.fromUnixTimestamp64Nano(int64().bind({ name: "created_at_ns", tableName: "orders" }), "UTC"),
    );
    expect(fromUnixTimestamp64NanoBuilt.query).toContain(
      "fromUnixTimestamp64Nano(`orders`.`created_at_ns`, {orm_param1:String})",
    );

    const withParamsBuilt = compileExpression(
      fn.withParams("quantileExact", [0.95], float64().bind({ name: "price", tableName: "orders" })),
    );
    expect(withParamsBuilt.query).toContain("quantileExact({orm_param1:Float64})(`orders`.`price`)");
    expect(withParamsBuilt.params).toEqual({
      orm_param1: 0.95,
    });

    const countBuilt = compileExpression(fn.count());
    expect(countBuilt.query).toContain("toFloat64(count())");
    expect(fn.count().decoder(10)).toBe(10);
    expect(fn.count().decoder("10")).toBe(10);
    expect(fn.count().decoder(1n)).toBe(1);
    expect(() => fn.count().decoder(true)).toThrow("Failed to decode count() result");
    expect(() => fn.count().decoder(-1)).toThrow("Failed to decode count() result");

    const countSafeBuilt = compileExpression(fn.count().toSafe());
    expect(countSafeBuilt.query).toContain("toString(count())");
    expect(fn.count().toSafe().decoder("10")).toBe("10");
    expect(fn.count().toSafe().decoder(1n)).toBe("1");

    const countMixedBuilt = compileExpression(fn.count().toMixed());
    expect(countMixedBuilt.query).toContain("toUInt64(count())");
    expect(fn.count().toMixed().decoder("10")).toBe("10");
    expect(fn.count().toMixed().decoder(10)).toBe(10);

    const countUnsafeBuilt = compileExpression(fn.count().toMixed().toUnsafe());
    expect(countUnsafeBuilt.query).toContain("toFloat64(count())");

    const countIfBuilt = compileExpression(fn.countIf(fn.not(int32().bind({ name: "id", tableName: "orders" }))));
    expect(countIfBuilt.query).toContain("toFloat64(countIf(not(`orders`.`id`)))");
    expect(fn.countIf(fn.not(int32())).decoder(9)).toBe(9);
    const countIfSafeBuilt = compileExpression(
      fn.countIf(fn.not(int32().bind({ name: "id", tableName: "orders" }))).toSafe(),
    );
    expect(countIfSafeBuilt.query).toContain("toString(countIf(not(`orders`.`id`)))");
    expect(fn.countIf(fn.not(int32())).toSafe().decoder("9")).toBe("9");

    const countWithArg = compileExpression(fn.count(string().bind({ name: "name", tableName: "orders" })));
    expect(countWithArg.query).toContain("toFloat64(count(`orders`.`name`))");

    const minBuilt = compileExpression(fn.min(int32().bind({ name: "id", tableName: "orders" })));
    expect(minBuilt.query).toContain("min(`orders`.`id`)");

    const maxBuilt = compileExpression(fn.max(string().bind({ name: "name", tableName: "orders" })));
    expect(maxBuilt.query).toContain("max(`orders`.`name`)");
  });

  it("compiles typed JSON, array and tuple helpers", function testCompileStructuredHelpers() {
    const jsonBuilt = compileExpression(
      fn.jsonExtract(sql.raw("payload"), arrayColumn(string()), "account", "regulatory"),
    );
    expect(jsonBuilt.query).toContain(
      "JSONExtract(payload, {orm_param1:String}, {orm_param2:String}, {orm_param3:String})",
    );
    expect(jsonBuilt.params).toEqual({
      orm_param1: "account",
      orm_param2: "regulatory",
      orm_param3: "Array(String)",
    });

    const jsonPathBuilt = compileExpression(fn.jsonExtract(sql.raw("payload"), nullable(string()), "orders", 2, 3n));
    expect(jsonPathBuilt.query).toContain(
      "JSONExtract(payload, {orm_param1:String}, {orm_param2:Int64}, {orm_param3:Int64}, {orm_param4:String})",
    );
    expect(jsonPathBuilt.params).toEqual({
      orm_param1: "orders",
      orm_param2: 2,
      orm_param3: 3n,
      orm_param4: "Nullable(String)",
    });

    const targetOrderBuilt = compileExpression(fn.arrayJoin(fn.arrayZip([10001, 10002], [9001, 9002])));
    expect(targetOrderBuilt.query).toContain(
      "arrayJoin(arrayZip({orm_param1:Array(Int64)}, {orm_param2:Array(Int64)}))",
    );
    expect(targetOrderBuilt.params).toEqual({
      orm_param1: [10001, 10002],
      orm_param2: [9001, 9002],
    });

    const tupleElementBuilt = compileExpression(fn.tupleElement(fn.tuple("ticket", 9001), 1));
    expect(tupleElementBuilt.query).toContain(
      "tupleElement(tuple({orm_param1:String}, {orm_param2:Int64}), {orm_param3:Int64})",
    );

    const tupleElementByNameBuilt = compileExpression(
      fn.tupleElement<string>(sql.raw("target_order"), "ticket", "missing"),
    );
    expect(tupleElementByNameBuilt.query).toContain(
      "tupleElement(target_order, {orm_param1:String}, {orm_param2:String})",
    );
    expect(tupleElementByNameBuilt.params).toEqual({
      orm_param1: "ticket",
      orm_param2: "missing",
    });

    const arrayBuilt = compileExpression(fn.array("vip", "pro"));
    expect(arrayBuilt.query).toContain("array({orm_param1:String}, {orm_param2:String})");

    const emptyArrayConcatBuilt = compileExpression(fn.arrayConcat());
    expect(emptyArrayConcatBuilt.query).toContain("arrayConcat()");

    const arrayConcatBuilt = compileExpression(fn.arrayConcat(["vip"], ["pro"]));
    expect(arrayConcatBuilt.query).toContain("arrayConcat({orm_param1:Array(String)}, {orm_param2:Array(String)})");

    const arrayElementBuilt = compileExpression(fn.arrayElement(["vip", "pro"], 2));
    expect(arrayElementBuilt.query).toContain("arrayElement({orm_param1:Array(String)}, {orm_param2:Int64})");

    const arrayElementOrNullBuilt = compileExpression(fn.arrayElementOrNull(["vip"], 2));
    expect(arrayElementOrNullBuilt.query).toContain(
      "arrayElementOrNull({orm_param1:Array(String)}, {orm_param2:Int64})",
    );

    const arraySliceBuilt = compileExpression(fn.arraySlice(["vip", "pro", "raw"], 2, 1));
    expect(arraySliceBuilt.query).toContain(
      "arraySlice({orm_param1:Array(String)}, {orm_param2:Int64}, {orm_param3:Int64})",
    );

    const openEndedArraySliceBuilt = compileExpression(fn.arraySlice(["vip", "pro", "raw"], 2));
    expect(openEndedArraySliceBuilt.query).toContain("arraySlice({orm_param1:Array(String)}, {orm_param2:Int64})");

    const arrayFlattenBuilt = compileExpression(fn.arrayFlatten([["vip"], ["pro"]]));
    expect(arrayFlattenBuilt.query).toContain("arrayFlatten({orm_param1:Array(Array(String))})");

    const arrayIntersectBuilt = compileExpression(fn.arrayIntersect(["vip", "pro"], ["pro"]));
    expect(arrayIntersectBuilt.query).toContain(
      "arrayIntersect({orm_param1:Array(String)}, {orm_param2:Array(String)})",
    );

    const indexOfBuilt = compileExpression(fn.indexOf(["vip", "pro"], "pro"));
    expect(indexOfBuilt.query).toContain("indexOf({orm_param1:Array(String)}, {orm_param2:String})");

    const lengthBuilt = compileExpression(fn.length(["vip", "pro"]));
    expect(lengthBuilt.query).toContain("length({orm_param1:Array(String)})");

    const notEmptyBuilt = compileExpression(fn.notEmpty(["vip"]));
    expect(notEmptyBuilt.query).toContain("notEmpty({orm_param1:Array(String)})");
  });

  it("compiles official array helper additions with ClickHouse function names", function testCompileOfficialArrayHelpers() {
    const lambda = sql.raw("x -> x > 1");
    const binaryLambda = sql.raw("(x, y) -> x = y");
    const helpers = {
      arrayAUCPR: fn.arrayAUCPR([0.1, 0.9], [0, 1]),
      arrayAll: fn.arrayAll(lambda, [1, 2]),
      arrayAutocorrelation: fn.arrayAutocorrelation([1, 2, 3], 2),
      arrayAvg: fn.arrayAvg(lambda, [1, 2]),
      arrayCompact: fn.arrayCompact(["vip", "vip"]),
      arrayCount: fn.arrayCount(lambda, [1, 2]),
      arrayCumSum: fn.arrayCumSum([1, 2]),
      arrayCumSumNonNegative: fn.arrayCumSumNonNegative([1, -2, 3]),
      arrayDifference: fn.arrayDifference([1, 3, 6]),
      arrayDistinct: fn.arrayDistinct(["vip", "vip"]),
      arrayDotProduct: fn.arrayDotProduct([1, 2], [3, 4]),
      arrayEnumerate: fn.arrayEnumerate(["vip"]),
      arrayEnumerateDense: fn.arrayEnumerateDense(["vip"]),
      arrayEnumerateDenseRanked: fn.arrayEnumerateDenseRanked(1, [[1, 2]], 2),
      arrayEnumerateUniq: fn.arrayEnumerateUniq(["vip"], ["pro"]),
      arrayEnumerateUniqRanked: fn.arrayEnumerateUniqRanked(1, [[1, 2]], 2),
      arrayExcept: fn.arrayExcept(["vip"], ["pro"]),
      arrayExists: fn.arrayExists(binaryLambda, [1, 2], [1, 3]),
      arrayFill: fn.arrayFill(lambda, [1, 0, 2]),
      arrayFilter: fn.arrayFilter(lambda, [1, 2]),
      arrayFirst: fn.arrayFirst(lambda, [1, 2]),
      arrayFirstIndex: fn.arrayFirstIndex(lambda, [1, 2]),
      arrayFirstOrNull: fn.arrayFirstOrNull(lambda, [1, 2]),
      arrayFold: fn.arrayFold(sql.raw("(acc, x) -> acc + x"), [1, 2], 0),
      arrayJaccardIndex: fn.arrayJaccardIndex(["vip"], ["vip", "pro"]),
      arrayLast: fn.arrayLast(lambda, [1, 2]),
      arrayLastIndex: fn.arrayLastIndex(lambda, [1, 2]),
      arrayLastOrNull: fn.arrayLastOrNull(lambda, [1, 2]),
      arrayLevenshteinDistance: fn.arrayLevenshteinDistance(["A"], ["B"]),
      arrayLevenshteinDistanceWeighted: fn.arrayLevenshteinDistanceWeighted(["A"], ["B"], [1], [1]),
      arrayMap: fn.arrayMap(lambda, [1, 2]),
      arrayMax: fn.arrayMax(lambda, [1, 2]),
      arrayMin: fn.arrayMin(lambda, [1, 2]),
      arrayNormalizedGini: fn.arrayNormalizedGini([0.9, 0.3], [1, 0]),
      arrayPartialReverseSort: fn.arrayPartialReverseSort(2, [5, 1, 3]),
      arrayPartialShuffle: fn.arrayPartialShuffle([1, 2, 3], 2, 42),
      arrayPartialSort: fn.arrayPartialSort(2, [5, 1, 3]),
      arrayPopBack: fn.arrayPopBack(["vip", "pro"]),
      arrayPopFront: fn.arrayPopFront(["vip", "pro"]),
      arrayProduct: fn.arrayProduct([1, 2, 3]),
      arrayPushBack: fn.arrayPushBack(["vip"], "pro"),
      arrayPushFront: fn.arrayPushFront(["pro"], "vip"),
      arrayROCAUC: fn.arrayROCAUC([0.1, 0.9], [0, 1], false, [0, 0, 2]),
      arrayRandomSample: fn.arrayRandomSample(["vip", "pro"], 1),
      arrayReduce: fn.arrayReduce("sum", [1, 2]),
      arrayReduceInRanges: fn.arrayReduceInRanges("sum", [[1, 2]], [1, 2]),
      arrayRemove: fn.arrayRemove(["vip", "pro"], "pro"),
      arrayResize: fn.arrayResize(["vip"], 2, "pro"),
      arrayReverse: fn.arrayReverse(["vip", "pro"]),
      arrayReverseFill: fn.arrayReverseFill(lambda, [1, 0, 2]),
      arrayReverseSort: fn.arrayReverseSort([2, 1]),
      arrayReverseSplit: fn.arrayReverseSplit(lambda, [1, 0, 2]),
      arrayRotateLeft: fn.arrayRotateLeft([1, 2, 3], 1),
      arrayRotateRight: fn.arrayRotateRight([1, 2, 3], 1),
      arrayShiftLeft: fn.arrayShiftLeft([1, 2, 3], 1, 0),
      arrayShiftRight: fn.arrayShiftRight([1, 2, 3], 1, 0),
      arrayShingles: fn.arrayShingles(["a", "b", "c"], 2),
      arrayShuffle: fn.arrayShuffle(["vip", "pro"], 42),
      arraySimilarity: fn.arraySimilarity(["A"], ["B"], [1], [1]),
      arraySort: fn.arraySort(lambda, [2, 1]),
      arraySplit: fn.arraySplit(lambda, [1, 0, 2]),
      arraySum: fn.arraySum(lambda, [1, 2]),
      arraySymmetricDifference: fn.arraySymmetricDifference(["vip"], ["pro"]),
      arrayTranspose: fn.arrayTranspose([
        [1, 2],
        [3, 4],
      ]),
      arrayUnion: fn.arrayUnion(["vip"], ["pro"]),
      arrayUniq: fn.arrayUniq(["vip", "vip"]),
      arrayWithConstant: fn.arrayWithConstant(2, "vip"),
      arrayZipUnaligned: fn.arrayZipUnaligned([1], ["vip"]),
      countEqual: fn.countEqual(["vip", "vip"], "vip"),
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
      emptyArrayToSingle: fn.emptyArrayToSingle(fn.emptyArrayString()),
      emptyArrayUInt16: fn.emptyArrayUInt16(),
      emptyArrayUInt32: fn.emptyArrayUInt32(),
      emptyArrayUInt64: fn.emptyArrayUInt64(),
      emptyArrayUInt8: fn.emptyArrayUInt8(),
      has: fn.has(["vip"], "vip"),
      hasAll: fn.hasAll(["vip", "pro"], ["vip"]),
      hasAny: fn.hasAny(["vip"], ["pro"]),
      hasSubstr: fn.hasSubstr(["vip", "pro"], ["vip"]),
      indexOfAssumeSorted: fn.indexOfAssumeSorted(["pro", "vip"], "vip"),
      kql_array_sort_asc: fn.kql_array_sort_asc(["pro", "vip"]),
      kql_array_sort_desc: fn.kql_array_sort_desc(["pro", "vip"]),
      range: fn.range(1, 5, 2),
      replicate: fn.replicate("vip", [1, 2]),
      reverse: fn.reverse(["vip", "pro"]),
    };

    for (const [name, expression] of Object.entries(helpers)) {
      expect(compileExpression(expression).query).toContain(`${name}(`);
    }
  });

  it("compiles higher-order array helpers in ClickHouse argument order", function testHigherOrderArrayHelperOrder() {
    const rangeMatcher = compileExpression(
      fn.arrayExists(sql.raw("(start_ts, end_ts) -> ts >= start_ts AND ts < end_ts"), [10, 20], [15, 25]),
    );
    expect(rangeMatcher.query).toContain(
      "arrayExists((start_ts, end_ts) -> ts >= start_ts AND ts < end_ts, {orm_param1:Array(Int64)}, {orm_param2:Array(Int64)})",
    );
    expect(rangeMatcher.params).toEqual({
      orm_param1: [10, 20],
      orm_param2: [15, 25],
    });

    const lambdaAvg = compileExpression(fn.arrayAvg(sql.raw("(x, y) -> x + y"), [1, 2], [3, 4]));
    expect(lambdaAvg.query).toContain(
      "arrayAvg((x, y) -> x + y, {orm_param1:Array(Int64)}, {orm_param2:Array(Int64)})",
    );

    const plainAvg = compileExpression(fn.arrayAvg([1, 2, 3]));
    expect(plainAvg.query).toContain("arrayAvg({orm_param1:Array(Int64)})");

    const folded = compileExpression(fn.arrayFold(sql.raw("(acc, x, y) -> acc + x * y"), [1, 2], [3, 4], 0));
    expect(folded.query).toContain(
      "arrayFold((acc, x, y) -> acc + x * y, {orm_param1:Array(Int64)}, {orm_param2:Array(Int64)}, {orm_param3:Int64})",
    );

    const reduced = compileExpression(fn.arrayReduce("sum", [1, 2]));
    expect(reduced.query).toContain("arrayReduce({orm_param1:String}, {orm_param2:Array(Int64)})");

    const ranged = compileExpression(fn.range(1, 5, 2));
    expect(ranged.query).toContain("range({orm_param1:Int64}, {orm_param2:Int64}, {orm_param3:Int64})");

    const rocAuc = compileExpression(fn.arrayROCAUC([0.1, 0.9], [0, 1], false, [0, 0, 2]));
    expect(rocAuc.query).toContain(
      "arrayROCAUC({orm_param1:Array(Float64)}, {orm_param2:Array(Int64)}, {orm_param3:Bool}, {orm_param4:Array(Int64)})",
    );
  });

  it("uses conservative aggregate decoders and covers not/coalesce/tuple/arrayZip", function testDecoders() {
    expect(fn.toString(int32()).decoder(12)).toBe("12");
    expect(fn.toDate(string()).decoder("2026-04-21")).toEqual(new Date("2026-04-21"));
    const existingDate = new Date("2026-04-21T12:34:56.000Z");
    expect(fn.toDate(string()).decoder(existingDate)).toBe(existingDate);
    expect(fn.toDateTime(string()).decoder("2026-04-21T00:00:00.000Z")).toEqual(new Date("2026-04-21T00:00:00.000Z"));
    expect(fn.toDate32(string()).decoder("2026-04-21")).toEqual(new Date("2026-04-21"));
    expect(fn.toDateTime32(string()).decoder("2026-04-21T00:00:00.000Z")).toEqual(new Date("2026-04-21T00:00:00.000Z"));
    expect(fn.toDateTime64(string(), 3).decoder("2026-04-21T00:00:00.123Z")).toEqual(
      new Date("2026-04-21T00:00:00.123Z"),
    );
    expect(fn.fromUnixTimestamp(int32()).decoder("2026-04-21T00:00:00.000Z")).toEqual(
      new Date("2026-04-21T00:00:00.000Z"),
    );
    expect(fn.fromUnixTimestamp(int32(), "%F").decoder(20260421)).toBe("20260421");
    expect(fn.fromUnixTimestamp64Milli(int64()).decoder("2026-04-21T00:00:00.123Z")).toEqual(
      new Date("2026-04-21T00:00:00.123Z"),
    );
    expect(fn.toUnixTimestamp(string()).decoder("1739489491")).toBe(1739489491);
    expect(fn.toUnixTimestamp64Milli(string()).decoder(1739489491011n)).toBe("1739489491011");
    expect(fn.toStartOfMonth(string()).decoder("2026-04-01")).toEqual(new Date("2026-04-01"));
    expect(fn.avg(int32()).decoder(4.5)).toBe(4.5);
    expect(fn.avg(int32()).decoder("8")).toBe(8);
    expect(fn.avg(int32()).decoder(7n)).toBe(7);
    expect(fn.sum(float64()).decoder("12.5")).toBe(12.5);
    expect(fn.sum(int32()).decoder(12)).toBe("12");
    expect(fn.sum(int32()).decoder(7n)).toBe("7");
    expect(fn.sumIf(float64(), fn.not(fn.count())).decoder(7n)).toBe(7);
    expect(fn.countIf(fn.not(int32())).decoder(9)).toBe(9);
    expect(fn.avg(int32()).decoder("4.5")).toBe(4.5);
    expect(fn.min(int32()).decoder("8")).toBe(8);
    expect(fn.max(string()).decoder(1n)).toBe("1");
    const aggregateDate = new Date("2026-04-21T12:34:56.000Z");
    expect(fn.min(dateTime()).decoder(aggregateDate)).toBe(aggregateDate);
    expect(fn.max(dateTime()).decoder("2026-04-21T00:00:00.000Z")).toEqual(new Date("2026-04-21T00:00:00.000Z"));
    expect(fn.uniqExact(int32()).decoder(5)).toBe(5);
    expect(() => fn.count().decoder(true)).toThrow("Failed to decode count() result");
    expect(fn.count().decoder(1n)).toBe(1);
    expect(fn.toString(int32()).decoder(1n)).toBe("1");
    expect(fn.toString(int32()).decoder(false)).toBe("false");
    expect(fn.avg(int32()).decoder(9n)).toBe(9);
    expect(fn.avg(int32()).decoder("9")).toBe(9);
    expect(() => fn.avg(int32()).decoder({})).toThrow("Cannot convert value to number");
    expect(() => fn.toString(int32()).decoder({})).toThrow("Cannot convert value to string");

    const coalesced = fn.coalesce(string(), int32());
    expect(coalesced.decoder(10)).toBe("10");
    expect(fn.coalesce().decoder({ raw: true })).toEqual({ raw: true });

    const price = float64().bind({ name: "price", tableName: "orders" });
    const coalescedFloat = fn.coalesce(price, 0);
    expect(coalescedFloat.sqlType).toBe("Float64");
    expect(compileExpression(coalescedFloat).query).toContain("coalesce(`orders`.`price`, {orm_param1:Float64})");
    expect(compileExpression(coalescedFloat).params).toEqual({
      orm_param1: 0,
    });

    const coalescedFloatSum = fn.coalesce(fn.sum(price), 0);
    expect(coalescedFloatSum.sqlType).toBe("Float64");
    expect(compileExpression(coalescedFloatSum).query).toContain(
      "coalesce(sum(`orders`.`price`), {orm_param1:Float64})",
    );
    expect(compileExpression(fn.coalesce(price, sql.raw("0"))).query).toContain("coalesce(`orders`.`price`, 0)");
    expect(compileExpression(fn.coalesce(price, fn.toString(0))).query).toContain(
      "coalesce(`orders`.`price`, toString({orm_param1:Int64}))",
    );
    expect(compileExpression(fn.coalesce(fn.call("unknown_type", price), 0)).query).toContain(
      "coalesce(unknown_type(`orders`.`price`), {orm_param1:Int64})",
    );

    const float32Price = float32().bind({ name: "price_32", tableName: "orders" });
    expect(compileExpression(fn.coalesce(float32Price, 0)).query).toContain(
      "coalesce(`orders`.`price_32`, {orm_param1:Float32})",
    );

    const bfloatPrice = bfloat16().bind({ name: "price_bfloat", tableName: "orders" });
    expect(compileExpression(fn.coalesce(bfloatPrice, 0)).query).toContain(
      "coalesce(`orders`.`price_bfloat`, {orm_param1:BFloat16})",
    );

    const volume = uint64().bind({ name: "volume", tableName: "orders" });
    expect(compileExpression(fn.coalesce(volume, 0)).query).toContain(
      "coalesce(`orders`.`volume`, {orm_param1:UInt64})",
    );

    const amount = decimal({ precision: 18, scale: 2 }).bind({ name: "amount", tableName: "orders" });
    expect(compileExpression(fn.coalesce(amount, "0.00")).query).toContain(
      "coalesce(`orders`.`amount`, {orm_param1:Decimal(18, 2)})",
    );
    expect(compileExpression(fn.coalesce(amount, 0.5)).query).toContain(
      "coalesce(`orders`.`amount`, {orm_param1:Decimal(18, 2)})",
    );

    const score = int32().bind({ name: "score", tableName: "orders" });
    expect(compileExpression(fn.coalesce(score, 1.5)).query).toContain(
      "coalesce(`orders`.`score`, {orm_param1:Float64})",
    );

    const name = string().bind({ name: "name", tableName: "orders" });
    expect(compileExpression(fn.coalesce(name, "missing")).query).toContain(
      "coalesce(`orders`.`name`, {orm_param1:String})",
    );

    const ticket = int64().bind({ name: "ticket", tableName: "orders" });
    expect(compileExpression(fn.coalesce(ticket, 1.5)).query).toContain(
      "coalesce(`orders`.`ticket`, {orm_param1:Float64})",
    );

    expect(fn.not(int32()).decoder(true)).toBe(true);
    expect(fn.not(int32()).decoder("true")).toBe(true);
    expect(fn.not(int32()).decoder(0)).toBe(false);
    expect(() => fn.not(int32()).decoder({})).toThrow("Cannot convert value to boolean");

    expect(fn.tuple().decoder([1, 2])).toEqual([1, 2]);
    expect(() => fn.tuple().decoder("bad")).toThrow("Cannot convert value to tuple array");

    expect(fn.arrayZip().decoder([[1, 2]])).toEqual([[1, 2]]);
    expect(() => fn.arrayZip().decoder("bad")).toThrow("Cannot convert value to arrayZip array");

    expect(fn.jsonExtract(sql.raw("payload"), arrayColumn(string())).decoder(["vip", 1])).toEqual(["vip", "1"]);
    expect(fn.array("vip").decoder(["vip"])).toEqual(["vip"]);
    expect(() => fn.array("vip").decoder("bad")).toThrow("Cannot convert value to array array");
    expect(fn.arrayConcat("vip").decoder(["vip"])).toEqual(["vip"]);
    expect(() => fn.arrayConcat("vip").decoder("bad")).toThrow("Cannot convert value to arrayConcat array");
    expect(fn.arraySlice("vip", 1).decoder(["vip"])).toEqual(["vip"]);
    expect(() => fn.arraySlice("vip", 1).decoder("bad")).toThrow("Cannot convert value to arraySlice array");
    expect(fn.arrayFlatten("vip").decoder(["vip"])).toEqual(["vip"]);
    expect(() => fn.arrayFlatten("vip").decoder("bad")).toThrow("Cannot convert value to arrayFlatten array");
    expect(fn.arrayIntersect("vip").decoder(["vip"])).toEqual(["vip"]);
    expect(() => fn.arrayIntersect("vip").decoder("bad")).toThrow("Cannot convert value to arrayIntersect array");
    expect(fn.arrayJoin<string>(["vip"]).decoder("vip")).toBe("vip");
    expect(fn.tupleElement<string>(fn.tuple("ticket"), 1).decoder("ticket")).toBe("ticket");
    expect(fn.jsonExtract(sql.raw("payload"), nullable(string())).decoder(null)).toBeNull();
    expect(fn.jsonExtract(sql.raw("payload"), nullable(string())).decoder("vip")).toBe("vip");
    expect(fn.indexOf(["vip"], "vip").decoder(1)).toBe("1");
    expect(fn.length(["vip"]).decoder(1n)).toBe("1");
    expect(fn.notEmpty(["vip"]).decoder(1)).toBe(true);
    expect(fn.arrayExists(sql.raw("x -> x > 1"), [1]).decoder(1)).toBe(true);
    expect(fn.arrayAll(sql.raw("x -> x > 1"), [1]).decoder(0)).toBe(false);
    expect(fn.has(["vip"], "vip").decoder(1)).toBe(true);
    expect(fn.hasAll(["vip"], ["vip"]).decoder("0")).toBe(false);
    expect(fn.hasAny(["vip"], ["pro"]).decoder(true)).toBe(true);
    expect(fn.arrayFirstIndex(sql.raw("x -> x > 1"), [1]).decoder("2")).toBe(2);
    expect(fn.arrayLastIndex(sql.raw("x -> x > 1"), [1]).decoder("3")).toBe(3);
    expect(fn.indexOfAssumeSorted(["vip"], "vip").decoder(1)).toBe("1");
    expect(fn.arrayCount(sql.raw("x -> x > 1"), [1]).decoder(2n)).toBe("2");
    expect(fn.arrayUniq(["vip", "vip"]).decoder(1)).toBe("1");
    expect(fn.countEqual(["vip"], "vip").decoder(2)).toBe("2");
    expect(fn.arrayAvg([1, 2]).decoder("1.5")).toBe(1.5);
    expect(fn.arrayAUCPR([0.1, 0.9], [0, 1]).decoder(1)).toBe(1);
    expect(fn.arrayJaccardIndex(["vip"], ["vip"]).decoder("0.5")).toBe(0.5);
    expect(fn.arrayLevenshteinDistance(["A"], ["B"]).decoder(1n)).toBe(1);
    expect(fn.arrayLevenshteinDistanceWeighted(["A"], ["B"], [1], [1]).decoder("2")).toBe(2);
    expect(fn.arrayROCAUC([0.1, 0.9], [0, 1]).decoder("1")).toBe(1);
    expect(fn.arraySimilarity(["A"], ["B"], [1], [1]).decoder(0.25)).toBe(0.25);
    expect(fn.arrayDotProduct<number>([1, 2], [3, 4]).decoder("11")).toBe("11");
    expect(fn.arrayMax<number>([1, 2]).decoder("2")).toBe("2");
    expect(fn.arrayMin<number>([1, 2]).decoder("1")).toBe("1");
    expect(fn.arraySum<number>([1, 2]).decoder("3")).toBe("3");
    expect(fn.arrayProduct<number>([1, 2]).decoder("2")).toBe("2");
    expect(fn.arrayReduce<string>("sum", [1, 2]).decoder("3")).toBe("3");
    expect(fn.arrayFold<number>(sql.raw("(acc, x) -> acc + x"), [1, 2], 0).decoder("3")).toBe("3");
    expect(fn.arrayFilter(sql.raw("x -> x > 1"), [1]).decoder([2])).toEqual([2]);
    expect(fn.arrayAutocorrelation([1, 2, 3]).decoder([1, 0.5])).toEqual([1, 0.5]);
    expect(fn.arrayCumSumNonNegative([1, -2, 3]).decoder([1, 0, 3])).toEqual([1, 0, 3]);
    expect(fn.arrayEnumerate(["vip"]).decoder([1])).toEqual([1]);
    expect(fn.arrayEnumerateDense(["vip"]).decoder([1])).toEqual([1]);
    expect(fn.arrayEnumerateDenseRanked(1, [["vip"]], 2).decoder([[1]])).toEqual([[1]]);
    expect(fn.arrayEnumerateUniq(["vip"]).decoder([1])).toEqual([1]);
    expect(fn.arrayEnumerateUniqRanked(1, [["vip"]], 2).decoder([[1]])).toEqual([[1]]);
    expect(fn.arrayFill(sql.raw("x -> x > 1"), [1]).decoder([1])).toEqual([1]);
    expect(fn.arrayReverseFill(sql.raw("x -> x > 1"), [1]).decoder([1])).toEqual([1]);
    expect(() => fn.arrayMap(sql.raw("x -> x + 1"), [1]).decoder("bad")).toThrow(
      "Cannot convert value to arrayMap array",
    );
    expect(fn.arrayPartialSort<number>(2, [3, 1]).decoder([1, 3])).toEqual([1, 3]);
    expect(fn.arrayPartialReverseSort<number>(2, [3, 1]).decoder([3, 1])).toEqual([3, 1]);
    expect(fn.arrayPartialShuffle<number>([1, 2], 1).decoder([2, 1])).toEqual([2, 1]);
    expect(fn.arrayRandomSample<number>([1, 2], 1).decoder([2])).toEqual([2]);
    expect(fn.arrayReverseSplit<number>(sql.raw("x -> x = 0"), [1]).decoder([[1]])).toEqual([[1]]);
    expect(fn.arraySplit<number>(sql.raw("x -> x = 0"), [1]).decoder([[1]])).toEqual([[1]]);
    expect(fn.arrayTranspose<number>([[1, 2]]).decoder([[1], [2]])).toEqual([[1], [2]]);
    expect(fn.arrayZipUnaligned([1], ["vip"]).decoder([[1, "vip"]])).toEqual([[1, "vip"]]);
    expect(fn.empty([]).decoder(0)).toBe(false);
    expect(fn.emptyArrayDate().decoder([])).toEqual([]);
    expect(fn.emptyArrayDateTime().decoder([])).toEqual([]);
    expect(fn.emptyArrayFloat32().decoder([])).toEqual([]);
    expect(fn.emptyArrayFloat64().decoder([])).toEqual([]);
    expect(fn.emptyArrayInt8().decoder([])).toEqual([]);
    expect(fn.emptyArrayInt16().decoder([])).toEqual([]);
    expect(fn.emptyArrayInt32().decoder([])).toEqual([]);
    expect(fn.emptyArrayString().decoder([])).toEqual([]);
    expect(fn.emptyArrayInt64().decoder(["1"])).toEqual(["1"]);
    expect(fn.emptyArrayUInt8().decoder([])).toEqual([]);
    expect(fn.emptyArrayUInt16().decoder([])).toEqual([]);
    expect(fn.emptyArrayUInt32().decoder([])).toEqual([]);
    expect(fn.emptyArrayUInt64().decoder(["1"])).toEqual(["1"]);
    expect(fn.hasSubstr(["vip"], ["vip"]).decoder("1")).toBe(true);
    expect(fn.kql_array_sort_asc(["vip"]).decoder([["vip"]])).toEqual([["vip"]]);
    expect(() => fn.emptyArrayFloat64().decoder("bad")).toThrow("Cannot convert value to emptyArrayFloat64 array");
  });

  it("compiles toDecimal* casts and Decimal-aware aggregates", function testDecimalAwareAggregates() {
    const amount = decimal({ precision: 18, scale: 5 }).bind({ name: "amount", tableName: "ledger" });

    const cast128 = fn.toDecimal128(amount, 5);
    expect(compileSql(sql`${cast128.compile(createContext())}`).query).toContain("toDecimal128(`ledger`.`amount`, 5)");
    expect(cast128.sqlType).toBe("Decimal(38, 5)");
    expect(cast128.decoder("1.23")).toBe("1.23");
    const cast64 = fn.toDecimal64(amount, 2);
    expect(compileSql(sql`${cast64.compile(createContext())}`).query).toContain("toDecimal64(`ledger`.`amount`, 2)");
    expect(fn.sum(cast64).sqlType).toBe("Decimal(38, 2)");
    const cast256 = fn.toDecimal256(amount, 8);
    expect(compileSql(sql`${cast256.compile(createContext())}`).query).toContain("toDecimal256(`ledger`.`amount`, 8)");
    expect(cast256.sqlType).toBe("Decimal(76, 8)");
    expect(() => fn.toDecimal128(amount, "5" as never)).toThrow(
      /toDecimal128 scale must be an integer between 0 and 38 \(the toDecimal128 fixed width\)/,
    );
    expect(() => fn.toDecimal32(amount, 10)).toThrow(
      /toDecimal32 scale must be an integer between 0 and 9 \(the toDecimal32 fixed width\)/,
    );

    const summed = fn.sum(amount);
    const summedCompiled = compileSql(sql`${summed.compile(createContext())}`);
    expect(summedCompiled.query).toBe("CAST(sum(`ledger`.`amount`) AS Decimal(38, 5))");
    expect(summed.sqlType).toBe("Decimal(38, 5)");
    expect(summed.decoder("9.99")).toBe("9.99");

    // avg(Decimal) intentionally does NOT auto-cast — aligned with ClickHouse's
    // native avg(Decimal) → Float64 behavior (sum-of-divides path).
    const averaged = fn.avg(amount);
    expect(compileSql(sql`${averaged.compile(createContext())}`).query).toBe("avg(`ledger`.`amount`)");
    expect(averaged.sqlType).toBe("Float64");
    expect(averaged.decoder("4.5")).toBe(4.5);

    const summedIf = fn.sumIf(amount, sql`1`);
    expect(compileSql(sql`${summedIf.compile(createContext())}`).query).toBe(
      "CAST(sumIf(`ledger`.`amount`, 1) AS Decimal(38, 5))",
    );

    const minimal = fn.min(amount);
    expect(compileSql(sql`${minimal.compile(createContext())}`).query).toBe(
      "CAST(min(`ledger`.`amount`) AS Decimal(18, 5))",
    );

    expect(fn.sum(int32()).sqlType).toBeUndefined();
    expect(fn.avg(int32()).decoder("4.5")).toBe(4.5);

    const floatAmount = float64().bind({ name: "amount", tableName: "ledger" });
    expect(fn.sum(floatAmount).sqlType).toBe("Float64");
    expect(fn.sumIf(floatAmount, sql`1`).sqlType).toBe("Float64");

    const nullableFloat = nullable(float64()).bind({ name: "score", tableName: "ledger" });
    const lowCardFloat = lowCardinality(float64()).bind({ name: "fee_ratio", tableName: "ledger" });
    expect(fn.sum(nullableFloat).sqlType).toBe("Float64");
    expect(fn.sum(lowCardFloat).sqlType).toBe("Float64");
    expect(fn.sum(nullableFloat).decoder("4.5")).toBe(4.5);
    expect(fn.sum(lowCardFloat).decoder("2.25")).toBe(2.25);

    // Wrapped Decimal columns must still trigger auto-cast.
    const nullableAmount = nullable(decimal({ precision: 18, scale: 5 })).bind({
      name: "amount",
      tableName: "ledger",
    });
    const summedNullable = fn.sum(nullableAmount);
    expect(compileSql(sql`${summedNullable.compile(createContext())}`).query).toBe(
      "CAST(sum(`ledger`.`amount`) AS Decimal(38, 5))",
    );

    // Wrappers (lowCardinality / nullable) must NOT cause avg to auto-cast either.
    const lowCardAmount = lowCardinality(decimal({ precision: 12, scale: 4 })).bind({
      name: "fee",
      tableName: "ledger",
    });
    expect(compileSql(sql`${fn.avg(lowCardAmount).compile(createContext())}`).query).toBe("avg(`ledger`.`fee`)");
    expect(fn.avg(lowCardAmount).sqlType).toBe("Float64");
  });

  it("fn.count and fn.countIf expose three chainable count modes", function testCountModes() {
    const defaultCount = fn.count();
    expect(defaultCount.sqlType).toBe("Float64");
    expect(compileExpression(defaultCount).query).toContain("toFloat64(count())");

    const safeCount = fn.count().toSafe();
    expect(safeCount.sqlType).toBe("String");
    expect(compileExpression(safeCount).query).toContain("toString(count())");

    const mixedCount = fn.count().toMixed();
    expect(mixedCount.sqlType).toBe("UInt64");
    expect(compileExpression(mixedCount).query).toContain("toUInt64(count())");

    // Mode switches are independent — switching from one mode to another must keep the inner SQL.
    const flippedToSafe = fn
      .count(int32().bind({ name: "id", tableName: "orders" }))
      .toMixed()
      .toSafe();
    expect(flippedToSafe.sqlType).toBe("String");
    expect(compileExpression(flippedToSafe).query).toContain("toString(count(`orders`.`id`))");

    const flippedToUnsafe = fn.count().toSafe().toUnsafe();
    expect(flippedToUnsafe.sqlType).toBe("Float64");
    expect(compileExpression(flippedToUnsafe).query).toContain("toFloat64(count())");

    // Decoder semantics per mode.
    expect(fn.count().decoder(0)).toBe(0);
    expect(fn.count().decoder("9007199254740991")).toBe(9007199254740991);
    expect(fn.count().decoder(42n)).toBe(42);
    expect(() => fn.count().decoder(Number.NaN)).toThrow("Failed to decode count() result");
    expect(() => fn.count().decoder(1.5)).toThrow("Failed to decode count() result");
    expect(() => fn.count().decoder(-1)).toThrow("Failed to decode count() result");
    expect(() => fn.count().decoder(true)).toThrow("Failed to decode count() result");
    expect(() => fn.count().decoder(" 7 ")).toThrow("Failed to decode count() result");

    expect(fn.count().toSafe().decoder("0")).toBe("0");
    expect(fn.count().toSafe().decoder(99n)).toBe("99");
    expect(fn.count().toSafe().decoder(7)).toBe("7");
    expect(() => fn.count().toSafe().decoder(-1n)).toThrow("Failed to decode count() result");
    expect(() => fn.count().toSafe().decoder("1.0")).toThrow("Failed to decode count() result");
    expect(() => fn.count().toSafe().decoder(Number.NaN)).toThrow("Failed to decode count() result");

    expect(fn.count().toMixed().decoder("100")).toBe("100");
    expect(fn.count().toMixed().decoder(100)).toBe(100);
    expect(fn.count().toMixed().decoder(7n)).toBe("7");
    expect(() => fn.count().toMixed().decoder(true)).toThrow("Failed to decode count() result");
    expect(() => fn.count().toMixed().decoder(-2)).toThrow("Failed to decode count() result");

    // countIf preserves the predicate body inside every wrapper.
    const condition = fn.not(int32().bind({ name: "id", tableName: "orders" }));
    expect(compileExpression(fn.countIf(condition)).query).toContain("toFloat64(countIf(not(`orders`.`id`)))");
    expect(compileExpression(fn.countIf(condition).toSafe()).query).toContain("toString(countIf(not(`orders`.`id`)))");
    expect(compileExpression(fn.countIf(condition).toMixed()).query).toContain("toUInt64(countIf(not(`orders`.`id`)))");
    expect(fn.countIf(condition).decoder(3)).toBe(3);
    expect(fn.countIf(condition).toSafe().decoder("3")).toBe("3");
    expect(fn.countIf(condition).toMixed().decoder("3")).toBe("3");

    // The same selection is reusable as a SQL operand inside other expressions (e.g. HAVING).
    const composed = fn.count();
    const composedQuery = compileExpression({
      compile: (ctx) => sql`${composed.compile(ctx)} > {orm_paramN:Int64}`,
    }).query;
    expect(composedQuery).toContain("toFloat64(count()) > {orm_paramN:Int64}");

    // .as() preserves chosen mode and decoder.
    const aliased = fn.count().toMixed().as("total");
    const aliasedBuilt = compileExpression(aliased);
    expect(aliasedBuilt.query).toContain("toUInt64(count())");
    expect(aliased.decoder("4")).toBe("4");

    // fn.uniqExact mirrors fn.count's three modes.
    const uniqArg = int32().bind({ name: "user_id", tableName: "events" });
    const defaultUniq = fn.uniqExact(uniqArg);
    expect(defaultUniq.sqlType).toBe("Float64");
    expect(compileExpression(defaultUniq).query).toContain("toFloat64(uniqExact(`events`.`user_id`))");
    expect(defaultUniq.decoder(7)).toBe(7);
    expect(defaultUniq.decoder("7")).toBe(7);
    expect(defaultUniq.decoder(7n)).toBe(7);
    expect(() => defaultUniq.decoder(-1)).toThrow("Failed to decode count() result");
    expect(() => defaultUniq.decoder(true)).toThrow("Failed to decode count() result");

    const safeUniq = fn.uniqExact(uniqArg).toSafe();
    expect(safeUniq.sqlType).toBe("String");
    expect(compileExpression(safeUniq).query).toContain("toString(uniqExact(`events`.`user_id`))");
    expect(safeUniq.decoder("99")).toBe("99");
    expect(safeUniq.decoder(99n)).toBe("99");

    const mixedUniq = fn.uniqExact(uniqArg).toMixed();
    expect(mixedUniq.sqlType).toBe("UInt64");
    expect(compileExpression(mixedUniq).query).toContain("toUInt64(uniqExact(`events`.`user_id`))");
    expect(mixedUniq.decoder("100")).toBe("100");
    expect(mixedUniq.decoder(100)).toBe(100);

    const unsafeUniq = fn.uniqExact(uniqArg).toMixed().toUnsafe();
    expect(unsafeUniq.sqlType).toBe("Float64");
    expect(compileExpression(unsafeUniq).query).toContain("toFloat64(uniqExact(`events`.`user_id`))");

    // .as() preserves chosen mode and decoder for uniqExact.
    const aliasedUniq = fn.uniqExact(uniqArg).toMixed().as("distinct_users");
    const aliasedUniqBuilt = compileExpression(aliasedUniq);
    expect(aliasedUniqBuilt.query).toContain("toUInt64(uniqExact(`events`.`user_id`))");
    expect(aliasedUniq.decoder("8")).toBe("8");

    // Embedded as a SQL operand (e.g. inside HAVING / ORDER BY) — exercises compile() reuse.
    const havingExpr = compileExpression({
      compile: (ctx) => sql`${defaultUniq.compile(ctx)} > {orm_paramN:Int64}`,
    }).query;
    expect(havingExpr).toContain("toFloat64(uniqExact(`events`.`user_id`)) > {orm_paramN:Int64}");

    // Boundary decoder coverage for uniqExact mirrors count.
    expect(fn.uniqExact(uniqArg).decoder(0)).toBe(0);
    expect(fn.uniqExact(uniqArg).decoder("9007199254740991")).toBe(9007199254740991);
    expect(() => fn.uniqExact(uniqArg).decoder(Number.NaN)).toThrow("Failed to decode count() result");
    expect(() => fn.uniqExact(uniqArg).decoder(1.5)).toThrow("Failed to decode count() result");
    expect(() => fn.uniqExact(uniqArg).toSafe().decoder(-1n)).toThrow("Failed to decode count() result");
    expect(() => fn.uniqExact(uniqArg).toSafe().decoder("1.0")).toThrow("Failed to decode count() result");
    expect(() => fn.uniqExact(uniqArg).toMixed().decoder(true)).toThrow("Failed to decode count() result");
  });

  it("compiles table functions with and without alias", function testTableFunctions() {
    const ctx = createContext();
    const source = tableFn.call("numbers", 10);
    const withoutAlias = compileSql(sql`${source.compileSource(ctx)}`, ctx);
    expect(withoutAlias.query).toContain("numbers({orm_param1:Int64})");
    expect(ctx.params).toEqual({
      orm_param1: 10,
    });

    const withAlias = compileSql(sql`${source.as("n").compileSource(createContext())}`);
    expect(withAlias.query).toContain("numbers({orm_param1:Int64}) as `n`");
  });
});

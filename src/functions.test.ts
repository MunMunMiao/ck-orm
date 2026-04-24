import { describe, expect, it } from "bun:test";
import { array as arrayColumn, dateTime, float64, int32, nullable, string } from "./columns";
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

describe("ck-orm functions", function describeClickHouseOrmFunctions() {
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

    const toDateTimeBuilt = compileExpression(
      fn.toDateTime(string().bind({ name: "created_at", tableName: "orders" }), "Asia/Shanghai"),
    );
    expect(toDateTimeBuilt.query).toContain("toDateTime(`orders`.`created_at`, {orm_param1:String})");
    expect(toDateTimeBuilt.params).toEqual({
      orm_param1: "Asia/Shanghai",
    });

    const toStartOfMonthBuilt = compileExpression(
      fn.toStartOfMonth(string().bind({ name: "created_at", tableName: "orders" })),
    );
    expect(toStartOfMonthBuilt.query).toContain("toStartOfMonth(`orders`.`created_at`)");

    const withParamsBuilt = compileExpression(
      fn.withParams("quantileExact", [0.95], float64().bind({ name: "price", tableName: "orders" })),
    );
    expect(withParamsBuilt.query).toContain("quantileExact({orm_param1:Float64})(`orders`.`price`)");
    expect(withParamsBuilt.params).toEqual({
      orm_param1: 0.95,
    });

    const countBuilt = compileExpression(fn.count());
    expect(countBuilt.query).toContain("count()");
    expect(fn.count().decoder(10)).toBe("10");

    const countIfBuilt = compileExpression(fn.countIf(fn.not(int32().bind({ name: "id", tableName: "orders" }))));
    expect(countIfBuilt.query).toContain("countIf(not(`orders`.`id`))");

    const countWithArg = compileExpression(fn.count(string().bind({ name: "name", tableName: "orders" })));
    expect(countWithArg.query).toContain("count(`orders`.`name`)");

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
    expect(fn.toStartOfMonth(string()).decoder("2026-04-01")).toEqual(new Date("2026-04-01"));
    expect(fn.avg(int32()).decoder(4.5)).toBe(4.5);
    expect(fn.avg(int32()).decoder("8")).toBe(8);
    expect(fn.avg(int32()).decoder(7n)).toBe(7);
    expect(fn.sum(float64()).decoder("12.5")).toBe(12.5);
    expect(fn.sum(int32()).decoder(12)).toBe("12");
    expect(fn.sum(int32()).decoder(7n)).toBe("7");
    expect(fn.sumIf(float64(), fn.not(fn.count())).decoder(7n)).toBe(7);
    expect(fn.countIf(fn.not(int32())).decoder(9)).toBe("9");
    expect(fn.avg(int32()).decoder("4.5")).toBe(4.5);
    expect(fn.min(int32()).decoder("8")).toBe(8);
    expect(fn.max(string()).decoder(1n)).toBe("1");
    const aggregateDate = new Date("2026-04-21T12:34:56.000Z");
    expect(fn.min(dateTime()).decoder(aggregateDate)).toBe(aggregateDate);
    expect(fn.max(dateTime()).decoder("2026-04-21T00:00:00.000Z")).toEqual(new Date("2026-04-21T00:00:00.000Z"));
    expect(fn.uniqExact(int32()).decoder(5)).toBe("5");
    expect(fn.count().decoder(true)).toBe("true");
    expect(fn.count().decoder(1n)).toBe("1");
    expect(fn.toString(int32()).decoder(1n)).toBe("1");
    expect(fn.toString(int32()).decoder(false)).toBe("false");
    expect(fn.avg(int32()).decoder(9n)).toBe(9);
    expect(fn.avg(int32()).decoder("9")).toBe(9);
    expect(() => fn.avg(int32()).decoder({})).toThrow("Cannot convert value to number");
    expect(() => fn.toString(int32()).decoder({})).toThrow("Cannot convert value to string");

    const coalesced = fn.coalesce(string(), int32());
    expect(coalesced.decoder(10)).toBe("10");
    expect(fn.coalesce().decoder({ raw: true })).toEqual({ raw: true });

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

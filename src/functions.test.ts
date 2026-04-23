import { describe, expect, it } from "bun:test";
import { array as arrayColumn, dateTime, float64, int32, nullable, string } from "./columns";
import { fn, tableFn } from "./functions";
import { compileSql, sql } from "./sql";

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

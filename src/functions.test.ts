import { describe, expect, it } from "bun:test";
import { float64, int32, string } from "./columns";
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

    const countWithArg = compileExpression(fn.count(string().bind({ name: "name", tableName: "orders" })));
    expect(countWithArg.query).toContain("count(`orders`.`name`)");
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
    expect(fn.avg(int32()).decoder("4.5")).toBe(4.5);
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

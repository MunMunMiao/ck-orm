import { describe, expect, it } from "bun:test";
import { isDecodeError } from "./errors";
import {
  compileValue,
  createExpression,
  decodeValue,
  ensureExpression,
  getExpressionSourceKey,
  isExpression,
  joinSqlParts,
  wrapSql,
} from "./query-shared";
import { compileSql, sql } from "./sql";

const createContext = () => ({
  params: {} as Record<string, unknown>,
  nextParamIndex: 0,
});

describe("ck-orm query shared helpers", function describeClickHouseOrmQuerySharedHelpers() {
  it("covers expressions, aliases, mapWith and type guards", function testExpressionHelpers() {
    const expression = createExpression<number>({
      compile: () => sql.raw("1"),
      decoder: (value) => Number(value),
      sqlType: "Int32",
    });

    expect(isExpression(expression)).toBe(true);
    expect(isExpression({})).toBe(false);
    expect(getExpressionSourceKey({})).toBeUndefined();

    const aliased = expression.as("one");
    expect(isExpression(aliased)).toBe(true);
    if (!isExpression(aliased)) {
      throw new Error("Expected aliased selection to remain an internal expression");
    }
    expect(aliased.outputAlias).toBe("one");

    const mapped = expression.mapWith((value) => String(value));
    expect(isExpression(mapped)).toBe(true);
    if (!isExpression(mapped)) {
      throw new Error("Expected mapped selection to remain an internal expression");
    }
    expect(mapped.decoder(2)).toBe("2");
  });

  it("compiles typed values and wraps raw sql", function testCompileValueHelpers() {
    const expression = createExpression<number>({
      compile: () => sql.raw("7"),
      decoder: (value) => Number(value),
      sqlType: "Int32",
    });

    const primitiveContext = createContext();
    compileSql(sql`${compileValue(1n, primitiveContext)}`, primitiveContext);
    compileSql(sql`${compileValue(true, primitiveContext)}`, primitiveContext);
    compileSql(sql`${compileValue(1.5, primitiveContext)}`, primitiveContext);
    compileSql(sql`${compileValue("name", primitiveContext)}`, primitiveContext);
    compileSql(sql`${compileValue(new Date("2026-04-21T00:00:00.000Z"), primitiveContext)}`, primitiveContext);
    compileSql(sql`${compileValue({ any: "thing" }, primitiveContext)}`, primitiveContext);
    expect(primitiveContext.params).toEqual({
      orm_param1: 1n,
      orm_param2: true,
      orm_param3: 1.5,
      orm_param4: "name",
      orm_param5: new Date("2026-04-21T00:00:00.000Z"),
      orm_param6: { any: "thing" },
    });

    const wrappedSql = wrapSql<string>(sql`raw_sql`, {
      decoder: (value) => String(value),
      sqlType: "String",
    });
    expect(wrappedSql.decoder(1)).toBe("1");

    const ensuredExpression = ensureExpression(expression);
    expect(ensuredExpression).toBe(expression);

    const directSql = sql`direct_sql`;
    const directSqlContext = createContext();
    expect(compileValue(directSql, directSqlContext)).toBe(directSql);

    const ensuredSql = ensureExpression(sql`wrapped`);
    expect(compileSql(sql`${ensuredSql.compile(createContext())}`).query).toContain("wrapped");
    expect(ensuredSql.decoder("same")).toBe("same");

    const ensuredPrimitive = ensureExpression(123, { sqlType: "Int32" });
    const primitiveBuilt = compileSql(sql`${ensuredPrimitive.compile(createContext())}`);
    expect(primitiveBuilt.query).toContain("{orm_param1:Int32}");

    const joinedEmpty = compileSql(sql`${joinSqlParts([], ", ")}`);
    expect(joinedEmpty.query).toBe("");
    const joined = compileSql(joinSqlParts([sql.raw("a"), sql.raw("b"), sql.raw("c")], ", "));
    expect(joined.query).toBe("a, b, c");

    const directContext = { params: {} as Record<string, unknown>, nextParamIndex: 0 };
    compileValue("kept", directContext, "String");
    expect(directContext.params.orm_param1).toBe("kept");
    expect(directContext.nextParamIndex).toBe(1);
  });

  it("decodes values and wraps decoder failures in DecodeError", function testDecodeValue() {
    expect(decodeValue((value) => Number(value), "2", "count")).toBe(2);
    expect(wrapSql(sql`plain`).decoder("plain")).toBe("plain");
    try {
      decodeValue(
        () => {
          throw new Error("bad");
        },
        "x",
        "count",
      );
      throw new Error("Expected decodeValue to fail");
    } catch (error) {
      expect(isDecodeError(error)).toBe(true);
      expect(error).toMatchObject({
        kind: "decode",
        executionState: "rejected",
        message: "[ck-orm] Failed to decode column: count",
      });
    }
  });
});

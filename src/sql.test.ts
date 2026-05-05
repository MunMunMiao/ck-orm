import { describe, expect, it } from "bun:test";
import { int32, string } from "./columns";
import { isClickHouseORMError } from "./errors";
import { fn } from "./functions";
import { assertDecimalParams, parseDecimalSqlType } from "./internal/decimal";
import { ckAlias, ckTable } from "./schema";
import { compileSql, sql } from "./sql";

const users = ckTable(
  "users",
  {
    id: int32(),
    name: string(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

describe("ck-orm sql", function describeClickHouseORMSql() {
  it("supports plain string sql fragments and structured interpolation", function testSqlInterpolation() {
    const built = compileSql(
      sql`select ${users.id}, ${fn.toString(users.name)} from ${users} where ${users.id} > ${1} and ${users.name} = ${"alice"}`,
    );

    expect(built.query).toContain("select `users`.`id`, toString(`users`.`name`) from `users`");
    expect(built.query).toContain("where `users`.`id` > {orm_param1:Int64} and `users`.`name` = {orm_param2:String}");
    expect(built.params).toEqual({
      orm_param1: 1,
      orm_param2: "alice",
    });

    expect(compileSql(sql("SELECT 1")).query).toBe("SELECT 1");
  });

  it("supports aliased table identifiers and non-primitive structured params", function testIdentifiersAndStructuredParams() {
    const aliasedUsers = ckAlias(users, "u");
    const built = compileSql(
      sql`select ${aliasedUsers.id} from ${aliasedUsers} where scores = ${[1, 2]} and props = ${{ a: 1 }} and tags = ${new Map([["k", 2]])}`,
    );

    expect(built.query).toContain("select `u`.`id` from `users` as `u`");
    expect(built.query).toContain("{orm_param1:Array(Int64)}");
    expect(built.query).toContain("{orm_param2:Map(String, Int64)}");
    expect(built.query).toContain("{orm_param3:Map(String, Int64)}");
    expect(built.params).toEqual({
      orm_param1: [1, 2],
      orm_param2: { a: 1 },
      orm_param3: new Map([["k", 2]]),
    });
  });

  it("rejects nullish raw params and preserves sql.join/sql.identifier helpers", function testSqlHelpers() {
    for (const value of [null, undefined]) {
      try {
        compileSql(sql`select ${value}`);
        throw new Error("Expected compileSql to fail for nullish raw params");
      } catch (error) {
        expect(isClickHouseORMError(error)).toBe(true);
        expect(error).toMatchObject({
          kind: "client_validation",
          executionState: "not_sent",
          message:
            "[ck-orm] Raw SQL parameters do not support null or undefined. Use ckSql`NULL` or builder expressions instead.",
        });
      }
    }

    const joined = compileSql(sql.join([sql.identifier("users"), sql.raw("final")], " "));
    expect(joined.query).toBe("`users` final");
    expect(compileSql(sql.join([sql.raw("left"), sql.raw("right")], sql.raw(" || "))).query).toBe("left || right");
    expect(compileSql(sql.join([], ", ")).query).toBe("");
    expect(() => compileSql({} as Parameters<typeof compileSql>[0])).toThrow(
      "Invalid SQL fragment: the provided fragment cannot be compiled",
    );
  });

  it("covers alias-only identifiers, compileSource interpolation and type inference fallbacks", function testSqlFallbacks() {
    const fragment = sql`value`;
    const aliasedFragment = fragment.as("value_alias");
    expect(aliasedFragment.outputAlias).toBe("value_alias");
    expect(fragment.mapWith((value) => Number(value)).decoder("7")).toBe(7);

    const source = fn.table.call("numbers", 3);

    const built = compileSql(
      sql`select ${sql.identifier({ as: "only_alias" })} from ${source} where bigint_value = ${1n} and bool_flag = ${true} and score = ${1.5} and created_at = ${new Date("2026-04-21T00:00:00.000Z")} and empty_array = ${[]} and mixed_array = ${[1, "two"]} and empty_map = ${new Map()} and uniform_map = ${new Map(
        [
          ["a", 1],
          ["b", 2],
        ],
      )} and mixed_map = ${new Map([
        ["a", 1],
        ["b", "two"],
      ])} and object_map = ${{ a: 1, b: "two" }}`,
    );

    expect(built.query).toContain("select `only_alias` from numbers({orm_param1:Int64})");
    expect(built.query).toContain("{orm_param2:Int64}");
    expect(built.query).toContain("{orm_param3:Bool}");
    expect(built.query).toContain("{orm_param4:Float64}");
    expect(built.query).toContain("{orm_param5:DateTime64(3)}");
    expect(built.query).toContain("{orm_param6:Array(String)}");
    expect(built.query).toContain("{orm_param7:Array(String)}");
    expect(built.query).toContain("{orm_param8:Map(String, String)}");
    expect(built.query).toContain("{orm_param9:Map(String, Int64)}");
    expect(built.query).toContain("{orm_param10:Map(String, String)}");
    expect(built.query).toContain("{orm_param11:Map(String, String)}");
    expect(built.params).toEqual({
      orm_param1: 3,
      orm_param2: 1n,
      orm_param3: true,
      orm_param4: 1.5,
      orm_param5: new Date("2026-04-21T00:00:00.000Z"),
      orm_param6: [],
      orm_param7: [1, "two"],
      orm_param8: new Map(),
      orm_param9: new Map([
        ["a", 1],
        ["b", 2],
      ]),
      orm_param10: new Map([
        ["a", 1],
        ["b", "two"],
      ]),
      orm_param11: { a: 1, b: "two" },
    });

    expect(() => compileSql(sql`select ${Symbol("bad") as unknown as string}`)).toThrow(
      "Unsupported SQL parameter value: Symbol(bad)",
    );
  });

  it("supports sql.decimal helper for Decimal precision casts", function testSqlDecimalHelper() {
    const cast = sql.decimal(sql`sum(${users.id})`, 20, 5);
    expect(compileSql(cast).query).toBe("CAST(sum(`users`.`id`) AS Decimal(20, 5))");
    expect(cast.decoder("12.34")).toBe("12.34");
    // Decoder coerces driver-side numbers (CH default JSON) into strings.
    expect(cast.decoder(12.34 as never)).toBe("12.34");

    const numericCast = sql.decimal(42, 18, 2);
    const builtNumeric = compileSql(numericCast);
    expect(builtNumeric.query).toBe("CAST({orm_param1:Int64} AS Decimal(18, 2))");
    expect(builtNumeric.params).toEqual({ orm_param1: 42 });

    expect(() => sql.decimal(sql`x`, 0, 0)).toThrow(/precision must be an integer between 1 and 76/);
    expect(() => sql.decimal(sql`x`, 5, 6)).toThrow(/scale must be an integer between 0 and precision/);
    expect(() => sql.decimal(sql`x`, 5.5 as number, 2)).toThrow(/precision must be an integer/);

    // Callers needing a different decoded shape chain .mapWith().
    const remapped = sql.decimal(sql`y`, 18, 2).mapWith((v) => Number(v));
    expect(remapped.decoder("1.50")).toBe(1.5);
  });
});

describe("ck-orm internal/decimal", function describeInternalDecimal() {
  it("validates precision and scale ranges", function testAssertDecimalParams() {
    expect(() => assertDecimalParams({ precision: 0, scale: 0 })).toThrow(
      /precision must be an integer between 1 and 76/,
    );
    expect(() => assertDecimalParams({ precision: 77, scale: 5 })).toThrow(
      /precision must be an integer between 1 and 76/,
    );
    expect(() => assertDecimalParams({ precision: 5.5, scale: 0 })).toThrow(/precision must be an integer/);
    expect(() => assertDecimalParams({ precision: 18, scale: -1 })).toThrow(
      /scale must be an integer between 0 and precision/,
    );
    expect(() => assertDecimalParams({ precision: 18, scale: 19 })).toThrow(
      /scale must be an integer between 0 and precision/,
    );
    expect(() => assertDecimalParams({ precision: 18, scale: 5 })).not.toThrow();
  });

  it("parses ClickHouse Decimal sqlType strings", function testParseDecimalSqlType() {
    expect(parseDecimalSqlType("Decimal(20, 5)")).toEqual({ precision: 20, scale: 5 });
    expect(parseDecimalSqlType("Decimal( 18 ,2 )")).toEqual({ precision: 18, scale: 2 });
    expect(parseDecimalSqlType("Decimal32(4)")).toEqual({ precision: 9, scale: 4 });
    expect(parseDecimalSqlType("Decimal64(10)")).toEqual({ precision: 18, scale: 10 });
    expect(parseDecimalSqlType("Decimal128(20)")).toEqual({ precision: 38, scale: 20 });
    expect(parseDecimalSqlType("Decimal256(30)")).toEqual({ precision: 76, scale: 30 });

    // Reject malformed / non-decimal / out-of-range inputs.
    expect(parseDecimalSqlType(undefined)).toBeUndefined();
    expect(parseDecimalSqlType("")).toBeUndefined();
    expect(parseDecimalSqlType("Float64")).toBeUndefined();
    expect(parseDecimalSqlType("Decimal(20)")).toBeUndefined();
    expect(parseDecimalSqlType("Decimal(20, 5)) garbage")).toBeUndefined();
    expect(parseDecimalSqlType("Decimal32(10)")).toBeUndefined();
    expect(parseDecimalSqlType("Decimal(0, 0)")).toBeUndefined();
    expect(parseDecimalSqlType("Decimal(20, 21)")).toBeUndefined();
    expect(parseDecimalSqlType("Nullable(Decimal(20, 5))")).toBeUndefined();
  });
});

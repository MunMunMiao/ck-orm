import { describe, expect, it } from "bun:test";
import { int32, string } from "./columns";
import { isClickHouseOrmError } from "./errors";
import { fn } from "./functions";
import { alias, chTable } from "./schema";
import { compileSql, sql } from "./sql";

const users = chTable(
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

describe("ck-orm sql", function describeClickHouseOrmSql() {
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
    const aliasedUsers = alias(users, "u");
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
        expect(isClickHouseOrmError(error)).toBe(true);
        expect(error).toMatchObject({
          kind: "client_validation",
          executionState: "not_sent",
          message:
            "[ck-orm] Raw SQL parameters do not support null or undefined. Use sql.raw('NULL') or builder expressions instead.",
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

    const source = {
      compileSource() {
        return sql.raw("numbers(3)");
      },
    };

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

    expect(built.query).toContain("select `only_alias` from numbers(3)");
    expect(built.query).toContain("{orm_param1:Int64}");
    expect(built.query).toContain("{orm_param2:Bool}");
    expect(built.query).toContain("{orm_param3:Float64}");
    expect(built.query).toContain("{orm_param4:DateTime64(3)}");
    expect(built.query).toContain("{orm_param5:Array(String)}");
    expect(built.query).toContain("{orm_param6:Array(String)}");
    expect(built.query).toContain("{orm_param7:Map(String, String)}");
    expect(built.query).toContain("{orm_param8:Map(String, Int64)}");
    expect(built.query).toContain("{orm_param9:Map(String, String)}");
    expect(built.query).toContain("{orm_param10:Map(String, String)}");
    expect(built.params).toEqual({
      orm_param1: 1n,
      orm_param2: true,
      orm_param3: 1.5,
      orm_param4: new Date("2026-04-21T00:00:00.000Z"),
      orm_param5: [],
      orm_param6: [1, "two"],
      orm_param7: new Map(),
      orm_param8: new Map([
        ["a", 1],
        ["b", 2],
      ]),
      orm_param9: new Map([
        ["a", 1],
        ["b", "two"],
      ]),
      orm_param10: { a: 1, b: "two" },
    });

    expect(() => compileSql(sql`select ${Symbol("bad") as unknown as string}`)).toThrow(
      "Unsupported SQL parameter value: Symbol(bad)",
    );
  });
});

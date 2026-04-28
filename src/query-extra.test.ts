import { describe, expect, it } from "bun:test";
import { array, float64, int32, string } from "./columns";
import { fn, tableFn } from "./functions";
import {
  and,
  asc,
  between,
  compileQuerySymbol,
  compileWithContextSymbol,
  contains,
  containsIgnoreCase,
  createInsertBuilder,
  createQueryClient,
  createSelectBuilder,
  createSessionId,
  decodeRow,
  desc,
  endsWith,
  eq,
  exists,
  expr,
  gt,
  gte,
  has,
  hasAll,
  hasAny,
  hasSubstr,
  ilike,
  inArray,
  like,
  lt,
  lte,
  ne,
  not,
  notExists,
  notIlike,
  notInArray,
  notLike,
  or,
  startsWith,
} from "./query";
import type { Predicate } from "./query-shared";
import { ckAlias, ckTable } from "./schema";
import { sql } from "./sql";

const normalizeSql = (value: string) => value.replace(/\s+/g, " ").trim();
const buildCompiled = (compiled: { statement: string; params: Record<string, unknown> }) => {
  return {
    query: compiled.statement,
    params: compiled.params,
  };
};

const orders = ckTable(
  "orders",
  {
    id: int32(),
    name: string(),
    amount: float64(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

const taggedOrders = ckTable(
  "tagged_orders",
  {
    id: int32(),
    tags: array(string()),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

describe("ck-orm query extras", function describeClickHouseORMQueryExtras() {
  it("covers builder errors, default selection, operators and table-function sources", function testBuilderBranches() {
    const db = createQueryClient({
      schema: { orders },
    });

    expect(() => db.select().buildSelectionItems()).toThrow(
      "select() without explicit selection requires from() first",
    );
    expect(() => db.select().from(tableFn.call("numbers", 5)).buildSelectionItems()).toThrow(
      "select() without explicit selection requires a source with known columns",
    );

    const subquery = db
      .select({
        id: orders.id,
        total: fn.sum(orders.amount).as("total_amount"),
      })
      .from(orders)
      .as("order_totals");

    const totals = db.$with("totals").as(
      db
        .select({
          id: orders.id,
        })
        .from(orders),
    );

    const built = buildCompiled(
      db
        .with(totals)
        .select()
        .from(orders)
        .innerJoin(subquery, eq(orders.id, subquery.id))
        .where(
          and(
            eq(orders.id, 1),
            between(orders.amount, 1, 10),
            inArray(orders.id, [1, 2]),
            notInArray(orders.id, []),
            notInArray(orders.id, [99, 100]),
            exists(subquery),
            notExists(totals),
            not(or(eq(orders.id, 2), undefined)),
          ),
        )
        .groupBy(orders.id, orders.name, orders.amount)
        .having(or(eq(orders.id, 1), undefined))
        .orderBy(orders.id, desc(orders.amount))
        .limit(10)
        .offset(5)
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain(
      "select `orders`.`id` as `__orm_1`, `orders`.`name` as `__orm_2`, `orders`.`amount` as `__orm_3`, `order_totals`.`id` as `__orm_4`, `order_totals`.`total_amount` as `__orm_5`",
    );
    expect(normalizeSql(built.query)).toContain(
      "inner join (select `orders`.`id` as `id`, sum(`orders`.`amount`) as `total_amount` from `orders`) as `order_totals` on `orders`.`id` = `order_totals`.`id`",
    );
    expect(normalizeSql(built.query)).toContain(
      "`orders`.`amount` between {orm_param2:Float64} and {orm_param3:Float64}",
    );
    expect(normalizeSql(built.query)).toContain("`orders`.`id` in ({orm_param4:Int32}, {orm_param5:Int32})");
    expect(normalizeSql(built.query)).toContain("1");
    expect(normalizeSql(built.query)).toContain(
      "exists (select `orders`.`id` as `id`, sum(`orders`.`amount`) as `total_amount` from `orders`)",
    );
    expect(normalizeSql(built.query)).toContain("not (exists (select `orders`.`id` as `id` from `orders`))");
    expect(normalizeSql(built.query)).toContain("not (`orders`.`id` = {orm_param8:Int32})");
    expect(normalizeSql(built.query)).toContain("order by `orders`.`id` ASC, `orders`.`amount` DESC");
    expect(normalizeSql(built.query)).toContain("limit {orm_param10:Int64}");
    expect(normalizeSql(built.query)).toContain("offset {orm_param11:Int64}");
    expect(built.params).toEqual({
      orm_param1: 1,
      orm_param2: 1,
      orm_param3: 10,
      orm_param4: 1,
      orm_param5: 2,
      orm_param6: 99,
      orm_param7: 100,
      orm_param8: 2,
      orm_param9: 1,
      orm_param10: 10,
      orm_param11: 5,
    });

    const tableFnBuilt = buildCompiled(
      db
        .select({
          value: fn.count(),
        })
        .from(tableFn.call("numbers", 3).as("n"))
        [compileQuerySymbol](),
    );
    expect(normalizeSql(tableFnBuilt.query)).toContain("from numbers({orm_param1:Int64}) as `n`");
  });

  it("covers typed value params, decodeRow and session id helpers", function testNamedParamsAndHelpers() {
    const db = createQueryClient({
      schema: { orders },
    });
    const totals = db
      .select({
        id: orders.id,
      })
      .from(orders)
      .as("totals");

    const built = buildCompiled(
      db
        .select({
          id: orders.id,
          total: fn.sum(orders.amount).as("total"),
        })
        .from(orders)
        .where(eq(orders.id, 1))
        .limit(10)
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("where `orders`.`id` = {orm_param1:Int32}");
    expect(normalizeSql(built.query)).toContain("limit {orm_param2:Int64}");
    expect(built.params).toEqual({
      orm_param1: 1,
      orm_param2: 10,
    });

    const compiledInsert = buildCompiled(
      db
        .insert(orders)
        .values({
          id: 1,
          name: "first",
          amount: 1.5,
        })
        [compileQuerySymbol](),
    );
    expect(compiledInsert.query).toContain("insert into `orders`");

    expect(
      buildCompiled(
        db
          .select({
            id: orders.id,
            name: orders.name,
          })
          .from(orders)
          .where(
            and(
              eq(orders.id, 1),
              ne(orders.id, 99),
              gt(orders.amount, 1),
              gte(orders.amount, 1),
              lt(orders.amount, 100),
              lte(orders.amount, 100),
              not(eq(orders.id, 2)),
            ),
          )
          .orderBy(asc(orders.id), desc(orders.id))
          [compileQuerySymbol](),
      ).query,
    ).toContain("DESC");

    expect(
      buildCompiled(
        db
          .select({
            id: orders.id,
            raw: expr(sql.raw("1")).as("raw"),
          })
          .from(orders)
          .where(
            and(
              eq(orders.id, 1),
              not(eq(orders.id, 2)),
              not(inArray(orders.id, [])),
              not(notInArray(orders.id, [])),
              not(eq(orders.id, 3)),
            ),
          )
          [compileQuerySymbol](),
      ).query,
    ).toContain("not");

    expect(expr(sql.raw("1").mapWith((value) => Number(value) + 1)).decoder("4")).toBe(5);
    expect(eq(orders.id, 1).decoder(1)).toBe(true);
    expect(and(eq(orders.id, 1), gt(orders.amount, 0)).decoder("1")).toBe(true);
    expect(or(eq(orders.id, 0), eq(orders.id, 1)).decoder(1)).toBe(true);
    expect(not(eq(orders.id, 1)).decoder(0)).toBe(false);
    expect(between(orders.amount, 1, 10).decoder(1)).toBe(true);
    expect(inArray(orders.id, [1, 2]).decoder(1)).toBe(true);
    expect(notInArray(orders.id, totals).decoder(0)).toBe(false);
    expect(exists(totals).decoder(1)).toBe(true);

    expect(
      decodeRow<{ id: number; amount: number }>({ id: "4", total_amount: "8.5" }, [
        {
          key: "id",
          sqlAlias: "id",
          decoder: (value) => Number(value),
          path: ["id"],
        },
        {
          key: "amount",
          sqlAlias: "total_amount",
          decoder: (value) => Number(value),
          path: ["amount"],
        },
      ]),
    ).toEqual({
      id: 4,
      amount: 8.5,
    });

    // Nested path collapses fields under a group key, and a fully-null nullable group becomes null.
    type ProfileRow = {
      readonly id: number;
      readonly profile: { readonly name: string; readonly email: string | null } | null;
      readonly meta: { readonly tag: string };
    };
    const decodeStr = (value: unknown) => String(value);
    const decoded = decodeRow<ProfileRow>(
      {
        id: "1",
        profile_name: null,
        profile_email: null,
        meta_tag: "vip",
      },
      [
        { key: "id", sqlAlias: "id", decoder: (v) => Number(v), path: ["id"] },
        {
          key: "profile.name",
          sqlAlias: "profile_name",
          decoder: decodeStr,
          path: ["profile", "name"],
          nullable: true,
          groupNullable: true,
        },
        {
          key: "profile.email",
          sqlAlias: "profile_email",
          decoder: decodeStr,
          path: ["profile", "email"],
          nullable: true,
          groupNullable: true,
        },
        {
          key: "meta.tag",
          sqlAlias: "meta_tag",
          decoder: decodeStr,
          path: ["meta", "tag"],
        },
      ],
    );
    expect(decoded).toEqual({ id: 1, profile: null, meta: { tag: "vip" } });

    // Same shape but with a non-null field present – nullable group survives with its fields.
    const populated = decodeRow<ProfileRow>(
      {
        id: "2",
        profile_name: "alice",
        profile_email: null,
        meta_tag: "core",
      },
      [
        { key: "id", sqlAlias: "id", decoder: (v) => Number(v), path: ["id"] },
        {
          key: "profile.name",
          sqlAlias: "profile_name",
          decoder: decodeStr,
          path: ["profile", "name"],
          nullable: true,
          groupNullable: true,
        },
        {
          key: "profile.email",
          sqlAlias: "profile_email",
          decoder: decodeStr,
          path: ["profile", "email"],
          nullable: true,
          groupNullable: true,
        },
        {
          key: "meta.tag",
          sqlAlias: "meta_tag",
          decoder: decodeStr,
          path: ["meta", "tag"],
        },
      ],
    );
    expect(populated).toEqual({
      id: 2,
      profile: { name: "alice", email: null },
      meta: { tag: "core" },
    });

    const sessionId = createSessionId();
    expect(sessionId.startsWith("ck_orm_")).toBe(true);
    expect(sessionId.includes("-")).toBe(false);
  });

  it("validates insert rows before SQL compilation", function testInsertValidation() {
    const db = createQueryClient({
      schema: { orders },
    });

    expect(() => db.insert(orders).values([])).toThrow("insert().values() requires at least one row");
    expect(() => db.insert(orders).values(null as never)).toThrow("insert().values() row 1 must be an object");
    expect(() => db.insert(orders).values({ id: 1, typo_amount: 2 } as never)).toThrow(
      "insert().values() row 1 contains unknown columns: typo_amount",
    );
    expect(() =>
      db
        .insert(orders)
        .values([{ id: 1, name: "first", amount: 1.25 }, { id: 2, name: "second", extra_field: true } as never]),
    ).toThrow("insert().values() row 2 contains unknown columns: extra_field");
  });

  it("supports predicate arrays and variadic where() assembly", function testPredicateAssembly() {
    const db = createQueryClient({
      schema: { orders },
    });

    const emptyPredicates: Predicate[] = [];
    const emptyQuery = buildCompiled(
      db
        .select({
          id: orders.id,
        })
        .from(orders)
        .where(...emptyPredicates)
        [compileQuerySymbol](),
    );

    expect(normalizeSql(emptyQuery.query)).not.toContain("where");

    const singlePredicate = eq(orders.id, 1);
    expect(and()).toBeUndefined();
    expect(or()).toBeUndefined();
    expect(and(singlePredicate)).toBe(singlePredicate);
    expect(or(singlePredicate)).toBe(singlePredicate);

    const singleQuery = buildCompiled(
      db
        .select({
          id: orders.id,
        })
        .from(orders)
        .where(singlePredicate)
        [compileQuerySymbol](),
    );

    expect(normalizeSql(singleQuery.query)).toContain("where `orders`.`id` = {orm_param1:Int32}");
    expect(singleQuery.params).toEqual({
      orm_param1: 1,
    });

    const groupedPredicates: Predicate[] = [];
    groupedPredicates.push(or(eq(orders.id, 1), eq(orders.id, 2)));
    groupedPredicates.push(gt(orders.amount, 10));
    groupedPredicates.push(between(orders.amount, 11, 20));
    groupedPredicates.push(
      exists(
        db
          .select({
            id: orders.id,
          })
          .from(orders)
          .where(eq(orders.id, 1))
          .as("matching_orders"),
      ),
    );

    const groupedQuery = buildCompiled(
      db
        .select({
          id: orders.id,
        })
        .from(orders)
        .where(...groupedPredicates)
        [compileQuerySymbol](),
    );

    expect(normalizeSql(groupedQuery.query)).toContain(
      "where ((`orders`.`id` = {orm_param1:Int32} or `orders`.`id` = {orm_param2:Int32}) and `orders`.`amount` > {orm_param3:Float64} and `orders`.`amount` between {orm_param4:Float64} and {orm_param5:Float64} and exists (select `orders`.`id` as `id` from `orders` where `orders`.`id` = {orm_param6:Int32}))",
    );
    expect(groupedQuery.params).toEqual({
      orm_param1: 1,
      orm_param2: 2,
      orm_param3: 10,
      orm_param4: 11,
      orm_param5: 20,
      orm_param6: 1,
    });

    expect(
      buildCompiled(
        db
          .select({
            id: orders.id,
          })
          .from(orders)
          .where(eq(orders.id, 99))
          [compileQuerySymbol](),
      ).query,
    ).toContain("where `orders`.`id` = {orm_param1:Int32}");
  });

  it("compiles has-style predicates with array-aware parameter typing", function testHasPredicates() {
    const db = createQueryClient({
      schema: { taggedOrders },
    });

    const built = buildCompiled(
      db
        .select({
          id: taggedOrders.id,
        })
        .from(taggedOrders)
        .where(
          has(taggedOrders.tags, "vip"),
          hasAll(taggedOrders.tags, ["vip", "pro"]),
          hasAny(taggedOrders.tags, []),
          hasSubstr(taggedOrders.tags, ["vip"]),
        )
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("has(`tagged_orders`.`tags`, {orm_param1:String})");
    expect(normalizeSql(built.query)).toContain("hasAll(`tagged_orders`.`tags`, {orm_param2:Array(String)})");
    expect(normalizeSql(built.query)).toContain("hasAny(`tagged_orders`.`tags`, {orm_param3:Array(String)})");
    expect(normalizeSql(built.query)).toContain("hasSubstr(`tagged_orders`.`tags`, {orm_param4:Array(String)})");
    expect(built.params).toEqual({
      orm_param1: "vip",
      orm_param2: ["vip", "pro"],
      orm_param3: [],
      orm_param4: ["vip"],
    });

    expect(has(taggedOrders.tags, "vip").decoder(1)).toBe(true);
    expect(hasAll(taggedOrders.tags, ["vip"]).decoder(0)).toBe(false);
    expect(hasAny(taggedOrders.tags, ["vip"]).decoder("1")).toBe(true);
    expect(hasSubstr(taggedOrders.tags, ["vip"]).decoder("1")).toBe(true);
  });

  it("supports drizzle-style db.count() for direct execution and scalar subqueries", async function testDbCount() {
    const compiledStatements: Array<{
      statement: string;
      params: Record<string, unknown>;
    }> = [];
    const db = createQueryClient({
      schema: { orders },
      runner: {
        async execute<TResult extends Record<string, unknown>>(compiled: {
          statement: string;
          params: Record<string, unknown>;
          selection?: readonly { decoder?: (value: unknown) => unknown }[];
        }) {
          compiledStatements.push({
            statement: compiled.statement,
            params: compiled.params,
          });
          const rawValue = compiled.statement.includes("toString(count())")
            ? "7"
            : compiled.statement.includes("toUInt64(count())")
              ? "7"
              : 7;
          const decoder = compiled.selection?.[0]?.decoder;
          return [{ value: decoder ? decoder(rawValue) : rawValue }] as TResult[];
        },
        async *iterator() {},
        async command() {},
      },
    });
    expect((db.count(orders) as { sqlType?: string }).sqlType).toBe("Float64");
    expect((db.count(orders).toSafe() as { sqlType?: string }).sqlType).toBe("String");
    expect((db.count(orders).toMixed() as { sqlType?: string }).sqlType).toBe("UInt64");

    const total = await db.count(orders, eq(orders.id, 1));
    expect(total).toBe(7);
    expect(normalizeSql(compiledStatements[0]?.statement ?? "")).toContain(
      "select toFloat64(count()) as `__orm_count` from `orders` where `orders`.`id` = {orm_param1:Int32}",
    );
    expect(compiledStatements[0]?.params).toEqual({
      orm_param1: 1,
    });

    const safeTotal = await db.count(orders, eq(orders.id, 1)).toSafe();
    expect(safeTotal).toBe("7");
    expect(normalizeSql(compiledStatements[1]?.statement ?? "")).toContain(
      "select toString(count()) as `__orm_count` from `orders` where `orders`.`id` = {orm_param1:Int32}",
    );
    expect(compiledStatements[1]?.params).toEqual({
      orm_param1: 1,
    });

    const topOrders = db
      .select({
        id: orders.id,
      })
      .from(orders)
      .where(gt(orders.amount, 10))
      .as("top_orders");

    await db.count(topOrders).execute();
    expect(normalizeSql(compiledStatements[2]?.statement ?? "")).toContain(
      "select toFloat64(count()) as `__orm_count` from (select `orders`.`id` as `id` from `orders` where `orders`.`amount` > {orm_param1:Float64}) as `top_orders`",
    );
    expect(compiledStatements[2]?.params).toEqual({
      orm_param1: 10,
    });

    const totals = db.$with("totals").as(
      db
        .select({
          id: orders.id,
        })
        .from(orders),
    );

    await db.with(totals).count(totals).execute();
    expect(normalizeSql(compiledStatements[3]?.statement ?? "")).toContain(
      "with `totals` as (select `orders`.`id` as `id` from `orders`) select toFloat64(count()) as `__orm_count` from `totals`",
    );

    const mixedTotal = await db.with(totals).count(totals).toMixed().execute();
    expect(mixedTotal).toBe("7");
    expect(normalizeSql(compiledStatements[4]?.statement ?? "")).toContain(
      "with `totals` as (select `orders`.`id` as `id` from `orders`) select toUInt64(count()) as `__orm_count` from `totals`",
    );

    const built = buildCompiled(
      db
        .select({
          id: orders.id,
          matchingOrderCount: db.count(orders, gt(orders.amount, 10)).toSafe().as("matching_order_count"),
        })
        .from(orders)
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain(
      "(select toString(count()) from `orders` where `orders`.`amount` > {orm_param1:Float64}) as `matching_order_count`",
    );
    expect(built.params).toEqual({
      orm_param1: 10,
    });

    const cteBoundDb = db.with(totals);
    const cteBuilt = buildCompiled(
      cteBoundDb
        .select({
          id: orders.id,
          totalCount: cteBoundDb.count(totals).toMixed().as("total_count"),
        })
        .from(orders)
        .where(
          and(
            inArray(
              orders.id,
              db
                .select({
                  id: orders.id,
                })
                .from(orders)
                .where(gt(orders.amount, 5))
                .as("matching_ids"),
            ),
            notInArray(orders.id, totals),
            exists(
              db
                .select({
                  id: orders.id,
                })
                .from(orders)
                .where(eq(orders.id, 1)),
            ),
          ),
        )
        [compileQuerySymbol](),
    );

    expect(normalizeSql(cteBuilt.query)).toContain(
      "with `totals` as (select `orders`.`id` as `id` from `orders`) select `orders`.`id` as `id`, (with `totals` as (select `orders`.`id` as `id` from `orders`) select toUInt64(count()) from `totals`) as `total_count` from `orders`",
    );
    expect(normalizeSql(cteBuilt.query)).toContain(
      "`orders`.`id` in (select `orders`.`id` as `id` from `orders` where `orders`.`amount` > {orm_param1:Float64})",
    );
    expect(normalizeSql(cteBuilt.query)).toContain("`orders`.`id` not in (select `orders`.`id` as `id` from `orders`)");
    expect(normalizeSql(cteBuilt.query)).toContain(
      "exists (select `orders`.`id` as `id` from `orders` where `orders`.`id` = {orm_param2:Int32})",
    );
    expect(cteBuilt.params).toEqual({
      orm_param1: 5,
      orm_param2: 1,
    });
  });

  it("keeps select builder chains immutable across factory facades", function testBuilderImmutability() {
    const db = createQueryClient({
      schema: { orders },
    });

    const base = db
      .select({
        id: orders.id,
      })
      .from(orders);
    const filtered = base.where(eq(orders.id, 1));
    const limited = filtered.limit(5);

    expect(filtered).not.toBe(base);
    expect(limited).not.toBe(filtered);
    expect(limited).not.toBe(base);

    const baseCompiled = buildCompiled(base[compileQuerySymbol]());
    const filteredCompiled = buildCompiled(filtered[compileQuerySymbol]());
    const limitedCompiled = buildCompiled(limited[compileQuerySymbol]());

    expect(normalizeSql(baseCompiled.query)).not.toContain("where");
    expect(normalizeSql(baseCompiled.query)).not.toContain("limit");
    expect(baseCompiled.params).toEqual({});

    expect(normalizeSql(filteredCompiled.query)).toContain("where `orders`.`id` = {orm_param1:Int32}");
    expect(normalizeSql(filteredCompiled.query)).not.toContain("limit");
    expect(filteredCompiled.params).toEqual({
      orm_param1: 1,
    });

    expect(normalizeSql(limitedCompiled.query)).toContain("where `orders`.`id` = {orm_param1:Int32}");
    expect(normalizeSql(limitedCompiled.query)).toContain("limit {orm_param2:Int64}");
    expect(limitedCompiled.params).toEqual({
      orm_param1: 1,
      orm_param2: 5,
    });
  });

  it("keeps derived query clients and queries from mutating their source", function testDerivedClientImmutability() {
    const db = createQueryClient({
      schema: { orders },
    });
    const baseQuery = db
      .select({
        id: orders.id,
      })
      .from(orders);
    const totals = db.$with("totals").as(
      db
        .select({
          id: orders.id,
        })
        .from(orders),
    );
    const cteDb = db.with(totals);
    const flaggedTotals = db.$with("flagged_totals").as(
      db
        .select({
          id: orders.id,
        })
        .from(orders)
        .where(gt(orders.amount, 10)),
    );
    const extendedDb = cteDb.with(flaggedTotals);

    const baseCompiled = buildCompiled(baseQuery[compileQuerySymbol]());
    const cteCompiled = buildCompiled(
      cteDb
        .select({
          id: totals.id,
        })
        .from(totals)
        [compileQuerySymbol](),
    );
    const extendedCompiled = buildCompiled(
      extendedDb
        .select({
          id: totals.id,
        })
        .from(totals)
        [compileQuerySymbol](),
    );

    expect(db.ctes).toEqual([]);
    expect(cteDb.ctes).toHaveLength(1);
    expect(extendedDb.ctes).toHaveLength(2);

    expect(normalizeSql(baseCompiled.query)).not.toContain("with `totals`");
    expect(baseCompiled.params).toEqual({});

    expect(normalizeSql(cteCompiled.query)).toContain("with `totals` as");
    expect(normalizeSql(cteCompiled.query)).not.toContain("`flagged_totals`");
    expect(cteCompiled.params).toEqual({});

    expect(normalizeSql(extendedCompiled.query)).toContain("with `totals` as");
    expect(normalizeSql(extendedCompiled.query)).toContain("`flagged_totals`");
    expect(extendedCompiled.params).toEqual({
      orm_param1: 10,
    });
  });

  it("covers compatibility factories and runner boundary errors", async function testCompatibilityFactoriesAndRunnerBoundaries() {
    const selectBuilder = createSelectBuilder<{ id: number }>({
      selection: {
        id: orders.id,
      },
    }).from(orders);
    expect(() => selectBuilder.execute()).toThrow(
      "execute() requires a clickhouseClient-backed query runner. Attach one with clickhouseClient(...).select(...) or clickhouseClient(...).from(table).",
    );

    const insertBuilder = createInsertBuilder(orders).values({
      id: 1,
    });
    expect(() => insertBuilder.execute()).toThrow(
      "execute() requires a clickhouseClient-backed query runner. Attach one with clickhouseClient(...).select(...) or clickhouseClient(...).from(table).",
    );

    const countDb = createQueryClient({
      schema: { orders },
      runner: {
        async execute() {
          return [];
        },
        async *iterator() {},
        async command() {
          return undefined;
        },
      },
    });

    await expect(countDb.count(orders).execute()).rejects.toThrow("count() query did not return a result row");
  });

  it("fails fast with an internal error when nested forced settings compile without active state", function testMissingCompileStateInvariant() {
    const nestedQuery = {
      [compileWithContextSymbol]() {
        return {
          kind: "compiled-query" as const,
          mode: "query" as const,
          statement: "select 1",
          params: {},
          selection: [],
          forcedSettings: {
            join_use_nulls: 1,
          },
        };
      },
      [compileQuerySymbol]() {
        return nestedQuery[compileWithContextSymbol]({
          params: {},
          nextParamIndex: 0,
        });
      },
    };

    const existsPredicate = exists(nestedQuery as never);
    expect(() =>
      existsPredicate.compile({
        params: {},
        nextParamIndex: 0,
      }),
    ).toThrow("Missing active compile state while collecting forced settings");
  });

  it("covers join-only metadata, insert DEFAULT rendering and array-function raw expressions", function testMetadataDefaultsAndArrayArgs() {
    const db = createQueryClient({
      schema: { orders, taggedOrders },
    });

    const joinOnlyCompiled = db
      .select({
        id: taggedOrders.id,
      })
      .leftJoin(taggedOrders, expr(sql.raw("1"), { sqlType: "Bool", decoder: (value) => Boolean(value) }))
      [compileWithContextSymbol]({
        params: {},
        nextParamIndex: 0,
      });
    expect(joinOnlyCompiled.metadata).toEqual({
      joinCount: 1,
    });

    const defaultInsert = buildCompiled(
      createInsertBuilder(orders)
        .values({
          id: 3,
        })
        [compileQuerySymbol](),
    );
    expect(defaultInsert.query).toContain("values ({orm_param1:Int32}, DEFAULT, DEFAULT)");
    expect(defaultInsert.params).toEqual({
      orm_param1: 3,
    });

    const rawArrayPredicate = buildCompiled(
      db
        .select({
          id: taggedOrders.id,
        })
        .from(taggedOrders)
        .where(hasAny(taggedOrders.tags, expr(sql.raw("['vip','pro']"))))
        [compileQuerySymbol](),
    );
    expect(normalizeSql(rawArrayPredicate.query)).toContain("hasAny(`tagged_orders`.`tags`, ['vip','pro'])");
  });

  it("rejects duplicate SQL aliases in explicit selections", function testDuplicateSelectionAliases() {
    const db = createQueryClient({
      schema: { orders },
    });

    expect(() =>
      db
        .select({
          id: orders.id,
          renamed: orders.name.as("id"),
        })
        .from(orders)
        [compileQuerySymbol](),
    ).toThrow('Duplicate SQL selection alias "id"');
  });

  it("skips generated insert columns and rejects invalid insert targets", function testGeneratedInsertColumns() {
    const generatedOrders = ckTable(
      "generated_orders",
      {
        id: int32(),
        name: string(),
        shardDay: int32("shard_day").materialized(sql`toYYYYMM(id)`),
        nameAlias: string("name_alias").aliasExpr(sql`name`),
      },
      (table) => ({
        engine: "MergeTree",
        orderBy: [table.id],
      }),
    );

    const compiled = buildCompiled(
      createInsertBuilder(generatedOrders)
        .values({
          id: 1,
          name: "alice",
        })
        [compileQuerySymbol](),
    );

    expect(normalizeSql(compiled.query)).toContain("insert into `generated_orders` (`id`, `name`) values");
    expect(compiled.query).not.toContain("shard_day");
    expect(compiled.query).not.toContain("name_alias");

    expect(() =>
      createInsertBuilder(generatedOrders).values({
        id: 1,
        name: "alice",
        shardDay: 202604,
      } as never),
    ).toThrow("cannot provide generated columns: shardDay");

    expect(() => createInsertBuilder(generatedOrders)[compileQuerySymbol]()).toThrow(
      "insert().values() must be called with at least one row before execute()",
    );
    expect(() => createInsertBuilder(ckAlias(generatedOrders, "g"))).toThrow(
      "insert() requires a base table and does not accept aliased table targets",
    );
  });

  it("keeps decodeRow resilient to unsupported nested selection paths", function testDecodeRowUnsupportedNestedPath() {
    const decoded = decodeRow<Record<string, unknown>>(
      {
        root: "value",
      },
      [
        {
          key: "root.deep.leaf",
          sqlAlias: "root",
          decoder: (value) => value,
          path: ["root", "deep", "leaf"] as unknown as [string, string],
        },
      ],
    );

    expect(decoded).toEqual({});
  });

  it("covers thenable helper catch/finally paths and invalid count decoding", async function testThenableCatchAndFinally() {
    const decodeDb = createQueryClient({
      schema: { orders },
    });

    const defaultCount = decodeDb.count(orders);
    expect(defaultCount.decoder(42)).toBe(42);
    expect(defaultCount.decoder("42")).toBe(42);
    expect(defaultCount.decoder(42n)).toBe(42);

    const safeCount = defaultCount.toSafe();
    expect(safeCount.decoder("42")).toBe("42");
    expect(safeCount.decoder(42)).toBe("42");
    expect(safeCount.decoder(42n)).toBe("42");

    const mixedCount = defaultCount.toMixed();
    expect(mixedCount.decoder("42")).toBe("42");
    expect(mixedCount.decoder(42)).toBe(42);
    expect(mixedCount.decoder(42n)).toBe("42");

    for (const invalidValue of ["not-a-number", -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, true, {}, null]) {
      expect(() => defaultCount.decoder(invalidValue)).toThrow("Failed to decode count() result");
      expect(() => mixedCount.decoder(invalidValue)).toThrow("Failed to decode count() result");
    }

    for (const invalidValue of ["01", "1.5", Number.MAX_SAFE_INTEGER + 1, true, {}, null]) {
      expect(() => safeCount.decoder(invalidValue)).toThrow("Failed to decode count() result");
    }

    expect(() => defaultCount.decoder(10n ** 400n)).toThrow("Failed to decode count() result");

    const countFailure = new Error("count failure");
    const countDb = createQueryClient({
      schema: { orders },
      runner: {
        async execute() {
          throw countFailure;
        },
        async *iterator() {},
        async command() {
          return undefined;
        },
      },
    });

    expect(await countDb.count(orders).catch(() => -1)).toBe(-1);

    let countFinallyCalls = 0;
    await expect(
      countDb.count(orders).finally(() => {
        countFinallyCalls += 1;
      }),
    ).rejects.toBe(countFailure);
    expect(countFinallyCalls).toBe(1);

    const selectFailure = new Error("select failure");
    const insertFailure = new Error("insert failure");
    const db = createQueryClient({
      schema: { orders },
      runner: {
        async execute() {
          throw selectFailure;
        },
        async *iterator() {},
        async command() {
          throw insertFailure;
        },
      },
    });

    expect(
      await db
        .select({
          id: orders.id,
        })
        .from(orders)
        .catch(() => [{ id: -1 }]),
    ).toEqual([{ id: -1 }]);

    let selectFinallyCalls = 0;
    await expect(
      db
        .select({
          id: orders.id,
        })
        .from(orders)
        .finally(() => {
          selectFinallyCalls += 1;
        }),
    ).rejects.toBe(selectFailure);
    expect(selectFinallyCalls).toBe(1);

    expect(
      await db
        .insert(orders)
        .values({
          id: 1,
          name: "broken",
          amount: 1.25,
        })
        .catch(() => "handled"),
    ).toBe("handled");

    let insertFinallyCalls = 0;
    await expect(
      db
        .insert(orders)
        .values({
          id: 1,
          name: "broken",
          amount: 1.25,
        })
        .finally(() => {
          insertFinallyCalls += 1;
        }),
    ).rejects.toBe(insertFailure);
    expect(insertFinallyCalls).toBe(1);
  });

  it("covers explicit then() calls and CTE sources", async function testThenAndCteSources() {
    const compiledStatements: string[] = [];
    const db = createQueryClient({
      schema: { orders },
      runner: {
        async execute<TResult extends Record<string, unknown>>(compiled: { statement: string }) {
          compiledStatements.push(compiled.statement);
          if (compiled.statement.includes("count()")) {
            return [{ value: 2 }] as TResult[];
          }
          return [{ id: 1 }] as TResult[];
        },
        async *iterator() {},
        async command() {
          return undefined;
        },
      },
    });

    expect(await db.count(orders).then((value) => value + 1)).toBe(3);
    expect(
      await db
        .select({
          id: orders.id,
        })
        .from(orders)
        .then((rows) => rows[0]?.id),
    ).toBe(1);
    expect(
      await db
        .insert(orders)
        .values({
          id: 1,
          name: "ok",
          amount: 1,
        })
        .then(() => "inserted"),
    ).toBe("inserted");

    const totals = db.$with("totals").as(
      db
        .select({
          id: orders.id,
        })
        .from(orders),
    );

    const cteQuery = buildCompiled(
      db
        .with(totals)
        .select({
          id: totals.id,
        })
        .from(totals)
        [compileQuerySymbol](),
    );

    expect(normalizeSql(cteQuery.query)).toContain("from `totals`");
    expect(compiledStatements.some((statement) => normalizeSql(statement).includes("select toFloat64(count())"))).toBe(
      true,
    );
  });

  it("covers case-insensitive like predicates", function testIlikePredicates() {
    const db = createQueryClient({
      schema: { orders },
    });

    const compiled = buildCompiled(
      db
        .select({
          id: orders.id,
        })
        .from(orders)
        .where(and(ilike(orders.name, "%AL%"), notIlike(orders.name, "%bot%")))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(compiled.query)).toContain("`orders`.`name` ilike {orm_param1:String}");
    expect(normalizeSql(compiled.query)).toContain("`orders`.`name` not ilike {orm_param2:String}");
    expect(compiled.params).toEqual({
      orm_param1: "%AL%",
      orm_param2: "%bot%",
    });
    expect(like(orders.name, "%AL%").decoder(1)).toBe(true);
    expect(notLike(orders.name, "%bot%").decoder(0)).toBe(false);
    expect(ilike(orders.name, "%AL%").decoder(1)).toBe(true);
    expect(notIlike(orders.name, "%bot%").decoder(0)).toBe(false);
  });

  it("covers literal-text pattern helpers", function testLiteralTextPatternHelpers() {
    const db = createQueryClient({
      schema: { orders },
    });

    const compiled = buildCompiled(
      db
        .select({
          id: orders.id,
        })
        .from(orders)
        .where(and(contains(orders.name, "50%"), startsWith(orders.name, "tag_"), endsWith(orders.name, "_done")))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(compiled.query)).toContain("`orders`.`name` like {orm_param1:String}");
    expect(normalizeSql(compiled.query)).toContain("`orders`.`name` like {orm_param2:String}");
    expect(normalizeSql(compiled.query)).toContain("`orders`.`name` like {orm_param3:String}");
    expect(compiled.params).toEqual({
      orm_param1: "%50\\%%",
      orm_param2: "tag\\_%",
      orm_param3: "%\\_done",
    });

    const caseInsensitive = buildCompiled(
      db
        .select({
          id: orders.id,
        })
        .from(orders)
        .where(containsIgnoreCase(orders.name, "AL%"))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(caseInsensitive.query)).toContain("`orders`.`name` ilike {orm_param1:String}");
    expect(caseInsensitive.params).toEqual({
      orm_param1: "%AL\\%%",
    });
  });
});

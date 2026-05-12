import { describe, expect, it } from "bun:test";
import { array, bool, date, date32, decimal, float64, int32, nested, nullable, string, tuple } from "./columns";
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
  isNotNull,
  isNull,
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

const typedEvents = ckTable(
  "typed_events",
  {
    id: int32(),
    businessDay: date(),
    localDay: date32("local_day"),
    optionalNote: nullable(string()),
    pair: tuple(string(), int32()),
    entries: nested({
      name: string(),
      score: int32(),
    }),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

const arrayEvents = ckTable(
  "array_events",
  {
    id: int32(),
    active: bool(),
    businessDays: array(date()),
    localDays: array(date32()),
    decimalValues: array(decimal({ precision: 10, scale: 2 })),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

describe("ck-orm query extras", function describeClickHouseORMQueryExtras() {
  it("covers builder errors, default selection, operators and table-function sources", function testBuilderBranches() {
    const db = createQueryClient();

    expect(() => db.select().buildSelectionItems()).toThrow(
      "select() without explicit selection requires from() first",
    );
    expect(() => db.select().from(tableFn.call("numbers", 5)).buildSelectionItems()).toThrow(
      "select() without explicit selection requires a source with known columns",
    );
    // Bare SelectBuilder (no `.as(name)`) as a source exposes no joinable
    // columns from outside — getSourceColumns returns undefined, so
    // implicit `select()` falls into the same error path.
    const bareSubquery = db.select({ id: orders.id }).from(orders);
    expect(() => db.select().from(bareSubquery).buildSelectionItems()).toThrow(
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
    expect(normalizeSql(built.query)).toContain("limit 10");
    expect(normalizeSql(built.query)).toContain("offset 5");
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
    const db = createQueryClient();
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
    expect(normalizeSql(built.query)).toContain("limit 10");
    expect(built.params).toEqual({
      orm_param1: 1,
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
    const db = createQueryClient();

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
    const db = createQueryClient();

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
    const db = createQueryClient();

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

    const encodedBuilt = buildCompiled(
      db
        .select({
          id: arrayEvents.id,
        })
        .from(arrayEvents)
        .where(
          has(arrayEvents.businessDays, new Date("2026-06-15T08:00:00.000Z")),
          hasAny(arrayEvents.localDays, [new Date("2026-06-16T01:00:00.000Z")]),
          hasAll(arrayEvents.decimalValues, ["10.50"]),
          hasSubstr(arrayEvents.businessDays, [new Date("2026-06-17T23:00:00.000Z")]),
        )
        [compileQuerySymbol](),
    );

    expect(normalizeSql(encodedBuilt.query)).toContain("has(`array_events`.`businessDays`, {orm_param1:Date})");
    expect(normalizeSql(encodedBuilt.query)).toContain(
      "hasAny(`array_events`.`localDays`, {orm_param2:Array(Date32)})",
    );
    expect(normalizeSql(encodedBuilt.query)).toContain(
      "hasAll(`array_events`.`decimalValues`, {orm_param3:Array(Decimal(10, 2))})",
    );
    expect(normalizeSql(encodedBuilt.query)).toContain(
      "hasSubstr(`array_events`.`businessDays`, {orm_param4:Array(Date)})",
    );
    expect(encodedBuilt.params).toEqual({
      orm_param1: "2026-06-15",
      orm_param2: ["2026-06-16"],
      orm_param3: ["10.50"],
      orm_param4: ["2026-06-17"],
    });
  });

  it("supports drizzle-style db.count() for direct execution and scalar subqueries", async function testDbCount() {
    const compiledStatements: Array<{
      statement: string;
      params: Record<string, unknown>;
    }> = [];
    const db = createQueryClient({
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
    const db = createQueryClient();

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
    expect(normalizeSql(limitedCompiled.query)).toContain("limit 5");
    expect(limitedCompiled.params).toEqual({
      orm_param1: 1,
    });
  });

  it("keeps derived query clients and queries from mutating their source", function testDerivedClientImmutability() {
    const db = createQueryClient();
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

  it("passes compiled select queries through the iterator runner", async function testSelectIteratorRunner() {
    const iteratorCalls: Array<{
      statement: string;
      params: Record<string, unknown>;
    }> = [];
    const db = createQueryClient({
      runner: {
        async execute() {
          return [];
        },
        async *iterator<TResult extends Record<string, unknown>>(compiled: {
          statement: string;
          params: Record<string, unknown>;
        }) {
          iteratorCalls.push({
            statement: compiled.statement,
            params: compiled.params,
          });
          yield { id: compiled.params.orm_param1 } as TResult;
        },
        async command() {
          return undefined;
        },
      },
    });

    const rows: Array<{ id: number }> = [];
    for await (const row of db
      .select({
        id: orders.id,
      })
      .from(orders)
      .where(eq(orders.id, 7))
      .iterator()) {
      rows.push(row);
    }

    expect(rows).toEqual([{ id: 7 }]);
    expect(iteratorCalls).toEqual([
      {
        statement: "select `orders`.`id` as `id` from `orders` where `orders`.`id` = {orm_param1:Int32}",
        params: {
          orm_param1: 7,
        },
      },
    ]);
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
          paramTypes: {},
          nextParamIndex: 0,
        });
      },
    };

    const existsPredicate = exists(nestedQuery as never);
    expect(() =>
      existsPredicate.compile({
        params: {},
        paramTypes: {},
        nextParamIndex: 0,
      }),
    ).toThrow("Missing active compile state while collecting forced settings");
  });

  it("covers join-only metadata, insert DEFAULT rendering and array-function raw expressions", function testMetadataDefaultsAndArrayArgs() {
    const db = createQueryClient();

    const joinOnlyCompiled = db
      .select({
        id: taggedOrders.id,
      })
      .leftJoin(taggedOrders, expr(sql.raw("1"), { sqlType: "Bool", decoder: (value) => Boolean(value) }))
      [compileWithContextSymbol]({
        params: {},
        paramTypes: {},
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

  it("uses column encoders for predicates and renders explicit nullable predicates", function testPredicateEncoders() {
    const db = createQueryClient();
    const compiled = db
      .select({
        id: typedEvents.id,
      })
      .from(typedEvents)
      .where(
        eq(typedEvents.businessDay, new Date("2026-06-15T08:00:00.000Z")),
        between(typedEvents.localDay, new Date("2026-06-16T01:00:00.000Z"), "2026-06-17" as never),
        inArray(typedEvents.businessDay, [new Date("2026-06-18T23:00:00.000Z"), "2026-06-19" as never]),
        isNull(typedEvents.optionalNote),
        isNotNull(typedEvents.optionalNote),
      )
      [compileQuerySymbol]();

    expect(normalizeSql(compiled.statement)).toContain("`typed_events`.`businessDay` = {orm_param1:Date}");
    expect(normalizeSql(compiled.statement)).toContain(
      "`typed_events`.`local_day` between {orm_param2:Date32} and {orm_param3:Date32}",
    );
    expect(normalizeSql(compiled.statement)).toContain(
      "`typed_events`.`businessDay` in ({orm_param4:Date}, {orm_param5:Date})",
    );
    expect(normalizeSql(compiled.statement)).toContain("`typed_events`.`optionalNote` is null");
    expect(normalizeSql(compiled.statement)).toContain("`typed_events`.`optionalNote` is not null");
    expect(isNull(typedEvents.optionalNote).decoder(1)).toBe(true);
    expect(isNotNull(typedEvents.optionalNote).decoder(0)).toBe(false);
    expect(compiled.params).toEqual({
      orm_param1: "2026-06-15",
      orm_param2: "2026-06-16",
      orm_param3: "2026-06-17",
      orm_param4: "2026-06-18",
      orm_param5: "2026-06-19",
    });

    expect(() => isNull(null)).toThrow("isNull() expects a SQL expression");
    expect(() => isNull(undefined)).toThrow("isNull() expects a SQL expression");
    expect(() => isNull(1)).toThrow("isNull() expects a SQL expression");
    expect(() => isNotNull(null)).toThrow("isNotNull() expects a SQL expression");
    expect(() => isNotNull(undefined)).toThrow("isNotNull() expects a SQL expression");
    expect(() => isNotNull("literal")).toThrow("isNotNull() expects a SQL expression");
    expect(() => eq(typedEvents.optionalNote, null)).toThrow("does not accept bare null");
    expect(() => ne(typedEvents.optionalNote, null)).toThrow("does not accept bare null");
    expect(() => gt(typedEvents.optionalNote, undefined)).toThrow("does not accept bare undefined");
    expect(() => gte(typedEvents.optionalNote, null)).toThrow("does not accept bare null");
    expect(() => lt(typedEvents.optionalNote, undefined)).toThrow("does not accept bare undefined");
    expect(() => lte(typedEvents.optionalNote, null)).toThrow("does not accept bare null");
    expect(() => between(typedEvents.optionalNote, null, "x")).toThrow("does not accept bare null");
    expect(() => between(typedEvents.optionalNote, "x", undefined)).toThrow("does not accept bare undefined");
  });

  it("rejects invalid predicate positions without rejecting boolean comparisons", function testPredicatePositionValidation() {
    const db = createQueryClient();
    const booleanComparison = buildCompiled(
      db
        .select({ id: arrayEvents.id })
        .from(arrayEvents)
        .where(eq(arrayEvents.active, false), arrayEvents.active, not(arrayEvents.active))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(booleanComparison.query)).toContain("`array_events`.`active` = {orm_param1:Bool}");
    expect(normalizeSql(booleanComparison.query)).toContain(
      "and `array_events`.`active` and not (`array_events`.`active`)",
    );
    expect(booleanComparison.params).toEqual({
      orm_param1: false,
    });

    const skipped = buildCompiled(
      db.select({ id: typedEvents.id }).from(typedEvents).where(undefined)[compileQuerySymbol](),
    );
    expect(normalizeSql(skipped.query)).not.toContain("where");

    expect(() =>
      db
        .select()
        .from(typedEvents)
        .where(null as never),
    ).toThrow("expects a SQL predicate or undefined");
    expect(() =>
      db
        .select()
        .from(typedEvents)
        .where(false as never),
    ).toThrow("use ck.eq(column, false)");
    expect(() =>
      db
        .select()
        .from(typedEvents)
        .where(true as never),
    ).toThrow("use ck.eq(column, true)");
    expect(() =>
      db
        .select()
        .from(typedEvents)
        .where(0 as never),
    ).toThrow("expects a SQL predicate or undefined");
    expect(() =>
      db
        .select()
        .from(typedEvents)
        .where("" as never),
    ).toThrow("expects a SQL predicate or undefined");
    expect(() =>
      db
        .select()
        .from(typedEvents)
        .where({} as never),
    ).toThrow("expects a SQL predicate or undefined");
    expect(() =>
      db
        .select()
        .from(typedEvents)
        .having(null as never),
    ).toThrow("expects a SQL predicate or undefined");
    expect(() => and(eq(typedEvents.id, 1), false as never)).toThrow("use ck.eq(column, false)");
    expect(() => or(eq(typedEvents.id, 1), 0 as never)).toThrow("expects a SQL predicate or undefined");

    const rawPredicate = buildCompiled(
      db
        .select({ id: typedEvents.id })
        .from(typedEvents)
        .where(sql.raw("1 = 1") as never)
        [compileQuerySymbol](),
    );
    expect(normalizeSql(rawPredicate.query)).toContain("where 1 = 1");
    expect((and(sql.raw("1 = 1") as never) as unknown as { decoder(value: unknown): boolean }).decoder(0)).toBe(false);
  });

  it("validates limit, offset and limitBy primitive values on the client", function testLimitValidation() {
    const db = createQueryClient();
    const limited = buildCompiled(
      db
        .select({ id: typedEvents.id })
        .from(typedEvents)
        .limit(0)
        .offset(5n)
        .limitBy([typedEvents.id], sql.raw("2"))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(limited.query)).toContain("limit 2 by `typed_events`.`id`");
    expect(normalizeSql(limited.query)).toContain("limit 0");
    expect(normalizeSql(limited.query)).toContain("offset 5");
    expect(limited.params).toEqual({});

    expect(() =>
      db
        .select()
        .from(typedEvents)
        .limit(null as never)
        [compileQuerySymbol](),
    ).toThrow("expects a non-negative safe integer");
    expect(() =>
      db
        .select()
        .from(typedEvents)
        .limit(undefined as never)
        [compileQuerySymbol](),
    ).toThrow("expects a non-negative safe integer");
    expect(() => db.select().from(typedEvents).limit(1.5)[compileQuerySymbol]()).toThrow(
      "expects a non-negative safe integer",
    );
    expect(() => db.select().from(typedEvents).offset(-1)[compileQuerySymbol]()).toThrow(
      "expects a non-negative safe integer",
    );
    expect(() => db.select().from(typedEvents).limit(Number.NaN)[compileQuerySymbol]()).toThrow(
      "expects a non-negative safe integer",
    );
    expect(() =>
      db
        .select()
        .from(typedEvents)
        .limitBy([typedEvents.id], "1" as never)
        [compileQuerySymbol](),
    ).toThrow("expects a non-negative safe integer");
    expect(() =>
      db
        .select()
        .from(typedEvents)
        .limit(typedEvents.id as never)
        [compileQuerySymbol](),
    ).toThrow("expects a non-negative safe integer or SQL fragment");
    expect(() =>
      db
        .select()
        .from(typedEvents)
        .limitBy([typedEvents.id], expr(sql.raw("2")) as never)
        [compileQuerySymbol](),
    ).toThrow("expects a non-negative safe integer or SQL fragment");
  });

  it("rejects nullish predicate values across collection and string helpers", function testNullishPredicateValues() {
    expect(() => inArray(orders.id, [1, null])).toThrow("does not accept bare null");
    expect(() => inArray(orders.id, [1, undefined])).toThrow("does not accept bare undefined");
    expect(() => notInArray(orders.id, [null])).toThrow("does not accept bare null");
    expect(() => inArray(orders.id, undefined as never)).toThrow("does not accept bare undefined");
    expect(() => like(orders.name, null as never)).toThrow("does not accept bare null");
    expect(() => notLike(orders.name, undefined as never)).toThrow("does not accept bare undefined");
    expect(() => like(orders.name, 1 as never)).toThrow("expects a string predicate value or SQL expression");
    expect(() => contains(orders.name, null as never)).toThrow("does not accept bare null");
    expect(() => contains(orders.name, 1 as never)).toThrow("expects a string predicate value");
    expect(() => startsWith(orders.name, undefined as never)).toThrow("does not accept bare undefined");
    expect(() => endsWith(orders.name, null as never)).toThrow("does not accept bare null");
    expect(() => containsIgnoreCase(orders.name, undefined as never)).toThrow("does not accept bare undefined");
    expect(() => has(taggedOrders.tags, null)).toThrow("does not accept bare null");
    expect(() => hasAll(taggedOrders.tags, ["vip", null])).toThrow("does not accept bare null");
    expect(() => hasAny(taggedOrders.tags, ["vip", undefined])).toThrow("does not accept bare undefined");
    expect(() => hasSubstr(taggedOrders.tags, undefined)).toThrow("does not accept bare undefined");

    const db = createQueryClient();
    const computedTags = expr<string[]>(sql.raw("['vip','pro']"), {
      decoder: (value) => value as string[],
      sqlType: "Array(String)",
    });
    const rawNullPredicates = buildCompiled(
      db
        .select({ id: taggedOrders.id })
        .from(taggedOrders)
        .where(
          isNull(sql.raw("NULL")),
          like(taggedOrders.tags, sql.raw("NULL")),
          has(taggedOrders.tags, ["vip"]),
          has(taggedOrders.tags, sql.raw("NULL")),
          hasAny(taggedOrders.tags, sql.raw("[NULL]")),
          hasAny(computedTags, ["vip"]),
        )
        [compileQuerySymbol](),
    );

    expect(normalizeSql(rawNullPredicates.query)).toContain("NULL is null");
    expect(normalizeSql(rawNullPredicates.query)).toContain("`tagged_orders`.`tags` like NULL");
    expect(normalizeSql(rawNullPredicates.query)).toContain("has(`tagged_orders`.`tags`, {orm_param1:Array(String)})");
    expect(rawNullPredicates.params).toMatchObject({
      orm_param1: ["vip"],
      orm_param2: ["vip"],
    });
    expect(normalizeSql(rawNullPredicates.query)).toContain("has(`tagged_orders`.`tags`, NULL)");
    expect(normalizeSql(rawNullPredicates.query)).toContain("hasAny(`tagged_orders`.`tags`, [NULL])");
    expect(normalizeSql(rawNullPredicates.query)).toContain("hasAny(['vip','pro'], {orm_param2:Array(String)})");
  });

  it("compiles tuple param types, insert NULL/DEFAULT, and Nested subcolumn values", function testTupleAndNestedInsert() {
    const db = createQueryClient();
    const compiled = db
      .insert(typedEvents)
      .values({
        id: 1,
        businessDay: new Date("2026-06-15T08:00:00.000Z"),
        optionalNote: null,
        pair: ["login", 42],
        entries: [
          { name: "first", score: 10 },
          { name: "second", score: 20 },
        ],
      })
      [compileQuerySymbol]();

    expect(normalizeSql(compiled.statement)).toContain(
      "insert into `typed_events` (`id`, `businessDay`, `local_day`, `optionalNote`, `pair`, `entries`.`name`, `entries`.`score`)",
    );
    expect(normalizeSql(compiled.statement)).toContain(
      "values ({orm_param1:Int32}, {orm_param2:Date}, DEFAULT, NULL, {orm_param3:Tuple(String, Int32)}, {orm_param4:Array(String)}, {orm_param5:Array(Int32)})",
    );
    expect(compiled.params).toEqual({
      orm_param1: 1,
      orm_param2: "2026-06-15",
      orm_param3: ["login", 42],
      orm_param4: ["first", "second"],
      orm_param5: [10, 20],
    });
    expect(compiled.paramTypes).toEqual({
      orm_param1: "Int32",
      orm_param2: "Date",
      orm_param3: "Tuple(String, Int32)",
      orm_param4: "Array(String)",
      orm_param5: "Array(Int32)",
    });

    const defaultNested = db
      .insert(typedEvents)
      .values({
        id: 2,
      })
      [compileQuerySymbol]();
    expect(normalizeSql(defaultNested.statement)).toContain(
      "values ({orm_param1:Int32}, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT)",
    );

    expect(() =>
      db
        .insert(typedEvents)
        .values({
          id: 3,
          entries: "bad" as never,
        })
        [compileQuerySymbol](),
    ).toThrow('Nested column "entries" expects an array of objects');
    expect(() =>
      db
        .insert(typedEvents)
        .values({
          id: 4,
          entries: [1 as never],
        })
        [compileQuerySymbol](),
    ).toThrow('Nested column "entries" item 1 must be an object');
    expect(() =>
      db
        .insert(typedEvents)
        .values({
          id: 2,
          entries: [{ name: "missing-score" } as never],
        })
        [compileQuerySymbol](),
    ).toThrow('Nested column "entries" item 1 is missing required field "score"');
  });

  it("rejects duplicate SQL aliases in explicit selections", function testDuplicateSelectionAliases() {
    const db = createQueryClient();

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
      "insert() requires .values(rows) or .fromSelect(selectBuilder) before execute()",
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
    const decodeDb = createQueryClient();

    const defaultCount = decodeDb.count(orders);
    expect(defaultCount.decoder(42)).toBe(42);
    expect(defaultCount.decoder("42")).toBe(42);
    expect(defaultCount.decoder(42n)).toBe(42);

    const unsafeCount = defaultCount.toUnsafe();
    expect(unsafeCount.decoder("42")).toBe(42);

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
    const db = createQueryClient();

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
    const db = createQueryClient();

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

describe("ck-orm bare SelectBuilder as subquery source", function describeBareBuilderSource() {
  it("accepts a bare builder in from() and assigns a per-compile auto alias", function testBareFrom() {
    const db = createQueryClient();
    const inner = db.select({ id: orders.id }).from(orders);

    // PR-A: bare-builder columns are not yet projectable from outside, so we
    // project an independent literal expression. (PR-B will let users access
    // `inner.id` on the bare builder directly.)
    const built = buildCompiled(
      db
        .select({ flag: expr(sql.raw("1")) })
        .from(inner)
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("from (select `orders`.`id` as `id` from `orders`) as `__sub_1`");
  });

  it("accepts a bare builder in inArray() and notInArray()", function testBareInArray() {
    const db = createQueryClient();
    const innerIds = db.select({ id: orders.id }).from(orders);

    const positive = buildCompiled(
      db.select({ id: orders.id }).from(orders).where(inArray(orders.id, innerIds))[compileQuerySymbol](),
    );
    expect(normalizeSql(positive.query)).toContain("`orders`.`id` in (select `orders`.`id` as `id` from `orders`)");

    const negative = buildCompiled(
      db
        .select({ id: orders.id })
        .from(orders)
        .where(notInArray(orders.id, db.select({ id: orders.id }).from(orders)))
        [compileQuerySymbol](),
    );
    expect(normalizeSql(negative.query)).toContain("`orders`.`id` not in (select `orders`.`id` as `id` from `orders`)");
  });

  it("gives separate bare builders separate auto aliases per compile", function testBareJoinDistinctAliases() {
    const db = createQueryClient();
    const subA = db.select({ id: orders.id }).from(orders);
    const subB = db.select({ id: orders.id }).from(orders);

    const built = buildCompiled(
      db.select({ id: orders.id }).from(subA).innerJoin(subB, eq(orders.id, orders.id))[compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("from (select `orders`.`id` as `id` from `orders`) as `__sub_1`");
    expect(normalizeSql(built.query)).toContain("inner join (select `orders`.`id` as `id` from `orders`) as `__sub_2`");
  });

  it("resets the auto-alias counter on each top-level compile", function testCounterStability() {
    const db = createQueryClient();
    const inner = db.select({ id: orders.id }).from(orders);
    const query = db.select({ flag: expr(sql.raw("1")) }).from(inner);

    const first = buildCompiled(query[compileQuerySymbol]());
    const second = buildCompiled(query[compileQuerySymbol]());

    expect(first.query).toBe(second.query);
    expect(normalizeSql(first.query)).toContain("as `__sub_1`");
  });

  it("rejects reusing the same bare builder instance twice as a source", function testBareDoubleUse() {
    const db = createQueryClient();
    const sub = db.select({ id: orders.id }).from(orders);

    expect(() =>
      db.select({ id: orders.id }).from(sub).innerJoin(sub, eq(orders.id, orders.id))[compileQuerySymbol](),
    ).toThrow(/SelectBuilder instance used twice/);
  });

  it("allows reusing the same bare builder across separate top-level compiles", function testBareCrossCompile() {
    const db = createQueryClient();
    const sub = db.select({ id: orders.id }).from(orders);

    const first = buildCompiled(
      db
        .select({ flag: expr(sql.raw("1")) })
        .from(sub)
        [compileQuerySymbol](),
    );
    const second = buildCompiled(
      db
        .select({ flag: expr(sql.raw("1")) })
        .from(sub)
        [compileQuerySymbol](),
    );

    expect(normalizeSql(first.query)).toContain("as `__sub_1`");
    expect(normalizeSql(second.query)).toContain("as `__sub_1`");
  });

  it("allows the same bare builder once as a source and once inside an inArray subquery", function testBareSourceAndInArray() {
    const db = createQueryClient();
    const sub = db.select({ id: orders.id }).from(orders);

    const built = buildCompiled(
      db.select({ id: orders.id }).from(sub).where(inArray(orders.id, sub))[compileQuerySymbol](),
    );

    // Both occurrences share the same auto-alias for the duration of one
    // compile, because they correspond to the same logical subquery instance.
    expect(normalizeSql(built.query)).toContain("from (select `orders`.`id` as `id` from `orders`) as `__sub_1`");
    expect(normalizeSql(built.query)).toContain("where `orders`.`id` in (select `orders`.`id` as `id` from `orders`)");
  });

  it("exposes column refs on a variable-bound bare builder", function testBareBuilderColumnRefs() {
    const db = createQueryClient();
    const sub = db
      .select({
        owner_id: orders.id,
        total: fn.sum(orders.amount).as("total_amount"),
      })
      .from(orders);

    // sub.owner_id and sub.total are auto column refs on the bare builder.
    const built = buildCompiled(db.select({ ownerId: sub.owner_id, total: sub.total }).from(sub)[compileQuerySymbol]());

    expect(normalizeSql(built.query)).toContain(
      "select `__sub_1`.`owner_id` as `ownerId`, `__sub_1`.`total_amount` as `total`",
    );
    expect(normalizeSql(built.query)).toContain(
      "from (select `orders`.`id` as `owner_id`, sum(`orders`.`amount`) as `total_amount` from `orders`) as `__sub_1`",
    );
  });

  it("preserves column refs across .where() / .orderBy() / .limit() chain steps", function testBareColumnRefsAfterChain() {
    const db = createQueryClient();
    const sub = db
      .select({ id: orders.id, amount: orders.amount })
      .from(orders)
      .where(eq(orders.id, 1))
      .orderBy(desc(orders.amount))
      .limit(5);

    // sub.id / sub.amount must still be reachable after the chain
    expect(sub.id).toBeDefined();
    expect(sub.amount).toBeDefined();

    const built = buildCompiled(
      db.select({ outerId: sub.id, outerAmount: sub.amount }).from(sub)[compileQuerySymbol](),
    );
    expect(normalizeSql(built.query)).toContain(
      "select `__sub_1`.`id` as `outerId`, `__sub_1`.`amount` as `outerAmount`",
    );
  });

  it("can use bare builder column refs in join conditions", function testBareJoinCondition() {
    const db = createQueryClient();
    const sub = db.select({ user_id: orders.id, total: fn.sum(orders.amount).as("total") }).from(orders);

    const built = buildCompiled(
      db
        .select({ id: orders.id, total: sub.total })
        .from(orders)
        .innerJoin(sub, eq(orders.id, sub.user_id))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain(
      "inner join (select `orders`.`id` as `user_id`, sum(`orders`.`amount`) as `total` from `orders`) as `__sub_1`",
    );
    expect(normalizeSql(built.query)).toContain("on `orders`.`id` = `__sub_1`.`user_id`");
  });

  it("does not expose column refs through Object.keys / JSON.stringify", function testNonEnumerableRefs() {
    const db = createQueryClient();
    const sub = db.select({ id: orders.id, amount: orders.amount }).from(orders);

    // Auto column refs are intentionally non-enumerable so they don't appear
    // in iteration / serialization.
    expect(Object.keys(sub)).not.toContain("id");
    expect(Object.keys(sub)).not.toContain("amount");
    expect(JSON.stringify(sub)).not.toContain("amount");

    // But they're still accessible via direct property access.
    expect(sub.id).toBeDefined();
    expect(sub.amount).toBeDefined();
  });

  it("keeps SelectBuilder methods when a selection key collides with a method name", function testBuilderMethodPriority() {
    // The selection is cast to `Record<string, unknown>` so TS doesn't try to
    // infer a narrow shape from the literal — the column-refs intersection
    // also gets erased to `unknown` for that cast path (its `string extends
    // keyof TResult` guard kicks in). What we're testing is the runtime
    // behaviour: the conflicting key is skipped during column-ref attachment,
    // so `sub.from` keeps returning the builder method.
    const db = createQueryClient();
    const sub = db
      .select({
        id: orders.id,
        from: orders.name,
      } as Record<string, unknown>)
      .from(orders);

    expect(typeof sub.from).toBe("function");
    // `id` is non-conflicting and remains accessible.
    expect((sub as unknown as Record<string, unknown>).id).toBeDefined();
  });

  it("supports callback-style innerJoin/leftJoin for inline bare builders", function testJoinCallback() {
    const db = createQueryClient();

    const innerBuilt = buildCompiled(
      db
        .select({ id: orders.id })
        .from(orders)
        .innerJoin(
          db.select({ ref_id: orders.id, ref_total: fn.sum(orders.amount).as("ref_total") }).from(orders),
          (joined) => eq(orders.id, joined.ref_id),
        )
        [compileQuerySymbol](),
    );

    expect(normalizeSql(innerBuilt.query)).toContain(
      "inner join (select `orders`.`id` as `ref_id`, sum(`orders`.`amount`) as `ref_total` from `orders`) as `__sub_1`",
    );
    expect(normalizeSql(innerBuilt.query)).toContain("on `orders`.`id` = `__sub_1`.`ref_id`");

    const leftBuilt = buildCompiled(
      db
        .select({ id: orders.id })
        .from(orders)
        .leftJoin(db.select({ ref_id: orders.id }).from(orders), (joined) => eq(orders.id, joined.ref_id))
        [compileQuerySymbol](),
    );
    expect(normalizeSql(leftBuilt.query)).toContain(
      "left join (select `orders`.`id` as `ref_id` from `orders`) as `__sub_1`",
    );
    expect(normalizeSql(leftBuilt.query)).toContain("on `orders`.`id` = `__sub_1`.`ref_id`");
  });

  it("supports callback-style join on regular table sources too", function testCallbackJoinOnTable() {
    const db = createQueryClient();
    const built = buildCompiled(
      db
        .select({ id: orders.id })
        .from(orders)
        .innerJoin(taggedOrders, (joined) => eq(orders.id, joined.id))
        [compileQuerySymbol](),
    );
    expect(normalizeSql(built.query)).toContain("inner join `tagged_orders`");
    expect(normalizeSql(built.query)).toContain("on `orders`.`id` = `tagged_orders`.`id`");
  });

  it("supports callback-style join on named .as(...) subqueries", function testCallbackJoinOnNamedSubquery() {
    const db = createQueryClient();
    const named = db
      .select({ ref_id: orders.id, ref_total: fn.sum(orders.amount).as("ref_total") })
      .from(orders)
      .as("named_ref");

    const built = buildCompiled(
      db
        .select({ id: orders.id, total: named.ref_total })
        .from(orders)
        .innerJoin(named, (joined) => eq(orders.id, joined.ref_id))
        [compileQuerySymbol](),
    );

    // Named alias is preserved (no fallback to `__sub_N`).
    expect(normalizeSql(built.query)).toContain(
      "inner join (select `orders`.`id` as `ref_id`, sum(`orders`.`amount`) as `ref_total` from `orders`) as `named_ref`",
    );
    expect(normalizeSql(built.query)).toContain("on `orders`.`id` = `named_ref`.`ref_id`");
    expect(normalizeSql(built.query)).not.toContain("__sub_");
  });

  it("callback receives the same builder reference whose column refs match the join source alias", function testCallbackJoinedRefEquality() {
    const db = createQueryClient();
    const sub = db.select({ ref_id: orders.id }).from(orders);

    let capturedJoined: typeof sub | undefined;
    buildCompiled(
      db
        .select({ id: orders.id })
        .from(orders)
        .innerJoin(sub, (joined) => {
          capturedJoined = joined;
          return eq(orders.id, joined.ref_id);
        })
        [compileQuerySymbol](),
    );

    // Callback parameter must be the same builder instance, so its column
    // refs and the SQL `as __sub_N` alias share the same per-compile entry.
    expect(capturedJoined).toBe(sub);
  });

  it("mixes named .as() subqueries and bare builders without alias collisions", function testMixedNamedAndBare() {
    const db = createQueryClient();
    const named = db.select({ id: orders.id }).from(orders).as("named_sub");
    const bare = db.select({ id: orders.id }).from(orders);

    const built = buildCompiled(
      db.select({ id: orders.id }).from(named).innerJoin(bare, eq(orders.id, named.id))[compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("as `named_sub`");
    expect(normalizeSql(built.query)).toContain("as `__sub_1`");
  });

  it("compiles insert.fromSelect into INSERT INTO ... SELECT", function testInsertFromSelectBasicSql() {
    const db = createQueryClient();
    const compiled = buildCompiled(
      db
        .insert(orders)
        .fromSelect(db.select({ id: orders.id, name: orders.name, amount: orders.amount }).from(orders))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(compiled.query)).toBe(
      "insert into `orders` (`id`, `name`, `amount`) select `orders`.`id` as `id`, `orders`.`name` as `name`, `orders`.`amount` as `amount` from `orders`",
    );
    expect(compiled.params).toEqual({});
  });

  it("aligns insert column list with the select projection key order, not table column order", function testInsertFromSelectKeyOrder() {
    const db = createQueryClient();
    const compiled = buildCompiled(
      db
        .insert(orders)
        .fromSelect(db.select({ amount: orders.amount, id: orders.id, name: orders.name }).from(orders))
        [compileQuerySymbol](),
    );

    // Projection-key order is `amount, id, name`; insert column list mirrors
    // that so ClickHouse's position-based alignment lands each projection in
    // the right column even though the table-declared order is `id, name, amount`.
    expect(normalizeSql(compiled.query)).toContain("insert into `orders` (`amount`, `id`, `name`)");
    expect(normalizeSql(compiled.query)).toContain(
      "select `orders`.`amount` as `amount`, `orders`.`id` as `id`, `orders`.`name` as `name` from `orders`",
    );
  });

  it("flows WHERE parameters from the nested select into the outer compile context", function testInsertFromSelectParamFlow() {
    const db = createQueryClient();
    const compiled = buildCompiled(
      db
        .insert(orders)
        .fromSelect(
          db
            .select({ id: orders.id, name: orders.name, amount: orders.amount })
            .from(orders)
            .where(gt(orders.amount, 100)),
        )
        [compileQuerySymbol](),
    );

    expect(normalizeSql(compiled.query)).toContain("where `orders`.`amount` > {orm_param1:Float64}");
    expect(compiled.params).toEqual({ orm_param1: 100 });
  });

  it("renders a WITH-CTE in the nested select between the INSERT clause and the SELECT", function testInsertFromSelectWithCte() {
    const db = createQueryClient();
    const totals = db.$with("totals").as(db.select({ id: orders.id }).from(orders));

    const compiled = buildCompiled(
      db
        .insert(orders)
        .fromSelect(
          db
            .with(totals)
            .select({ id: orders.id, name: orders.name, amount: orders.amount })
            .from(orders)
            .innerJoin(totals, eq(orders.id, totals.id)),
        )
        [compileQuerySymbol](),
    );

    expect(normalizeSql(compiled.query)).toContain("insert into `orders` (`id`, `name`, `amount`) with `totals` as");
    expect(normalizeSql(compiled.query)).toContain("inner join `totals`");
  });

  it("rejects insert.fromSelect when required columns are missing from the projection", function testInsertFromSelectMissingRequired() {
    const db = createQueryClient();
    expect(() =>
      createInsertBuilder(orders)
        .fromSelect(
          // Cast to `never` so the runtime branch is exercised even though
          // the FromSelectShapeConstraint type would already reject this.
          db.select({ id: orders.id, name: orders.name }).from(orders) as never,
        )
        [compileQuerySymbol](),
    ).toThrow("insert().fromSelect() select is missing required columns: amount");
  });

  it("rejects insert.fromSelect when the projection includes a column the target table does not have", function testInsertFromSelectUnknownColumn() {
    const db = createQueryClient();
    expect(() =>
      createInsertBuilder(orders)
        .fromSelect(
          db
            .select({
              id: orders.id,
              name: orders.name,
              amount: orders.amount,
              extra_ghost: orders.id,
            })
            .from(orders) as never,
        )
        [compileQuerySymbol](),
    ).toThrow('insert().fromSelect() projects unknown column "extra_ghost"');
  });

  it("rejects insert.fromSelect when targeting a generated column", function testInsertFromSelectGenerated() {
    const generatedOrders = ckTable(
      "fs_generated_orders",
      {
        id: int32(),
        name: string(),
        amount: float64(),
        shardDay: int32("shard_day").materialized(sql`toYYYYMM(id)`),
      },
      (table) => ({
        engine: "MergeTree",
        orderBy: [table.id],
      }),
    );
    const db = createQueryClient();
    expect(() =>
      createInsertBuilder(generatedOrders)
        .fromSelect(
          db
            .select({
              id: orders.id,
              name: orders.name,
              amount: orders.amount,
              shardDay: orders.id,
            })
            .from(orders) as never,
        )
        [compileQuerySymbol](),
    ).toThrow('insert().fromSelect() cannot target generated column "shardDay"');
  });

  it("rejects insert.fromSelect that follows .values() and vice versa", function testInsertBuilderMutualExclusion() {
    const db = createQueryClient();
    const sourceSelect = db.select({ id: orders.id, name: orders.name, amount: orders.amount }).from(orders);

    const valuesFirst = createInsertBuilder(orders).values({
      id: 1,
      name: "alice",
      amount: 1,
    });
    expect(() =>
      (valuesFirst as unknown as { fromSelect(q: never): unknown }).fromSelect(sourceSelect as never),
    ).toThrow("insert().fromSelect() cannot follow insert().values()");

    const fromSelectFirst = createInsertBuilder(orders).fromSelect(sourceSelect as never);
    expect(() =>
      (
        fromSelectFirst as unknown as {
          values(rows: { id: number; name: string; amount: number }): unknown;
        }
      ).values({ id: 1, name: "alice", amount: 1 }),
    ).toThrow("insert().values() cannot follow insert().fromSelect()");

    expect(() =>
      (fromSelectFirst as unknown as { fromSelect(q: never): unknown }).fromSelect(sourceSelect as never),
    ).toThrow("insert().fromSelect() cannot be called twice");
  });

  it("rejects insert.fromSelect when handed something other than a SelectBuilder", function testInsertFromSelectNonBuilder() {
    expect(() =>
      createInsertBuilder(orders).fromSelect({
        // shape-faking object missing the `[selectBuilderKindSymbol]` brand
        execute: async () => [],
        buildSelectionItems: () => [],
      } as never),
    ).toThrow("insert().fromSelect() expects a SelectBuilder");
  });

  it("rejects insert.fromSelect on a select that projects no columns", function testInsertFromSelectEmptyProjection() {
    const emptyOrders = ckTable("fs_empty_orders", {
      id: int32(),
    });
    const _db = createQueryClient();
    const emptyBuilder = createSelectBuilder<Record<string, unknown>>({
      selection: {},
    }).from(emptyOrders);
    expect(() =>
      createInsertBuilder(emptyOrders)
        .fromSelect(emptyBuilder as never)
        [compileQuerySymbol](),
    ).toThrow("insert().fromSelect() requires the select query to project at least one column");
  });

  it("rejects insert without .values() or .fromSelect()", function testInsertEmptyExecute() {
    expect(() => createInsertBuilder(orders)[compileQuerySymbol]()).toThrow(
      "insert() requires .values(rows) or .fromSelect(selectBuilder) before execute()",
    );
  });

  it("renders configured physical column names in both the INSERT list and SELECT aliases", function testInsertFromSelectPhysicalNameMapping() {
    const remappedTarget = ckTable("fs_remap_target", {
      dealTicket: int32("deal_ticket"),
      userId: int32("user_id"),
      label: string(),
    });
    const remappedSource = ckTable("fs_remap_source", {
      dealTicket: int32("deal_ticket"),
      userId: int32("user_id"),
      label: string(),
    });
    const db = createQueryClient();

    const compiled = buildCompiled(
      db
        .insert(remappedTarget)
        .fromSelect(
          db
            .select({
              userId: remappedSource.userId,
              dealTicket: remappedSource.dealTicket,
              label: remappedSource.label,
            })
            .from(remappedSource),
        )
        [compileQuerySymbol](),
    );

    // INSERT column list must use the physical names (`deal_ticket`,
    // `user_id`) — ClickHouse stores the columns under those names and the
    // ORM has to address them directly. SELECT aliases, on the other hand,
    // are read by no one (CH aligns INSERT cols ↔ SELECT projection by
    // position, not by name), so they default to the JS key. The
    // ck-orm-side guarantee that makes "by name" alignment safe is that the
    // INSERT list and SELECT projection share the same projection-key order
    // — `userId` first, `dealTicket` second, `label` third — not that the
    // two lists share textual aliases.
    expect(normalizeSql(compiled.query)).toContain("insert into `fs_remap_target` (`user_id`, `deal_ticket`, `label`)");
    expect(normalizeSql(compiled.query)).toContain(
      "select `fs_remap_source`.`user_id` as `userId`, `fs_remap_source`.`deal_ticket` as `dealTicket`, `fs_remap_source`.`label` as `label` from `fs_remap_source`",
    );
  });

  it("forwards paramTypes from the nested SELECT into the compiled insert.fromSelect output", function testInsertFromSelectParamTypes() {
    const db = createQueryClient();
    const compiled = db
      .insert(orders)
      .fromSelect(
        db
          .select({ id: orders.id, name: orders.name, amount: orders.amount })
          .from(orders)
          .where(inArray(orders.id, [10, 20, 30]), gte(orders.amount, 100)),
      )
      [compileQuerySymbol]();

    // `inArray(col, [a,b,c])` expands into one parameter per element so the
    // server can plan it like a literal IN; the trailing `gte` adds one more.
    // The point of the assertion is that both the params *and* paramTypes
    // for every position in the nested SELECT survive the outer INSERT wrap.
    expect(compiled.params).toEqual({
      orm_param1: 10,
      orm_param2: 20,
      orm_param3: 30,
      orm_param4: 100,
    });
    expect(compiled.paramTypes).toEqual({
      orm_param1: "Int32",
      orm_param2: "Int32",
      orm_param3: "Int32",
      orm_param4: "Float64",
    });
  });

  it("bubbles join_use_nulls=1 forcedSettings out of a leftJoin-bearing fromSelect", function testInsertFromSelectForcedSettings() {
    const db = createQueryClient();
    const compiled = db
      .insert(orders)
      .fromSelect(
        db
          .select({ id: orders.id, name: orders.name, amount: orders.amount })
          .from(orders)
          .leftJoin(taggedOrders, eq(orders.id, taggedOrders.id)),
      )
      [compileQuerySymbol]();

    expect(compiled.forcedSettings).toEqual({ join_use_nulls: 1 });
  });

  it("preserves builder immutability — fromSelect returns a new builder without consuming the base", function testInsertBuilderImmutability() {
    const base = createInsertBuilder(orders);
    const sourceSelect = createSelectBuilder<{ id: number; name: string; amount: number }>({
      selection: { id: orders.id, name: orders.name, amount: orders.amount },
    }).from(orders);

    // After `fromSelect()` the returned builder has the from_select state…
    const fromSelectBuilder = base.fromSelect(sourceSelect as never);
    expect(() => (fromSelectBuilder as { [compileQuerySymbol](): unknown })[compileQuerySymbol]()).not.toThrow();

    // …but the original `base` builder must remain in `empty` state, so
    // compiling it still throws the "no values or fromSelect" error.
    expect(() => base[compileQuerySymbol]()).toThrow(
      "insert() requires .values(rows) or .fromSelect(selectBuilder) before execute()",
    );
  });

  it("preserves builder immutability for .values() — appending rows on the returned builder doesn't touch the base", function testInsertValuesBuilderImmutability() {
    const base = createInsertBuilder(orders);
    const oneRow = base.values({ id: 1, name: "alice", amount: 1 });
    const compiledOne = buildCompiled(oneRow[compileQuerySymbol]());
    expect(compiledOne.query).toContain("values ({orm_param1:Int32}, {orm_param2:String}, {orm_param3:Float64})");

    // .values() on the original `base` should not see the row inserted into
    // `oneRow`'s state — it must compile a fresh 1-row VALUES with the new
    // payload only.
    const otherRow = base.values({ id: 2, name: "bob", amount: 2 });
    const compiledOther = buildCompiled(otherRow[compileQuerySymbol]());
    expect(compiledOther.params).toEqual({
      orm_param1: 2,
      orm_param2: "bob",
      orm_param3: 2,
    });
  });

  it("nests a CTE chain inside insert.fromSelect — multiple $with stages survive the wrap", function testInsertFromSelectMultiStageCte() {
    const db = createQueryClient();
    const a = db.$with("stage_a").as(db.select({ id: orders.id }).from(orders));
    const b = db.$with("stage_b").as(db.select({ id: orders.id, doubled: fn.multiply(orders.amount, 2) }).from(orders));

    const compiled = buildCompiled(
      db
        .insert(orders)
        .fromSelect(
          db
            .with(a, b)
            .select({
              id: a.id,
              name: orders.name,
              amount: b.doubled,
            })
            .from(a)
            .innerJoin(orders, eq(a.id, orders.id))
            .innerJoin(b, eq(a.id, b.id)),
        )
        [compileQuerySymbol](),
    );

    expect(normalizeSql(compiled.query)).toContain("insert into `orders` (`id`, `name`, `amount`) with `stage_a` as");
    expect(normalizeSql(compiled.query)).toContain(", `stage_b` as");
    expect(normalizeSql(compiled.query)).toContain("inner join `orders` on");
    expect(normalizeSql(compiled.query)).toContain("inner join `stage_b`");
  });

  it("accepts a fromSelect whose source is a labelled subquery built via .as('alias')", function testInsertFromSelectSubqueryAsSource() {
    const db = createQueryClient();
    const sub = db
      .select({ id: orders.id, name: orders.name, amount: orders.amount })
      .from(orders)
      .where(gt(orders.amount, 10))
      .as("filtered_orders");

    const compiled = buildCompiled(
      db
        .insert(orders)
        .fromSelect(
          db
            .select({
              id: sub.id,
              name: sub.name,
              amount: sub.amount,
            })
            .from(sub),
        )
        [compileQuerySymbol](),
    );

    expect(normalizeSql(compiled.query)).toContain("insert into `orders` (`id`, `name`, `amount`)");
    expect(normalizeSql(compiled.query)).toContain("from (select");
    expect(normalizeSql(compiled.query)).toContain(") as `filtered_orders`");
    expect(normalizeSql(compiled.query)).toContain("where `orders`.`amount` > {orm_param1:Float64}");
  });

  it("rejects fromSelect when the target table is an alias", function testInsertFromSelectAliasTarget() {
    expect(() => createInsertBuilder(ckAlias(orders, "o"))).toThrow(
      "insert() requires a base table and does not accept aliased table targets",
    );
  });

  it("rejects fromSelect projecting a non-column-ref expression into a nested target column", function testInsertFromSelectNestedExpressionRejected() {
    const nestedTarget = ckTable("fs_nested_expr", {
      id: int32(),
      events: nested({
        name: string(),
        score: int32(),
      }),
    });
    const db = createQueryClient();

    // `orders.name` is a plain string column, not a nested column ref —
    // ck-orm rejects this at compile time because a single SQL expression
    // cannot fan out into the multiple parallel array fields a nested
    // column expands to physically.
    expect(() =>
      createInsertBuilder(nestedTarget)
        .fromSelect(
          db
            .select({
              id: orders.id,
              events: orders.name,
            })
            .from(orders) as never,
        )
        [compileQuerySymbol](),
    ).toThrow('insert().fromSelect() projection for nested column "events" must be a direct nested column reference');
  });

  it("accepts fromSelect that omits an optional nested column on the target", function testInsertFromSelectNestedOmitted() {
    const nestedTarget = ckTable("fs_nested_omit", {
      id: int32(),
      events: nested({
        name: string(),
        score: int32(),
      }),
    });
    const db = createQueryClient();

    // Nested column is optional in `$inferInsert` (Part A) — omit it from
    // the projection, ClickHouse fills it with an empty parallel array
    // server-side. Note: no `as never` cast — types accept this directly.
    const compiled = buildCompiled(
      createInsertBuilder(nestedTarget)
        .fromSelect(db.select({ id: orders.id }).from(orders))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(compiled.query)).toContain("insert into `fs_nested_omit` (`id`)");
    expect(normalizeSql(compiled.query)).not.toContain("`events`");
  });

  it("wraps an inner SELECT in a subquery and fans out nested column refs into dot-path projections", function testInsertFromSelectNestedFanOut() {
    const fanOutSource = ckTable("fs_fan_out_src", {
      id: int32(),
      events: nested({
        name: string(),
        score: int32(),
      }),
    });
    const fanOutTarget = ckTable("fs_fan_out_tgt", {
      id: int32(),
      events: nested({
        name: string(),
        score: int32(),
      }),
    });
    const db = createQueryClient();

    const compiled = buildCompiled(
      createInsertBuilder(fanOutTarget)
        .fromSelect(
          db
            .select({
              id: fanOutSource.id,
              events: fanOutSource.events,
            })
            .from(fanOutSource),
        )
        [compileQuerySymbol](),
    );

    const sqlText = normalizeSql(compiled.query);
    // INSERT list expanded to dot-path identifiers (each segment quoted separately)
    expect(sqlText).toContain("insert into `fs_fan_out_tgt` (`id`, `events`.`name`, `events`.`score`)");
    // Outer SELECT projects dot-path columns from the inner wrap-subquery
    expect(sqlText).toContain("`__ck_inner`.`id`, `__ck_inner`.`events`.`name`, `__ck_inner`.`events`.`score`");
    expect(sqlText).toContain("from (select");
    expect(sqlText).toContain(") as `__ck_inner`");
  });

  it("rejects fromSelect when the source nested shape is missing a target field", function testInsertFromSelectNestedShapeMismatch() {
    const slimSource = ckTable("fs_slim_src", {
      id: int32(),
      events: nested({
        name: string(),
        // no `score` field on the source
      }),
    });
    const wideTarget = ckTable("fs_wide_tgt", {
      id: int32(),
      events: nested({
        name: string(),
        score: int32(),
      }),
    });
    const db = createQueryClient();

    expect(() =>
      createInsertBuilder(wideTarget)
        .fromSelect(
          db
            .select({
              id: slimSource.id,
              events: slimSource.events,
            })
            .from(slimSource) as never,
        )
        [compileQuerySymbol](),
    ).toThrow('insert().fromSelect() nested column "events" shape mismatch: target requires field "score"');
  });

  it("rejects fromSelect that targets a generated column even with the from_select alignment", function testInsertFromSelectGeneratedExplicit() {
    const generatedTarget = ckTable(
      "fs_generated_explicit",
      {
        id: int32(),
        name: string(),
        amount: float64(),
        shardDay: int32("shard_day").materialized(sql`toYYYYMM(id)`),
      },
      (table) => ({
        engine: "MergeTree",
        orderBy: [table.id],
      }),
    );
    const db = createQueryClient();

    expect(() =>
      createInsertBuilder(generatedTarget)
        .fromSelect(
          db
            .select({
              id: orders.id,
              name: orders.name,
              amount: orders.amount,
              shardDay: orders.id,
            })
            .from(orders) as never,
        )
        [compileQuerySymbol](),
    ).toThrow('insert().fromSelect() cannot target generated column "shardDay"');
  });

  it("flips the runtime nestedRequiredOnInsert flag on the column produced by .requiredOnInsert()", function testRequiredOnInsertRuntimeFlag() {
    const base = nested({ name: string(), score: int32() });
    expect(base.nestedRequiredOnInsert).toBeFalsy();

    const refined = base.requiredOnInsert();
    expect(refined.nestedRequiredOnInsert).toBe(true);
    // The chain produces a fresh instance (so the flag participates in the
    // insert table-metadata snapshot below).
    expect(refined).not.toBe(base);

    // Subsequent calls remain idempotent at the semantic level — the flag
    // stays `true`. (Object identity changes again because each call rebuilds
    // the column, but consumers care about the flag, not the instance.)
    const refinedTwice = refined.requiredOnInsert();
    expect(refinedTwice.nestedRequiredOnInsert).toBe(true);

    // `requiredOnInsert()` on a non-nested column also sets the flag but
    // has no effect downstream because the column has no `nestedShape` to
    // be required about — exercises the chain method body for the
    // non-nested path so coverage stays at 100%.
    const stringColumn = string();
    const stringChained = stringColumn.requiredOnInsert();
    expect(stringChained.nestedRequiredOnInsert).toBe(true);
    expect(stringChained.nestedShape).toBeUndefined();
  });

  it("enforces requiredOnInsert at runtime for insert.fromSelect (`as never` bypass cannot drop required nested data)", function testRequiredOnInsertRuntimeGuard() {
    const sinkRequired = ckTable("fs_req_runtime_guard", {
      id: int32(),
      events: nested({ name: string(), score: int32() }).requiredOnInsert(),
    });
    const db = createQueryClient();

    // The type guard would catch this at compile time; cast through `never`
    // simulates a user who bypassed the type layer. Runtime must still
    // refuse — otherwise the contract becomes "TS-only" and a stale
    // forwarder could land silently incomplete rows.
    expect(() =>
      createInsertBuilder(sinkRequired)
        .fromSelect(db.select({ id: orders.id }).from(orders) as never)
        [compileQuerySymbol](),
    ).toThrow("insert().fromSelect() select is missing required columns: events");
  });

  it("propagates nestedShape through CTE references so wrap-subquery still recognises them as nested column refs", function testInsertFromSelectNestedThroughCte() {
    const nestedSource = ckTable("fs_cte_src", {
      id: int32(),
      events: nested({ name: string(), score: int32() }),
    });
    const nestedSink = ckTable("fs_cte_sink", {
      id: int32(),
      events: nested({ name: string(), score: int32() }),
    });
    const db = createQueryClient();

    // The CTE projects the nested column through one level of indirection.
    // `buildReferenceColumns` must propagate the source column's `nestedShape`
    // onto the CTE reference, otherwise `compileInsertFromSelect` would see
    // `kind: "expression"` without nested metadata and reject it.
    const filtered = db
      .$with("filtered")
      .as(
        db
          .select({ id: nestedSource.id, events: nestedSource.events })
          .from(nestedSource)
          .where(gt(nestedSource.id, 0)),
      );
    const compiled = buildCompiled(
      db
        .with(filtered)
        .insert(nestedSink)
        .fromSelect(db.with(filtered).select({ id: filtered.id, events: filtered.events }).from(filtered))
        [compileQuerySymbol](),
    );

    const sqlText = normalizeSql(compiled.query);
    expect(sqlText).toContain("insert into `fs_cte_sink` (`id`, `events`.`name`, `events`.`score`)");
    // Inner SELECT projects `filtered.events` as a single column; the outer
    // wrap-subquery accesses `.name` / `.score` sub-fields via dot-path.
    expect(sqlText).toContain("`__ck_inner`.`events`.`name`, `__ck_inner`.`events`.`score`");
    expect(sqlText).toContain("with `filtered` as");
  });

  it("propagates nestedShape through .as('alias') subquery references", function testInsertFromSelectNestedThroughSubquery() {
    const nestedSource = ckTable("fs_sub_src", {
      id: int32(),
      events: nested({ name: string(), score: int32() }),
    });
    const nestedSink = ckTable("fs_sub_sink", {
      id: int32(),
      events: nested({ name: string(), score: int32() }),
    });
    const db = createQueryClient();

    // `.as("aliased")` wraps the select into a subquery whose columns are
    // synthesised via `buildReferenceColumns`. The nested shape must survive
    // that wrap so the outer fromSelect can pick it up.
    const aliased = db.select({ id: nestedSource.id, events: nestedSource.events }).from(nestedSource).as("aliased");

    const compiled = buildCompiled(
      createInsertBuilder(nestedSink)
        .fromSelect(db.select({ id: aliased.id, events: aliased.events }).from(aliased))
        [compileQuerySymbol](),
    );

    const sqlText = normalizeSql(compiled.query);
    expect(sqlText).toContain("insert into `fs_sub_sink` (`id`, `events`.`name`, `events`.`score`)");
    expect(sqlText).toContain("`__ck_inner`.`events`.`name`, `__ck_inner`.`events`.`score`");
    expect(sqlText).toContain("as `aliased`");
  });

  it("accepts a source nested shape that is a superset of the target", function testInsertFromSelectNestedSourceSuperset() {
    // Source nested has more fields than target — target.fieldKeys ⊆
    // source.fieldKeys, so the missing-field check at the start of the
    // nested branch never trips. ClickHouse ignores the extra source field
    // at the wire level.
    const richSource = ckTable("fs_rich_src", {
      id: int32(),
      events: nested({
        name: string(),
        score: int32(),
        tag: string(),
      }),
    });
    const slimTarget = ckTable("fs_slim_tgt", {
      id: int32(),
      events: nested({
        name: string(),
        score: int32(),
      }),
    });
    const db = createQueryClient();

    const compiled = buildCompiled(
      createInsertBuilder(slimTarget)
        .fromSelect(db.select({ id: richSource.id, events: richSource.events }).from(richSource) as never)
        [compileQuerySymbol](),
    );

    const sqlText = normalizeSql(compiled.query);
    expect(sqlText).toContain("insert into `fs_slim_tgt` (`id`, `events`.`name`, `events`.`score`)");
    // Only `name` and `score` are projected on the outer SELECT — the source
    // `tag` field is intentionally dropped by ck-orm because the target has
    // no slot for it.
    expect(sqlText).toContain("`__ck_inner`.`events`.`name`, `__ck_inner`.`events`.`score`");
    expect(sqlText).not.toContain("`__ck_inner`.`events`.`tag`");
  });

  it("supports two nested columns side-by-side in one fromSelect projection", function testInsertFromSelectMultipleNestedColumns() {
    const dualSource = ckTable("fs_dual_src", {
      id: int32(),
      lineItems: nested({ sku: string(), qty: int32() }),
      statusHistory: nested({ status: string(), at: int32() }),
    });
    const dualSink = ckTable("fs_dual_sink", {
      id: int32(),
      lineItems: nested({ sku: string(), qty: int32() }),
      statusHistory: nested({ status: string(), at: int32() }),
    });
    const db = createQueryClient();

    const compiled = buildCompiled(
      createInsertBuilder(dualSink)
        .fromSelect(
          db
            .select({
              id: dualSource.id,
              lineItems: dualSource.lineItems,
              statusHistory: dualSource.statusHistory,
            })
            .from(dualSource),
        )
        [compileQuerySymbol](),
    );

    const sqlText = normalizeSql(compiled.query);
    // Both nested columns expand independently and stay adjacent in their
    // projection-key order.
    expect(sqlText).toContain(
      "insert into `fs_dual_sink` (`id`, `lineItems`.`sku`, `lineItems`.`qty`, `statusHistory`.`status`, `statusHistory`.`at`)",
    );
    expect(sqlText).toContain(
      "`__ck_inner`.`id`, `__ck_inner`.`lineItems`.`sku`, `__ck_inner`.`lineItems`.`qty`, `__ck_inner`.`statusHistory`.`status`, `__ck_inner`.`statusHistory`.`at`",
    );
  });

  it("rejects compiling an explicit empty-rows values() state", function testInsertValuesEmptyRowsRejected() {
    // Direct construction with `{ kind: "values", rows: [] }` bypasses the
    // public `.values(...)` factory (which normalises to non-empty rows).
    // The compile path must still surface the 0-row guard so a stale builder
    // can't accidentally produce an empty INSERT statement.
    const empty = createInsertBuilder(orders, undefined, { kind: "values", rows: [] });
    expect(() => empty[compileQuerySymbol]()).toThrow(
      "insert().values() must be called with at least one row before execute()",
    );
  });
});

describe("ck-orm anonymous CTE via $with()", function describeAnonymousCte() {
  it("renders WITH __cte_1 and references the same alias in outer FROM", function testAnonBasicShape() {
    const db = createQueryClient();
    const totals = db.$with().as(db.select({ id: orders.id }).from(orders));

    const built = buildCompiled(db.with(totals).select({ id: totals.id }).from(totals)[compileQuerySymbol]());

    expect(normalizeSql(built.query)).toContain("with `__cte_1` as (select `orders`.`id` as `id` from `orders`)");
    expect(normalizeSql(built.query)).toContain("select `__cte_1`.`id` as `id`");
    expect(normalizeSql(built.query)).toContain("from `__cte_1`");
  });

  it("resets the anonymous-CTE counter on each top-level compile", function testAnonCounterStability() {
    const db = createQueryClient();
    const totals = db.$with().as(db.select({ id: orders.id }).from(orders));
    const query = db.with(totals).select({ id: totals.id }).from(totals);

    const first = buildCompiled(query[compileQuerySymbol]());
    const second = buildCompiled(query[compileQuerySymbol]());

    expect(first.query).toBe(second.query);
    expect(normalizeSql(first.query)).toContain("`__cte_1`");
  });

  it("numbers multiple anonymous CTEs in declaration order", function testAnonMultipleCtes() {
    const db = createQueryClient();
    const a = db.$with().as(db.select({ id: orders.id }).from(orders));
    const b = db.$with().as(db.select({ id: orders.id, doubled: fn.multiply(orders.amount, 2) }).from(orders));

    const built = buildCompiled(
      db
        .with(a, b)
        .select({ aId: a.id, bDoubled: b.doubled })
        .from(a)
        .innerJoin(b, eq(a.id, b.id))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("`__cte_1` as (");
    expect(normalizeSql(built.query)).toContain("`__cte_2` as (");
    expect(normalizeSql(built.query)).toContain("`__cte_1`.`id` as `aId`");
    expect(normalizeSql(built.query)).toContain("`__cte_2`.`doubled` as `bDoubled`");
    expect(normalizeSql(built.query)).toContain("from `__cte_1`");
    expect(normalizeSql(built.query)).toContain("inner join `__cte_2`");
  });

  it("allows referencing the same anonymous CTE in FROM and JOIN without error", function testAnonRepeatRef() {
    const db = createQueryClient();
    const totals = db.$with().as(db.select({ id: orders.id, amount: orders.amount }).from(orders));

    const built = buildCompiled(
      db
        .with(totals)
        .select({ id: totals.id })
        .from(totals)
        .innerJoin(orders, eq(totals.id, orders.id))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("from `__cte_1`");
    expect(normalizeSql(built.query)).toContain("inner join `orders` on `__cte_1`.`id` = `orders`.`id`");
  });

  it("mixes named and anonymous CTEs without crosstalk", function testAnonMixedNamed() {
    const db = createQueryClient();
    const named = db.$with("named_totals").as(db.select({ id: orders.id }).from(orders));
    const anon = db.$with().as(db.select({ id: orders.id }).from(orders));

    const built = buildCompiled(
      db
        .with(named, anon)
        .select({ namedId: named.id, anonId: anon.id })
        .from(named)
        .innerJoin(anon, eq(named.id, anon.id))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("`named_totals` as (");
    expect(normalizeSql(built.query)).toContain("`__cte_1` as (");
    expect(normalizeSql(built.query)).toContain("`named_totals`.`id` as `namedId`");
    expect(normalizeSql(built.query)).toContain("`__cte_1`.`id` as `anonId`");
  });

  it("resolves the same anonymous CTE alias when referenced from a nested CTE query", function testAnonNestedRef() {
    const db = createQueryClient();
    const cte1 = db.$with().as(db.select({ id: orders.id }).from(orders));
    // cte2 references cte1 in its FROM but doesn't redeclare it — both are
    // attached on the outer `db.with(cte1, cte2)` below.
    const cte2 = db.$with().as(db.select({ id: cte1.id }).from(cte1));

    const built = buildCompiled(db.with(cte1, cte2).select({ id: cte2.id }).from(cte2)[compileQuerySymbol]());

    // cte1 is __cte_1 in the outer WITH; the nested CTE's body must reference
    // the same __cte_1 alias inside its FROM clause.
    expect(normalizeSql(built.query)).toContain("`__cte_1` as (select `orders`.`id` as `id` from `orders`)");
    expect(normalizeSql(built.query)).toContain("`__cte_2` as (select `__cte_1`.`id` as `id` from `__cte_1`)");
    expect(normalizeSql(built.query)).toContain("from `__cte_2`");
  });

  it("uses anonymous CTE column refs in WHERE and ORDER BY clauses", function testAnonInWhereOrderBy() {
    const db = createQueryClient();
    const totals = db.$with().as(
      db
        .select({ id: orders.id, total: fn.sum(orders.amount).as("t") })
        .from(orders)
        .groupBy(orders.id),
    );

    const built = buildCompiled(
      db
        .with(totals)
        .select({ id: totals.id, total: totals.total })
        .from(totals)
        .where(gt(totals.total, 100))
        .orderBy(desc(totals.total), totals.id)
        [compileQuerySymbol](),
    );

    // Every column reference (WHERE / ORDER BY / SELECT) must use the same
    // auto alias — proves the lazy resolver returns the cached id across
    // multiple compile-time touchpoints. `desc()` / default ASC produce
    // upper-case DESC / ASC suffixes in the compiled SQL.
    expect(normalizeSql(built.query)).toContain("where `__cte_1`.`t` >");
    expect(normalizeSql(built.query)).toContain("order by `__cte_1`.`t` DESC, `__cte_1`.`id` ASC");
    expect(normalizeSql(built.query)).toContain("select `__cte_1`.`id` as `id`, `__cte_1`.`t` as `total`");
  });

  it("uses anonymous CTE column refs in HAVING after GROUP BY", function testAnonInHaving() {
    const db = createQueryClient();
    const totals = db.$with().as(db.select({ id: orders.id, amount: orders.amount }).from(orders));

    const built = buildCompiled(
      db
        .with(totals)
        .select({ id: totals.id, sumAmount: fn.sum(totals.amount).as("sum_amount") })
        .from(totals)
        .groupBy(totals.id)
        .having(gt(fn.sum(totals.amount), 0))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("group by `__cte_1`.`id`");
    expect(normalizeSql(built.query)).toContain("having sum(`__cte_1`.`amount`) >");
  });

  it("keeps __sub_N and __cte_N counters independent when mixing bare subqueries with anonymous CTEs", function testAnonAndBareCountersIndependent() {
    const db = createQueryClient();
    const anonCte = db.$with().as(db.select({ id: orders.id }).from(orders));
    const bareSub = db.select({ id: orders.id }).from(orders);

    const built = buildCompiled(
      db
        .with(anonCte)
        .select({ id: anonCte.id })
        .from(anonCte)
        .innerJoin(bareSub, (joined) => eq(anonCte.id, joined.id))
        [compileQuerySymbol](),
    );

    // Bare subquery gets __sub_1, anonymous CTE gets __cte_1 — separate
    // counters guarantee neither numbering races against the other.
    expect(normalizeSql(built.query)).toContain("`__cte_1` as (");
    expect(normalizeSql(built.query)).toContain("as `__sub_1`");
    expect(normalizeSql(built.query)).not.toContain("`__cte_2`");
    expect(normalizeSql(built.query)).not.toContain("`__sub_2`");
  });

  it("supports count(anonymousCte) with predicate when nested as a scalar subquery", function testAnonCount() {
    const db = createQueryClient();
    const totals = db.$with().as(db.select({ id: orders.id, amount: orders.amount }).from(orders));

    // `db.count(...)` returns a CountQuery (Selection-shaped, no compileQuerySymbol).
    // Wrap it as a scalar subquery inside another SELECT to exercise the
    // anonymous CTE rendering on the count path via .compile().
    const cteDb = db.with(totals);
    const built = buildCompiled(
      db
        .select({
          id: orders.id,
          total: cteDb.count(totals, gt(totals.amount, 5)).toSafe().as("total"),
        })
        .from(orders)
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("with `__cte_1` as (");
    expect(normalizeSql(built.query)).toContain("from `__cte_1`");
    expect(normalizeSql(built.query)).toContain("where `__cte_1`.`amount` >");
  });

  it("supports anonymous CTE inside insert().fromSelect()", function testAnonInsertFromSelect() {
    const db = createQueryClient();
    // The anonymous CTE projects all three orders columns. `cte.columns.name`
    // is used in the outer SELECT because `name` is one of the CTE-reserved
    // keys (kind/name/query/columns) and is therefore not auto-attached on
    // the cte object — access via `cte.columns.name` is the documented
    // escape hatch. Mirrors the bare-SelectBuilder forbidden-keys behaviour.
    const filtered = db.$with().as(db.select({ id: orders.id, name: orders.name, amount: orders.amount }).from(orders));

    const built = buildCompiled(
      db
        .insert(orders)
        .fromSelect(
          db
            .with(filtered)
            .select({ id: filtered.id, name: filtered.columns.name, amount: filtered.amount })
            .from(filtered),
        )
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("insert into `orders`");
    expect(normalizeSql(built.query)).toContain("with `__cte_1` as (");
    expect(normalizeSql(built.query)).toContain("`__cte_1`.`id` as `id`");
    expect(normalizeSql(built.query)).toContain("`__cte_1`.`name` as `name`");
    expect(normalizeSql(built.query)).toContain("`__cte_1`.`amount` as `amount`");
    expect(normalizeSql(built.query)).toContain("from `__cte_1`");
  });

  it("masks CTE-reserved column keys (kind/name/query/columns) — access via cte.columns instead", function testAnonReservedKeys() {
    const db = createQueryClient();
    // A user selection that collides on the CTE-reserved keys. Without the
    // mask, `Object.assign(cte, columns)` would overwrite `cte.name`/etc and
    // break SQL rendering (`sql.identifier(cte.name)` would receive an
    // expression object). With the mask, the CTE meta survives intact.
    const cte = db.$with().as(db.select({ name: orders.name, query: orders.id, id: orders.id }).from(orders));

    // CTE meta still has the correct shape — name remains undefined for
    // anonymous, query is the inner select, columns map exposes ALL refs
    // (including the masked ones).
    expect(cte.name).toBeUndefined();
    expect(typeof cte.query).toBe("object");
    expect(cte.columns.name).toBeDefined();
    expect(cte.columns.query).toBeDefined();
    expect(cte.columns.id).toBeDefined();
    // Non-reserved keys still attach directly:
    expect(typeof (cte as unknown as Record<string, unknown>).id).toBe("object");

    const built = buildCompiled(
      db
        .with(cte)
        .select({
          id: cte.id,
          n: cte.columns.name,
          q: cte.columns.query,
        })
        .from(cte)
        [compileQuerySymbol](),
    );

    // SQL still renders correctly — the inner names appear unmangled.
    expect(normalizeSql(built.query)).toContain(
      "with `__cte_1` as (select `orders`.`name` as `name`, `orders`.`id` as `query`, `orders`.`id` as `id` from `orders`)",
    );
    expect(normalizeSql(built.query)).toContain("`__cte_1`.`id` as `id`");
    expect(normalizeSql(built.query)).toContain("`__cte_1`.`name` as `n`");
    expect(normalizeSql(built.query)).toContain("`__cte_1`.`query` as `q`");
  });

  it("supports three-level nesting of anonymous CTEs", function testAnonThreeLevelNesting() {
    const db = createQueryClient();
    const level1 = db.$with().as(db.select({ id: orders.id, amount: orders.amount }).from(orders));
    const level2 = db
      .$with()
      .as(db.select({ id: level1.id, doubled: fn.multiply(level1.amount, 2).as("d") }).from(level1));
    const level3 = db
      .$with()
      .as(db.select({ id: level2.id, quad: fn.multiply(level2.doubled, 2).as("q") }).from(level2));

    const built = buildCompiled(
      db.with(level1, level2, level3).select({ id: level3.id, q: level3.quad }).from(level3)[compileQuerySymbol](),
    );

    // Each level resolves to a distinct __cte_N and inner references the prior level.
    expect(normalizeSql(built.query)).toContain("`__cte_1` as (select `orders`.`id` as `id`");
    expect(normalizeSql(built.query)).toContain("`__cte_2` as (select `__cte_1`.`id` as `id`");
    expect(normalizeSql(built.query)).toContain("`__cte_3` as (select `__cte_2`.`id` as `id`");
    expect(normalizeSql(built.query)).toContain("from `__cte_3`");
  });

  it("preserves alias identity when the same anonymous CTE column ref appears in many places", function testAnonRefCacheHit() {
    const db = createQueryClient();
    // A column ref used in SELECT, WHERE, GROUP BY, HAVING, ORDER BY — five
    // separate touchpoints. Each must call resolveAnonymousCteAlias and hit
    // the same cached `__cte_1` value.
    const cte = db.$with().as(db.select({ id: orders.id, amount: orders.amount }).from(orders));

    const built = buildCompiled(
      db
        .with(cte)
        .select({ id: cte.id, total: fn.sum(cte.amount).as("total") })
        .from(cte)
        .where(gt(cte.amount, 0))
        .groupBy(cte.id)
        .having(gt(fn.sum(cte.amount), 1))
        .orderBy(cte.id)
        [compileQuerySymbol](),
    );

    // Sanity: every reference is `__cte_1`, none accidentally becomes `__cte_2`.
    expect(normalizeSql(built.query)).not.toContain("`__cte_2`");
    expect(normalizeSql(built.query)).toContain("select `__cte_1`.`id`");
    expect(normalizeSql(built.query)).toContain("sum(`__cte_1`.`amount`)");
    expect(normalizeSql(built.query)).toContain("where `__cte_1`.`amount` >");
    expect(normalizeSql(built.query)).toContain("group by `__cte_1`.`id`");
    expect(normalizeSql(built.query)).toContain("order by `__cte_1`.`id`");
  });

  it("supports leftJoin with anonymous CTE source", function testAnonLeftJoin() {
    const db = createQueryClient();
    const totals = db.$with().as(db.select({ id: orders.id, amount: orders.amount }).from(orders));

    const built = buildCompiled(
      db
        .with(totals)
        .select({ id: orders.id, name: orders.name, total: totals.amount })
        .from(orders)
        .leftJoin(totals, eq(orders.id, totals.id))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("left join `__cte_1` on `orders`.`id` = `__cte_1`.`id`");
    expect(normalizeSql(built.query)).toContain("`__cte_1`.`amount` as `total`");
  });

  it("accepts an anonymous CTE whose query already carries CTEs (chained .with)", function testAnonChainedClient() {
    const db = createQueryClient();
    const baseCte = db.$with("base").as(db.select({ id: orders.id }).from(orders));

    // Inner client carrying `baseCte` builds the anonymous CTE's body.
    const innerDb = db.with(baseCte);
    const derived = innerDb.$with().as(innerDb.select({ id: baseCte.id }).from(baseCte));

    const built = buildCompiled(innerDb.with(derived).select({ id: derived.id }).from(derived)[compileQuerySymbol]());

    expect(normalizeSql(built.query)).toContain("`base` as (select `orders`.`id` as `id` from `orders`)");
    expect(normalizeSql(built.query)).toContain("`__cte_1` as (");
    expect(normalizeSql(built.query)).toContain("from `__cte_1`");
  });

  it("does not leak the anonymous CTE auto-alias between two independently compiled queries", function testAnonAcrossQueries() {
    const db = createQueryClient();
    const aCte = db.$with().as(db.select({ id: orders.id }).from(orders));
    const bCte = db.$with().as(db.select({ id: orders.id }).from(orders));

    // Query 1: only aCte → must be __cte_1
    const q1 = buildCompiled(db.with(aCte).select({ id: aCte.id }).from(aCte)[compileQuerySymbol]());
    // Query 2: only bCte → also __cte_1 (counter resets per compile)
    const q2 = buildCompiled(db.with(bCte).select({ id: bCte.id }).from(bCte)[compileQuerySymbol]());

    expect(normalizeSql(q1.query)).toContain("`__cte_1`");
    expect(normalizeSql(q2.query)).toContain("`__cte_1`");
  });

  it("named $with() still infers literal alias type and renders the user-supplied name", function testNamedPathUnchanged() {
    const db = createQueryClient();
    // Existing named path: the literal "totals" must appear in SQL verbatim.
    const named = db.$with("totals").as(db.select({ id: orders.id }).from(orders));

    expect(named.kind).toBe("cte");
    expect(named.name).toBe("totals");

    const built = buildCompiled(db.with(named).select({ id: named.id }).from(named)[compileQuerySymbol]());
    expect(normalizeSql(built.query)).toContain("`totals` as (");
    expect(normalizeSql(built.query)).toContain("from `totals`");
    expect(normalizeSql(built.query)).not.toContain("__cte_");
  });

  it("anonymous CTE object exposes name === undefined and kind === 'cte'", function testAnonCteShape() {
    const db = createQueryClient();
    const cte = db.$with().as(db.select({ id: orders.id }).from(orders));

    expect(cte.kind).toBe("cte");
    expect(cte.name).toBeUndefined();
    expect(typeof cte.id).toBe("object");
  });

  it("anonymous CTE built on one QueryClient instance is usable from another", function testAnonAddedToConsumingClient() {
    // A library helper that returns an anonymous CTE attached to a freshly
    // created QueryClient — the consumer threads it through their own `db`
    // via `.with(cte)`. This is the realistic factory pattern.
    const factoryDb = createQueryClient();
    const reusableCte = factoryDb.$with().as(factoryDb.select({ id: orders.id }).from(orders));

    const consumerDb = createQueryClient();
    const built = buildCompiled(
      consumerDb.with(reusableCte).select({ id: reusableCte.id }).from(reusableCte)[compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("`__cte_1` as (");
    expect(normalizeSql(built.query)).toContain("from `__cte_1`");
  });

  it("rewriting the WeakMap cache hit path: two consecutive same-builder compiles still produce __cte_1", function testAnonCacheHitAcrossCompiles() {
    // This targets the `if (cached) return cached;` branch in
    // resolveAnonymousCteAlias. The first compile populates the cache via
    // renderCtes; renderSource then hits it. Subsequent compile uses a fresh
    // ctx, so the WeakMap is empty for that ctx — coverage of both miss and
    // hit branches comes naturally from any multi-touchpoint test, but we
    // assert it explicitly here.
    const db = createQueryClient();
    const cte = db.$with().as(db.select({ id: orders.id }).from(orders));
    const query = db.with(cte).select({ id: cte.id }).from(cte);

    const first = buildCompiled(query[compileQuerySymbol]());
    const second = buildCompiled(query[compileQuerySymbol]());
    const third = buildCompiled(query[compileQuerySymbol]());

    expect(first.query).toBe(second.query);
    expect(second.query).toBe(third.query);
    expect(normalizeSql(first.query)).toContain("`__cte_1`");
  });

  it("forwards anonymous CTE alias state into inner compileSql ctx (lazy refs inside expr() resolve correctly)", function testAnonAliasForwardingIntoCompileSql() {
    // Regression: when a lazy column ref is embedded inside a sql template
    // via `expr(sql`coalesce(${cte.col}, 0)`)`, normalizeTemplateValue wraps
    // it as a runtime chunk that gets evaluated during the inner compileSql
    // pass (a fresh ctx copy made by `compileSql`). Without forwarding the
    // bare-builder / CTE alias state through the copy, the inner ctx miss
    // would re-allocate `__cte_1` for the second CTE, producing
    // `coalesce(__cte_1.col, 0)` while the CTE is actually defined as
    // `__cte_2` in the WITH clause — wrong SQL.
    const db = createQueryClient();
    const a = db.$with().as(db.select({ id: orders.id }).from(orders));
    const b = db.$with().as(db.select({ id: orders.id, amount: orders.amount }).from(orders));

    const built = buildCompiled(
      db
        .with(a, b)
        .select({
          id: a.id,
          fallback: expr(sql`coalesce(${b.amount}, 0)`).as("fallback"),
        })
        .from(a)
        .leftJoin(b, eq(a.id, b.id))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("`__cte_1` as (select `orders`.`id` as `id` from `orders`)");
    expect(normalizeSql(built.query)).toContain("`__cte_2` as (");
    // The CRITICAL assertion: coalesce references __cte_2.amount, not __cte_1.amount.
    expect(normalizeSql(built.query)).toContain("coalesce(`__cte_2`.`amount`, 0) as `fallback`");
    expect(normalizeSql(built.query)).toContain("left join `__cte_2` on `__cte_1`.`id` = `__cte_2`.`id`");
  });
});

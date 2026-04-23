import { describe, expect, it } from "bun:test";
import { type array, decimal, type int16, int32, type int64, type nullable, string, type uint64 } from "./columns";
import { fn } from "./functions";
import {
  and,
  compileQuerySymbol,
  compileWithContextSymbol,
  createQueryClient,
  decodeRow,
  desc,
  eq,
  exists,
  expr,
  inArray,
} from "./query";
import { clickhouseClient } from "./runtime";
import type { ClickHouseTableEngine } from "./schema";
import { alias, chTable } from "./schema";
import { sql } from "./sql";
import { orderRewardLog } from "./test-schema/commerce";
import { shipmentOrder } from "./test-schema/fulfillment";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Expect<T extends true> = T;
type InferBuilderResult<T> = Awaited<T> extends Array<infer TResult> ? TResult : never;

type _Int16Type = Expect<
  Equal<ReturnType<typeof int16>["mapFromDriverValue"] extends (value: unknown) => infer T ? T : never, number>
>;
type _Int64Type = Expect<
  Equal<ReturnType<typeof int64>["mapFromDriverValue"] extends (value: unknown) => infer T ? T : never, string>
>;
type _UInt64Type = Expect<
  Equal<ReturnType<typeof uint64>["mapFromDriverValue"] extends (value: unknown) => infer T ? T : never, string>
>;
type _DecimalType = Expect<
  Equal<ReturnType<typeof decimal>["mapFromDriverValue"] extends (value: unknown) => infer T ? T : never, string>
>;
type _NullableType = Expect<
  Equal<
    ReturnType<typeof nullable<ReturnType<typeof string>>>["mapFromDriverValue"] extends (value: unknown) => infer T
      ? T
      : never,
    string | null
  >
>;
type _ArrayType = Expect<
  Equal<
    ReturnType<typeof array<ReturnType<typeof string>>>["mapFromDriverValue"] extends (value: unknown) => infer T
      ? T
      : never,
    string[]
  >
>;
type _EngineType = Expect<
  Equal<Extract<ClickHouseTableEngine, "ReplicatedReplacingMergeTree">, "ReplicatedReplacingMergeTree">
>;

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

const camelRewardLog = chTable(
  "order_reward_log",
  {
    userId: string("user_id"),
    rewardPoints: decimal("reward_points", { precision: 20, scale: 5 }),
    createdAt: int32("created_at"),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.userId, table.createdAt],
  }),
);

const pets = chTable(
  "pets",
  {
    id: int32(),
    owner_id: int32(),
    pet_name: string(),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.owner_id, table.id],
  }),
);

type TestSchema = {
  users: typeof users;
  pets: typeof pets;
};

const typeDb = createQueryClient<TestSchema, 1>({
  schema: { users, pets },
  joinUseNulls: 1,
});
const defaultLeftJoin = typeDb.select().from(users).leftJoin(pets, eq(users.id, pets.owner_id));
const explicitLeftJoin = typeDb
  .select({
    userId: users.id,
    petId: pets.id,
    rawFlag: expr(sql.raw("1"))
      .mapWith((value) => Number(value))
      .as("raw_flag"),
  })
  .from(users)
  .leftJoin(pets, eq(users.id, pets.owner_id));
const noNullsDb = createQueryClient<TestSchema, 0>({
  schema: { users, pets },
  joinUseNulls: 0,
});
const noNullsLeftJoin = noNullsDb.select().from(users).leftJoin(pets, eq(users.id, pets.owner_id));
const drizzleStyleOrderBy = typeDb
  .select()
  .from(users)
  .leftJoin(pets, eq(users.id, pets.owner_id))
  .orderBy(users.id, desc(pets.id));
const publicDb = clickhouseClient({
  databaseUrl: "http://127.0.0.1:8123",
  schema: { users, pets },
});
const publicLeftJoin = publicDb.select().from(users).leftJoin(pets, eq(users.id, pets.owner_id));
const publicRawQuery = sql`select 1 as one`;
const publicParamQuery = publicDb
  .select({
    userId: users.id,
    petId: pets.id,
  })
  .from(users)
  .leftJoin(pets, eq(users.id, pets.owner_id))
  .where(eq(users.id, 1));
const publicCountQuery = publicDb.count(users, eq(users.id, 1));
const publicSafeCountQuery = publicCountQuery.toSafe();
const publicUnsafeSafeCountQuery = publicCountQuery.toUnsafe().toSafe();
const publicMixedCountQuery = publicCountQuery.toMixed();
const publicCountSelection = publicDb
  .select({
    userCount: publicDb.count(pets, eq(pets.owner_id, users.id)).as("user_count"),
  })
  .from(users);
const publicCountModeSelection = publicDb
  .select({
    safeCount: publicDb.count(pets, eq(pets.owner_id, users.id)).toSafe().as("safe_count"),
    mixedCount: publicDb.count(pets, eq(pets.owner_id, users.id)).toMixed().as("mixed_count"),
  })
  .from(users);
const publicNoNullsDb = publicDb.withSettings({
  join_use_nulls: 0 as const,
});
const publicNoNullsLeftJoin = publicNoNullsDb.select().from(users).leftJoin(pets, eq(users.id, pets.owner_id));

const assertPublicClientRuntimeTypes = async () => {
  const typedRows: Array<
    { users: typeof users.$inferSelect } & {
      pets: typeof pets.$inferSelect | null;
    }
  > = await publicLeftJoin;
  const typedIterator: AsyncGenerator<
    { users: typeof users.$inferSelect } & {
      pets: typeof pets.$inferSelect | null;
    },
    void,
    unknown
  > = await publicLeftJoin.iterator();
  const explicitExecute: Promise<
    Array<
      { users: typeof users.$inferSelect } & {
        pets: typeof pets.$inferSelect | null;
      }
    >
  > = publicLeftJoin.execute({ query_id: "typed_query" });
  const rawExecute: Promise<Record<string, unknown>[]> = publicDb.execute(publicRawQuery);
  const rawStream: AsyncGenerator<Record<string, unknown>, void, unknown> = publicDb.stream(publicRawQuery);
  const noNullsRows: Array<{ users: typeof users.$inferSelect } & { pets: typeof pets.$inferSelect }> =
    await publicNoNullsLeftJoin;
  const paramRows: Array<{ userId: number; petId: number | null }> = await publicParamQuery;
  const paramIterator: AsyncGenerator<{ userId: number; petId: number | null }, void, unknown> =
    await publicParamQuery.iterator();
  const countValue: number = await publicCountQuery;
  const countExecute: Promise<number> = publicCountQuery.execute({
    query_id: "typed_count_query",
  });
  const safeCountValue: string = await publicSafeCountQuery;
  const safeCountExecute: Promise<string> = publicSafeCountQuery.execute();
  const unsafeSafeCountValue: string = await publicUnsafeSafeCountQuery;
  const mixedCountValue: number | string = await publicMixedCountQuery;
  const countRows: Array<{ userCount: number }> = await publicCountSelection;
  const modeCountRows: Array<{ safeCount: string; mixedCount: number | string }> = await publicCountModeSelection;
  const insertResult: undefined = await publicDb.insert(users).values({ id: 1, name: "alice" });

  // @ts-expect-error builder should no longer go through db.execute()
  void publicDb.execute(publicLeftJoin);
  // @ts-expect-error builder should no longer go through db.stream()
  void publicDb.stream(publicLeftJoin);

  void typedRows;
  void typedIterator;
  void explicitExecute;
  void rawExecute;
  void rawStream;
  void noNullsRows;
  void paramRows;
  void paramIterator;
  void countValue;
  void countExecute;
  void safeCountValue;
  void safeCountExecute;
  void unsafeSafeCountValue;
  void mixedCountValue;
  void countRows;
  void modeCountRows;
  void insertResult;
};

type _DefaultLeftJoinType = Expect<
  Equal<
    InferBuilderResult<typeof defaultLeftJoin>,
    { users: typeof users.$inferSelect } & {
      pets: typeof pets.$inferSelect | null;
    }
  >
>;
type _ExplicitLeftJoinType = Expect<
  Equal<InferBuilderResult<typeof explicitLeftJoin>, { userId: number; petId: number | null; rawFlag: number }>
>;
type _NoNullsLeftJoinType = Expect<
  Equal<
    InferBuilderResult<typeof noNullsLeftJoin>,
    { users: typeof users.$inferSelect } & { pets: typeof pets.$inferSelect }
  >
>;
type _OrderByColumnType = Expect<
  Equal<InferBuilderResult<typeof drizzleStyleOrderBy>, InferBuilderResult<typeof defaultLeftJoin>>
>;
type _PublicNoNullsLeftJoinType = Expect<
  Equal<
    InferBuilderResult<typeof publicNoNullsLeftJoin>,
    { users: typeof users.$inferSelect } & { pets: typeof pets.$inferSelect }
  >
>;
type _PublicCountQueryType = Expect<Equal<Awaited<typeof publicCountQuery>, number>>;
type _PublicSafeCountQueryType = Expect<Equal<Awaited<typeof publicSafeCountQuery>, string>>;
type _PublicUnsafeSafeCountQueryType = Expect<Equal<Awaited<typeof publicUnsafeSafeCountQuery>, string>>;
type _PublicMixedCountQueryType = Expect<Equal<Awaited<typeof publicMixedCountQuery>, number | string>>;
type _PublicCountSelectionType = Expect<Equal<InferBuilderResult<typeof publicCountSelection>, { userCount: number }>>;
type _PublicCountModeSelectionType = Expect<
  Equal<InferBuilderResult<typeof publicCountModeSelection>, { safeCount: string; mixedCount: number | string }>
>;

const typeAssertions: [
  _Int16Type,
  _Int64Type,
  _UInt64Type,
  _DecimalType,
  _NullableType,
  _ArrayType,
  _EngineType,
  _DefaultLeftJoinType,
  _ExplicitLeftJoinType,
  _NoNullsLeftJoinType,
  _OrderByColumnType,
  _PublicNoNullsLeftJoinType,
  _PublicCountQueryType,
  _PublicSafeCountQueryType,
  _PublicUnsafeSafeCountQueryType,
  _PublicMixedCountQueryType,
  _PublicCountSelectionType,
  _PublicCountModeSelectionType,
] = [true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true];

const normalizeSql = (value: string) => value.replace(/\s+/g, " ").trim();
const buildCompiled = (compiled: { statement: string; params: Record<string, unknown> }) => {
  return {
    query: compiled.statement,
    params: compiled.params,
  };
};

describe("ck-orm query compile", function describeClickHouseOrmQueryCompile() {
  it("compiles a drizzle-like select with alias, final, filters, limit and offset", function testCompileSelect() {
    expect(typeAssertions).toEqual([
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
    ]);
    expect(typeof assertPublicClientRuntimeTypes).toBe("function");
    expect(orderRewardLog.options.orderBy?.map((column) => column.name)).toEqual(["user_id", "created_at", "id"]);
    expect(orderRewardLog.options.versionColumn?.name).toBe("_peerdb_version");

    const aliasedLog = alias(orderRewardLog, "orl");
    expect(aliasedLog.options.orderBy?.map((column) => column.tableAlias)).toEqual(["orl", "orl", "orl"]);
    expect(aliasedLog.options.versionColumn?.tableAlias).toBe("orl");

    const db = createQueryClient({
      schema: {
        orderRewardLog,
      },
    });
    const query = db
      .select({
        userId: aliasedLog.user_id,
        totalRewardPoints: fn.sum(aliasedLog.reward_points).as("total_reward_points"),
        activeUsers: fn.uniqExact(aliasedLog.user_id),
      })
      .from(aliasedLog)
      .where(and(eq(aliasedLog.user_id, "u_100"), inArray(aliasedLog.channel, [1, 2])))
      .orderBy(desc(aliasedLog.created_at))
      .limit(20)
      .offset(40)
      .final();

    const built = buildCompiled(query[compileQuerySymbol]());

    expect(normalizeSql(built.query)).toContain(
      "select `orl`.`user_id` as `userId`, sum(`orl`.`reward_points`) as `total_reward_points`, uniqExact(`orl`.`user_id`) as `activeUsers`",
    );
    expect(normalizeSql(built.query)).toContain("from `order_reward_log` as `orl` final");
    expect(normalizeSql(built.query)).toContain(
      "where (`orl`.`user_id` = {orm_param1:String} and `orl`.`channel` in ({orm_param2:Int32}, {orm_param3:Int32}))",
    );
    expect(normalizeSql(built.query)).toContain("order by `orl`.`created_at` DESC");
    expect(normalizeSql(built.query)).toContain("limit {orm_param4:Int64}");
    expect(normalizeSql(built.query)).toContain("offset {orm_param5:Int64}");
    expect(built.params).toEqual({
      orm_param1: "u_100",
      orm_param2: 1,
      orm_param3: 2,
      orm_param4: 20,
      orm_param5: 40,
    });
  });

  it("compiles cte, subquery, tuple, arrayZip and limit by", function testCompileCteAndFunctions() {
    const db = createQueryClient({
      schema: {
        orderRewardLog,
        shipmentOrder,
      },
    });

    const ranked = db.$with("ranked").as(
      db
        .select({
          userId: orderRewardLog.user_id,
          totalRewardPoints: fn.sum(orderRewardLog.reward_points).as("total_reward_points"),
        })
        .from(orderRewardLog)
        .groupBy(orderRewardLog.user_id),
    );

    const latest = db
      .select({
        userId: shipmentOrder.user_id,
        pair: fn.tuple(shipmentOrder.user_id, shipmentOrder.order_id).as("pair"),
        zipped: fn.arrayZip(shipmentOrder.product_sku, shipmentOrder.shipment_id).as("zipped"),
      })
      .from(shipmentOrder)
      .limitBy([shipmentOrder.user_id], 1)
      .as("latest_shipments");

    const built = buildCompiled(
      db
        .with(ranked)
        .select({
          userId: ranked.userId,
          totalRewardPoints: ranked.totalRewardPoints,
          userIdFromShipment: latest.userId,
          pair: latest.pair,
          zipped: latest.zipped,
        })
        .from(ranked)
        .leftJoin(latest, eq(ranked.userId, latest.userId))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain(
      "with `ranked` as (select `order_reward_log`.`user_id` as `userId`, sum(`order_reward_log`.`reward_points`) as `total_reward_points`",
    );
    expect(normalizeSql(built.query)).toContain(
      "tuple(`shipment_orders`.`user_id`, `shipment_orders`.`order_id`) as `pair`",
    );
    expect(normalizeSql(built.query)).toContain(
      "arrayZip(`shipment_orders`.`product_sku`, `shipment_orders`.`shipment_id`) as `zipped`",
    );
    expect(normalizeSql(built.query)).toContain("limit {orm_param1:Int64} by `shipment_orders`.`user_id`");
    expect(built.params).toEqual({
      orm_param1: 1,
    });
  });

  it("compiles insert values using table schema types", function testCompileInsert() {
    const db = createQueryClient({
      schema: {
        orderRewardLog,
      },
    });

    const built = buildCompiled(
      db
        .insert(orderRewardLog)
        .values({
          id: 1,
          user_id: "u_1",
          membership_id: "member_1",
          campaign_id: 2,
          order_id: "10",
          product_sku: "SKU-RED-MUG",
          quantity: "1.00000",
          reward_points: "2.50000",
          channel: 1,
          event_type: "purchase",
          status: 1,
          region: "APAC",
          created_at: 1710000000,
          event_date: 20260421,
          _peerdb_synced_at: new Date("2026-04-21T00:00:00.000Z"),
          _peerdb_is_deleted: 0,
          _peerdb_version: "1",
        })
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain("insert into `order_reward_log`");
    expect(normalizeSql(built.query)).toContain("{orm_param1:Int32}");
    expect(normalizeSql(built.query)).toContain("{orm_param9:Int32}");
  });

  it("compiles physical column names while decoding and inserting logical keys", function testLogicalAndPhysicalNames() {
    const db = createQueryClient({
      schema: {
        camelRewardLog,
      },
    });

    const implicitSelect = db.select().from(camelRewardLog)[compileQuerySymbol]();
    expect(normalizeSql(implicitSelect.statement)).toContain(
      "select `order_reward_log`.`user_id` as `userId`, `order_reward_log`.`reward_points` as `rewardPoints`, `order_reward_log`.`created_at` as `createdAt`",
    );
    expect(
      decodeRow({ userId: "u_1", rewardPoints: "2.50000", createdAt: "1710000000" }, implicitSelect.selection),
    ).toEqual({
      userId: "u_1",
      rewardPoints: "2.50000",
      createdAt: 1710000000,
    });

    const aggregateSelect = db
      .select({
        userId: camelRewardLog.userId,
        totalRewardPoints: fn.sum(camelRewardLog.rewardPoints).as("total_reward_points"),
      })
      .from(camelRewardLog)
      .where(eq(camelRewardLog.userId, "u_1"))
      .groupBy(camelRewardLog.userId)
      .orderBy(desc(camelRewardLog.createdAt))
      [compileQuerySymbol]();

    expect(normalizeSql(aggregateSelect.statement)).toContain(
      "where `order_reward_log`.`user_id` = {orm_param1:String}",
    );
    expect(normalizeSql(aggregateSelect.statement)).toContain("group by `order_reward_log`.`user_id`");
    expect(normalizeSql(aggregateSelect.statement)).toContain("order by `order_reward_log`.`created_at` DESC");

    const insert = db
      .insert(camelRewardLog)
      .values({
        userId: "u_1",
        rewardPoints: "2.50000",
        createdAt: 1710000000,
      })
      [compileQuerySymbol]();

    expect(normalizeSql(insert.statement)).toContain(
      "insert into `order_reward_log` (`user_id`, `reward_points`, `created_at`)",
    );
    expect(insert.params).toEqual({
      orm_param1: "u_1",
      orm_param2: "2.50000",
      orm_param3: 1710000000,
    });
  });

  it("supports drizzle-style orderBy(column, desc(expr)) and nested default left-join selection", function testOrderByAndJoinShape() {
    const db = createQueryClient({
      schema: {
        users,
        pets,
      },
    });

    const built = buildCompiled(
      db
        .select()
        .from(users)
        .leftJoin(pets, eq(users.id, pets.owner_id))
        .orderBy(users.id, desc(pets.id))
        [compileQuerySymbol](),
    );

    expect(normalizeSql(built.query)).toContain(
      "select `users`.`id` as `__orm_1`, `users`.`name` as `__orm_2`, `pets`.`id` as `__orm_3`, `pets`.`owner_id` as `__orm_4`, `pets`.`pet_name` as `__orm_5`",
    );
    expect(normalizeSql(built.query)).toContain("left join `pets` on `users`.`id` = `pets`.`owner_id`");
    expect(normalizeSql(built.query)).toContain("order by `users`.`id` ASC, `pets`.`id` DESC");
  });

  it("propagates forcedSettings through nested subqueries, CTEs and predicate subqueries", function testForcedSettingsPropagation() {
    const joinedScope = publicDb
      .select({
        userId: users.id,
        petId: pets.id,
      })
      .from(users)
      .leftJoin(pets, eq(users.id, pets.owner_id));

    const joinedSubquery = joinedScope.as("joined_scope");
    const fromSubquery = publicDb
      .select({
        userId: joinedSubquery.userId,
      })
      .from(joinedSubquery)
      [compileQuerySymbol]();

    expect(fromSubquery.forcedSettings).toEqual({
      join_use_nulls: 1,
    });

    const joinedCte = publicDb.$with("joined_cte").as(joinedScope);
    const fromCte = publicDb
      .with(joinedCte)
      .select({
        userId: joinedCte.userId,
      })
      .from(joinedCte)
      [compileQuerySymbol]();

    expect(fromCte.forcedSettings).toEqual({
      join_use_nulls: 1,
    });

    const existsQuery = publicDb
      .select({
        userId: users.id,
      })
      .from(users)
      .where(exists(joinedScope.as("joined_exists")))
      [compileQuerySymbol]();

    expect(existsQuery.forcedSettings).toEqual({
      join_use_nulls: 1,
    });

    const inArrayQuery = publicDb
      .select({
        userId: users.id,
      })
      .from(users)
      .where(
        inArray(
          users.id,
          publicDb
            .select({
              userId: users.id,
            })
            .from(users)
            .leftJoin(pets, eq(users.id, pets.owner_id))
            .as("joined_user_ids"),
        ),
      )
      [compileQuerySymbol]();

    expect(inArrayQuery.forcedSettings).toEqual({
      join_use_nulls: 1,
    });
  });

  it("rejects conflicting forcedSettings while composing nested queries", function testForcedSettingsConflict() {
    const conflictingNestedQuery = {
      [compileWithContextSymbol]() {
        return {
          kind: "compiled-query" as const,
          mode: "query" as const,
          statement: "select 1 as forced_setting_probe",
          params: {},
          selection: [],
          forcedSettings: {
            join_use_nulls: 0 as const,
          },
        };
      },
    } as unknown as ReturnType<typeof publicDb.select>;

    expect(() =>
      publicDb
        .select({
          userId: users.id,
        })
        .from(users)
        .leftJoin(pets, eq(users.id, pets.owner_id))
        .where(exists(conflictingNestedQuery))
        [compileQuerySymbol](),
    ).toThrow('Conflicting forced setting "join_use_nulls"');
  });
});

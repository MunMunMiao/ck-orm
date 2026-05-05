import { expect, it } from "bun:test";
import { ck, ckAlias, ckSql, ckTable, ckType, fn, type Predicate } from "./ck-orm";
import { createE2EDb, createTempTableName, pets, rewardEvents, users, webEvents } from "./shared";
import { describeE2E } from "./test-helpers";

describeE2E("ck-orm e2e count and dynamic filters", function describeCountAndDynamicFilters() {
  it("supports drizzle-style db.count() across tables, subqueries, CTEs and scalar selections", async function testCountScenarios() {
    const db = createE2EDb();

    expect(await db.count(users)).toBe(5_000);
    expect(await db.count(users).toUnsafe()).toBe(5_000);
    expect(await db.count(users).toSafe()).toBe("5000");
    expect(await db.count(users).toMixed()).toBe("5000");
    expect(await db.count(users, ck.gt(users.id, 10), ck.lte(users.id, 20))).toBe(10);

    const groupedUsers = db
      .select({
        userId: webEvents.user_id,
      })
      .from(webEvents)
      .groupBy(webEvents.user_id)
      .orderBy(webEvents.user_id)
      .limit(25)
      .offset(10)
      .as("grouped_users");

    expect(await db.count(groupedUsers)).toBe(25);

    const vipUsers = db.$with("vip_users").as(
      db
        .select({
          userId: users.id,
        })
        .from(users)
        .where(ck.eq(users.tier, "vip"))
        .orderBy(users.id)
        .limit(20),
    );

    expect(await db.with(vipUsers).count(vipUsers)).toBe(20);

    const petCounts = await db
      .select({
        id: users.id,
        petCount: db.count(pets, ck.eq(pets.owner_id, users.id)).as("pet_count"),
        safePetCount: db.count(pets, ck.eq(pets.owner_id, users.id)).toSafe().as("safe_pet_count"),
      })
      .from(users)
      .where(ck.lte(users.id, 3))
      .orderBy(users.id);

    expect(petCounts).toEqual([
      { id: 1, petCount: 2, safePetCount: "2" },
      { id: 2, petCount: 2, safePetCount: "2" },
      { id: 3, petCount: 2, safePetCount: "2" },
    ]);
  });

  it("supports dynamic filters with optional and predicate-array composition", async function testDynamicFilters() {
    const db = createE2EDb();

    const optionalRows = await db
      .select({
        id: users.id,
        tier: users.tier,
      })
      .from(users)
      .where(ck.and(ck.gt(users.id, 2), ck.lte(users.id, 5), undefined))
      .orderBy(users.id);

    expect(optionalRows).toEqual([
      { id: 3, tier: "trial" },
      { id: 4, tier: "standard" },
      { id: 5, tier: "trial" },
    ]);

    const emptyPredicates: Predicate[] = [];
    const defaultRows = await db
      .select({
        id: users.id,
      })
      .from(users)
      .where(...emptyPredicates)
      .orderBy(users.id)
      .limit(2);

    expect(defaultRows).toEqual([{ id: 1 }, { id: 2 }]);

    const predicates: Predicate[] = [];
    predicates.push(ck.or(ck.eq(users.id, 1), ck.eq(users.id, 4001)));
    predicates.push(
      ck.exists(
        db
          .select({
            ownerId: pets.owner_id,
          })
          .from(pets)
          .where(ck.eq(pets.owner_id, users.id))
          .limit(1)
          .as("owned_pets"),
      ),
    );

    const helperRows = await db
      .select({
        id: users.id,
      })
      .from(users)
      .where(...predicates)
      .orderBy(users.id);

    expect(helperRows).toEqual([{ id: 1 }]);
  });

  it("supports counting session temporary tables and final subqueries", async function testSessionScopedCounts() {
    const db = createE2EDb();
    const tempTableName = createTempTableName("count_scope");
    const tempScope = ckTable(
      tempTableName,
      {
        user_id: ckType.int32(),
      },
      (table) => ({
        engine: "MergeTree",
        orderBy: [table.user_id],
      }),
    );

    const result = await db.runInSession(async (session) => {
      await session.createTemporaryTable(tempScope);
      await session.insertJsonEachRow(tempScope, [{ user_id: 1 }, { user_id: 2 }, { user_id: 3 }]);

      const scopedTempUsers = session
        .select({
          userId: tempScope.user_id,
        })
        .from(tempScope)
        .where(ck.gt(tempScope.user_id, 1))
        .as("scoped_temp_users");

      const activeRewards = session
        .select({
          userId: rewardEvents.user_id,
        })
        .from(rewardEvents)
        .final()
        .where(ck.eq(rewardEvents._peerdb_is_deleted, 0))
        .limit(10)
        .as("active_rewards");

      return {
        tempTotal: await session.count(tempScope),
        scopedTotal: await session.count(scopedTempUsers),
        rewardTotal: await session.count(activeRewards),
      };
    });

    expect(result).toEqual({
      tempTotal: 3,
      scopedTotal: 2,
      rewardTotal: 10,
    });
  });

  it("supports aliased FINAL sources with session temp-table joins and array lambdas", async function testFinalAliasTempJoinArrayLambda() {
    const db = createE2EDb();
    const tempTableName = createTempTableName("final_scope");
    const tempScope = ckTable(tempTableName, {
      user_id: ckType.string(),
      start_id_list: ckType.array(ckType.int64()),
      end_id_list: ckType.array(ckType.int64()),
    });

    const rows = await db.runInSession(async (session) => {
      await session.createTemporaryTable(tempScope);
      await session.insertJsonEachRow(tempScope, [
        {
          user_id: "user_1",
          start_id_list: [1],
          end_id_list: [5000],
        },
      ]);

      const rewardLog = ckAlias(rewardEvents, "reward_events_final");
      return await session
        .select({
          rewardId: rewardLog.id,
          userId: rewardLog.user_id,
        })
        .from(rewardLog)
        .innerJoin(tempScope, ck.eq(rewardLog.user_id, tempScope.user_id))
        .where(
          ck.eq(rewardLog._peerdb_is_deleted, 0),
          fn.arrayExists(
            ckSql`(start_id, end_id) -> ${rewardLog.id} >= start_id AND ${rewardLog.id} <= end_id`,
            tempScope.start_id_list,
            tempScope.end_id_list,
          ),
        )
        .final()
        .orderBy(rewardLog.id)
        .limit(1);
    });

    expect(rows).toEqual([
      {
        rewardId: 1,
        userId: "user_1",
      },
    ]);
  });

  it("supports aliased FINAL sources with temp joins, array lambdas, aggregation and iterators", async function testFinalAliasTempJoinAggregateIterator() {
    const db = createE2EDb();
    const tempTableName = createTempTableName("final_aggregate_scope");
    const tempScope = ckTable(tempTableName, {
      user_id: ckType.string(),
      start_id_list: ckType.array(ckType.int64()),
      end_id_list: ckType.array(ckType.int64()),
    });

    const rows = await db.runInSession(async (session) => {
      await session.createTemporaryTable(tempScope);
      await session.insertJsonEachRow(tempScope, [
        {
          user_id: "user_1",
          start_id_list: [1],
          end_id_list: [10_001],
        },
        {
          user_id: "user_2",
          start_id_list: [2],
          end_id_list: [10_002],
        },
      ]);

      const rewardLog = ckAlias(rewardEvents, "reward_events_aggregate_final");
      const iterator = session
        .select({
          userId: rewardLog.user_id,
          rewardCount: fn.count().as("reward_count"),
          totalRewardPoints: fn.sum(rewardLog.reward_points).as("total_reward_points"),
        })
        .from(rewardLog)
        .innerJoin(tempScope, ck.eq(rewardLog.user_id, tempScope.user_id))
        .where(
          ck.eq(rewardLog._peerdb_is_deleted, 0),
          fn.arrayExists(
            ckSql`(start_id, end_id) -> ${rewardLog.id} >= start_id AND ${rewardLog.id} <= end_id`,
            tempScope.start_id_list,
            tempScope.end_id_list,
          ),
        )
        .final()
        .groupBy(rewardLog.user_id)
        .having(ck.gt(fn.count(), 0))
        .orderBy(rewardLog.user_id)
        .iterator();

      const result: Array<{
        userId: string;
        rewardCount: number;
        totalRewardPoints: string;
      }> = [];
      for await (const row of iterator) {
        result.push(row);
      }
      return result;
    });

    expect(rows).toHaveLength(2);
    expect(rows.map((row) => row.userId)).toEqual(["user_1", "user_2"]);
    for (const row of rows) {
      expect(row.rewardCount).toBeGreaterThan(0);
      expect(row.totalRewardPoints).toMatch(/^-?\d+(?:\.\d+)?$/);
    }
  });
});

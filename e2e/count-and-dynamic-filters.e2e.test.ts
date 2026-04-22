import { expect, it } from "bun:test";
import { and, chTable, eq, exists, gt, int32, lte, or, type Predicate } from "./ck-orm";
import { createE2EDb, createTempTableName, pets, rewardEvents, users, webEvents } from "./shared";
import { describeE2E } from "./test-helpers";

describeE2E("ck-orm e2e count and dynamic filters", function describeCountAndDynamicFilters() {
  it("supports drizzle-style db.count() across tables, subqueries, CTEs and scalar selections", async function testCountScenarios() {
    const db = createE2EDb();

    expect(await db.count(users)).toBe(5_000);
    expect(await db.count(users, gt(users.id, 10), lte(users.id, 20))).toBe(10);

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
        .where(eq(users.tier, "vip"))
        .orderBy(users.id)
        .limit(20),
    );

    expect(await db.with(vipUsers).count(vipUsers)).toBe(20);

    const petCounts = await db
      .select({
        id: users.id,
        petCount: db.count(pets, eq(pets.owner_id, users.id)).as("pet_count"),
      })
      .from(users)
      .where(lte(users.id, 3))
      .orderBy(users.id);

    expect(petCounts).toEqual([
      { id: 1, petCount: 2 },
      { id: 2, petCount: 2 },
      { id: 3, petCount: 2 },
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
      .where(and(gt(users.id, 2), lte(users.id, 5), undefined))
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
    predicates.push(or(eq(users.id, 1), eq(users.id, 4001)));
    predicates.push(
      exists(
        db
          .select({
            ownerId: pets.owner_id,
          })
          .from(pets)
          .where(eq(pets.owner_id, users.id))
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
    const tempScope = chTable(
      tempTableName,
      {
        user_id: int32(),
      },
      (table) => ({
        engine: "MergeTree",
        orderBy: [table.user_id],
      }),
    );

    const result = await db.runInSession(async (sessionDb) => {
      await sessionDb.createTemporaryTable(tempTableName, "(user_id Int32)");
      await sessionDb.insertJsonEachRow(tempTableName, [{ user_id: 1 }, { user_id: 2 }, { user_id: 3 }]);

      const scopedTempUsers = sessionDb
        .select({
          userId: tempScope.user_id,
        })
        .from(tempScope)
        .where(gt(tempScope.user_id, 1))
        .as("scoped_temp_users");

      const activeRewards = sessionDb
        .select({
          userId: rewardEvents.user_id,
        })
        .from(rewardEvents)
        .final()
        .where(eq(rewardEvents._peerdb_is_deleted, 0))
        .limit(10)
        .as("active_rewards");

      return {
        tempTotal: await sessionDb.count(tempScope),
        scopedTotal: await sessionDb.count(scopedTempUsers),
        rewardTotal: await sessionDb.count(activeRewards),
      };
    });

    expect(result).toEqual({
      tempTotal: 3,
      scopedTotal: 2,
      rewardTotal: 10,
    });
  });
});

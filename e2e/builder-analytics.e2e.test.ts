import { expect, it } from "bun:test";
import { alias, and, desc, eq, exists, fn, inArray, lt, lte } from "./ck-orm";
import { createE2EDb, rewardEvents, users, webEvents } from "./shared";
import { describeE2E } from "./test-helpers";

describeE2E("ck-orm e2e builder analytics", function describeBuilderAnalytics() {
  it("supports with/$with and subquery.as for multi-stage analytical queries", async function testCtesAndSubqueries() {
    const db = createE2EDb();
    const monthly = db.$with("monthly").as(
      db
        .select({
          month: fn.toStartOfMonth(webEvents.viewed_at).as("month"),
          userId: webEvents.user_id,
          totalRevenue: fn.sum(webEvents.revenue).as("total_revenue"),
        })
        .from(webEvents)
        .where(lt(webEvents.user_id, 6))
        .groupBy(fn.toStartOfMonth(webEvents.viewed_at), webEvents.user_id),
    );

    const topUsers = db
      .with(monthly)
      .select({
        month: monthly.month,
        userId: monthly.userId,
        totalRevenue: monthly.totalRevenue,
      })
      .from(monthly)
      .orderBy(monthly.userId)
      .limit(5)
      .as("top_users");

    const rows = await db
      .select({
        userId: topUsers.userId,
        name: users.name,
        totalRevenue: topUsers.totalRevenue,
      })
      .from(topUsers)
      .innerJoin(users, eq(users.id, topUsers.userId))
      .orderBy(topUsers.userId);

    expect(rows).toHaveLength(5);
    expect(rows[0]).toMatchObject({
      userId: 1,
      name: "alice",
    });
  });

  it("supports groupBy, having, orderBy, limit, offset and limitBy", async function testAnalyticalBuilderFunctions() {
    const db = createE2EDb();

    const groupedRows = await db
      .select({
        country: webEvents.country,
        deviceType: webEvents.device_type,
        totalRevenue: fn.sum(webEvents.revenue).as("total_revenue"),
      })
      .from(webEvents)
      .groupBy(webEvents.country, webEvents.device_type)
      .having(fn.call("greater", fn.sum(webEvents.revenue), 0))
      .orderBy(webEvents.country, webEvents.device_type)
      .limit(6)
      .offset(2);

    expect(groupedRows).toHaveLength(6);
    expect(groupedRows[0]?.country).toBeDefined();
    expect(groupedRows[0]?.deviceType).toBeDefined();
    expect(groupedRows[0]?.totalRevenue).toMatch(/^\d+\.\d+$/);

    const topPerCountry = await db
      .select({
        country: webEvents.country,
        userId: webEvents.user_id,
        revenue: webEvents.revenue,
      })
      .from(webEvents)
      .orderBy(webEvents.country, desc(webEvents.revenue), webEvents.user_id)
      .limitBy([webEvents.country], 2);

    expect(topPerCountry).toHaveLength(8);
    const countryBuckets = new Map<string, number>();
    for (const row of topPerCountry) {
      countryBuckets.set(row.country, (countryBuckets.get(row.country) ?? 0) + 1);
    }
    expect([...countryBuckets.values()].sort()).toEqual([2, 2, 2, 2]);
  });

  it("supports innerJoin, leftJoin, exists, inArray and final in builder flows", async function testBuilderJoinAndFinalFlows() {
    const db = createE2EDb();
    const rewardScope = db
      .select({
        userId: rewardEvents.user_id,
      })
      .from(rewardEvents)
      .final()
      .where(eq(rewardEvents._peerdb_is_deleted, 0))
      .limit(10)
      .as("reward_scope");

    const owner = alias(users, "owner");
    const innerRows = await db
      .select({
        ownerId: owner.id,
        ownerName: owner.name,
        deviceType: webEvents.device_type,
      })
      .from(owner)
      .innerJoin(webEvents, eq(owner.id, webEvents.user_id))
      .where(
        and(
          inArray(
            owner.id,
            db
              .select({ userId: webEvents.user_id })
              .from(webEvents)
              .where(lte(webEvents.user_id, 5))
              .limit(5)
              .as("scoped_event_users"),
          ),
          exists(db.select({ userId: rewardScope.userId }).from(rewardScope).limit(1)),
        ),
      )
      .orderBy(owner.id, webEvents.event_id)
      .limit(5);

    expect(innerRows).toHaveLength(5);
    expect(innerRows[0]).toEqual({
      ownerId: 1,
      ownerName: "alice",
      deviceType: "ios",
    });

    const finalRewardScope = db
      .select({
        userId: rewardEvents.user_id,
      })
      .from(rewardEvents)
      .final()
      .where(eq(rewardEvents._peerdb_is_deleted, 0))
      .as("final_reward_scope");

    const leftRows = await db
      .select({
        ownerId: owner.id,
        rewardUserId: finalRewardScope.userId,
      })
      .from(owner)
      .leftJoin(finalRewardScope, eq(owner.name, finalRewardScope.userId))
      .where(eq(owner.id, 4001))
      .limit(1);

    expect(leftRows[0]).toEqual({
      ownerId: 4001,
      rewardUserId: "user_4001",
    });
  });
});

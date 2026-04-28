import { expect, it } from "bun:test";
import { ck, ckAlias, csql, fn } from "./ck-orm";
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
        .where(ck.lt(webEvents.user_id, 6))
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
      .innerJoin(users, ck.eq(users.id, topUsers.userId))
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
      .orderBy(webEvents.country, ck.desc(webEvents.revenue), webEvents.user_id)
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
      .where(ck.eq(rewardEvents._peerdb_is_deleted, 0))
      .limit(10)
      .as("reward_scope");

    const owner = ckAlias(users, "owner");
    const innerRows = await db
      .select({
        ownerId: owner.id,
        ownerName: owner.name,
        deviceType: webEvents.device_type,
      })
      .from(owner)
      .innerJoin(webEvents, ck.eq(owner.id, webEvents.user_id))
      .where(
        ck.and(
          ck.inArray(
            owner.id,
            db
              .select({ userId: webEvents.user_id })
              .from(webEvents)
              .where(ck.lte(webEvents.user_id, 5))
              .limit(5)
              .as("scoped_event_users"),
          ),
          ck.exists(db.select({ userId: rewardScope.userId }).from(rewardScope).limit(1)),
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
      .where(ck.eq(rewardEvents._peerdb_is_deleted, 0))
      .as("final_reward_scope");

    const leftRows = await db
      .select({
        ownerId: owner.id,
        rewardUserId: finalRewardScope.userId,
      })
      .from(owner)
      .leftJoin(finalRewardScope, ck.eq(owner.name, finalRewardScope.userId))
      .where(ck.eq(owner.id, 4001))
      .limit(1);

    expect(leftRows[0]).toEqual({
      ownerId: 4001,
      rewardUserId: "user_4001",
    });
  });

  it("preserves Decimal precision via auto-cast aggregates, fn.toDecimal128 and csql.decimal", async function testDecimalPrecisionPaths() {
    const db = createE2EDb();

    const aggregateRows = await db
      .select({
        country: webEvents.country,
        autoSum: fn.sum(webEvents.revenue).as("auto_sum"),
        toDecimal: fn.toDecimal128(fn.sum(webEvents.revenue), 2).as("to_decimal"),
        rawCast: csql.decimal(csql`sum(${webEvents.revenue})`, 18, 2).as("raw_cast"),
      })
      .from(webEvents)
      .groupBy(webEvents.country)
      .orderBy(webEvents.country)
      .limit(2);

    expect(aggregateRows.length).toBeGreaterThan(0);
    for (const row of aggregateRows) {
      expect(typeof row.autoSum).toBe("string");
      expect(typeof row.toDecimal).toBe("string");
      expect(typeof row.rawCast).toBe("string");
      expect(row.autoSum).toMatch(/^-?\d+(?:\.\d+)?$/);
      expect(row.toDecimal).toMatch(/^-?\d+(?:\.\d+)?$/);
      expect(row.rawCast).toMatch(/^-?\d+(?:\.\d+)?$/);
      expect(row.autoSum).toBe(row.rawCast);
    }

    const columnCastRows = await db
      .select({
        eventId: webEvents.event_id,
        widened: webEvents.revenue.cast(20, 5).as("widened"),
      })
      .from(webEvents)
      .orderBy(webEvents.event_id)
      .limit(3);

    expect(columnCastRows.length).toBeGreaterThan(0);
    for (const row of columnCastRows) {
      expect(typeof row.widened).toBe("string");
      expect(row.widened).toMatch(/^-?\d+(?:\.\d+)?$/);
    }

    const minMaxRow = await db
      .select({
        userId: rewardEvents.user_id,
        minRewardPoints: fn.min(rewardEvents.reward_points).as("min_reward_points"),
        maxRewardPoints: fn.max(rewardEvents.reward_points).as("max_reward_points"),
      })
      .from(rewardEvents)
      .groupBy(rewardEvents.user_id)
      .orderBy(rewardEvents.user_id)
      .limit(1);

    expect(minMaxRow).toHaveLength(1);
    expect(typeof minMaxRow[0]?.minRewardPoints).toBe("string");
    expect(typeof minMaxRow[0]?.maxRewardPoints).toBe("string");
    expect(minMaxRow[0]?.minRewardPoints).toMatch(/^-?\d+(?:\.\d+)?$/);
  });
});

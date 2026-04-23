import { expect, it } from "bun:test";
import { ck, fn } from "./ck-orm";
import { createE2EDb, users, webEvents } from "./shared";
import { describeE2E, expectDate, expectPresent } from "./test-helpers";

describeE2E("ck-orm e2e functions", function describeFunctions() {
  it("supports fn.call, fn.withParams and basic type-conversion helpers", async function testGenericAndConversionFunctions() {
    const db = createE2EDb();

    const [row] = await db
      .select({
        idText: fn.toString(users.id).as("id_text"),
        upperName: fn
          .call<string>("upper", users.name)
          .mapWith((value) => String(value))
          .as("upper_name"),
        createdAtDate: fn.toDate(users.created_at).as("created_at_date"),
        createdAtTime: fn.toDateTime(users.created_at).as("created_at_time"),
      })
      .from(users)
      .where(ck.eq(users.id, 1));

    const presentRow = expectPresent(row, "conversion row");
    expect(presentRow).toEqual({
      idText: "1",
      upperName: "ALICE",
      createdAtDate: presentRow.createdAtDate,
      createdAtTime: presentRow.createdAtTime,
    });
    expectDate(presentRow.createdAtDate);
    expectDate(presentRow.createdAtTime);

    const [quantileRow] = await db
      .select({
        medianUserId: fn
          .withParams<number>("quantile", [0.5], users.id)
          .mapWith((value) => Number(value))
          .as("median_user_id"),
      })
      .from(users);

    const presentQuantileRow = expectPresent(quantileRow, "quantileRow");
    expect(presentQuantileRow.medianUserId).toBeGreaterThan(2000);
    expect(presentQuantileRow.medianUserId).toBeLessThan(3000);
  });

  it("supports aggregate helpers and month bucketing helpers", async function testAggregateHelpers() {
    const db = createE2EDb();

    const [row] = await db
      .select({
        eventCount: fn.count(webEvents.event_id).as("event_count"),
        usEventCount: fn.countIf(ck.eq(webEvents.country, "US")).as("us_event_count"),
        totalRevenue: fn.sum(webEvents.revenue).as("total_revenue"),
        totalRevenueUs: fn.sumIf(webEvents.revenue, ck.eq(webEvents.country, "US")).as("total_revenue_us"),
        avgRevenue: fn.avg(webEvents.event_id).as("avg_event_id"),
        minEventId: fn.min(webEvents.event_id).as("min_event_id"),
        maxEventId: fn.max(webEvents.event_id).as("max_event_id"),
        uniqueUsers: fn.uniqExact(webEvents.user_id).as("unique_users"),
      })
      .from(webEvents);

    const presentAggregateRow = expectPresent(row, "aggregate row");
    expect(presentAggregateRow.eventCount).toBe("100000");
    expect(presentAggregateRow.usEventCount).toBe("25000");
    expect(presentAggregateRow.totalRevenue).toMatch(/^\d+\.\d+$/);
    expect(presentAggregateRow.totalRevenueUs).toMatch(/^\d+\.\d+$/);
    expect(presentAggregateRow.avgRevenue).toBeGreaterThan(50_000);
    expect(presentAggregateRow.minEventId).toBe(1n);
    expect(presentAggregateRow.maxEventId).toBe(100000n);
    expect(presentAggregateRow.uniqueUsers).toBe("5000");

    const monthBucket = fn.toStartOfMonth(webEvents.viewed_at).as("month");

    const [monthRow] = await db
      .select({
        firstViewedAt: fn.min(webEvents.viewed_at).as("first_viewed_at"),
        lastViewedAt: fn.max(webEvents.viewed_at).as("last_viewed_at"),
        month: monthBucket,
      })
      .from(webEvents)
      .groupBy(monthBucket)
      .orderBy(monthBucket)
      .limit(1);

    const presentMonthRow = expectPresent(monthRow, "monthRow");
    expectDate(presentMonthRow.firstViewedAt);
    expectDate(presentMonthRow.lastViewedAt);
    expectDate(presentMonthRow.month);
    expect(presentMonthRow.firstViewedAt.getTime()).toBeLessThan(presentMonthRow.lastViewedAt.getTime());
  });

  it("supports fn.coalesce, fn.tuple, fn.arrayZip and fn.not", async function testCompositeFunctions() {
    const db = createE2EDb();

    const [row] = await db
      .select({
        safeTier: fn.coalesce(users.tier, ck.sql.raw(`'missing'`)).as("safe_tier"),
        tupleValue: fn.tuple(users.id, users.name).as("tuple_value"),
        zippedTags: fn.arrayZip(webEvents.tags, webEvents.tag_scores).as("zipped_tags"),
        isNotVip: fn.not(ck.eq(users.tier, "vip")).as("is_not_vip"),
      })
      .from(users)
      .innerJoin(webEvents, ck.eq(users.id, webEvents.user_id))
      .where(ck.eq(users.id, 1))
      .orderBy(webEvents.event_id)
      .limit(1);

    const presentCompositeRow = expectPresent(row, "composite row");
    expect(presentCompositeRow.safeTier).toBe("vip");
    expect(presentCompositeRow.tupleValue).toEqual([1, "alice"]);
    expect(presentCompositeRow.zippedTags).toEqual([
      ["tag_0", 1],
      ["segment_0", 4],
    ]);
    expect(presentCompositeRow.isNotVip).toBe(false);
  });

  it("supports tableFn.call against the numbers table function", async function testTableFunction() {
    const db = createE2EDb();
    const numbers = fn.table.call("numbers", 5).as("n");

    const rows = await db
      .select({
        value: ck.expr(ck.sql<bigint>`number`.mapWith((value) => BigInt(String(value)))),
      })
      .from(numbers)
      .orderBy(ck.expr(ck.sql`number`));

    expect(rows).toEqual([{ value: 0 }, { value: 1 }, { value: 2 }, { value: 3 }, { value: 4 }]);
  });
});

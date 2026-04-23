import { expect, it } from "bun:test";
import { chType, ck, csql, fn } from "./ck-orm";
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
    expect(presentAggregateRow.minEventId).toBe("1");
    expect(presentAggregateRow.maxEventId).toBe("100000");
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
        safeTier: fn.coalesce(users.tier, csql`'missing'`).as("safe_tier"),
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

  it("supports typed JSONExtract, arrayJoin and array helper functions", async function testStructuredFunctionHelpers() {
    const db = createE2EDb();
    const payload = JSON.stringify({
      account: {
        audits: [
          {
            region: "EU",
          },
          {
            region: null,
          },
        ],
        tags: ["vip", "pro"],
        score: 12.5,
      },
      orders: [
        {
          ticket: 10001,
        },
        {
          ticket: 10002,
        },
      ],
    });

    const [jsonRow] = await db.select({
      tags: fn.jsonExtract(payload, chType.array(chType.string()), "account", "tags").as("tags"),
      score: fn.jsonExtract(payload, chType.float64(), "account", "score").as("score"),
      firstTicket: fn.jsonExtract(payload, chType.int64(), "orders", 1, "ticket").as("first_ticket"),
      secondTicket: fn.jsonExtract(payload, chType.int64(), "orders", 2, "ticket").as("second_ticket"),
      nullableRegion: fn
        .jsonExtract(payload, chType.nullable(chType.string()), "account", "audits", 2, "region")
        .as("nullable_region"),
    });

    expect(expectPresent(jsonRow, "json row")).toEqual({
      tags: ["vip", "pro"],
      score: 12.5,
      firstTicket: "10001",
      secondTicket: "10002",
      nullableRegion: null,
    });

    const [arrayRow] = await db.select({
      concat: fn.arrayConcat<string>(["vip"], ["pro"]).as("concat"),
      secondItem: fn.arrayElement<string>(fn.array("vip", "pro"), 2).as("second_item"),
      missingItem: fn.arrayElementOrNull<string>(fn.array("vip"), 2).as("missing_item"),
      slice: fn.arraySlice<string>(["vip", "pro", "raw"], 2, 2).as("slice"),
      openEndedSlice: fn.arraySlice<string>(["vip", "pro", "raw"], 2).as("open_ended_slice"),
      flattened: fn.arrayFlatten<string>([["vip"], ["pro"]]).as("flattened"),
      intersected: fn.arrayIntersect<string>(["vip", "pro"], ["pro", "raw"]).as("intersected"),
      proIndex: fn.indexOf(["vip", "pro"], "pro").as("pro_index"),
      tagCount: fn.length(["vip", "pro"]).as("tag_count"),
      hasTags: fn.notEmpty(["vip"]).as("has_tags"),
    });

    expect(expectPresent(arrayRow, "array row")).toEqual({
      concat: ["vip", "pro"],
      secondItem: "pro",
      missingItem: null,
      slice: ["pro", "raw"],
      openEndedSlice: ["pro", "raw"],
      flattened: ["vip", "pro"],
      intersected: ["pro"],
      proIndex: "2",
      tagCount: "2",
      hasTags: true,
    });

    const targetOrderTuples = db.$with("target_order_tuples").as(
      db.select({
        targetOrder: fn.arrayJoin(fn.arrayZip([10001, 10002], [9001, 9002])).as("target_order"),
      }),
    );

    const tupleRows = await db
      .with(targetOrderTuples)
      .select({
        orderTicket: fn.tupleElement<string>(targetOrderTuples.targetOrder, 1).as("order_ticket"),
        login: fn.tupleElement<string>(targetOrderTuples.targetOrder, 2).as("login"),
      })
      .from(targetOrderTuples);

    expect(tupleRows).toEqual([
      {
        orderTicket: "10001",
        login: "9001",
      },
      {
        orderTicket: "10002",
        login: "9002",
      },
    ]);

    const [tupleElementRow] = await db.select({
      namedValue: fn
        .tupleElement<string>(csql`CAST(('alice', 7), 'Tuple(name String, score UInt8)')`, "name")
        .as("named_value"),
      defaultedValue: fn.tupleElement<string>(csql`tuple('only')`, 2, "fallback").as("defaulted_value"),
    });

    expect(expectPresent(tupleElementRow, "tuple element row")).toEqual({
      namedValue: "alice",
      defaultedValue: "fallback",
    });

    const emptyJoinRows = await db.select({
      value: fn.arrayJoin<string>([]).as("value"),
    });

    expect(emptyJoinRows).toEqual([]);
  });

  it("supports tableFn.call against the numbers table function", async function testTableFunction() {
    const db = createE2EDb();
    const numbers = fn.table.call("numbers", 5).as("n");

    const rows = await db
      .select({
        value: ck.expr(csql<bigint>`number`.mapWith((value) => BigInt(String(value)))),
      })
      .from(numbers)
      .orderBy(ck.expr(csql`number`));

    expect(rows).toEqual([{ value: 0n }, { value: 1n }, { value: 2n }, { value: 3n }, { value: 4n }]);
  });
});

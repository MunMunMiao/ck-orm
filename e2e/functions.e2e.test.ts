import { expect, it } from "bun:test";
import { ck, ckSql, ckType, fn } from "./ck-orm";
import { createE2EDb, schemaPrimitives, users, webEvents } from "./shared";
import { describeE2E, expectDate, expectPresent, expectRejectsWithClickhouseError } from "./test-helpers";

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
        createdAtDate32: fn.toDate32(users.created_at).as("created_at_date32"),
        createdAtTime: fn.toDateTime(users.created_at).as("created_at_time"),
        createdAtTime32: fn.toDateTime32(users.created_at, "UTC").as("created_at_time32"),
        createdAtTime64: fn.toDateTime64(users.created_at, 3, "UTC").as("created_at_time64"),
        createdAtUnix: fn.toUnixTimestamp(users.created_at, "UTC").as("created_at_unix"),
        createdAtUnix64Second: fn.toUnixTimestamp64Second(users.created_at).as("created_at_unix64_second"),
        createdAtUnix64Milli: fn.toUnixTimestamp64Milli(users.created_at).as("created_at_unix64_milli"),
        createdAtUnix64Micro: fn.toUnixTimestamp64Micro(users.created_at).as("created_at_unix64_micro"),
        createdAtUnix64Nano: fn.toUnixTimestamp64Nano(users.created_at).as("created_at_unix64_nano"),
        fromUnixTimestamp: fn.fromUnixTimestamp(fn.toUnixTimestamp(users.created_at, "UTC")).as("from_unix_timestamp"),
        formattedUnixTimestamp: fn
          .fromUnixTimestamp(fn.toUnixTimestamp(users.created_at, "UTC"), "%Y-%m-%d", "UTC")
          .as("formatted_unix_timestamp"),
        roundTripSecond: fn
          .fromUnixTimestamp64Second(fn.toUnixTimestamp64Second(users.created_at), "UTC")
          .as("round_trip_second"),
        roundTripMilli: fn
          .fromUnixTimestamp64Milli(fn.toUnixTimestamp64Milli(users.created_at), "UTC")
          .as("round_trip_milli"),
        roundTripMicro: fn
          .fromUnixTimestamp64Micro(fn.toUnixTimestamp64Micro(users.created_at), "UTC")
          .as("round_trip_micro"),
        roundTripNano: fn
          .fromUnixTimestamp64Nano(fn.toUnixTimestamp64Nano(users.created_at), "UTC")
          .as("round_trip_nano"),
      })
      .from(users)
      .where(ck.eq(users.id, 1));

    const presentRow = expectPresent(row, "conversion row");
    expect(presentRow).toEqual({
      idText: "1",
      upperName: "ALICE",
      createdAtDate: presentRow.createdAtDate,
      createdAtDate32: presentRow.createdAtDate32,
      createdAtTime: presentRow.createdAtTime,
      createdAtTime32: presentRow.createdAtTime32,
      createdAtTime64: presentRow.createdAtTime64,
      createdAtUnix: presentRow.createdAtUnix,
      createdAtUnix64Second: presentRow.createdAtUnix64Second,
      createdAtUnix64Milli: presentRow.createdAtUnix64Milli,
      createdAtUnix64Micro: presentRow.createdAtUnix64Micro,
      createdAtUnix64Nano: presentRow.createdAtUnix64Nano,
      fromUnixTimestamp: presentRow.fromUnixTimestamp,
      formattedUnixTimestamp: "2026-01-01",
      roundTripSecond: presentRow.roundTripSecond,
      roundTripMilli: presentRow.roundTripMilli,
      roundTripMicro: presentRow.roundTripMicro,
      roundTripNano: presentRow.roundTripNano,
    });
    expectDate(presentRow.createdAtDate);
    expectDate(presentRow.createdAtDate32);
    expectDate(presentRow.createdAtTime);
    expectDate(presentRow.createdAtTime32);
    expectDate(presentRow.createdAtTime64);
    expectDate(presentRow.fromUnixTimestamp);
    expectDate(presentRow.roundTripSecond);
    expectDate(presentRow.roundTripMilli);
    expectDate(presentRow.roundTripMicro);
    expectDate(presentRow.roundTripNano);
    expect(presentRow.createdAtUnix).toBe(1767225600);
    expect(presentRow.createdAtUnix64Second).toBe("1767225600");
    expect(presentRow.createdAtUnix64Milli).toBe("1767225600000");
    expect(presentRow.createdAtUnix64Micro).toBe("1767225600000000");
    expect(presentRow.createdAtUnix64Nano).toBe("1767225600000000000");
    expect(presentRow.fromUnixTimestamp.getTime()).toBe(presentRow.createdAtTime.getTime());
    expect(presentRow.roundTripSecond.getTime()).toBe(presentRow.createdAtTime.getTime());
    expect(presentRow.roundTripMilli.getTime()).toBe(presentRow.createdAtTime.getTime());
    expect(presentRow.roundTripMicro.getTime()).toBe(presentRow.createdAtTime.getTime());
    expect(presentRow.roundTripNano.getTime()).toBe(presentRow.createdAtTime.getTime());

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
    expect(presentAggregateRow.eventCount).toBe(100000);
    expect(presentAggregateRow.usEventCount).toBe(25000);
    expect(presentAggregateRow.totalRevenue).toMatch(/^\d+\.\d+$/);
    expect(presentAggregateRow.totalRevenueUs).toMatch(/^\d+\.\d+$/);
    expect(presentAggregateRow.avgRevenue).toBeGreaterThan(50_000);
    expect(presentAggregateRow.minEventId).toBe("1");
    expect(presentAggregateRow.maxEventId).toBe("100000");
    expect(presentAggregateRow.uniqueUsers).toBe(5000);

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

  it("supports fn.count, fn.countIf and fn.uniqExact chainable modes (toUnsafe/toSafe/toMixed)", async function testCountSelectionModes() {
    const db = createE2EDb();

    const [row] = await db
      .select({
        defaultEventCount: fn.count(webEvents.event_id).as("default_event_count"),
        unsafeEventCount: fn.count(webEvents.event_id).toUnsafe().as("unsafe_event_count"),
        safeEventCount: fn.count(webEvents.event_id).toSafe().as("safe_event_count"),
        mixedEventCount: fn.count(webEvents.event_id).toMixed().as("mixed_event_count"),
        defaultUsCount: fn.countIf(ck.eq(webEvents.country, "US")).as("default_us_count"),
        safeUsCount: fn.countIf(ck.eq(webEvents.country, "US")).toSafe().as("safe_us_count"),
        mixedUsCount: fn.countIf(ck.eq(webEvents.country, "US")).toMixed().as("mixed_us_count"),
        defaultUniqUsers: fn.uniqExact(webEvents.user_id).as("default_uniq_users"),
        safeUniqUsers: fn.uniqExact(webEvents.user_id).toSafe().as("safe_uniq_users"),
        mixedUniqUsers: fn.uniqExact(webEvents.user_id).toMixed().as("mixed_uniq_users"),
      })
      .from(webEvents);

    const presentRow = expectPresent(row, "count modes row");
    // unsafe / default → number
    expect(presentRow.defaultEventCount).toBe(100000);
    expect(presentRow.unsafeEventCount).toBe(100000);
    expect(presentRow.defaultUsCount).toBe(25000);
    expect(presentRow.defaultUniqUsers).toBe(5000);
    // safe → string
    expect(presentRow.safeEventCount).toBe("100000");
    expect(presentRow.safeUsCount).toBe("25000");
    expect(presentRow.safeUniqUsers).toBe("5000");
    // mixed → string under default lossless 64-bit JSON settings
    expect(presentRow.mixedEventCount).toBe("100000");
    expect(presentRow.mixedUsCount).toBe("25000");
    expect(presentRow.mixedUniqUsers).toBe("5000");

    // The chosen mode controls the SQL semantics: count(...) used in HAVING with a numeric literal
    // requires a numeric (default/unsafe) or string-castable variant. Default mode here pairs with
    // groupBy-having to filter only groups that have rows.
    const groupedRows = await db
      .select({
        country: webEvents.country,
        eventCount: fn.count(webEvents.event_id).as("event_count"),
      })
      .from(webEvents)
      .groupBy(webEvents.country)
      .having(ck.gt(fn.count(webEvents.event_id), 1))
      .orderBy(ck.desc(fn.count(webEvents.event_id)))
      .limit(3);

    expect(groupedRows.length).toBeGreaterThan(0);
    for (const groupedRow of groupedRows) {
      expect(typeof groupedRow.eventCount).toBe("number");
      expect(groupedRow.eventCount).toBeGreaterThan(1);
    }

    // toSafe() embedded as a sub-expression keeps its String SQL semantics; comparing with a
    // string literal works at the SQL level (lexicographic), so we instead route through Number()
    // on the decoded result to confirm the safe-decoded shape.
    const [safeAggregateRow] = await db
      .select({
        safeEventCount: fn.count(webEvents.event_id).toSafe().as("safe_event_count"),
        mixedEventCount: fn.count(webEvents.event_id).toMixed().as("mixed_event_count"),
      })
      .from(webEvents)
      .where(ck.eq(webEvents.country, "US"));

    const presentSafeRow = expectPresent(safeAggregateRow, "safe aggregate row");
    expect(presentSafeRow.safeEventCount).toBe("25000");
    expect(presentSafeRow.mixedEventCount).toBe("25000");
    expect(Number(presentSafeRow.safeEventCount)).toBe(25000);

    // fn.uniqExact embedded as a HAVING / ORDER BY operand exercises the wrapped SQL
    // (toFloat64(uniqExact(...))) end-to-end against ClickHouse.
    const groupedUniqRows = await db
      .select({
        country: webEvents.country,
        uniqUsers: fn.uniqExact(webEvents.user_id).as("uniq_users"),
        uniqUsersExact: fn.uniqExact(webEvents.user_id).toSafe().as("uniq_users_exact"),
      })
      .from(webEvents)
      .groupBy(webEvents.country)
      .having(ck.gt(fn.uniqExact(webEvents.user_id), 1))
      .orderBy(ck.desc(fn.uniqExact(webEvents.user_id)))
      .limit(3);

    expect(groupedUniqRows.length).toBeGreaterThan(0);
    for (const groupedRow of groupedUniqRows) {
      expect(typeof groupedRow.uniqUsers).toBe("number");
      expect(groupedRow.uniqUsers).toBeGreaterThan(1);
      expect(typeof groupedRow.uniqUsersExact).toBe("string");
      expect(Number(groupedRow.uniqUsersExact)).toBe(groupedRow.uniqUsers);
    }
  });

  it("supports fn.coalesce, fn.tuple, fn.arrayZip and fn.not", async function testCompositeFunctions() {
    const db = createE2EDb();

    const [row] = await db
      .select({
        safeTier: fn.coalesce(users.tier, ckSql`'missing'`).as("safe_tier"),
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

  it("reproduces ClickHouse NO_COMMON_TYPE for old numeric coalesce shapes", async function testRawNumericCoalesceCommonTypeFailures() {
    const db = createE2EDb({
      clickhouse_settings: {
        use_variant_as_common_type: 0,
      },
    });

    await expectRejectsWithClickhouseError(
      db.execute(ckSql`select coalesce(CAST(NULL AS Nullable(Float64)), {fallback:Int64}) as value`, {
        query_params: {
          fallback: 0,
        },
      }),
      {
        kind: "request_failed",
        executionState: "rejected",
        clickhouseCode: 386,
        clickhouseName: "NO_COMMON_TYPE",
      },
    );

    await expectRejectsWithClickhouseError(
      db.execute(ckSql`select coalesce(CAST(NULL AS Nullable(UInt64)), {fallback:Int64}) as value`, {
        query_params: {
          fallback: 0,
        },
      }),
      {
        kind: "request_failed",
        executionState: "rejected",
        clickhouseCode: 386,
        clickhouseName: "NO_COMMON_TYPE",
      },
    );

    await expectRejectsWithClickhouseError(
      db.execute(ckSql`select coalesce(CAST(NULL AS Nullable(Decimal(18, 2))), {fallback:Float64}) as value`, {
        query_params: {
          fallback: 0.5,
        },
      }),
      {
        kind: "request_failed",
        executionState: "rejected",
        clickhouseCode: 386,
        clickhouseName: "NO_COMMON_TYPE",
      },
    );
  });

  it("keeps Float64 defaults typed through fn.coalesce and floating aggregates", async function testFloatCoalesceDefaults() {
    const db = createE2EDb({
      clickhouse_settings: {
        use_variant_as_common_type: 0,
      },
    });

    const [directRow] = await db
      .select({
        floatDefault: fn.coalesce(schemaPrimitives.float64_value, 0).as("float_default"),
        uint64Default: fn.coalesce(schemaPrimitives.uint64_value, 0).as("uint64_default"),
        decimalDefault: fn.coalesce(schemaPrimitives.decimal_value, "0.00").as("decimal_default"),
      })
      .from(schemaPrimitives)
      .where(ck.eq(schemaPrimitives.id, 1));

    expect(expectPresent(directRow, "direct float coalesce row")).toEqual({
      floatDefault: 6.5,
      uint64Default: "64",
      decimalDefault: "1234.56",
    });

    const [sumRow] = await db
      .select({
        sumDefault: fn.coalesce(fn.sum(schemaPrimitives.float64_value), 0).as("sum_default"),
      })
      .from(schemaPrimitives);

    expect(expectPresent(sumRow, "sum float coalesce row")).toEqual({
      sumDefault: 6.5,
    });

    const numericRollup = db
      .select({
        id: schemaPrimitives.id,
        price: schemaPrimitives.float64_value.as("price"),
        volume: schemaPrimitives.uint64_value.as("volume"),
        amount: schemaPrimitives.decimal_value.as("amount"),
        profit: fn.sum(schemaPrimitives.float64_value).as("profit"),
      })
      .from(schemaPrimitives)
      .groupBy(
        schemaPrimitives.id,
        schemaPrimitives.float64_value,
        schemaPrimitives.uint64_value,
        schemaPrimitives.decimal_value,
      )
      .as("numeric_rollup");

    const [leftJoinDefaultRow] = await db
      .select({
        openPrice: fn.coalesce(numericRollup.price, 0).as("open_price"),
        volume: fn.coalesce(numericRollup.volume, 0).as("volume"),
        amount: fn.coalesce(numericRollup.amount, "0.00").as("amount"),
        profit: fn.coalesce(numericRollup.profit, 0).as("profit"),
      })
      .from(schemaPrimitives)
      .leftJoin(numericRollup, ck.and(ck.eq(schemaPrimitives.id, numericRollup.id), ck.eq(numericRollup.id, -1)))
      .where(ck.eq(schemaPrimitives.id, 1));

    expect(expectPresent(leftJoinDefaultRow, "left join float coalesce row")).toEqual({
      openPrice: 0,
      volume: "0",
      amount: "0",
      profit: 0,
    });
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
      tags: fn.jsonExtract(payload, ckType.array(ckType.string()), "account", "tags").as("tags"),
      score: fn.jsonExtract(payload, ckType.float64(), "account", "score").as("score"),
      firstTicket: fn.jsonExtract(payload, ckType.int64(), "orders", 1, "ticket").as("first_ticket"),
      secondTicket: fn.jsonExtract(payload, ckType.int64(), "orders", 2, "ticket").as("second_ticket"),
      nullableRegion: fn
        .jsonExtract(payload, ckType.nullable(ckType.string()), "account", "audits", 2, "region")
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
        .tupleElement<string>(ckSql`CAST(('alice', 7), 'Tuple(name String, score UInt8)')`, "name")
        .as("named_value"),
      defaultedValue: fn.tupleElement<string>(ckSql`tuple('only')`, 2, "fallback").as("defaulted_value"),
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

  it("supports current ClickHouse array helper additions", async function testArrayHelperAdditions() {
    const db = createE2EDb();

    const [row] = await db.select({
      anyLarge: fn.arrayExists(ckSql`x -> x > 2`, [1, 2, 3]).as("any_large"),
      allPositive: fn.arrayAll(ckSql`x -> x > 0`, [1, 2, 3]).as("all_positive"),
      countLarge: fn.arrayCount(ckSql`x -> x > 1`, [1, 2, 3]).as("count_large"),
      filtered: fn.arrayFilter<number>(ckSql`x -> x > 1`, [1, 2, 3]).as("filtered"),
      mapped: fn.arrayMap<number>(ckSql`x -> x + 1`, [1, 2]).as("mapped"),
      firstLarge: fn.arrayFirst<number>(ckSql`x -> x > 1`, [1, 2, 3]).as("first_large"),
      firstLargeIndex: fn.arrayFirstIndex(ckSql`x -> x > 1`, [1, 2, 3]).as("first_large_index"),
      firstMissing: fn.arrayFirstOrNull<number>(ckSql`x -> x > 9`, [1, 2, 3]).as("first_missing"),
      lastLarge: fn.arrayLast<number>(ckSql`x -> x > 1`, [1, 2, 3]).as("last_large"),
      lastLargeIndex: fn.arrayLastIndex(ckSql`x -> x > 1`, [1, 2, 3]).as("last_large_index"),
      lastMissing: fn.arrayLastOrNull<number>(ckSql`x -> x > 9`, [1, 2, 3]).as("last_missing"),
      sortedAsc: fn.arraySort<number>([3, 1, 2]).as("sorted_asc"),
      sortedDesc: fn.arrayReverseSort<number>([3, 1, 2]).as("sorted_desc"),
      compacted: fn.arrayCompact<number>([1, 1, 2, 2, 3]).as("compacted"),
      distinctValues: fn.arrayDistinct<number>([1, 1, 2]).as("distinct_values"),
      diff: fn.arrayDifference<number>([1, 3, 6]).as("diff"),
      cumSum: fn.arrayCumSum<number>([1, 2, 3]).as("cum_sum"),
      enumerated: fn.arrayEnumerate(["a", "b"]).as("enumerated"),
      uniqCount: fn.arrayUniq(["vip", "vip", "pro"]).as("uniq_count"),
      equalCount: fn.countEqual([1, 1, 2], 1).as("equal_count"),
      isEmpty: fn.empty([]).as("is_empty"),
      emptyStrings: fn.emptyArrayString().as("empty_strings"),
      singleEmptyString: fn.emptyArrayToSingle<string>(fn.emptyArrayString()).as("single_empty_string"),
      hasTag: fn.has(["vip", "pro"], "vip").as("has_tag"),
      hasAllTags: fn.hasAll(["vip", "pro"], ["vip"]).as("has_all_tags"),
      hasAnyTags: fn.hasAny(["vip", "pro"], ["raw"]).as("has_any_tags"),
      hasSubPath: fn.hasSubstr(["vip", "pro", "raw"], ["vip", "pro"]).as("has_sub_path"),
      sortedIndex: fn.indexOfAssumeSorted([1, 2, 3], 2).as("sorted_index"),
      generatedRange: fn.range(1, 4).as("generated_range"),
      replicated: fn.replicate<string>("vip", [1, 2]).as("replicated"),
      reversed: fn.reverse<number>([1, 2, 3]).as("reversed"),
      excepted: fn.arrayExcept<string>(["vip", "pro"], ["pro"]).as("excepted"),
      removed: fn.arrayRemove<string>(["vip", "pro"], "pro").as("removed"),
      resized: fn.arrayResize<string>(["vip"], 2, "pro").as("resized"),
      rotatedLeft: fn.arrayRotateLeft<number>([1, 2, 3], 1).as("rotated_left"),
      shiftedLeft: fn.arrayShiftLeft<number>([1, 2, 3], 1, 0).as("shifted_left"),
      kqlAsc: fn.kql_array_sort_asc<number>([3, 1, 2]).as("kql_asc"),
      kqlDesc: fn.kql_array_sort_desc<number>([3, 1, 2]).as("kql_desc"),
    });

    expect(expectPresent(row, "array helper additions row")).toEqual({
      anyLarge: true,
      allPositive: true,
      countLarge: "2",
      filtered: ["2", "3"],
      mapped: ["2", "3"],
      firstLarge: "2",
      firstLargeIndex: 2,
      firstMissing: null,
      lastLarge: "3",
      lastLargeIndex: 3,
      lastMissing: null,
      sortedAsc: ["1", "2", "3"],
      sortedDesc: ["3", "2", "1"],
      compacted: ["1", "2", "3"],
      distinctValues: ["1", "2"],
      diff: ["0", "2", "3"],
      cumSum: ["1", "3", "6"],
      enumerated: [1, 2],
      uniqCount: "2",
      equalCount: "2",
      isEmpty: true,
      emptyStrings: [],
      singleEmptyString: [""],
      hasTag: true,
      hasAllTags: true,
      hasAnyTags: false,
      hasSubPath: true,
      sortedIndex: "2",
      generatedRange: ["1", "2", "3"],
      replicated: ["vip", "vip"],
      reversed: ["3", "2", "1"],
      excepted: ["vip"],
      removed: ["vip"],
      resized: ["vip", "pro"],
      rotatedLeft: ["2", "3", "1"],
      shiftedLeft: ["2", "3", "0"],
      kqlAsc: [["1", "2", "3"]],
      kqlDesc: [["3", "2", "1"]],
    });
  });

  it("supports deterministic higher-order array helpers", async function testHigherOrderArrayHelpers() {
    const db = createE2EDb();

    const [row] = await db.select({
      multiArrayExists: fn.arrayExists(ckSql`(x, y) -> x = y`, [1, 2, 3], [9, 2, 8]).as("multi_array_exists"),
      filled: fn.arrayFill<number>(ckSql`x -> x > 0`, [1, 0, 2, 0]).as("filled"),
      reverseFilled: fn.arrayReverseFill<number>(ckSql`x -> x > 0`, [1, 0, 2, 0]).as("reverse_filled"),
      split: fn.arraySplit<number>(ckSql`x -> x = 0`, [1, 0, 2, 3, 0, 4]).as("split"),
      reverseSplit: fn.arrayReverseSplit<number>(ckSql`x -> x = 0`, [1, 0, 2, 3, 0, 4]).as("reverse_split"),
      folded: fn
        .arrayFold<number>(ckSql`(acc, x, y) -> acc + x * y`, [1, 2, 3], [10, 20, 30], 0)
        .mapWith((value) => Number(value))
        .as("folded"),
      lambdaSum: fn
        .arraySum<number>(ckSql`(x, y) -> x + y`, [1, 2], [3, 4])
        .mapWith((value) => Number(value))
        .as("lambda_sum"),
      lambdaMax: fn
        .arrayMax<number>(ckSql`x -> -x`, [1, 2, 3])
        .mapWith((value) => Number(value))
        .as("lambda_max"),
    });

    expect(expectPresent(row, "higher-order array helpers row")).toEqual({
      multiArrayExists: true,
      filled: ["1", "1", "2", "2"],
      reverseFilled: ["1", "2", "2", "0"],
      split: [["1"], ["0", "2", "3"], ["0", "4"]],
      reverseSplit: [["1", "0"], ["2", "3", "0"], ["4"]],
      folded: 140,
      lambdaSum: 10,
      lambdaMax: -1,
    });
  });

  it("supports deterministic array set, shape and constructor helpers", async function testArrayShapeAndConstructors() {
    const db = createE2EDb();

    const [row] = await db.select({
      dense: fn.arrayEnumerateDense(["vip", "pro", "vip"]).as("dense"),
      uniqEnumerated: fn.arrayEnumerateUniq(["vip", "pro", "vip"]).as("uniq_enumerated"),
      unioned: fn.arrayUnion<number>([1, 2], [2, 3]).as("unioned"),
      symmetric: fn.arraySymmetricDifference<number>([1, 2], [2, 3]).as("symmetric"),
      shingles: fn.arrayShingles<readonly string[]>(["a", "b", "c"], 2).as("shingles"),
      zippedUnaligned: fn.arrayZipUnaligned([1, 2], ["a"]).as("zipped_unaligned"),
      popBack: fn.arrayPopBack<number>([1, 2, 3]).as("pop_back"),
      popFront: fn.arrayPopFront<number>([1, 2, 3]).as("pop_front"),
      pushBack: fn.arrayPushBack<number>([1, 2], 3).as("push_back"),
      pushFront: fn.arrayPushFront<number>([2, 3], 1).as("push_front"),
      withConstant: fn.arrayWithConstant<string>(3, "vip").as("with_constant"),
      emptyDate: fn.emptyArrayDate().as("empty_date"),
      emptyDateTime: fn.emptyArrayDateTime().as("empty_date_time"),
      emptyFloat32: fn.emptyArrayFloat32().as("empty_float32"),
      emptyFloat64: fn.emptyArrayFloat64().as("empty_float64"),
      emptyInt8: fn.emptyArrayInt8().as("empty_int8"),
      emptyInt16: fn.emptyArrayInt16().as("empty_int16"),
      emptyInt32: fn.emptyArrayInt32().as("empty_int32"),
      emptyInt64: fn.emptyArrayInt64().as("empty_int64"),
      emptyUInt8: fn.emptyArrayUInt8().as("empty_uint8"),
      emptyUInt16: fn.emptyArrayUInt16().as("empty_uint16"),
      emptyUInt32: fn.emptyArrayUInt32().as("empty_uint32"),
      emptyUInt64: fn.emptyArrayUInt64().as("empty_uint64"),
    });

    const presentRow = expectPresent(row, "array shape helpers row");
    expect({
      ...presentRow,
      symmetric: [...presentRow.symmetric].map(String).sort(),
      unioned: [...presentRow.unioned].map(String).sort(),
    }).toEqual({
      dense: [1, 2, 1],
      uniqEnumerated: [1, 1, 2],
      unioned: ["1", "2", "3"],
      symmetric: ["1", "3"],
      shingles: [
        ["a", "b"],
        ["b", "c"],
      ],
      zippedUnaligned: [
        ["1", "a"],
        ["2", null],
      ],
      popBack: ["1", "2"],
      popFront: ["2", "3"],
      pushBack: ["1", "2", "3"],
      pushFront: ["1", "2", "3"],
      withConstant: ["vip", "vip", "vip"],
      emptyDate: [],
      emptyDateTime: [],
      emptyFloat32: [],
      emptyFloat64: [],
      emptyInt8: [],
      emptyInt16: [],
      emptyInt32: [],
      emptyInt64: [],
      emptyUInt8: [],
      emptyUInt16: [],
      emptyUInt32: [],
      emptyUInt64: [],
    });
  });

  it("supports deterministic numeric, scoring and ordering array helpers", async function testNumericAndOrderingArrays() {
    const db = createE2EDb();

    const [row] = await db.select({
      avgValue: fn.arrayAvg([1, 2, 3]).as("avg_value"),
      sumValue: fn
        .arraySum<number>([1, 2, 3])
        .mapWith((value) => Number(value))
        .as("sum_value"),
      productValue: fn
        .arrayProduct<number>([1, 2, 3, 4])
        .mapWith((value) => Number(value))
        .as("product_value"),
      maxValue: fn
        .arrayMax<number>([1, 9, 3])
        .mapWith((value) => Number(value))
        .as("max_value"),
      minValue: fn
        .arrayMin<number>([1, 9, 3])
        .mapWith((value) => Number(value))
        .as("min_value"),
      dotProduct: fn
        .arrayDotProduct<number>([1, 2], [3, 4])
        .mapWith((value) => Number(value))
        .as("dot_product"),
      reduced: fn
        .arrayReduce<number>("sum", [1, 2, 3])
        .mapWith((value) => Number(value))
        .as("reduced"),
      jaccard: fn.arrayJaccardIndex([1, 2], [2, 3]).as("jaccard"),
      levenshtein: fn.arrayLevenshteinDistance(["a", "b"], ["a", "c"]).as("levenshtein"),
      rocAuc: fn.arrayROCAUC([0.1, 0.9], [0, 1]).as("roc_auc"),
      partialSortedHead: fn
        .arraySlice<number>(fn.arrayPartialSort<number>(ckSql`toUInt8(2)`, [5, 1, 3, 2]), 1, 2)
        .as("partial_sorted_head"),
      partialReverseSortedHead: fn
        .arraySlice<number>(fn.arrayPartialReverseSort<number>(ckSql`toUInt8(2)`, [5, 1, 3, 2]), 1, 2)
        .as("partial_reverse_sorted_head"),
      shuffledSorted: fn
        .arraySort<number>(fn.arrayShuffle<number>([3, 1, 2], ckSql`toUInt64(42)`))
        .as("shuffled_sorted"),
      randomSampleSize: fn
        .length(fn.arrayRandomSample<number>([1, 2, 3, 4], ckSql`toUInt8(2)`))
        .as("random_sample_size"),
      partialShuffleSize: fn
        .length(fn.arrayPartialShuffle<number>([1, 2, 3, 4], ckSql`toUInt8(2)`, ckSql`toUInt64(42)`))
        .as("partial_shuffle_size"),
      rotatedRight: fn.arrayRotateRight<number>([1, 2, 3], 1).as("rotated_right"),
      shiftedRight: fn.arrayShiftRight<number>([1, 2, 3], 1, 0).as("shifted_right"),
    });

    const presentRow = expectPresent(row, "numeric array helpers row");
    expect(presentRow).toMatchObject({
      avgValue: 2,
      sumValue: 6,
      productValue: 24,
      maxValue: 9,
      minValue: 1,
      dotProduct: 11,
      reduced: 6,
      levenshtein: 1,
      rocAuc: 1,
      partialSortedHead: ["1", "2"],
      partialReverseSortedHead: ["5", "3"],
      shuffledSorted: ["1", "2", "3"],
      randomSampleSize: "2",
      partialShuffleSize: "4",
      rotatedRight: ["3", "1", "2"],
      shiftedRight: ["0", "1", "2"],
    });
    expect(presentRow.jaccard).toBeCloseTo(1 / 3);
  });

  it("supports tableFn.call against the numbers table function", async function testTableFunction() {
    const db = createE2EDb();
    const numbers = fn.table.call("numbers", 5).as("n");

    const rows = await db
      .select({
        value: ck.expr(ckSql<bigint>`number`.mapWith((value) => BigInt(String(value)))),
      })
      .from(numbers)
      .orderBy(ck.expr(ckSql`number`));

    expect(rows).toEqual([{ value: 0n }, { value: 1n }, { value: 2n }, { value: 3n }, { value: 4n }]);
  });
});

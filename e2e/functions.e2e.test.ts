import { expect, it } from "bun:test";
import { ck, ckSql, ckTable, ckType, fn } from "./ck-orm";
import { createE2EDb, createTempTableName, schemaPrimitives, users, webEvents } from "./shared";
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

  it("supports replaceRegexpAll literals, dynamic arguments and nullable inputs", async function testReplaceRegexpAll() {
    const db = createE2EDb();

    const [literalRow] = await db.select({
      whitespace: fn.replaceRegexpAll("a   b", "[[:space:]]+", "-").as("whitespace"),
      captured: fn.replaceRegexpAll("abc123", "([a-z]+)([0-9]+)", String.raw`\2-\1`).as("captured"),
      zeroWidth: fn.replaceRegexpAll("abc", "^|$", "_").as("zero_width"),
      noMatch: fn.replaceRegexpAll("abc", "z+", "x").as("no_match"),
    });

    expect(expectPresent(literalRow, "replaceRegexpAll literal row")).toEqual({
      whitespace: "a-b",
      captured: "123-abc",
      zeroWidth: "_abc_",
      noMatch: "abc",
    });

    const tempTable = createTempTableName("replace_regexp_all");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      haystack: ckType.nullable(ckType.string()),
      pattern: ckType.nullable(ckType.string()),
      replacement: ckType.nullable(ckType.string()),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);
      await session.insert(scope).values([
        { id: 1, haystack: "a   b", pattern: "[[:space:]]+", replacement: "-" },
        { id: 2, haystack: "abc123", pattern: "([a-z]+)([0-9]+)", replacement: String.raw`\2-\1` },
        { id: 3, haystack: "abc", pattern: "^|$", replacement: "_" },
        { id: 4, haystack: null, pattern: "a", replacement: "b" },
        { id: 5, haystack: "abc", pattern: null, replacement: "x" },
        { id: 6, haystack: "abc", pattern: "a", replacement: null },
        { id: 7, haystack: "abc", pattern: "z+", replacement: "x" },
      ]);

      const rows = await session
        .select({
          id: scope.id,
          result: fn.replaceRegexpAll(scope.haystack, scope.pattern, scope.replacement).as("result"),
        })
        .from(scope)
        .orderBy(scope.id);

      expect(rows).toEqual([
        { id: 1, result: "a-b" },
        { id: 2, result: "123-abc" },
        { id: 3, result: "_abc_" },
        { id: 4, result: null },
        { id: 5, result: null },
        { id: 6, result: null },
        { id: 7, result: "abc" },
      ]);
    });
  });

  it("keeps invalid replaceRegexpAll patterns as ClickHouse errors", async function testReplaceRegexpAllErrors() {
    const db = createE2EDb();

    await expectRejectsWithClickhouseError(
      db
        .select({
          result: fn.replaceRegexpAll("abc", "(", "x").as("result"),
        })
        .execute(),
      {
        kind: "request_failed",
        executionState: "rejected",
        clickhouseCode: 36,
        clickhouseName: "BAD_ARGUMENTS",
      },
    );
  });

  it("supports parameterized AggregateFunction type literals in real DDL", async function testParameterizedAggregateFunctionType() {
    const db = createE2EDb();
    const tempTable = createTempTableName("agg_quantile_scope");
    const scope = ckTable(tempTable, {
      state: ckType.aggregateFunction("quantile(0.5)", ckType.float64()),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);
      await session.command(ckSql`
        INSERT INTO ${scope}
        SELECT quantileState(0.5)(toFloat64(number))
        FROM numbers(3)
      `);

      const [row] = await session.execute(ckSql`
        SELECT toFloat64(quantileMerge(0.5)(state)) AS value
        FROM ${scope}
      `);

      expect(Number(expectPresent(row, "quantile row").value)).toBe(1);
    });
  });

  it("supports ClickHouse type conversion helper families against real clickhouse", async function testTypeConversionHelpers() {
    const db = createE2EDb();

    const [row] = await db.select({
      castUInt32: fn
        .cast<number>("32", "UInt32")
        .mapWith((value) => Number(value))
        .as("cast_uint32"),
      dateAlias: fn.date("2026-01-10").as("date_alias"),
      accurateUInt8: fn
        .accurateCast<number>("8", "UInt8")
        .mapWith((value) => Number(value))
        .as("accurate_uint8"),
      accurateDefault: fn
        .accurateCastOrDefault<number>("bad", "UInt8", ckSql`toUInt8(7)`)
        .mapWith((value) => Number(value))
        .as("accurate_default"),
      accurateNull: fn
        .accurateCastOrNull<number>("bad", "UInt8")
        .mapWith((value) => (value === null ? null : Number(value)))
        .as("accurate_null"),
      reinterpretedUInt8: fn.reinterpretAsUInt8(fn.toFixedString("A", 1)).as("reinterpreted_uint8"),
      boolValue: fn.toBool(1).as("bool_value"),
      int32Value: fn.toInt32("42").as("int32_value"),
      int32Default: fn.toInt32OrDefault("bad", ckSql`toInt32(7)`).as("int32_default"),
      int32Null: fn.toInt32OrNull("bad").as("int32_null"),
      uint64Value: fn.toUInt64("18446744073709551615").as("uint64_value"),
      float32Value: fn.toFloat32("1.5").as("float32_value"),
      float32Default: fn.toFloat32OrDefault("bad", ckSql`toFloat32(2.5)`).as("float32_default"),
      bfloatValue: fn.toBFloat16("1.75").as("bfloat_value"),
      decimalValue: fn.toDecimal32("12.34", 2).as("decimal_value"),
      decimalDefault: fn.toDecimal32OrDefault("bad", 2, ckSql`CAST('1.23', 'Decimal32(2)')`).as("decimal_default"),
      decimalNull: fn.toDecimal32OrNull("bad", 2).as("decimal_null"),
      decimalZero: fn.toDecimal32OrZero("bad", 2).as("decimal_zero"),
      decimalString: fn.toDecimalString(ckSql`CAST(12.345 AS Decimal(9, 3))`, 2).as("decimal_string"),
      fixedString: fn.toFixedString("ABCD", 4).as("fixed_string"),
      dateNull: fn.toDateOrNull("bad").as("date_null"),
      dateDefault: fn.toDateOrDefault("bad", ckSql`toDate('2026-01-10')`).as("date_default"),
      dateTimeNull: fn.toDateTimeOrNull("bad", "UTC").as("date_time_null"),
      dateTimeDefault: fn
        .toDateTimeOrDefault("bad", "UTC", ckSql`toDateTime('2026-01-12 01:02:03', 'UTC')`)
        .as("date_time_default"),
      dateTime64Null: fn.toDateTime64OrNull("bad", 3, "UTC").as("date_time64_null"),
      dateTime64Default: fn
        .toDateTime64OrDefault("bad", 3, "UTC", ckSql`toDateTime64('2026-01-12 01:02:03.456', 3, 'UTC')`)
        .as("date_time64_default"),
      parsedDateTime: fn.parseDateTime("2026-01-12 01:02:03", ckSql`'%Y-%m-%d %H:%i:%s'`, "UTC").as("parsed_date_time"),
      parsedDateTimeNull: fn.parseDateTimeOrNull("bad", ckSql`'%Y-%m-%d %H:%i:%s'`, "UTC").as("parsed_date_time_null"),
      parsedDateTime64: fn
        .parseDateTime64("2026-01-12 01:02:03.456000", ckSql`'%Y-%m-%d %H:%i:%s.%f'`, "UTC")
        .as("parsed_date_time64"),
      parsedDateTime64BestEffort: fn
        .parseDateTime64BestEffort("2026-01-12 01:02:03.789", 3, "UTC")
        .as("parsed_date_time64_best_effort"),
      parsedDateTimeJoda: fn
        .parseDateTimeInJodaSyntax("2026-01-12 01:02:03", ckSql`'yyyy-MM-dd HH:mm:ss'`, "UTC")
        .as("parsed_date_time_joda"),
      parsedDateTime64JodaNull: fn
        .parseDateTime64InJodaSyntaxOrNull("bad", ckSql`'yyyy-MM-dd HH:mm:ss.SSS'`, "UTC")
        .as("parsed_date_time64_joda_null"),
      formattedDateTime: fn
        .formatDateTime(ckSql`toDateTime('2026-01-12 01:02:03', 'UTC')`, "%Y-%m-%d", "UTC")
        .as("formatted_date_time"),
      formattedRow: fn.formatRow(ckSql`'CSV'`, 7, "good").as("formatted_row"),
      formattedRowNoNewline: fn.formatRowNoNewline(ckSql`'CSV'`, 7, "good").as("formatted_row_no_newline"),
      lowCardinalityValue: fn.toLowCardinality<string>("vip").as("low_cardinality_value"),
      nullableValue: fn.toNullable<string>("vip").as("nullable_value"),
      timeValue: fn.toTime(ckSql`toDateTime('1970-01-01 12:34:56')`).as("time_value"),
      timeNull: fn.toTimeOrNull("bad").as("time_null"),
      time64Value: fn.toTime64("12:34:56.789123", 6).as("time64_value"),
      time64Zero: fn.toTime64OrZero("bad", 6).as("time64_zero"),
      intervalDay: fn.toDate(fn.call("plus", fn.toDate("2026-01-01"), fn.toIntervalDay(1))).as("interval_day"),
      intervalGeneric: fn
        .toDate(fn.call("plus", fn.toDate("2026-01-01"), fn.toInterval(1, "day")))
        .as("interval_generic"),
      uuidValue: fn.toUUID("123e4567-e89b-12d3-a456-426614174000").as("uuid_value"),
      uuidZero: fn.toUUIDOrZero("bad").as("uuid_zero"),
      cutToZero: fn.toStringCutToZero(ckSql`'abc\\0def'`).as("cut_to_zero"),
    });

    const presentRow = expectPresent(row, "type conversion row");
    expect(presentRow.castUInt32).toBe(32);
    expect(presentRow.accurateUInt8).toBe(8);
    expect(presentRow.accurateDefault).toBe(7);
    expect(presentRow.accurateNull).toBeNull();
    expect(presentRow.reinterpretedUInt8).toBe(65);
    expect(presentRow.boolValue).toBe(true);
    expect(presentRow.int32Value).toBe(42);
    expect(presentRow.int32Default).toBe(7);
    expect(presentRow.int32Null).toBeNull();
    expect(presentRow.uint64Value).toBe("18446744073709551615");
    expect(presentRow.float32Value).toBeCloseTo(1.5);
    expect(presentRow.float32Default).toBeCloseTo(2.5);
    expect(presentRow.bfloatValue).toBeCloseTo(1.75, 1);
    expect(presentRow.decimalValue).toBe("12.34");
    expect(presentRow.decimalDefault).toBe("1.23");
    expect(presentRow.decimalNull).toBeNull();
    expect(presentRow.decimalZero).toBe("0");
    expect(presentRow.decimalString).toBe("12.35");
    expect(presentRow.fixedString).toBe("ABCD");
    expect(presentRow.dateNull).toBeNull();
    expect(presentRow.dateTimeNull).toBeNull();
    expect(presentRow.dateTime64Null).toBeNull();
    expect(presentRow.parsedDateTimeNull).toBeNull();
    expect(presentRow.parsedDateTime64JodaNull).toBeNull();
    expect(presentRow.formattedDateTime).toBe("2026-01-12");
    expect(presentRow.formattedRow).toBe('7,"good"\n');
    expect(presentRow.formattedRowNoNewline).toBe('7,"good"');
    expect(presentRow.lowCardinalityValue).toBe("vip");
    expect(presentRow.nullableValue).toBe("vip");
    expect(presentRow.timeNull).toBeNull();
    expect(presentRow.uuidValue).toBe("123e4567-e89b-12d3-a456-426614174000");
    expect(presentRow.uuidZero).toBe("00000000-0000-0000-0000-000000000000");
    expect(presentRow.cutToZero).toBe("abc");

    for (const dateValue of [
      presentRow.dateAlias,
      presentRow.dateDefault,
      presentRow.dateTimeDefault,
      presentRow.dateTime64Default,
      presentRow.parsedDateTime,
      presentRow.parsedDateTime64,
      presentRow.parsedDateTime64BestEffort,
      presentRow.parsedDateTimeJoda,
      // toTime() is actually toTimeWithFixedDate — returns DateTime pinned to 1970-01-02.
      presentRow.timeValue,
      presentRow.intervalDay,
      presentRow.intervalGeneric,
    ]) {
      expectDate(dateValue);
    }
    expect(presentRow.dateAlias.toISOString().slice(0, 10)).toBe("2026-01-10");
    expect(presentRow.dateDefault.toISOString().slice(0, 10)).toBe("2026-01-10");
    expect(presentRow.dateTime64Default.getUTCMilliseconds()).toBe(456);
    expect(presentRow.parsedDateTime64.getUTCMilliseconds()).toBe(456);
    expect(presentRow.parsedDateTime64BestEffort.getUTCMilliseconds()).toBe(789);
    // toTime returns DateTime at 1970-01-02 plus the time-of-day (45_296_000 ms = 12h34m56s).
    expect(presentRow.timeValue.getTime()).toBe(86_400_000 + 45_296_000);
    // toTime64 returns the new Time64 data type — strings, no calendar date.
    expect(presentRow.time64Value).toBe("12:34:56.789123");
    expect(presentRow.time64Zero).toBe("00:00:00.000000");
    expect(presentRow.intervalDay.toISOString().slice(0, 10)).toBe("2026-01-02");
    expect(presentRow.intervalGeneric.toISOString().slice(0, 10)).toBe("2026-01-02");
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

  it("supports window expressions, conditional maxima and exact decimal division in builder queries", async function testHistoryQueryHelpers() {
    const db = createE2EDb();
    const rankInCountrySpec = {
      partitionBy: [webEvents.country],
      orderBy: [ck.asc(webEvents.event_id)],
    };
    const mixedRankInCountry = fn.over(fn.rowNumber().toMixed(), rankInCountrySpec);
    const visibleRankInCountry = fn.if(ck.eq(webEvents.country, "US"), mixedRankInCountry, 6);
    expect(visibleRankInCountry.sqlType).toBe("UInt64");

    const rows = await db
      .select({
        country: webEvents.country,
        eventId: webEvents.event_id,
        rankInCountry: fn.over(fn.rowNumber(), rankInCountrySpec).as("rank_in_country"),
        safeRankInCountry: fn.over(fn.rowNumber().toSafe(), rankInCountrySpec).as("safe_rank_in_country"),
        mixedRankInCountry: mixedRankInCountry.as("mixed_rank_in_country"),
        visibleRankInCountry: visibleRankInCountry.as("visible_rank_in_country"),
        visibleRankType: fn.call<string>("toTypeName", visibleRankInCountry).mapWith(String).as("visible_rank_type"),
        totalRevenueInCountry: fn
          .over(fn.sum(webEvents.revenue), {
            partitionBy: [webEvents.country],
          })
          .as("total_revenue_in_country"),
      })
      .from(webEvents)
      .where(ck.lte(webEvents.event_id, 20))
      .orderBy(webEvents.country, webEvents.event_id);

    const ranksByCountry = new Map<string, number>();
    const revenueByCountry = new Map<string, string>();
    for (const row of rows) {
      const nextRank = (ranksByCountry.get(row.country) ?? 0) + 1;
      expect(row.rankInCountry).toBe(nextRank);
      expect(row.safeRankInCountry).toBe(String(nextRank));
      expect(row.mixedRankInCountry).toBe(String(nextRank));
      expect(row.visibleRankType).toBe("UInt64");
      expect(row.visibleRankInCountry).toBe(row.country === "US" ? String(nextRank) : "6");
      ranksByCountry.set(row.country, nextRank);
      expect(row.totalRevenueInCountry).toMatch(/^\d+(?:\.\d+)?$/);
      expect(revenueByCountry.get(row.country) ?? row.totalRevenueInCountry).toBe(row.totalRevenueInCountry);
      revenueByCountry.set(row.country, row.totalRevenueInCountry);
    }

    const overPrecision = ckSql`toUInt64('9007199254740993')`;
    const [overPrecisionRow] = await db
      .select({
        unsafe: fn.toFloat64(overPrecision).as("unsafe"),
        safe: fn.toString(overPrecision).as("safe"),
        mixed: fn.toUInt64(overPrecision).as("mixed"),
      })
      .from(webEvents)
      .limit(1);
    expect(expectPresent(overPrecisionRow, "over-precision conversion row")).toEqual({
      unsafe: 9007199254740992,
      safe: "9007199254740993",
      mixed: "9007199254740993",
    });

    const [conditionalMaximum] = await db
      .select({
        maxUsRevenue: fn.maxIf(webEvents.revenue, ck.eq(webEvents.country, "US")).as("max_us_revenue"),
      })
      .from(webEvents);
    expect(expectPresent(conditionalMaximum, "conditional maximum row").maxUsRevenue).toMatch(/^\d+(?:\.\d+)?$/);

    const [division] = await db
      .select({
        exactRatio: fn.divideDecimal(fn.toDecimal64("10.00", 2), fn.toDecimal64("4.00", 2), 4).as("exact_ratio"),
      })
      .from(webEvents)
      .limit(1);
    expect(Number(expectPresent(division, "decimal division row").exactRatio)).toBe(2.5);
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

  it("decodes nested tuple groupArray and arrayReverseSort values", async function testNestedTupleArrayDecoders() {
    const db = createE2EDb();
    const tupleValue = fn.tuple(
      schemaPrimitives.date_time64_value,
      schemaPrimitives.bool_value,
      fn.toInt64OrNull("bad"),
      schemaPrimitives.int64_value,
      ckSql<bigint>`toInt64(9007199254740993)`.mapWith((value) => BigInt(String(value))),
    );

    const [row] = await db
      .select({
        items: fn.arrayReverseSort(fn.withParams("groupArray", [21], tupleValue)).as("items"),
      })
      .from(schemaPrimitives)
      .where(ck.eq(schemaPrimitives.id, 1));

    const presentRow = expectPresent(row, "nested tuple array row");
    expect(presentRow.items).toHaveLength(1);
    const item = expectPresent(presentRow.items[0], "nested tuple item");
    expectDate(item[0]);
    expect(item[0].getMilliseconds()).toBe(456);
    expect(item[1]).toBe(true);
    expect(item[2]).toBeNull();
    expect(item[3]).toBe("-64");
    expect(item[4]).toBe(9007199254740993n);
  });

  it("keeps unsafe coalesce fallbacks under ClickHouse control", async function testUnsafeCoalesceFallbacks() {
    const strictDb = createE2EDb({
      clickhouse_settings: {
        use_variant_as_common_type: 0,
      },
    });
    const noCommonType = {
      kind: "request_failed",
      executionState: "rejected",
      clickhouseCode: 386,
      clickhouseName: "NO_COMMON_TYPE",
    };

    await expectRejectsWithClickhouseError(
      strictDb
        .select({
          value: fn.coalesce(fn.nullIf(schemaPrimitives.float32_value, schemaPrimitives.float32_value), 0).as("value"),
        })
        .from(schemaPrimitives)
        .where(ck.eq(schemaPrimitives.id, 1))
        .execute(),
      noCommonType,
    );

    await expectRejectsWithClickhouseError(
      strictDb
        .select({
          value: fn
            .coalesce(fn.nullIf(schemaPrimitives.bfloat16_value, schemaPrimitives.bfloat16_value), 0)
            .as("value"),
        })
        .from(schemaPrimitives)
        .where(ck.eq(schemaPrimitives.id, 1))
        .execute(),
      noCommonType,
    );

    await expectRejectsWithClickhouseError(
      strictDb
        .select({
          value: fn
            .coalesce(fn.nullIf(schemaPrimitives.decimal_value, schemaPrimitives.decimal_value), 0.5)
            .as("value"),
        })
        .from(schemaPrimitives)
        .where(ck.eq(schemaPrimitives.id, 1))
        .execute(),
      noCommonType,
    );

    await expectRejectsWithClickhouseError(
      strictDb
        .select({
          value: fn.coalesce(fn.nullIf(schemaPrimitives.uint64_value, schemaPrimitives.uint64_value), 0n).as("value"),
        })
        .from(schemaPrimitives)
        .where(ck.eq(schemaPrimitives.id, 1))
        .execute(),
      noCommonType,
    );
  });

  it("validates conditional branch types against real ClickHouse", async function testConditionalBranchTypes() {
    const strictDb = createE2EDb({
      clickhouse_settings: {
        use_variant_as_common_type: 0,
      },
    });

    await expectRejectsWithClickhouseError(
      strictDb.execute(ckSql`select if(true, toUInt64(1), {fallback:Int64}) as value`, {
        query_params: {
          fallback: 6,
        },
      }),
      {
        kind: "request_failed",
        executionState: "rejected",
        clickhouseCode: 386,
        clickhouseName: "NO_COMMON_TYPE",
      },
    );

    const [promotedRow] = await strictDb.execute(
      ckSql`
        select
          toTypeName(if(false, toInt32(1), {fallback:Int64})) as type,
          if(false, toInt32(1), {fallback:Int64}) as value
      `,
      {
        query_params: {
          fallback: 3_000_000_000,
        },
      },
    );
    expect(expectPresent(promotedRow, "promoted conditional row")).toEqual({
      type: "Int64",
      value: "3000000000",
    });

    const tempTable = createTempTableName("conditional_branch_types");
    const scope = ckTable(tempTable, {
      rank: ckType.uint64(),
      ratio: ckType.float64(),
    });

    await strictDb.runInSession(async (session) => {
      await session.createTemporaryTable(scope);
      await session.insert(scope).values([{ rank: "1", ratio: 1.5 }]);

      const [typeRow] = await session
        .select({
          rankType: fn
            .call<string>("toTypeName", fn.if(true, scope.rank, 6))
            .mapWith(String)
            .as("rank_type"),
          ratioType: fn
            .call<string>("toTypeName", fn.if(true, scope.ratio, 0))
            .mapWith(String)
            .as("ratio_type"),
          multiRankType: fn
            .call<string>("toTypeName", fn.multiIf(true, scope.rank, false, 5, 6))
            .mapWith(String)
            .as("multi_rank_type"),
        })
        .from(scope);

      expect(expectPresent(typeRow, "conditional type row")).toEqual({
        rankType: "UInt64",
        ratioType: "Float64",
        multiRankType: "UInt64",
      });

      const conditionalValues = session.$with("conditional_values").as(
        session
          .select({
            rankOrFallback: fn.if(false, scope.rank, 6).as("rank_or_fallback"),
          })
          .from(scope),
      );

      const rows = await session
        .with(conditionalValues)
        .select({ rankOrFallback: conditionalValues.rankOrFallback })
        .from(conditionalValues)
        .where(ck.eq(conditionalValues.rankOrFallback, 6));

      expect(rows).toEqual([{ rankOrFallback: "6" }]);
    });

    const lowCardinalityA = ck.expr<string>(ckSql`toLowCardinality('a')`, {
      decoder: String,
      sqlType: "LowCardinality(String)",
    });
    const lowCardinalityB = ck.expr<string>(ckSql`toLowCardinality('b')`, {
      decoder: String,
      sqlType: "LowCardinality(String)",
    });
    const lowCardinalityConditional = fn.if(true, lowCardinalityA, lowCardinalityB);
    expect(lowCardinalityConditional.sqlType).toBeUndefined();

    const [sameTypeRow] = await strictDb.select({
      type: fn.call<string>("toTypeName", lowCardinalityConditional).mapWith(String).as("type"),
      value: lowCardinalityConditional.as("value"),
    });
    expect(expectPresent(sameTypeRow, "same-type conditional row")).toEqual({ type: "String", value: "a" });
  });

  it("delegates heterogeneous conditional types to ClickHouse settings", async function testConditionalServerSettings() {
    const conditional = fn.if(true, fn.toUInt64(1), fn.toInt64(-1));
    const negativeUIntFallback = fn.if(true, fn.toUInt64(1), -1);
    expect(conditional.sqlType).toBeUndefined();
    expect(negativeUIntFallback.sqlType).toBeUndefined();

    const strictDb = createE2EDb({
      clickhouse_settings: {
        use_variant_as_common_type: 0,
      },
    });
    const noCommonType = {
      kind: "request_failed",
      executionState: "rejected",
      clickhouseCode: 386,
      clickhouseName: "NO_COMMON_TYPE",
    };
    await expectRejectsWithClickhouseError(
      strictDb.select({ value: negativeUIntFallback.as("value") }).execute(),
      noCommonType,
    );
    await expectRejectsWithClickhouseError(strictDb.select({ value: conditional.as("value") }).execute(), noCommonType);

    const variantDb = createE2EDb({
      clickhouse_settings: {
        use_variant_as_common_type: 1,
      },
    });
    const [variantRow] = await variantDb
      .select({
        type: fn.call<string>("toTypeName", conditional).mapWith(String).as("type"),
        value: conditional.as("value"),
      })
      .execute();

    expect(expectPresent(variantRow, "variant conditional row")).toEqual({
      type: "Variant(Int64, UInt64)",
      value: "1",
    });
  });

  it("propagates nested conditional metadata through a subquery", async function testNestedConditionalSubquery() {
    const db = createE2EDb({
      clickhouse_settings: {
        use_variant_as_common_type: 0,
      },
    });
    const inner = fn.if(false, 0, schemaPrimitives.float64_value);
    const outer = fn.multiIf(false, 0, true, inner, 2);
    expect(inner.sqlType).toBe("Float64");
    expect(outer.sqlType).toBe("Float64");

    const conditionalScores = db
      .select({
        score: outer.as("score"),
      })
      .from(schemaPrimitives)
      .where(ck.eq(schemaPrimitives.id, 1))
      .as("conditional_scores");
    expect(conditionalScores.score.sqlType).toBe("Float64");

    const rows = await db
      .select({
        score: conditionalScores.score,
      })
      .from(conditionalScores)
      .where(ck.eq(conditionalScores.score, 6.5))
      .groupBy(conditionalScores.score)
      .orderBy(conditionalScores.score);

    expect(rows).toEqual([{ score: 6.5 }]);
  });

  it("keeps unproven conditional results on the transport decoder", async function testConditionalTransportFallbacks() {
    const strictDb = createE2EDb({
      clickhouse_settings: {
        use_variant_as_common_type: 0,
      },
    });
    const literalValue = fn.if(true, 1, 0);
    const promotedValue = fn.if(false, schemaPrimitives.int32_value, 3_000_000_000);
    const rawValue = fn.if(true, fn.toUInt64(1), ckSql`toUInt64(6)`);
    const dateValue = fn.if(
      false,
      schemaPrimitives.date_time64_value,
      fn.toDateTime64(schemaPrimitives.date_time_value, 3),
    );
    const decimalValue = fn.if(false, schemaPrimitives.decimal_value, fn.toDecimal64("0.00", 2));

    for (const selection of [literalValue, promotedValue, rawValue, dateValue, decimalValue]) {
      expect(selection.sqlType).toBeUndefined();
    }

    const [row] = await strictDb
      .select({
        literalType: fn.call<string>("toTypeName", literalValue).mapWith(String).as("literal_type"),
        literalRaw: literalValue.as("literal_raw"),
        literalMapped: literalValue.mapWith(Number).as("literal_mapped"),
        promotedType: fn.call<string>("toTypeName", promotedValue).mapWith(String).as("promoted_type"),
        promotedValue: promotedValue.as("promoted_value"),
        rawType: fn.call<string>("toTypeName", rawValue).mapWith(String).as("raw_type"),
        rawValue: rawValue.as("raw_value"),
        dateType: fn.call<string>("toTypeName", dateValue).mapWith(String).as("date_type"),
        dateRaw: dateValue.as("date_raw"),
        dateMapped: dateValue.mapWith((value) => new Date(String(value))).as("date_mapped"),
        decimalType: fn.call<string>("toTypeName", decimalValue).mapWith(String).as("decimal_type"),
        decimalValue: decimalValue.as("decimal_value"),
      })
      .from(schemaPrimitives)
      .where(ck.eq(schemaPrimitives.id, 1));

    const presentRow = expectPresent(row, "conditional transport fallback row");
    expect(presentRow).toEqual({
      literalType: "Int64",
      literalRaw: "1",
      literalMapped: 1,
      promotedType: "Int64",
      promotedValue: "3000000000",
      rawType: "UInt64",
      rawValue: "1",
      dateType: "DateTime64(3)",
      dateRaw: "2026-01-12T01:02:03.000Z",
      dateMapped: new Date("2026-01-12 01:02:03.000"),
      decimalType: "Decimal(18, 2)",
      decimalValue: "0",
    });
    expect(typeof presentRow.dateRaw).toBe("string");
    expectDate(presentRow.dateMapped);
  });

  it("covers every supported conditional anchor through the builder", async function testConditionalAnchorMatrix() {
    const db = createE2EDb({
      clickhouse_settings: {
        use_variant_as_common_type: 0,
      },
    });
    const boolValue = fn.if(true, fn.toBool(true), false);
    const intValue = fn.if(true, fn.toInt64(1), 0);
    const stringValue = fn.if(true, fn.toString("a"), "b");
    const lateMultiValue = fn.multiIf(false, 5, true, fn.toUInt64(7), 6);

    expect(boolValue.sqlType).toBe("Bool");
    expect(intValue.sqlType).toBe("Int64");
    expect(stringValue.sqlType).toBe("String");
    expect(lateMultiValue.sqlType).toBe("UInt64");

    const [row] = await db.select({
      boolType: fn.call<string>("toTypeName", boolValue).mapWith(String).as("bool_type"),
      boolValue: boolValue.as("bool_value"),
      intType: fn.call<string>("toTypeName", intValue).mapWith(String).as("int_type"),
      intValue: intValue.as("int_value"),
      stringType: fn.call<string>("toTypeName", stringValue).mapWith(String).as("string_type"),
      stringValue: stringValue.as("string_value"),
      lateMultiType: fn.call<string>("toTypeName", lateMultiValue).mapWith(String).as("late_multi_type"),
      lateMultiValue: lateMultiValue.as("late_multi_value"),
    });

    expect(expectPresent(row, "conditional anchor matrix row")).toEqual({
      boolType: "Bool",
      boolValue: true,
      intType: "Int64",
      intValue: "1",
      stringType: "String",
      stringValue: "a",
      lateMultiType: "UInt64",
      lateMultiValue: "7",
    });
  });

  it("keeps Float64 defaults typed through fn.coalesce and floating aggregates", async function testFloatCoalesceDefaults() {
    const db = createE2EDb({
      clickhouse_settings: {
        use_variant_as_common_type: 0,
      },
    });
    const float32Default = fn.coalesce(fn.toNullable(schemaPrimitives.float32_value), fn.toFloat32(0));
    const bfloatDefault = fn.coalesce(fn.toNullable(schemaPrimitives.bfloat16_value), fn.toBFloat16(0));

    const [directRow] = await db
      .select({
        floatDefault: fn.coalesce(schemaPrimitives.float64_value, 0).as("float_default"),
        float32Default: float32Default.as("float32_default"),
        float32Type: fn.call<string>("toTypeName", float32Default).mapWith(String).as("float32_type"),
        bfloatDefault: bfloatDefault.as("bfloat_default"),
        bfloatType: fn.call<string>("toTypeName", bfloatDefault).mapWith(String).as("bfloat_type"),
        uint64Default: fn.coalesce(schemaPrimitives.uint64_value, 0).as("uint64_default"),
        decimalDefault: fn.coalesce(schemaPrimitives.decimal_value, fn.toDecimal64("0.00", 2)).as("decimal_default"),
      })
      .from(schemaPrimitives)
      .where(ck.eq(schemaPrimitives.id, 1));

    expect(expectPresent(directRow, "direct float coalesce row")).toEqual({
      floatDefault: 6.5,
      float32Default: 3.25,
      float32Type: "Float32",
      bfloatDefault: 1.75,
      bfloatType: "BFloat16",
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
        amount: fn.coalesce(numericRollup.amount, fn.toDecimal64("0.00", 2)).as("amount"),
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
        targetOrder: fn
          .arrayJoin(fn.arrayZip([10001, 10002], [9001, 9002], ["alpha", "beta"], [1, 2]))
          .as("target_order"),
      }),
    );

    const tupleRows = await db
      .with(targetOrderTuples)
      .select({
        orderTicket: fn.tupleElement<string>(targetOrderTuples.targetOrder, 1).as("order_ticket"),
        login: fn.tupleElement<string>(targetOrderTuples.targetOrder, 2).as("login"),
        source: fn.tupleElement<string>(targetOrderTuples.targetOrder, 3).as("source"),
        shard: fn.tupleElement<string>(targetOrderTuples.targetOrder, 4).as("shard"),
      })
      .from(targetOrderTuples);

    expect(tupleRows).toEqual([
      {
        orderTicket: "10001",
        login: "9001",
        source: "alpha",
        shard: "1",
      },
      {
        orderTicket: "10002",
        login: "9002",
        source: "beta",
        shard: "2",
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
      randomSampleSize: fn.length(fn.arrayRandomSample<number>([1, 2, 3, 4], 2)).as("random_sample_size"),
      denseRanked: fn.arrayEnumerateDenseRanked(1, [[1, 2]], 2).as("dense_ranked"),
      uniqRanked: fn.arrayEnumerateUniqRanked(1, [[1, 2]], 2).as("uniq_ranked"),
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
      denseRanked: [[1, 2]],
      uniqRanked: [[1, 1]],
      partialShuffleSize: "4",
      rotatedRight: ["3", "1", "2"],
      shiftedRight: ["0", "1", "2"],
    });
    expect(presentRow.jaccard).toBeCloseTo(1 / 3);
  });

  it("supports fn.greatest / least / if / multiIf / nullIf / position* / argMax / argMin against real ClickHouse", async function testConditionalComparisonAndArgExtremumFunctions() {
    const db = createE2EDb();

    const rows = await db
      .select({
        id: users.id,
        tier: users.tier,
        greatest: fn.greatest<number>(users.id, 2).as("greatest"),
        least: fn.least<number>(users.id, 2).as("least"),
        tierIsVipFlag: fn
          .if(ck.eq(users.tier, "vip"), fn.toUInt8(1), fn.toUInt8(0))
          .mapWith(Number)
          .as("tier_is_vip_flag"),
        tierLabel: fn.multiIf(ck.eq(users.tier, "vip"), "V", ck.eq(users.tier, "standard"), "S", "T").as("tier_label"),
        nullIfVip: fn.nullIf<string>(users.tier, "vip").as("null_if_vip"),
        posAlice: fn.positionCaseInsensitive(users.name, "AL").as("pos_alice"),
        posAliceUtf8: fn.positionCaseInsensitiveUTF8(users.name, "AL").as("pos_alice_utf8"),
        posWithStart: fn.positionCaseInsensitive(users.name, "AL", fn.toUInt64(2)).as("pos_with_start"),
      })
      .from(users)
      .where(ck.lte(users.id, 3))
      .orderBy(users.id);

    // Notes:
    //   - tier seed: `multiIf(number % 7 = 0, 'vip', number % 3 = 0, 'standard', 'trial')`
    //     where number = id - 1. So id=1 → vip; id=2,3 → trial; id=4 → standard.
    //   - `fn.if(cond, toUInt8(1), toUInt8(0)).mapWith(Number)` explicitly opts into
    //     a JS number decoder instead of asserting a result type with a generic.
    expect(rows).toEqual([
      {
        id: 1,
        tier: "vip",
        greatest: 2,
        least: 1,
        tierIsVipFlag: 1,
        tierLabel: "V",
        nullIfVip: null,
        posAlice: "1",
        posAliceUtf8: "1",
        posWithStart: "0",
      },
      {
        id: 2,
        tier: "trial",
        greatest: 2,
        least: 2,
        tierIsVipFlag: 0,
        tierLabel: "T",
        nullIfVip: "trial",
        posAlice: "0",
        posAliceUtf8: "0",
        posWithStart: "0",
      },
      {
        id: 3,
        tier: "trial",
        greatest: 3,
        least: 2,
        tierIsVipFlag: 0,
        tierLabel: "T",
        nullIfVip: "trial",
        posAlice: "0",
        posAliceUtf8: "0",
        posWithStart: "0",
      },
    ]);

    const [aggregateRow] = await db
      .select({
        topName: fn.argMax<string>(users.name, users.id).as("top_name"),
        firstName: fn.argMin<string>(users.name, users.id).as("first_name"),
      })
      .from(users)
      .where(ck.lte(users.id, 3));

    expect(expectPresent(aggregateRow, "argMax/argMin row")).toEqual({
      topName: "charlie",
      firstName: "alice",
    });
  });

  it("supports fn arithmetic operators against real ClickHouse", async function testArithmeticOperators() {
    const db = createE2EDb();

    const rows = await db
      .select({
        id: users.id,
        plusTen: fn.plus(users.id, 10).as("plus_ten"),
        minusOne: fn.minus(users.id, 1).as("minus_one"),
        timesThree: fn.multiply(users.id, 3).as("times_three"),
        halfFloat: fn.divide(users.id, 2).as("half_float"),
        halfInt: fn.intDiv(users.id, 2).as("half_int"),
        modTwo: fn.modulo(users.id, 2).as("mod_two"),
        negated: fn.negate(users.id).as("negated"),
        absOfNegated: fn.abs(fn.negate(users.id)).as("abs_of_negated"),
        // OrZero variants: ClickHouse returns 0 instead of raising on zero divisor.
        safeDiv: fn.intDivOrZero(users.id, 0).as("safe_div"),
        safeMod: fn.moduloOrZero(users.id, 0).as("safe_mod"),
      })
      .from(users)
      .where(ck.lte(users.id, 3))
      .orderBy(users.id);

    expect(rows).toEqual([
      {
        id: 1,
        plusTen: 11,
        minusOne: 0,
        timesThree: 3,
        halfFloat: 0.5,
        halfInt: 0,
        modTwo: 1,
        negated: -1,
        absOfNegated: 1,
        safeDiv: 0,
        safeMod: 0,
      },
      {
        id: 2,
        plusTen: 12,
        minusOne: 1,
        timesThree: 6,
        halfFloat: 1,
        halfInt: 1,
        modTwo: 0,
        negated: -2,
        absOfNegated: 2,
        safeDiv: 0,
        safeMod: 0,
      },
      {
        id: 3,
        plusTen: 13,
        minusOne: 2,
        timesThree: 9,
        halfFloat: 1.5,
        halfInt: 1,
        modTwo: 1,
        negated: -3,
        absOfNegated: 3,
        safeDiv: 0,
        safeMod: 0,
      },
    ]);
  });

  it("reproduces the bucket-timestamp expr pattern using only fn.* arithmetic", async function testArithmeticBucketTimestamp() {
    const db = createE2EDb();
    // Seed: users.created_at = 2026-01-01 00:00:00 + (id - 1) seconds (UTC).
    // Anchor at midnight 2026-01-01 UTC, bucket size 60s.
    // The bucket calculation snaps each row's unix-seconds down to the
    // nearest 60-second boundary relative to anchor:
    //   intDiv(toInt64(toUnixTimestamp(created_at)) - anchor, 60) * 60 + anchor.
    const anchor = 1767225600;
    const bucketSeconds = 60;

    const rows = await db
      .select({
        id: users.id,
        // Pure number path: stays in Int32 family, sqlType + decoder both inherit number.
        bucketNumber: fn
          .plus(
            fn.multiply(
              fn.intDiv(fn.minus(fn.toUnixTimestamp(users.created_at, "UTC"), anchor), bucketSeconds),
              bucketSeconds,
            ),
            anchor,
          )
          .as("bucket_number"),
        // Int64 path mirroring the user's original ck.expr<number> example.
        // Int64 chain emits a string decoder; .mapWith(Number) opts into JS number.
        bucketInt64: fn
          .plus(
            fn.multiply(
              fn.intDiv(fn.minus(fn.toInt64(fn.toUnixTimestamp(users.created_at, "UTC")), anchor), bucketSeconds),
              bucketSeconds,
            ),
            anchor,
          )
          .mapWith<number>((value) => Number(value))
          .as("bucket_int64"),
      })
      .from(users)
      .where(ck.lte(users.id, 5))
      .orderBy(users.id);

    // All five rows fall within the same 60-second bucket starting at anchor.
    expect(rows).toEqual([
      { id: 1, bucketNumber: anchor, bucketInt64: anchor },
      { id: 2, bucketNumber: anchor, bucketInt64: anchor },
      { id: 3, bucketNumber: anchor, bucketInt64: anchor },
      { id: 4, bucketNumber: anchor, bucketInt64: anchor },
      { id: 5, bucketNumber: anchor, bucketInt64: anchor },
    ]);
  });

  it("uses fn arithmetic operators inside where/orderBy/groupBy clauses", async function testArithmeticInClauses() {
    const db = createE2EDb();

    // WHERE: select rows where id % 2 == 0 (even ids only)
    const evenRows = await db
      .select({ id: users.id })
      .from(users)
      .where(ck.eq(fn.modulo(users.id, 2), 0), ck.lte(users.id, 6))
      .orderBy(users.id);
    expect(evenRows).toEqual([{ id: 2 }, { id: 4 }, { id: 6 }]);

    // ORDER BY: sort by negate(id) desc → id ascending in result
    const orderedRows = await db
      .select({ id: users.id })
      .from(users)
      .where(ck.lte(users.id, 3))
      .orderBy(ck.desc(fn.negate(users.id)));
    expect(orderedRows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);

    // GROUP BY: count rows per intDiv(id, 2) bucket
    const groupRows = await db
      .select({
        bucket: fn.intDiv(users.id, 2).as("bucket"),
        cnt: fn.count().as("cnt"),
      })
      .from(users)
      .where(ck.lte(users.id, 6))
      .groupBy(fn.intDiv(users.id, 2))
      .orderBy(ck.asc(fn.intDiv(users.id, 2)));
    // id=1 → bucket 0; id=2,3 → bucket 1; id=4,5 → bucket 2; id=6,7 → bucket 3
    // With where id<=6: bucket 0 → {1}, bucket 1 → {2,3}, bucket 2 → {4,5}, bucket 3 → {6}
    expect(groupRows).toEqual([
      { bucket: 0, cnt: 1 },
      { bucket: 1, cnt: 2 },
      { bucket: 2, cnt: 2 },
      { bucket: 3, cnt: 1 },
    ]);
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

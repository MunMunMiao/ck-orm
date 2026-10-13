import { expect, it } from "bun:test";
import { ck, ckSql, ckTable, ckType, fn } from "./ck-orm";
import { createE2EDb, createTempTableName } from "./shared";
import { describeE2E, expectRejectsWithClickhouseError } from "./test-helpers";

describeE2E("ck-orm e2e timezone", function describeTimezone() {
  it("round-trips DateTime64 across UTC, Asia/Shanghai and America/New_York", async function testTimezoneRoundTrip() {
    const db = createE2EDb();
    const tableName = createTempTableName("tz_dt64");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      utc_at: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
      shanghai_at: ckType.dateTime64({ precision: 3, timezone: "Asia/Shanghai" }),
      ny_at: ckType.dateTime64({ precision: 3, timezone: "America/New_York" }),
    });

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          utc_at DateTime64(3, 'UTC'),
          shanghai_at DateTime64(3, 'Asia/Shanghai'),
          ny_at DateTime64(3, 'America/New_York')
        ) engine = Memory
      `);

      // Same moment in time, expressed three ways. ClickHouse stores DateTime64
      // as a single Unix scaled integer regardless of column timezone — the
      // timezone only affects display.
      const moment = new Date("2025-06-15T12:34:56.789Z");
      await db.insertJsonEachRow(table, [{ id: 1, utc_at: moment, shanghai_at: moment, ny_at: moment }]);

      const rows = await db.execute(ckSql`
        select
          id,
          toUnixTimestamp64Milli(utc_at) as utc_ms,
          toUnixTimestamp64Milli(shanghai_at) as shanghai_ms,
          toUnixTimestamp64Milli(ny_at) as ny_ms,
          toString(utc_at) as utc_str,
          toString(shanghai_at) as shanghai_str,
          toString(ny_at) as ny_str
        from ${ckSql.identifier(tableName)}
      `);

      expect(rows.length).toBe(1);
      const row = rows[0] as Record<string, string>;

      // All three columns store the same wall-clock instant.
      const expectedMs = String(moment.getTime());
      expect(row.utc_ms).toBe(expectedMs);
      expect(row.shanghai_ms).toBe(expectedMs);
      expect(row.ny_ms).toBe(expectedMs);

      // Display strings differ by zone offset.
      expect(row.utc_str).toBe("2025-06-15 12:34:56.789");
      expect(row.shanghai_str).toBe("2025-06-15 20:34:56.789"); // UTC+8
      expect(row.ny_str).toBe("2025-06-15 08:34:56.789"); // UTC−4 (EDT)
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("handles DST transition boundaries for America/New_York", async function testDstBoundary() {
    const db = createE2EDb();
    const tableName = createTempTableName("tz_dst");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      ny_at: ckType.dateTime64({ precision: 3, timezone: "America/New_York" }),
    });

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          ny_at DateTime64(3, 'America/New_York')
        ) engine = Memory
      `);

      // 2025 DST forward: 2025-03-09 02:00 EST → 03:00 EDT (skip 02:00-03:00 local).
      // 2025 DST back:    2025-11-02 02:00 EDT → 01:00 EST (repeat 01:00-02:00 local).
      // Each Date below is unambiguous because we anchor on UTC. Local hours
      // shown are after the DST rule has been applied.
      const beforeForward = new Date("2025-03-09T06:30:00.000Z"); // 01:30 EST (UTC-5)
      const afterForward = new Date("2025-03-09T07:30:00.000Z"); // 03:30 EDT (UTC-4)
      const beforeBack = new Date("2025-11-02T05:30:00.000Z"); // 01:30 EDT (UTC-4, first pass)
      const afterBack = new Date("2025-11-02T06:30:00.000Z"); // 01:30 EST (UTC-5, second pass)

      await db.insertJsonEachRow(table, [
        { id: 1, ny_at: beforeForward },
        { id: 2, ny_at: afterForward },
        { id: 3, ny_at: beforeBack },
        { id: 4, ny_at: afterBack },
      ]);

      const rows = await db.execute(ckSql`
        select id, toUnixTimestamp64Milli(ny_at) as ms, toString(ny_at) as display
        from ${ckSql.identifier(tableName)}
        order by id
      `);

      expect(rows.length).toBe(4);
      // Server-side conversion preserves Unix instant exactly across DST.
      expect((rows[0] as { ms: string }).ms).toBe(String(beforeForward.getTime()));
      expect((rows[1] as { ms: string }).ms).toBe(String(afterForward.getTime()));
      expect((rows[2] as { ms: string }).ms).toBe(String(beforeBack.getTime()));
      expect((rows[3] as { ms: string }).ms).toBe(String(afterBack.getTime()));

      // Display crosses the DST boundary: forward-skip leaves no 02:xx;
      // fall-back repeats 01:xx.
      expect((rows[0] as { display: string }).display).toBe("2025-03-09 01:30:00.000");
      expect((rows[1] as { display: string }).display).toBe("2025-03-09 03:30:00.000");
      expect((rows[2] as { display: string }).display).toBe("2025-11-02 01:30:00.000");
      expect((rows[3] as { display: string }).display).toBe("2025-11-02 01:30:00.000");
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("preserves pre-epoch DateTime64 values across timezone columns (Date32 range)", async function testPreEpochTimezone() {
    const db = createE2EDb();
    const tableName = createTempTableName("tz_preep");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      utc_at: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
      shanghai_at: ckType.dateTime64({ precision: 3, timezone: "Asia/Shanghai" }),
    });

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          utc_at DateTime64(3, 'UTC'),
          shanghai_at DateTime64(3, 'Asia/Shanghai')
        ) engine = Memory
      `);

      // Within DateTime64 range (1900-2299). 1969 was the original padStart bug
      // window — make sure both insertJsonEachRow and the round-trip stay clean.
      const preEpoch = new Date("1969-12-31T23:59:59.999Z");
      await db.insertJsonEachRow(table, [{ id: 1, utc_at: preEpoch, shanghai_at: preEpoch }]);

      const rows = await db.execute(ckSql`
        select
          toUnixTimestamp64Milli(utc_at) as utc_ms,
          toUnixTimestamp64Milli(shanghai_at) as shanghai_ms
        from ${ckSql.identifier(tableName)}
      `);

      const row = rows[0] as Record<string, string>;
      expect(row.utc_ms).toBe(String(preEpoch.getTime())); // -1
      expect(row.shanghai_ms).toBe(String(preEpoch.getTime()));
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("round-trips Date/Date32 columns from JS Date inputs", async function testDateAndDate32RoundTrip() {
    const db = createE2EDb();
    const tableName = createTempTableName("tz_date_only");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      d: ckType.date(),
      d32: ckType.date32(),
    });

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          d Date,
          d32 Date32
        ) engine = Memory
      `);

      // The encoder extracts the UTC YYYY-MM-DD portion, so a JS Date whose UTC
      // calendar day is 2026-06-15 always lands on that day regardless of the
      // wall-clock time. Pre-1970 only fits in Date32 (Date starts 1970-01-01).
      const modernUtc = new Date("2026-06-15T22:00:00.000Z"); // UTC day = 2026-06-15
      const date32Pre1970 = new Date("1969-12-31T23:30:00.000Z"); // UTC day = 1969-12-31
      await db.insertJsonEachRow(table, [
        { id: 1, d: modernUtc, d32: modernUtc },
        // Date column lower bound is 1970-01-01; clamp to that for Date.
        { id: 2, d: new Date("1970-01-01T00:00:00.000Z"), d32: date32Pre1970 },
        // Strings/numbers also pass through untouched.
        { id: 3, d: "2024-01-02", d32: "2024-01-02" },
      ]);

      const rows = await db.execute(ckSql`
        select id, toString(d) as d_str, toString(d32) as d32_str
        from ${ckSql.identifier(tableName)}
        order by id
      `);

      expect(rows).toEqual([
        { id: 1, d_str: "2026-06-15", d32_str: "2026-06-15" },
        { id: 2, d_str: "1970-01-01", d32_str: "1969-12-31" },
        { id: 3, d_str: "2024-01-02", d32_str: "2024-01-02" },
      ]);

      // Reading back as Date columns: the orm decoder returns JS Date at UTC midnight.
      const decoded = await db.select({ id: table.id, d: table.d, d32: table.d32 }).from(table).orderBy(table.id);
      expect(decoded[0].d).toEqual(new Date("2026-06-15T00:00:00.000Z"));
      expect(decoded[0].d32).toEqual(new Date("2026-06-15T00:00:00.000Z"));
      expect(decoded[1].d32).toEqual(new Date("1969-12-31T00:00:00.000Z"));
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("round-trips Time/Time64 columns as 'HH:MM:SS' strings, including negative and >24h values", async function testTimeRoundTrip() {
    const db = createE2EDb();
    const tableName = createTempTableName("tz_time");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      t: ckType.time(),
      t3: ckType.time64({ precision: 3 }),
      t6: ckType.time64({ precision: 6 }),
      t9: ckType.time64({ precision: 9 }),
    });

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          t Time,
          t3 Time64(3),
          t6 Time64(6),
          t9 Time64(9)
        ) engine = Memory
      `);

      await db.insertJsonEachRow(table, [
        { id: 1, t: "12:34:56", t3: "12:34:56.789", t6: "12:34:56.789012", t9: "12:34:56.789012345" },
        // Negative and >24h are key Time semantics that JS Date cannot express.
        { id: 2, t: "-01:30:00", t3: "-01:30:00.500", t6: "-01:30:00.500000", t9: "-01:30:00.500000000" },
        { id: 3, t: "999:59:59", t3: "999:59:59.999", t6: "999:59:59.999999", t9: "999:59:59.999999999" },
      ]);

      const rows = await db.execute(ckSql`
        select id, toString(t) as t, toString(t3) as t3, toString(t6) as t6, toString(t9) as t9
        from ${ckSql.identifier(tableName)}
        order by id
      `);

      expect(rows).toEqual([
        { id: 1, t: "12:34:56", t3: "12:34:56.789", t6: "12:34:56.789012", t9: "12:34:56.789012345" },
        { id: 2, t: "-01:30:00", t3: "-01:30:00.500", t6: "-01:30:00.500000", t9: "-01:30:00.500000000" },
        { id: 3, t: "999:59:59", t3: "999:59:59.999", t6: "999:59:59.999999", t9: "999:59:59.999999999" },
      ]);

      // Schema decoder returns string verbatim — round-trip preserves the exact
      // wire form (no precision loss, no Date conversion).
      const decoded = await db.select({ id: table.id, t: table.t, t3: table.t3 }).from(table).orderBy(table.id);
      expect(decoded[0].t).toBe("12:34:56");
      expect(decoded[1].t).toBe("-01:30:00");
      expect(decoded[2].t3).toBe("999:59:59.999");
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("rejects JS Date inputs for Time/Time64 columns at the client boundary", async function testTimeRejectsDate() {
    const db = createE2EDb();
    const tableName = createTempTableName("tz_time_reject");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      t: ckType.time(),
    });

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (id UInt32, t Time) engine = Memory
      `);

      // Encoder rejects Date because Time semantics (negative, >24h) cannot be
      // represented faithfully — this is a client-side guard, no request issued.
      await expect(db.insertJsonEachRow(table, [{ id: 1, t: new Date() as never }])).rejects.toThrow(
        /Time column values must be a 'HH:MM:SS' string or integer; JS Date is not appropriate/,
      );
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("binds JS Date values as UTC instants regardless of column or session timezone", async function testDateParamsKeepInstant() {
    const db = createE2EDb();
    const tableName = createTempTableName("tz_date_params");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      shanghaiAt: ckType.dateTime64("shanghai_at", { precision: 3, timezone: "Asia/Shanghai" }),
      plainAt: ckType.dateTime("plain_at"),
      history: ckType.array(ckType.dateTime64({ precision: 3, timezone: "Asia/Shanghai" })),
    });
    const moment = new Date("2026-06-15T00:00:00.123Z");
    const shanghaiSession = { clickhouse_settings: { session_timezone: "Asia/Shanghai" } } as const;

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          shanghai_at DateTime64(3, 'Asia/Shanghai'),
          plain_at DateTime,
          history Array(DateTime64(3, 'Asia/Shanghai'))
        ) engine = Memory
      `);

      await db
        .insert(table)
        .values({ id: 1, shanghaiAt: moment, plainAt: moment, history: [moment] })
        .execute(shanghaiSession);

      const stored = await db.execute(ckSql`
        select
          toUnixTimestamp64Milli(shanghai_at) as shanghai_ms,
          toUnixTimestamp(plain_at) as plain_s,
          toUnixTimestamp64Milli(history[1]) as history_ms
        from ${ckSql.identifier(tableName)}
      `);
      expect(stored).toEqual([
        {
          shanghai_ms: String(moment.getTime()),
          plain_s: Math.floor(moment.getTime() / 1000),
          history_ms: String(moment.getTime()),
        },
      ]);

      const matched = await db
        .select({ id: table.id })
        .from(table)
        .where(ck.eq(table.shanghaiAt, moment), ck.eq(table.plainAt, new Date("2026-06-15T00:00:00.000Z")))
        .execute(shanghaiSession);
      expect(matched).toEqual([{ id: 1 }]);

      const interpolated = await db.execute(
        ckSql`select toUnixTimestamp64Milli(${moment}) as ms, toUnixTimestamp64Milli(${[moment]}[1]) as array_ms`,
        shanghaiSession,
      );
      expect(interpolated).toEqual([{ ms: String(moment.getTime()), array_ms: String(moment.getTime()) }]);
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("matches JS Dates with inArray on DateTime, Nullable, LowCardinality and SimpleAggregateFunction(max, DateTime) columns", async function testDateTimeInArrayBinding() {
    const db = createE2EDb();
    const tableName = createTempTableName("tz_dt_in");
    const shanghaiDateTime = {
      ...ckType.dateTime(),
      sqlType: "DateTime('Asia/Shanghai')",
    } as unknown as ReturnType<typeof ckType.dateTime>;
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      ts: ckType.dateTime("ts"),
      tsn: ckType.nullable(ckType.dateTime("tsn")),
      lc: ckType.lowCardinality(ckType.dateTime("lc")),
      smax: ckType.simpleAggregateFunction<Date>("smax", {
        name: "max",
        value: shanghaiDateTime,
      }),
    });
    const wholeSecond = new Date("2026-06-15T00:00:00Z");
    const withMilliseconds = new Date("2026-06-15T00:00:01.999Z");

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          ts DateTime('Asia/Shanghai'),
          tsn Nullable(DateTime),
          lc LowCardinality(DateTime),
          smax SimpleAggregateFunction(max, DateTime('Asia/Shanghai'))
        ) engine = Memory
        settings allow_suspicious_low_cardinality_types = 1
      `);

      await db.insert(table).values([
        { id: 1, ts: wholeSecond, tsn: wholeSecond, lc: wholeSecond, smax: wholeSecond },
        { id: 2, ts: withMilliseconds, tsn: withMilliseconds, lc: withMilliseconds, smax: withMilliseconds },
      ]);

      const stored = await db.execute(ckSql`
        select
          id,
          toUnixTimestamp(ts) as ts,
          toUnixTimestamp(tsn) as tsn,
          toUnixTimestamp(lc) as lc,
          toUnixTimestamp(smax) as smax
        from ${ckSql.identifier(tableName)}
        order by id
      `);
      expect(stored).toEqual([
        { id: 1, ts: 1781481600, tsn: 1781481600, lc: 1781481600, smax: 1781481600 },
        { id: 2, ts: 1781481601, tsn: 1781481601, lc: 1781481601, smax: 1781481601 },
      ]);

      expect(
        await db
          .select({ id: table.id })
          .from(table)
          .where(ck.inArray(table.ts, [wholeSecond])),
      ).toEqual([{ id: 1 }]);
      expect(
        await db
          .select({ id: table.id })
          .from(table)
          .where(ck.inArray(table.tsn, [wholeSecond])),
      ).toEqual([{ id: 1 }]);
      expect(
        await db
          .select({ id: table.id })
          .from(table)
          .where(ck.inArray(table.lc, [wholeSecond])),
      ).toEqual([{ id: 1 }]);
      expect(
        await db
          .select({ id: table.id })
          .from(table)
          .where(ck.inArray(table.smax, [wholeSecond])),
      ).toEqual([{ id: 1 }]);
      expect(
        await db
          .select({ id: table.id })
          .from(table)
          .where(ck.notInArray(table.ts, [wholeSecond])),
      ).toEqual([{ id: 2 }]);
      expect(await db.select({ id: table.id }).from(table).where(ck.eq(table.lc, wholeSecond))).toEqual([{ id: 1 }]);
      expect(
        await db
          .select({ id: table.id })
          .from(table)
          .where(ck.gte(table.ts, new Date("2026-06-15T00:00:00.500Z"))),
      ).toEqual([{ id: 2 }]);

      await expectRejectsWithClickhouseError(
        db
          .select({ id: table.id })
          .from(table)
          .where(ck.inArray(table.ts, [withMilliseconds]))
          .execute(),
        {
          kind: "request_failed",
          executionState: "rejected",
          clickhouseCode: 53,
          clickhouseName: "TYPE_MISMATCH",
        },
      );
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("compares JS Date keys of Map values as UTC instants", async function testMapDateKeyInstant() {
    const db = createE2EDb();
    const shanghaiKeyed = fn.cast(
      ck.expr(ckSql`map(toDateTime64('2026-06-15 08:00:00.123', 3, 'Asia/Shanghai'), 'x')`),
      "Map(DateTime64(3, 'Asia/Shanghai'), String)",
    );
    const secondKeyed = fn.cast(
      ck.expr(ckSql`map(toDateTime('2026-06-15 08:00:00', 'Asia/Shanghai'), 'x')`),
      "Map(DateTime('Asia/Shanghai'), String)",
    );

    const [row] = await db.execute(ckSql`
      select
        ${ck.eq(shanghaiKeyed, new Map([[new Date("2026-06-15T00:00:00.123Z"), "x"]]))} as precise_hit,
        ${ck.eq(secondKeyed, new Map([[new Date("2026-06-15T00:00:00.000Z"), "x"]]))} as whole_hit
    `);

    expect(row).toEqual({ precise_hit: 1, whole_hit: 1 });
  });
});

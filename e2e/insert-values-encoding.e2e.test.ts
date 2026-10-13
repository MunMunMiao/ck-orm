import { expect, it } from "bun:test";
import { ckSql, ckTable, ckType } from "./ck-orm";
import { createE2EDb, createTempTableName } from "./shared";
import { describeE2E } from "./test-helpers";

// Pins the INSERT ... VALUES contract. Several groups fail on the current
// parameter encoding; phase 2 switches `.values()` to literal Values text.

const setSqlType = (column: { sqlType: string }, sqlType: string) => {
  column.sqlType = sqlType;
};

const utcDateTime = ckType.dateTime();
setSqlType(utcDateTime, "DateTime('UTC')");

const whole = new Date("2026-06-15T00:00:00.000Z");
const ms999 = new Date("2026-06-15T00:00:00.999Z");
const minus500 = new Date("1969-12-31T23:59:59.500Z");
const minus1500 = new Date("1969-12-31T23:59:58.500Z");
const fourDigit = new Date("1970-01-01T00:33:46.000Z");
const mixedThird = new Date("2026-06-15T00:00:00.123Z");

const truncSecond = (ms: number) => Math.trunc(ms / 1000) * 1000;
const truncTenth = (ms: number) => Math.trunc(ms / 100) * 100;

const INT128_MIN = "-170141183460469231731687303715884105728";
const INT128_MAX = "170141183460469231731687303715884105727";
const UINT256_MAX = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
const UINT64_MAX = "18446744073709551615";
const INT64_MIN = "-9223372036854775808";

const stringSamples = [
  "'",
  "\\",
  "\\'",
  "\t",
  "\n",
  "\r",
  "\0",
  "\b",
  "\f",
  "\v",
  "中文😀",
  "x'); DROP TABLE t; --",
  "'), ('",
  "DEFAULT",
  "NULL",
  "{orm_param1:String}",
  "lower('X')",
];

describeE2E("ck-orm e2e insert values encoding", function describeInsertValuesEncoding() {
  it("stores JS Date instants without a timezone shift", async function testInsertValuesDateInstants() {
    const db = createE2EDb();
    const tableName = createTempTableName("iv_dates");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      dt: ckType.dateTime("dt"),
      dt64: ckType.dateTime64("dt64", { precision: 3, timezone: "Asia/Shanghai" }),
      dt64_6: ckType.dateTime64("dt64_6", { precision: 6, timezone: "America/New_York" }),
      dt64_0: ckType.dateTime64("dt64_0", { precision: 0, timezone: "UTC" }),
      dt64_1: ckType.dateTime64("dt64_1", { precision: 1, timezone: "UTC" }),
      ndt64: ckType.nullable("ndt64", ckType.dateTime64({ precision: 3, timezone: "UTC" })),
      arr: ckType.array(ckType.dateTime64({ precision: 3, timezone: "UTC" })),
      m: ckType.map(ckType.string(), utcDateTime),
      tp: ckType.tuple(ckType.string(), ckType.dateTime64({ precision: 3, timezone: "Asia/Shanghai" })),
    });
    setSqlType(table.dt, "DateTime('Asia/Shanghai')");

    const instants = [whole, ms999, minus500, minus1500, fourDigit, whole];
    const exact = instants.map((value) => value.getTime());
    const inRangeDt = [whole, ms999, whole, whole, whole, whole];

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          dt DateTime('Asia/Shanghai'),
          dt64 DateTime64(3, 'Asia/Shanghai'),
          dt64_6 DateTime64(6, 'America/New_York'),
          dt64_0 DateTime64(0, 'UTC'),
          dt64_1 DateTime64(1, 'UTC'),
          ndt64 Nullable(DateTime64(3, 'UTC')),
          arr Array(DateTime64(3, 'UTC')),
          m Map(String, DateTime('UTC')),
          tp Tuple(String, DateTime64(3, 'Asia/Shanghai'))
        ) engine = Memory
      `);

      await db.insert(table).values([
        {
          id: 1,
          dt: inRangeDt[0],
          dt64: instants[0],
          dt64_6: instants[0],
          dt64_0: instants[0],
          dt64_1: instants[0],
          ndt64: instants[0],
          arr: [minus1500, mixedThird, minus500],
          m: { early: whole },
          tp: ["mark", whole],
        },
        {
          id: 2,
          dt: inRangeDt[1],
          dt64: instants[1],
          dt64_6: instants[1],
          dt64_0: instants[1],
          dt64_1: instants[1],
          ndt64: instants[1],
          arr: [whole],
          m: { early: whole, late: ms999 },
          tp: ["mark", ms999],
        },
        {
          id: 3,
          dt: inRangeDt[2],
          dt64: instants[2],
          dt64_6: instants[2],
          dt64_0: instants[2],
          dt64_1: instants[2],
          ndt64: instants[2],
          arr: [whole],
          m: { early: whole },
          tp: ["mark", whole],
        },
        {
          id: 4,
          dt: inRangeDt[3],
          dt64: instants[3],
          dt64_6: instants[3],
          dt64_0: instants[3],
          dt64_1: instants[3],
          ndt64: instants[3],
          arr: [whole],
          m: { early: whole },
          tp: ["mark", whole],
        },
        {
          id: 5,
          dt: inRangeDt[4],
          dt64: instants[4],
          dt64_6: instants[4],
          dt64_0: instants[4],
          dt64_1: instants[4],
          ndt64: instants[4],
          arr: [whole],
          m: { early: whole },
          tp: ["mark", whole],
        },
        {
          id: 6,
          dt: inRangeDt[5],
          dt64: instants[5],
          dt64_6: instants[5],
          dt64_0: instants[5],
          dt64_1: instants[5],
          ndt64: null,
          arr: [whole],
          m: { early: whole },
          tp: ["mark", whole],
        },
      ]);

      const rows = await db.execute(ckSql`
        select
          id,
          toString(toUnixTimestamp(dt) * 1000) as dt,
          toString(toUnixTimestamp64Milli(dt64)) as dt64,
          toString(toUnixTimestamp64Milli(dt64_6)) as dt64_6,
          toString(toUnixTimestamp64Milli(dt64_0)) as dt64_0,
          toString(toUnixTimestamp64Milli(dt64_1)) as dt64_1,
          if(isNull(ndt64), 'NULL', toString(toUnixTimestamp64Milli(ndt64))) as ndt64,
          arrayMap(x -> toString(toUnixTimestamp64Milli(x)), arr) as arr,
          mapApply((k, v) -> (k, toString(toUnixTimestamp(v) * 1000)), m) as m,
          tupleElement(tp, 1) as tp_label,
          toString(toUnixTimestamp64Milli(tupleElement(tp, 2))) as tp_ms
        from ${ckSql.identifier(tableName)}
        order by id
      `);

      expect(rows.map((row) => row.dt)).toEqual(inRangeDt.map((value) => String(truncSecond(value.getTime()))));
      expect(rows.map((row) => row.dt64)).toEqual(exact.map(String));
      expect(rows.map((row) => row.dt64_6)).toEqual(exact.map(String));
      expect(rows.map((row) => row.dt64_0)).toEqual(exact.map((ms) => String(truncSecond(ms))));
      expect(rows.map((row) => row.dt64_1)).toEqual(exact.map((ms) => String(truncTenth(ms))));
      expect(rows.map((row) => row.ndt64)).toEqual([...exact.slice(0, 5).map(String), "NULL"]);
      expect(rows[0]?.arr).toEqual([
        String(minus1500.getTime()),
        String(mixedThird.getTime()),
        String(minus500.getTime()),
      ]);
      expect(rows[1]?.m).toEqual({
        early: String(truncSecond(whole.getTime())),
        late: String(truncSecond(ms999.getTime())),
      });
      expect(rows[1]?.tp_label).toBe("mark");
      expect(rows[1]?.tp_ms).toBe(String(ms999.getTime()));
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("reads datetime strings in the column timezone and numbers as unix seconds", async function testInsertValuesDatetimeEscapes() {
    const db = createE2EDb();
    const tableName = createTempTableName("iv_dt_escape");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      shanghai: ckType.dateTime("shanghai"),
      shanghai64: ckType.dateTime64("shanghai64", { precision: 3, timezone: "Asia/Shanghai" }),
      utc: ckType.dateTime("utc"),
      utc64: ckType.dateTime64("utc64", { precision: 3, timezone: "UTC" }),
    });
    setSqlType(table.shanghai, "DateTime('Asia/Shanghai')");
    setSqlType(table.utc, "DateTime('UTC')");

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          shanghai DateTime('Asia/Shanghai'),
          shanghai64 DateTime64(3, 'Asia/Shanghai'),
          utc DateTime('UTC'),
          utc64 DateTime64(3, 'UTC')
        ) engine = Memory
      `);

      await db.insert(table).values({
        id: 1,
        shanghai: "2026-06-15 08:00:00" as never,
        shanghai64: "2026-06-15 08:00:00" as never,
        utc: 1781481600 as never,
        utc64: 1781481600 as never,
      });

      expect(
        await db.execute(ckSql`
          select
            toString(toUnixTimestamp(shanghai)) as shanghai,
            toString(toUnixTimestamp(shanghai64)) as shanghai64,
            toString(toUnixTimestamp(utc)) as utc,
            toString(toUnixTimestamp(utc64)) as utc64
          from ${ckSql.identifier(tableName)}
        `),
      ).toEqual([{ shanghai: "1781481600", shanghai64: "1781481600", utc: "1781481600", utc64: "1781481600" }]);
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("round-trips numeric extremes and finite floats", async function testInsertValuesNumbers() {
    const db = createE2EDb();
    const tableName = createTempTableName("iv_numbers");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      u64: ckType.uint64(),
      i64: ckType.int64(),
      i128: ckType.string("i128"),
      u256: ckType.string("u256"),
      dec: ckType.decimal({ precision: 38, scale: 10 }),
      f64: ckType.float64(),
      f32: ckType.float32(),
      bf: ckType.bfloat16(),
      flag: ckType.bool(),
      u64s: ckType.array(ckType.uint64()),
      u64m: ckType.map(ckType.string(), ckType.uint64()),
    });
    setSqlType(table.i128, "Int128");
    setSqlType(table.u256, "UInt256");

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          u64 UInt64,
          i64 Int64,
          i128 Int128,
          u256 UInt256,
          dec Decimal(38, 10),
          f64 Float64,
          f32 Float32,
          bf BFloat16,
          flag Bool,
          u64s Array(UInt64),
          u64m Map(String, UInt64)
        ) engine = Memory
      `);

      await db.insert(table).values([
        {
          id: 1,
          u64: UINT64_MAX,
          i64: INT64_MIN,
          i128: INT128_MIN,
          u256: "0",
          dec: "-12345678901234567890.0123456789",
          f64: -1.5e-3,
          f32: 1.5,
          bf: 1.5,
          flag: true,
          u64s: [UINT64_MAX],
          u64m: { max: UINT64_MAX },
        },
        {
          id: 2,
          u64: "0",
          i64: "0",
          i128: INT128_MAX,
          u256: UINT256_MAX,
          dec: "0",
          f64: 0,
          f32: 0,
          bf: 0,
          flag: false,
          u64s: ["0"],
          u64m: { zero: "0" },
        },
      ]);

      expect(
        await db.execute(ckSql`
          select
            id,
            toString(u64) as u64,
            toString(i64) as i64,
            toString(i128) as i128,
            toString(u256) as u256,
            toString(dec) as dec,
            toString(f64) as f64,
            toString(f32) as f32,
            toString(bf) as bf,
            toString(flag) as flag,
            arrayMap(x -> toString(x), u64s) as u64s,
            mapApply((k, v) -> (k, toString(v)), u64m) as u64m
          from ${ckSql.identifier(tableName)}
          order by id
        `),
      ).toEqual([
        {
          id: 1,
          u64: UINT64_MAX,
          i64: INT64_MIN,
          i128: INT128_MIN,
          u256: "0",
          dec: "-12345678901234567890.0123456789",
          f64: "-0.0015",
          f32: "1.5",
          bf: "1.5",
          flag: "true",
          u64s: [UINT64_MAX],
          u64m: { max: UINT64_MAX },
        },
        {
          id: 2,
          u64: "0",
          i64: "0",
          i128: INT128_MAX,
          u256: UINT256_MAX,
          dec: "0",
          f64: "0",
          f32: "0",
          bf: "0",
          flag: "false",
          u64s: ["0"],
          u64m: { zero: "0" },
        },
      ]);
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("round-trips strings, escapes, and scalar wire types byte for byte", async function testInsertValuesStrings() {
    const db = createE2EDb();
    const tableName = createTempTableName("iv_strings");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      s: ckType.string(),
      arr: ckType.array(ckType.string()),
      mp: ckType.map(ckType.string(), ckType.string()),
      fixed: ckType.fixedString({ length: 4 }),
      uuid: ckType.uuid(),
      ip4: ckType.ipv4(),
      ip6: ckType.ipv6(),
      color: ckType.enum8({ red: 1, blue: 2 }),
      lc: ckType.lowCardinality(ckType.string()),
      maybe: ckType.nullable(ckType.string()),
    });

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          s String,
          arr Array(String),
          mp Map(String, String),
          fixed FixedString(4),
          uuid UUID,
          ip4 IPv4,
          ip6 IPv6,
          color Enum8('red' = 1, 'blue' = 2),
          lc LowCardinality(String),
          maybe Nullable(String)
        ) engine = Memory
      `);

      await db.insert(table).values(
        stringSamples.map((sample, index) => ({
          id: index + 1,
          s: sample,
          arr: [sample],
          mp: { k: sample },
          fixed: "abcd",
          uuid: "550e8400-e29b-41d4-a716-446655440000",
          ip4: "127.0.0.1",
          ip6: "::1",
          color: "red" as const,
          lc: "lc",
          maybe: index === stringSamples.length - 1 ? null : "present",
        })),
      );

      const rows = await db.execute(ckSql`
        select id, s, arr, mp, toString(fixed) as fixed, toString(uuid) as uuid, toString(ip4) as ip4, toString(ip6) as ip6, toString(color) as color, lc, if(isNull(maybe), 'NULL', maybe) as maybe
        from ${ckSql.identifier(tableName)}
        order by id
      `);

      expect(rows.map((row) => row.s)).toEqual(stringSamples);
      expect(rows.map((row) => row.arr)).toEqual(stringSamples.map((sample) => [sample]));
      expect(rows.map((row) => row.mp)).toEqual(stringSamples.map((sample) => ({ k: sample })));
      expect(rows[0]).toMatchObject({
        fixed: "abcd",
        uuid: "550e8400-e29b-41d4-a716-446655440000",
        ip4: "127.0.0.1",
        ip6: "::1",
        color: "red",
        lc: "lc",
        maybe: "present",
      });
      expect(rows.at(-1)?.maybe).toBe("NULL");
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("round-trips arrays, tuples, maps, and nested columns", async function testInsertValuesContainers() {
    const db = createE2EDb();
    const tableName = createTempTableName("iv_containers");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      nums: ckType.array(ckType.nullable(ckType.int32())),
      pair: ckType.tuple(ckType.string(), ckType.int32()),
      named: ckType.tuple(ckType.string(), ckType.int32()),
      labels: ckType.map(ckType.string(), ckType.string()),
      grid: ckType.array(ckType.array(ckType.string())),
      lines: ckType.nested({
        sku: ckType.string(),
        qty: ckType.uint32(),
      }),
    });
    setSqlType(table.named, "Tuple(label String, n Int32)");

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          nums Array(Nullable(Int32)),
          pair Tuple(String, Int32),
          named Tuple(label String, n Int32),
          labels Map(String, String),
          grid Array(Array(String)),
          lines Nested(sku String, qty UInt32)
        ) engine = Memory
      `);

      await db.insert(table).values({
        id: 1,
        nums: [1, null, -2],
        pair: ["ab", 3],
        named: ["nm", 4],
        labels: { a: "b" },
        grid: [["x"], []],
        lines: [
          { sku: "a", qty: 1 },
          { sku: "b", qty: 2 },
        ],
      });

      expect(
        await db.execute(ckSql`
          select
            nums,
            tupleElement(pair, 1) as pair_a,
            tupleElement(pair, 2) as pair_b,
            tupleElement(named, 'label') as named_label,
            tupleElement(named, 'n') as named_n,
            labels,
            grid,
            \`lines.sku\` as sku,
            \`lines.qty\` as qty
          from ${ckSql.identifier(tableName)}
        `),
      ).toEqual([
        {
          nums: [1, null, -2],
          pair_a: "ab",
          pair_b: 3,
          named_label: "nm",
          named_n: 4,
          labels: { a: "b" },
          grid: [["x"], []],
          sku: ["a", "b"],
          qty: [1, 2],
        },
      ]);
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("stores JSON, Dynamic, and Variant values rather than placeholders", async function testInsertValuesDynamicTypes() {
    const db = createE2EDb();
    const tableName = createTempTableName("iv_dynamic");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      doc: ckType.json<{ n: number; s: string }>(),
      dyn: ckType.dynamic(),
      v: ckType.variant(ckType.string(), ckType.uint64()),
    });

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          doc JSON,
          dyn Dynamic,
          v Variant(String, UInt64)
        ) engine = Memory
      `);

      await db.insert(table).values([
        { id: 1, doc: { n: 1, s: "x" }, dyn: "abc", v: "7" },
        { id: 2, doc: { n: 1, s: "x" }, dyn: 42, v: 7 as never },
        { id: 3, doc: { n: 1, s: "x" }, dyn: 1.5, v: "7" },
      ]);

      expect(
        await db.execute(ckSql`
          select
            id,
            toString(doc.n) as n,
            toString(doc.s) as s,
            dynamicType(dyn) as dyn_type,
            toString(dyn) as dyn_value,
            variantType(v) as v_type,
            toString(v) as v_value
          from ${ckSql.identifier(tableName)}
          order by id
        `),
      ).toEqual([
        { id: 1, n: "1", s: "x", dyn_type: "String", dyn_value: "abc", v_type: "String", v_value: "7" },
        { id: 2, n: "1", s: "x", dyn_type: "Int64", dyn_value: "42", v_type: "UInt64", v_value: "7" },
        { id: 3, n: "1", s: "x", dyn_type: "Float64", dyn_value: "1.5", v_type: "String", v_value: "7" },
      ]);
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("applies per-row DEFAULT when columns are omitted", async function testInsertValuesPerRowDefault() {
    const db = createE2EDb();
    const tableName = createTempTableName("iv_defaults");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      note: ckType.string().default(ckSql`concat('n-', toString(id))`),
      maybe: ckType.nullable(ckType.string()).default(ckSql`'fallback'`),
      v: ckType.variant(ckType.string(), ckType.uint64()).default(ckSql`'vdflt'`),
      saf: ckType
        .simpleAggregateFunction<string | null>("saf", {
          name: "anyLast",
          value: ckType.nullable(ckType.string()),
        })
        .default(ckSql`'sdflt'`),
      lines: ckType.nested({
        sku: ckType.string(),
      }),
    });

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          note String default concat('n-', toString(id)),
          maybe Nullable(String) default 'fallback',
          v Variant(String, UInt64) default 'vdflt',
          saf SimpleAggregateFunction(anyLast, Nullable(String)) default 'sdflt',
          lines Nested(sku String)
        ) engine = AggregatingMergeTree order by id
      `);

      await db
        .insert(table)
        .values([
          { id: 1, note: "custom", maybe: "set", v: "explicit", saf: "set", lines: [{ sku: "a" }] },
          { id: 2 },
          { id: 3, maybe: null },
        ]);

      expect(
        await db.execute(ckSql`
          select
            id,
            note,
            if(isNull(maybe), 'NULL', maybe) as maybe,
            concat(variantType(v), ':', toString(v)) as v,
            if(isNull(saf), 'NULL', toString(saf)) as saf,
            toString(length(\`lines.sku\`)) as nsku,
            \`lines.sku\` as sku
          from ${ckSql.identifier(tableName)}
          order by id
        `),
      ).toEqual([
        { id: 1, note: "custom", maybe: "set", v: "String:explicit", saf: "set", nsku: "1", sku: ["a"] },
        { id: 2, note: "n-2", maybe: "fallback", v: "String:vdflt", saf: "sdflt", nsku: "0", sku: [] },
        { id: 3, note: "n-3", maybe: "NULL", v: "String:vdflt", saf: "sdflt", nsku: "0", sku: [] },
      ]);
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("inserts thousands of rows and a long string in one values call", async function testInsertValuesScale() {
    const db = createE2EDb();
    const wideName = createTempTableName("iv_scale");
    const longName = createTempTableName("iv_long");
    const wide = ckTable(wideName, {
      id: ckType.uint32(),
      a: ckType.string(),
      b: ckType.int32(),
      c: ckType.string(),
      d: ckType.int32(),
    });
    const longText = ckTable(longName, {
      id: ckType.uint32(),
      body: ckType.string(),
    });
    const problems: string[] = [];

    try {
      await db.command(ckSql`
          create table ${ckSql.identifier(wideName)} (
            id UInt32,
            a String,
            b Int32,
            c String,
            d Int32
          ) engine = Memory
        `);
      await db.command(ckSql`
          create table ${ckSql.identifier(longName)} (
            id UInt32,
            body String
          ) engine = Memory
        `);

      try {
        await db.insert(wide).values(
          Array.from({ length: 2000 }, (_, index) => ({
            id: index + 1,
            a: "a",
            b: index,
            c: "c",
            d: index,
          })),
        );
        const counted = await db.execute(ckSql`select toString(count()) as n from ${ckSql.identifier(wideName)}`);
        if (counted[0]?.n !== "2000") problems.push(`wide count ${String(counted[0]?.n)}`);
      } catch (error) {
        problems.push(`wide: ${error instanceof Error ? error.message : String(error)}`);
      }

      try {
        const body = "x".repeat(200_000);
        await db.insert(longText).values({ id: 1, body });
        const measured = await db.execute(ckSql`select toString(length(body)) as n from ${ckSql.identifier(longName)}`);
        if (measured[0]?.n !== "200000") problems.push(`long length ${String(measured[0]?.n)}`);
      } catch (error) {
        problems.push(`long: ${error instanceof Error ? error.message : String(error)}`);
      }

      expect(problems).toEqual([]);
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(wideName)}`);
      await db.command(ckSql`drop table if exists ${ckSql.identifier(longName)}`);
    }
  }, 120_000);

  it("rejects an ISO datetime string with a Z suffix", async function testInsertValuesRejectsIsoZ() {
    const db = createE2EDb();
    const tableName = createTempTableName("iv_reject_z");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      dt64: ckType.dateTime64("dt64", { precision: 3, timezone: "UTC" }),
    });

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          dt64 DateTime64(3, 'UTC')
        ) engine = Memory
      `);

      await expect(
        db
          .insert(table)
          .values({ id: 1, dt64: "2026-06-15T00:00:00Z" as never })
          .execute(),
      ).rejects.toThrow();
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });
});

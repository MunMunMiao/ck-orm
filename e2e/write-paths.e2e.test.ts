import { beforeEach, expect, it } from "bun:test";
import { chTable, chType, ck, csql } from "./ck-orm";
import { auditEvents, createE2EDb, createTempTableName, writePathBigInts } from "./shared";
import { describeE2E } from "./test-helpers";

describeE2E("ck-orm e2e write paths", function describeWritePaths() {
  beforeEach(async function truncateWritePathTables() {
    const db = createE2EDb();
    await db.command(csql`TRUNCATE TABLE audit_events`);
    await db.command(csql`TRUNCATE TABLE write_path_bigints`);
  });

  it("writes with insert builder via direct await", async function testInsertBuilderAwait() {
    const db = createE2EDb();
    await db.insert(auditEvents).values({
      id: 1,
      user_id: 1,
      event_name: "builder_insert",
      created_at: new Date("2026-04-22T11:00:00.000Z"),
    });

    const rows = await db
      .select({
        id: auditEvents.id,
        userId: auditEvents.user_id,
        eventName: auditEvents.event_name,
      })
      .from(auditEvents)
      .orderBy(auditEvents.id);

    expect(rows).toEqual([
      {
        id: 1,
        userId: 1,
        eventName: "builder_insert",
      },
    ]);
  });

  it("writes with insert builder via explicit execute()", async function testInsertBuilderExecute() {
    const db = createE2EDb();
    await db
      .insert(auditEvents)
      .values({
        id: 2,
        user_id: 2,
        event_name: "builder_execute",
        created_at: new Date("2026-04-22T11:01:00.000Z"),
      })
      .execute();

    const rows = await db
      .select({
        id: auditEvents.id,
        eventName: auditEvents.event_name,
      })
      .from(auditEvents)
      .orderBy(auditEvents.id);

    expect(rows).toEqual([
      {
        id: 2,
        eventName: "builder_execute",
      },
    ]);
  });

  it("writes array rows through insertJsonEachRow", async function testInsertJsonEachRowArray() {
    const db = createE2EDb();
    await db.insertJsonEachRow(auditEvents, [
      {
        id: 11,
        user_id: 1,
        event_name: "array_insert_one",
        created_at: "2026-04-22 11:10:00.000",
      },
      {
        id: 12,
        user_id: 2,
        event_name: "array_insert_two",
        created_at: "2026-04-22 11:11:00.000",
      },
    ]);

    const rows = await db
      .select({
        id: auditEvents.id,
        eventName: auditEvents.event_name,
      })
      .from(auditEvents)
      .orderBy(auditEvents.id);

    expect(rows).toEqual([
      {
        id: 11,
        eventName: "array_insert_one",
      },
      {
        id: 12,
        eventName: "array_insert_two",
      },
    ]);
  });

  it("writes async iterable rows through insertJsonEachRow", async function testInsertJsonEachRowAsyncIterable() {
    const db = createE2EDb();
    await db.insertJsonEachRow(
      auditEvents,
      (async function* rows() {
        yield {
          id: 21,
          user_id: 1,
          event_name: "async_insert_one",
          created_at: "2026-04-22 11:20:00.000",
        };
        yield {
          id: 22,
          user_id: 3,
          event_name: "async_insert_two",
          created_at: "2026-04-22 11:21:00.000",
        };
      })(),
    );

    const rows = await db
      .select({
        id: auditEvents.id,
        eventName: auditEvents.event_name,
      })
      .from(auditEvents)
      .orderBy(auditEvents.id);

    expect(rows).toEqual([
      {
        id: 21,
        eventName: "async_insert_one",
      },
      {
        id: 22,
        eventName: "async_insert_two",
      },
    ]);
  });

  it("treats empty JSONEachRow array inserts as no-ops", async function testInsertJsonEachRowEmptyArray() {
    const db = createE2EDb();
    await db.insertJsonEachRow(auditEvents, []);

    expect(await db.count(auditEvents)).toBe(0);
  });

  it("writes JSONEachRow rows with defaults, nullable values and compound columns", async function testJsonEachRowComplexColumns() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_json_each_row_complex");
    const scope = chTable(tempTable, {
      id: chType.int32(),
      note: chType.string().default(csql`'auto'`),
      nullable_note: chType.nullable(chType.string()),
      tags: chType.array(chType.nullable(chType.string())),
      scores: chType.map(chType.string(), chType.int32()),
      pair: chType.tuple(chType.string(), chType.int32()),
      doubled: chType.int32().materialized(csql`id * 2`),
      label: chType.string().aliasExpr(csql`concat('id=', toString(id))`),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);
      await session.insertJsonEachRow(scope, [
        {
          id: 1,
          nullable_note: null,
          tags: ["first", null],
          scores: { gold: 3 },
          pair: ["one", 11],
        },
        {
          id: 2,
          nullable_note: "present",
          tags: [],
          scores: {},
          pair: ["two", 22],
        },
      ]);

      const rows = await session
        .select({
          id: scope.id,
          note: scope.note,
          nullableNote: scope.nullable_note,
          tags: scope.tags,
          scores: scope.scores,
          pair: scope.pair,
          doubled: scope.doubled,
          label: scope.label,
        })
        .from(scope)
        .orderBy(scope.id);

      expect(rows).toEqual([
        {
          id: 1,
          note: "auto",
          nullableNote: null,
          tags: ["first", null],
          scores: { gold: 3 },
          pair: ["one", 11],
          doubled: 2,
          label: "id=1",
        },
        {
          id: 2,
          note: "auto",
          nullableNote: "present",
          tags: [],
          scores: {},
          pair: ["two", 22],
          doubled: 4,
          label: "id=2",
        },
      ]);
    });
  });

  it("supports JSONEachRow unknown-field skipping when ClickHouse setting is enabled", async function testJsonEachRowSkipUnknownFields() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_json_each_row_unknown");
    const scope = chTable(tempTable, {
      id: chType.int32(),
      label: chType.string(),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);
      await session.insertJsonEachRow(
        scope,
        [
          {
            id: 1,
            label: "kept",
            ignored_extra_field: "ignored",
          },
        ],
        {
          clickhouse_settings: {
            input_format_skip_unknown_fields: 1,
          },
        },
      );

      const rows = await session.select().from(scope);
      expect(rows).toEqual([{ id: 1, label: "kept" }]);
    });
  });

  it("round-trips camelCase keys with configured snake_case column names", async function testConfiguredColumnNameRoundTrip() {
    const db = createE2EDb();
    const tempTable = createTempTableName("tmp_column_name_mapping");
    const scope = chTable(tempTable, {
      userId: chType.string("user_id"),
      rewardPoints: chType.decimal("reward_points", { precision: 20, scale: 5 }),
      eventRank: chType.int32("event_rank"),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      const description = await session.execute(csql`DESCRIBE TABLE ${csql.identifier(tempTable)}`);
      expect(description.map((row) => row.name)).toEqual(["user_id", "reward_points", "event_rank"]);

      await session.insert(scope).values({
        userId: "u_builder",
        rewardPoints: "1.50000",
        eventRank: 1,
      });
      await session.insertJsonEachRow(scope, [
        {
          userId: "u_json",
          rewardPoints: "2.50000",
          eventRank: 2,
        },
      ]);
      await session.insertJsonEachRow(
        scope,
        (async function* rows() {
          yield {
            userId: "u_async",
            rewardPoints: "3.50000",
            eventRank: 3,
          };
        })(),
      );

      const implicitRows = await session.select().from(scope).orderBy(scope.eventRank);
      expect(implicitRows).toEqual([
        {
          userId: "u_builder",
          rewardPoints: "1.5",
          eventRank: 1,
        },
        {
          userId: "u_json",
          rewardPoints: "2.5",
          eventRank: 2,
        },
        {
          userId: "u_async",
          rewardPoints: "3.5",
          eventRank: 3,
        },
      ]);

      const explicitRows = await session
        .select({
          userId: scope.userId,
          rewardPoints: scope.rewardPoints,
        })
        .from(scope)
        .where(ck.eq(scope.eventRank, 2));

      expect(explicitRows).toEqual([
        {
          userId: "u_json",
          rewardPoints: "2.5",
        },
      ]);

      const asyncRows = await session
        .select({
          userId: scope.userId,
          eventRank: scope.eventRank,
        })
        .from(scope)
        .where(ck.eq(scope.eventRank, 3));

      expect(asyncRows).toEqual([
        {
          userId: "u_async",
          eventRank: 3,
        },
      ]);
    });
  });

  it("round-trips 64-bit schema columns through insert builder", async function testInsertBuilderBigIntRoundTrip() {
    const db = createE2EDb();
    await db.insert(writePathBigInts).values({
      id: 1,
      label: "builder_bigints",
      int64_value: "-9223372036854775000",
      uint64_value: "18446744073709551000",
    });

    const rows = await db
      .select({
        id: writePathBigInts.id,
        label: writePathBigInts.label,
        int64Value: writePathBigInts.int64_value,
        uint64Value: writePathBigInts.uint64_value,
      })
      .from(writePathBigInts)
      .orderBy(writePathBigInts.id);

    expect(rows).toEqual([
      {
        id: 1,
        label: "builder_bigints",
        int64Value: "-9223372036854775000",
        uint64Value: "18446744073709551000",
      },
    ]);
  });

  it("round-trips 64-bit schema columns through array insertJsonEachRow", async function testInsertJsonEachRowBigIntArray() {
    const db = createE2EDb();
    await db.insertJsonEachRow(writePathBigInts, [
      {
        id: 11,
        label: "array_bigints_one",
        int64_value: "-9000000000000000001",
        uint64_value: "18446744073709550001",
      },
      {
        id: 12,
        label: "array_bigints_two",
        int64_value: "9000000000000000001",
        uint64_value: "18446744073709550002",
      },
    ]);

    const rows = await db
      .select({
        id: writePathBigInts.id,
        label: writePathBigInts.label,
        int64Value: writePathBigInts.int64_value,
        uint64Value: writePathBigInts.uint64_value,
      })
      .from(writePathBigInts)
      .orderBy(writePathBigInts.id);

    expect(rows).toEqual([
      {
        id: 11,
        label: "array_bigints_one",
        int64Value: "-9000000000000000001",
        uint64Value: "18446744073709550001",
      },
      {
        id: 12,
        label: "array_bigints_two",
        int64Value: "9000000000000000001",
        uint64Value: "18446744073709550002",
      },
    ]);
  });

  it("round-trips 64-bit schema columns through async iterable insertJsonEachRow", async function testInsertJsonEachRowBigIntAsync() {
    const db = createE2EDb();
    await db.insertJsonEachRow(
      writePathBigInts,
      (async function* rows() {
        yield {
          id: 21,
          label: "async_bigints_one",
          int64_value: "-7777777777777777777",
          uint64_value: "17777777777777777777",
        };
        yield {
          id: 22,
          label: "async_bigints_two",
          int64_value: "7777777777777777777",
          uint64_value: "17777777777777777778",
        };
      })(),
    );

    const rows = await db
      .select({
        id: writePathBigInts.id,
        label: writePathBigInts.label,
        int64Value: writePathBigInts.int64_value,
        uint64Value: writePathBigInts.uint64_value,
      })
      .from(writePathBigInts)
      .orderBy(writePathBigInts.id);

    expect(rows).toEqual([
      {
        id: 21,
        label: "async_bigints_one",
        int64Value: "-7777777777777777777",
        uint64Value: "17777777777777777777",
      },
      {
        id: 22,
        label: "async_bigints_two",
        int64Value: "7777777777777777777",
        uint64Value: "17777777777777777778",
      },
    ]);
  });
});

import { beforeEach, expect, it } from "bun:test";
import { csql } from "./ck-orm";
import { auditEvents, createE2EDb, writePathBigInts } from "./shared";
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

import { beforeEach, expect, it } from "bun:test";
import { csql } from "./ck-orm";
import { auditEvents, createE2EDb } from "./shared";
import { describeE2E } from "./test-helpers";

describeE2E("ck-orm e2e write paths", function describeWritePaths() {
  beforeEach(async function truncateAuditEvents() {
    const db = createE2EDb();
    await db.command(csql`TRUNCATE TABLE audit_events`);
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
});

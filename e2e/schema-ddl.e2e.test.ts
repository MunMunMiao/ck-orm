import { expect, it } from "bun:test";
import { ck, ckSql, ckTable, ckType, fn } from "./ck-orm";
import { createE2EDb, createTempTableName } from "./shared";
import { describeE2E, expectRejectsWithClickhouseError } from "./test-helpers";

const showCreate = async (name: string) => {
  const db = createE2EDb();
  const rows = await db.execute(ckSql`SHOW CREATE TABLE ${ckSql.identifier(name)}`);
  return JSON.stringify(rows);
};

const columnNames = async (name: string) => {
  const db = createE2EDb();
  const rows = await db.execute(ckSql`DESCRIBE TABLE ${ckSql.identifier(name)}`);
  return rows.map((row) => row.name);
};

describeE2E("ck-orm e2e schema ddl", function describeSchemaDdl() {
  it("creates, skips and replaces persistent tables", async function testCreateTableModes() {
    const db = createE2EDb();
    const name = createTempTableName("ddl_table");
    const original = ckTable(
      name,
      {
        id: ckType.int32(),
        note: ckType.string(),
      },
      (table) => ({
        engine: "MergeTree",
        orderBy: [table.id],
      }),
    );
    const narrower = ckTable(
      name,
      {
        id: ckType.int32(),
      },
      (table) => ({
        engine: "MergeTree",
        orderBy: [table.id],
      }),
    );
    const replaced = ckTable(
      name,
      {
        id: ckType.int32(),
        label: ckType.string(),
      },
      (table) => ({
        engine: "MergeTree",
        orderBy: [table.id],
      }),
    );

    try {
      await db.createTable(original);
      await expectRejectsWithClickhouseError(db.createTable(original), {
        kind: "request_failed",
        clickhouseName: "TABLE_ALREADY_EXISTS",
      });
      await db.insert(original).values({ id: 1, note: "keep" });
      await db.createTable(narrower, { mode: "if_not_exists" });
      expect(await columnNames(name)).toEqual(["id", "note"]);
      expect(await db.select({ id: original.id, note: original.note }).from(original)).toEqual([
        { id: 1, note: "keep" },
      ]);

      await db.createTable(replaced, { mode: "or_replace" });
      expect(await columnNames(name)).toEqual(["id", "label"]);
      expect(await db.select({ id: replaced.id, label: replaced.label }).from(replaced)).toEqual([]);
    } finally {
      await db.dropTable(name, { ifExists: true, sync: true });
    }
  });

  it("writes renamed columns through a materialized view and stores select settings", async function testCreateMaterializedView() {
    const db = createE2EDb();
    const eventsName = createTempTableName("ddl_events");
    const minutesName = createTempTableName("ddl_minutes");
    const viewName = createTempTableName("ddl_mv");
    const srcName = createTempTableName("ddl_src");
    const dimName = createTempTableName("ddl_dim");
    const targetName = createTempTableName("ddl_target");
    const joinViewName = createTempTableName("ddl_join_mv");
    const events = ckTable(
      eventsName,
      {
        tenantId: ckType.string("tenant_id"),
        occurredAt: ckType.dateTime64("occurred_at", { precision: 3, timezone: "UTC" }),
        units: ckType.uint64(),
      },
      (table) => ({
        engine: "MergeTree",
        orderBy: [table.tenantId, table.occurredAt],
      }),
    );
    const minutes = ckTable(
      minutesName,
      {
        tenantId: ckType.string("tenant_id"),
        minute: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
        usageState: ckType.aggregateFunction("usage_state", {
          name: "sum",
          args: [ckType.uint64()],
        }),
      },
      (table) => ({
        engine: "AggregatingMergeTree",
        orderBy: [table.tenantId, table.minute],
      }),
    );
    const src = ckTable(srcName, { k: ckType.uint32() }, (table) => ({
      engine: "MergeTree",
      orderBy: [table.k],
    }));
    const dim = ckTable(
      dimName,
      {
        k: ckType.uint32(),
        label: ckType.string(),
      },
      (table) => ({
        engine: "MergeTree",
        orderBy: [table.k],
      }),
    );
    const target = ckTable(targetName, {
      k: ckType.nullable(ckType.uint32()),
      label: ckType.nullable(ckType.string()),
    });
    const created = [viewName, joinViewName, eventsName, minutesName, srcName, dimName, targetName];

    try {
      await db.createTable(events);
      await db.createTable(minutes);
      const minute = fn.call<Date>("toStartOfInterval", events.occurredAt, fn.toIntervalMinute(1), "UTC");
      await db.createMaterializedView(viewName, {
        to: minutes,
        as: db
          .select({
            tenantId: events.tenantId,
            minute,
            usageState: fn.call<string>("sumState", events.units),
          })
          .from(events)
          .groupBy(events.tenantId, minute),
      });

      await db.insert(events).values({
        tenantId: "alpha",
        occurredAt: new Date("2026-01-01T00:00:10.000Z"),
        units: "2",
      });
      await db.insert(events).values({
        tenantId: "alpha",
        occurredAt: new Date("2026-01-01T00:00:40.000Z"),
        units: "3",
      });
      expect(
        await db
          .select({
            tenantId: minutes.tenantId,
            usage: fn.toString(fn.call("sumMerge", minutes.usageState)),
          })
          .from(minutes)
          .groupBy(minutes.tenantId),
      ).toEqual([{ tenantId: "alpha", usage: "5" }]);

      const stored = await showCreate(viewName);
      expect(stored).toContain("_CAST(");
      expect(stored).not.toContain("{orm_param");

      const storedBefore = stored;
      await db.createMaterializedView(viewName, {
        to: minutes,
        mode: "if_not_exists",
        as: db
          .select({
            tenantId: events.tenantId,
            minute,
            usageState: fn.call<string>("sumState", events.units),
          })
          .from(events)
          .where(ck.eq(events.tenantId, "should-not-replace"))
          .groupBy(events.tenantId, minute),
      });
      expect(await showCreate(viewName)).toBe(storedBefore);
      await expectRejectsWithClickhouseError(
        db.createMaterializedView(viewName, {
          to: minutes,
          as: db
            .select({
              tenantId: events.tenantId,
              minute,
              usageState: fn.call<string>("sumState", events.units),
            })
            .from(events)
            .groupBy(events.tenantId, minute),
        }),
        {
          kind: "request_failed",
          clickhouseName: "TABLE_ALREADY_EXISTS",
        },
      );

      await db.createTable(src);
      await db.createTable(dim);
      await db.createTable(target);
      await db.createMaterializedView(joinViewName, {
        to: target,
        as: db.select({ k: src.k, label: dim.label }).from(src).leftJoin(dim, ck.eq(src.k, dim.k)),
      });
      expect(await showCreate(joinViewName)).toContain("SETTINGS join_use_nulls = 1");
      await db.insert(src).values({ k: 7 });
      expect(await db.select({ k: target.k, label: target.label }).from(target)).toEqual([{ k: 7, label: null }]);

      await db.dropTable(joinViewName, { sync: true });
      expect(await db.select({ k: target.k, label: target.label }).from(target)).toEqual([{ k: 7, label: null }]);
      await db.dropTable(viewName, { sync: true });
      expect(
        await db
          .select({
            tenantId: minutes.tenantId,
            usage: fn.toString(fn.call("sumMerge", minutes.usageState)),
          })
          .from(minutes)
          .groupBy(minutes.tenantId),
      ).toEqual([{ tenantId: "alpha", usage: "5" }]);
    } finally {
      for (const name of created) {
        await db.dropTable(name, { ifExists: true, sync: true });
      }
    }
  });

  it("drops missing and existing tables", async function testDropTable() {
    const db = createE2EDb();
    const name = createTempTableName("ddl_drop");
    const table = ckTable(name, { id: ckType.int32() }, (item) => ({
      engine: "MergeTree",
      orderBy: [item.id],
    }));

    await expectRejectsWithClickhouseError(db.dropTable(name), {
      kind: "request_failed",
      clickhouseName: "UNKNOWN_TABLE",
    });
    await db.dropTable(name, { ifExists: true });

    try {
      await db.createTable(table);
      await db.insert(table).values({ id: 1 });
      await db.dropTable(table, { sync: true });
      await expectRejectsWithClickhouseError(db.select({ id: table.id }).from(table).execute(), {
        kind: "request_failed",
        clickhouseName: "UNKNOWN_TABLE",
      });
    } finally {
      await db.dropTable(name, { ifExists: true, sync: true });
    }
  });
});

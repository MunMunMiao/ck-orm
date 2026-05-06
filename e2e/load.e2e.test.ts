import { expect, it } from "bun:test";
import { ckSql, ckTable, ckType } from "./ck-orm";
import { createE2EDb, createTempTableName } from "./shared";
import { describeE2E } from "./test-helpers";

// Heavy load tests are gated behind CK_ORM_E2E_LOAD=1 because they take
// 30+ seconds each and aren't appropriate for the default e2e job.
const loadDescribe = process.env.CK_ORM_E2E_LOAD === "1" ? describeE2E : describeE2E.skip;

const ROW_COUNT = Number(process.env.CK_ORM_E2E_LOAD_ROWS ?? 1_000_000);

const memoryDeltaMb = (start: NodeJS.MemoryUsage) => {
  const now = process.memoryUsage();
  return (now.heapUsed - start.heapUsed) / (1024 * 1024);
};

loadDescribe("ck-orm e2e load", function describeLoad() {
  it(`inserts ${ROW_COUNT.toLocaleString()} rows in batched insertJsonEachRow without unbounded memory`, async function testLargeAsyncInsert() {
    const db = createE2EDb();
    const tableName = createTempTableName("load_async");
    const table = ckTable(tableName, {
      id: ckType.uint32(),
      label: ckType.string(),
      created_at: ckType.dateTime64({ precision: 3 }),
    });

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (
          id UInt32,
          label String,
          created_at DateTime64(3)
        ) engine = MergeTree order by id
      `);

      const start = process.memoryUsage();
      const baseDate = new Date("2025-01-01T00:00:00Z");

      // Insert in batches of 50k rows to keep request bodies bounded; the
      // schema-aware encoder still runs per row, exercising the path.
      const BATCH = 50_000;
      for (let offset = 0; offset < ROW_COUNT; offset += BATCH) {
        const limit = Math.min(BATCH, ROW_COUNT - offset);
        const batch = Array.from({ length: limit }, (_, k) => {
          const i = offset + k + 1;
          return {
            id: i,
            label: `row-${i.toString(36)}`,
            created_at: new Date(baseDate.getTime() + i * 1000),
          };
        });
        await db.insertJsonEachRow(table, batch);
      }

      const deltaMb = memoryDeltaMb(start);
      // Streaming insert should not buffer the whole dataset. Allow generous
      // headroom for V8 GC noise; a buffered implementation would balloon to
      // 100MB+ for 1M small rows.
      expect(deltaMb).toBeLessThan(200);

      const [{ count }] = (await db.execute(
        ckSql`select count() as count from ${ckSql.identifier(tableName)}`,
      )) as Array<{ count: string }>;
      expect(Number(count)).toBe(ROW_COUNT);
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it(`streams ${ROW_COUNT.toLocaleString()} rows back via stream() without unbounded memory`, async function testLargeStream() {
    const db = createE2EDb();
    const tableName = createTempTableName("load_stream");

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (id UInt32) engine = MergeTree order by id
      `);
      await db.command(ckSql`insert into ${ckSql.identifier(tableName)} select number from numbers(${ROW_COUNT})`);

      const start = process.memoryUsage();
      let received = 0;
      let lastId = -1;

      for await (const row of db.stream<{ id: number }>(
        ckSql`select id from ${ckSql.identifier(tableName)} order by id`,
      )) {
        if (row.id <= lastId) {
          throw new Error(`Stream emitted out-of-order row: prev=${lastId} cur=${row.id}`);
        }
        lastId = row.id;
        received += 1;
      }

      expect(received).toBe(ROW_COUNT);
      expect(lastId).toBe(ROW_COUNT - 1);
      expect(memoryDeltaMb(start)).toBeLessThan(200);
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });

  it("aborts an in-flight stream cleanly and frees the underlying connection", async function testStreamAbort() {
    const db = createE2EDb();
    const tableName = createTempTableName("load_abort");

    try {
      await db.command(ckSql`
        create table ${ckSql.identifier(tableName)} (id UInt32) engine = MergeTree order by id
      `);
      await db.command(ckSql`insert into ${ckSql.identifier(tableName)} select number from numbers(${ROW_COUNT})`);

      const controller = new AbortController();
      let received = 0;
      let aborted: unknown;
      try {
        for await (const _row of db.stream<{ id: number }>(
          ckSql`select id from ${ckSql.identifier(tableName)} order by id`,
          { abort_signal: controller.signal },
        )) {
          received += 1;
          if (received >= 1000) {
            controller.abort();
            // The async generator may have buffered subsequent chunks already;
            // break explicitly so the loop releases the iterator promptly
            // rather than draining the buffer.
            break;
          }
        }
      } catch (error) {
        aborted = error;
      }

      // We aborted partway through. Allow some buffered overflow but the
      // total received must be far less than the full dataset.
      expect(received).toBeGreaterThanOrEqual(1000);
      expect(received).toBeLessThan(ROW_COUNT);
      // After abort, a follow-up query should still succeed — confirms no
      // dangling connection / session lock.
      const [{ ok }] = (await db.execute(ckSql`select 1 as ok`)) as Array<{ ok: number }>;
      expect(ok).toBe(1);
      // If an error was thrown during iteration, it should be marked aborted
      // so callers can distinguish from server-side failures.
      if (aborted !== undefined) {
        expect((aborted as { kind?: string }).kind).toBe("aborted");
      }
    } finally {
      await db.command(ckSql`drop table if exists ${ckSql.identifier(tableName)}`);
    }
  });
});

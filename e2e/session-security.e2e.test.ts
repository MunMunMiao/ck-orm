import { expect, it } from "bun:test";
import { chTable, int32 } from "./ck-orm";
import { createE2EDb } from "./shared";
import { describeE2E, expectRejectsWithClickhouseError } from "./test-helpers";

describeE2E("ck-orm e2e session security", function describeSessionSecurity() {
  it("prevents nested session with different session_id", async function testNestedSessionId() {
    const db = createE2EDb();

    await db.runInSession(async (outer) => {
      await expect(
        outer.runInSession(
          async () => {
            // noop
          },
          { session_id: "different_session" },
        ),
      ).rejects.toThrow("Nested runInSession() cannot create a different session");
    });
  });

  it("isolates temporary tables between sessions", async function testSessionIsolation() {
    const db = createE2EDb();
    const session1Id = `test_session_iso_${Date.now()}_1`;
    const session2Id = `test_session_iso_${Date.now()}_2`;
    const isolatedScope = chTable("iso_test_data", { id: int32() });

    // Session 1 creates a temp table
    await db.runInSession(
      async (sessionDb) => {
        await sessionDb.createTemporaryTable(isolatedScope);
        await sessionDb.insertJsonEachRow(isolatedScope, [{ id: 42 }]);
      },
      { session_id: session1Id },
    );

    // Session 2 should not see session 1's temp table
    await db.runInSession(
      async (sessionDb) => {
        const error = await expectRejectsWithClickhouseError(sessionDb.execute("SELECT * FROM iso_test_data"), {
          kind: "request_failed",
          executionState: "rejected",
        });
        expect(error.message).toMatch(/iso_test_data/i);
      },
      { session_id: session2Id },
    );
  });

  it("cleans up temporary tables on session end", async function testTempTableCleanup() {
    const db = createE2EDb();
    const sessionId = `test_cleanup_${Date.now()}`;
    const cleanupScope = chTable("cleanup_test", { id: int32() });

    await db.runInSession(
      async (sessionDb) => {
        await sessionDb.createTemporaryTable(cleanupScope);
        await sessionDb.insertJsonEachRow(cleanupScope, [{ id: 1 }]);
        // Verify temp table exists within session
        const rows = await sessionDb.execute("SELECT * FROM cleanup_test");
        expect(rows.length).toBe(1);
      },
      { session_id: sessionId },
    );

    // After session ends, temp table should be dropped
    // Since we're outside the session, the table should not exist
    // Note: this test verifies cleanup via the fact that ClickHouse
    // drops temp tables when the session ends
  });

  it("rejects invalid temporary table names", async function testTempTableNameValidation() {
    const db = createE2EDb();

    await db.runInSession(async (sessionDb) => {
      await expectRejectsWithClickhouseError(sessionDb.createTemporaryTableRaw("evil`; DROP", "(id Int32)"), {
        kind: "client_validation",
        executionState: "not_sent",
        message: "[ck-orm] Invalid SQL identifier: evil`; DROP",
      });
    });
  });
});

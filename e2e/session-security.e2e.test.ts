import { expect, it } from "bun:test";
import { ckSql, ckTable, ckType } from "./ck-orm";
import { createE2EDb, createTempTableName } from "./shared";
import { describeE2E, expectRejectsWithClickhouseError } from "./test-helpers";

describeE2E("ck-orm e2e session security", function describeSessionSecurity() {
  it("creates a distinct child session and rejects ancestor session_id reuse", async function testNestedSessionId() {
    const db = createE2EDb();
    let childSessionId = "";

    await db.runInSession(async (outer) => {
      await outer.runInSession(async (child) => {
        childSessionId = child.sessionId;
        expect(child.sessionId).not.toBe(outer.sessionId);
      });

      await expect(
        outer.runInSession(
          async () => {
            // noop
          },
          { session_id: outer.sessionId },
        ),
      ).rejects.toThrow("Nested runInSession() cannot reuse an existing session_id");
    });

    expect(childSessionId).not.toBe("");
  });

  it("rejects nested session_check=1", async function testNestedSessionCheck() {
    const db = createE2EDb();

    await db.runInSession(async (outer) => {
      await expectRejectsWithClickhouseError(
        outer.runInSession(
          async () => {
            // noop
          },
          { session_id: "child_session_e2e", session_check: 1 },
        ),
        {
          kind: "client_validation",
          executionState: "not_sent",
          message:
            "[ck-orm] Nested runInSession() cannot use session_check=1 because child sessions are created by ck-orm",
        },
      );
    });
  });

  it("supports three-level nested independent sessions", async function testThreeLevelNestedSessions() {
    const db = createE2EDb();
    const sessionIds: string[] = [];

    await db.runInSession(async (outer) => {
      sessionIds.push(outer.sessionId);
      expect(Number((await outer.execute(ckSql`SELECT 1 AS value`))[0]?.value)).toBe(1);

      await outer.runInSession(async (child) => {
        sessionIds.push(child.sessionId);
        expect(child.sessionId).not.toBe(outer.sessionId);
        expect(Number((await child.execute(ckSql`SELECT 1 AS value`))[0]?.value)).toBe(1);

        await child.runInSession(async (grandchild) => {
          sessionIds.push(grandchild.sessionId);
          expect(grandchild.sessionId).not.toBe(child.sessionId);
          expect(grandchild.sessionId).not.toBe(outer.sessionId);
          expect(Number((await grandchild.execute(ckSql`SELECT 1 AS value`))[0]?.value)).toBe(1);

          await expectRejectsWithClickhouseError(
            grandchild.runInSession(
              async () => {
                // noop
              },
              { session_id: outer.sessionId },
            ),
            {
              kind: "session",
              executionState: "not_sent",
              message: "[ck-orm] Nested runInSession() cannot reuse an existing session_id",
            },
          );
        });
      });
    });

    expect(new Set(sessionIds).size).toBe(3);
  });

  it("isolates temporary tables between sessions", async function testSessionIsolation() {
    const db = createE2EDb();
    const session1Id = `test_session_iso_${Date.now()}_1`;
    const session2Id = `test_session_iso_${Date.now()}_2`;
    const isolatedTable = createTempTableName("iso_test_data");
    const isolatedScope = ckTable(isolatedTable, { id: ckType.int32() });

    // Session 1 creates a temp table
    await db.runInSession(
      async (session) => {
        await session.createTemporaryTable(isolatedScope);
        await session.insertJsonEachRow(isolatedScope, [{ id: 42 }]);
      },
      { session_id: session1Id },
    );

    // Session 2 should not see session 1's temp table
    await db.runInSession(
      async (session) => {
        const error = await expectRejectsWithClickhouseError(
          session.execute(ckSql`select * from ${ckSql.identifier(isolatedTable)}`),
          {
            kind: "request_failed",
            executionState: "rejected",
          },
        );
        expect(error.message).toMatch(new RegExp(isolatedTable, "i"));
      },
      { session_id: session2Id },
    );
  });

  it("keeps sibling child sessions isolated from each other and from outer", async function testSiblingIsolation() {
    const db = createE2EDb();
    const outerTable = createTempTableName("outer_scope");
    const childOneTable = createTempTableName("child_one_scope");
    const childTwoTable = createTempTableName("child_two_scope");
    const outerScope = ckTable(outerTable, { id: ckType.int32() });
    const childOneScope = ckTable(childOneTable, { id: ckType.int32() });
    const childTwoScope = ckTable(childTwoTable, { id: ckType.int32() });

    await db.runInSession(
      async (session) => {
        await session.createTemporaryTable(outerScope);
        await session.insertJsonEachRow(outerScope, [{ id: 1 }]);
        expect(
          Number(
            (await session.execute(ckSql`select count() as total from ${ckSql.identifier(outerTable)}`))[0]?.total,
          ),
        ).toBe(1);

        await session.runInSession(async (childOne) => {
          const missingOuter = await expectRejectsWithClickhouseError(
            childOne.execute(ckSql`select count() as total from ${ckSql.identifier(outerTable)}`),
            {
              kind: "request_failed",
              executionState: "rejected",
            },
          );
          expect(missingOuter.message).toMatch(new RegExp(outerTable, "i"));

          await childOne.createTemporaryTable(childOneScope);
          await childOne.insertJsonEachRow(childOneScope, [{ id: 11 }]);
          expect(
            Number(
              (await childOne.execute(ckSql`select count() as total from ${ckSql.identifier(childOneTable)}`))[0]
                ?.total,
            ),
          ).toBe(1);
        });

        const missingChildOneFromOuter = await expectRejectsWithClickhouseError(
          session.execute(ckSql`select count() as total from ${ckSql.identifier(childOneTable)}`),
          {
            kind: "request_failed",
            executionState: "rejected",
          },
        );
        expect(missingChildOneFromOuter.message).toMatch(new RegExp(childOneTable, "i"));
        expect(
          Number(
            (await session.execute(ckSql`select count() as total from ${ckSql.identifier(outerTable)}`))[0]?.total,
          ),
        ).toBe(1);

        await session.runInSession(async (childTwo) => {
          const missingOuter = await expectRejectsWithClickhouseError(
            childTwo.execute(ckSql`select count() as total from ${ckSql.identifier(outerTable)}`),
            {
              kind: "request_failed",
              executionState: "rejected",
            },
          );
          expect(missingOuter.message).toMatch(new RegExp(outerTable, "i"));

          const missingChildOne = await expectRejectsWithClickhouseError(
            childTwo.execute(ckSql`select count() as total from ${ckSql.identifier(childOneTable)}`),
            {
              kind: "request_failed",
              executionState: "rejected",
            },
          );
          expect(missingChildOne.message).toMatch(new RegExp(childOneTable, "i"));

          await childTwo.createTemporaryTable(childTwoScope);
          await childTwo.insertJsonEachRow(childTwoScope, [{ id: 21 }]);
          expect(
            Number(
              (await childTwo.execute(ckSql`select count() as total from ${ckSql.identifier(childTwoTable)}`))[0]
                ?.total,
            ),
          ).toBe(1);
        });

        expect(
          Number(
            (await session.execute(ckSql`select count() as total from ${ckSql.identifier(outerTable)}`))[0]?.total,
          ),
        ).toBe(1);
      },
      { session_id: `test_sibling_${Date.now()}` },
    );
  });

  it("cleans up temporary tables on session end", async function testTempTableCleanup() {
    const db = createE2EDb();
    const sessionId = `test_cleanup_${Date.now()}`;
    const cleanupTable = createTempTableName("cleanup_test");
    const cleanupScope = ckTable(cleanupTable, { id: ckType.int32() });

    await db.runInSession(
      async (session) => {
        await session.createTemporaryTable(cleanupScope);
        await session.insertJsonEachRow(cleanupScope, [{ id: 1 }]);
        expect(
          Number(
            (await session.execute(ckSql`select count() as total from ${ckSql.identifier(cleanupTable)}`))[0]?.total,
          ),
        ).toBe(1);
      },
      { session_id: sessionId },
    );

    const error = await expectRejectsWithClickhouseError(
      db.execute(ckSql`select count() as total from ${ckSql.identifier(cleanupTable)}`, {
        session_id: sessionId,
      }),
      {
        kind: "request_failed",
        executionState: "rejected",
      },
    );
    expect(error.message).toMatch(new RegExp(cleanupTable, "i"));
  });

  it("inherits settings in nested child sessions without reusing the parent identity", async function testNestedWithSettings() {
    const db = createE2EDb();

    await db.runInSession(async (session) => {
      const [outerBefore] = await session.execute(ckSql`select getSetting('max_threads') as max_threads`);

      const childResult = await session.withSettings({ max_threads: 2 }).runInSession(async (child) => {
        const [childSetting] = await child.execute(ckSql`select getSetting('max_threads') as max_threads`);
        return {
          sessionId: child.sessionId,
          maxThreads: Number(childSetting?.max_threads),
        };
      });

      const [outerAfter] = await session.execute(ckSql`select getSetting('max_threads') as max_threads`);
      expect(childResult.sessionId).not.toBe(session.sessionId);
      expect(childResult.maxThreads).toBe(2);
      expect(Number(outerAfter?.max_threads)).toBe(Number(outerBefore?.max_threads));
    });
  });

  it("rejects invalid temporary table names", async function testTempTableNameValidation() {
    const db = createE2EDb();

    await db.runInSession(async (session) => {
      await expectRejectsWithClickhouseError(session.createTemporaryTableRaw("evil`; DROP", "(id Int32)"), {
        kind: "client_validation",
        executionState: "not_sent",
        message: "[ck-orm] Invalid SQL identifier: evil`; DROP",
      });
    });
  });
});

import { expect, it } from "bun:test";
import { ck, ckTable, ckType, csql } from "./ck-orm";
import { auditEvents, createE2EDb, createTempTableName } from "./shared";
import { describeE2E, expectRejectsWithClickhouseError, waitForQueryLogException } from "./test-helpers";

describeE2E("ck-orm e2e error contracts", function describeErrorContracts() {
  it("classifies SQL syntax errors as rejected requests and records them in query_log", async function testSyntaxErrors() {
    const db = createE2EDb();
    const queryId = `e2e_syntax_error_${Date.now()}`;

    await expectRejectsWithClickhouseError(db.execute(csql`SELCT 1`, { query_id: queryId }), {
      kind: "request_failed",
      executionState: "rejected",
      httpStatus: 400,
      clickhouseName: "SYNTAX_ERROR",
      queryId,
    });

    const row = await waitForQueryLogException(queryId);
    expect(row.type).toBe("ExceptionBeforeStart");
    expect(row.query_id).toBe(queryId);
    expect(Number(row.http_method)).toBeGreaterThan(0);
    expect(row.exception).toContain("SYNTAX_ERROR");
  });

  it("classifies missing tables as rejected requests and records them in query_log", async function testUnknownTableErrors() {
    const db = createE2EDb();
    const queryId = `e2e_unknown_table_${Date.now()}`;

    await expectRejectsWithClickhouseError(
      db.execute(csql`select count() as total from ${csql.identifier("missing_error_contract_table")}`, {
        query_id: queryId,
      }),
      {
        kind: "request_failed",
        executionState: "rejected",
        httpStatus: 404,
        clickhouseName: "UNKNOWN_TABLE",
        queryId,
      },
    );

    const row = await waitForQueryLogException(queryId);
    expect(row.type).toBe("ExceptionBeforeStart");
    expect(row.query_id).toBe(queryId);
    expect(row.exception).toContain("UNKNOWN_TABLE");
  });

  it("classifies session-expired temporary tables as rejected requests and records them in query_log", async function testSessionExpiredTempTables() {
    const db = createE2EDb();
    const sessionId = ck.createSessionId();
    const tempTable = createTempTableName("expired_scope");
    const tempScope = ckTable(tempTable, { id: ckType.int32() });

    await db.runInSession(
      async (session) => {
        await session.createTemporaryTable(tempScope);
        await session.insertJsonEachRow(tempScope, [{ id: 1 }]);
      },
      { session_id: sessionId },
    );

    const queryId = `e2e_expired_temp_${Date.now()}`;
    await expectRejectsWithClickhouseError(
      db.execute(csql`select count() as total from ${csql.identifier(tempTable)}`, {
        query_id: queryId,
        session_id: sessionId,
      }),
      {
        kind: "request_failed",
        executionState: "rejected",
        httpStatus: 404,
        clickhouseName: "UNKNOWN_TABLE",
        queryId,
        sessionId,
      },
    );

    const row = await waitForQueryLogException(queryId);
    expect(row.type).toBe("ExceptionBeforeStart");
    expect(row.query_id).toBe(queryId);
    expect(row.exception).toContain("UNKNOWN_TABLE");
  });

  it("classifies type-mismatched JSONEachRow inserts as rejected requests and records them in query_log", async function testInsertTypeMismatchErrors() {
    const db = createE2EDb();
    const queryId = `e2e_insert_type_mismatch_${Date.now()}`;

    await expectRejectsWithClickhouseError(
      db.insertJsonEachRow(
        auditEvents,
        [
          {
            id: "bad-id",
            user_id: 1,
            event_name: "broken_insert",
            created_at: "not-a-datetime",
          } as unknown as Record<string, unknown>,
        ],
        { query_id: queryId },
      ),
      {
        kind: "request_failed",
        executionState: "rejected",
        httpStatus: 400,
        queryId,
      },
    );

    const row = await waitForQueryLogException(queryId);
    expect(row.type).toBe("ExceptionWhileProcessing");
    expect(row.query_id).toBe(queryId);
    expect(row.exception_code).toBeGreaterThan(0);
  });

  it("classifies query parameter type mismatches as rejected requests and records them in query_log", async function testQueryParamTypeMismatchErrors() {
    const db = createE2EDb();
    const queryId = `e2e_query_param_type_mismatch_${Date.now()}`;

    await expectRejectsWithClickhouseError(
      db.execute(csql`select {bad_id:Int32} as bad_id`, {
        query_id: queryId,
        query_params: {
          bad_id: "not-an-int",
        },
      }),
      {
        kind: "request_failed",
        executionState: "rejected",
        queryId,
      },
    );

    const row = await waitForQueryLogException(queryId);
    expect(row.query_id).toBe(queryId);
    expect(row.exception_code).toBeGreaterThan(0);
  });

  it("classifies partial JSONEachRow insert failures without breaking the session", async function testPartialJsonEachRowInsertFailure() {
    const db = createE2EDb();
    const queryId = `e2e_partial_json_each_row_${Date.now()}`;
    const tempTable = createTempTableName("tmp_partial_json_each_row");
    const scope = ckTable(tempTable, {
      id: ckType.int32(),
      label: ckType.string(),
      created_at: ckType.dateTime64({ precision: 3 }),
    });

    await db.runInSession(async (session) => {
      await session.createTemporaryTable(scope);

      await expectRejectsWithClickhouseError(
        session.insertJsonEachRow(
          scope,
          [
            {
              id: 1,
              label: "valid-before-failure",
              created_at: "2026-04-24 01:02:03.456",
            },
            {
              id: "bad-id",
              label: "invalid-row",
              created_at: "not-a-date",
            } as unknown as Record<string, unknown>,
          ],
          {
            query_id: queryId,
          },
        ),
        {
          kind: "request_failed",
          executionState: "rejected",
          queryId,
          sessionId: session.sessionId,
        },
      );

      expect(await session.execute(csql`select 1 as still_usable`)).toEqual([{ still_usable: 1 }]);
    });

    const row = await waitForQueryLogException(queryId);
    expect(row.type).toBe("ExceptionWhileProcessing");
    expect(row.query_id).toBe(queryId);
    expect(row.exception_code).toBeGreaterThan(0);
  });
});

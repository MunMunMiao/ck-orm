import { expect, it } from "bun:test";
import { ckSql, ckTable, ckType } from "./ck-orm";
import { createE2EDb, createTempTableName, users } from "./shared";
import { describeE2E, expectClientValidationNotSent, expectNoMutationAfterRejectedInjection } from "./test-helpers";

describeE2E(
  "ck-orm e2e injection transport and trusted boundaries",
  function describeInjectionTransportAndBoundaries() {
    it("rejects query_params that try to collide with orm-managed parameter names", async function testReservedQueryParamPrefix() {
      const db = createE2EDb();

      await expectClientValidationNotSent(
        db.execute(ckSql`SELECT * FROM ${users} WHERE id = ${1}`, {
          query_params: {
            orm_param1: 999,
          },
        }),
        {
          message:
            '[ck-orm] query_params key "orm_param1" uses reserved internal prefix "orm_param". This prefix is reserved for ckSql`...` generated parameters.',
        },
      );
    });

    it("rejects invalid query parameter keys, query_ids, and session_ids before a request is sent", async function testTransportValidation() {
      const db = createE2EDb();

      const cases: Array<{
        readonly promise: Promise<unknown>;
        readonly message: string;
      }> = [
        {
          promise: db.execute(ckSql`select {safe_key:String} as value`, {
            query_params: {
              "bad-key": "unsafe",
            },
          }),
          message: '[ck-orm] Invalid query parameter key: "bad-key". Keys must match ^[a-zA-Z_][a-zA-Z0-9_]{0,99}$',
        },
        {
          promise: db.execute(ckSql`select 1`, {
            query_id: "bad query id",
          }),
          message:
            '[ck-orm] Invalid query_id: "bad query id". Must be 1-100 chars of alphanumerics, underscores, or hyphens.',
        },
        {
          promise: db.execute(ckSql`select 1`, {
            session_id: "bad session id",
          }),
          message:
            '[ck-orm] Invalid session_id: "bad session id". Must be 1-100 chars of alphanumerics, underscores, or hyphens.',
        },
      ];

      for (const testCase of cases) {
        await expectClientValidationNotSent(testCase.promise, {
          message: testCase.message,
        });
      }
    });

    it("rejects multi-statement createTemporaryTableRaw() definitions and leaves base tables untouched", async function testCreateTemporaryTableDefinitionValidation() {
      const db = createE2EDb();

      await db.runInSession(async (session) => {
        await expectClientValidationNotSent(
          session.createTemporaryTableRaw("tmp_evil", "(id Int32); DROP TABLE users"),
          {
            message:
              "[ck-orm] createTemporaryTableRaw() definition must not contain multiple statements; use developer-controlled SQL only",
          },
        );
      });

      await expectNoMutationAfterRejectedInjection();
    });

    it("allows semicolons inside createTemporaryTableRaw() string literals when the definition stays single-statement", async function testCreateTemporaryTableLiteralSemicolon() {
      const db = createE2EDb();
      const tempTable = createTempTableName("tmp_literal_semicolon");

      await db.runInSession(async (session) => {
        await session.createTemporaryTableRaw(tempTable, "(id Int32, note String DEFAULT ';')");
        await session.command(ckSql`INSERT INTO ${ckSql.identifier(tempTable)} (id) VALUES (${1})`);

        const rows = await session.execute(ckSql`
        select note
        from ${ckSql.identifier(tempTable)}
        where id = ${1}
      `);

        expect(rows).toEqual([{ note: ";" }]);
      });
    });

    it("supports session_timeout on new session blocks and session_check on continued sessions", async function testSessionLifetimeOptions() {
      const db = createE2EDb();
      const sessionId = createTempTableName("session_opts");
      const tempTable = createTempTableName("tmp_session_opts");

      try {
        await db.command(ckSql`CREATE TEMPORARY TABLE ${ckSql.identifier(tempTable)} (id Int32)`, {
          session_id: sessionId,
          session_timeout: 30,
        });
        await db.command(ckSql`INSERT INTO ${ckSql.identifier(tempTable)} (id) VALUES (${1})`, {
          session_id: sessionId,
          session_timeout: 30,
          session_check: 1,
        });

        const rows = await db.execute(ckSql`SELECT id FROM ${ckSql.identifier(tempTable)} ORDER BY id`, {
          session_id: sessionId,
          session_timeout: 30,
          session_check: 1,
        });
        expect(rows).toEqual([{ id: 1 }]);

        const sessionRows = await db.runInSession(
          async (session) => {
            const sessionTempTable = createTempTableName("tmp_run_in_session_opts");
            const sessionTempScope = ckTable(sessionTempTable, { id: ckType.int32() });
            await session.createTemporaryTable(sessionTempScope);
            await session.command(ckSql`INSERT INTO ${ckSql.identifier(sessionTempTable)} (id) VALUES (${2})`);
            return await session.execute(ckSql`SELECT id FROM ${ckSql.identifier(sessionTempTable)} ORDER BY id`);
          },
          {
            session_timeout: 30,
          },
        );
        expect(sessionRows).toEqual([{ id: 2 }]);
      } finally {
        await db.command(ckSql`DROP TABLE IF EXISTS ${ckSql.identifier(tempTable)}`, {
          session_id: sessionId,
          session_timeout: 30,
          session_check: 1,
          ignore_error_response: true,
        });
      }
    });

    it("rejects dangerous ckSql.join() separator patterns before a request is sent", async function testJoinSeparatorValidation() {
      const db = createE2EDb();
      const evilSeparator = "`) UNION ALL SELECT password FROM users; -- ";

      expect(() =>
        db
          .select()
          .from(users)
          .orderBy(ckSql.join([ckSql.identifier("a")], evilSeparator)),
      ).toThrow("Invalid SQL join separator");

      expect(() =>
        db
          .select()
          .from(users)
          .orderBy(ckSql.join([ckSql.identifier("a")], "a;b")),
      ).toThrow("Invalid SQL join separator");
    });

    it("supports structured temporary-table schema with DEFAULT, MATERIALIZED and ALIAS expressions", async function testStructuredTempTableSchema() {
      const db = createE2EDb();
      const tempTable = createTempTableName("tmp_structured_schema");
      const structuredScope = ckTable(tempTable, {
        base: ckType.int32(),
        note: ckType.string().default(ckSql`'auto'`),
        doubled: ckType.int32().materialized(ckSql`base * 2`),
        label: ckType.string().aliasExpr(ckSql`concat('n=', toString(base))`),
      });

      await db.runInSession(async (session) => {
        await session.createTemporaryTable(structuredScope);
        await session.insertJsonEachRow(structuredScope, [{ base: 7 }]);

        const rows = await session
          .select({
            base: structuredScope.base,
            note: structuredScope.note,
            doubled: structuredScope.doubled,
            label: structuredScope.label,
          })
          .from(structuredScope)
          .orderBy(structuredScope.base);

        expect(rows).toEqual([{ base: 7, note: "auto", doubled: 14, label: "n=7" }]);
      });
    });
  },
);

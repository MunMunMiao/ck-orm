import { it } from "bun:test";
import { ckAlias, ckSql, fn, type SQLFragment } from "./ck-orm";
import { createE2EDb, users } from "./shared";
import { describeE2E, expectClientValidationNotSent, expectNoMutationAfterRejectedInjection } from "./test-helpers";

describeE2E("ck-orm e2e injection identifiers", function describeInjectionIdentifiers() {
  it("rejects malicious string and object identifiers before a request is sent", async function testSqlIdentifierValidation() {
    const db = createE2EDb();
    const evilAlias = ckAlias(users, "owner`; DROP");

    const cases: Array<{
      readonly run: () => Promise<unknown> | unknown;
      readonly message: string;
    }> = [
      {
        run: () => db.execute(ckSql`select * from ${ckSql.identifier("users`; DROP")}`),
        message: "[ck-orm] Invalid SQL identifier: users`; DROP",
      },
      {
        run: () =>
          db.execute(
            ckSql`select ${ckSql.identifier({ table: "users", column: "id; DROP TABLE users" })} from ${users} limit 1`,
          ),
        message: "[ck-orm] Invalid SQL identifier: id; DROP TABLE users",
      },
      {
        run: () => db.execute(ckSql`select 1 as ${ckSql.identifier({ as: "user_id--comment" })}`),
        message: "[ck-orm] Invalid SQL identifier: user_id--comment",
      },
      {
        run: () =>
          db
            .select({
              id: evilAlias.id,
            })
            .from(evilAlias)
            .limit(1)
            .execute(),
        message: "[ck-orm] Invalid SQL identifier: owner`; DROP",
      },
    ];

    for (const testCase of cases) {
      await expectClientValidationNotSent(testCase.run, {
        message: testCase.message,
      });
    }
  });

  it("rejects malicious temporary table names before a request is sent", async function testTempTableNameValidation() {
    const db = createE2EDb();

    await db.runInSession(async (session) => {
      await expectClientValidationNotSent(session.createTemporaryTableRaw("evil`; DROP", "(id Int32)"), {
        message: "[ck-orm] Invalid SQL identifier: evil`; DROP",
      });
    });

    await expectNoMutationAfterRejectedInjection();
  });

  it("rejects malicious function names in fn.withParams() and fn.table.call()", async function testFunctionNameValidation() {
    const db = createE2EDb();
    const cases: Array<{
      readonly run: () => Promise<unknown> | unknown;
      readonly message: string;
    }> = [
      {
        run: () =>
          db.execute(ckSql`select ${fn.withParams("; DROP TABLE users; --", [0.95], users.id)} from ${users} limit 1`),
        message: "[ck-orm] Invalid function name: ; DROP TABLE users; --",
      },
      {
        run: () =>
          db.execute(
            ckSql`select * from ${fn.table.call("; DROP TABLE users; --", 5).as("evil_source") as SQLFragment<unknown>}`,
          ),
        message: "[ck-orm] Invalid function name: ; DROP TABLE users; --",
      },
    ];

    for (const testCase of cases) {
      await expectClientValidationNotSent(testCase.run, {
        message: testCase.message,
      });
    }

    await expectNoMutationAfterRejectedInjection();
  });
});

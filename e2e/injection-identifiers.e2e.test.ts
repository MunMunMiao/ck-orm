import { it } from "bun:test";
import { alias, fn, type SQLFragment, sql, tableFn } from "./ck-orm";
import { createE2EDb, users } from "./shared";
import { describeE2E, expectClientValidationNotSent, expectNoMutationAfterRejectedInjection } from "./test-helpers";

describeE2E("ck-orm e2e injection identifiers", function describeInjectionIdentifiers() {
  it("rejects malicious string and object identifiers before a request is sent", async function testSqlIdentifierValidation() {
    const db = createE2EDb();
    const evilAlias = alias(users, "owner`; DROP");

    const cases: Array<{
      readonly run: () => Promise<unknown> | unknown;
      readonly message: string;
    }> = [
      {
        run: () => db.execute(sql`select * from ${sql.identifier("users`; DROP")}`),
        message: "[ck-orm] Invalid SQL identifier: users`; DROP",
      },
      {
        run: () =>
          db.execute(
            sql`select ${sql.identifier({ table: "users", column: "id; DROP TABLE users" })} from ${users} limit 1`,
          ),
        message: "[ck-orm] Invalid SQL identifier: id; DROP TABLE users",
      },
      {
        run: () => db.execute(sql`select 1 as ${sql.identifier({ as: "user_id--comment" })}`),
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

    await db.runInSession(async (sessionDb) => {
      await expectClientValidationNotSent(sessionDb.createTemporaryTable("evil`; DROP", "(id Int32)"), {
        message: "[ck-orm] Invalid SQL identifier: evil`; DROP",
      });
    });

    await expectNoMutationAfterRejectedInjection();
  });

  it("rejects malicious function names in fn.withParams() and tableFn.call()", async function testFunctionNameValidation() {
    const db = createE2EDb();
    const cases: Array<{
      readonly run: () => Promise<unknown> | unknown;
      readonly message: string;
    }> = [
      {
        run: () =>
          db.execute(sql`select ${fn.withParams("; DROP TABLE users; --", [0.95], users.id)} from ${users} limit 1`),
        message: "[ck-orm] Invalid function name: ; DROP TABLE users; --",
      },
      {
        run: () =>
          db.execute(
            sql`select * from ${tableFn.call("; DROP TABLE users; --", 5).as("evil_source") as SQLFragment<unknown>}`,
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

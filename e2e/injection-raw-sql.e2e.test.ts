import { expect, it } from "bun:test";
import { sql } from "./ck-orm";
import { createE2EDb, users } from "./shared";
import { describeE2E, expectClientValidationNotSent, expectNoMutationAfterRejectedInjection } from "./test-helpers";

describeE2E("ck-orm e2e injection raw sql", function describeInjectionRawSql() {
  it("rejects stacked raw SQL in execute() and command() and leaves base tables untouched", async function testStackedRawSql() {
    const db = createE2EDb();
    const stackedSql = "SELECT 1; DROP TABLE IF EXISTS users";

    await expectClientValidationNotSent(db.execute(stackedSql), {
      message: "[ck-orm] Query contains multiple statements; only a single statement is allowed per request",
    });
    await expectNoMutationAfterRejectedInjection();

    await expectClientValidationNotSent(db.command(stackedSql), {
      message: "[ck-orm] Query contains multiple statements; only a single statement is allowed per request",
    });
    await expectNoMutationAfterRejectedInjection();
  });

  it("rejects stacked sql.raw fragments when the compiled builder is executed", async function testStackedSqlRawFragment() {
    const db = createE2EDb();
    const injectedOrder = db
      .select({
        id: users.id,
      })
      .from(users)
      .orderBy(sql.raw("1; DROP TABLE users; --"))
      .limit(1);

    await expectClientValidationNotSent(injectedOrder.execute(), {
      message: "[ck-orm] Query contains multiple statements; only a single statement is allowed per request",
    });

    await expectNoMutationAfterRejectedInjection();
  });

  it("allows semicolons inside string literals and block comments in raw SQL", async function testLiteralAndCommentSemicolons() {
    const db = createE2EDb();

    expect(await db.execute("SELECT '; DROP TABLE users; --' AS payload")).toEqual([
      { payload: "; DROP TABLE users; --" },
    ]);

    expect(await db.execute("SELECT 1 /* ; inside comment */ AS one;")).toEqual([{ one: 1 }]);
  });
});

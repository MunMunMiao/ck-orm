import { expect, it } from "bun:test";
import { csql } from "./ck-orm";
import { createE2EDb } from "./shared";
import { describeE2E, expectClientValidationNotSent, expectNoMutationAfterRejectedInjection } from "./test-helpers";

describeE2E("ck-orm e2e injection raw sql", function describeInjectionRawSql() {
  it("rejects stacked csql statements in execute() and command() and leaves base tables untouched", async function testStackedRawSql() {
    const db = createE2EDb();
    const stackedSql = csql`SELECT 1; DROP TABLE IF EXISTS users`;

    await expectClientValidationNotSent(db.execute(stackedSql), {
      message: "[ck-orm] Query contains multiple statements; only a single statement is allowed per request",
    });
    await expectNoMutationAfterRejectedInjection();

    await expectClientValidationNotSent(db.command(stackedSql), {
      message: "[ck-orm] Query contains multiple statements; only a single statement is allowed per request",
    });
    await expectNoMutationAfterRejectedInjection();
  });

  it("allows semicolons inside string literals and block comments in csql statements", async function testLiteralAndCommentSemicolons() {
    const db = createE2EDb();

    expect(await db.execute(csql`SELECT '; DROP TABLE users; --' AS payload`)).toEqual([
      { payload: "; DROP TABLE users; --" },
    ]);

    expect(await db.execute(csql`SELECT 1 /* ; inside comment */ AS one;`)).toEqual([{ one: 1 }]);
  });
});

import { expect, it } from "bun:test";
import { ck, ckSql } from "./ck-orm";
import { createE2EDb, users } from "./shared";
import { describeE2E } from "./test-helpers";

describeE2E("ck-orm e2e SQL injection foundations", function describeSqlInjection() {
  it("parameterizes classic payloads in builder equality filters", async function testParameterizedQueries() {
    const db = createE2EDb();

    const payloads = [
      "'; DROP TABLE users; --",
      "' OR '1'='1",
      "' UNION SELECT * FROM password --",
      "1; DELETE FROM users",
      "1' AND 1=1 --",
      "1' AND 1=2 --",
      "admin'--",
      "' UNION SELECT null, version(), null --",
    ];

    for (const payload of payloads) {
      const rows = await db.select().from(users).where(ck.eq(users.name, payload)).limit(1);

      expect(rows).toEqual([]);
    }
  });

  it("parameterizes classic payloads in raw template literals and preserves unicode separators", async function testRawTemplateLiteralParameterization() {
    const db = createE2EDb();
    const payloads = [
      "'; DROP TABLE users; --",
      "' OR '1'='1",
      "' UNION SELECT * FROM password --",
      "1; DELETE FROM users",
      "1' AND 1=1 --",
      "1' AND 1=2 --",
      "admin'--",
      "' UNION SELECT null, version(), null --",
      "hello\u2028world",
      "hello\u2029world",
    ];

    for (const payload of payloads) {
      const rows = await db.execute(ckSql`
        select ${users.id} as id
        from ${users}
        where ${users.name} = ${payload}
        limit 1
      `);

      expect(rows).toEqual([]);
    }
  });
});

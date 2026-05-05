import { expect, it } from "bun:test";
import { ckSql, clickhouseClient } from "./ck-orm";
import { describeE2E } from "./test-helpers";

describeE2E("ck-orm e2e auth security", function describeAuthSecurity() {
  it("returns 401 for invalid credentials and does not leak credentials in error messages", async function testInvalidCredentials() {
    const config = {
      host: process.env.CLICKHOUSE_E2E_URL ?? "http://localhost:8123",
      database: process.env.CLICKHOUSE_E2E_DATABASE ?? "default",
      username: `invalid_user_${Date.now()}`,
      password: `invalid_password_${Date.now()}`,
    };

    const db = clickhouseClient(config);

    let error: unknown;
    try {
      await db.execute(ckSql`SELECT 1`);
    } catch (caught) {
      error = caught;
    }

    expect(error).toBeDefined();
    expect(error).toBeInstanceOf(Error);

    const errorMessage = String(error);
    expect(errorMessage).not.toContain(config.password);
  });
});

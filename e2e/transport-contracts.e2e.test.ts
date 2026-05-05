import { expect, it } from "bun:test";
import { ckSql, clickhouseClient } from "./ck-orm";
import { getE2EConfig, getE2EDatabaseUrl, hasTransportRoleFixtures, transportRoleFixtures } from "./shared";
import { describeE2E, expectPresent, withFetchSampling } from "./test-helpers";

const createStructuredTransportDb = (overrides?: Record<string, unknown>) => {
  const config = getE2EConfig();
  return clickhouseClient({
    host: config.host,
    database: config.database,
    username: config.username,
    password: config.password,
    schema: {},
    ...(overrides ?? {}),
  });
};

const roleFixtureIt = hasTransportRoleFixtures ? it : it.skip;

describeE2E("ck-orm e2e transport contracts", function describeTransportContracts() {
  it("uses databaseUrl credentials while stripping them from the outgoing request URL", async function testDatabaseUrlFlow() {
    const config = getE2EConfig();

    await withFetchSampling(async (calls) => {
      const db = clickhouseClient({
        databaseUrl: getE2EDatabaseUrl(),
        schema: {},
      });

      const rows = await db.execute(ckSql`
        select
          currentUser() as current_user,
          currentDatabase() as current_database
      `);

      expect(rows).toEqual([
        {
          current_user: config.username,
          current_database: config.database,
        },
      ]);

      const call = expectPresent(calls[0], "fetch call");
      expect(call.url.username).toBe("");
      expect(call.url.password).toBe("");
      expect(call.url.searchParams.get("database")).toBe(config.database);
    });
  });

  it("merges http_headers without allowing Authorization override", async function testHeaderMergeAndAuthPrecedence() {
    const config = getE2EConfig();
    const basicAuth = `Basic ${Buffer.from(`${config.username}:${config.password}`).toString("base64")}`;

    await withFetchSampling(async (calls) => {
      const db = createStructuredTransportDb({
        http_headers: {
          Authorization: "Bearer client-attack",
          "x-client-header": "client",
          "x-shared-header": "client-shared",
        },
      });

      const rows = await db.execute(ckSql`select 1 as ok`, {
        http_headers: {
          Authorization: "Bearer request-attack",
          "x-per-request-header": "request",
          "x-shared-header": "request-shared",
        },
      });

      expect(rows).toEqual([{ ok: 1 }]);

      const call = expectPresent(calls[0], "fetch call");
      expect(call.requestHeaders.get("Authorization")).toBe(basicAuth);
      expect(call.requestHeaders.get("x-client-header")).toBe("client");
      expect(call.requestHeaders.get("x-per-request-header")).toBe("request");
      expect(call.requestHeaders.get("x-shared-header")).toBe("request-shared");
    });
  });

  roleFixtureIt(
    "propagates repeated role parameters and applies them on the server",
    async function testRolePropagation() {
      const analystRole = expectPresent(transportRoleFixtures.analyst, "analyst role");
      const auditorRole = expectPresent(transportRoleFixtures.auditor, "auditor role");
      const roleUsername = expectPresent(transportRoleFixtures.username, "role username");
      const rolePassword = expectPresent(transportRoleFixtures.password, "role password");
      const roles = [analystRole, auditorRole];

      await withFetchSampling(async (calls) => {
        const config = getE2EConfig();
        const db = clickhouseClient({
          host: config.host,
          database: config.database,
          username: roleUsername,
          password: rolePassword,
          schema: {},
          role: roles,
        });

        const rows = await db.execute(ckSql`
        select arraySort(currentRoles()) as current_roles
      `);

        expect(rows).toEqual([
          {
            current_roles: [...roles].sort(),
          },
        ]);

        const call = expectPresent(calls[0], "fetch call");
        expect(call.url.searchParams.getAll("role")).toEqual(roles);
      });
    },
  );

  it("requests real gzip-compressed responses when compression.response is enabled", async function testResponseCompression() {
    await withFetchSampling(async (calls) => {
      const db = createStructuredTransportDb({
        compression: {
          response: true,
        },
      });

      const rows = await db.execute(ckSql`
        select
          number,
          repeat('transport_payload_', 32) as payload,
          toUInt8(getSetting('enable_http_compression')) as compression_enabled
        from numbers(4096)
      `);

      expect(rows).toHaveLength(4096);
      expect((expectPresent(rows[0], "first row") as { compression_enabled: number }).compression_enabled).toBe(1);

      const call = expectPresent(calls[0], "fetch call");
      expect(call.requestHeaders.get("Accept-Encoding")).toBe("gzip");
      expect(call.responseHeaders.get("Content-Encoding")).toBe("gzip");
    });
  });
});

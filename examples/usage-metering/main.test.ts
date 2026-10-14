import { expect, test } from "bun:test";
import type { ClickHouseORMClient } from "ck-orm";
import { ckSql, clickhouseClient } from "ck-orm";
import { run } from "./main";

const databaseUrl = process.env.CLICKHOUSE_EXAMPLES_URL ?? "http://localhost:8123";
const realTest = test.skipIf(!process.env.CLICKHOUSE_EXAMPLES_URL);
const names = [
  "ck_orm_example_usage_events",
  "ck_orm_example_usage_minutes",
  "ck_orm_example_usage_mv",
];

const objects = (db: ClickHouseORMClient) =>
  db.execute(ckSql`SELECT name FROM system.tables
WHERE database = currentDatabase() AND has(${names}, name) ORDER BY name`);

const requestSql = async (request: Request) => {
  const copy = request.clone();
  const isForm = copy.headers.get("content-type")?.startsWith("multipart/form-data");
  const query = isForm
    ? String((await copy.formData()).get("query"))
    : (new URL(copy.url).searchParams.get("query") ?? "");
  return [query, isForm ? "" : await copy.text()].filter(Boolean).join("\n");
};

const fetchWithPausedMerges = (
  observer: ClickHouseORMClient,
  pause = () => observer.command(ckSql`SYSTEM STOP MERGES ck_orm_example_usage_minutes`),
) => {
  let paused = false;
  return async (request: Request) => {
    const sql = await requestSql(request);
    if (!paused && sql.startsWith("insert into ")) {
      // CREATE responses must reach run before a pause failure can interrupt it.
      paused = true;
      await pause();
    }
    return globalThis.fetch(request);
  };
};

realTest("merges two unmerged usage blocks into isolated UTC minute totals", async () => {
  const observer = clickhouseClient({ databaseUrl });
  expect(await objects(observer)).toEqual([]);
  let states: Record<string, unknown>[] = [];
  const send = fetchWithPausedMerges(observer);
  const db = clickhouseClient({
    databaseUrl,
    clickhouse_settings: { session_timezone: "America/Los_Angeles" },
    fetch: async (request) => {
      const sql = await requestSql(request);
      const response = await send(request);
      if (response.ok && sql.startsWith("select ")) {
        states = await observer.execute(ckSql`SELECT toUInt32(count()) AS states
FROM ck_orm_example_usage_minutes
WHERE tenant_id = 'alpha' AND user_id = 'alex' AND meter = 'api_tokens'
  AND minute = toDateTime64('2026-01-01 00:00:00', 3, 'UTC')`);
      }
      return response;
    },
  });

  const rows = await run(db);

  expect(rows).toEqual([
    {
      tenantId: "alpha",
      userId: "alex",
      minute: new Date("2026-01-01T00:00:00Z"),
      usage: "10",
      events: "3",
    },
    {
      tenantId: "alpha",
      userId: "alex",
      minute: new Date("2026-01-01T00:01:00Z"),
      usage: "23",
      events: "1",
    },
    {
      tenantId: "alpha",
      userId: "blair",
      minute: new Date("2026-01-01T00:00:00Z"),
      usage: "7",
      events: "1",
    },
    {
      tenantId: "alpha",
      userId: "blair",
      minute: new Date("2026-01-01T00:01:00Z"),
      usage: "17",
      events: "1",
    },
    {
      tenantId: "beta",
      userId: "alex",
      minute: new Date("2026-01-01T00:00:00Z"),
      usage: "11",
      events: "1",
    },
    {
      tenantId: "beta",
      userId: "alex",
      minute: new Date("2026-01-01T00:01:00Z"),
      usage: "19",
      events: "1",
    },
  ]);
  expect(states).toEqual([{ states: 2 }]);
  expect(await objects(observer)).toEqual([]);
});

realTest("cleans up owned objects when merge pause fails", async () => {
  const observer = clickhouseClient({ databaseUrl });
  expect(await objects(observer)).toEqual([]);
  const pauseFailure = new Error("injected merge pause failure");
  let owned: string[] = [];
  const db = clickhouseClient({
    databaseUrl,
    fetch: fetchWithPausedMerges(observer, async () => {
      owned = (await objects(observer)).map((row) => String(row.name));
      expect(
        await observer.execute(ckSql`SELECT toUInt32(count()) AS rows
FROM ck_orm_example_usage_events`),
      ).toEqual([{ rows: 0 }]);
      throw pauseFailure;
    }),
  });

  try {
    await expect(run(db)).rejects.toMatchObject({ cause: pauseFailure });
    expect(await objects(observer)).toEqual([]);
  } finally {
    for (const name of names.toReversed()) {
      if (owned.includes(name)) {
        await observer.command(
          ckSql`DROP TABLE IF EXISTS ${ckSql.identifier(name)} SYNC`,
        );
      }
    }
  }
});

realTest("preserves SELECT and DROP errors while finishing cleanup", async () => {
  const observer = clickhouseClient({ databaseUrl });
  expect(await objects(observer)).toEqual([]);
  const owned: string[] = [];
  const drops: string[] = [];
  const dropFailure = "injected first DROP response failure";
  let selectResponse = "";
  let selectQueryId: string | null = null;
  let dropQueryId: string | null = null;
  const db = clickhouseClient({
    databaseUrl,
    clickhouse_settings: { max_result_rows: 1, result_overflow_mode: "throw" },
    fetch: async (request) => {
      const sql = await requestSql(request);
      const response = await globalThis.fetch(request);
      const create = /^CREATE (?:TABLE|MATERIALIZED VIEW) `([^`]+)`/.exec(sql);
      if (response.ok && create?.[1]) owned.push(create[1]);
      if (sql.startsWith("select ")) {
        selectQueryId = new URL(request.url).searchParams.get("query_id");
        selectResponse = await response.clone().text();
      }
      if (response.ok && sql.startsWith("DROP TABLE ")) {
        drops.push(sql);
        if (drops.length === 1) {
          dropQueryId = new URL(request.url).searchParams.get("query_id");
          // The real DROP succeeds; only its response is replaced.
          await response.text();
          return new Response(dropFailure, { status: 503 });
        }
      }
      return response;
    },
  });

  try {
    const failure = await run(db).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(AggregateError);
    const errors = (failure as AggregateError).errors;
    expect(errors).toHaveLength(2);
    expect(selectResponse).toContain("TOO_MANY_ROWS_OR_BYTES");
    expect(errors[0]).toMatchObject({
      kind: "request_failed",
      clickhouseName: "TOO_MANY_ROWS_OR_BYTES",
      queryId: selectQueryId,
      responseText: selectResponse.trim(),
    });
    expect(errors[1]).toMatchObject({
      kind: "request_failed",
      httpStatus: 503,
      queryId: dropQueryId,
      responseText: dropFailure,
    });
    expect(drops).toEqual([
      "DROP TABLE `ck_orm_example_usage_mv` SYNC",
      "DROP TABLE `ck_orm_example_usage_minutes` SYNC",
      "DROP TABLE `ck_orm_example_usage_events` SYNC",
    ]);
    expect(await objects(observer)).toEqual([]);
  } finally {
    for (const name of owned.reverse()) {
      await observer.command(ckSql`DROP TABLE IF EXISTS ${ckSql.identifier(name)} SYNC`);
    }
  }
});

realTest("cleans up all owned objects after a real SELECT failure", async () => {
  const db = clickhouseClient({ databaseUrl });
  expect(await objects(db)).toEqual([]);
  const limited = clickhouseClient({
    databaseUrl,
    clickhouse_settings: { max_result_rows: 1, result_overflow_mode: "throw" },
  });

  await expect(run(limited)).rejects.toThrow(/TOO_MANY_ROWS_OR_BYTES/);
  expect(await objects(db)).toEqual([]);
});

for (const name of names) {
  realTest(`preserves existing ${name} when CREATE fails`, async () => {
    const db = clickhouseClient({ databaseUrl });
    expect(await objects(db)).toEqual([]);
    const identifier = ckSql.identifier(name);
    // CREATE is outside the try: a failed CREATE must never authorize a DROP.
    await db.command(ckSql`CREATE TABLE ${identifier} (marker String) ENGINE = Memory`);
    try {
      await db.command(ckSql`INSERT INTO ${identifier} VALUES ('keep-me')`);

      await expect(run(db)).rejects.toThrow(/already exists/);

      expect(await objects(db)).toEqual([{ name }]);
      expect(await db.execute(ckSql`SELECT marker FROM ${identifier}`)).toEqual([
        { marker: "keep-me" },
      ]);
    } finally {
      await db.command(ckSql`DROP TABLE ${identifier}`);
    }
  });
}

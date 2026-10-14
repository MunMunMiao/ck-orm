import { expect, test } from "bun:test";
import { clickhouseClient } from "ck-orm";
import { run } from "./main";

const databaseUrl = process.env.CLICKHOUSE_EXAMPLES_URL ?? "";
const realTest = test.skipIf(!process.env.CLICKHOUSE_EXAMPLES_URL);

// Reads a request the way the usage-metering and literal-search tests do:
// multipart requests carry the statement in the `query` field, others in the body or URL.
const readRequest = async (request: Request) => {
  const copy = request.clone();
  const isForm =
    copy.headers.get("content-type")?.startsWith("multipart/form-data") ?? false;
  if (isForm) {
    const form = await copy.formData();
    return { sql: String(form.get("query")), form };
  }
  const sql = [new URL(copy.url).searchParams.get("query") ?? "", await copy.text()]
    .filter(Boolean)
    .join("\n");
  return { sql, form: undefined };
};

// Stands in for ClickHouse: records every statement and answers with no rows.
const recordingDb = () => {
  const selects: { sql: string; form?: FormData }[] = [];
  const fetch = async (request: Request) => {
    const { sql, form } = await readRequest(request);
    if (sql.startsWith("select")) selects.push({ sql, form });
    return Response.json({ data: [] });
  };
  return { selects, db: clickhouseClient({ host: "http://clickhouse.test", fetch }) };
};

test("builds the page and count queries from the filters", async () => {
  const { selects, db } = recordingDb();

  expect(await run(db, { name: "chat", page: 2 })).toEqual({ total: 0, rows: [] });

  // The page query is sent first, then the count query.
  expect(selects).toHaveLength(2);
  const [page, count] = selects;
  const where =
    "(`traces`.`project_id` = {orm_param1:String} and `traces`.`name` = {orm_param2:String})";
  expect(page.sql).toContain(where);
  expect(page.sql).toContain("limit 2 offset 2");
  expect(page.sql).not.toContain("has(");
  expect(page.form?.get("param_orm_param1")).toBe("acme");
  expect(page.form?.get("param_orm_param2")).toBe("chat");

  expect(count.sql).toContain("select (select toFloat64(count()) from");
  expect(count.sql).toContain(where);
});

test("omits the optional predicates when no filters are given", async () => {
  const { selects, db } = recordingDb();

  await run(db);

  expect(selects).toHaveLength(2);
  // The statement is followed by a FORMAT line, so compare the first line only.
  const [statement] = selects[0].sql.split("\r\n");
  expect(statement).toContain(
    "from `traces` where `traces`.`project_id` = {orm_param1:String} order by",
  );
  expect(statement).not.toContain(" and ");
  expect(statement.endsWith("limit 2 offset 0")).toBe(true);
});

// t4 and t5 share a timestamp and sort by trace id; x1 belongs to another project and never appears.
realTest(
  "pages through the filtered traces and keeps the total past the last page",
  async () => {
    const db = clickhouseClient({ databaseUrl });
    const t1 = {
      traceId: "t1",
      name: "chat",
      userId: "alice",
      latencyMs: 820,
      totalCost: "0.0012",
      timestamp: new Date("2026-03-01T10:00:00.000Z"),
    };
    const t2 = {
      traceId: "t2",
      name: "chat",
      userId: "bob",
      latencyMs: 2400,
      totalCost: "0.0045",
      timestamp: new Date("2026-03-01T10:05:00.000Z"),
    };
    const t3 = {
      traceId: "t3",
      name: "summarize",
      userId: "alice",
      latencyMs: 1500,
      totalCost: "0.002",
      timestamp: new Date("2026-03-01T10:10:00.000Z"),
    };
    const t4 = {
      traceId: "t4",
      name: "chat",
      userId: "alice",
      latencyMs: 3100,
      totalCost: "0.0061",
      timestamp: new Date("2026-03-01T10:15:00.000Z"),
    };
    const t5 = {
      traceId: "t5",
      name: "chat",
      userId: "carol",
      latencyMs: 450,
      totalCost: "0.0007",
      timestamp: new Date("2026-03-01T10:15:00.000Z"),
    };

    expect(await run(db)).toEqual({ total: 5, rows: [t4, t5] });
    expect(await run(db, { page: 2 })).toEqual({ total: 5, rows: [t3, t2] });
    expect(await run(db, { page: 3 })).toEqual({ total: 5, rows: [t1] });
    expect(await run(db, { page: 4 })).toEqual({ total: 5, rows: [] });
  },
);

// Chat traces at or above 800 ms are t1, t2 and t4 (total 3); the newest two, t4 and t2, fill page 1. t3 is not a chat.
realTest("combines optional filters with AND", async () => {
  const db = clickhouseClient({ databaseUrl });

  expect(await run(db, { name: "chat", minLatencyMs: 800 })).toEqual({
    total: 3,
    rows: [
      {
        traceId: "t4",
        name: "chat",
        userId: "alice",
        latencyMs: 3100,
        totalCost: "0.0061",
        timestamp: new Date("2026-03-01T10:15:00.000Z"),
      },
      {
        traceId: "t2",
        name: "chat",
        userId: "bob",
        latencyMs: 2400,
        totalCost: "0.0045",
        timestamp: new Date("2026-03-01T10:05:00.000Z"),
      },
    ],
  });
  expect(await run(db, { tag: "prod", userId: "alice" })).toEqual({
    total: 2,
    rows: [
      {
        traceId: "t3",
        name: "summarize",
        userId: "alice",
        latencyMs: 1500,
        totalCost: "0.002",
        timestamp: new Date("2026-03-01T10:10:00.000Z"),
      },
      {
        traceId: "t1",
        name: "chat",
        userId: "alice",
        latencyMs: 820,
        totalCost: "0.0012",
        timestamp: new Date("2026-03-01T10:00:00.000Z"),
      },
    ],
  });
  expect(await run(db, { name: "missing" })).toEqual({ total: 0, rows: [] });
});

// The window is [from, to): t2 at 10:05 is included, t4 at 10:15 is excluded.
realTest("applies a half-open time window", async () => {
  const db = clickhouseClient({ databaseUrl });

  expect(
    await run(db, {
      from: new Date("2026-03-01T10:05:00.000Z"),
      to: new Date("2026-03-01T10:15:00.000Z"),
    }),
  ).toEqual({
    total: 2,
    rows: [
      {
        traceId: "t3",
        name: "summarize",
        userId: "alice",
        latencyMs: 1500,
        totalCost: "0.002",
        timestamp: new Date("2026-03-01T10:10:00.000Z"),
      },
      {
        traceId: "t2",
        name: "chat",
        userId: "bob",
        latencyMs: 2400,
        totalCost: "0.0045",
        timestamp: new Date("2026-03-01T10:05:00.000Z"),
      },
    ],
  });
});

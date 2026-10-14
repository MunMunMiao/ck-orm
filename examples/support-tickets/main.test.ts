import { expect, test } from "bun:test";
import { clickhouseClient } from "ck-orm";
import type { TicketId } from "./main";
import { run } from "./main";

const id = (value: number) => value as TicketId;

const requestSql = async (request: Request) => {
  const copy = request.clone();
  const isForm = copy.headers.get("content-type")?.startsWith("multipart/form-data");
  const query = isForm
    ? String((await copy.formData()).get("query"))
    : (new URL(copy.url).searchParams.get("query") ?? "");
  return [query, isForm ? "" : await copy.text()].filter(Boolean).join("\n");
};

test("validates the email before any insert request and normalizes it", async () => {
  const requests: Request[] = [];
  const fakeDb = clickhouseClient({
    host: "http://clickhouse.test",
    fetch: async (request) => {
      requests.push(request.clone());
      return Response.json({ data: [] });
    },
  });

  expect(await run(fakeDb)).toEqual({
    queue: [],
    rejected:
      'client_validation: [ck-orm] Standard Schema validation failed: Expected an email address, got "not-an-email"',
  });

  const inserts: { request: Request; sql: string }[] = [];
  for (const request of requests) {
    const sql = await requestSql(request);
    if (sql.startsWith("insert into `tickets`")) inserts.push({ request, sql });
  }
  expect(inserts).toHaveLength(1);

  const body = (await inserts[0]?.request.text()) ?? "";
  expect(body).toContain("'ana@example.com'");
  expect(body).not.toContain("Ana@Example.com");
  expect(body).not.toContain("not-an-email");
});

test.skipIf(!process.env.CLICKHOUSE_EXAMPLES_URL)(
  "runs the queue against real ClickHouse",
  async () => {
    const db = clickhouseClient({
      databaseUrl: process.env.CLICKHOUSE_EXAMPLES_URL ?? "",
    });

    // 103 (pending) and 104 (solved) are outside the queue.
    // urgent sorts before normal, and equal priorities sort by ticket id.
    // 101's email comes back trimmed and lowercased.
    expect(await run(db)).toEqual({
      queue: [
        {
          ticketId: id(102),
          status: "new",
          priority: "urgent",
          requesterEmail: "ben@example.com",
          plan: "enterprise",
        },
        {
          ticketId: id(105),
          status: "open",
          priority: "urgent",
          requesterEmail: "eve@example.com",
          plan: "free",
        },
        {
          ticketId: id(101),
          status: "open",
          priority: "normal",
          requesterEmail: "ana@example.com",
          plan: "team",
        },
      ],
      rejected:
        'client_validation: [ck-orm] Standard Schema validation failed: Expected an email address, got "not-an-email"',
    });
  },
);

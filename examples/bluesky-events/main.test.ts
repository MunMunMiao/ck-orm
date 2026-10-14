import { expect, test } from "bun:test";
import { clickhouseClient } from "ck-orm";
import { run } from "./main";

const requestSql = async (request: Request) => {
  const copy = request.clone();
  const isForm = copy.headers.get("content-type")?.startsWith("multipart/form-data");
  const query = isForm
    ? String((await copy.formData()).get("query"))
    : (new URL(copy.url).searchParams.get("query") ?? "");
  return [query, isForm ? "" : await copy.text()].filter(Boolean).join("\n");
};

test("runs the queries against a fake transport and sends the expected SQL", async () => {
  const sent: string[] = [];
  const fakeDb = clickhouseClient({
    host: "http://clickhouse.test",
    fetch: async (request: Request) => {
      sent.push(await requestSql(request));
      return Response.json({ data: [] });
    },
  });

  expect(await run(fakeDb)).toEqual({ byCollection: [], posts: [] });

  const createSql = sent.find((sql) => sql.includes("CREATE TEMPORARY TABLE"));
  expect(createSql).toContain(
    "`data` JSON(commit.collection LowCardinality(String), commit.operation LowCardinality(String), did String, kind LowCardinality(String), time_us UInt64)",
  );

  const postsSql = sent.find((sql) => sql.includes("commit.record.text"));
  expect(postsSql).toContain("`bluesky`.`data`.commit.record.text.:String");
  expect(postsSql).toContain("`bluesky`.`data`.^commit");
});

const realTest = test.skipIf(!process.env.CLICKHOUSE_EXAMPLES_URL);

realTest(
  "reads typed paths, cast paths and subobjects from real ClickHouse",
  async () => {
    const db = clickhouseClient({
      databaseUrl: process.env.CLICKHOUSE_EXAMPLES_URL ?? "",
    });
    const { byCollection, posts } = await run(db);

    expect(byCollection).toEqual([
      // The like count is 2, not 3, because the delete is excluded.
      { collection: "app.bsky.feed.like", events: 2, users: 2 },
      // Like and post tie at 2 and sort by collection name.
      { collection: "app.bsky.feed.post", events: 2, users: 2 },
      { collection: "app.bsky.graph.follow", events: 1, users: 1 },
    ]);
    // time_us keeps all 16 digits as a string.
    expect(posts).toEqual([
      {
        did: "did:plc:alice",
        timeUs: "1732206349000167",
        text: "hello",
        commit: {
          collection: "app.bsky.feed.post",
          operation: "create",
          record: { langs: ["en"], text: "hello" },
          rev: "rev-1732206349000167",
        },
      },
      {
        did: "did:plc:carol",
        timeUs: "1732206352000000",
        text: "bonjour",
        commit: {
          collection: "app.bsky.feed.post",
          operation: "create",
          record: { langs: ["fr"], text: "bonjour" },
          rev: "rev-1732206352000000",
        },
      },
    ]);
  },
);

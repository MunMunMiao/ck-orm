/**
 * Querying JSON event payloads
 *
 * The event shape, typed paths and the first query follow ClickHouse JSONBench:
 * https://github.com/ClickHouse/JSONBench
 * The events below are a small synthetic sample, not the benchmark dataset.
 */
import type { ClickHouseORMClient } from "ck-orm";
import { ck, ckTable, ckType, clickhouseClient, fn } from "ck-orm";

type BlueskyRecord = { text?: string; langs?: string[]; subject?: string };

type BlueskyEvent = {
  did: string;
  time_us: string; // UInt64 typed paths decode losslessly as strings.
  kind: string;
  // Identity and account events have no commit; deletes have no record.
  commit?: {
    rev: string;
    operation: string;
    collection: string;
    record?: BlueskyRecord;
  };
};

const bluesky = ckTable("bluesky", {
  data: ckType.json<BlueskyEvent>("data", {
    typeHints: {
      kind: ckType.lowCardinality(ckType.string()),
      "commit.operation": ckType.lowCardinality(ckType.string()),
      "commit.collection": ckType.lowCardinality(ckType.string()),
      did: ckType.string(),
      time_us: ckType.uint64(),
    },
  }),
});

const event = (
  did: string,
  time_us: string,
  operation: string,
  collection: string,
  record: BlueskyRecord,
) => ({
  data: {
    did,
    time_us,
    kind: "commit",
    commit: { rev: `rev-${time_us}`, operation, collection, record },
  },
});

export const run = (db: ClickHouseORMClient) =>
  db.runInSession(async (session) => {
    await session.createTemporaryTable(bluesky);
    await session.insert(bluesky).values([
      event("did:plc:alice", "1732206349000167", "create", "app.bsky.feed.post", {
        text: "hello",
        langs: ["en"],
      }),
      event("did:plc:bob", "1732206350000000", "create", "app.bsky.feed.like", {
        subject: "at://did:plc:alice/app.bsky.feed.post/1",
      }),
      event("did:plc:alice", "1732206351000000", "create", "app.bsky.feed.like", {
        subject: "at://did:plc:carol/app.bsky.feed.post/2",
      }),
      event("did:plc:carol", "1732206352000000", "create", "app.bsky.feed.post", {
        text: "bonjour",
        langs: ["fr"],
      }),
      event("did:plc:bob", "1732206353000000", "delete", "app.bsky.feed.like", {}),
      event("did:plc:alice", "1732206354000000", "create", "app.bsky.graph.follow", {
        subject: "did:plc:carol",
      }),
    ]);

    const collection = bluesky.data.path("commit.collection");
    const created = [
      ck.eq(bluesky.data.path("kind"), "commit"),
      ck.eq(bluesky.data.path("commit.operation"), "create"),
    ];

    // Events and distinct users per collection, for created commits only
    // (JSONBench query 2).
    // The collection name breaks ties in the count so the order is deterministic.
    const byCollection = await session
      .select({
        collection,
        events: fn.count(),
        users: fn.uniqExact(bluesky.data.path("did")),
      })
      .from(bluesky)
      .where(...created)
      .groupBy(collection)
      .orderBy(ck.desc(fn.count()), ck.asc(collection));

    // castPath is for paths that are not typed hints: ClickHouse rejects a `.:Type`
    // cast on a typed path.
    // subobject returns the nested object as JSON.
    const posts = await session
      .select({
        did: bluesky.data.path("did"),
        timeUs: bluesky.data.path("time_us"),
        text: bluesky.data.castPath("commit.record.text", ckType.string()),
        commit: bluesky.data.subobject("commit"),
      })
      .from(bluesky)
      .where(...created, ck.eq(collection, "app.bsky.feed.post"))
      .orderBy(ck.asc(bluesky.data.path("time_us")));

    return { byCollection, posts };
  });

if (import.meta.main) {
  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
  });
  console.log(await run(db));
  // {
  //   byCollection: [
  //     { collection: "app.bsky.feed.like", events: 2, users: 2 },
  //     { collection: "app.bsky.feed.post", events: 2, users: 2 },
  //     { collection: "app.bsky.graph.follow", events: 1, users: 1 }
  //   ],
  //   posts: [
  //     {
  //       did: "did:plc:alice",
  //       timeUs: "1732206349000167",
  //       text: "hello",
  //       commit: {
  //         collection: "app.bsky.feed.post", operation: "create",
  //         record: { langs: ["en"], text: "hello" }, rev: "rev-1732206349000167"
  //       }
  //     },
  //     {
  //       did: "did:plc:carol",
  //       timeUs: "1732206352000000",
  //       text: "bonjour",
  //       commit: {
  //         collection: "app.bsky.feed.post", operation: "create",
  //         record: { langs: ["fr"], text: "bonjour" }, rev: "rev-1732206352000000"
  //       }
  //     }
  //   ]
  // }
}

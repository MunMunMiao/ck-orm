/**
 * Website analytics
 *
 * Adapted from Plausible's historical raw-event analytics use case:
 * https://plausible.io/blog/april-2020-recap
 * Uses uniqExact instead of historical uniq so visitor counts here are exact.
 */
import type { ClickHouseORMClient } from "ck-orm";
import { ck, ckTable, ckType, clickhouseClient, fn } from "ck-orm";

const events = ckTable("website_events", {
  siteId: ckType.string("site_id"),
  userId: ckType.string("user_id"),
  name: ckType.string(),
  pathname: ckType.string(),
  timestamp: ckType.dateTime64({ precision: 3, timezone: "UTC" }),
});

const start = new Date("2026-01-01T00:00:00.000Z");
const end = new Date("2026-01-02T00:00:00.000Z");

// A session temporary table: no persistent tables or schema changes.
export const run = (db: ClickHouseORMClient) =>
  db.runInSession(async (session) => {
    await session.createTemporaryTable(events);
    await session.insert(events).values([
      {
        siteId: "example.com",
        userId: "alice",
        name: "pageview",
        pathname: "/",
        timestamp: new Date("2026-01-01T00:00:00.000Z"),
      },
      {
        siteId: "example.com",
        userId: "alice",
        name: "pageview",
        pathname: "/",
        timestamp: new Date("2026-01-01T12:00:00.000Z"),
      },
      {
        siteId: "example.com",
        userId: "bob",
        name: "pageview",
        pathname: "/",
        timestamp: new Date("2026-01-01T23:59:59.999Z"),
      },
      {
        siteId: "example.com",
        userId: "alice",
        name: "pageview",
        pathname: "/pricing",
        timestamp: new Date("2026-01-01T12:00:00.000Z"),
      },
      {
        siteId: "example.com",
        userId: "before",
        name: "pageview",
        pathname: "/",
        timestamp: new Date("2025-12-31T23:59:59.999Z"),
      },
      {
        siteId: "example.com",
        userId: "at-end",
        name: "pageview",
        pathname: "/",
        timestamp: new Date("2026-01-02T00:00:00.000Z"),
      },
      {
        siteId: "other.example",
        userId: "other",
        name: "pageview",
        pathname: "/",
        timestamp: new Date("2026-01-01T12:00:00.000Z"),
      },
      {
        siteId: "example.com",
        userId: "custom",
        name: "signup",
        pathname: "/",
        timestamp: new Date("2026-01-01T12:00:00.000Z"),
      },
    ]);

    // /: Alice + Alice + Bob = 3 pageviews, 2 visitors; /pricing: Alice = 1, 1.
    // Page-level visitors overlap, so their counts must not be summed across paths.
    return session
      .select({
        pathname: events.pathname,
        pageviews: fn.count().as("pageviews"),
        visitors: fn.uniqExact(events.userId).as("visitors"),
      })
      .from(events)
      .where(
        ck.eq(events.siteId, "example.com"),
        ck.eq(events.name, "pageview"),
        ck.gte(events.timestamp, start),
        ck.lt(events.timestamp, end),
      )
      .groupBy(events.pathname)
      .orderBy(ck.asc(events.pathname));
  });

if (import.meta.main) {
  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
  });
  const rows = await run(db);
  console.log(rows);
  // [
  //   { pathname: "/", pageviews: 3, visitors: 2 },
  //   { pathname: "/pricing", pageviews: 1, visitors: 1 }
  // ]
}

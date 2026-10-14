/**
 * Rolling up CDC rows inside ClickHouse
 *
 * The `_peerdb_version` / `_peerdb_is_deleted` columns and the
 * `FINAL` + `_peerdb_is_deleted = 0` read follow ClickHouse's CDC deduplication guide:
 * https://clickhouse.com/docs/integrations/clickpipes/postgres/deduplication
 *
 * The data is synthetic. The session temporary tables stand in for the tables
 * ClickPipes manages.
 */
import type { ClickHouseORMClient } from "ck-orm";
import { ck, ckTable, ckType, clickhouseClient, fn } from "ck-orm";

// ClickPipes (PeerDB) writes Postgres CDC rows: an update is a new row with a higher
// _peerdb_version, and a delete is a new row with a higher version and
// _peerdb_is_deleted = 1.
const ordersCdc = ckTable(
  "orders_cdc",
  {
    orderId: ckType.uint32("order_id"),
    customerId: ckType.string("customer_id"),
    status: ckType.string(),
    amount: ckType.decimal({ precision: 18, scale: 2 }),
    createdAt: ckType.dateTime64("created_at", { precision: 3, timezone: "UTC" }),
    isDeleted: ckType.int8("_peerdb_is_deleted"),
    version: ckType.int64("_peerdb_version"),
  },
  (table) => ({
    engine: "ReplacingMergeTree",
    orderBy: [table.orderId],
    versionColumn: table.version,
  }),
);

const currentOrders = ckTable("current_orders", {
  orderId: ckType.uint32("order_id"),
  customerId: ckType.string("customer_id"),
  status: ckType.string(),
  amount: ckType.decimal({ precision: 18, scale: 2 }),
  day: ckType.date(),
});

const dailyRevenue = ckTable("daily_revenue", {
  day: ckType.date(),
  orders: ckType.uint64(),
  revenue: ckType.decimal({ precision: 38, scale: 2 }),
});

// Session temporary tables only: no persistent tables or schema changes.
export const run = (db: ClickHouseORMClient) =>
  db.runInSession(async (session) => {
    await session.createTemporaryTable(ordersCdc);
    await session.createTemporaryTable(currentOrders);
    await session.createTemporaryTable(dailyRevenue);

    // Two separate inserts create two data parts, which is what makes FINAL necessary.
    // ck-orm keeps 64-bit integers lossless as strings, so version is a string.
    await session.insert(ordersCdc).values([
      {
        orderId: 1,
        customerId: "c1",
        status: "paid",
        amount: "120.00",
        createdAt: new Date("2026-02-01T09:00:00.000Z"),
        isDeleted: 0,
        version: "1",
      },
      {
        orderId: 2,
        customerId: "c2",
        status: "paid",
        amount: "80.50",
        createdAt: new Date("2026-02-01T15:30:00.000Z"),
        isDeleted: 0,
        version: "1",
      },
      {
        orderId: 3,
        customerId: "c1",
        status: "paid",
        amount: "42.00",
        createdAt: new Date("2026-02-02T08:00:00.000Z"),
        isDeleted: 0,
        version: "1",
      },
      {
        orderId: 4,
        customerId: "c3",
        status: "pending",
        amount: "10.00",
        createdAt: new Date("2026-02-02T23:59:59.999Z"),
        isDeleted: 0,
        version: "1",
      },
    ]);
    await session.insert(ordersCdc).values([
      {
        // update
        orderId: 2,
        customerId: "c2",
        status: "refunded",
        amount: "80.50",
        createdAt: new Date("2026-02-01T15:30:00.000Z"),
        isDeleted: 0,
        version: "2",
      },
      {
        // delete
        orderId: 3,
        customerId: "c1",
        status: "paid",
        amount: "42.00",
        createdAt: new Date("2026-02-02T08:00:00.000Z"),
        isDeleted: 1,
        version: "2",
      },
      {
        // update
        orderId: 4,
        customerId: "c3",
        status: "paid",
        amount: "10.00",
        createdAt: new Date("2026-02-02T23:59:59.999Z"),
        isDeleted: 0,
        version: "2",
      },
    ]);

    // Step 1: FINAL keeps only the newest version of each order; deleted orders are
    // dropped.
    await session.insert(currentOrders).fromSelect(
      session
        .select({
          orderId: ordersCdc.orderId,
          customerId: ordersCdc.customerId,
          status: ordersCdc.status,
          amount: ordersCdc.amount,
          day: fn.toDate(ordersCdc.createdAt),
        })
        .from(ordersCdc)
        .final()
        .where(ck.eq(ordersCdc.isDeleted, 0)),
    );

    // Step 2: roll up the deduplicated orders into daily revenue.
    // The UInt64 column takes the string form of the count (toSafe()), the same rule
    // as insert().values().
    await session.insert(dailyRevenue).fromSelect(
      session
        .select({
          day: currentOrders.day,
          orders: fn.count().toSafe(),
          revenue: fn.sum(currentOrders.amount),
        })
        .from(currentOrders)
        .where(ck.eq(currentOrders.status, "paid"))
        .groupBy(currentOrders.day),
    );

    // Only the small daily result travels back to JavaScript.
    return session.select().from(dailyRevenue).orderBy(ck.asc(dailyRevenue.day));
  });

if (import.meta.main) {
  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
  });
  console.log(await run(db));
  // [
  //   { day: 2026-02-01T00:00:00.000Z, orders: "1", revenue: "120" },
  //   { day: 2026-02-02T00:00:00.000Z, orders: "1", revenue: "10" }
  // ]
}

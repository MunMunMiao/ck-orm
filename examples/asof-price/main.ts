/**
 * ASOF price
 *
 * Inspired by Coinhall's time-series analytics; synthetic data, not customer SQL.
 * Matches each trade to the latest same-symbol quote at or before its time.
 */
import type { ClickHouseORMClient } from "ck-orm";
import { ckSql, ckTable, ckType, clickhouseClient } from "ck-orm";

const trades = ckTable("trades", {
  tradeId: ckType.uint32("trade_id"),
  symbol: ckType.string(),
  tradeTime: ckType.dateTime64("trade_time", { precision: 3, timezone: "UTC" }),
});

const quotes = ckTable("quotes", {
  symbol: ckType.string(),
  quoteTime: ckType.dateTime64("quote_time", { precision: 3, timezone: "UTC" }),
  price: ckType.decimal({ precision: 20, scale: 5 }),
});

// Session temporary tables: the example needs no tables on the server.
export const run = (db: ClickHouseORMClient) =>
  db.runInSession(async (session) => {
    await session.createTemporaryTable(trades);
    await session.createTemporaryTable(quotes);
    await session.insert(trades).values([
      {
        tradeId: 6,
        symbol: "BTCUSD",
        tradeTime: new Date("2026-01-01T09:59:59Z"),
      },
      {
        tradeId: 4,
        symbol: "ETHUSD",
        tradeTime: new Date("2026-01-01T10:00:05Z"),
      },
      {
        tradeId: 2,
        symbol: "BTCUSD",
        tradeTime: new Date("2026-01-01T10:00:05Z"),
      },
      {
        tradeId: 8,
        symbol: "ETHUSD",
        tradeTime: new Date("2026-01-01T10:00:10Z"),
      },
      {
        tradeId: 5,
        symbol: "DOGEUSD",
        tradeTime: new Date("2026-01-01T10:00:20Z"),
      },
      {
        tradeId: 1,
        symbol: "BTCUSD",
        tradeTime: new Date("2026-01-01T10:00:00Z"),
      },
      {
        tradeId: 7,
        symbol: "BTCUSD",
        tradeTime: new Date("2026-01-01T10:00:30Z"),
      },
      {
        tradeId: 3,
        symbol: "BTCUSD",
        tradeTime: new Date("2026-01-01T10:00:10Z"),
      },
    ]);
    await session.insert(quotes).values([
      {
        symbol: "BTCUSD",
        quoteTime: new Date("2026-01-01T10:01:00Z"),
        price: "99999.99999",
      },
      {
        symbol: "ETHUSD",
        quoteTime: new Date("2026-01-01T10:00:10Z"),
        price: "3510.54321",
      },
      {
        symbol: "BTCUSD",
        quoteTime: new Date("2026-01-01T10:00:00Z"),
        price: "65000.00001",
      },
      {
        symbol: "ETHUSD",
        quoteTime: new Date("2026-01-01T10:00:03Z"),
        price: "3500.12345",
      },
      {
        symbol: "BTCUSD",
        quoteTime: new Date("2026-01-01T10:00:10Z"),
        price: "65010.00002",
      },
    ]);

    // ck-orm keeps Decimal JSON values as strings; unmatched prices remain null.
    return session.execute(ckSql`SELECT
      t.tradeId,
      t.symbol,
      q.price
    FROM
    (
      SELECT trade_id AS tradeId, symbol, trade_time AS tradeTime
      FROM ${trades}
      ORDER BY symbol, tradeTime
    ) AS t
    ASOF LEFT JOIN
    (
      SELECT symbol, quote_time AS quoteTime, price
      FROM ${quotes}
      ORDER BY symbol, quoteTime
    ) AS q
    ON t.symbol = q.symbol AND t.tradeTime >= q.quoteTime
    ORDER BY t.tradeId
    SETTINGS join_use_nulls = 1`);
  });

if (import.meta.main) {
  const db = clickhouseClient({
    databaseUrl: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
  });
  console.log(await run(db));
  // [
  //   { tradeId: 1, symbol: "BTCUSD", price: "65000.00001" },
  //   { tradeId: 2, symbol: "BTCUSD", price: "65000.00001" },
  //   { tradeId: 3, symbol: "BTCUSD", price: "65010.00002" },
  //   { tradeId: 4, symbol: "ETHUSD", price: "3500.12345" },
  //   { tradeId: 5, symbol: "DOGEUSD", price: null },
  //   { tradeId: 6, symbol: "BTCUSD", price: null },
  //   { tradeId: 7, symbol: "BTCUSD", price: "65010.00002" },
  //   { tradeId: 8, symbol: "ETHUSD", price: "3510.54321" },
  // ]
}

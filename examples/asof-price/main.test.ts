import { expect, test } from "bun:test";
import { clickhouseClient } from "ck-orm";
import { run } from "./main";

test.skipIf(!process.env.CLICKHOUSE_EXAMPLES_URL)(
  "matches the latest same-symbol quote at or before each trade, never a future quote",
  async () => {
    const db = clickhouseClient({
      databaseUrl: process.env.CLICKHOUSE_EXAMPLES_URL,
    });

    expect(await run(db)).toEqual([
      { tradeId: 1, symbol: "BTCUSD", price: "65000.00001" },
      { tradeId: 2, symbol: "BTCUSD", price: "65000.00001" },
      { tradeId: 3, symbol: "BTCUSD", price: "65010.00002" },
      { tradeId: 4, symbol: "ETHUSD", price: "3500.12345" },
      { tradeId: 5, symbol: "DOGEUSD", price: null },
      { tradeId: 6, symbol: "BTCUSD", price: null },
      { tradeId: 7, symbol: "BTCUSD", price: "65010.00002" },
      { tradeId: 8, symbol: "ETHUSD", price: "3510.54321" },
    ]);
  },
  10_000,
);

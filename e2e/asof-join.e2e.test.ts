import { expect, it } from "bun:test";
import { ck, ckSql, ckTable, ckType, type Session } from "./ck-orm";
import { createE2EDb, quoteSnapshots, tradeFills } from "./shared";
import { describeE2E, expectRejectsWithClickhouseError } from "./test-helpers";

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

const tradeRows = [
  { tradeId: 6, symbol: "BTCUSD", tradeTime: new Date("2026-01-01T09:59:59Z") },
  { tradeId: 4, symbol: "ETHUSD", tradeTime: new Date("2026-01-01T10:00:05Z") },
  { tradeId: 2, symbol: "BTCUSD", tradeTime: new Date("2026-01-01T10:00:05Z") },
  { tradeId: 8, symbol: "ETHUSD", tradeTime: new Date("2026-01-01T10:00:10Z") },
  { tradeId: 5, symbol: "DOGEUSD", tradeTime: new Date("2026-01-01T10:00:20Z") },
  { tradeId: 1, symbol: "BTCUSD", tradeTime: new Date("2026-01-01T10:00:00Z") },
  { tradeId: 7, symbol: "BTCUSD", tradeTime: new Date("2026-01-01T10:00:30Z") },
  { tradeId: 3, symbol: "BTCUSD", tradeTime: new Date("2026-01-01T10:00:10Z") },
];

const quoteRows = [
  { symbol: "BTCUSD", quoteTime: new Date("2026-01-01T10:01:00Z"), price: "99999.99999" },
  { symbol: "ETHUSD", quoteTime: new Date("2026-01-01T10:00:10Z"), price: "3510.54321" },
  { symbol: "BTCUSD", quoteTime: new Date("2026-01-01T10:00:00Z"), price: "65000.00001" },
  { symbol: "ETHUSD", quoteTime: new Date("2026-01-01T10:00:03Z"), price: "3500.12345" },
  { symbol: "BTCUSD", quoteTime: new Date("2026-01-01T10:00:10Z"), price: "65010.00002" },
];

const seedAsof = async <TJoinUseNulls extends 0 | 1>(session: Session<TJoinUseNulls>) => {
  await session.createTemporaryTable(trades);
  await session.createTemporaryTable(quotes);
  await session.insert(trades).values(tradeRows);
  await session.insert(quotes).values(quoteRows);
};

const pricesFor = async <TJoinUseNulls extends 0 | 1>(session: Session<TJoinUseNulls>, op: typeof ck.gte) => {
  const rows = await session
    .select({ tradeId: trades.tradeId, price: quotes.price })
    .from(trades)
    .asofLeftJoin(quotes, ck.and(ck.eq(trades.symbol, quotes.symbol), op(trades.tradeTime, quotes.quoteTime)))
    .orderBy(ck.asc(trades.tradeId));
  return rows.map((row) => row.price);
};

describeE2E("ck-orm e2e asof join", function describeAsofJoin() {
  it("matches the raw ASOF LEFT JOIN over seeded trade fills", async function testSeededAsofLeftJoin() {
    const db = createE2EDb();
    const raw = await db.execute(ckSql`
      SELECT
        f.trade_id,
        f.symbol,
        q.bid
      FROM
      (
        SELECT trade_id, symbol, filled_at
        FROM trade_fills
        WHERE trade_id IN (1, 2, 3)
        ORDER BY symbol, filled_at
      ) AS f
      ASOF LEFT JOIN
      (
        SELECT symbol, quote_time, bid
        FROM quote_snapshots
        ORDER BY symbol, quote_time
      ) AS q
      ON f.symbol = q.symbol AND f.filled_at >= q.quote_time
      ORDER BY f.trade_id
    `);
    const built = await db
      .select({
        trade_id: tradeFills.trade_id,
        symbol: tradeFills.symbol,
        bid: quoteSnapshots.bid,
      })
      .from(tradeFills)
      .asofLeftJoin(
        quoteSnapshots,
        ck.and(
          ck.eq(tradeFills.symbol, quoteSnapshots.symbol),
          ck.gte(tradeFills.filled_at, quoteSnapshots.quote_time),
        ),
      )
      .where(ck.inArray(tradeFills.trade_id, [1, 2, 3]))
      .orderBy(tradeFills.trade_id);

    // execute() leaves the raw rows untyped. Compare the decoded trade_id, symbol, and bid values.
    expect(built).toEqual(raw as typeof built);
  });

  it("matches the closest quote for each comparison and drops unmatched inner rows", async function testAsofPriceFixture() {
    const db = createE2EDb();
    const prices = await db.runInSession(async (session) => {
      await seedAsof(session);
      return {
        gte: await pricesFor(session, ck.gte),
        gt: await pricesFor(session, ck.gt),
        lte: await pricesFor(session, ck.lte),
        lt: await pricesFor(session, ck.lt),
        inner: await session
          .select({ tradeId: trades.tradeId, price: quotes.price })
          .from(trades)
          .asofJoin(quotes, ck.and(ck.eq(trades.symbol, quotes.symbol), ck.gte(trades.tradeTime, quotes.quoteTime)))
          .orderBy(ck.asc(trades.tradeId)),
      };
    });

    expect(prices.gte).toEqual([
      "65000.00001",
      "65000.00001",
      "65010.00002",
      "3500.12345",
      null,
      null,
      "65010.00002",
      "3510.54321",
    ]);
    expect(prices.gt).toEqual([
      null,
      "65000.00001",
      "65000.00001",
      "3500.12345",
      null,
      null,
      "65010.00002",
      "3500.12345",
    ]);
    expect(prices.lte).toEqual([
      "65000.00001",
      "65010.00002",
      "65010.00002",
      "3510.54321",
      null,
      "65000.00001",
      "99999.99999",
      "3510.54321",
    ]);
    expect(prices.lt).toEqual([
      "65010.00002",
      "65010.00002",
      "99999.99999",
      "3510.54321",
      null,
      "65000.00001",
      "99999.99999",
      null,
    ]);
    expect(prices.inner.map((row) => row.tradeId)).toEqual([1, 2, 3, 4, 7, 8]);

    const zero = createE2EDb().withSettings({ join_use_nulls: 0 });
    const zeroPrices = await zero.runInSession(async (session) => {
      await seedAsof(session);
      return pricesFor(session, ck.gte);
    });
    expect(zeroPrices[4]).toBe("0");
    expect(zeroPrices[5]).toBe("0");
  });

  it("rejects an ASOF condition with no inequality or with two inequalities", async function testAsofJoinOnRejected() {
    const db = createE2EDb();
    await db.runInSession(async (session) => {
      await session.createTemporaryTable(trades);
      await session.createTemporaryTable(quotes);
      const query = (on: ReturnType<typeof ck.eq>) =>
        session.select({ tradeId: trades.tradeId }).from(trades).asofLeftJoin(quotes, on);

      await expectRejectsWithClickhouseError(query(ck.eq(trades.symbol, quotes.symbol)).execute(), {
        kind: "request_failed",
        clickhouseName: "INVALID_JOIN_ON_EXPRESSION",
      });
      await expectRejectsWithClickhouseError(
        query(
          ck.and(
            ck.eq(trades.symbol, quotes.symbol),
            ck.gte(trades.tradeTime, quotes.quoteTime),
            ck.lt(trades.tradeTime, quotes.quoteTime),
          ),
        ).execute(),
        {
          kind: "request_failed",
          clickhouseName: "INVALID_JOIN_ON_EXPRESSION",
        },
      );
    });
  });
});

import { expect, it } from "bun:test";
import { ck, ckSql, fn } from "./ck-orm";
import { createE2EDb, scenarioSchema } from "./shared";
import { describeE2E, expectPresent } from "./test-helpers";

const { stockTickTrades, dexSwaps, solanaTransactions, nycTaxiTrips } = scenarioSchema;

describeE2E("ck-orm e2e — finance / crypto / time-series scenarios", function describeFinanceScenarios() {
  it("Stock tick trades: VWAP per symbol from raw ticks", async function testStockVwap() {
    const db = createE2EDb();
    const rows = await db
      .select({
        symbol: stockTickTrades.symbol,
        vwap: ck
          .expr<string>(
            ckSql`sum(${stockTickTrades.price} * ${stockTickTrades.trade_size}) / sum(${stockTickTrades.trade_size})`,
            { decoder: (value) => String(value), sqlType: "Decimal(38,8)" },
          )
          .as("vwap"),
        tradeCount: fn.count().as("trade_count"),
      })
      .from(stockTickTrades)
      .groupBy(stockTickTrades.symbol)
      .orderBy(stockTickTrades.symbol)
      .execute();
    expect(rows.length).toBe(2);
    const aapl = rows.find((row) => row.symbol === "AAPL");
    expect(expectPresent(aapl, "AAPL row").tradeCount).toBe(2);
  });

  it("DEX swaps: latest price per chain for ETH/USDC", async function testDexPrice() {
    const db = createE2EDb();
    const rows = await db
      .select({
        chainId: dexSwaps.chain_id,
        // Decimal(20,6) / Decimal(38,18) trips ClickHouse's strict scale check.
        // Cast both sides to Float64 first — sub-cent precision is irrelevant
        // for "latest price" reporting.
        latestPrice: ck
          .expr<number>(
            ckSql`argMax(toFloat64(${dexSwaps.amount_out_usd}) / toFloat64(${dexSwaps.amount_in}), ${dexSwaps.block_ts})`,
            { decoder: (value) => Number(value), sqlType: "Float64" },
          )
          .as("latest_price"),
      })
      .from(dexSwaps)
      .where(ck.and(ck.eq(dexSwaps.token_in, "ETH"), ck.eq(dexSwaps.token_out, "USDC")))
      .groupBy(dexSwaps.chain_id)
      .orderBy(dexSwaps.chain_id)
      .execute();
    expect(rows.length).toBe(2);
    const chains = rows.map((row) => row.chainId).sort();
    expect(chains).toEqual(["arbitrum", "ethereum"]);
  });

  it("Solana transactions: top compute_units_used", async function testSolanaCompute() {
    const db = createE2EDb();
    const rows = await db
      .select({
        signature: solanaTransactions.signature,
        compute: solanaTransactions.compute_units_used,
        success: solanaTransactions.success,
      })
      .from(solanaTransactions)
      .orderBy(ck.desc(solanaTransactions.compute_units_used))
      .limit(5)
      .execute();
    expect(rows.length).toBeGreaterThan(0);
    expect(rows[0]?.success).toBe(true);
  });

  it("NYC taxi: trip distance vs total fare per payment type", async function testNycTaxi() {
    const db = createE2EDb();
    const rows = await db
      .select({
        paymentType: nycTaxiTrips.payment_type,
        trips: fn.count().as("trips"),
        avgFare: fn.avg(nycTaxiTrips.total_amount).as("avg_fare"),
        totalTipPct: ck
          .expr<number>(ckSql`round(sum(${nycTaxiTrips.tip_amount}) / sum(${nycTaxiTrips.fare_amount}) * 100, 2)`, {
            decoder: (value) => Number(value),
            sqlType: "Float64",
          })
          .as("total_tip_pct"),
      })
      .from(nycTaxiTrips)
      .groupBy(nycTaxiTrips.payment_type)
      .orderBy(ck.desc(fn.count()))
      .execute();
    expect(rows.length).toBe(2);
    const credit = rows.find((row) => row.paymentType === "CRE");
    expect(expectPresent(credit, "credit-card row").trips).toBe(2);
  });
});

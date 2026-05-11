// Stock tick store — see ./README.md.
import { ck, ckSql, fn } from "../../ck-orm";
import { stockTickTrades } from "../../schema/scenarios";

export { stockTickTrades };

/** VWAP per symbol = sum(price * size) / sum(size). */
export const buildStockVwapExample = () => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
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
      .groupBy(stockTickTrades.symbol),
});

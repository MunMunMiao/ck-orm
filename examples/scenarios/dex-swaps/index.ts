// Cross-chain DEX swaps — see ./README.md.
import { ck, ckSql } from "../../ck-orm";
import { dexSwaps } from "../../schema/scenarios";

export { dexSwaps };

/** Latest cross-chain price for a pair, via argMax(price, block_ts). */
export const buildDexLatestPriceExample = (tokenIn: string, tokenOut: string) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        chainId: dexSwaps.chain_id,
        latestPrice: ck
          .expr<number>(
            ckSql`argMax(toFloat64(${dexSwaps.amount_out_usd}) / toFloat64(${dexSwaps.amount_in}), ${dexSwaps.block_ts})`,
            { decoder: (value) => Number(value), sqlType: "Float64" },
          )
          .as("latest_price"),
      })
      .from(dexSwaps)
      .where(ck.and(ck.eq(dexSwaps.token_in, tokenIn), ck.eq(dexSwaps.token_out, tokenOut)))
      .groupBy(dexSwaps.chain_id),
});

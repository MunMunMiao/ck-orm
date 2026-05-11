// Solana on-chain transactions — see ./README.md.
import { ck } from "../../ck-orm";
import { solanaTransactions } from "../../schema/scenarios";

export { solanaTransactions };

/** Top transactions by compute_units_used. */
export const buildSolanaTopComputeExample = (limit: number) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        signature: solanaTransactions.signature,
        compute: solanaTransactions.compute_units_used,
        success: solanaTransactions.success,
      })
      .from(solanaTransactions)
      .orderBy(ck.desc(solanaTransactions.compute_units_used))
      .limit(limit),
});

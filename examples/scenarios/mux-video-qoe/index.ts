// Mux video QoE — see ./README.md.
import { ck, ckSql, fn } from "../../ck-orm";
import { muxVideoQoe } from "../../schema/scenarios";

export { muxVideoQoe };

/** Sign-aware rebuffer totals per CDN. */
export const buildMuxCdnQualityExample = () => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        cdn: muxVideoQoe.cdn,
        weightedRebuffers: ck
          .expr<string>(ckSql`sum(${muxVideoQoe.rebuffer_count} * ${muxVideoQoe.sign})`, {
            decoder: (value) => String(value),
            sqlType: "Int64",
          })
          .as("weighted_rebuffers"),
        signSum: fn.sum(muxVideoQoe.sign).as("sign_sum"),
      })
      .from(muxVideoQoe)
      .groupBy(muxVideoQoe.cdn),
});

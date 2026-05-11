// RTB ad tracking — see ./README.md.
import { ck, fn } from "../../ck-orm";
import { rtbAdClicks, rtbAdImpressions } from "../../schema/scenarios";

export { rtbAdClicks, rtbAdImpressions };

/** CTR per campaign — impressions ⟕ clicks. */
export const buildRtbCtrExample = () => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        campaign: rtbAdImpressions.campaign_id,
        impressions: fn.count().as("impressions"),
        clicks: fn.countIf(ck.isNotNull(rtbAdClicks.click_id)).as("clicks"),
      })
      .from(rtbAdImpressions)
      .leftJoin(rtbAdClicks, ck.eq(rtbAdImpressions.impression_id, rtbAdClicks.impression_id))
      .groupBy(rtbAdImpressions.campaign_id),
});

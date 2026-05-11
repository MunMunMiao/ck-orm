// GrowthBook A/B testing — see ./README.md.
import { ck, fn } from "../../ck-orm";
import { growthbookConversions, growthbookExposures } from "../../schema/scenarios";

export { growthbookConversions, growthbookExposures };

/** Conversion rate per variation. */
export const buildGrowthbookConversionExample = (experimentId: string) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        variation: growthbookExposures.variation_id,
        exposed: fn.uniqExact(growthbookExposures.user_id).as("exposed"),
        converted: fn.uniqExact(growthbookConversions.user_id).as("converted"),
      })
      .from(growthbookExposures)
      .leftJoin(growthbookConversions, ck.eq(growthbookExposures.user_id, growthbookConversions.user_id))
      .where(ck.eq(growthbookExposures.experiment_id, experimentId))
      .groupBy(growthbookExposures.variation_id),
});

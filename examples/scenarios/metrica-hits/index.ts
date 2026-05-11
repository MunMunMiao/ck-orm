// Yandex.Metrica hits — see ./README.md.
import { ck, fn } from "../../ck-orm";
import { metricaHits } from "../../schema/scenarios";

export { metricaHits };

/** Top UTM campaigns by hits + uniqueUsers. */
export const buildMetricaTopCampaignsExample = (counterId: number) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        campaign: metricaHits.utm_campaign,
        hits: fn.count().as("hits"),
        uniqueUsers: fn.uniqExact(metricaHits.user_id).as("unique_users"),
      })
      .from(metricaHits)
      .where(ck.and(ck.eq(metricaHits.counter_id, counterId), ck.ne(metricaHits.utm_campaign, "")))
      .groupBy(metricaHits.utm_campaign)
      .orderBy(ck.desc(fn.count())),
});

// Snowplow canonical events — see ./README.md.
import { fn } from "../../ck-orm";
import { snowplowEvents } from "../../schema/scenarios";

export { snowplowEvents };

/** Traffic source breakdown by referrer medium. */
export const buildSnowplowTrafficSourcesExample = () => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        medium: snowplowEvents.refr_medium,
        pageViews: fn.count().as("page_views"),
        uniqueVisitors: fn.uniqExact(snowplowEvents.domain_userid).as("unique_visitors"),
      })
      .from(snowplowEvents)
      .groupBy(snowplowEvents.refr_medium),
});

// PostHog product analytics events — see ./README.md.
import { ck, ckSql } from "../../ck-orm";
import { posthogEvents } from "../../schema/scenarios";

export { posthogEvents };

/** `windowFunnel(86400)(signup, checkout, payment)` over one user's events. */
export const buildPosthogFunnelExample = (teamId: number, distinctId: string) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        steps: ck
          .expr<number>(
            ckSql`windowFunnel(86400)(toDateTime(${posthogEvents.timestamp}),
              ${posthogEvents.event} = 'user_signed_up',
              ${posthogEvents.event} = 'checkout_started',
              ${posthogEvents.event} = 'payment_succeeded')`,
            { decoder: (value) => Number(value), sqlType: "UInt8" },
          )
          .as("steps"),
      })
      .from(posthogEvents)
      .where(ck.and(ck.eq(posthogEvents.team_id, teamId), ck.eq(posthogEvents.distinct_id, distinctId)))
      .groupBy(posthogEvents.distinct_id),
});

// OpenMeter SaaS usage metering — see ./README.md.
import { fn } from "../../ck-orm";
import { meterEvents } from "../../schema/scenarios";

export { meterEvents };

/** Usage roll-up per customer + meter — the billing-page invoice preview. */
export const buildSaasUsageRollupExample = () => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        customer: meterEvents.customer_id,
        meter: meterEvents.meter_slug,
        usage: fn.sum(meterEvents.value).as("usage"),
        events: fn.count().as("events"),
      })
      .from(meterEvents)
      .groupBy(meterEvents.customer_id, meterEvents.meter_slug),
});

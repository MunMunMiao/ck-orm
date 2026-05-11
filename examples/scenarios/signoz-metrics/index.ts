// SigNoz metric samples — see ./README.md.
import { ck, fn } from "../../ck-orm";
import { signozMetricsSamples } from "../../schema/scenarios";

export { signozMetricsSamples };

/** PromQL `avg() by (fingerprint)` — average value per label set. */
export const buildSignozMetricAverageByFingerprint = (metricName: string) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        fingerprint: signozMetricsSamples.fingerprint,
        samples: fn.count().as("samples"),
        avgValue: fn.avg(signozMetricsSamples.value).as("avg_value"),
      })
      .from(signozMetricsSamples)
      .where(ck.eq(signozMetricsSamples.metric_name, metricName))
      .groupBy(signozMetricsSamples.fingerprint),
});

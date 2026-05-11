// ML feature store — see ./README.md.
import { ck, ckSql, fn } from "../../ck-orm";
import { mlUserEvents } from "../../schema/scenarios";

export { mlUserEvents };

/** Per-domain unique IPs + bounce rate — the inference-time feature vector. */
export const buildMlFeatureWindowExample = () => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        domain: mlUserEvents.domain,
        uniqueIps: fn.uniqExact(mlUserEvents.client_ip).as("unique_ips"),
        bounceRate: ck
          .expr<number>(ckSql`avg(${mlUserEvents.is_bounce})`, {
            decoder: (value) => Number(value),
            sqlType: "Float64",
          })
          .as("bounce_rate"),
        events: fn.count().as("events"),
      })
      .from(mlUserEvents)
      .groupBy(mlUserEvents.domain),
});

// EMQ MQTT industrial IoT telemetry — see ./README.md.
import { ck, fn } from "../../ck-orm";
import { iotTelemetry } from "../../schema/scenarios";

export { iotTelemetry };

/** Devices that crossed a temperature threshold in the time window. */
export const buildIotAnomaliesExample = (thresholdC: number) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        deviceId: iotTelemetry.device_id,
        anomalies: fn.count().as("anomalies"),
        peakTemp: fn.max(iotTelemetry.value_float).as("peak_temp"),
      })
      .from(iotTelemetry)
      .where(ck.and(ck.eq(iotTelemetry.metric_name, "temperature"), ck.gt(iotTelemetry.value_float, thresholdC)))
      .groupBy(iotTelemetry.device_id),
});

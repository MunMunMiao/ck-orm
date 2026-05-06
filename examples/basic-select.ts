import { ck, ckAlias, fn } from "./ck-orm";
import { createProbeDb } from "./probe-client";
import { probeTelemetry } from "./schema/probe";

export const buildSignalLeaderboardExample = () => {
  const probeDb = createProbeDb();
  const telemetry = ckAlias(probeTelemetry, "pt");
  const totalSignalStrength = fn.sum(telemetry.signalStrength).as("total_signal_strength");

  const query = probeDb
    .select({
      probeId: telemetry.probeId,
      totalSignalStrength,
      sampleCount: fn.count().as("sample_count"),
      missionCount: fn.uniqExact(telemetry.missionId).as("mission_count"),
    })
    .from(telemetry)
    .where(ck.and(ck.isNull(telemetry.deletedAt), ck.inArray(telemetry.status, [1, 2])))
    .groupBy(telemetry.probeId)
    .orderBy(ck.desc(totalSignalStrength))
    .limit(20)
    .final();

  return {
    query,
  };
};

export const runSignalLeaderboardExample = async () => {
  const { query } = buildSignalLeaderboardExample();
  return query.execute();
};

import { ck, fn } from "./ck-orm";
import { createProbeDb } from "./probe-client";
import { probeTelemetry } from "./schema/probe";

export const buildSignalSummaryWithLatestSampleExample = () => {
  const probeDb = createProbeDb();

  const rankedProbes = probeDb.$with("ranked_probes").as(
    probeDb
      .select({
        probeId: probeTelemetry.probeId,
        totalSignalStrength: fn.sum(probeTelemetry.signalStrength).as("total_signal_strength"),
      })
      .from(probeTelemetry)
      .groupBy(probeTelemetry.probeId),
  );

  const latestSample = probeDb
    .select({
      probeId: probeTelemetry.probeId,
      createdAt: probeTelemetry.createdAt,
    })
    .from(probeTelemetry)
    .orderBy(ck.desc(probeTelemetry.createdAt))
    .limit(10)
    .as("latest_sample");

  const query = probeDb
    .with(rankedProbes)
    .select({
      probeId: rankedProbes.probeId,
      totalSignalStrength: rankedProbes.totalSignalStrength,
      latestCreatedAt: latestSample.createdAt,
    })
    .from(rankedProbes)
    .leftJoin(latestSample, ck.eq(rankedProbes.probeId, latestSample.probeId));

  return {
    query,
  };
};

export const runSignalSummaryWithLatestSampleExample = async () => {
  const { query } = buildSignalSummaryWithLatestSampleExample();
  return query.execute();
};

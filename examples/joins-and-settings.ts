import { ck } from "./ck-orm";
import { createProbeDb } from "./probe-client";
import { probeSignalRollup, probeTelemetry } from "./schema/probe";

export const buildDefaultLeftJoinExample = () => {
  const probeDb = createProbeDb();
  const query = probeDb
    .select()
    .from(probeSignalRollup)
    .leftJoin(probeTelemetry, ck.eq(probeSignalRollup.probeId, probeTelemetry.probeId));

  return {
    query,
  };
};

export const runDefaultLeftJoinExample = async () => {
  const { query } = buildDefaultLeftJoinExample();
  return query.execute();
};

export const runClickHouseDefaultJoinExample = async () => {
  const probeDb = createProbeDb();
  const rawDefaultDb = probeDb.withSettings({
    join_use_nulls: 0,
  });

  return rawDefaultDb
    .select()
    .from(probeSignalRollup)
    .leftJoin(probeTelemetry, ck.eq(probeSignalRollup.probeId, probeTelemetry.probeId))
    .execute();
};

export const buildExplicitSelectJoinExample = () => {
  const probeDb = createProbeDb();
  const query = probeDb
    .select({
      probeId: probeSignalRollup.probeId,
      bucketStart: probeSignalRollup.bucketStart,
      sampleId: probeTelemetry.sampleId,
      signalStrength: probeTelemetry.signalStrength,
    })
    .from(probeSignalRollup)
    .leftJoin(probeTelemetry, ck.eq(probeSignalRollup.probeId, probeTelemetry.probeId));

  return {
    query,
  };
};

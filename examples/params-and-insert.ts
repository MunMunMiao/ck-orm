import { ck, ckSql } from "./ck-orm";
import { createProbeDb } from "./probe-client";
import { probeSignalRollup, probeTelemetry } from "./schema/probe";

export const buildDirectValueQueryExample = () => {
  const probeDb = createProbeDb();

  const query = probeDb
    .select({
      probeId: probeTelemetry.probeId,
      signalStrength: probeTelemetry.signalStrength,
    })
    .from(probeTelemetry)
    .where(ck.eq(probeTelemetry.probeId, "probe_alpha"))
    .limit(10);

  return {
    query,
  };
};

export const runDirectValueQueryExample = async () => {
  const { query } = buildDirectValueQueryExample();
  return query.execute();
};

export const runRawQueryParamsExample = async () => {
  const probeDb = createProbeDb();

  return probeDb.execute(
    ckSql`select probe_id, signal_strength from probe_telemetry where probe_id = {probe_id:String} limit {limit:Int64}`,
    {
      query_params: {
        probe_id: "probe_alpha",
        limit: 10,
      },
    },
  );
};

export const buildInsertExample = () => {
  const probeDb = createProbeDb();
  const insert = probeDb.insert(probeSignalRollup).values({
    probeId: "probe_alpha",
    bucketStart: new Date("2026-04-24T00:00:00.000Z"),
    bucketDay: new Date("2026-04-24T00:00:00.000Z"),
    totalSignalStrength: "142.50000",
    sampleCount: "12",
    updatedAt: new Date("2026-04-24T00:05:00.000Z"),
    ingestVersion: "1",
  });

  return {
    insert,
  };
};

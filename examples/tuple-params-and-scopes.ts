import { ck, ckSql, fn } from "./ck-orm";
import { createProbeDb } from "./probe-client";
import { probeTelemetry } from "./schema/probe";

export const buildTupleScopeQueryExample = () => {
  const probeDb = createProbeDb();
  const sampleIds = [900001, 900002, 900003];
  const probeIds = ["probe_alpha", "probe_beta", "probe_gamma"];

  const targetSamples = probeDb.$with("target_samples").as(
    probeDb.select({
      pair: fn.arrayJoin(fn.arrayZip(sampleIds, probeIds)).as("pair"),
    }),
  );

  const targetTelemetry = probeDb.$with("target_telemetry").as(
    probeDb
      .with(targetSamples)
      .select({
        sampleId: fn.tupleElement<number>(targetSamples.pair, 1).as("sample_id"),
        probeId: fn.tupleElement<string>(targetSamples.pair, 2).as("probe_id"),
      })
      .from(targetSamples),
  );

  const scopedSamples = probeDb
    .select({
      pair: fn.tuple(targetTelemetry.sampleId, targetTelemetry.probeId).as("pair"),
    })
    .from(targetTelemetry)
    .as("scoped_samples");

  const query = probeDb
    .with(targetSamples, targetTelemetry)
    .select({
      sampleId: probeTelemetry.sampleId,
      probeId: probeTelemetry.probeId,
      signalStrength: probeTelemetry.signalStrength,
    })
    .from(probeTelemetry)
    .where(ck.inArray(fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId), scopedSamples));

  return {
    query,
  };
};

export const runTupleQueryParamExample = async () => {
  const probeDb = createProbeDb();

  return probeDb.execute(
    ckSql`
      SELECT
        {sample_key:Tuple(Int64, String)} AS sample_key,
        {windows:Array(Tuple(DateTime64(3, 'UTC'), DateTime64(3, 'UTC')))} AS windows
    `,
    {
      query_params: {
        sample_key: [900001, "probe_alpha"],
        windows: [
          [new Date("2026-06-15T00:00:00.000Z"), new Date("2026-06-16T00:00:00.000Z")],
          [new Date("2026-06-17T00:00:00.000Z"), new Date("2026-06-18T00:00:00.000Z")],
        ],
      },
    },
  );
};

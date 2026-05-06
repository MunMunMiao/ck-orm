import { ck, ckType, fn } from "./ck-orm";
import { createProbeDb } from "./probe-client";
import { probeTelemetry } from "./schema/probe";

export const buildJsonAlertFilterExample = () => {
  const probeDb = createProbeDb();
  const alertTags = fn.jsonExtract(probeTelemetry.metadata, ckType.array(ckType.string()), "alerts");
  const riskScore = fn.jsonExtract(probeTelemetry.metadata, ckType.nullable(ckType.float64()), "risk", "score");

  const query = probeDb
    .select({
      probeId: probeTelemetry.probeId,
      sampleId: probeTelemetry.sampleId,
      alertTags: alertTags.as("alert_tags"),
      riskScore: riskScore.as("risk_score"),
    })
    .from(probeTelemetry)
    .where(ck.isNull(probeTelemetry.deletedAt), ck.hasAny(alertTags, ["thermal", "battery"]), ck.gte(riskScore, 80))
    .limit(100);

  return {
    query,
  };
};

export const buildArrayHelperProjectionExample = () => {
  const probeDb = createProbeDb();
  const alertTags = fn.jsonExtract(probeTelemetry.metadata, ckType.array(ckType.string()), "alerts");
  const normalizedTags = fn.arrayConcat<string>(probeTelemetry.tags, fn.array("telemetry")).as("normalized_tags");

  const query = probeDb
    .select({
      probeId: probeTelemetry.probeId,
      firstTag: fn.arrayElement<string>(probeTelemetry.tags, 1).as("first_tag"),
      maybeSecondTag: fn.arrayElementOrNull<string>(probeTelemetry.tags, 2).as("maybe_second_tag"),
      topTwoAlerts: fn.arraySlice<string>(alertTags, 1, 2).as("top_two_alerts"),
      flattenedAlerts: fn.arrayFlatten<string>(fn.array(alertTags)).as("flattened_alerts"),
      matchingAlerts: fn.arrayIntersect<string>(alertTags, ["thermal", "battery", "signal"]).as("matching_alerts"),
      normalizedTags,
      tagPosition: fn.indexOf(probeTelemetry.tags, "field").as("tag_position"),
      tagCount: fn.length(probeTelemetry.tags).as("tag_count"),
      hasTags: fn.notEmpty(probeTelemetry.tags).as("has_tags"),
    })
    .from(probeTelemetry)
    .where(ck.hasAll(normalizedTags, ["telemetry"]));

  return {
    query,
  };
};

export const buildArrayZipTupleElementScopeExample = () => {
  const probeDb = createProbeDb();
  const sampleIds = [900001, 900002, 900003];
  const probeIds = ["probe_alpha", "probe_beta", "probe_gamma"];

  const targetPairs = probeDb.$with("target_pairs").as(
    probeDb.select({
      pair: fn.arrayJoin(fn.arrayZip(sampleIds, probeIds)).as("pair"),
    }),
  );

  const targetTelemetry = probeDb.$with("target_telemetry").as(
    probeDb
      .with(targetPairs)
      .select({
        sampleId: fn.tupleElement<number>(targetPairs.pair, 1).as("sample_id"),
        probeId: fn.tupleElement<string>(targetPairs.pair, 2).as("probe_id"),
      })
      .from(targetPairs),
  );

  const scopedSamples = probeDb
    .select({
      pair: fn.tuple(targetTelemetry.sampleId, targetTelemetry.probeId).as("pair"),
    })
    .from(targetTelemetry);

  const query = probeDb
    .with(targetPairs, targetTelemetry)
    .select({
      sampleId: probeTelemetry.sampleId,
      probeId: probeTelemetry.probeId,
      signalStrength: probeTelemetry.signalStrength,
    })
    .from(probeTelemetry)
    .where(ck.inArray(fn.tuple(probeTelemetry.sampleId, probeTelemetry.probeId), scopedSamples.as("scoped_samples")))
    .orderBy(ck.desc(probeTelemetry.createdAt));

  return {
    query,
  };
};

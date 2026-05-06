import { ck, type Predicate } from "./ck-orm";
import { createProbeDb } from "./probe-client";
import { probeTelemetry } from "./schema/probe";

export type ProbeFilterInput = {
  readonly probeId?: string;
  readonly missionIds?: readonly number[];
  readonly minSignalStrength?: string;
  readonly status?: number;
  readonly includeDeleted?: boolean;
};

export const buildOptionalProbeFiltersExample = (input: ProbeFilterInput = {}) => {
  const probeDb = createProbeDb();
  const predicates: Predicate[] = [];

  if (input.probeId !== undefined) {
    predicates.push(ck.eq(probeTelemetry.probeId, input.probeId));
  }

  if (input.missionIds !== undefined && input.missionIds.length > 0) {
    predicates.push(ck.inArray(probeTelemetry.missionId, input.missionIds));
  }

  if (input.minSignalStrength !== undefined) {
    predicates.push(ck.gte(probeTelemetry.signalStrength, input.minSignalStrength));
  }

  if (input.status !== undefined) {
    predicates.push(ck.eq(probeTelemetry.status, input.status));
  }

  if (input.includeDeleted !== true) {
    predicates.push(ck.isNull(probeTelemetry.deletedAt));
  }

  const query = probeDb
    .select({
      probeId: probeTelemetry.probeId,
      sampleId: probeTelemetry.sampleId,
      signalStrength: probeTelemetry.signalStrength,
      deletedAt: probeTelemetry.deletedAt,
    })
    .from(probeTelemetry)
    .where(...predicates)
    .orderBy(ck.desc(probeTelemetry.createdAt))
    .limit(100);

  return {
    query,
  };
};

export const buildNullAwareStatusExample = () => {
  const probeDb = createProbeDb();

  const query = probeDb
    .select({
      probeId: probeTelemetry.probeId,
      sampleId: probeTelemetry.sampleId,
      deletedAt: probeTelemetry.deletedAt,
    })
    .from(probeTelemetry)
    .where(
      ck.or(
        ck.isNull(probeTelemetry.deletedAt),
        ck.and(ck.isNotNull(probeTelemetry.deletedAt), ck.eq(probeTelemetry.status, 9)),
      ),
    );

  return {
    query,
  };
};

export const runOptionalProbeFiltersExample = async () => {
  const { query } = buildOptionalProbeFiltersExample({
    includeDeleted: false,
    minSignalStrength: "100.00000",
    missionIds: [10, 20, 30],
  });

  return query.execute();
};

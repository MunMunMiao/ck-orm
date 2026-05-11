// OpenTelemetry Collector trace table — see ./README.md.
import { ck } from "../../ck-orm";
import { otelTraces } from "../../schema/scenarios";

export { otelTraces };

/** Stitch every span belonging to a trace, ordered by timestamp (flamegraph). */
export const buildOtelTraceById = (traceId: string) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db.select().from(otelTraces).where(ck.eq(otelTraces.trace_id, traceId)).orderBy(otelTraces.timestamp),
});

import { ck, fn } from "./ck-orm";
import { createProbeDb } from "./probe-client";
import { probeTelemetry } from "./schema/probe";

// Demonstrates the three "bare builder" patterns introduced when `.as(name)`
// became optional for subqueries: variable-binding, inline callback joins,
// and inline `inArray()`.
//
// See README "Subqueries and CTEs › Style guide" for when each pattern is
// recommended.

// Pattern A — variable binding.
// The inner SELECT isn't given an explicit alias; the framework assigns a
// per-compile auto alias (`__sub_1`, `__sub_2`, …) at SQL render time. The
// builder still exposes typed column refs so `signalTotals.totalSignalStrength`
// works in the outer projection / join condition.
export const buildBareBuilderVariableBindExample = () => {
  const probeDb = createProbeDb();

  const signalTotals = probeDb
    .select({
      probeId: probeTelemetry.probeId,
      totalSignalStrength: fn.sum(probeTelemetry.signalStrength).as("total_signal_strength"),
    })
    .from(probeTelemetry)
    .groupBy(probeTelemetry.probeId);

  const query = probeDb
    .select({
      probeId: signalTotals.probeId,
      totalSignalStrength: signalTotals.totalSignalStrength,
    })
    .from(signalTotals)
    .orderBy(ck.desc(signalTotals.totalSignalStrength))
    .limit(10);

  return { query };
};

// Pattern B — inline callback join.
// When the subquery is only used in one join and you don't want to bind a
// variable, the `(joined) => Predicate` overload gives you a typed handle to
// the inline builder so the join condition can reference its columns.
export const buildBareBuilderCallbackJoinExample = () => {
  const probeDb = createProbeDb();

  const query = probeDb
    .select({
      probeId: probeTelemetry.probeId,
      latestCreatedAt: probeTelemetry.createdAt,
    })
    .from(probeTelemetry)
    .innerJoin(
      probeDb
        .select({
          peerProbeId: probeTelemetry.probeId,
          peerCount: fn.count().as("peer_count"),
        })
        .from(probeTelemetry)
        .groupBy(probeTelemetry.probeId),
      (joined) => ck.eq(probeTelemetry.probeId, joined.peerProbeId),
    );

  return { query };
};

// Pattern C — bare builder inside `inArray()`.
// `IN (SELECT ...)` reads the subquery as a value-set; the outer query
// never names its alias. Auto-aliasing is the natural fit — no `.as(name)`
// would have been used anyway.
export const buildBareBuilderInArrayExample = () => {
  const probeDb = createProbeDb();

  const query = probeDb
    .select({
      probeId: probeTelemetry.probeId,
      signalStrength: probeTelemetry.signalStrength,
    })
    .from(probeTelemetry)
    .where(
      ck.inArray(
        probeTelemetry.probeId,
        probeDb
          .select({ activeProbeId: probeTelemetry.probeId })
          .from(probeTelemetry)
          .where(ck.isNotNull(probeTelemetry.deletedAt)),
      ),
    );

  return { query };
};

export const runBareBuilderVariableBindExample = async () => {
  const { query } = buildBareBuilderVariableBindExample();
  return query.execute();
};

export const runBareBuilderCallbackJoinExample = async () => {
  const { query } = buildBareBuilderCallbackJoinExample();
  return query.execute();
};

export const runBareBuilderInArrayExample = async () => {
  const { query } = buildBareBuilderInArrayExample();
  return query.execute();
};

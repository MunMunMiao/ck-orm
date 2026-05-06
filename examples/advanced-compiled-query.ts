import { type CompiledQuery, ck, ckSql, fn } from "./ck-orm";
import { createProbeDb } from "./probe-client";
import { probeSignalRollup } from "./schema/probe";

const oneQuery = {
  kind: "compiled-query",
  mode: "query",
  statement: "SELECT 1 AS one",
  params: {},
  selection: [
    {
      key: "one",
      sqlAlias: "one",
      path: ["one"],
      decoder(value: unknown) {
        return Number(value);
      },
    },
  ],
  metadata: {
    tags: ["example", "compiled"],
  },
} satisfies CompiledQuery<{ one: number }>;

export const runExecuteCompiledExample = async () => {
  const probeDb = createProbeDb();
  const sessionId = ck.createSessionId();

  return probeDb.executeCompiled<{ one: number }>(oneQuery, {
    session_id: sessionId,
  });
};

export const runIteratorCompiledExample = async () => {
  const probeDb = createProbeDb();
  const rows: Array<{ one: number }> = [];

  for await (const row of probeDb.iteratorCompiled<{ one: number }>(oneQuery)) {
    rows.push(row);
  }

  return rows;
};

export const decodeCompiledRowExample = () => {
  return ck.decodeRow<{ one: number }>({ one: "1" }, oneQuery.selection);
};

/**
 * Decimal precision example: every numeric value travels as `string` end to end.
 *
 * - `fn.sum(decimalColumn)` auto-injects `CAST(... AS Decimal(38, 5))`.
 * - `ckSql.decimal(...)` wraps a hand-written expression into a precision cast.
 * - The decoded row keeps the value as `string`, ready for `decimal.js` on the consumer side.
 */
export const runDecimalPrecisionAggregate = async () => {
  const probeDb = createProbeDb();

  const summary = await probeDb
    .select({
      probeId: probeSignalRollup.probeId,
      totalSignalStrength: fn.sum(probeSignalRollup.totalSignalStrength).as("total_signal_strength"),
      avgSignalPerSample: ckSql
        .decimal(
          ckSql`sum(${probeSignalRollup.totalSignalStrength}) / nullIf(sum(${probeSignalRollup.sampleCount}), 0)`,
          20,
          5,
        )
        .as("avg_signal_per_sample"),
    })
    .from(probeSignalRollup)
    .groupBy(probeSignalRollup.probeId)
    .execute();

  return summary;
};

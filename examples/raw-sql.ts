import { ckSql, fn } from "./ck-orm";
import { createProbeDb } from "./probe-client";
import { probeTelemetry } from "./schema/probe";

export const buildRawExpressionExample = () => {
  const probeDb = createProbeDb();

  const query = probeDb
    .select({
      probeId: probeTelemetry.probeId,
      month: fn.toStartOfMonth(probeTelemetry.createdAt).as("month"),
      createdAtText: fn.toString(probeTelemetry.createdAt).as("created_at_text"),
    })
    .from(probeTelemetry);

  return {
    query,
  };
};

export const runRawQueryExample = async () => {
  const probeDb = createProbeDb();
  const minStatus = 1;

  return probeDb.execute(ckSql`
    select
      ${probeTelemetry.probeId},
      ${fn.sum(probeTelemetry.signalStrength)} as total_signal_strength
    from ${probeTelemetry}
    where ${probeTelemetry.status} >= ${minStatus}
    group by ${probeTelemetry.probeId}
  `);
};

export const runTaggedTemplateRawQueryExample = async () => {
  const probeDb = createProbeDb();
  return probeDb.execute(ckSql`SELECT 1 AS one`);
};

export const runIdentifierQueryExample = async () => {
  const probeDb = createProbeDb();
  const selectedColumns = ckSql.join([ckSql.identifier("probe_id"), ckSql.identifier("signal_strength")], ", ");

  return probeDb.execute(
    ckSql`
      SELECT ${selectedColumns}
      FROM ${ckSql.identifier("probe_telemetry")}
      WHERE ${ckSql.identifier("status")} = ${1}
      LIMIT ${10}
    `,
  );
};

export const buildTableFunctionExample = () => {
  const probeDb = createProbeDb();
  const numbers = fn.table.call("numbers", 10).as("n");

  const query = probeDb
    .select({
      total: fn.count(),
    })
    .from(numbers);

  return {
    query,
  };
};

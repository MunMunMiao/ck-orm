import { ckSql, ckTable, ckType } from "./ck-orm";
import { createProbeDb } from "./probe-client";

export const exportSignalSummaryForLargeProbeScope = async (
  probeIds: string[],
  onRow: (row: Record<string, unknown>) => Promise<void> | void,
) => {
  const probeDb = createProbeDb();
  const tmpProbeScope = ckTable("tmp_probe_scope", {
    probe_id: ckType.string(),
  });

  return probeDb.runInSession(async (sessionDb) => {
    // Temporary tables stay scoped to this Session and disappear after cleanup.
    await sessionDb.createTemporaryTable(tmpProbeScope);
    await sessionDb.insertJsonEachRow(
      tmpProbeScope,
      probeIds.map((probe_id) => ({ probe_id })),
      {
        query_id: "probe_scope_seed",
      },
    );

    const scopeSummary = await sessionDb.execute(ckSql`
      select
        count() as scoped_probe_count
      from tmp_probe_scope
    `);

    for await (const row of sessionDb.stream(
      ckSql`
        SELECT
          probe_id,
          sum(signal_strength) AS total_signal_strength,
          count() AS total_rows
        FROM probe_telemetry
        WHERE probe_id IN (SELECT probe_id FROM tmp_probe_scope)
        GROUP BY probe_id
        ORDER BY total_signal_strength DESC
      `,
      {
        format: "JSONEachRow",
        query_id: "probe_scope_export",
      },
    )) {
      await onRow(row);
    }

    return scopeSummary;
  });
};

import { ckSql, ckTable, ckType } from "./ck-orm";
import { createProbeDb } from "./probe-client";

export const runSessionTempTableExample = async () => {
  const probeDb = createProbeDb();
  const tmpScope = ckTable("tmp_scope", {
    probe_id: ckType.string(),
  });

  return probeDb.runInSession(async (sessionDb) => {
    // Temporary tables live only inside this Session and are cleaned up automatically.
    await sessionDb.createTemporaryTable(tmpScope);
    await sessionDb.insertJsonEachRow(tmpScope, [{ probe_id: "probe_alpha" }, { probe_id: "probe_beta" }]);

    return sessionDb.execute(
      ckSql`
        SELECT probe_id
        FROM probe_telemetry
        WHERE probe_id IN (SELECT probe_id FROM tmp_scope)
      `,
    );
  });
};

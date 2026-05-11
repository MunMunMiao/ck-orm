// SigNoz distributed traces — see ./README.md.
import { fn } from "../../ck-orm";
import { signozTraces } from "../../schema/scenarios";

export { signozTraces };

/** Error / total counts per service — the APM "error rate" panel. */
export const buildSignozErrorRateByService = () => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        service: signozTraces.service_name,
        errors: fn.countIf(signozTraces.has_error).as("errors"),
        total: fn.count().as("total"),
      })
      .from(signozTraces)
      .groupBy(signozTraces.service_name),
});

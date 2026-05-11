// ClickHouse internal 19 PiB logging platform — see ./README.md.
//
// The shared schema lives in `examples/schema/scenarios.ts` so the e2e suite
// can seed it once and exercise every example against a real ClickHouse.
import { ck, fn } from "../../ck-orm";
import { clickhouseLogPlatform } from "../../schema/scenarios";

export { clickhouseLogPlatform };

/** Top services by error count — "what is on fire right now?" */
export const buildClickhouseLogErrorsByService = () => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        service: clickhouseLogPlatform.service_name,
        errorCount: fn.count().as("error_count"),
      })
      .from(clickhouseLogPlatform)
      .where(ck.inArray(clickhouseLogPlatform.severity_text, ["ERROR", "CRITICAL", "FATAL"]))
      .groupBy(clickhouseLogPlatform.service_name)
      .orderBy(ck.desc(fn.count())),
});

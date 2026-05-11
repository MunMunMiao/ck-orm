// Uber schema-agnostic logs — see ./README.md.
import { ck } from "../../ck-orm";
import { uberSchemaAgnosticLogs } from "../../schema/scenarios";

export { uberSchemaAgnosticLogs };

/** Point-lookup by the materialized `request_id` column. */
export const buildUberLookupByRequestId = (requestId: string) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db.select().from(uberSchemaAgnosticLogs).where(ck.eq(uberSchemaAgnosticLogs.request_id, requestId)),
});

// ClickHouse 24.x+ NewJSON events — see ./README.md.
import { ck, ckType } from "../../ck-orm";
import { newjsonEvents } from "../../schema/scenarios";

export { newjsonEvents };

/**
 * Selects typed paths off the JSON column and filters by user id in WHERE.
 * `payload.user_id` is declared as `ckType.uint64()` in `typeHints`, so the
 * decoded TypeScript value is a lossless string.
 */
export const buildNewjsonRevenueQuery = (userId: string) => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        id: newjsonEvents.id,
        userId: newjsonEvents.payload.path("user_id"),
        action: newjsonEvents.payload.path("action"),
        sessionTier: newjsonEvents.payload.path("session.tier"),
        // revenue is a dynamic path — castPath forces the decode through
        // ckType.float64() for callers that know the value is numeric.
        revenue: newjsonEvents.payload.castPath("revenue", ckType.float64()),
      })
      .from(newjsonEvents)
      .where(ck.eq(newjsonEvents.payload.path("user_id"), userId)),
});

/**
 * Pulls the `session` sub-object as a JSON value preserving its dynamic
 * structure — useful when downstream code wants the whole nested object
 * rather than a handful of paths.
 */
export const buildNewjsonSubobjectQuery = () => ({
  select: (db: ReturnType<typeof import("../../ck-orm").clickhouseClient>) =>
    db
      .select({
        id: newjsonEvents.id,
        session: newjsonEvents.payload.subobject("session"),
      })
      .from(newjsonEvents),
});

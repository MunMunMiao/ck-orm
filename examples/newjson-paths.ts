// End-to-end demo of the ClickHouse 24.x+ `JSON` data type ("NewJSON") API:
// parameterized DDL with typeHints / SKIP rules, the `path()` / `castPath()`
// / `subobject()` / `merged()` / `arrayPath()` column methods, the
// `fn.json*` namespace for dynamic-path call sites, the `$type<{ select,
// insert }>` divergence for typeHints whose write input is wider than the
// read output, and `$validator()` integration.
//
// See README "JSON column type" section for the conceptual overview.

import { ck, ckSql, ckTable, ckType, fn, type StandardSchemaV1 } from "./ck-orm";
import { createProbeDb } from "./probe-client";

// ---------------------------------------------------------------------------
// Schema
//
// `payload` mixes (a) **typed** sub-paths whose ClickHouse storage type is
// fixed at DDL time and (b) **dynamic** sub-paths whose type is inferred on
// insert. `$type<{ select, insert }>` reflects the asymmetric `UInt64`
// decode contract: the database hands back a lossless string, but the
// insert API accepts string / number / bigint.
// ---------------------------------------------------------------------------

type EventPayloadSelect = {
  user_id: string;
  action: string;
  revenue?: number;
  session: { id: string; tier: number };
};

type EventPayloadInsert = {
  // uint64 typeHint widens the insert input — `payload.user_id` accepts
  // string / number / bigint and the encoder normalizes to a string.
  user_id: string | number | bigint;
  action: string;
  revenue?: number;
  session: { id: string; tier: number };
};

export const userEvents = ckTable(
  "newjson_user_events",
  {
    id: ckType.uint64(),
    receivedAt: ckType.dateTime64("received_at", { precision: 3, timezone: "UTC" }),
    // The headline column — every NewJSON DDL knob in one place.
    payload: ckType
      .json("payload", {
        // Cap subcolumn explosion to 256; everything else lands in shared data.
        maxDynamicPaths: 256,
        maxDynamicTypes: 16,
        // typed-path subcolumns. `typeHints` keys are constrained by
        // `Paths<T>`, so a typo here fails at compile time once `$type`
        // narrows the shape below.
        typeHints: {
          user_id: ckType.uint64(),
          "session.tier": ckType.uint8(),
        },
        // Server-side path drop — `debug` is never persisted, even when the
        // ingestor accidentally includes it.
        skip: ["debug"],
        skipRegexp: ["^_tmp_"],
      })
      // Diverge select / insert: SELECT decodes uint64 → string, INSERT
      // accepts string | number | bigint and ck-orm normalizes.
      .$type<{ select: EventPayloadSelect; insert: EventPayloadInsert }>(),
    // A second JSON column with a DEFAULT — `$inferInsert` marks it optional.
    extras: ckType.json<{ note: string }>("extras").default(ckSql`'{}'`),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.receivedAt, table.id],
  }),
);

// $inferInsert / $inferSelect now read cleanly in callers:
//   $inferInsert: {
//     id: string;
//     receivedAt: Date;
//     payload: { user_id: string|number|bigint; action: string; ... };
//     extras?: { note: string };          // optional (DEFAULT '{}')
//   }
//   $inferSelect: {
//     id: string;
//     receivedAt: Date;
//     payload: { user_id: string; action: string; ... };
//     extras: { note: string };
//   }
export type UserEventsInsert = typeof userEvents.$inferInsert;
export type UserEventsRow = typeof userEvents.$inferSelect;

// ---------------------------------------------------------------------------
// Optional: `$validator(schema)` for deep shape validation on every row.
// We attach it to a separate, narrower fixture below so the rest of the demo
// keeps its native `path()` / `castPath()` ergonomics on `userEvents.payload`.
// ---------------------------------------------------------------------------

type RevenuePayload = { user_id: string; revenue: number };

const revenueValidator = {
  "~standard": {
    version: 1,
    vendor: "ck-orm-newjson-example",
    validate(value: unknown) {
      if (
        typeof value !== "object" ||
        value === null ||
        Array.isArray(value) ||
        typeof (value as { user_id?: unknown }).user_id !== "string" ||
        typeof (value as { revenue?: unknown }).revenue !== "number" ||
        !Number.isFinite((value as { revenue: number }).revenue) ||
        (value as { revenue: number }).revenue < 0
      ) {
        return {
          issues: [{ message: "payload must include string user_id and finite non-negative revenue" }],
        };
      }
      return { value: value as RevenuePayload };
    },
  },
} satisfies StandardSchemaV1<RevenuePayload, RevenuePayload>;

export const revenueEvents = ckTable(
  "newjson_revenue_events",
  {
    id: ckType.uint64(),
    // The validator runs once per row on both encode and decode. Failures
    // raise `client_validation` / `decode` errors with the schema's issue
    // messages mapped into the column scope.
    payload: ckType.json<RevenuePayload>("payload").$validator(revenueValidator),
  },
  (table) => ({
    engine: "MergeTree",
    orderBy: [table.id],
  }),
);

// ---------------------------------------------------------------------------
// 1. SELECT typed sub-paths through the column's .path() / .castPath()
// methods. Return types come from `typeHints` when the path is typed,
// otherwise from `PathValue<T, P>`.
// ---------------------------------------------------------------------------

export const buildNewjsonTypedPathQuery = (minTier: number) => {
  const probeDb = createProbeDb();
  return probeDb
    .select({
      id: userEvents.id,
      userId: userEvents.payload.path("user_id"), // string (uint64 lossless decode)
      action: userEvents.payload.path("action"), // string (dynamic but typed via T)
      tier: userEvents.payload.path("session.tier"), // number (UInt8 typeHint)
      // dynamic path — force-decode through `Float64` even though `revenue`
      // is not in `typeHints`. Useful when the upstream is known to emit a
      // numeric value at this path but `T` declares it `number | undefined`.
      revenueCast: userEvents.payload.castPath("revenue", ckType.float64()),
      // sub-object — preserves JSON shape, useful when downstream code wants
      // the whole nested blob instead of unpacking it path-by-path.
      sessionObject: userEvents.payload.subobject("session"),
    })
    .from(userEvents)
    .where(ck.gte(userEvents.payload.path("session.tier"), minTier))
    .orderBy(ck.desc(userEvents.receivedAt));
};

// ---------------------------------------------------------------------------
// 2. fn.json* namespace — same SQL surface, but the path string can be
// computed at runtime (URLs, user input, config), trading the compile-time
// `Paths<T>` validation for flexibility.
// ---------------------------------------------------------------------------

export const buildNewjsonDynamicPathQuery = (dynamicPath: string) => {
  const probeDb = createProbeDb();
  return probeDb
    .select({
      id: userEvents.id,
      raw: fn.jsonPath<string>(userEvents.payload, dynamicPath),
      casted: fn.jsonCast(userEvents.payload, dynamicPath, ckType.string()),
      kind: fn.dynamicType(fn.jsonPath(userEvents.payload, dynamicPath)),
      merged: fn.jsonMerged(userEvents.payload, "session"),
    })
    .from(userEvents);
};

// ---------------------------------------------------------------------------
// 3. INSERT through the builder. ck-orm's per-column guard rejects top-level
// non-objects with a column-scoped client_validation error before the row
// ever reaches the wire (e.g. `payload: ["bad"]` would throw `JSON column
// expects a plain object, got array`). The compiler will refuse it first
// thanks to `$type<{ insert: ... }>`.
// ---------------------------------------------------------------------------

export const buildNewjsonInsertExample = () => {
  const probeDb = createProbeDb();
  const rows: UserEventsInsert[] = [
    {
      id: "1",
      receivedAt: new Date("2026-05-12T00:00:00Z"),
      payload: {
        // string / number / bigint all accepted thanks to `$type<{ insert }>`
        user_id: 123n,
        action: "login",
        revenue: 4.99,
        session: { id: "s-1", tier: 7 },
      },
      // `extras` omitted — CH fills `'{}'` via the DEFAULT expression.
    },
    {
      id: "2",
      receivedAt: new Date("2026-05-12T00:00:01Z"),
      payload: {
        user_id: "987654321",
        action: "logout",
        session: { id: "s-2", tier: 3 },
      },
      extras: { note: "explicit extras" },
    },
  ];
  return probeDb.insert(userEvents).values(rows);
};

// ---------------------------------------------------------------------------
// 4. INSERT against the `$validator`-decorated column. The schema runs
// before ck-orm's built-in guard on encode; a failure throws
// `client_validation` with the validator's issue messages aggregated.
// ---------------------------------------------------------------------------

export const buildNewjsonValidatorInsertExample = () => {
  const probeDb = createProbeDb();
  return probeDb.insert(revenueEvents).values({
    id: "1",
    payload: { user_id: "alice", revenue: 19.99 },
  });
};

// ---------------------------------------------------------------------------
// 5. The exact same `payload.session.tier > N` filter expressed three ways
// — useful as a side-by-side reference when learning the API.
// ---------------------------------------------------------------------------

export const buildNewjsonFilterTrioExample = (minTier: number) => {
  const probeDb = createProbeDb();

  // (a) typed-method form — most ergonomic, full IDE completion on the path.
  const typedMethod = probeDb
    .select({ id: userEvents.id })
    .from(userEvents)
    .where(ck.gt(userEvents.payload.path("session.tier"), minTier));

  // (b) fn.* form — same SQL, lets the path string be dynamic.
  const fnNamespace = probeDb
    .select({ id: userEvents.id })
    .from(userEvents)
    .where(ck.gt(fn.jsonPath<number>(userEvents.payload, "session.tier"), minTier));

  // (c) raw SQL escape hatch — pin the rendering when you need verbatim CH.
  const rawSql = probeDb
    .select({ id: userEvents.id })
    .from(userEvents)
    .where(ckSql`${userEvents.payload.path("session.tier")} > ${minTier}`);

  return { typedMethod, fnNamespace, rawSql };
};

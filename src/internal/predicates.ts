/**
 * Shared structural type guards. Centralises four near-identical predicates
 * that previously lived in `errors.ts`, `query-shared.ts`, `columns.ts`, and
 * `query.ts` — keeping the precise contract (e.g. whether arrays count as
 * "object record") in one place avoids subtle drift between call sites.
 */

/**
 * Any non-`null` object — including arrays, Dates, Maps, etc. Use when the
 * caller only needs `value` to be reachable as `{ ... }` for property reads.
 */
export const isRecord = (value: unknown): value is Record<string, unknown> => {
  return typeof value === "object" && value !== null;
};

/**
 * A plain object record (`{ ... }`) — explicitly rejects arrays. Use when the
 * caller's intent is "either a row literal or a config object", and arrays
 * have a separate handling path.
 */
export const isPlainObject = (value: unknown): value is Record<string, unknown> => {
  return typeof value === "object" && value !== null && !Array.isArray(value);
};

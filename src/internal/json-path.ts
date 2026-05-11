import { createClientValidationError } from "../errors";
import { assertValidSqlIdentifier } from "./identifier";

/**
 * Splits a dotted JSON path string (`"a.b.c"`) into its segment array. Empty
 * input and segments separated by stray dots are rejected so downstream SQL
 * renderers can rely on the result containing only valid segments.
 *
 * ClickHouse 24.x+ JSON path syntax uses dot-separated identifiers for both
 * `typeHints` keys (`a.b UInt32`) and the access expression (`json.a.b`).
 * Each segment must be a plain SQL identifier — the same rule as a column
 * name — so the validation delegates to `assertValidSqlIdentifier` once per
 * segment.
 */
export const parseJsonPathSegments = (path: string): string[] => {
  if (typeof path !== "string" || path.length === 0) {
    throw createClientValidationError("JSON path must be a non-empty string");
  }
  const segments = path.split(".");
  for (const segment of segments) {
    if (segment.length === 0) {
      throw createClientValidationError(`Invalid JSON path "${path}": empty segment`);
    }
    assertValidSqlIdentifier(segment);
  }
  return segments;
};

/**
 * Asserts that `path` is a valid dotted JSON path. Throws a
 * `client_validation` ClickHouseORMError otherwise. Exists so call sites that
 * only need validation (not the segments) read clearly.
 */
export const assertJsonPathIdentifier = (path: string): void => {
  parseJsonPathSegments(path);
};

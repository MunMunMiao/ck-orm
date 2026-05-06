import { createClientValidationError } from "../errors";

const padNumber = (value: number, width: number) => value.toString().padStart(width, "0");

/**
 * Format a JS `Date` as a ClickHouse SQL date-time literal in UTC.
 *
 * Output: `YYYY-MM-DD HH:MM:SS` when `precision <= 0`, otherwise
 * `YYYY-MM-DD HH:MM:SS.fff…` with the fractional component padded
 * (left) and truncated (right) to exactly `precision` digits.
 *
 * Used by `formatQueryParamValue` for query-parameter binding. ClickHouse's
 * query-parameter parser accepts this format including pre-epoch dates
 * (where Unix-seconds with a negative fractional like `"-0.001"` would be
 * silently treated as positive). For column-typed encoding paths that need
 * timezone-agnostic insertion (`insertJsonEachRow`), pass the Date through
 * to `JSON.stringify` instead — that emits ISO 8601 with `Z`, which ClickHouse
 * accepts via `date_time_input_format=best_effort`.
 *
 * Throws `client_validation` if the Date is invalid.
 */
export const formatClickHouseDateTime = (value: Date, precision: number): string => {
  if (Number.isNaN(value.getTime())) {
    throw createClientValidationError("Cannot format invalid Date as ClickHouse date-time");
  }
  const datePart =
    `${padNumber(value.getUTCFullYear(), 4)}-` +
    `${padNumber(value.getUTCMonth() + 1, 2)}-` +
    `${padNumber(value.getUTCDate(), 2)} ` +
    `${padNumber(value.getUTCHours(), 2)}:` +
    `${padNumber(value.getUTCMinutes(), 2)}:` +
    `${padNumber(value.getUTCSeconds(), 2)}`;
  if (precision <= 0) {
    return datePart;
  }
  const ms = value.getUTCMilliseconds();
  const fractional = ms.toString().padStart(3, "0").padEnd(precision, "0").slice(0, precision);
  return `${datePart}.${fractional}`;
};

import { createDecodeError } from "./errors";

/**
 * Shared driver-value coercion helpers.
 *
 * Both `columns.ts` (for column `mapFromDriverValue`) and `functions.ts`
 * (for SQL-function expression decoders) need to convert raw HTTP/JSON
 * driver output into typed JS values. The two files used to ship two
 * subtly-different copies of the same logic; this module is the single
 * source of truth.
 *
 * All converters throw a `DecodeError` (with the original value
 * attached) on failure.
 */

export const toNumber = (value: unknown): number => {
  let result: number;
  if (typeof value === "number") {
    result = value;
  } else if (typeof value === "string" || typeof value === "bigint") {
    result = Number(value);
  } else {
    throw createDecodeError(`Cannot convert value to number: ${String(value)}`, value);
  }
  if (!Number.isFinite(result)) {
    throw createDecodeError(`Cannot convert value to finite number: ${String(value)}`, value);
  }
  return result;
};

export const toIntegerNumber = (
  value: unknown,
  options: {
    readonly min: number;
    readonly max: number;
  },
): number => {
  const result = toNumber(value);
  if (!Number.isInteger(result) || result < options.min || result > options.max) {
    throw createDecodeError(
      `Cannot convert value to integer in range ${options.min}..${options.max}: ${String(value)}`,
      value,
    );
  }
  return result;
};

const INTEGER_STRING_PATTERN = /^-?(0|[1-9]\d*)$/;
const UNSIGNED_INTEGER_STRING_PATTERN = /^(0|[1-9]\d*)$/;

export const toIntegerString = (value: unknown, options?: { readonly unsigned?: boolean }): string => {
  const pattern = options?.unsigned ? UNSIGNED_INTEGER_STRING_PATTERN : INTEGER_STRING_PATTERN;
  let result: string;
  if (typeof value === "string") {
    result = value;
  } else if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) {
      throw createDecodeError(`Cannot convert value to integer string: ${String(value)}`, value);
    }
    result = String(value);
  } else if (typeof value === "bigint") {
    result = String(value);
  } else {
    throw createDecodeError(`Cannot convert value to string: ${String(value)}`, value);
  }
  if (!pattern.test(result)) {
    throw createDecodeError(`Cannot convert value to integer string: ${String(value)}`, value);
  }
  return result;
};

export const toStringValue = (value: unknown): string => {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
    return String(value);
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  throw createDecodeError(`Cannot convert value to string: ${String(value)}`, value);
};

// Date-only forms (`YYYY-MM-DD`) intentionally fall through to the native
// Date parser; only timezone-less datetimes are pinned to UTC here.
const NAIVE_DATETIME_PATTERN = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?$/;

const parseNaiveDateTimeAsUtc = (value: string): Date | null | undefined => {
  const match = NAIVE_DATETIME_PATTERN.exec(value);
  if (!match) {
    return undefined;
  }
  const [, yearRaw, monthRaw, dayRaw, hourRaw, minuteRaw, secondRaw, fractionalRaw] = match;
  const year = Number(yearRaw);
  const month = Number(monthRaw);
  const day = Number(dayRaw);
  const hour = Number(hourRaw);
  const minute = Number(minuteRaw);
  const second = Number(secondRaw);
  const millisecond = Number((fractionalRaw ?? "").slice(0, 3).padEnd(3, "0"));
  const timestamp = Date.UTC(year, month - 1, day, hour, minute, second, millisecond);
  const date = new Date(timestamp);
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day ||
    date.getUTCHours() !== hour ||
    date.getUTCMinutes() !== minute ||
    date.getUTCSeconds() !== second
  ) {
    return null;
  }
  return date;
};

export const toDate = (value: unknown): Date => {
  let date: Date;
  if (value instanceof Date) {
    date = value;
  } else if (typeof value === "string") {
    const parsedNaiveDateTime = parseNaiveDateTimeAsUtc(value);
    if (parsedNaiveDateTime === null) {
      throw createDecodeError(`Cannot convert value to valid Date: ${String(value)}`, value);
    }
    date = parsedNaiveDateTime ?? new Date(value);
  } else if (typeof value === "number") {
    date = new Date(value);
  } else {
    throw createDecodeError(`Cannot convert value to Date: ${String(value)}`, value);
  }
  if (Number.isNaN(date.getTime())) {
    throw createDecodeError(`Cannot convert value to valid Date: ${String(value)}`, value);
  }
  return date;
};

export const toBoolean = (value: unknown): boolean => {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw createDecodeError(`Cannot convert non-finite number to boolean: ${String(value)}`, value);
    }
    return value !== 0;
  }
  if (typeof value === "string") {
    return value === "1" || value.toLowerCase() === "true";
  }
  throw createDecodeError(`Cannot convert value to boolean: ${String(value)}`, value);
};

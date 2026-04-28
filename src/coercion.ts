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

export const toDate = (value: unknown): Date => {
  let date: Date;
  if (value instanceof Date) {
    date = value;
  } else if (typeof value === "string" || typeof value === "number") {
    date = new Date(value);
  } else {
    throw createDecodeError(`Cannot convert value to Date: ${String(value)}`, value);
  }
  if (Number.isNaN(date.getTime())) {
    throw createDecodeError(`Cannot convert value to valid Date: ${String(value)}`, value);
  }
  return date;
};

const TIME_VALUE_PATTERN = /^(-?)(\d+):([0-5]\d):([0-5]\d)(?:\.(\d{1,9}))?$/;

export const toTimeDate = (value: unknown): Date => {
  if (typeof value === "string") {
    const match = TIME_VALUE_PATTERN.exec(value);
    if (match) {
      const [, sign, hoursRaw, minutesRaw, secondsRaw, fractionalRaw] = match;
      const hours = Number(hoursRaw);
      const minutes = Number(minutesRaw);
      const seconds = Number(secondsRaw);
      const milliseconds = Number((fractionalRaw ?? "").slice(0, 3).padEnd(3, "0"));
      const totalMilliseconds = (((hours * 60 + minutes) * 60 + seconds) * 1000 + milliseconds) * (sign ? -1 : 1);
      return new Date(totalMilliseconds);
    }
  }
  return toDate(value);
};

export const toBoolean = (value: unknown): boolean => {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    return value === "1" || value.toLowerCase() === "true";
  }
  throw createDecodeError(`Cannot convert value to boolean: ${String(value)}`, value);
};

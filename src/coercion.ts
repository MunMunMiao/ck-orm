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
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string" || typeof value === "bigint") {
    return Number(value);
  }
  throw createDecodeError(`Cannot convert value to number: ${String(value)}`, value);
};

export const toIntegerString = (value: unknown): string => {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "bigint") {
    return String(value);
  }
  throw createDecodeError(`Cannot convert value to string: ${String(value)}`, value);
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
  if (value instanceof Date) {
    return value;
  }
  if (typeof value === "string" || typeof value === "number") {
    return new Date(value);
  }
  throw createDecodeError(`Cannot convert value to Date: ${String(value)}`, value);
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

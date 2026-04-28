import { createClientValidationError } from "../errors";
import type { Decoder } from "../query-shared";
import { type SQLFragment, sql } from "../sql";

export type CountMode = "unsafe" | "safe" | "mixed";

export type CountModeResult<TMode extends CountMode> = TMode extends "safe"
  ? string
  : TMode extends "mixed"
    ? number | string
    : number;

export type CountSqlType = "Float64" | "String" | "UInt64";

const COUNT_DECIMAL_PATTERN = /^(0|[1-9]\d*)$/;

const createInvalidCountValueError = (value: unknown) =>
  createClientValidationError(
    `Failed to decode count() result: ${String(value)}. Expected a non-negative integer count value from ClickHouse.`,
    { cause: value },
  );

const isNonNegativeIntegerNumber = (value: number): boolean => {
  return Number.isFinite(value) && Number.isInteger(value) && value >= 0;
};

export const countUnsafeDecoder: Decoder<number> = (value) => {
  if (typeof value === "number" && isNonNegativeIntegerNumber(value)) {
    return value;
  }

  if (typeof value === "bigint" && value >= 0n) {
    const nextValue = Number(value);
    if (isNonNegativeIntegerNumber(nextValue)) {
      return nextValue;
    }
  }

  if (typeof value === "string" && value.length > 0 && value.trim() === value) {
    const nextValue = Number(value);
    if (isNonNegativeIntegerNumber(nextValue)) {
      return nextValue;
    }
  }

  throw createInvalidCountValueError(value);
};

export const countSafeDecoder: Decoder<string> = (value) => {
  if (typeof value === "string" && COUNT_DECIMAL_PATTERN.test(value)) {
    return value;
  }

  if (typeof value === "bigint" && value >= 0n) {
    return value.toString();
  }

  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) {
    return String(value);
  }

  throw createInvalidCountValueError(value);
};

export const countMixedDecoder: Decoder<number | string> = (value) => {
  if (typeof value === "string" && COUNT_DECIMAL_PATTERN.test(value)) {
    return value;
  }

  if (typeof value === "number" && isNonNegativeIntegerNumber(value)) {
    return value;
  }

  if (typeof value === "bigint" && value >= 0n) {
    return value.toString();
  }

  throw createInvalidCountValueError(value);
};

export const getCountSqlType = (mode: CountMode): CountSqlType => {
  switch (mode) {
    case "safe":
      return "String";
    case "mixed":
      return "UInt64";
    case "unsafe":
      return "Float64";
  }
};

export const getCountDecoder = <TMode extends CountMode>(mode: TMode): Decoder<CountModeResult<TMode>> => {
  switch (mode) {
    case "safe":
      return countSafeDecoder as Decoder<CountModeResult<TMode>>;
    case "mixed":
      return countMixedDecoder as Decoder<CountModeResult<TMode>>;
    case "unsafe":
      return countUnsafeDecoder as Decoder<CountModeResult<TMode>>;
  }

  throw createClientValidationError(`Unknown count mode: ${String(mode)}`);
};

export const wrapCountSql = (inner: SQLFragment, mode: CountMode): SQLFragment => {
  switch (mode) {
    case "safe":
      return sql`${sql.raw("toString(")}${inner}${sql.raw(")")}`;
    case "mixed":
      return sql`${sql.raw("toUInt64(")}${inner}${sql.raw(")")}`;
    case "unsafe":
      return sql`${sql.raw("toFloat64(")}${inner}${sql.raw(")")}`;
  }
};

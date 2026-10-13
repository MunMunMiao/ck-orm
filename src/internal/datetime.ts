import { createClientValidationError } from "../errors";
import {
  getArrayElementType,
  getMapKeyValueTypes,
  getTupleElementTypes,
  unwrapNullableLowCardinalityType,
  unwrapStorageType,
} from "./clickhouse-type";

export const UTC_INSTANT_TYPE = "DateTime64(3, 'UTC')";
const UTC_SECOND_TYPE = "DateTime('UTC')";

const DATE_TIME_TYPE = /^DateTime(?:64)?(?:\(|$)/;

// Map keys can be Dates too; plain-object keys are always strings.
const childValues = (value: object): Iterable<unknown> =>
  value instanceof Map ? [...value.keys(), ...value.values()] : Array.isArray(value) ? value : Object.values(value);

const mapKeysAndValues = (value: object): [unknown[], unknown[]] =>
  value instanceof Map ? [[...value.keys()], [...value.values()]] : [Object.keys(value), Object.values(value)];

const hasSubSecondDate = (value: unknown): boolean => {
  if (value instanceof Date) {
    return value.getUTCMilliseconds() !== 0;
  }
  if (typeof value !== "object" || value === null) {
    return false;
  }
  for (const item of childValues(value)) {
    if (hasSubSecondDate(item)) {
      return true;
    }
  }
  return false;
};

// Drop LowCardinality and SimpleAggregateFunction. Keep Nullable only when the
// stored type itself is nullable — `Nullable` may sit outside or inside those
// storage wrappers.
const bindDateTimePosition = (sqlType: string, subSecond: boolean): string | undefined => {
  const core = unwrapNullableLowCardinalityType(sqlType);
  const instant = DATE_TIME_TYPE.test(core)
    ? core.startsWith("DateTime64") || subSecond
      ? UTC_INSTANT_TYPE
      : UTC_SECOND_TYPE
    : undefined;
  return instant === undefined
    ? undefined
    : unwrapStorageType(sqlType).startsWith("Nullable(")
      ? `Nullable(${instant})`
      : instant;
};

const containsDate = (value: unknown): boolean => {
  if (value instanceof Date) {
    return true;
  }
  if (typeof value !== "object" || value === null) {
    return false;
  }
  for (const item of childValues(value)) {
    if (containsDate(item)) {
      return true;
    }
  }
  return false;
};

// A datetime string anywhere under an Array or Map element whose type was
// rewritten to UTC would be parsed in UTC instead of the column timezone.
const hasDateTimeString = (sqlType: string, value: unknown): boolean => {
  if (typeof value === "string") {
    return DATE_TIME_TYPE.test(unwrapNullableLowCardinalityType(sqlType));
  }
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const tupleTypes = getTupleElementTypes(sqlType);
  if (tupleTypes && Array.isArray(value)) {
    return tupleTypes.some((elementType, index) => hasDateTimeString(elementType, value[index]));
  }
  const arrayElementType = getArrayElementType(sqlType);
  if (arrayElementType && Array.isArray(value)) {
    return value.some((item) => hasDateTimeString(arrayElementType, item));
  }
  const mapTypes = getMapKeyValueTypes(sqlType);
  if (mapTypes) {
    const [keys, values] = mapKeysAndValues(value);
    return (
      keys.some((key) => hasDateTimeString(mapTypes[0], key)) ||
      values.some((item) => hasDateTimeString(mapTypes[1], item))
    );
  }
  return false;
};

// Every DateTime position in the element type. Array and Map elements share
// one type, so they cannot each keep a different timezone.
const rewriteDateTimePositions = (sqlType: string, subSecond: boolean): string => {
  const instant = bindDateTimePosition(sqlType, subSecond);
  if (instant !== undefined) {
    return instant;
  }
  const tupleTypes = getTupleElementTypes(sqlType);
  if (tupleTypes) {
    const bound = tupleTypes.map((part) => rewriteDateTimePositions(part, subSecond));
    return bound.every((part, index) => part === tupleTypes[index]) ? sqlType : `Tuple(${bound.join(", ")})`;
  }
  const arrayElementType = getArrayElementType(sqlType);
  if (arrayElementType) {
    const bound = rewriteDateTimePositions(arrayElementType, subSecond);
    return bound === arrayElementType ? sqlType : `Array(${bound})`;
  }
  const mapTypes = getMapKeyValueTypes(sqlType);
  if (mapTypes) {
    const [keyType, valueType] = mapTypes;
    const boundKey = rewriteDateTimePositions(keyType, subSecond);
    const boundValue = rewriteDateTimePositions(valueType, subSecond);
    return boundKey === keyType && boundValue === valueType ? sqlType : `Map(${boundKey}, ${boundValue})`;
  }
  return sqlType;
};

// Returns the bound item type, or `itemType` itself when no Date rewrote it.
const bindItemsAsUtc = (sqlType: string, itemType: string, items: readonly unknown[], subSecond: boolean): string => {
  if (!items.some(containsDate)) {
    return itemType;
  }
  const rewritten = rewriteDateTimePositions(itemType, subSecond);
  if (rewritten === itemType) {
    return itemType;
  }
  if (items.some((item) => hasDateTimeString(itemType, item))) {
    throw createClientValidationError(
      `Cannot bind a ${sqlType} parameter that mixes JS Date values with datetime strings: ` +
        "Dates are sent as UTC instants, strings in the column timezone. Convert one side first.",
    );
  }
  return rewritten;
};

const bindDates = (sqlType: string, value: unknown, subSecond: boolean): string => {
  if (value instanceof Date) {
    return bindDateTimePosition(sqlType, subSecond) ?? sqlType;
  }
  if (typeof value !== "object" || value === null) {
    return sqlType;
  }
  const tupleTypes = getTupleElementTypes(sqlType);
  if (tupleTypes && Array.isArray(value)) {
    const bound = tupleTypes.map((elementType, index) => bindDates(elementType, value[index], subSecond));
    return bound.every((elementType, index) => elementType === tupleTypes[index])
      ? sqlType
      : `Tuple(${bound.join(", ")})`;
  }
  const arrayElementType = getArrayElementType(sqlType);
  if (arrayElementType && Array.isArray(value)) {
    const bound = bindItemsAsUtc(sqlType, arrayElementType, value, subSecond);
    return bound === arrayElementType ? sqlType : `Array(${bound})`;
  }
  const mapTypes = getMapKeyValueTypes(sqlType);
  if (mapTypes) {
    const [keyType, valueType] = mapTypes;
    const [keys, values] = mapKeysAndValues(value);
    const boundKey = bindItemsAsUtc(sqlType, keyType, keys, subSecond);
    const boundValue = bindItemsAsUtc(sqlType, valueType, values, subSecond);
    return boundKey === keyType && boundValue === valueType ? sqlType : `Map(${boundKey}, ${boundValue})`;
  }
  return sqlType;
};

/**
 * Rewrite DateTime/DateTime64 positions that hold a JS `Date` so ClickHouse
 * parses them as UTC instants.
 *
 * Sub-second precision is decided once for the parameter: if any Date in the
 * value has milliseconds, every 32-bit `DateTime` position binds as
 * `DateTime64(3, 'UTC')`; otherwise those positions bind as `DateTime('UTC')`.
 * `DateTime64` positions always bind as `DateTime64(3, 'UTC')`. The rewritten
 * position drops `LowCardinality` and `SimpleAggregateFunction` and keeps
 * `Nullable` only when the stored type is nullable.
 *
 * An Array or Map whose elements contain a Date rewrites every DateTime
 * position in the element type, so elements cannot disagree. A datetime
 * string anywhere under that element is rejected. A top-level Tuple keeps
 * independent positions. Strings and numbers keep the declared type when no
 * Date rewrote it.
 *
 * Dates are sent as UTC wall-clock text (`formatClickHouseDateTime`). A Date
 * with milliseconds written into a `DateTime` column truncates to the second.
 */
export const bindDatesAsUtc = (sqlType: string, value: unknown): string => {
  return bindDates(sqlType, value, hasSubSecondDate(value));
};

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
  const msPadded = value.getUTCMilliseconds().toString().padStart(3, "0");
  // `precision === 3` (common DateTime64(3)) hits the fast path with no
  // further string ops; lower precision truncates, higher precision pads.
  const fractional =
    precision === 3 ? msPadded : precision < 3 ? msPadded.slice(0, precision) : msPadded.padEnd(precision, "0");
  return `${datePart}.${fractional}`;
};

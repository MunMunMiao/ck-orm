import { createClientValidationError } from "../errors";

const TYPE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const INTEGER_LITERAL = /^-?(0|[1-9]\d*)$/;
const NUMERIC_LITERAL = /^-?(?:0|[1-9]\d*)(?:\.\d+)?$/;
const UNSIGNED_INTEGER_LITERAL = /^(0|[1-9]\d*)$/;
const DECIMAL_FAMILY = new Set(["Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256"]);
const FIXED_PRECISION_DECIMAL_MAX_SCALE: Record<string, number> = {
  Decimal32: 9,
  Decimal64: 18,
  Decimal128: 38,
  Decimal256: 76,
};

const SIMPLE_TYPES = new Set([
  "Nothing",
  "Null",
  "Bool",
  "UInt8",
  "UInt16",
  "UInt32",
  "UInt64",
  "UInt128",
  "UInt256",
  "Int8",
  "Int16",
  "Int32",
  "Int64",
  "Int128",
  "Int256",
  "Float32",
  "Float64",
  "BFloat16",
  "String",
  "Date",
  "Date32",
  "DateTime",
  "Time",
  "UUID",
  "IPv4",
  "IPv6",
  "JSON",
  "Dynamic",
  "Point",
  "Ring",
  "LineString",
  "MultiLineString",
  "Polygon",
  "MultiPolygon",
]);

const isValidIdentifier = (value: string): boolean => TYPE_IDENTIFIER.test(value);

const hasControlCharacter = (value: string): boolean => {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 0x1f || code === 0x7f) {
      return true;
    }
  }
  return false;
};

const ensureSafeTypeCharacters = (value: string, label = "ClickHouse type literal"): void => {
  if (
    hasControlCharacter(value) ||
    value.includes(";") ||
    value.includes("--") ||
    value.includes("/*") ||
    value.includes("*/") ||
    value.includes("`") ||
    value.includes('"')
  ) {
    throw createClientValidationError(`Invalid ${label}: ${value}`);
  }
};

// Returns the index just past the closing quote, or -1 if `value` does not
// start at `start` with a complete quoted literal. Used in two modes: as a
// scanner (callers throw when -1) and as a probe (callers branch on -1).
// Exception-as-control-flow is hostile to V8 optimisation — keep this -1.
const tryReadSqlSingleQuotedLiteral = (value: string, start = 0): number => {
  if (value[start] !== "'") {
    return -1;
  }

  for (let index = start + 1; index < value.length; index += 1) {
    const char = value[index];
    if (char === "\\") {
      index += 1;
      if (index >= value.length) {
        return -1;
      }
      continue;
    }
    if (char === "'") {
      return index + 1;
    }
  }

  return -1;
};

const readSqlSingleQuotedLiteral = (value: string, start = 0): number => {
  const end = tryReadSqlSingleQuotedLiteral(value, start);
  if (end < 0) {
    throw createClientValidationError(`Invalid ClickHouse type literal: ${value}`);
  }
  return end;
};

export const splitTopLevelTypeList = (value: string): string[] => {
  const parts: string[] = [];
  let start = 0;
  let depth = 0;

  for (let index = 0; index < value.length; index += 1) {
    const char = value[index];
    if (char === "'") {
      index = readSqlSingleQuotedLiteral(value, index) - 1;
      continue;
    }
    if (char === "(") {
      depth += 1;
      continue;
    }
    if (char === ")") {
      depth -= 1;
      if (depth < 0) {
        throw createClientValidationError(`Invalid ClickHouse type literal: ${value}`);
      }
      continue;
    }
    if (char === "," && depth === 0) {
      const part = value.slice(start, index).trim();
      if (part === "") {
        throw createClientValidationError(`Invalid ClickHouse type literal: ${value}`);
      }
      parts.push(part);
      start = index + 1;
    }
  }

  if (depth !== 0) {
    throw createClientValidationError(`Invalid ClickHouse type literal: ${value}`);
  }

  const tail = value.slice(start).trim();
  if (tail === "") {
    throw createClientValidationError(`Invalid ClickHouse type literal: ${value}`);
  }
  parts.push(tail);
  return parts;
};

const splitTopLevelWhitespace = (
  value: string,
):
  | {
      readonly left: string;
      readonly right: string;
    }
  | undefined => {
  let depth = 0;

  for (let index = 0; index < value.length; index += 1) {
    const char = value[index];
    if (char === "'") {
      index = readSqlSingleQuotedLiteral(value, index) - 1;
      continue;
    }
    if (char === "(") {
      depth += 1;
      continue;
    }
    if (char === ")") {
      depth -= 1;
      continue;
    }
    if (depth === 0 && /\s/.test(char)) {
      const left = value.slice(0, index).trim();
      const right = value.slice(index + 1).trim();
      if (left && right) return { left, right };
    }
  }

  return undefined;
};

const splitTypeCall = (
  value: string,
): {
  readonly name: string;
  readonly args?: string;
} => {
  const firstParen = value.indexOf("(");
  if (firstParen === -1) {
    return { name: value };
  }

  const name = value.slice(0, firstParen).trim();
  const args = value.slice(firstParen + 1, -1);
  if (!value.endsWith(")") || !isValidIdentifier(name)) {
    throw createClientValidationError(`Invalid ClickHouse type literal: ${value}`);
  }

  let depth = 0;
  for (let index = firstParen; index < value.length; index += 1) {
    const char = value[index];
    if (char === "'") {
      index = readSqlSingleQuotedLiteral(value, index) - 1;
      continue;
    }
    if (char === "(") {
      depth += 1;
    } else if (char === ")") {
      depth -= 1;
      if (depth === 0 && index !== value.length - 1) {
        throw createClientValidationError(`Invalid ClickHouse type literal: ${value}`);
      }
    }
  }
  if (depth !== 0) {
    throw createClientValidationError(`Invalid ClickHouse type literal: ${value}`);
  }

  return { name, args };
};

const requireUnsignedInteger = (value: string, label: string, max?: number): number => {
  if (!UNSIGNED_INTEGER_LITERAL.test(value)) {
    throw createClientValidationError(`${label} must be an unsigned integer, got ${value}`);
  }
  const result = Number(value);
  if (!Number.isSafeInteger(result) || (max !== undefined && result > max)) {
    throw createClientValidationError(`${label} is out of range, got ${value}`);
  }
  return result;
};

const requireSingleQuotedLiteral = (value: string): void => {
  const end = readSqlSingleQuotedLiteral(value);
  if (end !== value.length) {
    throw createClientValidationError(`Invalid ClickHouse type literal: ${value}`);
  }
};

const validateEnumEntry = (value: string): void => {
  const trimmed = value.trim();
  const literalEnd = readSqlSingleQuotedLiteral(trimmed);
  const rest = trimmed.slice(literalEnd).trim();
  if (!rest.startsWith("=") || !INTEGER_LITERAL.test(rest.slice(1).trim())) {
    throw createClientValidationError(`Invalid ClickHouse enum type entry: ${value}`);
  }
};

const isAggregateParameterLiteral = (value: string): boolean => {
  const trimmed = value.trim();
  if (NUMERIC_LITERAL.test(trimmed)) {
    return true;
  }
  return tryReadSqlSingleQuotedLiteral(trimmed) === trimmed.length;
};

const validateAggregateFunctionSignature = (value: string): void => {
  const trimmed = value.trim();
  const { name, args } = splitTypeCall(trimmed);
  if (!isValidIdentifier(name)) {
    throw createClientValidationError(`Invalid AggregateFunction signature: ${value}`);
  }
  if (args === undefined) {
    return;
  }

  let parameters: string[];
  try {
    parameters = splitTopLevelTypeList(args);
  } catch {
    throw createClientValidationError(`Invalid AggregateFunction signature: ${value}`);
  }
  for (const parameter of parameters) {
    if (!isAggregateParameterLiteral(parameter)) {
      throw createClientValidationError(`AggregateFunction parameters must be string or numeric literals: ${value}`);
    }
  }
};

export const normalizeAggregateFunctionSignature = (value: unknown): string => {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw createClientValidationError(`AggregateFunction signature must be a non-empty string, got ${String(value)}`);
  }
  const trimmed = value.trim();
  ensureSafeTypeCharacters(trimmed, "AggregateFunction signature");
  validateAggregateFunctionSignature(trimmed);
  return trimmed;
};

// Maximum recursion depth for parameterised type literals. Real ClickHouse
// types (e.g. `Array(Map(String, Tuple(Int64, Array(String))))`) rarely
// exceed 5 levels; 100 is a generous safety net that prevents pathological
// input (`Array(Array(Array(... 50000 levels ...)))`) from blowing the call
// stack via deep recursion.
const MAX_TYPE_NESTING_DEPTH = 100;

const validateAggregateFunctionArgs = (name: string, args: readonly string[], depth: number): void => {
  if (args.length < 2) {
    throw createClientValidationError(`Invalid ClickHouse ${name} type literal`);
  }
  if (name === "AggregateFunction") {
    normalizeAggregateFunctionSignature(args[0]);
  } else if (!isValidIdentifier(args[0])) {
    throw createClientValidationError(`Invalid ClickHouse ${name} type literal`);
  }
  for (const arg of args.slice(1)) {
    validateType(arg, depth);
  }
};

const validateTupleElement = (value: string, depth: number): void => {
  // `Tuple(name Type)` is valid ClickHouse syntax — peel a leading identifier
  // before validating. We probe `splitTopLevelWhitespace` first instead of
  // try/catching `validateType`, since exception-as-control-flow is hostile
  // to V8 optimisation on the hot validation path.
  const named = splitTopLevelWhitespace(value);
  if (named && isValidIdentifier(named.left)) {
    validateType(named.right, depth);
    return;
  }
  validateType(value, depth);
};

const validateNestedElement = (value: string, depth: number): void => {
  const named = splitTopLevelWhitespace(value);
  if (!named || !isValidIdentifier(named.left)) {
    throw createClientValidationError(`Invalid ClickHouse Nested field type: ${value}`);
  }
  validateType(named.right, depth);
};

const validateType = (value: string, depth = 0): void => {
  if (depth > MAX_TYPE_NESTING_DEPTH) {
    throw createClientValidationError(`ClickHouse type literal nested deeper than ${MAX_TYPE_NESTING_DEPTH} levels`);
  }
  const trimmed = value.trim();
  const { name, args } = splitTypeCall(trimmed);
  if (!isValidIdentifier(name)) {
    throw createClientValidationError(`Invalid ClickHouse type literal: ${value}`);
  }

  if (args === undefined) {
    if (!SIMPLE_TYPES.has(name)) {
      throw createClientValidationError(`Unsupported ClickHouse type literal: ${value}`);
    }
    return;
  }

  const nextDepth = depth + 1;
  const parts = splitTopLevelTypeList(args);
  if (name === "Nullable" || name === "Array" || name === "LowCardinality") {
    if (parts.length !== 1) {
      throw createClientValidationError(`ClickHouse ${name} type expects exactly one argument`);
    }
    validateType(parts[0], nextDepth);
    return;
  }

  if (name === "Tuple" || name === "Variant") {
    const validateElement = name === "Tuple" ? validateTupleElement : validateType;
    for (const part of parts) {
      validateElement(part, nextDepth);
    }
    return;
  }

  if (name === "Nested") {
    for (const part of parts) {
      validateNestedElement(part, nextDepth);
    }
    return;
  }

  if (name === "Map") {
    if (parts.length !== 2) {
      throw createClientValidationError("ClickHouse Map type expects exactly two arguments");
    }
    validateType(parts[0], nextDepth);
    validateType(parts[1], nextDepth);
    return;
  }

  if (name === "FixedString") {
    if (parts.length !== 1) {
      throw createClientValidationError("ClickHouse FixedString type expects one length argument");
    }
    requireUnsignedInteger(parts[0], "FixedString length");
    return;
  }

  if (DECIMAL_FAMILY.has(name)) {
    if (name === "Decimal") {
      if (parts.length !== 2) {
        throw createClientValidationError("ClickHouse Decimal type expects precision and scale");
      }
      const precision = requireUnsignedInteger(parts[0], "Decimal precision", 76);
      const scale = requireUnsignedInteger(parts[1], "Decimal scale", 76);
      if (precision < 1 || scale > precision) {
        throw createClientValidationError(`Invalid ClickHouse Decimal type literal: ${value}`);
      }
      return;
    }

    if (parts.length !== 1) {
      throw createClientValidationError(`ClickHouse ${name} type expects one scale argument`);
    }
    requireUnsignedInteger(parts[0], `${name} scale`, FIXED_PRECISION_DECIMAL_MAX_SCALE[name]);
    return;
  }

  if (name === "DateTime") {
    if (parts.length !== 1) {
      throw createClientValidationError("ClickHouse DateTime type expects at most one timezone argument");
    }
    requireSingleQuotedLiteral(parts[0]);
    return;
  }

  if (name === "DateTime64") {
    if (parts.length !== 1 && parts.length !== 2) {
      throw createClientValidationError("ClickHouse DateTime64 type expects precision and optional timezone");
    }
    requireUnsignedInteger(parts[0], "DateTime64 precision", 9);
    if (parts[1] !== undefined) {
      requireSingleQuotedLiteral(parts[1]);
    }
    return;
  }

  if (name === "Time64") {
    if (parts.length !== 1) {
      throw createClientValidationError("ClickHouse Time64 type expects one precision argument");
    }
    requireUnsignedInteger(parts[0], "Time64 precision", 9);
    return;
  }

  if (name === "Enum8" || name === "Enum16") {
    for (const part of parts) {
      validateEnumEntry(part);
    }
    return;
  }

  if (name === "AggregateFunction" || name === "SimpleAggregateFunction") {
    validateAggregateFunctionArgs(name, parts, nextDepth);
    return;
  }

  if (name === "Object") {
    if (parts.length !== 1) {
      throw createClientValidationError("ClickHouse Object type expects one argument");
    }
    requireSingleQuotedLiteral(parts[0]);
    return;
  }

  if (name === "QBit") {
    if (parts.length !== 2) {
      throw createClientValidationError("ClickHouse QBit type expects element type and dimensions");
    }
    validateType(parts[0], nextDepth);
    requireUnsignedInteger(parts[1], "QBit dimensions");
    return;
  }

  throw createClientValidationError(`Unsupported ClickHouse type literal: ${value}`);
};

export const normalizeClickHouseTypeLiteral = (value: unknown): string => {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw createClientValidationError(`ClickHouse type literal must be a non-empty string, got ${String(value)}`);
  }
  const trimmed = value.trim();
  ensureSafeTypeCharacters(trimmed);
  validateType(trimmed);
  return trimmed;
};

const unwrapTypeCall = (value: string, names: readonly string[]): string => {
  let current = value.trim();
  while (true) {
    const { name, args } = splitTypeCall(current);
    if (!args || !names.includes(name)) {
      return current;
    }
    const parts = splitTopLevelTypeList(args);
    if (parts.length !== 1) {
      return current;
    }
    current = parts[0].trim();
  }
};

export const unwrapNullableLowCardinalityType = (value: string): string => {
  return unwrapTypeCall(value, ["Nullable", "LowCardinality"]);
};

export const getArrayElementType = (sqlType: string | undefined): string | undefined => {
  if (!sqlType) {
    return undefined;
  }
  const unwrapped = unwrapNullableLowCardinalityType(sqlType);
  const { name, args } = splitTypeCall(unwrapped);
  if (name !== "Array" || args === undefined) {
    return undefined;
  }
  const parts = splitTopLevelTypeList(args);
  return parts.length === 1 ? parts[0] : undefined;
};

export const getTupleElementTypes = (sqlType: string | undefined): readonly string[] | undefined => {
  if (!sqlType) {
    return undefined;
  }
  const unwrapped = unwrapNullableLowCardinalityType(sqlType);
  const { name, args } = splitTypeCall(unwrapped);
  if (name !== "Tuple" || args === undefined) {
    return undefined;
  }
  return splitTopLevelTypeList(args).map((part) => {
    const named = splitTopLevelWhitespace(part);
    return named && isValidIdentifier(named.left) ? named.right : part;
  });
};

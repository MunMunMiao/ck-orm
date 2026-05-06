import {
  toBoolean as toBooleanCoercion,
  toDate as toDateCoercion,
  toIntegerNumber as toIntegerNumberCoercion,
  toIntegerString as toIntegerStringCoercion,
  toNumber as toNumberCoercion,
  toStringValue as toStringCoercion,
  toTimeDate as toTimeDateCoercion,
} from "./coercion";
import type { AnyColumn } from "./columns";
import { createClientValidationError, createDecodeError } from "./errors";
import { normalizeClickHouseTypeLiteral } from "./internal/clickhouse-type";
import {
  type CountMode,
  type CountModeResult,
  type CountSqlType,
  getCountDecoder,
  getCountSqlType,
  wrapCountSql,
} from "./internal/count";
import { type DecimalParams, formatDecimalSqlType, parseDecimalSqlType } from "./internal/decimal";
import { assertValidSqlIdentifier } from "./internal/identifier";
import { createTableFunctionSource } from "./query";
import {
  type BuildContext,
  compileValue,
  createExpression,
  type Decoder,
  ensureExpression,
  type InferData,
  isExpression,
  joinSqlParts,
  passThroughDecoder,
  type Selection,
} from "./query-shared";
import { isSqlFragment, type SQLFragment, sql } from "./sql";

/* ── shared helpers ────────────────────────────────────────────── */

export type JsonPathSegment = string | number | bigint;

const compileFunctionCall = (name: string, args: readonly unknown[], ctx: BuildContext) => {
  assertValidSqlIdentifier(name, "function");
  const compiledArgs = args.map((argument) => compileValue(argument, ctx));
  return sql`${sql.raw(name)}(${joinSqlParts(compiledArgs, ", ")})`;
};

const createFunctionExpression = <TData>(
  name: string,
  args: readonly unknown[],
  config?: {
    decoder?: Decoder<TData>;
    sqlType?: string;
  },
): Selection<TData> => {
  return createExpression({
    compile: (ctx) => compileFunctionCall(name, args, ctx),
    decoder: config?.decoder ?? (passThroughDecoder as Decoder<TData>),
    sqlType: config?.sqlType,
  });
};

const createParameterizedFunctionExpression = <TData>(
  name: string,
  parameters: readonly unknown[],
  args: readonly unknown[],
  config?: {
    decoder?: Decoder<TData>;
    sqlType?: string;
  },
): Selection<TData> => {
  return createExpression({
    compile: (ctx) => {
      assertValidSqlIdentifier(name, "function");
      const compiledParameters = parameters.map((argument) => compileValue(argument, ctx));
      const compiledArgs = args.map((argument) => compileValue(argument, ctx));
      return sql`${sql.raw(name)}(${joinSqlParts(compiledParameters, ", ")})(${joinSqlParts(compiledArgs, ", ")})`;
    },
    decoder: config?.decoder ?? (passThroughDecoder as Decoder<TData>),
    sqlType: config?.sqlType,
  });
};

const unwrapNullableOrLowCardinalitySqlType = (sqlType?: string): string | undefined => {
  let current = sqlType?.trim();
  while (current) {
    const nullableMatch = current.match(/^Nullable\((.*)\)$/);
    if (nullableMatch) {
      current = nullableMatch[1].trim();
      continue;
    }
    const lowCardinalityMatch = current.match(/^LowCardinality\((.*)\)$/);
    if (lowCardinalityMatch) {
      current = lowCardinalityMatch[1].trim();
      continue;
    }
    return current;
  }
  return current;
};

const isFloatingSqlType = (sqlType?: string) => {
  const unwrapped = unwrapNullableOrLowCardinalitySqlType(sqlType);
  return unwrapped?.startsWith("Float") || unwrapped?.startsWith("BFloat");
};

const isSafeIntegerNumber = (value: unknown): value is number => {
  return typeof value === "number" && Number.isSafeInteger(value);
};

const isNonNegativeIntegerLiteral = (value: unknown) => {
  return (isSafeIntegerNumber(value) && value >= 0) || (typeof value === "bigint" && value >= 0n);
};

const isDecimalFallbackLiteral = (value: unknown) => {
  return (
    typeof value === "string" || typeof value === "bigint" || (typeof value === "number" && Number.isFinite(value))
  );
};

const resolveCoalesceFallbackSqlType = (firstSqlType: string | undefined, fallbackValue: unknown) => {
  if (isExpression(fallbackValue) || isSqlFragment(fallbackValue)) {
    return undefined;
  }

  const unwrapped = unwrapNullableOrLowCardinalitySqlType(firstSqlType);
  if (!unwrapped) {
    return undefined;
  }

  if (isFloatingSqlType(unwrapped) && isSafeIntegerNumber(fallbackValue)) {
    return unwrapped;
  }

  if (unwrapped === "UInt64" && isNonNegativeIntegerLiteral(fallbackValue)) {
    return "UInt64";
  }

  const decimalParams = parseDecimalSqlType(unwrapped);
  if (decimalParams && isDecimalFallbackLiteral(fallbackValue)) {
    return formatDecimalSqlType(decimalParams);
  }

  return undefined;
};

const numberDecoder: Decoder<number> = toNumberCoercion;
const stringDecoder: Decoder<string> = toStringCoercion;
const dateDecoder: Decoder<Date> = toDateCoercion;
const timeDecoder: Decoder<Date> = toTimeDateCoercion;
const booleanDecoder: Decoder<boolean> = toBooleanCoercion;

const nullableDecoder =
  <TData>(decoder: Decoder<TData>): Decoder<TData | null> =>
  (value) =>
    value === null ? null : decoder(value);

const createIntegerNumberDecoder =
  (min: number, max: number): Decoder<number> =>
  (value) =>
    toIntegerNumberCoercion(value, { min, max });

const int8Decoder = createIntegerNumberDecoder(-128, 127);
const int16Decoder = createIntegerNumberDecoder(-32768, 32767);
const int32Decoder = createIntegerNumberDecoder(-2147483648, 2147483647);
const uint8Decoder = createIntegerNumberDecoder(0, 255);
const uint16Decoder = createIntegerNumberDecoder(0, 65535);
const uint32Decoder = createIntegerNumberDecoder(0, 4294967295);
const intStringDecoder: Decoder<string> = (value) => toIntegerStringCoercion(value);
const uintStringDecoder: Decoder<string> = (value) => toIntegerStringCoercion(value, { unsigned: true });

const FIXED_WIDTH_DECIMAL_PRECISION = {
  toDecimal32: 9,
  toDecimal64: 18,
  toDecimal128: 38,
  toDecimal256: 76,
  toDecimal32OrZero: 9,
  toDecimal64OrZero: 18,
  toDecimal128OrZero: 38,
  toDecimal256OrZero: 76,
  toDecimal32OrNull: 9,
  toDecimal64OrNull: 18,
  toDecimal128OrNull: 38,
  toDecimal256OrNull: 76,
  toDecimal32OrDefault: 9,
  toDecimal64OrDefault: 18,
  toDecimal128OrDefault: 38,
  toDecimal256OrDefault: 76,
} as const;

type FixedWidthDecimalName = keyof typeof FIXED_WIDTH_DECIMAL_PRECISION;

const assertDateTime64Scale: (scale: unknown) => asserts scale is number = (scale) => {
  if (typeof scale !== "number" || !Number.isInteger(scale) || scale < 0 || scale > 9) {
    throw createClientValidationError(`toDateTime64 scale must be an integer between 0 and 9, got ${String(scale)}`);
  }
};

const assertNonNegativeInteger: (label: string, value: unknown) => asserts value is number = (label, value) => {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw createClientValidationError(`${label} must be a non-negative integer, got ${String(value)}`);
  }
};

const assertPositiveInteger: (label: string, value: unknown) => asserts value is number = (label, value) => {
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw createClientValidationError(`${label} must be a positive integer, got ${String(value)}`);
  }
};

const escapeSqlSingleQuoted = (value: string) => value.replaceAll("\\", "\\\\").replaceAll("'", "\\'");

const createStringLiteral = (label: string, value: unknown): SQLFragment => {
  if (typeof value !== "string" || value.length === 0) {
    throw createClientValidationError(`${label} must be a non-empty string, got ${String(value)}`);
  }
  return sql.raw(`'${escapeSqlSingleQuoted(value)}'`);
};

const createTupleElementIndexLiteral = (value: unknown): SQLFragment => {
  if (typeof value === "string") {
    if (value.length === 0) {
      throw createClientValidationError(
        `tupleElement indexOrName must be a positive safe integer or non-empty string, got ${String(value)}`,
      );
    }
    return createStringLiteral("tupleElement indexOrName", value);
  }

  if (typeof value === "number") {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw createClientValidationError(
        `tupleElement indexOrName must be a positive safe integer or non-empty string, got ${String(value)}`,
      );
    }
    return sql.raw(String(value));
  }

  if (typeof value === "bigint") {
    if (value <= 0n) {
      throw createClientValidationError(
        `tupleElement indexOrName must be a positive safe integer or non-empty string, got ${String(value)}`,
      );
    }
    return sql.raw(value.toString());
  }

  throw createClientValidationError(
    `tupleElement indexOrName must be a positive safe integer or non-empty string, got ${String(value)}`,
  );
};

const createConstIntegerLiteral = (
  helperName: string,
  value: unknown,
  options: { positive?: boolean; nonNegative?: boolean },
): SQLFragment => {
  const assertBounds = (raw: number | bigint): void => {
    if (options.positive && raw <= 0) {
      throw createClientValidationError(`${helperName} expects a positive integer constant, got ${String(value)}`);
    }
    if (options.nonNegative && raw < 0) {
      throw createClientValidationError(`${helperName} expects a non-negative integer constant, got ${String(value)}`);
    }
  };

  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) {
      throw createClientValidationError(`${helperName} expects a safe integer constant, got ${String(value)}`);
    }
    assertBounds(value);
    return sql.raw(String(value));
  }

  if (typeof value === "bigint") {
    assertBounds(value);
    return sql.raw(value.toString());
  }

  if (isSqlFragment(value)) {
    return value;
  }

  throw createClientValidationError(`${helperName} requires a ClickHouse constant integer, got ${String(value)}`);
};

const createTypeLiteral = (value: unknown): SQLFragment => {
  return sql.raw(normalizeClickHouseTypeLiteral(value));
};

const INTERVAL_UNITS = new Set([
  "nanosecond",
  "microsecond",
  "millisecond",
  "second",
  "minute",
  "hour",
  "day",
  "week",
  "month",
  "quarter",
  "year",
]);

const createIntervalUnitLiteral = (value: unknown): SQLFragment => {
  const unitLiteral = createStringLiteral("toInterval unit", value);
  if (typeof value !== "string" || !INTERVAL_UNITS.has(value.toLowerCase())) {
    throw createClientValidationError(`toInterval unit must be a valid ClickHouse interval unit, got ${String(value)}`);
  }
  return unitLiteral;
};

const isDecimalColumnLike = (value: unknown): value is { decimalConfig: DecimalParams } => {
  return (
    typeof value === "object" &&
    value !== null &&
    "decimalConfig" in value &&
    typeof (value as { decimalConfig?: unknown }).decimalConfig === "object" &&
    (value as { decimalConfig?: unknown }).decimalConfig !== null
  );
};

const resolveDecimalParams = (expression: unknown): DecimalParams | undefined => {
  if (isDecimalColumnLike(expression)) {
    return expression.decimalConfig;
  }
  if (typeof expression !== "object" || expression === null || !("sqlType" in expression)) return undefined;
  const sqlType = (expression as { sqlType?: unknown }).sqlType;
  return typeof sqlType === "string" ? parseDecimalSqlType(sqlType) : undefined;
};

const widenForSum = (params: DecimalParams): DecimalParams => {
  return {
    precision: Math.min(76, Math.max(38, params.precision)),
    scale: params.scale,
  };
};

const createFixedWidthDecimalCast = <TData = string>(
  fnName: FixedWidthDecimalName,
  expression: unknown,
  scale: unknown,
  config?: {
    readonly decoder?: Decoder<TData>;
    readonly extraArgs?: readonly unknown[];
  },
): Selection<TData> => {
  const precision = FIXED_WIDTH_DECIMAL_PRECISION[fnName];
  if (typeof scale !== "number" || !Number.isInteger(scale) || scale < 0 || scale > precision) {
    throw createClientValidationError(
      `${fnName} scale must be an integer between 0 and ${precision} (the ${fnName} fixed width), got ${String(scale)}`,
    );
  }
  // ClickHouse `toDecimalNN(expr, scale)` requires a UInt8 literal for `scale`,
  // so we inline it instead of binding through the parameter channel (which
  // would produce `{orm_paramN:Int64}` and trigger ILLEGAL_TYPE_OF_ARGUMENT).
  const sqlType = `Decimal(${precision}, ${scale})`;
  return createExpression<TData>({
    compile: (ctx) => {
      assertValidSqlIdentifier(fnName, "function");
      const compiledArgs = [
        compileValue(expression, ctx),
        sql.raw(String(scale)),
        ...(config?.extraArgs ?? []).map((argument) => compileValue(argument, ctx)),
      ];
      return sql`${sql.raw(fnName)}(${joinSqlParts(compiledArgs, ", ")})`;
    },
    decoder: config?.decoder ?? (stringDecoder as Decoder<TData>),
    sqlType,
  });
};

const createDateTime64Cast = (expression: unknown, scale: unknown, timezone?: unknown): Selection<Date> => {
  assertDateTime64Scale(scale);
  return createExpression<Date>({
    compile: (ctx) => {
      assertValidSqlIdentifier("toDateTime64", "function");
      const compiledArgs =
        timezone === undefined
          ? [compileValue(expression, ctx), sql.raw(String(scale))]
          : [compileValue(expression, ctx), sql.raw(String(scale)), compileValue(timezone, ctx)];
      return sql`toDateTime64(${joinSqlParts(compiledArgs, ", ")})`;
    },
    decoder: dateDecoder,
    sqlType: `DateTime64(${scale})`,
  });
};

const arrayDecoder = <TData>(label: string): Decoder<TData[]> => {
  return (value) => {
    if (!Array.isArray(value)) {
      throw createDecodeError(`Cannot convert value to ${label} array: ${String(value)}`, value);
    }
    return value as TData[];
  };
};

const resolveAggregateDecoder = (expression?: unknown): Decoder<number | string> => {
  const wrapped = expression ? ensureExpression(expression) : undefined;
  if (wrapped && isFloatingSqlType(wrapped.sqlType)) {
    return numberDecoder;
  }
  return stringDecoder;
};

type DecimalAwareAggregateConfig = {
  /**
   * Whether the aggregate may widen the precision (e.g. SUM accumulates
   * across rows so we promote the precision up to 38). MIN/MAX preserve
   * the column precision verbatim.
   */
  readonly widenForSum?: boolean;
};

const buildDecimalAwareAggregate = (
  name: string,
  expression: unknown,
  config?: DecimalAwareAggregateConfig,
): Selection<string> | undefined => {
  const params = resolveDecimalParams(expression);
  if (!params) return undefined;
  const target = config?.widenForSum ? widenForSum(params) : params;
  const sqlType = `Decimal(${target.precision}, ${target.scale})`;
  return createExpression<string>({
    compile: (ctx) => {
      assertValidSqlIdentifier(name, "function");
      const compiledArg = compileValue(expression, ctx);
      return sql`CAST(${sql.raw(name)}(${compiledArg}) AS ${sql.raw(sqlType)})`;
    },
    decoder: stringDecoder,
    sqlType,
  });
};

const buildDecimalAwareSumIf = (expression: unknown, condition: unknown): Selection<string> | undefined => {
  const params = resolveDecimalParams(expression);
  if (!params) return undefined;
  const target = widenForSum(params);
  const sqlType = `Decimal(${target.precision}, ${target.scale})`;
  return createExpression<string>({
    compile: (ctx) => {
      assertValidSqlIdentifier("sumIf", "function");
      const compiledExpr = compileValue(expression, ctx);
      const compiledCond = compileValue(condition, ctx);
      return sql`CAST(sumIf(${compiledExpr}, ${compiledCond}) AS ${sql.raw(sqlType)})`;
    },
    decoder: stringDecoder,
    sqlType,
  });
};

const createArrayExpression = <TData>(
  name: string,
  args: readonly unknown[],
  sqlType = "Array",
): Selection<TData[]> => {
  return createFunctionExpression<TData[]>(name, args, {
    decoder: arrayDecoder<TData>(name),
    sqlType,
  });
};

const createBooleanExpression = (name: string, args: readonly unknown[]): Selection<boolean> => {
  return createFunctionExpression(name, args, {
    decoder: booleanDecoder,
    sqlType: "Bool",
  });
};

const createNumberExpression = (name: string, args: readonly unknown[], sqlType = "Float64"): Selection<number> => {
  return createFunctionExpression(name, args, {
    decoder: numberDecoder,
    sqlType,
  });
};

const createCoalesceExpression = <TData>(args: readonly unknown[]): Selection<TData> => {
  const firstExpression = args.length > 0 ? ensureExpression<TData>(args[0]) : undefined;
  return createExpression({
    compile: (ctx) => {
      assertValidSqlIdentifier("coalesce", "function");
      const compiledArgs = args.map((argument, index) =>
        compileValue(
          argument,
          ctx,
          index === 0 ? undefined : resolveCoalesceFallbackSqlType(firstExpression?.sqlType, argument),
        ),
      );
      return sql`coalesce(${joinSqlParts(compiledArgs, ", ")})`;
    },
    decoder: firstExpression?.decoder ?? (passThroughDecoder as Decoder<TData>),
    sqlType: firstExpression?.sqlType,
  });
};

const createUInt32Expression = (name: string, args: readonly unknown[]): Selection<number> => {
  return createNumberExpression(name, args, "UInt32");
};

const createUInt64Expression = (name: string, args: readonly unknown[]): Selection<string> => {
  return createFunctionExpression(name, args, {
    decoder: stringDecoder,
    sqlType: "UInt64",
  });
};

const createInt64Expression = (name: string, args: readonly unknown[]): Selection<string> => {
  return createFunctionExpression(name, args, {
    decoder: stringDecoder,
    sqlType: "Int64",
  });
};

const createTypedConversionExpression = <TData>(
  name: string,
  args: readonly unknown[],
  decoder: Decoder<TData>,
  sqlType?: string,
): Selection<TData> => {
  return createFunctionExpression(name, args, {
    decoder,
    sqlType,
  });
};

const createNullableTypedConversionExpression = <TData>(
  name: string,
  args: readonly unknown[],
  decoder: Decoder<TData>,
  sqlType?: string,
): Selection<TData | null> => {
  return createTypedConversionExpression(
    name,
    args,
    nullableDecoder(decoder),
    sqlType ? `Nullable(${sqlType})` : undefined,
  );
};

const createCastExpression = <TData>(
  expression: unknown,
  targetType: string,
  decoder?: Decoder<TData>,
): Selection<TData> => {
  const normalizedTargetType = normalizeClickHouseTypeLiteral(targetType);
  return createExpression<TData>({
    compile: (ctx) => {
      const compiledExpr = compileValue(expression, ctx);
      return sql`CAST(${compiledExpr} AS ${createTypeLiteral(normalizedTargetType)})`;
    },
    decoder: decoder ?? (passThroughDecoder as Decoder<TData>),
    sqlType: normalizedTargetType,
  });
};

const createTypeStringFunctionExpression = <TData>(
  name: string,
  expression: unknown,
  targetType: string,
  config?: {
    readonly decoder?: Decoder<TData>;
    readonly nullable?: boolean;
    readonly extraArgs?: readonly unknown[];
  },
): Selection<TData> => {
  const normalizedTargetType = normalizeClickHouseTypeLiteral(targetType);
  return createExpression<TData>({
    compile: (ctx) => {
      assertValidSqlIdentifier(name, "function");
      const compiledArgs = [
        compileValue(expression, ctx),
        createStringLiteral(`${name} type`, normalizedTargetType),
        ...(config?.extraArgs ?? []).map((argument) => compileValue(argument, ctx)),
      ];
      return sql`${sql.raw(name)}(${joinSqlParts(compiledArgs, ", ")})`;
    },
    decoder: config?.decoder ?? (passThroughDecoder as Decoder<TData>),
    sqlType: config?.nullable ? `Nullable(${normalizedTargetType})` : normalizedTargetType,
  });
};

const createFixedStringExpression = (expression: unknown, length: unknown): Selection<string> => {
  assertPositiveInteger("toFixedString length", length);
  return createExpression<string>({
    compile: (ctx) => {
      const compiledExpr = compileValue(expression, ctx);
      return sql`toFixedString(${compiledExpr}, ${sql.raw(String(length))})`;
    },
    decoder: stringDecoder,
    sqlType: `FixedString(${length})`,
  });
};

const createDateTime64ConversionExpression = <TData>(
  name: string,
  expression: unknown,
  scale: unknown,
  timezone: unknown | undefined,
  defaultValue: unknown | undefined,
  decoder: Decoder<TData>,
): Selection<TData> => {
  assertDateTime64Scale(scale);
  return createExpression<TData>({
    compile: (ctx) => {
      const compiledArgs = [
        compileValue(expression, ctx),
        sql.raw(String(scale)),
        ...(timezone === undefined ? [] : [compileValue(timezone, ctx)]),
        ...(defaultValue === undefined ? [] : [compileValue(defaultValue, ctx)]),
      ];
      return sql`${sql.raw(name)}(${joinSqlParts(compiledArgs, ", ")})`;
    },
    decoder,
    sqlType: `DateTime64(${scale})`,
  });
};

const createTime64Expression = <TData>(
  name: string,
  expression: unknown,
  precision: unknown,
  decoder: Decoder<TData>,
): Selection<TData> => {
  assertDateTime64Scale(precision);
  return createExpression<TData>({
    compile: (ctx) => {
      const compiledExpr = compileValue(expression, ctx);
      return sql`${sql.raw(name)}(${compiledExpr}, ${sql.raw(String(precision))})`;
    },
    decoder,
    sqlType: `Time64(${precision})`,
  });
};

const createDateTime64ParseExpression = <TData>(
  name: string,
  leadingArgs: readonly unknown[],
  precision: unknown | undefined,
  timezone: unknown | undefined,
  decoder: Decoder<TData>,
): Selection<TData> => {
  if (precision !== undefined) assertDateTime64Scale(precision);
  return createExpression<TData>({
    compile: (ctx) => {
      const compiledArgs = [
        ...leadingArgs.map((argument) => compileValue(argument, ctx)),
        ...(precision === undefined ? [] : [sql.raw(String(precision))]),
        ...(timezone === undefined ? [] : [compileValue(timezone, ctx)]),
      ];
      return sql`${sql.raw(name)}(${joinSqlParts(compiledArgs, ", ")})`;
    },
    decoder,
    sqlType: precision === undefined ? "DateTime64" : `DateTime64(${precision})`,
  });
};

const createDateTime64BestEffortExpression = <TData>(
  name: string,
  expression: unknown,
  precisionOrTimezone: unknown | undefined,
  timezone: unknown | undefined,
  decoder: Decoder<TData>,
): Selection<TData> => {
  const precision = typeof precisionOrTimezone === "number" ? precisionOrTimezone : undefined;
  const resolvedTimezone = precision === undefined ? precisionOrTimezone : timezone;
  return createDateTime64ParseExpression(name, [expression], precision, resolvedTimezone, decoder);
};

const createIntervalExpression = (name: string, value: unknown, unit?: unknown): Selection<unknown> => {
  if (unit !== undefined) {
    createIntervalUnitLiteral(unit);
  }
  return createExpression<unknown>({
    compile: (ctx) => {
      const compiledArgs =
        unit === undefined ? [compileValue(value, ctx)] : [compileValue(value, ctx), createIntervalUnitLiteral(unit)];
      return sql`${sql.raw(name)}(${joinSqlParts(compiledArgs, ", ")})`;
    },
    decoder: passThroughDecoder,
    sqlType: "Interval",
  });
};

const withOptional = (args: readonly unknown[], optional: unknown | undefined): readonly unknown[] => {
  return optional === undefined ? args : [...args, optional];
};

function createFromUnixTimestampExpression(timestamp: unknown): Selection<Date>;
function createFromUnixTimestampExpression(timestamp: unknown, format: unknown, timezone?: unknown): Selection<string>;
function createFromUnixTimestampExpression(
  timestamp: unknown,
  format?: unknown,
  timezone?: unknown,
): Selection<Date | string> {
  if (format === undefined) {
    return createFunctionExpression("fromUnixTimestamp", [timestamp], {
      decoder: dateDecoder,
      sqlType: "DateTime",
    });
  }
  return createFunctionExpression("fromUnixTimestamp", withOptional([timestamp, format], timezone), {
    decoder: stringDecoder,
    sqlType: "String",
  });
}

const createJsonExtractExpression = <TColumn extends AnyColumn>(
  json: unknown,
  returnType: TColumn,
  path: readonly JsonPathSegment[],
): Selection<InferData<TColumn>> => {
  return createExpression({
    compile: (ctx) => compileFunctionCall("JSONExtract", [json, ...path, returnType.sqlType], ctx),
    decoder: (value) => returnType.mapFromDriverValue(value) as InferData<TColumn>,
    sqlType: returnType.sqlType,
  });
};

/* ── scalar functions ──────────────────────────────────────────── */

const scalarFns = {
  call<TData = unknown>(name: string, ...args: unknown[]): Selection<TData> {
    return createFunctionExpression<TData>(name, args);
  },
  withParams<TData = unknown>(name: string, parameters: readonly unknown[], ...args: unknown[]): Selection<TData> {
    return createParameterizedFunctionExpression<TData>(name, parameters, args);
  },
  jsonExtract<TColumn extends AnyColumn>(
    json: unknown,
    returnType: TColumn,
    ...path: JsonPathSegment[]
  ): Selection<InferData<TColumn>> {
    return createJsonExtractExpression(json, returnType, path);
  },
  cast<TData = unknown>(expression: unknown, targetType: string): Selection<TData> {
    return createCastExpression<TData>(expression, targetType);
  },
  date(expression: unknown): Selection<Date> {
    return createTypedConversionExpression("DATE", [expression], dateDecoder, "Date");
  },
  accurateCast<TData = unknown>(expression: unknown, targetType: string): Selection<TData> {
    return createTypeStringFunctionExpression<TData>("accurateCast", expression, targetType);
  },
  accurateCastOrDefault<TData = unknown>(
    expression: unknown,
    targetType: string,
    defaultValue?: unknown,
  ): Selection<TData> {
    return createTypeStringFunctionExpression<TData>("accurateCastOrDefault", expression, targetType, {
      extraArgs: defaultValue === undefined ? [] : [defaultValue],
    });
  },
  accurateCastOrNull<TData = unknown>(expression: unknown, targetType: string): Selection<TData | null> {
    return createTypeStringFunctionExpression<TData | null>("accurateCastOrNull", expression, targetType, {
      decoder: nullableDecoder(passThroughDecoder as Decoder<TData>),
      nullable: true,
    });
  },
  formatRow(format: unknown, ...args: unknown[]): Selection<string> {
    return createTypedConversionExpression("formatRow", [format, ...args], stringDecoder, "String");
  },
  formatRowNoNewline(format: unknown, ...args: unknown[]): Selection<string> {
    return createTypedConversionExpression("formatRowNoNewline", [format, ...args], stringDecoder, "String");
  },
  formatDateTime(expression: unknown, format: unknown, timezone?: unknown): Selection<string> {
    return createTypedConversionExpression(
      "formatDateTime",
      withOptional([expression, format], timezone),
      stringDecoder,
      "String",
    );
  },
  parseDateTime(expression: unknown, format: unknown, timezone?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "parseDateTime",
      withOptional([expression, format], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTimeOrNull(expression: unknown, format: unknown, timezone?: unknown): Selection<Date | null> {
    return createNullableTypedConversionExpression(
      "parseDateTimeOrNull",
      withOptional([expression, format], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTimeOrZero(expression: unknown, format: unknown, timezone?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "parseDateTimeOrZero",
      withOptional([expression, format], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTimeBestEffort(expression: unknown, timezone?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "parseDateTimeBestEffort",
      withOptional([expression], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTimeBestEffortOrNull(expression: unknown, timezone?: unknown): Selection<Date | null> {
    return createNullableTypedConversionExpression(
      "parseDateTimeBestEffortOrNull",
      withOptional([expression], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTimeBestEffortOrZero(expression: unknown, timezone?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "parseDateTimeBestEffortOrZero",
      withOptional([expression], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTimeBestEffortUS(expression: unknown, timezone?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "parseDateTimeBestEffortUS",
      withOptional([expression], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTimeBestEffortUSOrNull(expression: unknown, timezone?: unknown): Selection<Date | null> {
    return createNullableTypedConversionExpression(
      "parseDateTimeBestEffortUSOrNull",
      withOptional([expression], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTimeBestEffortUSOrZero(expression: unknown, timezone?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "parseDateTimeBestEffortUSOrZero",
      withOptional([expression], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTimeInJodaSyntax(expression: unknown, format: unknown, timezone?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "parseDateTimeInJodaSyntax",
      withOptional([expression, format], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTimeInJodaSyntaxOrNull(expression: unknown, format: unknown, timezone?: unknown): Selection<Date | null> {
    return createNullableTypedConversionExpression(
      "parseDateTimeInJodaSyntaxOrNull",
      withOptional([expression, format], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTimeInJodaSyntaxOrZero(expression: unknown, format: unknown, timezone?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "parseDateTimeInJodaSyntaxOrZero",
      withOptional([expression, format], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTime32BestEffort(expression: unknown, timezone?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "parseDateTime32BestEffort",
      withOptional([expression], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTime32BestEffortOrNull(expression: unknown, timezone?: unknown): Selection<Date | null> {
    return createNullableTypedConversionExpression(
      "parseDateTime32BestEffortOrNull",
      withOptional([expression], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTime32BestEffortOrZero(expression: unknown, timezone?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "parseDateTime32BestEffortOrZero",
      withOptional([expression], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  parseDateTime64(expression: unknown, format: unknown, timezone?: unknown): Selection<Date> {
    return createDateTime64ParseExpression("parseDateTime64", [expression, format], undefined, timezone, dateDecoder);
  },
  parseDateTime64OrNull(expression: unknown, format: unknown, timezone?: unknown): Selection<Date | null> {
    return createDateTime64ParseExpression(
      "parseDateTime64OrNull",
      [expression, format],
      undefined,
      timezone,
      nullableDecoder(dateDecoder),
    );
  },
  parseDateTime64OrZero(expression: unknown, format: unknown, timezone?: unknown): Selection<Date> {
    return createDateTime64ParseExpression(
      "parseDateTime64OrZero",
      [expression, format],
      undefined,
      timezone,
      dateDecoder,
    );
  },
  parseDateTime64BestEffort(expression: unknown, precisionOrTimezone?: unknown, timezone?: unknown): Selection<Date> {
    return createDateTime64BestEffortExpression(
      "parseDateTime64BestEffort",
      expression,
      precisionOrTimezone,
      timezone,
      dateDecoder,
    );
  },
  parseDateTime64BestEffortOrNull(
    expression: unknown,
    precisionOrTimezone?: unknown,
    timezone?: unknown,
  ): Selection<Date | null> {
    return createDateTime64BestEffortExpression(
      "parseDateTime64BestEffortOrNull",
      expression,
      precisionOrTimezone,
      timezone,
      nullableDecoder(dateDecoder),
    );
  },
  parseDateTime64BestEffortOrZero(
    expression: unknown,
    precisionOrTimezone?: unknown,
    timezone?: unknown,
  ): Selection<Date> {
    return createDateTime64BestEffortExpression(
      "parseDateTime64BestEffortOrZero",
      expression,
      precisionOrTimezone,
      timezone,
      dateDecoder,
    );
  },
  parseDateTime64BestEffortUS(expression: unknown, precisionOrTimezone?: unknown, timezone?: unknown): Selection<Date> {
    return createDateTime64BestEffortExpression(
      "parseDateTime64BestEffortUS",
      expression,
      precisionOrTimezone,
      timezone,
      dateDecoder,
    );
  },
  parseDateTime64BestEffortUSOrNull(
    expression: unknown,
    precisionOrTimezone?: unknown,
    timezone?: unknown,
  ): Selection<Date | null> {
    return createDateTime64BestEffortExpression(
      "parseDateTime64BestEffortUSOrNull",
      expression,
      precisionOrTimezone,
      timezone,
      nullableDecoder(dateDecoder),
    );
  },
  parseDateTime64BestEffortUSOrZero(
    expression: unknown,
    precisionOrTimezone?: unknown,
    timezone?: unknown,
  ): Selection<Date> {
    return createDateTime64BestEffortExpression(
      "parseDateTime64BestEffortUSOrZero",
      expression,
      precisionOrTimezone,
      timezone,
      dateDecoder,
    );
  },
  parseDateTime64InJodaSyntax(expression: unknown, format: unknown, timezone?: unknown): Selection<Date> {
    return createDateTime64ParseExpression(
      "parseDateTime64InJodaSyntax",
      [expression, format],
      undefined,
      timezone,
      dateDecoder,
    );
  },
  parseDateTime64InJodaSyntaxOrNull(expression: unknown, format: unknown, timezone?: unknown): Selection<Date | null> {
    return createDateTime64ParseExpression(
      "parseDateTime64InJodaSyntaxOrNull",
      [expression, format],
      undefined,
      timezone,
      nullableDecoder(dateDecoder),
    );
  },
  parseDateTime64InJodaSyntaxOrZero(expression: unknown, format: unknown, timezone?: unknown): Selection<Date> {
    return createDateTime64ParseExpression(
      "parseDateTime64InJodaSyntaxOrZero",
      [expression, format],
      undefined,
      timezone,
      dateDecoder,
    );
  },
  reinterpret<TData = unknown>(expression: unknown, targetType: string): Selection<TData> {
    return createTypeStringFunctionExpression<TData>("reinterpret", expression, targetType);
  },
  reinterpretAsDate(expression: unknown): Selection<Date> {
    return createTypedConversionExpression("reinterpretAsDate", [expression], dateDecoder, "Date");
  },
  reinterpretAsDateTime(expression: unknown): Selection<Date> {
    return createTypedConversionExpression("reinterpretAsDateTime", [expression], dateDecoder, "DateTime");
  },
  reinterpretAsFixedString(expression: unknown): Selection<string> {
    return createTypedConversionExpression("reinterpretAsFixedString", [expression], stringDecoder, "FixedString");
  },
  reinterpretAsFloat32(expression: unknown): Selection<number> {
    return createTypedConversionExpression("reinterpretAsFloat32", [expression], numberDecoder, "Float32");
  },
  reinterpretAsFloat64(expression: unknown): Selection<number> {
    return createTypedConversionExpression("reinterpretAsFloat64", [expression], numberDecoder, "Float64");
  },
  reinterpretAsInt8(expression: unknown): Selection<number> {
    return createTypedConversionExpression("reinterpretAsInt8", [expression], int8Decoder, "Int8");
  },
  reinterpretAsInt16(expression: unknown): Selection<number> {
    return createTypedConversionExpression("reinterpretAsInt16", [expression], int16Decoder, "Int16");
  },
  reinterpretAsInt32(expression: unknown): Selection<number> {
    return createTypedConversionExpression("reinterpretAsInt32", [expression], int32Decoder, "Int32");
  },
  reinterpretAsInt64(expression: unknown): Selection<string> {
    return createTypedConversionExpression("reinterpretAsInt64", [expression], intStringDecoder, "Int64");
  },
  reinterpretAsInt128(expression: unknown): Selection<string> {
    return createTypedConversionExpression("reinterpretAsInt128", [expression], intStringDecoder, "Int128");
  },
  reinterpretAsInt256(expression: unknown): Selection<string> {
    return createTypedConversionExpression("reinterpretAsInt256", [expression], intStringDecoder, "Int256");
  },
  reinterpretAsString(expression: unknown): Selection<string> {
    return createTypedConversionExpression("reinterpretAsString", [expression], stringDecoder, "String");
  },
  reinterpretAsUInt8(expression: unknown): Selection<number> {
    return createTypedConversionExpression("reinterpretAsUInt8", [expression], uint8Decoder, "UInt8");
  },
  reinterpretAsUInt16(expression: unknown): Selection<number> {
    return createTypedConversionExpression("reinterpretAsUInt16", [expression], uint16Decoder, "UInt16");
  },
  reinterpretAsUInt32(expression: unknown): Selection<number> {
    return createTypedConversionExpression("reinterpretAsUInt32", [expression], uint32Decoder, "UInt32");
  },
  reinterpretAsUInt64(expression: unknown): Selection<string> {
    return createTypedConversionExpression("reinterpretAsUInt64", [expression], uintStringDecoder, "UInt64");
  },
  reinterpretAsUInt128(expression: unknown): Selection<string> {
    return createTypedConversionExpression("reinterpretAsUInt128", [expression], uintStringDecoder, "UInt128");
  },
  reinterpretAsUInt256(expression: unknown): Selection<string> {
    return createTypedConversionExpression("reinterpretAsUInt256", [expression], uintStringDecoder, "UInt256");
  },
  reinterpretAsUUID(expression: unknown): Selection<string> {
    return createTypedConversionExpression("reinterpretAsUUID", [expression], stringDecoder, "UUID");
  },
  toString(expression: unknown, timezone?: unknown): Selection<string> {
    const args = timezone === undefined ? [expression] : [expression, timezone];
    return createFunctionExpression("toString", args, {
      decoder: stringDecoder,
      sqlType: "String",
    });
  },
  toStringCutToZero(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toStringCutToZero", [expression], stringDecoder, "String");
  },
  toBFloat16(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toBFloat16", [expression], numberDecoder, "BFloat16");
  },
  toBFloat16OrNull(expression: unknown): Selection<number | null> {
    return createNullableTypedConversionExpression("toBFloat16OrNull", [expression], numberDecoder, "BFloat16");
  },
  toBFloat16OrZero(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toBFloat16OrZero", [expression], numberDecoder, "BFloat16");
  },
  toBool(expression: unknown): Selection<boolean> {
    return createTypedConversionExpression("toBool", [expression], booleanDecoder, "Bool");
  },
  toInt8(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toInt8", [expression], int8Decoder, "Int8");
  },
  toInt8OrDefault(expression: unknown, defaultValue?: unknown): Selection<number> {
    return createTypedConversionExpression(
      "toInt8OrDefault",
      withOptional([expression], defaultValue),
      int8Decoder,
      "Int8",
    );
  },
  toInt8OrNull(expression: unknown): Selection<number | null> {
    return createNullableTypedConversionExpression("toInt8OrNull", [expression], int8Decoder, "Int8");
  },
  toInt8OrZero(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toInt8OrZero", [expression], int8Decoder, "Int8");
  },
  toInt16(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toInt16", [expression], int16Decoder, "Int16");
  },
  toInt16OrDefault(expression: unknown, defaultValue?: unknown): Selection<number> {
    return createTypedConversionExpression(
      "toInt16OrDefault",
      withOptional([expression], defaultValue),
      int16Decoder,
      "Int16",
    );
  },
  toInt16OrNull(expression: unknown): Selection<number | null> {
    return createNullableTypedConversionExpression("toInt16OrNull", [expression], int16Decoder, "Int16");
  },
  toInt16OrZero(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toInt16OrZero", [expression], int16Decoder, "Int16");
  },
  toInt32(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toInt32", [expression], int32Decoder, "Int32");
  },
  toInt32OrDefault(expression: unknown, defaultValue?: unknown): Selection<number> {
    return createTypedConversionExpression(
      "toInt32OrDefault",
      withOptional([expression], defaultValue),
      int32Decoder,
      "Int32",
    );
  },
  toInt32OrNull(expression: unknown): Selection<number | null> {
    return createNullableTypedConversionExpression("toInt32OrNull", [expression], int32Decoder, "Int32");
  },
  toInt32OrZero(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toInt32OrZero", [expression], int32Decoder, "Int32");
  },
  toInt64(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toInt64", [expression], intStringDecoder, "Int64");
  },
  toInt64OrDefault(expression: unknown, defaultValue?: unknown): Selection<string> {
    return createTypedConversionExpression(
      "toInt64OrDefault",
      withOptional([expression], defaultValue),
      intStringDecoder,
      "Int64",
    );
  },
  toInt64OrNull(expression: unknown): Selection<string | null> {
    return createNullableTypedConversionExpression("toInt64OrNull", [expression], intStringDecoder, "Int64");
  },
  toInt64OrZero(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toInt64OrZero", [expression], intStringDecoder, "Int64");
  },
  toInt128(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toInt128", [expression], intStringDecoder, "Int128");
  },
  toInt128OrDefault(expression: unknown, defaultValue?: unknown): Selection<string> {
    return createTypedConversionExpression(
      "toInt128OrDefault",
      withOptional([expression], defaultValue),
      intStringDecoder,
      "Int128",
    );
  },
  toInt128OrNull(expression: unknown): Selection<string | null> {
    return createNullableTypedConversionExpression("toInt128OrNull", [expression], intStringDecoder, "Int128");
  },
  toInt128OrZero(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toInt128OrZero", [expression], intStringDecoder, "Int128");
  },
  toInt256(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toInt256", [expression], intStringDecoder, "Int256");
  },
  toInt256OrDefault(expression: unknown, defaultValue?: unknown): Selection<string> {
    return createTypedConversionExpression(
      "toInt256OrDefault",
      withOptional([expression], defaultValue),
      intStringDecoder,
      "Int256",
    );
  },
  toInt256OrNull(expression: unknown): Selection<string | null> {
    return createNullableTypedConversionExpression("toInt256OrNull", [expression], intStringDecoder, "Int256");
  },
  toInt256OrZero(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toInt256OrZero", [expression], intStringDecoder, "Int256");
  },
  toUInt8(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toUInt8", [expression], uint8Decoder, "UInt8");
  },
  toUInt8OrDefault(expression: unknown, defaultValue?: unknown): Selection<number> {
    return createTypedConversionExpression(
      "toUInt8OrDefault",
      withOptional([expression], defaultValue),
      uint8Decoder,
      "UInt8",
    );
  },
  toUInt8OrNull(expression: unknown): Selection<number | null> {
    return createNullableTypedConversionExpression("toUInt8OrNull", [expression], uint8Decoder, "UInt8");
  },
  toUInt8OrZero(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toUInt8OrZero", [expression], uint8Decoder, "UInt8");
  },
  toUInt16(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toUInt16", [expression], uint16Decoder, "UInt16");
  },
  toUInt16OrDefault(expression: unknown, defaultValue?: unknown): Selection<number> {
    return createTypedConversionExpression(
      "toUInt16OrDefault",
      withOptional([expression], defaultValue),
      uint16Decoder,
      "UInt16",
    );
  },
  toUInt16OrNull(expression: unknown): Selection<number | null> {
    return createNullableTypedConversionExpression("toUInt16OrNull", [expression], uint16Decoder, "UInt16");
  },
  toUInt16OrZero(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toUInt16OrZero", [expression], uint16Decoder, "UInt16");
  },
  toUInt32(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toUInt32", [expression], uint32Decoder, "UInt32");
  },
  toUInt32OrDefault(expression: unknown, defaultValue?: unknown): Selection<number> {
    return createTypedConversionExpression(
      "toUInt32OrDefault",
      withOptional([expression], defaultValue),
      uint32Decoder,
      "UInt32",
    );
  },
  toUInt32OrNull(expression: unknown): Selection<number | null> {
    return createNullableTypedConversionExpression("toUInt32OrNull", [expression], uint32Decoder, "UInt32");
  },
  toUInt32OrZero(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toUInt32OrZero", [expression], uint32Decoder, "UInt32");
  },
  toUInt64(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toUInt64", [expression], uintStringDecoder, "UInt64");
  },
  toUInt64OrDefault(expression: unknown, defaultValue?: unknown): Selection<string> {
    return createTypedConversionExpression(
      "toUInt64OrDefault",
      withOptional([expression], defaultValue),
      uintStringDecoder,
      "UInt64",
    );
  },
  toUInt64OrNull(expression: unknown): Selection<string | null> {
    return createNullableTypedConversionExpression("toUInt64OrNull", [expression], uintStringDecoder, "UInt64");
  },
  toUInt64OrZero(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toUInt64OrZero", [expression], uintStringDecoder, "UInt64");
  },
  toUInt128(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toUInt128", [expression], uintStringDecoder, "UInt128");
  },
  toUInt128OrDefault(expression: unknown, defaultValue?: unknown): Selection<string> {
    return createTypedConversionExpression(
      "toUInt128OrDefault",
      withOptional([expression], defaultValue),
      uintStringDecoder,
      "UInt128",
    );
  },
  toUInt128OrNull(expression: unknown): Selection<string | null> {
    return createNullableTypedConversionExpression("toUInt128OrNull", [expression], uintStringDecoder, "UInt128");
  },
  toUInt128OrZero(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toUInt128OrZero", [expression], uintStringDecoder, "UInt128");
  },
  toUInt256(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toUInt256", [expression], uintStringDecoder, "UInt256");
  },
  toUInt256OrDefault(expression: unknown, defaultValue?: unknown): Selection<string> {
    return createTypedConversionExpression(
      "toUInt256OrDefault",
      withOptional([expression], defaultValue),
      uintStringDecoder,
      "UInt256",
    );
  },
  toUInt256OrNull(expression: unknown): Selection<string | null> {
    return createNullableTypedConversionExpression("toUInt256OrNull", [expression], uintStringDecoder, "UInt256");
  },
  toUInt256OrZero(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toUInt256OrZero", [expression], uintStringDecoder, "UInt256");
  },
  toFloat32(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toFloat32", [expression], numberDecoder, "Float32");
  },
  toFloat32OrDefault(expression: unknown, defaultValue?: unknown): Selection<number> {
    return createTypedConversionExpression(
      "toFloat32OrDefault",
      withOptional([expression], defaultValue),
      numberDecoder,
      "Float32",
    );
  },
  toFloat32OrNull(expression: unknown): Selection<number | null> {
    return createNullableTypedConversionExpression("toFloat32OrNull", [expression], numberDecoder, "Float32");
  },
  toFloat32OrZero(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toFloat32OrZero", [expression], numberDecoder, "Float32");
  },
  toFloat64(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toFloat64", [expression], numberDecoder, "Float64");
  },
  toFloat64OrDefault(expression: unknown, defaultValue?: unknown): Selection<number> {
    return createTypedConversionExpression(
      "toFloat64OrDefault",
      withOptional([expression], defaultValue),
      numberDecoder,
      "Float64",
    );
  },
  toFloat64OrNull(expression: unknown): Selection<number | null> {
    return createNullableTypedConversionExpression("toFloat64OrNull", [expression], numberDecoder, "Float64");
  },
  toFloat64OrZero(expression: unknown): Selection<number> {
    return createTypedConversionExpression("toFloat64OrZero", [expression], numberDecoder, "Float64");
  },
  toDecimal32(expression: unknown, scale: number): Selection<string> {
    return createFixedWidthDecimalCast("toDecimal32", expression, scale);
  },
  toDecimal64(expression: unknown, scale: number): Selection<string> {
    return createFixedWidthDecimalCast("toDecimal64", expression, scale);
  },
  toDecimal128(expression: unknown, scale: number): Selection<string> {
    return createFixedWidthDecimalCast("toDecimal128", expression, scale);
  },
  toDecimal256(expression: unknown, scale: number): Selection<string> {
    return createFixedWidthDecimalCast("toDecimal256", expression, scale);
  },
  toDecimal32OrDefault(expression: unknown, scale: number, defaultValue?: unknown): Selection<string> {
    return createFixedWidthDecimalCast("toDecimal32OrDefault", expression, scale, {
      extraArgs: defaultValue === undefined ? [] : [defaultValue],
    });
  },
  toDecimal64OrDefault(expression: unknown, scale: number, defaultValue?: unknown): Selection<string> {
    return createFixedWidthDecimalCast("toDecimal64OrDefault", expression, scale, {
      extraArgs: defaultValue === undefined ? [] : [defaultValue],
    });
  },
  toDecimal128OrDefault(expression: unknown, scale: number, defaultValue?: unknown): Selection<string> {
    return createFixedWidthDecimalCast("toDecimal128OrDefault", expression, scale, {
      extraArgs: defaultValue === undefined ? [] : [defaultValue],
    });
  },
  toDecimal256OrDefault(expression: unknown, scale: number, defaultValue?: unknown): Selection<string> {
    return createFixedWidthDecimalCast("toDecimal256OrDefault", expression, scale, {
      extraArgs: defaultValue === undefined ? [] : [defaultValue],
    });
  },
  toDecimal32OrNull(expression: unknown, scale: number): Selection<string | null> {
    return createFixedWidthDecimalCast("toDecimal32OrNull", expression, scale, {
      decoder: nullableDecoder(stringDecoder),
    });
  },
  toDecimal64OrNull(expression: unknown, scale: number): Selection<string | null> {
    return createFixedWidthDecimalCast("toDecimal64OrNull", expression, scale, {
      decoder: nullableDecoder(stringDecoder),
    });
  },
  toDecimal128OrNull(expression: unknown, scale: number): Selection<string | null> {
    return createFixedWidthDecimalCast("toDecimal128OrNull", expression, scale, {
      decoder: nullableDecoder(stringDecoder),
    });
  },
  toDecimal256OrNull(expression: unknown, scale: number): Selection<string | null> {
    return createFixedWidthDecimalCast("toDecimal256OrNull", expression, scale, {
      decoder: nullableDecoder(stringDecoder),
    });
  },
  toDecimal32OrZero(expression: unknown, scale: number): Selection<string> {
    return createFixedWidthDecimalCast("toDecimal32OrZero", expression, scale);
  },
  toDecimal64OrZero(expression: unknown, scale: number): Selection<string> {
    return createFixedWidthDecimalCast("toDecimal64OrZero", expression, scale);
  },
  toDecimal128OrZero(expression: unknown, scale: number): Selection<string> {
    return createFixedWidthDecimalCast("toDecimal128OrZero", expression, scale);
  },
  toDecimal256OrZero(expression: unknown, scale: number): Selection<string> {
    return createFixedWidthDecimalCast("toDecimal256OrZero", expression, scale);
  },
  toDecimalString(expression: unknown, scale: number): Selection<string> {
    assertNonNegativeInteger("toDecimalString scale", scale);
    return createExpression<string>({
      compile: (ctx) => {
        const compiledExpr = compileValue(expression, ctx);
        return sql`toDecimalString(${compiledExpr}, ${sql.raw(String(scale))})`;
      },
      decoder: stringDecoder,
      sqlType: "String",
    });
  },
  toFixedString(expression: unknown, length: number): Selection<string> {
    return createFixedStringExpression(expression, length);
  },
  toDate(expression: unknown): Selection<Date> {
    return createFunctionExpression("toDate", [expression], {
      decoder: dateDecoder,
      sqlType: "Date",
    });
  },
  toDateOrDefault(expression: unknown, defaultValue?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "toDateOrDefault",
      withOptional([expression], defaultValue),
      dateDecoder,
      "Date",
    );
  },
  toDateOrNull(expression: unknown): Selection<Date | null> {
    return createNullableTypedConversionExpression("toDateOrNull", [expression], dateDecoder, "Date");
  },
  toDateOrZero(expression: unknown): Selection<Date> {
    return createTypedConversionExpression("toDateOrZero", [expression], dateDecoder, "Date");
  },
  toDate32(expression: unknown): Selection<Date> {
    return createFunctionExpression("toDate32", [expression], {
      decoder: dateDecoder,
      sqlType: "Date32",
    });
  },
  toDate32OrDefault(expression: unknown, defaultValue?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "toDate32OrDefault",
      withOptional([expression], defaultValue),
      dateDecoder,
      "Date32",
    );
  },
  toDate32OrNull(expression: unknown): Selection<Date | null> {
    return createNullableTypedConversionExpression("toDate32OrNull", [expression], dateDecoder, "Date32");
  },
  toDate32OrZero(expression: unknown): Selection<Date> {
    return createTypedConversionExpression("toDate32OrZero", [expression], dateDecoder, "Date32");
  },
  toDateTime(expression: unknown, timezone?: unknown): Selection<Date> {
    const args = timezone === undefined ? [expression] : [expression, timezone];
    return createFunctionExpression("toDateTime", args, {
      decoder: dateDecoder,
      sqlType: "DateTime",
    });
  },
  toDateTimeOrDefault(expression: unknown, timezone?: unknown, defaultValue?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "toDateTimeOrDefault",
      [
        expression,
        ...(timezone === undefined ? [] : [timezone]),
        ...(defaultValue === undefined ? [] : [defaultValue]),
      ],
      dateDecoder,
      "DateTime",
    );
  },
  toDateTimeOrNull(expression: unknown, timezone?: unknown): Selection<Date | null> {
    return createNullableTypedConversionExpression(
      "toDateTimeOrNull",
      withOptional([expression], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  toDateTimeOrZero(expression: unknown, timezone?: unknown): Selection<Date> {
    return createTypedConversionExpression(
      "toDateTimeOrZero",
      withOptional([expression], timezone),
      dateDecoder,
      "DateTime",
    );
  },
  toDateTime32(expression: unknown, timezone?: unknown): Selection<Date> {
    return createFunctionExpression("toDateTime32", withOptional([expression], timezone), {
      decoder: dateDecoder,
      sqlType: "DateTime",
    });
  },
  toDateTime64(expression: unknown, scale: number, timezone?: unknown): Selection<Date> {
    return createDateTime64Cast(expression, scale, timezone);
  },
  toDateTime64OrDefault(
    expression: unknown,
    scale: number,
    timezone?: unknown,
    defaultValue?: unknown,
  ): Selection<Date> {
    return createDateTime64ConversionExpression(
      "toDateTime64OrDefault",
      expression,
      scale,
      timezone,
      defaultValue,
      dateDecoder,
    );
  },
  toDateTime64OrNull(expression: unknown, scale: number, timezone?: unknown): Selection<Date | null> {
    return createDateTime64ConversionExpression(
      "toDateTime64OrNull",
      expression,
      scale,
      timezone,
      undefined,
      nullableDecoder(dateDecoder),
    );
  },
  toDateTime64OrZero(expression: unknown, scale: number, timezone?: unknown): Selection<Date> {
    return createDateTime64ConversionExpression(
      "toDateTime64OrZero",
      expression,
      scale,
      timezone,
      undefined,
      dateDecoder,
    );
  },
  fromUnixTimestamp: createFromUnixTimestampExpression,
  fromUnixTimestamp64Second(expression: unknown, timezone?: unknown): Selection<Date> {
    return createFunctionExpression("fromUnixTimestamp64Second", withOptional([expression], timezone), {
      decoder: dateDecoder,
      sqlType: "DateTime64(0)",
    });
  },
  fromUnixTimestamp64Milli(expression: unknown, timezone?: unknown): Selection<Date> {
    return createFunctionExpression("fromUnixTimestamp64Milli", withOptional([expression], timezone), {
      decoder: dateDecoder,
      sqlType: "DateTime64(3)",
    });
  },
  fromUnixTimestamp64Micro(expression: unknown, timezone?: unknown): Selection<Date> {
    return createFunctionExpression("fromUnixTimestamp64Micro", withOptional([expression], timezone), {
      decoder: dateDecoder,
      sqlType: "DateTime64(6)",
    });
  },
  fromUnixTimestamp64Nano(expression: unknown, timezone?: unknown): Selection<Date> {
    return createFunctionExpression("fromUnixTimestamp64Nano", withOptional([expression], timezone), {
      decoder: dateDecoder,
      sqlType: "DateTime64(9)",
    });
  },
  toStartOfMonth(expression: unknown): Selection<Date> {
    return createFunctionExpression("toStartOfMonth", [expression], {
      decoder: dateDecoder,
      sqlType: "Date",
    });
  },
  toUnixTimestamp(expression: unknown, timezone?: unknown): Selection<number> {
    return createUInt32Expression("toUnixTimestamp", withOptional([expression], timezone));
  },
  toUnixTimestamp64Second(expression: unknown): Selection<string> {
    return createInt64Expression("toUnixTimestamp64Second", [expression]);
  },
  toUnixTimestamp64Milli(expression: unknown): Selection<string> {
    return createInt64Expression("toUnixTimestamp64Milli", [expression]);
  },
  toUnixTimestamp64Micro(expression: unknown): Selection<string> {
    return createInt64Expression("toUnixTimestamp64Micro", [expression]);
  },
  toUnixTimestamp64Nano(expression: unknown): Selection<string> {
    return createInt64Expression("toUnixTimestamp64Nano", [expression]);
  },
  toLowCardinality<TData = unknown>(expression: unknown): Selection<TData> {
    return createTypedConversionExpression<TData>(
      "toLowCardinality",
      [expression],
      passThroughDecoder as Decoder<TData>,
      "LowCardinality",
    );
  },
  toNullable<TData = unknown>(expression: unknown): Selection<TData | null> {
    return createTypedConversionExpression<TData | null>(
      "toNullable",
      [expression],
      nullableDecoder(passThroughDecoder as Decoder<TData>),
      "Nullable",
    );
  },
  toTime(expression: unknown): Selection<Date> {
    return createTypedConversionExpression("toTime", [expression], timeDecoder, "Time");
  },
  toTimeOrNull(expression: unknown): Selection<Date | null> {
    return createNullableTypedConversionExpression("toTimeOrNull", [expression], timeDecoder, "Time");
  },
  toTimeOrZero(expression: unknown): Selection<Date> {
    return createTypedConversionExpression("toTimeOrZero", [expression], timeDecoder, "Time");
  },
  toTime64(expression: unknown, precision: number): Selection<Date> {
    return createTime64Expression("toTime64", expression, precision, timeDecoder);
  },
  toTime64OrNull(expression: unknown, precision: number): Selection<Date | null> {
    return createTime64Expression("toTime64OrNull", expression, precision, nullableDecoder(timeDecoder));
  },
  toTime64OrZero(expression: unknown, precision: number): Selection<Date> {
    return createTime64Expression("toTime64OrZero", expression, precision, timeDecoder);
  },
  toInterval(value: unknown, unit: string): Selection<unknown> {
    return createIntervalExpression("toInterval", value, unit);
  },
  toIntervalNanosecond(value: unknown): Selection<unknown> {
    return createIntervalExpression("toIntervalNanosecond", value);
  },
  toIntervalMicrosecond(value: unknown): Selection<unknown> {
    return createIntervalExpression("toIntervalMicrosecond", value);
  },
  toIntervalMillisecond(value: unknown): Selection<unknown> {
    return createIntervalExpression("toIntervalMillisecond", value);
  },
  toIntervalSecond(value: unknown): Selection<unknown> {
    return createIntervalExpression("toIntervalSecond", value);
  },
  toIntervalMinute(value: unknown): Selection<unknown> {
    return createIntervalExpression("toIntervalMinute", value);
  },
  toIntervalHour(value: unknown): Selection<unknown> {
    return createIntervalExpression("toIntervalHour", value);
  },
  toIntervalDay(value: unknown): Selection<unknown> {
    return createIntervalExpression("toIntervalDay", value);
  },
  toIntervalWeek(value: unknown): Selection<unknown> {
    return createIntervalExpression("toIntervalWeek", value);
  },
  toIntervalMonth(value: unknown): Selection<unknown> {
    return createIntervalExpression("toIntervalMonth", value);
  },
  toIntervalQuarter(value: unknown): Selection<unknown> {
    return createIntervalExpression("toIntervalQuarter", value);
  },
  toIntervalYear(value: unknown): Selection<unknown> {
    return createIntervalExpression("toIntervalYear", value);
  },
  toUUID(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toUUID", [expression], stringDecoder, "UUID");
  },
  toUUIDOrZero(expression: unknown): Selection<string> {
    return createTypedConversionExpression("toUUIDOrZero", [expression], stringDecoder, "UUID");
  },
  coalesce<TData = unknown>(...args: unknown[]): Selection<TData> {
    return createCoalesceExpression<TData>(args);
  },
  tuple(...args: unknown[]): Selection<unknown[]> {
    return createFunctionExpression("tuple", args, {
      decoder: (value) => {
        if (!Array.isArray(value)) {
          throw createDecodeError(`Cannot convert value to tuple array: ${String(value)}`, value);
        }
        return value;
      },
      sqlType: "Tuple",
    });
  },
  arrayAUCPR(scores: unknown, labels: unknown, partialOffsets?: unknown): Selection<number> {
    return createNumberExpression("arrayAUCPR", withOptional([scores, labels], partialOffsets));
  },
  arrayAll(lambda: unknown, sourceArray: unknown, ...conditionArrays: unknown[]): Selection<boolean> {
    return createBooleanExpression("arrayAll", [lambda, sourceArray, ...conditionArrays]);
  },
  arrayAutocorrelation(array: unknown, maxLag?: unknown): Selection<number[]> {
    return createArrayExpression<number>("arrayAutocorrelation", withOptional([array], maxLag), "Array(Float64)");
  },
  arrayAvg(lambdaOrArray: unknown, array?: unknown, ...conditionArrays: unknown[]): Selection<number> {
    const args = array === undefined ? [lambdaOrArray] : [lambdaOrArray, array, ...conditionArrays];
    return createNumberExpression("arrayAvg", args);
  },
  arrayCompact<TData = unknown>(array: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayCompact", [array]);
  },
  arrayZip(...args: unknown[]): Selection<unknown[]> {
    return createArrayExpression<unknown>("arrayZip", args);
  },
  array<TData = unknown>(first: unknown, ...rest: unknown[]): Selection<TData[]> {
    return createArrayExpression<TData>("array", [first, ...rest]);
  },
  arrayConcat<TData = unknown>(...arrays: unknown[]): Selection<TData[]> {
    return createArrayExpression<TData>("arrayConcat", arrays);
  },
  arrayCount(lambdaOrArray: unknown, array?: unknown, ...conditionArrays: unknown[]): Selection<string> {
    const args = array === undefined ? [lambdaOrArray] : [lambdaOrArray, array, ...conditionArrays];
    return createUInt64Expression("arrayCount", args);
  },
  arrayCumSum<TData = unknown>(
    lambdaOrArray: unknown,
    array?: unknown,
    ...conditionArrays: unknown[]
  ): Selection<TData[]> {
    const args = array === undefined ? [lambdaOrArray] : [lambdaOrArray, array, ...conditionArrays];
    return createArrayExpression<TData>("arrayCumSum", args);
  },
  arrayCumSumNonNegative<TData = unknown>(
    lambdaOrArray: unknown,
    array?: unknown,
    ...conditionArrays: unknown[]
  ): Selection<TData[]> {
    const args = array === undefined ? [lambdaOrArray] : [lambdaOrArray, array, ...conditionArrays];
    return createArrayExpression<TData>("arrayCumSumNonNegative", args);
  },
  arrayDifference<TData = unknown>(array: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayDifference", [array]);
  },
  arrayDistinct<TData = unknown>(array: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayDistinct", [array]);
  },
  arrayDotProduct<TData = unknown>(first: unknown, second: unknown): Selection<TData> {
    return createFunctionExpression<TData>("arrayDotProduct", [first, second]);
  },
  arrayElement<TData = unknown>(array: unknown, index: unknown): Selection<TData> {
    return createFunctionExpression<TData>("arrayElement", [array, index]);
  },
  arrayElementOrNull<TData = unknown>(array: unknown, index: unknown): Selection<TData | null> {
    return createFunctionExpression<TData | null>("arrayElementOrNull", [array, index]);
  },
  arrayEnumerate(array: unknown): Selection<number[]> {
    return createArrayExpression<number>("arrayEnumerate", [array], "Array(UInt32)");
  },
  arrayEnumerateDense(array: unknown): Selection<number[]> {
    return createArrayExpression<number>("arrayEnumerateDense", [array], "Array(UInt32)");
  },
  arrayEnumerateDenseRanked(clearDepth: unknown, array: unknown, maxArrayDepth: unknown): Selection<number[]> {
    return createExpression({
      compile: (ctx) =>
        sql`arrayEnumerateDenseRanked(${joinSqlParts(
          [
            createConstIntegerLiteral("arrayEnumerateDenseRanked clearDepth", clearDepth, { positive: true }),
            compileValue(array, ctx),
            createConstIntegerLiteral("arrayEnumerateDenseRanked maxArrayDepth", maxArrayDepth, { positive: true }),
          ],
          ", ",
        )})`,
      decoder: arrayDecoder<number>("arrayEnumerateDenseRanked"),
      sqlType: "Array(UInt32)",
    });
  },
  arrayEnumerateUniq(...arrays: unknown[]): Selection<number[]> {
    return createArrayExpression<number>("arrayEnumerateUniq", arrays, "Array(UInt32)");
  },
  arrayEnumerateUniqRanked(clearDepth: unknown, array: unknown, maxArrayDepth: unknown): Selection<number[]> {
    return createExpression({
      compile: (ctx) =>
        sql`arrayEnumerateUniqRanked(${joinSqlParts(
          [
            createConstIntegerLiteral("arrayEnumerateUniqRanked clearDepth", clearDepth, { positive: true }),
            compileValue(array, ctx),
            createConstIntegerLiteral("arrayEnumerateUniqRanked maxArrayDepth", maxArrayDepth, { positive: true }),
          ],
          ", ",
        )})`,
      decoder: arrayDecoder<number>("arrayEnumerateUniqRanked"),
      sqlType: "Array(UInt32)",
    });
  },
  arrayExcept<TData = unknown>(source: unknown, except: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayExcept", [source, except]);
  },
  arrayExists(lambda: unknown, sourceArray: unknown, ...conditionArrays: unknown[]): Selection<boolean> {
    return createBooleanExpression("arrayExists", [lambda, sourceArray, ...conditionArrays]);
  },
  arrayFill<TData = unknown>(lambda: unknown, sourceArray: unknown, ...conditionArrays: unknown[]): Selection<TData[]> {
    return createArrayExpression<TData>("arrayFill", [lambda, sourceArray, ...conditionArrays]);
  },
  arrayFilter<_TData = unknown>(
    lambda: unknown,
    sourceArray: unknown,
    ...conditionArrays: unknown[]
  ): Selection<unknown[]> {
    return createArrayExpression<unknown>("arrayFilter", [lambda, sourceArray, ...conditionArrays]);
  },
  arrayFirst<TData = unknown>(lambda: unknown, sourceArray: unknown, ...conditionArrays: unknown[]): Selection<TData> {
    return createFunctionExpression<TData>("arrayFirst", [lambda, sourceArray, ...conditionArrays]);
  },
  arrayFirstIndex(lambda: unknown, sourceArray: unknown, ...conditionArrays: unknown[]): Selection<number> {
    return createUInt32Expression("arrayFirstIndex", [lambda, sourceArray, ...conditionArrays]);
  },
  arrayFirstOrNull<TData = unknown>(
    lambda: unknown,
    sourceArray: unknown,
    ...conditionArrays: unknown[]
  ): Selection<TData | null> {
    return createFunctionExpression<TData | null>("arrayFirstOrNull", [lambda, sourceArray, ...conditionArrays]);
  },
  arraySlice<TData = unknown>(array: unknown, offset: unknown, length?: unknown): Selection<TData[]> {
    const args = length === undefined ? [array, offset] : [array, offset, length];
    return createArrayExpression<TData>("arraySlice", args);
  },
  arrayFold<TData = unknown>(
    lambda: unknown,
    firstArray: unknown,
    ...arraysAndAccumulator: unknown[]
  ): Selection<TData> {
    return createFunctionExpression<TData>("arrayFold", [lambda, firstArray, ...arraysAndAccumulator]);
  },
  arrayFlatten<TData = unknown>(array: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayFlatten", [array]);
  },
  arrayIntersect<TData = unknown>(first: unknown, ...rest: unknown[]): Selection<TData[]> {
    return createArrayExpression<TData>("arrayIntersect", [first, ...rest]);
  },
  arrayJaccardIndex(first: unknown, second: unknown): Selection<number> {
    return createNumberExpression("arrayJaccardIndex", [first, second]);
  },
  arrayLast<TData = unknown>(lambda: unknown, sourceArray: unknown, ...conditionArrays: unknown[]): Selection<TData> {
    return createFunctionExpression<TData>("arrayLast", [lambda, sourceArray, ...conditionArrays]);
  },
  arrayLastIndex(lambda: unknown, sourceArray: unknown, ...conditionArrays: unknown[]): Selection<number> {
    return createUInt32Expression("arrayLastIndex", [lambda, sourceArray, ...conditionArrays]);
  },
  arrayLastOrNull<TData = unknown>(
    lambda: unknown,
    sourceArray: unknown,
    ...conditionArrays: unknown[]
  ): Selection<TData | null> {
    return createFunctionExpression<TData | null>("arrayLastOrNull", [lambda, sourceArray, ...conditionArrays]);
  },
  arrayLevenshteinDistance(from: unknown, to: unknown): Selection<number> {
    return createNumberExpression("arrayLevenshteinDistance", [from, to]);
  },
  arrayLevenshteinDistanceWeighted(
    from: unknown,
    to: unknown,
    fromWeights: unknown,
    toWeights: unknown,
  ): Selection<number> {
    return createNumberExpression("arrayLevenshteinDistanceWeighted", [from, to, fromWeights, toWeights]);
  },
  arrayMap<_TData = unknown>(
    lambda: unknown,
    sourceArray: unknown,
    ...conditionArrays: unknown[]
  ): Selection<unknown[]> {
    return createArrayExpression<unknown>("arrayMap", [lambda, sourceArray, ...conditionArrays]);
  },
  arrayMax<TData = unknown>(lambdaOrArray: unknown, array?: unknown, ...conditionArrays: unknown[]): Selection<TData> {
    const args = array === undefined ? [lambdaOrArray] : [lambdaOrArray, array, ...conditionArrays];
    return createFunctionExpression<TData>("arrayMax", args);
  },
  arrayMin<TData = unknown>(lambdaOrArray: unknown, array?: unknown, ...conditionArrays: unknown[]): Selection<TData> {
    const args = array === undefined ? [lambdaOrArray] : [lambdaOrArray, array, ...conditionArrays];
    return createFunctionExpression<TData>("arrayMin", args);
  },
  arrayNormalizedGini<TData = unknown[]>(predicted: unknown, label: unknown): Selection<TData> {
    return createFunctionExpression<TData>("arrayNormalizedGini", [predicted, label]);
  },
  arrayPartialReverseSort<TData = unknown>(first: unknown, ...rest: unknown[]): Selection<TData[]> {
    return createArrayExpression<TData>("arrayPartialReverseSort", [first, ...rest]);
  },
  arrayPartialShuffle<TData = unknown>(array: unknown, limit: unknown, seed?: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayPartialShuffle", withOptional([array, limit], seed));
  },
  arrayPartialSort<TData = unknown>(first: unknown, ...rest: unknown[]): Selection<TData[]> {
    return createArrayExpression<TData>("arrayPartialSort", [first, ...rest]);
  },
  arrayPopBack<TData = unknown>(array: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayPopBack", [array]);
  },
  arrayPopFront<TData = unknown>(array: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayPopFront", [array]);
  },
  arrayProduct<TData = unknown>(
    lambdaOrArray: unknown,
    array?: unknown,
    ...conditionArrays: unknown[]
  ): Selection<TData> {
    const args = array === undefined ? [lambdaOrArray] : [lambdaOrArray, array, ...conditionArrays];
    return createFunctionExpression<TData>("arrayProduct", args);
  },
  arrayPushBack<TData = unknown>(array: unknown, value: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayPushBack", [array, value]);
  },
  arrayPushFront<TData = unknown>(array: unknown, value: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayPushFront", [array, value]);
  },
  arrayROCAUC(scores: unknown, labels: unknown, scale?: unknown, partialOffsets?: unknown): Selection<number> {
    const args = scale === undefined ? [scores, labels] : withOptional([scores, labels, scale], partialOffsets);
    return createNumberExpression("arrayROCAUC", args);
  },
  arrayRandomSample<TData = unknown>(array: unknown, samples: unknown): Selection<TData[]> {
    return createExpression({
      compile: (ctx) =>
        sql`arrayRandomSample(${joinSqlParts(
          [
            compileValue(array, ctx),
            createConstIntegerLiteral("arrayRandomSample samples", samples, { nonNegative: true }),
          ],
          ", ",
        )})`,
      decoder: arrayDecoder<TData>("arrayRandomSample"),
    });
  },
  arrayReduce<TData = unknown>(
    aggregateFunction: unknown,
    firstArray: unknown,
    ...restArrays: unknown[]
  ): Selection<TData> {
    return createFunctionExpression<TData>("arrayReduce", [aggregateFunction, firstArray, ...restArrays]);
  },
  arrayReduceInRanges<TData = unknown>(
    aggregateFunction: unknown,
    ranges: unknown,
    firstArray: unknown,
    ...restArrays: unknown[]
  ): Selection<TData[]> {
    return createArrayExpression<TData>("arrayReduceInRanges", [aggregateFunction, ranges, firstArray, ...restArrays]);
  },
  arrayRemove<TData = unknown>(array: unknown, value: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayRemove", [array, value]);
  },
  arrayResize<TData = unknown>(array: unknown, size: unknown, extender?: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayResize", withOptional([array, size], extender));
  },
  arrayReverse<TData = unknown>(array: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayReverse", [array]);
  },
  arrayReverseFill<TData = unknown>(
    lambda: unknown,
    sourceArray: unknown,
    ...conditionArrays: unknown[]
  ): Selection<TData[]> {
    return createArrayExpression<TData>("arrayReverseFill", [lambda, sourceArray, ...conditionArrays]);
  },
  arrayReverseSort<TData = unknown>(first: unknown, ...rest: unknown[]): Selection<TData[]> {
    return createArrayExpression<TData>("arrayReverseSort", [first, ...rest]);
  },
  arrayReverseSplit<TData = unknown>(
    lambda: unknown,
    sourceArray: unknown,
    ...conditionArrays: unknown[]
  ): Selection<TData[]> {
    return createArrayExpression<TData>("arrayReverseSplit", [lambda, sourceArray, ...conditionArrays]);
  },
  arrayRotateLeft<TData = unknown>(array: unknown, n: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayRotateLeft", [array, n]);
  },
  arrayRotateRight<TData = unknown>(array: unknown, n: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayRotateRight", [array, n]);
  },
  arrayShiftLeft<TData = unknown>(array: unknown, n: unknown, defaultValue?: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayShiftLeft", withOptional([array, n], defaultValue));
  },
  arrayShiftRight<TData = unknown>(array: unknown, n: unknown, defaultValue?: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayShiftRight", withOptional([array, n], defaultValue));
  },
  arrayShingles<TData = unknown>(array: unknown, length: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayShingles", [array, length]);
  },
  arrayShuffle<TData = unknown>(array: unknown, seed?: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayShuffle", withOptional([array], seed));
  },
  arraySimilarity(from: unknown, to: unknown, fromWeights: unknown, toWeights: unknown): Selection<number> {
    return createNumberExpression("arraySimilarity", [from, to, fromWeights, toWeights]);
  },
  arraySort<TData = unknown>(first: unknown, ...rest: unknown[]): Selection<TData[]> {
    return createArrayExpression<TData>("arraySort", [first, ...rest]);
  },
  arraySplit<TData = unknown>(
    lambda: unknown,
    sourceArray: unknown,
    ...conditionArrays: unknown[]
  ): Selection<TData[]> {
    return createArrayExpression<TData>("arraySplit", [lambda, sourceArray, ...conditionArrays]);
  },
  arraySum<TData = unknown>(lambdaOrArray: unknown, array?: unknown, ...conditionArrays: unknown[]): Selection<TData> {
    const args = array === undefined ? [lambdaOrArray] : [lambdaOrArray, array, ...conditionArrays];
    return createFunctionExpression<TData>("arraySum", args);
  },
  arraySymmetricDifference<TData = unknown>(first: unknown, ...rest: unknown[]): Selection<TData[]> {
    return createArrayExpression<TData>("arraySymmetricDifference", [first, ...rest]);
  },
  arrayTranspose<TData = unknown>(array: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayTranspose", [array]);
  },
  arrayUnion<TData = unknown>(first: unknown, ...rest: unknown[]): Selection<TData[]> {
    return createArrayExpression<TData>("arrayUnion", [first, ...rest]);
  },
  arrayUniq(...arrays: unknown[]): Selection<string> {
    return createUInt64Expression("arrayUniq", arrays);
  },
  arrayWithConstant<TData = unknown>(length: unknown, value: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayWithConstant", [length, value]);
  },
  arrayZipUnaligned(...args: unknown[]): Selection<unknown[]> {
    return createArrayExpression<unknown>("arrayZipUnaligned", args);
  },
  countEqual(array: unknown, value: unknown): Selection<string> {
    return createUInt64Expression("countEqual", [array, value]);
  },
  empty(value: unknown): Selection<boolean> {
    return createBooleanExpression("empty", [value]);
  },
  emptyArrayDate(): Selection<Date[]> {
    return createArrayExpression<Date>("emptyArrayDate", [], "Array(Date)");
  },
  emptyArrayDateTime(): Selection<Date[]> {
    return createArrayExpression<Date>("emptyArrayDateTime", [], "Array(DateTime)");
  },
  emptyArrayFloat32(): Selection<number[]> {
    return createArrayExpression<number>("emptyArrayFloat32", [], "Array(Float32)");
  },
  emptyArrayFloat64(): Selection<number[]> {
    return createArrayExpression<number>("emptyArrayFloat64", [], "Array(Float64)");
  },
  emptyArrayInt16(): Selection<number[]> {
    return createArrayExpression<number>("emptyArrayInt16", [], "Array(Int16)");
  },
  emptyArrayInt32(): Selection<number[]> {
    return createArrayExpression<number>("emptyArrayInt32", [], "Array(Int32)");
  },
  emptyArrayInt64(): Selection<string[]> {
    return createArrayExpression<string>("emptyArrayInt64", [], "Array(Int64)");
  },
  emptyArrayInt8(): Selection<number[]> {
    return createArrayExpression<number>("emptyArrayInt8", [], "Array(Int8)");
  },
  emptyArrayString(): Selection<string[]> {
    return createArrayExpression<string>("emptyArrayString", [], "Array(String)");
  },
  emptyArrayToSingle<TData = unknown>(array: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("emptyArrayToSingle", [array]);
  },
  emptyArrayUInt16(): Selection<number[]> {
    return createArrayExpression<number>("emptyArrayUInt16", [], "Array(UInt16)");
  },
  emptyArrayUInt32(): Selection<number[]> {
    return createArrayExpression<number>("emptyArrayUInt32", [], "Array(UInt32)");
  },
  emptyArrayUInt64(): Selection<string[]> {
    return createArrayExpression<string>("emptyArrayUInt64", [], "Array(UInt64)");
  },
  emptyArrayUInt8(): Selection<number[]> {
    return createArrayExpression<number>("emptyArrayUInt8", [], "Array(UInt8)");
  },
  has(haystack: unknown, needle: unknown): Selection<boolean> {
    return createBooleanExpression("has", [haystack, needle]);
  },
  hasAll(set: unknown, subset: unknown): Selection<boolean> {
    return createBooleanExpression("hasAll", [set, subset]);
  },
  hasAny(arrX: unknown, arrY: unknown): Selection<boolean> {
    return createBooleanExpression("hasAny", [arrX, arrY]);
  },
  hasSubstr(array: unknown, needle: unknown): Selection<boolean> {
    return createBooleanExpression("hasSubstr", [array, needle]);
  },
  indexOfAssumeSorted(array: unknown, value: unknown): Selection<string> {
    return createUInt64Expression("indexOfAssumeSorted", [array, value]);
  },
  indexOf(array: unknown, value: unknown): Selection<string> {
    return createUInt64Expression("indexOf", [array, value]);
  },
  kql_array_sort_asc<TData = unknown[]>(first: unknown, ...rest: unknown[]): Selection<TData> {
    return createFunctionExpression<TData>("kql_array_sort_asc", [first, ...rest]);
  },
  kql_array_sort_desc<TData = unknown[]>(first: unknown, ...rest: unknown[]): Selection<TData> {
    return createFunctionExpression<TData>("kql_array_sort_desc", [first, ...rest]);
  },
  length(value: unknown): Selection<string> {
    return createUInt64Expression("length", [value]);
  },
  notEmpty(value: unknown): Selection<boolean> {
    return createBooleanExpression("notEmpty", [value]);
  },
  range<_TData = number>(first: unknown, second?: unknown, step?: unknown): Selection<unknown[]> {
    const args = second === undefined ? [first] : withOptional([first, second], step);
    return createArrayExpression<unknown>("range", args);
  },
  replicate<TData = unknown>(value: unknown, array: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("replicate", [value, array]);
  },
  reverse<TData = unknown>(value: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("reverse", [value]);
  },
  arrayJoin<TData = unknown>(array: unknown): Selection<TData> {
    return createFunctionExpression<TData>("arrayJoin", [array]);
  },
  tupleElement<TData = unknown>(
    tuple: unknown,
    indexOrName: JsonPathSegment,
    defaultValue?: unknown,
  ): Selection<TData> {
    return createExpression({
      compile: (ctx) => {
        const args =
          defaultValue === undefined
            ? [compileValue(tuple, ctx), createTupleElementIndexLiteral(indexOrName)]
            : [compileValue(tuple, ctx), createTupleElementIndexLiteral(indexOrName), compileValue(defaultValue, ctx)];
        return sql`tupleElement(${joinSqlParts(args, ", ")})`;
      },
      decoder: passThroughDecoder as Decoder<TData>,
    });
  },
  not(expression: unknown): Selection<boolean> {
    return createFunctionExpression("not", [expression], {
      decoder: booleanDecoder,
      sqlType: "Bool",
    });
  },
};

/* ── aggregate functions ───────────────────────────────────────── */

export type CountSelection<TMode extends CountMode = "unsafe"> = Selection<CountModeResult<TMode>> & {
  readonly sqlType: CountSqlType;
  toSafe(): CountSelection<"safe">;
  toUnsafe(): CountSelection<"unsafe">;
  toMixed(): CountSelection<"mixed">;
};

const createCountSelection = <TMode extends CountMode>(
  innerCall: (ctx: BuildContext) => SQLFragment,
  mode: TMode,
): CountSelection<TMode> => {
  const expr = createExpression<CountModeResult<TMode>>({
    compile: (ctx) => wrapCountSql(innerCall(ctx), mode),
    decoder: getCountDecoder(mode),
    sqlType: getCountSqlType(mode),
  });
  return Object.assign(expr, {
    toSafe(): CountSelection<"safe"> {
      return createCountSelection(innerCall, "safe");
    },
    toUnsafe(): CountSelection<"unsafe"> {
      return createCountSelection(innerCall, "unsafe");
    },
    toMixed(): CountSelection<"mixed"> {
      return createCountSelection(innerCall, "mixed");
    },
  }) as unknown as CountSelection<TMode>;
};

const aggregateFns = {
  count(expression?: unknown): CountSelection {
    const args = expression === undefined ? [] : [expression];
    return createCountSelection((ctx) => compileFunctionCall("count", args, ctx), "unsafe");
  },
  countIf(condition: unknown): CountSelection {
    return createCountSelection((ctx) => compileFunctionCall("countIf", [condition], ctx), "unsafe");
  },
  sum(expression: unknown): Selection<number | string> {
    const decimalAware = buildDecimalAwareAggregate("sum", expression, { widenForSum: true });
    if (decimalAware) return decimalAware;
    const sqlType = isFloatingSqlType(ensureExpression(expression).sqlType) ? "Float64" : undefined;
    return createFunctionExpression<number | string>("sum", [expression], {
      decoder: resolveAggregateDecoder(expression),
      sqlType,
    });
  },
  sumIf(expression: unknown, condition: unknown): Selection<number | string> {
    const decimalAware = buildDecimalAwareSumIf(expression, condition);
    if (decimalAware) return decimalAware;
    const sqlType = isFloatingSqlType(ensureExpression(expression).sqlType) ? "Float64" : undefined;
    return createFunctionExpression<number | string>("sumIf", [expression, condition], {
      decoder: resolveAggregateDecoder(expression),
      sqlType,
    });
  },
  avg(expression: unknown): Selection<number> {
    // ClickHouse `avg(Decimal)` runs internally on Float64 (sum-of-divides).
    // Don't auto-cast back to Decimal — the round-trip wouldn't recover lost
    // precision and would lie about the runtime path. Match CH native behavior.
    return createFunctionExpression<number>("avg", [expression], {
      decoder: numberDecoder,
      sqlType: "Float64",
    });
  },
  min<TData = unknown>(expression: unknown): Selection<TData> {
    const decimalAware = buildDecimalAwareAggregate("min", expression);
    if (decimalAware) return decimalAware as Selection<TData>;
    const wrapped = ensureExpression<TData>(expression);
    return createFunctionExpression<TData>("min", [expression], {
      decoder: wrapped.decoder,
      sqlType: wrapped.sqlType,
    });
  },
  max<TData = unknown>(expression: unknown): Selection<TData> {
    const decimalAware = buildDecimalAwareAggregate("max", expression);
    if (decimalAware) return decimalAware as Selection<TData>;
    const wrapped = ensureExpression<TData>(expression);
    return createFunctionExpression<TData>("max", [expression], {
      decoder: wrapped.decoder,
      sqlType: wrapped.sqlType,
    });
  },
  uniqExact(expression: unknown): CountSelection {
    return createCountSelection((ctx) => compileFunctionCall("uniqExact", [expression], ctx), "unsafe");
  },
};

/* ── table functions ───────────────────────────────────────────── */

const tableFns = {
  call(name: string, ...args: unknown[]) {
    return createTableFunctionSource((ctx) => compileFunctionCall(name, args, ctx));
  },
};

/* ── public API ────────────────────────────────────────────────── */

/**
 * ClickHouse SQL function helpers.
 *
 * Grouped internally by category but exposed as a single `fn` object so that
 * `fn.sum(...)`, `fn.coalesce(...)`, `fn.table.call(...)`, `Object.keys(fn)`
 * and dynamic access (`fn[name]`) all keep working.
 */
export const fn = {
  ...scalarFns,
  ...aggregateFns,
  table: tableFns,
};

/**
 * Low-level table function helpers shared by internals and `fn.table`.
 */
export const tableFn = {
  ...tableFns,
};

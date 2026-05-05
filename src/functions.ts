import {
  toBoolean as toBooleanCoercion,
  toDate as toDateCoercion,
  toNumber as toNumberCoercion,
  toStringValue as toStringCoercion,
} from "./coercion";
import type { AnyColumn } from "./columns";
import { createClientValidationError, createDecodeError } from "./errors";
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
const booleanDecoder: Decoder<boolean> = toBooleanCoercion;

const FIXED_WIDTH_DECIMAL_PRECISION = {
  toDecimal32: 9,
  toDecimal64: 18,
  toDecimal128: 38,
  toDecimal256: 76,
} as const;

type FixedWidthDecimalName = keyof typeof FIXED_WIDTH_DECIMAL_PRECISION;

const assertDateTime64Scale: (scale: unknown) => asserts scale is number = (scale) => {
  if (typeof scale !== "number" || !Number.isInteger(scale) || scale < 0 || scale > 9) {
    throw createClientValidationError(`toDateTime64 scale must be an integer between 0 and 9, got ${String(scale)}`);
  }
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

const createFixedWidthDecimalCast = (
  fnName: FixedWidthDecimalName,
  expression: unknown,
  scale: unknown,
): Selection<string> => {
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
  return createExpression<string>({
    compile: (ctx) => {
      assertValidSqlIdentifier(fnName, "function");
      const compiledExpr = compileValue(expression, ctx);
      return sql`${sql.raw(fnName)}(${compiledExpr}, ${sql.raw(String(scale))})`;
    },
    decoder: stringDecoder,
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
  toString(expression: unknown, timezone?: unknown): Selection<string> {
    const args = timezone === undefined ? [expression] : [expression, timezone];
    return createFunctionExpression("toString", args, {
      decoder: stringDecoder,
      sqlType: "String",
    });
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
  toDate(expression: unknown): Selection<Date> {
    return createFunctionExpression("toDate", [expression], {
      decoder: dateDecoder,
      sqlType: "Date",
    });
  },
  toDate32(expression: unknown): Selection<Date> {
    return createFunctionExpression("toDate32", [expression], {
      decoder: dateDecoder,
      sqlType: "Date32",
    });
  },
  toDateTime(expression: unknown, timezone?: unknown): Selection<Date> {
    const args = timezone === undefined ? [expression] : [expression, timezone];
    return createFunctionExpression("toDateTime", args, {
      decoder: dateDecoder,
      sqlType: "DateTime",
    });
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
    return createArrayExpression<number>(
      "arrayEnumerateDenseRanked",
      [clearDepth, array, maxArrayDepth],
      "Array(UInt32)",
    );
  },
  arrayEnumerateUniq(...arrays: unknown[]): Selection<number[]> {
    return createArrayExpression<number>("arrayEnumerateUniq", arrays, "Array(UInt32)");
  },
  arrayEnumerateUniqRanked(clearDepth: unknown, array: unknown, maxArrayDepth: unknown): Selection<number[]> {
    return createArrayExpression<number>(
      "arrayEnumerateUniqRanked",
      [clearDepth, array, maxArrayDepth],
      "Array(UInt32)",
    );
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
    return createArrayExpression<TData>("arrayRandomSample", [array, samples]);
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
    const args = defaultValue === undefined ? [tuple, indexOrName] : [tuple, indexOrName, defaultValue];
    return createFunctionExpression<TData>("tupleElement", args);
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

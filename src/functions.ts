import {
  toBoolean as toBooleanCoercion,
  toDate as toDateCoercion,
  toNumber as toNumberCoercion,
  toStringValue as toStringCoercion,
} from "./coercion";
import type { AnyColumn } from "./columns";
import { createDecodeError } from "./errors";
import { assertValidSqlIdentifier } from "./internal/identifier";
import { createTableFunctionSource } from "./query";
import {
  type BuildContext,
  compileValue,
  createExpression,
  type Decoder,
  ensureExpression,
  type InferData,
  joinSqlParts,
  passThroughDecoder,
  type Selection,
} from "./query-shared";
import { sql } from "./sql";

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

const isFloatingSqlType = (sqlType?: string) => {
  return sqlType?.startsWith("Float") || sqlType?.startsWith("BFloat");
};

const numberDecoder: Decoder<number> = toNumberCoercion;
const stringDecoder: Decoder<string> = toStringCoercion;
const dateDecoder: Decoder<Date> = toDateCoercion;
const booleanDecoder: Decoder<boolean> = toBooleanCoercion;

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
  toDate(expression: unknown): Selection<Date> {
    return createFunctionExpression("toDate", [expression], {
      decoder: dateDecoder,
      sqlType: "Date",
    });
  },
  toDateTime(expression: unknown, timezone?: unknown): Selection<Date> {
    const args = timezone === undefined ? [expression] : [expression, timezone];
    return createFunctionExpression("toDateTime", args, {
      decoder: dateDecoder,
      sqlType: "DateTime",
    });
  },
  toStartOfMonth(expression: unknown): Selection<Date> {
    return createFunctionExpression("toStartOfMonth", [expression], {
      decoder: dateDecoder,
      sqlType: "Date",
    });
  },
  coalesce<TData = unknown>(...args: unknown[]): Selection<TData> {
    const decoder =
      args.length > 0 && ensureExpression(args[0]).decoder
        ? (ensureExpression(args[0]).decoder as Decoder<TData>)
        : (passThroughDecoder as Decoder<TData>);
    return createFunctionExpression("coalesce", args, {
      decoder,
    });
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
  arrayZip(...args: unknown[]): Selection<unknown[]> {
    return createArrayExpression<unknown>("arrayZip", args);
  },
  array<TData = unknown>(first: unknown, ...rest: unknown[]): Selection<TData[]> {
    return createArrayExpression<TData>("array", [first, ...rest]);
  },
  arrayConcat<TData = unknown>(...arrays: unknown[]): Selection<TData[]> {
    return createArrayExpression<TData>("arrayConcat", arrays);
  },
  arrayElement<TData = unknown>(array: unknown, index: unknown): Selection<TData> {
    return createFunctionExpression<TData>("arrayElement", [array, index]);
  },
  arrayElementOrNull<TData = unknown>(array: unknown, index: unknown): Selection<TData | null> {
    return createFunctionExpression<TData | null>("arrayElementOrNull", [array, index]);
  },
  arraySlice<TData = unknown>(array: unknown, offset: unknown, length?: unknown): Selection<TData[]> {
    const args = length === undefined ? [array, offset] : [array, offset, length];
    return createArrayExpression<TData>("arraySlice", args);
  },
  arrayFlatten<TData = unknown>(array: unknown): Selection<TData[]> {
    return createArrayExpression<TData>("arrayFlatten", [array]);
  },
  arrayIntersect<TData = unknown>(first: unknown, ...rest: unknown[]): Selection<TData[]> {
    return createArrayExpression<TData>("arrayIntersect", [first, ...rest]);
  },
  indexOf(array: unknown, value: unknown): Selection<string> {
    return createFunctionExpression("indexOf", [array, value], {
      decoder: stringDecoder,
      sqlType: "UInt64",
    });
  },
  length(value: unknown): Selection<string> {
    return createFunctionExpression("length", [value], {
      decoder: stringDecoder,
      sqlType: "UInt64",
    });
  },
  notEmpty(value: unknown): Selection<boolean> {
    return createFunctionExpression("notEmpty", [value], {
      decoder: booleanDecoder,
      sqlType: "Bool",
    });
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

const aggregateFns = {
  count(expression?: unknown): Selection<string> {
    const args = expression === undefined ? [] : [expression];
    return createFunctionExpression("count", args, {
      decoder: stringDecoder,
      sqlType: "UInt64",
    });
  },
  countIf(condition: unknown): Selection<string> {
    return createFunctionExpression("countIf", [condition], {
      decoder: stringDecoder,
      sqlType: "UInt64",
    });
  },
  sum(expression: unknown): Selection<number | string> {
    return createFunctionExpression<number | string>("sum", [expression], {
      decoder: resolveAggregateDecoder(expression),
    });
  },
  sumIf(expression: unknown, condition: unknown): Selection<number | string> {
    return createFunctionExpression<number | string>("sumIf", [expression, condition], {
      decoder: resolveAggregateDecoder(expression),
    });
  },
  avg(expression: unknown): Selection<number> {
    return createFunctionExpression("avg", [expression], {
      decoder: numberDecoder,
      sqlType: "Float64",
    });
  },
  min<TData = unknown>(expression: unknown): Selection<TData> {
    const wrapped = ensureExpression<TData>(expression);
    return createFunctionExpression<TData>("min", [expression], {
      decoder: wrapped.decoder,
      sqlType: wrapped.sqlType,
    });
  },
  max<TData = unknown>(expression: unknown): Selection<TData> {
    const wrapped = ensureExpression<TData>(expression);
    return createFunctionExpression<TData>("max", [expression], {
      decoder: wrapped.decoder,
      sqlType: wrapped.sqlType,
    });
  },
  uniqExact(expression: unknown): Selection<string> {
    return createFunctionExpression("uniqExact", [expression], {
      decoder: stringDecoder,
      sqlType: "UInt64",
    });
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

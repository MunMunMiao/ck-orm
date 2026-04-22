import {
  toBoolean as toBooleanCoercion,
  toDate as toDateCoercion,
  toNumber as toNumberCoercion,
  toStringValue as toStringCoercion,
} from "./coercion";
import { createDecodeError } from "./errors";
import { assertValidSqlIdentifier } from "./internal/identifier";
import { createTableFunctionSource } from "./query";
import {
  type BuildContext,
  compileValue,
  createExpression,
  type Decoder,
  ensureExpression,
  joinSqlParts,
  passThroughDecoder,
  type SqlExpression,
} from "./query-shared";
import { sql } from "./sql";

/* ── shared helpers ────────────────────────────────────────────── */

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
): SqlExpression<TData> => {
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
): SqlExpression<TData> => {
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

const resolveAggregateDecoder = (expression?: unknown): Decoder<number | string> => {
  const wrapped = expression ? ensureExpression(expression) : undefined;
  if (wrapped && isFloatingSqlType(wrapped.sqlType)) {
    return numberDecoder;
  }
  return stringDecoder;
};

/* ── scalar functions ──────────────────────────────────────────── */

const scalarFns = {
  call<TData = unknown>(name: string, ...args: unknown[]): SqlExpression<TData> {
    return createFunctionExpression<TData>(name, args);
  },
  withParams<TData = unknown>(name: string, parameters: readonly unknown[], ...args: unknown[]): SqlExpression<TData> {
    return createParameterizedFunctionExpression<TData>(name, parameters, args);
  },
  toString(expression: unknown, timezone?: unknown): SqlExpression<string> {
    const args = timezone === undefined ? [expression] : [expression, timezone];
    return createFunctionExpression("toString", args, {
      decoder: stringDecoder,
      sqlType: "String",
    });
  },
  toDate(expression: unknown): SqlExpression<Date> {
    return createFunctionExpression("toDate", [expression], {
      decoder: dateDecoder,
      sqlType: "Date",
    });
  },
  toDateTime(expression: unknown, timezone?: unknown): SqlExpression<Date> {
    const args = timezone === undefined ? [expression] : [expression, timezone];
    return createFunctionExpression("toDateTime", args, {
      decoder: dateDecoder,
      sqlType: "DateTime",
    });
  },
  toStartOfMonth(expression: unknown): SqlExpression<Date> {
    return createFunctionExpression("toStartOfMonth", [expression], {
      decoder: dateDecoder,
      sqlType: "Date",
    });
  },
  coalesce<TData = unknown>(...args: unknown[]): SqlExpression<TData> {
    const decoder =
      args.length > 0 && ensureExpression(args[0]).decoder
        ? (ensureExpression(args[0]).decoder as Decoder<TData>)
        : (passThroughDecoder as Decoder<TData>);
    return createFunctionExpression("coalesce", args, {
      decoder,
    });
  },
  tuple(...args: unknown[]): SqlExpression<unknown[]> {
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
  arrayZip(...args: unknown[]): SqlExpression<unknown[]> {
    return createFunctionExpression("arrayZip", args, {
      decoder: (value) => {
        if (!Array.isArray(value)) {
          throw createDecodeError(`Cannot convert value to arrayZip array: ${String(value)}`, value);
        }
        return value;
      },
      sqlType: "Array",
    });
  },
  not(expression: unknown): SqlExpression<boolean> {
    return createFunctionExpression("not", [expression], {
      decoder: booleanDecoder,
      sqlType: "Bool",
    });
  },
};

/* ── aggregate functions ───────────────────────────────────────── */

const aggregateFns = {
  count(expression?: unknown): SqlExpression<string> {
    const args = expression === undefined ? [] : [expression];
    return createFunctionExpression("count", args, {
      decoder: stringDecoder,
      sqlType: "UInt64",
    });
  },
  sum(expression: unknown): SqlExpression<number | string> {
    return createFunctionExpression<number | string>("sum", [expression], {
      decoder: resolveAggregateDecoder(expression),
    });
  },
  sumIf(expression: unknown, condition: unknown): SqlExpression<number | string> {
    return createFunctionExpression<number | string>("sumIf", [expression, condition], {
      decoder: resolveAggregateDecoder(expression),
    });
  },
  avg(expression: unknown): SqlExpression<number> {
    return createFunctionExpression("avg", [expression], {
      decoder: numberDecoder,
      sqlType: "Float64",
    });
  },
  uniqExact(expression: unknown): SqlExpression<string> {
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
 * `fn.sum(...)`, `fn.coalesce(...)`, `Object.keys(fn)` and dynamic access
 * (`fn[name]`) all keep working.
 */
export const fn = {
  ...scalarFns,
  ...aggregateFns,
};

/**
 * Table function helpers (e.g. `tableFn.call("numbers", 100)`).
 */
export const tableFn = {
  ...tableFns,
};

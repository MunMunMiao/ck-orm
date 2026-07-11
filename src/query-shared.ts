import { createDecodeError } from "./errors";
import { isRecord } from "./internal/predicates";
import {
  allocParam,
  type BuildContext,
  inferPrimitiveType,
  isSqlFragment,
  isTrustedSqlExpressionObject,
  type SQLFragment,
  sql,
  trustSqlExpressionObject,
} from "./sql";

export type { DecodeError } from "./errors";
export type { BuildContext, QueryParams } from "./sql";

// Phantom brand symbols — declared as unique compile-time symbols, never
// materialised at runtime. They make `TData` / `TSourceKey` reachable for
// `InferData` and source-key inference without paying for a per-expression
// property slot. Marked optional so the runtime objects don't need to define
// them; TypeScript still infers the parameter through the brand position.
declare const dataTypeBrand: unique symbol;
declare const sourceKeyBrand: unique symbol;
declare const insertDataBrand: unique symbol;
declare const isGeneratedBrand: unique symbol;
declare const hasDefaultBrand: unique symbol;
declare const nestedColumnBrand: unique symbol;
declare const nestedRequiredBrand: unique symbol;

export type Decoder<TData> = (value: unknown) => TData;

export type Encoder<TData> = (value: TData) => unknown;

export interface TypedValue<TData> {
  readonly [dataTypeBrand]?: TData;
}

export type InferData<TValue> = TValue extends TypedValue<infer TData> ? TData : never;

// Phantom marker carrying insert-side type information for a column. `TInsert`
// is the type accepted at insert time (defaults to the column's select TData
// via `Column extends ColumnIoMarker<TData, false, false>`). `TIsGenerated`
// removes the column from `$inferInsert` when set to `true` (materialized /
// alias columns). `THasDefault` flips the column to optional in `$inferInsert`
// when set to `true` (columns with a DEFAULT expression).
export interface ColumnIoMarker<
  TInsert = unknown,
  TIsGenerated extends boolean = false,
  THasDefault extends boolean = false,
> {
  readonly [insertDataBrand]?: TInsert;
  readonly [isGeneratedBrand]?: TIsGenerated;
  readonly [hasDefaultBrand]?: THasDefault;
}

export type InferInsertData<TValue> = TValue extends ColumnIoMarker<infer TInsert, boolean, boolean> ? TInsert : never;

export type IsColumnGenerated<TValue> =
  TValue extends ColumnIoMarker<unknown, infer G, boolean> ? (G extends true ? true : false) : false;

export type ColumnHasDefault<TValue> =
  TValue extends ColumnIoMarker<unknown, boolean, infer D> ? (D extends true ? true : false) : false;

// Phantom marker carrying nested-column insert data. Set by `ckType.nested()`
// and preserved across every column-chain method (`.default` / `.$type` /
// `.$validator` / etc) because it uses unique symbols orthogonal to
// `ColumnIoMarker`. Drives `RequiredInsertKeys` to treat nested columns as
// optional on insert by default — mirroring the runtime where a missing
// nested value becomes ClickHouse's empty parallel array.
//
// `TRequiredOnInsert` is flipped to `true` by `Column.requiredOnInsert()` for
// callers who want a business-level "must supply nested data" guard at the
// type layer.
export interface NestedColumnBrand<TInsert = unknown, TRequiredOnInsert extends boolean = false> {
  readonly [nestedColumnBrand]?: TInsert;
  readonly [nestedRequiredBrand]?: TRequiredOnInsert;
}

export type IsNestedColumn<TValue> = TValue extends NestedColumnBrand<unknown, boolean> ? true : false;

export type IsNestedRequiredOnInsert<TValue> =
  TValue extends NestedColumnBrand<unknown, infer R> ? (R extends true ? true : false) : false;

export interface Selection<TData = unknown, TSourceKey extends string | undefined = string | undefined>
  extends TypedValue<TData> {
  readonly [sourceKeyBrand]?: TSourceKey;
  as<TAlias extends string>(alias: TAlias): AliasedSelection<TData, TAlias, TSourceKey>;
  mapWith<TNext>(decoder: Decoder<TNext>): Selection<TNext, TSourceKey>;
}

export interface SqlExpression<TData = unknown, TSourceKey extends string | undefined = string | undefined>
  extends Selection<TData, TSourceKey> {
  readonly kind: "expression" | "column";
  readonly sqlType?: string;
  readonly decoder: Decoder<TData>;
  readonly outputAlias?: string;
  readonly sourceKey?: TSourceKey;
  compile(ctx: BuildContext): SQLFragment;
}

export type Predicate<TSourceKey extends string | undefined = string | undefined> = Selection<boolean, TSourceKey>;

export interface AliasedSelection<
  TData = unknown,
  _TAlias extends string = string,
  TSourceKey extends string | undefined = string | undefined,
> extends Selection<TData, TSourceKey> {}

export interface Order {
  readonly expression: Selection<unknown>;
  readonly direction: "asc" | "desc";
}

/** @internal Window-capable `fn.*` expressions install this compiler hook. */
export type WindowExpressionCompiler = (ctx: BuildContext, clause: SQLFragment) => SQLFragment;

const windowExpressionCompilerSymbol = Symbol("clickhouseORMWindowExpressionCompiler");

type WindowCapableExpression = SqlExpression<unknown> & {
  readonly [windowExpressionCompilerSymbol]: WindowExpressionCompiler;
};

export interface SelectionMeta<TData = unknown> {
  readonly key: string;
  readonly sqlAlias: string;
  readonly decoder: Decoder<TData>;
  readonly path: readonly [string] | readonly [string, string];
  readonly nullable?: boolean;
  readonly groupNullable?: boolean;
}

export const passThroughDecoder: Decoder<unknown> = (value) => value;

export const createExpression = <TData, TSourceKey extends string | undefined = string | undefined>(config: {
  compile: (ctx: BuildContext) => SQLFragment;
  decoder: Decoder<TData>;
  sqlType?: string;
  outputAlias?: string;
  sourceKey?: TSourceKey;
  windowCompiler?: WindowExpressionCompiler;
}): SqlExpression<TData, TSourceKey> => {
  const expression = {
    kind: "expression" as const,
    sqlType: config.sqlType,
    decoder: config.decoder,
    outputAlias: config.outputAlias,
    sourceKey: config.sourceKey,
    compile: config.compile,
    [windowExpressionCompilerSymbol]: config.windowCompiler,
    as<TAlias extends string>(alias: TAlias): AliasedSelection<TData, TAlias, TSourceKey> {
      return createExpression({
        ...config,
        outputAlias: alias,
      }) as AliasedSelection<TData, TAlias, TSourceKey>;
    },
    mapWith<TNext>(decoder: Decoder<TNext>): SqlExpression<TNext, TSourceKey> {
      return createExpression({
        ...config,
        decoder,
      }) as SqlExpression<TNext, TSourceKey>;
    },
  };

  return trustSqlExpressionObject(expression);
};

export const hasWindowExpressionCompiler = (value: unknown): value is WindowCapableExpression => {
  return (
    isExpression(value) && typeof (value as WindowCapableExpression)[windowExpressionCompilerSymbol] === "function"
  );
};

export const compileWindowExpression = (
  value: WindowCapableExpression,
  ctx: BuildContext,
  clause: SQLFragment,
): SQLFragment => value[windowExpressionCompilerSymbol](ctx, clause);

export const isExpression = (value: unknown): value is SqlExpression<unknown> => {
  return (
    isRecord(value) &&
    isTrustedSqlExpressionObject(value) &&
    (value.kind === "expression" || value.kind === "column") &&
    typeof value.compile === "function"
  );
};

export const getExpressionSourceKey = (value: unknown): string | undefined => {
  if (!isExpression(value)) {
    return undefined;
  }
  return value.sourceKey;
};

const createTypedParam = (ctx: BuildContext, value: unknown, sqlType: string): SQLFragment => {
  return sql.raw(allocParam(ctx, value, sqlType));
};

export const compileValue = (value: unknown, ctx: BuildContext, sqlType?: string): SQLFragment => {
  if (isExpression(value)) {
    return value.compile(ctx);
  }
  if (isSqlFragment(value)) {
    return value;
  }

  return createTypedParam(ctx, value, sqlType ?? inferPrimitiveType(value));
};

export const wrapSql = <TData = unknown>(
  value: SQLFragment,
  config?: { decoder?: Decoder<TData>; sqlType?: string },
): SqlExpression<TData> => {
  return createExpression({
    compile: () => value,
    decoder: config?.decoder ?? (passThroughDecoder as Decoder<TData>),
    sqlType: config?.sqlType,
  });
};

export const ensureExpression = <TData = unknown>(
  value: unknown,
  config?: { decoder?: Decoder<TData>; sqlType?: string },
): SqlExpression<TData> => {
  if (isExpression(value)) {
    return value as SqlExpression<TData>;
  }
  if (isSqlFragment(value)) {
    return wrapSql(value, {
      decoder: (config?.decoder ?? value.decoder) as Decoder<TData>,
      sqlType: config?.sqlType,
    });
  }
  return createExpression({
    compile: (ctx) => compileValue(value, ctx, config?.sqlType),
    decoder: config?.decoder ?? (passThroughDecoder as Decoder<TData>),
    sqlType: config?.sqlType,
  });
};

export const joinSqlParts = (parts: SQLFragment[], separator: string): SQLFragment => {
  if (parts.length === 0) {
    return sql.raw("");
  }
  return sql.join(parts, separator);
};

export const decodeValue = <TData>(decoder: Decoder<TData>, value: unknown, columnName: string): TData => {
  try {
    return decoder(value);
  } catch (error) {
    throw createDecodeError(`Failed to decode column: ${columnName}`, value, {
      responseText: error instanceof Error ? error.message : String(error),
    });
  }
};

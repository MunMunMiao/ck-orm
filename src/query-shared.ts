import { createDecodeError } from "./errors";
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

const dataTypeSymbol = Symbol("clickhouseORMDataType");
const sourceKeySymbol = Symbol("clickhouseORMSourceKey");

export type Decoder<TData> = (value: unknown) => TData;

export type Encoder<TData> = (value: TData) => unknown;

export interface TypedValue<TData> {
  readonly [dataTypeSymbol]: TData;
}

export type InferData<TValue> = TValue extends TypedValue<infer TData> ? TData : never;

export interface Selection<TData = unknown, TSourceKey extends string | undefined = string | undefined>
  extends TypedValue<TData> {
  readonly [sourceKeySymbol]: TSourceKey;
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
}): SqlExpression<TData, TSourceKey> => {
  const expression = {
    [dataTypeSymbol]: undefined as TData,
    [sourceKeySymbol]: undefined as TSourceKey,
    kind: "expression" as const,
    sqlType: config.sqlType,
    decoder: config.decoder,
    outputAlias: config.outputAlias,
    sourceKey: config.sourceKey,
    compile: config.compile,
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

const isObject = (value: unknown): value is Record<string, unknown> => {
  return typeof value === "object" && value !== null;
};

export const isExpression = (value: unknown): value is SqlExpression<unknown> => {
  return (
    isObject(value) &&
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

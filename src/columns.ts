import { toBoolean, toDate, toIntegerNumber, toIntegerString, toNumber, toStringValue, toTimeDate } from "./coercion";
import { createClientValidationError, createDecodeError, type DecodeError, isDecodeError } from "./errors";
import { assertDecimalParams, type DecimalParams, formatDecimalSqlType } from "./internal/decimal";
import { assertValidSqlIdentifier } from "./internal/identifier";
import { createExpression, type Decoder, type Encoder, type InferData, type SqlExpression } from "./query-shared";
import { type SQLFragment, sql, trustSqlExpressionObject } from "./sql";

export interface ColumnBinding<
  TTableName extends string = string,
  TTableAlias extends string | undefined = string | undefined,
> {
  readonly key?: string;
  readonly name: string;
  readonly tableName: TTableName;
  readonly tableAlias?: TTableAlias;
}

type ResolveSourceKey<
  TTableName extends string | undefined,
  TTableAlias extends string | undefined,
> = TTableAlias extends string ? TTableAlias : TTableName;

export interface Column<
  TData = unknown,
  TSqlType extends string = string,
  TTableName extends string | undefined = string | undefined,
  TTableAlias extends string | undefined = string | undefined,
> extends SqlExpression<TData, ResolveSourceKey<TTableName, TTableAlias>> {
  readonly kind: "column";
  readonly key?: string;
  readonly name?: string;
  readonly configuredName?: string;
  readonly tableName?: TTableName;
  readonly tableAlias?: TTableAlias;
  readonly sqlType: TSqlType;
  readonly ddl?: ColumnDdlConfig;
  readonly decimalConfig?: DecimalParams;
  mapToDriverValue(value: TData): unknown;
  mapFromDriverValue(value: unknown): TData;
  cast(precision: number, scale: number): SQLFragment<string>;
  bind<TNextTableName extends string, TNextTableAlias extends string | undefined = undefined>(
    binding: ColumnBinding<TNextTableName, TNextTableAlias>,
  ): Column<TData, TSqlType, TNextTableName, TNextTableAlias>;
  default(expression: DdlFragmentInput): Column<TData, TSqlType, TTableName, TTableAlias>;
  materialized(expression: DdlFragmentInput): Column<TData, TSqlType, TTableName, TTableAlias>;
  aliasExpr(expression: DdlFragmentInput): Column<TData, TSqlType, TTableName, TTableAlias>;
  comment(text: string): Column<TData, TSqlType, TTableName, TTableAlias>;
  codec(expression: DdlFragmentInput): Column<TData, TSqlType, TTableName, TTableAlias>;
  ttl(expression: DdlFragmentInput): Column<TData, TSqlType, TTableName, TTableAlias>;
}

export type AnyColumn = Column<unknown, string, string | undefined, string | undefined>;

export type DdlFragmentInput = string | SQLFragment<unknown>;

export interface ColumnDdlConfig {
  readonly default?: DdlFragmentInput;
  readonly materialized?: DdlFragmentInput;
  readonly aliasExpr?: DdlFragmentInput;
  readonly comment?: string;
  readonly codec?: DdlFragmentInput;
  readonly ttl?: DdlFragmentInput;
}

type ColumnFactoryConfig<TData, TSqlType extends string> = {
  readonly configuredName?: string;
  readonly sqlType: TSqlType;
  readonly mapFromDriverValue: Decoder<TData>;
  readonly mapToDriverValue?: Encoder<TData>;
  readonly ddl?: ColumnDdlConfig;
  readonly decimalConfig?: DecimalParams;
  readonly rejectObjectInput?: boolean;
};

const identity = <TData>(value: TData) => value;

type ColumnName = string;
type OptionalColumnName = ColumnName | undefined;
type DecimalConfig = {
  readonly precision: number;
  readonly scale: number;
};
type FixedStringConfig = {
  readonly length: number;
};
type PrecisionConfig = {
  readonly precision: number;
};
type DateTime64Config = PrecisionConfig & {
  readonly timezone?: string;
};
type QBitConfig = {
  readonly dimensions: number;
};
type AggregateFunctionConfig = {
  readonly name: string;
  readonly args?: readonly (AnyColumn | string)[];
};
type SimpleAggregateFunctionConfig = {
  readonly name: string;
  readonly value: AnyColumn;
};

const normalizeConfiguredName = (name: OptionalColumnName): OptionalColumnName => {
  if (name === undefined) {
    return undefined;
  }
  assertValidSqlIdentifier(name);
  return name;
};

const isObjectRecord = (value: unknown): value is Record<string, unknown> => {
  return typeof value === "object" && value !== null && !Array.isArray(value);
};

const isAggregateFunctionConfig = (value: unknown): value is AggregateFunctionConfig => {
  return isObjectRecord(value) && !("kind" in value) && typeof value.name === "string";
};

const withConfiguredName = <TData, TSqlType extends string>(
  config: Omit<ColumnFactoryConfig<TData, TSqlType>, "configuredName">,
  name?: string,
): ColumnFactoryConfig<TData, TSqlType> => {
  return {
    ...config,
    configuredName: normalizeConfiguredName(name),
  };
};

const parseNamedConfig = <TConfig extends object>(
  builderName: string,
  first: string | TConfig,
  second?: TConfig,
): {
  readonly name?: string;
  readonly config: TConfig;
} => {
  if (typeof first === "string") {
    if (!isObjectRecord(second)) {
      throw createClientValidationError(`${builderName}() requires a config object after the column name`);
    }
    return {
      name: normalizeConfiguredName(first),
      config: second,
    };
  }

  if (isObjectRecord(first) && second === undefined) {
    return {
      config: first,
    };
  }

  throw createClientValidationError(`${builderName}() requires a config object`);
};

const parseNamedValue = <TValue>(
  builderName: string,
  first: string | TValue,
  second?: TValue,
): {
  readonly name?: string;
  readonly value: TValue;
} => {
  if (typeof first === "string") {
    if (second === undefined) {
      throw createClientValidationError(`${builderName}() requires a value after the column name`);
    }
    return {
      name: normalizeConfiguredName(first),
      value: second,
    };
  }

  return {
    value: first,
  };
};

const parseNamedPair = <TLeft, TRight>(
  builderName: string,
  first: string | TLeft,
  second: TLeft | TRight,
  third?: TRight,
): {
  readonly name?: string;
  readonly left: TLeft;
  readonly right: TRight;
} => {
  if (typeof first === "string") {
    if (third === undefined) {
      throw createClientValidationError(`${builderName}() requires two values after the column name`);
    }
    return {
      name: normalizeConfiguredName(first),
      left: second as TLeft,
      right: third,
    };
  }

  return {
    left: first,
    right: second as TRight,
  };
};

const assertPositiveInteger = (label: string, value: number): void => {
  if (!Number.isInteger(value) || value <= 0) {
    throw createClientValidationError(`${label} must be a positive integer, got ${value}`);
  }
};

const assertPrecision = (label: string, value: number): void => {
  if (!Number.isInteger(value) || value < 0 || value > 9) {
    throw createClientValidationError(`${label} must be an integer between 0 and 9, got ${value}`);
  }
};

const assertSafeAggregateTypeArg = (value: string): void => {
  const trimmed = value.trim();
  const hasUnsafeToken = /;|--|\/\*|\*\/|`|"|\n|\r/.test(trimmed);
  const hasValidShape = /^[A-Za-z][A-Za-z0-9_]*(?:\([A-Za-z0-9_,\s'()+\-./:]*\))?$/.test(trimmed);
  if (trimmed === "" || hasUnsafeToken || !hasValidShape) {
    throw createClientValidationError(
      `Invalid AggregateFunction argument type: ${value}. Use ckType column helpers for complex types.`,
    );
  }
};

const rethrowDecodeWithPath = (error: unknown, segment: string, originalValue: unknown): DecodeError => {
  if (isDecodeError(error)) {
    const innerPath = error.path ?? "";
    const combined = innerPath.startsWith("[")
      ? `${segment}${innerPath}`
      : innerPath
        ? `${segment}.${innerPath}`
        : segment;
    return createDecodeError(
      error.message.replace(/^\[ck-orm\]\s*/, "").replace(/\s*\(at .*\)$/, ""),
      error.causeValue,
      {
        path: combined,
      },
    );
  }
  const message = error instanceof Error ? error.message : String(error);
  return createDecodeError(message, originalValue, { path: segment });
};

const ddlModeLabels = {
  default: "DEFAULT",
  materialized: "MATERIALIZED",
  aliasExpr: "ALIAS",
} as const;

type DdlModeKey = keyof typeof ddlModeLabels;

const mergeColumnDdl = (
  current: ColumnDdlConfig | undefined,
  patch: Partial<ColumnDdlConfig>,
  mode?: DdlModeKey,
): ColumnDdlConfig => {
  if (mode) {
    for (const key of Object.keys(ddlModeLabels) as DdlModeKey[]) {
      if (key !== mode && current?.[key] !== undefined) {
        throw createClientValidationError(
          `Column DDL cannot combine ${ddlModeLabels[mode]} with ${ddlModeLabels[key]}`,
        );
      }
    }
  }

  return {
    ...(current ?? {}),
    ...patch,
  };
};

const createColumnFactory = <
  TData,
  TSqlType extends string,
  TTableName extends string | undefined = undefined,
  TTableAlias extends string | undefined = undefined,
>(
  config: ColumnFactoryConfig<TData, TSqlType>,
  binding?: ColumnBinding<string, string | undefined>,
): Column<TData, TSqlType, TTableName, TTableAlias> => {
  const expression = createExpression<TData>({
    compile: () => {
      if (!binding?.name || !binding?.tableName) {
        throw new Error(`Unbound column cannot be compiled: ${config.sqlType}`);
      }

      if (binding.tableAlias) {
        return sql.identifier({
          table: binding.tableAlias,
          column: binding.name,
        });
      }

      return sql.identifier({
        table: binding.tableName,
        column: binding.name,
      });
    },
    decoder: config.mapFromDriverValue,
    sqlType: config.sqlType,
    sourceKey: (binding?.tableAlias ?? binding?.tableName) as ResolveSourceKey<TTableName, TTableAlias>,
  });

  const column = {
    ...expression,
    kind: "column",
    key: binding?.key ?? binding?.name,
    name: binding?.name,
    configuredName: config.configuredName,
    tableName: binding?.tableName as TTableName | undefined,
    tableAlias: binding?.tableAlias as TTableAlias | undefined,
    sqlType: config.sqlType,
    ddl: config.ddl,
    decimalConfig: config.decimalConfig,
    mapFromDriverValue(value: unknown) {
      return config.mapFromDriverValue(value);
    },
    mapToDriverValue(value: TData) {
      if (config.rejectObjectInput && typeof value === "object" && value !== null && !(value instanceof Date)) {
        const columnLabel = binding?.name ?? config.configuredName ?? config.sqlType;
        throw createClientValidationError(
          `${config.sqlType} column "${columnLabel}" expects string | number; got an object. ` +
            `If you are using decimal.js, call .toFixed(scale) before passing it to ck-orm.`,
        );
      }
      return (config.mapToDriverValue ?? identity)(value);
    },
    cast(precision: number, scale: number): SQLFragment<string> {
      return sql.decimal(this, precision, scale);
    },
    bind<TNextTableName extends string, TNextTableAlias extends string | undefined = undefined>(
      nextBinding: ColumnBinding<TNextTableName, TNextTableAlias>,
    ) {
      return createColumnFactory<TData, TSqlType, TNextTableName, TNextTableAlias>(config, nextBinding);
    },
    default(expression: DdlFragmentInput) {
      return createColumnFactory<TData, TSqlType, TTableName, TTableAlias>(
        {
          ...config,
          ddl: mergeColumnDdl(config.ddl, { default: expression }, "default"),
        },
        binding,
      );
    },
    materialized(expression: DdlFragmentInput) {
      return createColumnFactory<TData, TSqlType, TTableName, TTableAlias>(
        {
          ...config,
          ddl: mergeColumnDdl(config.ddl, { materialized: expression }, "materialized"),
        },
        binding,
      );
    },
    aliasExpr(expression: DdlFragmentInput) {
      return createColumnFactory<TData, TSqlType, TTableName, TTableAlias>(
        {
          ...config,
          ddl: mergeColumnDdl(config.ddl, { aliasExpr: expression }, "aliasExpr"),
        },
        binding,
      );
    },
    comment(text: string) {
      return createColumnFactory<TData, TSqlType, TTableName, TTableAlias>(
        {
          ...config,
          ddl: mergeColumnDdl(config.ddl, { comment: text }),
        },
        binding,
      );
    },
    codec(expression: DdlFragmentInput) {
      return createColumnFactory<TData, TSqlType, TTableName, TTableAlias>(
        {
          ...config,
          ddl: mergeColumnDdl(config.ddl, { codec: expression }),
        },
        binding,
      );
    },
    ttl(expression: DdlFragmentInput) {
      return createColumnFactory<TData, TSqlType, TTableName, TTableAlias>(
        {
          ...config,
          ddl: mergeColumnDdl(config.ddl, { ttl: expression }),
        },
        binding,
      );
    },
  } as unknown as Column<TData, TSqlType, TTableName, TTableAlias>;
  return trustSqlExpressionObject(column);
};

export type Int8<TData extends number = number> = Column<TData, "Int8">;
export type Int16<TData extends number = number> = Column<TData, "Int16">;
export type Int32<TData extends number = number> = Column<TData, "Int32">;
export type Int64<TData extends string = string> = Column<TData, "Int64">;
export type UInt8<TData extends number = number> = Column<TData, "UInt8">;
export type UInt16<TData extends number = number> = Column<TData, "UInt16">;
export type UInt32<TData extends number = number> = Column<TData, "UInt32">;
export type UInt64<TData extends string = string> = Column<TData, "UInt64">;
export type Float32<TData extends number = number> = Column<TData, "Float32">;
export type Float64<TData extends number = number> = Column<TData, "Float64">;
export type BFloat16<TData extends number = number> = Column<TData, "BFloat16">;
export type StringColumn<TData extends string = string> = Column<TData, "String">;
export type FixedString<TData extends string = string> = Column<TData, `FixedString(${number})`>;
export type Decimal<TData extends string = string> = Column<TData, string>;
export type DateColumn<TData extends Date = Date> = Column<TData, "Date">;
export type Date32<TData extends Date = Date> = Column<TData, "Date32">;
export type Time<TData extends Date = Date> = Column<TData, "Time">;
export type Time64<TData extends Date = Date> = Column<TData, string>;
export type DateTime<TData extends Date = Date> = Column<TData, "DateTime">;
export type DateTime64<TData extends Date = Date> = Column<TData, string>;
export type Bool<TData extends boolean = boolean> = Column<TData, "Bool">;
export type UUID<TData extends string = string> = Column<TData, "UUID">;
export type IPv4<TData extends string = string> = Column<TData, "IPv4">;
export type IPv6<TData extends string = string> = Column<TData, "IPv6">;
export type JsonColumn<TData = unknown> = Column<TData, "JSON">;
export type Dynamic<TData = unknown> = Column<TData, "Dynamic">;
export type QBit<TData extends readonly number[] = readonly number[]> = Column<TData, string>;
export type Enum8<TData extends string = string> = Column<TData, string>;
export type Enum16<TData extends string = string> = Column<TData, string>;
export type Nullable<TInner extends AnyColumn> = Column<InferData<TInner> | null, string>;
export type ArrayColumn<TInner extends AnyColumn> = Column<InferData<TInner>[], string>;
export type TupleColumn<TItems extends readonly AnyColumn[]> = Column<
  { [K in keyof TItems]: InferData<TItems[K]> },
  string
>;
export type MapColumn<_TKey extends AnyColumn, TValue extends AnyColumn> = Column<
  Record<string, InferData<TValue>>,
  string
>;
export type VariantColumn<TItems extends readonly AnyColumn[]> = Column<InferData<TItems[number]>, string>;
export type LowCardinality<TInner extends AnyColumn> = Column<InferData<TInner>, string>;
export type NestedColumn<TShape extends Record<string, AnyColumn>> = Column<
  { [K in keyof TShape]: InferData<TShape[K]> }[],
  string
>;
export type AggregateFunction<TData = string> = Column<TData, string>;
export type SimpleAggregateFunction<TData = string> = Column<TData, string>;
export type Point<TData extends readonly [number, number] = readonly [number, number]> = Column<TData, "Point">;
export type Ring<TData extends readonly [number, number][] = readonly [number, number][]> = Column<TData, "Ring">;
export type LineString<TData extends readonly [number, number][] = readonly [number, number][]> = Column<
  TData,
  "LineString"
>;
export type MultiLineString<TData extends readonly [number, number][][] = readonly [number, number][][]> = Column<
  TData,
  "MultiLineString"
>;
export type Polygon<TData extends readonly [number, number][][] = readonly [number, number][][]> = Column<
  TData,
  "Polygon"
>;
export type MultiPolygon<TData extends readonly [number, number][][][] = readonly [number, number][][][]> = Column<
  TData,
  "MultiPolygon"
>;

const numericColumn = <TData, TSqlType extends string>(sqlType: TSqlType, decoder: Decoder<TData>, name?: string) =>
  createColumnFactory<TData, TSqlType>(
    withConfiguredName({ sqlType, mapFromDriverValue: decoder, mapToDriverValue: decoder }, name),
  );

const integerStringColumn = <TSqlType extends "Int64" | "UInt64">(
  sqlType: TSqlType,
  name?: string,
  unsigned?: boolean,
) =>
  createColumnFactory<string, TSqlType>(
    withConfiguredName(
      {
        sqlType,
        mapFromDriverValue: (value) => toIntegerString(value, { unsigned }),
        mapToDriverValue: (value) => toIntegerString(value, { unsigned }),
      },
      name,
    ),
  );

const geometryColumn = <TData, TSqlType extends string>(sqlType: TSqlType, name?: string) =>
  createColumnFactory<TData, TSqlType>(
    withConfiguredName(
      {
        sqlType,
        mapFromDriverValue: (value) => value as TData,
      },
      name,
    ),
  );

export const int8 = (name?: string): Int8<number> =>
  numericColumn("Int8", (value) => toIntegerNumber(value, { min: -128, max: 127 }), name);
export const int16 = (name?: string): Int16<number> =>
  numericColumn("Int16", (value) => toIntegerNumber(value, { min: -32768, max: 32767 }), name);
export const int32 = (name?: string): Int32<number> =>
  numericColumn("Int32", (value) => toIntegerNumber(value, { min: -2147483648, max: 2147483647 }), name);
export const int64 = (name?: string): Int64<string> => integerStringColumn("Int64", name);
export const uint8 = (name?: string): UInt8<number> =>
  numericColumn("UInt8", (value) => toIntegerNumber(value, { min: 0, max: 255 }), name);
export const uint16 = (name?: string): UInt16<number> =>
  numericColumn("UInt16", (value) => toIntegerNumber(value, { min: 0, max: 65535 }), name);
export const uint32 = (name?: string): UInt32<number> =>
  numericColumn("UInt32", (value) => toIntegerNumber(value, { min: 0, max: 4294967295 }), name);
export const uint64 = (name?: string): UInt64<string> => integerStringColumn("UInt64", name, true);
export const float32 = (name?: string): Float32<number> => numericColumn("Float32", toNumber, name);
export const float64 = (name?: string): Float64<number> => numericColumn("Float64", toNumber, name);
export const bfloat16 = (name?: string): BFloat16<number> => numericColumn("BFloat16", toNumber, name);
export const string = (name?: string): StringColumn<string> =>
  createColumnFactory(withConfiguredName({ sqlType: "String", mapFromDriverValue: toStringValue }, name));
export function fixedString(config: FixedStringConfig): FixedString<string>;
export function fixedString(name: string, config: FixedStringConfig): FixedString<string>;
export function fixedString(first: string | FixedStringConfig, second?: FixedStringConfig): FixedString<string> {
  const { name, config } = parseNamedConfig("fixedString", first, second);
  const { length } = config;
  if (!Number.isInteger(length) || length <= 0) {
    throw createClientValidationError(`fixedString length must be a positive integer, got ${length}`);
  }
  return createColumnFactory({
    configuredName: name,
    sqlType: `FixedString(${length})`,
    mapFromDriverValue: toStringValue,
  });
}
export function decimal(config: DecimalConfig): Decimal<string>;
export function decimal(name: string, config: DecimalConfig): Decimal<string>;
export function decimal(first: string | DecimalConfig, second?: DecimalConfig): Decimal<string> {
  const { name, config } = parseNamedConfig("decimal", first, second);
  const { precision, scale } = config;
  assertDecimalParams({ precision, scale }, "decimal");
  return createColumnFactory({
    configuredName: name,
    sqlType: formatDecimalSqlType({ precision, scale }),
    mapFromDriverValue: toStringValue,
    mapToDriverValue: toStringValue,
    decimalConfig: { precision, scale },
    rejectObjectInput: true,
  });
}
export const date = (name?: string): DateColumn<Date> =>
  createColumnFactory(withConfiguredName({ sqlType: "Date", mapFromDriverValue: toDate }, name));
export const date32 = (name?: string): Date32<Date> =>
  createColumnFactory(withConfiguredName({ sqlType: "Date32", mapFromDriverValue: toDate }, name));
export const time = (name?: string): Time<Date> =>
  createColumnFactory(withConfiguredName({ sqlType: "Time", mapFromDriverValue: toTimeDate }, name));
export function time64(config: PrecisionConfig): Time64<Date>;
export function time64(name: string, config: PrecisionConfig): Time64<Date>;
export function time64(first: string | PrecisionConfig, second?: PrecisionConfig): Time64<Date> {
  const { name, config } = parseNamedConfig("time64", first, second);
  const { precision } = config;
  assertPrecision("time64 precision", precision);
  return createColumnFactory({
    configuredName: name,
    sqlType: `Time64(${precision})`,
    mapFromDriverValue: toTimeDate,
  });
}
export const dateTime = (name?: string): DateTime<Date> =>
  createColumnFactory(withConfiguredName({ sqlType: "DateTime", mapFromDriverValue: toDate }, name));
const escapeSqlSingleQuoted = (value: string) => value.replaceAll("\\", "\\\\").replaceAll("'", "\\'");

export function dateTime64(config: DateTime64Config): DateTime64<Date>;
export function dateTime64(name: string, config: DateTime64Config): DateTime64<Date>;
export function dateTime64(first: string | DateTime64Config, second?: DateTime64Config): DateTime64<Date> {
  const { name, config } = parseNamedConfig("dateTime64", first, second);
  const { precision, timezone } = config;
  assertPrecision("dateTime64 precision", precision);
  const suffix = timezone ? `, '${escapeSqlSingleQuoted(timezone)}'` : "";
  return createColumnFactory({
    configuredName: name,
    sqlType: `DateTime64(${precision}${suffix})`,
    mapFromDriverValue: toDate,
  });
}
export const bool = (name?: string): Bool<boolean> =>
  createColumnFactory(withConfiguredName({ sqlType: "Bool", mapFromDriverValue: toBoolean }, name));
export const uuid = (name?: string): UUID<string> =>
  createColumnFactory(withConfiguredName({ sqlType: "UUID", mapFromDriverValue: toStringValue }, name));
export const ipv4 = (name?: string): IPv4<string> =>
  createColumnFactory(withConfiguredName({ sqlType: "IPv4", mapFromDriverValue: toStringValue }, name));
export const ipv6 = (name?: string): IPv6<string> =>
  createColumnFactory(withConfiguredName({ sqlType: "IPv6", mapFromDriverValue: toStringValue }, name));
export const json = <TData = unknown>(name?: string): JsonColumn<TData> =>
  createColumnFactory(
    withConfiguredName(
      {
        sqlType: "JSON",
        mapFromDriverValue: (value) => value as TData,
        mapToDriverValue: (value) => value,
      },
      name,
    ),
  );
export const dynamic = <TData = unknown>(name?: string): Dynamic<TData> =>
  createColumnFactory(
    withConfiguredName(
      {
        sqlType: "Dynamic",
        mapFromDriverValue: (value) => value as TData,
        mapToDriverValue: (value) => value,
      },
      name,
    ),
  );
export function qbit<TInner extends Float32 | Float64 | BFloat16, TData extends readonly number[] = readonly number[]>(
  inner: TInner,
  config: QBitConfig,
): QBit<TData>;
export function qbit<TInner extends Float32 | Float64 | BFloat16, TData extends readonly number[] = readonly number[]>(
  name: string,
  inner: TInner,
  config: QBitConfig,
): QBit<TData>;
export function qbit<TInner extends Float32 | Float64 | BFloat16, TData extends readonly number[] = readonly number[]>(
  first: string | TInner,
  second: TInner | QBitConfig,
  third?: QBitConfig,
): QBit<TData> {
  const { name, left: inner, right: config } = parseNamedPair<TInner, QBitConfig>("qbit", first, second, third);
  const { dimensions } = config;
  assertPositiveInteger("qbit dimensions", dimensions);
  return createColumnFactory({
    configuredName: name,
    sqlType: `QBit(${inner.sqlType}, ${dimensions})`,
    mapFromDriverValue: (value) => {
      if (!Array.isArray(value)) {
        throw createDecodeError(`Cannot convert value to qbit array: ${String(value)}`, value);
      }
      return value as unknown as TData;
    },
    mapToDriverValue: (value) => value,
  });
}
export function enum8<TData extends string = string>(values: Record<string, number>): Enum8<TData>;
export function enum8<TData extends string = string>(name: string, values: Record<string, number>): Enum8<TData>;
export function enum8<TData extends string = string>(
  first: string | Record<string, number>,
  second?: Record<string, number>,
): Enum8<TData> {
  const { name, value: values } = parseNamedValue<Record<string, number>>("enum8", first, second);
  return createColumnFactory({
    configuredName: name,
    sqlType: `Enum8(${Object.entries(values)
      .map(([key, value]) => `'${escapeSqlSingleQuoted(key)}' = ${value}`)
      .join(", ")})`,
    mapFromDriverValue: toStringValue as Decoder<TData>,
  });
}
export function enum16<TData extends string = string>(values: Record<string, number>): Enum16<TData>;
export function enum16<TData extends string = string>(name: string, values: Record<string, number>): Enum16<TData>;
export function enum16<TData extends string = string>(
  first: string | Record<string, number>,
  second?: Record<string, number>,
): Enum16<TData> {
  const { name, value: values } = parseNamedValue<Record<string, number>>("enum16", first, second);
  return createColumnFactory({
    configuredName: name,
    sqlType: `Enum16(${Object.entries(values)
      .map(([key, value]) => `'${escapeSqlSingleQuoted(key)}' = ${value}`)
      .join(", ")})`,
    mapFromDriverValue: toStringValue as Decoder<TData>,
  });
}
export function nullable<TInner extends AnyColumn>(inner: TInner): Nullable<TInner>;
export function nullable<TInner extends AnyColumn>(name: string, inner: TInner): Nullable<TInner>;
export function nullable<TInner extends AnyColumn>(first: string | TInner, second?: TInner): Nullable<TInner> {
  const { name, value: inner } = parseNamedValue<TInner>("nullable", first, second);
  if (/^(Array|Map|Tuple)\(/.test(inner.sqlType)) {
    throw createClientValidationError(
      `Nullable(${inner.sqlType}) is not supported by ClickHouse; wrap Nullable around fields inside the composite type instead`,
    );
  }

  return createColumnFactory<InferData<TInner> | null, string>({
    configuredName: name,
    sqlType: `Nullable(${inner.sqlType})`,
    mapFromDriverValue: (value) => {
      if (value === null || value === undefined) {
        return null;
      }
      return inner.mapFromDriverValue(value) as InferData<TInner>;
    },
    mapToDriverValue: (value) => {
      if (value === null) {
        return null;
      }
      return inner.mapToDriverValue(value as InferData<TInner>);
    },
    decimalConfig: inner.decimalConfig,
  });
}
export function array<TInner extends AnyColumn>(inner: TInner): ArrayColumn<TInner>;
export function array<TInner extends AnyColumn>(name: string, inner: TInner): ArrayColumn<TInner>;
export function array<TInner extends AnyColumn>(first: string | TInner, second?: TInner): ArrayColumn<TInner> {
  const { name, value: inner } = parseNamedValue<TInner>("array", first, second);
  return createColumnFactory({
    configuredName: name,
    sqlType: `Array(${inner.sqlType})`,
    mapFromDriverValue: (value) => {
      if (!Array.isArray(value)) {
        throw createDecodeError(`Cannot convert value to array: ${String(value)}`, value);
      }
      return value.map((item, index) => {
        try {
          return inner.mapFromDriverValue(item) as InferData<TInner>;
        } catch (error) {
          throw rethrowDecodeWithPath(error, `[${index}]`, item);
        }
      });
    },
    mapToDriverValue: (value) => value.map((item) => inner.mapToDriverValue(item)),
  });
}
export function tuple<const TItems extends readonly AnyColumn[]>(...items: TItems): TupleColumn<TItems>;
export function tuple<const TItems extends readonly AnyColumn[]>(name: string, ...items: TItems): TupleColumn<TItems>;
export function tuple<const TItems extends readonly AnyColumn[]>(
  first: string | TItems[number],
  ...rest: TItems
): TupleColumn<TItems> {
  const name = typeof first === "string" ? normalizeConfiguredName(first) : undefined;
  const items = (typeof first === "string" ? rest : [first, ...rest]) as unknown as TItems;
  return createColumnFactory<{ [K in keyof TItems]: InferData<TItems[K]> }, string>({
    configuredName: name,
    sqlType: `Tuple(${items.map((item) => item.sqlType).join(", ")})`,
    mapFromDriverValue: (value) => {
      if (!Array.isArray(value)) {
        throw createDecodeError(`Cannot convert value to tuple: ${String(value)}`, value);
      }
      if (value.length !== items.length) {
        throw createDecodeError(
          `Cannot convert value to tuple: expected ${items.length} items, got ${value.length}`,
          value,
        );
      }
      return value.map((item, index) => {
        try {
          return items[index]?.mapFromDriverValue(item);
        } catch (error) {
          throw rethrowDecodeWithPath(error, `[${index}]`, item);
        }
      }) as {
        [K in keyof TItems]: InferData<TItems[K]>;
      };
    },
    mapToDriverValue: (value) => {
      if (!Array.isArray(value)) {
        throw createClientValidationError(`Cannot convert value to tuple: ${String(value)}`);
      }
      if (value.length !== items.length) {
        throw createClientValidationError(
          `Cannot convert value to tuple: expected ${items.length} items, got ${value.length}`,
        );
      }
      return value.map((item, index) => items[index]?.mapToDriverValue(item)) as unknown[];
    },
  });
}
export function map<TKey extends AnyColumn, TValue extends AnyColumn>(
  key: TKey,
  value: TValue,
): MapColumn<TKey, TValue>;
export function map<TKey extends AnyColumn, TValue extends AnyColumn>(
  name: string,
  key: TKey,
  value: TValue,
): MapColumn<TKey, TValue>;
export function map<TKey extends AnyColumn, TValue extends AnyColumn>(
  first: string | TKey,
  second: TKey | TValue,
  third?: TValue,
): MapColumn<TKey, TValue> {
  const { name, left: key, right: value } = parseNamedPair<TKey, TValue>("map", first, second, third);
  if (key.sqlType !== "String") {
    throw createClientValidationError(
      `ckType.map() currently supports only String keys because JavaScript records cannot represent ClickHouse duplicate map keys; got ${key.sqlType}`,
    );
  }
  return createColumnFactory<Record<string, InferData<TValue>>, string>({
    configuredName: name,
    sqlType: `Map(${key.sqlType}, ${value.sqlType})`,
    mapFromDriverValue: (input) => {
      if (typeof input !== "object" || input === null) {
        throw createDecodeError(`Cannot convert value to map: ${String(input)}`, input);
      }
      const record: Record<string, InferData<TValue>> = {};
      for (const [recordKey, recordValue] of Object.entries(input)) {
        try {
          record[recordKey] = value.mapFromDriverValue(recordValue) as InferData<TValue>;
        } catch (error) {
          throw rethrowDecodeWithPath(error, `[${JSON.stringify(recordKey)}]`, recordValue);
        }
      }
      return record;
    },
    mapToDriverValue: (input) => {
      const record: Record<string, unknown> = {};
      for (const [recordKey, recordValue] of Object.entries(input)) {
        record[recordKey] = value.mapToDriverValue(recordValue);
      }
      return record;
    },
  });
}
export function variant<const TItems extends readonly AnyColumn[]>(...items: TItems): VariantColumn<TItems>;
export function variant<const TItems extends readonly AnyColumn[]>(
  name: string,
  ...items: TItems
): VariantColumn<TItems>;
export function variant<const TItems extends readonly AnyColumn[]>(
  first: string | TItems[number],
  ...rest: TItems
): VariantColumn<TItems> {
  const name = typeof first === "string" ? normalizeConfiguredName(first) : undefined;
  const items = (typeof first === "string" ? rest : [first, ...rest]) as unknown as TItems;
  return createColumnFactory<InferData<TItems[number]>, string>({
    configuredName: name,
    sqlType: `Variant(${items.map((item) => item.sqlType).join(", ")})`,
    mapFromDriverValue: (value) => value as InferData<TItems[number]>,
    mapToDriverValue: (value) => value,
  });
}
export function lowCardinality<TInner extends AnyColumn>(inner: TInner): LowCardinality<TInner>;
export function lowCardinality<TInner extends AnyColumn>(name: string, inner: TInner): LowCardinality<TInner>;
export function lowCardinality<TInner extends AnyColumn>(
  first: string | TInner,
  second?: TInner,
): LowCardinality<TInner> {
  const { name, value: inner } = parseNamedValue<TInner>("lowCardinality", first, second);
  return createColumnFactory<InferData<TInner>, string>({
    configuredName: name,
    sqlType: `LowCardinality(${inner.sqlType})`,
    mapFromDriverValue: (value) => inner.mapFromDriverValue(value) as InferData<TInner>,
    mapToDriverValue: (value) => inner.mapToDriverValue(value),
    decimalConfig: inner.decimalConfig,
  });
}
export function nested<TShape extends Record<string, AnyColumn>>(shape: TShape): NestedColumn<TShape>;
export function nested<TShape extends Record<string, AnyColumn>>(name: string, shape: TShape): NestedColumn<TShape>;
export function nested<TShape extends Record<string, AnyColumn>>(
  first: string | TShape,
  second?: TShape,
): NestedColumn<TShape> {
  const { name, value: shape } = parseNamedValue<TShape>("nested", first, second);
  return createColumnFactory<{ [K in keyof TShape]: InferData<TShape[K]> }[], string>({
    configuredName: name,
    sqlType: `Nested(${Object.entries(shape)
      .map(([key, value]) => {
        assertValidSqlIdentifier(key, "nested column");
        return `${key} ${value.sqlType}`;
      })
      .join(", ")})`,
    mapFromDriverValue: (value) => {
      if (!Array.isArray(value)) {
        throw createDecodeError(`Cannot convert value to nested: ${String(value)}`, value);
      }
      return value.map((item, index) => {
        if (typeof item !== "object" || item === null) {
          throw createDecodeError(`Cannot convert nested item: ${String(item)}`, item, {
            path: `[${index}]`,
          });
        }
        const record = {} as { [K in keyof TShape]: InferData<TShape[K]> };
        for (const [key, column] of Object.entries(shape)) {
          try {
            (record as Record<string, unknown>)[key] = (column as AnyColumn).mapFromDriverValue(
              (item as Record<string, unknown>)[key],
            );
          } catch (error) {
            throw rethrowDecodeWithPath(error, `[${index}].${key}`, (item as Record<string, unknown>)[key]);
          }
        }
        return record;
      });
    },
    mapToDriverValue: (value) =>
      value.map((item) => {
        const record: Record<string, unknown> = {};
        for (const [key, column] of Object.entries(shape)) {
          record[key] = (column as AnyColumn).mapToDriverValue((item as Record<string, unknown>)[key]);
        }
        return record;
      }),
  });
}
export function aggregateFunction<TData = string>(
  name: string,
  ...args: (AnyColumn | string)[]
): AggregateFunction<TData>;
export function aggregateFunction<TData = string>(
  columnName: string,
  config: AggregateFunctionConfig,
): AggregateFunction<TData>;
export function aggregateFunction<TData = string>(
  first: string,
  ...rest: (AnyColumn | string | AggregateFunctionConfig)[]
): AggregateFunction<TData> {
  const namedConfig = rest.length === 1 && isAggregateFunctionConfig(rest[0]) ? rest[0] : undefined;
  const configuredName = namedConfig ? normalizeConfiguredName(first) : undefined;
  const name = namedConfig ? String(namedConfig.name) : first;
  const args = namedConfig ? (namedConfig.args ?? []) : (rest as (AnyColumn | string)[]);
  assertValidSqlIdentifier(name, "aggregate function");
  for (const arg of args) {
    if (typeof arg === "string") {
      assertSafeAggregateTypeArg(arg);
    }
  }
  return createColumnFactory({
    configuredName,
    sqlType: `AggregateFunction(${name}${args.length > 0 ? `, ${args.map((arg) => (typeof arg === "string" ? arg : arg.sqlType)).join(", ")}` : ""})`,
    mapFromDriverValue: (input) => input as TData,
    mapToDriverValue: (input) => input,
  });
}
export function simpleAggregateFunction<TData = string>(name: string, value: AnyColumn): SimpleAggregateFunction<TData>;
export function simpleAggregateFunction<TData = string>(
  columnName: string,
  config: SimpleAggregateFunctionConfig,
): SimpleAggregateFunction<TData>;
export function simpleAggregateFunction<TData = string>(
  first: string,
  second: AnyColumn | SimpleAggregateFunctionConfig,
): SimpleAggregateFunction<TData> {
  const namedConfig = isObjectRecord(second) && "name" in second && "value" in second ? second : undefined;
  const configuredName = namedConfig ? normalizeConfiguredName(first) : undefined;
  const name = namedConfig ? String(namedConfig.name) : first;
  const value = namedConfig ? (namedConfig.value as AnyColumn) : (second as AnyColumn);
  assertValidSqlIdentifier(name, "simple aggregate function");
  return createColumnFactory({
    configuredName,
    sqlType: `SimpleAggregateFunction(${name}, ${value.sqlType})`,
    mapFromDriverValue: (input) => input as TData,
    mapToDriverValue: (input) => input,
  });
}
export const point = (name?: string): Point => geometryColumn<readonly [number, number], "Point">("Point", name);
export const ring = (name?: string): Ring => geometryColumn<readonly [number, number][], "Ring">("Ring", name);
export const lineString = (name?: string): LineString =>
  geometryColumn<readonly [number, number][], "LineString">("LineString", name);
export const multiLineString = (name?: string): MultiLineString =>
  geometryColumn<readonly [number, number][][], "MultiLineString">("MultiLineString", name);
export const polygon = (name?: string): Polygon =>
  geometryColumn<readonly [number, number][][], "Polygon">("Polygon", name);
export const multiPolygon = (name?: string): MultiPolygon =>
  geometryColumn<readonly [number, number][][][], "MultiPolygon">("MultiPolygon", name);

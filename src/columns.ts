import { toBigInt, toBoolean, toDate, toNumber, toStringValue } from "./coercion";
import { createClientValidationError, createDecodeError, type DecodeError, isDecodeError } from "./errors";
import { assertValidSqlIdentifier } from "./internal/identifier";
import { createExpression, type Decoder, type Encoder, type InferData, type SqlExpression } from "./query-shared";
import { type SQLFragment, sql } from "./sql";

export interface ColumnBinding<
  TTableName extends string = string,
  TTableAlias extends string | undefined = string | undefined,
> {
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
  readonly name?: string;
  readonly tableName?: TTableName;
  readonly tableAlias?: TTableAlias;
  readonly sqlType: TSqlType;
  readonly ddl?: ColumnDdlConfig;
  mapToDriverValue(value: TData): unknown;
  mapFromDriverValue(value: unknown): TData;
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
  readonly sqlType: TSqlType;
  readonly mapFromDriverValue: Decoder<TData>;
  readonly mapToDriverValue?: Encoder<TData>;
  readonly ddl?: ColumnDdlConfig;
};

const identity = <TData>(value: TData) => value;

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

  return {
    ...expression,
    kind: "column",
    name: binding?.name,
    tableName: binding?.tableName as TTableName | undefined,
    tableAlias: binding?.tableAlias as TTableAlias | undefined,
    sqlType: config.sqlType,
    ddl: config.ddl,
    mapFromDriverValue(value: unknown) {
      return config.mapFromDriverValue(value);
    },
    mapToDriverValue(value: TData) {
      return (config.mapToDriverValue ?? identity)(value);
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
};

export type Int8<TData extends number = number> = Column<TData, "Int8">;
export type Int16<TData extends number = number> = Column<TData, "Int16">;
export type Int32<TData extends number = number> = Column<TData, "Int32">;
export type Int64<TData extends bigint = bigint> = Column<TData, "Int64">;
export type UInt8<TData extends number = number> = Column<TData, "UInt8">;
export type UInt16<TData extends number = number> = Column<TData, "UInt16">;
export type UInt32<TData extends number = number> = Column<TData, "UInt32">;
export type UInt64<TData extends bigint = bigint> = Column<TData, "UInt64">;
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

const numericColumn = <TData, TSqlType extends string>(sqlType: TSqlType, decoder: Decoder<TData>) =>
  createColumnFactory<TData, TSqlType>({ sqlType, mapFromDriverValue: decoder });

const geometryColumn = <TData, TSqlType extends string>(sqlType: TSqlType) =>
  createColumnFactory<TData, TSqlType>({
    sqlType,
    mapFromDriverValue: (value) => value as TData,
  });

export const int8 = (): Int8<number> => numericColumn("Int8", toNumber);
export const int16 = (): Int16<number> => numericColumn("Int16", toNumber);
export const int32 = (): Int32<number> => numericColumn("Int32", toNumber);
export const int64 = (): Int64<bigint> => numericColumn("Int64", toBigInt);
export const uint8 = (): UInt8<number> => numericColumn("UInt8", toNumber);
export const uint16 = (): UInt16<number> => numericColumn("UInt16", toNumber);
export const uint32 = (): UInt32<number> => numericColumn("UInt32", toNumber);
export const uint64 = (): UInt64<bigint> => numericColumn("UInt64", toBigInt);
export const float32 = (): Float32<number> => numericColumn("Float32", toNumber);
export const float64 = (): Float64<number> => numericColumn("Float64", toNumber);
export const bfloat16 = (): BFloat16<number> => numericColumn("BFloat16", toNumber);
export const string = (): StringColumn<string> =>
  createColumnFactory({ sqlType: "String", mapFromDriverValue: toStringValue });
export const fixedString = (length: number): FixedString<string> => {
  if (!Number.isInteger(length) || length <= 0) {
    throw createClientValidationError(`fixedString length must be a positive integer, got ${length}`);
  }
  return createColumnFactory({
    sqlType: `FixedString(${length})`,
    mapFromDriverValue: toStringValue,
  });
};
export const decimal = (precision: number, scale: number): Decimal<string> => {
  if (!Number.isInteger(precision) || precision < 1 || precision > 76) {
    throw createClientValidationError(`decimal precision must be an integer between 1 and 76, got ${precision}`);
  }
  if (!Number.isInteger(scale) || scale < 0 || scale > precision) {
    throw createClientValidationError(
      `decimal scale must be an integer between 0 and precision (${precision}), got ${scale}`,
    );
  }
  return createColumnFactory({
    sqlType: `Decimal(${precision}, ${scale})`,
    mapFromDriverValue: toStringValue,
    mapToDriverValue: toStringValue,
  });
};
export const date = (): DateColumn<Date> => createColumnFactory({ sqlType: "Date", mapFromDriverValue: toDate });
export const date32 = (): Date32<Date> => createColumnFactory({ sqlType: "Date32", mapFromDriverValue: toDate });
export const time = (): Time<Date> => createColumnFactory({ sqlType: "Time", mapFromDriverValue: toDate });
export const time64 = (precision: number): Time64<Date> => {
  if (!Number.isInteger(precision) || precision < 0 || precision > 9) {
    throw createClientValidationError(`time64 precision must be an integer between 0 and 9, got ${precision}`);
  }
  return createColumnFactory({
    sqlType: `Time64(${precision})`,
    mapFromDriverValue: toDate,
  });
};
export const dateTime = (): DateTime<Date> => createColumnFactory({ sqlType: "DateTime", mapFromDriverValue: toDate });
const escapeSqlSingleQuoted = (value: string) => value.replaceAll("\\", "\\\\").replaceAll("'", "\\'");

export const dateTime64 = (precision: number, timezone?: string): DateTime64<Date> => {
  const suffix = timezone ? `, '${escapeSqlSingleQuoted(timezone)}'` : "";
  return createColumnFactory({
    sqlType: `DateTime64(${precision}${suffix})`,
    mapFromDriverValue: toDate,
  });
};
export const bool = (): Bool<boolean> => createColumnFactory({ sqlType: "Bool", mapFromDriverValue: toBoolean });
export const uuid = (): UUID<string> => createColumnFactory({ sqlType: "UUID", mapFromDriverValue: toStringValue });
export const ipv4 = (): IPv4<string> => createColumnFactory({ sqlType: "IPv4", mapFromDriverValue: toStringValue });
export const ipv6 = (): IPv6<string> => createColumnFactory({ sqlType: "IPv6", mapFromDriverValue: toStringValue });
export const json = <TData = unknown>(): JsonColumn<TData> =>
  createColumnFactory({
    sqlType: "JSON",
    mapFromDriverValue: (value) => value as TData,
    mapToDriverValue: (value) => value,
  });
export const dynamic = <TData = unknown>(): Dynamic<TData> =>
  createColumnFactory({
    sqlType: "Dynamic",
    mapFromDriverValue: (value) => value as TData,
    mapToDriverValue: (value) => value,
  });
export const qbit = <TInner extends Float32 | Float64 | BFloat16, TData extends readonly number[] = readonly number[]>(
  inner: TInner,
  dimensions: number,
): QBit<TData> => {
  if (!Number.isInteger(dimensions) || dimensions <= 0) {
    throw createClientValidationError(`qbit dimensions must be a positive integer, got ${dimensions}`);
  }
  return createColumnFactory({
    sqlType: `QBit(${inner.sqlType}, ${dimensions})`,
    mapFromDriverValue: (value) => {
      if (!Array.isArray(value)) {
        throw createDecodeError(`Cannot convert value to qbit array: ${String(value)}`, value);
      }
      return value as unknown as TData;
    },
    mapToDriverValue: (value) => value,
  });
};
export const enum8 = <TData extends string = string>(values: Record<string, number>): Enum8<TData> =>
  createColumnFactory({
    sqlType: `Enum8(${Object.entries(values)
      .map(([key, value]) => `'${escapeSqlSingleQuoted(key)}' = ${value}`)
      .join(", ")})`,
    mapFromDriverValue: toStringValue as Decoder<TData>,
  });
export const enum16 = <TData extends string = string>(values: Record<string, number>): Enum16<TData> =>
  createColumnFactory({
    sqlType: `Enum16(${Object.entries(values)
      .map(([key, value]) => `'${escapeSqlSingleQuoted(key)}' = ${value}`)
      .join(", ")})`,
    mapFromDriverValue: toStringValue as Decoder<TData>,
  });
export const nullable = <TInner extends AnyColumn>(inner: TInner): Nullable<TInner> =>
  createColumnFactory<InferData<TInner> | null, string>({
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
  });
export const array = <TInner extends AnyColumn>(inner: TInner): ArrayColumn<TInner> =>
  createColumnFactory<InferData<TInner>[], string>({
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
export const tuple = <const TItems extends readonly AnyColumn[]>(...items: TItems): TupleColumn<TItems> =>
  createColumnFactory<{ [K in keyof TItems]: InferData<TItems[K]> }, string>({
    sqlType: `Tuple(${items.map((item) => item.sqlType).join(", ")})`,
    mapFromDriverValue: (value) => {
      if (!Array.isArray(value)) {
        throw createDecodeError(`Cannot convert value to tuple: ${String(value)}`, value);
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
    mapToDriverValue: (value) => value.map((item, index) => items[index]?.mapToDriverValue(item)) as unknown[],
  });
export const map = <TKey extends AnyColumn, TValue extends AnyColumn>(
  key: TKey,
  value: TValue,
): MapColumn<TKey, TValue> =>
  createColumnFactory<Record<string, InferData<TValue>>, string>({
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
export const variant = <const TItems extends readonly AnyColumn[]>(...items: TItems): VariantColumn<TItems> =>
  createColumnFactory<InferData<TItems[number]>, string>({
    sqlType: `Variant(${items.map((item) => item.sqlType).join(", ")})`,
    mapFromDriverValue: (value) => value as InferData<TItems[number]>,
    mapToDriverValue: (value) => value,
  });
export const lowCardinality = <TInner extends AnyColumn>(inner: TInner): LowCardinality<TInner> =>
  createColumnFactory<InferData<TInner>, string>({
    sqlType: `LowCardinality(${inner.sqlType})`,
    mapFromDriverValue: (value) => inner.mapFromDriverValue(value) as InferData<TInner>,
    mapToDriverValue: (value) => inner.mapToDriverValue(value),
  });
export const nested = <TShape extends Record<string, AnyColumn>>(shape: TShape): NestedColumn<TShape> =>
  createColumnFactory<{ [K in keyof TShape]: InferData<TShape[K]> }[], string>({
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
export const aggregateFunction = <TData = string>(
  name: string,
  ...args: (AnyColumn | string)[]
): AggregateFunction<TData> => {
  assertValidSqlIdentifier(name, "aggregate function");
  return createColumnFactory({
    sqlType: `AggregateFunction(${name}${args.length > 0 ? `, ${args.map((arg) => (typeof arg === "string" ? arg : arg.sqlType)).join(", ")}` : ""})`,
    mapFromDriverValue: (input) => input as TData,
    mapToDriverValue: (input) => input,
  });
};
export const simpleAggregateFunction = <TData = string>(
  name: string,
  value: AnyColumn,
): SimpleAggregateFunction<TData> => {
  assertValidSqlIdentifier(name, "simple aggregate function");
  return createColumnFactory({
    sqlType: `SimpleAggregateFunction(${name}, ${value.sqlType})`,
    mapFromDriverValue: (input) => input as TData,
    mapToDriverValue: (input) => input,
  });
};
export const point = (): Point => geometryColumn<readonly [number, number], "Point">("Point");
export const ring = (): Ring => geometryColumn<readonly [number, number][], "Ring">("Ring");
export const lineString = (): LineString => geometryColumn<readonly [number, number][], "LineString">("LineString");
export const multiLineString = (): MultiLineString =>
  geometryColumn<readonly [number, number][][], "MultiLineString">("MultiLineString");
export const polygon = (): Polygon => geometryColumn<readonly [number, number][][], "Polygon">("Polygon");
export const multiPolygon = (): MultiPolygon =>
  geometryColumn<readonly [number, number][][][], "MultiPolygon">("MultiPolygon");

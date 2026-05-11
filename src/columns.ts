import { toBoolean, toDate, toIntegerNumber, toIntegerString, toNumber, toStringValue } from "./coercion";
import { createClientValidationError, createDecodeError, type DecodeError, isDecodeError } from "./errors";
import { assertIntegerInRange, assertNonNegativeInteger, assertPositiveInteger } from "./internal/assert";
import { normalizeAggregateFunctionSignature, normalizeClickHouseTypeLiteral } from "./internal/clickhouse-type";
import { assertDecimalParams, type DecimalParams, formatDecimalSqlType } from "./internal/decimal";
import { escapeSqlSingleQuoted } from "./internal/escape";
import { assertValidSqlIdentifier } from "./internal/identifier";
import { assertJsonPathIdentifier, parseJsonPathSegments } from "./internal/json-path";
import { isPlainObject } from "./internal/predicates";
import {
  formatStandardSchemaIssues,
  type InferStandardSchemaInput,
  type InferStandardSchemaOutput,
  isStandardSchemaFailure,
  type StandardSchemaV1,
} from "./internal/standard-schema";
import {
  type ColumnIoMarker,
  createExpression,
  type Decoder,
  type Encoder,
  type InferData,
  type SqlExpression,
} from "./query-shared";
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
  // Intentionally does NOT extend ColumnIoMarker — DDL modifiers
  // (`.default()` / `.materialized()` / `.aliasExpr()`) and `$type<{select,
  // insert}>()` intersect the marker on the call site. Helper types
  // (`InsertDataOf` / `IsColumnGenerated` / `ColumnHasDefault`) fall back to
  // the column's own `TData` when no marker is intersected.
  readonly kind: "column";
  readonly key?: string;
  readonly name?: string;
  readonly configuredName?: string;
  readonly tableName?: TTableName;
  readonly tableAlias?: TTableAlias;
  readonly sqlType: TSqlType;
  readonly ddl?: ColumnDdlConfig;
  readonly decimalConfig?: DecimalParams;
  readonly nestedShape?: Record<string, AnyColumn>;
  mapToDriverValue(value: TData): unknown;
  mapFromDriverValue(value: unknown): TData;
  cast(precision: number, scale: number): SQLFragment<string>;
  bind<TNextTableName extends string, TNextTableAlias extends string | undefined = undefined>(
    binding: ColumnBinding<TNextTableName, TNextTableAlias>,
  ): Column<TData, TSqlType, TNextTableName, TNextTableAlias>;
  default(
    expression: DdlFragmentInput,
  ): Column<TData, TSqlType, TTableName, TTableAlias> & ColumnIoMarker<TData, false, true>;
  materialized(
    expression: DdlFragmentInput,
  ): Column<TData, TSqlType, TTableName, TTableAlias> & ColumnIoMarker<never, true, false>;
  aliasExpr(
    expression: DdlFragmentInput,
  ): Column<TData, TSqlType, TTableName, TTableAlias> & ColumnIoMarker<never, true, false>;
  comment(text: string): Column<TData, TSqlType, TTableName, TTableAlias>;
  codec(expression: DdlFragmentInput): Column<TData, TSqlType, TTableName, TTableAlias>;
  ttl(expression: DdlFragmentInput): Column<TData, TSqlType, TTableName, TTableAlias>;
  $type<TIo extends { select: unknown; insert?: unknown }>(): Column<TIo["select"], TSqlType, TTableName, TTableAlias> &
    ColumnIoMarker<TIo extends { insert: infer I } ? I : TIo["select"], false, false>;
  $type<TNext>(): Column<TNext, TSqlType, TTableName, TTableAlias>;
  $validator<TSchema extends StandardSchemaV1<unknown, unknown>>(
    schema: TSchema,
  ): Column<InferStandardSchemaOutput<TSchema>, TSqlType, TTableName, TTableAlias> &
    ColumnIoMarker<InferStandardSchemaInput<TSchema>, false, false>;
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
  readonly nestedShape?: Record<string, AnyColumn>;
  readonly rejectObjectInput?: boolean;
  readonly validator?: StandardSchemaV1<unknown, unknown>;
};

const identity = <TData>(value: TData) => value;

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

const normalizeConfiguredName = (name: string | undefined): string | undefined => {
  if (name === undefined) {
    return undefined;
  }
  assertValidSqlIdentifier(name);
  return name;
};

const isAggregateFunctionConfig = (value: unknown): value is AggregateFunctionConfig => {
  return isPlainObject(value) && !("kind" in value) && typeof value.name === "string";
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
    if (!isPlainObject(second)) {
      throw createClientValidationError(`${builderName}() requires a config object after the column name`);
    }
    return {
      name: normalizeConfiguredName(first),
      config: second,
    };
  }

  if (isPlainObject(first) && second === undefined) return { config: first };

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

const normalizeSafeAggregateTypeArg = (value: string): string => {
  try {
    return normalizeClickHouseTypeLiteral(value);
  } catch {
    throw createClientValidationError(
      `Invalid AggregateFunction argument type: ${value}. Use ckType column helpers for complex types.`,
    );
  }
};

const normalizeAggregateFunctionName = (value: string): string => {
  try {
    return normalizeAggregateFunctionSignature(value);
  } catch {
    throw createClientValidationError(`Invalid aggregate function name: ${value}`);
  }
};

// Strip the `[ck-orm] ` prefix and trailing ` (at path)` suffix that
// `createDecodeError` already added — we re-attach a richer path further
// up the container's decode chain.
const DECODE_ERROR_PREFIX_PATTERN = /^\[ck-orm\]\s*/;
const DECODE_ERROR_PATH_SUFFIX_PATTERN = /\s*\(at .*\)$/;

const rethrowDecodeWithPath = (error: unknown, segment: string, originalValue: unknown): DecodeError => {
  if (isDecodeError(error)) {
    const innerPath = error.path ?? "";
    const combined = innerPath.startsWith("[")
      ? `${segment}${innerPath}`
      : innerPath
        ? `${segment}.${innerPath}`
        : segment;
    return createDecodeError(
      error.message.replace(DECODE_ERROR_PREFIX_PATTERN, "").replace(DECODE_ERROR_PATH_SUFFIX_PATTERN, ""),
      error.causeValue,
      {
        path: combined,
      },
    );
  }
  const message = error instanceof Error ? error.message : String(error);
  return createDecodeError(message, originalValue, { path: segment });
};

// Both `tuple` decode and encode paths assert the same array shape; the only
// difference is the error kind (`decode` vs `client_validation`). Hand the
// factory in so the message text stays in lockstep across the two branches.
type TupleErrorFactory = (message: string, value: unknown) => Error;
const tupleDecodeErrorFactory: TupleErrorFactory = (message, value) => createDecodeError(message, value);
const tupleClientValidationErrorFactory: TupleErrorFactory = (message) => createClientValidationError(message);

function assertTupleArrayShape(
  value: unknown,
  expectedLength: number,
  errorFactory: TupleErrorFactory,
): asserts value is unknown[] {
  if (!Array.isArray(value)) {
    throw errorFactory(`Cannot convert value to tuple: ${String(value)}`, value);
  }
  if (value.length !== expectedLength) {
    throw errorFactory(`Cannot convert value to tuple: expected ${expectedLength} items, got ${value.length}`, value);
  }
}

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

// === JSON column support ===========================================

type JsonHintEntry = {
  readonly segments: readonly string[];
  readonly decode: Decoder<unknown>;
  readonly encode: Encoder<unknown>;
};

/**
 * Compiles the `typeHints` map into a flat list of `{ segments, decode,
 * encode }` triples that the per-row encoder/decoder can walk linearly
 * without re-parsing path strings.
 */
const compileJsonHintEntries = (
  typeHints: JsonConfig<JsonShape>["typeHints"] | undefined,
): readonly JsonHintEntry[] => {
  if (!typeHints) return [];
  const entries: JsonHintEntry[] = [];
  for (const [path, hintColumn] of Object.entries(typeHints)) {
    if (!hintColumn) continue;
    const baseEncode = (hintColumn.mapToDriverValue ?? identity) as Encoder<unknown>;
    entries.push({
      segments: parseJsonPathSegments(path),
      decode: hintColumn.mapFromDriverValue as Decoder<unknown>,
      encode: baseEncode,
    });
  }
  return entries;
};

const getByJsonPath = (root: Record<string, unknown>, segments: readonly string[]): unknown => {
  let current: unknown = root;
  for (const segment of segments) {
    if (current === null || current === undefined) return undefined;
    if (typeof current !== "object" || Array.isArray(current)) return undefined;
    current = (current as Record<string, unknown>)[segment];
  }
  return current;
};

const setByJsonPath = (root: Record<string, unknown>, segments: readonly string[], value: unknown): void => {
  if (segments.length === 0) return;
  let cursor: Record<string, unknown> = root;
  for (let index = 0; index < segments.length - 1; index += 1) {
    // biome-ignore lint/style/noNonNullAssertion: bounded loop guarantees presence
    const segment = segments[index]!;
    const next = cursor[segment];
    if (next === null || next === undefined || typeof next !== "object" || Array.isArray(next)) {
      // Leaf is in this segment-1 slot or path drilled into a non-object —
      // either way bail out; getByJsonPath would have returned `undefined`
      // for the same input and the caller already skipped via `continue`.
      return;
    }
    cursor = next as Record<string, unknown>;
  }
  // biome-ignore lint/style/noNonNullAssertion: segments.length >= 1 enforced above
  cursor[segments[segments.length - 1]!] = value;
};

/**
 * Renders the `sqlType` string for a ClickHouse new-version `JSON` column.
 *
 * Output order is fixed and lexicographically stable so external schema
 * diff tools see the same string given the same configuration regardless of
 * how the user populated `typeHints` / `skip` / `skipRegexp`.
 *
 *   JSON
 *   JSON(max_dynamic_paths=N)
 *   JSON(max_dynamic_paths=N, max_dynamic_types=M, a.b UInt32, c String, SKIP debug, SKIP REGEXP '^_tmp')
 */
const renderJsonSqlType = (config?: JsonConfig<JsonShape>): string => {
  if (!config) return "JSON";
  const parts: string[] = [];
  if (config.maxDynamicPaths !== undefined) {
    assertNonNegativeInteger("json.maxDynamicPaths", config.maxDynamicPaths);
    parts.push(`max_dynamic_paths=${config.maxDynamicPaths}`);
  }
  if (config.maxDynamicTypes !== undefined) {
    assertIntegerInRange("json.maxDynamicTypes", config.maxDynamicTypes, 1, 255);
    parts.push(`max_dynamic_types=${config.maxDynamicTypes}`);
  }
  if (config.typeHints) {
    const sortedHints = Object.entries(config.typeHints)
      .filter((entry): entry is [string, AnyColumn] => entry[1] !== undefined)
      .sort((left, right) => (left[0] < right[0] ? -1 : left[0] > right[0] ? 1 : 0));
    for (const [path, hintColumn] of sortedHints) {
      assertJsonPathIdentifier(path);
      parts.push(`${path} ${hintColumn.sqlType}`);
    }
  }
  if (config.skip) {
    for (const path of [...config.skip].sort()) {
      assertJsonPathIdentifier(path);
      parts.push(`SKIP ${path}`);
    }
  }
  if (config.skipRegexp) {
    for (const pattern of [...config.skipRegexp].sort()) {
      parts.push(`SKIP REGEXP '${escapeSqlSingleQuoted(pattern)}'`);
    }
  }
  return parts.length === 0 ? "JSON" : `JSON(${parts.join(", ")})`;
};

/**
 * Hard-validates that an incoming JSON value is a plain object (not array,
 * not scalar, not function). The check exists so the user gets a clear,
 * column-scoped error message rather than the opaque
 * `Cannot parse JSON object` ClickHouse will throw later.
 *
 * `errorFactory` lets the same predicate emit `decode` errors on the read
 * path and `client_validation` errors on the write path with identical
 * wording.
 */
function assertJsonObjectShape(
  value: unknown,
  errorFactory: (message: string, value: unknown) => Error,
): asserts value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    const description = value === null ? "null" : Array.isArray(value) ? "array" : typeof value;
    throw errorFactory(`JSON column expects a plain object, got ${description}`, value);
  }
}

const jsonDecodeErrorFactory = (message: string, value: unknown) => createDecodeError(message, value);
const jsonEncodeErrorFactory = (message: string, _value: unknown) => createClientValidationError(message);

const makeJsonDecoder = <T extends JsonShape>(hintEntries: readonly JsonHintEntry[]): Decoder<T> => {
  return (value) => {
    if (value === null || value === undefined) return value as unknown as T;
    assertJsonObjectShape(value, jsonDecodeErrorFactory);
    if (hintEntries.length === 0) return value as unknown as T;
    const out: Record<string, unknown> = { ...value };
    for (const { segments, decode } of hintEntries) {
      const leaf = getByJsonPath(out, segments);
      if (leaf === undefined) continue;
      try {
        setByJsonPath(out, segments, decode(leaf));
      } catch (err) {
        throw rethrowDecodeWithPath(err, segments.join("."), leaf);
      }
    }
    return out as unknown as T;
  };
};

const makeJsonEncoder = <T extends JsonShape>(hintEntries: readonly JsonHintEntry[]): Encoder<T> => {
  return (value) => {
    if (value === null || value === undefined) return value;
    assertJsonObjectShape(value, jsonEncodeErrorFactory);
    if (hintEntries.length === 0) return value;
    const out: Record<string, unknown> = { ...value };
    for (const { segments, encode } of hintEntries) {
      const leaf = getByJsonPath(out, segments);
      if (leaf === undefined) continue;
      setByJsonPath(out, segments, encode(leaf));
    }
    return out;
  };
};

const isJsonSqlType = (sqlType: string): boolean => sqlType === "JSON" || sqlType.startsWith("JSON(");

export const makeJsonPathFragment = (
  column: AnyColumn,
  segments: readonly string[],
  cast: AnyColumn | undefined,
): SQLFragment<unknown> => {
  const pathText = segments.join(".");
  if (cast) {
    // `column.a.b.:UInt64` — cast.sqlType embedded verbatim; safe because
    // sqlType strings are constructed by ck-orm internals only.
    return sql`${column}.${sql.raw(pathText)}.:${sql.raw(cast.sqlType)}`.mapWith(cast.mapFromDriverValue);
  }
  return sql`${column}.${sql.raw(pathText)}`;
};

export const makeJsonSubobjectFragment = (column: AnyColumn, segments: readonly string[]): SQLFragment<unknown> => {
  // ClickHouse syntax: `column.^a.b.c` — the `^` prefix only attaches to
  // the first segment, downstream segments are plain dot-joined.
  if (segments.length === 0) return sql`${column}`;
  // biome-ignore lint/style/noNonNullAssertion: length checked
  const head = segments[0]!;
  const tail = segments.slice(1);
  const pathText = tail.length > 0 ? `^${head}.${tail.join(".")}` : `^${head}`;
  return sql`${column}.${sql.raw(pathText)}`;
};

export const makeJsonMergedFragment = (column: AnyColumn, segments: readonly string[]): SQLFragment<unknown> => {
  // ClickHouse syntax: `column.@a.b` — `@` prefix on the head segment.
  if (segments.length === 0) return sql`${column}`;
  // biome-ignore lint/style/noNonNullAssertion: length checked
  const head = segments[0]!;
  const tail = segments.slice(1);
  const pathText = tail.length > 0 ? `@${head}.${tail.join(".")}` : `@${head}`;
  return sql`${column}.${sql.raw(pathText)}`;
};

export const makeJsonArrayFragment = (column: AnyColumn, segments: readonly string[]): SQLFragment<unknown> => {
  // ClickHouse syntax: `column.a.b[]` — trailing `[]` after the joined path.
  const pathText = segments.join(".");
  return sql`${column}.${sql.raw(`${pathText}[]`)}`;
};

const attachJsonMethods = (column: Record<string, unknown>): void => {
  const target = column as Record<string, unknown> & { readonly sqlType: string };
  column.path = function path(rawPath: string): SQLFragment<unknown> {
    const segments = parseJsonPathSegments(rawPath);
    return makeJsonPathFragment(target as unknown as AnyColumn, segments, undefined);
  };
  column.castPath = function castPath(rawPath: string, as: AnyColumn): SQLFragment<unknown> {
    const segments = parseJsonPathSegments(rawPath);
    return makeJsonPathFragment(target as unknown as AnyColumn, segments, as);
  };
  column.subobject = function subobject(rawPath: string): SQLFragment<unknown> {
    const segments = parseJsonPathSegments(rawPath);
    return makeJsonSubobjectFragment(target as unknown as AnyColumn, segments);
  };
  column.merged = function merged(rawPath: string): SQLFragment<unknown> {
    const segments = parseJsonPathSegments(rawPath);
    return makeJsonMergedFragment(target as unknown as AnyColumn, segments);
  };
  column.arrayPath = function arrayPath(rawPath: string): SQLFragment<unknown> {
    const segments = parseJsonPathSegments(rawPath);
    return makeJsonArrayFragment(target as unknown as AnyColumn, segments);
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
  // Build the bound `table.column` SQL fragment once at column-definition
  // time — the `binding` is fixed for the lifetime of this column, so the
  // compile callback only needs to hand back the cached fragment. A wide
  // SELECT over 50 columns now skips 50 `sql.identifier({...})` allocations
  // per query.
  const identifierFragment =
    binding?.name && binding?.tableName
      ? sql.identifier({ table: binding.tableAlias ?? binding.tableName, column: binding.name })
      : undefined;
  // Pick the compile callback at factory time too — bound columns get a
  // straight `() => identifierFragment`, unbound columns get a thrower. Saves
  // a per-compile branch for the common bound case.
  const compile: () => SQLFragment = identifierFragment
    ? () => identifierFragment
    : () => {
        throw new Error(`Unbound column cannot be compiled: ${config.sqlType}`);
      };
  const expression = createExpression<TData>({
    compile,
    decoder: config.mapFromDriverValue,
    sqlType: config.sqlType,
    sourceKey: (binding?.tableAlias ?? binding?.tableName) as ResolveSourceKey<TTableName, TTableAlias>,
  });

  // The driver-side encoder is fully determined at column-definition time:
  // bake the `rejectObjectInput` guard and the `mapToDriverValue ?? identity`
  // fallback into one closure so the per-row encode loop only does the
  // actual work — no recurring branch, no recurring `??`.
  const baseEncoder: Encoder<TData> = config.mapToDriverValue ?? (identity as Encoder<TData>);
  const mapToDriverValue: (value: TData) => unknown = config.rejectObjectInput
    ? (value) => {
        if (typeof value === "object" && value !== null && !(value instanceof Date)) {
          const columnLabel = binding?.name ?? config.configuredName ?? config.sqlType;
          throw createClientValidationError(
            `${config.sqlType} column "${columnLabel}" expects string | number; got an object. ` +
              `If you are using decimal.js, call .toFixed(scale) before passing it to ck-orm.`,
          );
        }
        return baseEncoder(value);
      }
    : baseEncoder;

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
    nestedShape: config.nestedShape,
    // Direct reference — no extra forwarding closure on the per-row decode
    // loop. Wide tables decoding millions of rows skip a function call per
    // column-cell.
    mapFromDriverValue: config.mapFromDriverValue,
    mapToDriverValue,
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
    $type<TNext>() {
      return createColumnFactory<TNext, TSqlType, TTableName, TTableAlias>(
        config as unknown as ColumnFactoryConfig<TNext, TSqlType>,
        binding,
      );
    },
    $validator<TSchema extends StandardSchemaV1<unknown, unknown>>(schema: TSchema) {
      type ValidatedOutput = InferStandardSchemaOutput<TSchema>;
      const baseDecode = config.mapFromDriverValue;
      const baseEncode = config.mapToDriverValue ?? (identity as Encoder<TData>);

      const validateSync = (value: unknown, context: "decode" | "encode") => {
        const result = schema["~standard"].validate(value);
        if (result instanceof Promise) {
          const label = context === "decode" ? "decode" : "insert";
          const message = `Async Standard Schema validators are not supported on the ${label} path`;
          if (context === "decode") {
            throw createDecodeError(message, value);
          }
          throw createClientValidationError(message);
        }
        if (isStandardSchemaFailure(result)) {
          const message = formatStandardSchemaIssues(result.issues);
          if (context === "decode") {
            throw createDecodeError(message, value);
          }
          throw createClientValidationError(message);
        }
        return result.value;
      };

      const validatedDecode: Decoder<ValidatedOutput> = (value) => {
        const decoded = baseDecode(value);
        return validateSync(decoded, "decode") as ValidatedOutput;
      };

      const validatedEncode: Encoder<InferStandardSchemaInput<TSchema>> = (value) => {
        const validated = validateSync(value, "encode");
        return baseEncode(validated as TData);
      };

      return createColumnFactory<ValidatedOutput, TSqlType, TTableName, TTableAlias>(
        {
          ...config,
          mapFromDriverValue: validatedDecode,
          mapToDriverValue: validatedEncode as unknown as Encoder<ValidatedOutput>,
          validator: schema,
        },
        binding,
      );
    },
  } as unknown as Column<TData, TSqlType, TTableName, TTableAlias>;
  if (isJsonSqlType(config.sqlType)) {
    // Attach `path` / `castPath` / `subobject` / `merged` / `arrayPath` so
    // `$type` / `$validator` / `.default()` / `.bind()` chains (each of
    // which re-enters `createColumnFactory`) keep the JSON DSL alive.
    attachJsonMethods(column as unknown as Record<string, unknown>);
  }
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
export type Time<TData extends string = string> = Column<TData, "Time">;
export type Time64<TData extends string = string> = Column<TData, string>;
export type DateTime<TData extends Date = Date> = Column<TData, "DateTime">;
export type DateTime64<TData extends Date = Date> = Column<TData, string>;
export type Bool<TData extends boolean = boolean> = Column<TData, "Bool">;
export type UUID<TData extends string = string> = Column<TData, "UUID">;
export type IPv4<TData extends string = string> = Column<TData, "IPv4">;
export type IPv6<TData extends string = string> = Column<TData, "IPv6">;
/**
 * The set of values a ClickHouse `JSON` column may hold at its top level.
 *
 * ClickHouse 24.x+ `JSON` columns can only store JSON objects — top-level
 * arrays, strings, numbers, or booleans are rejected by the server. Enforce
 * the same shape in the TypeScript layer so `ckType.json<string[]>()` and
 * friends fail at compile time rather than at insert time.
 */
export type JsonShape = { [key: string]: unknown };

/**
 * Dotted-path literal union of every reachable nested key in a `JsonShape`.
 *
 * `Paths<{ a: { b: number }; c: string }>` evaluates to `"a" | "a.b" | "c"`.
 * Used to constrain `JsonConfig["typeHints"]` keys and to validate the
 * argument of `JsonColumn.path()` / `castPath()` / `subobject()` so typos
 * fail at compile time.
 */
export type Paths<T> = T extends JsonShape
  ? {
      [K in keyof T & string]: T[K] extends JsonShape ? K | `${K}.${Paths<T[K]>}` : K;
    }[keyof T & string]
  : never;

/**
 * Resolves the TypeScript type at a given dotted path inside a `JsonShape`.
 * `PathValue<{ a: { b: number } }, "a.b">` evaluates to `number`. Unknown
 * paths fall back to `unknown` so dynamic-path call sites stay sound.
 */
export type PathValue<T, P extends string> = P extends `${infer Head}.${infer Rest}`
  ? Head extends keyof T
    ? PathValue<T[Head], Rest>
    : unknown
  : P extends keyof T
    ? T[P]
    : unknown;

/**
 * Parameterized configuration for a ClickHouse new-version `JSON` column.
 *
 * Mirrors the ClickHouse DDL surface area:
 *   `JSON(max_dynamic_paths=N, max_dynamic_types=M, a.b UInt32, SKIP path, SKIP REGEXP '...')`
 *
 * `typeHints` keys are constrained to `Paths<T>` so the schema and the
 * runtime decoder graph stay in lockstep — a typed path declared here is
 * automatically routed through the hint column's `mapFromDriverValue` /
 * `mapToDriverValue` on every row.
 */
export type JsonConfig<T extends JsonShape> = {
  readonly maxDynamicPaths?: number;
  readonly maxDynamicTypes?: number;
  readonly typeHints?: Partial<Record<Paths<T>, AnyColumn>>;
  readonly skip?: readonly string[];
  readonly skipRegexp?: readonly string[];
};

/**
 * ClickHouse new-version `JSON` column. `TData` is constrained to a top-level
 * object because the underlying ClickHouse `JSON` type rejects top-level
 * arrays, strings, numbers, etc.
 *
 * `path` / `castPath` / `subobject` / `merged` / `arrayPath` render the
 * ClickHouse path-access syntax (`json.a.b`, `json.a.b.:UInt64`, `json.^a`,
 * `json.@a`, `json.a[]`). Each is also exposed in the `fn.*` namespace
 * (`fn.jsonPath`, `fn.jsonCast`, ...) for dynamic-path call sites that
 * cannot satisfy `Paths<TData>`.
 */
export interface JsonColumn<
  TData extends JsonShape = JsonShape,
  TTableName extends string | undefined = string | undefined,
  TTableAlias extends string | undefined = string | undefined,
> extends Column<TData, string, TTableName, TTableAlias> {
  path<P extends Paths<TData>>(path: P): SQLFragment<PathValue<TData, P>>;
  castPath<P extends Paths<TData>, TCol extends AnyColumn>(path: P, as: TCol): SQLFragment<InferData<TCol>>;
  subobject<P extends Paths<TData>>(path: P): SQLFragment<unknown>;
  merged<P extends Paths<TData>>(path: P): SQLFragment<unknown>;
  arrayPath<P extends Paths<TData>>(path: P): SQLFragment<unknown>;

  // --- chain modifiers — re-typed to return JsonColumn so path methods
  // survive in the static type the way they do at runtime (each chain link
  // re-enters createColumnFactory which re-attaches them).
  //
  // Note: `$validator` is NOT overridden — its `Column.$validator` contract
  // accepts `StandardSchemaV1<unknown, unknown>`, and a narrower JSON-only
  // override would violate LSP. After `$validator(schema)` the column type
  // erases back to plain `Column`; the runtime path methods are still
  // attached and callable (verified in tests), but you need a type
  // assertion if you want to use them through the static type.
  $type<TIo extends { select: JsonShape; insert?: unknown }>(): JsonColumn<TIo["select"], TTableName, TTableAlias> &
    ColumnIoMarker<TIo extends { insert: infer I } ? I : TIo["select"], false, false>;
  $type<TNext extends JsonShape>(): JsonColumn<TNext, TTableName, TTableAlias>;
  default(
    expression: DdlFragmentInput,
  ): JsonColumn<TData, TTableName, TTableAlias> & ColumnIoMarker<TData, false, true>;
  materialized(
    expression: DdlFragmentInput,
  ): JsonColumn<TData, TTableName, TTableAlias> & ColumnIoMarker<never, true, false>;
  aliasExpr(
    expression: DdlFragmentInput,
  ): JsonColumn<TData, TTableName, TTableAlias> & ColumnIoMarker<never, true, false>;
  comment(text: string): JsonColumn<TData, TTableName, TTableAlias>;
  codec(expression: DdlFragmentInput): JsonColumn<TData, TTableName, TTableAlias>;
  ttl(expression: DdlFragmentInput): JsonColumn<TData, TTableName, TTableAlias>;
  bind<TNextTableName extends string, TNextTableAlias extends string | undefined = undefined>(
    binding: ColumnBinding<TNextTableName, TNextTableAlias>,
  ): JsonColumn<TData, TNextTableName, TNextTableAlias>;
}

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
) => {
  // The driver-side and JS-side encodings are identical for these widths;
  // share one closure instead of allocating two.
  const transform = (value: unknown): string => toIntegerString(value, { unsigned });
  return createColumnFactory<string, TSqlType>(
    withConfiguredName({ sqlType, mapFromDriverValue: transform, mapToDriverValue: transform }, name),
  );
};

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
  assertPositiveInteger("fixedString length", length);
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
    mapToDriverValue: toDecimalDriverValue,
    decimalConfig: { precision, scale },
    rejectObjectInput: true,
  });
}

/**
 * Encoder for `decimal()` columns. Rejects JS `number` inputs because IEEE 754
 * doubles cannot represent values like `0.1 + 0.2` precisely (round-trips
 * `"0.30000000000000004"`) nor scientific-notation results like `1e30`
 * (ClickHouse's Decimal parser rejects `"1e+30"` outright). Pass strings or
 * BigInts to preserve precision — same convention as Prisma, TypeORM, and
 * Sequelize.
 */
const toDecimalDriverValue = (value: unknown): string => {
  if (typeof value === "number") {
    throw createClientValidationError(
      `decimal column requires a string or bigint value to preserve precision; ` +
        `got JS number ${value}. Use String(value) or BigInt(value) at the call site instead.`,
    );
  }
  return toStringValue(value);
};

/**
 * Encoder for `date`/`date32` columns. ClickHouse Date columns reject ISO 8601
 * strings with `Z` suffix even with `best_effort`, so we cannot let `Date`
 * pass through to `JSON.stringify`. We extract the UTC `YYYY-MM-DD` part
 * before serialising. Strings and numbers pass through verbatim.
 *
 * Why default to UTC: a JS `Date` is an unambiguous UTC instant (per
 * [ECMA-262](https://tc39.es/ecma262/#sec-date-objects)), so taking its UTC
 * date components gives a deterministic answer. Callers who want a different
 * date can construct the `Date` so its UTC components hit the desired day,
 * or pass a pre-formatted `'YYYY-MM-DD'` string.
 */
const createDateOnlyEncoder = (columnLabel: "Date" | "Date32"): Encoder<Date> => {
  return (value) => {
    if (typeof value === "string" || typeof value === "number") {
      return value;
    }
    if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
      throw createClientValidationError(`${columnLabel} column values must be a Date, string, or number`);
    }
    // toISOString() always returns 'YYYY-MM-DDTHH:mm:ss.sssZ'; slice to date part.
    return value.toISOString().slice(0, 10);
  };
};

export const date = (name?: string): DateColumn<Date> =>
  createColumnFactory(
    withConfiguredName(
      {
        sqlType: "Date",
        mapFromDriverValue: toDate,
        mapToDriverValue: createDateOnlyEncoder("Date"),
      },
      name,
    ),
  );

export const date32 = (name?: string): Date32<Date> =>
  createColumnFactory(
    withConfiguredName(
      {
        sqlType: "Date32",
        mapFromDriverValue: toDate,
        mapToDriverValue: createDateOnlyEncoder("Date32"),
      },
      name,
    ),
  );

/**
 * Encoder for `time`/`time64` columns. ClickHouse `Time`/`Time64` represents
 * time-of-day or duration **independent of any calendar date**, with a range
 * of `[-999:59:59, +999:59:59]` (signed, can exceed 24h). JS `Date` cannot
 * faithfully represent this — it's bound to a specific instant on a calendar.
 *
 * Therefore Date inputs are rejected; callers must pass `'HH:MM:SS[.fff]'`
 * strings or integers (interpreted by ClickHouse as scaled units per
 * `Time64(N)` precision). This matches the official `@clickhouse/client`
 * pattern (see `examples/node/coding/time_time64.ts`).
 */
const createTimeEncoder = (columnLabel: "Time" | "Time64"): Encoder<string> => {
  return (value) => {
    if (typeof value === "string" || typeof value === "number") {
      return value;
    }
    throw createClientValidationError(
      `${columnLabel} column values must be a 'HH:MM:SS' string or integer; ` +
        `JS Date is not appropriate (${columnLabel} can be negative or exceed 24h, Date cannot represent that). ` +
        `See https://clickhouse.com/docs/sql-reference/data-types/${columnLabel === "Time" ? "time" : "time64"}`,
    );
  };
};

export const time = (name?: string): Time<string> =>
  createColumnFactory(
    withConfiguredName(
      { sqlType: "Time", mapFromDriverValue: toStringValue, mapToDriverValue: createTimeEncoder("Time") },
      name,
    ),
  );
export function time64(config: PrecisionConfig): Time64<string>;
export function time64(name: string, config: PrecisionConfig): Time64<string>;
export function time64(first: string | PrecisionConfig, second?: PrecisionConfig): Time64<string> {
  const { name, config } = parseNamedConfig("time64", first, second);
  const { precision } = config;
  assertIntegerInRange("time64 precision", precision, 0, 9);
  return createColumnFactory({
    configuredName: name,
    sqlType: `Time64(${precision})`,
    mapFromDriverValue: toStringValue,
    mapToDriverValue: createTimeEncoder("Time64"),
  });
}
/**
 * Encoder for `dateTime`/`dateTime64` columns. `Date` passes through unchanged
 * so `JSON.stringify` (in `insertJsonEachRow`) emits ISO 8601 with `Z`,
 * which `date_time_input_format=best_effort` (auto-injected by
 * `insertJsonEachRow`) accepts as a timezone-anchored UTC instant. Strings
 * and numbers pass through for callers who already produce a
 * ClickHouse-acceptable literal or Unix timestamp.
 */
const createDateTimeEncoder = (columnLabel: "DateTime" | "DateTime64"): Encoder<Date> => {
  return (value) => {
    if (typeof value === "string" || typeof value === "number") {
      return value;
    }
    if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
      throw createClientValidationError(`${columnLabel} column values must be a Date, string, or number`);
    }
    return value;
  };
};

export const dateTime = (name?: string): DateTime<Date> =>
  createColumnFactory(
    withConfiguredName(
      {
        sqlType: "DateTime",
        mapFromDriverValue: toDate,
        mapToDriverValue: createDateTimeEncoder("DateTime"),
      },
      name,
    ),
  );

export function dateTime64(config: DateTime64Config): DateTime64<Date>;
export function dateTime64(name: string, config: DateTime64Config): DateTime64<Date>;
export function dateTime64(first: string | DateTime64Config, second?: DateTime64Config): DateTime64<Date> {
  const { name, config } = parseNamedConfig("dateTime64", first, second);
  const { precision, timezone } = config;
  assertIntegerInRange("dateTime64 precision", precision, 0, 9);
  const suffix = timezone ? `, '${escapeSqlSingleQuoted(timezone)}'` : "";
  return createColumnFactory({
    configuredName: name,
    sqlType: `DateTime64(${precision}${suffix})`,
    mapFromDriverValue: toDate,
    mapToDriverValue: createDateTimeEncoder("DateTime64"),
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
export function json<T extends JsonShape = JsonShape>(): JsonColumn<T>;
export function json<T extends JsonShape = JsonShape>(name: string): JsonColumn<T>;
export function json<T extends JsonShape = JsonShape>(config: JsonConfig<T>): JsonColumn<T>;
export function json<T extends JsonShape = JsonShape>(name: string, config: JsonConfig<T>): JsonColumn<T>;
export function json<T extends JsonShape = JsonShape>(
  first?: string | JsonConfig<T>,
  second?: JsonConfig<T>,
): JsonColumn<T> {
  let name: string | undefined;
  let config: JsonConfig<T> | undefined;
  if (typeof first === "string") {
    name = first;
    config = second;
  } else if (isPlainObject(first)) {
    config = first as JsonConfig<T>;
  }
  const hintEntries = compileJsonHintEntries(config?.typeHints);
  return createColumnFactory({
    configuredName: name,
    sqlType: renderJsonSqlType(config as JsonConfig<JsonShape> | undefined),
    mapFromDriverValue: makeJsonDecoder<T>(hintEntries),
    mapToDriverValue: makeJsonEncoder<T>(hintEntries),
  }) as unknown as JsonColumn<T>;
}
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
    mapToDriverValue: identity,
  });
}
// `Enum8` and `Enum16` differ only in width — same key/value validation, same
// decoder. Render the body once and let the public factories pick the label.
const buildEnumColumn = <TData extends string>(
  label: "Enum8" | "Enum16",
  configuredName: string | undefined,
  values: Record<string, number>,
) => {
  const entries = Object.entries(values)
    .map(([key, value]) => `'${escapeSqlSingleQuoted(key)}' = ${value}`)
    .join(", ");
  return createColumnFactory<TData, string>({
    configuredName,
    sqlType: `${label}(${entries})`,
    mapFromDriverValue: toStringValue as Decoder<TData>,
  });
};

export function enum8<const TValues extends Record<string, number>>(
  values: TValues,
): Enum8<Extract<keyof TValues, string>>;
export function enum8<const TValues extends Record<string, number>>(
  name: string,
  values: TValues,
): Enum8<Extract<keyof TValues, string>>;
export function enum8<const TValues extends Record<string, number>>(
  first: string | TValues,
  second?: TValues,
): Enum8<Extract<keyof TValues, string>> {
  const { name, value: values } = parseNamedValue<TValues>("enum8", first, second);
  return buildEnumColumn<Extract<keyof TValues, string>>("Enum8", name, values);
}
export function enum16<const TValues extends Record<string, number>>(
  values: TValues,
): Enum16<Extract<keyof TValues, string>>;
export function enum16<const TValues extends Record<string, number>>(
  name: string,
  values: TValues,
): Enum16<Extract<keyof TValues, string>>;
export function enum16<const TValues extends Record<string, number>>(
  first: string | TValues,
  second?: TValues,
): Enum16<Extract<keyof TValues, string>> {
  const { name, value: values } = parseNamedValue<TValues>("enum16", first, second);
  return buildEnumColumn<Extract<keyof TValues, string>>("Enum16", name, values);
}
// ClickHouse rejects `Nullable(Array|Map|Tuple)` outright — the rule is
// stable, so cache the regex at module load instead of paying the literal
// cost on every `nullable(...)` factory call.
const NULLABLE_FORBIDDEN_INNER_PATTERN = /^(Array|Map|Tuple)\(/;

export function nullable<TInner extends AnyColumn>(inner: TInner): Nullable<TInner>;
export function nullable<TInner extends AnyColumn>(name: string, inner: TInner): Nullable<TInner>;
export function nullable<TInner extends AnyColumn>(first: string | TInner, second?: TInner): Nullable<TInner> {
  const { name, value: inner } = parseNamedValue<TInner>("nullable", first, second);
  if (NULLABLE_FORBIDDEN_INNER_PATTERN.test(inner.sqlType)) {
    throw createClientValidationError(
      `Nullable(${inner.sqlType}) is not supported by ClickHouse; wrap Nullable around fields inside the composite type instead`,
    );
  }

  return createColumnFactory<InferData<TInner> | null, string>({
    configuredName: name,
    sqlType: `Nullable(${inner.sqlType})`,
    mapFromDriverValue: (value) => {
      if (value === null) {
        return null;
      }
      // ClickHouse JSON output never emits `undefined`; treating it as `null`
      // would mask a driver-layer bug. Surface it as a decode error so the
      // caller learns about the malformed payload instead of silently
      // turning it into a NULL.
      if (value === undefined) {
        throw createDecodeError("Nullable column received undefined from the driver; expected null", value);
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
      assertTupleArrayShape(value, items.length, tupleDecodeErrorFactory);
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
      assertTupleArrayShape(value, items.length, tupleClientValidationErrorFactory);
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
    // `LowCardinality(T)` is a storage hint — the wire shape and JS shape are
    // identical to `T`, so forward the inner column's encoders directly
    // instead of wrapping them in passthrough closures.
    sqlType: `LowCardinality(${inner.sqlType})`,
    mapFromDriverValue: inner.mapFromDriverValue as Decoder<InferData<TInner>>,
    mapToDriverValue: inner.mapToDriverValue as Encoder<InferData<TInner>>,
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
  // `shape` is fixed for the lifetime of this column. Captures the entries
  // here so the encode/decode hot loops never re-walk `Object.entries(shape)`
  // per row — for a wide nested column over millions of rows that is the
  // difference between O(rows × fields) entries calls and a single one.
  const shapeEntries = Object.entries(shape) as [string, AnyColumn][];
  for (const [key] of shapeEntries) {
    assertValidSqlIdentifier(key, "nested column");
  }
  const sqlType = `Nested(${shapeEntries.map(([key, value]) => `${key} ${value.sqlType}`).join(", ")})`;

  return createColumnFactory<{ [K in keyof TShape]: InferData<TShape[K]> }[], string>({
    configuredName: name,
    sqlType,
    nestedShape: shape,
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
        const itemRecord = item as Record<string, unknown>;
        const record = {} as Record<string, unknown>;
        for (const [key, column] of shapeEntries) {
          try {
            record[key] = column.mapFromDriverValue(itemRecord[key]);
          } catch (error) {
            throw rethrowDecodeWithPath(error, `[${index}].${key}`, itemRecord[key]);
          }
        }
        return record as { [K in keyof TShape]: InferData<TShape[K]> };
      });
    },
    mapToDriverValue: (value) =>
      value.map((item) => {
        if (typeof item !== "object" || item === null) {
          throw createClientValidationError(`Cannot convert nested item: ${String(item)}`);
        }
        const itemRecord = item as Record<string, unknown>;
        const record: Record<string, unknown> = {};
        for (const [key, column] of shapeEntries) {
          if (!Object.hasOwn(itemRecord, key)) {
            throw createClientValidationError(`Nested item is missing required field "${key}"`);
          }
          record[key] = column.mapToDriverValue(itemRecord[key]);
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
  const name = normalizeAggregateFunctionName(namedConfig ? String(namedConfig.name) : first);
  const args = namedConfig ? (namedConfig.args ?? []) : (rest as (AnyColumn | string)[]);
  const normalizedArgs = args.map((arg) =>
    typeof arg === "string" ? normalizeSafeAggregateTypeArg(arg) : arg.sqlType,
  );
  return createColumnFactory({
    configuredName,
    sqlType: `AggregateFunction(${name}${normalizedArgs.length > 0 ? `, ${normalizedArgs.join(", ")}` : ""})`,
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
  const namedConfig = isPlainObject(second) && "name" in second && "value" in second ? second : undefined;
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

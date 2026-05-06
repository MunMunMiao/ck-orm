import { toStringValue } from "./coercion";
import { createClientValidationError } from "./errors";
import { assertDecimalParams, formatDecimalSqlType } from "./internal/decimal";
import { assertValidSqlIdentifier } from "./internal/identifier";

const sqlBrand = Symbol("clickhouseORMSqlBrand");
const compileSqlSymbol = Symbol("clickhouseORMCompileSql");

// Allow-lists distinguishing framework-built compilable objects from arbitrary
// user-supplied values. Anything looking-like a `compile`/`compileSource`
// callable still has to pass through these `WeakSet`s before its compile path
// runs — otherwise a hostile literal could ship its own SQL into the wire.
const trustedExpressionObjects = new WeakSet<object>();
const trustedSourceObjects = new WeakSet<object>();

export type QueryParams = Record<string, unknown>;
export type QueryParamTypes = Record<string, string>;

export interface BuildContext {
  params: QueryParams;
  paramTypes: QueryParamTypes;
  nextParamIndex: number;
}

type IdentifierValue =
  | string
  | {
      readonly table?: string;
      readonly column?: string;
      readonly as?: string;
    };

type CompilableExpression = {
  compile(ctx: BuildContext): SQLFragment<unknown>;
};

type CompilableSource = {
  compileSource(ctx: BuildContext): SQLFragment<unknown>;
};

type TableLike = {
  readonly kind: "table";
  readonly originalName: string;
  readonly alias?: string;
};

type SqlChunk =
  | { readonly kind: "text"; readonly value: string }
  | { readonly kind: "identifier"; readonly value: IdentifierValue }
  | {
      readonly kind: "param";
      readonly value: unknown;
      readonly sqlType?: string;
    }
  | { readonly kind: "fragment"; readonly value: SQLFragment<unknown> }
  | {
      readonly kind: "runtime";
      readonly value: (ctx: BuildContext) => SQLFragment<unknown>;
    };

export const trustSqlExpressionObject = <TValue extends object>(value: TValue): TValue => {
  trustedExpressionObjects.add(value);
  return value;
};

export const isTrustedSqlExpressionObject = (value: object): boolean => trustedExpressionObjects.has(value);

export const trustSqlSourceObject = <TValue extends object>(value: TValue): TValue => {
  trustedSourceObjects.add(value);
  return value;
};

const INT64_MIN = -(1n << 63n);
const INT64_MAX = (1n << 63n) - 1n;
const UINT64_MAX = (1n << 64n) - 1n;
const INT128_MIN = -(1n << 127n);
const INT128_MAX = (1n << 127n) - 1n;
const UINT128_MAX = (1n << 128n) - 1n;
const INT256_MIN = -(1n << 255n);
const INT256_MAX = (1n << 255n) - 1n;
const UINT256_MAX = (1n << 256n) - 1n;

const inferBigIntType = (value: bigint): string => {
  if (value >= INT64_MIN && value <= INT64_MAX) return "Int64";
  if (value >= 0n && value <= UINT64_MAX) return "UInt64";
  if (value >= INT128_MIN && value <= INT128_MAX) return "Int128";
  if (value >= 0n && value <= UINT128_MAX) return "UInt128";
  if (value >= INT256_MIN && value <= INT256_MAX) return "Int256";
  if (value >= 0n && value <= UINT256_MAX) return "UInt256";
  throw createClientValidationError(`BigInt value ${value} is out of all ClickHouse integer ranges (max UInt256)`);
};

const inferArrayType = (values: readonly unknown[], seen: WeakSet<object>): string => {
  if (values.length === 0) {
    return "Array(String)";
  }

  const elementType = inferPrimitiveTypeWithSeen(values[0], seen);
  for (let index = 1; index < values.length; index += 1) {
    if (inferPrimitiveTypeWithSeen(values[index], seen) !== elementType) {
      return "Array(String)";
    }
  }
  return `Array(${elementType})`;
};

const inferMapValueType = (values: readonly unknown[], seen: WeakSet<object>): string => {
  if (values.length === 0) {
    return "String";
  }

  const firstType = inferPrimitiveTypeWithSeen(values[0], seen);
  for (let index = 1; index < values.length; index += 1) {
    if (inferPrimitiveTypeWithSeen(values[index], seen) !== firstType) return "String";
  }
  return firstType;
};

const trackCycle = <T>(value: object, seen: WeakSet<object>, label: string, fn: () => T): T => {
  if (seen.has(value)) {
    throw createClientValidationError(`Cannot infer SQL type for ${label} containing a circular reference`);
  }
  seen.add(value);
  try {
    return fn();
  } finally {
    seen.delete(value);
  }
};

const inferPrimitiveTypeWithSeen = (value: unknown, seen: WeakSet<object>): string => {
  switch (typeof value) {
    case "bigint":
      return inferBigIntType(value);
    case "boolean":
      return "Bool";
    case "number":
      return Number.isSafeInteger(value) ? "Int64" : "Float64";
    case "string":
      return "String";
    default:
      if (value instanceof Date) {
        return "DateTime64(3)";
      }
      if (Array.isArray(value)) {
        return trackCycle(value, seen, "array", () => inferArrayType(value, seen));
      }
      if (value instanceof Map) {
        return trackCycle(value, seen, "Map", () => `Map(String, ${inferMapValueType([...value.values()], seen)})`);
      }
      if (typeof value === "object" && value !== null) {
        return trackCycle(value, seen, "object", () => `Map(String, ${inferMapValueType(Object.values(value), seen)})`);
      }
      throw createClientValidationError(`Unsupported SQL parameter value: ${String(value)}`);
  }
};

export const inferPrimitiveType = (value: unknown): string => inferPrimitiveTypeWithSeen(value, new WeakSet());

// Whitespace and grouping punctuation only. Keyword/operator separators must
// be passed as `SQLFragment` (e.g. `sql.join(parts, sql\` OR \`)`) so the
// raw-SQL intent is explicit at the call site.
const VALID_JOIN_SEPARATOR = /^[\s,()]+$/;

const assertValidJoinSeparator = (value: string): void => {
  if (!VALID_JOIN_SEPARATOR.test(value)) {
    throw createClientValidationError(
      `Invalid SQL join separator: "${value}". ` +
        `String separators may only contain whitespace and the punctuation ", ( )". ` +
        `Pass an SQLFragment (e.g. sql\` OR \`) for keyword separators.`,
    );
  }
};

const escapeIdentifier = (value: string) => {
  assertValidSqlIdentifier(value);
  return `\`${value.replaceAll("`", "``")}\``;
};

/**
 * Render a ClickHouse identifier to its escaped, backtick-quoted SQL form
 * without going through the `compileSql(sql.identifier(...)).query` pipeline.
 *
 * Use this from contexts that already produce raw SQL strings (DDL builders,
 * temp-table hooks, transport scaffolding) — the parameter channel never sees
 * the identifier, so the full fragment compiler is dead weight.
 *
 * For composing identifiers inside a `sql\`...\`` template, keep using
 * `sql.identifier(...)` — that path lazy-resolves and lets the fragment carry
 * the correct kind through `compileChunk`.
 */
export const quoteIdentifier = (value: IdentifierValue): string => renderIdentifier(value);

const renderIdentifier = (value: IdentifierValue) => {
  if (typeof value === "string") {
    return escapeIdentifier(value);
  }

  const parts: string[] = [];
  if (value.table !== undefined) {
    if (value.table === "") {
      throw createClientValidationError("Invalid SQL identifier object: table must be a non-empty identifier");
    }
    parts.push(escapeIdentifier(value.table));
  }
  if (value.column !== undefined) {
    if (value.column === "") {
      throw createClientValidationError("Invalid SQL identifier object: column must be a non-empty identifier");
    }
    parts.push(escapeIdentifier(value.column));
  }
  const rendered = parts.join(".");

  if (value.as !== undefined) {
    if (value.as === "") {
      throw createClientValidationError("Invalid SQL identifier object: as must be a non-empty identifier");
    }
    if (!rendered) {
      return escapeIdentifier(value.as);
    }
    return `${rendered} as ${escapeIdentifier(value.as)}`;
  }

  if (!rendered) {
    throw createClientValidationError(
      "Invalid SQL identifier object: provide table, column, or as with a non-empty identifier",
    );
  }

  return rendered;
};

const createSqlFragment = <TData = unknown>(config: {
  chunks: readonly SqlChunk[];
  decoder?: (value: unknown) => TData;
  outputAlias?: string;
}): SQLFragment<TData> => {
  const fragment: CompilableSqlFragment<TData> = {
    [sqlBrand]: true,
    chunks: config.chunks,
    decoder: (config.decoder ?? ((value: unknown) => value as TData)) as (value: unknown) => TData,
    outputAlias: config.outputAlias,
    as<TAlias extends string>(alias: TAlias) {
      return createSqlFragment<TData>({
        chunks: config.chunks,
        decoder: fragment.decoder,
        outputAlias: alias,
      });
    },
    mapWith<TNext>(decoder: (value: unknown) => TNext) {
      return createSqlFragment<TNext>({
        chunks: config.chunks,
        decoder,
        outputAlias: config.outputAlias,
      });
    },
    [compileSqlSymbol](ctx: BuildContext) {
      return config.chunks.map((chunk) => compileChunk(chunk, ctx)).join("");
    },
  };

  return fragment;
};

/**
 * Allocates a new positional ClickHouse parameter slot (`{orm_paramN:Type}`)
 * inside `ctx`. Centralises the previously-implicit contract between
 * `sql.ts` and `query-shared.ts` that both sides must agree on the
 * `orm_paramN` naming scheme and on the `nextParamIndex` increment order.
 *
 * Returns the parameter reference string that callers should splice into
 * the compiled SQL output (e.g. `{orm_param3:String}`).
 */
export const allocParam = (ctx: BuildContext, value: unknown, sqlType?: string): string => {
  if (value === undefined || (value === null && sqlType === undefined)) {
    throw createClientValidationError(
      "Raw SQL parameters do not support null or undefined. Use ckSql`NULL` or builder expressions instead.",
    );
  }
  ctx.nextParamIndex += 1;
  const paramName = `orm_param${ctx.nextParamIndex}`;
  const resolvedType = sqlType ?? inferPrimitiveType(value);
  ctx.params[paramName] = value;
  ctx.paramTypes[paramName] = resolvedType;
  return `{${paramName}:${resolvedType}}`;
};

const compileChunk = (chunk: SqlChunk, ctx: BuildContext): string => {
  switch (chunk.kind) {
    case "text":
      return chunk.value;
    case "identifier":
      return renderIdentifier(chunk.value);
    case "fragment":
      return (chunk.value as CompilableSqlFragment)[compileSqlSymbol](ctx);
    case "runtime": {
      const resolved = chunk.value(ctx);
      if (!isCompilableSqlFragment(resolved)) {
        throw createClientValidationError("Invalid SQL fragment: runtime chunks must return a trusted SQL fragment");
      }
      return resolved[compileSqlSymbol](ctx);
    }
    case "param":
      return allocParam(ctx, chunk.value, chunk.sqlType);
  }
};

export interface SQLFragment<TData = unknown> {
  readonly [sqlBrand]: true;
  readonly chunks: readonly SqlChunk[];
  readonly decoder: (value: unknown) => TData;
  readonly outputAlias?: string;
  as<TAlias extends string>(alias: TAlias): SQLFragment<TData>;
  mapWith<TNext>(decoder: (value: unknown) => TNext): SQLFragment<TNext>;
}

type CompilableSqlFragment<TData = unknown> = SQLFragment<TData> & {
  [compileSqlSymbol](ctx: BuildContext): string;
};

const isCompilableSqlFragment = (value: SQLFragment<unknown>): value is CompilableSqlFragment => {
  return compileSqlSymbol in value && typeof value[compileSqlSymbol] === "function";
};

export const isSqlFragment = (value: unknown): value is SQLFragment<unknown> => {
  return typeof value === "object" && value !== null && sqlBrand in value;
};

const isCompilableExpression = (value: unknown): value is CompilableExpression => {
  return (
    typeof value === "object" &&
    value !== null &&
    trustedExpressionObjects.has(value) &&
    "compile" in value &&
    typeof value.compile === "function"
  );
};

const isCompilableSource = (value: unknown): value is CompilableSource => {
  return (
    typeof value === "object" &&
    value !== null &&
    trustedSourceObjects.has(value) &&
    "compileSource" in value &&
    typeof value.compileSource === "function"
  );
};

const isTableLike = (value: unknown): value is TableLike => {
  return (
    typeof value === "object" &&
    value !== null &&
    "kind" in value &&
    value.kind === "table" &&
    "originalName" in value &&
    typeof value.originalName === "string"
  );
};

const normalizeTemplateValue = (value: unknown): SQLFragment<unknown> => {
  if (isSqlFragment(value)) {
    return value;
  }
  if (isCompilableExpression(value)) {
    return createSqlFragment({
      chunks: [
        {
          kind: "runtime",
          value: (ctx) => value.compile(ctx),
        },
      ],
    });
  }
  if (isCompilableSource(value)) {
    return createSqlFragment({
      chunks: [
        {
          kind: "runtime",
          value: (ctx) => value.compileSource(ctx),
        },
      ],
    });
  }
  if (isTableLike(value)) {
    return createSqlFragment({
      chunks: [
        {
          kind: "identifier",
          value: {
            table: value.originalName,
            as: value.alias,
          },
        },
      ],
    });
  }
  return createSqlFragment({
    chunks: [{ kind: "param", value }],
  });
};

type SQLFactory = {
  <TData = unknown>(strings: TemplateStringsArray, ...values: unknown[]): SQLFragment<TData>;
  <TData = unknown>(value: string): SQLFragment<TData>;
  /**
   * Inject a verbatim SQL fragment with **no** parameter binding,
   * escaping, or validation. The string is concatenated into the
   * compiled statement byte-for-byte.
   *
   * **Security:** never pass user input to `sql.raw`. Doing so
   * re-introduces SQL injection. Use the tagged-template form
   * (\`sql\`SELECT ${value}\`\`) or `sql.identifier(...)` for any
   * value derived from end-user input.
   *
   * Intended uses: SQL keywords (`asc`/`desc`), operator literals,
   * pre-validated constants, and the `NULL` literal in places where
   * `null` would otherwise be coerced to a parameter.
   */
  raw(value: string): SQLFragment;
  join(parts: readonly SQLFragment<unknown>[], separator?: string | SQLFragment<unknown>): SQLFragment;
  identifier(value: IdentifierValue): SQLFragment;
  /**
   * Wraps an expression in `CAST(... AS Decimal(precision, scale))` and decodes
   * the row value as `string` (via `toStringValue`) — so any aggregation /
   * arithmetic over a Decimal column round-trips without precision loss.
   *
   * `precision` / `scale` are validated (1 ≤ P ≤ 76, 0 ≤ S ≤ P) and inlined
   * verbatim — ClickHouse does not accept parameterised Decimal precision.
   *
   * The result is always `SQLFragment<string>`. If you need a different decoded
   * shape (branded string, number, etc.), chain `.mapWith(...)` after this
   * helper — that keeps the runtime decoder and the static type aligned.
   */
  decimal(expression: unknown, precision: number, scale: number): SQLFragment<string>;
};

const createSqlFactory = (): SQLFactory => {
  const sqlFactory = (<TData = unknown>(
    first: string | TemplateStringsArray,
    ...values: unknown[]
  ): SQLFragment<TData> => {
    if (typeof first === "string") {
      return createSqlFragment<TData>({
        chunks: [{ kind: "text", value: first }],
      });
    }

    const chunks: SqlChunk[] = [];
    for (let index = 0; index < first.length; index += 1) {
      const text = first[index];
      if (text) {
        chunks.push({ kind: "text", value: text });
      }
      if (index < values.length) {
        const value = values[index];
        chunks.push({ kind: "fragment", value: normalizeTemplateValue(value) });
      }
    }

    return createSqlFragment<TData>({ chunks });
  }) as SQLFactory;

  /**
   * @warning This embeds raw SQL text without any escaping. Never pass user input to this function.
   */
  sqlFactory.raw = (value: string) =>
    createSqlFragment({
      chunks: [{ kind: "text", value }],
    });

  sqlFactory.join = (parts: readonly SQLFragment<unknown>[], separator: string | SQLFragment<unknown> = ", ") => {
    if (parts.length === 0) {
      return sqlFactory.raw("");
    }
    const chunks: SqlChunk[] = [];
    let separatorFragment: SQLFragment;
    if (typeof separator === "string") {
      assertValidJoinSeparator(separator);
      separatorFragment = sqlFactory.raw(separator);
    } else {
      separatorFragment = separator;
    }

    for (const [index, part] of parts.entries()) {
      if (index > 0) {
        chunks.push({ kind: "fragment", value: separatorFragment });
      }
      chunks.push({ kind: "fragment", value: part });
    }

    return createSqlFragment({ chunks });
  };

  sqlFactory.identifier = (value: IdentifierValue) =>
    createSqlFragment({
      chunks: [{ kind: "identifier", value }],
    });

  sqlFactory.decimal = (expression: unknown, precision: number, scale: number): SQLFragment<string> => {
    assertDecimalParams({ precision, scale }, "sql.decimal");
    const inner = isSqlFragment(expression) ? expression : sqlFactory`${expression}`;
    const sqlType = formatDecimalSqlType({ precision, scale });
    const fragment = sqlFactory`CAST(${inner} AS ${sqlFactory.raw(sqlType)})`;
    return fragment.mapWith((value) => toStringValue(value));
  };

  return sqlFactory;
};

export const sql = createSqlFactory();

export const compileSql = (statement: SQLFragment<unknown>, initialContext?: Partial<BuildContext>) => {
  if (!isCompilableSqlFragment(statement)) {
    throw createClientValidationError("Invalid SQL fragment: the provided fragment cannot be compiled");
  }
  const ctx: BuildContext = {
    params: { ...(initialContext?.params ?? {}) },
    paramTypes: { ...(initialContext?.paramTypes ?? {}) },
    nextParamIndex: initialContext?.nextParamIndex ?? 0,
  };
  const query = statement[compileSqlSymbol](ctx);
  return {
    query,
    params: ctx.params,
    paramTypes: ctx.paramTypes,
    nextParamIndex: ctx.nextParamIndex,
  };
};

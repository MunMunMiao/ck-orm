import { createClientValidationError } from "./errors";
import { assertValidSqlIdentifier } from "./internal/identifier";

const sqlBrand = Symbol("clickhouseORMSqlBrand");
const compileSqlSymbol = Symbol("clickhouseORMCompileSql");

export type QueryParams = Record<string, unknown>;

export interface BuildContext {
  params: QueryParams;
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
      readonly value: (ctx: BuildContext) => string | SQLFragment<unknown>;
    };

const inferArrayType = (values: readonly unknown[]): string => {
  if (values.length === 0) {
    return "Array(String)";
  }

  const elementType = inferPrimitiveType(values[0]);
  for (const value of values.slice(1)) {
    if (inferPrimitiveType(value) !== elementType) {
      return "Array(String)";
    }
  }
  return `Array(${elementType})`;
};

const inferMapValueType = (values: readonly unknown[]): string => {
  if (values.length === 0) {
    return "String";
  }

  const firstType = inferPrimitiveType(values[0]);
  for (const value of values.slice(1)) {
    if (inferPrimitiveType(value) !== firstType) return "String";
  }
  return firstType;
};

export const inferPrimitiveType = (value: unknown): string => {
  switch (typeof value) {
    case "bigint":
      return "Int64";
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
        return inferArrayType(value);
      }
      if (value instanceof Map) {
        return `Map(String, ${inferMapValueType([...value.values()])})`;
      }
      if (typeof value === "object" && value !== null) {
        return `Map(String, ${inferMapValueType(Object.values(value))})`;
      }
      throw createClientValidationError(`Unsupported SQL parameter value: ${String(value)}`);
  }
};

const VALID_JOIN_SEPARATOR = /^[\s,()A-Za-z0-9_+\-=<>!]+$/;
const JOIN_SEPARATOR_DENYLIST = /;|--|\/\*|\*\/|`|'|"/;

const assertValidJoinSeparator = (value: string): void => {
  if (!VALID_JOIN_SEPARATOR.test(value) || JOIN_SEPARATOR_DENYLIST.test(value)) {
    throw createClientValidationError(
      `Invalid SQL join separator: "${value}". ` +
        `Separators must match /^[\\s,()A-Za-z0-9_+\\-=<>!]+$/ and cannot contain ; -- /* */ \` ' "`,
    );
  }
};

const escapeIdentifier = (value: string) => {
  assertValidSqlIdentifier(value);
  return `\`${value.replaceAll("`", "``")}\``;
};

const renderIdentifier = (value: IdentifierValue) => {
  if (typeof value === "string") {
    return escapeIdentifier(value);
  }

  const parts: string[] = [];
  if (value.table) {
    parts.push(escapeIdentifier(value.table));
  }
  if (value.column) {
    parts.push(escapeIdentifier(value.column));
  }
  const rendered = parts.join(".");

  if (value.as) {
    if (!rendered) {
      return escapeIdentifier(value.as);
    }
    return `${rendered} as ${escapeIdentifier(value.as)}`;
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
  if (value === null || value === undefined) {
    throw createClientValidationError(
      "Raw SQL parameters do not support null or undefined. Use csql`NULL` or builder expressions instead.",
    );
  }
  ctx.nextParamIndex += 1;
  const paramName = `orm_param${ctx.nextParamIndex}`;
  ctx.params[paramName] = value;
  return `{${paramName}:${sqlType ?? inferPrimitiveType(value)}}`;
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
      return typeof resolved === "string" ? resolved : (resolved as CompilableSqlFragment)[compileSqlSymbol](ctx);
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
  return typeof value === "object" && value !== null && "compile" in value && typeof value.compile === "function";
};

const isCompilableSource = (value: unknown): value is CompilableSource => {
  return (
    typeof value === "object" && value !== null && "compileSource" in value && typeof value.compileSource === "function"
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

  return sqlFactory;
};

export const sql = createSqlFactory();

export const compileSql = (statement: SQLFragment<unknown>, initialContext?: Partial<BuildContext>) => {
  if (!isCompilableSqlFragment(statement)) {
    throw createClientValidationError("Invalid SQL fragment: the provided fragment cannot be compiled");
  }
  const ctx: BuildContext = {
    params: { ...(initialContext?.params ?? {}) },
    nextParamIndex: initialContext?.nextParamIndex ?? 0,
  };
  const query = statement[compileSqlSymbol](ctx);
  return {
    query,
    params: ctx.params,
    nextParamIndex: ctx.nextParamIndex,
  };
};

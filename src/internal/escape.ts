/**
 * Escapes a string for embedding inside a single-quoted ClickHouse SQL
 * literal — `\` and `'` are doubled-back-slash / backslash-quote respectively.
 *
 * Only used by call sites that already control the surrounding quoting (DDL
 * builders, enum/decimal type literals, query-param wire format). For
 * end-user input always go through the parameter channel instead.
 */
export const escapeSqlSingleQuoted = (value: string): string => value.replaceAll("\\", "\\\\").replaceAll("'", "\\'");

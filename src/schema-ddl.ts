import type { AnyColumn, ColumnDdlConfig, DdlFragmentInput } from "./columns";
import { createClientValidationError } from "./errors";
import { assertValidSqlIdentifier } from "./internal/identifier";
import { normalizeSingleStatementSql } from "./runtime/sql-scan";
import { type AnyTable, type ClickHouseTableEngine, mergeTreeTableEngines, type TableOptions } from "./schema";
import { compileSql, type SQLFragment, sql } from "./sql";

export type CreateTemporaryTableMode = "create" | "if_not_exists" | "or_replace";

type TableExpressionInput = AnyColumn | DdlFragmentInput;

const mergeTreeEngineNames = new Set<string>(mergeTreeTableEngines);
const tempEngineDenyPrefixes = ["Replicated"];
const tempEngineDenySet = new Set(["KeeperMap"]);

const escapeSqlSingleQuoted = (value: string) => value.replaceAll("\\", "\\\\").replaceAll("'", "\\'");

const renderStringLiteral = (value: string) => `'${escapeSqlSingleQuoted(value)}'`;

const renderColumnName = (column: AnyColumn) => {
  if (!column.name) {
    throw createClientValidationError(`Expected bound column name for ${column.sqlType}`);
  }
  return compileSql(sql.identifier(column.name)).query;
};

const compileDdlFragment = (value: DdlFragmentInput | SQLFragment<unknown>, label: string) => {
  if (typeof value === "string") {
    return normalizeSingleStatementSql(value, `${label} must not contain multiple statements`).trim();
  }

  const compiled = compileSql(value);
  if (Object.keys(compiled.params).length > 0) {
    throw createClientValidationError(
      `${label} must not use SQL parameters; use literal SQL fragments for schema DDL metadata`,
    );
  }
  return normalizeSingleStatementSql(compiled.query, `${label} must not contain multiple statements`).trim();
};

const renderColumnDdl = (ddl: ColumnDdlConfig | undefined): string[] => {
  if (!ddl) {
    return [];
  }

  const parts: string[] = [];
  if (ddl.default !== undefined) {
    parts.push(`DEFAULT ${compileDdlFragment(ddl.default, "column DEFAULT expression")}`);
  }
  if (ddl.materialized !== undefined) {
    parts.push(`MATERIALIZED ${compileDdlFragment(ddl.materialized, "column MATERIALIZED expression")}`);
  }
  if (ddl.aliasExpr !== undefined) {
    parts.push(`ALIAS ${compileDdlFragment(ddl.aliasExpr, "column ALIAS expression")}`);
  }
  if (ddl.comment !== undefined) {
    parts.push(`COMMENT ${renderStringLiteral(ddl.comment)}`);
  }
  if (ddl.codec !== undefined) {
    parts.push(`CODEC(${compileDdlFragment(ddl.codec, "column CODEC clause")})`);
  }
  if (ddl.ttl !== undefined) {
    parts.push(`TTL ${compileDdlFragment(ddl.ttl, "column TTL clause")}`);
  }
  return parts;
};

const renderColumnDefinition = (column: AnyColumn) => {
  return [renderColumnName(column), column.sqlType, ...renderColumnDdl(column.ddl)].join(" ");
};

const isColumnLike = (value: unknown): value is AnyColumn => {
  return typeof value === "object" && value !== null && "kind" in value && value.kind === "column";
};

const renderTableExpression = (value: TableExpressionInput, label: string) => {
  return isColumnLike(value) ? renderColumnName(value) : compileDdlFragment(value, label);
};

const renderExpressionList = (
  keyword: string,
  value: TableOptions["partitionBy"] | TableOptions["primaryKey"] | TableOptions["sampleBy"] | TableOptions["orderBy"],
  label: string,
) => {
  if (value === undefined) {
    return undefined;
  }

  if (Array.isArray(value)) {
    const rendered = value.map((entry) => renderTableExpression(entry, label));
    return `${keyword} (${rendered.join(", ")})`;
  }

  return `${keyword} ${renderTableExpression(value as TableExpressionInput, label)}`;
};

const renderTableTtl = (value: TableOptions["ttl"]) => {
  if (value === undefined) {
    return undefined;
  }

  if (Array.isArray(value)) {
    return `TTL ${value.map((entry) => compileDdlFragment(entry, "table TTL clause")).join(", ")}`;
  }

  return `TTL ${compileDdlFragment(value as DdlFragmentInput, "table TTL clause")}`;
};

const renderSettingsValue = (value: string | number | boolean | SQLFragment<unknown>) => {
  if (typeof value === "string") {
    return renderStringLiteral(value);
  }
  if (typeof value === "number") {
    return String(value);
  }
  if (typeof value === "boolean") {
    return value ? "1" : "0";
  }
  return compileDdlFragment(value, "table SETTINGS value");
};

const renderSettings = (settings: TableOptions["settings"]) => {
  if (!settings || Object.keys(settings).length === 0) {
    return undefined;
  }

  const entries = Object.entries(settings).map(([key, value]) => {
    assertValidSqlIdentifier(key);
    return `${key} = ${renderSettingsValue(value)}`;
  });
  return `SETTINGS ${entries.join(", ")}`;
};

const extractEngineName = (value: string) => {
  const match = /^\s*([A-Za-z_][A-Za-z0-9_]*)/.exec(value);
  return match?.[1];
};

const resolveEngineClause = (
  engine: ClickHouseTableEngine | SQLFragment<unknown> | undefined,
  options: TableOptions,
  temporary: boolean,
) => {
  if (engine === undefined) {
    return {
      text: temporary ? "Memory" : "MergeTree",
      engineName: temporary ? "Memory" : "MergeTree",
    };
  }

  if (typeof engine === "string") {
    if (engine === "ReplacingMergeTree" && options.versionColumn?.name) {
      return {
        text: `ReplacingMergeTree(${renderColumnName(options.versionColumn)})`,
        engineName: engine,
      };
    }

    return {
      text: engine,
      engineName: engine,
    };
  }

  if (options.versionColumn) {
    throw createClientValidationError(
      "versionColumn only supports string engine names; inline the full engine expression when using custom engine SQL",
    );
  }

  const text = compileDdlFragment(engine, temporary ? "temporary table engine" : "table engine");
  return {
    text,
    engineName: extractEngineName(text),
  };
};

const requiresOrderBy = (engineName: string | undefined) => {
  if (!engineName) {
    return false;
  }
  return mergeTreeEngineNames.has(engineName);
};

const validateCreateTableTarget = (table: AnyTable) => {
  if (table.alias) {
    throw createClientValidationError("Schema DDL requires a base table, not an aliased table");
  }
  compileSql(sql.identifier({ table: table.originalName })).query;
};

const validateTemporaryTable = (table: AnyTable, engineName: string | undefined) => {
  validateCreateTableTarget(table);
  if (
    engineName &&
    (tempEngineDenySet.has(engineName) || tempEngineDenyPrefixes.some((prefix) => engineName.startsWith(prefix)))
  ) {
    throw createClientValidationError(
      `Temporary tables do not support engine ${engineName}; use a non-replicated engine other than KeeperMap`,
    );
  }
};

const renderCreateTableClauses = (
  table: AnyTable,
  engineText: string,
  engineName: string | undefined,
  temporary: boolean,
) => {
  const clauses: string[] = [`ENGINE = ${engineText}`];

  if (table.options.comment) {
    clauses.push(`COMMENT ${renderStringLiteral(table.options.comment)}`);
  }

  const partitionBy = renderExpressionList("PARTITION BY", table.options.partitionBy, "PARTITION BY clause");
  if (partitionBy) {
    clauses.push(partitionBy);
  }

  const primaryKey = renderExpressionList("PRIMARY KEY", table.options.primaryKey, "PRIMARY KEY clause");
  if (primaryKey) {
    clauses.push(primaryKey);
  }

  const orderBy = renderExpressionList("ORDER BY", table.options.orderBy, "ORDER BY clause");
  if (orderBy) {
    clauses.push(orderBy);
  } else if (requiresOrderBy(engineName)) {
    clauses.push("ORDER BY tuple()");
  } else if (!temporary && engineName === undefined) {
    clauses.push("ORDER BY tuple()");
  }

  const sampleBy = renderExpressionList("SAMPLE BY", table.options.sampleBy, "SAMPLE BY clause");
  if (sampleBy) {
    clauses.push(sampleBy);
  }

  const ttl = renderTableTtl(table.options.ttl);
  if (ttl) {
    clauses.push(ttl);
  }

  const settings = renderSettings(table.options.settings);
  if (settings) {
    clauses.push(settings);
  }

  return clauses.join("\n");
};

const renderColumns = (table: AnyTable) => {
  return Object.values(table.columns)
    .map((column) => `  ${renderColumnDefinition(column as AnyColumn)}`)
    .join(",\n");
};

export const buildCreateTableStatement = (table: AnyTable) => {
  validateCreateTableTarget(table);
  const { text: engineText, engineName } = resolveEngineClause(table.options.engine, table.options, false);

  return `
CREATE TABLE ${compileSql(sql.identifier({ table: table.originalName })).query}
(
${renderColumns(table)}
)
${renderCreateTableClauses(table, engineText, engineName, false)}
  `.trim();
};

export const buildCreateTemporaryTableStatement = (table: AnyTable, mode: CreateTemporaryTableMode = "create") => {
  const { text: engineText, engineName } = resolveEngineClause(table.options.engine, table.options, true);
  validateTemporaryTable(table, engineName);

  const modePrefix = mode === "or_replace" ? "OR REPLACE " : "";
  const existenceClause = mode === "if_not_exists" ? "IF NOT EXISTS " : "";

  return `
CREATE ${modePrefix}TEMPORARY TABLE ${existenceClause}${compileSql(sql.identifier({ table: table.originalName })).query}
(
${renderColumns(table)}
)
${renderCreateTableClauses(table, engineText, engineName, true)}
  `.trim();
};

export const buildDropTableStatement = (tableName: string) => {
  return `DROP TABLE IF EXISTS ${compileSql(sql.identifier({ table: tableName })).query}`;
};

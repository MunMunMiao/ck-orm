import { createClientValidationError } from "../errors";

/**
 * Roles a SQL identifier may play. Used in error messages so callers can tell
 * `Invalid SQL identifier:` apart from `Invalid aggregate function name:` etc.
 */
export type SqlIdentifierRole =
  | "identifier"
  | "nested column"
  | "function"
  | "aggregate function"
  | "simple aggregate function";

const VALID_SQL_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

const ROLE_MESSAGES: Record<SqlIdentifierRole, string> = {
  identifier: "Invalid SQL identifier",
  "nested column": "Invalid nested column name",
  function: "Invalid function name",
  "aggregate function": "Invalid aggregate function name",
  "simple aggregate function": "Invalid simple aggregate function name",
};

/**
 * Validates that `value` is a safe SQL identifier (ASCII letters, digits, and
 * underscores; cannot start with a digit). Throws a `client_validation`
 * `ClickHouseORMError` when invalid.
 *
 * Centralises five duplicated regex checks across `sql.ts`, `columns.ts`, and
 * `functions.ts` so the rule and error message stay in lockstep.
 */
export const assertValidSqlIdentifier = (value: string, role: SqlIdentifierRole = "identifier"): void => {
  if (!VALID_SQL_IDENTIFIER.test(value)) {
    throw createClientValidationError(`${ROLE_MESSAGES[role]}: ${value}`);
  }
};

import { createClientValidationError } from "../errors";

type SqlScanState =
  | "code"
  | "single_quote"
  | "double_quote"
  | "backtick"
  | "line_comment"
  | "block_comment"
  | "heredoc";

const heredocOpeningPattern = /^\$[A-Za-z_][A-Za-z0-9_]*\$/;

const scanTopLevelSemicolons = (statement: string) => {
  const positions: number[] = [];
  let sawCodeAfterSemicolon = false;
  let state: SqlScanState = "code";
  let heredocTerminator = "";

  for (let index = 0; index < statement.length; index += 1) {
    const char = statement[index];
    const nextChar = statement[index + 1];

    switch (state) {
      case "code":
        if (char === "-" && nextChar === "-") {
          state = "line_comment";
          index += 1;
          continue;
        }
        if (char === "#") {
          state = "line_comment";
          continue;
        }
        if (char === "/" && nextChar === "*") {
          state = "block_comment";
          index += 1;
          continue;
        }
        if (char === "$") {
          const heredocOpening = heredocOpeningPattern.exec(statement.slice(index))?.[0];
          if (heredocOpening) {
            if (positions.length > 0) {
              sawCodeAfterSemicolon = true;
            }
            heredocTerminator = heredocOpening;
            state = "heredoc";
            index += heredocOpening.length - 1;
            continue;
          }
        }
        if (char === ";") {
          positions.push(index);
          continue;
        }
        if (char === "'") {
          if (positions.length > 0) {
            sawCodeAfterSemicolon = true;
          }
          state = "single_quote";
          continue;
        }
        if (char === '"') {
          if (positions.length > 0) {
            sawCodeAfterSemicolon = true;
          }
          state = "double_quote";
          continue;
        }
        if (char === "`") {
          if (positions.length > 0) {
            sawCodeAfterSemicolon = true;
          }
          state = "backtick";
          continue;
        }
        if (!/\s/.test(char) && positions.length > 0) {
          sawCodeAfterSemicolon = true;
        }
        continue;
      case "single_quote":
        if (char === "\\") {
          index += 1;
          continue;
        }
        if (char === "'" && nextChar === "'") {
          index += 1;
          continue;
        }
        if (char === "'") {
          state = "code";
        }
        continue;
      case "double_quote":
        if (char === "\\") {
          index += 1;
          continue;
        }
        if (char === '"' && nextChar === '"') {
          index += 1;
          continue;
        }
        if (char === '"') {
          state = "code";
        }
        continue;
      case "backtick":
        if (char === "`" && nextChar === "`") {
          index += 1;
          continue;
        }
        if (char === "`") {
          state = "code";
        }
        continue;
      case "line_comment":
        if (char === "\n" || char === "\r") {
          state = "code";
        }
        continue;
      case "block_comment":
        if (char === "*" && nextChar === "/") {
          state = "code";
          index += 1;
        }
        continue;
      case "heredoc":
        if (heredocTerminator && statement.startsWith(heredocTerminator, index)) {
          state = "code";
          index += heredocTerminator.length - 1;
          heredocTerminator = "";
        }
        continue;
    }
  }

  // If we exit the loop in a non-code, non-line-comment state, the input has
  // an unterminated literal / block comment / heredoc. Line comments are
  // allowed to run to EOF (their terminator is `\n`, which is implicit at
  // end-of-input). Letting unterminated literals through would smuggle a
  // semicolon inside a never-closing string past the multi-statement scan
  // and turn the wire query into something other than what the user wrote.
  if (state === "single_quote" || state === "double_quote" || state === "backtick") {
    throw createClientValidationError("Unterminated string literal in SQL statement");
  }
  if (state === "block_comment") {
    throw createClientValidationError("Unterminated block comment in SQL statement");
  }
  if (state === "heredoc") {
    throw createClientValidationError(`Unterminated heredoc literal ${heredocTerminator} in SQL statement`);
  }

  return {
    positions,
    sawCodeAfterSemicolon,
  };
};

const removeCharactersAtPositions = (input: string, positions: readonly number[]) => {
  if (positions.length === 0) {
    return input;
  }

  // `positions` is already in ascending order (produced left-to-right by the
  // top-level scanner), so this is a single-pass slice/join — no Set lookup,
  // no `next += ch` cons-string degradation on long SQL.
  const parts: string[] = [];
  let cursor = 0;
  for (const position of positions) {
    parts.push(input.slice(cursor, position));
    cursor = position + 1;
  }
  parts.push(input.slice(cursor));
  return parts.join("");
};

export const normalizeSingleStatementSql = (statement: string, inlineSemicolonMessage: string) => {
  let normalized = statement.trim();
  const { positions, sawCodeAfterSemicolon } = scanTopLevelSemicolons(normalized);
  if (sawCodeAfterSemicolon) {
    throw createClientValidationError(inlineSemicolonMessage);
  }
  normalized = removeCharactersAtPositions(normalized, positions).trim();
  return normalized;
};

export const normalizeQuery = (query: string, format?: string) => {
  const normalized = normalizeSingleStatementSql(
    query,
    "Query contains multiple statements; only a single statement is allowed per request",
  );
  if (!format) {
    return normalized;
  }
  return `${normalized}\nFORMAT ${format}`;
};

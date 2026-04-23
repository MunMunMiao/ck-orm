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

  return {
    positions,
    sawCodeAfterSemicolon,
  };
};

const removeCharactersAtPositions = (input: string, positions: readonly number[]) => {
  if (positions.length === 0) {
    return input;
  }

  const positionSet = new Set(positions);
  let next = "";
  for (let index = 0; index < input.length; index += 1) {
    if (positionSet.has(index)) {
      continue;
    }
    next += input[index];
  }
  return next;
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

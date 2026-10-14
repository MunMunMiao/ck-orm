// Lays out SQL for reading. It only changes whitespace between tokens: quoted text is never touched, and anything the
// scanner cannot lay out with certainty (comments, heredocs, unbalanced brackets) comes back unchanged.
//
// The compiler emits a statement on one line, so the layout works on that:
// - each clause (select, from, join, where, ...) starts a line, and select/with items go one per line;
// - a bracket group that holds a query (a subquery, a CTE body) is indented one level, with its closing bracket on its own line;
// - a bracket group of conditions that has and/or in it (the shape of `and(...)` and `or(...)`) puts each condition on its own line;
// - everything inside other brackets, such as function arguments, stays on its line.

const INDENT = "  ";

// A quoted string or identifier, a run of whitespace, a word or number, or any other single character.
const TOKEN = /\s+|'(?:[^'\\]|\\[\s\S]|'')*'|"(?:[^"\\]|\\[\s\S]|"")*"|`(?:[^`]|``)*`|\w+|[\s\S]/gy;
const UNSCANNABLE = new Set(["'", '"', "`", "#", "$"]);
const CLOSER = new Map([
  ["(", ")"],
  ["[", "]"],
  ["{", "}"],
]);

const CLAUSES = new Set(["select", "from", "where", "having", "limit", "offset"]);
// Clauses that are two words, by their first word.
const SECOND_WORD = new Map([
  ["group", "by"],
  ["order", "by"],
  ["inner", "join"],
  ["left", "join"],
]);

const isSpace = (token: string) => /^\s/.test(token);

const tokenize = (text: string): string[] | undefined => {
  const tokens = text.match(TOKEN) ?? [];
  // Comments and heredocs would change meaning once line breaks move; an unterminated quote is not valid SQL.
  const unscannable = tokens.some(
    (token, index) =>
      UNSCANNABLE.has(token) ||
      (token === "-" && tokens[index + 1] === "-") ||
      (token === "/" && tokens[index + 1] === "*"),
  );
  return unscannable ? undefined : tokens;
};

type Frame = {
  /** Closing bracket this frame waits for; empty for the statement itself. */
  readonly closer: string;
  /** Indent level of the clauses of a query frame; `undefined` for a plain bracket group. */
  readonly indent?: number;
  /** Whether commas split items (after select or with). */
  items: boolean;
  /** Whether a token was already written in this frame. */
  started: boolean;
  /** Indent level of the conditions of a condition group; `undefined` for other frames. */
  readonly conditions?: number;
  /** `between` words in a condition group still waiting for their `and`. */
  between: number;
};

// A bracket group of conditions: not a function call or a subscript (no word or closing bracket right before it),
// and with an `and` or `or` of its own (not the `and` of a `between`) before its closing bracket.
const holdsConditions = (tokens: readonly string[], open: number): boolean => {
  const previous = tokens[open - 1];
  if (previous !== undefined && !isSpace(previous) && /^[\w)\]}'"`]/.test(previous)) return false;
  let depth = 0;
  let between = 0;
  for (let index = open; index < tokens.length; index += 1) {
    const token = tokens[index] as string;
    if (CLOSER.has(token)) depth += 1;
    else if (token === ")" || token === "]" || token === "}") depth -= 1;
    if (depth === 0) return false;
    const word = token.toLowerCase();
    if (depth !== 1) continue;
    if (word === "between") between += 1;
    else if (word === "and" && between > 0) between -= 1;
    else if (word === "and" || word === "or") return true;
  }
  return false;
};

export const prettyStatement = (statement: string): string => {
  const tokens = tokenize(statement);
  if (!tokens) return statement;

  const frames: Frame[] = [{ closer: "", indent: 0, items: false, started: false, between: 0 }];
  let out = "";
  let lineIndent = 0;
  let skipSpace = true;
  const write = (text: string) => {
    out += text;
    skipSpace = false;
  };
  const breakLine = (level: number) => {
    out = `${out.trimEnd()}\n${INDENT.repeat(level)}`;
    lineIndent = level;
    skipSpace = true;
  };
  const nextToken = (from: number) => {
    let index = from;
    while (tokens[index] !== undefined && isSpace(tokens[index] as string)) index += 1;
    return index;
  };

  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index] as string;
    const frame = frames[frames.length - 1] as Frame;
    if (isSpace(token)) {
      if (!skipSpace) write(token);
      continue;
    }
    const word = token.toLowerCase();

    if (frame.indent !== undefined) {
      const secondIndex = nextToken(index + 1);
      let clause: string | undefined;
      let end = index;
      if (CLAUSES.has(word) || (word === "with" && !frame.started)) {
        clause = token;
      } else if (SECOND_WORD.has(word) && tokens[secondIndex]?.toLowerCase() === SECOND_WORD.get(word)) {
        clause = `${token} ${tokens[secondIndex]}`;
        end = secondIndex;
      }
      if (clause !== undefined) {
        if (frame.started) breakLine(frame.indent);
        write(clause);
        index = end;
        frame.started = true;
        frame.items = word === "select" || word === "with";
        if (frame.items) breakLine(frame.indent + 1);
        continue;
      }
      if (token === "," && frame.items) {
        write(",");
        breakLine(frame.indent + 1);
        continue;
      }
    }
    if (frame.conditions !== undefined) {
      if (word === "between") frame.between += 1;
      else if (word === "and" && frame.between > 0) frame.between -= 1;
      else if ((word === "and" || word === "or") && frame.started) breakLine(frame.conditions);
    }
    frame.started = true;

    if (CLOSER.has(token)) {
      const first = tokens[nextToken(index + 1)]?.toLowerCase();
      write(token);
      if (token === "(" && (first === "select" || first === "with")) {
        frames.push({ closer: ")", indent: lineIndent + 1, items: false, started: false, between: 0 });
        breakLine(lineIndent + 1);
      } else if (token === "(" && holdsConditions(tokens, index)) {
        frames.push({ closer: ")", conditions: lineIndent + 1, items: false, started: false, between: 0 });
        breakLine(lineIndent + 1);
      } else {
        frames.push({ closer: CLOSER.get(token) as string, items: false, started: false, between: 0 });
      }
      continue;
    }
    if (token === ")" || token === "]" || token === "}") {
      if (frames.length === 1 || frame.closer !== token) return statement;
      frames.pop();
      if (frame.indent !== undefined) breakLine(frame.indent - 1);
      if (frame.conditions !== undefined) breakLine(frame.conditions - 1);
      write(token);
      continue;
    }
    write(token);
  }

  return frames.length === 1 ? out.trimEnd() : statement;
};

/** Puts each row of an `insert ... format Values` body on its own line. */
export const prettyRows = (body: string): string => {
  const tokens = tokenize(body);
  if (!tokens) return body;

  let depth = 0;
  let out = "";
  let skipSpace = false;
  for (const token of tokens) {
    if (isSpace(token)) {
      if (!skipSpace) out += token;
      continue;
    }
    skipSpace = false;
    if (CLOSER.has(token)) depth += 1;
    if (token === ")" || token === "]" || token === "}") depth -= 1;
    if (token === "," && depth === 0) {
      out = `${out.trimEnd()},\n`;
      skipSpace = true;
      continue;
    }
    out += token;
  }
  return depth === 0 ? out.trimEnd() : body;
};

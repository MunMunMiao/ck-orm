import { createHighlighter, type ThemeRegistration } from "shiki";

type Palette = { fg: string; keyword: string; string: string; number: string; comment: string };

// Two small themes built from the page palette, so code reads the same as the rest of the design.
const theme = (name: string, type: "light" | "dark", color: Palette): ThemeRegistration => ({
  name,
  type,
  colors: { "editor.foreground": color.fg, "editor.background": "#00000000" },
  tokenColors: [
    { scope: ["comment", "punctuation.definition.comment"], settings: { foreground: color.comment } },
    { scope: ["string", "punctuation.definition.string"], settings: { foreground: color.string } },
    { scope: ["constant.numeric"], settings: { foreground: color.number } },
    {
      scope: [
        "keyword",
        "storage",
        "constant.language",
        "support.type.primitive",
        "keyword.operator.new",
        "keyword.operator.expression",
        "keyword.operator.logical.sql",
        "support.function",
      ],
      settings: { foreground: color.keyword },
    },
    {
      scope: ["keyword.operator", "punctuation", "meta.template.expression", "string.quoted.other.backtick"],
      settings: { foreground: color.fg },
    },
  ],
});

const lightTheme = theme("ck-light", "light", {
  fg: "#151a1f",
  keyword: "#2b5e8c",
  string: "#3c7a2e",
  number: "#a3501a",
  comment: "#636b72",
});

const darkTheme = theme("ck-dark", "dark", {
  fg: "#e7e9e4",
  keyword: "#8db7e0",
  string: "#a9d18e",
  number: "#f0a36e",
  comment: "#848d95",
});

// ck-orm entry points get the brand accent, so readers can spot the library API inside a snippet.
const API_NAMES = new Set(["ck", "fn", "ckType", "ckSql", "ckTable", "ckAlias", "clickhouseClient"]);

// ClickHouse typed placeholders such as {orm_param1:Int16}: values travel here, never as SQL text.
const PARAM_PLACEHOLDER = /\{\w+:[A-Z][^}]*\}/g;
const LANGUAGES = ["ts", "bash", "sql"] as const;
type Language = (typeof LANGUAGES)[number];
const isLanguage = (value: string): value is Language => (LANGUAGES as readonly string[]).includes(value);

export type Highlight = (code: string, language?: string) => string;

export const createCodeHighlighter = async (): Promise<Highlight> => {
  const highlighter = await createHighlighter({ themes: [lightTheme, darkTheme], langs: [...LANGUAGES] });
  const cache = new Map<string, string>();

  // Shiki merges same-colored tokens (`ck.eq(` is one span), so find API names in the per-scope
  // explanation instead and wrap them with a decoration. Strings and comments are skipped.
  const apiRanges = (code: string, lang: Language) => {
    const ranges: { start: number; end: number }[] = [];
    for (const line of highlighter.codeToTokensBase(code, { lang, theme: darkTheme, includeExplanation: true })) {
      for (const token of line) {
        let offset = token.offset;
        for (const part of token.explanation ?? []) {
          const literal = part.scopes.some(({ scopeName }) => /^(string|comment)/.test(scopeName));
          if (!literal && API_NAMES.has(part.content))
            ranges.push({ start: offset, end: offset + part.content.length });
          offset += part.content.length;
        }
      }
    }
    return ranges;
  };

  return (code, language = "") => {
    const key = `${language}\0${code}`;
    const cached = cache.get(key);
    if (cached !== undefined) return cached;
    const lang = isLanguage(language) ? language : "text";
    const params = [...code.matchAll(PARAM_PLACEHOLDER)].map((match) => ({
      start: match.index,
      end: match.index + match[0].length,
      properties: { class: "t-param" },
    }));
    const api = (lang === "ts" ? apiRanges(code, lang) : []).map((range) => ({
      ...range,
      properties: { class: "t-api" },
    }));
    const html = highlighter.codeToHtml(code, {
      lang,
      themes: { light: "ck-light", dark: "ck-dark" },
      defaultColor: false,
      decorations: [...params, ...api],
      // Lets the stylesheet treat languages differently, e.g. wrap SQL.
      transformers: [
        {
          pre(node) {
            node.properties["data-language"] = lang;
          },
        },
      ],
    });
    cache.set(key, html);
    return html;
  };
};

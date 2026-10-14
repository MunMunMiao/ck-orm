import esES from "./strings/es-es";
import frFR from "./strings/fr-fr";
import jaJP from "./strings/ja-jp";
import koKR from "./strings/ko-kr";
import ruRU from "./strings/ru-ru";
import zhHans from "./strings/zh-hans";
import zhHantHK from "./strings/zh-hant-hk";
import zhHantTW from "./strings/zh-hant-tw";

export const LOCALES = [
  { tag: "en-US", name: "English (US)" },
  { tag: "zh-Hans", name: "简体中文" },
  { tag: "zh-Hant-TW", name: "繁體中文（台灣）" },
  { tag: "zh-Hant-HK", name: "繁體中文（香港）" },
  { tag: "ja-JP", name: "日本語（日本）" },
  { tag: "ko-KR", name: "한국어(대한민국)" },
  { tag: "ru-RU", name: "Русский (Россия)" },
  { tag: "es-ES", name: "Español (España)" },
  { tag: "fr-FR", name: "Français (France)" },
] as const;

export type LocaleTag = (typeof LOCALES)[number]["tag"];
export type Locale = (typeof LOCALES)[number];
export const DEFAULT_TAG: "en-US" = "en-US";

// GitHub Pages is case-sensitive while macOS is not, so output and translation directories use lowercase tags.
export const localeDir = (tag: LocaleTag): string => tag.toLowerCase();

export type Strings = {
  site: {
    /** Site navigation landmark label; no placeholders. */
    nav: string;
    /** Header link to the examples pages; no placeholders. */
    examples: string;
    /** Language switcher label; no placeholders. */
    language: string;
    /** Mobile navigation close button label; no placeholders. */
    closeNav: string;
    /** Footer license text; no placeholders. */
    license: string;
    /** Footer link to edit the current content on GitHub; no placeholders. */
    editPage: string;
    /** Footer link to the English llms.txt for AI agents; no placeholders. */
    llmsFooter: string;
  };
  theme: {
    /** Theme button aria-label and title with {mode} and {next} theme names. */
    label: string;
    /** Theme option following the system; no placeholders. */
    auto: string;
    /** Light theme option; no placeholders. */
    light: string;
    /** Dark theme option; no placeholders. */
    dark: string;
  };
  code: {
    /** Copy button's initial label; no placeholders. */
    copy: string;
    /** Copy button label after success; no placeholders. */
    copied: string;
    /** Copy button label after failure; no placeholders. */
    copyFailed: string;
    /** Tablist label for labelled code groups; no placeholders. */
    variants: string;
    /** Heading anchor aria-label; no placeholders. */
    sectionLink: string;
  };
  docs: {
    /** Documentation page title; no placeholders. */
    title: string;
    /** Documentation page meta description; no placeholders. */
    description: string;
    /** Skip link to the rendered reference; no placeholders. */
    skip: string;
    /** Documentation table-of-contents label; no placeholders. */
    tocLabel: string;
    /** Mobile reference navigation button label; no placeholders. */
    openNav: string;
    /** Reference navigation dialog and nav labels; no placeholders. */
    navLabel: string;
    /** Pre-1.0 warning in the documentation hero; no placeholders. */
    badge: string;
    /** Documentation hero heading; no placeholders. */
    headline: string;
    /** Documentation hero introduction paragraph; no placeholders. */
    intro: string;
    /** Documentation hero link to installation; no placeholders. */
    ctaInstall: string;
    /** Documentation hero link to examples; no placeholders. */
    ctaExamples: string;
  };
  examples: {
    /** Examples page title; no placeholders. */
    title: string;
    /** Examples page meta description; no placeholders. */
    description: string;
    /** Skip link to the first example; no placeholders. */
    skip: string;
    /** Examples table-of-contents label; no placeholders. */
    tocLabel: string;
    /** Mobile examples navigation button label; no placeholders. */
    openNav: string;
    /** Examples navigation dialog and nav labels; no placeholders. */
    navLabel: string;
    /** Examples page heading; no placeholders. */
    heading: string;
    /** Examples introduction with {folder}, {main}, and {package} inline elements. */
    intro: string;
    /** Optional per-example explanation and run requirements, keyed by folder name. */
    details: Record<string, string>;
    /** Example tablist label with the example {title}. */
    tabsLabel: string;
  };
};

const EN: Strings = {
  site: {
    nav: "Site",
    examples: "Examples",
    language: "Language",
    closeNav: "Close navigation",
    license: "MIT License",
    editPage: "Edit this page on GitHub",
    llmsFooter: "llms.txt for AI agents",
  },
  theme: {
    label: "Theme: {mode}. Switch to {next}.",
    auto: "Auto",
    light: "Light",
    dark: "Dark",
  },
  code: {
    copy: "Copy",
    copied: "Copied",
    copyFailed: "Copy failed",
    variants: "Code variants",
    sectionLink: "Link to this section",
  },
  docs: {
    title: "ck-orm — type-safe ClickHouse for TypeScript",
    description:
      "ck-orm is a typed ClickHouse query layer for modern JavaScript runtimes: schema DSL, query builder, raw SQL templates, sessions and observability.",
    skip: "Skip to the reference",
    tocLabel: "Reference",
    openNav: "Open the reference navigation",
    navLabel: "Reference navigation",
    badge: "Before 0.1.0, releases can break the API. Pin an exact version.",
    headline: "Type-safe ClickHouse, without hiding ClickHouse",
    intro:
      "ck-orm is a schema DSL, typed query builder and SQL template layer for ClickHouse on modern JavaScript runtimes. Queries compile to readable ClickHouse SQL, values travel as typed parameters, and 64-bit integers and Decimals decode as exact strings.",
    ctaInstall: "Start with installation",
    ctaExamples: "See the examples",
  },
  examples: {
    title: "Examples — ck-orm",
    description: "Runnable ck-orm examples with the exact SQL each one sends to ClickHouse.",
    skip: "Skip to the examples",
    tocLabel: "Examples",
    openNav: "Open the examples navigation",
    navLabel: "Examples navigation",
    heading: "Examples",
    intro:
      "Each example is a runnable project in {folder}: a {main} and a {package}. This page shows that same file. The build runs it through ck-orm, so the SQL tab lists every request ClickHouse receives. The comment at the end of each file shows the rows it prints.",
    details: {
      "website-analytics":
        "Counts pageviews and exact distinct visitors by path for one site in a UTC [start, end) interval. Uses session temporary tables; visitor counts can overlap across paths.",
      "usage-metering":
        "Aggregates usage by tenant, user and UTC minute across insert batches. Run serially in a disposable database with CREATE, SELECT, INSERT and DROP permissions. Uses fixed-name persistent tables and a materialized view, and cleans up only objects it successfully created. An interrupted process or a lost CREATE response can leave objects for manual inspection.",
      "request-latency":
        "Aggregates successful request counts, weighted average latency and exact p95/p99 by tenant, operation and UTC minute. Run serially in a disposable database with CREATE, SELECT, INSERT and DROP permissions. Cleans up only its successfully created fixed-name tables and materialized view; an interrupted process or a lost CREATE response can leave objects. Exact quantile states retain all samples, so assess memory use for production workloads.",
      "asof-price":
        "ASOF LEFT JOIN matches each trade to the latest same-symbol quote at or before its time. Decimal prices remain strings and missing historical quotes return null. Uses session temporary tables.",
      "dynamic-signals":
        "Filters changing model signals in Map(String, Float64), checking that the key exists before applying an inclusive threshold. A stored zero remains a real score. Uses session temporary tables.",
    },
    tabsLabel: "{title}: source and SQL",
  },
};

export const STRINGS: Partial<Record<LocaleTag, Strings>> & { "en-US": Strings } = {
  "en-US": EN,
  "zh-Hans": zhHans,
  "zh-Hant-TW": zhHantTW,
  "zh-Hant-HK": zhHantHK,
  "ja-JP": jaJP,
  "ko-KR": koKR,
  "ru-RU": ruRU,
  "es-ES": esES,
  "fr-FR": frFR,
};

export const checkPlaceholders = (tag: LocaleTag, strings: Strings): void => {
  const names = (value: string) => [...new Set([...value.matchAll(/\{(\w+)\}/g)].map((match) => match[1]))].sort();
  const format = (values: string[]) => values.map((value) => `{${value}}`).join(", ");

  const check = (path: string, translated: unknown, english: unknown): void => {
    if (typeof english === "string") {
      const expected = names(english);
      if (typeof translated !== "string") throw new Error(`STRINGS[${JSON.stringify(tag)}].${path}: expected a string`);
      const actual = names(translated);
      if (actual.join("\0") !== expected.join("\0"))
        throw new Error(
          `STRINGS[${JSON.stringify(tag)}].${path}: placeholders ${format(actual)} differ from en-US ${format(expected)}`,
        );
      return;
    }
    for (const [key, value] of Object.entries(english ?? {}))
      check(path ? `${path}.${key}` : key, (translated as Record<string, unknown>)[key], value);
  };

  check("", strings, EN);
};

for (const [tag, strings] of Object.entries(STRINGS))
  if (tag !== DEFAULT_TAG) checkPlaceholders(tag as LocaleTag, strings);

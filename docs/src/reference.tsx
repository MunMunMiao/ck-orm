import { isValidElement, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import type { Highlight } from "./highlight";
import type { Strings } from "./i18n";

export type Heading = { level: 2 | 3; id: string; content: ReactNode; section: string };

export const textOf = (node: ReactNode): string => {
  if (node === null || node === undefined || typeof node === "boolean") return "";
  if (typeof node === "string" || typeof node === "number" || typeof node === "bigint") return String(node);
  if (Array.isArray(node)) return node.map(textOf).join("");
  if (isValidElement<{ children?: ReactNode }>(node)) return textOf(node.props.children);
  return "";
};

// GitHub's heading slug rules, so links such as #fncount--fnuniqexact-modes match on GitHub and on the site.
export const createSlugger = () => {
  const counts = new Map<string, number>();
  return (text: string) => {
    const base = text
      .trim()
      .toLowerCase()
      .replace(/[^\p{L}\p{N}\s_-]/gu, "")
      .replace(/\s/g, "-");
    const count = counts.get(base) ?? 0;
    counts.set(base, count + 1);
    return count ? `${base}-${count}` : base;
  };
};

export const Html = ({ html, className }: { html: string; className?: string }) => (
  // biome-ignore lint/security/noDangerouslySetInnerHtml: build-time HTML from Shiki and docs/reference.md.
  <div className={className} dangerouslySetInnerHTML={{ __html: html }} />
);

export const CodeBlock = ({ html, language, copy }: { html: string; language?: string; copy: string }) => (
  <div className="codeblock">
    <div className="codebar">
      <span>{language || "text"}</span>
      <button type="button" className="btn-copy" data-copy="">
        {copy}
      </button>
    </div>
    <Html html={html} />
  </div>
);

type CodeTab = { label: string; html: string };

// One code block with a tab per variant (e.g. one per package manager). Panels switch in client.ts.
export const CodeTabs = ({ id, label, tabs, copy }: { id: string; label: string; tabs: CodeTab[]; copy: string }) => (
  <div className="codeblock">
    <div className="codebar">
      <div role="tablist" aria-label={label} className="flex">
        {tabs.map((tab, index) => (
          <button
            key={tab.label}
            type="button"
            role="tab"
            className="seg"
            id={`${id}-tab-${index}`}
            aria-controls={`${id}-panel-${index}`}
            aria-selected={index === 0}
            tabIndex={index === 0 ? 0 : -1}
          >
            {tab.label}
          </button>
        ))}
      </div>
      <button type="button" className="btn-copy" data-copy="">
        {copy}
      </button>
    </div>
    {tabs.map((tab, index) => (
      <div
        key={tab.label}
        role="tabpanel"
        id={`${id}-panel-${index}`}
        aria-labelledby={`${id}-tab-${index}`}
        hidden={index !== 0}
      >
        <Html html={tab.html} />
      </div>
    ))}
  </div>
);

// VitePress-style code groups: consecutive fences labelled like ```bash [npm] become one tabbed block.
// Bun.markdown drops everything after the language, so the group is wrapped in a ````tabs fence first
// and split again when rendered. On GitHub the source still reads as plain consecutive code blocks.
const LABELLED_FENCE = "```\\w+ \\[[^\\]\\n]+\\]\\n[\\s\\S]*?\\n```";
const CODE_GROUP = new RegExp(`^${LABELLED_FENCE}(?:\\n+${LABELLED_FENCE})*$`, "gm");
const groupCodeTabs = (markdown: string) => markdown.replace(CODE_GROUP, (group) => `\`\`\`\`tabs\n${group}\n\`\`\`\``);
const parseCodeTabs = (source: string) =>
  [...source.matchAll(/```(\w+) \[([^\]\n]+)\]\n([\s\S]*?)\n```/g)].map(([, language, label, code]) => ({
    language,
    label,
    code,
  }));

export type Reference = {
  html: string;
  headings: Heading[];
  /** Every heading id (h2–h4) mapped to the id of the h2 section that contains it. */
  anchors: Map<string, string>;
  /** Every h2-h4 in document order. */
  outline: OutlineItem[];
  /** Every fenced code block in document order; a code group is one fence holding its raw source. */
  fences: Fence[];
  /** File label used in error messages. */
  source: string;
};

export type OutlineItem = { level: 2 | 3 | 4; id: string; text: string };
export type Fence = { language: string; code: string };

type RenderOptions = { linkBaseUrl: string; ui: Strings["code"]; source: string; canonical?: Reference };

const missingLinks = (markdown: string, anchors: Map<string, string>) => [
  ...new Set([...markdown.matchAll(/\]\(#([^)\s]+)\)/g)].map((match) => match[1]).filter((id) => !anchors.has(id))),
];

const quoteLine = (line: string) => JSON.stringify(line.length <= 80 ? line : `${line.slice(0, 77)}...`);

const validateTranslation = (reference: Reference, canonical: Reference, markdown: string): string[] => {
  const problems: string[] = [];
  if (reference.outline.length !== canonical.outline.length)
    problems.push(
      `heading count differs: expected ${canonical.outline.length} headings, found ${reference.outline.length}`,
    );

  let levelProblems = 0;
  for (
    let index = 0;
    index < Math.min(reference.outline.length, canonical.outline.length) && levelProblems < 5;
    index++
  ) {
    const expected = canonical.outline[index];
    const found = reference.outline[index];
    if (expected.level === found.level) continue;
    problems.push(
      `heading #${index + 1}: expected "${"#".repeat(expected.level)} ${expected.text}" (#${expected.id}), found "${"#".repeat(found.level)} ${found.text}"`,
    );
    levelProblems++;
  }

  if (reference.fences.length !== canonical.fences.length)
    problems.push(`code block count differs: expected ${canonical.fences.length}, found ${reference.fences.length}`);

  let codeProblems = 0;
  for (let index = 0; index < Math.min(reference.fences.length, canonical.fences.length) && codeProblems < 5; index++) {
    const expected = canonical.fences[index];
    const found = reference.fences[index];
    const label = `code block #${index + 1} (${expected.language || "text"})`;
    if (expected.language !== found.language) {
      problems.push(
        `${label}: language differs from the English one: expected ${JSON.stringify(expected.language)}, found ${JSON.stringify(found.language)}`,
      );
      codeProblems++;
      continue;
    }
    if (expected.code === found.code) continue;

    const expectedLines = expected.code.split("\n");
    const foundLines = found.code.split("\n");
    const firstDifference = expectedLines.findIndex((line, lineIndex) => line !== foundLines[lineIndex]);
    const line = firstDifference >= 0 ? firstDifference + 1 : Math.min(expectedLines.length, foundLines.length) + 1;
    problems.push(
      `${label}: content differs from the English one at line ${line}: expected ${quoteLine(expectedLines[line - 1] ?? "")}, found ${quoteLine(foundLines[line - 1] ?? "")}`,
    );
    codeProblems++;
  }

  const broken = missingLinks(markdown, canonical.anchors);
  if (broken.length > 0) problems.push(`links to missing headings: ${broken.join(", ")}`);
  return problems;
};

export const renderReference = (markdown: string, highlight: Highlight, options: RenderOptions): Reference => {
  const { linkBaseUrl, ui, source, canonical } = options;
  const slug = createSlugger();
  const headings: Heading[] = [];
  const anchors = new Map<string, string>();
  const outline: OutlineItem[] = [];
  const fences: Fence[] = [];
  let section = "";
  let codeGroups = 0;
  let headingIndex = 0;

  const heading =
    (level: 2 | 3 | 4) =>
    ({ children }: { children?: ReactNode }) => {
      const text = textOf(children);
      const expected = canonical?.outline[headingIndex++];
      const id = expected?.id ?? slug(text);
      if (level === 2) section = id;
      anchors.set(id, section);
      if (level !== 4) headings.push({ level, id, content: children, section });
      outline.push({ level, id, text });
      const Tag = `h${level}` as const;
      return (
        <Tag id={id} data-section={section}>
          {children}
          <a className="anchor" href={`#${id}`} aria-label={ui.sectionLink}>
            #
          </a>
        </Tag>
      );
    };

  const html = renderToStaticMarkup(
    Bun.markdown.react(groupCodeTabs(markdown), {
      h2: heading(2),
      h3: heading(3),
      h4: heading(4),
      pre: ({ language, children }) => {
        const code = textOf(children).replace(/\n$/, "");
        fences.push({ language: language ?? "", code });
        if (language !== "tabs")
          return <CodeBlock language={language} html={highlight(code, language)} copy={ui.copy} />;
        const tabs = parseCodeTabs(code).map((tab) => ({ label: tab.label, html: highlight(tab.code, tab.language) }));
        return <CodeTabs id={`code-group-${++codeGroups}`} label={ui.variants} tabs={tabs} copy={ui.copy} />;
      },
      a: ({ href, children }) => (
        <a href={/^(?:[a-z]+:|#)/i.test(href) ? href : new URL(href, linkBaseUrl).href}>{children}</a>
      ),
      table: ({ children }) => (
        <div className="table-wrap">
          <table>{children}</table>
        </div>
      ),
    }),
  );

  const reference: Reference = { html, headings, anchors, outline, fences, source };
  const broken = missingLinks(markdown, canonical?.anchors ?? anchors);
  if (!canonical && broken.length > 0) throw new Error(`${source} links to missing headings: ${broken.join(", ")}`);

  if (canonical) {
    const problems = validateTranslation(reference, canonical, markdown);
    if (problems.length > 0) {
      const shown = problems.slice(0, 8).map((problem) => `  - ${problem}`);
      if (problems.length > shown.length) shown.push(`  - ... and ${problems.length - shown.length} more`);
      throw new Error(`${source} does not match ${canonical.source}:\n${shown.join("\n")}`);
    }
  }

  return reference;
};

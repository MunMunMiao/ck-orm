// llms.txt output in the shape community tools (vitepress-plugin-llms, Mintlify, starlight-llms-txt) generate:
// one markdown file per section, an index with a link and description per section, and one full-text file.
// Everything is derived from docs/reference.md, the same source the website renders, so they cannot drift apart.
import type { Example } from "./examples";
import { STRINGS } from "./i18n";
import { createSlugger } from "./reference";

type Section = { title: string; id: string; body: string; subheadings: string[] };

export type LlmsFile = { path: string; content: string };

export const splitSections = (markdown: string): Section[] => {
  const sections: Section[] = [];
  const slug = createSlugger();
  let inFence = false;
  for (const line of markdown.split("\n")) {
    if (/^(```|~~~)/.test(line)) inFence = !inFence;
    const section = sections.at(-1);
    if (!inFence && line.startsWith("## ")) {
      const title = line.slice(3).trim();
      sections.push({ title, id: slug(title.replaceAll("`", "")), body: "", subheadings: [] });
    } else if (section) {
      if (!inFence && line.startsWith("### ")) section.subheadings.push(line.slice(4).trim());
      section.body += `${line}\n`;
    }
  }
  for (const section of sections) section.body = section.body.trim();
  return sections;
};

// First sentence of the first prose paragraph, plus the subsection titles, like a page description.
// Sections without prose (e.g. Installation) fall back to the first line of their first code block.
const describe = ({ body, subheadings }: Section) => {
  const blocks = body.split(/\n{2,}/).map((block) => block.trim());
  const paragraph = blocks.find((block) => block && !/^(#|```|[-*] |\||>|\d+\.|<)/.test(block));
  const sentence = paragraph
    ?.replace(/\s+/g, " ")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .match(/^.*?[.!?:](?=\s|$)/)?.[0]
    .replace(/:$/, ".");
  const covers = subheadings.length > 0 ? `Covers ${subheadings.join(", ")}.` : "";
  const code = blocks.find((block) => block.startsWith("```"))?.split("\n")[1];
  const description = [sentence, covers].filter(Boolean).join(" ") || (code ? `\`${code}\`` : "");
  return description ? `: ${description}` : "";
};

export const buildLlmsFiles = (input: {
  /** docs/reference.md: H1 title, blockquote summary, intro, then one h2 per section. */
  markdown: string;
  version: string;
  siteUrl: string;
  /** Base URL that relative links in the reference resolve against. */
  linkBaseUrl: string;
  repoUrl: string;
  /** Heading id → id of its h2 section, from the rendered reference. */
  anchors: Map<string, string>;
  /** The examples page content, for examples/index.md. */
  examples: Example[];
}): LlmsFile[] => {
  const { markdown, version, siteUrl, linkBaseUrl, repoUrl, anchors, examples } = input;
  const start = markdown.search(/^## /m);
  const preamble = start >= 0 ? markdown.slice(0, start) : "";
  const sections = splitSections(start >= 0 ? markdown.slice(start) : markdown);
  const sectionUrl = (id: string) => `${siteUrl}/reference/${id}.md`;

  const title = preamble.match(/^# (.+)$/m)?.[1] ?? "ck-orm";
  const summary = preamble.match(/^> (.+)$/m)?.[1];
  const intro = preamble
    .replace(/^# .+$/m, "")
    .replace(/^> .+$/m, "")
    .trim();

  const absoluteLinks = (text: string, anchorUrl: (id: string) => string) =>
    text.replace(/\]\(([^)\s]+)\)/g, (match, href: string) => {
      if (href.startsWith("#")) return `](${anchorUrl(href.slice(1))})`;
      if (/^[a-z]+:/i.test(href)) return match;
      return `](${new URL(href, linkBaseUrl).href})`;
    });

  const header = [`# ${title}`, `> ${summary ?? title} This reference covers version ${version}.`];

  const mentalModel = sections.find((section) => section.id === "mental-model");
  const index = [
    ...header,
    absoluteLinks(intro, (id) => sectionUrl(anchors.get(id) ?? id)),
    mentalModel ? absoluteLinks(mentalModel.body, (id) => sectionUrl(anchors.get(id) ?? id)) : undefined,
    "## Docs",
    sections.map((section) => `- [${section.title}](${sectionUrl(section.id)})${describe(section)}`).join("\n"),
    "## Examples",
    `- [Examples](${siteUrl}/examples/index.md): Runnable queries with the exact SQL each one sends to ClickHouse: ${examples.map((example) => example.id).join(", ")}.`,
    "## Optional",
    [
      `- [Complete reference](${siteUrl}/llms-full.txt): Every section above in one file.`,
      `- [Type declarations](https://cdn.jsdelivr.net/npm/ck-orm@${version}/index.d.ts): Bundled \`.d.ts\` with the exact signature of every public export.`,
      `- [End-to-end tests](${repoUrl}/tree/main/e2e): Runnable scenarios against a real ClickHouse server.`,
    ].join("\n"),
  ].filter(Boolean);

  const full = [
    ...header,
    absoluteLinks(intro, (id) => `#${id}`),
    ...sections.map((section) => `## ${section.title}\n\n${absoluteLinks(section.body, (id) => `#${id}`)}`),
  ].filter(Boolean);
  const fullText = `${full.join("\n\n")}\n`;

  return [
    { path: "llms.txt", content: `${index.join("\n\n")}\n` },
    { path: "llms-full.txt", content: fullText },
    // llmstxt.org v2: the markdown twin of each page.
    { path: "index.md", content: fullText },
    { path: "examples/index.md", content: examplesMarkdown(examples, repoUrl) },
    ...sections.map((section) => ({
      path: `reference/${section.id}.md`,
      content: `# ${section.title}\n\n${absoluteLinks(section.body, (id) => sectionUrl(anchors.get(id) ?? id))}\n`,
    })),
  ];
};

const fence = (language: string, code: string) => `\`\`\`${language}\n${code}\n\`\`\``;

const examplesMarkdown = (examples: Example[], repoUrl: string) =>
  `${[
    "# ck-orm examples",
    `> Runnable ck-orm examples with the exact SQL each one sends to ClickHouse. It is generated by running every example when the docs are built. The comment at the end of each file shows the rows it prints.`,
    `Each example is a folder in [examples/](${repoUrl}/tree/main/examples) with a \`main.ts\` and a \`package.json\`. The SQL lists every request in order. A placeholder such as \`{orm_param1:String}\` is a typed parameter: ck-orm sends its value apart from the SQL, and the \`-- param\` lines under the statement show it.`,
    ...examples.flatMap((example) => [
      `## ${example.id}`,
      `Source: [${example.path}](${repoUrl}/blob/main/${example.path})`,
      STRINGS["en-US"].examples.details[example.id] ?? "",
      fence("bash", `CLICKHOUSE_URL=http://localhost:8123 bun ${example.path}`),
      fence("ts", example.source),
      "ClickHouse receives:",
      fence("sql", example.sql),
    ]),
  ].join("\n\n")}\n`;

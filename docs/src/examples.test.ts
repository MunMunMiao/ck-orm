import { describe, expect, test } from "bun:test";
import { existsSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import type { Example } from "./examples";
import type { Highlight } from "./highlight";
import { LOCALES, STRINGS } from "./i18n";
import { buildLlmsFiles } from "./llms";
import { renderExamplesPage } from "./page";

const examples: Example[] = ["decimal-sum", "literal-search", "usage-metering", "request-latency"].map((id) => ({
  id,
  path: `examples/${id}/main.ts`,
  source: `console.log("${id}");`,
  sql: "SELECT 1;",
}));
const highlight: Highlight = (code) => `<pre><code>${code}</code></pre>`;
const repoUrl = "https://example.com/repo";
const siteUrl = "https://example.com";

// renderToStaticMarkup escapes the same five characters as React's server renderer.
const escapeHtml = (value: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#x27;");

const section = (html: string, id: string) =>
  html.split(`<section aria-labelledby="${id}">`)[1]?.split("</section>")[0] ?? "";

const examplesMarkdown = () => {
  const file = buildLlmsFiles({
    markdown: "# Reference\n\n## Examples\n\nSee the examples page.\n",
    version: "0.0.0",
    siteUrl,
    linkBaseUrl: `${repoUrl}/blob/main/docs/`,
    repoUrl,
    anchors: new Map([["examples", "examples"]]),
    examples,
  }).find((file) => file.path === "examples/index.md");
  if (!file) throw new Error("Missing examples Markdown output");
  return file.content;
};

const markdownSection = (markdown: string, title: string) =>
  markdown.split(`## ${title}\n`)[1]?.split("\n## ")[0] ?? "";

const expectOrder = (html: string, fragments: string[]) => {
  let from = 0;
  for (const fragment of fragments) {
    const index = html.indexOf(fragment, from);
    expect(index).toBeGreaterThanOrEqual(from);
    from = index + fragment.length;
  }
};

describe("example instructions", () => {
  test("renders each run command and localized title, summary and notes inside its own section", () => {
    for (const locale of LOCALES) {
      const strings = STRINGS[locale.tag];
      if (!strings) throw new Error(`Missing locale strings: ${locale.tag}`);
      const html = renderExamplesPage({
        locale,
        locales: [...LOCALES],
        ui: strings,
        repoUrl,
        siteUrl,
        examples,
        highlight,
      });

      const introduction = html.split('<section aria-labelledby="')[0];
      expect(introduction).not.toContain("CLICKHOUSE_URL=");
      for (const example of examples) {
        const item = strings.examples.items[example.id];
        if (!item) throw new Error(`Missing examples.items[${JSON.stringify(example.id)}] for ${locale.tag}`);
        const body = section(html, example.id);
        const runCommand = `CLICKHOUSE_URL=http://localhost:8123 bun ${example.path}`;
        expect(body).toContain(runCommand);

        const heading = body.match(/<h2\b([^>]*)>([\s\S]*?)<\/h2>/);
        expect(heading?.[1]).toContain(`id="${example.id}"`);
        expect(heading?.[1]).toContain(`data-section="${example.id}"`);
        expect(heading?.[2]?.trim()).toBe(escapeHtml(item.title));
        expect(heading?.[2]?.trim()).not.toBe(example.id);

        const tocText = [
          ...html.matchAll(new RegExp(`<a href="#${example.id}" data-target="${example.id}">([\\s\\S]*?)</a>`, "g")),
        ].map((match) => match[1]?.trim());
        expect(tocText.length).toBeGreaterThan(0);
        for (const text of tocText) {
          expect(text).toBe(escapeHtml(item.title));
          expect(text).not.toBe(example.id);
        }

        const tabsLabel = strings.examples.tabsLabel.replace("{title}", () => item.title);
        expect(body).toContain(`aria-label="${escapeHtml(tabsLabel)}"`);
        if (item.title !== example.id) {
          const folderLabel = strings.examples.tabsLabel.replace("{title}", () => example.id);
          expect(body).not.toContain(`aria-label="${escapeHtml(folderLabel)}"`);
        }

        const headingEnd = body.indexOf("</h2>");
        expect(headingEnd).toBeGreaterThanOrEqual(0);
        expectOrder(body.slice(headingEnd), [
          escapeHtml(item.summary),
          ...(item.notes ? [escapeHtml(item.notes)] : []),
          example.path,
          runCommand,
        ]);
      }

      const usage = strings.examples.items["usage-metering"];
      const latency = strings.examples.items["request-latency"];
      if (!usage) throw new Error(`Missing examples.items["usage-metering"] for ${locale.tag}`);
      if (!latency) throw new Error(`Missing examples.items["request-latency"] for ${locale.tag}`);
      const usageSection = section(html, "usage-metering");
      const latencySection = section(html, "request-latency");
      const decimalSection = section(html, "decimal-sum");
      expect(usageSection).toContain(escapeHtml(usage.summary));
      if (usage.notes) expect(usageSection).toContain(escapeHtml(usage.notes));
      expect(usageSection).not.toContain(escapeHtml(latency.summary));
      if (latency.notes) expect(usageSection).not.toContain(escapeHtml(latency.notes));
      expect(latencySection).toContain(escapeHtml(latency.summary));
      if (latency.notes) expect(latencySection).toContain(escapeHtml(latency.notes));
      expect(latencySection).not.toContain(escapeHtml(usage.summary));
      if (usage.notes) expect(latencySection).not.toContain(escapeHtml(usage.notes));
      if (usage.notes) expect(decimalSection).not.toContain(escapeHtml(usage.notes));
      if (latency.notes) expect(decimalSection).not.toContain(escapeHtml(latency.notes));
      expect(decimalSection).not.toContain("permissions");
    }
  });

  test("places markdown run commands and persistent-table requirements in each example", () => {
    const markdown = examplesMarkdown();
    const items = STRINGS["en-US"].examples.items;
    const introduction = markdown.split("\n## ")[0];
    expect(introduction).not.toContain("CLICKHOUSE_URL=");
    expect(introduction).not.toContain("persistent tables");
    for (const example of examples) {
      const item = items[example.id];
      if (!item) throw new Error(`Missing English examples.items[${JSON.stringify(example.id)}]`);
      const body = markdownSection(markdown, item.title);
      expect(markdown).toContain(`## ${item.title}\n`);
      expect(markdown).not.toContain(`## ${example.id}\n`);
      expect(body).toContain(item.summary);
      if (item.notes) expect(body).toContain(item.notes);
      expect(body).toContain(`CLICKHOUSE_URL=http://localhost:8123 bun ${example.path}`);
    }
    const usage = items["usage-metering"];
    const latency = items["request-latency"];
    const decimal = items["decimal-sum"];
    if (!usage || !latency || !decimal) throw new Error("Missing English example items");
    expect(markdownSection(markdown, usage.title)).toContain("Run serially in a disposable database");
    expect(markdownSection(markdown, latency.title)).toContain("Run serially in a disposable database");
    expect(markdownSection(markdown, decimal.title)).not.toContain("persistent tables");
  });

  test("English example items match the examples folders", () => {
    const examplesRoot = fileURLToPath(new URL("../../examples/", import.meta.url));
    const folders = readdirSync(examplesRoot, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => entry.name)
      .filter((name) => {
        const main = join(examplesRoot, name, "main.ts");
        return existsSync(main) && statSync(main).isFile();
      })
      .sort();
    const keys = Object.keys(STRINGS["en-US"].examples.items).sort();
    expect(keys).toEqual(folders);
    for (const item of Object.values(STRINGS["en-US"].examples.items)) {
      expect(item.title.length).toBeGreaterThan(0);
      expect(item.summary.length).toBeGreaterThan(0);
    }
  });

  test("opens every absolute link on the page in a new tab", () => {
    const html = renderExamplesPage({
      locale: LOCALES[0],
      locales: [...LOCALES],
      ui: STRINGS["en-US"],
      repoUrl,
      siteUrl,
      examples,
      highlight,
    });
    const external = (html.match(/<a [^>]*>/g) ?? []).filter((tag) => /href="https?:\/\//.test(tag));
    expect(external.length).toBeGreaterThanOrEqual(4);
    for (const tag of external) {
      expect(tag).toContain('target="_blank"');
      expect(tag).toContain('rel="noopener"');
    }
  });
});

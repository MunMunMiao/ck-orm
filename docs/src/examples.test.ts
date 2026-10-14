import { describe, expect, test } from "bun:test";
import type { Example } from "./examples";
import type { Highlight } from "./highlight";
import { LOCALES, STRINGS } from "./i18n";
import { buildLlmsFiles } from "./llms";
import { renderExamplesPage } from "./page";

const examples: Example[] = ["decimal-sum", "usage-metering", "request-latency"].map((id) => ({
  id,
  path: `examples/${id}/main.ts`,
  source: `console.log("${id}");`,
  sql: "SELECT 1;",
}));
const highlight: Highlight = (code) => `<pre><code>${code}</code></pre>`;
const repoUrl = "https://example.com/repo";
const siteUrl = "https://example.com";

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

const markdownSection = (markdown: string, id: string) => markdown.split(`## ${id}\n`)[1]?.split("\n## ")[0] ?? "";

describe("example instructions", () => {
  test("renders each run command and localized instructions inside its own section", () => {
    for (const locale of LOCALES) {
      const strings = STRINGS[locale.tag];
      if (!strings) throw new Error(`Missing locale strings: ${locale.tag}`);
      const details = {
        "usage-metering": `${locale.tag}: usage permissions`,
        "request-latency": `${locale.tag}: latency permissions`,
      };
      const html = renderExamplesPage({
        locale,
        locales: [...LOCALES],
        ui: { ...strings, examples: Object.assign({}, strings.examples, { details }) },
        repoUrl,
        siteUrl,
        examples,
        highlight,
      });

      const introduction = html.split('<section aria-labelledby="')[0];
      expect(introduction).not.toContain("CLICKHOUSE_URL=");
      for (const example of examples) {
        expect(section(html, example.id)).toContain(`CLICKHOUSE_URL=http://localhost:8123 bun ${example.path}`);
      }
      expect(section(html, "usage-metering")).toContain(details["usage-metering"]);
      expect(section(html, "usage-metering")).not.toContain(details["request-latency"]);
      expect(section(html, "request-latency")).toContain(details["request-latency"]);
      expect(section(html, "decimal-sum")).not.toContain("permissions");
    }
  });

  test("places markdown run commands and persistent-table requirements in each example", () => {
    const markdown = examplesMarkdown();
    const introduction = markdown.split("\n## ")[0];
    expect(introduction).not.toContain("CLICKHOUSE_URL=");
    expect(introduction).not.toContain("persistent tables");
    for (const example of examples) {
      expect(markdownSection(markdown, example.id)).toContain(
        `CLICKHOUSE_URL=http://localhost:8123 bun ${example.path}`,
      );
    }
    expect(markdownSection(markdown, "usage-metering")).toContain("Run serially in a disposable database");
    expect(markdownSection(markdown, "request-latency")).toContain("Run serially in a disposable database");
    expect(markdownSection(markdown, "decimal-sum")).not.toContain("persistent tables");
  });
});

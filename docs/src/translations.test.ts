import { describe, expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Highlight } from "./highlight";
import type { Strings } from "./i18n";
import { renderReference, textOf } from "./reference";
import { bodyFromFirstSection, checkSections, findTranslations, sectionHash, stripSourceMarkers } from "./translations";

const highlight: Highlight = (code) => `<pre>${code}</pre>`;
const ui: Strings["code"] = {
  copy: "Copy",
  copied: "Copied",
  copyFailed: "Copy failed",
  variants: "Code variants",
  sectionLink: "Link to this section",
};
const render = (markdown: string, source = "translated-reference.md", canonical?: ReturnType<typeof renderReference>) =>
  renderReference(markdown, highlight, { linkBaseUrl: "https://example.com/docs/", ui, source, canonical });
const message = (run: () => unknown): string => {
  try {
    run();
  } catch (error) {
    return (error as Error).message;
  }
  throw new Error("expected rendering to fail");
};

describe("translation helpers", () => {
  test("strips source markers outside code fences only", () => {
    const markdown = [
      "## One",
      "<!-- src:1234abcd -->",
      "before",
      "```md",
      "<!-- src:deadbeef -->",
      "```",
      "after",
    ].join("\n");
    expect(stripSourceMarkers(markdown)).toBe(
      ["## One", "before", "```md", "<!-- src:deadbeef -->", "```", "after"].join("\n"),
    );
  });

  test("section hashes are stable across line endings", () => {
    expect(sectionHash("  alpha\r\nbeta  \n")).toBe(sectionHash("alpha\nbeta"));
    expect(sectionHash("alpha\nbeta")).toHaveLength(8);
  });

  test("reports current, stale, unmarked and missing sections", () => {
    const english = ["## One", "body", "", "## Two", "body", "", "## Three", "body", "", "## Four", "body"].join("\n");
    const translated = [
      "## One",
      `<!-- src:${sectionHash("body")} -->`,
      "body",
      "",
      "## Two",
      "body",
      "",
      "## Three",
      `<!-- src:${sectionHash("other")} -->`,
      "body",
    ].join("\n");
    expect(checkSections(english, translated)).toEqual([
      { title: "One", state: "current" },
      { title: "Two", state: "unmarked" },
      { title: "Three", state: "stale" },
      { title: "Four", state: "missing" },
    ]);
  });

  test("requires the input to begin at its first section", () => {
    expect(bodyFromFirstSection("## One\nbody", "translation.md")).toBe("## One\nbody");
    expect(message(() => bodyFromFirstSection("# Title\n\nintro\n\n## One", "translation.md"))).toContain(
      "content precedes",
    );
    expect(message(() => bodyFromFirstSection("# Title", "translation.md"))).toContain('no "##" heading found');
  });
});

describe("renderReference", () => {
  const english = ["## Alpha", "text", "", "### Beta", "child"].join("\n");
  const canonical = render(english, "docs/reference.md");

  test("uses canonical ids while preserving translated heading text", () => {
    const translated = render(["## Different", "text", "", "### Other", "child"].join("\n"), undefined, canonical);
    expect(translated.outline.map(({ id }) => id)).toEqual(["alpha", "beta"]);
    expect(textOf(translated.headings[0].content)).toBe("Different");
  });

  test("reports heading count mismatches", () => {
    expect(message(() => render("## Alpha\ntext", undefined, canonical))).toContain("expected 2 headings, found 1");
  });

  test("reports changed heading levels", () => {
    const translated = ["## Alpha", "text", "", "## Beta", "child"].join("\n");
    expect(message(() => render(translated, undefined, canonical))).toContain(
      'heading #2: expected "### Beta" (#beta), found "## Beta"',
    );
  });

  test("reports code content that differs by one byte", () => {
    const expected = ["## Alpha", "", "```ts", "const value = 1;", "```"].join("\n");
    const found = ["## Alpha", "", "```ts", "const value = 2;", "```"].join("\n");
    const error = message(() => render(found, undefined, render(expected, "docs/reference.md")));
    expect(error).toContain("code block #1 (ts): content differs from the English one at line 1");
    expect(error).toContain('expected "const value = 1;", found "const value = 2;"');
  });

  test("checks translated links against English anchors", () => {
    const error = message(() => render("## Alpha\n\ntext\n\n[bad](#missing)", undefined, canonical));
    expect(error).toContain("links to missing headings: missing");
  });
});

describe("findTranslations", () => {
  test("only reads known locale directories; STYLE.md and unknown directories are ignored", async () => {
    const root = await mkdtemp(join(tmpdir(), "ck-orm-i18n-"));
    const warn = spyOn(console, "warn").mockImplementation(() => {});
    try {
      await writeFile(join(root, "STYLE.md"), "# Style guide\n");
      await mkdir(join(root, "ja-jp"));
      await writeFile(join(root, "ja-jp", "reference.md"), "## One\nbody\n");
      await mkdir(join(root, "xx-xx"));
      const found = await findTranslations(root);
      expect(found.map(({ locale }) => locale.tag)).toEqual(["ja-JP"]);
      expect(warn.mock.calls.map(([message]) => String(message))).toEqual([
        "docs/i18n/xx-xx: not a known locale directory, skipped",
      ]);
    } finally {
      warn.mockRestore();
      await rm(root, { recursive: true, force: true });
    }
  });

  test("returns no translations when the directory does not exist", async () => {
    expect(await findTranslations(join(tmpdir(), "ck-orm-i18n-missing"))).toEqual([]);
  });
});

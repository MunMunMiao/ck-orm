import { rm } from "node:fs/promises";
import { join } from "node:path";
import tailwind from "bun-plugin-tailwind";
import { examples } from "./src/examples";
import { createCodeHighlighter } from "./src/highlight";
import { DEFAULT_TAG, LOCALES, type Locale, localeDir, STRINGS, type Strings } from "./src/i18n";
import { buildLlmsFiles } from "./src/llms";
import { renderDocsPage, renderExamplesPage } from "./src/page";
import { type Reference, renderReference } from "./src/reference";
import { bodyFromFirstSection, checkSections, findTranslations, stripSourceMarkers } from "./src/translations";

const DOCS = import.meta.dir;
const ROOT = join(DOCS, "..");
const DIST = join(DOCS, "dist");
const STAGE = join(DOCS, ".build");

const pkg: { version: string; homepage: string; repository: { url: string } } = await Bun.file(
  join(ROOT, "package.json"),
).json();
const markdown = await Bun.file(join(DOCS, "reference.md")).text();
const repoUrl = pkg.repository.url.replace(/^git\+/, "").replace(/\.git$/, "");
const siteUrl = pkg.homepage.replace(/\/$/, "");
// Relative links in reference.md are written relative to docs/, like any file viewed on GitHub.
const linkBaseUrl = `${repoUrl}/blob/main/docs/`;

// The page hero replaces the reference preamble (title, summary, intro); the body starts at the first h2.
const start = markdown.search(/^## /m);
if (start < 0) throw new Error("docs/reference.md must contain at least one ## section");
const englishBody = markdown.slice(start);

const highlight = await createCodeHighlighter();
const defaultLocale = LOCALES[0];
const defaultStrings = STRINGS[DEFAULT_TAG];
const englishReference = renderReference(englishBody, highlight, {
  linkBaseUrl,
  siteUrl,
  ui: defaultStrings.code,
  source: "docs/reference.md",
});

const built: { locale: Locale; strings: Strings; reference: Reference }[] = [
  { locale: defaultLocale, strings: defaultStrings, reference: englishReference },
];
const failures: string[] = [];
for (const translation of await findTranslations()) {
  const { locale, source, strings } = translation;
  if (!strings) {
    console.warn(`${source}: no UI strings for ${locale.tag} in docs/src/i18n.ts, skipped`);
    continue;
  }
  try {
    const body = bodyFromFirstSection(translation.markdown, source);
    const sections = checkSections(englishBody, body);
    const stale = sections.filter(({ state }) => state === "stale").length;
    const unmarked = sections.filter(({ state }) => state === "unmarked").length;
    const missing = sections.filter(({ state }) => state === "missing").length;
    if (stale + unmarked + missing > 0)
      console.warn(
        `${locale.tag}: ${stale} of ${sections.length} sections are stale, ${unmarked} unmarked, ${missing} missing; run "bun run i18n:status" for the list`,
      );
    built.push({
      locale,
      strings,
      reference: renderReference(stripSourceMarkers(body), highlight, {
        linkBaseUrl,
        siteUrl,
        ui: strings.code,
        source,
        canonical: englishReference,
      }),
    });
  } catch (error) {
    failures.push(error instanceof Error ? error.message : String(error));
  }
}
if (failures.length > 0) throw new Error(failures.join("\n\n"));

const locales = built.map(({ locale }) => locale);
const pages = new Map<string, string>();
for (const { locale, strings: ui, reference } of built) {
  const dir = locale.tag === DEFAULT_TAG ? "" : `${localeDir(locale.tag)}/`;
  const pageBase = { locale, locales, ui, repoUrl, siteUrl };
  pages.set(`${dir}index.html`, renderDocsPage({ ...pageBase, reference }));
  pages.set(`${dir}examples/index.html`, renderExamplesPage({ ...pageBase, examples, highlight }));
}

await rm(DIST, { recursive: true, force: true });
await rm(STAGE, { recursive: true, force: true });
// Each language gets standalone pages with styles and the client script inlined.
for (const [path, html] of pages) await Bun.write(join(STAGE, path), html);

const result = await Bun.build({
  entrypoints: [...pages.keys()].map((path) => join(STAGE, path)),
  outdir: DIST,
  // Explicit: with many nested entry points Bun 1.4 can infer a wrong root and write to dist/.build/.
  root: STAGE,
  compile: true,
  target: "browser",
  minify: true,
  plugins: [tailwind],
});
if (!result.success) throw new AggregateError(result.logs, "Docs bundle failed");

const llmsFiles = buildLlmsFiles({
  markdown,
  version: pkg.version,
  siteUrl,
  linkBaseUrl,
  repoUrl,
  anchors: englishReference.anchors,
  examples,
});
for (const file of llmsFiles) await Bun.write(join(DIST, file.path), file.content);

for (const output of result.outputs)
  console.log(`${output.path.replace(`${DOCS}/`, "")}  ${(output.size / 1024).toFixed(0)} KB`);
console.log(`languages: ${locales.map(({ tag }) => tag).join(", ")}; ${llmsFiles.length} llms files for ${siteUrl}`);

if (Bun.argv.includes("--serve")) {
  const server = Bun.serve({
    port: Number(process.env.PORT ?? 4173),
    async fetch(request) {
      const path = new URL(request.url).pathname;
      const file = Bun.file(join(DIST, path.endsWith("/") ? `${path}index.html` : path));
      // no-cache: the preview must never show a page from a previous build.
      return (await file.exists())
        ? new Response(file, { headers: { "cache-control": "no-cache" } })
        : new Response("Not found", { status: 404 });
    },
  });
  console.log(`Preview: ${server.url}`);
}

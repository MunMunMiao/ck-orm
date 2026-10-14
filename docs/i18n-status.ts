import { join } from "node:path";
import { DEFAULT_TAG, LOCALES } from "./src/i18n";
import { splitSections } from "./src/llms";
import {
  bodyFromFirstSection,
  checkSections,
  findTranslations,
  type SectionState,
  sectionHash,
} from "./src/translations";

const englishMarkdown = await Bun.file(join(import.meta.dir, "reference.md")).text();
const englishStart = englishMarkdown.search(/^## /m);
if (englishStart < 0) throw new Error('docs/reference.md: no "##" heading found');
const english = bodyFromFirstSection(englishMarkdown.slice(englishStart), "docs/reference.md");

if (process.argv.includes("--hashes")) {
  for (const section of splitSections(english))
    console.log(`<!-- src:${sectionHash(section.body)} -->  ## ${section.title}`);
  process.exit(0);
}

const translations = await findTranslations();
// With --strict the exit code is 1 when any translation is not fully current, so that CI can enforce it.
let outdated = false;
for (const translation of translations) {
  const translated = bodyFromFirstSection(translation.markdown, translation.source);
  const sections = checkSections(english, translated);
  const counts = Object.fromEntries(
    (["current", "stale", "unmarked", "missing"] as SectionState[]).map((state) => [
      state,
      sections.filter((section) => section.state === state).length,
    ]),
  ) as Record<SectionState, number>;

  console.log(translation.source);
  console.log(
    `${sections.length} sections: ${counts.current} current, ${counts.stale} stale, ${counts.unmarked} unmarked, ${counts.missing} missing`,
  );
  for (const section of sections)
    if (section.state !== "current") console.log(`  ${section.state.padEnd(10)}## ${section.title}`);
  if (!translation.strings) console.log("  no UI strings in src/i18n.ts; skipped by the build");
  const surplus = Math.max(0, splitSections(translated).length - sections.length);
  console.log(`  surplus sections: ${surplus}`);
  if (counts.current !== sections.length || surplus > 0) outdated = true;
}

const translatedTags = new Set(translations.map(({ locale }) => locale.tag));
const unavailable = LOCALES.filter(({ tag }) => tag !== DEFAULT_TAG && !translatedTags.has(tag));
if (unavailable.length > 0) console.log(`no translation yet: ${unavailable.map(({ tag }) => tag).join(", ")}`);

if (process.argv.includes("--strict") && (outdated || unavailable.length > 0)) {
  console.error("i18n:status --strict: some translations are not fully current");
  process.exit(1);
}

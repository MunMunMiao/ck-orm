import { createHash } from "node:crypto";
import type { Dirent } from "node:fs";
import { readdir } from "node:fs/promises";
import { join } from "node:path";
import { DEFAULT_TAG, LOCALES, type Locale, localeDir, STRINGS, type Strings } from "./i18n";
import { splitSections } from "./llms";

export type Translation = {
  locale: Locale;
  source: string;
  markdown: string;
  strings?: Strings;
};

export const findTranslations = async (root = join(import.meta.dir, "../i18n")): Promise<Translation[]> => {
  let entries: Dirent[];
  try {
    entries = await readdir(root, { withFileTypes: true });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }

  const known = new Set(LOCALES.filter(({ tag }) => tag !== DEFAULT_TAG).map(({ tag }) => localeDir(tag)));
  for (const entry of entries)
    if (entry.isDirectory() && !known.has(entry.name))
      console.warn(`docs/i18n/${entry.name}: not a known locale directory, skipped`);

  const translations: Translation[] = [];
  for (const locale of LOCALES) {
    if (locale.tag === DEFAULT_TAG) continue;
    const dir = localeDir(locale.tag);
    const source = `docs/i18n/${dir}/reference.md`;
    const file = Bun.file(join(root, dir, "reference.md"));
    if (!(await file.exists())) continue;
    translations.push({ locale, source, markdown: await file.text(), strings: STRINGS[locale.tag] });
  }
  return translations;
};

const SOURCE_MARKER = /^<!-- src:[0-9a-f]+ -->\r?$/;

export const stripSourceMarkers = (markdown: string): string => {
  let inFence = false;
  return markdown
    .split("\n")
    .filter((line) => {
      if (/^(```|~~~)/.test(line)) inFence = !inFence;
      return inFence || !SOURCE_MARKER.test(line);
    })
    .join("\n");
};

export const sectionHash = (body: string): string =>
  createHash("sha256").update(body.replace(/\r\n/g, "\n").trim()).digest("hex").slice(0, 8);

export type SectionState = "current" | "stale" | "unmarked" | "missing";

export const checkSections = (english: string, translated: string): { title: string; state: SectionState }[] => {
  const translatedSections = splitSections(translated);
  return splitSections(english).map((section, index) => {
    const candidate = translatedSections[index];
    if (!candidate) return { title: section.title, state: "missing" };
    const marker = candidate.body.split("\n", 1)[0].match(/^<!-- src:([0-9a-f]+) -->\r?$/);
    if (!marker) return { title: section.title, state: "unmarked" };
    return { title: section.title, state: marker[1] === sectionHash(section.body) ? "current" : "stale" };
  });
};

export const bodyFromFirstSection = (markdown: string, source: string): string => {
  const start = markdown.search(/^## /m);
  if (start < 0) throw new Error(`${source}: no "##" heading found`);
  if (markdown.slice(0, start).trim()) throw new Error(`${source}: content precedes the first "##" heading`);
  return markdown.slice(start);
};

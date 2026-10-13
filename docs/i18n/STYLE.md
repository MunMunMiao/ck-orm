# Translation style guide for the ck-orm docs

This file holds the rules shared by every translation under `docs/i18n/<lowercase-tag>/reference.md` and every UI table under `docs/src/strings/<lowercase-tag>.ts` (sections 1-4).
Each language has its own file, `docs/i18n/<lowercase-tag>/STYLE.md`, with its register, terminology table and sources, punctuation and patterns to avoid: zh-Hans, zh-Hant-TW, zh-Hant-HK, ja-JP, ko-KR, ru-RU, es-ES, fr-FR. Read this file and your own language file before translating anything; you do not need the other languages' files.

Maintenance commands (run in `docs/`):

- `bun run i18n:status` lists, per language, which sections are `current`, `stale`, `unmarked` or `missing` against the English source.
- `bun run i18n:status --hashes` prints the hash that goes into each `<!-- src:HASH -->` marker.
- `bun run build` fails when a translation changes the heading structure, the code blocks or the link anchors.

## 1. What is translated and what is not

| Item | Rule |
| --- | --- |
| Prose, list items, blockquote text, bold and italic text | Translate. |
| Heading text | Translate; heading level and order stay. Anchors keep the English ids by position. |
| Link text | Translate. Link targets, `(#id)` anchors and relative paths never change. |
| Table cells | Translate prose cells. Cells that are only code stay. Column structure and `|` separators stay. |
| Fenced code blocks, including comments inside them | Never translate; byte-identical to English (the build enforces it). |
| Inline code (backticks) | Never translate, never reformat, never add or drop backticks. |
| Product and technical proper nouns, API names, keywords, identifiers | Keep in English (see section 3). |
| UI strings (`docs/src/strings/*.ts`) | Translate the values only; placeholders `{mode} {next} {folder} {main} {package} {title}` stay verbatim. |
| `llms.txt` and the other llms files | English only. |
| Example notes and example code on the examples page | English only. Only the page chrome is translated (UI table). |

Other format rules:

- Each `##` heading is followed by exactly one `<!-- src:HASH -->` line once the section is translated. No other HTML comment may appear in a translation:
  the Markdown renderer prints it as visible text.
- Do not add, remove or reorder headings, code blocks, links or tables. Do not add translator's notes or explanations that the English does not contain.
- Emphasis must render. CommonMark may fail to treat `**` as bold next to full-width punctuation or quotation marks; reword or move the punctuation so that no literal `**` shows up.
- Straight ASCII quotes and apostrophes are fine in the Markdown source; the language files say when typographic forms are required.

## 2. Voice shared by all languages

The reader is a developer, and the writer is another developer from the same country explaining the library to a colleague. Concretely:

- Say what the code does and what to do about it, in the direct, spoken-but-professional register that local developer documentation uses. The language files give the exact register.
- Never mirror the English sentence structure. Read the English, understand it, and write the sentence a local developer would have written. English nominal chains
  ("make everyday ClickHouse access easier to structure") and passive constructions ("is rejected", "is decoded conservatively") become the natural local pattern.
- Keep the meaning exact. Do not add claims, do not drop caveats, and do not soften or strengthen warnings. "Cannot", "must", "never" and "rejects" keep their force.
- Keep the shape: roughly the same sentence count and order, the same list items, the same table rows. Merging or splitting a sentence is fine; omitting content is not.
- Be consistent. One concept, one rendering, throughout the whole file. Before translating a section, skim the sections already translated in the same file and reuse their terms.
- Avoid officialese, bureaucratese and calques. Each language file lists concrete patterns to avoid. Automated checks catch only a few of them, so read your own text aloud.
- Do not use "this document", "this section", "the user" or other self-referential or third-person phrasing when the English says "you" or has no subject; address the reader the way your language file says.
- No emoji, no kaomoji, no persona, no jokes that the English does not contain.

## 3. Terms that stay in English in every language, and concept words

These names are never translated, transliterated, expanded or re-spelled, in prose, headings, link text, table cells and UI strings:

- Products and projects: ClickHouse, ck-orm, OpenTelemetry, TypeScript, JavaScript, Bun, Node.js, npm, pnpm, Yarn, Drizzle, GitHub, Docker, Zod (and any other library name that occurs in the text).
- Acronyms and technical proper nouns: SQL, JSON, DDL, NULL, CTE (CTEs), HTTP, UTC, API (APIs), DSL, RE2, Unix, Decimal, Float64 and the other ClickHouse type names, SELECT and the other SQL keywords.
- Anything that is an identifier, option name, function name, setting name or file name, wherever it appears (even outside backticks).
- File and product names in UI strings: `llms.txt`, `MIT`, version numbers such as `0.1.0`.

### Concept words

Everything else is a concept word ("query", "session", "column", "password", "permission", "page", "documentation", "error", "request", "timeout", "behavior", ...). Concept words are translated into the local language
with the rendering local developers actually use, which is what the terminology table in each language file records.

- A concept word stays in English only when the table says so for a reason: it is a JavaScript/TypeScript/ClickHouse/SQL term of art that developers in that language demonstrably leave in English
  (for example `thenable`, `span`, `hook` in many languages), or it is an SQL keyword or API name.
- Leaving ordinary English words in running text ("password", "permission", "page", "documentation", "behavior", "production") is a defect, not a cautious choice.
  The result must read as one language with a few technical names inside it, not as English with the grammar swapped.
- "unverified" in a table row only means that no cited source confirms the regional form of the rendering. It never means "keep English". For such rows use the plain textbook or dictionary rendering,
  preferably one attested in several regions, and the row still counts as decided.
  The one exception is `peer dependency`, which the project owner decided to keep in English in es-ES and zh-Hant-HK (it is the name of npm's `peerDependencies` field).
- Whatever is decided is applied consistently across the whole file. Where the table keeps English, use the English word in its normal spelling.

## 4. Working method for a section

1. Read the English section and the surrounding code so the facts are right. Never guess what a code example does.
2. Translate in place, keeping the file buildable after every section; add the `<!-- src:HASH -->` marker only when the section is finished.
3. Check the result: `bun run i18n:status` for the markers and `bun run build` for headings, code blocks, links and anchors. Fix every error before moving on.
4. Read the result back as the reader would. If a sentence sounds translated, rewrite it. Unsure about a choice? Pick the more conservative wording and list it as uncertain in your final message.

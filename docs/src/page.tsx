import { Fragment, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import type { Example } from "./examples";
import type { Highlight } from "./highlight";
import { DEFAULT_TAG, type Locale, localeDir, type Strings } from "./i18n";
import { CodeBlock, CodeTabs, type Heading, Html } from "./reference";

// Runs before first paint so dark mode never flashes light. No stored choice means Auto (follow the system).
const THEME_SCRIPT = `var m;try{m=localStorage.getItem("theme")}catch(e){}m=m==="light"||m==="dark"?m:"auto";var r=document.documentElement;r.dataset.theme=m;r.classList.toggle("dark",m==="dark"||(m==="auto"&&matchMedia("(prefers-color-scheme: dark)").matches))`;

const FAVICON =
  "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='7' fill='%23151A1F'/%3E%3Cg fill='%23FFCC00'%3E%3Crect x='6' y='8' width='2.6' height='16'/%3E%3Crect x='10.2' y='8' width='2.6' height='16'/%3E%3Crect x='14.4' y='8' width='2.6' height='16'/%3E%3Crect x='18.6' y='8' width='2.6' height='16'/%3E%3Crect x='23' y='14.5' width='3' height='3'/%3E%3C/g%3E%3C/svg%3E";

type PageName = "docs" | "examples";
type PageBase = { locale: Locale; locales: Locale[]; ui: Strings; repoUrl: string; siteUrl: string };

const fill = (template: string, values: Record<string, ReactNode>): ReactNode[] =>
  template.split(/(\{\w+\})/).map((part, index) => {
    const name = /^\{(\w+)\}$/.exec(part)?.[1];
    // biome-ignore lint/suspicious/noArrayIndexKey: template chunks never reorder.
    return <Fragment key={index}>{name ? values[name] : part}</Fragment>;
  });

const fillString = (template: string, values: Record<string, string>): string =>
  template.replace(/\{(\w+)\}/g, (_, name: string) => values[name]);

const pagePaths = (locale: Locale, page: PageName) => ({
  siteRoot: "../".repeat((locale.tag === DEFAULT_TAG ? 0 : 1) + (page === "examples" ? 1 : 0)),
  localeRoot: page === "examples" ? "../" : "",
});

const languageHref = (siteRoot: string, locale: Locale, page: PageName): string => {
  const localePath = locale.tag === DEFAULT_TAG ? "" : `${localeDir(locale.tag)}/`;
  return `${siteRoot}${localePath}${page === "examples" ? "examples/" : ""}` || "./";
};

const absoluteLanguageHref = (siteUrl: string, locale: Locale, page: PageName): string => {
  const localePath = locale.tag === DEFAULT_TAG ? "" : `${localeDir(locale.tag)}/`;
  return `${siteUrl}/${localePath}${page === "examples" ? "examples/" : ""}`;
};

const Logo = () => (
  <svg viewBox="0 0 32 32" className="size-7" aria-hidden="true">
    <rect width="32" height="32" rx="7" fill="#151A1F" />
    <rect width="31" height="31" x=".5" y=".5" rx="6.5" fill="none" className="stroke-white/10" />
    <g fill="#FFCC00">
      <rect x="6" y="8" width="2.6" height="16" />
      <rect x="10.2" y="8" width="2.6" height="16" />
      <rect x="14.4" y="8" width="2.6" height="16" />
      <rect x="18.6" y="8" width="2.6" height="16" />
      <rect x="23" y="14.5" width="3" height="3" />
    </g>
  </svg>
);

const Globe = () => (
  <svg viewBox="0 0 20 20" className="size-5" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true">
    <circle cx="10" cy="10" r="7.5" />
    <path d="M2.8 10h14.4M10 2.5c2 2.1 3 4.6 3 7.5s-1 5.4-3 7.5c-2-2.1-3-4.6-3-7.5s1-5.4 3-7.5Z" />
  </svg>
);

const Toc = ({ headings }: { headings: Heading[] }) => (
  <ol>
    {headings
      .filter((heading) => heading.level === 2)
      .map((section) => {
        const children = headings.filter((heading) => heading.level === 3 && heading.section === section.id);
        return (
          <li key={section.id} data-section={section.id}>
            <a href={`#${section.id}`} data-target={section.id}>
              {section.content}
            </a>
            {children.length > 0 && (
              <ol>
                {children.map((child) => (
                  <li key={child.id}>
                    <a href={`#${child.id}`} data-target={child.id}>
                      {child.content}
                    </a>
                  </li>
                ))}
              </ol>
            )}
          </li>
        );
      })}
  </ol>
);

// Cycles Auto → Light → Dark; the icon shows the current mode (styles.css), client.ts handles clicks.
const ThemeButton = ({ ui }: { ui: Strings["theme"] }) => (
  <button
    id="theme"
    type="button"
    className="grid size-9 place-items-center rounded-sm text-muted hover:text-fg"
    aria-label={fillString(ui.label, { mode: ui.auto, next: ui.light })}
  >
    <svg
      viewBox="0 0 20 20"
      className="theme-icon size-[18px]"
      data-mode="auto"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      aria-hidden="true"
    >
      <rect x="2.5" y="3.5" width="15" height="10" rx="1.5" />
      <path d="M7 16.5h6M10 13.5v3" />
    </svg>
    <svg
      viewBox="0 0 20 20"
      className="theme-icon size-[18px]"
      data-mode="light"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      aria-hidden="true"
    >
      <circle cx="10" cy="10" r="3.4" />
      <path d="M10 1.8v2.1M10 16.1v2.1M1.8 10h2.1M16.1 10h2.1M4.2 4.2l1.5 1.5M14.3 14.3l1.5 1.5M4.2 15.8l1.5-1.5M14.3 5.7l1.5-1.5" />
    </svg>
    <svg viewBox="0 0 20 20" className="theme-icon size-[18px]" data-mode="dark" fill="currentColor" aria-hidden="true">
      <path d="M15.5 12.6A6.5 6.5 0 0 1 7.4 4.5a6.5 6.5 0 1 0 8.1 8.1Z" />
    </svg>
  </button>
);

const LanguageSwitcher = ({
  locale,
  locales,
  siteRoot,
  page,
  label,
}: {
  locale: Locale;
  locales: Locale[];
  siteRoot: string;
  page: PageName;
  label: string;
}) => {
  if (locales.length <= 1) return null;
  return (
    <details id="lang-menu" className="relative">
      <summary
        className="flex cursor-pointer list-none items-center gap-2 rounded-sm px-1.5 py-1.5 text-muted hover:text-fg sm:px-2 [&::-webkit-details-marker]:hidden"
        aria-label={`${label}: ${locale.name}`}
      >
        <Globe />
        <span className="hidden sm:inline">{locale.name}</span>
      </summary>
      <ul className="absolute top-[calc(100%+0.5rem)] right-0 z-40 max-h-[calc(100dvh-4rem)] w-max max-w-[calc(100vw-2rem)] overflow-y-auto rounded-sm border border-rule bg-paper py-1 shadow-lg">
        {locales.map((item) => (
          <li key={item.tag}>
            <a
              href={languageHref(siteRoot, item, page)}
              lang={item.tag}
              hrefLang={item.tag}
              data-lang-link=""
              className="flex px-3 py-2 font-sans text-fg hover:bg-panel aria-[current=true]:font-semibold"
              aria-current={item.tag === locale.tag ? "true" : undefined}
            >
              {item.name}
            </a>
          </li>
        ))}
      </ul>
    </details>
  );
};

const MenuButton = ({ label }: { label: string }) => (
  <button
    id="nav-open"
    type="button"
    className="-ml-2 grid size-10 place-items-center rounded-sm text-muted hover:text-fg lg:hidden"
    aria-label={label}
  >
    <svg viewBox="0 0 20 20" className="size-5" fill="currentColor" aria-hidden="true">
      <rect x="3" y="5" width="14" height="1.6" />
      <rect x="3" y="9.2" width="14" height="1.6" />
      <rect x="3" y="13.4" width="9" height="1.6" />
    </svg>
  </button>
);

const NavDialog = ({
  navLabel,
  tocLabel,
  closeLabel,
  headings,
}: {
  navLabel: string;
  tocLabel: string;
  closeLabel: string;
  headings: Heading[];
}) => (
  <dialog
    id="nav-dialog"
    className="m-0 h-dvh max-h-none w-[min(20rem,86vw)] max-w-none border-r border-rule bg-paper text-fg backdrop:bg-ink/50"
    aria-label={navLabel}
  >
    <div className="flex h-full flex-col">
      <div className="flex h-14 shrink-0 items-center justify-between border-b border-rule px-4">
        <span className="font-mono text-[0.6875rem] tracking-[0.08em] text-muted uppercase">{tocLabel}</span>
        <button
          id="nav-close"
          type="button"
          className="grid size-10 place-items-center text-muted hover:text-fg"
          aria-label={closeLabel}
        >
          <svg
            viewBox="0 0 20 20"
            className="size-5"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.6"
            aria-hidden="true"
          >
            <path d="M5 5l10 10M15 5L5 15" />
          </svg>
        </button>
      </div>
      <nav className="toc flex-1 overflow-y-auto overscroll-contain py-4 pr-2 pl-1" aria-label={tocLabel}>
        <Toc headings={headings} />
      </nav>
    </div>
  </dialog>
);

// Sticky table of contents on large screens; the same list opens from the menu button on small ones.
const WithToc = ({ label, headings, children }: { label: string; headings: Heading[]; children: ReactNode }) => (
  <div className="mx-auto max-w-[80rem] px-4 sm:px-6 lg:grid lg:grid-cols-[15.5rem_minmax(0,1fr)] lg:gap-12">
    <aside
      id="toc-aside"
      className="sticky top-14 hidden h-[calc(100dvh-3.5rem)] overflow-y-auto overscroll-contain py-10 lg:block"
    >
      <p className="mb-3 px-3 font-mono text-[0.6875rem] tracking-[0.08em] text-muted uppercase">{label}</p>
      <nav className="toc" aria-label={label}>
        <Toc headings={headings} />
      </nav>
    </aside>
    <div className="min-w-0">{children}</div>
  </div>
);

const NAV_LINK = "rounded-sm px-2 py-1.5 text-muted hover:text-fg aria-[current=page]:text-fg";

const Layout = ({
  page,
  locale,
  locales,
  ui,
  repoUrl,
  siteUrl,
  skipHref,
  headings,
  children,
}: PageBase & {
  page: PageName;
  skipHref: string;
  headings: Heading[];
  children: ReactNode;
}) => {
  const pageUi = page === "docs" ? ui.docs : ui.examples;
  const { siteRoot, localeRoot } = pagePaths(locale, page);
  const defaultLocale = locales[0];
  // The switcher adds a control to the header; tighten the spacing below sm so it still fits in 320 px.
  const multi = locales.length > 1;
  const navLink = multi ? `${NAV_LINK} max-sm:px-1.5` : NAV_LINK;
  const editUrl =
    page === "examples"
      ? `${repoUrl}/tree/main/examples`
      : locale.tag === DEFAULT_TAG
        ? `${repoUrl}/edit/main/docs/reference.md`
        : `${repoUrl}/edit/main/docs/i18n/${localeDir(locale.tag)}/reference.md`;
  return (
    <html
      lang={locale.tag}
      data-i18n-copy={ui.code.copy}
      data-i18n-copied={ui.code.copied}
      data-i18n-copy-failed={ui.code.copyFailed}
      data-i18n-theme={ui.theme.label}
      data-i18n-theme-auto={ui.theme.auto}
      data-i18n-theme-light={ui.theme.light}
      data-i18n-theme-dark={ui.theme.dark}
    >
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>{pageUi.title}</title>
        <meta name="description" content={pageUi.description} />
        <meta name="color-scheme" content="light dark" />
        <meta name="theme-color" content="#FCFCFA" media="(prefers-color-scheme: light)" />
        <meta name="theme-color" content="#0E1215" media="(prefers-color-scheme: dark)" />
        <link rel="icon" href={FAVICON} />
        {locales.map((item) => (
          <link key={item.tag} rel="alternate" hrefLang={item.tag} href={absoluteLanguageHref(siteUrl, item, page)} />
        ))}
        <link rel="alternate" hrefLang="x-default" href={absoluteLanguageHref(siteUrl, defaultLocale, page)} />
        <link rel="describedby" href={`${siteRoot}llms.txt`} />
        <link
          rel="alternate"
          type="text/markdown"
          href={
            locale.tag === DEFAULT_TAG
              ? "index.md"
              : `${siteRoot}${page === "examples" ? "examples/index.md" : "index.md"}`
          }
        />
        {/* biome-ignore lint/security/noDangerouslySetInnerHtml: static theme bootstrap defined above. */}
        <script dangerouslySetInnerHTML={{ __html: THEME_SCRIPT }} />
        <link rel="stylesheet" href={`${siteRoot}../src/styles.css`} />
        <script type="module" src={`${siteRoot}../src/client.ts`} />
      </head>

      <body className="bg-paper font-sans text-fg antialiased">
        <a
          href={skipHref}
          className="sr-only z-50 bg-signal px-3 py-2 font-medium text-ink focus:not-sr-only focus:fixed focus:top-2 focus:left-2"
        >
          {pageUi.skip}
        </a>

        <header className="sticky top-0 z-30 border-b border-rule bg-paper/90 backdrop-blur">
          <div className="mx-auto flex h-14 max-w-[80rem] items-center gap-2 px-4 sm:px-6">
            <MenuButton label={pageUi.openNav} />
            <a
              href={page === "docs" ? "#top" : localeRoot}
              className="flex items-center gap-2.5 text-[1.05rem] font-semibold tracking-tight"
            >
              <Logo />
              ck-orm
            </a>

            <nav
              className={
                multi ? "ml-auto flex items-center gap-0 text-sm sm:gap-1" : "ml-auto flex items-center gap-1 text-sm"
              }
              aria-label={ui.site.nav}
            >
              <a
                href={`${localeRoot}examples/`}
                className={navLink}
                aria-current={page === "examples" ? "page" : undefined}
              >
                {ui.site.examples}
              </a>
              <a href={`${siteRoot}llms.txt`} className={`hidden sm:block ${navLink}`}>
                llms.txt
              </a>
              <a href="https://www.npmjs.com/package/ck-orm" className={`hidden sm:block ${navLink}`}>
                npm
              </a>
              <a href={repoUrl} className={navLink}>
                GitHub
              </a>
              <LanguageSwitcher
                locale={locale}
                locales={locales}
                siteRoot={siteRoot}
                page={page}
                label={ui.site.language}
              />
              <ThemeButton ui={ui.theme} />
            </nav>
          </div>
        </header>

        {children}

        <footer className="border-t border-rule">
          <div className="mx-auto flex max-w-[80rem] flex-wrap gap-x-6 gap-y-2 px-4 py-8 text-sm text-muted sm:px-6">
            <span>{ui.site.license}</span>
            <a href={editUrl} className="hover:text-fg">
              {ui.site.editPage}
            </a>
            <a href={`${siteRoot}llms.txt`} className="hover:text-fg">
              {ui.site.llmsFooter}
            </a>
          </div>
        </footer>

        <NavDialog
          navLabel={pageUi.navLabel}
          tocLabel={pageUi.tocLabel}
          closeLabel={ui.site.closeNav}
          headings={headings}
        />
      </body>
    </html>
  );
};

const CTA = "inline-block font-medium underline decoration-signal decoration-2 underline-offset-4";

const DocsPage = ({
  locale,
  locales,
  ui,
  repoUrl,
  siteUrl,
  reference,
}: PageBase & { reference: { html: string; headings: Heading[] } }) => {
  return (
    <Layout
      page="docs"
      locale={locale}
      locales={locales}
      ui={ui}
      repoUrl={repoUrl}
      siteUrl={siteUrl}
      skipHref="#reference"
      headings={reference.headings}
    >
      <main id="top">
        <section className="mx-auto max-w-[80rem] px-4 pt-10 pb-14 sm:px-6 sm:pt-16 lg:pb-20">
          <p className="inline-flex items-center gap-2 rounded-sm border border-rule px-2.5 py-1 font-mono text-xs text-muted">
            <span className="size-1.5 bg-signal" aria-hidden="true" />
            {ui.docs.badge}
          </p>

          <h1 className="mt-6 font-display text-[length:calc(clamp(3.1rem,8.4vw,6.75rem)*var(--display-scale))] leading-(--display-leading) font-bold tracking-[-0.005em] text-balance uppercase">
            {ui.docs.headline}
          </h1>

          <p className="mt-8 max-w-[58ch] text-lg leading-relaxed text-muted sm:text-xl">{ui.docs.intro}</p>
          <div className="mt-5 flex flex-wrap gap-x-6 gap-y-3">
            <a href="#installation" className={CTA}>
              {ui.docs.ctaInstall}
            </a>
            <a href="examples/" className={CTA}>
              {ui.docs.ctaExamples}
            </a>
          </div>
        </section>

        <div id="reference" className="border-t border-rule">
          <WithToc label={ui.docs.tocLabel} headings={reference.headings}>
            <Html className="doc max-w-[48rem] py-12 lg:py-14" html={reference.html} />
          </WithToc>
        </div>
      </main>
    </Layout>
  );
};

const ExamplesPage = ({
  locale,
  locales,
  ui,
  repoUrl,
  siteUrl,
  examples,
  highlight,
}: PageBase & { examples: Example[]; highlight: Highlight }) => {
  const headings: Heading[] = examples.map((example) => ({
    level: 2,
    id: example.id,
    content: example.id,
    section: example.id,
  }));
  const runCommand = `CLICKHOUSE_URL=http://localhost:8123 bun examples/${examples[0].id}/main.ts`;
  return (
    <Layout
      page="examples"
      locale={locale}
      locales={locales}
      ui={ui}
      repoUrl={repoUrl}
      siteUrl={siteUrl}
      skipHref={`#${examples[0].id}`}
      headings={headings}
    >
      <main id="top">
        <WithToc label={ui.examples.tocLabel} headings={headings}>
          <div className="max-w-[48rem] pt-12 pb-20 lg:pt-14">
            <h1 className="font-display text-[length:calc(clamp(2.75rem,6vw,4.5rem)*var(--display-scale))] leading-(--display-leading) font-bold uppercase">
              {ui.examples.heading}
            </h1>
            <p className="mt-6 max-w-[62ch] text-lg leading-relaxed text-muted">
              {fill(ui.examples.intro, {
                folder: (
                  <a href={`${repoUrl}/tree/main/examples`} className={CTA}>
                    examples/
                  </a>
                ),
                main: <code className="font-mono text-[0.9em] text-fg">main.ts</code>,
                package: <code className="font-mono text-[0.9em] text-fg">package.json</code>,
              })}
            </p>
            <p className="mt-6 max-w-[62ch] text-muted">{ui.examples.runIntro}</p>
            <div className="mt-3 max-w-[48rem]">
              <CodeBlock language="bash" html={highlight(runCommand, "bash")} copy={ui.code.copy} />
            </div>

            {examples.map((example) => (
              <section key={example.id} aria-labelledby={example.id}>
                <h2 id={example.id} data-section={example.id} className="section-title">
                  {example.id}
                </h2>
                <a
                  href={`${repoUrl}/blob/main/${example.path}`}
                  className="mt-3 inline-block font-mono text-[0.8125rem] text-muted underline decoration-rule underline-offset-4 hover:text-fg"
                >
                  {example.path}
                </a>
                <div className="mt-4">
                  <CodeTabs
                    id={`example-${example.id}`}
                    label={fillString(ui.examples.tabsLabel, { title: example.id })}
                    tabs={[
                      { label: "main.ts", html: highlight(example.source, "ts") },
                      { label: "SQL", html: highlight(example.sql, "sql") },
                    ]}
                    copy={ui.code.copy}
                  />
                </div>
              </section>
            ))}
          </div>
        </WithToc>
      </main>
    </Layout>
  );
};

const documentHtml = (element: ReactNode) => `<!doctype html>${renderToStaticMarkup(element)}`;

export const renderDocsPage = (props: PageBase & { reference: { html: string; headings: Heading[] } }) =>
  documentHtml(<DocsPage {...props} />);
export const renderExamplesPage = (props: PageBase & { examples: Example[]; highlight: Highlight }) =>
  documentHtml(<ExamplesPage {...props} />);

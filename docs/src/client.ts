const $ = <T extends Element = HTMLElement>(selector: string, root: ParentNode = document) =>
  root.querySelector<T>(selector);
const $$ = <T extends Element = HTMLElement>(selector: string, root: ParentNode = document) => [
  ...root.querySelectorAll<T>(selector),
];

const { i18nCopy, i18nCopied, i18nCopyFailed, i18nTheme, i18nThemeAuto, i18nThemeLight, i18nThemeDark } =
  document.documentElement.dataset;
const localized = (value: string | undefined, attribute: `data-${string}`): string => {
  if (value === undefined || value === "") throw new Error(`Missing or empty localization attribute: ${attribute}`);
  return value;
};
const COPY = localized(i18nCopy, "data-i18n-copy");
const COPIED = localized(i18nCopied, "data-i18n-copied");
const COPY_FAILED = localized(i18nCopyFailed, "data-i18n-copy-failed");
const THEME = localized(i18nTheme, "data-i18n-theme");

// Theme: Auto (follow the system, the default), Light or Dark. The button cycles through them in that order.
type ThemeMode = "auto" | "light" | "dark";
const THEME_LABELS: Record<ThemeMode, string> = {
  auto: localized(i18nThemeAuto, "data-i18n-theme-auto"),
  light: localized(i18nThemeLight, "data-i18n-theme-light"),
  dark: localized(i18nThemeDark, "data-i18n-theme-dark"),
};
const NEXT_THEME: Record<ThemeMode, ThemeMode> = { auto: "light", light: "dark", dark: "auto" };
const systemDark = matchMedia("(prefers-color-scheme: dark)");
const themeButton = $("#theme");

const currentTheme = (): ThemeMode => {
  const mode = document.documentElement.dataset.theme;
  return mode === "light" || mode === "dark" ? mode : "auto";
};
const applyTheme = (mode: ThemeMode) => {
  const root = document.documentElement;
  root.dataset.theme = mode;
  root.classList.toggle("dark", mode === "dark" || (mode === "auto" && systemDark.matches));
  const label = THEME.replaceAll("{mode}", THEME_LABELS[mode]).replaceAll("{next}", THEME_LABELS[NEXT_THEME[mode]]);
  themeButton?.setAttribute("aria-label", label);
  themeButton?.setAttribute("title", label);
};

applyTheme(currentTheme());
themeButton?.addEventListener("click", () => {
  const mode = NEXT_THEME[currentTheme()];
  applyTheme(mode);
  try {
    if (mode === "auto") localStorage.removeItem("theme");
    else localStorage.setItem("theme", mode);
  } catch {
    // Storage is blocked (e.g. strict privacy modes); the choice still applies until the page reloads.
  }
});
systemDark.addEventListener("change", () => {
  if (currentTheme() === "auto") applyTheme("auto");
});

// Tabs (WAI-ARIA tabs pattern, automatic activation): the hero examples and tabbed code blocks.
for (const tablist of $$('[role="tablist"]')) {
  const tabs = $$('[role="tab"]', tablist);
  const selectTab = (tab: HTMLElement) => {
    for (const other of tabs) {
      const selected = other === tab;
      other.setAttribute("aria-selected", String(selected));
      other.tabIndex = selected ? 0 : -1;
      const panel = document.getElementById(other.getAttribute("aria-controls") ?? "");
      if (panel) panel.hidden = !selected;
    }
  };
  for (const tab of tabs) {
    tab.addEventListener("click", () => selectTab(tab));
    tab.addEventListener("keydown", (event) => {
      const step = event.key === "ArrowRight" ? 1 : event.key === "ArrowLeft" ? -1 : 0;
      if (!step) return;
      const next = tabs[(tabs.indexOf(tab) + step + tabs.length) % tabs.length];
      selectTab(next);
      next.focus();
    });
  }
}

// Copy buttons copy their code block (the visible tab when the block has several).
document.addEventListener("click", async (event) => {
  const button = (event.target as Element).closest<HTMLElement>("[data-copy]");
  if (!button) return;
  const block = button.closest(".codeblock");
  const source = block?.querySelector('[role="tabpanel"]:not([hidden]) code') ?? block?.querySelector("code");
  try {
    await navigator.clipboard.writeText(source?.textContent ?? "");
    button.textContent = COPIED;
  } catch {
    button.textContent = COPY_FAILED;
  }
  setTimeout(() => {
    button.textContent = COPY;
  }, 1600);
});

// Mobile navigation.
const navDialog = $<HTMLDialogElement>("#nav-dialog");
$("#nav-open")?.addEventListener("click", () => navDialog?.showModal());
$("#nav-close")?.addEventListener("click", () => navDialog?.close());
navDialog?.addEventListener("click", (event) => {
  const target = event.target as Element;
  if (target === navDialog || target.closest("a")) navDialog.close();
});

// Keep language links on the current fragment and dismiss the dropdown when it loses focus.
const languageMenu = $<HTMLDetailsElement>("#lang-menu");
if (languageMenu) {
  const languageLinks = $$<HTMLAnchorElement>("[data-lang-link]");
  const updateLanguageLinks = () => {
    for (const link of languageLinks) link.hash = location.hash;
  };
  updateLanguageLinks();
  addEventListener("hashchange", updateLanguageLinks);

  const summary = $<HTMLElement>("summary", languageMenu);
  document.addEventListener("keydown", (event) => {
    if (!languageMenu.open || event.key !== "Escape") return;
    languageMenu.open = false;
    summary?.focus();
  });
  document.addEventListener("click", (event) => {
    if (languageMenu.open && event.target instanceof Element && !languageMenu.contains(event.target)) {
      languageMenu.open = false;
    }
  });
}

// Scrollspy: highlight the last heading above the sticky header and expand its section.
// Reference sections and examples both mark their headings with data-section.
const headings = $$("main :is(h2, h3)[data-section]");
const aside = $("#toc-aside");
let queued = false;
const updateScrollspy = () => {
  queued = false;
  let current: HTMLElement | undefined;
  for (const heading of headings) {
    if (heading.getBoundingClientRect().top > 120) break;
    current = heading;
  }
  for (const link of $$(".toc a")) {
    if (current && link.dataset.target === current.id) link.setAttribute("aria-current", "location");
    else link.removeAttribute("aria-current");
  }
  for (const item of $$(".toc > ol > li")) {
    item.toggleAttribute("data-open", current !== undefined && item.dataset.section === current.dataset.section);
  }
  const active = aside && $("a[aria-current]", aside);
  if (aside && active && aside.offsetParent) {
    const box = aside.getBoundingClientRect();
    const target = active.getBoundingClientRect();
    if (target.top < box.top || target.bottom > box.bottom) aside.scrollTop += target.top - box.top - box.height / 3;
  }
};
addEventListener(
  "scroll",
  () => {
    if (queued) return;
    queued = true;
    requestAnimationFrame(updateScrollspy);
  },
  { passive: true },
);
updateScrollspy();

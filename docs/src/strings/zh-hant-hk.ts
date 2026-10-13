import type { Strings } from "../i18n";

export default {
  site: {
    nav: "網站",
    examples: "範例",
    language: "語言",
    closeNav: "關閉導覽",
    license: "MIT 授權",
    editPage: "在 GitHub 編輯這頁",
    llmsFooter: "AI 代理程式用的 llms.txt",
  },
  theme: {
    label: "主題：{mode}。切換至 {next}。",
    auto: "跟隨系統",
    light: "淺色",
    dark: "深色",
  },
  code: {
    copy: "複製",
    copied: "已複製",
    copyFailed: "複製失敗",
    variants: "程式碼選項",
    sectionLink: "連結到這一節",
  },
  docs: {
    title: "ck-orm：適用於 TypeScript 的類型安全 ClickHouse",
    description:
      "ck-orm 是適用於現代 JavaScript 執行環境的具類型 ClickHouse 查詢層，提供模式 DSL、查詢建構器、原始 SQL 模板、工作階段與可觀測性。",
    skip: "跳到參考資料",
    tocLabel: "參考資料",
    openNav: "開啟參考資料導覽",
    navLabel: "參考資料導覽",
    badge: "0.1.0 前的版本仍可能包含 API 破壞性變更。請鎖定確切版本。",
    headline: "類型安全 ClickHouse，又不掩蓋 ClickHouse",
    intro:
      "ck-orm 是適用於現代 JavaScript 執行環境的 ClickHouse 模式 DSL、具類型查詢建構器和 SQL 模板層。查詢會編譯成易讀的 ClickHouse SQL，數值會以具類型參數傳遞，64 位元整數和 Decimal 會解碼成精確字串。",
    ctaInstall: "從安裝開始",
    ctaExamples: "查看範例",
  },
  examples: {
    title: "ck-orm 範例",
    description: "可執行的 ck-orm 範例，並顯示每個範例實際傳送到 ClickHouse 的 SQL。",
    skip: "跳到範例",
    tocLabel: "範例",
    openNav: "開啟範例導覽",
    navLabel: "範例導覽",
    heading: "範例",
    intro:
      "每個範例都是 {folder} 內可執行的項目，包含 {main} 和 {package}。這一頁會顯示同一份檔案。SQL 分頁會列出它送給 ClickHouse 的每一個請求，範例自己的單元測試會檢查這些 SQL。檔案末尾的註解會列出這個範例輸出的資料列。",
    runIntro: "每個範例都會建立自己的工作階段暫存資料表，因此可以在任何 ClickHouse 伺服器上執行：",
    tabsLabel: "{title}：原始碼和 SQL",
  },
} satisfies Strings;

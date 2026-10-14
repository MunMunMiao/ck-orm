import type { Strings } from "../i18n";

export default {
  site: {
    nav: "網站",
    examples: "範例",
    language: "語言",
    closeNav: "關閉導覽",
    license: "MIT 授權",
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
      "每個範例都是 {folder} 內可執行的項目，包含 {main} 和 {package}。這一頁會顯示同一份檔案。建置時會用 ck-orm 執行它，所以 SQL 分頁會列出 ClickHouse 收到的每一個請求。檔案末尾的註解會列出這個範例輸出的資料列。",
    items: {
      "decimal-sum": {
        title: "精確 Decimal 合計",
        summary: "加總 Decimal(20, 5) 欄位，並把合計解碼成字串，因此絕不會經過浮點數。",
      },
      "literal-search": {
        title: "安全字面值搜尋",
        summary:
          '數值會以具類型查詢參數傳遞，絕不會拼接進 SQL；ck.contains 會逸出 % 和 _，因此 "alpha%" 會按字面匹配。',
      },
      "left-join": {
        title: "含 NULL 的 LEFT JOIN",
        summary:
          "leftJoin() 會送出 join_use_nulls = 1，因此未配對的右側欄位在 ClickHouse 是 NULL，在資料列類型中是 null。",
      },
      "website-analytics": {
        title: "網站分析",
        summary: "在 UTC [start, end) 時間範圍內，按路徑統計單一網站的頁面瀏覽量和精確不重複訪客數。",
        notes: "使用工作階段暫存資料表；不同路徑的訪客數可能重疊。",
      },
      "asof-price": {
        title: "ASOF JOIN 最新報價",
        summary:
          "使用 ASOF LEFT JOIN 為每筆交易配對同一交易代號在該時刻或之前的最新報價。Decimal 價格保持為字串，缺少歷史報價時返回 null。",
        notes: "使用工作階段暫存資料表。",
      },
      "dynamic-signals": {
        title: "Map 訊號篩選",
        summary:
          "篩選 Map(String, Float64) 中變動的模型訊號，先檢查鍵是否存在，再套用包含邊界值的門檻。已儲存的零仍然是真實分數。",
        notes: "使用工作階段暫存資料表。",
      },
      "trace-list": {
        title: "附篩選條件和總數的追蹤列表",
        summary:
          "用可選條件建立一份篩選條件列表，同時用於 LIMIT/OFFSET 分頁查詢和總數統計，因此請求超出最後一頁的頁面時，仍會返回總數。",
        notes: "使用工作階段暫存資料表。",
      },
      "support-tickets": {
        title: "具類型的欄位與執行階段驗證",
        summary:
          "Enum8 欄位、$type 品牌類型和 $type JSON 結構讓資料列類型更精確；手寫的 Standard Schema 驗證器會標準化電郵地址，並在發送任何請求前拒絕無效的地址。",
        notes: "使用工作階段暫存資料表。",
      },
      "bluesky-events": {
        title: "查詢 JSON 事件內容",
        summary:
          "按照 ClickHouse JSONBench 的查詢，用具類型的 JSON 路徑按集合統計 Bluesky 事件，再用類型轉換後的動態路徑和 JSON 子物件讀取帖文。",
        notes: "使用工作階段暫存資料表。",
      },
      "cdc-rollup": {
        title: "在 ClickHouse 內匯總 CDC 資料列",
        summary:
          "用 FINAL 為 Postgres CDC 資料列去除重複，再用 INSERT ... SELECT 匯總成每日收入，只有最終結果的資料列會返回到 JavaScript 端。",
        notes: "使用工作階段暫存資料表。",
      },
      "usage-metering": {
        title: "用量計量",
        summary: "按租戶、用戶和 UTC 分鐘，用插入觸發的物化檢視聚合多個插入批次的用量。",
        notes:
          "請在可丟棄的數據庫內順序執行，該數據庫要有 CREATE、SELECT、INSERT 和 DROP 權限。使用固定名稱的持久資料表及物化檢視，只清理已成功建立的物件。進程中斷或 CREATE 回應遺失後，可能需要手動檢查殘留物件。",
      },
      "request-latency": {
        title: "請求延遲百分位數",
        summary: "按租戶、操作和 UTC 分鐘，用插入觸發的物化檢視聚合成功請求數、加權平均延遲及精確 p95/p99。",
        notes:
          "請在可丟棄的數據庫內順序執行，該數據庫要有 CREATE、SELECT、INSERT 和 DROP 權限。只清理已成功建立的固定名稱資料表和物化檢視；進程中斷或 CREATE 回應遺失可能留下物件。精確分位數狀態保留全部樣本，生產環境使用時需評估記憶體用量。",
      },
      "query-plugins": {
        title: "查詢插件",
        summary: "插件為每個請求加入標頭，並按順序記錄 ck-orm 送出的語句，包括工作階段暫存資料表的 CREATE 和 DROP。",
        notes: "使用工作階段暫存資料表。",
      },
      "otel-tracing": {
        title: "OpenTelemetry 追蹤",
        summary:
          "ck-orm/otel 的 otelTracing() 為每個請求記錄一個 span。範例先啟動帶有記憶體內匯出器的 OpenTelemetry SDK，再印出這些 span，它們嵌套在範例自己開啟的 span 之下。",
        notes: "使用工作階段暫存資料表。",
      },
    },
    tabsLabel: "{title}：原始碼和 SQL",
  },
} satisfies Strings;

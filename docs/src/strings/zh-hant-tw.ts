import type { Strings } from "../i18n";

export default {
  site: {
    nav: "網站導覽",
    examples: "範例",
    language: "語言",
    closeNav: "關閉導覽",
    license: "MIT 授權條款",
  },
  theme: {
    label: "主題：{mode}。切換到 {next}。",
    auto: "跟隨系統",
    light: "淺色",
    dark: "深色",
  },
  code: {
    copy: "複製",
    copied: "已複製",
    copyFailed: "複製失敗",
    variants: "程式碼選項",
    sectionLink: "連結到這個章節",
  },
  docs: {
    title: "ck-orm：適用於 TypeScript 的型別安全 ClickHouse",
    description:
      "ck-orm 是用在現代 JavaScript 執行環境的有型別 ClickHouse 查詢層，提供 schema DSL、查詢建構器、原始 SQL 範本、工作階段與可觀察性。",
    skip: "跳到參考資料",
    tocLabel: "參考資料",
    openNav: "開啟參考資料導覽",
    navLabel: "參考資料導覽",
    badge: "0.1.0 之前的版本可能破壞 API。請鎖定確切版本。",
    headline: "型別安全 ClickHouse，不隱藏 ClickHouse",
    intro:
      "ck-orm 是用在現代 JavaScript 執行環境的 ClickHouse schema DSL、有型別查詢建構器和 SQL 範本層。查詢會編譯成容易閱讀的 ClickHouse SQL，數值以有型別參數傳遞，64 位元整數與 Decimal 會解碼成精確字串。",
    ctaInstall: "從安裝開始",
    ctaExamples: "查看範例",
  },
  examples: {
    title: "ck-orm 範例",
    description: "可執行的 ck-orm 範例，會顯示每個範例實際傳給 ClickHouse 的 SQL。",
    skip: "跳到範例",
    tocLabel: "範例",
    openNav: "開啟範例導覽",
    navLabel: "範例導覽",
    heading: "範例",
    intro:
      "每個範例都是 {folder} 中可執行的專案，裡面有 {main} 和 {package}。這頁顯示的就是同一份檔案。建置流程會用 ck-orm 執行它，因此 SQL 分頁會列出 ClickHouse 收到的每個請求。檔案最後的註解則列出它輸出的資料列。",
    items: {
      "decimal-sum": {
        title: "精確 Decimal 總計",
        summary: "加總 Decimal(20, 5) 欄位，並把總計解碼成字串，所以絕不會經過浮點數。",
      },
      "literal-search": {
        title: "安全字面文字搜尋",
        summary: '值以有型別查詢參數傳遞，絕不會拼進 SQL，ck.contains 會跳脫 % 和 _，所以 "alpha%" 會按字面相符。',
      },
      "left-join": {
        title: "含 NULL 的左聯結",
        summary:
          "leftJoin() 會送出 join_use_nulls = 1，所以沒有配對到的右側欄位在 ClickHouse 中是 NULL，在資料列型別中是 null。",
      },
      "website-analytics": {
        title: "網站分析",
        summary: "在 UTC [start, end) 時間範圍內，依路徑統計單一網站的頁面瀏覽量和精確不重複訪客數。",
        notes: "使用工作階段暫存資料表；不同路徑的訪客可能重疊。",
      },
      "asof-price": {
        title: "ASOF JOIN 最新報價",
        summary:
          "使用 ASOF LEFT JOIN 為每筆交易配對同一交易商品在該時刻或之前的最新報價。Decimal 價格保持為字串，缺少歷史報價時回傳 null。",
        notes: "使用工作階段暫存資料表。",
      },
      "dynamic-signals": {
        title: "Map 訊號篩選",
        summary:
          "篩選 Map(String, Float64) 中變動的模型訊號，先檢查鍵是否存在，再套用包含邊界值的門檻。已儲存的零仍然是真實分數。",
        notes: "使用工作階段暫存資料表。",
      },
      "trace-list": {
        title: "含篩選條件與總數的追蹤清單",
        summary:
          "以選用條件建立一份篩選條件清單，同時用於 LIMIT/OFFSET 分頁查詢和總數統計，因此頁碼超過最後一頁時仍會回傳總數。",
        notes: "使用工作階段暫存資料表。",
      },
      "support-tickets": {
        title: "有型別的欄位與執行階段驗證",
        summary:
          "Enum8 欄位、$type 品牌型別和 $type JSON 資料形狀讓資料列型別更精確；手寫的 Standard Schema 驗證器會正規化電子郵件地址，並在送出任何請求前拒絕無效的地址。",
        notes: "使用工作階段暫存資料表。",
      },
      "bluesky-events": {
        title: "查詢 JSON 事件資料",
        summary:
          "依照 ClickHouse JSONBench 的查詢，透過有型別的 JSON 路徑依 collection 統計 Bluesky 事件，再用已轉型的動態路徑和 JSON 子物件讀取貼文。",
        notes: "使用工作階段暫存資料表。",
      },
      "cdc-rollup": {
        title: "在 ClickHouse 內彙總 CDC 資料列",
        summary:
          "用 FINAL 對 Postgres CDC 資料列去除重複，再用 INSERT ... SELECT 彙總成每日營收，只有最終結果資料列會回到 JavaScript。",
        notes: "使用工作階段暫存資料表。",
      },
      "usage-metering": {
        title: "用量計量",
        summary: "用插入觸發的實體化檢視，依租戶、使用者和 UTC 分鐘彙總多個插入批次的用量。",
        notes:
          "請在可丟棄的資料庫內循序執行，並要有 CREATE、SELECT、INSERT 和 DROP 權限。使用固定名稱的持久資料表及實體化檢視，僅清理已成功建立的物件。程序中斷或 CREATE 回應遺失後，可能需要手動檢查殘留物件。",
      },
      "request-latency": {
        title: "請求延遲百分位數",
        summary: "用插入觸發的實體化檢視，依租戶、操作和 UTC 分鐘彙總成功請求數、加權平均延遲及精確 p95/p99。",
        notes:
          "請在可丟棄的資料庫內循序執行，並要有 CREATE、SELECT、INSERT 和 DROP 權限。僅清理已成功建立的固定名稱資料表和實體化檢視；程序中斷或 CREATE 回應遺失可能留下物件。精確分位數狀態保留全部樣本，正式環境使用時需評估記憶體用量。",
      },
      "query-plugins": {
        title: "查詢外掛程式",
        summary:
          "外掛程式替每個請求加上標頭，並依序記錄 ck-orm 送出的陳述式，包括工作階段暫存資料表的 CREATE 和 DROP。",
        notes: "使用工作階段暫存資料表。",
      },
      "otel-tracing": {
        title: "OpenTelemetry 追蹤",
        summary:
          "ck-orm/otel 的 otelTracing() 會為每個請求記錄一個 span。範例用記憶體內的匯出器啟動 OpenTelemetry SDK，再印出這些 span；它們的父 span 是範例自己開啟的 span。",
        notes: "使用工作階段暫存資料表。",
      },
    },
    tabsLabel: "{title}：原始碼和 SQL",
  },
} satisfies Strings;

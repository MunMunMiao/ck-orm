## zh-Hant-HK - 繁體中文（香港）

> 本文件只規定 `docs/reference.md` 的香港書面繁體中文寫法。它不改動程式碼、指令、URL、識別碼、API 名稱或 Markdown 結構。

### 1. 變體與讀者

- 目標變體是香港書面中文，不是口語粵文，也不是把台灣繁體字逐字搬過來。IANA 的語言標籤資料列出 `zh-Hant-HK`；它只界定語言、文字和地區範圍，不會替我們決定每個 IT 詞語。
  - https://www.iana.org/assignments/language-tags
- 讀者是會寫 TypeScript、SQL 和 ClickHouse 的香港開發者。說明要直接、可執行，像同事向同事解釋程式，而不是公文或逐句翻譯。
- 香港詞形優先參考政府 IT 詞彙表、教育局 ICT 詞彙表及香港本地技術資料。沒有直接香港用例的概念標為 `unverified`，但仍須選定具體中文寫法。
- 正文禁止使用口語粵字，例如 `嘅`、`係`、`咗`、`啲`、`喺`、`冇`、`唔`、`佢哋`。程式碼、原始日誌、URL 和使用者提供的引文不受這項改寫規則影響。

### 2. 語氣與句法

用短句、主動句和清楚的主語。先說動作和結果，再補充限制；同一段只處理一個主要動作。可以使用「你」，但不要用命令式口吻訓斥讀者。

| 不自然 | 建議 |
| --- | --- |
| 本函式將會對資料進行處理。 | 這個函數會處理資料。 |
| 在設定無效之情況下，應予以報錯。 | 設定無效時，直接返回錯誤。 |
| 透過重試來恢復連線。 | 重試可以恢復連線。 |
| 務請注意，敬請先執行遷移。 | 請先執行遷移，再啟動應用程式。 |

依據：Microsoft 的寫作建議強調直接表達；Google 的開發者風格指南要求清楚、簡單、少用客套語。香港本地詞形另以 OGCIO 和教育局詞彙表為準。

- https://learn.microsoft.com/zh-cn/style-guide/top-10-tips-style-voice
- https://developers.google.cn/style/tone
- https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf

### 3. 避免的官方腔和翻譯腔

以下是優先改寫的模式，不是對每個詞的機械禁用；若技術語義確實需要，保留準確性。

| 避免 | 推薦 | 說明 |
| --- | --- | --- |
| 進行查詢、進行設定、進行驗證 | 查詢、設定、驗證 | 動詞已經足夠。 |
| 予以處理、予以拒絕 | 處理、拒絕 | 用具體動詞。 |
| 務請、敬請 | 請；直接說明動作 | 不用公函口吻。 |
| 據悉、茲、現予 | 根據……；現在…… | 說明來源或動作。 |
| 倘若……，乃…… | 如果……，就…… | 使用日常連接詞。 |
| 乃、遂 | 就；因此 | 避免古雅書面語。 |
| 相關設定、相關資料 | 這項設定；這些資料 | 指明對象。 |
| 本文件、本章、下述 | 這份文件；這一節；以下 | 需要指示時才指示。 |
| 透過……來…… | 用……；……可以…… | 不要把兩個介詞套在一起。 |
| 得以完成 | 可以完成；完成 | 直接說能力或結果。 |
| 以便…… | 讓……；這樣就…… | 用自然的目的句。 |
| 針對……進行處理 | 處理…… | 直接說對象。 |
| 將……予以…… | ……；把…… | 改成主動句。 |
| 被……所拒絕 | 被……拒絕；……拒絕 | 刪除多餘的「所」。 |
| 可……（連續堆疊） | 可以……；……會…… | 避免含糊的能力句。 |

### 4. 標點、空格與排版

- 中文正文使用全形中文標點：`，。！？：；`。中文標點貼近前面的內容，不在標點前加空格；中文和拉丁字母、數字、單位或行內程式碼之間通常留一個 ASCII 空格。這是本專案正文規則，不改動程式碼片段內部。
- 外層引號用「……」，嵌套引號用『……』。香港大學出版社的中文寫作指南也採用這一組引號；引用台灣或英文原文時，保留原文引號並標明是引文。
- 省略號寫 `……`，破折號寫 `——`。指令、URL、正則表達式、版本號和圍欄程式碼區塊原樣保留，不能改動其中的 `...`、`--` 或半形標點。
- 冒號後接說明或列表時不額外插入空格；列表項是完整句便用句號，短語不強行補句號。
- 中文語境使用全形括號（ ）。API 參數、檔名和程式碼中的半形括號不改。
- 不手工加入 NBSP。不可斷空格只有在原始程式碼或連結本身存在時才保留；一般正文按本節的 ASCII 空格規則處理。

依據：香港大學出版社指南直接規定全形標點、直角引號、破折號和省略號；Markdown 的程式碼、URL 和行內程式碼仍遵守共享規則第 1 節的 byte-identical 約束。

中英文之間加空格是本專案的選擇：香港大學出版社指南沒有規定中英文間距，它的示例（「2010年7月8日」「24個月」）也不加空格；香港政府網頁（例如 DATA.GOV.HK 的「HTTP請求方法」）同樣不加空格；Microsoft zh-hk 說明頁（「您可以使用 Dataverse 做為資料存放區」）和 Apple 香港說明頁（「iPhone 使用手冊」）則在拉丁文字前後加空格。技術文件沿用加空格的寫法，不與共享規則衝突。

- https://hkupress.hku.hk/image/catalog/publish-with-the-press/HKUP_Chinese_Style_Guide.pdf
- https://www.w3.org/International/clreq/

### 5. 數字、日期與單位

- 技術文件中的數量、版本、連接埠、狀態碼、時間和百分比使用阿拉伯數字：`3 次`、`64 KB`、`2026 年 10 月 12 日`、`99.9%`。
- 數字和中文量詞之間留一個空格；百分號貼近數字；單位前留一個空格。`64-bit`、`HTTP/2`、`v1.4.3`、`10–20` 等協定、版本和範圍寫法保持一致。
- 小數使用半形句點；不要把技術值改寫成中文數字。指令、JSON、SQL、版本號和 API 參數仍按原文輸出。
- 「第 1 步」使用阿拉伯數字；標題和表格中的數字格式保持一致。

### 6. 術語表

本表覆蓋 the concept list 的概念；另外補上校準段落所需的數據庫。每個普通概念都要有中文寫法。中文概念詞沒有性別或屈折變化；固定名稱的大小寫、數值和單位按第 5 節處理。只有 SQL 關鍵字、JavaScript／TypeScript 術語或 API／產品固定名稱，才可以在「keep English?」寫「是」；`unverified` 只表示沒有找到足夠的香港直接用例，不表示可以把英文留在正文。

| English | local rendering | keep English? | status | source / note |
| --- | --- | --- | --- | --- |
| schema; schema DSL | schema → 模式；schema DSL → 模式 DSL | 否 | attested | OGCIO、EDB 均列 schema／模式；DSL 是固定縮寫。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| database | database → 數據庫 | 否 | attested | OGCIO 列 database → 數據庫。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| column | column → 欄位 | 否 | attested | 香港教育局資料庫 FAQ 使用「欄位」。https://cdrcloudsams.edb.gov.hk/%E5%B8%B8%E8%A6%8B%E5%95%8F%E9%A1%8C/ |
| table | table → 資料表 | 否 | attested | 教育局《資訊及通訊科技課程及評估指引（中四至中六）》（2021）寫「在最多三個資料表中操控當中的數據」「查詢多個資料表」（全文約 18 處）；Microsoft zh-hk 的 Access 說明頁標題也用「資料表」。教育局較舊的課程文件寫「表格」。不要寫成資料庫。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/curriculum-doc/ICT_C&A%20Guide_c_final.pdf 、https://support.microsoft.com/zh-hk/office/%E5%BB%BA%E7%AB%8B%E7%94%A2%E7%94%9F%E8%B3%87%E6%96%99%E8%A1%A8%E6%9F%A5%E8%A9%A2-96424f9e-82fd-411e-aca4-e21ad0a94f1b |
| temporary table | temporary table → 暫存資料表 | 否 | inferred | 由 temporary → 暫存和 table → 資料表組合；待香港開發者確認慣用詞。 |
| table engine | table engine → 資料表引擎 | 否 | unverified | 未找到直接香港用例；採普通技術中文，待母語者確認。 |
| column type; type definition | column type → 欄位類型；type definition → 類型定義 | 否 | inferred | 由欄位、類型和 Microsoft zh-TW 的型別文件用法推定。https://learn.microsoft.com/zh-tw/dotnet/csharp/language-reference/language-specification/expressions |
| nullable; optional; required | nullable → 可為 NULL；optional → 可選；required → 必填 | 否 | inferred | 由專案固定的 NULL 和 Microsoft 類型文件中的 optional／required 語義推定。 |
| default value | default value → 預設值 | 否 | attested | OGCIO 列 default → 預設值。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| modifier | modifier → 修飾符 | 否 | attested | Microsoft zh-TW 語言參考使用「修飾詞／修飾符」；本表統一用「修飾符」。https://learn.microsoft.com/zh-tw/dotnet/csharp/language-reference/keywords/ |
| materialized column; alias column | materialized column → 物化欄位；alias column → 別名欄位 | 否 | unverified | 未找到直接香港用例；採 ClickHouse 技術語境的普通中文，待母語者確認。 |
| array | array → 陣列 | 否 | attested | 教育局 ICT Glossary 列 array → 陣列。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf |
| tuple | tuple → 元組 | 否 | attested | Microsoft zh-TW 資料類型文件使用「元組」。https://learn.microsoft.com/zh-tw/dotnet/visual-basic/programming-guide/language-features/data-types/tuples |
| map | map → 對映 | 否 | unverified | 未找到足夠的香港技術用例；採普通繁體中文，待母語者確認。 |
| nested | nested → 嵌套 | 否 | attested | 教育局課程指引寫「嵌套循環」（nested loop）。台灣版 Microsoft 文件寫「巢狀」，本表不採用。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/curriculum-doc/ICT_C&A%20Guide_c_final.pdf |
| enum | enum → 列舉 | 否 | attested | Microsoft zh-TW 類型文件使用「列舉」。https://learn.microsoft.com/zh-tw/dotnet/csharp/language-reference/builtin-types/enum |
| literal | literal → 字面值 | 否 | unverified | 未找到足夠的香港技術用例；採普通教材寫法，待母語者確認。 |
| string | string → 字串 | 否 | attested | 教育局 ICT Glossary 列 string → 字串。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf |
| integer; 64-bit integer | integer → 整數；64-bit integer → 64 位元整數 | 否 | inferred | 由香港教育局的布爾／字串詞形和 Microsoft 數值類型文件的中文技術寫法推定。https://learn.microsoft.com/zh-tw/dotnet/csharp/language-reference/builtin-types/integral-numeric-types |
| floating point | floating point → 浮點數 | 否 | attested | Microsoft zh-TW OpenTelemetry 文件使用「浮點值」；本表按數值類型寫成「浮點數」。https://learn.microsoft.com/zh-tw/dotnet/core/diagnostics/observability-with-otel |
| boolean | boolean → 布爾值 | 否 | attested | 教育局詞彙表列 Boolean → 布爾，課程指引亦多處寫「布爾」；「布爾值」是同一詞在值語境的自然加詞。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf 、https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/curriculum-doc/ICT_C&A%20Guide_c_final.pdf |
| date; timestamp; timezone | date → 日期；timestamp → 時間戳記；timezone → 時區 | 否 | inferred | 由 Microsoft zh-TW 日期時間 API 的固定中文詞形推定；ClickHouse 專用細節仍待確認。https://learn.microsoft.com/zh-tw/dotnet/api/system.datetime |
| precision; scale | precision → 精確度；scale → 小數位數 | 否 | unverified | 未找到足夠的香港數值型別用例；採普通技術中文，待母語者確認。 |
| exact value; lossless | exact value → 精確值；lossless → 無損 | 否 | unverified | 未找到直接香港用例；採普通教材寫法，待母語者確認。 |
| JSON path | JSON path → JSON 路徑 | 否 | inferred | JSON 是固定名稱，path 按香港及繁體技術文件的普通寫法譯為「路徑」。 |
| generic type; type inference / inferred | generic type → 泛型；type inference → 類型推斷；inferred → 推斷出的 | 否 | attested | Microsoft zh-TW C# 文件使用「泛型」和「型別推斷」；本表將型別統一為「類型」。https://learn.microsoft.com/zh-tw/dotnet/csharp/language-reference/language-specification/expressions |
| typed; untyped | typed → 具類型；untyped → 無類型 | 否 | inferred | 由類型文件的中文句法推定；不是把 typed／untyped 留成英文。 |
| type assertion | type assertion → 類型斷言 | 否 | unverified | 未找到足夠的香港 TypeScript 用例；採普通技術中文，待母語者確認。 |
| runtime validation; validator | runtime validation → 執行階段驗證；validator → 驗證器 | 否 | inferred | 由 Microsoft 的 runtime → 執行階段及 validation → 驗證用法組合。https://learn.microsoft.com/zh-tw/aspnet/core/fundamentals/environments?view=aspnetcore-10.0 |
| encoder; decoder; decoding | encoder → 編碼器；decoder → 解碼器；decoding → 解碼 | 否 | inferred | 由香港 OGCIO 的 encryption／encoding 詞形及普通技術中文組合推定。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| query | query → 查詢 | 否 | attested | 香港教育局資料庫 FAQ 使用「查詢」；Microsoft zh-TW SQL 文件也使用「查詢」。https://cdrcloudsams.edb.gov.hk/%E5%B8%B8%E8%A6%8B%E5%95%8F%E9%A1%8C/ |
| query builder | query builder → 查詢建構器 | 否 | inferred | 由 query → 查詢和 builder → 建構器組合；保留功能含義，不留普通英文。 |
| builder | builder → 建構器 | 否 | unverified | 未找到直接香港用例；採普通技術中文，待母語者確認。 |
| chain / chainable | chain → 鏈式；chainable → 可鏈式 | 否 | unverified | 未找到足夠的香港 JavaScript 用例；採普通技術中文，待母語者確認。 |
| subquery; bare subquery | subquery → 子查詢；bare subquery → 獨立子查詢 | 否 | unverified | 未找到直接香港用例；按 SQL 語義給出具體寫法，待母語者確認。 |
| anonymous CTE | anonymous CTE → 匿名 CTE | 否 | inferred | CTE 是固定縮寫，anonymous 譯為「匿名」。 |
| join; left join; outer join; group by; order by; limit; insert; insert from select | join → JOIN；left join → LEFT JOIN；outer join → OUTER JOIN；group by → GROUP BY；order by → ORDER BY；limit → LIMIT；insert → INSERT；insert from select → INSERT FROM SELECT | 是 | inferred | SQL 關鍵字按共享規則第 3 節原樣保留；這不是把普通概念詞留成英文。 |
| null semantics | null semantics → NULL 語義 | 否 | inferred | NULL 是固定 SQL 名稱，semantics 按技術中文譯為「語義」。 |
| predicate; filter; where clause | predicate → 判斷條件；filter → 篩選條件；where clause → WHERE 子句 | 否 | inferred | 由 SQL 文件常見的條件／篩選／子句寫法推定；WHERE 只保留關鍵字。 |
| projection; selection (select list) | projection → 投影；selection (select list) → SELECT 清單 | 否 | unverified | 未找到足夠的香港查詢建構器用例；採普通資料庫中文，待母語者確認。 |
| alias; expression; window expression | alias → 別名；expression → 運算式；window expression → 視窗運算式 | 否 | inferred | 香港沒有單一標準：Microsoft zh-hk 的 Access 說明頁寫「運算式」（僅摘要）；OGCIO 詞彙表的 STIX／CybOX 條目寫「表達式」；教育局課程指引寫「算式」。本表暫保留「運算式」（全文一致），建議待香港讀者確認是否改「表達式」，見第 10 節。https://support.microsoft.com/zh-hk/office/%E5%BB%BA%E7%AB%8B%E7%94%A2%E7%94%9F%E8%B3%87%E6%96%99%E8%A1%A8%E6%9F%A5%E8%A9%A2-96424f9e-82fd-411e-aca4-e21ad0a94f1b 、https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/curriculum-doc/ICT_C&A%20Guide_c_final.pdf |
| aggregate | aggregate → 聚合 | 否 | inferred | 由香港 SQL 教學的聚合查詢語境推定；待母語者確認資料庫術語偏好。https://hktutorlink.hk/teaching-resources/dse/ict/dse-ict-advanced-sql-joins-and-aggregations |
| function | function → 函數 | 否 | attested | 教育局 ICT Glossary 及香港 SQL 教學使用「函數」。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf |
| table function | table function → 資料表函數 | 否 | unverified | 未找到足夠的香港 ClickHouse 直接用例；採具體技術中文，待母語者確認。 |
| scalar | scalar → 標量 | 否 | attested | 教育局詞彙表列 scalar type → 標量類形。台灣版 Microsoft 文件寫「純量」，本表不採用。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf |
| raw SQL; SQL template; SQL fragment | raw SQL → 原始 SQL；SQL template → SQL 模板；SQL fragment → SQL 片段 | 否 | inferred | SQL 是固定名稱；raw、template、fragment 分別按普通技術中文處理。模板一詞採 OGCIO 寫法。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| parameter | parameter → 參數 | 否 | attested | 教育局 ICT Glossary 列 parameter → 參數。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf |
| bound parameter | bound parameter → 繫結參數 | 否 | inferred | 由 Microsoft zh-TW 的繫結／binding 用法和 parameter → 參數組合。 |
| placeholder; query parameter | placeholder → 佔位符；query parameter → 查詢參數 | 否 | inferred | 由 query、parameter 的已定詞形和普通技術中文組合。 |
| compile / compiled SQL | compile → 編譯；compiled SQL → 已編譯 SQL | 否 | inferred | 由編譯器文件的中文固定用法推定；SQL 保留。 |
| thenable | thenable → thenable | 是 | attested | MDN zh-TW 直接保留 thenable；它是 JavaScript Promise 術語。https://developer.mozilla.org/zh-TW/docs/Web/JavaScript/Reference/Global_Objects/Promise/resolve |
| helper; namespace; factory | helper → 輔助工具；namespace → 命名空間；factory → 工廠函數 | 否 | inferred | 由 Microsoft C#／TypeScript 文件的命名空間、工廠和輔助語境推定。 |
| callback; iterator; variadic | callback → 回呼函數；iterator → 迭代器；variadic → 可變參數 | 否 | inferred | 由 MDN／Microsoft JavaScript API 的回呼、迭代器和參數寫法推定。 |
| write; batch; bulk insert | write → 寫入；batch → 批次；bulk insert → 批次插入 | 否 | inferred | 由 OGCIO 的 write／batch 技術語境和普通資料庫中文組合推定。 |
| stream / streaming | stream → 串流；streaming → 串流處理 | 否 | attested | OGCIO 列 stream → 串流。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| row; result set | row → 資料列；result set → 結果集 | 否 | inferred | 由資料庫中文的列／結果集固定搭配推定；待香港開發者確認。 |
| replacement (ReplacingMergeTree); versioning; deletion flag | replacement → 替換（ReplacingMergeTree）；versioning → 版本管理；deletion flag → 刪除標記 | 否 | unverified | 未找到足夠的香港 ClickHouse 直接用例；保留類別名稱，其他概念具體翻譯。 |
| client | client → 客戶端 | 否 | attested | 教育局課程指引寫「客戶端界面」「客戶端和伺服器端程式」；香港資訊安全網頁面直接使用「客戶端」；OGCIO 另列 client → 客戶。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/curriculum-doc/ICT_C&A%20Guide_c_final.pdf 、https://www.infosec.gov.hk/tc/best-practices/business/securing-web-application |
| connection; server; replica | connection → 連線；server → 伺服器；replica → 副本 | 否 | inferred | OGCIO 直接列 server → 伺服器；香港技術頁面普遍用連線；replica 採普通資料庫中文。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| request; response; transport; HTTP request | request → 請求；response → 回應；transport → 傳輸；HTTP request → HTTP 請求 | 否 | attested | DATA.GOV.HK《應用程式介面規範》寫「HTTP請求方法」「如果請求含有語法錯誤」；教育局課程指引寫「包括請求和回應的概念」；數字政策辦公室《社交媒體保安實務指引》寫「跨網站請求偽造」，《流動保安實務指引》寫「客戶端的請求」（兩份均僅摘要）。舊稿寫「要求」，但香港資訊安全網的「要求」是 require／requirement 的意思（僅摘要），查無香港一手來源用「要求」指 request 名詞。https://data.gov.hk/tc/help/api-spec 、https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/curriculum-doc/ICT_C&A%20Guide_c_final.pdf 、https://www.govcert.gov.hk/doc/PG_for_Social_Media_Security-v1.1_TC.pdf 、https://www.govcert.gov.hk/doc/ispg-sm03-v2.0_tc.pdf |
| return; returned value | returns → 返回；return type → 返回類型 | 否 | attested | DATA.GOV.HK《應用程式介面規範》以「返回」表示 API 回傳結果（「狀態碼 200 返回」「結果會返回」）。台灣版寫「傳回」，本表不採用。https://data.gov.hk/tc/help/api-spec |
| timeout | timeout → 逾時 | 否 | attested | 數字政策辦公室（前 OGCIO）《英漢資訊科技辭彙》列 time-out → 逾時；Microsoft zh-TW SQL 文件亦用「逾時」。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| settings; configuration / config; option | settings → 設定；configuration → 配置；config → 配置；option → 選項 | 否 | attested | OGCIO 詞彙表列 configuration → 配置、system configuration → 系統配置（只有 configuration management 寫「組態管理」）；《保護關鍵基礎設施電腦系統條例》實務守則寫「配置管理」「保安配置」；教育局課程指引寫「動態主機配置協定」「瀏覽器設定」；數字政策辦公室 GovCERT 的指引也寫「配置」。反例：香港資訊安全網《網上應用系統保安》頁面同時寫「組態」和「設置」，所以香港用法並不一致；本表不改詞，仍用「配置」。台灣版 Microsoft 文件寫「組態」。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://www.occics.gov.hk/filemanager/tc/content_19/CoP_zh_v1.0.pdf 、https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/curriculum-doc/ICT_C&A%20Guide_c_final.pdf 、https://www.infosec.gov.hk/tc/best-practices/business/securing-web-application |
| session; temporary-table workflow | session → 工作階段；temporary-table workflow → 暫存資料表工作流程 | 否 | attested | 《保護關鍵基礎設施電腦系統條例》實務守則寫「工作階段管理」；「智方便」用戶使用條款寫「登入工作階段」（僅摘要，未能開啟全文）。數字政策辦公室指引與香港資訊安全網則寫「對話管理」，列為候選，見第 10 節。https://www.occics.gov.hk/filemanager/tc/content_19/CoP_zh_v1.0.pdf 、https://core.iamsmart.gov.hk/api/v1/uc/m/getProtocol/info?source=app&lang=zh-HK |
| transaction; concurrency | transaction → 交易；concurrency → 並行 | 否 | unverified | 香港來源的 transaction 詞形不適合資料庫語境；採普通開發者寫法，待母語者確認。 |
| lifecycle; cleanup | lifecycle → 生命週期；cleanup → 清理 | 否 | attested | OGCIO 詞彙表寫「系統生命周期」「系統發展周期」，教育局詞彙表寫「周期」；香港民辦教育機構的課程大綱（僅摘要）與 Microsoft zh-TW 另有「週期」寫法。兩種並存，本表保留「生命週期」，見第 10 節。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf |
| runtime method; execute; fetch; abort / cancel | runtime method → 執行階段方法；execute → 執行；fetch → 擷取；abort → 中止；cancel → 取消 | 否 | inferred | runtime／cancel 等詞形見 Microsoft／MDN API 文件；其餘按普通技術中文組合。https://developer.mozilla.org/zh-TW/docs/Web/API/XMLHttpRequest |
| observability; tracing | observability → 可觀測性；tracing → 追蹤 | 否 | attested | Microsoft zh-TW OpenTelemetry 文件使用「可觀測性」和「追蹤」。https://learn.microsoft.com/zh-tw/dotnet/core/diagnostics/observability-with-otel |
| span | span → span | 是 | attested | 香港：未找到來源。台灣：Microsoft zh-TW 文件同時使用「跨度」和 API 術語 Span；Google Cloud 追蹤說明用「範圍」「時距」；iThome 文章保留 Span。本指南在 API 語境保留 span。https://learn.microsoft.com/zh-tw/azure/foundry/observability/concepts/trace-agent-concept |
| logging; logger; metrics | logging → 記錄；logger → 記錄器；metrics → 指標 | 否 | inferred | 台灣：Microsoft zh-TW OpenTelemetry 文件使用記錄／指標；記錄器由 logging → 記錄組合。https://learn.microsoft.com/zh-tw/dotnet/core/diagnostics/observability-with-otel |
| instrumentation | instrumentation → 插裝 | 否 | unverified | 香港：未找到用例。候選寫法按地區：台灣——Microsoft 寫「檢測」，AWS、Google 寫「檢測」「插樁」，iThome 寫「儀器化」「埋點」；其他地區——zh.wikipedia 和 topic.alibabacloud.com 的測試詞彙表寫「插裝」。目前選擇維持「插裝」，見第 10 節。 |
| exporter | exporter → 匯出器 | 否 | unverified | 台灣：Microsoft Learn zh-TW OpenTelemetry 文件使用「匯出器」。香港：未找到來源。https://learn.microsoft.com/zh-tw/dotnet/core/diagnostics/observability-with-otel |
| Collector (OpenTelemetry Collector) | Collector → 收集器 | 否 | unverified | 台灣：只見社群文章寫「收集器」；Microsoft 和 Google 的文件保留英文 Collector。香港：未找到來源。 |
| SDK; tracer provider; W3C propagator | SDK → SDK；tracer provider → tracer provider；W3C propagator → W3C propagator | 是 | unverified | 香港、台灣和其他地區都未找到中文詞形；SDK 是縮寫，tracer provider 和 propagator 是 OpenTelemetry API 名稱，保留英文。 |
| active (span) | active → 作用中 | 否 | unverified | 台灣：只見台灣來源寫「作用中」。香港：未找到來源。 |
| trace context | trace context → 追蹤脈絡 | 否 | unverified | 香港和其他地區都未找到穩定詞形。信心低；建議改為在英文專名中保留「W3C Trace Context」。reference.md 目前寫「W3C 追蹤脈絡」，這次未改動，見第 10 節。 |
| plugin | plugin → 插件 | 否 | attested | 香港：教育局《ICT 詞彙表》（2023）列 plug-in → 插件；香港警務處網頁（「瀏覽器與插件」）和公務員事務局招聘申請常見問題（「用戶代理切換插件」）同樣寫「插件」。少數寫法：數字政策辦公室 GovCERT《社交媒體保安實務指引》（僅摘要）寫「外掛程式」，見第 10 節。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf 、https://www.police.gov.hk/offbeat120/cybersecurity/73_what-is-dark-web.html 、https://www.csb.gov.hk/tc_chi/recruit/application/files/common/FAQ_C.pdf 、https://www.govcert.gov.hk/doc/PG_for_Social_Media_Security-v1.1_TC.pdf |
| hook | hook → hook | 是 | unverified | 香港：未找到來源。先前列為依據的 React 文件是簡體中文頁面，不是香港來源。台灣：Microsoft 頁面寫「掛鉤」，Anthropic 的 zh-TW Claude Code 文件則保留 hook。hook 是 JavaScript／React 術語，沿用英文，待香港母語者確認。https://zh-hans.react.dev/learn/state-a-components-memory |
| entry point | entry point → 進入點；main entry → 主進入點 | 否 | attested | 香港：數字政策辦公室《英漢資訊科技辭彙》（2025 年 11 月版）列 entry point → 進入點。信心中低：只有這一份詞彙表，且是從 PDF 抽取文字得來，未見其他香港技術文件用例。原稿的「入口點」「主入口」已改掉。 https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| event | event → 事件 | 否 | attested | Microsoft／MDN API 文件普遍使用「事件」。https://developer.mozilla.org/zh-TW/docs/Web/API/XMLHttpRequest |
| error; exception; throws / rejects; error model; validation error; stack; retry | error → 錯誤；exception → 例外狀況；throws → 擲回；rejects → 拒絕；error model → 錯誤模型；validation error → 驗證錯誤；stack → 堆疊；retry → 重試 | 否 | inferred | 香港政府頁面直接使用錯誤；EDB 列 stack → 堆疊；其他詞形由 Microsoft／Promise 文件組合推定。throws → 擲回沒有香港一手用例，候選「拋出」，見第 10 節。https://www.gov.hk/tc/residents/taxes/etax/faq/faq_about_etax_technical.htm |
| statement; SQL statement | statement → 語句；SQL statement → SQL 語句 | 否 | attested | 教育局詞彙表與課程指引寫「賦值語句」「輸入語句」「SQL語句」。台灣版 Microsoft 文件寫「陳述式」，本表不採用。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf 、https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/curriculum-doc/ICT_C&A%20Guide_c_final.pdf |
| process (operating system); compatible | process → 進程；compatible → 兼容；compatibility → 兼容性 | 否 | attested | 香港：數字政策辦公室（前 OGCIO）詞彙表列 process → 進程、compatibility → 兼容性。台灣版 Microsoft 文件寫「處理程序」「相容」，本表不採用。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| environment variable | environment variable → 環境變量 | 否 | unverified | 由教育局詞彙表的 variable → 變量（香港）推定。Microsoft zh-hk 支援頁的標題寫「環境變數」（香港地區頁面，但詞形與台灣相同）。沒有找到直接寫「環境變量」的香港來源。 |
| edge | edge → 邊緣 | 否 | attested | 香港：數字政策辦公室詞彙表列 edge computing → 邊緣運算；本文寫「邊緣 worker」。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| character; character set | character → 字符；character set → 字符集 | 否 | attested | OGCIO 與教育局詞彙表列 character → 字符；官方名稱「香港增補字符集」（HKSCS）亦用「字符」。台灣版寫「字元」，本表不採用。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf |
| security; SQL injection; untrusted input; identifier quoting; escaping | security → 保安；SQL injection → SQL 注入；untrusted input → 不受信任的輸入；identifier quoting → 識別碼加引號；escaping → 逸出 | 否 | inferred | OGCIO／香港資訊安全網使用保安和不受信任；SQL 注入、加引號、逸出按安全 API 的普通技術中文推定。https://www.infosec.gov.hk/tc/best-practices/business/securing-web-application |
| credentials; password; allow-list; permission | credentials → 登入憑證；password → 密碼；allow-list → 允許清單；permission → 權限 | 否 | attested | 香港資訊安全網使用登入憑證、密碼、權限；允許清單採安全文件的包容性中文寫法。https://www.infosec.gov.hk/tc/knowledge-centre/phishing |
| installation; install | installation → 安裝；install → 安裝 | 否 | attested | OGCIO 列 install → 安裝。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| peer dependency | peer dependency → peer dependency | 是 | inferred | npm 專有詞（package.json 的 peerDependencies 欄位）；未找到香港慣用中文詞形，而「同儕相依套件」是生硬的直譯，所以保留英文。台灣：realnewbie.com 的文章寫「對等依賴」，但多數部落格保留英文 peer dependency；不改詞。待母語者確認。 |
| version; pin a version; release | version → 版本；pin a version → 鎖定版本；release → 發行 | 否 | inferred | OGCIO 直接列 version／release 的香港詞形；pin a version 按套件管理語境組合。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| breaking change; before 1.0 / pre-release | breaking change → 破壞性變更；before 1.0 → 1.0 前；pre-release → 發行前版本 | 否 | attested | Microsoft zh-TW 預覽 API 文件使用「破壞性更改」和「發行前版本」；本表將更改統一為香港常用的變更。https://learn.microsoft.com/zh-tw/dotnet/fundamentals/apicompat/preview-apis |
| migration (schema migration); production; example; runnable | migration → 遷移（schema migration → 模式遷移）；production → 生產環境；example → 範例；runnable → 可執行 | 否 | attested | OGCIO 列 migration → 遷移；Microsoft zh-TW 使用生產環境、範例和可執行的同類寫法。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| reference; documentation; page; section | reference → 參考資料；documentation → 說明文件；page → 頁面；section → 一節 | 否 | inferred | 由香港政府／Microsoft 文件的頁面、文件和參考資料寫法推定；section 在正文按「一節」處理。 |
| caveat; note | caveat → 注意事項；note → 備註 | 否 | inferred | 由 Microsoft 文件的注意／備註欄位和香港書面技術文風推定。 |
| behavior; semantics; contract; mode; public API | behavior → 行為；semantics → 語義；contract → 契約；mode → 模式；public API → 公開 API | 否 | inferred | Microsoft zh-TW 使用行為、模式和 API；契約／語義按程式設計語境推定。https://learn.microsoft.com/zh-tw/aspnet/core/fundamentals/environments?view=aspnetcore-10.0 |
| stable; internal; deprecated; shape; surface; layer | stable → 穩定；internal → 內部；deprecated → 已棄用；shape → 結構；surface → 對外界面；layer → 層 | 否 | inferred | 由 Microsoft API 相容性文件的穩定／預覽／淘汰語境和 TypeScript 的 shape／API surface 用法推定。https://learn.microsoft.com/zh-tw/dotnet/fundamentals/apicompat/preview-apis |
| keeps X easy; without hiding; make everyday access easier to structure; is rejected; is not validated; is decoded conservatively; carries; exposes; follows; stays; leaves; pass; wrap; compose; accepts; returns; throws | keeps X easy → 讓 X 保持簡單；without hiding → 又不掩蓋；make everyday access easier to structure → 讓日常的 ClickHouse 存取更有條理；is rejected → 會被拒絕；is not validated → 不會經過驗證；is decoded conservatively → 會以保守方式解碼；carries → 帶有；exposes → 公開；follows → 遵循；stays → 維持；leaves → 保留；pass → 傳入；wrap → 包裝；compose → 組合；accepts → 接受；returns → 返回；throws → 擲回 | 否 | inferred | 這些是句法和動詞的本地化寫法，不按英文被動式或名詞鏈逐字翻譯；採第 2、3 節的主動句規則。 |
| ClickHouse; ck-orm; TypeScript; JavaScript; SQL; JSON; Decimal; DDL; NULL; CTE; HTTP; UTC; OpenTelemetry; Bun; npm; pnpm; Yarn; Node.js; Drizzle; GitHub; llms.txt; MIT; API names; identifiers; keywords | 原樣保留：ClickHouse；ck-orm；TypeScript；JavaScript；SQL；JSON；Decimal；DDL；NULL；CTE；HTTP；UTC；OpenTelemetry；Bun；npm；pnpm；Yarn；Node.js；Drizzle；GitHub；llms.txt；MIT；API 名稱；識別碼；關鍵字 | 是 | inferred | 共享規則第 3 節的專案固定規則；這些是產品名、技術專名、API 名稱、識別碼或關鍵字，不是普通概念詞。 |

### 7. 不要混入台灣或中國內地詞形

以下只列有來源顯示「香港形式」與台灣或中國內地形式不同的項目。兩個獨立來源都支持差異時用 `ERR`；只有一方可核對時，規則只用 `WARN`。`資料` 和 `數據` 在香港來源中都有實際用例，所以不作全域污染規則，應按上下文判斷。

| 不要寫 | 香港正文優先寫成 | 證據 |
| --- | --- | --- |
| 資料庫 | 數據庫 | 香港 OGCIO、EDB 均用「數據庫」；Microsoft zh-TW 用「資料庫」。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf 、https://learn.microsoft.com/zh-tw/azure/sap/workloads/businessobjects-deployment-guide-windows |
| 介面 | 界面 | 香港 OGCIO／EDB 用「界面」；Microsoft zh-TW 用「介面」。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf 、https://learn.microsoft.com/zh-tw/windows/win32/network-interfaces |
| 變數 | 變量 | 香港 EDB 用「變量」；Microsoft zh-TW 用「變數」。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf 、https://learn.microsoft.com/zh-tw/csharp/language-reference/keywords/variables |
| 演算法 | 算法 | 香港 OGCIO／EDB 用「算法」；Microsoft zh-TW 用「演算法」。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf 、https://learn.microsoft.com/zh-tw/azure/architecture/ai-ml/ai-overview |
| 布林、布林值 | 布爾、布爾值 | 香港 EDB 用「布爾」；Microsoft zh-TW 用「布林值」。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf 、https://learn.microsoft.com/zh-tw/dotnet/csharp/language-reference/builtin-types/bool |
| 佇列 | 隊列 | 香港 EDB 用「隊列」；Microsoft zh-TW 用「佇列」。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf 、https://learn.microsoft.com/zh-tw/azure/storage/queues/storage-queues-introduction |
| 效能 | 性能 | 香港 OGCIO 用「性能」；Microsoft zh-TW 用「效能」。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://learn.microsoft.com/zh-tw/sql/relational-databases/performance/monitor-and-tune-for-performance |
| 範本、樣板 | 模板 | 香港 OGCIO 用「模板」；Microsoft zh-TW 技術頁面使用「範本」。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://learn.microsoft.com/zh-tw/dotnet/core/tools/templates |
| 安全性 | 保安 | 香港 OGCIO／資訊安全網使用「保安」；Microsoft zh-TW 安全頁面使用「安全性」。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://www.infosec.gov.hk/tc/best-practices/business/securing-web-application 、https://learn.microsoft.com/zh-tw/security/ |
| 軟體 | 軟件 | 香港 OGCIO／資訊安全網使用「軟件」；Microsoft zh-TW 使用「軟體」。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://www.infosec.gov.hk/tc/knowledge-centre/phishing 、https://learn.microsoft.com/zh-tw/windows/win32/appuistart/usability-in-software-design |
| 硬體 | 硬件 | OGCIO 使用「硬件」；Microsoft zh-TW 使用「硬體」；只有一個直接香港詞彙表支持香港形式，因此規則只作 `WARN`。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://learn.microsoft.com/zh-tw/windows/win32/appuistart/usability-in-software-design |
| 網路 | 網絡 | 香港 OGCIO／資訊安全網使用「網絡」；Microsoft zh-TW 使用「網路」。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://www.infosec.gov.hk/tc/knowledge-centre/phishing 、https://learn.microsoft.com/zh-tw/windows/win32/network-interfaces |
| 用户、软件 | 用戶、軟件 | 香港 OGCIO 使用「用戶、軟件」；Microsoft zh-CN 使用簡體「用户、软件」。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access |
| 函式 | 函數 | 香港 EDB／本地 SQL 教學使用「函數」；Microsoft zh-TW 使用「函式」；引用台灣文件時可能合法，因此只作 `WARN`。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf 、https://hktutorlink.hk/teaching-resources/dse/ict/dse-ict-advanced-sql-joins-and-aggregations 、https://learn.microsoft.com/zh-tw/cpp/c-language/functions-c |
| 要求（作 request 名詞） | 請求 | 香港 DATA.GOV.HK、教育局、數字政策辦公室的文件都用「請求」；Microsoft zh-TW 用「要求」。台灣一側沒有逐項抓取核對，所以只作 `WARN`。https://data.gov.hk/tc/help/api-spec 、https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/curriculum-doc/ICT_C&A%20Guide_c_final.pdf |
| 傳回、回傳 | 返回 | DATA.GOV.HK 用「返回」；台灣一側未逐項核對，只作 `WARN`。https://data.gov.hk/tc/help/api-spec |
| 純量 | 標量 | 教育局詞彙表列 scalar type → 標量類形；只作 `WARN`。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf |
| 巢狀 | 嵌套 | 教育局課程指引用「嵌套循環」；只作 `WARN`。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/curriculum-doc/ICT_C&A%20Guide_c_final.pdf |
| 組態 | 配置 | OGCIO 詞彙表與《保護關鍵基礎設施電腦系統條例》實務守則用「配置」；只有 configuration management 見「組態管理」，所以只作 `WARN`。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf 、https://www.occics.gov.hk/filemanager/tc/content_19/CoP_zh_v1.0.pdf |
| 陳述式 | 語句 | 教育局詞彙表與課程指引用「語句」；只作 `WARN`。https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf |
| 處理程序（指 process） | 進程 | OGCIO 詞彙表列 process → 進程；「處理程序」另有 procedure 的意思，不能全域禁用，只作 `WARN`。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| 字元 | 字符 | OGCIO、教育局詞彙表與「香港增補字符集」官方名稱都用「字符」；只作 `WARN`。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |
| 相容（compatible） | 兼容 | OGCIO 詞彙表列 compatibility → 兼容性；只作 `WARN`。https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf |

### 8. 校準段落

使用 `ck-orm` 時，請留意以下界線：

- `ckType.*` 定義模式的欄位類型。
- `ckTable(...)` 定義資料表模式。
- `ck.*` 是查詢輔助工具命名空間。
- `fn.*` 是 SQL 函數輔助工具命名空間。
- 模式描述資料表和欄位，不描述數據庫名稱。
- 數據庫連線放在 `clickhouseClient(...)` 上。
- 建構器查詢是預設路徑；遇到 ClickHouse 專用運算式時，可使用原始 SQL。
- `runInSession()` 是 ClickHouse 的工作階段輔助工具，不是交易。
- `plugins` 會觀察 ck-orm 送出的每個請求；追蹤是需要自行啟用的 `ck-orm/otel` 插件，不包含在主進入點內
- `leftJoin()` 預設使用 SQL 風格的 NULL 語義。
- 數值較大或風險較高的結果會以保守方式解碼，而不是靜默強制轉換。

ck-orm 沒有必要的依賴項。OpenTelemetry 追蹤放在獨立的進入點 `ck-orm/otel`，`@opentelemetry/api` 是只供它使用的可選 peer dependency。主進入點 `ck-orm` 絕不會匯入 OpenTelemetry，所以不做追蹤的應用程式、瀏覽器打包檔案和邊緣 worker 不會安裝，也不會打包 OpenTelemetry 的任何內容。要追蹤查詢，請按 [OpenTelemetry 插件](#opentelemetry-plugin) 的設定步驟操作。

### 9. 參考來源與證據邊界

- IANA language tags：https://www.iana.org/assignments/language-tags
- 數字政策辦公室（前 OGCIO）English-Chinese Glossary of IT Terms（2025 年 11 月版；舊的 ogcio.gov.hk 網址會以 301 轉址到此）：https://www.digitalpolicy.gov.hk/en/our_work/digital_infrastructure/methodology/glossary/doc/IT_Glossary.pdf
- 教育局 ICT Glossary：https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/resources/computer-edu/ICT_glossary.pdf
- 香港教育局資料庫 FAQ：https://cdrcloudsams.edb.gov.hk/%E5%B8%B8%E8%A6%8B%E5%95%8F%E9%A1%8C/
- 教育局書面語與粵語課程框架：https://www.edb.gov.hk/attachment/sc/curriculum-development/major-level-of-edu/special-educational-needs/cl-curriculum-second-lang-alf/Four%20strands%20of%20the%20ALF/CL_Alf_Framework_Introduction_final_20180824.pdf
- 香港大學出版社中文寫作指南：https://hkupress.hku.hk/image/catalog/publish-with-the-press/HKUP_Chinese_Style_Guide.pdf
- 香港資訊安全網：https://www.infosec.gov.hk/tc/knowledge-centre/phishing
- 香港資訊安全網網上應用系統保安：https://www.infosec.gov.hk/tc/best-practices/business/securing-web-application
- 香港警務處「什麼是暗網」（瀏覽器與插件）：https://www.police.gov.hk/offbeat120/cybersecurity/73_what-is-dark-web.html
- 公務員事務局招聘申請常見問題（用戶代理切換插件）：https://www.csb.gov.hk/tc_chi/recruit/application/files/common/FAQ_C.pdf
- MDN zh-TW XMLHttpRequest：https://developer.mozilla.org/zh-TW/docs/Web/API/XMLHttpRequest
- MDN zh-TW HTTP session：https://developer.mozilla.org/zh-TW/docs/Web/HTTP/Guides/Session
- MDN zh-TW Promise.resolve：https://developer.mozilla.org/zh-TW/docs/Web/JavaScript/Reference/Global_Objects/Promise/resolve
- Microsoft zh-TW 查詢逾時：https://learn.microsoft.com/zh-tw/troubleshoot/sql/database-engine/performance/troubleshoot-query-timeouts
- Microsoft zh-TW session：https://learn.microsoft.com/zh-tw/azure/service-bus-messaging/message-sessions
- Microsoft zh-TW 設定與生產環境：https://learn.microsoft.com/zh-tw/aspnet/core/fundamentals/configuration/?view=aspnetcore-10.0
- Microsoft zh-TW OpenTelemetry 可觀測性：https://learn.microsoft.com/zh-tw/dotnet/core/diagnostics/observability-with-otel
- Microsoft zh-TW API 相容性與預覽版本：https://learn.microsoft.com/zh-tw/dotnet/fundamentals/apicompat/preview-apis
- React 簡體中文文件的 hook 用法（簡體中文頁面，不是香港來源）：https://zh-hans.react.dev/learn/state-a-components-memory
- 教育局《資訊及通訊科技課程及評估指引（中四至中六）》（2021，中文）：https://www.edb.gov.hk/attachment/tc/curriculum-development/kla/technology-edu/curriculum-doc/ICT_C&A%20Guide_c_final.pdf
- DATA.GOV.HK 應用程式介面規範（中文）：https://data.gov.hk/tc/help/api-spec
- 保護關鍵基礎設施電腦系統安全專員辦事處《實務守則》：https://www.occics.gov.hk/filemanager/tc/content_19/CoP_zh_v1.0.pdf
- 數字政策辦公室《社交媒體保安實務指引》（僅摘要）：https://www.govcert.gov.hk/doc/PG_for_Social_Media_Security-v1.1_TC.pdf
- 數字政策辦公室《流動保安實務指引》（ISPG-SM03，僅摘要）：https://www.govcert.gov.hk/doc/ispg-sm03-v2.0_tc.pdf
- 「智方便」用戶使用條款（僅摘要）：https://core.iamsmart.gov.hk/api/v1/uc/m/getProtocol/info?source=app&lang=zh-HK
- 香港銀行同業結算公司「轉數快」常見問題（「綁定」，僅摘要）：https://fps.hkicl.com.hk/chi/fps/consumers/frequently_asked_questions.php
- Microsoft zh-hk 說明頁（Access，產生資料表查詢，僅摘要）：https://support.microsoft.com/zh-hk/office/%E5%BB%BA%E7%AB%8B%E7%94%A2%E7%94%9F%E8%B3%87%E6%96%99%E8%A1%A8%E6%9F%A5%E8%A9%A2-96424f9e-82fd-411e-aca4-e21ad0a94f1b
- Microsoft zh-hk Learn 頁（Dataverse 資料庫）：https://learn.microsoft.com/zh-hk/power-platform/admin/create-database
- Apple 香港 iPhone 使用手冊（僅摘要）：https://support.apple.com/zh-hk/guide/iphone/iph60ba71065/ios
- Chromium zh-HK 字串表（Google Chrome，僅摘要）：https://chromium.googlesource.com/chromium/src/+/64fa90e7e17e6511a5b75ef44573555f26e1d24c/chrome/app/resources/google_chrome_strings_zh-HK.xtb
- Unicode CLDR zh_Hant_HK（已檢視其非繼承條目；只含地區、貨幣、時區名稱等，沒有 Copy／Theme 這類界面詞）：https://raw.githubusercontent.com/unicode-org/cldr/main/common/main/zh_Hant_HK.xml

Microsoft Language Portal 的 zh-HK 詞條仍無法直接檢索。Microsoft Learn 與 Support 確有 zh-hk 頁面，但數量有限，用詞大多與 zh-TW 相同（資料表、使用者、運算式、安全性），個別 zh-hk 網址其實顯示 zh-tw 機器翻譯內容，所以只把 Microsoft zh-hk 當作旁證，不用它推翻政府與教育局的一手詞形。本表只把來源實際顯示的詞形當成證據；由來源組合推定的複合詞和未找到直接用例的詞，已分別標成 `inferred` 和 `unverified`。

### 10. 待確認項

以下項目查不到足夠的香港一手用例，或香港來源之間互相矛盾，目前保留原寫法，需要香港開發者拍板：

- `throws` 目前寫「擲回」（台灣版 Microsoft 詞形），候選「拋出」；查無香港一手用例。
- `runtime`／`compile time` 目前寫「執行階段」「編譯時期」（台灣版 Microsoft 詞形，Microsoft zh-hk 亦同），候選「運行時」「編譯時」（教育局課程指引有「運行錯誤」「運行時」），也可以考慮「執行環境」（指 Node／Bun 這類 runtime）。
- `expression` 目前寫「運算式」，候選「表達式」（OGCIO 詞彙表；全文已有「正則表達式」）；教育局另用「算式」。
- `session` 目前寫「工作階段」，候選「對話」（數字政策辦公室指引用「對話管理」）；「會話」查無香港一手用例。
- 「繫結」（bind）候選「綁定」；「綁定」見於香港銀行同業結算公司「轉數快」常見問題，但那是日常用法，不是程式設計語境。
- `call` 目前寫「呼叫」，DATA.GOV.HK 也寫「成功的呼叫」，OGCIO 與教育局詞彙表則寫「調用」，兩者並存，暫不統一。
- `row` 目前寫「資料列」（台灣版 Microsoft 詞形），教育局用「記錄」指 record；候選「記錄」「行」。
- 「對映」（map）、「列舉」（enum）、「回呼」（callback）、「聯集」（union）、「字面值」（literal）、「契約」（contract）、「萬用字元」（wildcard）、「識別碼」（identifier；教育局詞彙表寫「識別符」）：沒有香港一手用例，沿用台灣版常見詞形。
- 「生命週期」與 OGCIO／教育局的「生命周期」並存；「允許清單」與 OGCIO 的「白名單」；「建置」（UI 表用於 build）；UI 表的「AI 代理程式」。
- `instrumentation` 目前寫「插裝」，香港查無用例；候選「檢測」「儀器化」「埋點」「插樁」（地區標註見第 6 節）。
- `plugin` 目前寫「插件」（教育局詞彙表、警務處、公務員事務局均用）；數字政策辦公室 GovCERT 一份指引寫「外掛程式」，屬少數寫法，待確認是否需要統一。
- 「資料」與「數據」：教育局課程指引同樣混用（資料表、數據庫、數據），全文暫按「資料表／資料列／資料」和「數據庫／數據類型」處理。
- `transaction` 是否使用「交易」；`table engine`、`peer dependency`、`同儕相依套件`、`識別碼加引號` 等較少見的 ClickHouse／npm 詞語。
- 發布前由香港母語開發者完整通讀，特別檢查第 8 節的兩段是否自然而沒有混入口語粵字、台灣詞形或普通英文概念詞。



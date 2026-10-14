## zh-Hant-TW - 繁體中文（台灣）

### 1. Variant and reader

本節適用於臺灣軟體開發者閱讀的繁體中文：語言標籤明確使用 `zh-Hant-TW`，也就是以繁體字書寫的臺灣中文。Unicode CLDR 的 locale 模型會分開處理語言、文字系統和地區；本專案因此固定指定 `Hant` 與 `TW`，不能把 `zh-Hans` 的簡體字直接轉成繁體字就當成臺灣用語。[Unicode CLDR](https://unicode.org/reports/tr35/tr35-info.html)、[Unicode Language and Script Data](https://www.unicode.org/reports/tr35/)

讀者假設熟悉 TypeScript、SQL、HTTP 和一般資料庫術語，不需要每個 API 名稱都翻譯。臺灣開發者文件常保留 `schema`、`props`、`JSON` 等技術詞；MDN zh-TW、Microsoft Learn zh-TW 和 Prisma Taiwan 可作為查詞樣本，但單一網站不能代表所有臺灣開發者的固定用語。[MDN zh-TW Fetch](https://developer.mozilla.org/zh-TW/docs/Web/API/Fetch_API/Using_Fetch)、[MDN zh-TW JavaScript 資料結構](https://developer.mozilla.org/zh-TW/docs/Web/JavaScript/Guide/Data_structures)、[Prisma Taiwan](https://prisma.tw/docs/orm/prisma-schema/overview/generators)

### 2. Address, register and voice

使用同儕之間的半正式、直接語氣。能省略主詞就省略；需要明確指向讀者時，普通說明使用「你」。Microsoft Traditional Chinese Style Guide 明確建議 UI 的單數 you 使用「您」，而 MDN zh-TW 的開發者說明可見「你可以」；本專案把「你／省略主詞」定為普通開發者說明的專案寫法，不把「您」判成語法錯誤。[Microsoft Traditional Chinese Style Guide（鏡像）](https://www.pendeba.org/sites/pendeba.future.org/files/zho-twn-styleguide.pdf)、[MDN zh-TW Fetch](https://developer.mozilla.org/zh-TW/docs/Web/API/Fetch_API/Using_Fetch)

句子以一個主要動作為主，長句拆開；命令句直接用「請 + 動詞」，不要堆疊「務必」「進行」「加以」。英文的 “you can …” 多半寫成「你可以…」或直接用「可以…」；英文的 “must …” 寫成「必須…」或「請…」，依規範強度選擇。開頭直接說明動作或結果，結尾用句號；不要用「綜上所述」「特此說明」收尾。Microsoft 指南要求清楚、簡潔、接近日常對話，也提醒避免逐字翻譯和不必要的被動句。[Microsoft Traditional Chinese Style Guide（鏡像）](https://www.pendeba.org/sites/pendeba.future.org/files/zho-twn-styleguide.pdf)

UI 字串維持短而可掃讀：導覽項用名詞（例如「範例」「語言」），按鈕用動詞（例如「複製」「關閉導覽」）；`{mode}`、`{next}`、`{folder}` 等預留位置原樣保留。前半可由 Microsoft 的 UI 原則支持，後半是本專案寫法，不宣稱為臺灣通用慣例。[Microsoft Traditional Chinese Style Guide（鏡像）](https://www.pendeba.org/sites/pendeba.future.org/files/zho-twn-styleguide.pdf)

四組校準例句如下；「不自然」是官方腔、直譯腔或不自然的寫法。

| 英文原句 | 不自然 | 建議 |
|---|---|---|
| Use `ck-orm` with these boundaries in mind: | 使用 `ck-orm` 時，請牢記以下邊界： | 使用 `ck-orm` 時，記得把下面這些界線分清楚： |
| The main `ck-orm` entry never imports OpenTelemetry, so an application, browser bundle or edge worker that does not trace installs and bundles none of it. | `ck-orm` 的主要進入點絕不會匯入 OpenTelemetry，因此未使用追蹤功能的應用程式、瀏覽器打包檔案或邊緣 worker，將不會安裝亦不會打包其任何內容。 | `ck-orm` 的主進入點從不匯入 OpenTelemetry，所以不追蹤的應用程式、瀏覽器打包產物或邊緣 worker 既不用安裝 OpenTelemetry，也不會把它打包進去。 |
| That means: | 這意味著： | 也就是說： |
| `ck-orm` is designed to make unsafe SQL construction difficult by default. | `ck-orm` 的設計旨在預設使不安全 SQL 建構變得困難。 | `ck-orm` 預設讓你很難組出不安全的 SQL。 |

### 3. Words and patterns to avoid

下表是本變體的編輯禁用或警示清單；「警示」表示在特定技術語境可能合法，但通常應換成右欄的直白說法。Microsoft 指南明確要求避免逐字翻譯、連續修飾語、被動句和古雅措辭，並以簡短現代句子為例；下列詞形是本專案的編輯警示，不宣稱每個詞在所有語境都錯。[Microsoft Traditional Chinese Style Guide（鏡像）](https://www.pendeba.org/sites/pendeba.future.org/files/zho-twn-styleguide.pdf)

| Avoid | Why | Say instead |
|---|---|---|
| 倘若 | 官方／書面腔；技術說明通常只需要條件 | 如果／若 |
| 茲、茲此 | 公文語，不像開發者文件 | 現在／這裡，或刪除 |
| 敬請 | 對外公文敬語，會把普通操作寫得過度客氣 | 請 |
| 請務必 | 多餘的強調；只有真的有強制性時才保留「必須」 | 請／必須 |
| 予以 | 名詞化官樣語，遮住真正的動詞 | 直接使用動詞，例如「拒絕」「驗證」 |
| 加以 | 名詞化官樣語；常可直接刪除 | 直接使用動詞，例如「設定」「解碼」 |
| 藉由 | 警示：不是錯字，但常是翻譯腔 | 用／透過 |
| 進行查詢、進行驗證、進行設定 | 「進行」沒有增加資訊 | 查詢、驗證、設定 |
| 針對…進行 | 冗長的名詞化結構 | 對…查詢／驗證… |
| 上述、下列 | 索引腔；如果指涉清楚，可保留，但一般說明優先直說 | 前面提到的、下面的，或直接省略 |
| 具備 | 警示：描述能力時常比「有」正式而不必要 | 有／支援 |
| 將…予以… | 雙重官樣結構，阻礙讀者快速找動作 | 直接寫主詞 + 動詞 |
| …中（表示正在做事） | 把英文進行式硬翻成名詞後綴 | 正在…，或直接用動詞 |
| 被拒絕、被驗證 | 沒有交代誰做事的被動句；需要突出受事者時可以保留 | 伺服器拒絕、系統驗證 |
| 意味著 | 警示：抽象直譯腔；因果清楚時不必抽象化 | 也就是說／表示 |
| 以…之方式 | 古雅、名詞化且不自然 | 用…／以…方式 |

### 4. Punctuation, typography and spacing

- 中文正文使用全形「，」「。」「：」「；」「？」「！」；英文名稱、數字、版本號、URL、行內程式碼和程式碼使用半形字元。這裡的地區證據只支持「臺灣中文編寫體例」的範例，不足以概括所有政府文件；本專案另以 Microsoft 指南和以下編寫體例作為編輯依據。[《文化資產保存學刊》撰稿體例（桃園市政府網站所載）](https://ws.tycg.gov.tw/Download.ashx?n=6ZmE5Lu2MuaSsOeov%2BmrlOS%2Biy5L6LLnBkZg%3D%3D&u=LzAwMS9VcGxvYWQvMTExL1JlbEZpbGUvMTI3MTkvMTA5ODU3Mi%2FpmYTku7Yy5pKw56i%2F6auU5L6LLnBkZg%3D%3D)、[Microsoft Traditional Chinese Style Guide（鏡像）](https://www.pendeba.org/sites/pendeba.future.org/files/zho-twn-styleguide.pdf)
- 引號依教育部《重訂標點符號手冊》使用外層「」與內層『』；直接引用的句末標點放在引號內，嵌入句子的短語不另加句末標點。[教育部重訂標點符號手冊：引號](https://language.moe.gov.tw/001/upload/files/site_content/m0001/hau/h6.htm)
- 中文正文與拉丁字母、阿拉伯數字、行內程式碼之間留一個半形空格；中文標點緊貼前面的文字，不在全形標點前後另加空格。Microsoft 指南明確說明中英文字母／數字之間的空格和中文標點周圍的空格規則。[Microsoft Traditional Chinese Style Guide（鏡像）](https://www.pendeba.org/sites/pendeba.future.org/files/zho-twn-styleguide.pdf)
- 需要省略時使用中文刪節號「……」；如果是程式碼、命令、URL 或原文中的三個句點「...」，保持原樣。教育部手冊列中文刪節號的形式；Microsoft 指南另處理英文 UI／文件例外，因此兩者不可混用。[教育部重訂標點符號手冊](https://language.moe.gov.tw/001/Upload/FILES/SITE_CONTENT/M0001/HAU/haushou.htm)、[Microsoft Traditional Chinese Style Guide（鏡像）](https://www.pendeba.org/sites/pendeba.future.org/files/zho-twn-styleguide.pdf)
- 中文正文的長破折號使用教育部規範的「──」；能用冒號、逗號或括號說清楚時優先改寫，不用英文長破折號取代。[教育部重訂標點符號手冊：破折號](https://language.moe.gov.tw/001/upload/files/site_content/m0001/hau/h10.htm)
- 引出程式碼區塊、清單、解釋或引用時使用全形冒號「：」；若後面是清單或程式碼區塊，冒號後換行。一般中文句子不使用半形冒號。[教育部重訂標點符號手冊：冒號](https://language.moe.gov.tw/001/upload/files/site_content/m0001/hau/h5.htm)
- 中文條列若每項是完整句，項末用句號；若只是片語，整份清單保持一致，可不加句號。這是本專案的清單排版規則，不把英文分號逐項直譯成中文分號。
- 標題採句首大寫：中文標題不為了模仿英文標題式大小寫而加大寫；其中的產品名、API 名和縮寫保留原本大小寫。這是本專案的編輯規則。
- 不讓縮寫在換行時被拆開；必要時使用不換行空格，或讓整個行內程式碼一起換行。Microsoft 指南明確建議縮寫使用不換行空格。[Microsoft Traditional Chinese Style Guide（鏡像）](https://www.pendeba.org/sites/pendeba.future.org/files/zho-twn-styleguide.pdf)
- 括號使用半形 ( )，內側不留空格；中文書名用《》，文章或章節可用〈〉。強調或新術語才用引號，不要用引號包住每個 API 名稱。[教育部重訂標點符號手冊：引號](https://language.moe.gov.tw/001/upload/files/site_content/m0001/hau/h6.htm)、[《文化資產保存學刊》撰稿體例](https://ws.tycg.gov.tw/Download.ashx?n=6ZmE5Lu2MuaSsOeov%2BmrlOS%2Biy5L6LLnBkZg%3D%3D&u=LzAwMS9VcGxvYWQvMTExL1JlbEZpbGUvMTI3MTkvMTA5ODU3Mi%2FpmYTku7Yy5pKw56i%2F6auU5L6LLnBkZg%3D%3D)
- 粗體、斜體只標示真正需要注意的條件或結果；不要用粗體模擬標題，也不要改動行內程式碼的內容。這是專案排版規則；程式碼和行內程式碼仍遵守共享規則第 1 節的位元組完全相同要求。

### 5. Numbers, units and dates

- 中文技術文件使用半形阿拉伯數字；小數點用句點，千分位用逗號，例如 1,234.56。桃園市政府網站所載的中文編寫體例示範千分位數字；這是編寫體例證據，不是臺灣政府的統一規範。[《文化資產保存學刊》撰稿體例](https://ws.tycg.gov.tw/Download.ashx?n=6ZmE5Lu2MuaSsOeov%2BmrlOS%2Biy5L6LLnBkZg%3D%3D&u=LzAwMS9VcGxvYWQvMTExL1JlbEZpbGUvMTI3MTkvMTA5ODU3Mi%2FpmYTku7Yy5pKw56i%2F6auU5L6LLnBkZg%3D%3D)
- 數字與拉丁字母單位之間留一個半形空格，例如 120 ns、64 bit。百分比在本專案寫成 80%，不加空格；前者可由上述中文編寫體例支持，後者是本專案寫法。
- 序數在中文句子寫第 1 個、第 2 頁；不要為了英文化寫 1st。這個空格形式在本專案屬未驗證的寫法；若原始識別碼是 1st，保持原樣。
- 版本號是識別碼，保持 0.1.0、1.0.0-beta.1 的半形數字、句點和連字號，不改成中文標點。這是由 API／套件版本的機器可讀性推得的專案規則，未另找到臺灣官方術語條目。
- 64-bit 在中文敘述寫 64 位元；若是型別或命令列旗標，保持原始 64-bit。Microsoft Learn 和 MDN zh-TW 都使用「位元」這個術語。[Microsoft Learn：內建型別](https://learn.microsoft.com/zh-tw/dotnet/csharp/fundamentals/types/built-in-types)、[MDN zh-TW：資料型別與資料結構](https://developer.mozilla.org/zh-TW/docs/Web/JavaScript/Guide/Data_structures)
- 日期一般寫 2026 年 10 月 12 日；記錄、URL、版本或可排序資料使用 ISO 2026-10-12。時間優先使用 24 小時制 17:32:45；跨系統時間使用 2026-10-12T17:32:45Z，Z 表示 UTC。Microsoft 的 zh-TW 格式文件示範中文年月日、24 小時時間和 ISO 8601。[Microsoft Learn：DateTimeFormatter](https://learn.microsoft.com/zh-tw/uwp/api/windows.globalization.datetimeformatting.datetimeformatter?view=winrt-26100)、[Microsoft Learn：自訂日期時間格式](https://learn.microsoft.com/zh-tw/biztalk/core/custom-date-time-formats)
- “for example” 在中文正文寫「例如」；要引出範例清單時寫「例如：」。不要把 e.g. 硬塞進一般中文句子；若它屬於 API 或原始文字，保持原樣。臺灣智慧財產局文件實際使用「例如：」。[智慧財產局文件](https://www.tipo.gov.tw/wSite/public/Attachment/0/f1747644830923.pdf)

### 6. Terminology

「保留英文」只表示在技術正文中保留該術語的英文形狀；中文解釋仍可跟在後面。API 名稱、識別字、SQL 關鍵字和本專案指定的產品名不得翻譯。普通概念詞即使暫時找不到地區來源，也必須填入具體的中文寫法，不能因為 status 是 unverified 就保留英文。

| English | local rendering | keep English? | status | source / note |
|---|---|---:|---|---|
| ClickHouse; ck-orm; TypeScript; JavaScript; SQL; JSON; Decimal; DDL; NULL; CTE; HTTP; UTC; OpenTelemetry; Bun; npm; pnpm; Yarn; Node.js; Drizzle; GitHub; llms.txt; MIT | 各自原樣保留 | yes | attested — https://clickhouse.com/docs/en/intro | 品牌、產品名、縮寫、檔名、版本相關名稱和固定技術名稱不是普通概念詞；TypeScript 和 Decimal 也原樣保留：https://www.typescriptlang.org/docs/ |
| API name; identifier; keyword | 原樣保留 | yes | attested — https://www.typescriptlang.org/docs/ | API 名稱、識別字和 SQL 關鍵字必須可直接對應程式碼；中文說明另譯。
| schema; schema DSL | schema；schema DSL | yes | attested — https://prisma.tw/docs/orm/prisma-schema/overview/generators | 臺灣開發者文件直接使用 schema；DSL 是縮寫。必要時首次補「結構描述」。
| database; column; table; temporary table; table engine; column type; type definition | 資料庫；欄位；資料表；暫存資料表；資料表引擎；欄位型別；型別定義 | no | attested — https://learn.microsoft.com/zh-tw/sql/relational-databases/tables/tables?view=sql-server-ver17 | table engine 的「引擎」是依 ClickHouse 語境推得；不保留整個英文詞組。
| nullable; optional; required; default value | 可為 NULL；選用；必要；預設值 | no | attested — https://learn.microsoft.com/zh-tw/dotnet/csharp/language-reference/builtin-types/nullable-reference-types | NULL 是 SQL 關鍵字；其他詞形見 Microsoft Learn zh-TW。
| modifier; materialized column; alias column | 修飾詞；實體化欄位；別名欄位 | no | unverified | 沒有本次可引用的臺灣固定術語；這些是 plain textbook rendering，母語者確認。
| array; string; integer; 64-bit integer; floating point; boolean; date; timestamp; timezone | 陣列；字串；整數；64 位元整數；浮點數；布林值；日期；時間戳記；時區 | no | attested — https://developer.mozilla.org/zh-TW/docs/Web/JavaScript/Guide/Data_structures | 數字與「位元」之間留空格；日期時間詞形參考 Microsoft zh-TW。
| tuple; map; nested; enum | tuple；map；nested；enum | yes | attested — https://clickhouse.com/docs/en/sql-reference/data-types/tuple | 這些是 ClickHouse 型別或語言型別名稱，保留原形；不是把普通概念詞留成英文。
| literal; precision; scale; exact value; lossless | 字面值；精確度；小數位數；精確值；不失真 | no | inferred — from https://learn.microsoft.com/zh-tw/dotnet/csharp/fundamentals/types/built-in-types and Microsoft Decimal terminology | lossless 的固定臺灣搭配仍未核實，先用 plain textbook rendering。
| JSON path; generic type; type inference; inferred; typed; untyped; type assertion | JSON 路徑；泛型；型別推論；推斷出的型別；有型別；無型別；型別斷言 | no | inferred — from MDN zh-TW data structures and Taiwan TypeScript terminology | JSON 保留；path 譯為「路徑」。
| runtime validation; validator; encoder; decoder; decoding | 執行階段驗證；驗證器；編碼器；解碼器；解碼 | no | inferred — from Microsoft Learn zh-TW validation/runtime terminology and MDN data terminology |
| query; query builder; builder | 查詢；查詢建構器；建構器 | no | attested — https://learn.microsoft.com/zh-tw/sql/tools/visual-studio-code-extensions/github-copilot/smart-query-builder?view=sql-server-ver17 |
| chain; chainable; subquery; bare subquery; anonymous CTE | 鏈式；可鏈式；子查詢；未包裝的子查詢；匿名 CTE | no | inferred — from Microsoft query terminology and the ordinary meanings of chain, bare and anonymous | 不寫不自然的「裸子查詢」。
| join; left join; outer join; null semantics; predicate; filter | 聯結；左聯結；外部聯結；NULL 語意；述詞；篩選條件 | no | inferred — from Taiwan SQL terminology and https://learn.microsoft.com/zh-tw/sql/tools/visual-studio-code-extensions/github-copilot/smart-query-builder?view=sql-server-ver17 | SQL 關鍵字另依程式碼規則保留；述詞仍需母語者確認。
| where clause; group by; order by; limit | WHERE 子句；GROUP BY 子句；ORDER BY 子句；LIMIT 子句 | no | attested — https://learn.microsoft.com/zh-tw/office/vba/access/concepts/structured-query-language/retrieve-records-using-access-sql | SQL 關鍵字保留，clause 譯為「子句」。
| projection; selection (select list); alias; expression; window expression; aggregate | 投影；選取清單；別名；運算式；視窗運算式；彙總 | no | inferred — from Taiwan relational-query terminology |
| function; table function; scalar | 函式；資料表函式；純量 | no | attested — https://learn.microsoft.com/zh-tw/dotnet/fsharp/language-reference/functions/ | MDN zh-TW 也可見「函數」；本指南統一用「函式」，母語者仍應確認。
| raw SQL; SQL template; SQL fragment | 原始 SQL；SQL 範本；SQL 片段 | no | inferred — from SQL terminology and Taiwan technical writing | SQL 保留；raw、template、fragment 都要翻譯。
| parameter; bound parameter; placeholder; query parameter | 參數；參數繫結；預留位置；查詢參數 | no | attested — https://learn.microsoft.com/zh-tw/aspnet/web-pages/overview/getting-started/introducing-aspnet-web-pages-2/form-basics | bound parameter 的「參數繫結」是從 Taiwan「繫結」用法推得，母語者確認。
| compile; compiled SQL; thenable; helper; namespace; factory; callback; iterator; variadic | 編譯；編譯後的 SQL；具有 then 方法的物件；輔助工具；命名空間；工廠函式；回呼函式；迭代器；可變參數 | no | inferred — from TypeScript、MDN zh-TW and Taiwan developer terminology | then 是識別字，不翻譯。
| insert; write; batch; bulk insert; insert from select; stream; streaming | 插入；寫入；批次；大量插入；從 SELECT 插入；串流；串流處理 | no | attested — https://learn.microsoft.com/zh-tw/sql/connect/ado-net/sql/table-valued-parameters?view=sql-server-ver17 | SELECT 是 SQL 關鍵字。
| row; result set; replacement (ReplacingMergeTree); versioning; deletion flag | 資料列；結果集；替換（ReplacingMergeTree）；版本控管；刪除旗標 | no | inferred — from Microsoft ADO.NET terminology and ClickHouse engine terminology | ReplacingMergeTree 是引擎名稱，保留原形。
| client; connection; server; replica; request; response; transport; HTTP request | 用戶端；連線；伺服器；副本；請求；回應；傳輸；HTTP 請求 | no | attested — https://developer.mozilla.org/zh-TW/docs/Web/API/Fetch_API/Using_Fetch | client 也有「客戶端」用法；本指南統一「用戶端」。
| timeout; settings; configuration; config; option; session; temporary-table workflow; transaction | 逾時；設定；設定；設定；選項；工作階段；暫存資料表流程；交易 | no | attested — https://learn.microsoft.com/zh-tw/windows-server/administration/windows-commands/query-session and https://learn.microsoft.com/zh-tw/sql/t-sql/language-elements/transactions-transact-sql?view=sql-server-ver17 | 若 config 是識別字則保留識別字；普通說明譯為「設定」。
| concurrency; lifecycle; cleanup; runtime method; execute; fetch; abort; cancel | 並行；生命週期；清理；執行階段方法；執行；擷取；中止；取消 | no | inferred — from Taiwan runtime/API terminology | concurrency 尚無本次可引用的固定臺灣詞形，採 plain textbook rendering。
| observability; tracing; logging; logger; metrics | 可觀察性；追蹤；記錄；記錄器；指標 | no | attested — https://learn.microsoft.com/zh-tw/azure/well-architected/operational-excellence/observability and https://learn.microsoft.com/zh-tw/agent-framework/agents/observability | 「可觀察性」（Microsoft Learn）與「可觀測性」（AWS、Google Cloud）依廠商而異，Microsoft 自己的頁面也偶爾混用；本指南選「可觀察性」。
| span | span（可觀察性一節首次出現時寫「span（跨度）」，之後只寫 span） | yes | attested — https://learn.microsoft.com/zh-tw/agent-framework/agents/observability and https://learn.microsoft.com/zh-tw/azure/azure-monitor/app/opentelemetry-add-modify | 不譯為「區間」。Microsoft Agent Framework observability 頁面（zh-tw）只寫「區間」1 次，「跨度」卻有 5 次；Azure Monitor opentelemetry-add-modify 頁面（zh-tw）兩者都沒用。AWS、Google Cloud 的 zh-TW 文件寫「跨度」或「範圍」，或直接保留 Span；iThome 文章保留 Span（約 330 次，「跨度」僅 1 次）。子標題寫 `#### Span`。「區間」表示範圍、間隔時不是 span，不在此限。 |
| instrumentation | 檢測 | no | attested — https://learn.microsoft.com/zh-tw/azure/azure-monitor/app/opentelemetry-add-modify and https://docs.aws.amazon.com/zh_tw/xray/latest/devguide/xray-sdk-migration.html and https://cloud.google.com/trace/docs/setup?hl=zh-TW | Azure Monitor opentelemetry-add-modify 頁面（zh-tw）用「檢測」31 次、「儀器化」0 次；AWS X-Ray devguide（zh_tw）和 Google Cloud Trace（zh-TW）同樣用「檢測」。不用「儀器化」。Microsoft Agent Framework observability 頁面（zh-tw）則寫「儀器化」「儀器」，同一廠商內並不一致，本指南依多數來源選「檢測」。 |
| exporter | 匯出器 | no | attested — https://learn.microsoft.com/zh-tw/azure/azure-monitor/app/opentelemetry-configuration and https://learn.microsoft.com/zh-tw/azure/azure-monitor/app/opentelemetry-add-modify | Microsoft Azure Monitor 頁面（zh-tw）寫「匯出器」；AWS、Google Cloud 也有「匯出工具」的寫法，兩者都通，本指南統一「匯出器」。 |
| Collector | 收集器 | no | attested — https://docs.aws.amazon.com/zh_tw/xray/latest/devguide/xray-sdk-migration.html and https://cloud.google.com/trace/docs/setup?hl=zh-TW | AWS X-Ray 移轉頁面（zh_tw）寫「收集器」26 次；Google Cloud Trace（zh-TW）也寫「收集器」。 |
| propagator | 傳播器 | no | attested — https://docs.aws.amazon.com/zh_tw/xray/latest/devguide/xray-sdk-migration.html and https://learn.microsoft.com/zh-tw/agent-framework/agents/observability | AWS X-Ray 移轉頁面（zh_tw）寫「X-Ray 傳播器」，Microsoft Agent Framework observability 頁面（zh-tw）寫「OpenTelemetry 傳播器」。識別字 `propagateTraceContext` 保留原文。 |
| tracer provider | tracer provider | yes | inferred — OpenTelemetry API 的類別名稱 `TracerProvider` | AWS 與 Microsoft 的繁中文件只零星寫「追蹤器提供者」，沒有穩定的譯法，且它是 API 識別字，因此保留英文。 |
| context manager | 脈絡管理器 | no | unverified — 沒有找到臺灣來源 | Python 的 zh-tw 詞彙表把 with 陳述式的 context manager 譯為「情境管理器」，那是另一個概念，本指南不採用；這裡是 OpenTelemetry 的 context manager，需母語者確認。 |
| SDK | SDK | yes | attested — https://learn.microsoft.com/zh-tw/azure/azure-monitor/app/javascript-sdk | SDK 是縮寫，與 API 一樣原樣保留。 |
| hook | hook | yes | attested — https://zh-hans.react.dev/reference/react/hooks and https://react.com.tw/reference/react/hooks | React 語境的 Hook 是開發者實際保留的技術術語；臺灣社群站 react.com.tw（非官方翻譯）也保留英文 Hook。Microsoft Win32 文件（zh-tw）把 Windows 的 hook 寫成「掛鉤」，屬於另一個領域的用法，本指南不採用。 |
| plugin | 外掛程式 | no | attested — https://learn.microsoft.com/zh-tw/azure/azure-monitor/app/javascript-sdk and https://docs.aws.amazon.com/zh_tw/xray/latest/devguide/xray-sdk-migration.html and https://zh.wikipedia.org/zh-tw/外掛程式 | Microsoft Azure Monitor JavaScript SDK 頁面（zh-tw）寫「外掛程式」，AWS X-Ray 移轉頁面（zh_tw）寫「服務外掛程式」；中文維基百科「外掛程式」條目的臺灣用字欄列出「外掛程式」「外掛」「擴充功能」。「插件」是中國大陸用法，不使用。識別字 `plugins`、`ClickHouseORMPlugin` 保留原文。 |
| event; error; exception; throws; rejects; error model; validation error; stack; retry | 事件；錯誤；例外；拋出；拒絕；錯誤模型；驗證錯誤；堆疊；重試 | no | attested — https://learn.microsoft.com/zh-tw/dotnet/api/system.servicemodel.dispatcher.ierrorhandler?view=netcore-1.1 and https://developer.mozilla.org/zh-TW/docs/Web/API/Fetch_API/Using_Fetch | call stack 可寫「呼叫堆疊」。
| security; SQL injection; untrusted input; identifier quoting; escaping; credentials; password; allow-list; permission | 安全性；SQL 注入；不受信任的輸入；識別字引用；跳脫；認證資訊；密碼；允許清單；權限 | no | attested — https://developer.mozilla.org/zh-TW/docs/Glossary/SQL_Injection and https://learn.microsoft.com/zh-tw/aspnet/core/fundamentals/servers/kestrel/security-considerations?view=aspnetcore-10.0 | SQL 保留；injection 譯為「注入」。
| installation; install; version; pin a version; release; breaking change | 安裝；安裝；版本；鎖定版本；發行；重大變更 | no | attested — https://learn.microsoft.com/zh-tw/dotnet/framework/migration-guide/version-compatibility | 詞形見 Microsoft Learn zh-TW。
| peer dependency | peer dependency | 是 | inferred — X 搜尋（grok-4.7，2026-10-15；貼文未另行重新驗證） | 「同儕依賴」「對等依賴」「同儕相依」「對等相依」「同層依賴」在 X 上都沒有可用的台灣開發者貼文；prisma.tw 是非官方社群翻譯站，只有一處寫「peer dependency（同儕依賴）」；台灣部落格有「對等依賴」（realnewbie.com），多數直接寫英文。因此保留英文。一般詞 dependency 仍用「依賴」。 |
| entry point | 進入點 | no | attested — https://learn.microsoft.com/zh-tw/dotnet/csharp/fundamentals/program-structure/main-command-line | Microsoft C# Main 命令列頁面（zh-tw）「進入點」「入口點」並用；本指南選「進入點」。
| process | 程序 | no | attested — https://learn.microsoft.com/zh-tw/dotnet/standard/threading/threads-and-threading and https://docs.aws.amazon.com/zh_tw/lambda/latest/dg/lambda-runtime-environment.html | 指作業系統的 process 時寫「程序」（Microsoft 執行緒頁面、AWS Lambda 執行環境頁面）；不寫「處理程序」。
| edge | 邊緣 | no | attested — https://docs.aws.amazon.com/zh_tw/AmazonCloudFront/latest/DeveloperGuide/edge-functions.html | AWS CloudFront 寫「邊緣函數」，Microsoft Front Door 寫「邊緣位置」；`edge worker` 寫「邊緣 worker」。
| in-memory | 記憶體內 | no | attested — https://learn.microsoft.com/zh-tw/sql/relational-databases/in-memory-oltp/overview-and-usage-scenarios | Microsoft 記憶體內 OLTP 頁面（zh-tw）也寫「記憶體內部」；兩種都可以，本指南用「記憶體內」。
| before 1.0; pre-release; migration; production; example; runnable | 1.0 之前；預發行；遷移；正式環境；範例；可執行 | no | attested — https://learn.microsoft.com/zh-tw/ef/core/managing-schemas/migrations/applying | schema migration 寫「schema 遷移」；production 不寫成「生產」。
| reference; documentation; page; section; caveat; note | 參考資料；文件；頁面；章節；注意事項；備註 | no | attested — https://developer.mozilla.org/zh-TW/docs/Web/API/Fetch_API/Using_Fetch and https://learn.microsoft.com/zh-tw/dotnet/standard/metadata-and-self-describing-components |
| behavior; semantics; contract; mode; public API; stable; internal; deprecated; shape; surface; layer | 行為；語意；契約；模式；公開 API；穩定；內部；已棄用；資料形狀；介面範圍；層 | no | inferred — from Taiwan API and framework terminology | shape、surface 的固定臺灣搭配仍需母語者確認。
| keeps X easy; without hiding; make everyday access easier to structure | 讓 X 更容易；不隱藏；讓平常使用 ClickHouse 時更容易整理 | no | inferred — idiomatic rewrites following the shared direct-voice rule | 依完整句子改寫，不逐字對應。
| is rejected; is not validated; is decoded conservatively | 不接受；未經驗證；採保守方式解碼 | no | inferred — idiomatic active and technical rewrites | 有明確主體時寫「伺服器拒絕」。
| carries; exposes; follows; stays; leaves; pass; wrap; compose; accepts; returns; throws | 帶有；提供；遵循；維持；保留；傳入；包裝；組合；接受；回傳；拋出 | no | inferred — idiomatic verb choices from Taiwan developer documentation |

### 7. Wording guard

下面只列有成對來源的 contamination WARN。左欄來源證明不採用的形式，右欄來源證明臺灣頁面的 preferred rendering；沒有成對證據的差異不設規則。這些規則是保守警示，不把每個詞形都宣稱成所有臺灣文件的唯一標準。

| Concept | 不採用的形式 | Preferred zh-Hant-TW | Level | Evidence: bad side / preferred side |
|---|---|---|---|---|
| database, table, field, temporary table | 数据库、数据表、字段、临时表 | 資料庫、資料表、欄位、暫存資料表 | WARN | bad: https://learn.microsoft.com/zh-cn/sql/relational-databases/tables/tables?view=sql-server-ver17; preferred: https://learn.microsoft.com/zh-tw/sql/relational-databases/tables/tables?view=sql-server-ver17 and https://learn.microsoft.com/zh-tw/sql/t-sql/statements/create-table-transact-sql?view=sql-server-ver17 |
| query, transaction, statement, rollback | 查询、事务、语句、回滚 | 查詢、交易、陳述式、復原 | WARN | bad: https://learn.microsoft.com/zh-cn/sql/t-sql/language-elements/transactions-transact-sql?view=sql-server-ver17; preferred: https://learn.microsoft.com/zh-tw/sql/t-sql/language-elements/transactions-transact-sql?view=sql-server-ver17 |
| default, optional, conversion | 默认、可选、转换 | 預設、選用、轉換 | WARN | bad: https://learn.microsoft.com/zh-cn/powerquery-m/table-transformcolumns; preferred: https://learn.microsoft.com/zh-tw/powerquery-m/table-transformcolumns |
| request, response, network, fetch | 请求、响应、网络、获取 | 請求、回應、網路、擷取 | WARN | bad: https://developer.mozilla.org/zh-CN/docs/Web/API/Fetch_API/Using_Fetch; preferred: https://developer.mozilla.org/zh-TW/docs/Web/API/Fetch_API/Using_Fetch |
| user, object, array, string, type | 用户、对象、数组、字符串、类型 | 使用者、物件、陣列、字串、型別 | WARN | bad: https://developer.mozilla.org/zh-CN/docs/Web/JavaScript/Guide; preferred: https://developer.mozilla.org/zh-TW/docs/Web/JavaScript/Guide and https://developer.mozilla.org/zh-TW/docs/Web/JavaScript/Guide/Data_structures |
| component, code, documentation, compatibility | 组件、代码、文档、兼容 | 元件、程式碼、文件、相容 | WARN | bad: https://learn.microsoft.com/zh-cn/dotnet/standard/metadata-and-self-describing-components and https://cn.vuejs.org/guide/essentials/component-basics.html; preferred: https://learn.microsoft.com/zh-tw/dotnet/standard/metadata-and-self-describing-components and https://book.vue.tw/v1/CH2/2-4-slots |
| debug, thread, cache, software, hardware | 调试、线程、缓存、软件、硬件 | 除錯、執行緒、快取、軟體、硬體 | WARN | bad: https://learn.microsoft.com/zh-cn/windows-hardware/drivers/debugger/getting-started-with-windows-debugging and https://learn.microsoft.com/zh-cn/windows-hardware/drivers/kernel/always-preemptible-and-always-interruptible; preferred: https://learn.microsoft.com/zh-tw/windows-hardware/drivers/debugger/getting-started-with-windows-debugging, https://learn.microsoft.com/zh-tw/windows/win32/fileio/file-caching and https://learn.microsoft.com/zh-tw/windows-hardware/drivers/kernel/always-preemptible-and-always-interruptible |
| interface, exception, return, call | 接口、异常、返回、调用 | 介面、例外、回傳、呼叫 | WARN | bad: https://learn.microsoft.com/zh-cn/dotnet/api/system.servicemodel.dispatcher.ierrorhandler?view=netcore-1.1; preferred: https://learn.microsoft.com/zh-tw/dotnet/api/system.servicemodel.dispatcher.ierrorhandler?view=netcore-1.1 |
| parameter, permission, password, link | 参数、权限、密码、链接 | 參數、權限、密碼、連結 | WARN | bad: https://developer.mozilla.org/zh-CN/docs/Web/API/Fetch_API/Using_Fetch and https://learn.microsoft.com/zh-cn/aspnet/core/fundamentals/servers/kestrel/security-considerations?view=aspnetcore-10.0; preferred: https://developer.mozilla.org/zh-TW/docs/Web/API/Fetch_API/Using_Fetch, https://learn.microsoft.com/zh-tw/aspnet/core/fundamentals/servers/kestrel/security-considerations?view=aspnetcore-10.0 and https://developer.mozilla.org/zh-TW/docs/Learn_web_development/Core/Structuring_content/Creating_links |
| setting, validation, error, log, number, timezone | 设置、验证、错误、记录、数字、时区 | 設定、驗證、錯誤、記錄、數字、時區 | WARN | bad: https://learn.microsoft.com/zh-cn/aspnet/core/fundamentals/servers/kestrel/security-considerations?view=aspnetcore-10.0 and https://developer.mozilla.org/zh-CN/docs/Web/JavaScript/Guide/Data_structures; preferred: https://learn.microsoft.com/zh-tw/aspnet/core/fundamentals/servers/kestrel/security-considerations?view=aspnetcore-10.0, https://developer.mozilla.org/zh-TW/docs/Web/JavaScript/Guide/Data_structures and https://learn.microsoft.com/zh-tw/uwp/api/windows.globalization.datetimeformatting.datetimeformatter?view=winrt-26100 |
| import, export, delete, return | 导入、导出、删除、返回 | 匯入、匯出、刪除、回傳 | WARN | bad: https://learn.microsoft.com/zh-cn/power-apps/user/import-data and https://learn.microsoft.com/zh-cn/visualstudio/python/refactoring-python-code?view=visualstudio; preferred: https://learn.microsoft.com/zh-tw/power-apps/user/import-data, https://learn.microsoft.com/zh-tw/visualstudio/python/refactoring-python-code?view=visualstudio and https://learn.microsoft.com/zh-tw/dotnet/api/system.servicemodel.dispatcher.ierrorhandler?view=netcore-1.1 |

### 8. Model passage

#### 心智模型

使用 `ck-orm` 時，記得把下面這些界線分清楚：

- `ckType.*` 定義 schema 的欄位型別
- `ckTable(...)` 定義資料表的 schema
- `ck.*` 是查詢輔助工具的命名空間
- `fn.*` 是 SQL 函式輔助工具的命名空間
- schema 描述資料表和欄位，不描述資料庫名稱
- `clickhouseClient(...)` 負責資料庫連線
- 建構器查詢是預設方式；遇到 ClickHouse 專用運算式時，也可以使用原始 SQL
- `runInSession()` 是 ClickHouse 的工作階段輔助工具，不是交易
- `plugins` 可觀察 ck-orm 送出的每個請求；追蹤是需要自行加入的 `ck-orm/otel` 外掛程式，不在主進入點裡
- `leftJoin()` 和 `asofLeftJoin()` 預設採用 SQL 風格的 NULL 語意
- 數值很大或風險較高的結果會採保守方式解碼，不會默默強制轉型

#### 安裝

ck-orm 沒有必要的依賴。OpenTelemetry 追蹤是獨立的進入點 `ck-orm/otel`，`@opentelemetry/api` 是選用的 peer dependency，只有這個進入點會用到。`ck-orm` 的主進入點從不匯入 OpenTelemetry，所以不追蹤的應用程式、瀏覽器打包產物或邊緣 worker 既不用安裝 OpenTelemetry，也不會把它打包進去。要追蹤查詢，請依照 [OpenTelemetry 外掛程式](#opentelemetry-plugin)的設定步驟操作。

### 9. Sources consulted

- [MDN zh-TW Fetch API](https://developer.mozilla.org/zh-TW/docs/Web/API/Fetch_API/Using_Fetch)：確認請求、回應、擷取、參數、選項、回呼、物件和 Promise 錯誤相關詞形。
- [MDN zh-TW JavaScript 資料型別與資料結構](https://developer.mozilla.org/zh-TW/docs/Web/JavaScript/Guide/Data_structures)：確認字串、陣列、型別、布林值、物件和數字相關詞形。
- [MDN zh-TW SQL 注入](https://developer.mozilla.org/zh-TW/docs/Glossary/SQL_Injection)：確認「SQL 注入」而不是把普通英文詞留在正文。
- [Microsoft Learn zh-TW SQL tables](https://learn.microsoft.com/zh-tw/sql/relational-databases/tables/tables?view=sql-server-ver17)、[CREATE TABLE](https://learn.microsoft.com/zh-tw/sql/t-sql/statements/create-table-transact-sql?view=sql-server-ver17)、[transactions](https://learn.microsoft.com/zh-tw/sql/t-sql/language-elements/transactions-transact-sql?view=sql-server-ver17)：確認資料庫、資料表、欄位、暫存資料表、交易、陳述式和復原。
- [Microsoft Learn zh-TW ADO.NET parameters](https://learn.microsoft.com/zh-tw/sql/connect/ado-net/sql/table-valued-parameters?view=sql-server-ver17)、[placeholder](https://learn.microsoft.com/zh-tw/aspnet/web-pages/overview/getting-started/introducing-aspnet-web-pages-2/form-basics)：確認參數、資料列、結果集、批次、串流和預留位置。
- [Microsoft Learn zh-TW query builder](https://learn.microsoft.com/zh-tw/sql/tools/visual-studio-code-extensions/github-copilot/smart-query-builder?view=sql-server-ver17)：確認查詢建構器、聯結、篩選和彙總。
- [Microsoft Learn zh-TW observability](https://learn.microsoft.com/zh-tw/azure/well-architected/operational-excellence/observability)、[Agent Framework observability](https://learn.microsoft.com/zh-tw/agent-framework/agents/observability)：確認可觀察性、追蹤、指標和記錄；來源自身混用可觀察性／可觀測性，因此本指南明確選定前者。Agent Framework 頁面只用「區間」1 次、「跨度」5 次，不能支持「區間」，span 因此保留英文。
- [Microsoft Learn zh-TW Azure Monitor](https://learn.microsoft.com/zh-tw/azure/azure-monitor/app/opentelemetry-add-modify)、[Azure Monitor 設定](https://learn.microsoft.com/zh-tw/azure/azure-monitor/app/opentelemetry-configuration)、[Azure Monitor JavaScript SDK](https://learn.microsoft.com/zh-tw/azure/azure-monitor/app/javascript-sdk)、[AWS X-Ray 移轉](https://docs.aws.amazon.com/zh_tw/xray/latest/devguide/xray-sdk-migration.html)、[Google Cloud Trace](https://cloud.google.com/trace/docs/setup?hl=zh-TW)：確認檢測、匯出器（匯出工具）、收集器、傳播器和外掛程式。
- [Microsoft Learn zh-TW 執行緒](https://learn.microsoft.com/zh-tw/dotnet/standard/threading/threads-and-threading)、[AWS Lambda 執行環境](https://docs.aws.amazon.com/zh_tw/lambda/latest/dg/lambda-runtime-environment.html)、[AWS CloudFront 邊緣函數](https://docs.aws.amazon.com/zh_tw/AmazonCloudFront/latest/DeveloperGuide/edge-functions.html)、[C# Main 命令列](https://learn.microsoft.com/zh-tw/dotnet/csharp/fundamentals/program-structure/main-command-line)：確認程序、邊緣和進入點。
- [Microsoft Learn zh-TW session](https://learn.microsoft.com/zh-tw/windows-server/administration/windows-commands/query-session)、[session timeout](https://learn.microsoft.com/zh-tw/microsoft-365/enterprise/session-timeouts?view=o365-worldwide)：確認工作階段和逾時；來源也混用會話，本指南選工作階段。
- [Microsoft Learn zh-TW built-in types](https://learn.microsoft.com/zh-tw/dotnet/csharp/fundamentals/types/built-in-types)、[nullable](https://learn.microsoft.com/zh-tw/dotnet/csharp/language-reference/builtin-types/nullable-reference-types)：確認位元、字面值、浮點數、布林值、型別和可為 NULL。
- [Microsoft Learn zh-TW migration](https://learn.microsoft.com/zh-tw/ef/core/managing-schemas/migrations/applying)、[version compatibility](https://learn.microsoft.com/zh-tw/dotnet/framework/migration-guide/version-compatibility)：確認遷移、相容性、重大變更和已棄用。
- [ClickHouse data types](https://clickhouse.com/docs/en/sql-reference/data-types/tuple)、[TypeScript documentation](https://www.typescriptlang.org/docs/)、[Prisma Taiwan](https://prisma.tw/docs/orm/prisma-schema/overview/generators)：確認 ClickHouse 型別、產品名稱與 schema／DSL 等技術名稱的保留邊界。
- [React 中文 Hook 文件](https://zh-hans.react.dev/reference/react/hooks)、[React Taiwan](https://react.com.tw/reference/react/hooks)（社群站，非官方翻譯）：確認 React 語境的 Hook 是開發者實際保留的技術術語。
- [Prisma 繁體中文 OpenTelemetry 追蹤](https://prisma.tw/docs/orm/prisma-client/observability-and-logging/opentelemetry-tracing)：非官方社群翻譯站，只有一處寫「peer dependency（同儕依賴）」，證據偏弱，見第 10 節。
- [教育部《重訂標點符號手冊》](https://language.moe.gov.tw/001/Upload/FILES/SITE_CONTENT/M0001/HAU/haushou.htm)、[Microsoft Traditional Chinese Style Guide（鏡像）](https://www.pendeba.org/sites/pendeba.future.org/files/zho-twn-styleguide.pdf)：確認標點、空格、直接語氣、主動句和排版規則。

### 10. Open questions

- 「資料表引擎」是否比保留 table engine 更符合熟悉 ClickHouse 的臺灣開發者用語。
- 「參數繫結」是否比「繫結參數」或「參數化」更自然。
- span 目前保留英文，並在首次出現時附「跨度」；Microsoft 用「跨度」、iThome 多保留 Span，是否改成直接用「跨度」需臺灣母語開發者確認。
- peer dependency 已改為保留英文（2026-10-15 決定）：「同儕依賴」只有一個非官方站的單一用例，X 上五種中文譯名都沒有台灣開發者貼文，台灣部落格多數直接寫英文，另有「對等依賴」（realnewbie.com）。若母語者認為應譯，再從「對等依賴」與「同儕依賴」中選一個。
- 「資料形狀」是否是 shape 在 API 文件中的自然臺灣譯法，還是應改成「結構」。
- client 選「用戶端」而不是「客戶端」，以及 session 選「工作階段」而不是「會話」，應由臺灣母語開發者確認全站一致性。



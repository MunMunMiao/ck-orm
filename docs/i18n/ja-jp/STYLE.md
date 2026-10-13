## ja-JP - 日本語（日本）

### 1. 対象地域と読者

この節の対象は、日本でソフトウェアを開発する読者が読む `ja-JP` です。日本語としては日本向けの現代的な技術文書を採用し、API名、識別子、SQLキーワード、製品名、コードは英語のままにします。日本語に置き換えるのは、開発者が日本語で実際に使う概念名だけです。Microsoftの日本語ローカライズガイドは市場ごとの技術文書の慣用と表記を定める資料であり、JTFは実務翻訳の日本語表記を統一する資料です。<https://learn.microsoft.com/ja-jp/globalization/reference/microsoft-style-guides> <https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf>

読者はJavaScript、TypeScript、SQL、ClickHouse、Bunの用語に慣れた開発者です。したがって、用語を無理に和訳して説明を重くせず、`クエリ`、`サーバー`、`リクエスト`、`レスポンス` のように日本語の開発者文書で定着した表記を使います。MDN、TypeScript、React、Vueの日本語文書でも、この種のカタカナ語とAPI名の組合せが使われています。<https://developer.mozilla.org/ja/docs/Web> <https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> <https://ja.react.dev/learn/passing-props-to-a-component> <https://ja.vuejs.org/guide/introduction>

このガイドで「採用」と書いたものは、参照先で実際に確認できた表記（attested）です。「推奨」は複数の attested な選択肢からこのプロジェクト向けに選んだもの（inferred）で、「unverified」は一次資料で十分に確認できなかったため、一般的な開発者語彙から暫定採用したものです。

### 2. 呼びかけ・文体・トーン

本文は一貫したです・ます調にします。同僚に説明するような自然な開発者文書の口調にし、官僚的・公用文的な表現や英語の逐語訳調は避けます。見出しと短い箇条書きは体言止めでも構いませんが、同じ階層では文体をそろえます。JTFの実務向けガイドとMicrosoftの簡潔な文体の原則を参照します。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf> <https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences>

読者を明示する必要がない限り、二人称の「あなた」や「ユーザー」を主語にしません。操作を直接促すときは `〜してください`、説明では `〜します`、能力や条件では `〜できます` を使います。「していただく」「なさる」などの過度な敬語は避けます。これはこのガイドの編集ルールです。簡潔で人に話しかけるような文体という一般原則も参照します。<https://learn.microsoft.com/ja-jp/style-guide/brand-voice-above-all-simple-human>

一文一義を基本にし、英語の長い修飾をそのまま一文へ詰め込みません。三つ以上の事項は箇条書きにし、本文では `〜することができます` より `〜できます`、`〜を行います` より直接的な動詞を優先します。文化庁の公用文資料とMicrosoftのライティング原則も、短い文と箇条書きを勧めています。<https://www.bunka.go.jp/seisaku/bunkashingikai/kokugo/kokugo_kadai/iinkai_41/pdf/92851101_02.pdf> <https://learn.microsoft.com/ja-jp/style-guide/global-communications/writing-tips>

| English source sentence | BAD. officialese / translationese | Good. colloquial developer Japanese |
|---|---|---|
| ck-orm has one peer dependency, `@opentelemetry/api`. Bun, npm and pnpm install it automatically; Yarn does not, so the Yarn command adds it explicitly. | **BAD.** ck-ormには `@opentelemetry/api`という1つのピア依存関係が存在し、Bun、npm、pnpmはそれを自動的にインストールしますが、Yarnはそうではなく、Yarnコマンドはそれを明示的に追加するものとなります。 | ck-ormのピア依存関係は`@opentelemetry/api`1つです。Bun、npm、pnpmでは自動で入り、Yarnではコマンドに明示的に追加します。 |
| The examples below use a single table so the main flow stays easy to follow. | **BAD.** 以下の例は、主要なフローを容易に追跡することができるように、単一のテーブルを使用するものとなります。 | 以下の例では、流れを追いやすくするため、1つのテーブルだけを使います。 |
| Builder queries are thenable, so `await query` executes the query directly. | **BAD.** ビルダークエリはthenableであるため、`await query`によりクエリを直接実行することができます。 | ビルダーのクエリはthenableなので、`await query`だけでそのまま実行できます。 |
| Avoid forwarding those details directly to untrusted clients. | **BAD.** これらの詳細を信頼されていないクライアントに直接転送することは回避してください。 | その詳細を、信頼できないクライアントへそのまま返さないでください。 |

英語の命令文を逐語訳して文末をすべて `〜してください` にする必要はありません。危険を避ける注意書きは `〜しないでください`、仕様の説明は `〜します`、選択肢の説明は `〜できます` とし、同じ段落で `です。ます。です。ます。` が機械的に続かないように文を組み替えます。この切り替えはプロジェクトの編集判断です。短く直接的に書く方針はJTFとMicrosoftの資料を参照します。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf> <https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences>

### 3. 避ける語とパターン

下表は「常に文法違反」という意味ではなく、このプロジェクトの開発者向け本文で優先的に避ける形です。簡潔な語、具体的な動詞、一つの概念に一つの用語を使うというMicrosoftの原則を基準にしています。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences>

| avoid | why | say instead |
|---|---|---|
| 〜することができます | 可能表現が重くなるため、短い動詞を優先します。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> | 〜できます |
| 〜を行います | 名詞化で動作がぼやけるため、具体的な動詞を使います。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> | 直接の動詞（検証します、実行します、追加します） |
| 〜となります | 状態を断定する文では遠回しです。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> | 〜です（状態変化なら `〜になります`） |
| 〜になります | 状態変化でない断定では冗長です。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> | 〜です、〜します |
| および | 開発者向け本文では硬く見えやすいため、自然な列挙を使います。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> | `、`、または `と` |
| 再度 | 日常語より硬いため、文脈に合う短い語を使います。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> | もう一度、再び |
| 詳細な | 名詞を重ねた硬い修飾になりやすい語です。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> | 詳しい、具体的な |
| 未検証、未設定、未指定 | 状態を `未〜` だけで示さず、読み手に分かる文にします。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> | 検証されていません、設定されていません、指定されていません |
| 〜することにより | 長い接続表現です。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> | 〜すると、〜して |
| 〜において | 場所や対象を硬く言い過ぎます。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> | 〜で、〜について |
| 〜を実施します | `行います` と同じく動作がぼやけます。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> | 〜します、〜を実行します |
| 〜を可能にします | 英語の形を残しやすい表現です。簡潔な動詞を選びます。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> | 〜できます、〜しやすくします |
| ユーザー様、お客様 | 開発者向けの中立的な説明では過剰な敬称です。これはプロジェクトの文体ルールです。 | ユーザー、または主語を省略 |
| あなたは〜 | 手順で主語を毎回明示すると不自然です。これはプロジェクトの文体ルールです。 | 主語を省略、必要なら `ユーザーは〜` |
| 〜に関して | `について` より硬い接続表現です。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> | 〜について |
| 最新と同期する（最新状態にする意味） | `sync` の対象がない文脈では意味が変わるため、動作に合う動詞を選びます。これはプロジェクトの編集判断です。 | 最新に保つ |

### 4. 句読点・表記・空白

- 日本語の本文は全角の `、` と `。` を使います。半角の `,` と `.` を日本語の読点・句点として使いません。数字とアルファベットは半角ASCIIです。JTFのスタイルガイドとスタイルチェッカーがこの組合せを明示しています。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf> <https://www.jtf.jp/jp/style_guide/jtfstylechecker.html>
- 日本語の引用は `「」`、文献名や書名は `『』`、コード・識別子・リテラルはMarkdownのバッククォートを使います。引用符の使い分けはJTFの表記を参照し、コードの正確な表記はプロジェクト契約です。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf>
- 通常の本文では、日本語とLatin文字・数字の境界に半角スペースを入れません（`TypeScriptの型`、`HTTPリクエスト`、`1つ`）。JTF v4.0の「全角・半角の間に空白を入れない」規則を採用します。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf>
- 複数語の英語固有名詞内部のスペースは保持します（`OpenTelemetry API` など）。コード、URL、製品名、既存の識別子の空白は変更しません。カタカナ複合語の中黒・スペース・無区切りには実務上の揺れがあるため、この文書では読みやすい無区切りを基本にします。JTFの一貫性の原則を参照します。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf>
- カタカナの長音は原則として `ー` を付けます。本文では `サーバー`、`ユーザー`、`パラメーター`、`コンピューター`、`メモリー` と書きます。文化庁の「外来語の表記」とJTFの表記を参照します。技術分野や既存の固有名で長音を省く慣用がある場合は、その名前を勝手に変更しません。<https://www.bunka.go.jp/kokugo_nihongo/sisaku/joho/joho/kijun/naikaku/gairai/honbun06.html> <https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf>
- JIS Z 8301に準じる技術文書では3音以上の語尾の長音を省く慣用がある一方、一般向け文書では長音を付けるべきだという整理もあります。本プロジェクトは日本の開発者向け公開文書として、本文の一般語は `サーバー` を採用し、API名・製品名・既存の型名だけ原綴りを保持します。JISの個別規格本文そのものではなく、JIS Z 8301に関する技術文書で確認した対立なので、最終的な社内用語集で再確認します。<https://www.jsa.or.jp/datas/media/10000/md_2464.pdf> <https://www.bunka.go.jp/seisaku/bunkashingikai/kokugo/kokugo_kadai/iinkai_38/pdf/92620301_02.pdf>
- Unicodeのノーブレークスペースは日本語本文に挿入しません。改行を防ぐ必要があるコード・URL・リテラルは原文のまま保持します。これはJTFの通常の空白なし方針を実装上拡張したプロジェクト規則です。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf>
- 三点リーダーは説明文ではできるだけ文を完結させます。省略記号が必要な場合は、原文にある `...` または `…` を保持し、新しい表記を追加しません。これはプロジェクトの形式ルールです。
- 日本語本文にem dash、en dash、長いダッシュを新しく使わず、文を分けるか読点で書きます。ハイフンやダッシュがAPI名、バージョン、`64-bit` などの値の一部なら保持します。これはプロジェクトの形式ルールです。JTFのチェッカーは記号の確認に使います。<https://www.jtf.jp/jp/style_guide/jtfstylechecker.html>
- コードブロックやリストの前は、短いラベルに続く場合を除き、コロンではなく完結した文と `。` を使います（`次の例を確認してください。`）。ラベルが必要なら全角コロン `：` を使い、半角 `:` を日本語の区切りとして新たに使いません。JTFは全角コロンと過剰使用の回避を示しています。<https://www.jtf.jp/jp/style_guide/jtfstylechecker.html>
- 箇条書きは同じ文法形をそろえます。語句の列挙なら句点を付けず、独立した文なら各項目を `。` で閉じます。3項目以上の列挙を本文の長い一文にしない方針は文化庁の資料に基づきます。<https://www.bunka.go.jp/seisaku/bunkashingikai/kokugo/kokugo_kadai/iinkai_41/pdf/92851101_02.pdf>
- 日本語見出しは文頭の大文字化を行わず、簡潔な名詞句を基本にして末尾の `。` を付けません。API名の大文字小文字は保持します。文化庁は見出しを簡潔にし、見出しだけで内容を把握できるようにする方針を示しています。<https://www.bunka.go.jp/seisaku/bunkashingikai/kokugo/kokugo_kadai/iinkai_41/pdf/92851101_02.pdf>
- 強調はMarkdownの `**太字**` に限定し、下線や装飾目的の全角記号を増やしません。コードは強調と混ぜず、バッククォート内を変更しません。形式と用語を一貫させる原則はGoogleの開発者向けスタイルガイドにもあります。<https://developers.google.com/style>
- 日本語本文の括弧は `（ ）` を使い、括弧の内外に不要な空白を入れません。コード、関数呼び出し、英語のAPI表記はASCIIの `()`、`[]` を保持します。全角・半角の役割分担はJTFの表記規則に基づきます。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf>

### 5. 数字・単位・日付

- 数字は半角ASCIIを使います。小数点は `.`、3桁区切りは `,`（`1,000`、`1.5`）です。JTFは数字・アルファベットを半角にし、桁区切りと小数点をこの形で示しています。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf>
- 単位は文書内で統一します。このガイドでは `1.5秒`、`20ms`、`64ビット整数`、`10%` のように、日本語と単位の境界には空白を入れません。JTFは単位の前後に空白を入れる形・入れない形のどちらも認めるため、ここはプロジェクトの一貫性ルールです。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf>
- バイト、ビット、ミリ秒などは値の意味がコードに近い場合に ASCII の単位表記を保持し、文章上の型名は `64ビット整数`、`浮動小数点数` とします。Microsoft の日本語技術文書には `64 ビット整数` の表記もあるため、空白の有無は要ネイティブ確認です。<https://learn.microsoft.com/ja-jp/windows/win32/wmisdk/numbers>
- バージョンは `0.1.0`、`1.4.3` のように原文の半角数字とピリオドを保持します。`v0.1.0` を本文で補うかどうかは原文に合わせ、数字を漢数字にしません。JTF の半角数字・句読点方針に基づくプロジェクト規則です。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf>
- 順序は `第1回`、`1番目` のようにアラビア数字を使います。助数詞は `1つ`、`2か月`、`10か所` を基本にします。JTF は技術文書の数値と助数詞の表記例を示しています。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf>
- 日付は日本語本文で `2026年10月12日`、機械可読値で `2026-10-12`、タイムスタンプで `2026-10-12T14:30:00Z` を使います。時刻は24時間制の `14:30`、タイムゾーン名は `UTC`、`JST` のままにします。ISO形式は識別子・値として保持し、日本語の日付は読み手向けに年・月・日を付けるという用途分けです。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf>
- 「例えば」は本文では `たとえば`、短いラベルでは `例` とします。`e.g.` は日本語本文で使いません。これは簡潔さと表記の一貫性を優先するプロジェクトの推奨です。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf> <https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences>

### 6. 用語

`keep English?` が `yes` なのは、SQLキーワード、JavaScriptのプロトコル名、製品名など、開発者がその英語表記を実際に使う場合だけです。API名や識別子という**概念**は日本語にし、実際のAPI名・識別子だけを原綴りで残します。`status` は `attested`（URLで表記を確認）、`inferred`（参照資料からこの文書向けに推定）、`unverified`（出典未確認の教科書的な暫定形。日本語母語話者が確認）を示します。カタカナ語は長音符号を省略せず、コード・識別子・SQL句は原文のままにします。

| English | local rendering | keep English? | status | source / note |
|---|---|---:|---|---|
| ClickHouse | ClickHouse | yes | attested | 公式日本語サイトの製品名。<https://clickhouse.com/ja> |
| ck-orm | ck-orm | yes | inferred | 共通規則第3節のプロジェクト名規則から推定。 |
| TypeScript | TypeScript | yes | attested | 公式日本語ドキュメントの表記。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| JavaScript | JavaScript | yes | attested | MDN日本語の言語名表記。<https://developer.mozilla.org/ja/docs/Web> |
| SQL | SQL | yes | attested | Microsoft SQL Server日本語ドキュメントの表記。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| JSON | JSON | yes | attested | MDN日本語のデータ形式名表記。<https://developer.mozilla.org/ja/docs/Web/JavaScript/Reference/Global_Objects/JSON> |
| Decimal | Decimal | yes | inferred | ClickHouseの型名を原綴りで扱うプロジェクト規則と公式サイトの表記から推定。<https://clickhouse.com/ja> |
| DDL | DDL | yes | inferred | SQLの技術略語を保持する共通ポリシーとMicrosoft SQL資料から推定。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| NULL | NULL | yes | attested | SQLとTypeScriptの日本語資料で原綴りを確認。<https://learn.microsoft.com/ja-jp/dotnet/csharp/advanced-topics/update-applications/nullable-migration-strategies> |
| CTE | CTE | yes | attested | SQLの用語として原綴りを保持する日本語資料。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| HTTP | HTTP | yes | attested | MDN日本語のプロトコル名表記。<https://developer.mozilla.org/ja/docs/Web/HTTP/Guides/Session> |
| UTC | UTC | yes | inferred | 時刻標準名を原綴りで保持する共通ポリシーから推定。 |
| OpenTelemetry | OpenTelemetry | yes | attested | 公式日本語ドキュメントの製品名。<https://opentelemetry.io/ja/docs/> |
| Bun | Bun | yes | inferred | 共通規則第3節の製品名規則から推定。 |
| npm | npm | yes | inferred | 共通規則第3節のツール名規則から推定。 |
| pnpm | pnpm | yes | attested | 公式日本語設定資料の製品名表記。<https://pnpm.netlify.app/ja/next/npmrc> |
| Yarn | Yarn | yes | inferred | パッケージマネージャー名を原綴りで保持する共通ポリシーから推定。 |
| Node.js | Node.js | yes | attested | MDN日本語の開発環境名表記。<https://developer.mozilla.org/ja/docs/Web/WebDriver/Reference/Errors> |
| Drizzle | Drizzle | yes | inferred | ライブラリ名を原綴りで保持する共通ポリシーから推定。 |
| GitHub | GitHub | yes | inferred | サービス名を原綴りで保持する共通ポリシーから推定。 |
| `llms.txt` | `llms.txt` | yes | inferred | ファイル名を原綴りで保持する共通ポリシーから推定。 |
| MIT | MIT | yes | inferred | ライセンス名を原綴りで保持する共通ポリシーから推定。 |
| API names | API名 | no | attested | Microsoft日本語資料の「公開API」「パブリックAPI」から推定。<https://learn.microsoft.com/ja-jp/dynamics365/supply-chain/inventory/inventory-visibility-api> |
| identifier | 識別子 | no | attested | MDN日本語のDOM・HTTP資料で確認。<https://developer.mozilla.org/ja/docs/Web/API/Document_Object_Model> |
| keywords | キーワード | no | attested | TypeScript日本語ドキュメントの用語から確認。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| SQL keywords | SQLキーワード（原綴り） | yes | attested | SQL句を原綴りで示すMicrosoft資料。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| boundary | 境界 | no | unverified | 出典未確認。モデル文の文脈に合わせた教科書的な暫定形。日本語母語話者が確認する。 |
| database | データベース | no | attested | ClickHouse日本語資料の一般用語。<https://clickhouse.com/ja> |
| schema | スキーマ | no | attested | Microsoft SQL Server日本語の定着語。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| schema DSL | スキーマDSL | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| column | カラム | no | attested | ClickHouse日本語サイトの表記。<https://clickhouse.com/ja> |
| table | テーブル | no | attested | ClickHouse日本語サイトの表記。<https://clickhouse.com/ja> |
| temporary table | 一時テーブル | no | attested | Microsoft SQL Server日本語の表記。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| table engine | テーブルエンジン | no | attested | ClickHouse日本語資料の用語。<https://clickhouse.com/ja/blog/query-your-catalog-clickhouse-cloud-jp> |
| column type | カラム型 | no | inferred | ClickHouseの「カラム」とTypeScriptの「型」を組み合わせた文脈上の形から推定。<https://clickhouse.com/ja> <https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| type definition | 型定義 | no | attested | TypeScript日本語ドキュメントの用語。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| nullable | NULL許容 | no | attested | Microsoft日本語のnull許容資料。<https://learn.microsoft.com/ja-jp/dotnet/csharp/advanced-topics/update-applications/nullable-migration-strategies> |
| optional | オプショナル | no | attested | TypeScript日本語ドキュメントの型説明から確認。<https://www.typescriptlang.org/ja/docs/handbook/type-checking-javascript-files.html> |
| required | 必須 | no | attested | OpenTelemetry日本語設定資料で確認。<https://opentelemetry.io/ja/docs/collector/internal-telemetry/> |
| default value | デフォルト値 | no | attested | React日本語ドキュメントの表記。<https://ja.react.dev/learn/passing-props-to-a-component> |
| default | デフォルト | no | attested | React日本語ドキュメントの用法から確認。<https://ja.react.dev/learn/passing-props-to-a-component> |
| modifier | 修飾子 | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| materialized column | マテリアライズドカラム | no | unverified | 出典未確認。ClickHouse固有語の教科書的な暫定形。日本語母語話者が確認する。 |
| alias column | エイリアスカラム | no | inferred | ClickHouse資料の「エイリアス」と「カラム」から推定。<https://clickhouse.com/ja> |
| array | 配列 | no | attested | React日本語ドキュメントの表記。<https://ja.react.dev/learn/passing-props-to-a-component> |
| tuple | タプル | no | attested | TypeScript日本語ドキュメントの型用語。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| map | マップ | no | attested | MDN日本語のMap APIの説明。<https://developer.mozilla.org/ja/docs/Web/JavaScript/Reference/Global_Objects/Map> |
| nested | ネスト | no | attested | ClickHouse日本語資料の型説明。<https://clickhouse.com/ja> |
| enum | 列挙型 | no | attested | TypeScript日本語ドキュメントの型用語。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| literal | リテラル | no | attested | TypeScript日本語ドキュメントの型用語。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| string | 文字列 | no | attested | TypeScript日本語ドキュメントの型用語。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| integer | 整数 | no | attested | Microsoft日本語の数値型資料。<https://learn.microsoft.com/ja-jp/dotnet/csharp/language-reference/builtin-types/integral-numeric-types> |
| numeric result | 数値結果 | no | inferred | Microsoft日本語の数値型用語と文脈上の「結果」から推定。<https://learn.microsoft.com/ja-jp/dotnet/csharp/language-reference/builtin-types/integral-numeric-types> |
| 64-bit integer | 64ビット整数 | no | attested | Microsoft日本語の数値資料。長音を付け、本文では空白を入れない。<https://learn.microsoft.com/ja-jp/windows/win32/wmisdk/numbers> |
| floating point | 浮動小数点数 | no | attested | Microsoft日本語の数値型資料。<https://learn.microsoft.com/ja-jp/dotnet/csharp/language-reference/builtin-types/integral-numeric-types> |
| boolean | 真偽値 | no | inferred | TypeScriptの型用語とReactの日本語文書での型説明から推定。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> <https://ja.react.dev/learn/passing-props-to-a-component> |
| date | 日付 | no | attested | MDN日本語のDate関連資料の一般用語。<https://developer.mozilla.org/ja/docs/Web> |
| timestamp | タイムスタンプ | no | attested | MDN日本語のWeb技術資料で確認。<https://developer.mozilla.org/ja/docs/Web> |
| timezone | タイムゾーン | no | inferred | MDNの日時関連用語とJTFのカタカナ表記から推定。<https://developer.mozilla.org/ja/docs/Web> <https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf> |
| precision | 精度 | no | attested | ClickHouse日本語資料の性能・数値説明で確認。<https://clickhouse.com/ja> |
| scale | スケール | no | unverified | 出典未確認。Decimalの教科書的な暫定形。日本語母語話者が確認する。 |
| exact value | 正確な値 | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| lossless | 精度を保つ | no | unverified | 出典未確認。文脈に合わせた教科書的な暫定形。日本語母語話者が確認する。 |
| JSON path | JSONパス | no | attested | ClickHouse日本語資料のJSON用語。<https://clickhouse.com/ja/blog/clickhouse-release-26-09-jp> |
| generic type | ジェネリック型 | no | attested | TypeScript日本語ドキュメントの型用語。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| type inference | 型推論 | no | attested | TypeScript日本語ドキュメントの用語。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| inferred | 推論される | no | attested | TypeScript日本語ドキュメントの「推測」「推論」の説明から確認。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| typed | 型付き | no | inferred | TypeScriptの型用語から文中の形を推定。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| untyped | 型なし | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| type assertion | 型アサーション | no | attested | TypeScript日本語ドキュメントの用語。<https://www.typescriptlang.org/ja/docs/handbook/2/everyday-types.html#type-assertions> |
| runtime validation | 実行時検証 | no | inferred | TypeScriptの型とMicrosoftの検証・セキュリティ資料から推定。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> <https://learn.microsoft.com/ja-jp/sql/relational-databases/security/sql-injection?view=sql-server-ver17> |
| validator | バリデーター | no | unverified | 出典未確認。長音を付けた教科書的な暫定形。日本語母語話者が確認する。 |
| encoder | エンコーダー | no | attested | Microsoft日本語の文字エンコーディング資料。長音を付ける。<https://learn.microsoft.com/ja-jp/dotnet/standard/base-types/character-encoding> |
| decoder | デコーダー | no | attested | Microsoft日本語の文字エンコーディング資料。長音を付ける。<https://learn.microsoft.com/ja-jp/dotnet/standard/base-types/character-encoding> |
| decoding | デコード | no | attested | Microsoft日本語の文字エンコーディング資料。動詞は「デコードする」。<https://learn.microsoft.com/ja-jp/dotnet/standard/base-types/character-encoding> |
| query | クエリ | no | attested | ClickHouse日本語サイトとMicrosoft SQL資料の表記。<https://clickhouse.com/ja> <https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| query builder | クエリビルダー | no | inferred | `クエリ`と開発者文書で定着した`ビルダー`を組み合わせた形から推定。<https://clickhouse.com/ja> <https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| builder | ビルダー | no | inferred | TypeScript・Reactの開発者文書でのAPI構築語彙から推定。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> <https://ja.react.dev/learn/passing-props-to-a-component> |
| chain | チェーン | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| chainable | チェーン可能 | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| subquery | サブクエリ | no | attested | Microsoftのクエリ資料の表記。<https://learn.microsoft.com/ja-jp/kusto/concepts/query-limits?view=azure-data-explorer> |
| bare subquery | エイリアスなしのサブクエリ | no | unverified | 出典未確認。意味を明示した教科書的な暫定形。日本語母語話者が確認する。 |
| anonymous CTE | 無名CTE | no | unverified | 出典未確認。`CTE`を保持した教科書的な暫定形。日本語母語話者が確認する。 |
| join | 結合 | no | attested | ClickHouse日本語資料のJOIN説明から確認。SQL句の`JOIN`は原綴り。<https://clickhouse.com/ja> |
| left join | 左結合 | no | attested | ClickHouseとSQLの日本語資料の用語から確認。SQL句の`LEFT JOIN`は原綴り。<https://clickhouse.com/ja> |
| outer join | 外部結合 | no | attested | Microsoft SQL Server日本語の用語。SQL句の`OUTER JOIN`は原綴り。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| null semantics | NULLの扱い | no | inferred | SQLのNULLと日本語技術文書の説明語彙から推定。<https://learn.microsoft.com/ja-jp/dotnet/csharp/advanced-topics/update-applications/nullable-migration-strategies> |
| predicate | 述語 | no | attested | Microsoft SQL Server日本語の用語。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| filter | フィルター | no | attested | ClickHouse日本語資料の表記。<https://clickhouse.com/ja> |
| where clause | WHERE句 | no | attested | Microsoft SQL Server日本語の表記。`WHERE`はSQLキーワード。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| group by | GROUP BY | yes | attested | SQLキーワードを原綴りで使うMicrosoft資料。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| order by | ORDER BY | yes | attested | SQLキーワードを原綴りで使うMicrosoft資料。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| limit | LIMIT | yes | attested | ClickHouse日本語資料でSQL句を原綴りで確認。<https://clickhouse.com/ja> |
| projection | プロジェクション | no | attested | ClickHouse日本語資料の機能名。<https://clickhouse.com/ja/clickhouse> |
| selection (select list) | 選択リスト | no | attested | PostgreSQL日本語資料の表記。`SELECT`はSQLキーワード。<https://www.postgresql.jp/document/16.0/html/queries-select-lists.html> |
| alias | エイリアス | no | attested | Microsoft SQL Server日本語の表記。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| expression | 式 | no | attested | Microsoft SQL Server日本語の表記。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| window expression | ウィンドウ式 | no | unverified | 出典未確認。SQL用語としての教科書的な暫定形。日本語母語話者が確認する。 |
| aggregate | 集約 | no | attested | Microsoft SQL Server日本語の表記。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| function | 関数 | no | attested | Microsoft SQL Server日本語の表記。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| table function | テーブル関数 | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| scalar | スカラー | no | attested | Microsoft SQL Server日本語の表記。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| raw SQL | 生SQL | no | unverified | 出典未確認。`SQL`を保持した教科書的な暫定形。日本語母語話者が確認する。 |
| SQL template | SQLテンプレート | no | unverified | 出典未確認。`SQL`を保持した教科書的な暫定形。日本語母語話者が確認する。 |
| SQL fragment | SQLフラグメント | no | unverified | 出典未確認。`SQL`を保持した教科書的な暫定形。日本語母語話者が確認する。 |
| parameter | パラメーター | no | attested | Microsoft日本語のクエリパラメーター資料。長音を付ける。<https://learn.microsoft.com/ja-jp/graph/query-parameters> |
| bound parameter | バインドパラメーター | no | inferred | Microsoftのパラメーター資料と日本語開発者文書の「バインド」から推定。<https://learn.microsoft.com/ja-jp/graph/query-parameters> |
| placeholder | プレースホルダー | no | inferred | Microsoftのパラメーター資料と日本語開発者文書の用語から推定。長音を付ける。<https://learn.microsoft.com/ja-jp/graph/query-parameters> |
| query parameter | クエリパラメーター | no | attested | Microsoft日本語の表記。`クエリ`に統一する。<https://learn.microsoft.com/ja-jp/graph/query-parameters> |
| compile | コンパイル | no | attested | TypeScript日本語ドキュメントの表記。動詞は「コンパイルする」。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| compiled SQL | コンパイル済みSQL | no | unverified | 出典未確認。`SQL`を保持した教科書的な暫定形。日本語母語話者が確認する。 |
| thenable | thenable | yes | attested | MDNがJavaScriptのプロトコル名として`thenable`を使用。<https://developer.mozilla.org/ja/docs/Web/JavaScript/Reference/Global_Objects/Promise> |
| helper | ヘルパー | no | inferred | React・TypeScript日本語文書の開発者語彙から推定。長音を付ける。<https://ja.react.dev/learn/passing-props-to-a-component> <https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| namespace | 名前空間 | no | attested | TypeScript日本語ドキュメントの表記。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| factory | ファクトリ | no | unverified | 出典未確認。長音を付けた教科書的な暫定形。日本語母語話者が確認する。 |
| callback | コールバック | no | attested | Vue日本語ドキュメントの表記。長音を付ける。<https://v2.ja.vuejs.org/v2/guide/instance> |
| iterator | イテレーター | no | attested | JavaScriptの反復プロトコルに関するMDN日本語の用語。<https://developer.mozilla.org/ja/docs/Web/JavaScript/Reference/Iteration_protocols> |
| variadic | 可変長引数 | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| insert | INSERT | yes | attested | ClickHouseでSQL句を原綴りで使う。<https://clickhouse.com/ja> |
| write | 書き込み | no | unverified | 出典未確認。動詞は「書き込む」とする教科書的な暫定形。日本語母語話者が確認する。 |
| batch | バッチ | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| bulk insert | 一括INSERT | no | inferred | SQLの`INSERT`を保持し、「bulk」を一括とした文脈上の形。<https://clickhouse.com/ja> |
| insert from select | INSERT ... SELECT（SELECTからの挿入） | no | unverified | 出典未確認。SQL構文を保持した教科書的な暫定形。日本語母語話者が確認する。 |
| stream | ストリーム | no | attested | MDN Streams API日本語の表記。<https://developer.mozilla.org/ja/docs/Web/API/Streams_API/Concepts> |
| streaming | ストリーミング | no | attested | MDN Streams API日本語の表記。<https://developer.mozilla.org/ja/docs/Web/API/Streams_API/Concepts> |
| row | 行 | no | attested | Microsoft SQL Server日本語の表記。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| result set | 結果セット | no | attested | Microsoft SQL Server日本語の表記。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| replacement (ReplacingMergeTree) | 置換（ReplacingMergeTree） | no | unverified | 出典未確認。`ReplacingMergeTree`は製品のエンジン名として原綴りを保持する教科書的な暫定形。日本語母語話者が確認する。 |
| versioning | バージョン管理 | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| deletion flag | 削除フラグ | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| client | クライアント | no | attested | MDN日本語のHTTP資料の表記。長音を付ける。<https://developer.mozilla.org/ja/docs/Web/HTTP/Guides/Session> |
| connection | 接続 | no | attested | MDN日本語のセッション資料の表記。<https://developer.mozilla.org/ja/docs/Web/Security/Authentication/Session_management> |
| server | サーバー | no | attested | MDN日本語のサーバーサイド資料の表記。長音を付ける。<https://developer.mozilla.org/ja/docs/Learn_web_development/Extensions/Server-side/First_steps/Introduction> |
| replica | レプリカ | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| request | リクエスト | no | attested | MDN日本語のHTTP資料の表記。長音を付ける。<https://developer.mozilla.org/ja/docs/Web/HTTP/Guides/Session> |
| response | レスポンス | no | attested | MDN日本語のResponse APIの表記。<https://developer.mozilla.org/ja/docs/Web/API/Response/Response> |
| transport | トランスポート | no | attested | OpenTelemetry日本語資料の「トランスポートプロトコル」。長音を付ける。<https://opentelemetry.io/ja/docs/collector/internal-telemetry/> |
| HTTP request | HTTPリクエスト | no | attested | MDN日本語のHTTP資料。日本語本文では空白を入れない。<https://developer.mozilla.org/ja/docs/Web/Security/Authentication/Session_management> |
| timeout | タイムアウト | no | attested | MDN日本語のWebDriver資料。<https://developer.mozilla.org/ja/docs/Web/WebDriver/Reference/Errors> |
| settings | 設定 | no | attested | Microsoft SQL Server日本語資料の「設定」「構成」。<https://learn.microsoft.com/ja-jp/sql/database-engine/configure-windows/configure-the-query-governor-cost-limit-server-configuration-option?view=sql-server-ver17> |
| configuration | 設定 | no | attested | OpenTelemetry日本語の設定資料。<https://opentelemetry.io/ja/docs/zero-code/python/configuration/> |
| config | 設定（識別子の`config`は原綴り） | no | inferred | OpenTelemetry日本語の設定資料と識別子保持規則から推定。<https://opentelemetry.io/ja/docs/zero-code/python/configuration/> |
| option | オプション | no | attested | React日本語ドキュメントの表記。長音を付ける。<https://ja.react.dev/learn/passing-props-to-a-component> |
| session | セッション | no | attested | MDN日本語のセッション管理資料。<https://developer.mozilla.org/ja/docs/Web/Security/Authentication/Session_management> |
| temporary-table workflow | 一時テーブルを使う処理 | no | unverified | 出典未確認。英語の抽象名詞を避けた教科書的な暫定形。日本語母語話者が確認する。 |
| transaction | トランザクション | no | inferred | OpenTelemetry用語集と日本語データベース文書の定着語から推定。<https://opentelemetry.io/ja/docs/concepts/glossary/> |
| concurrency | 並行性 | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| lifecycle | ライフサイクル | no | attested | Vue日本語ドキュメントの表記。<https://ja.vuejs.org/guide/introduction> |
| cleanup | クリーンアップ | no | attested | React日本語ドキュメントの表記。<https://ja.react.dev/learn/synchronizing-with-effects> |
| runtime method | ランタイムメソッド | no | unverified | 出典未確認。長音を付けた教科書的な暫定形。日本語母語話者が確認する。 |
| command | コマンド | no | attested | MDN日本語のWebDriver資料の表記。<https://developer.mozilla.org/ja/docs/Web/WebDriver/Reference/Errors> |
| execute | 実行する | no | attested | Microsoft SQL Server日本語の表記。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| fetch | 取得する | no | unverified | 出典未確認。文脈に合わせた教科書的な暫定形。日本語母語話者が確認する。 |
| abort | 中断する | no | attested | MDN日本語のAbort APIの用語。<https://developer.mozilla.org/ja/docs/Web/API/AbortController> |
| cancel | キャンセルする | no | inferred | MDNの中断APIと日本語開発者文書の用語から推定。<https://developer.mozilla.org/ja/docs/Web/API/AbortController> |
| observability | オブザーバビリティ | no | attested | ClickHouseとOpenTelemetryの公式日本語資料。<https://clickhouse.com/ja> <https://opentelemetry.io/ja/docs/> |
| tracing | トレーシング | no | attested | OpenTelemetry日本語の計装資料。<https://opentelemetry.io/ja/docs/concepts/context-propagation/> |
| span | スパン | no | attested | OpenTelemetry日本語用語集。JavaScriptのAPI名ではなく定着した専門語。<https://opentelemetry.io/ja/docs/concepts/glossary/> |
| logging | ロギング | no | attested | OpenTelemetry日本語のログ資料。<https://opentelemetry.io/ja/docs/concepts/signals/logs/> |
| logger | ロガー | no | attested | OpenTelemetry日本語のAPI資料。<https://opentelemetry.io/ja/docs/languages/java/api/> |
| metrics | メトリクス | no | attested | OpenTelemetry日本語用語集。<https://opentelemetry.io/ja/docs/concepts/glossary/> |
| instrumentation | 計装 | no | attested | OpenTelemetry日本語の計装資料。<https://opentelemetry.io/ja/docs/languages/swift/instrumentation/> |
| hook | フック | no | attested | Vue日本語ドキュメントのライフサイクルフック。<https://v2.ja.vuejs.org/v2/guide/instance> |
| event | イベント | no | attested | OpenTelemetry日本語資料のスパンイベント。<https://opentelemetry.io/ja/docs/concepts/context-propagation/> |
| error | エラー | no | attested | MDNとReactの日本語ドキュメントの表記。<https://developer.mozilla.org/ja/docs/Web/WebDriver/Reference/Errors> <https://ja.react.dev/learn/react-compiler/debugging> |
| exception | 例外 | no | attested | TypeScript・JavaScript日本語文書の一般用語。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| throws | 例外を投げる | no | unverified | 出典未確認。JavaScriptの動作を明示する教科書的な暫定形。日本語母語話者が確認する。 |
| rejects | 拒否する | no | unverified | 出典未確認。Promiseの動作を日本語化した教科書的な暫定形。日本語母語話者が確認する。 |
| error model | エラーの扱い | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| validation error | 検証エラー | no | inferred | Microsoftの検証・セキュリティ資料での「検証」と「エラー」から推定。<https://learn.microsoft.com/ja-jp/sql/relational-databases/security/sql-injection?view=sql-server-ver17> |
| stack | スタック | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| retry | リトライ | no | unverified | 出典未確認。長音を付けた教科書的な暫定形。日本語母語話者が確認する。 |
| security | セキュリティ | no | attested | MDN日本語のSecurity資料。長音を付ける。<https://developer.mozilla.org/ja/docs/Web/Security> |
| SQL injection | SQLインジェクション | no | attested | Microsoft日本語のセキュリティ資料。<https://learn.microsoft.com/ja-jp/sql/relational-databases/security/sql-injection?view=sql-server-ver17> |
| untrusted input | 信頼できない入力 | no | attested | MDN日本語のセキュリティ資料から確認。<https://developer.mozilla.org/ja/docs/Web/Security> |
| identifier quoting | 識別子のクォート | no | unverified | 出典未確認。SQL操作を明示した教科書的な暫定形。日本語母語話者が確認する。 |
| escaping | エスケープ | no | attested | Microsoft日本語のSQLインジェクション資料の用語。<https://learn.microsoft.com/ja-jp/sql/relational-databases/security/sql-injection?view=sql-server-ver17> |
| credentials | 資格情報 | no | attested | MDN日本語のCredential用語集。<https://developer.mozilla.org/ja/docs/Glossary/Credential> |
| password | パスワード | no | attested | MDN・Microsoft日本語資料の表記。<https://developer.mozilla.org/ja/docs/Web/Security/Authentication/Session_management> |
| allow-list | 許可リスト | no | attested | Microsoft日本語のセキュリティ資料の表記。<https://learn.microsoft.com/ja-jp/sql/relational-databases/security/sql-injection?view=sql-server-ver17> |
| permission | 権限 | no | attested | Microsoft日本語のアクセス許可資料。<https://learn.microsoft.com/ja-jp/partner-center/account-settings/permissions-overview> |
| installation | インストール | no | attested | TypeScript日本語ドキュメントの表記。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| install | インストールする | no | attested | TypeScript日本語ドキュメントの動詞用法。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| peer dependency | ピア依存関係 | no | attested | pnpm日本語資料の表記。<https://pnpm.netlify.app/ja/next/npmrc> |
| version | バージョン | no | attested | TypeScript日本語ドキュメントの表記。長音を付ける。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| pin a version | バージョンを固定する | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| release | リリース | no | attested | ClickHouse日本語のリリース資料の表記。<https://clickhouse.com/ja/blog/managed-postgres-notifications-observability-backups-jp> |
| breaking change | 破壊的変更 | no | attested | Microsoft .NET日本語の表記。<https://learn.microsoft.com/ja-jp/dotnet/core/compatibility/3.0> |
| before 1.0 | 1.0未満 | no | unverified | 出典未確認。数字を保った教科書的な暫定形。日本語母語話者が確認する。 |
| pre-release | プレリリース | no | inferred | TypeScript・Microsoft日本語資料のプレビュー／リリース語彙から推定。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> <https://learn.microsoft.com/ja-jp/microsoft-copilot-studio/agents-experience/authoring-agent-model-availability> |
| migration (schema migration) | マイグレーション | no | attested | Microsoft日本語の移行資料の表記。<https://learn.microsoft.com/ja-jp/dotnet/csharp/advanced-topics/update-applications/nullable-migration-strategies> |
| production | 本番環境 | no | attested | ClickHouseとMicrosoftの日本語資料の表記。<https://clickhouse.com/ja/blog/linkedin-observability-at-scale-jp> <https://learn.microsoft.com/ja-jp/microsoft-copilot-studio/agents-experience/authoring-agent-model-availability> |
| example | 例 | no | attested | React・MDN日本語ドキュメントの表記。<https://ja.react.dev/learn/tutorial-tic-tac-toe> |
| runnable | 実行可能 | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| reference | リファレンス | no | attested | MDN日本語の文書分類。<https://developer.mozilla.org/ja/docs/MDN/Writing_guidelines/What_we_write> |
| documentation | ドキュメント | no | attested | MDN日本語の文書説明。<https://developer.mozilla.org/ja/docs/Web> |
| page | ページ | no | attested | MDN日本語のページ表記。<https://developer.mozilla.org/ja/docs/Web/API/Document_Object_Model> |
| section | セクション | no | attested | MDN日本語の目次・節見出し。<https://developer.mozilla.org/ja/docs/Web> |
| caveat | 注意点 | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| note | 注記 | no | inferred | MDN日本語の「メモ」と技術文書の注記用法から推定。<https://developer.mozilla.org/ja/docs/MDN/Writing_guidelines/What_we_write> |
| behavior | 動作 | no | attested | React日本語ドキュメントの表記。<https://ja.react.dev/learn/react-compiler/debugging> |
| semantics | 意味 | no | unverified | 出典未確認。教科書的な暫定形。`NULL semantics`は`NULLの扱い`とする。日本語母語話者が確認する。 |
| contract | 契約 | no | unverified | 出典未確認。APIの契約を表す教科書的な暫定形。日本語母語話者が確認する。 |
| mode | モード | no | attested | React日本語ドキュメントの開発モード表記。<https://ja.react.dev/learn/synchronizing-with-effects> |
| public API | 公開API | no | attested | Microsoft日本語資料の「公開API」「パブリックAPI」。<https://learn.microsoft.com/ja-jp/dynamics365/supply-chain/inventory/inventory-visibility-api> |
| stable | 安定版 | no | attested | OpenTelemetry日本語のstable signalの表記。<https://opentelemetry.io/ja/docs/concepts/signals/logs/> |
| internal | 内部 | no | attested | OpenTelemetry日本語の内部テレメトリー資料。<https://opentelemetry.io/ja/docs/collector/internal-telemetry/> |
| deprecated | 非推奨 | no | attested | TypeScript日本語ドキュメントのAPI用語。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| shape | 形 | no | attested | React日本語ドキュメントのデータ構造の説明から確認。<https://ja.react.dev/learn/tutorial-tic-tac-toe> |
| surface | APIの公開範囲 | no | unverified | 出典未確認。`surface`を直訳しない教科書的な暫定形。日本語母語話者が確認する。 |
| layer | レイヤー | no | attested | ClickHouse日本語資料のアーキテクチャ用語。<https://clickhouse.com/ja/clickhouse> |
| keeps X easy | Xを使いやすくする | no | unverified | 出典未確認。英語の形を直訳しない教科書的な暫定形。日本語母語話者が確認する。 |
| without hiding | 隠さずに | no | unverified | 出典未確認。教科書的な暫定形。日本語母語話者が確認する。 |
| make everyday access easier to structure | 普段使う処理を組み立てやすくする | no | unverified | 出典未確認。英語の抽象名詞を重ねない教科書的な暫定形。日本語母語話者が確認する。 |
| is rejected | 受け付けない | no | unverified | 出典未確認。対象に合わせた教科書的な暫定形。日本語母語話者が確認する。 |
| is not validated | 検証されない | no | unverified | 出典未確認。受動の意味を保った教科書的な暫定形。日本語母語話者が確認する。 |
| is decoded conservatively | 精度を失わないようにデコードする | no | unverified | 出典未確認。数値の意味を明示した教科書的な暫定形。日本語母語話者が確認する。 |
| carries | 渡す／保持する | no | unverified | 出典未確認。データフローに応じて使い分ける教科書的な暫定形。日本語母語話者が確認する。 |
| exposes | 提供する／公開する | no | unverified | 出典未確認。APIの動作に応じて使い分ける教科書的な暫定形。日本語母語話者が確認する。 |
| follows | 従う | no | unverified | 出典未確認。規則・方針の文脈で使う教科書的な暫定形。日本語母語話者が確認する。 |
| stays | そのままにする／保つ | no | unverified | 出典未確認。状態変化の有無に応じて使い分ける教科書的な暫定形。日本語母語話者が確認する。 |
| leaves | 任せる／残す | no | unverified | 出典未確認。操作主体に応じて使い分ける教科書的な暫定形。日本語母語話者が確認する。 |
| pass | 渡す | no | unverified | 出典未確認。引数・値の文脈で使う教科書的な暫定形。日本語母語話者が確認する。 |
| wrap | 包む／ラップする | no | unverified | 出典未確認。概念なら「包む」、API動作なら「ラップする」とする暫定形。日本語母語話者が確認する。 |
| compose | 組み合わせる | no | unverified | 出典未確認。クエリや関数の構成を表す教科書的な暫定形。日本語母語話者が確認する。 |
| accepts | 受け付ける | no | unverified | 出典未確認。入力・引数の受付を表す教科書的な暫定形。日本語母語話者が確認する。 |
| returns | 返す | no | unverified | 出典未確認。戻り値・レスポンスの文脈で使う教科書的な暫定形。日本語母語話者が確認する。 |

### 7. 表現チェック

ここでの `WARN` は、別の文脈では成立する表記を、このガイドでは優先しないという意味です。`ERR` は製品名や言語名を勝手に日本語化した場合だけに使います。長音の差は実際の日本語資料に両方の形があるため、自然な引用や固有名を誤って止めないよう `WARN` にします。

| Do not use | Preferred | Level | Why / source |
|---|---|---|---|
| ユーザ | ユーザー | WARN | `ユーザ`はIPAの用語集に現れる一方、JTFは「ユーザー」を標準形として「ユーザ」と対比している。bad source: <https://www.ipa.go.jp/archive/publish/qv6pgp0000000xdn-att/000005114.pdf>; preferred source: <https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf> |
| コンピュータ | コンピューター | WARN | `コンピュータ`はIPA資料に現れる一方、JTFは「コンピューター」を標準形として「コンピュータ」と対比している。bad source: <https://www.ipa.go.jp/archive/publish/qv6pgp0000000xdn-att/000005114.pdf>; preferred source: <https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf> |
| サーバ | サーバー | WARN | `サーバ`はIPAのセキュリティ資料に現れる一方、MDNは「サーバー」を使い、JTFも長音を原則省略しない。bad source: <https://www.ipa.go.jp/archive/security/jisec/software/certified-cert/c0160_rpt.pdf>; preferred sources: <https://developer.mozilla.org/ja/docs/Learn_web_development/Extensions/Server-side/First_steps/Introduction> <https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf> |

次の表現は、この本文では `WARN` として避けます。文脈上自然なら機械的に誤りとは扱いません。

| Do not use | Preferred | Why / source |
|---|---|---|
| および | `、` / と | Microsoftの簡潔な語の原則に合わせ、自然な列挙を優先する。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> |
| 再度 | もう一度 | Microsoftの簡潔な語の原則に合わせ、日常語を優先する。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> |
| 詳細な | 詳しい | 名詞を重ねず、意味の明確な語を選ぶ。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> |
| 未検証 | 検証されていません | 状態を読み手に分かる文で示す。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> |
| することができます | できます | 可能表現を短い動詞にする。<https://learn.microsoft.com/ja-jp/style-guide/word-choice/use-simple-words-concise-sentences> |

次の訳は、製品名・言語名・データ形式名を日本語化しているため `ERR` です。

| Do not use | Preferred | Why / source |
|---|---|---|
| クリックハウス | ClickHouse | ClickHouse公式日本語サイトでも製品名は`ClickHouse`のまま。<https://clickhouse.com/ja> |
| タイプスクリプト | TypeScript | TypeScript公式日本語ドキュメントの製品名表記。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> |
| ジャバスクリプト | JavaScript | MDN日本語の言語名表記。<https://developer.mozilla.org/ja/docs/Web> |
| エスキューエル | SQL | Microsoft SQL Server日本語資料でSQLは原綴り。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16> |
| ジェイソン | JSON | MDN日本語のデータ形式名表記。<https://developer.mozilla.org/ja/docs/Web/JavaScript/Reference/Global_Objects/JSON> |

### 8. モデル文

#### メンタルモデル

`ck-orm`は、次の境界を意識して使います。

- `ckType.*`でスキーマのカラム型を定義します。
- `ckTable(...)`でテーブルのスキーマを定義します。
- `ck.*`はクエリヘルパーの名前空間です。
- `fn.*`はSQL関数ヘルパーの名前空間です。
- スキーマはテーブルとカラムを表しますが、データベース名は表しません。
- データベース接続は`clickhouseClient(...)`に設定します。
- ビルダーのクエリを基本の経路として使い、ClickHouse固有の式には生SQLも使えます。
- `runInSession()`はClickHouseのセッションヘルパーであり、トランザクションではありません。
- `leftJoin()`はデフォルトでSQLと同じNULLの扱いを使います。
- 桁数が大きい、または精度を失うおそれのある数値結果は、暗黙に変換せず、精度を保ってデコードします。

#### インストール

ck-ormのピア依存関係は`@opentelemetry/api`の1つだけです。Bun、npm、pnpmでは自動でインストールされますが、Yarnでは自動で入らないため、Yarnのコマンドに明示的に追加します。

### 9. 参照した資料

- JTF日本語標準スタイルガイド v4.0: 長音、空白、句読点、数字、見出し。<https://www.jtf.jp/jp/style_guide/pdf/jtf_style_guide.pdf>
- 文化庁「外来語の表記」: カタカナ語の長音。<https://www.bunka.go.jp/kokugo_nihongo/sisaku/joho/joho/kijun/naikaku/gairai/honbun06.html>
- MDN日本語: HTTP、セッション、リクエスト、レスポンス、タイムアウト、セキュリティ、資格情報、ドキュメント。<https://developer.mozilla.org/ja/docs/Web/Security/Authentication/Session_management> <https://developer.mozilla.org/ja/docs/Web/WebDriver/Reference/Errors> <https://developer.mozilla.org/ja/docs/Web/Security> <https://developer.mozilla.org/ja/docs/Glossary/Credential> <https://developer.mozilla.org/ja/docs/MDN/Writing_guidelines/What_we_write>
- TypeScript日本語ドキュメント: 型、型推論、ジェネリック、型アサーション、コンパイル、非推奨。<https://www.typescriptlang.org/ja/docs/handbook/intro-to-js-ts.html> <https://www.typescriptlang.org/ja/docs/handbook/2/everyday-types.html#type-assertions>
- React日本語ドキュメント: デフォルト値、配列、動作、クリーンアップ、ページ、例。<https://ja.react.dev/learn/passing-props-to-a-component> <https://ja.react.dev/learn/synchronizing-with-effects> <https://ja.react.dev/learn/react-compiler/debugging> <https://ja.react.dev/learn/tutorial-tic-tac-toe>
- Vue.js日本語ドキュメント: コンポーネント、ライフサイクル、フック、コールバック。<https://ja.vuejs.org/guide/introduction> <https://v2.ja.vuejs.org/v2/guide/instance>
- Microsoft SQL Server日本語: スキーマ、テーブル、クエリ、WHERE句、集約、スカラー、行、結果セット。<https://learn.microsoft.com/ja-jp/sql/relational-databases/query-processing-architecture-guide?view=sql-server-ver16>
- Microsoft日本語: パラメーター、権限、セキュリティ、破壊的変更、公開API、本番環境。<https://learn.microsoft.com/ja-jp/graph/query-parameters> <https://learn.microsoft.com/ja-jp/partner-center/account-settings/permissions-overview> <https://learn.microsoft.com/ja-jp/sql/relational-databases/security/sql-injection?view=sql-server-ver17> <https://learn.microsoft.com/ja-jp/dotnet/core/compatibility/3.0> <https://learn.microsoft.com/ja-jp/dynamics365/supply-chain/inventory/inventory-visibility-api> <https://learn.microsoft.com/ja-jp/microsoft-copilot-studio/agents-experience/authoring-agent-model-availability>
- ClickHouse日本語: クエリ、カラム、テーブルエンジン、プロジェクション、JSONパス、オブザーバビリティ。<https://clickhouse.com/ja> <https://clickhouse.com/ja/clickhouse> <https://clickhouse.com/ja/blog/query-your-catalog-clickhouse-cloud-jp> <https://clickhouse.com/ja/blog/clickhouse-release-26-09-jp>
- OpenTelemetry日本語: オブザーバビリティ、トレーシング、スパン、ログ、メトリクス、計装、トランスポート。<https://opentelemetry.io/ja/docs/> <https://opentelemetry.io/ja/docs/concepts/glossary/> <https://opentelemetry.io/ja/docs/concepts/context-propagation/> <https://opentelemetry.io/ja/docs/collector/internal-telemetry/>
- pnpm日本語: ピア依存関係。<https://pnpm.netlify.app/ja/next/npmrc>
- IPA資料: 長音を省略した「ユーザ」「コンピュータ」「サーバ」の実例。<https://www.ipa.go.jp/archive/publish/qv6pgp0000000xdn-att/000005114.pdf> <https://www.ipa.go.jp/archive/security/jisec/software/certified-cert/c0160_rpt.pdf>

### 10. 未解決の点

- `クエリ`と`クエリー`のどちらを日本のTypeScript・ClickHouse開発者がこのプロジェクトで最も自然と感じるか。公式資料では両方の表記があるため、全体の検索性も含めて母語話者が確認する。
- `カラム`と`列`の使い分け。ClickHouse資料では両方が現れるため、SQLの一般概念とClickHouse固有の説明で`カラム`に統一してよいか確認する。
- `スキーマDSL`、`マテリアライズドカラム`、`エイリアスなしのサブクエリ`、`ウィンドウ式`、`生SQL`が、日本の開発者向け文書として自然か確認する。
- `クエリビルダー`、`チェーン可能`、`バインドパラメーター`、`APIの公開範囲`の語感と、文脈ごとの使い分けを確認する。
- `64ビット整数`の空白なし表記と、`HTTPリクエスト`の空白なし表記が、このサイトの表示幅・検索性に適しているか確認する。



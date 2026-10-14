import type { Strings } from "../i18n";

export default {
  site: {
    nav: "サイト",
    examples: "例",
    language: "言語",
    closeNav: "ナビゲーションを閉じる",
    license: "MITライセンス",
  },
  theme: {
    label: "テーマ：{mode}。{next}に切り替えます。",
    auto: "自動",
    light: "ライト",
    dark: "ダーク",
  },
  code: {
    copy: "コピー",
    copied: "コピーしました",
    copyFailed: "コピーできませんでした",
    variants: "コードのバリアント",
    sectionLink: "このセクションへのリンク",
  },
  docs: {
    title: "ck-orm：TypeScript向けの型安全なClickHouse",
    description:
      "ck-ormは、新しいJavaScriptランタイム向けの型付きClickHouseクエリレイヤーです。スキーマDSL、クエリビルダー、生SQLテンプレート、セッション、オブザーバビリティを備えています。",
    skip: "リファレンスへ移動",
    tocLabel: "リファレンス",
    openNav: "リファレンスナビゲーションを開く",
    navLabel: "リファレンスナビゲーション",
    badge: "0.1.0より前のバージョンでは、リリースでAPIが壊れる可能性があります。正確なバージョンを固定してください。",
    headline: "型安全なClickHouse、ClickHouseを隠さない",
    intro:
      "ck-ormは、新しいJavaScriptランタイムでClickHouseを扱う、スキーマDSL、型付きクエリビルダー、SQLテンプレートレイヤーです。クエリは読みやすいClickHouse SQLにコンパイルされ、値は型付きパラメーターとして渡されます。64ビット整数とDecimalは正確な文字列にデコードします。",
    ctaInstall: "インストールから始める",
    ctaExamples: "例を見る",
  },
  examples: {
    title: "例：ck-orm",
    description: "実行可能なck-ormの例です。各例がClickHouseへ送るSQLをそのまま確認できます。",
    skip: "例へ移動",
    tocLabel: "例",
    openNav: "例のナビゲーションを開く",
    navLabel: "例のナビゲーション",
    heading: "例",
    intro:
      "各例は{folder}にある実行可能なプロジェクトで、{main}と{package}があります。このページでは、そのファイルをそのまま表示します。ビルドではck-ormを通して実行するので、SQLタブにClickHouseが受け取るリクエストがすべて表示されます。各ファイル末尾のコメントには、その例が出力する行が書かれています。",
    items: {
      "decimal-sum": {
        title: "正確なDecimal合計",
        summary: "Decimal(20, 5)カラムを合計し、合計を文字列としてデコードするので、浮動小数点数を決して経由しません。",
      },
      "literal-search": {
        title: "安全なリテラル検索",
        summary:
          '値は型付きクエリパラメーターとして渡され、SQLには決して埋め込まれません。ck.containsは%と_をエスケープするので、"alpha%"はリテラルとして一致します。',
      },
      "left-join": {
        title: "NULL付き左結合",
        summary:
          "leftJoin()はjoin_use_nulls = 1を送るので、一致しなかった右側のカラムはClickHouseではNULL、行の型ではnullです。",
      },
      "website-analytics": {
        title: "ウェブサイト分析",
        summary: "UTCの[start, end)区間内で、単一サイトのページビュー数と正確なユニーク訪問者数をパス別に集計します。",
        notes: "セッション一時テーブルを使用します。同じ訪問者が複数のパスでカウントされることがあります。",
      },
      "asof-price": {
        title: "ASOF JOINによる最新気配値",
        summary:
          "ASOF LEFT JOINで、各取引を同じシンボルの気配値のうち、取引時刻以前で最も新しいものに対応付けます。Decimalの価格は文字列のまま保持され、過去の気配値がない場合はnullを返します。",
        notes: "セッション一時テーブルを使用します。",
      },
      "dynamic-signals": {
        title: "Mapシグナルのフィルタリング",
        summary:
          "Map(String, Float64)に格納された変化するモデルシグナルを、キーの存在を確認してから境界値を含むしきい値でフィルタリングします。格納されたゼロも実際のスコアのままです。",
        notes: "セッション一時テーブルを使用します。",
      },
      "trace-list": {
        title: "フィルターと総件数付きのトレース一覧",
        summary:
          "任意の条件から1つのフィルターリストを作り、LIMIT/OFFSETによるページ取得と総件数の集計の両方に使います。最終ページより後のページを指定しても総件数を返します。",
        notes: "セッション一時テーブルを使用します。",
      },
      "support-tickets": {
        title: "型付きカラムと実行時検証",
        summary:
          "Enum8カラム、$typeのブランド型、$typeで指定したJSONの形で行の型を絞り込みます。手書きのStandard Schemaバリデーターはメールアドレスを正規化し、無効なアドレスは何も送信する前に拒否します。",
        notes: "セッション一時テーブルを使用します。",
      },
      "bluesky-events": {
        title: "JSONイベントペイロードのクエリ",
        summary:
          "ClickHouse JSONBenchのクエリに沿って、型付きのJSONパスでBlueskyイベントをコレクション別に数え、キャストした動的パスとJSONサブオブジェクトで投稿を読み取ります。",
        notes: "セッション一時テーブルを使用します。",
      },
      "cdc-rollup": {
        title: "ClickHouse内でのCDC行の集約",
        summary:
          "Postgres CDCの行をFINALで重複排除し、INSERT ... SELECTで日次売上に集約します。JavaScriptに戻るのは最終結果の行だけです。",
        notes: "セッション一時テーブルを使用します。",
      },
      "usage-metering": {
        title: "使用量計測",
        summary:
          "複数の挿入バッチにまたがる使用量を、テナント、ユーザー、UTCの分単位で、挿入時にトリガーされるマテリアライズドビューを使って集計します。",
        notes:
          "CREATE、SELECT、INSERT、DROPの各権限がある使い捨てのデータベースで、直列に実行してください。固定名の永続テーブルとマテリアライズドビューを使用し、作成に成功したオブジェクトだけを削除します。プロセスが中断した場合やCREATEのレスポンスが失われた場合は、オブジェクトが残り、手動での確認が必要になることがあります。",
      },
      "request-latency": {
        title: "リクエストレイテンシーのパーセンタイル",
        summary:
          "成功したリクエストの件数、加重平均レイテンシー、正確なp95/p99を、テナント、操作、UTCの分単位で、挿入時にトリガーされるマテリアライズドビューを使って集計します。",
        notes:
          "CREATE、SELECT、INSERT、DROPの各権限がある使い捨てのデータベースで、直列に実行してください。作成に成功した固定名のテーブルとマテリアライズドビューだけを削除します。プロセスが中断した場合やCREATEのレスポンスが失われた場合は、オブジェクトが残ることがあります。正確な分位点の集計状態はすべてのサンプルを保持するため、本番ワークロードではメモリ使用量を評価してください。",
      },
      "query-plugins": {
        title: "クエリプラグイン",
        summary:
          "プラグインがすべてのリクエストにヘッダーを追加し、ck-ormが送るステートメントを送信順にログへ出力します。セッション一時テーブルのCREATEとDROPも含まれます。",
        notes: "セッション一時テーブルを使用します。",
      },
      "otel-tracing": {
        title: "OpenTelemetryトレーシング",
        summary:
          "ck-orm/otelのotelTracing()は、リクエストごとにスパンを1つ記録します。この例ではメモリ内エクスポーターを使ってOpenTelemetry SDKを起動し、スパンを出力します。出力されるスパンは、例自身のスパンの子になっています。",
        notes: "セッション一時テーブルを使用します。",
      },
    },
    tabsLabel: "{title}：ソースとSQL",
  },
} satisfies Strings;

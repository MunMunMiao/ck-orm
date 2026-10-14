import type { Strings } from "../i18n";

export default {
  site: {
    nav: "サイト",
    examples: "例",
    language: "言語",
    closeNav: "ナビゲーションを閉じる",
    license: "MITライセンス",
    editPage: "GitHubでこのページを編集",
    llmsFooter: "AIエージェント向けllms.txt",
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
    details: {
      "website-analytics":
        "UTCの[start, end)区間内における単一サイトのページビュー数と正確なユニーク訪問者数をパス別に集計します。セッション一時テーブルを使用します。同じ訪問者が複数のパスでカウントされることがあります。",
      "usage-metering":
        "複数の挿入バッチにまたがる使用量を、テナント、ユーザー、UTCの分単位で集計します。CREATE、SELECT、INSERT、DROPの各権限がある使い捨てのデータベースで、直列に実行してください。固定名の永続テーブルとマテリアライズドビューを使用し、作成に成功したオブジェクトだけを削除します。プロセスが中断した場合やCREATEの応答が失われた場合は、オブジェクトが残り、手動での確認が必要になることがあります。",
      "request-latency":
        "成功したリクエストの件数、加重平均レイテンシー、正確なp95/p99を、テナント、操作、UTCの分単位で集計します。CREATE、SELECT、INSERT、DROPの各権限がある使い捨てのデータベースで、直列に実行してください。作成に成功した固定名のテーブルとマテリアライズドビューだけを削除します。プロセスが中断した場合やCREATEの応答が失われた場合は、オブジェクトが残ることがあります。正確な分位点の集計状態はすべてのサンプルを保持するため、本番ワークロードではメモリ使用量を評価してください。",
      "asof-price":
        "ASOF LEFT JOINで、各取引を同じシンボルの気配値のうち、取引時刻以前で最も新しいものに対応付けます。Decimalの価格は文字列のまま保持され、過去の気配値がない場合はnullを返します。セッション一時テーブルを使用します。",
      "dynamic-signals":
        "Map(String, Float64)に格納された変化するモデルシグナルを、キーの存在を確認してから境界値を含むしきい値でフィルタリングします。格納されたゼロも有効なスコアとして扱われます。セッション一時テーブルを使用します。",
    },
    tabsLabel: "{title}：ソースとSQL",
  },
} satisfies Strings;

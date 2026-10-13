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
      "各例は{folder}にある実行可能なプロジェクトで、{main}と{package}があります。このページでは、そのファイルをそのまま表示します。SQLタブには、その例がClickHouseに送るリクエストがすべて表示され、その例のユニットテストがこのSQLを検証しています。各ファイル末尾のコメントには、その例が出力する行が書かれています。",
    runIntro: "各例はセッション専用の一時テーブルを作成するため、どのClickHouseサーバーでも実行できます：",
    tabsLabel: "{title}：ソースとSQL",
  },
} satisfies Strings;

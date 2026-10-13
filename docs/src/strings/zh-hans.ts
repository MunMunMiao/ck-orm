import type { Strings } from "../i18n";

export default {
  site: {
    nav: "站点导航",
    examples: "示例",
    language: "语言",
    closeNav: "关闭导航",
    license: "MIT 许可证",
    editPage: "在 GitHub 上编辑本页",
    llmsFooter: "AI 智能体用的 llms.txt",
  },
  theme: {
    label: "主题：{mode}。切换到 {next}。",
    auto: "自动",
    light: "浅色",
    dark: "深色",
  },
  code: {
    copy: "复制",
    copied: "已复制",
    copyFailed: "复制失败",
    variants: "代码变体",
    sectionLink: "链接到此章节",
  },
  docs: {
    title: "ck-orm — 面向 TypeScript 的类型安全 ClickHouse",
    description:
      "ck-orm 是面向现代 JavaScript 运行时的带类型 ClickHouse 查询层，提供模式 DSL、查询构建器、原始 SQL 模板、会话和可观测性。",
    skip: "跳到参考",
    tocLabel: "参考",
    openNav: "打开参考导航",
    navLabel: "参考导航",
    badge: "0.1.0 之前的发布可能破坏 API。请固定确切版本。",
    headline: "类型安全 ClickHouse，不隐藏 ClickHouse",
    intro:
      "ck-orm 是面向现代 JavaScript 运行时的 ClickHouse 模式 DSL、带类型查询构建器和 SQL 模板层。查询会编译成易读的 ClickHouse SQL，值以带类型参数传递，64 位整数和 Decimal 会解码为精确字符串。",
    ctaInstall: "从安装开始",
    ctaExamples: "查看示例",
  },
  examples: {
    title: "示例 — ck-orm",
    description: "可运行的 ck-orm 示例，并展示每个示例发送给 ClickHouse 的确切 SQL。",
    skip: "跳到示例",
    tocLabel: "示例",
    openNav: "打开示例导航",
    navLabel: "示例导航",
    heading: "示例",
    intro:
      "每个示例都是 {folder} 中一个可运行的项目，包含 {main} 和 {package}。这里展示的正是这个文件。SQL 标签页会列出它发给 ClickHouse 的每个请求，示例自己的单元测试会检查这些 SQL。每个文件末尾的注释会列出它打印的行。",
    runIntro: "每个示例都会创建自己的会话临时表，因此可以在任何 ClickHouse 服务器上运行：",
    tabsLabel: "{title}：源代码和 SQL",
  },
} satisfies Strings;

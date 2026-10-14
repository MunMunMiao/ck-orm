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
      "每个示例都是 {folder} 中一个可运行的项目，包含 {main} 和 {package}。这里展示的正是这个文件。构建会用 ck-orm 运行它，所以 SQL 标签页会列出 ClickHouse 收到的每个请求。每个文件末尾的注释会列出它打印的行。",
    details: {
      "website-analytics":
        "在 UTC [start, end) 时间范围内，按路径统计单个站点的页面浏览量和精确独立访客数。使用会话临时表；不同路径的访客可能重叠。",
      "usage-metering":
        "按租户、用户和 UTC 分钟合并多个插入批次的用量。请在可随时清理的测试数据库内串行运行，并授予 CREATE、SELECT、INSERT 和 DROP 权限。使用固定名称的持久表及物化视图，仅清理已成功创建的对象。进程中断或 CREATE 响应丢失后，可能需要手动检查残留对象。",
      "request-latency":
        "按租户、操作和 UTC 分钟聚合成功请求数、加权平均延迟及精确 p95/p99。请在可随时清理的测试数据库内串行运行，并授予 CREATE、SELECT、INSERT 和 DROP 权限。仅清理已成功创建的固定名称表和物化视图；进程中断或 CREATE 响应丢失可能留下对象。精确分位数状态保留全部样本，生产使用时需评估内存占用。",
      "asof-price":
        "使用 ASOF LEFT JOIN 为每笔交易匹配同一交易品种在该时刻或之前的最新报价。Decimal 价格保持为字符串，缺少历史报价时返回 null。使用会话临时表。",
      "dynamic-signals":
        "筛选 Map(String, Float64) 中变化的模型信号，先检查键是否存在，再应用包含边界值的阈值。已存储的零仍然是真实分数。使用会话临时表。",
    },
    tabsLabel: "{title}：源代码和 SQL",
  },
} satisfies Strings;

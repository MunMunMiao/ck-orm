import type { Strings } from "../i18n";

export default {
  site: {
    nav: "站点导航",
    examples: "示例",
    language: "语言",
    closeNav: "关闭导航",
    license: "MIT 许可证",
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
    items: {
      "decimal-sum": {
        title: "精确 Decimal 合计",
        summary: "对 Decimal(20, 5) 列求和，并把合计解码为字符串，因此合计绝不会经过浮点数。",
      },
      "literal-search": {
        title: "安全字面文本搜索",
        summary:
          '值以带类型的查询参数传递，绝不会拼接进 SQL；ck.contains 会转义 % 和 _，因此 "alpha%" 按字面文本匹配。',
      },
      "left-join": {
        title: "带 NULL 的左连接",
        summary: "leftJoin() 会发送 join_use_nulls = 1，因此未匹配的右侧列在 ClickHouse 中为 NULL，在行类型中为 null。",
      },
      "website-analytics": {
        title: "网站分析",
        summary: "在 UTC [start, end) 时间范围内，按路径统计单个站点的页面浏览量和精确独立访客数。",
        notes: "使用会话临时表；不同路径的访客可能重叠。",
      },
      "asof-price": {
        title: "ASOF JOIN 最新报价",
        summary:
          "使用 ASOF LEFT JOIN 为每笔交易匹配同一交易品种在该时刻或之前的最新报价。Decimal 价格保持为字符串，缺少历史报价时返回 null。",
        notes: "使用会话临时表。",
      },
      "dynamic-signals": {
        title: "Map 信号过滤",
        summary:
          "过滤 Map(String, Float64) 中变化的模型信号，先检查键是否存在，再应用包含边界值的阈值。已存储的零仍然是真实分数。",
        notes: "使用会话临时表。",
      },
      "trace-list": {
        title: "带过滤条件和总数的追踪列表",
        summary:
          "用可选条件构建一份过滤条件列表，LIMIT/OFFSET 分页查询和总数统计共用这份列表，因此即使页码超过最后一页，仍会返回总数。",
        notes: "使用会话临时表。",
      },
      "support-tickets": {
        title: "带类型的列与运行时验证",
        summary:
          "Enum8 列、$type 品牌类型和 $type JSON 结构会收窄行类型；手写的 Standard Schema 验证器会规范化邮箱地址，并在发送任何内容之前拒绝无效地址。",
        notes: "使用会话临时表。",
      },
      "bluesky-events": {
        title: "查询 JSON 事件载荷",
        summary:
          "按照 ClickHouse JSONBench 的查询，通过带类型的 JSON 路径按集合统计 Bluesky 事件，再用经过类型转换的动态路径和 JSON 子对象读取帖子。",
        notes: "使用会话临时表。",
      },
      "cdc-rollup": {
        title: "在 ClickHouse 内汇总 CDC 行",
        summary:
          "用 FINAL 对 Postgres CDC 行去重，再用 INSERT ... SELECT 汇总为每日收入，因此只有最终结果行会返回给 JavaScript。",
        notes: "使用会话临时表。",
      },
      "usage-metering": {
        title: "用量计量",
        summary: "用插入触发的物化视图，按租户、用户和 UTC 分钟，跨插入批次聚合用量。",
        notes:
          "请在可丢弃的数据库内串行运行，需要 CREATE、SELECT、INSERT 和 DROP 权限。使用固定名称的持久表及物化视图，仅清理已成功创建的对象。进程中断或 CREATE 响应丢失后，可能需要手动检查残留对象。",
      },
      "request-latency": {
        title: "请求延迟百分位数",
        summary: "用插入触发的物化视图，按租户、操作和 UTC 分钟聚合成功请求数、加权平均延迟及精确 p95/p99。",
        notes:
          "请在可丢弃的数据库内串行运行，需要 CREATE、SELECT、INSERT 和 DROP 权限。仅清理已成功创建的固定名称表和物化视图；进程中断或 CREATE 响应丢失可能留下对象。精确分位数状态保留全部样本，因此需要为生产环境中的工作负载评估内存占用。",
      },
    },
    tabsLabel: "{title}：源代码和 SQL",
  },
} satisfies Strings;

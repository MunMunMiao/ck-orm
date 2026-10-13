## zh-Hans - 简体中文

> 本文件只规定 `docs/reference.md` 的简体中文写法。它不改变代码、命令、URL、标识符、API 名称或 Markdown 结构。

### 1. 变体与读者

- 目标变体是 `zh-Hans` 书面简体中文。IANA 的语言标签注册资料列出 `zh-Hans`；Unicode CLDR 提供 locale 和 likely-subtag 数据。它们只确定变体范围，不替我们决定每个技术词的译法。
  - https://www.iana.org/assignments/language-tags
  - https://www.unicode.org/reports/tr35/
- 读者是会写 TypeScript、SQL 和 ClickHouse 的开发者。他们需要能直接执行的说明，不需要公文式背景铺垫。
- 术语优先参考简体中文的 React、Vue、TypeScript、MDN、Microsoft 和 OpenTelemetry 页面。页面只支持它实际出现的词形；没有直接词形证据的行标为 `unverified`。

### 2. 语气与句法

用短句、主动句和明确主语。先说动作和结果，再补限制；同一段只承载一个主要动作。可以使用“你”，但不要命令式地训话。

| BAD | GOOD |
| --- | --- |
| 该函数用于对结果进行处理。 | 这个函数用来处理结果。 |
| 在配置无效的情况下，应当予以报错。 | 配置无效时，直接返回错误。 |
| 通过重试来恢复连接。 | 重试可以恢复连接。 |
| 请注意，务必先执行迁移。 | 先运行迁移，再启动应用。 |

依据：Microsoft 的中文技术写作建议快速进入重点，Google 的开发者风格指南要求清楚、直接、少用客套话。

- https://learn.microsoft.com/zh-cn/style-guide/top-10-tips-style-voice
- https://developers.google.cn/style/tone

### 3. 避免的中式表达

这些是优先改写的模式，不是对每个词的机械禁用。若技术语义确实需要保留，保留准确性并可不改。

| 避免 | 推荐 | 说明 |
| --- | --- | --- |
| 进行查询、进行安装、进行配置 | 查询、安装、配置 | 动词本身已经足够。 |
| 实现……功能 | 支持……、完成…… | 避免空泛的“实现功能”。 |
| 针对……进行处理 | 处理…… | 直接说动作和对象。 |
| 在……的情况下 | ……时 | 条件简单时不用套话。 |
| 通过……来…… | 用……；……可以…… | “通过”和“来”不要成套出现。 |
| 对于……来说 | ……；对…… | 按语义选择主语或介词。 |
| 被……所…… | ……；由…… | 删除多余的“被……所”。 |
| 其、该、此（连续出现） | 这个、它、名词本身 | 让指代对象可见。 |
| 从而 | 因此；这样就 | 只有确有因果时才保留。 |
| 具备……能力 | 能……；支持…… | 用动词表达能力。 |
| 如……所示 | 如下；见…… | 直接指向内容。 |
| 请注意 | 注意……；……需要…… | 把提醒改成事实或动作。 |
| 应当、务必（连续命令） | 需要；请先…… | 只在确有强制性时使用。 |
| 并非不能不…… | 可以……；不能…… | 避免双重否定。 |
| 基于……进行…… | 根据……；用…… | 删除名词化套句。 |
| 予以、加以、作出处理 | 处理；返回；拒绝 | 换成具体动词。 |

### 4. 标点、空格与排版

- 中文正文使用全角中文标点：`，。！？：；`。中文标点贴前面的内容，不在标点前加空格；英文、数字、单位和代码片段之间通常保留一个 ASCII 空格。这条是正文排版规则，不改变代码 span 内部。
- 外层引号使用 `“……”`，嵌套引号使用 `‘……’`。不要把繁体中文的 `「」`、`『』` 带入简体正文；若是在引用原文，保留原文并标明它是引用。
- 省略号写 `……`，破折号写 `——`。英文代码、命令、URL、正则表达式和 fenced code block 原样保留，不能把其中的 `...` 或 `--` 改掉。
- 冒号后接说明或列表时不额外插入空格；列表项如果是完整句，使用句号，短语不强行补句号。
- 括号按中文语境使用全角 `（ ）`；API 参数、文件名和代码中的半角括号不改。
- 不手工插入 NBSP。只有源代码或链接本身已有不可断空格时才保留；普通中文与 Latin/数字的间距按本节规则处理。

参考：中文技术文案指南明确了中英文之间的空格和中文引号，W3C 的中文排版资料说明了繁体中文常用的直角引号。

- https://github.com/aaranxu/chinese-copywriting-guidelines
- https://www.w3.org/TR/clreq/

### 5. 数字、日期与单位

- 技术文档中的数量、版本、端口、状态码、时间和百分比使用阿拉伯数字：`3 次`、`64 KB`、`2026 年 10 月 12 日`、`99.9%`。
- 数字与中文量词之间留一个空格；百分号贴数字；单位前留一个空格。`64-bit`、`HTTP/2`、`v1.4.3`、`10–20` 这类协议、版本和范围写法保持稳定。
- 小数使用半角句点；不要把技术值写成汉字数字。命令、JSON、SQL、版本号和 API 参数仍按原文输出。
- “第 1 步”使用阿拉伯数字；标题和表格中的数字格式保持同一层级一致。

### 6. 术语表

术语表覆盖 the concept list 的全部概念。若一行列出多个概念，`local rendering` 按分号与 English 概念逐项对应；不能因为状态是 `unverified` 就把普通概念词留成英文。

| English | local rendering | keep English? | status | source / note |
| --- | --- | --- | --- | --- |
| schema | 模式 | 否 | unverified（普通书面译法；待熟悉 ClickHouse 的母语开发者确认） | `schema` 也有人写“架构”，本表统一用“模式”。 |
| schema DSL | 模式 DSL | 否 | inferred（由 `schema` 的“模式”与固定词 `DSL` 组合） | `DSL` 按共享规则保留。 |
| boundary / boundaries | 边界 | 否 | unverified（普通书面译法；待母语开发者确认） | 规则范围语境统一用“边界”。 |
| column | 列 | 否 | attested（https://learn.microsoft.com/zh-cn/sql/relational-databases/databases/databases?view=sql-server-ver16） | 数据库语境使用“列”。 |
| table | 表 | 否 | attested（https://learn.microsoft.com/zh-cn/sql/relational-databases/databases/databases?view=sql-server-ver16） | 数据库语境使用“表”。 |
| database | 数据库 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | 不把普通概念写成 `database`。 |
| database name | 数据库名 | 否 | inferred（由已证实的“数据库”与“名称”省略组合） | 复合名词中不写英文。 |
| temporary table | 临时表 | 否 | inferred（由“临时”与已证实的“表”组合） | 不写成“temporary table”。 |
| table engine | 表引擎 | 否 | unverified（普通书面译法；待 ClickHouse 开发者确认） | ClickHouse 专有语境可能有团队术语。 |
| column type | 列类型 | 否 | inferred（由“列”与 TypeScript 中文文档中的“类型”组合） | `type` 在类型语境统一译为“类型”。 |
| type definition | 类型定义 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/everyday-types.html） | “类型定义”是中文 TypeScript 文档的常用写法。 |
| nullable | 可为 NULL | 否 | inferred（由中文 TypeScript 文档对 `null` 的说明与 SQL `NULL` 约定组合） | `NULL` 是固定 SQL 词；“可为”按句子需要变为“允许为”。 |
| optional | 可选 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/basic-types.html） | 属性或参数作定语时写“可选的”。 |
| required | 必需 | 否 | attested（https://cn.vuejs.org/api/sfc-script-setup） | 名词前作定语时可写“必需的”。 |
| default value | 默认值 | 否 | attested（https://cn.vuejs.org/api/sfc-script-setup） | 不写成“缺省值”，除非原文明确是系统术语。 |
| modifier | 修饰符 | 否 | attested（https://cn.vuejs.org/api/sfc-script-setup） | 与 TypeScript/Vue 文档一致。 |
| materialized column | 物化列 | 否 | unverified（普通书面译法；待 ClickHouse 开发者确认） | 保留 `column` 的“列”。 |
| alias column | 别名列 | 否 | inferred（由“别名”与已证实的“列”组合） | `alias` 不保留英文。 |
| array | 数组 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/basic-types.html） | 复数按中文习惯不额外加标记。 |
| tuple | 元组 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/basic-types.html） | TypeScript 类型名使用“元组”。 |
| map | 映射 | 否 | inferred（由 TypeScript 中文类型文档的“映射类型”推得） | 数据结构语境可按句法写“映射表”。 |
| nested | 嵌套 | 否 | attested（https://zh-hans.react.dev/learn） | 作动词时写“嵌套”，作定语时写“嵌套的”。 |
| enum | 枚举 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/everyday-types.html） | 不写“枚举值”除非原文有 value。 |
| literal | 字面量 | 否 | attested（https://cn.vuejs.org/api/sfc-script-setup） | TypeScript/Vue 类型语境使用“字面量”。 |
| string | 字符串 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/basic-types.html） | 具体类型和值都用“字符串”。 |
| integer | 整数 | 否 | unverified（普通书面译法；待母语开发者确认） | 不把它写成“整型”以避免与类型名混淆。 |
| 64-bit integer | 64 位整数 | 否 | inferred（由中文技术文档的数字与单位排版规则推得） | `64-bit` 在正文中写成“64 位”。 |
| floating point | 浮点数 | 否 | unverified（普通书面译法；待母语开发者确认） | 需要时写“浮点数值”。 |
| boolean | 布尔值 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/basic-types.html） | 类型名作定语时写“布尔类型”。 |
| date | 日期 | 否 | attested（https://learn.microsoft.com/zh-cn/sql/relational-databases/databases/databases?view=sql-server-ver16） | 与“时间戳”区分。 |
| timestamp | 时间戳 | 否 | unverified（普通书面译法；待母语开发者确认） | 不写英文 `timestamp`。 |
| timezone | 时区 | 否 | unverified（普通书面译法；待母语开发者确认） | 不写“时区信息”除非原文有 information。 |
| precision | 精度 | 否 | unverified（普通书面译法；待母语开发者确认） | 数值语境优先用“精度”。 |
| scale | 小数位数 | 否 | unverified（普通书面译法；待母语开发者确认） | Decimal 语境不要误写成“比例”。 |
| exact value | 精确值 | 否 | unverified（普通书面译法；待母语开发者确认） | 强调数值不近似时使用。 |
| lossless | 无损 | 否 | unverified（普通书面译法；待母语开发者确认） | 作定语时写“无损的”。 |
| JSON path | JSON 路径 | 否 | inferred（由固定词 `JSON` 与中文技术文档中的“路径”组合） | `JSON` 原样保留。 |
| generic type | 泛型 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/generics.html） | 类型语境不写“通用类型”。 |
| type inference | 类型推断 | 否 | inferred（由 TypeScript 中文文档的“推断”术语组合） | 与“类型推导”保持一处一词。 |
| inferred | 推断出的 | 否 | inferred（由“类型推断”派生） | 作谓语时按句子改为“推断得到的”。 |
| typed | 带类型的 | 否 | unverified（普通书面译法；待母语开发者确认） | 需要强调 TypeScript 时也可写“有类型的”。 |
| untyped | 无类型的 | 否 | unverified（普通书面译法；待母语开发者确认） | 不写英文 `untyped`。 |
| type assertion | 类型断言 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/basic-types.html） | TypeScript 固定术语。 |
| runtime validation | 运行时验证 | 否 | inferred（由中文 Vue 文档的“运行时验证”与术语表组合） | `runtime` 不保留英文。 |
| validator | 验证器 | 否 | attested（https://v2.cn.vuejs.org/v2/api/） | 不是“校验器”；全文件统一“验证器”。 |
| encoder | 编码器 | 否 | unverified（普通书面译法；待母语开发者确认） | 与“解码器”成对。 |
| decoder | 解码器 | 否 | unverified（普通书面译法；待母语开发者确认） | 与“编码器”成对。 |
| decoding | 解码 | 否 | unverified（普通书面译法；待母语开发者确认） | 作定语时写“解码过程”。 |
| query | 查询 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/HTTP/Guides/Session） | 数据库和 HTTP 语境都用“查询”时按上下文判断。 |
| query builder | 查询构建器 | 否 | inferred（由官方中文文档的“查询”与“构建器”组合） | 不写 `query builder`。 |
| builder | 构建器 | 否 | unverified（普通书面译法；待母语开发者确认） | 只有明确指 API 构建器时使用。 |
| default path | 默认路径 | 否 | inferred（由“默认值”的“默认”与“路径”组合） | 默认执行方式用“默认路径”。 |
| chain | 链式调用 | 否 | unverified（普通书面译法；待母语开发者确认） | 比“链”更符合开发者说明。 |
| chainable | 可链式调用的 | 否 | unverified（普通书面译法；待母语开发者确认） | 作方法定语时保留“的”。 |
| subquery | 子查询 | 否 | attested（https://learn.microsoft.com/zh-cn/sql/relational-databases/databases/databases?view=sql-server-ver16） | SQL 语境使用“子查询”。 |
| bare subquery | 裸子查询 | 否 | unverified（普通书面译法；待 ClickHouse 开发者确认） | “裸”表示没有别名或包装，按上下文保留。 |
| anonymous CTE | 匿名 CTE | 否 | inferred（由“匿名”与固定 SQL 词 `CTE` 组合） | `CTE` 原样保留。 |
| join | 连接 | 否 | inferred（由 Microsoft 中文 SQL 文档的连接语境推得） | SQL 关键字在代码中仍写 `JOIN`；正文概念译为“连接”。 |
| left join | 左连接 | 否 | inferred（由“连接”与方向词组合） | 代码/API 名保持原样；正文不写英文短语。 |
| outer join | 外连接 | 否 | inferred（由“连接”与 SQL 方向词组合） | 需要区分时写“外连接”。 |
| ClickHouse-specific expression | ClickHouse 专用表达式 | 否 | inferred（由固定项目名与“专用”“表达式”组合） | `ClickHouse` 原样保留，普通修饰词翻译。 |
| null semantics | NULL 语义 | 否 | inferred（由 `NULL` 固定词与中文技术文档的“语义”组合） | `NULL` 是 SQL 固定词，不把整个短语留成英文。 |
| predicate | 谓词 | 否 | unverified（普通书面译法；待 ClickHouse 开发者确认） | SQL 语境优先使用“谓词”。 |
| filter | 过滤条件 | 否 | attested（https://zh-hans.react.dev/learn/build-a-react-app-from-scratch） | 作动词时写“过滤”，作名词时写“过滤条件”。 |
| where clause | WHERE 子句 | 否 | inferred（由 SQL 中文文档的“子句”与固定关键字 `WHERE` 组合） | 只保留关键字，不保留英文 `where clause`。 |
| group by | GROUP BY | 是 | attested（https://learn.microsoft.com/zh-cn/sql/t-sql/queries/select-clause-transact-sql?view=sql-server-ver17） | SQL 关键字；正文可写“GROUP BY 子句”。 |
| order by | ORDER BY | 是 | attested（https://learn.microsoft.com/zh-cn/office/vba/access/concepts/structured-query-language/retrieve-records-using-access-sql） | SQL 关键字；不要音译。 |
| limit | LIMIT | 是 | inferred（SQL/ClickHouse 关键字，按代码与 SQL 文档惯例保留） | 只在 SQL 语法语境保留。 |
| projection | 投影 | 否 | unverified（普通书面译法；待 ClickHouse 开发者确认） | 查询结果列语境使用“投影”。 |
| selection (select list) | 选择（SELECT 列表） | 否 | inferred（由 SQL 中文文档的“选择列表”与固定关键字 `SELECT` 组合） | `SELECT` 是关键字，不把整个概念留成英文。 |
| alias | 别名 | 否 | attested（https://learn.microsoft.com/zh-tw/office/vba/access/concepts/structured-query-language/retrieve-records-using-access-sql） | SQL 和 TypeScript 语境都用“别名”。 |
| expression | 表达式 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/everyday-types.html） | 不写“表达式体”除非原文有 body。 |
| window expression | 窗口表达式 | 否 | inferred（由中文 SQL 文档的“窗口”与“表达式”组合） | ClickHouse 专有语境待母语者确认。 |
| aggregate | 聚合 | 否 | unverified（普通书面译法；待 ClickHouse 开发者确认） | 函数语境可写“聚合函数”。 |
| function | 函数 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/everyday-types.html） | API 语境不写英文 `function`。 |
| table function | 表函数 | 否 | unverified（普通书面译法；待 ClickHouse 开发者确认） | 与“函数”保持一致。 |
| scalar | 标量 | 否 | unverified（普通书面译法；待 ClickHouse 开发者确认） | 不写“标量值”除非原文有 value。 |
| raw SQL | 原始 SQL | 否 | inferred（由中文技术文档中“原始”与固定词 `SQL` 组合） | `SQL` 原样保留。 |
| SQL template | SQL 模板 | 否 | inferred（由固定词 `SQL` 与中文“模板”组合） | 不写英文 `template`。 |
| SQL fragment | SQL 片段 | 否 | inferred（由固定词 `SQL` 与中文“片段”组合） | 不写英文 `fragment`。 |
| parameter | 参数 | 否 | attested（https://learn.microsoft.com/zh-cn/sql/relational-databases/stored-procedures/specify-parameter-names?view=sql-server-ver16） | API 和 SQL 语境都用“参数”。 |
| bound parameter | 绑定参数 | 否 | inferred（由 Microsoft 中文文档的“参数化查询”与“绑定”用法组合） | 不写 `bound parameter`。 |
| placeholder | 占位符 | 否 | attested（https://learn.microsoft.com/zh-tw/office/vba/access/concepts/structured-query-language/retrieve-records-using-access-sql） | 参数或模板中的占位位置用“占位符”。 |
| query parameter | 查询参数 | 否 | attested（https://zh-hans.react.dev/learn/build-a-react-app-from-scratch） | 不写英文 `query parameter`。 |
| compile | 编译 | 否 | attested（https://cn.vuejs.org/api/sfc-script-setup） | 编译器动作统一写“编译”。 |
| compiled SQL | 编译后的 SQL | 否 | inferred（由“编译”与固定词 `SQL` 组合） | 按句法写“编译出的 SQL”也可。 |
| thenable | thenable（带有 `then` 方法的对象） | 是 | inferred（JavaScript/TypeScript API 术语；中文说明补足含义） | 这是允许保留的 API 术语，不是普通英文词。 |
| helper | 辅助函数 | 否 | attested（https://cn.vuejs.org/api/sfc-script-setup） | 函数语境不写“助手”。 |
| namespace | 命名空间 | 否 | attested（https://cn.vuejs.org/api/sfc-script-setup） | TypeScript/Vue 文档均使用“命名空间”。 |
| factory | 工厂函数 | 否 | attested（https://v2.cn.vuejs.org/v2/api/） | 只有明确是函数时补“函数”。 |
| callback | 回调 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Glossary/Callback_function） | 不写英文 `callback`。 |
| iterator | 迭代器 | 否 | unverified（普通书面译法；待母语开发者确认） | JavaScript 语境优先用“迭代器”。 |
| variadic | 可变参数 | 否 | unverified（普通书面译法；待母语开发者确认） | 不写“变长的”除非强调参数数量。 |
| insert | INSERT | 是 | attested（https://learn.microsoft.com/zh-tw/sql/t-sql/statements/insert-transact-sql?view=fabric） | SQL 关键字；代码和语法说明保留。 |
| write | 写入 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | 数据写操作统一用“写入”。 |
| batch | 批处理 | 否 | unverified（普通书面译法；待母语开发者确认） | 批量操作也可按上下文写“批次”。 |
| bulk insert | 批量插入 | 否 | inferred（由“批量”与 `insert` 的中文动作“插入”组合） | 不写英文短语。 |
| insert from select | INSERT FROM SELECT | 是 | inferred（完整 SQL 语法片段，关键字按 SQL 约定保留） | 不把 SQL 语法短语改写成普通中文。 |
| stream | 流 | 否 | unverified（普通书面译法；待母语开发者确认） | 数据流语境使用“流”。 |
| streaming | 流式处理 | 否 | unverified（普通书面译法；待母语开发者确认） | 作形容词时写“流式的”。 |
| row | 行 | 否 | attested（https://learn.microsoft.com/zh-cn/sql/relational-databases/databases/databases?view=sql-server-ver16） | 数据库表的 row 统一用“行”。 |
| result set | 结果集 | 否 | attested（https://learn.microsoft.com/zh-cn/sql/t-sql/database-console-commands/dbcc-checkdb-transact-sql?view=sql-server-ver17） | 不写“结果集合”。 |
| replacement (ReplacingMergeTree) | 替换（ReplacingMergeTree） | 否 | unverified（普通书面译法；待 ClickHouse 开发者确认） | `ReplacingMergeTree` 是产品/引擎名，原样保留。 |
| versioning | 版本管理 | 否 | unverified（普通书面译法；待母语开发者确认） | 持续维护版本时用“版本管理”。 |
| deletion flag | 删除标记 | 否 | unverified（普通书面译法；待母语开发者确认） | 不写英文 `flag`。 |
| client | 客户端 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | 与“服务器”成对。 |
| connection | 连接 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | 连接对象和连接过程都按上下文使用。 |
| server | 服务器 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | 不写“伺服器”。 |
| replica | 副本 | 否 | unverified（普通书面译法；待 ClickHouse 开发者确认） | 数据库副本语境使用“副本”。 |
| request | 请求 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/HTTP/Guides/Session） | HTTP 和 API 语境统一用“请求”。 |
| response | 响应 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/HTTP/Guides/Session） | 不写“回应”。 |
| transport | 传输 | 否 | inferred（由 MDN/ WebRTC 中文文档的“传输”用法推得） | 传输层语境写“传输”。 |
| HTTP request | HTTP 请求 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/API/XMLHttpRequest_API/Synchronous_and_Asynchronous_Requests） | `HTTP` 固定保留，`request` 翻译。 |
| timeout | 超时 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/API/XMLHttpRequest_API/Synchronous_and_Asynchronous_Requests） | 属性名在代码中仍按原文。 |
| settings | 设置 | 否 | attested（https://cn.vuejs.org/api/sfc-script-setup） | 配置项集合也可按上下文写“设置”。 |
| configuration | 配置 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | 不写“组态”。 |
| config | 配置 | 否 | inferred（`configuration` 的常用缩写；由中文技术文档的“配置”推得） | 作为标识符、选项名或文件名时按共享规则原样保留。 |
| option | 选项 | 否 | attested（https://cn.vuejs.org/api/sfc-script-setup） | API 选项统一用“选项”。 |
| session | 会话 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/HTTP/Guides/Session） | 不写英文 `session`。 |
| temporary-table workflow | 临时表流程 | 否 | inferred（由“临时表”与“流程”组合） | 过程说明中也可写“临时表工作流”，但本表统一中文。 |
| transaction | 事务 | 否 | attested（https://learn.microsoft.com/zh-cn/sql/relational-databases/sql-server-transaction-locking-and-row-versioning-guide?view=sql-server-2017） | 数据库语境固定用“事务”。 |
| concurrency | 并发 | 否 | unverified（普通书面译法；待 ClickHouse 开发者确认） | 不写“并行”来替代并发。 |
| lifecycle | 生命周期 | 否 | attested（https://cn.vuejs.org/api/sfc-script-setup） | 对象或请求生命周期都用“生命周期”。 |
| cleanup | 清理 | 否 | unverified（普通书面译法；待母语开发者确认） | 资源释放语境按上下文可写“清理工作”。 |
| runtime method | 运行时方法 | 否 | inferred（由“运行时”与“方法”组合） | `runtime` 不保留英文。 |
| lives on | 挂在……上 | 否 | unverified（普通技术写法；待母语者确认） | 描述连接或配置归属时按上下文改写。 |
| execute | 执行 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | SQL 或函数动作统一用“执行”。 |
| fetch | 获取 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/API/IndexedDB_API/Using_IndexedDB） | 网络 API 名 `fetch` 仍原样；概念动作译为“获取”。 |
| abort | 中止 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/API/AbortController） | 取消控制器语境用“中止”。 |
| cancel | 取消 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/API/AbortController） | 用户动作或请求语境都用“取消”。 |
| observability | 可观测性 | 否 | attested（https://opentelemetry.io/zh/docs/concepts/observability-primer/） | OpenTelemetry 中文文档直接使用“可观测”。 |
| tracing | 追踪 | 否 | attested（https://opentelemetry.io/zh/docs/concepts/observability-primer/） | 分布式 tracing 译为“追踪”。 |
| span | span（跨度） | 是 | attested（https://opentelemetry.io/zh/docs/concepts/observability-primer/） | OpenTelemetry 中文文档同时给出 Span 与“跨度”；API 语境保留 `span` 这一术语。 |
| logging | 日志记录 | 否 | attested（https://opentelemetry.io/zh/docs/languages/js/instrumentation/） | 动作写“记录日志”或“日志记录”，名词统一后者。 |
| logger | 日志记录器 | 否 | inferred（由 OpenTelemetry 的“日志记录”与中文“记录器”组合） | 不把普通概念留成 `logger`。 |
| metrics | 指标 | 否 | attested（https://opentelemetry.io/zh/docs/languages/js/instrumentation/） | 可观测性语境固定用“指标”。 |
| instrumentation | 插桩 | 否 | attested（https://opentelemetry.io/zh/docs/languages/js/instrumentation/） | OpenTelemetry 中文文档标题直接使用“插桩”。 |
| hook | Hook | 是 | attested（https://zh-hans.react.dev/learn） | React 中文文档使用 Hook；这是 JavaScript/React API 术语，不是普通英文名词。 |
| event | 事件 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Learn_web_development/Core/Scripting/Events） | 不写英文 `event`。 |
| error | 错误 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/HTTP/Reference/Status） | 返回值、异常和页面提示都按上下文用“错误”。 |
| exception | 异常 | 否 | attested（https://opentelemetry.io/zh/docs/languages/js/instrumentation/） | JavaScript 错误对象语境使用“异常”。 |
| throws | 抛出 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/everyday-types.html） | 函数动作写“抛出错误”。 |
| rejects | 拒绝 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/JavaScript/Reference/Global_Objects/Promise/all） | Promise 语境写“拒绝”。 |
| error model | 错误模型 | 否 | inferred（由“错误”与“模型”组合） | 不写英文 `error model`。 |
| validation error | 验证错误 | 否 | inferred（由“验证”与“错误”组合） | 与“验证器”保持词根一致。 |
| stack | 堆栈 | 否 | unverified（普通书面译法；待母语开发者确认） | 错误信息语境优先用“堆栈”。 |
| retry | 重试 | 否 | unverified（普通书面译法；待母语开发者确认） | 动作写“重试”，不要写 `retry`。 |
| security | 安全 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/Security/Attacks/SQL_injection） | 安全语境不写“安全性”替代所有用法。 |
| SQL injection | SQL 注入 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/Security/Attacks/SQL_injection） | `SQL` 固定保留，`injection` 翻译。 |
| untrusted input | 不受信任的输入 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/Security/Attacks/SQL_injection） | 不写“非信任输入”。 |
| identifier quoting | 标识符引用 | 否 | unverified（普通书面译法；待 SQL 开发者确认） | 这里的 quoting 不是字符串引号排版。 |
| escaping | 转义 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/Security/Attacks/SQL_injection） | 需要说明 escape 字符时仍译为“转义”。 |
| credentials | 凭据 | 否 | attested（https://learn.microsoft.com/zh-cn/microsoft-365/enterprise/session-timeouts?view=o365-worldwide） | 认证语境使用“凭据”。 |
| password | 密码 | 否 | attested（https://learn.microsoft.com/zh-cn/entra/identity/authentication/howto-authentication-temporary-access-pass） | 不写英文 `password`。 |
| allow-list | 允许列表 | 否 | unverified（普通书面译法；待母语开发者确认） | 不把它误写成“白名单”；当前只决定为“允许列表”。 |
| permission | 权限 | 否 | attested（https://learn.microsoft.com/zh-cn/sql/relational-databases/databases/databases?view=sql-server-ver16） | 不写“许可”替代访问控制语境。 |
| installation | 安装 | 否 | attested（https://cn.vuejs.org/guide/essentials/component-basics） | 标题和正文都用“安装”。 |
| install | 安装 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | 动词和名词不另造词。 |
| peer dependency | 对等依赖项 | 否 | attested（https://learn.microsoft.com/zh-cn/microsoftteams/platform/apps-in-teams-meetings/teams-live-share-media-capabilities） | “依赖项”是名词；不要在正文留下 `peer dependency`。 |
| version | 版本 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/basic-types.html） | 版本号本身按项目规则保留。 |
| pin a version | 固定版本 | 否 | unverified（普通书面译法；待母语开发者确认） | 动作写“固定版本号”时补“号”。 |
| release | 发布 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | 不写“发行”替代软件发布。 |
| breaking change | 破坏性变更 | 否 | inferred（由中文软件升级文档常用的“破坏性变更”组合） | 需要强调兼容性时可写“会破坏兼容性的变更”。 |
| before 1.0 | 1.0 之前 | 否 | inferred（由中文数字日期规则与原句语义组合） | 版本号前后保留半角数字。 |
| pre-release | 预发布 | 否 | unverified（普通书面译法；待母语开发者确认） | 版本语境不写英文 `pre-release`。 |
| migration | 迁移 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | 数据库和版本升级都用“迁移”。 |
| schema migration | 模式迁移 | 否 | inferred（由“模式”与已证实的“迁移”组合） | 不写 `schema 迁移`。 |
| production | 生产环境 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | 作定语时写“生产环境中的”。 |
| example | 示例 | 否 | attested（https://zh-hans.react.dev/learn） | 不写“范例”作为默认写法。 |
| runnable | 可运行的 | 否 | inferred（由中文开发者文档的“可运行”与形容词词尾组合） | 作名词时写“可运行示例”。 |
| reference | 参考 | 否 | attested（https://zh-hans.react.dev/learn） | `reference` 页面可按标题写“参考”。 |
| documentation | 文档 | 否 | attested（https://zh-hans.react.dev/learn） | 普通概念不保留英文 `documentation`。 |
| page | 页面 | 否 | attested（https://zh-hans.react.dev/learn） | 网页语境统一用“页面”。 |
| section | 章节 | 否 | attested（https://zh-hans.react.dev/learn） | 文档结构语境不用“节”作为默认写法。 |
| caveat | 注意事项 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | 警告性质强时按上下文改写为“限制”。 |
| note | 备注 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/HTTP/Guides/Session） | Markdown 的 note 不译成“注释”以免和代码注释混淆。 |
| behavior | 行为 | 否 | attested（https://opentelemetry.io/zh/docs/concepts/observability-primer/） | API 行为统一用“行为”。 |
| semantics | 语义 | 否 | unverified（普通书面译法；待母语开发者确认） | SQL 语境不要写成“语意”。 |
| contract | 契约 | 否 | unverified（普通书面译法；待母语开发者确认） | 并发或 API 约束语境使用“契约”。 |
| mode | 模式 | 否 | attested（https://v2.cn.vuejs.org/v2/api/） | 不写“方式”替代明确的运行模式。 |
| public API | 公共 API | 否 | inferred（由中文技术文档的“公共”与固定词 `API` 组合） | `API` 固定保留，`public` 必须翻译。 |
| stable | 稳定的 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | 版本语境可写“稳定版”。 |
| internal | 内部的 | 否 | unverified（普通书面译法；待母语开发者确认） | 访问范围语境可写“内部”。 |
| deprecated | 已弃用 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/API/IndexedDB_API/Using_IndexedDB） | 不写“已废弃”。 |
| shape | 结构 | 否 | inferred（由 React/TypeScript 中文文档对数据结构的用法推得） | 类型对象语境优先用“结构”。 |
| surface | 接口面 | 否 | unverified（普通书面译法；待 API 设计者确认） | 如果上下文强调可调用范围，也可写“接口范围”。 |
| layer | 层 | 否 | attested（https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access） | 架构或协议语境都用“层”。 |
| keeps X easy | 让 X 更好用 | 否 | unverified（普通口语化技术写法；待母语者确认） | 按主语和宾语改写，不逐词直译。 |
| without hiding | 不隐藏…… | 否 | unverified（普通口语化技术写法；待母语者确认） | 根据对象补出“隐藏什么”。 |
| make everyday access easier to structure | 让日常使用 ClickHouse 更容易组织 | 否 | unverified（普通口语化技术写法；待母语者确认） | 保留 ClickHouse；按中文动宾顺序重写。 |
| is rejected | 直接拒绝 | 否 | unverified（普通技术写法；待母语者确认） | 被动句按上下文改成主动句。 |
| is not validated | 不做验证 | 否 | unverified（普通技术写法；待母语者确认） | 强调行为时可写“不会验证”。 |
| is decoded conservatively | 谨慎解码 | 否 | unverified（普通技术写法；待母语者确认） | 数值语境不写“保守地解码”。 |
| large or high-risk numeric result | 大数或高风险数值结果 | 否 | unverified（普通技术写法；待母语者确认） | 不把 numeric 留成英文。 |
| silently coerced | 静默强制转换 | 否 | unverified（普通技术写法；待母语者确认） | 与“谨慎解码”配对时使用。 |
| carries | 携带 | 否 | unverified（普通技术写法；待母语者确认） | 上下文是数据时也可写“带有”。 |
| exposes | 暴露 | 否 | unverified（普通技术写法；待母语者确认） | API 提供能力时按语义改为“提供”。 |
| follows | 遵循 | 否 | unverified（普通技术写法；待母语者确认） | 规则、协议和流程分别按上下文改写。 |
| stays | 保持 | 否 | unverified（普通技术写法；待母语者确认） | 状态不变时写“保持”。 |
| leaves | 保留 | 否 | unverified（普通技术写法；待母语者确认） | 处理结果或内容时写“保留”。 |
| pass | 传入 | 否 | unverified（普通技术写法；待母语者确认） | 函数参数语境使用“传入”。 |
| wrap | 封装 | 否 | unverified（普通技术写法；待母语者确认） | 包装对象时按上下文选择“封装”或“包装”。 |
| compose | 组合 | 否 | attested（https://zh-hans.react.dev/learn） | 组件、查询和函数都按上下文写“组合”。 |
| accepts | 接受 | 否 | unverified（普通技术写法；待母语者确认） | 参数语境也可写“接收”。 |
| returns | 返回 | 否 | attested（https://developer.mozilla.org/zh-CN/docs/Web/API/IndexedDB_API/Using_IndexedDB） | 函数和 API 结果统一用“返回”。 |
| throws | 抛出 | 否 | attested（https://www.typescriptlang.org/zh/docs/handbook/2/everyday-types.html） | 与 `throws` 概念行保持一致。 |
| Project and library names: ClickHouse; ck-orm; OpenTelemetry; TypeScript; JavaScript; Bun; Node.js; npm; pnpm; Yarn; Drizzle; GitHub; Docker; Zod | 原样保留 | 是 | inferred（由共享规则第 3 节固定名政策） | 项目名、库名和运行时名不是概念词；不得音译或意译。 |
| Fixed technical tokens: SQL; JSON; DDL; NULL; CTE; HTTP; UTC; Decimal; Float64; SELECT; API names; identifiers; option names; file names | 原样保留 | 是 | inferred（由共享规则第 3 节的固定词政策） | SQL 关键字、API 名、标识符、选项名和文件名按原拼写保留。 |
| UI and release tokens: llms.txt; MIT; version numbers | 原样保留 | 是 | inferred（由共享规则第 3 节的项目约定） | 这些固定 token 不音译、不改大小写。 |

### 7. 不要混入其他变体

以下只列能分别找到两侧来源的区域词形差异。规则针对译文正文；代码、原始日志、URL、用户提供的原文和明确的引用不套用。它们不是“另一地区错误”，只是本文件的目标变体应优先使用简体大陆形式。

| 不要写 | 写成 | 依据 |
| --- | --- | --- |
| 資料 | 数据 | 繁体 React 页面写“資料”，简体 React 页面写“数据”：https://zh-hant.react.dev/learn 、https://zh-hans.react.dev/learn |
| 資料庫 | 数据库 | 台湾 Microsoft 页面写“資料庫”，大陆 Microsoft 页面写“数据库”：https://learn.microsoft.com/zh-tw/azure/azure-sql/database/single-database-manage?view=azuresql 、https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access |
| 欄位 | 列 | 台湾 Microsoft 页面写“欄位”，大陆 Microsoft 页面写“列”：https://learn.microsoft.com/zh-tw/sql/relational-databases/tables/add-columns-to-a-table-database-engine?view=sql-server-ver17 、https://learn.microsoft.com/zh-cn/sql/relational-databases/databases/databases?view=sql-server-ver16 |
| 伺服器 | 服务器 | 台湾 Microsoft 页面写“伺服器”，大陆 Microsoft 页面写“服务器”：https://learn.microsoft.com/zh-tw/ssms/quickstarts/ssms-connect-query-sql-server 、https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access |
| 連線 | 连接 | 台湾 Microsoft 页面写“連線”，大陆 Microsoft 页面写“连接”：https://learn.microsoft.com/zh-tw/ssms/quickstarts/ssms-connect-query-sql-server 、https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access |
| 建置 | 构建 | 台湾 Microsoft 页面写“建置”，大陆 Microsoft 页面写“构建”：https://learn.microsoft.com/zh-tw/windows/apps/windows-app-sdk/ 、https://learn.microsoft.com/zh-cn/windows/apps/introduction |

不要添加没有证据的“大陆禁词”或把 `白名单` 强行改成另一种政治术语；术语表中的 `allow-list` 目前只给出项目内的“允许列表”，状态是 `unverified`。区域表中的每个规则都只作为 WARN，避免把引用或用户原文误判成译文错误。

### 8. 校准段落

使用 `ck-orm` 时，请牢记以下边界：

- `ckType.*` 定义模式中的列类型。
- `ckTable(...)` 定义表的模式。
- `ck.*` 是查询辅助函数命名空间。
- `fn.*` 是 SQL 函数辅助函数命名空间。
- 模式描述表和列，而不是数据库名。
- 数据库连接挂在 `clickhouseClient(...)` 上。
- 构建器查询是默认路径；需要 ClickHouse 专用表达式时，可以使用原始 SQL。
- `runInSession()` 是 ClickHouse 的会话辅助函数，不是事务。
- `leftJoin()` 默认使用 SQL 风格的 NULL 语义。
- 大数或高风险数值结果要谨慎解码，不要静默强制转换。

ck-orm 有一个对等依赖项，`@opentelemetry/api`。Bun、npm 和 pnpm 会自动安装它；Yarn 不会，因此 Yarn 命令会显式添加它。

### 9. 参考来源与证据边界

- IANA language tags：https://www.iana.org/assignments/language-tags
- Unicode CLDR：https://www.unicode.org/reports/tr35/
- React 简体中文：https://zh-hans.react.dev/learn
- React 繁体中文：https://zh-hant.react.dev/learn
- Vue 简体中文：https://cn.vuejs.org/guide/essentials/component-basics
- Vue API：https://cn.vuejs.org/api/sfc-script-setup
- TypeScript 中文手册：https://www.typescriptlang.org/zh/docs/handbook/2/everyday-types.html
- TypeScript 中文基础类型：https://www.typescriptlang.org/zh/docs/handbook/2/basic-types.html
- TypeScript 中文泛型：https://www.typescriptlang.org/zh/docs/handbook/2/generics.html
- MDN 简体中文回调术语：https://developer.mozilla.org/zh-CN/docs/Glossary/Callback_function
- MDN 简体中文 HTTP 会话：https://developer.mozilla.org/zh-CN/docs/Web/HTTP/Guides/Session
- MDN 简体中文异步请求：https://developer.mozilla.org/zh-CN/docs/Web/API/XMLHttpRequest_API/Synchronous_and_Asynchronous_Requests
- MDN 简体中文事件：https://developer.mozilla.org/zh-CN/docs/Learn_web_development/Core/Scripting/Events
- MDN SQL 注入：https://developer.mozilla.org/zh-CN/docs/Web/Security/Attacks/SQL_injection
- Microsoft 中文风格指南：https://learn.microsoft.com/zh-cn/style-guide/top-10-tips-style-voice
- Microsoft 简体 SQLite 页面：https://learn.microsoft.com/zh-cn/windows/apps/develop/data-access/sqlite-data-access
- Microsoft 简体数据库页面：https://learn.microsoft.com/zh-cn/sql/relational-databases/databases/databases?view=sql-server-ver16
- Microsoft 简体 peer dependency 页面：https://learn.microsoft.com/zh-cn/microsoftteams/platform/apps-in-teams-meetings/teams-live-share-media-capabilities
- OpenTelemetry 中文插桩概念：https://opentelemetry.io/zh/docs/concepts/instrumentation/
- OpenTelemetry 中文可观测性入门：https://opentelemetry.io/zh/docs/concepts/observability-primer/
- Google 开发者风格指南：https://developers.google.cn/style/tone
- 中文文案排版指南：https://github.com/aaranxu/chinese-copywriting-guidelines
- W3C 中文排版需求：https://www.w3.org/TR/clreq/

网页内容可能更新；本文件只把能在当前来源中看到的词形写成已证实，不把机器翻译的一次出现当成普遍规范。区域差异表中的两侧词形分别来自对应 locale 的页面。

### 10. 待确认项

- `schema`／“模式”、`表引擎`、`谓词`、`投影`、`表函数`、`allow-list`／“允许列表”和 `surface`／“接口面”仍需要熟悉 ClickHouse 的母语开发者确认。
- `join`／“连接”、`thenable`、`span` 和 `Hook` 在不同团队里可能按 SQL、JavaScript 或 OpenTelemetry 的 API 习惯保留英文；需要确认本项目是否接受表中当前选择。
- “日志记录器”“可链式调用的”“谨慎解码”等短语是按中文技术写作推定的自然写法，需要母语者通读确认；它们不是“unverified 所以保留英文”。
- 区域差异表只证明页面中观察到的两侧词形，不证明任一词形在所有上下文都唯一正确。



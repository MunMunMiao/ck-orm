# `if` / `multiIf` 分支类型契约修复设计

## 文档状态

- 状态：已批准并实施（2026-07-14）
- 基线：`ck-orm@0.0.26`
- 变更级别：破坏性类型与运行时行为修正
- 适用范围：`fn.if`、`fn.multiIf`，以及 `coalesce` 的安全收紧

## 结论

当前 `fn.if` / `fn.multiIf` 把首个值分支的 `sqlType` 和 decoder 当作整个表达式的结果契约，但 ClickHouse 的真实结果类型由全部值分支共同决定。该实现会产生三类问题：

1. 普通 JavaScript literal 被独立推断为 `Int64`，可能与 `Float64` 或 `UInt64` expression 不兼容；
2. ClickHouse 成功提升结果类型时，ORM 仍发布错误的首分支元数据；
3. 错误 decoder 可能把合法服务端结果误解码，甚至直接抛出范围错误。

本次采用保守修复：

- 只对能够证明无损的普通 literal 做上下文定型；
- 只有全部值分支能够证明收敛到同一 SQL 类型时，才发布结果 `sqlType` 和 decoder；
- 无法证明时不转换 expression/raw，不推测 ClickHouse common supertype，并降级为未知结果契约；
- 删除 `fn.if<TData>` / `fn.multiIf<TData>` 的不安全泛型断言；
- 不在 ORM 中复制 ClickHouse 的完整类型系统。

## 已验证事实

### ClickHouse 规则

- [`row_number`](https://clickhouse.com/docs/sql-reference/window-functions/row_number) 返回 `UInt64`；
- [`multiIf`](https://clickhouse.com/docs/sql-reference/functions/conditional-functions#multiif) 的全部值分支必须存在共同超类型；
- ClickHouse 会在执行条件前确定 conditional 的结果类型；
- [`use_variant_as_common_type`](https://clickhouse.com/docs/operations/settings/settings#use_variant_as_common_type) 开启时，无共同类型的分支可以得到 `Variant(...)`；关闭时，相同查询可能返回 `NO_COMMON_TYPE`。

本地 ClickHouse 26.3.10.60 实测：

| 表达式 | `use_variant_as_common_type=0` | `use_variant_as_common_type=1` |
|---|---|---|
| `if(..., Float64, Int64)` | `NO_COMMON_TYPE` | `Variant(Float64, Int64)` |
| `if(..., UInt64, Int64)` | `NO_COMMON_TYPE` | `Variant(Int64, UInt64)` |
| `multiIf(..., UInt64, Int64)` | `NO_COMMON_TYPE` | `Variant(Int64, UInt64)` |

ORM 不应根据服务端版本或设置选择不同实现。修复后的 builder 应为可证明的 literal 场景生成稳定类型；无法证明的 expression 组合继续由开发者和 ClickHouse 决定。

### 当前 ORM 行为

当前通用函数编译器独立执行：

```ts
args.map((argument) => compileValue(argument, ctx))
```

`fn.if` 复制 `thenValue` 的 decoder/sqlType，`fn.multiIf` 复制第一个值分支的 decoder/sqlType。普通 JavaScript safe integer 默认推断为 `Int64`。

例如：

```ts
fn.if(condition, int32Expression, 3_000_000_000)
```

ClickHouse 的实际共同类型是 `Int64`，而当前 ORM 发布 `Int32`。当服务端返回 `"3000000000"` 时，现有 Int32 decoder 会因超出 `[-2147483648, 2147483647]` 而报错。

显式泛型也不能修复该问题：

```ts
fn.if<number>(condition, 1, 0)
```

`<number>` 只影响 TypeScript，不会改变 SQL 参数类型或安装 number decoder。JSON transport 仍可能返回 `"1"` / `"0"`。

## 目标

1. `if` 与 `multiIf` 使用相同的值分支类型计划；
2. `Float64` / `UInt64` expression 配合安全整数 literal 时不再生成冲突参数类型；
3. 结果 `sqlType` 与 decoder 只在 ORM 能证明时发布；
4. expression、raw SQL 和未知类型永远不被自动 cast；
5. CTE/subquery 传播的元数据不能继续沿用错误首分支类型；
6. 不因自动定型造成回绕、舍入或 Decimal 截断；
7. 保持 API 数量不变，不新增 typed-literal 或 strict conditional helper；
8. 文档与测试明确区分 TypeScript 类型、SQL 类型和运行时 decoder。

## 非目标

- 不实现完整的 ClickHouse `getLeastSupertype`；
- 不检测 ClickHouse 版本或读取服务端设置；
- 不自动统一异构 expression；
- 不在本次支持 Decimal、Float32、BFloat16、窄整数或包装类型的 literal 定型；
- 不在本次增加裸 `null` 分支支持；
- 不修改 `rowNumber()` 默认的 unsafe 模式；
- 不增加 `multiIf` 参数数量校验；
- 不同时重构 `greatest`、`least`、`nullIf` 或全部算术函数。

## 破坏性公共 API 决策

### 删除不安全显式泛型

当前签名：

```ts
if<TData = unknown>(condition: unknown, thenValue: unknown, elseValue: unknown): Selection<TData>
multiIf<TData = unknown>(...args: unknown[]): Selection<TData>
```

修复后：

```ts
if(condition: unknown, thenValue: unknown, elseValue: unknown): Selection<unknown>
multiIf(...args: unknown[]): Selection<unknown>
```

原因：当前 `Selection` 类型没有在 TypeScript 泛型中携带可比较的 SQL type witness。运行时 planner 能判断 `sqlType`，但不能把该判断反馈给编译期。继续允许 `<TData>` 会保留一个无法验证的类型逃生口。

调用方需要确定 JS 输出类型时，必须同时完成两件事：

1. 用显式转换函数让所有值分支具有同一服务端 SQL 类型；
2. 用 `.mapWith(...)` 明确最终 JS 解码契约。

`.mapWith(...)` 只负责结果解码，不会生成 cast，也不能单独修复异构分支。

### 不新增替代 helper

本次不新增 `ifAs`、`typedIf`、type witness 或 typed literal API。现有 `fn.toUInt64`、`fn.toFloat64`、`fn.toString`、`fn.toDecimal*`、`fn.cast` 和 `.mapWith(...)` 已能表达显式契约。只有真实调用统计证明 `Selection<unknown>` 造成普遍负担时，再单独设计 typed conditional API。

## 内部类型计划

### 分支位置

`if` 的值分支：

```text
condition, thenValue, elseValue
           ^^^^^^^^^  ^^^^^^^^^
```

`multiIf` 的值分支：

```text
cond1, value1, cond2, value2, ..., elseValue
       ^^^^^^         ^^^^^^       ^^^^^^^^^
```

condition 不参与结果类型计划。

### 计划结果

```ts
type ConditionalBranchPlan = {
  readonly parameterSqlTypes: readonly (string | undefined)[]
  readonly resultSqlType?: string
  readonly resultDecoder: Decoder<unknown>
}
```

`parameterSqlTypes` 只对应值分支。`undefined` 表示保留现有 `compileValue` 默认推断。

### Anchor 选择

1. 按值分支顺序寻找第一个具有已知 `sqlType` 的 expression；
2. 该类型仅作为候选 anchor，不代表 ClickHouse 的 common supertype；
3. 如果没有已知类型 expression，不做上下文定型，结果元数据保持未知。

SQLFragment 没有可信 `sqlType` 元数据，即使携带 TypeScript 泛型也不能作为 anchor。

第一版只对 `Bool`、`Int64`、`UInt64`、`Float64`、`String` 五种完整 anchor 类型发布结果元数据。两个 expression 的 `sqlType` 文字相同仍不足以证明 ClickHouse 结果类型相同：ClickHouse 26.3 实测会把同型 `LowCardinality(String)` 归一为 `String`，把同型 `Point` 归一为 `Tuple(Float64, Float64)`。其他类型保持 pass-through，避免在 builder 中复制 `getLeastSupertype` 和别名归一规则。

### 普通 literal 兼容规则

先使用现有 `inferPrimitiveType(value)` 得到 literal 默认 SQL 类型。第一版只接受
`Bool`、`Int64`、`UInt64`、`Float64`、`String` 五种标量 anchor：

1. 默认类型与上述标量 anchor 完全相同：兼容，不需要改写；
2. anchor 为 `Float64`，value 为 `Number.isSafeInteger(value)`：定型为 `Float64`；
3. anchor 为 `UInt64`，value 为非负 safe integer number：定型为 `UInt64`；
4. 其他组合：不兼容，不自动定型。

第一版不定型：

- `Float32` / `BFloat16`；
- `Int8/16/32`、`UInt8/16/32`；
- Decimal；
- bigint 到其他目标类型；
- Nullable、LowCardinality、Array、Tuple、Map、Variant；
- `null` / `undefined`。

这里的“不兼容”不代表 builder 报错。它只表示 ORM 放弃自动计划，保留原参数类型并让 ClickHouse 决定执行结果。

### Expression 与 raw 规则

- anchor 属于五种支持类型且 expression 的 `sqlType` 与 anchor 完全相同：兼容；
- anchor 不在支持类型内时，即使 expression 完全同型也整体降级；
- expression 的 `sqlType` 不同或缺失：整个计划无法证明；
- SQLFragment/raw：整个计划无法证明；
- 不对 expression/raw 添加 cast；
- 不做部分定型：只要一个值分支无法收敛，所有 literal 都保留默认类型。

禁止部分定型可以避免 ORM 在未知组合中改变 ClickHouse 原本的共同类型选择。

### 结果元数据规则

所有值分支都能证明收敛到 anchor 时：

- `resultSqlType = anchor.sqlType`；
- `resultDecoder = anchor.decoder`；
- 每个需要定型的 literal 使用 anchor SQL 类型编译。

如果任一分支无法证明：

- `resultSqlType = undefined`；
- `resultDecoder = passThroughDecoder`；
- 所有参数按原有默认规则编译。

五种支持类型中，同 SQL 类型 expression 上的自定义 decoder 由第一个 typed anchor 决定，并作用于整个 conditional 结果。用户自定义 decoder 必须能处理该 SQL 类型的全部合法传输值；这是所有 `.mapWith(...)` 表达式已有的责任边界。

## 编译流程

```text
提取值分支
    ↓
寻找第一个已知 sqlType expression
    ↓
检查 anchor 是否属于五种支持类型
    ↓
逐分支检查 exact type / safe literal
    ↓
全部兼容？ ── 否 ─→ 默认编译 + unknown metadata + pass-through
    │
    是
    ↓
按计划定型 literal + anchor metadata/decoder
```

`fn.if` / `fn.multiIf` 不再直接调用通用 `createFunctionExpression`，而使用专用 conditional expression builder。通用函数路径保持不变。

`fn.if` / `fn.multiIf` 是标量函数。`createConditionalExpression` 只安装 `compile`，不安装 `windowCompiler`，因此把 conditional 直接传给 `fn.over(...)` 会在客户端被拒绝。值分支仍可包含合法的 window selection，例如 `fn.over(fn.rowNumber().toMixed())`。

## `coalesce` 安全收紧

现有 `coalesce` 已经对 fallback literal 做上下文定型，但白名单会允许服务端静默改变值：

- `Float32(16777217)` 变为 `16777216`；
- 超范围 `UInt64` 可能回绕；
- Decimal 超 scale 可能截断；
- BFloat 精度不足。

本次只复用安全 literal 判断，删除以下自动定型：

- Float32 / BFloat；
- Decimal；
- 未证明处于 UInt64 范围的 bigint。

保留：

- `Float64 + safe integer number`；
- `UInt64 + 非负 safe integer number`；
- literal 默认类型已经与上述标量目标类型完全相同的场景。

本次不修复 `coalesce` 的完整结果元数据和 Nullable 消除规则；它们属于独立设计，因为：

```text
if(Nullable(UInt64), UInt64)       -> Nullable(UInt64)
coalesce(Nullable(UInt64), UInt64) -> UInt64
```

## 延后处理的相关函数

### `greatest` / `least`

两者同样可能提升到最大兼容类型，当前首参数元数据策略并不可靠。但其 NULL 行为受 `least_greatest_legacy_null_behavior` 等设置影响，本次只在文档中登记缺口，不复用 conditional planner。

### 裸 `null`

`if(condition, expression, null)` 通常应得到 `Nullable(T)`，但 ClickHouse 不允许 `Nullable(Array|Map|Tuple)`，包装类型也有独立规则。本次继续要求开发者使用显式 SQL/转换表达 Nullable 契约，后续再设计 scalar-only 支持。

## 用户用法变化

### 安全 literal SQL 得到简化

修复前，为避免 `UInt64/Int64` 冲突需要显式转换 fallback：

```ts
const rank = fn.over(fn.rowNumber().toMixed())
const result = fn.if(condition, rank, fn.toUInt64(6))
```

修复后，builder 可以安全地把非负 safe integer literal 定型为 `UInt64`：

```ts
const rank = fn.over(fn.rowNumber().toMixed())
const result = fn.if(condition, rank, 6)
```

`.toMixed()` 仍然保留，因为它表达调用方对 UInt64 精度与传输形态的选择。

### 显式泛型不再可用

修复前的不安全写法：

```ts
fn.if<number>(condition, 1, 0)
fn.multiIf<string>(conditionA, "A", "B")
```

修复后会产生 TypeScript 编译错误。需要确定输出类型时，调用方必须先统一 SQL 类型，再提供 decoder：

```ts
fn
  .if(condition, fn.toUInt64(valueA), fn.toUInt64(valueB))
  .mapWith((value) => String(value))
```

这增加了一次显式 `.mapWith(...)`，但消除了“静态声明为 number，运行时返回 string”的隐式不一致。

### 异构 expression 保持显式

```ts
// 不自动决定 Int32 / Int64 的共同类型。
fn.if(condition, int32Expression, int64Expression)

// 调用方明确选择服务端类型。
fn.if(condition, fn.toInt64(int32Expression), int64Expression)
```

## 兼容性影响

### 源码兼容

- `fn.if<TData>` / `fn.multiIf<TData>` 调用会编译失败；
- 无泛型 conditional 的静态结果统一为 `Selection<unknown>`；
- 依赖 conditional 推导结果字段类型的代码需要显式 `.mapWith(...)`。

### SQL 兼容

在可证明场景中，literal 参数类型会改变：

```text
Float64 expression + 0: Int64 -> Float64
UInt64 expression + 0:  Int64 -> UInt64
```

这是预期修复。异构 expression/raw 的 SQL 不变。

### 运行时兼容

无法证明的组合不再沿用首分支 decoder，而返回 ClickHouse transport 的原始值。例如某些被错误解码成 number 的 Int64 结果可能恢复为 string。该变化必须写入发布说明。

### 服务端兼容

库不强制 `use_variant_as_common_type`，不针对 ClickHouse 版本分支。严格模式可能继续拒绝开发者提供的异构 expression；Variant 模式可能返回 Variant。两者都属于 ClickHouse 和调用方的类型选择。

## 测试设计

### 单元测试

在 `src/functions.test.ts` 覆盖：

1. `if(Float64 expression, safe integer)` 将 literal 编译为 `Float64`；
2. `if(UInt64 expression, non-negative safe integer)` 将 literal 编译为 `UInt64`；
3. `multiIf` 对全部 value 位置统一计划；
4. 五类支持 anchor 下所有 typed expression 完全同型时保留 anchor sqlType/decoder；
5. 任一 expression 类型不同，所有 literal 保持默认类型，结果元数据降级；
6. 任一 raw/未知分支使结果元数据降级；
7. 负 UInt64 fallback、Float32、BFloat、Decimal、bigint 不自动定型；
8. 只有 literal、没有 anchor 时保持默认参数规则；
9. `coalesce` 不再把危险 literal 定型为 Float32/BFloat/Decimal/越界 UInt64；
10. pass-through decoder 不再触发错误的首分支范围检查；
11. DateTime64、LowCardinality 等非白名单同型 expression 仍降级，且真实 ClickHouse 用例记录 LowCardinality 的类型归一反例。

### 类型测试

在 `src/public_api.typecheck.ts` 或现有 type scenario 中覆盖：

```ts
const result: Selection<unknown> = fn.if(condition, valueA, valueB)

// @ts-expect-error explicit result generic was removed
fn.if<number>(condition, valueA, valueB)

// @ts-expect-error explicit result generic was removed
fn.multiIf<string>(condition, valueA, valueB)
```

同时验证 `.mapWith(...)` 能建立显式 JS 输出类型。

### ClickHouse E2E

在 `e2e/functions.e2e.test.ts` 使用真实 ClickHouse：

1. `use_variant_as_common_type=0` 下复现 raw `Float64/Int64` 和 `UInt64/Int64` 失败；
2. helper 的 Float64 literal 场景成功，`toTypeName` 为 `Float64`；
3. helper 的 UInt64 literal 场景成功，`toTypeName` 为 `UInt64`；
4. `multiIf` 的所有值分支得到同一 UInt64 类型；
5. `Int32/Int64` expression 由 ClickHouse 提升为 `Int64`，ORM 不再使用 Int32 decoder；
6. `use_variant_as_common_type=1` 下异构分支仍由 ClickHouse 返回 Variant，ORM 不发布错误首分支元数据；
7. conditional 放入 CTE/subquery 后，可证明场景的后续比较参数使用正确 SQL 类型；
8. 真实查询结果与 `.mapWith(...)` 声明的 JS 类型一致。

测试必须同时断言生成参数类型、`Selection.sqlType`、`toTypeName(...)` 和实际 decoder 输出，不能只做 SQL 字符串快照。

## 文档修订

README 必须删除：

```text
Return type follows the then branch (or first arg)
```

替换为：

- conditional 的服务端类型由所有值分支共同决定；
- ORM 仅对安全 literal 做上下文定型；
- `fn.if` / `fn.multiIf` 返回 `Selection<unknown>`；
- 异构 expression 需要显式转换；
- `.mapWith(...)` 只定义 JS decoder，不改变 SQL 类型；
- `fn.over(fn.rowNumber().toMixed())` 进入 conditional 时，fallback safe integer 可自动按 UInt64 编译。

发布说明必须列出泛型移除、静态结果变为 unknown、SQL 参数类型变化以及未知组合 decoder 改为 pass-through。

## 实现范围

预计修改：

- `src/functions.ts`
- `src/functions.test.ts`
- `src/public_api.typecheck.ts`
- `e2e/functions.e2e.test.ts`
- `README.md`

如现有 public API matrix 直接依赖 conditional 泛型，再同步修改对应 `src/type-scenarios/*.typecheck.ts`。不新增依赖，不拆分生产源码文件。

## 验收标准

1. `fn.if<TData>` / `fn.multiIf<TData>` 已从公共 API 删除；
2. conditional 默认静态类型为 `Selection<unknown>`；
3. Float64/UInt64 safe integer literal 在 `if` 和 `multiIf` 中正确上下文定型；
4. 任一异构 expression/raw/未知分支都不会被自动 cast；
5. 无法证明时不再发布首分支 sqlType/decoder；
6. `Int32/Int64` 结果不再被 Int32 decoder 错误解码；
7. `coalesce` 不再进行已知会静默回绕、舍入或截断的 literal 定型；
8. CTE/subquery 不再传播错误 conditional 元数据；
9. 单元、类型、构建和 E2E 测试全部通过；
10. 覆盖率保持 100%；
11. README 与发布说明完整记录破坏性迁移方式；
12. 仓库中不包含内部项目名、本机路径、提交 SHA 或业务规则。

## 后续阶段

以下内容需要独立设计与批准：

1. `coalesce` 完整结果元数据与 Nullable 消除；
2. `greatest` / `least` 最大兼容类型元数据；
3. scalar-only 裸 `null` conditional；
4. 用真实 SQL type witness 恢复安全的 conditional 静态类型推导；
5. 调用统计证明有必要后，再评估 typed conditional helper。

# 告警渠道决策（T001 / FR-OPS-3）

> 类型：决策备忘（decision memo）。`tasks.md` T001 要求"确定告警通道 → 写 `.specify/memory` 备查"。本文件即该备查记录。

| 字段 | 内容 |
|---|---|
| **决策** | 告警渠道 = **飞书**（经 hermes_orchestrator MCP 桥推送） |
| **日期** | 2026-08-31 |
| **状态** | ✅ 用户已拍板（"飞书吧"） |
| **依据** | FR-OPS-3（失败告警，不注册新账号，用既有渠道）；constitution 原则 X（宁可响亮失败，不可静默继续） |

## 1. 为什么是飞书（而非 spec 候选的邮件 / Telegram bot）

FR-OPS-3 与 spec §6.2（行 214）给出的候选是「邮件（既有邮箱）/ Telegram bot（既有账号）」。**实盘调研（2026-08-31，FinAI2.0 + 本机两仓）推翻了这两个候选的"既有"前提**：

| 渠道 | 实测现状 | 结论 |
|---|---|---|
| **飞书** | `hermes_orchestrator` MCP 已连接飞书平台，存在 22 个 dm/群组会话（用户：飞书用户8189JF）；`finai/credentials.py:130` 已有 `get_feishu_credentials()` 框架（读 `FEISHU_<AGENT>_APP_ID/APP_SECRET/ENDPOINT`） | ✅ **唯一已接通、无需注册新账号**的渠道 |
| 邮件 / SMTP | 两仓无任何 SMTP/webhook key；`.env` 仅 3 个 `CITYDATA_*` 键 | ❌ 需从零配置，且邮箱 SMTP 授权码属新凭据 |
| Telegram bot | spec 候选，但 hermes 未连接 Telegram 平台，无 bot token | ❌ 需注册 bot、配置 token，违反"不注册新账号" |

⛔ **遵守"用既有渠道、不注册新账号"原则 ⇒ 飞书是唯一不违反该原则的选项。**

## 2. 落地方式（待 Phase 4 日终任务 / 采集管道接告警时实施）

- **载体**：hermes_orchestrator MCP 的飞书会话（dm 或专用告警群）。
- **凭据**：在 `.env` 增配 `FEISHU_<AGENT>_APP_ID` / `FEISHU_<AGENT>_APP_SECRET`（值仅存 `.env`，⛔ 不入 git、不打印，只显键名）；代码统一走 `finai/credentials.py::get_feishu_credentials()`，不写字符串字面量。
- **触发点**（原则 X + FR-OPS-3）：采集校验失败、无人值守任务异常、对账出现未解释差异、熔断触发 → 立即推送，不静默带伤运行。
- ⛔ **本备忘只锁渠道选型**；具体接哪个会话 / 哪个 Agent 的 app_id、消息格式，到 T403（日终任务）/ T105 采集器接告警时再定，届时仍须不打印密钥值。

## 3. 偏离登记

spec FR-OPS-3 行 142 与 §6.2 行 214 的字面候选为「邮件 / Telegram bot」，本次改选飞书。属**在 spec 允许范围内选用"既有渠道"**（spec 原文："候选：…等——不注册新账号"，飞书符合"既有 + 不注册新账号"的约束内核）。如 spec 三件套后续定稿，应把候选列表补入"飞书（hermes 桥）"以免口径漂移。

## 修订日志

| 日期 | 内容 | 依据 |
|---|---|---|
| 2026-08-31 | 初版：定飞书为告警渠道 | 用户拍板（"飞书吧"）+ 实盘渠道调研（hermes MCP 已连飞书；SMTP/Telegram 均未配置） |

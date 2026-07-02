# AGENTS.md — realtime-loadtest 开发指南

给任何 AI 模型/agent 或人类接手本项目用。读完这份就能直接干活，不用重新摸索。

## 这是什么

Azure OpenAI `gpt-realtime` (GA) 的 WebSocket 压测工具，测 TPM/RPM 上限、找 429 临界点，
并生成可拿去跟 Azure 对峙配额问题的 HTML 报告。全部逻辑在单文件 `realtime_loadtest.py`。

## 开发工作流（重要）

本目录 `~/realtime_loadtest` 就是标准 git 仓库，remote 已配好
(`origin` → github.com/frankxzx/realtime-loadtest)。

```bash
cd ~/realtime_loadtest
# 改 realtime_loadtest.py ...
python3 -c "import ast; ast.parse(open('realtime_loadtest.py').read())"   # 语法自检
git add -A && git commit -m "..." && git push
```

- **直接在这里改 → commit → push**，没有 cp/同步步骤。
- 约定：**改完自动 push，不用问用户**。
- 依赖极少：`pip install websockets`（音频用自带 `hello_world.wav`，不需要 ffmpeg）。

## 怎么跑

```bash
cp .env.example .env && vi .env        # 填 endpoint / api-key / 部署名
python3 realtime_loadtest.py --mode text      --concurrency 10 --duration 60 --html
python3 realtime_loadtest.py --mode audio     --concurrency 5  --duration 60 --html
python3 realtime_loadtest.py --mode transcribe --transcribe-model gpt-realtime-whisper --language en --reuse-conn --html
python3 realtime_loadtest.py --mode text --ramp --ramp-start 5 --ramp-max 100 --ramp-step 5
```

三种模式：`text`(文本补全) / `audio`(语音对话) / `transcribe`(纯转写，独立测 whisper 配额)。
`--ramp` 分批递增并发找 429 临界点。`--html` 生成自包含报告。

**测 whisper 上限必须加 `--reuse-conn`**：转写要经由 realtime 会话才到得了转写模型，
两级配额是串联的。默认（不复用）每次转写都新建 realtime 会话，429 会先撞
realtime 部署的会话创建限流，whisper 根本没被打满。`--reuse-conn` 让每个 worker
只建一次会话，在同一条 WS 上循环 append→commit→completed，负载才真正落到转写模型。
确认 429 归属看 `rate_limits.updated` 的 `name` 字段（报告里单列）。

## 代码结构（单文件分区）

| 区 | 内容 |
|---|---|
| 配置 / .env 加载 | `_load_dotenv`，读脚本同目录的 `.env` |
| 异常类型 | `RateLimitError` / `TranscriptionFailed` / `ResponseFailed` |
| contextvars | `_CTX_BATCH/_CTX_BATCH_CC/_CTX_SEQ/_CTX_WORKER` — 把「第几批第几个」带进日志深处 |
| `EventLog` | ANSI 控制台 + HTML 结构化日志 |
| `GlobalStats` | 计数/滚动 RPM·TPM/首次异常/rate_limits 上报，per-batch |
| 测试音频 | 自带 `hello_world.wav` → say 直出 PCM → 正弦波（三级兜底） |
| WS 助手 | `_ws_send` / `_wait_event` / `_wait_response_done` / `_wait_transcription_completed` |
| 会话 | `run_text_session` / `run_audio_session` / `run_transcribe_session` |
| 编排 | `worker_loop` / `monitor_loop` / `run_load_test` / `run_ramp_test` |
| CLI / main | `parse_args` / `main` |
| `_HTML_TEMPLATE` | 自包含深色报告（卡片/异常分类/时序图/429详情/配额/日志表/CSV） |

## GA schema 铁律（踩过很多坑，改前必看）

> **规则：动任何 API 字段/事件 schema 前，先查官方文档确认，别凭记忆。**
> 参考：[Azure Realtime 参考](https://learn.microsoft.com/en-us/azure/foundry/openai/realtime-audio-reference)、
> [OpenAI Realtime](https://developers.openai.com/api/docs/guides/realtime)。

GA `session.update` 里**已删除**的字段（加回去必报错）：
- `temperature`、`max_response_output_tokens` — 没了
- `modalities` — 改叫 `output_modalities`，且**不能是空数组**（至少含 `"text"` 或 `"audio"`）
- 顶层 `input_audio_format`、`input_audio_transcription` — 移到 `audio.input.*` 下

正确 `session`（audio）关键形状：
- `type:"realtime"`（Azure 必填）
- `audio.input`: `{format:{type:"audio/pcm",rate:24000}, noise_reduction:{type:"far_field"},
  transcription:{model:"<部署名>"}, turn_detection:{type:"server_vad",...,create_response:false}}`
- `audio.output`: `{format:{...}, voice:"alloy"}`

流程差异：
- **audio 对话**：`input_audio_buffer.append` → `response.create`（**不发 commit**，turn_detection.create_response:false 防重复）
- **transcribe 纯转写**：`session.type:"realtime"` + `output_modalities:["text"]`，
  `append` → `input_audio_buffer.commit`（commit 触发转写）→ 等
  `conversation.item.input_audio_transcription.completed`（**不发 response.create**）

WS URL：`wss://{endpoint}/openai/v1/realtime?model={realtime部署名}`（transcribe 也连 realtime 部署，
转写模型部署名放 `audio.input.transcription.model`）。转写 token 只在 `.completed` 事件的 `usage` 里，
不在 `response.done`。Azure 要求 transcription.model 填**部署名**（不是 `whisper-1` 这种通用名）。

## 异常/429 模型（6 类，别漏）

429 不是单一 HTTP 错误，取决于何时撞限流。**429 按来源分两类，归因完全不同**：
- **握手 429（`source="handshake"`）**：WS 握手被拒 → `InvalidStatus`，错误信息带
  `onHandshake Operation`，是 **S0 tier 连接建立速率限制**，跟模型/转写配额无关。
  缓解：`--reuse-conn` + `--connect-stagger`（默认 0.25s/个错峰）。
- **会话内 429（`source="session"`）**：`error` 事件 / `transcription.failed` 限流，
  这才是模型（whisper）配额被打满的证据。
明细：
- **HTTP 429**：WS 握手被拒 → `InvalidStatus`（连接级）
- **`error` 事件**：会话中报错（JSON，非 HTTP）
- **`response.done` status=failed**：`status_details.error`（曾被误当成功，已修）
- **`input_audio_transcription.failed`**：转写失败（限流或 `audio_unintelligible`）
- **`rate_limits.updated`**：Azure 主动上报 `{name,limit,remaining,reset_seconds}` — 对峙金证据，报告里单列

判限流用关键字：`rate/429/rate_limit/too many requests/quota/exceeded`（见 `_is_rate_limit`）。
计数拆 6 类：429 / 转写失败 / response失败 / 超时 / 连接错误 / 其他失败。
每个请求带 batch(第几批) + seq(批内第几个)，首次异常会定位到「第几批第几个请求」。

## SSL

已全局关闭校验（`_SSL_CTX`，`CERT_NONE`）——压测便利，生产勿用。

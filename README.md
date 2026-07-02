# realtime-loadtest

Azure OpenAI `gpt-realtime` WebSocket 压测工具，用于测量 TPM / RPM 上限。

支持：
- **文本模式** — 纯文本对话，快速摸清 TPM/RPM 配额
- **音频模式** — 发送固定的真实 TTS 语音（自带 `hello_world.wav`），走完整语音对话链路
- **转写模式** — 纯转写会话（`session.type=transcription`），**独立测 input audio transcription 模型（gpt-realtime-whisper）的 RPM/TPM**，不走对话补全
- **Ramp 模式** — 自动递增并发数，找到 429 限流临界点
- **HTML 报告** — 自包含报告，含时序图、摘要卡片、429 详情、CSV 导出（可直接发给 Azure 支持）

## 依赖

```bash
pip install websockets
```

音频 / 转写模式使用仓库自带的固定音频 `hello_world.wav`（真实 "Hello world" TTS，24kHz mono PCM16，~0.74s），**无需 `ffmpeg` 或任何额外依赖**。若该文件缺失，脚本会依次尝试 `macOS say` 直出 PCM、最后回退到正弦波。

## WebSocket 端点

使用 GA（正式版）API：

```
wss://<resource>.openai.azure.com/openai/v1/realtime?model=<deployment>
```

> Preview 版（`/openai/realtime?api-version=...&deployment=...`）已于 2026-04-30 废弃。

## 快速开始

```bash
cp .env.example .env
vi .env   # 填入 AZURE_OPENAI_ENDPOINT / AZURE_OPENAI_API_KEY / REALTIME_DEPLOYMENT

python3 realtime_loadtest.py --mode text --concurrency 10 --duration 60
```

### 文本模式

```bash
python3 realtime_loadtest.py --mode text --concurrency 10 --duration 60
```

### 音频模式（语音对话链路）

```bash
python3 realtime_loadtest.py --mode audio --concurrency 5 --duration 60
```

音频模式启动时加载仓库自带的固定 `hello_world.wav`（~0.74s，24kHz mono PCM16），所有并发连接共用同一份音频数据，转写结果稳定可复现。

### 转写模式（独立测转写模型配额）

```bash
python3 realtime_loadtest.py --mode transcribe \
  --transcribe-model gpt-realtime-whisper --language en \
  --reuse-conn --concurrency 10 --duration 60 --html
```

> **测转写模型上限必须加 `--reuse-conn`。** 转写请求必须经由 realtime 会话才到得了转写模型，两级配额是串联的：默认（不复用）每次转写都新建 realtime 会话，429 会先撞 **realtime 部署的会话创建限流**，转写模型根本没被打满。`--reuse-conn` 让每个 worker 只建一次会话，在同一条 WS 上循环 `append → commit → completed`，负载才真正落到转写模型上。429 归属看 `rate_limits.updated` 的 `name` 字段（报告里单列）。

用 `session.type = "realtime"` + `output_modalities = ["text"]` 开一个**纯转写会话**，靠不发 `response.create` 来避免任何 LLM 补全，只命中 input audio transcription 模型（如 `gpt-realtime-whisper`），因此报告里的 token / RPM 全部归属转写模型本身——用来确认转写模型的配额是否被真正吃满。（`output_modalities` 不能为空数组，API 要求至少含 `text` 或 `audio`）

流程：`session.update(type=realtime, output_modalities=["text"])` → `input_audio_buffer.append` → `input_audio_buffer.commit`（触发转写）→ 等 `conversation.item.input_audio_transcription.completed`（取 `transcript` + `usage`）。

- WebSocket 仍连 **realtime 部署**（`--deployment`），转写模型部署名走 `--transcribe-model`（默认 `$WHISPER_DEPLOYMENT`）
- `--language` 可选，留空自动检测
- `whisper-1` 是按时长计费（无 token）；`gpt-realtime-whisper` 按 token 计费，报告 TPM 才有意义

## 429 与所有异常来源（重要）

Realtime API 是 WebSocket，**429 不是单一的 HTTP 报错**——取决于什么时候撞限流，会以不同形式出现（已对照 [Azure 官方文档](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/realtime-audio-websockets) 与 [OpenAI Realtime 规范](https://developers.openai.com/api/docs/guides/realtime) 核实）：

| 来源 | 何时 | 形式 | 说明 |
|---|---|---|---|
| **HTTP 429（握手）** | 建立连接时 | WS upgrade 返回 429（非 101），`InvalidStatus` | 连接级限流：并发连接数/连接速率超限，可能带 `Retry-After` 头 |
| **`error` 事件** | 会话中 | JSON 事件 `{"type":"error","error":{type,code,message,param,event_id}}` | 会话内报错，限流也可能走这里（非 HTTP） |
| **`response.done` status=failed** | 生成响应时 | 事件里 `response.status="failed"` + `status_details.error` | 之前会被误当成成功，现已修复：按 `status_details` 分类 |
| **`input_audio_transcription.failed`** | 转写时 | JSON 事件带 `error.code` | 限流走这里，或 `audio_unintelligible` 等真实转写失败 |
| **`rate_limits.updated`** | 每次 response.done 后 | `{rate_limits:[{name,limit,remaining,reset_seconds}]}` | **Azure 主动上报的配额**——对峙金证据，直接看到官方声明的 limit/remaining |

**脚本把异常拆成 6 类分别计数**：**429 限流 / 转写失败 / response失败 / 超时 / 连接错误 / 其他失败**。判定限流用关键字（rate / 429 / rate_limit / too many requests / quota / exceeded），`.failed`、`error`、`response.done` 里命中的都归 429。

**429 再按来源拆成两类，归因完全不同**（报告卡片、异常 chips、429 详情表、CSV 都分列）：

| source | 触发点 | 归因 | 典型错误信息 |
|---|---|---|---|
| `handshake` | WS 握手被拒（HTTP 429） | **连接建立速率限制**（如 S0 tier 的 onHandshake call rate），**不是模型配额** | `Requests to the onHandshake Operation ... exceeded call rate limit of your current OpenAI S0 pricing tier` |
| `session` | 会话内 `error` / `transcription.failed` 限流 | **模型/转写配额被打满**——测 whisper 上限要看的就是它 | `rate_limit_exceeded` 等 |

如果报告里 429 全是 `handshake`，说明连模型都没摸到就被连接速率拦了：开 `--reuse-conn`、调大 `--connect-stagger`，或申请提升 tier；只有 `session` 429 才能证明转写模型配额到顶。

log 和 HTML 报告都会体现：
- HTML「异常分类」chips（6 类计数）+ 首次异常定位（第几批第几个）
- HTML「Azure 上报的配额」区：`rate_limits.updated` 里 Azure 声明的 limit / 最低 remaining / 每次采样明细 —— 这是证明「配额没给够/被提前限流」最硬的证据
- `rate_limits.updated` 也会实时打印到控制台日志

### Ramp 模式（找限流临界点）

```bash
python3 realtime_loadtest.py --mode text --ramp \
  --ramp-start 5 --ramp-max 100 --ramp-step 5 --ramp-step-duration 30
```

从 5 并发开始，每步增加 5，每步跑 30 秒，遇到 429 立即停止并输出汇总表。

**每一步就是「一批」（batch）**。压测按批递增，每批内部给每个请求一个递增序号（`#seq`），因此日志里能精确定位**第几批、第几个请求首次出现异常**：

- 控制台每行日志带 `B<批号> [W<worker>#<序号>]` 前缀
- 首次异常（429 / 失败 / 连接错误）会打印醒目横幅：`★★★ 首次异常 ★★★ 第 N 批(并发 X) · 第 M 个请求 · Wkk`
- Ramp 汇总表新增「首次异常」列
- HTML 报告：摘要卡片「首次异常(第几批第几个)」、日志表新增「批 / #」列 + 批次筛选 + 「仅异常」勾选、首次异常行高亮标 `⚑`、429 详情表带批次/序号、CSV 导出含 batch/seq

### 生成对峙报告

用于向 Azure OpenAI 投诉或对峙配额问题：

```bash
python3 realtime_loadtest.py \
  --mode text --concurrency 20 --duration 120 \
  --expected-tpm 50000 --expected-rpm 100 --region eastus2 \
  --html
```

生成 `realtime_report_text_c20_<时间戳>.html`，包含：
- **测试元信息**：端点、部署、区域、时间、期望配额 vs 实测
- **摘要卡片**：峰值 RPM/TPM、首次 429 触发时刻、成功率、P95 延迟
- **时序图**：RPM/TPM 折线 + 429 事件标注 + 期望配额虚线
- **429 详情表**：每次限流的错误码、Retry-After、RPM/TPM 快照
- **CSV 导出**：一键导出含时序数据的 CSV

## 实时输出

```
时间    RPM(1m)    TPM(1m)   成功   429  失败   P50ms   P95ms    总Token
   5s       120       2400      10     0     0     320     450       200
  10s       240       4800      20     0     0     310     440       400
  15s       360       7200      30     3     3     315     460       600
```

| 列 | 说明 |
|---|---|
| RPM(1m) / TPM(1m) | 滚动 60 秒窗口的实时值，与 Azure 限流计算方式一致 |
| 429 | 触达配额上限的次数（黄色高亮） |
| P50 / P95 | 端到端延迟（连接建立 → response.done） |

## 参数说明

| 参数 | 默认 | 说明 |
|---|---|---|
| `--mode` | `text` | `text` / `audio` / `transcribe` |
| `--deployment` | `$REALTIME_DEPLOYMENT` | realtime 部署名（WS 连接用） |
| `--transcribe-model` | `$WHISPER_DEPLOYMENT` | 转写模型部署名（仅 `transcribe` 模式） |
| `--language` | — | 转写语言 ISO-639-1（仅 `transcribe` 模式，留空自动检测） |
| `--reuse-conn` | 关 | `transcribe` 模式复用 WS：每 worker 一次会话内循环转写，测转写模型上限必开 |
| `--connect-stagger` | `0.25` | worker 首次握手错峰间隔（秒/个），防一批 worker 同时握手撞 S0 tier 连接速率（`onHandshake` 429）；设 0 关闭 |
| `--concurrency` | `5` | 并发 WebSocket 连接数 |
| `--duration` | `60` | 压测持续秒数 |
| `--interval` | `0` | 每个 worker 两次请求间隔（秒） |
| `--html` | — | 生成 HTML 报告 |
| `--expected-tpm` | `0` | Azure 承诺的 TPM 配额（写入报告用于对比） |
| `--expected-rpm` | `0` | Azure 承诺的 RPM 配额（写入报告用于对比） |
| `--region` | — | Azure 区域（如 `eastus2`），写入报告 |
| `--verbose / -v` | — | 打印所有 WS 发送/接收事件 |
| `--ramp` | — | 开启自动递增并发模式 |
| `--ramp-start` | `1` | 起始并发数 |
| `--ramp-max` | `50` | 最大并发数 |
| `--ramp-step` | `5` | 每步增量 |
| `--ramp-step-duration` | `30` | 每步持续秒数 |

## 环境变量

```bash
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com
AZURE_OPENAI_API_KEY=your-api-key-here
REALTIME_DEPLOYMENT=gpt-realtime-1.5
WHISPER_DEPLOYMENT=gpt-realtime-whisper   # 音频模式使用
```

脚本启动时自动加载与脚本同目录的 `.env`，不需要 `source`。

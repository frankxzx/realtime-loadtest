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
  --concurrency 10 --duration 60 --html
```

用 `session.type = "realtime"` + `output_modalities = ["text"]` 开一个**纯转写会话**，靠不发 `response.create` 来避免任何 LLM 补全，只命中 input audio transcription 模型（如 `gpt-realtime-whisper`），因此报告里的 token / RPM 全部归属转写模型本身——用来确认转写模型的配额是否被真正吃满。（`output_modalities` 不能为空数组，API 要求至少含 `text` 或 `audio`）

流程：`session.update(type=realtime, output_modalities=["text"])` → `input_audio_buffer.append` → `input_audio_buffer.commit`（触发转写）→ 等 `conversation.item.input_audio_transcription.completed`（取 `transcript` + `usage`）。

- WebSocket 仍连 **realtime 部署**（`--deployment`），转写模型部署名走 `--transcribe-model`（默认 `$WHISPER_DEPLOYMENT`）
- `--language` 可选，留空自动检测
- `whisper-1` 是按时长计费（无 token）；`gpt-realtime-whisper` 按 token 计费，报告 TPM 才有意义

**异常分类**：转写被限流时，429 不一定走 WS 握手，常以 `conversation.item.input_audio_transcription.failed` 事件返回。脚本会把异常拆成 5 类并分别计数：**429 限流 / 转写失败 / 超时 / 连接错误 / 其他失败**。`.failed` 里带限流关键字的归 429，其余归「转写失败」。log 和 HTML 报告都会体现（HTML 有「异常分类」chips + 首次异常定位）。

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

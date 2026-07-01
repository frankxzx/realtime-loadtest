# realtime-loadtest

Azure OpenAI `gpt-realtime` WebSocket 压测工具，用于测量 TPM / RPM 上限。

支持：
- **文本模式** — 纯文本对话，快速摸清 TPM/RPM 配额
- **音频模式** — 发送真实 TTS 语音（macOS `say` 生成），触发 Whisper 转写链路
- **Ramp 模式** — 自动递增并发数，找到 429 限流临界点
- **HTML 报告** — 自包含报告，含时序图、摘要卡片、429 详情、CSV 导出（可直接发给 Azure 支持）

## 依赖

```bash
pip install websockets
```

音频模式额外需要 macOS 内置 `say` 命令和 `ffmpeg`：

```bash
brew install ffmpeg
```

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

### 音频模式（触发 Whisper 转写）

```bash
python3 realtime_loadtest.py --mode audio --concurrency 5 --duration 60
```

音频模式会在启动时用 `say "Hello world"` 生成一段 ~0.7s 的 PCM16 24kHz 语音，所有并发连接共用同一份音频数据。若 `say`/`ffmpeg` 不可用，自动回退到 440Hz 正弦波。

### Ramp 模式（找限流临界点）

```bash
python3 realtime_loadtest.py --mode text --ramp \
  --ramp-start 5 --ramp-max 100 --ramp-step 5 --ramp-step-duration 30
```

从 5 并发开始，每步增加 5，每步跑 30 秒，遇到 429 立即停止并输出汇总表。

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
| `--mode` | `text` | `text` 或 `audio` |
| `--deployment` | `$REALTIME_DEPLOYMENT` | Azure 部署名 |
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

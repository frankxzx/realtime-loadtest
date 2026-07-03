# realtime-loadtest

Azure OpenAI `gpt-realtime` WebSocket 压测工具，用于测量 TPM / RPM 上限。

支持：
- **文本模式** — 纯文本对话，快速摸清 TPM/RPM 配额
- **音频模式** — 发送固定的真实 TTS 语音（自带 `hello_world.wav`），走完整语音对话链路
- **转写模式** — 纯转写会话（`session.type=transcription`），**独立测 input audio transcription 模型（gpt-realtime-whisper）的 RPM/TPM**，不走对话补全
- **对话场景模式（chat）** — 保险坐席×AI客户：注入 mock 多轮聊天历史（坐席一轮 ≈ 1 分钟话术），模型扮演客户回话；配 `--sync-fire` 可让 N 路连接**同一时间戳**齐发请求
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

> **测转写模型上限必须加 `--reuse-conn`，最好再加 `--pipeline N`。** 转写请求必须经由 realtime 会话才到得了转写模型，两级配额是串联的：默认（不复用）每次转写都新建 realtime 会话，429 会先撞 **realtime 部署的会话创建限流**（S0 tier `onHandshake`），转写模型根本没被打满。
>
> - `--reuse-conn`（串行复用）：每 worker 一次握手，同一条 WS 循环 `append → commit → completed`。但每连接同时只有 **1 个在途转写**，吞吐被单次转写延迟（~4.5s）限死——10 并发每分钟才 ~130 次。
> - `--pipeline N`（管道化，隐含复用）：`commit` 是异步的，**不等上一个 completed 就连发 N 个 commit**，服务端并行转写、完成事件按 `item_id` 对账（官方文档明确"完成事件顺序不保证，用 item_id 匹配"）。总在途 = 并发 × N：`--concurrency 10 --pipeline 10` = **100 个在途转写，只需 10 次握手**，这才是打满转写模型配额的压力形态。
> - `--burst N`（脉冲）：持续模式（`--duration`）测的是"稳态能扛多少"；burst 测的是"**瞬间打 N 个会发生什么**"。N 个请求按并发均分，每条连接**一口气 commit 完不等回包**，然后等全部结算：成功多少、`session` 429 多少、什么错误码，一目了然。`--burst 1000 --concurrency 10` = 瞬间 1000 个在途转写。

```bash
# 脉冲测试：一次性打 1000 个转写请求，看服务端在什么量级报什么错
python3 realtime_loadtest.py --mode transcribe \
  --transcribe-model gpt-realtime-whisper --language en \
  --burst 1000 --concurrency 10 --html
```

用 `session.type = "realtime"` + `output_modalities = ["text"]` 开一个**纯转写会话**，靠不发 `response.create` 来避免任何 LLM 补全，只命中 input audio transcription 模型（如 `gpt-realtime-whisper`），因此报告里的 token / RPM 全部归属转写模型本身——用来确认转写模型的配额是否被真正吃满。（`output_modalities` 不能为空数组，API 要求至少含 `text` 或 `audio`）

流程：`session.update(type=realtime, output_modalities=["text"])` → `input_audio_buffer.append` → `input_audio_buffer.commit`（触发转写）→ 等 `conversation.item.input_audio_transcription.completed`（取 `transcript` + `usage`）。

- WebSocket 仍连 **realtime 部署**（`--deployment`），转写模型部署名走 `--transcribe-model`（默认 `$WHISPER_DEPLOYMENT`）
- `--language` 可选，留空自动检测
- **whisper 家族（含 `gpt-realtime-whisper`）按音频时长计费**（$0.017/分钟），`completed` 事件的 `usage` 是 `{"type":"duration","seconds":N}`，**没有 token 计数**——这是官方行为，不是异常。脚本识别此形态：累计"转写音频秒数"，报告里 Token/TPM 卡片自动换成「转写速率 (s/min)」和「转写音频总时长」；whisper 场景看 **RPM 和转写速率**，TPM 恒为 0 无意义
- `gpt-4o-transcribe` 系才按 token 计费（`usage` 带 `input_tokens`/`output_tokens`），报告 TPM 有意义

### 对话场景模式（chat，模拟真实业务流量）

```bash
# 100 路并发，全部建好连接、注好历史后，同一时刻齐发 response.create
python3 realtime_loadtest.py --mode chat --concurrency 100 --sync-fire --html
```

模拟真实的保险电销业务形态：每条连接注入一段 **mock 多轮聊天历史**（坐席一轮 ≈ 1 分钟话术、约 280 字，客户简短回应），然后把坐席的最新话术发给模型，**模型扮演客户**生成回复。内置 3 个话本（重疾险电销 / 百万医疗险续保 / 增额寿养老年金），按 worker 轮换，避免所有请求命中同一份 prompt。

- 历史经 `conversation.item.create` 注入：坐席 = `user`/`input_text`，客户 = `assistant`/`output_text`（GA schema）
- 每轮请求带全量历史上下文（1500+ 字），token 负载贴近真实生产
- **`--sync-fire`（齐射）**：worker 仍按 `--connect-stagger` 错峰握手（避开 `onHandshake` 429 干扰归因），全部注好历史后集合，**同一时刻**齐发 `response.create`，打完这一轮即止（忽略 `--duration`）。用来回答"**同一时间戳打 100 个并发请求，模型配额会怎样**"。控制台会打出开火时刻与就绪路数（`⚡ 同步开火: 100/100 路就绪 @ <UTC时间戳>`），齐射的延迟从开火时刻起算
- 不加 `--sync-fire`：持续模式，每 worker 循环整段话本直到 `--duration` 到点，会话结束自动重连开新会话

### 1006 随机断连排查（chat 模式）

生产上 WebSocket 挂 1006（异常断连：没收到 close frame，TCP 被直接掐）且**时间点随机、阶段随机**（可能第 5 分钟、第 7 分钟；可能正在生成、也可能刚发完话术）时，用长电话 soak 做**统计型重现 + 取证**：

```bash
# 100 路整通长电话（同一条 WS 不重连），每轮回复后静默 45s 模拟坐席讲话，跑 30 分钟
python3 realtime_loadtest.py --mode chat --concurrency 100 --session-loop \
  --turn-gap 45 --duration 1800 --html
```

- `--session-loop`：同一条 WS 循环话本直到 duration，不主动断开——单连接存活十几分钟，才够得着「第 5/7 分钟随机断」的窗口；断了会自动重连继续攒时长
- `--turn-gap N`：每轮回复后静默 N 秒（坐席讲话中，WS 完全空闲只剩 keepalive ping），复刻真实通话的「说完刚发送/空闲」窗口

**断连取证（自动）**：每次中途断连记录 close code（没收到 close frame 即 1006）、**断连时所处阶段**（`send_turn` 刚发完话术 / `awaiting_response` 正在生成 / `idle_gap` 静默中）、**连接存活时长**、最近一次 keepalive RTT。汇总输出怎么读：

| 证据 | 怎么读 |
|---|---|
| `abnormal_close_by_code` | 断连按 close code 分布；全是 1006 = 被硬掐，出现 1011/1001 = 服务端体面关闭 |
| **同时刻断连成簇检测**（2s 窗口） | **成簇（N 路同秒死）= 服务端/网关侧事件**（节点回收、扩缩容、负载丢弃）；**随机分散 = 单连接层面**（网络路径、keepalive） |
| 连接存活时长分布 | 集中在固定值（如 ~240s / ~30min）= 空闲超时或会话寿命；随机 = 容量/基础设施 |
| `abnormal_per_conn_hour` | 断连率按「连接·小时」归一化，**直接和生产断连率对比**，判断是否复现了同一量级 |
| 事件循环滞后探针 | monitor 每 5s 测压测机 loop lag，>200ms 告警——lag 大时 pong 回不及时，1006 可能是压测机自伤，别甩锅服务端 |

排查 keepalive 假设可调 `--ping-interval` / `--ping-timeout`（默认 20/20s；`--ping-interval 0` 禁用客户端 ping，看纯靠服务端 ping 会不会被掐）。

### 本地复现 1006：带病客户端 A/B 对照（`--sim-pcm-accumulate` + `mock_gateway.py`）

如果怀疑 1006 的根因在**自己客户端**（典型病灶：录音用 bytes 属性 `buf += pcm_chunk` 累积——O(n²) 全量复制；以及"说完一轮同步转 MP3"跑在事件循环上），可以在本地闭环复现，不用碰 Azure：

```bash
# 终端 1：严格网关 mock——每 5s ping 一次，5s 等不到 pong 直接 RST（不发 close frame，
#         客户端视角就是 1006，与生产网关行为一致）
python3 mock_gateway.py --port 9800

# 终端 2：
export AZURE_OPENAI_ENDPOINT=http://127.0.0.1:9800 AZURE_OPENAI_API_KEY=local

# A 干净对照组：预期 0 断连、loop lag 几 ms
python3 realtime_loadtest.py --mode chat --concurrency 8 --session-loop \
  --turn-gap 2 --duration 60 --connect-stagger 0.05

# B 带病实验组：预期 loop lag 飙到秒级、1006 全进程成簇
python3 realtime_loadtest.py --mode chat --concurrency 6 --session-loop \
  --turn-gap 2 --duration 75 --connect-stagger 0.05 \
  --sim-pcm-accumulate --sim-rate-mb-min 30
```

`--sim-pcm-accumulate` 给每条连接开一个后台"录音"任务，完整复刻病灶：`bytes += chunk`（按墙钟补账，事件循环卡住期间"到达"的帧解卡后突发处理——复刻"越卡越追账、越追越卡"的正反馈），且每轮说完后按 `--sim-encode-mbps`（默认 5MB/s）同步"转码"硬阻塞。`--sim-rate-mb-min` 默认 2.88（24kHz/16bit 真实速率）；本地快速复现调到 30~60，等效于把通话时间轴压缩、几十秒内看到第 5-7 分钟的病情。

判读：A、B 唯一区别是客户端有没有病，服务端（mock）完全相同。若 A 组 0 断连、B 组 1006 且伴随 loop-lag 告警——1006 是客户端事件循环被强占导致 pong 超时被网关掐，与服务端无关。生产验证同理：给生产进程加 loop-lag 探针、把 1006 时间戳和"通话结束/导出 MP3"时刻对表。修复：缓冲改 `bytearray.extend()`（或 list 攒块最后 `join`），转码用 `asyncio.create_subprocess_exec` 异步调 ffmpeg 或丢给独立 worker 进程——编码一毫秒都不该发生在事件循环线程上。

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
| `--mode` | `text` | `text` / `audio` / `transcribe` / `chat`（保险坐席对话场景，AI 扮演客户） |
| `--sync-fire` | 关 | 仅 `chat` 模式：全员建连+注入历史后**同一时刻**齐发一轮 `response.create`，打完即止（忽略 `--duration`） |
| `--session-loop` | 关 | 仅 `chat` 模式：同一条 WS 循环话本到 `--duration` 不重连（整通长电话），钓随机断连（1006） |
| `--turn-gap` | `0` | 仅 `chat` 模式：每轮回复后静默秒数（坐席讲话中 WS 完全空闲），真实通话形态建议 30~60 |
| `--ping-interval` | `20` | `chat` 模式 WS keepalive ping 间隔秒，`0` 禁用客户端 ping |
| `--ping-timeout` | `20` | `chat` 模式等 pong 超时秒，超时客户端按 keepalive 失败断开 |
| `--sim-pcm-accumulate` | 关 | 仅 `chat` 模式：带病客户端模拟（`bytes+=` 录音累积 + 每轮同步转码阻塞），本地复现 1006 用 |
| `--sim-rate-mb-min` | `2.88` | 模拟录音累积速率 MB/分钟（2.88=24kHz/16bit 真实速率；快速复现用 30~60） |
| `--sim-encode-mbps` | `5` | 模拟同步转码速度 MB/s，阻塞时长=缓冲/速度；`0` 只累积不转码 |
| `--deployment` | `$REALTIME_DEPLOYMENT` | realtime 部署名（WS 连接用） |
| `--transcribe-model` | `$WHISPER_DEPLOYMENT` | 转写模型部署名（仅 `transcribe` 模式） |
| `--language` | — | 转写语言 ISO-639-1（仅 `transcribe` 模式，留空自动检测） |
| `--reuse-conn` | 关 | `transcribe` 模式复用 WS：每 worker 一次会话内循环转写，测转写模型上限必开 |
| `--pipeline` | `1` | 每条连接在途转写数（管道深度），>1 隐含复用连接；总在途=并发×管道 |
| `--burst` | `0` | 脉冲模式（仅 `transcribe`）：N 个请求按并发均分、每连接一口气 commit 完，等全部结算后结束，忽略 `--duration` |
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

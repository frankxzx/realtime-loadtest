# 生产 WebSocket 1006 断连：根因与最小修复方案

## 结论（一句话）

1006 不是 Azure 侧问题，是**客户端事件循环被强占** → keepalive `pong` 回不出去 →
被网关按超时 `RST` 掐掉（不发 close frame，客户端记为 1006）。两处病灶都在音频处理链路：
录音缓冲 `bytes += chunk` 的 O(n²) 复制，以及同步 ffmpeg/pydub 转码阻塞事件循环。

## 症状为何对得上

- **时间随机（第 5/7 分钟都有）**：由「哪次转码/复制停顿恰好撞上网关 ping 窗口」决定，
  不是固定超时 → 看起来随机。
- **阶段随机（正在生成 / 刚说完）**：生成时下行 delta 最猛、背压最快；刚说完是做大块
  append/转码的时刻。两处都在制造事件循环停顿。
- **CPU/内存监控正常**：GIL 限制单 Python 进程只吃满一个核，16 核机器总占用显示个位数
  百分比；`bytes +=` 复制是瞬时的、旧缓冲立即释放，RSS 几乎不涨。
  **唯一能看见它的指标是事件循环滞后（loop lag），而这个大概率没在采。**

## 本地已复现（A/B 对照，见 README「本地复现 1006」）

同一个 mock 网关，唯一变量是客户端有没有病灶：

| | A 干净客户端 | B 带病客户端（`--sim-pcm-accumulate`） |
|---|---|---|
| 中途断连 | 0 | 6 次，全部 code=1006 |
| 断连形态 | — | 同进程成簇：+52s×3 路、+79s×3 路 |
| 事件循环滞后 | 2.7ms | 2.7s → 10.5s → 23.1s（O(n²) 正反馈滚雪球） |

复现命令见 `README.md`，工具为本仓库 `realtime_loadtest.py --mode chat` + `mock_gateway.py`。

---

## 修复（分级：先止血，再到位）

### 修复 0：两行止血补丁（侵入最小，先上这个）

> **生产实况更正（已核对实际代码）**：收集侧是 `b64decode(delta)` → `list.append`
> 且每轮重置——**无累积病灶，修复①不适用，可跳过**。真实病灶在轮末：
> `asyncio.create_task(handle_*_audio_convert_upload(...))` —— **create_task 不会把
> 工作挪出事件循环**，若该函数内部转码是同步的（subprocess.run / pydub.export /
> wave+audioop），数秒硬阻塞照样落在 loop 上。**只需修复②**，两种形态
> （3 行手术级 / 整体替换级）见 [`examples/fix_convert_upload.py`](../examples/fix_convert_upload.py)，
> 已验证 100 路并发转码期间事件循环最大滞后 72ms。
> 以下 ①①-A/①-B 仅当代码里另有 `+=` 累积时才需要。

两个病灶各改一处、共约四行、零结构改动，1006 的机制链两个源头即断。
注意**只改其一不够**：只改 extend 治不了轮末同步转码那次 3-5 秒硬阻塞
（恰恰最匹配「刚说完就断」的生产观察）；只改转码治不了 O(n²) 慢性累积
（10 分钟以上长通话单独致死）。

```python
# ① 治 O(n²)。生产实际累积的是 base64 字符串（self.buf += delta_b64，str 挂属性，
#    实测 7 分钟 10.7s，O(n²)）。两个版本任选：

# ①-A 语义零变化版（3 行）：join 与重复 += 产出逐字节相同
self.chunks = []                                  # 原 self.buf = ""
self.chunks.append(delta_b64)                     # 原 self.buf += delta_b64
pcm = base64.b64decode("".join(self.chunks))      # 原 b64decode(self.buf)

# ①-B 推荐版（2 改 1 删）：逐 chunk 独立解码进 bytearray，顺手排掉填充哑弹
self.buf = bytearray()
self.buf.extend(base64.b64decode(delta_b64))      # 每 delta 本就是独立完整 base64
# 收尾的 base64.b64decode(self.buf) 整行删掉，直接用 self.buf 喂后面

# ② 治转码阻塞：原同步转码函数一个字不改，挪进线程池
self._export_mp3(wav_path, mp3_path)                       # ← 病灶（卡 loop 数秒）
await asyncio.get_running_loop().run_in_executor(
    None, self._export_mp3, wav_path, mp3_path)            # ← 线程等 ffmpeg，GIL 释放
# 默认线程池 ~32 工位，100 路同时收尾自然排队 = 免费限流
```

> **base64 拼接解码的哑弹（实测确认）**：带 `=` 填充的 chunk 拼接后整体
> `b64decode`，会在第一个 `=` 处**静默截断丢数据、不报错**。现在没炸只是因为
> Realtime 音频 delta 的字节数恰好是 3 的倍数（无中间填充）——这不是 API 契约。
> ①-B 逐 chunk 解码天然免疫；①-A 保留此隐性假设，故推荐 ①-B。

实测收益：①把 7 分钟通话的累积停顿从 ~10.7s 压到 ~1ms；②把轮末 N 秒硬阻塞
从事件循环上清零。上线后用 loop-lag 探针 + 1006 率验证（见下文）。

修复 1/2 是**目标态**（内存有界、崩溃可救半截音频、甩掉 wave/audioop 依赖），
留作下个迭代或 Python 3.13 镜像升级时一并做，不是止血必需。

### 修复 1：别把整通电话的音频攒内存 → 流式写盘 + 轮末转码

根子上的问题不是「`+=` 用错类型」，是**整通电话的音频不该攒在内存里**。

**注意 base64 陷阱**：Realtime WS 里音频是 base64 字符串。`str` 和 `bytes` 都不可变，
`+=` 拼接一律 O(n²)，base64 救不了（还大 33%）。CPython 对「**局部变量 + str**」的 `+=`
有个原地 realloc 特判会让它变快，但**换成 `bytes`、或存到 `self.buf` 属性、或全局变量，
优化立刻失效跌回 O(n²)**——而生产音频缓冲几乎必然是 `self.buf`（属性）或已解码的 PCM
`bytes`，两者都在悬崖下面。实测（累积 7 分钟音频）：

| 累积写法 | 7 分钟耗时 | 复杂度 |
|---|---|---|
| 局部 `s += b64`（str，踩中 CPython 特判） | 20ms | O(n) |
| 局部 `b += pcm`（bytes） | 8.9 秒 | O(n²) |
| 属性 `self.buf += b64`（str） | 10.7 秒 | O(n²) |
| **流式写盘 + `list`/文件**（修复） | ~90ms，内存恒定 | O(n) |

**正确姿势**：每个音频 delta 到达 → base64 解码一次 → 流式写入本轮 `.pcm` 文件（内存恒定、
无不可变拼接）；一轮 QA 结束后异步转 MP3（见修复 2）。参考实现见
[`examples/turn_audio_recorder.py`](../examples/turn_audio_recorder.py)（`start/feed/finish`
三调用即可贴进现有 WS 循环，自带自测；已验证 feed 峰值内存 KB 级、100 路并发转码不死锁）。

```python
# 极简版（不落盘、内存内一轮）：list 攒 chunk，收尾 join 一次，同样 O(n) 且不踩 CPython 陷阱
self.chunks = []
def on_delta(delta_b64: str):
    self.chunks.append(base64.b64decode(delta_b64))   # O(1) 追加
pcm = b"".join(self.chunks)                           # 收尾一次，O(n)
```

> 若同时存在「每来一个 chunk 就对全量缓冲调一次 `audioop.*`」，那是同类 O(n²) 病灶，
> 改成收尾一次性处理，或用流式/分块处理。

### 修复 2：同步转码 → 异步子进程（转码移出事件循环）

`audioop`/`wave` 产不出 MP3，转码必然是 ffmpeg（可能经 pydub `.export()`）。若用
`subprocess.run(...)` 或 pydub `.export()`（内部同步 wait 子进程），事件循环线程会
**干等整个编码时长**——几分钟通话卡住数秒，同进程所有连接一起挨刀。ffmpeg 编码本身在
独立进程、不占 GIL，问题只在**调用方式是同步的**。

```python
# ── 病灶（同步阻塞事件循环）──
import subprocess
def export_mp3(wav_path, mp3_path):
    subprocess.run(["ffmpeg", "-y", "-i", wav_path, mp3_path], check=True)
# 或 pydub：AudioSegment.from_wav(wav_path).export(mp3_path, format="mp3")

# ── 修复（异步子进程，不阻塞 loop）──
async def export_mp3(wav_path, mp3_path):
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-i", wav_path, mp3_path,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
    )
    _, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {err.decode()[:500]}")
```

- 若调用点在同步上下文里，用 `await loop.run_in_executor(None, blocking_export, ...)`
  把它丢到线程池，同样能让出事件循环。
- 给 ffmpeg 喂数据**用临时文件，别用 stdin 管道塞整段**（同步写大管道也会卡 loop）。
- 转码是纯 CPU 活，量大时更稳的做法是丢给**独立 worker 进程/任务队列**，收发进程只做 IO。

---

## 上线前后验证

1. **加 loop-lag 探针**（生产进程常驻），把告警时间戳和 1006 时间戳对表——终审证据：

   ```python
   async def loop_lag_probe(interval=1.0, warn_ms=200):
       while True:
           t0 = time.perf_counter()
           await asyncio.sleep(interval)
           lag = (time.perf_counter() - t0 - interval) * 1000
           if lag > warn_ms:
               logging.warning("event-loop lag %.0fms", lag)
   ```

2. **判据修正**：1006 同进程「成簇」既可能是服务端事件，也可能是客户端进程卡死。
   用 lag 区分：**成簇 + lag 飙升 = 客户端（本案）**；成簇 + lag 正常 = 才查服务端/网关。

3. **闭环回归**：修复后用本仓库 A/B 重跑，B 组从「1006 成簇 + 秒级 lag」变为「0 断连 +
   毫秒级 lag」即确认。

## 附：环境提醒

- `audioop` 在 Python 3.13 已从标准库**移除**（pydub 内部也依赖它）。基础镜像升级到 3.13+
  会直接 import 失败，重构时一并处理（3.13+ 可用 `audioop-lts` 续命）。

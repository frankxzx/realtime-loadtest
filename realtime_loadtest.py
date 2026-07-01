#!/usr/bin/env python3
"""
Azure OpenAI Realtime API 压测脚本
测试 gpt-realtime-1.5 (文本/音频生成) + gpt-realtime-whisper (语音转写) 的 TPM/RPM 上限

用法:
  export AZURE_OPENAI_ENDPOINT="https://xxx.openai.azure.com"
  export AZURE_OPENAI_API_KEY="your-key"
  export REALTIME_DEPLOYMENT="gpt-realtime-1.5"        # 文本/音频模型部署名
  export WHISPER_DEPLOYMENT="gpt-realtime-whisper"     # 可选，单独测 Whisper

  # 文本模式（测 TPM/RPM）
  python3 realtime_loadtest.py --mode text --concurrency 10 --duration 60

  # 音频输入模式（触发 Whisper 转写）
  python3 realtime_loadtest.py --mode audio --concurrency 5 --duration 60

  # 自动递增并发，找到 429 临界点
  python3 realtime_loadtest.py --mode text --ramp --ramp-start 1 --ramp-max 50 --ramp-step 5
"""

import asyncio
import websockets
import json
import time
import base64
import math
import struct
import os
import sys
import argparse
import statistics
import subprocess
import tempfile
from dataclasses import dataclass, field
from collections import deque

# ─── 配置 ──────────────────────────────────────────────────────────────────────
ENDPOINT   = os.environ.get("AZURE_OPENAI_ENDPOINT", "").rstrip("/")
API_KEY    = os.environ.get("AZURE_OPENAI_API_KEY", "")
DEPLOYMENT = os.environ.get("REALTIME_DEPLOYMENT", "gpt-realtime-1.5")
WHISPER_DEPLOYMENT = os.environ.get("WHISPER_DEPLOYMENT", "")

# 压测用的短文本 prompt（控制 token 数量）
TEXT_PROMPTS = [
    "Reply with exactly: OK",
    "Say: yes",
    "One word: hello",
    "Answer: done",
    "Respond: ack",
]

# ─── 统计结构 ───────────────────────────────────────────────────────────────────
@dataclass
class GlobalStats:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    total_requests:     int = 0
    success:            int = 0
    failed:             int = 0
    rate_limited_429:   int = 0
    connection_errors:  int = 0

    input_tokens:       int = 0
    output_tokens:      int = 0
    total_tokens:       int = 0

    # 滑动窗口（60s）
    req_timestamps:     deque = field(default_factory=lambda: deque())
    token_timestamps:   deque = field(default_factory=lambda: deque())

    latencies:          list = field(default_factory=list)
    errors:             list = field(default_factory=list)

    start_time:         float = field(default_factory=time.monotonic)

    async def record_success(self, input_tok: int, output_tok: int, latency: float):
        async with self.lock:
            now = time.monotonic()
            self.total_requests += 1
            self.success += 1
            self.input_tokens += input_tok
            self.output_tokens += output_tok
            self.total_tokens += input_tok + output_tok
            self.latencies.append(latency)
            self.req_timestamps.append(now)
            for _ in range(input_tok + output_tok):
                self.token_timestamps.append(now)

    async def record_failure(self, reason: str, is_rate_limit: bool = False):
        async with self.lock:
            now = time.monotonic()
            self.total_requests += 1
            self.failed += 1
            if is_rate_limit:
                self.rate_limited_429 += 1
            self.req_timestamps.append(now)
            self.errors.append(reason[:120])

    async def record_connection_error(self, reason: str):
        async with self.lock:
            self.connection_errors += 1
            self.errors.append(f"[CONN] {reason[:100]}")

    def current_rpm(self) -> float:
        now = time.monotonic()
        cutoff = now - 60
        while self.req_timestamps and self.req_timestamps[0] < cutoff:
            self.req_timestamps.popleft()
        return len(self.req_timestamps)

    def current_tpm(self) -> float:
        now = time.monotonic()
        cutoff = now - 60
        while self.token_timestamps and self.token_timestamps[0] < cutoff:
            self.token_timestamps.popleft()
        return len(self.token_timestamps)

    def summary(self) -> dict:
        elapsed = time.monotonic() - self.start_time
        lats = self.latencies
        return {
            "elapsed_s":       round(elapsed, 1),
            "total_requests":  self.total_requests,
            "success":         self.success,
            "failed":          self.failed,
            "rate_limited_429": self.rate_limited_429,
            "connection_errors": self.connection_errors,
            "input_tokens":    self.input_tokens,
            "output_tokens":   self.output_tokens,
            "total_tokens":    self.total_tokens,
            "avg_rpm":         round(self.success / elapsed * 60, 1) if elapsed > 0 else 0,
            "avg_tpm":         round(self.total_tokens / elapsed * 60, 1) if elapsed > 0 else 0,
            "current_rpm_1m":  self.current_rpm(),
            "current_tpm_1m":  self.current_tpm(),
            "latency_p50_ms":  round(statistics.median(lats) * 1000, 1) if lats else 0,
            "latency_p95_ms":  round(sorted(lats)[int(len(lats)*0.95)] * 1000, 1) if lats else 0,
            "latency_p99_ms":  round(sorted(lats)[int(len(lats)*0.99)] * 1000, 1) if lats else 0,
            "latency_max_ms":  round(max(lats) * 1000, 1) if lats else 0,
        }


# ─── 辅助：生成测试用 PCM16 音频（24kHz, 1ch）──────────────────────────────────
def _generate_audio_via_say(text: str = "Hello world") -> bytes:
    """用 macOS say + ffmpeg 生成真实 TTS 语音，返回 PCM16 24kHz mono 字节"""
    with tempfile.NamedTemporaryFile(suffix=".aiff", delete=False) as aiff_f:
        aiff_path = aiff_f.name
    pcm_path = aiff_path.replace(".aiff", ".pcm")
    try:
        subprocess.run(["say", text, "-o", aiff_path], check=True, capture_output=True)
        subprocess.run(
            ["ffmpeg", "-y", "-i", aiff_path,
             "-ar", "24000", "-ac", "1", "-f", "s16le", pcm_path],
            check=True, capture_output=True,
        )
        with open(pcm_path, "rb") as f:
            return f.read()
    finally:
        for p in (aiff_path, pcm_path):
            try:
                os.unlink(p)
            except FileNotFoundError:
                pass


def _generate_audio_fallback(duration_s: float = 1.5, sample_rate: int = 24000) -> bytes:
    """fallback：440Hz 正弦波 PCM16"""
    n = int(sample_rate * duration_s)
    buf = bytearray()
    for i in range(n):
        buf += struct.pack("<h", int(math.sin(2 * math.pi * 440 * i / sample_rate) * 0.6 * 32767))
    return bytes(buf)


def _load_test_audio() -> str:
    try:
        pcm = _generate_audio_via_say("Hello world")
        print(f"[音频] 使用 macOS say 生成 'Hello world' TTS ({len(pcm)//2/24000:.2f}s)")
    except Exception as e:
        print(f"[音频] say/ffmpeg 不可用({e})，回退到正弦波")
        pcm = _generate_audio_fallback()
    return base64.b64encode(pcm).decode()


TEST_AUDIO_B64 = _load_test_audio()


# ─── WebSocket 单次会话 ─────────────────────────────────────────────────────────
def build_ws_url(deployment: str) -> str:
    # GA endpoint: /openai/v1/realtime?model=<deployment>
    ep = ENDPOINT.replace("https://", "wss://").replace("http://", "ws://")
    return f"{ep}/openai/v1/realtime?model={deployment}"


async def run_text_session(
    stats: GlobalStats,
    deployment: str,
    prompt_idx: int,
    timeout: float = 30.0,
):
    """发送一次文本对话，记录 token 用量和延迟"""
    url = build_ws_url(deployment)
    headers = {"api-key": API_KEY}
    prompt = TEXT_PROMPTS[prompt_idx % len(TEXT_PROMPTS)]
    t_start = time.monotonic()

    try:
        async with websockets.connect(
            url,
            additional_headers=headers,
            open_timeout=10,
            close_timeout=5,
        ) as ws:
            # 等待 session.created
            await _wait_event(ws, "session.created", timeout=10)

            # 配置 session（纯文本，关闭音频输出节省 token）
            await ws.send(json.dumps({
                "type": "session.update",
                "session": {
                    "modalities": ["text"],
                    "instructions": "You are a minimal assistant. Reply as briefly as possible.",
                    "temperature": 0.1,
                    "max_response_output_tokens": 20,
                    "turn_detection": None,
                }
            }))
            await _wait_event(ws, "session.updated", timeout=10)

            # 发送用户消息
            await ws.send(json.dumps({
                "type": "conversation.item.create",
                "item": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": prompt}],
                }
            }))
            await ws.send(json.dumps({"type": "response.create"}))

            # 等待 response.done，提取 token 用量
            input_tok, output_tok = await _wait_response_done(ws, timeout=timeout)
            latency = time.monotonic() - t_start
            await stats.record_success(input_tok, output_tok, latency)

    except websockets.exceptions.InvalidStatus as e:
        is_429 = "429" in str(e)
        await stats.record_failure(f"HTTP {e}", is_rate_limit=is_429)
    except asyncio.TimeoutError:
        await stats.record_failure("Timeout")
    except Exception as e:
        err = str(e)
        if "429" in err:
            await stats.record_failure(err, is_rate_limit=True)
        else:
            await stats.record_connection_error(err)


async def run_audio_session(
    stats: GlobalStats,
    deployment: str,
    timeout: float = 45.0,
):
    """发送音频输入（触发 Whisper 转写），记录 token 和延迟"""
    url = build_ws_url(deployment)
    headers = {"api-key": API_KEY}
    t_start = time.monotonic()

    try:
        async with websockets.connect(
            url,
            additional_headers=headers,
            open_timeout=10,
            close_timeout=5,
        ) as ws:
            await _wait_event(ws, "session.created", timeout=10)

            # 配置：音频输入 + 文本输出（节省音频 token）
            await ws.send(json.dumps({
                "type": "session.update",
                "session": {
                    "modalities": ["text"],
                    "instructions": "Transcribe the audio and reply with one word.",
                    "input_audio_format": "pcm16",
                    "input_audio_transcription": {"model": "whisper-1"},
                    "temperature": 0.1,
                    "max_response_output_tokens": 10,
                    "turn_detection": None,
                }
            }))
            await _wait_event(ws, "session.updated", timeout=10)

            # 发送音频数据
            await ws.send(json.dumps({
                "type": "input_audio_buffer.append",
                "audio": TEST_AUDIO_B64,
            }))
            await ws.send(json.dumps({"type": "input_audio_buffer.commit"}))
            await ws.send(json.dumps({"type": "response.create"}))

            input_tok, output_tok = await _wait_response_done(ws, timeout=timeout)
            latency = time.monotonic() - t_start
            await stats.record_success(input_tok, output_tok, latency)

    except websockets.exceptions.InvalidStatus as e:
        is_429 = "429" in str(e)
        await stats.record_failure(f"HTTP {e}", is_rate_limit=is_429)
    except asyncio.TimeoutError:
        await stats.record_failure("Timeout")
    except Exception as e:
        err = str(e)
        if "429" in err:
            await stats.record_failure(err, is_rate_limit=True)
        else:
            await stats.record_connection_error(err)


async def _wait_event(ws, event_type: str, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise asyncio.TimeoutError(f"Waiting for {event_type}")
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        evt = json.loads(raw)
        if evt.get("type") == event_type:
            return evt
        if evt.get("type") == "error":
            code = evt.get("error", {}).get("code", "")
            msg  = evt.get("error", {}).get("message", str(evt))
            raise websockets.exceptions.InvalidStatus(
                websockets.http11.Response(
                    429 if "rate" in msg.lower() or "429" in str(code) else 500,
                    "Error", [], b""
                )
            ) if False else Exception(f"[{code}] {msg}")


async def _wait_response_done(ws, timeout: float) -> tuple[int, int]:
    """等待 response.done，返回 (input_tokens, output_tokens)"""
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise asyncio.TimeoutError("Waiting for response.done")
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        evt = json.loads(raw)
        t = evt.get("type", "")

        if t == "response.done":
            usage = evt.get("response", {}).get("usage", {})
            input_tok  = usage.get("input_tokens", 0)
            output_tok = usage.get("output_tokens", 0)
            return input_tok, output_tok

        if t == "error":
            code = evt.get("error", {}).get("code", "")
            msg  = evt.get("error", {}).get("message", str(evt))
            raise Exception(f"[{code}] {msg}")


# ─── 工作池 ────────────────────────────────────────────────────────────────────
async def worker_loop(
    stats: GlobalStats,
    mode: str,
    deployment: str,
    stop_event: asyncio.Event,
    worker_id: int,
    request_interval: float = 0.0,
):
    """单个 worker：循环发请求直到 stop_event 置位"""
    idx = worker_id
    while not stop_event.is_set():
        if mode == "text":
            await run_text_session(stats, deployment, idx)
        else:
            await run_audio_session(stats, deployment)
        idx += 1
        if request_interval > 0:
            await asyncio.sleep(request_interval)


async def monitor_loop(stats: GlobalStats, stop_event: asyncio.Event, interval: float = 5.0):
    """定期打印实时统计"""
    print(f"\n{'─'*72}")
    print(f"{'时间':>6}  {'RPM(1m)':>8}  {'TPM(1m)':>9}  {'成功':>6}  {'429':>5}  "
          f"{'失败':>5}  {'P50ms':>7}  {'P95ms':>7}  {'总Token':>9}")
    print(f"{'─'*72}")
    t0 = time.monotonic()
    while not stop_event.is_set():
        await asyncio.sleep(interval)
        s = stats.summary()
        elapsed = int(time.monotonic() - t0)
        print(
            f"{elapsed:>5}s"
            f"  {s['current_rpm_1m']:>8.0f}"
            f"  {s['current_tpm_1m']:>9.0f}"
            f"  {s['success']:>6}"
            f"  {s['rate_limited_429']:>5}"
            f"  {s['failed']:>5}"
            f"  {s['latency_p50_ms']:>7.0f}"
            f"  {s['latency_p95_ms']:>7.0f}"
            f"  {s['total_tokens']:>9}"
        )


# ─── 主压测逻辑 ─────────────────────────────────────────────────────────────────
async def run_load_test(
    mode: str,
    deployment: str,
    concurrency: int,
    duration: float,
    request_interval: float = 0.0,
):
    print(f"\n[压测配置]")
    print(f"  模式:       {mode}")
    print(f"  部署:       {deployment}")
    print(f"  并发数:     {concurrency}")
    print(f"  持续时间:   {duration}s")
    print(f"  WebSocket:  {build_ws_url(deployment)}")

    stats = GlobalStats()
    stop_event = asyncio.Event()

    workers = [
        asyncio.create_task(
            worker_loop(stats, mode, deployment, stop_event, i, request_interval)
        )
        for i in range(concurrency)
    ]
    monitor = asyncio.create_task(monitor_loop(stats, stop_event))

    await asyncio.sleep(duration)
    stop_event.set()

    for w in workers:
        w.cancel()
    monitor.cancel()
    await asyncio.gather(*workers, monitor, return_exceptions=True)

    return stats


async def run_ramp_test(
    mode: str,
    deployment: str,
    ramp_start: int,
    ramp_max: int,
    ramp_step: int,
    step_duration: float = 30.0,
):
    """递增并发数，找 429 临界点"""
    print(f"\n[Ramp 测试] {ramp_start} -> {ramp_max} 并发，每步 {step_duration}s")
    results = []
    concurrency = ramp_start

    while concurrency <= ramp_max:
        print(f"\n{'='*50}")
        print(f">>> 并发: {concurrency}")
        stats = await run_load_test(mode, deployment, concurrency, step_duration)
        s = stats.summary()
        s["concurrency"] = concurrency
        results.append(s)

        print(f"\n  RPM={s['avg_rpm']}  TPM={s['avg_tpm']}  "
              f"429s={s['rate_limited_429']}  P95={s['latency_p95_ms']}ms")

        if s["rate_limited_429"] > 0:
            print(f"\n!!! 在并发={concurrency} 时触发 429 限流")
            print(f"!!! 上一个稳定并发: {concurrency - ramp_step}")
            break

        concurrency += ramp_step

    print(f"\n{'='*60}")
    print("Ramp 测试汇总:")
    print(f"{'并发':>6}  {'RPM':>8}  {'TPM':>9}  {'429':>6}  {'P95ms':>8}")
    for r in results:
        mark = " <-- 限流" if r["rate_limited_429"] > 0 else ""
        print(f"{r['concurrency']:>6}  {r['avg_rpm']:>8.1f}  {r['avg_tpm']:>9.1f}  "
              f"{r['rate_limited_429']:>6}  {r['latency_p95_ms']:>8.1f}{mark}")
    return results


# ─── CLI ───────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="Azure OpenAI Realtime API WebSocket 压测工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--mode", choices=["text", "audio"], default="text",
                   help="text=纯文本对话; audio=音频输入(触发Whisper)")
    p.add_argument("--deployment", default=DEPLOYMENT,
                   help="Azure 部署名 (默认读 REALTIME_DEPLOYMENT 环境变量)")
    p.add_argument("--concurrency", type=int, default=5,
                   help="并发 WebSocket 连接数")
    p.add_argument("--duration", type=float, default=60.0,
                   help="压测持续秒数")
    p.add_argument("--interval", type=float, default=0.0,
                   help="每个 worker 两次请求之间的等待秒数（0=无等待）")

    # Ramp 模式
    p.add_argument("--ramp", action="store_true",
                   help="自动递增并发，找 429 限流临界点")
    p.add_argument("--ramp-start", type=int, default=1)
    p.add_argument("--ramp-max",   type=int, default=50)
    p.add_argument("--ramp-step",  type=int, default=5)
    p.add_argument("--ramp-step-duration", type=float, default=30.0,
                   help="每个并发等级的测试时长（秒）")
    return p.parse_args()


def main():
    args = parse_args()

    if not ENDPOINT or not API_KEY:
        print("错误: 请设置环境变量 AZURE_OPENAI_ENDPOINT 和 AZURE_OPENAI_API_KEY")
        sys.exit(1)

    deployment = args.deployment
    if not deployment:
        print("错误: 请设置 --deployment 或 REALTIME_DEPLOYMENT 环境变量")
        sys.exit(1)

    # Ctrl+C 优雅退出
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        if args.ramp:
            loop.run_until_complete(
                run_ramp_test(
                    args.mode, deployment,
                    args.ramp_start, args.ramp_max, args.ramp_step,
                    args.ramp_step_duration,
                )
            )
        else:
            stats = loop.run_until_complete(
                run_load_test(
                    args.mode, deployment,
                    args.concurrency, args.duration, args.interval,
                )
            )
            s = stats.summary()
            print(f"\n{'='*60}")
            print("最终统计:")
            for k, v in s.items():
                print(f"  {k:<22}: {v}")

            if stats.errors:
                print(f"\n最近错误样本 (最多10条):")
                for e in stats.errors[-10:]:
                    print(f"  {e}")

    except KeyboardInterrupt:
        print("\n用户中断")
    finally:
        loop.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Azure OpenAI Realtime API 压测脚本 (GA)
测试 gpt-realtime-1.5 + gpt-realtime-whisper 的 TPM/RPM 上限

用法:
  cp .env.example .env && vi .env

  # 文本模式
  python3 realtime_loadtest.py --mode text --concurrency 10 --duration 60

  # 音频模式（触发 Whisper）
  python3 realtime_loadtest.py --mode audio --concurrency 5 --duration 60

  # Ramp 模式（自动找 429 临界点）
  python3 realtime_loadtest.py --mode text --ramp --ramp-start 1 --ramp-max 50 --ramp-step 5

  # 输出 HTML 日志
  python3 realtime_loadtest.py --mode text --concurrency 5 --duration 30 --html
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
import ssl
import traceback
from dataclasses import dataclass, field
from collections import deque
from datetime import datetime

# ─── SSL（关闭验证）─────────────────────────────────────────────────────────────
_SSL_CTX = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


# ─── .env 加载 ──────────────────────────────────────────────────────────────────
def _load_dotenv() -> None:
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
        os.path.join(os.getcwd(), ".env"),
    ]
    for path in candidates:
        try:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
            print(f"[env] loaded {path}")
            return
        except FileNotFoundError:
            continue


_load_dotenv()

# ─── 配置 ──────────────────────────────────────────────────────────────────────
ENDPOINT           = os.environ.get("AZURE_OPENAI_ENDPOINT", "").rstrip("/")
API_KEY            = os.environ.get("AZURE_OPENAI_API_KEY", "")
DEPLOYMENT         = os.environ.get("REALTIME_DEPLOYMENT", "gpt-realtime-1.5")
WHISPER_DEPLOYMENT = os.environ.get("WHISPER_DEPLOYMENT", "")

TEXT_PROMPTS = [
    "Reply with exactly: OK",
    "Say: yes",
    "One word: hello",
    "Answer: done",
    "Respond: ack",
]


# ─── 结构化日志 ─────────────────────────────────────────────────────────────────
# ANSI 颜色
_C = {
    "reset":  "\033[0m",
    "gray":   "\033[90m",
    "cyan":   "\033[96m",
    "green":  "\033[92m",
    "yellow": "\033[93m",
    "red":    "\033[91m",
    "bold":   "\033[1m",
    "dim":    "\033[2m",
}

_T0 = time.monotonic()


class EventLog:
    """收集所有事件日志，支持控制台输出和 HTML 导出"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose      # True = 打印每条 WS 消息
        self.entries: list[dict] = []

    def _entry(self, level: str, worker: str, direction: str,
               event: str, detail: str = "", error: str = "") -> dict:
        now = time.monotonic()
        e = {
            "elapsed":   round(now - _T0, 3),
            "ts":        datetime.now().strftime("%H:%M:%S.%f")[:-3],
            "level":     level,      # INFO WARN ERROR DEBUG
            "worker":    worker,
            "direction": direction,  # → ← ✓ ✗ ⚡ ·
            "event":     event,
            "detail":    detail,
            "error":     error,
        }
        self.entries.append(e)
        return e

    def _print(self, e: dict) -> None:
        level_color = {
            "INFO":  _C["green"],
            "WARN":  _C["yellow"],
            "ERROR": _C["red"],
            "DEBUG": _C["dim"],
        }.get(e["level"], "")

        dir_color = {
            "→": _C["cyan"],
            "←": _C["green"],
            "✓": _C["green"] + _C["bold"],
            "✗": _C["red"],
            "⚡": _C["yellow"],
            "·": _C["gray"],
        }.get(e["direction"], "")

        ts    = f"{_C['gray']}{e['ts']}{_C['reset']}"
        wid   = f"{_C['dim']}[{e['worker']:>3}]{_C['reset']}"
        direc = f"{dir_color}{e['direction']}{_C['reset']}"
        evt   = f"{level_color}{e['event']:<28}{_C['reset']}"
        det   = f"{_C['dim']}{e['detail']}{_C['reset']}" if e["detail"] else ""
        err   = f" {_C['red']}{e['error']}{_C['reset']}"  if e["error"]  else ""

        print(f"{ts} {wid} {direc} {evt}{det}{err}")

    # ── 对外接口 ────────────────────────────────────────────────────────────────
    def send(self, worker: str, event: str, payload: dict | None = None) -> None:
        if not self.verbose:
            return
        detail = _fmt_payload(payload) if payload else ""
        e = self._entry("DEBUG", worker, "→", event, detail)
        self._print(e)

    def recv(self, worker: str, event: str, payload: dict | None = None) -> None:
        if not self.verbose:
            return
        detail = _fmt_payload(payload) if payload else ""
        e = self._entry("DEBUG", worker, "←", event, detail)
        self._print(e)

    def info(self, worker: str, msg: str, detail: str = "") -> None:
        e = self._entry("INFO", worker, "·", msg, detail)
        self._print(e)

    def success(self, worker: str, event: str, detail: str = "") -> None:
        e = self._entry("INFO", worker, "✓", event, detail)
        self._print(e)

    def warn(self, worker: str, event: str, detail: str = "") -> None:
        e = self._entry("WARN", worker, "⚡", event, detail)
        self._print(e)

    def error(self, worker: str, event: str, detail: str = "", exc: BaseException | None = None) -> None:
        err_str = ""
        if exc:
            tb = traceback.extract_tb(exc.__traceback__)
            if tb:
                last = tb[-1]
                err_str = f"{type(exc).__name__} @ {last.filename.split('/')[-1]}:{last.lineno}"
            else:
                err_str = f"{type(exc).__name__}: {exc}"
        e = self._entry("ERROR", worker, "✗", event, detail, err_str)
        self._print(e)

    def rate_limit(self, worker: str, detail: str = "") -> None:
        e = self._entry("WARN", worker, "⚡", "429 Rate Limited", detail)
        self._print(e)

    # ── HTML 报告 ───────────────────────────────────────────────────────────────
    def write_html(self, path: str) -> None:
        rows_js = json.dumps(self.entries, ensure_ascii=False)
        html = _HTML_TEMPLATE.replace("__ROWS__", rows_js)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"\n[log] HTML 报告已写入: {path}")


def _fmt_payload(p: dict) -> str:
    """从 payload 提取关键字段显示"""
    skip = {"audio", "instructions"}
    parts = []
    for k, v in p.items():
        if k in skip:
            continue
        if isinstance(v, (dict, list)):
            parts.append(f"{k}={json.dumps(v, ensure_ascii=False)}")
        else:
            sv = str(v)
            parts.append(f"{k}={sv[:60]}")
    return "  " + "  ".join(parts) if parts else ""


# ─── 全局日志实例（在 main 里替换 verbose） ─────────────────────────────────────
LOG = EventLog(verbose=False)


# ─── 统计结构 ───────────────────────────────────────────────────────────────────
@dataclass
class GlobalStats:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    total_requests:    int = 0
    success:           int = 0
    failed:            int = 0
    rate_limited_429:  int = 0
    connection_errors: int = 0

    input_tokens:  int = 0
    output_tokens: int = 0
    total_tokens:  int = 0

    req_timestamps:   deque = field(default_factory=deque)
    token_timestamps: deque = field(default_factory=deque)

    latencies: list = field(default_factory=list)
    errors:    list = field(default_factory=list)

    start_time: float = field(default_factory=time.monotonic)

    async def record_success(self, input_tok: int, output_tok: int, latency: float):
        async with self.lock:
            now = time.monotonic()
            self.total_requests += 1
            self.success += 1
            self.input_tokens  += input_tok
            self.output_tokens += output_tok
            self.total_tokens  += input_tok + output_tok
            self.latencies.append(latency)
            self.req_timestamps.append(now)
            for _ in range(input_tok + output_tok):
                self.token_timestamps.append(now)

    async def record_failure(self, reason: str, is_rate_limit: bool = False):
        async with self.lock:
            self.total_requests += 1
            self.failed += 1
            if is_rate_limit:
                self.rate_limited_429 += 1
            self.req_timestamps.append(time.monotonic())
            self.errors.append(reason[:120])

    async def record_connection_error(self, reason: str):
        async with self.lock:
            self.connection_errors += 1
            self.errors.append(f"[CONN] {reason[:100]}")

    def current_rpm(self) -> float:
        cutoff = time.monotonic() - 60
        while self.req_timestamps and self.req_timestamps[0] < cutoff:
            self.req_timestamps.popleft()
        return len(self.req_timestamps)

    def current_tpm(self) -> float:
        cutoff = time.monotonic() - 60
        while self.token_timestamps and self.token_timestamps[0] < cutoff:
            self.token_timestamps.popleft()
        return len(self.token_timestamps)

    def summary(self) -> dict:
        elapsed = time.monotonic() - self.start_time
        lats = self.latencies
        return {
            "elapsed_s":         round(elapsed, 1),
            "total_requests":    self.total_requests,
            "success":           self.success,
            "failed":            self.failed,
            "rate_limited_429":  self.rate_limited_429,
            "connection_errors": self.connection_errors,
            "input_tokens":      self.input_tokens,
            "output_tokens":     self.output_tokens,
            "total_tokens":      self.total_tokens,
            "avg_rpm":           round(self.success / elapsed * 60, 1) if elapsed > 0 else 0,
            "avg_tpm":           round(self.total_tokens / elapsed * 60, 1) if elapsed > 0 else 0,
            "current_rpm_1m":    self.current_rpm(),
            "current_tpm_1m":    self.current_tpm(),
            "latency_p50_ms":    round(statistics.median(lats) * 1000, 1) if lats else 0,
            "latency_p95_ms":    round(sorted(lats)[int(len(lats) * 0.95)] * 1000, 1) if lats else 0,
            "latency_p99_ms":    round(sorted(lats)[int(len(lats) * 0.99)] * 1000, 1) if lats else 0,
            "latency_max_ms":    round(max(lats) * 1000, 1) if lats else 0,
        }


# ─── 音频生成 ───────────────────────────────────────────────────────────────────
def _generate_audio_via_say(text: str = "Hello world") -> bytes:
    with tempfile.NamedTemporaryFile(suffix=".aiff", delete=False) as f:
        aiff_path = f.name
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


def _generate_audio_fallback(duration_s: float = 1.5, sr: int = 24000) -> bytes:
    n = int(sr * duration_s)
    buf = bytearray()
    for i in range(n):
        buf += struct.pack("<h", int(math.sin(2 * math.pi * 440 * i / sr) * 0.6 * 32767))
    return bytes(buf)


def _load_test_audio() -> str:
    try:
        pcm = _generate_audio_via_say("Hello world")
        print(f"[音频] macOS say 生成 'Hello world' TTS ({len(pcm) // 2 / 24000:.2f}s)")
    except Exception as e:
        print(f"[音频] say/ffmpeg 不可用({e})，回退到正弦波")
        pcm = _generate_audio_fallback()
    return base64.b64encode(pcm).decode()


TEST_AUDIO_B64 = _load_test_audio()


# ─── WebSocket URL ──────────────────────────────────────────────────────────────
def build_ws_url(deployment: str) -> str:
    # GA endpoint: /openai/v1/realtime?model=<deployment>
    ep = ENDPOINT.replace("https://", "wss://").replace("http://", "ws://")
    return f"{ep}/openai/v1/realtime?model={deployment}"


# ─── WebSocket helpers ──────────────────────────────────────────────────────────
async def _ws_send(ws, worker: str, payload: dict) -> None:
    LOG.send(worker, payload.get("type", "?"), payload)
    await ws.send(json.dumps(payload))


async def _wait_event(ws, worker: str, event_type: str, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise asyncio.TimeoutError(f"等待 {event_type} 超时")
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        evt = json.loads(raw)
        t = evt.get("type", "")
        LOG.recv(worker, t, {k: v for k, v in evt.items() if k != "type"})
        if t == event_type:
            return evt
        if t == "error":
            code = evt.get("error", {}).get("code", "")
            msg  = evt.get("error", {}).get("message", str(evt))
            raise Exception(f"[{code}] {msg}")


async def _wait_response_done(ws, worker: str, timeout: float) -> tuple[int, int]:
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise asyncio.TimeoutError("等待 response.done 超时")
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        evt = json.loads(raw)
        t = evt.get("type", "")
        LOG.recv(worker, t, {k: v for k, v in evt.items() if k not in ("type", "delta")})
        if t == "response.done":
            usage      = evt.get("response", {}).get("usage", {})
            input_tok  = usage.get("input_tokens", 0)
            output_tok = usage.get("output_tokens", 0)
            return input_tok, output_tok
        if t == "error":
            code = evt.get("error", {}).get("code", "")
            msg  = evt.get("error", {}).get("message", str(evt))
            raise Exception(f"[{code}] {msg}")


# ─── 单次会话：文本 ─────────────────────────────────────────────────────────────
async def run_text_session(
    stats: GlobalStats,
    deployment: str,
    worker_id: int,
    prompt_idx: int,
    timeout: float = 30.0,
) -> None:
    wid    = f"W{worker_id:02d}"
    url    = build_ws_url(deployment)
    prompt = TEXT_PROMPTS[prompt_idx % len(TEXT_PROMPTS)]
    t_start = time.monotonic()

    try:
        async with websockets.connect(
            url,
            additional_headers={"api-key": API_KEY},
            open_timeout=10,
            close_timeout=5,
            ssl=_SSL_CTX,
        ) as ws:
            LOG.info(wid, "connected", url)
            await _wait_event(ws, wid, "session.created", timeout=10)

            # GA session.update: type=realtime, output_modalities（非 modalities）
            await _ws_send(ws, wid, {
                "type": "session.update",
                "session": {
                    "type": "realtime",
                    "output_modalities": ["text"],
                    "instructions": "You are a minimal assistant. Reply as briefly as possible.",
                    "temperature": 0.1,
                    "max_response_output_tokens": 20,
                },
            })
            await _wait_event(ws, wid, "session.updated", timeout=10)

            await _ws_send(ws, wid, {
                "type": "conversation.item.create",
                "item": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": prompt}],
                },
            })
            await _ws_send(ws, wid, {"type": "response.create"})

            input_tok, output_tok = await _wait_response_done(ws, wid, timeout=timeout)
            latency = time.monotonic() - t_start
            LOG.success(wid, "response.done",
                        f"in={input_tok} out={output_tok} lat={latency:.2f}s")
            await stats.record_success(input_tok, output_tok, latency)

    except websockets.exceptions.InvalidStatus as e:
        is_429 = "429" in str(e)
        if is_429:
            LOG.rate_limit(wid, str(e))
        else:
            LOG.error(wid, "InvalidStatus", str(e), e)
        await stats.record_failure(str(e), is_rate_limit=is_429)
    except asyncio.TimeoutError as e:
        LOG.error(wid, "Timeout", str(e), e)
        await stats.record_failure("Timeout")
    except Exception as e:
        err = str(e)
        if "429" in err or "rate" in err.lower():
            LOG.rate_limit(wid, err)
            await stats.record_failure(err, is_rate_limit=True)
        else:
            LOG.error(wid, "Exception", err, e)
            await stats.record_connection_error(err)


# ─── 单次会话：音频（Whisper）──────────────────────────────────────────────────
async def run_audio_session(
    stats: GlobalStats,
    deployment: str,
    worker_id: int,
    timeout: float = 45.0,
) -> None:
    wid     = f"W{worker_id:02d}"
    url     = build_ws_url(deployment)
    t_start = time.monotonic()

    try:
        async with websockets.connect(
            url,
            additional_headers={"api-key": API_KEY},
            open_timeout=10,
            close_timeout=5,
            ssl=_SSL_CTX,
        ) as ws:
            LOG.info(wid, "connected", url)
            await _wait_event(ws, wid, "session.created", timeout=10)

            # GA session.update: audio 格式移到 audio.input.format
            await _ws_send(ws, wid, {
                "type": "session.update",
                "session": {
                    "type": "realtime",
                    "output_modalities": ["text"],
                    "instructions": "Transcribe the audio and reply with one word.",
                    "audio": {
                        "input": {
                            "format": {"type": "audio/pcm", "rate": 24000},
                        },
                    },
                    "temperature": 0.1,
                    "max_response_output_tokens": 10,
                },
            })
            await _wait_event(ws, wid, "session.updated", timeout=10)

            await _ws_send(ws, wid, {
                "type": "input_audio_buffer.append",
                "audio": TEST_AUDIO_B64,
            })
            await _ws_send(ws, wid, {"type": "input_audio_buffer.commit"})
            await _ws_send(ws, wid, {"type": "response.create"})

            input_tok, output_tok = await _wait_response_done(ws, wid, timeout=timeout)
            latency = time.monotonic() - t_start
            LOG.success(wid, "response.done",
                        f"in={input_tok} out={output_tok} lat={latency:.2f}s")
            await stats.record_success(input_tok, output_tok, latency)

    except websockets.exceptions.InvalidStatus as e:
        is_429 = "429" in str(e)
        if is_429:
            LOG.rate_limit(wid, str(e))
        else:
            LOG.error(wid, "InvalidStatus", str(e), e)
        await stats.record_failure(str(e), is_rate_limit=is_429)
    except asyncio.TimeoutError as e:
        LOG.error(wid, "Timeout", str(e), e)
        await stats.record_failure("Timeout")
    except Exception as e:
        err = str(e)
        if "429" in err or "rate" in err.lower():
            LOG.rate_limit(wid, err)
            await stats.record_failure(err, is_rate_limit=True)
        else:
            LOG.error(wid, "Exception", err, e)
            await stats.record_connection_error(err)


# ─── Worker 池 ──────────────────────────────────────────────────────────────────
async def worker_loop(
    stats: GlobalStats,
    mode: str,
    deployment: str,
    stop_event: asyncio.Event,
    worker_id: int,
    request_interval: float = 0.0,
) -> None:
    idx = worker_id
    while not stop_event.is_set():
        if mode == "text":
            await run_text_session(stats, deployment, worker_id, idx)
        else:
            await run_audio_session(stats, deployment, worker_id)
        idx += 1
        if request_interval > 0:
            await asyncio.sleep(request_interval)


# ─── 实时统计表格 ───────────────────────────────────────────────────────────────
async def monitor_loop(stats: GlobalStats, stop_event: asyncio.Event, interval: float = 5.0) -> None:
    hdr = (f"\n{'─'*76}\n"
           f"{'时间':>6}  {'RPM(1m)':>8}  {'TPM(1m)':>9}  {'成功':>6}  "
           f"{'429':>5}  {'失败':>5}  {'P50ms':>7}  {'P95ms':>7}  {'总Token':>9}\n"
           f"{'─'*76}")
    print(hdr)
    t0 = time.monotonic()
    while not stop_event.is_set():
        await asyncio.sleep(interval)
        s = stats.summary()
        elapsed = int(time.monotonic() - t0)
        c429 = _C["yellow"] if s["rate_limited_429"] > 0 else ""
        rst  = _C["reset"]  if s["rate_limited_429"] > 0 else ""
        print(
            f"{elapsed:>5}s"
            f"  {s['current_rpm_1m']:>8.0f}"
            f"  {s['current_tpm_1m']:>9.0f}"
            f"  {s['success']:>6}"
            f"  {c429}{s['rate_limited_429']:>5}{rst}"
            f"  {s['failed']:>5}"
            f"  {s['latency_p50_ms']:>7.0f}"
            f"  {s['latency_p95_ms']:>7.0f}"
            f"  {s['total_tokens']:>9}"
        )


# ─── 主压测 ─────────────────────────────────────────────────────────────────────
async def run_load_test(
    mode: str,
    deployment: str,
    concurrency: int,
    duration: float,
    request_interval: float = 0.0,
) -> GlobalStats:
    print(f"\n{_C['bold']}[压测配置]{_C['reset']}")
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
) -> list[dict]:
    print(f"\n[Ramp 测试] {ramp_start} → {ramp_max} 并发，每步 {step_duration}s")
    results = []
    concurrency = ramp_start

    while concurrency <= ramp_max:
        print(f"\n{'='*50}\n>>> 并发: {concurrency}")
        stats = await run_load_test(mode, deployment, concurrency, step_duration)
        s = stats.summary()
        s["concurrency"] = concurrency
        results.append(s)
        print(f"\n  RPM={s['avg_rpm']}  TPM={s['avg_tpm']}  "
              f"429s={s['rate_limited_429']}  P95={s['latency_p95_ms']}ms")

        if s["rate_limited_429"] > 0:
            print(f"\n{_C['yellow']}!!! 并发={concurrency} 时触发 429，"
                  f"上一个稳定并发: {concurrency - ramp_step}{_C['reset']}")
            break
        concurrency += ramp_step

    print(f"\n{'='*60}\nRamp 汇总:")
    print(f"{'并发':>6}  {'RPM':>8}  {'TPM':>9}  {'429':>6}  {'P95ms':>8}")
    for r in results:
        mark = f" {_C['yellow']}<-- 限流{_C['reset']}" if r["rate_limited_429"] > 0 else ""
        print(f"{r['concurrency']:>6}  {r['avg_rpm']:>8.1f}  {r['avg_tpm']:>9.1f}  "
              f"{r['rate_limited_429']:>6}  {r['latency_p95_ms']:>8.1f}{mark}")
    return results


# ─── CLI ────────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Azure OpenAI Realtime API WebSocket 压测工具 (GA)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--mode", choices=["text", "audio"], default="text")
    p.add_argument("--deployment", default=DEPLOYMENT,
                   help="Azure 部署名 (默认 $REALTIME_DEPLOYMENT)")
    p.add_argument("--concurrency", type=int, default=5)
    p.add_argument("--duration",    type=float, default=60.0)
    p.add_argument("--interval",    type=float, default=0.0,
                   help="worker 两次请求间隔秒数")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="打印每条 WebSocket 消息（发送/接收）")
    p.add_argument("--html", action="store_true",
                   help="测试结束后生成 HTML 日志报告")

    p.add_argument("--ramp",               action="store_true")
    p.add_argument("--ramp-start",         type=int,   default=1)
    p.add_argument("--ramp-max",           type=int,   default=50)
    p.add_argument("--ramp-step",          type=int,   default=5)
    p.add_argument("--ramp-step-duration", type=float, default=30.0)
    return p.parse_args()


def main() -> None:
    global LOG
    args = parse_args()

    if not ENDPOINT or not API_KEY:
        print("错误: 请设置 AZURE_OPENAI_ENDPOINT 和 AZURE_OPENAI_API_KEY")
        sys.exit(1)
    if not args.deployment:
        print("错误: 请设置 --deployment 或 REALTIME_DEPLOYMENT")
        sys.exit(1)

    LOG = EventLog(verbose=args.verbose)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        if args.ramp:
            loop.run_until_complete(
                run_ramp_test(
                    args.mode, args.deployment,
                    args.ramp_start, args.ramp_max,
                    args.ramp_step, args.ramp_step_duration,
                )
            )
        else:
            stats = loop.run_until_complete(
                run_load_test(
                    args.mode, args.deployment,
                    args.concurrency, args.duration, args.interval,
                )
            )
            s = stats.summary()
            print(f"\n{'='*60}\n{_C['bold']}最终统计:{_C['reset']}")
            for k, v in s.items():
                print(f"  {k:<22}: {v}")

            if stats.errors:
                print(f"\n{_C['red']}错误样本 (最近10条):{_C['reset']}")
                for e in stats.errors[-10:]:
                    print(f"  {e}")

    except KeyboardInterrupt:
        print("\n用户中断")
    finally:
        if args.html:
            ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                f"realtime_log_{ts}.html")
            LOG.write_html(path)
        loop.close()


# ─── HTML 报告模板 ──────────────────────────────────────────────────────────────
_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<title>Realtime Loadtest Log</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: "SF Mono", "Fira Code", monospace; font-size: 12px;
         background: #0d1117; color: #c9d1d9; }
  #toolbar { position: sticky; top: 0; background: #161b22; padding: 8px 12px;
             display: flex; gap: 8px; flex-wrap: wrap; z-index: 10;
             border-bottom: 1px solid #30363d; }
  #toolbar input, #toolbar select {
    background: #0d1117; color: #c9d1d9; border: 1px solid #30363d;
    border-radius: 4px; padding: 4px 8px; font-size: 12px; }
  #toolbar input { width: 200px; }
  #count { color: #8b949e; align-self: center; margin-left: auto; }
  table { width: 100%; border-collapse: collapse; }
  th { position: sticky; top: 41px; background: #161b22; color: #8b949e;
       font-weight: normal; text-align: left; padding: 4px 8px;
       border-bottom: 1px solid #21262d; white-space: nowrap; }
  tr:hover { background: #161b22; }
  td { padding: 3px 8px; border-bottom: 1px solid #21262d; vertical-align: top; }
  .ts    { color: #8b949e; white-space: nowrap; }
  .wid   { color: #79c0ff; white-space: nowrap; }
  .dir   { text-align: center; }
  .dir.s { color: #56d364; }  /* send */
  .dir.r { color: #58a6ff; }  /* recv */
  .dir.ok{ color: #56d364; font-weight: bold; }
  .dir.er{ color: #f85149; }
  .dir.rl{ color: #d29922; }
  .dir.i { color: #8b949e; }
  .evt   { white-space: nowrap; }
  .lvl-INFO  { color: #c9d1d9; }
  .lvl-WARN  { color: #d29922; }
  .lvl-ERROR { color: #f85149; }
  .lvl-DEBUG { color: #6e7681; }
  .detail{ color: #8b949e; word-break: break-all; }
  .errstr{ color: #f85149; }
  td.dir { width: 20px; }
  td.ts  { width: 110px; }
  td.wid { width: 50px; }
  td.evt { width: 220px; }
</style>
</head>
<body>
<div id="toolbar">
  <input id="search" placeholder="搜索事件/详情..." oninput="render()">
  <select id="fLevel" onchange="render()">
    <option value="">全部级别</option>
    <option>INFO</option><option>WARN</option><option>ERROR</option><option>DEBUG</option>
  </select>
  <select id="fWorker" onchange="render()"></select>
  <select id="fDir" onchange="render()">
    <option value="">全部方向</option>
    <option value="→">→ 发送</option>
    <option value="←">← 接收</option>
    <option value="✓">✓ 成功</option>
    <option value="✗">✗ 错误</option>
    <option value="⚡">⚡ 限流</option>
  </select>
  <span id="count"></span>
</div>
<table>
<thead><tr>
  <th>时间</th><th>+秒</th><th>Worker</th><th>方向</th>
  <th>事件</th><th>详情</th><th>错误</th>
</tr></thead>
<tbody id="tbody"></tbody>
</table>
<script>
const ROWS = __ROWS__;

// 初始化 worker 筛选
const workers = [...new Set(ROWS.map(r => r.worker))].sort();
const wSel = document.getElementById('fWorker');
wSel.innerHTML = '<option value="">全部 Worker</option>' +
  workers.map(w => `<option>${w}</option>`).join('');

const DIR_CLASS = {'→':'s','←':'r','✓':'ok','✗':'er','⚡':'rl','·':'i'};

function render() {
  const q   = document.getElementById('search').value.toLowerCase();
  const lvl = document.getElementById('fLevel').value;
  const wid = document.getElementById('fWorker').value;
  const dir = document.getElementById('fDir').value;
  let rows = ROWS.filter(r =>
    (!lvl || r.level === lvl) &&
    (!wid || r.worker === wid) &&
    (!dir || r.direction === dir) &&
    (!q   || (r.event+r.detail+r.error+r.worker).toLowerCase().includes(q))
  );
  document.getElementById('count').textContent = `${rows.length} / ${ROWS.length} 条`;
  document.getElementById('tbody').innerHTML = rows.map(r => `
    <tr class="lvl-${r.level}">
      <td class="ts">${r.ts}</td>
      <td class="ts">+${r.elapsed}s</td>
      <td class="wid">${r.worker}</td>
      <td class="dir ${DIR_CLASS[r.direction]||''}">${r.direction}</td>
      <td class="evt">${r.event}</td>
      <td class="detail">${esc(r.detail)}</td>
      <td class="errstr">${esc(r.error)}</td>
    </tr>`).join('');
}

function esc(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

render();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Azure OpenAI Realtime API 压测脚本 (GA)
测试 gpt-realtime-1.5 + gpt-realtime-whisper 的 TPM/RPM 上限

用法:
  cp .env.example .env && vi .env

  python3 realtime_loadtest.py --mode text --concurrency 10 --duration 60 --html
  python3 realtime_loadtest.py --mode audio --concurrency 5 --duration 60 --html
  python3 realtime_loadtest.py --mode text --ramp --ramp-start 1 --ramp-max 50 --ramp-step 5 --html

  # 带期望配额对比（拿去跟 Azure 对峙用）
  python3 realtime_loadtest.py --mode text --concurrency 20 --duration 120 \\
      --expected-tpm 50000 --expected-rpm 100 --region eastus2 --html
"""

import asyncio
import websockets
from websockets.exceptions import InvalidStatus
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
from datetime import datetime, timezone

# ─── SSL ────────────────────────────────────────────────────────────────────────
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


# ─── 限流异常（携带完整错误信息）────────────────────────────────────────────────
class RateLimitError(Exception):
    def __init__(self, message: str, code: str = "", retry_after: str = ""):
        super().__init__(message)
        self.code = code
        self.retry_after = retry_after


# ─── 结构化日志 ─────────────────────────────────────────────────────────────────
_C = {
    "reset": "\033[0m", "gray": "\033[90m", "cyan": "\033[96m",
    "green": "\033[92m", "yellow": "\033[93m", "red": "\033[91m",
    "bold": "\033[1m", "dim": "\033[2m",
}
_T0 = time.monotonic()


class EventLog:
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.entries: list[dict] = []

    def _entry(self, level, worker, direction, event, detail="", error="") -> dict:
        e = {
            "elapsed":   round(time.monotonic() - _T0, 3),
            "ts":        datetime.now().strftime("%H:%M:%S.%f")[:-3],
            "level":     level,
            "worker":    worker,
            "direction": direction,
            "event":     event,
            "detail":    detail,
            "error":     error,
        }
        self.entries.append(e)
        return e

    def _print(self, e: dict) -> None:
        lc = {"INFO": _C["green"], "WARN": _C["yellow"],
              "ERROR": _C["red"], "DEBUG": _C["dim"]}.get(e["level"], "")
        dc = {"→": _C["cyan"], "←": _C["green"], "✓": _C["green"] + _C["bold"],
              "✗": _C["red"], "⚡": _C["yellow"], "·": _C["gray"]}.get(e["direction"], "")
        ts  = f"{_C['gray']}{e['ts']}{_C['reset']}"
        wid = f"{_C['dim']}[{e['worker']:>3}]{_C['reset']}"
        d   = f"{dc}{e['direction']}{_C['reset']}"
        ev  = f"{lc}{e['event']:<28}{_C['reset']}"
        det = f"{_C['dim']}{e['detail']}{_C['reset']}" if e["detail"] else ""
        err = f" {_C['red']}{e['error']}{_C['reset']}" if e["error"] else ""
        print(f"{ts} {wid} {d} {ev}{det}{err}")

    def send(self, worker, event, payload=None):
        if not self.verbose:
            return
        detail = _fmt_payload(payload) if payload else ""
        self._print(self._entry("DEBUG", worker, "→", event, detail))

    def recv(self, worker, event, payload=None):
        if not self.verbose:
            return
        detail = _fmt_payload(payload) if payload else ""
        self._print(self._entry("DEBUG", worker, "←", event, detail))

    def info(self, worker, msg, detail=""):
        self._print(self._entry("INFO", worker, "·", msg, detail))

    def success(self, worker, event, detail=""):
        self._print(self._entry("INFO", worker, "✓", event, detail))

    def warn(self, worker, event, detail=""):
        self._print(self._entry("WARN", worker, "⚡", event, detail))

    def error(self, worker, event, detail="", exc: BaseException | None = None):
        err_str = ""
        if exc:
            tb = traceback.extract_tb(exc.__traceback__)
            if tb:
                last = tb[-1]
                err_str = f"{type(exc).__name__} @ {last.filename.split('/')[-1]}:{last.lineno}"
            else:
                err_str = f"{type(exc).__name__}: {exc}"
        self._print(self._entry("ERROR", worker, "✗", event, detail, err_str))

    def rate_limit(self, worker, detail=""):
        self._print(self._entry("WARN", worker, "⚡", "429 Rate Limited", detail))

    def write_html(self, path: str, meta: dict) -> None:
        payload = json.dumps({
            "rows": self.entries,
            "meta": meta,
        }, ensure_ascii=False)
        html = _HTML_TEMPLATE.replace("__PAYLOAD__", payload)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"\n[log] HTML 报告: {path}")


def _fmt_payload(p: dict) -> str:
    skip = {"audio", "instructions"}
    parts = []
    for k, v in p.items():
        if k in skip:
            continue
        sv = json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)
        parts.append(f"{k}={sv[:60]}")
    return "  " + "  ".join(parts) if parts else ""


LOG = EventLog(verbose=False)


# ─── 统计结构 ───────────────────────────────────────────────────────────────────
@dataclass
class GlobalStats:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    total_requests:    int   = 0
    success:           int   = 0
    failed:            int   = 0
    rate_limited_429:  int   = 0
    connection_errors: int   = 0
    input_tokens:      int   = 0
    output_tokens:     int   = 0
    total_tokens:      int   = 0

    req_timestamps:   deque = field(default_factory=deque)
    token_timestamps: deque = field(default_factory=deque)
    latencies:        list  = field(default_factory=list)
    errors:           list  = field(default_factory=list)

    # 新增：对峙用关键数据
    peak_rpm:           float        = 0.0
    peak_tpm:           float        = 0.0
    first_429_elapsed:  float | None = None
    first_429_rpm:      float | None = None
    first_429_tpm:      float | None = None
    rate_limit_details: list         = field(default_factory=list)
    timeseries:         list         = field(default_factory=list)  # [{elapsed,rpm,tpm,ok,e429}]

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

    async def record_rate_limit(self, message: str, code: str = "", retry_after: str = ""):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.start_time
            self.total_requests += 1
            self.failed += 1
            self.rate_limited_429 += 1
            self.req_timestamps.append(now)

            rpm = self._rpm_unlocked()
            tpm = self._tpm_unlocked()

            if self.first_429_elapsed is None:
                self.first_429_elapsed = round(elapsed, 1)
                self.first_429_rpm     = rpm
                self.first_429_tpm     = tpm

            self.rate_limit_details.append({
                "elapsed":     round(elapsed, 1),
                "code":        code,
                "message":     message[:300],
                "retry_after": retry_after,
                "rpm":         rpm,
                "tpm":         tpm,
            })
            self.errors.append(f"[429:{code}] {message[:100]}")

    async def record_failure(self, reason: str):
        async with self.lock:
            self.total_requests += 1
            self.failed += 1
            self.req_timestamps.append(time.monotonic())
            self.errors.append(reason[:120])

    async def record_connection_error(self, reason: str):
        async with self.lock:
            self.connection_errors += 1
            self.errors.append(f"[CONN] {reason[:100]}")

    def _rpm_unlocked(self) -> float:
        now = time.monotonic()
        cutoff = now - 60
        return sum(1 for t in self.req_timestamps if t > cutoff)

    def _tpm_unlocked(self) -> float:
        cutoff = time.monotonic() - 60
        return sum(1 for t in self.token_timestamps if t > cutoff)

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

    def snapshot(self, elapsed: float):
        """monitor_loop 每 5s 调用一次，记录时序快照"""
        rpm = self.current_rpm()
        tpm = self.current_tpm()
        if rpm > self.peak_rpm:
            self.peak_rpm = rpm
        if tpm > self.peak_tpm:
            self.peak_tpm = tpm
        self.timeseries.append({
            "elapsed": round(elapsed, 1),
            "rpm":     round(rpm, 1),
            "tpm":     round(tpm, 1),
            "ok":      self.success,
            "e429":    self.rate_limited_429,
            "err":     self.failed,
        })

    def summary(self) -> dict:
        elapsed = time.monotonic() - self.start_time
        lats = self.latencies
        ok_rate = round(self.success / self.total_requests * 100, 1) if self.total_requests else 0
        return {
            "elapsed_s":          round(elapsed, 1),
            "total_requests":     self.total_requests,
            "success":            self.success,
            "success_rate_pct":   ok_rate,
            "failed":             self.failed,
            "rate_limited_429":   self.rate_limited_429,
            "connection_errors":  self.connection_errors,
            "input_tokens":       self.input_tokens,
            "output_tokens":      self.output_tokens,
            "total_tokens":       self.total_tokens,
            "avg_rpm":            round(self.success / elapsed * 60, 1) if elapsed > 0 else 0,
            "avg_tpm":            round(self.total_tokens / elapsed * 60, 1) if elapsed > 0 else 0,
            "peak_rpm":           round(self.peak_rpm, 1),
            "peak_tpm":           round(self.peak_tpm, 1),
            "first_429_elapsed_s": self.first_429_elapsed,
            "first_429_rpm":      self.first_429_rpm,
            "first_429_tpm":      self.first_429_tpm,
            "latency_p50_ms":     round(statistics.median(lats) * 1000, 1) if lats else 0,
            "latency_p95_ms":     round(sorted(lats)[int(len(lats) * 0.95)] * 1000, 1) if lats else 0,
            "latency_p99_ms":     round(sorted(lats)[int(len(lats) * 0.99)] * 1000, 1) if lats else 0,
            "latency_max_ms":     round(max(lats) * 1000, 1) if lats else 0,
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


# ─── WebSocket ──────────────────────────────────────────────────────────────────
def build_ws_url(deployment: str) -> str:
    ep = ENDPOINT.replace("https://", "wss://").replace("http://", "ws://")
    return f"{ep}/openai/v1/realtime?model={deployment}"


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
            _raise_ws_error(evt)


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
            usage = evt.get("response", {}).get("usage", {})
            return usage.get("input_tokens", 0), usage.get("output_tokens", 0)
        if t == "error":
            _raise_ws_error(evt)


async def _wait_transcription_completed(ws, worker: str, timeout: float) -> tuple[int, int, str]:
    """等待 conversation.item.input_audio_transcription.completed，返回 (in_tok, out_tok, transcript)"""
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise asyncio.TimeoutError("等待 transcription.completed 超时")
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        evt = json.loads(raw)
        t = evt.get("type", "")
        LOG.recv(worker, t, {k: v for k, v in evt.items() if k not in ("type", "delta")})
        if t == "conversation.item.input_audio_transcription.completed":
            usage = evt.get("usage", {}) or {}
            in_tok  = usage.get("input_tokens", 0)
            out_tok = usage.get("output_tokens", 0)
            # whisper-1 是按时长计费(usage.type=="duration")，无 token，则记 0
            return in_tok, out_tok, evt.get("transcript", "")
        if t == "conversation.item.input_audio_transcription.failed":
            err = evt.get("error", {})
            _raise_ws_error({"error": err})
        if t == "error":
            _raise_ws_error(evt)


def _raise_ws_error(evt: dict):
    err  = evt.get("error", {})
    code = err.get("code", "")
    msg  = err.get("message", str(evt))
    if "rate" in msg.lower() or "429" in str(code) or "rate_limit" in str(code).lower():
        raise RateLimitError(msg, code=code)
    raise Exception(f"[{code}] {msg}")


def _parse_invalid_status(e: InvalidStatus) -> tuple[bool, str, str, str]:
    """返回 (is_429, code, message, retry_after)"""
    is_429 = e.response.status_code == 429
    code = retry_after = message = ""
    try:
        body = json.loads(e.response.body)
        code    = body.get("error", {}).get("code", "")
        message = body.get("error", {}).get("message", "")
    except Exception:
        message = str(e)
    try:
        retry_after = dict(e.response.headers).get("Retry-After", "")
    except Exception:
        pass
    return is_429, code, message or str(e), retry_after


# ─── 单次会话：文本 ─────────────────────────────────────────────────────────────
async def run_text_session(
    stats: GlobalStats, deployment: str, worker_id: int,
    prompt_idx: int, timeout: float = 30.0,
) -> None:
    wid    = f"W{worker_id:02d}"
    prompt = TEXT_PROMPTS[prompt_idx % len(TEXT_PROMPTS)]
    t_start = time.monotonic()
    try:
        async with websockets.connect(
            build_ws_url(deployment),
            additional_headers={"api-key": API_KEY},
            open_timeout=10, close_timeout=5, ssl=_SSL_CTX,
        ) as ws:
            LOG.info(wid, "connected")
            await _wait_event(ws, wid, "session.created", timeout=10)
            await _ws_send(ws, wid, {
                "type": "session.update",
                "session": {
                    "type": "realtime",
                    "output_modalities": ["text"],
                    "instructions": "You are a minimal assistant. Reply as briefly as possible.",
                },
            })
            await _wait_event(ws, wid, "session.updated", timeout=10)
            await _ws_send(ws, wid, {
                "type": "conversation.item.create",
                "item": {
                    "type": "message", "role": "user",
                    "content": [{"type": "input_text", "text": prompt}],
                },
            })
            await _ws_send(ws, wid, {"type": "response.create"})
            input_tok, output_tok = await _wait_response_done(ws, wid, timeout=timeout)
            latency = time.monotonic() - t_start
            LOG.success(wid, "response.done",
                        f"in={input_tok} out={output_tok} lat={latency:.2f}s")
            await stats.record_success(input_tok, output_tok, latency)

    except RateLimitError as e:
        LOG.rate_limit(wid, f"[{e.code}] {e}")
        await stats.record_rate_limit(str(e), e.code, e.retry_after)
    except InvalidStatus as e:
        is_429, code, msg, retry_after = _parse_invalid_status(e)
        if is_429:
            LOG.rate_limit(wid, f"[{code}] {msg} retry_after={retry_after}")
            await stats.record_rate_limit(msg, code, retry_after)
        else:
            LOG.error(wid, f"HTTP {e.response.status_code}", msg, e)
            await stats.record_failure(msg)
    except asyncio.TimeoutError as e:
        LOG.error(wid, "Timeout", str(e), e)
        await stats.record_failure("Timeout")
    except Exception as e:
        LOG.error(wid, "Exception", str(e), e)
        await stats.record_connection_error(str(e))


# ─── 单次会话：音频 ─────────────────────────────────────────────────────────────
async def run_audio_session(
    stats: GlobalStats, deployment: str, worker_id: int, timeout: float = 45.0,
) -> None:
    wid = f"W{worker_id:02d}"
    t_start = time.monotonic()
    try:
        async with websockets.connect(
            build_ws_url(deployment),
            additional_headers={"api-key": API_KEY},
            open_timeout=10, close_timeout=5, ssl=_SSL_CTX,
        ) as ws:
            LOG.info(wid, "connected")
            await _wait_event(ws, wid, "session.created", timeout=10)
            await _ws_send(ws, wid, {
                "type": "session.update",
                "session": {
                    "type": "realtime",
                    "instructions": "Transcribe the audio and reply with one word.",
                    "audio": {
                        "input": {
                            "format": {"type": "audio/pcm", "rate": 24000},
                            "noise_reduction": {"type": "far_field"},
                            "transcription": {
                                "model": WHISPER_DEPLOYMENT or "gpt-realtime-whisper",
                            },
                            "turn_detection": {
                                "type": "server_vad",
                                "threshold": 0.7,
                                "prefix_padding_ms": 200,
                                "silence_duration_ms": 800,
                                "create_response": False,
                            },
                        },
                        "output": {
                            "format": {"type": "audio/pcm", "rate": 24000},
                            "voice": "alloy",
                        },
                    },
                },
            })
            await _wait_event(ws, wid, "session.updated", timeout=10)
            await _ws_send(ws, wid, {"type": "input_audio_buffer.append", "audio": TEST_AUDIO_B64})
            await _ws_send(ws, wid, {"type": "response.create"})
            input_tok, output_tok = await _wait_response_done(ws, wid, timeout=timeout)
            latency = time.monotonic() - t_start
            LOG.success(wid, "response.done",
                        f"in={input_tok} out={output_tok} lat={latency:.2f}s")
            await stats.record_success(input_tok, output_tok, latency)

    except RateLimitError as e:
        LOG.rate_limit(wid, f"[{e.code}] {e}")
        await stats.record_rate_limit(str(e), e.code, e.retry_after)
    except InvalidStatus as e:
        is_429, code, msg, retry_after = _parse_invalid_status(e)
        if is_429:
            LOG.rate_limit(wid, f"[{code}] {msg} retry_after={retry_after}")
            await stats.record_rate_limit(msg, code, retry_after)
        else:
            LOG.error(wid, f"HTTP {e.response.status_code}", msg, e)
            await stats.record_failure(msg)
    except asyncio.TimeoutError as e:
        LOG.error(wid, "Timeout", str(e), e)
        await stats.record_failure("Timeout")
    except Exception as e:
        LOG.error(wid, "Exception", str(e), e)
        await stats.record_connection_error(str(e))


# ─── 单次会话：转写专用（input audio transcription 模型独立压测）─────────────────
async def run_transcribe_session(
    stats: GlobalStats, deployment: str, worker_id: int,
    transcribe_model: str, language: str = "", timeout: float = 45.0,
) -> None:
    """
    纯转写会话：session.type="realtime" + output_modalities=[]，只命中 input
    audio transcription 模型（gpt-realtime-whisper），不产生任何输出模态（不走
    LLM 补全），从而干净地测量转写模型自己的 RPM/TPM。
    连接走 realtime 部署，转写模型部署名放在 audio.input.transcription.model。
    turn_detection=None，手动 commit 触发转写。
    """
    wid = f"W{worker_id:02d}"
    t_start = time.monotonic()
    transcription: dict = {"model": transcribe_model}
    if language:
        transcription["language"] = language
    try:
        async with websockets.connect(
            build_ws_url(deployment),
            additional_headers={"api-key": API_KEY},
            open_timeout=10, close_timeout=5, ssl=_SSL_CTX,
        ) as ws:
            LOG.info(wid, "connected")
            await _wait_event(ws, wid, "session.created", timeout=10)
            await _ws_send(ws, wid, {
                "type": "session.update",
                "session": {
                    "type": "realtime",
                    "output_modalities": [],
                    "audio": {
                        "input": {
                            "format": {"type": "audio/pcm", "rate": 24000},
                            "noise_reduction": {"type": "far_field"},
                            "transcription": transcription,
                            "turn_detection": None,
                        },
                    },
                },
            })
            await _wait_event(ws, wid, "session.updated", timeout=10)
            await _ws_send(ws, wid, {"type": "input_audio_buffer.append", "audio": TEST_AUDIO_B64})
            await _ws_send(ws, wid, {"type": "input_audio_buffer.commit"})
            input_tok, output_tok, transcript = await _wait_transcription_completed(
                ws, wid, timeout=timeout)
            latency = time.monotonic() - t_start
            LOG.success(wid, "transcription.completed",
                        f'in={input_tok} out={output_tok} lat={latency:.2f}s "{transcript[:30]}"')
            await stats.record_success(input_tok, output_tok, latency)

    except RateLimitError as e:
        LOG.rate_limit(wid, f"[{e.code}] {e}")
        await stats.record_rate_limit(str(e), e.code, e.retry_after)
    except InvalidStatus as e:
        is_429, code, msg, retry_after = _parse_invalid_status(e)
        if is_429:
            LOG.rate_limit(wid, f"[{code}] {msg} retry_after={retry_after}")
            await stats.record_rate_limit(msg, code, retry_after)
        else:
            LOG.error(wid, f"HTTP {e.response.status_code}", msg, e)
            await stats.record_failure(msg)
    except asyncio.TimeoutError as e:
        LOG.error(wid, "Timeout", str(e), e)
        await stats.record_failure("Timeout")
    except Exception as e:
        LOG.error(wid, "Exception", str(e), e)
        await stats.record_connection_error(str(e))


# ─── Worker 池 ──────────────────────────────────────────────────────────────────
async def worker_loop(
    stats: GlobalStats, mode: str, deployment: str,
    stop_event: asyncio.Event, worker_id: int, request_interval: float = 0.0,
    transcribe_model: str = "", language: str = "",
) -> None:
    idx = worker_id
    while not stop_event.is_set():
        if mode == "text":
            await run_text_session(stats, deployment, worker_id, idx)
        elif mode == "transcribe":
            await run_transcribe_session(stats, deployment, worker_id,
                                         transcribe_model, language)
        else:
            await run_audio_session(stats, deployment, worker_id)
        idx += 1
        if request_interval > 0:
            await asyncio.sleep(request_interval)


# ─── 实时监控 ───────────────────────────────────────────────────────────────────
async def monitor_loop(
    stats: GlobalStats, stop_event: asyncio.Event, interval: float = 5.0,
) -> None:
    hdr = (f"\n{'─'*80}\n"
           f"{'时间':>6}  {'RPM(1m)':>8}  {'TPM(1m)':>9}  {'成功':>6}  "
           f"{'429':>5}  {'失败':>5}  {'P50ms':>7}  {'P95ms':>7}  {'总Token':>9}\n"
           f"{'─'*80}")
    print(hdr)
    t0 = time.monotonic()
    while not stop_event.is_set():
        await asyncio.sleep(interval)
        elapsed = time.monotonic() - t0
        stats.snapshot(elapsed)
        s    = stats.summary()
        snap = stats.timeseries[-1] if stats.timeseries else {"rpm": 0.0, "tpm": 0.0}
        c429 = _C["yellow"] if s["rate_limited_429"] > 0 else ""
        rst  = _C["reset"]  if s["rate_limited_429"] > 0 else ""
        print(
            f"{int(elapsed):>5}s"
            f"  {snap['rpm']:>8.0f}"
            f"  {snap['tpm']:>9.0f}"
            f"  {s['success']:>6}"
            f"  {c429}{s['rate_limited_429']:>5}{rst}"
            f"  {s['failed']:>5}"
            f"  {s['latency_p50_ms']:>7.0f}"
            f"  {s['latency_p95_ms']:>7.0f}"
            f"  {s['total_tokens']:>9}"
        )


# ─── 主压测 ─────────────────────────────────────────────────────────────────────
async def run_load_test(
    mode: str, deployment: str, concurrency: int, duration: float,
    request_interval: float = 0.0, transcribe_model: str = "", language: str = "",
) -> GlobalStats:
    print(f"\n{_C['bold']}[压测]{_C['reset']} "
          f"mode={mode} deployment={deployment} concurrency={concurrency} duration={duration}s")
    print(f"  endpoint: {build_ws_url(deployment)}")
    if mode == "transcribe":
        print(f"  transcription model: {transcribe_model}"
              f"{f'  language={language}' if language else ''}")

    stats = GlobalStats()
    stop_event = asyncio.Event()
    workers = [
        asyncio.create_task(
            worker_loop(stats, mode, deployment, stop_event, i, request_interval,
                        transcribe_model, language)
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
    mode: str, deployment: str,
    ramp_start: int, ramp_max: int, ramp_step: int, step_duration: float,
    transcribe_model: str = "", language: str = "",
) -> tuple[list[dict], GlobalStats | None]:
    print(f"\n[Ramp] {ramp_start}→{ramp_max} 并发，每步 {step_duration}s")
    results = []
    last_stats = None
    concurrency = ramp_start

    while concurrency <= ramp_max:
        print(f"\n{'='*50}\n>>> 并发: {concurrency}")
        stats = await run_load_test(mode, deployment, concurrency, step_duration,
                                    0.0, transcribe_model, language)
        last_stats = stats
        s = stats.summary()
        s["concurrency"] = concurrency
        results.append(s)
        print(f"  RPM={s['avg_rpm']}  TPM={s['avg_tpm']}  "
              f"429s={s['rate_limited_429']}  P95={s['latency_p95_ms']}ms")

        if s["rate_limited_429"] > 0:
            print(f"\n{_C['yellow']}!!! 并发={concurrency} 触发 429，"
                  f"上一个稳定并发: {concurrency - ramp_step}{_C['reset']}")
            break
        concurrency += ramp_step

    print(f"\n{'='*60}\nRamp 汇总:")
    print(f"{'并发':>6}  {'RPM':>8}  {'TPM':>9}  {'429':>6}  {'P95ms':>8}")
    for r in results:
        mark = f" {_C['yellow']}<-- 限流{_C['reset']}" if r["rate_limited_429"] > 0 else ""
        print(f"{r['concurrency']:>6}  {r['avg_rpm']:>8.1f}  {r['avg_tpm']:>9.1f}  "
              f"{r['rate_limited_429']:>6}  {r['latency_p95_ms']:>8.1f}{mark}")
    return results, last_stats


# ─── CLI ────────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Azure OpenAI Realtime API 压测工具 (GA)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--mode", choices=["text", "audio", "transcribe"], default="text",
                   help="text=文本补全  audio=语音对话  transcribe=纯转写(独立测 whisper 配额)")
    p.add_argument("--deployment",  default=DEPLOYMENT,
                   help="realtime 部署名(WS连接用)；transcribe 模式也连它")
    p.add_argument("--transcribe-model", default=WHISPER_DEPLOYMENT,
                   help="转写模型部署名(仅 transcribe 模式)，默认 $WHISPER_DEPLOYMENT")
    p.add_argument("--language",    default="",
                   help="转写语言 ISO-639-1(如 en/zh)，留空自动检测(仅 transcribe 模式)")
    p.add_argument("--concurrency", type=int,   default=5)
    p.add_argument("--duration",    type=float, default=60.0)
    p.add_argument("--interval",    type=float, default=0.0)
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--html",        action="store_true", help="生成 HTML 报告")

    # 对峙用：填入 Azure 承诺的配额
    p.add_argument("--expected-tpm", type=int, default=0,
                   help="Azure 承诺的 TPM 配额（用于报告对比）")
    p.add_argument("--expected-rpm", type=int, default=0,
                   help="Azure 承诺的 RPM 配额（用于报告对比）")
    p.add_argument("--region",       default="",
                   help="Azure 区域（如 eastus2），写入报告")

    # Ramp
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
    if args.mode == "transcribe" and not args.transcribe_model:
        print("错误: transcribe 模式需要 --transcribe-model 或 WHISPER_DEPLOYMENT")
        sys.exit(1)

    LOG = EventLog(verbose=args.verbose)
    test_start_utc = datetime.now(timezone.utc).isoformat()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    stats = None
    ramp_results = None

    try:
        if args.ramp:
            ramp_results, stats = loop.run_until_complete(
                run_ramp_test(
                    args.mode, args.deployment,
                    args.ramp_start, args.ramp_max,
                    args.ramp_step, args.ramp_step_duration,
                    args.transcribe_model, args.language,
                )
            )
        else:
            stats = loop.run_until_complete(
                run_load_test(
                    args.mode, args.deployment,
                    args.concurrency, args.duration, args.interval,
                    args.transcribe_model, args.language,
                )
            )
            s = stats.summary()
            print(f"\n{'='*60}\n{_C['bold']}最终统计:{_C['reset']}")
            for k, v in s.items():
                print(f"  {k:<26}: {v}")
            if stats.errors:
                print(f"\n{_C['red']}错误样本 (最近10条):{_C['reset']}")
                for e in stats.errors[-10:]:
                    print(f"  {e}")

    except KeyboardInterrupt:
        print("\n用户中断")
    finally:
        if args.html and stats:
            s = stats.summary()
            meta = {
                "test_start_utc":  test_start_utc,
                "endpoint":        ENDPOINT,
                "deployment":      args.deployment,
                "transcribe_model": args.transcribe_model if args.mode == "transcribe" else "",
                "region":          args.region,
                "mode":            args.mode,
                "concurrency":     args.concurrency,
                "duration_s":      args.duration,
                "expected_tpm":    args.expected_tpm,
                "expected_rpm":    args.expected_rpm,
                "summary":         s,
                "rate_limit_details": stats.rate_limit_details,
                "timeseries":      stats.timeseries,
                "ramp_results":    ramp_results or [],
            }
            ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
            name = f"realtime_report_{args.mode}_c{args.concurrency}_{ts}.html"
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), name)
            LOG.write_html(path, meta)
        loop.close()


# ─── HTML 报告模板 ──────────────────────────────────────────────────────────────
_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<title>Azure Realtime Loadtest Report</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:"SF Mono","Fira Code",monospace;font-size:12px;background:#0d1117;color:#c9d1d9}
h2{font-size:14px;color:#8b949e;margin-bottom:10px;font-weight:normal;text-transform:uppercase;letter-spacing:.05em}
/* ── 顶部元信息 ── */
#meta{background:#161b22;border-bottom:1px solid #30363d;padding:14px 18px;display:flex;flex-wrap:wrap;gap:6px 20px}
#meta .kv{display:flex;gap:6px}
#meta .k{color:#8b949e}
#meta .v{color:#79c0ff}
/* ── 布局 ── */
.section{padding:16px 18px;border-bottom:1px solid #21262d}
/* ── 摘要卡片 ── */
#cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:10px}
.card{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:12px}
.card .label{color:#8b949e;font-size:11px;margin-bottom:4px}
.card .value{font-size:22px;font-weight:bold;color:#c9d1d9}
.card .sub{color:#8b949e;font-size:11px;margin-top:2px}
.card.warn .value{color:#d29922}
.card.danger .value{color:#f85149}
.card.good .value{color:#56d364}
/* ── 图表 ── */
#chart-wrap{position:relative;height:260px;margin-top:4px}
#chart{width:100%;height:100%}
/* ── 429 表 ── */
#rl-table{width:100%;border-collapse:collapse}
#rl-table th,#rl-table td{padding:5px 10px;text-align:left;border-bottom:1px solid #21262d}
#rl-table th{color:#8b949e;font-weight:normal;background:#161b22}
#rl-table td.t{color:#d29922}
/* ── 工具栏 ── */
#toolbar{position:sticky;top:0;background:#161b22;padding:8px 12px;display:flex;gap:8px;
  flex-wrap:wrap;z-index:10;border-bottom:1px solid #30363d}
#toolbar input,#toolbar select{background:#0d1117;color:#c9d1d9;border:1px solid #30363d;
  border-radius:4px;padding:4px 8px;font-size:12px}
#toolbar input{width:200px}
#count,#export-btn{color:#8b949e;align-self:center;cursor:pointer}
#export-btn{margin-left:auto;background:#21262d;border:1px solid #30363d;
  border-radius:4px;padding:4px 10px;color:#79c0ff}
#export-btn:hover{background:#30363d}
/* ── 日志表 ── */
table.log{width:100%;border-collapse:collapse}
table.log th{position:sticky;top:41px;background:#161b22;color:#8b949e;font-weight:normal;
  text-align:left;padding:4px 8px;border-bottom:1px solid #21262d;white-space:nowrap}
table.log tr:hover{background:#161b22}
table.log td{padding:3px 8px;border-bottom:1px solid #21262d;vertical-align:top}
.ts{color:#8b949e;white-space:nowrap}
.wid{color:#79c0ff;white-space:nowrap}
.dir{text-align:center}
.s{color:#56d364}.r{color:#58a6ff}.ok{color:#56d364;font-weight:bold}
.er{color:#f85149}.rl{color:#d29922}.i{color:#8b949e}
.lvl-INFO{color:#c9d1d9}.lvl-WARN{color:#d29922}.lvl-ERROR{color:#f85149}.lvl-DEBUG{color:#6e7681}
.detail{color:#8b949e;word-break:break-all}
.errstr{color:#f85149}
td.dir{width:20px}td.ts{width:110px}td.wid{width:50px}td.evt{width:220px;white-space:nowrap}
</style>
</head>
<body>
<div id="meta"></div>

<div class="section">
  <h2>关键指标</h2>
  <div id="cards"></div>
</div>

<div class="section">
  <h2>RPM / TPM 时序</h2>
  <div id="chart-wrap"><canvas id="chart"></canvas></div>
</div>

<div class="section" id="rl-section" style="display:none">
  <h2>429 限流详情</h2>
  <table id="rl-table">
    <thead><tr>
      <th>+时间(s)</th><th>RPM@事件</th><th>TPM@事件</th>
      <th>错误码</th><th>Retry-After</th><th>错误信息</th>
    </tr></thead>
    <tbody id="rl-body"></tbody>
  </table>
</div>

<div class="section">
  <h2>事件日志</h2>
  <div id="toolbar">
    <input id="search" placeholder="搜索事件/详情/错误..." oninput="renderLog()">
    <select id="fLevel" onchange="renderLog()">
      <option value="">全部级别</option>
      <option>INFO</option><option>WARN</option><option>ERROR</option><option>DEBUG</option>
    </select>
    <select id="fWorker" onchange="renderLog()"></select>
    <select id="fDir" onchange="renderLog()">
      <option value="">全部方向</option>
      <option value="→">→ 发送</option><option value="←">← 接收</option>
      <option value="✓">✓ 成功</option><option value="✗">✗ 错误</option>
      <option value="⚡">⚡ 限流</option>
    </select>
    <span id="count"></span>
    <button id="export-btn" onclick="exportCSV()">⬇ 导出 CSV</button>
  </div>
  <table class="log">
    <thead><tr>
      <th>时间</th><th>+秒</th><th>Worker</th><th>方向</th>
      <th>事件</th><th>详情</th><th>错误</th>
    </tr></thead>
    <tbody id="tbody"></tbody>
  </table>
</div>

<script>
const DATA  = __PAYLOAD__;
const META  = DATA.meta;
const ROWS  = DATA.rows;
const SUM   = META.summary || {};
const TS    = META.timeseries || [];
const RL    = META.rate_limit_details || [];
const RAMP  = META.ramp_results || [];

// ── 元信息条 ──────────────────────────────────────────────────────────────────
const metaFields = [
  ["时间",     META.test_start_utc],
  ["Endpoint", META.endpoint],
  ["部署",     META.deployment],
  ["区域",     META.region || "—"],
  ["模式",     META.mode],
  ...(META.transcribe_model ? [["转写模型", META.transcribe_model]] : []),
  ["并发",     META.concurrency],
  ["时长",     META.duration_s + "s"],
  ["期望TPM",  META.expected_tpm || "—"],
  ["期望RPM",  META.expected_rpm || "—"],
];
document.getElementById("meta").innerHTML =
  metaFields.map(([k,v]) => `<div class="kv"><span class="k">${k}:</span><span class="v">${esc(String(v||""))}</span></div>`).join("");

// ── 摘要卡片 ──────────────────────────────────────────────────────────────────
function pct(n,d){return d?((n/d)*100).toFixed(1)+"%":"—"}
const quota_rpm = META.expected_rpm || 0;
const quota_tpm = META.expected_tpm || 0;
const peak_rpm  = SUM.peak_rpm || 0;
const peak_tpm  = SUM.peak_tpm || 0;
const ok_rate   = SUM.success_rate_pct || 0;
const f429      = SUM.first_429_elapsed_s;

function quotaLine(peak, quota, unit) {
  if (!quota) return "";
  const ratio = (peak/quota*100).toFixed(1);
  const cls   = ratio >= 90 ? "color:#f85149" : ratio >= 70 ? "color:#d29922" : "color:#56d364";
  return `期望${quota.toLocaleString()} ${unit}，实测 <span style="${cls}">${ratio}%</span>`;
}

const cards = [
  {label:"总请求数",   value: (SUM.total_requests||0).toLocaleString(), sub:"",       cls:""},
  {label:"成功率",     value: ok_rate+"%",                              sub:`成功 ${(SUM.success||0).toLocaleString()} 次`, cls: ok_rate>=95?"good":ok_rate>=80?"warn":"danger"},
  {label:"峰值 RPM",   value: peak_rpm.toLocaleString(),               sub: quotaLine(peak_rpm, quota_rpm, "RPM"),    cls: quota_rpm&&peak_rpm<quota_rpm*0.7?"danger":""},
  {label:"峰值 TPM",   value: peak_tpm.toLocaleString(),               sub: quotaLine(peak_tpm, quota_tpm, "TPM"),    cls: quota_tpm&&peak_tpm<quota_tpm*0.7?"danger":""},
  {label:"429 次数",   value: (SUM.rate_limited_429||0).toLocaleString(), sub: f429!=null?`首次 +${f429}s`:"未触发", cls: SUM.rate_limited_429>0?"danger":"good"},
  {label:"首次限流时",  value: f429!=null?"+"+f429+"s":"—",            sub: f429!=null?`RPM=${SUM.first_429_rpm} TPM=${SUM.first_429_tpm}`:"",  cls: f429!=null?"danger":""},
  {label:"P95 延迟",   value: (SUM.latency_p95_ms||0)+"ms",           sub:`P99=${SUM.latency_p99_ms||0}ms Max=${SUM.latency_max_ms||0}ms`, cls:""},
  {label:"总 Token",   value: (SUM.total_tokens||0).toLocaleString(),  sub:`in=${(SUM.input_tokens||0).toLocaleString()} out=${(SUM.output_tokens||0).toLocaleString()}`, cls:""},
];
document.getElementById("cards").innerHTML = cards.map(c =>
  `<div class="card ${c.cls}">
    <div class="label">${c.label}</div>
    <div class="value">${c.value}</div>
    ${c.sub?`<div class="sub">${c.sub}</div>`:""}
  </div>`).join("");

// ── 429 详情表 ────────────────────────────────────────────────────────────────
if (RL.length > 0) {
  document.getElementById("rl-section").style.display = "";
  document.getElementById("rl-body").innerHTML = RL.map(r =>
    `<tr>
      <td class="t">+${r.elapsed}s</td>
      <td>${r.rpm}</td><td>${r.tpm}</td>
      <td style="color:#f85149">${esc(r.code)}</td>
      <td>${esc(r.retry_after||"—")}</td>
      <td class="detail">${esc(r.message)}</td>
    </tr>`).join("");
}

// ── 时序图 ────────────────────────────────────────────────────────────────────
(function drawChart(){
  const canvas = document.getElementById("chart");
  const W = canvas.offsetWidth, H = canvas.offsetHeight;
  canvas.width = W; canvas.height = H;
  const ctx = canvas.getContext("2d");
  if (!TS.length) { ctx.fillStyle="#8b949e"; ctx.fillText("无时序数据",W/2-40,H/2); return; }

  const PAD = {top:20, right:80, bottom:36, left:70};
  const cw = W - PAD.left - PAD.right;
  const ch = H - PAD.top  - PAD.bottom;

  const maxElapsed = Math.max(...TS.map(d=>d.elapsed), 1);
  const maxRPM     = Math.max(...TS.map(d=>d.rpm), quota_rpm, 1);
  const maxTPM     = Math.max(...TS.map(d=>d.tpm), quota_tpm, 1);

  function xp(e){return PAD.left + e/maxElapsed*cw}
  function yRPM(v){return PAD.top + ch - v/maxRPM*ch}
  function yTPM(v){return PAD.top + ch - v/maxTPM*ch}

  // 背景格线
  ctx.strokeStyle="#21262d"; ctx.lineWidth=1;
  for(let i=0;i<=5;i++){
    const y = PAD.top + i/5*ch;
    ctx.beginPath(); ctx.moveTo(PAD.left,y); ctx.lineTo(PAD.left+cw,y); ctx.stroke();
  }

  // 期望配额横线
  if(quota_rpm){
    ctx.setLineDash([6,4]); ctx.strokeStyle="#d29922"; ctx.lineWidth=1.2;
    const y = yRPM(quota_rpm);
    ctx.beginPath(); ctx.moveTo(PAD.left,y); ctx.lineTo(PAD.left+cw,y); ctx.stroke();
    ctx.fillStyle="#d29922"; ctx.font="10px monospace";
    ctx.fillText("RPM quota "+quota_rpm, PAD.left+4, y-4);
  }
  if(quota_tpm){
    ctx.setLineDash([6,4]); ctx.strokeStyle="#e3b341"; ctx.lineWidth=1.2;
    const y = yTPM(quota_tpm);
    ctx.beginPath(); ctx.moveTo(PAD.left,y); ctx.lineTo(PAD.left+cw,y); ctx.stroke();
    ctx.fillStyle="#e3b341"; ctx.font="10px monospace";
    ctx.fillText("TPM quota "+quota_tpm.toLocaleString(), PAD.left+4, y-4);
  }
  ctx.setLineDash([]);

  // 429 事件竖线
  RL.forEach(r=>{
    const x = xp(r.elapsed);
    ctx.strokeStyle="rgba(248,81,73,0.6)"; ctx.lineWidth=1.5;
    ctx.beginPath(); ctx.moveTo(x, PAD.top); ctx.lineTo(x, PAD.top+ch); ctx.stroke();
    ctx.fillStyle="#f85149"; ctx.font="10px monospace";
    ctx.fillText("429", x+3, PAD.top+12);
  });

  // RPM 线（蓝）
  ctx.strokeStyle="#58a6ff"; ctx.lineWidth=2;
  ctx.beginPath();
  TS.forEach((d,i)=>{
    i===0 ? ctx.moveTo(xp(d.elapsed),yRPM(d.rpm)) : ctx.lineTo(xp(d.elapsed),yRPM(d.rpm));
  });
  ctx.stroke();

  // TPM 线（绿，右轴）
  ctx.strokeStyle="#56d364"; ctx.lineWidth=2;
  ctx.beginPath();
  TS.forEach((d,i)=>{
    i===0 ? ctx.moveTo(xp(d.elapsed),yTPM(d.tpm)) : ctx.lineTo(xp(d.elapsed),yTPM(d.tpm));
  });
  ctx.stroke();

  // 轴标签
  ctx.fillStyle="#8b949e"; ctx.font="10px monospace";
  // X轴
  for(let i=0;i<=4;i++){
    const e = maxElapsed*i/4;
    ctx.fillText(Math.round(e)+"s", xp(e)-8, PAD.top+ch+14);
  }
  // Y轴左（RPM）
  ctx.fillStyle="#58a6ff";
  for(let i=0;i<=4;i++){
    const v = maxRPM*i/4;
    ctx.fillText(Math.round(v), 4, yRPM(v)+4);
  }
  ctx.save(); ctx.translate(14, PAD.top+ch/2); ctx.rotate(-Math.PI/2);
  ctx.fillText("RPM", -12, 0); ctx.restore();
  // Y轴右（TPM）
  ctx.fillStyle="#56d364";
  for(let i=0;i<=4;i++){
    const v = maxTPM*i/4;
    ctx.fillText(Math.round(v), PAD.left+cw+4, yTPM(v)+4);
  }
  ctx.save(); ctx.translate(W-6, PAD.top+ch/2); ctx.rotate(Math.PI/2);
  ctx.fillText("TPM", -12, 0); ctx.restore();

  // 图例
  const lx = PAD.left+10, ly = PAD.top+6;
  [[" RPM","#58a6ff"],[" TPM","#56d364"],["— 配额","#d29922"],["↑ 429","#f85149"]].forEach(([label,color],i)=>{
    ctx.fillStyle=color;
    ctx.fillRect(lx+i*80, ly, 10, 10);
    ctx.fillStyle="#c9d1d9";
    ctx.fillText(label, lx+i*80+12, ly+9);
  });
})();

// ── 日志表 ───────────────────────────────────────────────────────────────────
const workers = [...new Set(ROWS.map(r=>r.worker))].sort();
const wSel = document.getElementById("fWorker");
wSel.innerHTML = '<option value="">全部 Worker</option>' +
  workers.map(w=>`<option>${w}</option>`).join("");

const DIR_CLASS = {"→":"s","←":"r","✓":"ok","✗":"er","⚡":"rl","·":"i"};
function renderLog(){
  const q   = document.getElementById("search").value.toLowerCase();
  const lvl = document.getElementById("fLevel").value;
  const wid = document.getElementById("fWorker").value;
  const dir = document.getElementById("fDir").value;
  const rows = ROWS.filter(r=>
    (!lvl||r.level===lvl)&&(!wid||r.worker===wid)&&
    (!dir||r.direction===dir)&&
    (!q||(r.event+r.detail+r.error+r.worker).toLowerCase().includes(q))
  );
  document.getElementById("count").textContent = `${rows.length}/${ROWS.length} 条`;
  document.getElementById("tbody").innerHTML = rows.map(r=>`
    <tr class="lvl-${r.level}">
      <td class="ts">${r.ts}</td>
      <td class="ts">+${r.elapsed}s</td>
      <td class="wid">${r.worker}</td>
      <td class="dir ${DIR_CLASS[r.direction]||""}">${r.direction}</td>
      <td class="evt">${r.event}</td>
      <td class="detail">${esc(r.detail)}</td>
      <td class="errstr">${esc(r.error)}</td>
    </tr>`).join("");
}
renderLog();

// ── CSV 导出 ─────────────────────────────────────────────────────────────────
function exportCSV(){
  const hdr = ["elapsed","ts","level","worker","direction","event","detail","error"];
  const lines = [hdr.join(",")];
  ROWS.forEach(r=>{
    lines.push(hdr.map(k=>'"'+String(r[k]||"").replace(/"/g,'""')+'"').join(","));
  });
  // 也附上 429 详情
  lines.push("","# 429 详情","elapsed,code,message,retry_after,rpm,tpm");
  RL.forEach(r=>{
    lines.push([r.elapsed,r.code,'"'+r.message.replace(/"/g,'""')+'"',r.retry_after,r.rpm,r.tpm].join(","));
  });
  // 时序数据
  lines.push("","# 时序","elapsed,rpm,tpm,ok,e429,err");
  TS.forEach(r=>{
    lines.push([r.elapsed,r.rpm,r.tpm,r.ok,r.e429,r.err].join(","));
  });
  const blob = new Blob([lines.join("\n")],{type:"text/csv;charset=utf-8"});
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "realtime_loadtest.csv";
  a.click();
}

function esc(s){
  return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()

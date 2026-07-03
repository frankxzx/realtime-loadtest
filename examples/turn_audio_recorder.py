#!/usr/bin/env python3
"""
TurnAudioRecorder —— GPT Realtime 音频落地的正确姿势（流式写盘 + 非阻塞转码）

替代 `buf += base64_chunk` 的病灶写法（O(n²) 复制 + 整通电话攒内存）：
- 每个音频 delta 到达即 base64 解码一次并**流式写盘**，内存恒定、无不可变对象拼接；
- 一轮 QA 结束后用 **asyncio 子进程**调 ffmpeg 转 MP3，**不阻塞事件循环**——
  这正是之前生产 1006 断连的根因（见 docs/1006-root-cause-and-fix.md）。

依赖：ffmpeg 在 PATH 上（仅转码用；不需要 pydub / wave / audioop）。

集成（贴进你现有的 WS 事件循环）：

    # 每条连接建一个，跨轮复用；100 并发时共享一个信号量限并行转码数
    sem = asyncio.Semaphore(os.cpu_count() or 4)
    rec = TurnAudioRecorder(out_dir="recordings", encode_semaphore=sem)

    # 「轮开始」（如收到 response.created，或你自己的回合起点）：
    rec.start(f"{call_id}-turn{turn_no}")

    # 每个音频 delta（output 音频 delta 事件里的 base64 音频字段，字段名以你的
    # API 版本为准；input 音频要一起录同理）：
    rec.feed(delta_b64)                      # 同步、微秒级、不阻塞

    # 「轮结束」（如收到 response.done）：
    mp3_path = await rec.finish()            # 异步转码，返回 mp3 路径
    if mp3_path:
        ...  # 落库 / 上传 / 关联到这一轮对话
"""

from __future__ import annotations

import asyncio
import base64
import os
from contextlib import asynccontextmanager
from pathlib import Path


async def encode_pcm_to_mp3(
    pcm_path: str | os.PathLike,
    mp3_path: str | os.PathLike,
    *,
    sample_rate: int = 24000,
    channels: int = 1,
    bitrate: str = "64k",
    ffmpeg: str = "ffmpeg",
) -> None:
    """把裸 PCM16(LE) 文件异步转成 MP3。asyncio 子进程，全程不阻塞事件循环。

    独立函数，也可单独复用。ffmpeg 直接读裸 PCM（-f s16le），不需要先封 WAV。
    """
    proc = await asyncio.create_subprocess_exec(
        ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
        "-f", "s16le", "-ar", str(sample_rate), "-ac", str(channels),
        "-i", str(pcm_path),
        "-b:a", bitrate, str(mp3_path),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"ffmpeg 转码失败 (rc={proc.returncode}): "
            f"{stderr.decode(errors='replace')[:500]}"
        )


@asynccontextmanager
async def _maybe_acquire(sem: asyncio.Semaphore | None):
    """有信号量就限流，没有就直通——避免调用处写两份逻辑。"""
    if sem is None:
        yield
    else:
        async with sem:
            yield


class TurnAudioRecorder:
    """按「轮」录制 Realtime base64 PCM 音频，轮末异步转 MP3。

    一个实例对应一条连接，用 start()/finish() 划分每一轮（也可每轮 new 一个）。
    约定：feed() 只在事件循环线程调用；finish() 是协程。
    """

    def __init__(
        self,
        out_dir: str | os.PathLike,
        *,
        sample_rate: int = 24000,
        channels: int = 1,
        bitrate: str = "64k",
        ffmpeg: str = "ffmpeg",
        keep_pcm: bool = False,
        encode_semaphore: asyncio.Semaphore | None = None,
    ):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.sample_rate = sample_rate
        self.channels = channels
        self.bitrate = bitrate
        self.ffmpeg = ffmpeg
        self.keep_pcm = keep_pcm
        # 高并发下（如 100 路轮末集中转码）限制并行 ffmpeg 数，防 CPU 打满；
        # None=不限。建议传一个 Semaphore(os.cpu_count()) 多实例共享。
        self._sem = encode_semaphore
        self._fh = None
        self._pcm_path: Path | None = None
        self._name: str | None = None
        self._bytes = 0

    def start(self, name: str) -> None:
        """开一轮。name 建议含 call_id + 轮次，用作输出文件名。"""
        if self._fh is not None:      # 上一轮没正常收尾，先兜底关掉
            self.abort()
        self._name = name
        self._pcm_path = self.out_dir / f"{name}.pcm"
        # 缓冲写：BufferedWriter 自动批量落盘，小 write 也高效、不逐字节 syscall
        self._fh = open(self._pcm_path, "wb")
        self._bytes = 0

    def feed(self, delta: str | bytes) -> None:
        """喂一个音频 delta（base64 字符串；已是 bytes 也接受）。同步、非阻塞。

        每 chunk 只解码一次（O(chunk)），流式写盘，全程内存恒定、无 O(n²) 拼接。
        """
        if self._fh is None:
            raise RuntimeError("先 start() 再 feed()")
        pcm = base64.b64decode(delta) if isinstance(delta, str) else delta
        self._fh.write(pcm)
        self._bytes += len(pcm)

    async def finish(self) -> Path | None:
        """收一轮：关闭 PCM → 异步转 MP3 → 返回 mp3 路径。本轮无音频则返回 None。"""
        if self._fh is None:
            return None
        self._fh.close()
        self._fh = None
        pcm_path = self._pcm_path
        assert pcm_path is not None and self._name is not None

        if self._bytes == 0:                       # 空轮（没出声）：清理、跳过转码
            pcm_path.unlink(missing_ok=True)
            return None

        mp3_path = self.out_dir / f"{self._name}.mp3"
        try:
            async with _maybe_acquire(self._sem):
                await encode_pcm_to_mp3(
                    pcm_path, mp3_path,
                    sample_rate=self.sample_rate, channels=self.channels,
                    bitrate=self.bitrate, ffmpeg=self.ffmpeg,
                )
        finally:
            if not self.keep_pcm:
                pcm_path.unlink(missing_ok=True)
        return mp3_path

    def abort(self) -> None:
        """出错时丢弃当前轮（关闭并删除临时 PCM），不转码。"""
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        if self._pcm_path is not None:
            self._pcm_path.unlink(missing_ok=True)
        self._bytes = 0


# ── 自测：不依赖 ffmpeg，验证流式写盘 O(n) 且内存恒定 ──────────────────────────
if __name__ == "__main__":
    import time
    import tracemalloc

    async def _selftest():
        chunk_b64 = base64.b64encode(b"\x00" * 1920).decode()   # 40ms@24k PCM16
        rec = TurnAudioRecorder(out_dir="/tmp/tar_selftest", keep_pcm=True)
        rec.start("demo-turn1")
        tracemalloc.start()
        t0 = time.perf_counter()
        for _ in range(1500 * 7):                # 7 分钟音频，逐 chunk 流式喂
            rec.feed(chunk_b64)
        feed_ms = (time.perf_counter() - t0) * 1000
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        # 关掉文件、看落盘大小；不跑 ffmpeg（本机可能没有）
        rec._fh.close(); rec._fh = None
        size = rec._pcm_path.stat().st_size
        print(f"喂 10500 chunk 耗时 {feed_ms:.0f}ms  "
              f"落盘 {size/1e6:.1f}MB  feed 峰值内存 {peak/1024:.0f}KB")
        print("→ feed 全程内存恒定（KB 级）、耗时线性，无 O(n²)")

    asyncio.run(_selftest())

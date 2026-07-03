#!/usr/bin/env python3
"""
convert_upload 修复参考 —— 针对「asyncio.create_task 包着同步转码」的病灶。

生产实况（已看代码确认）：
- 收集侧没病：response.output_audio.delta → b64decode → list.append，正是推荐写法。
- 病灶在轮末：
      task = asyncio.create_task(self.handle_ai_audio_convert_upload(response_audio_chunks))
      await asyncio.gather(task_upload_agent, task_upload_ai, task_analyze)
  **create_task 不会把工作挪出事件循环**——它只是把协程排进同一个 loop。
  若 handle_*_convert_upload 内部是同步转码（subprocess.run / pydub.export /
  wave+audioop），数秒硬阻塞照样落在事件循环上 → 同进程所有连接 pong 不出去
  → 被网关按超时 RST → 1006（详见 docs/1006-root-cause-and-fix.md）。

两级修法（选一个）：

  A. 手术级（3 行，先止血）：原同步转码函数一个字不改，把调用包进线程池——

         # 原（在 handle_*_audio_convert_upload 内部）:
         mp3_path = self._convert_to_mp3(pcm)              # 同步，卡 loop 数秒
         # 改:
         mp3_path = await asyncio.get_running_loop().run_in_executor(
             None, self._convert_to_mp3, pcm)              # 线程等 ffmpeg，GIL 释放

     注意：如果上传用的是 requests/同步 boto3，同样包一层 run_in_executor。

  B. 到位级：用本文件的 pcm_chunks_to_mp3() 整体替换「拼 WAV → 转 MP3」那段。
     异步 ffmpeg 子进程 + 管道喂 PCM，全程不落 WAV、不碰 wave/audioop/pydub
     （audioop 在 Python 3.13 已移除，迟早要甩）。重采样/改声道直接加 ffmpeg
     出参（如输出 16k：把 "-b:a" 前插 "-ar", "16000"），不需要 audioop.ratecv。
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path


async def pcm_chunks_to_mp3(
    chunks: list[bytes],
    mp3_path: str | os.PathLike,
    *,
    sample_rate: int = 24000,       # Realtime GA 输出 PCM 默认 24kHz/16bit/mono
    channels: int = 1,
    bitrate: str = "64k",
    ffmpeg: str = "ffmpeg",
    semaphore: asyncio.Semaphore | None = None,
) -> tuple[Path | None, int]:
    """PCM chunk 列表 → MP3 文件。全程不阻塞事件循环。

    返回 (mp3 路径, 音频时长 ms)；本轮无音频返回 (None, 0)。
    semaphore 用于限制并行 ffmpeg 数（多连接共享一个，建议 Semaphore(cpu 核数)）。
    """
    if not chunks:
        return None, 0
    pcm = b"".join(chunks)          # 一次 O(n) 拼接，几 MB 级 ~几 ms，可留在 loop 上
    duration_ms = int(len(pcm) * 1000 / (sample_rate * channels * 2))
    mp3_path = Path(mp3_path)
    mp3_path.parent.mkdir(parents=True, exist_ok=True)

    async def _encode() -> None:
        proc = await asyncio.create_subprocess_exec(
            ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
            "-f", "s16le", "-ar", str(sample_rate), "-ac", str(channels),
            "-i", "pipe:0",                       # 裸 PCM 走管道，不落 WAV 临时文件
            "-b:a", bitrate, str(mp3_path),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        # communicate 是 asyncio 流式写入，自带流控，不会卡事件循环
        _, stderr = await proc.communicate(pcm)
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg 转码失败 (rc={proc.returncode}): "
                               f"{stderr.decode(errors='replace')[:400]}")

    if semaphore is None:
        await _encode()
    else:
        async with semaphore:
            await _encode()
    return mp3_path, duration_ms


# ── 集成示意：替换后的 handle_*_audio_convert_upload ────────────────────────────
#
# class RealtimeSession:
#     def __init__(self, ...):
#         # 全实例共享，压住轮末 100 路同时转码的并行度（挂类属性或注入均可）
#         self._ffmpeg_sem = asyncio.Semaphore(os.cpu_count() or 4)
#
#     async def handle_ai_audio_convert_upload(self, chunks: list[bytes]):
#         mp3_path, duration_ms = await pcm_chunks_to_mp3(
#             chunks,
#             f"{self.record_dir}/{self.conversation_id}-r{self.round_number}-ai.mp3",
#             semaphore=self._ffmpeg_sem,
#         )
#         if mp3_path is None:
#             return None, 0
#         url = await self._upload_mp3(mp3_path)   # 上传须是异步的(aiohttp/aioboto3)；
#         return url, duration_ms                  # 若是 requests/同步 boto3，包 executor
#
# 轮末的 create_task + gather 结构保持原样即可——内部不再阻塞后，
# create_task 才真正起到「并行」的作用。


# ── 自测：用假 ffmpeg 桩验证整条异步链路（无需真 ffmpeg）────────────────────────
if __name__ == "__main__":
    import stat
    import sys
    import tempfile
    import time

    async def _selftest() -> None:
        tmp = Path(tempfile.mkdtemp(prefix="fix_cu_"))
        stub = tmp / "fake_ffmpeg.sh"                 # 读 stdin 写出参，模拟转码
        stub.write_text('#!/bin/bash\nout="${@: -1}"; cat > "$out"\n')
        stub.chmod(stub.stat().st_mode | stat.S_IEXEC)

        sem = asyncio.Semaphore(4)
        chunks = [b"\x00" * 1920 for _ in range(1500)]     # 1 分钟 24k PCM

        # 单路：路径/时长/内容
        p, ms = await pcm_chunks_to_mp3(chunks, tmp / "one.mp3",
                                        ffmpeg=str(stub), semaphore=sem)
        assert p and p.stat().st_size == 1920 * 1500 and ms == 60_000, (p, ms)
        # 空轮
        p2, ms2 = await pcm_chunks_to_mp3([], tmp / "empty.mp3", ffmpeg=str(stub))
        assert p2 is None and ms2 == 0
        # 100 路并发轮末转码，经信号量不死锁；顺带测事件循环期间是否仍在转
        t0 = time.perf_counter()
        lag_probe: list[float] = []
        async def probe():
            while time.perf_counter() - t0 < 3:
                t = time.perf_counter()
                await asyncio.sleep(0.05)
                lag_probe.append(time.perf_counter() - t - 0.05)
        results, _ = await asyncio.gather(
            asyncio.gather(*[
                pcm_chunks_to_mp3(chunks, tmp / f"c{i}.mp3",
                                  ffmpeg=str(stub), semaphore=sem)
                for i in range(100)
            ]),
            probe(),
        )
        assert all(p and p.exists() for p, _ in results)
        worst = max(lag_probe) * 1000
        print(f"单路 OK(60000ms)  空轮 OK  100 路并发 OK  "
              f"并发期间事件循环最大滞后 {worst:.1f}ms")
        assert worst < 200, "转码期间事件循环被卡，修复无效!"
        print("→ 转码/喂管道全程不阻塞事件循环，修复有效")

    sys.exit(asyncio.run(_selftest()))

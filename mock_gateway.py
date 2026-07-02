#!/usr/bin/env python3
"""
本地 1006 复现用的「严格网关」mock Realtime 服务端。

行为对齐真实网关/Azure 前端的关键特征：
- keepalive 看门狗：每 --ping-interval 秒 ping 一次客户端，--ping-timeout 秒内
  等不到 pong 就 transport.abort()——发 TCP RST、**不发 close frame**，
  客户端视角就是 1006（和生产看到的一模一样）。
- 正常业务：session.created/updated、response.create → 延迟 0.3~0.8s 回
  response.done（带 usage），足够 chat 模式全流程跑通。
- 明文 ws://，不用折腾证书（realtime_loadtest.py 对 http:// endpoint 自动不启 TLS）。

用法（配合 realtime_loadtest.py 做干净/带病 A/B 对照，详见 README「本地复现 1006」）:

  python3 mock_gateway.py --port 9800 --ping-interval 5 --ping-timeout 5

  export AZURE_OPENAI_ENDPOINT=http://127.0.0.1:9800 AZURE_OPENAI_API_KEY=local
  # A 干净对照组（预期 0 断连）
  python3 realtime_loadtest.py --mode chat --concurrency 8 --session-loop \
      --turn-gap 2 --duration 60 --connect-stagger 0.05
  # B 带病实验组（预期 loop lag 飙升 + 1006 成簇）
  python3 realtime_loadtest.py --mode chat --concurrency 8 --session-loop \
      --turn-gap 2 --duration 90 --connect-stagger 0.05 \
      --sim-pcm-accumulate --sim-rate-mb-min 60
"""

import argparse
import asyncio
import json
import random
import time

import websockets
from websockets.exceptions import ConnectionClosed

T0 = time.monotonic()


def log(msg: str) -> None:
    print(f"[mock +{time.monotonic() - T0:7.1f}s] {msg}", flush=True)


async def keepalive_watchdog(ws, interval: float, timeout: float) -> None:
    """严格网关行为：pong 超时 → RST 掐线（不发 close frame → 客户端记 1006）"""
    while True:
        await asyncio.sleep(interval)
        try:
            pong = await ws.ping()
            await asyncio.wait_for(pong, timeout=timeout)
        except asyncio.TimeoutError:
            log(f"conn#{ws.id} pong 超时 {timeout}s → RST (客户端将看到 1006)")
            ws.transport.abort()
            return
        except ConnectionClosed:
            return


def make_handler(args):
    async def handler(ws):
        dog = asyncio.create_task(
            keepalive_watchdog(ws, args.ping_interval, args.ping_timeout))
        try:
            await ws.send(json.dumps({"type": "session.created", "session": {}}))
            async for raw in ws:
                evt = json.loads(raw)
                t = evt.get("type")
                if t == "session.update":
                    await ws.send(json.dumps(
                        {"type": "session.updated", "session": {}}))
                elif t == "response.create":
                    await asyncio.sleep(random.uniform(0.3, 0.8))
                    await ws.send(json.dumps({"type": "response.done", "response": {
                        "status": "completed",
                        "usage": {"input_tokens": 1300, "output_tokens": 55},
                    }}))
        except ConnectionClosed:
            pass
        finally:
            dog.cancel()
    return handler


async def main() -> None:
    p = argparse.ArgumentParser(description="1006 复现用严格网关 mock（明文 ws://）")
    p.add_argument("--port", type=int, default=9800)
    p.add_argument("--ping-interval", type=float, default=5.0,
                   help="看门狗 ping 间隔秒（默认 5，比生产网关更严，方便快速复现）")
    p.add_argument("--ping-timeout", type=float, default=5.0,
                   help="等 pong 超时秒，超时 RST 掐线（默认 5）")
    args = p.parse_args()
    # 关闭库自带 keepalive，只用看门狗，保证掐线方式是 RST 而不是体面 close
    async with websockets.serve(make_handler(args), "127.0.0.1", args.port,
                                ping_interval=None):
        log(f"严格网关就绪 ws://127.0.0.1:{args.port}  "
            f"看门狗 ping={args.ping_interval}s pong超时={args.ping_timeout}s → RST")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
Realtime API 后端 WebSocket 代理

浏览器 ──ws──▶ 本代理 ──wss(api-key)──▶ Azure OpenAI Realtime

存在的理由（正是浏览器做不到的两件事）：
  1) 不把 api-key 暴露到浏览器：密钥只存在于这一层，浏览器连的是你自己的域。
  2) 把浏览器看不到的「握手 HTTP 状态码」翻译成自定义 WS close code。
     W3C 规定 WS 握手失败时浏览器只给 close(1006)，429/401/5xx 全长一样；
     代理能读到真实状态码，用 4xxx 区间的自定义 close code 转达给前端：
        4429  上游握手限流（连接级 429），reason = Retry-After 秒数
        4401  上游鉴权失败（401/403）
        4400  请求缺少 model 参数
        4502  上游握手其它失败/不可达

会话内的错误（error / response.done / transcription.failed / rate_limits.updated）
本就是 JSON 消息，代理原样双向透传，前端 realtime-error-monitor.js 直接能解析，
无需在这里做任何特殊处理。

依赖：websockets>=14（本仓库压测脚本同款）。运行：
  export AZURE_OPENAI_ENDPOINT=https://xxx.openai.azure.com
  export AZURE_OPENAI_API_KEY=sk-...
  python3 backend/realtime_proxy.py            # 默认 0.0.0.0:8080
前端连接：new WebSocket("ws://localhost:8080/realtime?model=gpt-realtime")
"""

import asyncio
import os
from urllib.parse import urlparse, parse_qs

from websockets.asyncio.server import serve
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus, ConnectionClosed

# ─── 配置 ────────────────────────────────────────────────────────────────────────
ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT", "").rstrip("/")
API_KEY  = os.environ.get("AZURE_OPENAI_API_KEY", "")
HOST     = os.environ.get("PROXY_HOST", "0.0.0.0")
PORT     = int(os.environ.get("PROXY_PORT", "8080"))

# 自定义 close code（3000-4999 为应用私有区间，浏览器 close 事件能原样收到）
CLOSE_RATE_LIMIT = 4429   # 握手被限流（连接级 429）
CLOSE_AUTH       = 4401   # 上游鉴权失败
CLOSE_BAD_REQ    = 4400   # 请求参数问题
CLOSE_UPSTREAM   = 4502   # 上游其它握手失败/不可达


def build_upstream_url(model: str) -> str:
    ep = ENDPOINT.replace("https://", "wss://").replace("http://", "ws://")
    return f"{ep}/openai/v1/realtime?model={model}"


async def authorize_client(client_ws) -> bool:
    """在此校验你自己的用户身份（不是 Azure 的 key）。默认放行——生产环境务必替换。
    可用信息示例：
        token = parse_qs(urlparse(client_ws.request.path).query).get("token", [""])[0]
        auth  = client_ws.request.headers.get("Authorization", "")
        return await verify(token or auth)
    """
    del client_ws  # 默认实现不校验；删除引用以示"此处应使用它"
    return True


async def _relay(client_ws, upstream_ws) -> None:
    """双向透传，任一端断开则同时关闭另一端；把上游的会话级关闭原因带回浏览器。"""
    async def pump(src, dst):
        try:
            async for msg in src:        # msg 可能是 str(JSON 事件) 或 bytes(音频)，都直接转发
                await dst.send(msg)
        except ConnectionClosed:
            pass

    c2u = asyncio.create_task(pump(client_ws, upstream_ws))
    u2c = asyncio.create_task(pump(upstream_ws, client_ws))
    _, pending = await asyncio.wait({c2u, u2c}, return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()

    # 透传上游关闭原因（如 session_expired 会带 1011/原因）；1005/1006 是本地态不可转发
    code = upstream_ws.close_code
    reason = (upstream_ws.close_reason or "")[:120]
    if code in (None, 1005, 1006):
        code = 1000
    try:
        await client_ws.close(code, reason)
    except Exception:
        pass


async def handle_client(client_ws) -> None:
    q = parse_qs(urlparse(client_ws.request.path).query)
    model = (q.get("model") or [""])[0]
    if not model:
        await client_ws.close(CLOSE_BAD_REQ, "missing model")
        return
    if not await authorize_client(client_ws):
        await client_ws.close(CLOSE_AUTH, "unauthorized")
        return

    try:
        upstream_ws = await connect(
            build_upstream_url(model),
            additional_headers={"api-key": API_KEY},
        )
    except InvalidStatus as e:
        status = e.response.status_code
        if status == 429:
            # 用 Headers.get（大小写不敏感）；dict(headers) 会把 key 转小写导致取不到
            retry = e.response.headers.get("Retry-After", "") or ""
            # reason 只放 Retry-After 秒数，前端 monitor 会解析成 retryAfter 数字
            await client_ws.close(CLOSE_RATE_LIMIT, str(retry)[:120])
        elif status in (401, 403):
            await client_ws.close(CLOSE_AUTH, f"upstream {status}")
        else:
            await client_ws.close(CLOSE_UPSTREAM, f"upstream {status}")
        return
    except Exception as e:
        await client_ws.close(CLOSE_UPSTREAM, str(e)[:120])
        return

    async with upstream_ws:
        await _relay(client_ws, upstream_ws)


async def main() -> None:
    if not ENDPOINT or not API_KEY:
        raise SystemExit("请设置 AZURE_OPENAI_ENDPOINT 和 AZURE_OPENAI_API_KEY")
    async with serve(handle_client, HOST, PORT, max_size=None):  # max_size=None: 不限帧大小(音频)
        print(f"[proxy] ws://{HOST}:{PORT}/realtime?model=...  →  {ENDPOINT}")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

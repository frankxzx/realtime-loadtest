# Realtime API 前端异常捕捉 — 集成文档

给 Vue 项目用的 Realtime API（Azure/OpenAI GA）异常旁路监听器。**零侵入**：
不改写你的 `onmessage`、不代理 WebSocket、不发送任何消息，只 `addEventListener`
旁挂一份监听，`detach()` 后彻底移除，随时可拆。

两个文件，无 npm 依赖，复制进项目即可：

| 文件 | 作用 |
|---|---|
| `realtime-error-monitor.js` | 框架无关的核心：解析 + 分类 + 回调 |
| `useRealtimeErrors.js` | Vue 3 组合式封装：响应式状态 + 自动清理 |

## 一、最快接入（Vue 3）

```vue
<script setup>
import { useRealtimeErrors } from "@/lib/useRealtimeErrors";

const { state, attach } = useRealtimeErrors({
  onEvent: (type, payload) => {           // 可选：所有异常的总线，接 toast/埋点
    if (type === "rate_limit") toast.warn(`限流: ${payload.message}`);
  },
});

// 在你现有建连代码里加一行（重连时对新 ws 再调一次即可，自动卸旧监听）
const ws = new WebSocket(url, protocols);
attach(ws);
</script>

<template>
  <!-- state 是响应式的，直接绑 -->
  <div v-if="state.quotaLow" class="banner">配额即将耗尽，可能被限流</div>
  <div v-if="state.rateLimited">已被限流 {{ state.rateLimited }} 次</div>
</template>
```

组件卸载自动 `detach`，不需要手动清理。不用 Vue 的话直接用核心：

```js
import { attachRealtimeMonitor } from "./realtime-error-monitor.js";
const detach = attachRealtimeMonitor(ws, {
  onRateLimit: (p) => console.warn("限流", p.source, p.code, p.message),
  onAny:       (type, p) => report(type, p),   // 统一上报
});
```

## 二、能捕到什么 —— 4 条错误通道 + 1 条预警通道

WebSocket 升级完成后就没有 HTTP 状态码了，**会话内所有异常（包括限流）都以
JSON 事件的形式下发**，本封装按官方 GA schema 逐条解析：

| 回调 | 触发事件 | payload 关键字段 | 建议处理 |
|---|---|---|---|
| `onRateLimit` | 三处限流合并：顶层 `error`、`response.done` failed、转写 failed，code/message 命中限流特征 | `source`(error/response/transcription), `code`, `message`, `raw` | 退避重试；`raw` 里有完整原始事件 |
| `onApiError` | 顶层 `error` 事件（非限流） | `type`, `code`, `message`, `param` | 按 `type` 区分：`invalid_request_error` 是自己的请求有问题，`server_error` 可重试 |
| `onResponseFailed` | `response.done` `status:"failed"`（非限流） | `code`, `type` | 官方 schema 里此处 error **没有 message 字段**，看 `raw` |
| `onResponseIncomplete` | `response.done` `status:"incomplete"` | `reason`: `max_output_tokens` \| `content_filter` | 不是错误，是截断；已生成部分可用 |
| `onResponseCancelled` | `response.done` `status:"cancelled"` | `reason`: `turn_detected` \| `client_cancelled` | VAD 对话里被用户插话打断属正常，一般忽略 |
| `onTranscriptionFailed` | `conversation.item.input_audio_transcription.failed`（非限流） | `itemId`, `code`, `message` | 官方故意不走顶层 error，就是为了让你能按 `itemId` 对上是哪条转写挂了 |
| `onRateLimits` | `rate_limits.updated`（服务端主动推的配额遥测） | `limits`: [{name, limit, remaining, reset_seconds}], `low`: 低于水位线的项 | **预警通道**：`remaining` 触底 = 即将 429，提前降速比事后重试体验好 |
| `onConnectFailed` | open 之前就 close | `closeCode`(通常 1006), `reason` | 见下节浏览器限制 |
| `onAbnormalClose` | 建连成功后非 1000 断开 | `closeCode`, `reason` | 1011=服务端内部错误；指数退避重连 |
| `onAny` | 以上全部 | `(type, payload)` | 统一埋点/上报 |

限流判定与压测脚本 `realtime_loadtest.py` 的 `_is_rate_limit` 同款宽匹配
（`rate limit` / `too many requests` / `quota` / `exceeded` / `429`），原始
message 始终保留在 `payload.raw`，误伤可事后甄别。

## 三、浏览器的一个硬限制：握手 429 看不到状态码

压测脚本（Python）能区分「握手 HTTP 429（连接级限流）」和「会话内 429（模型
配额）」，**浏览器做不到前者**：W3C 规定 WebSocket 握手失败时 JS 拿不到 HTTP
状态码，429/401/403/5xx 一律表现为 `onerror` + `close(1006)`。所以：

- 本封装把「open 之前就 close」统一报成 `onConnectFailed`，语义是"握手被拒，
  原因未知，可能是限流/鉴权/网络"。
- **想在前端精确感知握手 429，唯一正路是加服务端代理**：浏览器 → 你的后端
  （能看到 Azure 返回的 429 和 Retry-After）→ Azure。代理把状态码翻译成自定义
  close code（如 4429）或先用 HTTP 接口下发临时凭证时直接返回 429。生产环境
  本来也不应把 API key 放进浏览器，这两件事可以一起解决。
- 纯前端的务实做法：`onConnectFailed` 后指数退避（1s/2s/4s...+ 抖动）重连，
  连续多次失败再提示用户。

## 四、429 应对策略（推荐组合）

1. **事前**：监听 `onRateLimits`，`state.quotaLow` 为 true（默认剩余 <10%，
   可用 `lowWatermark` 调）就主动降速/排队，别撞墙。
2. **事中**：`onRateLimit` 触发后按 `reset_seconds`（在 `state.quota` 里）或
   指数退避等待再重试；同一会话连接通常还活着，不必重连。
3. **握手层**：`onConnectFailed` 用指数退避重连（很可能是连接级限流）。

## 五、WebRTC 说明

浏览器端用 WebRTC 跑 Realtime 时，事件走 `oai-events` DataChannel，JSON 格式
相同——`attach(dataChannel)` 直接可用（open/close 语义一致），媒体层异常
（ICE 断连等）不在本封装范围。

## 六、useRealtimeErrors 状态一览

```js
state.connected      // 是否成功建连过
state.rateLimited    // 累计限流次数
state.lastRateLimit  // 最近一次限流 {source, code, message, at}
state.lastError      // 最近一条非限流异常 {kind, ...payload, at}
state.quota          // {requests: {limit, remaining, resetSeconds}, ...}
state.quotaLow       // 任一配额剩余低于水位线
state.log            // 最近 50 条异常事件（maxLog 可调），最新在前
```

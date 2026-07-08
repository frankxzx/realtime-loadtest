/**
 * realtime-error-monitor.js — Azure/OpenAI Realtime API 前端异常捕捉（零侵入）
 *
 * 只做一件事：旁路监听一条已有的 WebSocket（或 WebRTC DataChannel），
 * 把 Realtime API 的 4 条错误通道解析成类型化回调。不改写 onmessage、
 * 不发送任何消息、不影响业务收发；detach() 后彻底移除。
 *
 * 4 条错误通道（对齐 GA schema，与 realtime_loadtest.py 的归因一致）：
 *   1. 顶层 `error` 事件                         → onRateLimit / onApiError
 *   2. `response.done` status=failed/incomplete  → onRateLimit / onResponseFailed / onResponseIncomplete
 *   3. `conversation.item.input_audio_transcription.failed` → onRateLimit / onTranscriptionFailed
 *   4. 连接层 close/error                        → onConnectFailed / onAbnormalClose
 * 另有配额遥测：`rate_limits.updated` → onRateLimits（remaining 触底≈即将 429）
 *
 * 浏览器限制（重要）：WS 握手被 HTTP 429/401 拒绝时，浏览器只给 close(1006)，
 * 看不到状态码。若经过 backend/realtime_proxy.py 代理，握手层的 HTTP 状态码会被
 * 翻译成自定义 close code（4429/4401/...），本监听器识别后把握手 429 直接并入
 * onRateLimit（source="handshake"）——业务侧零改动即可同时感知会话内/握手两种 429。
 * 未走代理时（浏览器直连），握手失败一律归到 onConnectFailed。
 */

/** 与压测脚本 _is_rate_limit 同款宽匹配；原始 message 始终在 payload.raw 里保留 */
const RATE_LIMIT_RE = /rate.?limit|too.?many.?requests|quota|exceeded|429/i;

/** backend/realtime_proxy.py 用来转达握手层 HTTP 状态码的自定义 close code 约定 */
const PROXY_CLOSE_CODES = { 4429: "rate_limit", 4401: "auth", 4400: "bad_request", 4502: "upstream" };

export function isRateLimitError(code, message) {
  return RATE_LIMIT_RE.test(String(code || "")) || RATE_LIMIT_RE.test(String(message || ""));
}

/**
 * @param {WebSocket|RTCDataChannel} channel 已创建的连接（任意 readyState 均可）
 * @param {object} handlers 全部可选：
 *   onRateLimit({source, code, message, retryAfter?, raw})  限流（source: error|response|transcription|handshake）
 *                                                  handshake 来自代理翻译的 4429，带 retryAfter(秒)
 *   onApiError({type, code, message, param, raw})  顶层 error 事件（非限流）
 *   onResponseFailed({code, type, raw})            response.done failed（非限流）
 *   onResponseIncomplete({reason, raw})            截断：max_output_tokens | content_filter
 *   onResponseCancelled({reason, raw})             取消：turn_detected | client_cancelled（VAD 下属正常）
 *   onTranscriptionFailed({itemId, code, message, raw}) 转写失败（非限流）
 *   onRateLimits({limits, low, raw})               rate_limits.updated；low=remaining/limit<lowWatermark 的项
 *   onConnectFailed({closeCode, reason, kind})     握手失败；kind 来自代理约定(auth/bad_request/upstream/unknown)
 *   onAbnormalClose({closeCode, reason})           建连成功后的非 1000 断开
 *   onAny(type, payload)                           以上所有事件的总线（type 为回调名去掉 on 的小写蛇形）
 * @param {object} opts { lowWatermark = 0.1, proxyCloseCodes } 低水位阈值 / 覆盖代理 close code 映射
 * @returns {() => void} detach 函数
 */
export function attachRealtimeMonitor(channel, handlers = {}, opts = {}) {
  const lowWatermark = opts.lowWatermark ?? 0.1;
  const proxyCloseCodes = opts.proxyCloseCodes ?? PROXY_CLOSE_CODES;
  let opened = channel.readyState === 1; // WebSocket.OPEN 与 RTCDataChannel "open" 均为 1/"open"
  if (typeof channel.readyState === "string") opened = channel.readyState === "open";

  const emit = (name, payload) => {
    const cb = "on" + name.replace(/(^|_)(\w)/g, (_, __, c) => c.toUpperCase());
    // 回调异常不外冒：既不影响业务监听器，也不污染全局 onerror/错误上报
    try { handlers.onAny?.(name, payload); } catch (e) { console.error("[realtime-monitor] onAny 回调异常:", e); }
    try { handlers[cb]?.(payload); } catch (e) { console.error(`[realtime-monitor] ${cb} 回调异常:`, e); }
  };

  const onOpen = () => { opened = true; };

  const onMessage = (e) => {
    let evt;
    try { evt = JSON.parse(e.data); } catch { return; } // 音频二进制/非 JSON 一律忽略
    switch (evt.type) {
      case "error": {
        const err = evt.error || {};
        if (isRateLimitError(err.code, err.message)) {
          emit("rate_limit", { source: "error", code: err.code || "", message: err.message || "", raw: evt });
        } else {
          emit("api_error", { type: err.type || "", code: err.code || "",
                              message: err.message || "", param: err.param || "", raw: evt });
        }
        break;
      }
      case "response.done": {
        const resp = evt.response || {};
        const sd   = resp.status_details || {};
        if (resp.status === "failed") {
          const err = sd.error || {};
          if (isRateLimitError(err.code, err.type)) {
            emit("rate_limit", { source: "response", code: err.code || "", message: err.type || "", raw: evt });
          } else {
            emit("response_failed", { code: err.code || "", type: err.type || "", raw: evt });
          }
        } else if (resp.status === "incomplete") {
          emit("response_incomplete", { reason: sd.reason || "", raw: evt });
        } else if (resp.status === "cancelled") {
          emit("response_cancelled", { reason: sd.reason || "", raw: evt });
        }
        break;
      }
      case "conversation.item.input_audio_transcription.failed": {
        const err = evt.error || {};
        if (isRateLimitError(err.code, err.message)) {
          emit("rate_limit", { source: "transcription", code: err.code || "", message: err.message || "", raw: evt });
        } else {
          emit("transcription_failed", { itemId: evt.item_id || "", code: err.code || err.type || "",
                                         message: err.message || "", raw: evt });
        }
        break;
      }
      case "rate_limits.updated": {
        const limits = evt.rate_limits || [];
        const low = limits.filter(r => r.limit > 0 && r.remaining / r.limit < lowWatermark);
        emit("rate_limits", { limits, low, raw: evt });
        break;
      }
    }
  };

  const onClose = (e) => {
    const code = e.code ?? null;
    const reason = e.reason || "";
    const mapped = proxyCloseCodes[code];
    // 代理翻译的握手 429 → 直接并入限流通道（source=handshake），业务侧零改动即可感知
    if (mapped === "rate_limit") {
      const s = reason.trim();
      const retryAfter = /^\d+$/.test(s) ? Number(s) : null;
      emit("rate_limit", {
        source: "handshake", code: "rate_limit_exceeded",
        message: retryAfter != null ? `握手被限流(连接级)，Retry-After=${retryAfter}s` : "握手被限流(连接级)",
        retryAfter, raw: e,
      });
      return;
    }
    if (!opened) emit("connect_failed", { closeCode: code, reason, kind: mapped || "unknown" });
    else if (code !== 1000) emit("abnormal_close", { closeCode: code, reason });
  };

  const onError = () => { /* 浏览器 error 事件无信息量，紧随的 close 才有 code，这里不上报 */ };

  channel.addEventListener("open", onOpen);
  channel.addEventListener("message", onMessage);
  channel.addEventListener("close", onClose);
  channel.addEventListener("error", onError);

  return function detach() {
    channel.removeEventListener("open", onOpen);
    channel.removeEventListener("message", onMessage);
    channel.removeEventListener("close", onClose);
    channel.removeEventListener("error", onError);
  };
}

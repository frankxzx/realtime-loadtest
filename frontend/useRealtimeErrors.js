/**
 * useRealtimeErrors.js — realtime-error-monitor 的 Vue 3 组合式封装
 *
 * 用法（对现有代码零改动，只在拿到 ws 的地方加一行 attach(ws)）：
 *   const { state, attach, detach } = useRealtimeErrors();
 *   attach(ws);  // ws 建好后旁挂；重连时对新 ws 再调一次即可（自动卸旧的）
 *
 * 组件卸载时自动 detach，无需手动清理。
 */
import { reactive, onBeforeUnmount, getCurrentScope } from "vue";
import { attachRealtimeMonitor } from "./realtime-error-monitor.js";

export function useRealtimeErrors(options = {}) {
  const maxLog = options.maxLog ?? 50;

  const state = reactive({
    connected: false,          // 是否成功建连过（握手失败时保持 false）
    rateLimited: 0,            // 累计限流次数（三种 source 合并）
    lastRateLimit: null,       // {source, code, message, at}
    lastError: null,           // 最近一条非限流异常 {kind, code, message, at}
    quota: {},                 // name -> {limit, remaining, resetSeconds}（rate_limits.updated）
    quotaLow: false,           // 任一配额 remaining 低于水位线
    log: [],                   // 最近 maxLog 条异常事件 {kind, detail, at}，原始事件在 detail.raw
  });

  const push = (kind, detail) => {
    state.log.unshift({ kind, detail, at: Date.now() });
    if (state.log.length > maxLog) state.log.pop();
  };

  let _detach = null;

  function attach(channel) {
    _detach?.();
    _detach = attachRealtimeMonitor(channel, {
      onAny: (type, payload) => {
        if (type !== "rate_limits") push(type, payload);
        options.onEvent?.(type, payload);         // 业务侧想额外处理（弹 toast/上报）从这里接
      },
      onRateLimit: (p) => {
        state.rateLimited += 1;
        state.lastRateLimit = { ...p, at: Date.now() };
      },
      onApiError:            (p) => { state.lastError = { kind: "api_error", ...p, at: Date.now() }; },
      onResponseFailed:      (p) => { state.lastError = { kind: "response_failed", ...p, at: Date.now() }; },
      onTranscriptionFailed: (p) => { state.lastError = { kind: "transcription_failed", ...p, at: Date.now() }; },
      onConnectFailed:       (p) => { state.lastError = { kind: "connect_failed", ...p, at: Date.now() }; },
      onAbnormalClose:       (p) => { state.lastError = { kind: "abnormal_close", ...p, at: Date.now() }; },
      onRateLimits: ({ limits, low }) => {
        for (const r of limits) {
          state.quota[r.name] = { limit: r.limit, remaining: r.remaining, resetSeconds: r.reset_seconds };
        }
        state.quotaLow = low.length > 0;
      },
    }, options);
    channel.addEventListener("open", () => { state.connected = true; }, { once: true });
    if (channel.readyState === 1 || channel.readyState === "open") state.connected = true;
  }

  function detach() { _detach?.(); _detach = null; }

  if (getCurrentScope()) onBeforeUnmount(detach);

  return { state, attach, detach };
}

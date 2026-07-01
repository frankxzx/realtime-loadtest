# CLAUDE.md

本项目的完整开发指南见 **[AGENTS.md](./AGENTS.md)** —— 项目用途、开发工作流、
代码结构、GA schema 铁律、异常模型都在里面。接手前先读它。

要点速记：
- 单文件 `realtime_loadtest.py`；本目录即 git 仓库，直接改 → commit → **自动 push**。
- 动任何 Realtime API 字段/事件前，**先查官方文档**，别凭记忆（这个 API 改得很勤）。
- GA 里没有 `temperature`/`max_response_output_tokens`；用 `output_modalities`（非空，非 `modalities`）。

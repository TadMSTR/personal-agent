# Changelog

## [0.1.0] — 2026-05-03

### Added
- Matrix poll loop (`manager.py`): spawns and resumes stateful `claude -p` sessions per Matrix thread
- SQLite session store with `session_retention_days` cleanup (hourly, cascades to event_aliases)
- Operator gate in `handle_event()` — non-operator messages rejected before any tool call
- Structured logging via Python `logging` (timestamped, PM2-compatible)
- PM2 service definition (`ecosystem.config.js`, `start.sh`)
- scoped-mcp manifest (`~/.claude/manifests/personal-agent.yml`): 11-module tool surface with argument filters
  - Modules: matrix, task-queue, agent-bus, memory-search, memory-metadata, matrix-mcp, searxng, plane, pm2, backrest, homelab-ops
  - `no-credentials` filter blocks credential-shaped strings from all tool args
  - `personal-agent-path-scope` filter restricts filesystem reads to agent's own dirs + shared comms/memory
- Rate limiter for session resumes: 3s minimum between resumes, queue-depth warning at >3

### Security
- M1: Path scope restricted to `~/.claude/memory/feedback_` prefix — no broad memory access
- M2: Removed `fetch_url` / `search_and_fetch` from searxng surface (SSRF prevention)
- L3: Full stderr routed to `log.error`; Matrix gets sanitized `[Error: claude exited with code N]`
- L4: Messages >32,000 chars rejected before subprocess invocation

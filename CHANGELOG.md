# Changelog

## [0.2.0] — 2026-05-04

### Added
- Idle-based session rollover: background monitor fires every 60s; sessions idle past 1800s
  are automatically rolled over (commits `aca536a`, `c7570ee`)
- `trigger_rollover()`: resumes session with summarization prompt, writes handoff file
  to `~/.claude/projects/personal-agent/handoffs/<session_id>.md`, retires old session,
  allocates new session_id (no spawn until next user message)
- Continuity injection on first post-rollover message: handoff text + last-10 transcript
  turns prepended to user message; spawned with `--append-system-prompt`; `handoff_injected`
  flag prevents double-injection on subsequent messages
- `read_last_n_turns()` helper reads Claude JSONL transcript to extract last N user/assistant
  turns as plain text for context injection
- `startup_notify` message updated to `personal-agent v0.2 online.`

### Changed
- Schema migration runs automatically on startup when v0.1 schema is detected
  (UNIQUE on `thread_root_id` dropped to support retired+active rows per thread)
- New columns: `status` (active | retired), `handoff_injected`, `previous_session_id`
- New index: `idx_sessions_status` for efficient idle monitor queries
- `get_session_by_event()` now filters `AND status = 'active'` — retired sessions ignored
- `cleanup_old_sessions()` also prunes orphan `event_aliases` rows

### Security
- FW-01: Handoff file written atomically (.tmp → chmod 600 → rename) — no partial-file risk
- MIG-01: Schema migration wrapped in explicit `BEGIN; … COMMIT;` — crash-safe
- CONC-01: `trigger_rollover()` acquires `_room_lock` for full duration — prevents
  concurrent resume calls against the same session_id during rollover
- M1: Path scope restricted to `~/.claude/memory/feedback_` prefix (manifest-level)
- M2: `fetch_url` / `search_and_fetch` removed from searxng surface (SSRF prevention)
- L1: Session retention cleanup added — runs hourly, cascades to `event_aliases`
- L2: Resume rate limiter: 3s minimum between resumes; queue-depth warning at >3;
  rate-exceeded path posts "still processing — retry in Ns" to room
- L3: Full stderr routed to `log.error`; Matrix gets sanitized error message
- L4: Messages >32,000 chars rejected before subprocess invocation

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

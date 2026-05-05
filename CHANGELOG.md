# Changelog

## [0.3.0] — 2026-05-04

### Added
- Three OR'd rollover triggers replacing the fixed 1800s idle threshold (commit `e0d2018`):
  - `token_budget`: rollover at fill ≥ 80%
  - `nightly_cutoff`: rollover when session was created before the last
    `nightly_rollover_hour` boundary (default 4 AM local)
  - `idle`: dynamic threshold (360s/1200s/1800s at fill ≥80% / ≥60% / else),
    gated by a 40% minimum-fill floor
- `get_token_fill_pct()`: estimates context fill from JSONL transcript
- `idle_threshold_for_fill()`: maps fill to idle window
- `last_nightly_cutoff()`: most recent local-time nightly_hour boundary
- `should_rollover()`: per-session trigger evaluation, returns `(do_rollover, trigger, threshold)`
- `update_token_fill()`: updates `sessions.token_fill_pct` after each claude exit
- `context_window_tokens` config key (default 200000) — denominator for fill_pct
- `nightly_rollover_hour` config key (default 4) — wake-cycle aligned reset hour

### Changed
- `idle_monitor()` now evaluates `should_rollover()` per active session instead of a fixed cutoff
- `trigger_rollover()` accepts `trigger` and `threshold` arguments; recheck branches on trigger type
- Startup notification bumped to `personal-agent v0.3 online.` (commit `a625827`)

### Security (v0.3 audit — commit `ae9276b`)
- M1: `get_token_fill_pct()` rewritten to reverse-scan transcript and sum
  `input_tokens + cache_creation_input_tokens + cache_read_input_tokens` from the
  latest entry with usage. Original implementation summed `input_tokens` only,
  undercounting by ~95% under prompt caching and silently no-op'ing the
  `token_budget` trigger.
- M2: `trigger_rollover()` recheck branches on trigger type — `idle` validates
  freshness, `token_budget` re-reads fill, `age_cap` re-checks age. The v0.2
  recheck always tested idleness, silently dropping `token_budget` and
  `age_cap` rollovers when the user was active.
- L1: subsumed by M1's reverse-scan-and-break — full transcript no longer
  read inside `_room_lock`.
- L2: `200_000` divisor lifted to `context_window_tokens` config key
  (default 200000), threaded through `idle_monitor` and `handle_event`.

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

### Security (v0.2 audit — commit `26c561c`)
- M1: `trigger_rollover()` rechecks session freshness under `_room_lock` before
  generating handoff — prevents wasted handoff on a freshly-active session
- M2: `handle_event()` re-fetches session row inside `_room_lock` — prevents
  resuming a retired session when idle monitor races the handler
- L1: `handoff_injected` set before `spawn_personal()` so `TimeoutError`
  does not cause double-injection on retry
- L2: Handoff `.tmp` file created with mode `0o600` via `os.open()` — eliminates
  brief permissions window from the prior `write_text()+chmod()` sequence
- L3: `read_last_n_turns()` capped to 32KB; `handoff_text` capped to 16KB in
  continuity injection — prevents `ARG_MAX` overflow
- L4: `trigger_rollover()` uses `HANDOFF_TIMEOUT_SECONDS=300` instead of full
  `subprocess_timeout` — bounds room lock duration to 5 min for handoff generation

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

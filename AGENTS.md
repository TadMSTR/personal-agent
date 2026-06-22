# AGENTS.md — personal-agent

Context for agents working on this repo.

## What this is

The `manager.py` daemon that drives a continuous-conversation Claude Code agent
over Matrix without ever compacting context. It rolls `claude -p` sessions over at
a token budget below the auto-compact trigger and bridges continuity with a
self-captured raw-tail handoff.

Persona-neutral: the name ("Harlock") is in `config.*.yml`, never in the code.

## Key invariants — do not break

- **Ordering:** `ROLLOVER_BUDGET (130k) < auto-compact trigger (~167k) < hard limit`.
  Never raise the budget above the trigger. The trigger is pinned via the agent's
  `CLAUDE_CODE_AUTO_COMPACT_WINDOW=180000`.
- **One active session per room** is DB-enforced (`one_active_per_room` partial
  unique index). Routing is *not* thread-based: every trusted message continues
  the active session; spawn only when none is active. (Porting the old
  thread/room-root logic reintroduces the resume bug — don't.)
- **fill = input-side token sum** of the latest turn
  (`input + cache_read + cache_creation`), read from the `stream-json` `result`
  event. Because the full history is re-sent each turn, that *is* the current
  context size. Do not accumulate across turns.
- **Never read the agent's transcripts.** The agent may run as an isolated OS user
  with `0600` files. Continuity comes from the manager's own `turns` table only.
  Cold-archiving raw transcripts belongs to `scripts/harlock-archive`, which runs
  *as the agent user*.

## Security posture (carried from matrix-dispatcher)

- Bot creds from env vars, asserted at startup; never passed into the agent
  subprocess (minimal env allowlist + `sudo` scrub).
- Parameterized SQL throughout.
- `session_id` validated with `uuid.UUID()` before reaching argv.
- Logs: IDs/actions/exit codes only — no message bodies.
- `sessions.db` (+ WAL/SHM) forced to `0600`.

## Do not set `DISABLE_AUTO_COMPACT`

No-compaction is achieved by rolling below the trigger; auto-compact stays enabled
as a graceful backstop for a rare oversized single turn.

## Tests

`python -m pytest tests/ -q`. Tests use an in-memory DB and stubbed stream-json —
no Matrix or subprocess needed.

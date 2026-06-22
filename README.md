# personal-agent

A daemon that drives a **single continuous conversation** with a Claude Code
agent over Matrix, where **context never compacts**. Instead of letting the CLI
auto-compact, the manager rolls to a fresh `claude -p` session before the
compaction trigger and bridges continuity with a raw last-N-turn handoff. Older
context is recalled on demand through your memory/search systems.

This deployment is **"Harlock"** — but the name is config, not code. Pick your own
in `config.<name>.yml`.

## Why

`claude -p` auto-compacts when the context fills, which silently rewrites history
mid-conversation. For a long-lived assistant you'd rather keep each session small
and *deliberately* roll over, preserving the raw recent turns and offloading the
rest to durable memory. The manager makes that rollover explicit and keeps the
session fill safely below the compaction threshold.

```
rollover budget (130k)  <  auto-compact trigger (~167k)  <  hard context limit
```

With `CLAUDE_CODE_AUTO_COMPACT_WINDOW=180000` pinned in the agent's settings, the
trigger is a deterministic ~167k (`window − 13k`, minus the output reservation).
The manager rolls at 130k — a ~37k margin — so auto-compact (left enabled as a
backstop) effectively never fires.

## How it works

- **One active session per room**, DB-enforced via a partial unique index. Every
  trusted message continues that session (`--resume`); a fresh session is opened
  only when none is active. (No thread routing — this fixes the classic
  "room-root message spawns instead of resuming" bug.)
- **Self-captured turns.** The manager parses each turn's `stream-json` stdout and
  stores the exchange (user text, assistant text, tool names, token fill) in its
  own SQLite DB. It never reads the agent's transcripts — which matters when the
  agent runs as an isolated OS user with `0600` files.
- **Pinned model.** The manager passes `--model` (config `deployment.model`,
  default `claude-sonnet-4-6`) so the budget math matches a known context window.
  Leaving it unset inherits the host default — on a 1M-window model the budget
  below is wrong (a single tool-loading turn can exceed 200k and the 180k
  auto-compact pin is ineffective).
- **Rollover triggers** (`rollover_budget` config-driven, default 130000):
  | trigger | check | when |
  |---|---|---|
  | `token_budget` | `fill ≥ rollover_budget` | post-turn (primary) |
  | `idle` | idle > 1h **and** fill < 40% of budget | pre-turn |
  | `nightly` | last used before 04:00 UTC | pre-turn |
- **Warm-context bridge.** On rollover the manager writes a durable handoff note
  (memsearch-indexed) and, on the next message, **inlines** the handoff into the
  new session's first prompt. The handoff is an **Ollama summary** of the tail
  (`summarize:latest`) plus the last 2 raw turns; it falls back to the full raw
  last-6-turn dump if Ollama is disabled or unavailable.
- **Per-turn datetime tag.** Every user turn is prefixed with
  `[time: 2026-06-16 17:26 EDT | +3h12m since last]`. The elapsed-time delta is
  the signal; behavioural guidance (tight continuation vs. re-orient after a long
  gap) lives in the agent's `CLAUDE.md`.
- **Idle-harvest.** A background loop captures a long-idle session into the
  memsearch session tier and marks it `expired`. Cold-archiving the agent's raw
  transcripts is a *separate, scoped* job (see below).

## Commands

Sent in the room (`!` prefix — Element intercepts `/`):

| command | effect |
|---|---|
| `!recap [N]` | last N turns, walking rollover boundaries (default 5) |
| `!sessions` | recent sessions in the room with state/fill |
| `!cancel` | SIGTERM the active turn |
| `!mirror` | adopt the most recent local session (deployment-dependent) |
| `!help` | command list |

## Layout

```
manager.py                  the daemon
config.example.yml          template (copy to config.<name>.yml)
start.sh                    sources creds env, selects config, execs the daemon
ecosystem.config.js         PM2 process definition
scripts/harlock-archive     cold-archive janitor (runs AS the agent user)
systemd/                    timer + service for the janitor
tests/test_manager.py       unit tests
```

## Configuration

Bot credentials are **never** in the config file — they come from env vars
(`PERSONAL_AGENT_HOMESERVER`, `PERSONAL_AGENT_USER_ID`,
`PERSONAL_AGENT_ACCESS_TOKEN`), loaded by `start.sh` from a `chmod 600` env file.
Everything else is in `config.<name>.yml`; see `config.example.yml`.

## Isolated-agent deployments

When the agent runs as a **different, hardened OS user** (Harlock runs as
`agent-harlock`), the manager:

- launches via `sudo -n -u <agent_user> -- <claude_bin> …` (a NOPASSWD sudoers
  grant scoped to exactly that command);
- **cannot and does not** read the agent's `0600` transcripts or write into its
  home — continuity is entirely self-captured + inline-injected;
- delegates raw-transcript cold-archiving to `scripts/harlock-archive`, which runs
  **as the agent user** via `systemd/harlock-archive.{service,timer}`. That's the
  correct privilege boundary: the file owner does its own housekeeping.

## Running

```bash
python -m venv venv && venv/bin/pip install -r requirements.txt
cp config.example.yml config.harlock.yml   # edit it
# create ~/.claude-secrets/personal-agent.env (chmod 600) with the 3 env vars
pm2 start ecosystem.config.js
```

Install the archive janitor (root):

```bash
install -m 0755 scripts/harlock-archive /usr/local/bin/harlock-archive
install -m 0644 systemd/harlock-archive.{service,timer} /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now harlock-archive.timer
```

## Tests

```bash
python -m pytest tests/ -q
```

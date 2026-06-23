"""personal-agent manager — a continuous-conversation daemon over claude -p.

One Matrix room maps to one *continuous conversation*, backed by a chain of
`claude -p` sessions. Context never compacts: the manager rolls to a fresh
session before the auto-compact trigger (130k budget < 167k trigger) and bridges
continuity with a raw last-N-turn handoff that it injects inline into the next
session's first prompt. Deeper history is recalled on-demand by the agent via the
platform's memory systems.

This repo is persona-neutral. The deployment in `config.*.yml` decides the name
(this one is "Harlock"); nothing in the code hard-codes it.

Design notes / security posture (carried from matrix-dispatcher):
  - Bot credentials come from env vars, asserted non-empty at startup. They never
    flow into the agent subprocess (sudo scrubs env; we also pass a minimal
    allowlist).
  - All SQL is parameterized — no f-string SQL.
  - session_id values are validated with uuid.UUID() before reaching argv
    (--session-id / --resume).
  - Logs carry event/room/session IDs, actions, exit codes — never message bodies.
  - sessions.db (and WAL/SHM) are forced to mode 600.

Self-captured turns: because the agent runs as a *different, isolated* OS user
whose transcripts are mode 0600, the manager cannot read the agent's JSONL. It
does not need to: it owns the full stream-json stdout of every turn, so it
persists each exchange into its own (ted-owned) `turns` table and builds the
handoff tail, !recap, and idle-harvest notes from that.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sqlite3
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
from nio import AsyncClient, RoomMessageText, SyncResponse

# ---------------------------------------------------------------------------
# Defaults (overridable per-deployment via config.yml)
# ---------------------------------------------------------------------------

ROLLOVER_BUDGET = 130_000          # roll at this input-token fill. Default assumes
                                   # the design model (Sonnet, 200k window; ~167k
                                   # auto-compact trigger). Override via config
                                   # `rollover_budget` if you pin a different model.
SUBPROCESS_TIMEOUT_SECONDS = 600   # per-turn ceiling
RATE_LIMIT_SECONDS = 3             # min gap between turns in a room (light guard)
HANDOFF_TAIL_TURNS = 6             # raw turns carried across a rollover
RECAP_DEFAULT_TURNS = 5
RECAP_MAX_TURNS = 20

# Pre-turn idle rollover: a thin, gone-cold context is cheaper to abandon than
# resume. Fires only when a new message arrives after a long gap.
IDLE_ROLLOVER_THRESHOLD_S = 3600   # 1h (cache TTL is ~1h)
IDLE_ROLLOVER_FILL_FRACTION = 0.40 # only roll-on-idle if fill < 40% of budget

# Nightly rollover cutoff (UTC hour). A session last used before today's cutoff
# is rolled on the next inbound message so each "day" starts fresh.
NIGHTLY_CUTOFF_HOUR_UTC = 4

# Idle-harvest (archival): capture an idle session to the memsearch-indexed tier
# and expire it. Independent of inbound messages.
IDLE_HARVEST_THRESHOLD_S = 4200    # 70 min (just past the 1h cache TTL)
IDLE_CHECK_INTERVAL = 300          # 5 min polling

# Retention cleanup: prune rolled/expired sessions (and their turns/aliases).
RETENTION_DAYS = 30
CLEANUP_INTERVAL_SECONDS = 86400

MAX_MESSAGE_CHARS = 32_000         # reject oversized inbound messages

# Cancel-registration retry window (matches matrix-dispatcher).
CANCEL_REGISTRATION_WAIT_SECONDS = 1.0
CANCEL_POLL_INTERVAL_SECONDS = 0.05

# --dangerously-skip-permissions: required for headless `claude -p` so MCP tool
# calls don't dead-end on permission prompts that have no interactive UI. The
# real per-call boundary is the scoped-mcp manifest (tool_allowlist +
# argument_filters); settings.json permissions.allow is the secondary layer.
# Proven pattern across forge's claude -p workloads.
_CLAUDE_FLAGS = ["--dangerously-skip-permissions"]

# ---------------------------------------------------------------------------
# Runtime state (process-local; lost on restart — acceptable).
# ---------------------------------------------------------------------------

_room_locks: dict[str, asyncio.Lock] = {}
_active_processes: dict[str, asyncio.subprocess.Process] = {}
_last_turn_at: dict[str, float] = {}
_handlers: set[asyncio.Task] = set()


def _room_lock(room_id: str) -> asyncio.Lock:
    if room_id not in _room_locks:
        _room_locks[room_id] = asyncio.Lock()
    return _room_locks[room_id]


# ---------------------------------------------------------------------------
# Logging — no message bodies
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("personal-agent")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(os.environ.get(
    "PERSONAL_AGENT_CONFIG",
    str(Path(__file__).parent / "config.yml"),
))
DB_PATH = Path(os.environ.get(
    "PERSONAL_AGENT_DB",
    str(Path.home() / ".claude" / "data" / "personal-agent" / "sessions.db"),
))


def load_config() -> dict:
    with CONFIG_PATH.open() as f:
        return yaml.safe_load(f)


def _atomic_write(path: Path, content: str) -> None:
    """Write to a .tmp (mode 600 from creation) then rename — readers never see
    a partial file, and there is no world-readable window (FW-01)."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(content)
    tmp.replace(path)


# ---------------------------------------------------------------------------
# DB layer — parameterized queries throughout
# ---------------------------------------------------------------------------

def open_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(DB_PATH) + suffix)
        if path.exists():
            os.chmod(path, 0o600)
    return db


def init_db(db: sqlite3.Connection) -> None:
    db.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
          session_id      TEXT PRIMARY KEY,
          room_id         TEXT NOT NULL,
          state           TEXT NOT NULL,      -- 'active' | 'rolled' | 'expired'
          thread_root_id  TEXT,
          prev_session_id TEXT,
          fill_tokens     INTEGER DEFAULT 0,
          turn_count      INTEGER DEFAULT 0,
          created_at      INTEGER NOT NULL,
          last_used_at    INTEGER NOT NULL,
          rolled_reason   TEXT
        );
        -- DB-enforced single active session per room (race guard).
        CREATE UNIQUE INDEX IF NOT EXISTS one_active_per_room
          ON sessions(room_id) WHERE state='active';
        CREATE INDEX IF NOT EXISTS idx_sessions_room ON sessions(room_id, last_used_at);

        -- Self-captured conversation, built from stream-json stdout. This is the
        -- manager's own copy — the agent's JSONL is unreadable (0600, isolated user).
        CREATE TABLE IF NOT EXISTS turns (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          session_id     TEXT NOT NULL,
          room_id        TEXT NOT NULL,
          turn_index     INTEGER NOT NULL,
          user_text      TEXT NOT NULL,
          assistant_text TEXT NOT NULL,
          tools_used     TEXT DEFAULT '',
          fill_tokens    INTEGER DEFAULT 0,
          created_at     INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_turns_session ON turns(session_id, turn_index);

        -- Carried from matrix-dispatcher: reply-to-event resolution + poll cursor.
        CREATE TABLE IF NOT EXISTS event_aliases (
          event_id   TEXT PRIMARY KEY,
          session_id TEXT NOT NULL,
          room_id    TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS poll_state (
          room_id    TEXT PRIMARY KEY,
          since      TEXT NOT NULL,
          updated_at INTEGER NOT NULL
        );
    """)
    db.commit()


# --- poll cursor -----------------------------------------------------------

def get_since(db: sqlite3.Connection) -> str | None:
    row = db.execute(
        "SELECT since FROM poll_state WHERE room_id = ?", ("global",)
    ).fetchone()
    return row["since"] if row else None


def set_since(db: sqlite3.Connection, since: str) -> None:
    db.execute(
        "INSERT OR REPLACE INTO poll_state (room_id, since, updated_at) VALUES (?, ?, ?)",
        ("global", since, int(time.time())),
    )
    db.commit()


# --- sessions --------------------------------------------------------------

def get_active_session(db: sqlite3.Connection, room_id: str) -> sqlite3.Row | None:
    return db.execute(
        "SELECT * FROM sessions WHERE room_id = ? AND state = 'active'", (room_id,)
    ).fetchone()


def get_session(db: sqlite3.Connection, session_id: str) -> sqlite3.Row | None:
    return db.execute(
        "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
    ).fetchone()


def last_rolled_session(db: sqlite3.Connection, room_id: str) -> sqlite3.Row | None:
    """Most recent non-active session in a room — the chain backref for a new one."""
    return db.execute(
        "SELECT * FROM sessions WHERE room_id = ? AND state != 'active' "
        "ORDER BY last_used_at DESC LIMIT 1",
        (room_id,),
    ).fetchone()


def insert_session(
    db: sqlite3.Connection,
    session_id: str,
    room_id: str,
    thread_root_id: str | None,
    prev_session_id: str | None,
) -> None:
    now = int(time.time())
    db.execute(
        """INSERT INTO sessions
           (session_id, room_id, state, thread_root_id, prev_session_id,
            fill_tokens, turn_count, created_at, last_used_at, rolled_reason)
           VALUES (?, ?, 'active', ?, ?, 0, 0, ?, ?, NULL)""",
        (session_id, room_id, thread_root_id, prev_session_id, now, now),
    )
    db.commit()


def update_session_after_turn(
    db: sqlite3.Connection, session_id: str, fill_tokens: int,
) -> None:
    db.execute(
        "UPDATE sessions SET fill_tokens = ?, turn_count = turn_count + 1, "
        "last_used_at = ? WHERE session_id = ?",
        (fill_tokens, int(time.time()), session_id),
    )
    db.commit()


def mark_session(db: sqlite3.Connection, session_id: str, state: str, reason: str) -> None:
    db.execute(
        "UPDATE sessions SET state = ?, rolled_reason = ? WHERE session_id = ?",
        (state, reason, session_id),
    )
    db.commit()


# --- turns -----------------------------------------------------------------

def insert_turn(
    db: sqlite3.Connection,
    session_id: str,
    room_id: str,
    turn_index: int,
    user_text: str,
    assistant_text: str,
    tools_used: str,
    fill_tokens: int,
) -> None:
    db.execute(
        """INSERT INTO turns
           (session_id, room_id, turn_index, user_text, assistant_text,
            tools_used, fill_tokens, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (session_id, room_id, turn_index, user_text, assistant_text,
         tools_used, fill_tokens, int(time.time())),
    )
    db.commit()


def turns_for_session(db: sqlite3.Connection, session_id: str) -> list[sqlite3.Row]:
    return db.execute(
        "SELECT * FROM turns WHERE session_id = ? ORDER BY turn_index ASC",
        (session_id,),
    ).fetchall()


def last_n_turns_across_chain(
    db: sqlite3.Connection, session_id: str, n: int,
) -> list[sqlite3.Row]:
    """Collect the last n turns walking prev_session_id backward across rollovers."""
    collected: list[sqlite3.Row] = []
    sid: str | None = session_id
    seen: set[str] = set()
    while sid and sid not in seen and len(collected) < n:
        seen.add(sid)
        rows = db.execute(
            "SELECT * FROM turns WHERE session_id = ? ORDER BY turn_index DESC",
            (sid,),
        ).fetchall()
        for row in rows:
            collected.append(row)
            if len(collected) >= n:
                break
        sess = get_session(db, sid)
        sid = sess["prev_session_id"] if sess else None
    collected.reverse()  # chronological
    return collected


# --- event aliases ---------------------------------------------------------

def register_alias(db: sqlite3.Connection, event_id: str, session_id: str, room_id: str) -> None:
    if not event_id:
        return
    db.execute(
        "INSERT OR IGNORE INTO event_aliases (event_id, session_id, room_id) VALUES (?, ?, ?)",
        (event_id, session_id, room_id),
    )
    db.commit()


# ---------------------------------------------------------------------------
# Credentials — assert non-empty at startup
# ---------------------------------------------------------------------------

def get_credentials() -> tuple[str, str, str]:
    homeserver = os.environ.get("PERSONAL_AGENT_HOMESERVER", "").strip()
    user_id = os.environ.get("PERSONAL_AGENT_USER_ID", "").strip()
    token = os.environ.get("PERSONAL_AGENT_ACCESS_TOKEN", "").strip()
    missing = [k for k, v in [
        ("PERSONAL_AGENT_HOMESERVER", homeserver),
        ("PERSONAL_AGENT_USER_ID", user_id),
        ("PERSONAL_AGENT_ACCESS_TOKEN", token),
    ] if not v]
    if missing:
        log.error("Missing required env vars at startup: %s", ", ".join(missing))
        sys.exit(1)
    return homeserver, user_id, token


# ---------------------------------------------------------------------------
# Matrix helpers
# ---------------------------------------------------------------------------

async def post_message(
    client: AsyncClient, room_id: str, body: str, reply_to: str | None = None,
) -> str:
    content: dict = {"msgtype": "m.text", "body": body}
    if reply_to:
        content["m.relates_to"] = {"m.in_reply_to": {"event_id": reply_to}}
    resp = await client.room_send(
        room_id=room_id, message_type="m.room.message", content=content,
    )
    return getattr(resp, "event_id", "")


def split_on_paragraphs(text: str, max_len: int) -> list[str]:
    if len(text) <= max_len:
        return [text]
    chunks: list[str] = []
    current = ""
    for para in text.split("\n\n"):
        candidate = (current + "\n\n" + para).lstrip("\n") if current else para
        if len(candidate) <= max_len:
            current = candidate
        else:
            if current:
                chunks.append(current)
            if len(para) > max_len:
                for i in range(0, len(para), max_len):
                    chunks.append(para[i:i + max_len])
                current = ""
            else:
                current = para
    if current:
        chunks.append(current)
    return chunks or [text[:max_len]]


# ---------------------------------------------------------------------------
# Subprocess launch — minimal env; sudo to the isolated agent user
# ---------------------------------------------------------------------------

def _minimal_env() -> dict[str, str]:
    """Minimal, explicit env for the launcher. sudo scrubs most of it anyway;
    the agent's own settings.json supplies CLAUDE_CODE_* / proxy / langfuse vars.
    The bot Matrix token is deliberately NOT included."""
    env = {
        "HOME": os.environ.get("HOME", ""),
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "LANG": os.environ.get("LANG", "en_US.UTF-8"),
        "TERM": os.environ.get("TERM", "xterm"),
        "USER": os.environ.get("USER", "ted"),
    }
    return {k: v for k, v in env.items() if v}


def _launch_args(deploy: dict, *, session_id: str, resume: bool, prompt: str) -> list[str]:
    """Build the argv for a turn. session_id is uuid-validated before this is called."""
    claude_bin = deploy["claude_bin"]
    agent_user = deploy.get("agent_user")
    base: list[str]
    if agent_user:
        # NOPASSWD sudoers grant is scoped to exactly claude_bin as agent_user.
        # --chdir sets CWD as the target user (requires CWD=* in the sudoers rule;
        # sudo ≥1.9.3). This ensures transcripts land in the correct project bucket
        # without requiring ted to have traverse permission on the agent's home.
        base = ["sudo", "-n", "-u", agent_user,
                "--chdir", deploy["project_dir"], "--", claude_bin]
    else:
        base = [claude_bin]
    # Pin the model from config so the rollover math (which assumes a known
    # context window) holds regardless of the host's default model. The design
    # targets Sonnet (200k window); the 130k budget sits below its ~167k
    # auto-compact trigger. Leaving this unset inherits the host default (opus
    # 4.8 [1m], 1M window) for which these numbers do NOT apply.
    model_flag = ["--model", deploy["model"]] if deploy.get("model") else []
    session_flag = ["--resume", session_id] if resume else ["--session-id", session_id]
    return base + ["-p", *_CLAUDE_FLAGS, *model_flag, *session_flag,
                   "--output-format", "stream-json", "--verbose", prompt]


def _validate_session_id(session_id: str) -> None:
    """Reject anything that is not a UUID before it reaches argv."""
    uuid.UUID(session_id)


async def run_claude(
    deploy: dict, room_id: str, *, session_id: str, resume: bool, prompt: str,
    timeout: int,
) -> tuple[int, str]:
    """Run one turn. Returns (exit_code, raw_stdout). Raises asyncio.TimeoutError."""
    _validate_session_id(session_id)
    args = _launch_args(deploy, session_id=session_id, resume=resume, prompt=prompt)
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdin=asyncio.subprocess.DEVNULL,  # prompt is an argv positional; closing
        stdout=asyncio.subprocess.PIPE,    # stdin avoids claude's 3s stdin wait and
        stderr=asyncio.subprocess.PIPE,    # any block when run headless under PM2.
        # For isolated deployments (agent_user set), CWD is handled by
        # sudo --chdir (manager may not have traverse permission on agent's home).
        cwd=None if deploy.get("agent_user") else deploy.get("project_dir"),
        env=_minimal_env(),
    )
    _active_processes[room_id] = proc
    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        try:
            await proc.communicate()
        except Exception:
            pass
        raise
    finally:
        _active_processes.pop(room_id, None)
    rc = proc.returncode if proc.returncode is not None else -1
    stdout = stdout_b.decode(errors="replace")
    if rc != 0:
        stderr = stderr_b.decode(errors="replace").strip()
        # Log stderr to PM2 logs; do NOT surface it to Matrix (L1: OE-02 — paths may appear).
        log.error("action=claude_nonzero room=%s session=%s rc=%d stderr_preview=%r",
                  room_id, session_id, rc, stderr[:200])
        return rc, stdout if stdout.strip() else stderr[:1000]
    return rc, stdout


# ---------------------------------------------------------------------------
# stream-json parsing
# ---------------------------------------------------------------------------

def _blocks_text(content: object) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                t = block.get("text", "")
                if isinstance(t, str):
                    parts.append(t)
        return "\n".join(parts)
    return ""


def parse_stream_json(stdout: str) -> dict:
    """Extract the response text, token fill, and tool names from stream-json.

    Returns {"text": str, "fill": int, "tools": list[str], "is_error": bool}.
    fill = input-side context size of this turn = the current context size,
    because claude re-sends the full history each turn.
    """
    text = ""
    fill = 0
    tools: list[str] = []
    is_error = False
    seen_tools: set[str] = set()
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        otype = obj.get("type")
        if otype == "assistant":
            msg = obj.get("message", {}) or {}
            for block in msg.get("content", []) or []:
                if isinstance(block, dict) and block.get("type") == "tool_use":
                    name = block.get("name")
                    if isinstance(name, str) and name not in seen_tools:
                        seen_tools.add(name)
                        tools.append(name)
        elif otype == "result":
            is_error = bool(obj.get("is_error")) or obj.get("subtype") != "success"
            result_text = obj.get("result")
            if isinstance(result_text, str) and result_text:
                text = result_text
            usage = obj.get("usage") or {}
            fill = (
                int(usage.get("input_tokens", 0) or 0)
                + int(usage.get("cache_read_input_tokens", 0) or 0)
                + int(usage.get("cache_creation_input_tokens", 0) or 0)
            )
    return {"text": text.strip(), "fill": fill, "tools": tools, "is_error": is_error}


# ---------------------------------------------------------------------------
# Typing indicators
# ---------------------------------------------------------------------------

async def _typing_on(client: AsyncClient, room_id: str) -> None:
    try:
        await client.room_typing(room_id, typing_state=True, timeout=30000)
    except Exception as e:
        log.warning("action=typing_error phase=on error_type=%s", type(e).__name__)


async def _typing_off(client: AsyncClient, room_id: str) -> None:
    try:
        await client.room_typing(room_id, typing_state=False)
    except Exception as e:
        log.warning("action=typing_error phase=off error_type=%s", type(e).__name__)


# ---------------------------------------------------------------------------
# Cold-start memsearch injection (carried from v0.4)
# ---------------------------------------------------------------------------

async def query_memsearch(deploy: dict, query: str, max_results: int = 5) -> str:
    """Shell out to the memsearch CLI; return result text or '' on failure.

    Async subprocess (no event-loop block); minimal env so no secret leaks;
    byte-aware truncation. Degrades silently — injection is best-effort."""
    memsearch_bin = deploy.get("memsearch_bin")
    if not memsearch_bin:
        return ""
    try:
        proc = await asyncio.create_subprocess_exec(
            memsearch_bin, "search", query[:200], f"--limit={max_results}",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=_minimal_env(),
        )
        try:
            stdout_b, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            try:
                await proc.communicate()
            except Exception:
                pass
            log.warning("action=memsearch_failed error_type=TimeoutError")
            return ""
        if proc.returncode != 0:
            return ""
        text = stdout_b.decode(errors="replace").strip()
        encoded = text.encode("utf-8", errors="replace")
        if len(encoded) > 8_000:
            text = encoded[:8_000].decode("utf-8", errors="replace") \
                + "\n[…memsearch results truncated…]"
        return text
    except Exception as e:
        log.warning("action=memsearch_failed error_type=%s", type(e).__name__)
        return ""


def _sanitize_injection(text: str) -> str:
    """Drop any line containing the closing delimiter so adversarial memory /
    summary content cannot break out of a `[… ---]` injection block."""
    if not text:
        return text
    return "\n".join(line for line in text.splitlines() if "---]" not in line)


# ---------------------------------------------------------------------------
# Ollama handoff summarizer (with raw-tail fallback)
# ---------------------------------------------------------------------------

OLLAMA_HANDOFF_PROMPT = (
    "Summarize this conversation as a handoff to your future self continuing the "
    "same chat. Capture open threads, pending tasks/delegations, decisions made, "
    "user state, and next steps. Prioritize fast-changing state over stable facts "
    "(stable facts are recoverable from memory search). Be specific and terse. "
    "Plain text, no preamble, under 250 words.\n\nConversation:\n"
)


def _ollama_summarize_sync(deploy: dict, conversation: str) -> str:
    """Blocking Ollama call — run via asyncio.to_thread. Returns '' on any failure."""
    url = deploy.get("ollama_url")
    model = deploy.get("ollama_model")
    if not url or not model:
        return ""
    payload = json.dumps({
        "model": model,
        "prompt": OLLAMA_HANDOFF_PROMPT + conversation,
        "stream": False,
        "options": {"temperature": 0.2},
    }).encode()
    req = urllib.request.Request(
        url.rstrip("/") + "/api/generate", data=payload,
        headers={"Content-Type": "application/json", "X-Queue-Priority": "high"},
    )
    try:
        with urllib.request.urlopen(req, timeout=deploy.get("ollama_timeout", 60)) as resp:
            data = json.loads(resp.read().decode(errors="replace"))
        return (data.get("response") or "").strip()
    except (urllib.error.URLError, OSError, json.JSONDecodeError, ValueError) as e:
        log.warning("action=ollama_summarize_failed error_type=%s", type(e).__name__)
        return ""


async def summarize_handoff(deploy: dict, turns: list[sqlite3.Row]) -> str:
    """Ollama summary of the tail, or '' if disabled/unavailable (caller falls
    back to the raw tail)."""
    if not turns or not deploy.get("ollama_model"):
        return ""
    convo = _render_tail(turns)
    summary = await asyncio.to_thread(_ollama_summarize_sync, deploy, convo)
    return _sanitize_injection(summary)


# ---------------------------------------------------------------------------
# Per-turn datetime tag
# ---------------------------------------------------------------------------

def _fmt_delta(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"+{seconds}s since last"
    mins = seconds // 60
    if mins < 60:
        return f"+{mins}m since last"
    hours = mins // 60
    rem = mins % 60
    if hours < 24:
        return f"+{hours}h{rem:02d}m since last"
    days = hours // 24
    return f"+{days}d{hours % 24}h since last"


def datetime_tag(now_unix: float, last_used_at: int | None, tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    stamp = datetime.fromtimestamp(now_unix, tz).strftime("%Y-%m-%d %H:%M %Z")
    if last_used_at:
        delta = _fmt_delta(now_unix - last_used_at)
    else:
        delta = "first message"
    return f"[time: {stamp} | {delta}]"


# ---------------------------------------------------------------------------
# Rollover + warm-context bridge
# ---------------------------------------------------------------------------

def _render_tail(turns: list[sqlite3.Row]) -> str:
    out = []
    for t in turns:
        out.append(f"**Ted:**\n{t['user_text']}")
        out.append(f"**You (Harlock):**\n{t['assistant_text']}")
    return "\n\n".join(out)


def write_rollover_note(deploy: dict, sess: sqlite3.Row, turns: list[sqlite3.Row]) -> str:
    """Write a durable raw-tail handoff note to the manager's (ted-owned)
    memsearch-indexed working tier. Returns the path (or '' on failure)."""
    note_dir = Path(deploy["working_note_dir"])
    try:
        note_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        log.warning("action=rollover_note_dir_error err=%s", e)
        return ""
    short = sess["session_id"][:8]
    path = note_dir / f"rollover-{short}.md"
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    expires = datetime.fromtimestamp(
        time.time() + 90 * 86400, timezone.utc
    ).strftime("%Y-%m-%d")
    body = (
        f"---\n"
        f"tier: working\n"
        f"created: {today}\n"
        f"source: {deploy['name']}\n"
        f"expires: {expires}\n"
        f"tags: [{deploy['name']}, rollover, handoff]\n"
        f"session_id: {sess['session_id']}\n"
        f"prev_session_id: {sess['prev_session_id'] or ''}\n"
        f"---\n\n"
        f"# Rollover handoff — session {short}\n\n"
        f"Continuous conversation rolled to a fresh session "
        f"(reason: {sess['rolled_reason'] or 'token_budget'}). "
        f"Raw last {len(turns)} turns for warm continuity:\n\n"
        f"{_render_tail(turns)}\n"
    )
    try:
        _atomic_write(path, body)
    except OSError as e:
        log.warning("action=rollover_note_write_error err=%s", e)
        return ""
    return str(path)


def roll_over(db: sqlite3.Connection, deploy: dict, sess: sqlite3.Row, reason: str) -> None:
    tail = last_n_turns_across_chain(db, sess["session_id"], HANDOFF_TAIL_TURNS)
    mark_session(db, sess["session_id"], "rolled", reason)
    note = write_rollover_note(deploy, _row_with(sess, "rolled_reason", reason), tail)
    log.info(
        "action=rollover room=%s session=%s reason=%s tail_turns=%d note=%s",
        sess["room_id"], sess["session_id"], reason, len(tail), bool(note),
    )


def _row_with(row: sqlite3.Row, key: str, value) -> dict:
    d = dict(row)
    d[key] = value
    return d


async def build_warm_prefix(
    db: sqlite3.Connection, prev_session_id: str | None, deploy: dict,
) -> str:
    """Inline handoff injected into a fresh session's first prompt.

    The agent runs as an isolated user, so we cannot write a file into its home
    for a SessionStart hook to pick up — we hand the bridge to it directly.
    Prefer an Ollama summary of the tail; always include the last 2 raw turns
    for immediate continuity; fall back to the full raw tail if Ollama is off
    or unavailable."""
    if not prev_session_id:
        return ""
    tail = last_n_turns_across_chain(db, prev_session_id, HANDOFF_TAIL_TURNS)
    if not tail:
        return ""
    summary = await summarize_handoff(deploy, tail)
    if summary:
        body = (
            f"Summary of the prior context:\n{summary}\n\n"
            f"Most recent turns verbatim:\n{_sanitize_injection(_render_tail(tail[-2:]))}"
        )
    else:
        body = f"Recent turns verbatim:\n{_sanitize_injection(_render_tail(tail))}"
    return (
        "[Continuity handoff — this is a fresh session continuing an ongoing "
        "conversation. The prior context was rolled to stay below the compaction "
        "limit; it was NOT lost. Use memsearch/qmd to recall anything older.\n"
        f"{body}\n---]\n\n[Current message:]\n"
    )


# ---------------------------------------------------------------------------
# Pre-turn roll decision
# ---------------------------------------------------------------------------

def _past_nightly_cutoff(last_used_at: int, now_unix: float) -> bool:
    now_dt = datetime.fromtimestamp(now_unix, timezone.utc)
    cutoff = now_dt.replace(
        hour=NIGHTLY_CUTOFF_HOUR_UTC, minute=0, second=0, microsecond=0
    )
    if now_dt < cutoff:
        # Before today's cutoff — the boundary that matters is yesterday's.
        cutoff -= timedelta(days=1)
    return last_used_at < cutoff.timestamp()


def should_roll_preturn(sess: sqlite3.Row, now_unix: float) -> str | None:
    idle = now_unix - sess["last_used_at"]
    if _past_nightly_cutoff(sess["last_used_at"], now_unix):
        return "nightly"
    if (idle > IDLE_ROLLOVER_THRESHOLD_S
            and sess["fill_tokens"] < ROLLOVER_BUDGET * IDLE_ROLLOVER_FILL_FRACTION):
        return "idle"
    return None


# ---------------------------------------------------------------------------
# Idle-harvest (Phase 7 steps 1 & 3 — step 2 is the scoped systemd janitor)
# ---------------------------------------------------------------------------

def render_session_note(deploy: dict, sess: sqlite3.Row, turns: list[sqlite3.Row]) -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    expires = datetime.fromtimestamp(
        time.time() + 90 * 86400, timezone.utc
    ).strftime("%Y-%m-%d")
    created_iso = datetime.fromtimestamp(sess["created_at"], timezone.utc).isoformat()
    last_iso = datetime.fromtimestamp(sess["last_used_at"], timezone.utc).isoformat()
    all_tools: list[str] = []
    for t in turns:
        for name in (t["tools_used"] or "").split(","):
            name = name.strip()
            if name and name not in all_tools:
                all_tools.append(name)
    first_user = turns[0]["user_text"][:120] if turns else ""
    last_user = turns[-1]["user_text"][:120] if turns else ""
    tail = _render_tail(turns[-4:]) if turns else "(no turns)"
    return (
        f"---\n"
        f"tier: session\n"
        f"created: {today}\n"
        f"source: {deploy['name']}\n"
        f"expires: {expires}\n"
        f"tags: [{deploy['name']}, session, {today}]\n"
        f"session_id: {sess['session_id']}\n"
        f"prev_session_id: {sess['prev_session_id'] or ''}\n"
        f"---\n\n"
        f"## Conversation\n"
        f"- **Date:** {created_iso} → {last_iso}\n"
        f"- **Turns:** {sess['turn_count']}\n"
        f"- **Tools used:** {', '.join(all_tools) or '(none)'}\n"
        f"- **Topics:** {first_user} … {last_user}\n\n"
        f"## Recent context (last 4 turns)\n{tail}\n"
    )


def harvest_session(db: sqlite3.Connection, deploy: dict, sess: sqlite3.Row) -> None:
    turns = turns_for_session(db, sess["session_id"])
    note_dir = Path(deploy["session_note_dir"])
    short = sess["session_id"][:8]
    try:
        note_dir.mkdir(parents=True, exist_ok=True)
        _atomic_write(note_dir / f"{deploy['name']}-session-{short}.md",
                      render_session_note(deploy, sess, turns))
        wrote = True
    except OSError as e:
        log.warning("action=harvest_note_error session=%s err=%s", sess["session_id"], e)
        wrote = False
    mark_session(db, sess["session_id"], "expired", "idle_harvest")
    log.info(
        "action=harvest room=%s session=%s note=%s (jsonl cold-archive handled "
        "by scoped agent-user janitor)",
        sess["room_id"], sess["session_id"], wrote,
    )


async def idle_harvest_loop(db: sqlite3.Connection, deploy: dict) -> None:
    while True:
        await asyncio.sleep(IDLE_CHECK_INTERVAL)
        try:
            now = int(time.time())
            rows = db.execute(
                "SELECT * FROM sessions WHERE state = 'active' "
                "AND ? - last_used_at > ?",
                (now, IDLE_HARVEST_THRESHOLD_S),
            ).fetchall()
            for sess in rows:
                # Don't harvest a session mid-turn.
                if _active_processes.get(sess["room_id"]) is not None:
                    continue
                async with _room_lock(sess["room_id"]):
                    fresh = get_session(db, sess["session_id"])
                    if fresh and fresh["state"] == "active":
                        harvest_session(db, deploy, fresh)
        except Exception:
            log.exception("action=idle_harvest_error")


def run_cleanup(db: sqlite3.Connection, retention_days: int) -> tuple[int, int]:
    """Delete non-active sessions older than retention, plus their turns and
    orphaned aliases. Active sessions are never pruned."""
    cutoff = int(time.time()) - retention_days * 86400
    with db:
        gone = db.execute(
            "SELECT session_id FROM sessions WHERE state != 'active' AND last_used_at < ?",
            (cutoff,),
        ).fetchall()
        ids = [r["session_id"] for r in gone]
        for sid in ids:
            db.execute("DELETE FROM turns WHERE session_id = ?", (sid,))
            db.execute("DELETE FROM event_aliases WHERE session_id = ?", (sid,))
            db.execute("DELETE FROM sessions WHERE session_id = ?", (sid,))
    if ids:
        log.info("action=cleanup sessions_deleted=%d retention_days=%d",
                 len(ids), retention_days)
    return len(ids), 0


async def cleanup_loop(db: sqlite3.Connection, retention_days: int) -> None:
    while True:
        try:
            run_cleanup(db, retention_days)
        except Exception:
            log.exception("action=cleanup_error")
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

HELP_TEXT = (
    "Commands (! prefix — Element intercepts /-commands client-side):\n"
    "  !recap [N]   — last N turns across rollover boundaries (default 5)\n"
    "  !sessions    — recent sessions in this room\n"
    "  !cancel      — SIGTERM the active turn in this room\n"
    "  !mirror      — adopt the most recent unmirrored local session (deployment-dependent)\n"
    "  !help        — this message\n\n"
    "Otherwise just talk — every message continues the one ongoing conversation."
)


def _parse_n(arg: str, default: int, lo: int, hi: int) -> int:
    try:
        n = int(arg.strip())
    except ValueError:
        return default
    return max(lo, min(n, hi))


async def handle_help(client, room_id, event, mention_user) -> None:
    log.info("action=cmd_help room=%s event_id=%s", room_id, event.event_id)
    await post_message(client, room_id, f"{mention_user}\n\n{HELP_TEXT}", reply_to=event.event_id)


async def handle_recap(client, room_id, event, mention_user, db, max_len, arg) -> None:
    n = _parse_n(arg, RECAP_DEFAULT_TURNS, 1, RECAP_MAX_TURNS)
    log.info("action=cmd_recap room=%s event_id=%s n=%d", room_id, event.event_id, n)
    latest = db.execute(
        "SELECT * FROM sessions WHERE room_id = ? ORDER BY last_used_at DESC LIMIT 1",
        (room_id,),
    ).fetchone()
    if latest is None:
        await post_message(client, room_id, f"{mention_user} No prior conversation to recap.",
                           reply_to=event.event_id)
        return
    turns = last_n_turns_across_chain(db, latest["session_id"], n)
    if not turns:
        await post_message(client, room_id, f"{mention_user} No readable turns yet.",
                           reply_to=event.event_id)
        return
    header = f"{mention_user} Recap — last {len(turns)} turns (across rollovers):\n\n"
    for chunk in split_on_paragraphs(header + _render_tail(turns), max_len):
        await post_message(client, room_id, chunk, reply_to=event.event_id)


async def handle_sessions(client, room_id, event, mention_user, db) -> None:
    log.info("action=cmd_sessions room=%s event_id=%s", room_id, event.event_id)
    rows = db.execute(
        "SELECT * FROM sessions WHERE room_id = ? ORDER BY last_used_at DESC LIMIT 10",
        (room_id,),
    ).fetchall()
    if not rows:
        await post_message(client, room_id, f"{mention_user} No sessions yet in this room.",
                           reply_to=event.event_id)
        return
    lines = [f"{mention_user} Recent sessions in this room:"]
    for i, r in enumerate(rows, 1):
        last = datetime.fromtimestamp(r["last_used_at"], timezone.utc).strftime("%Y-%m-%d %H:%M")
        lines.append(
            f"{i}. ({r['session_id'][:8]}) {r['state']} · {r['turn_count']} turns · "
            f"~{r['fill_tokens']//1000}k · {last}Z · {r['rolled_reason'] or '-'}"
        )
    await post_message(client, room_id, "\n".join(lines), reply_to=event.event_id)


async def handle_cancel(client, room_id, event, mention_user) -> None:
    proc = _active_processes.get(room_id)
    if proc is None:
        lock = _room_locks.get(room_id)
        if lock is not None and lock.locked():
            elapsed = 0.0
            while elapsed < CANCEL_REGISTRATION_WAIT_SECONDS:
                await asyncio.sleep(CANCEL_POLL_INTERVAL_SECONDS)
                elapsed += CANCEL_POLL_INTERVAL_SECONDS
                proc = _active_processes.get(room_id)
                if proc is not None:
                    break
    if proc is None:
        log.info("action=cmd_cancel_noop room=%s event_id=%s", room_id, event.event_id)
        await post_message(client, room_id, f"{mention_user} No active turn in this room.",
                           reply_to=event.event_id)
        return
    pid = proc.pid
    log.info("action=cmd_cancel room=%s event_id=%s pid=%s", room_id, event.event_id, pid)
    try:
        proc.send_signal(signal.SIGTERM)
    except ProcessLookupError:
        pass
    await post_message(client, room_id, f"{mention_user} Sent SIGTERM to active turn (pid {pid}).",
                       reply_to=event.event_id)


def find_unmirrored_session_id(db, deploy, room_id) -> str | None:
    """Most recent JSONL in project_dir not already tracked. uuid-validated.

    In an isolated deployment (agent runs as a different user, transcripts 0600)
    the manager cannot read these — this naturally returns None there."""
    project_dir = deploy["project_dir"]
    encoded = project_dir.replace("/", "-")
    jsonl_dir = Path(deploy["agent_home"]) / ".claude" / "projects" / encoded \
        if deploy.get("agent_home") else Path.home() / ".claude" / "projects" / encoded
    if not jsonl_dir.exists():
        return None
    known = {r["session_id"] for r in db.execute(
        "SELECT session_id FROM sessions WHERE room_id = ?", (room_id,)).fetchall()}
    candidates = []
    try:
        for path in jsonl_dir.glob("*.jsonl"):
            if path.stem in known:
                continue
            try:
                uuid.UUID(path.stem)
            except ValueError:
                continue
            candidates.append(path)
    except OSError:
        return None
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime).stem


async def handle_mirror(client, room_id, event, mention_user, db, deploy) -> None:
    log.info("action=cmd_mirror room=%s event_id=%s", room_id, event.event_id)
    session_id = find_unmirrored_session_id(db, deploy, room_id)
    if session_id is None:
        await post_message(
            client, room_id,
            f"{mention_user} No unmirrored local session found "
            f"(expected in this isolated deployment — the agent's transcripts "
            f"aren't manager-readable).",
            reply_to=event.event_id)
        return
    # Adopt it as the room's active session (close any existing active first).
    existing = get_active_session(db, room_id)
    if existing:
        mark_session(db, existing["session_id"], "rolled", "mirror_replaced")
    insert_session(db, session_id, room_id, event.event_id, None)
    await post_message(
        client, room_id,
        f"{mention_user} Adopted session {session_id[:8]} as active. Reply to continue.",
        reply_to=event.event_id)


# ---------------------------------------------------------------------------
# Core message handling — continuous-session flow
# ---------------------------------------------------------------------------

async def handle_event(
    client: AsyncClient,
    room_id: str,
    event: RoomMessageText,
    deploy: dict,
    trusted_sender: str,
    mention_user: str,
    max_message_length: int,
    db: sqlite3.Connection,
    timeout: int,
) -> None:
    if event.sender != trusted_sender:
        return

    user_message = event.body.strip()

    # Commands (room-level; "!" prefix because Element eats "/").
    if user_message.startswith("!"):
        parts = user_message.split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""
        if cmd == "!help":
            await handle_help(client, room_id, event, mention_user)
        elif cmd == "!recap":
            await handle_recap(client, room_id, event, mention_user, db, max_message_length, arg)
        elif cmd == "!sessions":
            await handle_sessions(client, room_id, event, mention_user, db)
        elif cmd == "!cancel":
            await handle_cancel(client, room_id, event, mention_user)
        elif cmd == "!mirror":
            await handle_mirror(client, room_id, event, mention_user, db, deploy)
        else:
            await post_message(client, room_id,
                               f"{mention_user} Unknown command `{cmd}`. Send `!help`.",
                               reply_to=event.event_id)
        return

    if not user_message:
        return
    if len(user_message) > MAX_MESSAGE_CHARS:
        await post_message(
            client, room_id,
            f"{mention_user} Message too long ({len(user_message):,} chars, "
            f"limit {MAX_MESSAGE_CHARS:,}).",
            reply_to=event.event_id)
        return

    async with _room_lock(room_id):
        now = time.time()
        last = _last_turn_at.get(room_id, 0.0)
        if now - last < RATE_LIMIT_SECONDS:
            return  # silent drop of bursty duplicates; continuous chat needs no nag
        _last_turn_at[room_id] = now

        sess = get_active_session(db, room_id)

        # Orphan guard: an active session with zero successful turns means a
        # prior spawn failed before claude created a resumable session on disk.
        # Resuming it would fail forever — retire it and open fresh instead.
        if sess is not None and sess["turn_count"] == 0:
            mark_session(db, sess["session_id"], "expired", "orphan_no_turn")
            log.info("action=orphan_retire room=%s session=%s", room_id, sess["session_id"])
            sess = None

        # Pre-turn roll (idle / nightly).
        if sess is not None:
            reason = should_roll_preturn(sess, now)
            if reason:
                roll_over(db, deploy, sess, reason)
                sess = None

        warm_prefix = ""
        if sess is None:
            prev = last_rolled_session(db, room_id)
            prev_id = prev["session_id"] if prev else None
            warm_prefix = await build_warm_prefix(db, prev_id, deploy)
            # Cold-start memsearch injection only for a truly fresh chain (no
            # warm handoff) — when there IS a handoff, that's the context.
            if not warm_prefix and deploy.get("memsearch_bin"):
                mem = _sanitize_injection(await query_memsearch(deploy, user_message))
                if mem:
                    warm_prefix = f"[Relevant prior context:\n{mem}\n---]\n\n"
                    log.info("action=memsearch_inject room=%s bytes=%d", room_id, len(mem))
            session_id = str(uuid.uuid4())
            insert_session(db, session_id, room_id, event.event_id, prev_id)
            resume = False
            last_used = prev["last_used_at"] if prev else None
            log.info("action=open_session room=%s session=%s prev=%s",
                     room_id, session_id, prev_id)
        else:
            session_id = sess["session_id"]
            resume = True
            last_used = sess["last_used_at"]
            log.info("action=resume room=%s session=%s", room_id, session_id)

        tag = datetime_tag(now, last_used, deploy.get("timezone", "America/New_York"))
        prompt = f"{warm_prefix}{tag}\n{user_message}"

        ack = await post_message(client, room_id, f"… ({session_id[:8]})",
                                 reply_to=event.event_id)
        register_alias(db, ack, session_id, room_id)

        await _typing_on(client, room_id)
        try:
            rc, stdout = await run_claude(
                deploy, room_id, session_id=session_id, resume=resume,
                prompt=prompt, timeout=timeout,
            )
        except asyncio.TimeoutError:
            log.error("action=turn_timeout room=%s session=%s", room_id, session_id)
            await post_message(client, room_id,
                               f"{mention_user} Turn timed out after {timeout}s.",
                               reply_to=ack or event.event_id)
            return
        finally:
            await _typing_off(client, room_id)

        parsed = parse_stream_json(stdout)
        response = parsed["text"] or "(no output)"
        if rc != 0 or parsed["is_error"]:
            # Keep verbose detail in PM2 logs only — not in Matrix (L1: OE-02).
            log.error("action=turn_error room=%s session=%s rc=%d response_preview=%r",
                      room_id, session_id, rc, response[:200])
            response = f"turn error (rc={rc}) — check manager logs"

        # Persist the turn (self-captured store) + advance session fill.
        current = get_session(db, session_id)
        turn_index = current["turn_count"] if current else 0
        insert_turn(db, session_id, room_id, turn_index,
                    user_message, parsed["text"], ",".join(parsed["tools"]),
                    parsed["fill"])
        update_session_after_turn(db, session_id, parsed["fill"])
        log.info("action=turn_complete room=%s session=%s rc=%d fill=%d tools=%d",
                 room_id, session_id, rc, parsed["fill"], len(parsed["tools"]))

        for i, chunk in enumerate(split_on_paragraphs(response, max_message_length)):
            text = f"{mention_user} {chunk}" if i == 0 else chunk
            ev = await post_message(client, room_id, text, reply_to=ack or event.event_id)
            register_alias(db, ev, session_id, room_id)

        # Post-turn roll (token budget — the primary trigger).
        if parsed["fill"] >= ROLLOVER_BUDGET:
            fresh = get_session(db, session_id)
            if fresh and fresh["state"] == "active":
                roll_over(db, deploy, fresh, "token_budget")


# ---------------------------------------------------------------------------
# Polling loop
# ---------------------------------------------------------------------------

async def poll_loop(client: AsyncClient, config: dict, db: sqlite3.Connection) -> None:
    trusted_sender = config.get("trusted_sender", "")
    mention_user = config.get("mention_user", "")
    poll_interval = config.get("poll_interval_seconds", 5)
    max_message_length = config.get("max_message_length", 4000)
    timeout = config.get("subprocess_timeout_seconds", SUBPROCESS_TIMEOUT_SECONDS)

    deploy = config["deployment"]
    room_id = deploy["room_id"]

    since = get_since(db)
    if since is None:
        seed = await client.sync(timeout=0, since=None, full_state=False)
        if isinstance(seed, SyncResponse):
            since = seed.next_batch
            set_since(db, since)
            log.info("action=poll_seed since=%s", since)
        else:
            log.warning("action=poll_seed_error response=%s", type(seed).__name__)

    log.info("action=poll_start room=%s since=%s", room_id, since)

    while True:
        try:
            resp = await client.sync(timeout=0, since=since, full_state=False)
            if not isinstance(resp, SyncResponse):
                log.warning("action=sync_error response=%s", type(resp).__name__)
                await asyncio.sleep(poll_interval)
                continue
            since = resp.next_batch
            set_since(db, since)
            for rid, room_info in resp.rooms.join.items():
                if rid != room_id:
                    continue
                for event in room_info.timeline.events:
                    if not isinstance(event, RoomMessageText):
                        continue
                    task = asyncio.create_task(handle_event(
                        client=client, room_id=room_id, event=event, deploy=deploy,
                        trusted_sender=trusted_sender, mention_user=mention_user,
                        max_message_length=max_message_length, db=db, timeout=timeout,
                    ))
                    _handlers.add(task)
                    task.add_done_callback(_handlers.discard)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("action=poll_error")
        await asyncio.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    homeserver, user_id, token = get_credentials()
    config = load_config()
    deploy = config["deployment"]

    # Config-driven rollover budget (default tuned for the design model, Sonnet).
    global ROLLOVER_BUDGET
    ROLLOVER_BUDGET = int(config.get("rollover_budget", ROLLOVER_BUDGET))
    log.info("action=config model=%s rollover_budget=%d",
             deploy.get("model") or "(host default)", ROLLOVER_BUDGET)

    db = open_db()
    init_db(db)

    client = AsyncClient(homeserver, user_id)
    client.access_token = token
    client.user_id = user_id

    log.info("action=startup user_id=%s homeserver=%s name=%s",
             user_id, homeserver, deploy.get("name"))

    if config.get("startup_notification", True):
        try:
            await post_message(client, deploy["room_id"],
                               f"{deploy.get('name','agent')} manager online.")
        except Exception:
            log.exception("action=startup_notify_error")

    retention_days = int(config.get("session_retention_days", RETENTION_DAYS))
    harvest_task = asyncio.create_task(idle_harvest_loop(db, deploy))
    cleanup_task = asyncio.create_task(cleanup_loop(db, retention_days))
    try:
        await poll_loop(client, config, db)
    finally:
        for t in (harvest_task, cleanup_task):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        for rid, proc in list(_active_processes.items()):
            try:
                proc.send_signal(signal.SIGTERM)
                log.info("action=shutdown_sigterm room=%s pid=%s", rid, proc.pid)
            except ProcessLookupError:
                pass
        if _handlers:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*_handlers, return_exceptions=True), timeout=10)
            except asyncio.TimeoutError:
                log.warning("action=shutdown_handlers_timeout outstanding=%d", len(_handlers))
        db.close()
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())

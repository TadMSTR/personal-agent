"""personal-agent manager — Matrix poll loop + claude-code session lifecycle.

v0.1 scope:
  - Polls #personal:claudebox.me for messages from the operator only
  - Spawns claude -p with --session-id on room-root messages
  - Resumes on thread replies (matched via event_aliases)
  - Timestamp-prefixes every user message before passing to claude
  - Structured logs (no message body content)
  - Posts response chunks back to Matrix with @ted mention on first chunk

v0.2 scope (added):
  - Background idle monitor — sessions idle past idle_threshold_seconds are
    rolled over: handoff summary generated via the live session, written to
    handoffs/<session_id>.md, old session marked retired, new session_id
    allocated against the same thread_root.
  - Continuity injection — first user message after rollover is spawned (not
    resumed) with the handoff text and last-10 transcript turns prepended.
  - Multiple sessions per Matrix thread (status='active' filter); thread_root_id
    no longer UNIQUE in sessions; event_aliases FK dropped (FK requires UNIQUE).

v0.3 scope (added):
  - Three OR'd rollover triggers: dynamic idle threshold (scales with token fill),
    direct token-budget trigger at >=80% fill, 24-hour age cap.
  - token_fill_pct updated in sessions table after each claude response.
  - idle_monitor now calls should_rollover() per session instead of a fixed cutoff.

v0.4 scope (added):
  - Typing indicators: client.room_typing before/after every claude invocation.
  - Cold-start memsearch: first message of a brand-new session queries memsearch
    for relevant prior context and prepends top results to the spawn message.

Future phases extend this file:
  v0.5 — task-queue delegation + agent-bus event subscription
  v0.6 — Gitea-backed self-modification with locked-section validation
  v0.7 — sleep-window scripts (separate PM2 entries)

Hardening (mirrored from matrix-dispatcher):
  - Subprocess env is a minimal allowlist; CLAUDE_API_KEY etc. never leak in
  - SQLite uses parameterized queries throughout (no f-string SQL)
  - Logs never carry message body, tool args, or Claude output
  - requirements.txt pins exact versions
  - Credentials loaded from env file (sourced by start.sh); asserted at startup
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from nio import AsyncClient, RoomMessageText, SyncResponse

# ---------------------------------------------------------------------------
# Runtime state
# ---------------------------------------------------------------------------

_room_lock: asyncio.Lock | None = None  # set lazily in main()
_active_processes: dict[str, asyncio.subprocess.Process] = {}
_last_spawn_at: float = 0.0
_last_resume_at: float = 0.0
_last_cleanup_at: float = 0.0
_handlers: set[asyncio.Task] = set()

DEFAULT_SUBPROCESS_TIMEOUT = 1800
DEFAULT_CONTEXT_WINDOW_TOKENS = 200_000
RATE_LIMIT_SECONDS = 5
RESUME_RATE_LIMIT_SECONDS = 3
CLEANUP_INTERVAL_SECONDS = 3600

# ---------------------------------------------------------------------------
# Logging — structured, no message body content
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("personal-agent")

# ---------------------------------------------------------------------------
# Paths and config
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).parent / "config.yml"
DATA_DIR = Path.home() / ".claude" / "data" / "personal-agent"
DB_PATH = DATA_DIR / "sessions.db"
HANDOFFS_DIR = DATA_DIR / "handoffs"


def load_config() -> dict:
    with CONFIG_PATH.open() as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# SQLite layer — parameterized queries throughout
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


def _sessions_has_unique_thread_root(db: sqlite3.Connection) -> bool:
    row = db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='sessions'"
    ).fetchone()
    return bool(row) and "UNIQUE" in (row["sql"] or "")


def init_db(db: sqlite3.Connection) -> None:
    # Fresh-install path. CREATE IF NOT EXISTS preserves any prior schema —
    # the migration block below handles the v0.1 → v0.2 reshape.
    db.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
          session_id          TEXT PRIMARY KEY,
          thread_root_id      TEXT NOT NULL,
          room_id             TEXT NOT NULL,
          created_at          INTEGER NOT NULL,
          last_message_at     INTEGER NOT NULL,
          token_fill_pct      REAL DEFAULT 0.0,
          status              TEXT DEFAULT 'active',
          previous_session_id TEXT,
          handoff_injected    INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_last_msg ON sessions(last_message_at);
        CREATE INDEX IF NOT EXISTS idx_sessions_thread_root ON sessions(thread_root_id);
        CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);

        -- Maps any manager-posted event_id (ack, response chunks) back to a
        -- thread_root, so replies to those events resolve to the right active
        -- session. v0.2: FK removed because thread_root_id is no longer UNIQUE
        -- (multiple sessions per thread: at most one active + N retired).
        CREATE TABLE IF NOT EXISTS event_aliases (
          event_id       TEXT PRIMARY KEY,
          thread_root_id TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_event_aliases_thread_root
          ON event_aliases(thread_root_id);

        CREATE TABLE IF NOT EXISTS poll_state (
          id         INTEGER PRIMARY KEY CHECK (id = 1),
          since      TEXT NOT NULL,
          updated_at INTEGER NOT NULL
        );
    """)

    # v0.1 → v0.2 migration: drop UNIQUE on sessions.thread_root_id and the
    # event_aliases FK that depended on it. Recreates both tables in place.
    # Wrapped in explicit BEGIN/COMMIT so a process kill during migration
    # rolls back to the v0.1 schema rather than leaving sessions dropped
    # before the rename completes.
    if _sessions_has_unique_thread_root(db):
        log.info("action=schema_migration from=v0.1 to=v0.2")
        db.executescript("""
            BEGIN;
            DROP TABLE IF EXISTS sessions_new;
            DROP TABLE IF EXISTS event_aliases_new;

            CREATE TABLE sessions_new (
              session_id          TEXT PRIMARY KEY,
              thread_root_id      TEXT NOT NULL,
              room_id             TEXT NOT NULL,
              created_at          INTEGER NOT NULL,
              last_message_at     INTEGER NOT NULL,
              token_fill_pct      REAL DEFAULT 0.0,
              status              TEXT DEFAULT 'active',
              previous_session_id TEXT,
              handoff_injected    INTEGER DEFAULT 0
            );
            INSERT INTO sessions_new SELECT * FROM sessions;
            DROP TABLE sessions;
            ALTER TABLE sessions_new RENAME TO sessions;
            CREATE INDEX idx_sessions_last_msg ON sessions(last_message_at);
            CREATE INDEX idx_sessions_thread_root ON sessions(thread_root_id);
            CREATE INDEX idx_sessions_status ON sessions(status);

            CREATE TABLE event_aliases_new (
              event_id       TEXT PRIMARY KEY,
              thread_root_id TEXT NOT NULL
            );
            INSERT INTO event_aliases_new SELECT event_id, thread_root_id FROM event_aliases;
            DROP TABLE event_aliases;
            ALTER TABLE event_aliases_new RENAME TO event_aliases;
            CREATE INDEX idx_event_aliases_thread_root ON event_aliases(thread_root_id);
            COMMIT;
        """)

    db.commit()


def get_since(db: sqlite3.Connection) -> str | None:
    row = db.execute("SELECT since FROM poll_state WHERE id = 1").fetchone()
    return row["since"] if row else None


def set_since(db: sqlite3.Connection, since: str) -> None:
    db.execute(
        "INSERT OR REPLACE INTO poll_state (id, since, updated_at) VALUES (1, ?, ?)",
        (since, int(time.time())),
    )
    db.commit()


def insert_session(
    db: sqlite3.Connection,
    session_id: str,
    thread_root_id: str,
    room_id: str,
) -> None:
    now = int(time.time())
    db.execute(
        """INSERT INTO sessions
           (session_id, thread_root_id, room_id, created_at, last_message_at)
           VALUES (?, ?, ?, ?, ?)""",
        (session_id, thread_root_id, room_id, now, now),
    )
    db.commit()


def touch_session(db: sqlite3.Connection, session_id: str) -> None:
    db.execute(
        "UPDATE sessions SET last_message_at = ? WHERE session_id = ?",
        (int(time.time()), session_id),
    )
    db.commit()


def get_session_by_event(db: sqlite3.Connection, event_id: str) -> sqlite3.Row | None:
    """Resolve any event_id (thread root, ack, or response chunk) to the active
    session for that thread. Retired rows are ignored."""
    row = db.execute(
        "SELECT * FROM sessions WHERE thread_root_id = ? AND status = 'active'",
        (event_id,),
    ).fetchone()
    if row:
        return row
    alias = db.execute(
        "SELECT thread_root_id FROM event_aliases WHERE event_id = ?", (event_id,)
    ).fetchone()
    if alias:
        return db.execute(
            "SELECT * FROM sessions "
            "WHERE thread_root_id = ? AND status = 'active'",
            (alias["thread_root_id"],),
        ).fetchone()
    return None


def get_latest_active_session(db: sqlite3.Connection) -> sqlite3.Row | None:
    """Return the most recently active session, or None if no active session exists."""
    return db.execute(
        "SELECT * FROM sessions WHERE status = 'active' "
        "ORDER BY last_message_at DESC LIMIT 1"
    ).fetchone()


def register_alias(
    db: sqlite3.Connection, event_id: str, thread_root_id: str
) -> None:
    if not event_id:
        return
    db.execute(
        "INSERT OR IGNORE INTO event_aliases (event_id, thread_root_id) VALUES (?, ?)",
        (event_id, thread_root_id),
    )
    db.commit()


def cleanup_old_sessions(db: sqlite3.Connection, retention_days: int) -> None:
    cutoff = int(time.time()) - (retention_days * 86400)
    with db:
        deleted = db.execute(
            "DELETE FROM sessions WHERE last_message_at < ?", (cutoff,)
        ).rowcount
        # Orphan aliases: with FK gone, prune aliases whose thread_root_id
        # has no surviving session row.
        orphans = db.execute(
            "DELETE FROM event_aliases WHERE thread_root_id NOT IN "
            "(SELECT thread_root_id FROM sessions)"
        ).rowcount
    if deleted or orphans:
        log.info(
            "action=session_cleanup deleted=%d alias_orphans=%d retention_days=%d",
            deleted, orphans, retention_days,
        )


# ---------------------------------------------------------------------------
# Credentials — assert at startup
# ---------------------------------------------------------------------------

def get_credentials() -> tuple[str, str, str, str]:
    homeserver = os.environ.get("PERSONAL_HOMESERVER", "").strip()
    user_id = os.environ.get("PERSONAL_USER_ID", "").strip()
    token = os.environ.get("PERSONAL_ACCESS_TOKEN", "").strip()
    room_id = os.environ.get("PERSONAL_ROOM_ID", "").strip()

    missing = [k for k, v in [
        ("PERSONAL_HOMESERVER", homeserver),
        ("PERSONAL_USER_ID", user_id),
        ("PERSONAL_ACCESS_TOKEN", token),
        ("PERSONAL_ROOM_ID", room_id),
    ] if not v]

    if missing:
        log.error("action=startup_error missing_var=%s", ",".join(missing))
        sys.exit(1)

    return homeserver, user_id, token, room_id


# ---------------------------------------------------------------------------
# Matrix helpers
# ---------------------------------------------------------------------------

async def post_message(
    client: AsyncClient,
    room_id: str,
    body: str,
    reply_to: str | None = None,
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
    paragraphs = text.split("\n\n")
    current = ""
    for para in paragraphs:
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
# Timestamp injection — every user message is stamped before reaching claude
# ---------------------------------------------------------------------------

def stamp(message: str) -> str:
    """Prepend the current local time so the agent always knows when it is."""
    now = datetime.now().astimezone()
    return f"[{now.strftime('%A, %Y-%m-%d %H:%M %Z')}]\n\n{message}"


def query_memsearch(query: str, max_results: int = 5) -> str:
    """Shell out to memsearch and return result text, or empty string on failure."""
    try:
        result = subprocess.run(
            ["memsearch", "search", query[:200], f"--limit={max_results}"],
            capture_output=True, text=True, timeout=15,
        )
        text = result.stdout.strip() if result.returncode == 0 else ""
        # Cap at ~2000 tokens (≈8000 chars) to avoid bloating the spawn message
        if len(text.encode()) > 8_000:
            text = text[:8_000] + "\n[…memsearch results truncated…]"
        return text
    except Exception as e:
        log.warning("action=memsearch_failed error_type=%s", type(e).__name__)
        return ""


# ---------------------------------------------------------------------------
# Subprocess — minimal env allowlist
# ---------------------------------------------------------------------------

def _minimal_env() -> dict[str, str]:
    env = {
        "HOME": os.environ["HOME"],
        "PATH": os.environ["PATH"],
        "AGENT_ID": "personal",
        "AGENT_TYPE": "personal-agent",
        "LANG": os.environ.get("LANG", "en_US.UTF-8"),
        "TERM": os.environ.get("TERM", "xterm"),
        "USER": os.environ.get("USER", "ted"),
    }
    # Explicit allowlist — never glob CLAUDE_*, which would leak any
    # CLAUDE_API_KEY / CLAUDE_CODE_OAUTH_TOKEN into agent subprocesses.
    for key in ("CLAUDE_CONFIG_DIR",):
        if key in os.environ:
            env[key] = os.environ[key]
    return env


async def _run_claude(
    args: list[str],
    project_dir: str,
    timeout: int,
    proc_key: str,
) -> tuple[int, str]:
    """Run claude with the given args; register PID for /cancel-style intervention."""
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=project_dir,
        env=_minimal_env(),
    )
    _active_processes[proc_key] = proc
    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            proc.communicate(), timeout=timeout,
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        try:
            await proc.communicate()
        except Exception:
            pass
        raise
    finally:
        _active_processes.pop(proc_key, None)
    rc = proc.returncode if proc.returncode is not None else -1
    stdout = stdout_b.decode(errors="replace").strip()
    stderr = stderr_b.decode(errors="replace").strip()
    if rc == 0:
        output = stdout
    else:
        log.error("action=claude_error rc=%d stderr=%s", rc, stderr)
        output = f"[Error: claude exited with code {rc}]"
    return rc, output


# --dangerously-skip-permissions: required for headless claude -p so MCP tool
# calls don't dead-end on prompts that have no interactive UI to answer them.
# This is the proven pattern used by memory-sync-weekly.sh, memory-promote-daily.sh,
# librarian.sh, and other production claude -p workloads on this host.
# permissions.allow in settings.json is the secondary scoping layer; the actual
# per-call boundary is the scoped-mcp manifest (tool_allowlist + argument_filters).
_CLAUDE_FLAGS = ["--dangerously-skip-permissions"]


async def spawn_personal(
    session_id: str, message: str, project_dir: str, timeout: int,
) -> tuple[int, str]:
    return await _run_claude(
        ["claude", "-p", *_CLAUDE_FLAGS,
         "--session-id", session_id, message],
        project_dir, timeout, proc_key=session_id,
    )


async def resume_personal(
    session_id: str, message: str, project_dir: str, timeout: int,
) -> tuple[int, str]:
    return await _run_claude(
        ["claude", "-p", *_CLAUDE_FLAGS,
         "--resume", session_id, message],
        project_dir, timeout, proc_key=session_id,
    )


async def _typing_on(client: AsyncClient, room_id: str) -> None:
    try:
        await client.room_typing(room_id, typing=True, timeout=30000)
    except Exception as e:
        log.warning("action=typing_error phase=on error_type=%s", type(e).__name__)


async def _typing_off(client: AsyncClient, room_id: str) -> None:
    try:
        await client.room_typing(room_id, typing=False)
    except Exception as e:
        log.warning("action=typing_error phase=off error_type=%s", type(e).__name__)


# ---------------------------------------------------------------------------
# v0.2 — transcript reader + rollover
# ---------------------------------------------------------------------------

def transcripts_dir(project_dir: str) -> Path:
    """Map a project_dir to its Claude Code transcript directory.
    Path is encoded by replacing every non-alphanumeric / non-hyphen char with
    a hyphen — the same encoding Claude Code itself uses."""
    encoded = re.sub(r"[^a-zA-Z0-9-]", "-", project_dir)
    return Path.home() / ".claude" / "projects" / encoded


def read_last_n_turns(
    session_id: str, project_dir: str, n: int = 10, max_bytes: int = 32_000
) -> str:
    """Return last N user/assistant turns from a session's JSONL transcript
    as plain text, or empty string if the transcript is missing or unparsable.
    Truncates to max_bytes to prevent ARG_MAX overflow in continuity injection."""
    transcript = transcripts_dir(project_dir) / f"{session_id}.jsonl"
    if not transcript.exists():
        return ""
    turns: list[str] = []
    try:
        for line in transcript.read_text(errors="replace").splitlines():
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            msg_type = entry.get("type")
            message = entry.get("message") or {}
            content = message.get("content")
            if msg_type == "user" and isinstance(content, str):
                turns.append(f"[user] {content}")
            elif msg_type == "assistant" and isinstance(content, list):
                texts = [
                    c.get("text", "")
                    for c in content
                    if isinstance(c, dict) and c.get("type") == "text"
                ]
                joined = " ".join(t for t in texts if t).strip()
                if joined:
                    turns.append(f"[assistant] {joined}")
    except OSError as exc:
        log.warning("action=transcript_read_error session=%s err=%s",
                    session_id[:8], exc)
        return ""
    result = "\n\n".join(turns[-n:])
    if len(result.encode()) > max_bytes:
        result = "[…truncated…]\n\n" + result[-max_bytes:]
    return result


def get_token_fill_pct(
    session_id: str,
    project_dir: str,
    context_window: int = DEFAULT_CONTEXT_WINDOW_TOKENS,
) -> float:
    """Estimate context fill from the latest transcript entry's usage.
    Returns 0.0–1.0.

    Claude Code uses prompt caching, so per-turn `input_tokens` reflects only new
    content; the bulk of context lives in `cache_read_input_tokens` and
    `cache_creation_input_tokens`. The true fill at any moment is the sum of
    those three on the latest entry that carries usage. Reverse-scan the file
    and break on the first such entry."""
    transcript = transcripts_dir(project_dir) / f"{session_id}.jsonl"
    if not transcript.exists():
        return 0.0
    try:
        lines = transcript.read_text(errors="replace").splitlines()
    except OSError:
        return 0.0
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        usage = (obj.get("message") or {}).get("usage") or {}
        if not usage:
            continue
        total = (
            usage.get("input_tokens", 0)
            + usage.get("cache_creation_input_tokens", 0)
            + usage.get("cache_read_input_tokens", 0)
        )
        if total <= 0:
            continue
        return min(total / context_window, 1.0)
    return 0.0


def idle_threshold_for_fill(token_fill_pct: float) -> int:
    """Dynamic idle threshold — shorter window as context fills up."""
    if token_fill_pct >= 0.80:
        return 360    # 6 min — near-full, roll over quickly
    if token_fill_pct >= 0.60:
        return 1200   # 20 min
    return 1800       # 30 min default


IDLE_MIN_FILL = 0.40
DEFAULT_NIGHTLY_HOUR = 4


def last_nightly_cutoff(now_ts: float, nightly_hour: int) -> float:
    """Most recent occurrence of `nightly_hour:00:00` in local time, as a
    Unix timestamp. If `now` is before today's cutoff, returns yesterday's."""
    now_local = datetime.fromtimestamp(now_ts)
    today = now_local.replace(
        hour=nightly_hour, minute=0, second=0, microsecond=0,
    )
    if now_local < today:
        today -= timedelta(days=1)
    return today.timestamp()


def should_rollover(
    row: sqlite3.Row,
    now: float,
    nightly_hour: int = DEFAULT_NIGHTLY_HOUR,
) -> tuple[bool, str, int]:
    """Return (should_rollover, trigger_name, idle_threshold_used).

    Three OR'd triggers:
      token_budget    — fill >= 80% regardless of idle time
      nightly_cutoff  — session was created before the most recent local
                        nightly_hour boundary (default 4 AM)
      idle            — idle time exceeds dynamic threshold based on fill,
                        and fill is high enough to warrant a handoff
    """
    fill = row["token_fill_pct"] or 0.0
    threshold = idle_threshold_for_fill(fill)
    idle_secs = now - row["last_message_at"]

    if fill >= 0.80:
        return True, "token_budget", threshold
    if row["created_at"] < last_nightly_cutoff(now, nightly_hour):
        return True, "nightly_cutoff", threshold
    # Idle floor: skip rollover for sessions that haven't accumulated meaningful
    # context yet. Below the floor the session would generate a thin handoff
    # for very little real content, and the nightly cutoff sweeps these anyway.
    if idle_secs > threshold and fill >= IDLE_MIN_FILL:
        return True, "idle", threshold
    return False, "", threshold


def update_token_fill(
    db: sqlite3.Connection,
    session_id: str,
    project_dir: str,
    context_window: int = DEFAULT_CONTEXT_WINDOW_TOKENS,
) -> float:
    """Read transcript, update token_fill_pct in sessions table, return new value."""
    fill = get_token_fill_pct(session_id, project_dir, context_window)
    with db:
        db.execute(
            "UPDATE sessions SET token_fill_pct=? WHERE session_id=?",
            (fill, session_id),
        )
    if fill >= 0.70:
        log.info("action=token_fill_updated session_id=%s fill_pct=%.3f",
                 session_id[:8], fill)
    return fill


HANDOFF_TIMEOUT_SECONDS = 300  # 5 min ceiling for ~400-word summary

HANDOFF_PROMPT = (
    "Summarize this conversation as a handoff to your future self. "
    "Include: open threads, user state, decisions made, next steps. "
    "Prioritize fast-changing state (open tasks, pending delegations, "
    "unresolved questions, recent decisions) over stable facts — stable "
    "facts are already in memsearch and don't need regenerating. "
    "Be specific. ~400 words. Plain text only."
)


async def trigger_rollover(
    db: sqlite3.Connection,
    session_id: str,
    thread_root_id: str,
    room_id: str,
    project_dir: str,
    subprocess_timeout: int,
    threshold: int,
    trigger: str = "idle",
    context_window: int = DEFAULT_CONTEXT_WINDOW_TOKENS,
    nightly_hour: int = DEFAULT_NIGHTLY_HOUR,
) -> str | None:
    """Generate handoff, retire old session, allocate new session_id (do NOT
    spawn). Returns new_session_id on success, None on failure (old session
    stays active so the next user message resumes normally).

    Acquires _room_lock for the duration of the resume_personal call so a
    concurrent user message handler can't run a second `claude -p --resume`
    against the same session_id and corrupt the shared JSONL transcript."""
    assert _room_lock is not None
    short_id = session_id[:8]
    log.info("action=rollover_start session_id=%s trigger=%s", short_id, trigger)
    async with _room_lock:
        # Recheck the trigger-specific condition under the lock. A user message
        # may have arrived between snapshot and lock-acquire, but for
        # token_budget and age_cap that doesn't disqualify the rollover — only
        # the idle trigger cares about freshness.
        fresh = db.execute(
            "SELECT last_message_at, created_at, status FROM sessions "
            "WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if not fresh or fresh["status"] != "active":
            log.info("action=rollover_skip_stale session_id=%s reason=inactive",
                     short_id)
            return None
        now_int = int(time.time())
        if trigger == "idle":
            if fresh["last_message_at"] >= now_int - threshold:
                log.info("action=rollover_skip_stale session_id=%s reason=idle_reset",
                         short_id)
                return None
        elif trigger == "token_budget":
            current_fill = get_token_fill_pct(session_id, project_dir, context_window)
            if current_fill < 0.80:
                log.info(
                    "action=rollover_skip_stale session_id=%s reason=fill_drop fill=%.3f",
                    short_id, current_fill,
                )
                return None
        elif trigger == "nightly_cutoff":
            if fresh["created_at"] >= last_nightly_cutoff(now_int, nightly_hour):
                log.info(
                    "action=rollover_skip_stale session_id=%s reason=post_cutoff",
                    short_id,
                )
                return None

        try:
            rc, handoff_text = await resume_personal(
                session_id, HANDOFF_PROMPT, project_dir, HANDOFF_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            log.error("action=rollover_handoff_timeout session_id=%s", short_id)
            return None
        if rc != 0:
            log.error("action=rollover_handoff_error session_id=%s rc=%d",
                      short_id, rc)
            return None

        last_10 = read_last_n_turns(session_id, project_dir, n=10)

        HANDOFFS_DIR.mkdir(parents=True, exist_ok=True)
        handoff_path = HANDOFFS_DIR / f"{session_id}.md"
        # Atomic write: stage to .tmp with mode 0o600 from creation (no
        # permissions window), then rename. Guarantees readers see either the
        # prior content or the complete new content, never a partial file.
        tmp_path = handoff_path.with_suffix(".md.tmp")
        body = (
            f"# Handoff from session {short_id}\n\n"
            f"{handoff_text}\n\n"
            f"## Last 10 messages\n\n{last_10}\n"
        )
        fd = os.open(str(tmp_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(body)
        tmp_path.replace(handoff_path)

        new_id = str(uuid.uuid4())
        now_ts = int(time.time())
        with db:
            db.execute(
                "UPDATE sessions SET status='retired' WHERE session_id=?",
                (session_id,),
            )
            db.execute(
                """INSERT INTO sessions
                     (session_id, thread_root_id, room_id, created_at,
                      last_message_at, status, previous_session_id,
                      handoff_injected)
                   VALUES (?, ?, ?, ?, ?, 'active', ?, 0)""",
                (new_id, thread_root_id, room_id, now_ts, now_ts, session_id),
            )
    log.info(
        "action=rollover_complete old=%s new=%s handoff_bytes=%d",
        short_id, new_id[:8], len(body),
    )
    return new_id


async def idle_monitor(
    db: sqlite3.Connection,
    config: dict,
) -> None:
    """Background task — every idle_check_interval_seconds, evaluate all active
    sessions against three OR'd triggers (token_budget, age_cap, idle) and roll
    over any that qualify."""
    interval = int(config.get("idle_check_interval_seconds", 60))
    project_dir = config["project_dir"]
    subprocess_timeout = int(config.get(
        "subprocess_timeout_seconds", DEFAULT_SUBPROCESS_TIMEOUT
    ))
    context_window = int(config.get(
        "context_window_tokens", DEFAULT_CONTEXT_WINDOW_TOKENS
    ))
    nightly_hour = int(config.get(
        "nightly_rollover_hour", DEFAULT_NIGHTLY_HOUR
    ))
    log.info(
        "action=idle_monitor_start interval_s=%d mode=dynamic_triggers nightly_hour=%d",
        interval, nightly_hour,
    )
    while True:
        try:
            now = time.time()
            rows = db.execute(
                "SELECT session_id, thread_root_id, room_id, "
                "last_message_at, created_at, token_fill_pct "
                "FROM sessions WHERE status='active' "
                "ORDER BY last_message_at ASC",
            ).fetchall()
            for row in rows:
                do_rollover, trigger, threshold = should_rollover(
                    row, now, nightly_hour,
                )
                if do_rollover:
                    await trigger_rollover(
                        db,
                        session_id=row["session_id"],
                        thread_root_id=row["thread_root_id"],
                        room_id=row["room_id"],
                        project_dir=project_dir,
                        subprocess_timeout=subprocess_timeout,
                        threshold=threshold,
                        trigger=trigger,
                        context_window=context_window,
                        nightly_hour=nightly_hour,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("action=idle_monitor_error")
        await asyncio.sleep(interval)


# ---------------------------------------------------------------------------
# Thread / reply resolution
# ---------------------------------------------------------------------------

def extract_thread_root(event: RoomMessageText) -> str | None:
    """Return the thread-root event ID for a reply, or None for a room-root message."""
    source = getattr(event, "source", {})
    content = source.get("content", {}) if isinstance(source, dict) else {}
    relates_to = content.get("m.relates_to")
    if not isinstance(relates_to, dict):
        return None
    candidate: object | None = None
    if relates_to.get("rel_type") == "m.thread":
        candidate = relates_to.get("event_id")
    else:
        in_reply_to = relates_to.get("m.in_reply_to")
        if isinstance(in_reply_to, dict):
            candidate = in_reply_to.get("event_id")
    if candidate is None:
        return None
    if not isinstance(candidate, str):
        log.warning(
            "action=malformed_relates_to event_id=%s type=%s",
            getattr(event, "event_id", "?"), type(candidate).__name__,
        )
        return None
    return candidate


# ---------------------------------------------------------------------------
# Response posting
# ---------------------------------------------------------------------------

async def _post_response(
    client: AsyncClient,
    room_id: str,
    output: str,
    exit_code: int,
    session_id: str,
    mention: str,
    max_message_length: int,
    reply_target: str,
    db: sqlite3.Connection,
    thread_root_id: str,
) -> None:
    short_id = session_id[:8]
    if exit_code != 0:
        event_id = await post_message(
            client, room_id,
            f"{mention} Session {short_id} error:\n\n{output}",
            reply_to=reply_target,
        )
        register_alias(db, event_id, thread_root_id)
        return
    if not output:
        output = "(no output)"
    chunks = split_on_paragraphs(output, max_message_length)
    for i, chunk in enumerate(chunks):
        text = f"{mention} {chunk}" if i == 0 else chunk
        event_id = await post_message(client, room_id, text, reply_to=reply_target)
        register_alias(db, event_id, thread_root_id)


# ---------------------------------------------------------------------------
# Event handler
# ---------------------------------------------------------------------------

async def handle_event(
    client: AsyncClient,
    room_id: str,
    event: RoomMessageText,
    operator_user_id: str,
    project_dir: str,
    db: sqlite3.Connection,
    max_message_length: int,
    subprocess_timeout: int,
    context_window: int = DEFAULT_CONTEXT_WINDOW_TOKENS,
) -> None:
    global _last_spawn_at, _room_lock
    assert _room_lock is not None

    # Sender gate — silently drop non-operator messages
    if event.sender != operator_user_id:
        return

    log.info(
        "action=message_received room=%s sender=%s msg_len=%d is_thread=%s event_id=%s",
        room_id, event.sender, len(event.body or ""),
        bool(extract_thread_root(event)), event.event_id,
    )

    user_message = (event.body or "").strip()
    if not user_message:
        return
    if len(user_message) > 32_000:
        await post_message(
            client, room_id,
            f"Message too long ({len(user_message):,} chars, limit is 32,000).",
            reply_to=event.event_id,
        )
        return
    thread_root = extract_thread_root(event)
    mention = operator_user_id  # @ted mention on first chunk

    # Thread reply → resume the matching session
    if thread_root is not None:
        row = get_session_by_event(db, thread_root)
        if row is not None:
            session_id = row["session_id"]
            thread_root_id = row["thread_root_id"]
            short_id = session_id[:8]
            log.info(
                "action=session_resume session_id=%s age_min=%.1f",
                short_id,
                (time.time() - row["created_at"]) / 60.0,
            )
            queue_depth = len(_handlers)
            if queue_depth > 3:
                log.warning(
                    "action=handler_queue_depth depth=%d session_id=%s",
                    queue_depth, short_id,
                )
            ack_event_id = await post_message(
                client, room_id,
                f"Thinking... (session {short_id})",
                reply_to=event.event_id,
            )
            register_alias(db, ack_event_id, thread_root_id)
            async with _room_lock:
                # M2: re-fetch session row under lock — idle_monitor may have
                # rolled over this session between get_session_by_event() and
                # lock-acquire. Use fresh data for all subsequent logic.
                fresh = get_session_by_event(db, thread_root)
                if fresh is None or fresh["session_id"] != row["session_id"]:
                    row = fresh
                    if row is None:
                        log.info(
                            "action=session_rolled_over_during_handler "
                            "thread=%s", thread_root[:8],
                        )
                        return
                    session_id = row["session_id"]
                    thread_root_id = row["thread_root_id"]
                    short_id = session_id[:8]
                global _last_resume_at
                now = time.time()
                if now - _last_resume_at < RESUME_RATE_LIMIT_SECONDS:
                    remaining = int(RESUME_RATE_LIMIT_SECONDS - (now - _last_resume_at))
                    log.info(
                        "action=resume_rate_limited session_id=%s wait_s=%d",
                        short_id, remaining,
                    )
                    rate_event_id = await post_message(
                        client, room_id,
                        f"{mention} Still processing — retry in {remaining}s.",
                        reply_to=ack_event_id or event.event_id,
                    )
                    register_alias(db, rate_event_id, thread_root_id)
                    return
                _last_resume_at = now
                start = time.time()
                needs_handoff = (
                    not row["handoff_injected"]
                    and row["previous_session_id"]
                )
                await _typing_on(client, room_id)
                try:
                    if needs_handoff:
                        prev_id = row["previous_session_id"]
                        handoff_path = HANDOFFS_DIR / f"{prev_id}.md"
                        handoff_text = (
                            handoff_path.read_text(errors="replace")
                            if handoff_path.exists() else ""
                        )
                        # L3: cap handoff_text to prevent oversized CLI args
                        if len(handoff_text.encode()) > 16_000:
                            handoff_text = (
                                handoff_text[:16_000] + "\n[…handoff truncated…]"
                            )
                        full_message = (
                            "[Session context — recent conversation summary "
                            "and last messages:\n"
                            f"{handoff_text}\n"
                            "---]\n\n"
                            f"{stamp(user_message)}"
                        )
                        log.info(
                            "action=continuity_inject session_id=%s prev=%s "
                            "handoff_bytes=%d",
                            short_id, prev_id[:8], len(handoff_text),
                        )
                        # L1: set handoff_injected before spawn so a
                        # TimeoutError doesn't cause double-injection on retry
                        with db:
                            db.execute(
                                "UPDATE sessions SET handoff_injected=1 "
                                "WHERE session_id=?",
                                (session_id,),
                            )
                        rc, output = await spawn_personal(
                            session_id, full_message, project_dir,
                            subprocess_timeout,
                        )
                    else:
                        rc, output = await resume_personal(
                            session_id, stamp(user_message), project_dir,
                            subprocess_timeout,
                        )
                except asyncio.TimeoutError:
                    log.error("action=resume_timeout session_id=%s", short_id)
                    timeout_event_id = await post_message(
                        client, room_id,
                        f"{mention} Session {short_id} timed out after "
                        f"{subprocess_timeout}s.",
                        reply_to=ack_event_id or event.event_id,
                    )
                    register_alias(db, timeout_event_id, thread_root_id)
                    return
                finally:
                    await _typing_off(client, room_id)
                touch_session(db, session_id)
                update_token_fill(db, session_id, project_dir, context_window)
                log.info(
                    "action=claude_exit session_id=%s rc=%d elapsed_s=%.1f",
                    short_id, rc, time.time() - start,
                )
                await _post_response(
                    client, room_id, output, rc, session_id, mention,
                    max_message_length,
                    reply_target=ack_event_id or event.event_id,
                    db=db, thread_root_id=thread_root_id,
                )
            return
        log.info(
            "action=orphaned_reply event_id=%s thread_root=%s",
            event.event_id, thread_root,
        )

    # Room-root → resume active session if one exists, otherwise spawn new
    async with _room_lock:
        active_row = get_latest_active_session(db)
        if active_row is not None:
            # Resume the existing session — same path as a thread reply
            session_id = active_row["session_id"]
            thread_root_id = active_row["thread_root_id"]
            short_id = session_id[:8]
            log.info(
                "action=session_resume session_id=%s age_min=%.1f trigger=room_root",
                short_id,
                (time.time() - active_row["created_at"]) / 60.0,
            )
            # M2: re-fetch under lock — idle_monitor may have retired this session
            fresh = db.execute(
                "SELECT * FROM sessions WHERE session_id = ? AND status = 'active'",
                (session_id,),
            ).fetchone()
            if fresh is None:
                log.info(
                    "action=session_rolled_over_during_handler session_id=%s",
                    short_id,
                )
                active_row = None  # fall through to spawn
            else:
                now = time.time()
                if now - _last_resume_at < RESUME_RATE_LIMIT_SECONDS:
                    remaining = int(RESUME_RATE_LIMIT_SECONDS - (now - _last_resume_at))
                    log.info(
                        "action=resume_rate_limited session_id=%s wait_s=%d",
                        short_id, remaining,
                    )
                    rate_event_id = await post_message(
                        client, room_id,
                        f"{mention} Still processing — retry in {remaining}s.",
                        reply_to=event.event_id,
                    )
                    register_alias(db, rate_event_id, thread_root_id)
                    return
                _last_resume_at = now
                ack_event_id = await post_message(
                    client, room_id,
                    f"Thinking... (session {short_id})",
                    reply_to=event.event_id,
                )
                register_alias(db, ack_event_id, thread_root_id)
                register_alias(db, event.event_id, thread_root_id)
                start = time.time()
                needs_handoff = (
                    not fresh["handoff_injected"]
                    and fresh["previous_session_id"]
                )
                await _typing_on(client, room_id)
                try:
                    if needs_handoff:
                        prev_id = fresh["previous_session_id"]
                        handoff_path = HANDOFFS_DIR / f"{prev_id}.md"
                        handoff_text = (
                            handoff_path.read_text(errors="replace")
                            if handoff_path.exists() else ""
                        )
                        if len(handoff_text.encode()) > 16_000:
                            handoff_text = (
                                handoff_text[:16_000] + "\n[…handoff truncated…]"
                            )
                        full_message = (
                            "[Session context — recent conversation summary "
                            "and last messages:\n"
                            f"{handoff_text}\n"
                            "---]\n\n"
                            f"{stamp(user_message)}"
                        )
                        log.info(
                            "action=continuity_inject session_id=%s prev=%s "
                            "handoff_bytes=%d",
                            short_id, prev_id[:8], len(handoff_text),
                        )
                        with db:
                            db.execute(
                                "UPDATE sessions SET handoff_injected=1 "
                                "WHERE session_id=?",
                                (session_id,),
                            )
                        rc, output = await spawn_personal(
                            session_id, full_message, project_dir,
                            subprocess_timeout,
                        )
                    else:
                        rc, output = await resume_personal(
                            session_id, stamp(user_message), project_dir,
                            subprocess_timeout,
                        )
                except asyncio.TimeoutError:
                    log.error("action=resume_timeout session_id=%s", short_id)
                    timeout_event_id = await post_message(
                        client, room_id,
                        f"{mention} Session {short_id} timed out after "
                        f"{subprocess_timeout}s.",
                        reply_to=ack_event_id or event.event_id,
                    )
                    register_alias(db, timeout_event_id, thread_root_id)
                    return
                finally:
                    await _typing_off(client, room_id)
                update_token_fill(db, session_id, project_dir, context_window)
                log.info(
                    "action=claude_exit session_id=%s rc=%d elapsed_s=%.1f",
                    short_id, rc, time.time() - start,
                )
                await _post_response(
                    client, room_id, output, rc, session_id, mention,
                    max_message_length,
                    reply_target=ack_event_id or event.event_id,
                    db=db, thread_root_id=thread_root_id,
                )
                return

        # No active session → spawn new
        now = time.time()
        if now - _last_spawn_at < RATE_LIMIT_SECONDS:
            remaining = int(RATE_LIMIT_SECONDS - (now - _last_spawn_at))
            log.info("action=rate_limited event_id=%s remaining=%d",
                     event.event_id, remaining)
            await post_message(
                client, room_id,
                f"{mention} Rate-limited; try again in {remaining}s.",
                reply_to=event.event_id,
            )
            return
        _last_spawn_at = now

        session_id = str(uuid.uuid4())
        short_id = session_id[:8]
        log.info(
            "action=session_spawn session_id=%s trigger=new event_id=%s",
            short_id, event.event_id,
        )
        ack_event_id = await post_message(
            client, room_id,
            f"Thinking... (session {short_id})",
            reply_to=event.event_id,
        )
        # Persist before spawning — replies can resume even if spawn errors
        insert_session(db, session_id, event.event_id, room_id)
        register_alias(db, ack_event_id, event.event_id)

        # v0.4: cold-start memsearch — prepend relevant prior context to first message
        relevant_memory = query_memsearch(user_message)
        if relevant_memory:
            spawn_message = (
                f"[Relevant prior context:\n{relevant_memory}\n---]\n\n"
                f"{stamp(user_message)}"
            )
            log.info(
                "action=memsearch_inject session_id=%s bytes=%d",
                short_id, len(relevant_memory),
            )
        else:
            spawn_message = stamp(user_message)

        start = time.time()
        await _typing_on(client, room_id)
        try:
            rc, output = await spawn_personal(
                session_id, spawn_message, project_dir,
                subprocess_timeout,
            )
        except asyncio.TimeoutError:
            log.error("action=spawn_timeout session_id=%s", short_id)
            timeout_event_id = await post_message(
                client, room_id,
                f"{mention} Session {short_id} timed out after "
                f"{subprocess_timeout}s.",
                reply_to=ack_event_id or event.event_id,
            )
            register_alias(db, timeout_event_id, event.event_id)
            return
        finally:
            await _typing_off(client, room_id)
        update_token_fill(db, session_id, project_dir, context_window)
        log.info(
            "action=claude_exit session_id=%s rc=%d elapsed_s=%.1f",
            short_id, rc, time.time() - start,
        )
        await _post_response(
            client, room_id, output, rc, session_id, mention,
            max_message_length,
            reply_target=ack_event_id or event.event_id,
            db=db, thread_root_id=event.event_id,
        )


# ---------------------------------------------------------------------------
# Polling loop
# ---------------------------------------------------------------------------

async def poll_loop(
    client: AsyncClient,
    config: dict,
    db: sqlite3.Connection,
    operator_user_id: str,
    room_id: str,
) -> None:
    poll_interval = int(config.get("poll_interval_seconds", 5))
    max_message_length = int(config.get("max_message_length", 4000))
    project_dir = config["project_dir"]
    subprocess_timeout = int(config.get(
        "subprocess_timeout_seconds", DEFAULT_SUBPROCESS_TIMEOUT
    ))
    context_window = int(config.get(
        "context_window_tokens", DEFAULT_CONTEXT_WINDOW_TOKENS
    ))

    since = get_since(db)

    # Cold-start seeding: capture next_batch without processing recent events,
    # preventing re-spawns for messages already answered before this run.
    if since is None:
        seed = await client.sync(timeout=0, since=None, full_state=False)
        if isinstance(seed, SyncResponse):
            since = seed.next_batch
            set_since(db, since)
            log.info("action=poll_seed since=%s", since)
        else:
            log.warning("action=poll_seed_error response=%s", type(seed).__name__)

    log.info(
        "action=poll_start room=%s operator=%s since=%s",
        room_id, operator_user_id, since,
    )

    while True:
        try:
            resp = await client.sync(timeout=0, since=since, full_state=False)
            if not isinstance(resp, SyncResponse):
                log.warning("action=sync_error response=%s", type(resp).__name__)
                await asyncio.sleep(poll_interval)
                continue
            since = resp.next_batch
            set_since(db, since)

            global _last_cleanup_at
            now_ts = time.time()
            if now_ts - _last_cleanup_at > CLEANUP_INTERVAL_SECONDS:
                retention_days = int(config.get("session_retention_days", 30))
                cleanup_old_sessions(db, retention_days)
                _last_cleanup_at = now_ts

            for joined_room_id, room_info in resp.rooms.join.items():
                if joined_room_id != room_id:
                    continue
                for event in room_info.timeline.events:
                    if not isinstance(event, RoomMessageText):
                        continue
                    task = asyncio.create_task(handle_event(
                        client=client,
                        room_id=room_id,
                        event=event,
                        operator_user_id=operator_user_id,
                        project_dir=project_dir,
                        db=db,
                        max_message_length=max_message_length,
                        subprocess_timeout=subprocess_timeout,
                        context_window=context_window,
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
    global _room_lock

    homeserver, user_id, token, room_id = get_credentials()
    config = load_config()

    # Override room_id from env if config doesn't match — env wins
    config_room_id = (config.get("room_id") or "").strip()
    if config_room_id and config_room_id != room_id:
        log.warning(
            "action=room_id_mismatch env=%s config=%s using=env",
            room_id, config_room_id,
        )

    operator_user_id = config.get("operator_user_id", "").strip()
    if not operator_user_id:
        log.error("action=startup_error missing_var=operator_user_id_in_config")
        sys.exit(1)

    project_dir = config.get("project_dir", "").strip()
    if not project_dir or not Path(project_dir).is_dir():
        log.error("action=startup_error reason=project_dir_missing path=%s",
                  project_dir)
        sys.exit(1)

    db = open_db()
    init_db(db)

    _room_lock = asyncio.Lock()

    client = AsyncClient(homeserver, user_id)
    client.access_token = token
    client.user_id = user_id

    log.info(
        "action=startup user_id=%s homeserver=%s room_id=%s project_dir=%s",
        user_id, homeserver, room_id, project_dir,
    )

    if config.get("startup_notify"):
        try:
            await post_message(
                client, room_id,
                "personal-agent v0.3 online.",
            )
        except Exception:
            log.exception("action=startup_notify_error")

    idle_task = asyncio.create_task(idle_monitor(db, config))
    try:
        await poll_loop(client, config, db, operator_user_id, room_id)
    finally:
        idle_task.cancel()
        try:
            await idle_task
        except (asyncio.CancelledError, Exception):
            pass
        for proc_key, proc in list(_active_processes.items()):
            try:
                proc.send_signal(signal.SIGTERM)
                log.info("action=shutdown_sigterm session=%s pid=%s",
                         proc_key[:8], proc.pid)
            except ProcessLookupError:
                pass
        if _handlers:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*_handlers, return_exceptions=True),
                    timeout=10,
                )
            except asyncio.TimeoutError:
                log.warning(
                    "action=shutdown_handlers_timeout outstanding=%d",
                    len(_handlers),
                )
        db.close()
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())

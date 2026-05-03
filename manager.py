"""personal-agent manager — Matrix poll loop + claude-code session lifecycle.

v0.1 scope:
  - Polls #personal:claudebox.me for messages from the operator only
  - Spawns claude -p with --session-id on room-root messages
  - Resumes on thread replies (matched via event_aliases)
  - Timestamp-prefixes every user message before passing to claude
  - Structured logs (no message body content)
  - Posts response chunks back to Matrix with @ted mention on first chunk

Future phases extend this file:
  v0.2 — idle rollover with handoff injection
  v0.3 — token-fill / age triggers
  v0.4 — typing indicators + cold-start memsearch injection
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
import signal
import sqlite3
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import yaml
from nio import AsyncClient, RoomMessageText, SyncResponse

# ---------------------------------------------------------------------------
# Runtime state
# ---------------------------------------------------------------------------

_room_lock: asyncio.Lock | None = None  # set lazily in main()
_active_processes: dict[str, asyncio.subprocess.Process] = {}
_last_spawn_at: float = 0.0
_handlers: set[asyncio.Task] = set()

DEFAULT_SUBPROCESS_TIMEOUT = 1800
RATE_LIMIT_SECONDS = 5

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
DB_PATH = Path.home() / ".claude" / "data" / "personal-agent" / "sessions.db"


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


def init_db(db: sqlite3.Connection) -> None:
    db.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
          session_id          TEXT PRIMARY KEY,
          thread_root_id      TEXT NOT NULL UNIQUE,
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

        -- Maps any manager-posted event_id (ack, response chunks) back to a
        -- thread_root, so replies to those events resolve to the right session.
        CREATE TABLE IF NOT EXISTS event_aliases (
          event_id       TEXT PRIMARY KEY,
          thread_root_id TEXT NOT NULL REFERENCES sessions(thread_root_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS poll_state (
          id         INTEGER PRIMARY KEY CHECK (id = 1),
          since      TEXT NOT NULL,
          updated_at INTEGER NOT NULL
        );
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
    """Resolve any event_id (thread root, ack, or response chunk) to a session row."""
    row = db.execute(
        "SELECT * FROM sessions WHERE thread_root_id = ?", (event_id,)
    ).fetchone()
    if row:
        return row
    alias = db.execute(
        "SELECT thread_root_id FROM event_aliases WHERE event_id = ?", (event_id,)
    ).fetchone()
    if alias:
        return db.execute(
            "SELECT * FROM sessions WHERE thread_root_id = ?",
            (alias["thread_root_id"],),
        ).fetchone()
    return None


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
    output = stdout if rc == 0 else stderr[:500]
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
            ack_event_id = await post_message(
                client, room_id,
                f"Thinking... (session {short_id})",
                reply_to=event.event_id,
            )
            register_alias(db, ack_event_id, thread_root_id)
            async with _room_lock:
                start = time.time()
                try:
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
                touch_session(db, session_id)
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

    # Room-root (or orphaned reply) → spawn new session, atomically
    async with _room_lock:
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

        start = time.time()
        try:
            rc, output = await spawn_personal(
                session_id, stamp(user_message), project_dir,
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
                "personal-agent v0.1 online.",
            )
        except Exception:
            log.exception("action=startup_notify_error")

    try:
        await poll_loop(client, config, db, operator_user_id, room_id)
    finally:
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

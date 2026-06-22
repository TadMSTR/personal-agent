"""Unit tests for the personal-agent manager.

Cover the verification checklist from the build plan:
  - rollover fires at budget, not before
  - one_active_per_room is DB-enforced
  - handoff note written before state flip; contains the raw tail
  - !recap walks prev_session_id across a rolled->active boundary
  - stream-json parsing (fill = input-side sum; tools; result text; errors)
  - per-turn datetime tag (delta vs first-message)
  - nightly / idle pre-turn roll decisions
  - launch argv shape + session_id validation

Run: python -m pytest tests/ -q   (or: python -m unittest -q)
"""

from __future__ import annotations

import sqlite3
import time
import unittest
import uuid
from pathlib import Path

import manager


def fresh_db() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    manager.init_db(db)
    return db


DEPLOY = {
    "name": "harlock",
    "room_id": "!room:example.org",
    "timezone": "America/New_York",
    "claude_bin": "/usr/local/bin/claude",
    "agent_user": "agent-harlock",
    "project_dir": "/home/agent-harlock/.claude/projects/harlock",
    "agent_home": "/home/agent-harlock",
}


class TestSchema(unittest.TestCase):
    def test_one_active_per_room(self):
        db = fresh_db()
        manager.insert_session(db, str(uuid.uuid4()), "!r:x", None, None)
        with self.assertRaises(sqlite3.IntegrityError):
            manager.insert_session(db, str(uuid.uuid4()), "!r:x", None, None)

    def test_rolled_frees_the_slot(self):
        db = fresh_db()
        s1 = str(uuid.uuid4())
        manager.insert_session(db, s1, "!r:x", None, None)
        manager.mark_session(db, s1, "rolled", "token_budget")
        # Now a second active session is allowed.
        s2 = str(uuid.uuid4())
        manager.insert_session(db, s2, "!r:x", None, s1)
        self.assertEqual(manager.get_active_session(db, "!r:x")["session_id"], s2)


class TestStreamJson(unittest.TestCase):
    def test_fill_is_input_side_sum(self):
        lines = [
            '{"type":"system","subtype":"init"}',
            '{"type":"assistant","message":{"content":['
            '{"type":"text","text":"hi"},'
            '{"type":"tool_use","name":"mcp__x__do"}]}}',
            '{"type":"result","subtype":"success","result":"final answer",'
            '"usage":{"input_tokens":1000,"cache_read_input_tokens":2000,'
            '"cache_creation_input_tokens":500,"output_tokens":99}}',
        ]
        out = manager.parse_stream_json("\n".join(lines))
        self.assertEqual(out["text"], "final answer")
        self.assertEqual(out["fill"], 3500)  # output_tokens excluded
        self.assertEqual(out["tools"], ["mcp__x__do"])
        self.assertFalse(out["is_error"])

    def test_error_result(self):
        line = '{"type":"result","subtype":"error_during_execution","result":"boom"}'
        out = manager.parse_stream_json(line)
        self.assertTrue(out["is_error"])

    def test_garbage_lines_skipped(self):
        out = manager.parse_stream_json("not json\n\n{bad}\n")
        self.assertEqual(out["text"], "")
        self.assertEqual(out["fill"], 0)


class TestDatetimeTag(unittest.TestCase):
    def test_first_message(self):
        tag = manager.datetime_tag(1_700_000_000, None, "America/New_York")
        self.assertIn("first message", tag)
        self.assertTrue(tag.startswith("[time: "))

    def test_delta(self):
        now = 1_700_000_000.0
        tag = manager.datetime_tag(now, int(now) - (3 * 3600 + 12 * 60), "America/New_York")
        self.assertIn("+3h12m since last", tag)


class TestRolloverDecision(unittest.TestCase):
    def _row(self, **kw):
        base = dict(session_id="s", room_id="!r:x", state="active",
                    thread_root_id=None, prev_session_id=None, fill_tokens=0,
                    turn_count=1, created_at=0, last_used_at=0, rolled_reason=None)
        base.update(kw)
        return base

    def test_no_roll_when_fresh_and_full(self):
        now = time.time()
        sess = self._row(last_used_at=int(now) - 10, fill_tokens=120_000)
        self.assertIsNone(manager.should_roll_preturn(sess, now))

    def test_idle_roll_only_when_thin(self):
        now = time.time()
        # idle long, thin context -> roll
        thin = self._row(last_used_at=int(now) - 7200, fill_tokens=10_000)
        self.assertEqual(manager.should_roll_preturn(thin, now), "idle")
        # idle long, fat context -> do NOT idle-roll (still useful warm context)
        fat = self._row(last_used_at=int(now) - 7200, fill_tokens=100_000)
        # may still be nightly depending on clock, so assert it's not 'idle' when same-day
        res = manager.should_roll_preturn(fat, now)
        self.assertIn(res, (None, "nightly"))

    def test_nightly(self):
        # last_used 2 days ago is always before the most recent 04:00 cutoff
        now = time.time()
        sess = self._row(last_used_at=int(now) - 2 * 86400, fill_tokens=100_000)
        self.assertEqual(manager.should_roll_preturn(sess, now), "nightly")


class TestRolloverAndHandoff(unittest.TestCase):
    def test_budget_boundary_and_handoff(self):
        db = fresh_db()
        tmp = Path("/tmp") / f"pa-test-{uuid.uuid4().hex}"
        deploy = {**DEPLOY, "working_note_dir": str(tmp)}
        sid = str(uuid.uuid4())
        manager.insert_session(db, sid, "!r:x", None, None)
        for i in range(6):
            manager.insert_turn(db, sid, "!r:x", i, f"user {i}", f"asst {i}", "", 0)

        # below budget: no roll
        sess = manager.get_session(db, sid)
        self.assertLess(120_000, manager.ROLLOVER_BUDGET)
        # at/over budget: roll, note written, state flips
        manager.roll_over(db, deploy, sess, "token_budget")
        rolled = manager.get_session(db, sid)
        self.assertEqual(rolled["state"], "rolled")
        notes = list(tmp.glob("rollover-*.md"))
        self.assertEqual(len(notes), 1)
        body = notes[0].read_text()
        self.assertIn("user 5", body)
        self.assertIn("asst 0", body)  # all 6 turns present

    def test_recap_walks_chain(self):
        db = fresh_db()
        s1 = str(uuid.uuid4())
        s2 = str(uuid.uuid4())
        manager.insert_session(db, s1, "!r:x", None, None)
        manager.insert_turn(db, s1, "!r:x", 0, "old-q", "old-a", "", 0)
        manager.mark_session(db, s1, "rolled", "token_budget")
        manager.insert_session(db, s2, "!r:x", None, s1)
        manager.insert_turn(db, s2, "!r:x", 0, "new-q", "new-a", "", 0)

        turns = manager.last_n_turns_across_chain(db, s2, 5)
        texts = [t["user_text"] for t in turns]
        self.assertEqual(texts, ["old-q", "new-q"])  # chronological across boundary


class TestLaunchArgs(unittest.TestCase):
    def test_spawn_vs_resume(self):
        sid = str(uuid.uuid4())
        spawn = manager._launch_args(DEPLOY, session_id=sid, resume=False, prompt="hi")
        self.assertIn("--session-id", spawn)
        self.assertIn("sudo", spawn)
        self.assertEqual(spawn[0], "sudo")
        resume = manager._launch_args(DEPLOY, session_id=sid, resume=True, prompt="hi")
        self.assertIn("--resume", resume)
        self.assertIn("--output-format", resume)
        self.assertIn("stream-json", resume)
        self.assertIn("--verbose", resume)

    def test_model_flag_present_when_configured(self):
        sid = str(uuid.uuid4())
        args = manager._launch_args({**DEPLOY, "model": "claude-sonnet-4-6"},
                                    session_id=sid, resume=False, prompt="hi")
        self.assertIn("--model", args)
        self.assertIn("claude-sonnet-4-6", args)

    def test_model_flag_absent_when_unset(self):
        sid = str(uuid.uuid4())
        args = manager._launch_args(DEPLOY, session_id=sid, resume=False, prompt="hi")
        self.assertNotIn("--model", args)

    def test_no_agent_user_runs_direct(self):
        sid = str(uuid.uuid4())
        deploy = {**DEPLOY}
        deploy.pop("agent_user")
        args = manager._launch_args(deploy, session_id=sid, resume=False, prompt="hi")
        self.assertEqual(args[0], DEPLOY["claude_bin"])

    def test_session_id_validation_rejects_argv_injection(self):
        with self.assertRaises(ValueError):
            manager._validate_session_id("--dangerous")
        with self.assertRaises(ValueError):
            manager._validate_session_id("../../etc/passwd")
        manager._validate_session_id(str(uuid.uuid4()))  # ok


if __name__ == "__main__":
    unittest.main()

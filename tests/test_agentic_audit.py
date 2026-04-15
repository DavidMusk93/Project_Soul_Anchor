import unittest

from soul_anchor.agentic.decision_engine import DecisionEngine
from soul_anchor.agentic.gating import MemoryGating
from soul_anchor.agentic.tools import MemoryToolAPI
from soul_anchor.manager import MemoryManager


class TestAgenticAudit(unittest.TestCase):
    def setUp(self):
        self.manager = MemoryManager(db_path=":memory:")
        self.manager.connect()

        self.tools = MemoryToolAPI(self.manager)
        self.engine = DecisionEngine()
        self.gating = MemoryGating(self.manager)

        # Contract (TDD): implementation should live in soul_anchor/agentic/audit.py
        from soul_anchor.agentic.audit import (  # noqa: PLC0415
            AgenticLoopRunner,
            AuditRecorder,
            AuditVerifier,
        )

        self.recorder = AuditRecorder(self.manager)
        self.verifier = AuditVerifier()
        self.runner = AgenticLoopRunner(
            decision_engine=self.engine,
            gating=self.gating,
            tools=self.tools,
            audit_recorder=self.recorder,
            audit_verifier=self.verifier,
        )

    def tearDown(self):
        self.manager.close()

    def _audit_rows(self, action_type: str):
        return self.manager.conn.execute(
            """
            SELECT action_type, session_id, user_id, decision_payload, tool_payload, result_summary
            FROM memory_audit_log
            WHERE action_type = ?
            ORDER BY id
            """,
            [action_type],
        ).fetchall()

    def test_audit_recorder_writes_decision_log(self):
        decision = self.engine.decide(
            session_id="s1",
            user_id="u1",
            event_type="user_message",
            content="还是按上次那种方式来。",
        )

        audit_id = self.recorder.record_decision(
            session_id="s1",
            user_id="u1",
            decision=decision,
            input_payload={"event_type": "user_message", "content": "还是按上次那种方式来。"},
        )
        self.assertIsInstance(audit_id, int)

        rows = self._audit_rows("decision")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "s1")
        self.assertEqual(rows[0][2], "u1")
        self.assertIsInstance(rows[0][3], dict)  # decision_payload
        self.assertIn("should_recall_context", rows[0][3])
        self.assertIsInstance(rows[0][4], dict)  # tool_payload (input_payload)

    def test_audit_verifier_detects_decision_action_mismatch(self):
        decision = self.engine.decide(
            session_id="s2",
            user_id="u2",
            event_type="user_message",
            content="今天继续推进。",
        )
        # Force a mismatch: decision says no knowledge recall, but action list includes it.
        actions = ["search_knowledge"]

        with self.assertRaises(ValueError):
            self.verifier.assert_consistent(decision=decision, executed_actions=actions)

    def test_closed_loop_runs_and_writes_audit(self):
        # Seed a knowledge item so search_knowledge can return something.
        self.manager.save_knowledge(
            {
                "user_id": "u3",
                "knowledge_type": "workflow",
                "title": "TDD",
                "canonical_text": "Write tests first, then implement until green.",
                "keywords": "tdd,testing",
                "stability_score": 0.9,
            }
        )
        # Seed some context so search_context can return something.
        self.manager.save_episode(
            {
                "session_id": "s3",
                "user_id": "u3",
                "event_type": "task_state",
                "content": "上次我们做了 Phase 3.1 schema。",
                "salience_score": 0.9,
            }
        )

        loop_result = self.runner.run_event(
            session_id="s3",
            user_id="u3",
            event_type="user_message",
            content="还是按上次那种方式来，先写测试再实现。",
        )

        self.assertIn("decision", loop_result)
        self.assertIn("executed_actions", loop_result)
        self.assertIn("audit_ids", loop_result)
        self.assertIn("decision_audit_id", loop_result["audit_ids"])

        # Audit rows should include a decision entry + tool entries.
        self.assertEqual(len(self._audit_rows("decision")), 1)
        self.assertGreaterEqual(len(self._audit_rows("save_episode")), 1)
        self.assertGreaterEqual(len(self._audit_rows("search_context")), 1)
        self.assertGreaterEqual(len(self._audit_rows("search_knowledge")), 1)

    def test_closed_loop_creates_candidate_and_writes_audit(self):
        loop_result = self.runner.run_event(
            session_id="s4",
            user_id="u4",
            event_type="user_message",
            content="请记住我偏好简短提交，并且必须带 detail。",
        )

        self.assertIn("save_knowledge_candidate", loop_result["executed_actions"])

        row = self.manager.conn.execute(
            """
            SELECT user_id, knowledge_type, title, canonical_text, status
            FROM knowledge_candidate
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "u4")
        self.assertEqual(row[4], "pending")

        self.assertGreaterEqual(len(self._audit_rows("save_knowledge_candidate")), 1)


if __name__ == "__main__":
    unittest.main()

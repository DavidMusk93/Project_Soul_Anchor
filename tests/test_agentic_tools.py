import unittest

from soul_anchor.manager import MemoryManager


class TestMemoryToolAPI(unittest.TestCase):
    def setUp(self):
        self.manager = MemoryManager(db_path=":memory:")
        self.manager.connect()

        # Note: The implementation will live in soul_anchor/agentic/tools.py.
        # These tests define the Phase 3.1 tool contract (TDD).
        from soul_anchor.agentic.tools import MemoryToolAPI  # noqa: PLC0415

        self.tools = MemoryToolAPI(self.manager)

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

    def test_search_context_returns_results_and_writes_audit(self):
        self.manager.save_episode(
            {
                "session_id": "s1",
                "user_id": "u1",
                "event_type": "task_state",
                "content": "今天要实现 Memory Tool API。",
                "salience_score": 0.8,
            }
        )
        self.manager.save_episode(
            {
                "session_id": "s1",
                "user_id": "u1",
                "event_type": "note",
                "content": "这条不相关。",
                "salience_score": 0.2,
            }
        )

        results = self.tools.search_context(session_id="s1", user_id="u1", query="Memory Tool", top_k=5)

        self.assertIsInstance(results, list)
        self.assertGreaterEqual(len(results), 1)
        self.assertIn("id", results[0])
        self.assertIn("content", results[0])

        rows = self._audit_rows("search_context")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "search_context")
        self.assertEqual(rows[0][1], "s1")
        self.assertEqual(rows[0][2], "u1")
        self.assertIsNone(rows[0][3])  # decision_payload (Phase 3.1: tools can be called standalone)
        self.assertIsInstance(rows[0][4], dict)  # tool_payload
        self.assertIn("query", rows[0][4])

    def test_search_knowledge_returns_results_and_writes_audit(self):
        self.manager.save_knowledge(
            {
                "user_id": "u1",
                "knowledge_type": "workflow",
                "title": "TDD",
                "canonical_text": "Write tests first, then implement until green.",
                "keywords": "tdd,testing",
                "stability_score": 0.9,
            }
        )

        results = self.tools.search_knowledge(user_id="u1", query="tests first", top_k=3)

        self.assertIsInstance(results, list)
        self.assertGreaterEqual(len(results), 1)
        self.assertIn("title", results[0])

        rows = self._audit_rows("search_knowledge")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], "u1")
        self.assertIsInstance(rows[0][4], dict)
        self.assertEqual(rows[0][4]["top_k"], 3)

    def test_save_episode_returns_id_and_writes_audit(self):
        record_id = self.tools.save_episode(
            {
                "session_id": "s2",
                "user_id": "u2",
                "event_type": "user_message",
                "content": "请记住我偏好简短提交。",
            }
        )

        self.assertIsInstance(record_id, int)
        row = self.manager.conn.execute(
            "SELECT content FROM context_stream WHERE id = ?",
            [record_id],
        ).fetchone()
        self.assertEqual(row[0], "请记住我偏好简短提交。")

        rows = self._audit_rows("save_episode")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "s2")
        self.assertEqual(rows[0][2], "u2")

    def test_save_knowledge_candidate_writes_candidate_and_audit(self):
        candidate_id = self.tools.save_knowledge_candidate(
            {
                "user_id": "u3",
                "knowledge_type": "workflow",
                "title": "Commit Discipline",
                "canonical_text": "Every change should be committed with a short subject and a detailed body.",
                "source_refs": "context_stream:1",
                "candidate_payload": {
                    "reason": "user preference mentioned multiple times",
                    "evidence": ["Every change after should be committed"],
                },
                "confidence_score": 0.82,
            }
        )

        self.assertIsInstance(candidate_id, int)
        row = self.manager.conn.execute(
            """
            SELECT user_id, knowledge_type, title, status, candidate_payload
            FROM knowledge_candidate
            WHERE id = ?
            """,
            [candidate_id],
        ).fetchone()

        self.assertEqual(row[0], "u3")
        self.assertEqual(row[1], "workflow")
        self.assertEqual(row[2], "Commit Discipline")
        self.assertEqual(row[3], "pending")
        self.assertIsInstance(row[4], dict)
        self.assertEqual(row[4]["reason"], "user preference mentioned multiple times")

        rows = self._audit_rows("save_knowledge_candidate")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], "u3")
        self.assertIsInstance(rows[0][4], dict)
        self.assertEqual(rows[0][4]["title"], "Commit Discipline")

    def test_audit_recent_actions_returns_latest_actions(self):
        self.tools.save_episode(
            {"session_id": "s4", "user_id": "u4", "event_type": "note", "content": "A"}
        )
        self.tools.save_episode(
            {"session_id": "s4", "user_id": "u4", "event_type": "note", "content": "B"}
        )

        actions = self.tools.audit_recent_actions(limit=2)
        self.assertEqual(len(actions), 2)
        self.assertEqual(actions[0]["action_type"], "save_episode")
        self.assertEqual(actions[1]["action_type"], "save_episode")


if __name__ == "__main__":
    unittest.main()


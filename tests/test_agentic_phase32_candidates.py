import unittest

from soul_anchor.agentic.tools import MemoryToolAPI
from soul_anchor.manager import MemoryManager


class TestPhase32CandidateMerge(unittest.TestCase):
    def setUp(self):
        self.manager = MemoryManager(db_path=":memory:")
        self.manager.connect()
        self.tools = MemoryToolAPI(self.manager)

        # Contract (TDD): implementation should live in soul_anchor/agentic/candidates.py
        from soul_anchor.agentic.candidates import CandidateProcessor  # noqa: PLC0415

        self.processor = CandidateProcessor(self.manager)

    def tearDown(self):
        self.manager.close()

    def _candidate_row(self, candidate_id: int):
        return self.manager.conn.execute(
            """
            SELECT status, reviewed_at IS NOT NULL, candidate_payload
            FROM knowledge_candidate
            WHERE id = ?
            """,
            [candidate_id],
        ).fetchone()

    def _count_semantic(self, user_id: str, canonical_text: str) -> int:
        return int(
            self.manager.conn.execute(
                """
                SELECT count(*)
                FROM semantic_knowledge
                WHERE user_id = ?
                  AND canonical_text = ?
                  AND is_active = TRUE
                """,
                [user_id, canonical_text],
            ).fetchone()[0]
        )

    def _conflicts(self):
        return self.manager.conn.execute(
            """
            SELECT conflict_type, status, candidate_id, existing_knowledge_id, details
            FROM conflict_registry
            ORDER BY id
            """
        ).fetchall()

    def _audit(self, action_type: str):
        return self.manager.conn.execute(
            """
            SELECT action_type, user_id, tool_payload
            FROM memory_audit_log
            WHERE action_type = ?
            ORDER BY id
            """,
            [action_type],
        ).fetchall()

    def test_merge_pending_candidate_into_semantic_knowledge(self):
        candidate_id = self.tools.save_knowledge_candidate(
            {
                "user_id": "u1",
                "knowledge_type": "workflow",
                "title": "Commit Discipline",
                "canonical_text": "Every change should be committed with a short subject and a detailed body.",
                "candidate_payload": {"source": "user_preference"},
                "confidence_score": 0.8,
            }
        )

        results = self.processor.process_pending(limit=10)
        self.assertGreaterEqual(results["merged"], 1)

        status, reviewed, payload = self._candidate_row(candidate_id)
        self.assertEqual(status, "merged")
        self.assertTrue(reviewed)
        self.assertIsInstance(payload, dict)
        self.assertIn("merged_knowledge_id", payload)

        self.assertEqual(
            self._count_semantic(
                "u1",
                "Every change should be committed with a short subject and a detailed body.",
            ),
            1,
        )
        self.assertEqual(self._conflicts(), [])
        self.assertGreaterEqual(len(self._audit("merge_candidate")), 1)

    def test_duplicate_candidate_against_semantic_marks_duplicate(self):
        self.manager.save_knowledge(
            {
                "user_id": "u2",
                "knowledge_type": "workflow",
                "title": "Commit Discipline",
                "canonical_text": "same text",
            }
        )
        candidate_id = self.tools.save_knowledge_candidate(
            {
                "user_id": "u2",
                "knowledge_type": "workflow",
                "title": "Commit Discipline (dup)",
                "canonical_text": "same text",
                "candidate_payload": {},
            }
        )

        results = self.processor.process_pending(limit=10)
        self.assertGreaterEqual(results["duplicates"], 1)

        status, reviewed, payload = self._candidate_row(candidate_id)
        self.assertEqual(status, "duplicate")
        self.assertTrue(reviewed)
        self.assertIsInstance(payload, dict)
        self.assertIn("duplicate_of", payload)
        self.assertEqual(self._count_semantic("u2", "same text"), 1)
        self.assertGreaterEqual(len(self._audit("candidate_duplicate")), 1)

    def test_conflict_candidate_registers_conflict_and_marks_candidate(self):
        existing_id = self.manager.save_knowledge(
            {
                "user_id": "u3",
                "knowledge_type": "workflow",
                "title": "Commit Discipline",
                "canonical_text": "old rule",
            }
        )
        candidate_id = self.tools.save_knowledge_candidate(
            {
                "user_id": "u3",
                "knowledge_type": "workflow",
                "title": "Commit Discipline",
                "canonical_text": "new conflicting rule",
                "candidate_payload": {"note": "conflict"},
            }
        )

        results = self.processor.process_pending(limit=10)
        self.assertGreaterEqual(results["conflicts"], 1)

        status, reviewed, _ = self._candidate_row(candidate_id)
        self.assertEqual(status, "conflict")
        self.assertTrue(reviewed)

        conflicts = self._conflicts()
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0][0], "title_conflict")
        self.assertEqual(conflicts[0][1], "open")
        self.assertEqual(conflicts[0][2], candidate_id)
        self.assertEqual(conflicts[0][3], existing_id)
        self.assertIsInstance(conflicts[0][4], dict)
        self.assertGreaterEqual(len(self._audit("candidate_conflict")), 1)

    def test_duplicate_candidate_against_candidate_is_handled(self):
        first_id = self.tools.save_knowledge_candidate(
            {
                "user_id": "u4",
                "knowledge_type": "workflow",
                "title": "Rule A",
                "canonical_text": "same text",
                "candidate_payload": {},
            }
        )
        second_id = self.tools.save_knowledge_candidate(
            {
                "user_id": "u4",
                "knowledge_type": "workflow",
                "title": "Rule A (dup)",
                "canonical_text": "same text",
                "candidate_payload": {},
            }
        )

        results = self.processor.process_pending(limit=10)
        self.assertGreaterEqual(results["merged"], 1)
        self.assertGreaterEqual(results["duplicates"], 1)

        status1, _, _ = self._candidate_row(first_id)
        status2, _, payload2 = self._candidate_row(second_id)
        self.assertEqual(status1, "merged")
        self.assertEqual(status2, "duplicate")
        self.assertIsInstance(payload2, dict)
        self.assertIn("duplicate_of", payload2)


if __name__ == "__main__":
    unittest.main()


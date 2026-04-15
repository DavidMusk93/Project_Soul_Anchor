import unittest

from soul_anchor.agentic.candidates import CandidateProcessor
from soul_anchor.agentic.tools import MemoryToolAPI
from soul_anchor.manager import MemoryManager


class TestPhase32ConflictResolution(unittest.TestCase):
    def setUp(self):
        self.manager = MemoryManager(db_path=":memory:")
        self.manager.connect()
        self.tools = MemoryToolAPI(self.manager)
        self.processor = CandidateProcessor(self.manager)

        # Contract (TDD): implementation should live in soul_anchor/agentic/conflicts.py
        from soul_anchor.agentic.conflicts import ConflictResolver  # noqa: PLC0415

        self.resolver = ConflictResolver(self.manager)

    def tearDown(self):
        self.manager.close()

    def _latest_conflict(self):
        return self.manager.conn.execute(
            """
            SELECT id, candidate_id, existing_knowledge_id, conflict_type, status, resolved_at IS NOT NULL
            FROM conflict_registry
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

    def _semantic_text(self, knowledge_id: int) -> str:
        return self.manager.conn.execute(
            """
            SELECT canonical_text
            FROM semantic_knowledge
            WHERE id = ?
            """,
            [knowledge_id],
        ).fetchone()[0]

    def _candidate_row(self, candidate_id: int):
        return self.manager.conn.execute(
            """
            SELECT status, reviewed_at IS NOT NULL, candidate_payload, canonical_text
            FROM knowledge_candidate
            WHERE id = ?
            """,
            [candidate_id],
        ).fetchone()

    def _audit_rows(self, action_type: str):
        return self.manager.conn.execute(
            """
            SELECT action_type, user_id, tool_payload
            FROM memory_audit_log
            WHERE action_type = ?
            ORDER BY id
            """,
            [action_type],
        ).fetchall()

    def _create_conflict(self):
        existing_id = self.manager.save_knowledge(
            {
                "user_id": "u1",
                "knowledge_type": "workflow",
                "title": "Commit Discipline",
                "canonical_text": "old rule",
            }
        )
        candidate_id = self.tools.save_knowledge_candidate(
            {
                "user_id": "u1",
                "knowledge_type": "workflow",
                "title": "Commit Discipline",
                "canonical_text": "new conflicting rule",
                "candidate_payload": {"note": "conflict"},
            }
        )

        results = self.processor.process_pending(limit=10)
        self.assertGreaterEqual(results["conflicts"], 1)

        conflict = self._latest_conflict()
        self.assertIsNotNone(conflict)
        conflict_id, c_id, k_id, conflict_type, status, resolved = conflict
        self.assertEqual(c_id, candidate_id)
        self.assertEqual(k_id, existing_id)
        self.assertEqual(conflict_type, "title_conflict")
        self.assertEqual(status, "open")
        self.assertFalse(resolved)
        return int(conflict_id), int(candidate_id), int(existing_id)

    def test_resolve_conflict_keep_existing(self):
        conflict_id, candidate_id, existing_id = self._create_conflict()

        before = self._semantic_text(existing_id)
        self.resolver.resolve(conflict_id=conflict_id, strategy="keep_existing")
        after = self._semantic_text(existing_id)

        self.assertEqual(before, after)

        conflict = self._latest_conflict()
        self.assertEqual(conflict[0], conflict_id)
        self.assertEqual(conflict[4], "resolved")
        self.assertTrue(conflict[5])

        status, reviewed, payload, _ = self._candidate_row(candidate_id)
        self.assertEqual(status, "rejected")
        self.assertTrue(reviewed)
        self.assertIsInstance(payload, dict)
        self.assertEqual(payload.get("resolution"), "keep_existing")

        self.assertGreaterEqual(len(self._audit_rows("resolve_conflict")), 1)

    def test_resolve_conflict_replace(self):
        conflict_id, candidate_id, existing_id = self._create_conflict()

        _, _, _, candidate_text = self._candidate_row(candidate_id)
        self.resolver.resolve(conflict_id=conflict_id, strategy="replace")

        self.assertEqual(self._semantic_text(existing_id), candidate_text)

        conflict = self._latest_conflict()
        self.assertEqual(conflict[0], conflict_id)
        self.assertEqual(conflict[4], "resolved")
        self.assertTrue(conflict[5])

        status, reviewed, payload, _ = self._candidate_row(candidate_id)
        self.assertEqual(status, "applied")
        self.assertTrue(reviewed)
        self.assertIsInstance(payload, dict)
        self.assertEqual(payload.get("resolution"), "replace")
        self.assertEqual(payload.get("applied_to"), existing_id)

        self.assertGreaterEqual(len(self._audit_rows("resolve_conflict")), 1)

    def test_resolve_conflict_merge_text(self):
        conflict_id, candidate_id, existing_id = self._create_conflict()

        existing_text = self._semantic_text(existing_id)
        _, _, _, candidate_text = self._candidate_row(candidate_id)

        self.resolver.resolve(conflict_id=conflict_id, strategy="merge_text")

        merged = self._semantic_text(existing_id)
        self.assertIn(existing_text, merged)
        self.assertIn(candidate_text, merged)
        self.assertNotEqual(merged, existing_text)

        conflict = self._latest_conflict()
        self.assertEqual(conflict[4], "resolved")
        self.assertTrue(conflict[5])

        status, reviewed, payload, _ = self._candidate_row(candidate_id)
        self.assertEqual(status, "applied")
        self.assertTrue(reviewed)
        self.assertEqual(payload.get("resolution"), "merge_text")
        self.assertEqual(payload.get("applied_to"), existing_id)

        self.assertGreaterEqual(len(self._audit_rows("resolve_conflict")), 1)


if __name__ == "__main__":
    unittest.main()


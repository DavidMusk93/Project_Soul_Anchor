import unittest

from soul_anchor.manager import MemoryManager


class TestPhase33KnowledgeVersioning(unittest.TestCase):
    def setUp(self):
        self.manager = MemoryManager(db_path=":memory:")
        self.manager.connect()

        # Contract (TDD): implementation should live in soul_anchor/agentic/versioning.py
        from soul_anchor.agentic.versioning import KnowledgeVersioning  # noqa: PLC0415

        self.versioning = KnowledgeVersioning(self.manager)

    def tearDown(self):
        self.manager.close()

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

    def test_create_snapshot_persists_payload_and_audit(self):
        knowledge_id = self.manager.save_knowledge(
            {
                "user_id": "u1",
                "knowledge_type": "workflow",
                "title": "Commit Discipline",
                "canonical_text": "old text",
                "metadata": {"v": 1},
            }
        )

        snapshot_id = self.versioning.create_snapshot(
            knowledge_id=knowledge_id,
            reason="before update",
        )
        self.assertIsInstance(snapshot_id, int)

        row = self.manager.conn.execute(
            """
            SELECT knowledge_id, reason, snapshot_payload
            FROM knowledge_version_snapshot
            WHERE id = ?
            """,
            [snapshot_id],
        ).fetchone()
        self.assertEqual(row[0], knowledge_id)
        self.assertEqual(row[1], "before update")
        self.assertIsInstance(row[2], dict)
        self.assertEqual(row[2]["canonical_text"], "old text")
        self.assertEqual(row[2]["metadata"]["v"], 1)

        audits = self._audit("create_snapshot")
        self.assertGreaterEqual(len(audits), 1)
        self.assertEqual(audits[-1][1], "u1")
        self.assertEqual(audits[-1][2]["knowledge_id"], knowledge_id)

    def test_rollback_to_snapshot_restores_canonical_text_and_metadata(self):
        knowledge_id = self.manager.save_knowledge(
            {
                "user_id": "u2",
                "knowledge_type": "workflow",
                "title": "Rule",
                "canonical_text": "v1",
                "metadata": {"v": 1},
            }
        )

        snapshot_id = self.versioning.create_snapshot(knowledge_id=knowledge_id, reason="v1")

        # mutate knowledge
        self.manager.conn.execute(
            """
            UPDATE semantic_knowledge
            SET canonical_text = ?, metadata = json(?)::VARIANT, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            ["v2", '{"v":2}', knowledge_id],
        )

        self.versioning.rollback_to_snapshot(
            knowledge_id=knowledge_id,
            snapshot_id=snapshot_id,
            reason="rollback to v1",
        )

        row = self.manager.conn.execute(
            """
            SELECT canonical_text, metadata
            FROM semantic_knowledge
            WHERE id = ?
            """,
            [knowledge_id],
        ).fetchone()
        self.assertEqual(row[0], "v1")
        self.assertIsInstance(row[1], dict)
        self.assertEqual(row[1]["v"], 1)

        audits = self._audit("rollback_to_snapshot")
        self.assertGreaterEqual(len(audits), 1)
        self.assertEqual(audits[-1][1], "u2")
        self.assertEqual(audits[-1][2]["knowledge_id"], knowledge_id)
        self.assertEqual(audits[-1][2]["snapshot_id"], snapshot_id)


if __name__ == "__main__":
    unittest.main()


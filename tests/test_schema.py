import unittest

from soul_anchor.manager import MemoryManager


class TestSchema(unittest.TestCase):
    def setUp(self):
        self.manager = MemoryManager(db_path=":memory:")
        self.manager.connect()

    def tearDown(self):
        self.manager.close()

    def test_phase1_schema_is_initialized(self):
        tables = self.manager.conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
            """
        ).fetchall()

        self.assertIn(("context_stream",), tables)
        self.assertIn(("core_contract",), tables)
        self.assertIn(("semantic_knowledge",), tables)

    def test_phase1_schema_has_extensible_columns(self):
        context_columns = {
            name: data_type
            for _, name, data_type, _, _, _ in self.manager.conn.execute(
                "PRAGMA table_info('context_stream')"
            ).fetchall()
        }
        knowledge_columns = {
            name: data_type
            for _, name, data_type, _, _, _ in self.manager.conn.execute(
                "PRAGMA table_info('semantic_knowledge')"
            ).fetchall()
        }

        self.assertEqual(context_columns["metadata"], "VARIANT")
        self.assertEqual(context_columns["embedding"], "FLOAT[]")
        self.assertEqual(knowledge_columns["metadata"], "VARIANT")
        self.assertEqual(knowledge_columns["embedding"], "FLOAT[]")

    def test_phase31_schema_is_initialized(self):
        tables = self.manager.conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
            """
        ).fetchall()

        # Phase 3.1 adds candidate + audit tables (schema-first).
        self.assertIn(("knowledge_candidate",), tables)
        self.assertIn(("memory_audit_log",), tables)

    def test_phase31_schema_columns(self):
        candidate_columns = {
            name: data_type
            for _, name, data_type, _, _, _ in self.manager.conn.execute(
                "PRAGMA table_info('knowledge_candidate')"
            ).fetchall()
        }
        audit_columns = {
            name: data_type
            for _, name, data_type, _, _, _ in self.manager.conn.execute(
                "PRAGMA table_info('memory_audit_log')"
            ).fetchall()
        }

        self.assertEqual(candidate_columns["candidate_payload"], "VARIANT")
        self.assertEqual(candidate_columns["status"], "VARCHAR")
        self.assertEqual(audit_columns["decision_payload"], "VARIANT")
        self.assertEqual(audit_columns["tool_payload"], "VARIANT")

    def test_phase32_schema_is_initialized(self):
        tables = self.manager.conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
            """
        ).fetchall()

        self.assertIn(("conflict_registry",), tables)

    def test_phase32_schema_columns(self):
        conflict_columns = {
            name: data_type
            for _, name, data_type, _, _, _ in self.manager.conn.execute(
                "PRAGMA table_info('conflict_registry')"
            ).fetchall()
        }

        self.assertEqual(conflict_columns["details"], "VARIANT")
        self.assertEqual(conflict_columns["status"], "VARCHAR")

    def test_phase33_schema_is_initialized(self):
        tables = self.manager.conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
            """
        ).fetchall()

        self.assertIn(("knowledge_version_snapshot",), tables)

    def test_phase33_schema_columns(self):
        columns = {
            name: data_type
            for _, name, data_type, _, _, _ in self.manager.conn.execute(
                "PRAGMA table_info('knowledge_version_snapshot')"
            ).fetchall()
        }

        self.assertEqual(columns["knowledge_id"], "BIGINT")
        self.assertEqual(columns["snapshot_payload"], "VARIANT")
        self.assertEqual(columns["reason"], "VARCHAR")


if __name__ == "__main__":
    unittest.main()

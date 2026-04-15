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

        self.assertEqual(
            tables,
            [
                ("context_stream",),
                ("core_contract",),
                ("semantic_knowledge",),
            ],
        )

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


if __name__ == "__main__":
    unittest.main()

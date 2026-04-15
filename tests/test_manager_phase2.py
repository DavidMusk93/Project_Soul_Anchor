import datetime
import unittest

from soul_anchor.manager import MemoryManager


class TestMemoryManagerPhase2(unittest.TestCase):
    def setUp(self):
        self.manager = MemoryManager(db_path=":memory:")
        self.manager.connect()

    def tearDown(self):
        self.manager.close()

    def test_phase2_search_knowledge_prefers_relevance_then_stability(self):
        self.manager.save_knowledge(
            {
                "user_id": "david",
                "knowledge_type": "architecture",
                "title": "DuckDB 检索架构",
                "canonical_text": "DuckDB FTS can accelerate memory retrieval for the local memory store.",
                "keywords": "duckdb,fts,retrieval,memory",
                "stability_score": 0.75,
                "confidence_score": 0.8,
            }
        )
        self.manager.save_knowledge(
            {
                "user_id": "david",
                "knowledge_type": "architecture",
                "title": "稳定但不相关的规范",
                "canonical_text": "This document is very stable but unrelated to retrieval.",
                "keywords": "governance,policy",
                "stability_score": 0.99,
                "confidence_score": 0.99,
            }
        )

        results = self.manager.search_knowledge(
            query="duckdb retrieval",
            user_id="david",
            top_k=2,
        )

        self.assertEqual(results[0]["title"], "DuckDB 检索架构")
        self.assertGreaterEqual(results[0]["retrieval_score"], results[1]["retrieval_score"])

    def test_phase2_search_knowledge_returns_match_explanations(self):
        self.manager.save_knowledge(
            {
                "user_id": "david",
                "knowledge_type": "workflow",
                "title": "Pytest fixtures",
                "canonical_text": "pytest fixtures help manage setup and teardown.",
                "keywords": "pytest,fixtures,tdd",
                "stability_score": 0.9,
            }
        )

        result = self.manager.search_knowledge(
            query="pytest fixtures",
            user_id="david",
            top_k=1,
        )[0]

        self.assertIn("match_reasons", result)
        self.assertTrue(result["match_reasons"]["title"])
        self.assertTrue(result["match_reasons"]["keywords"])

    def test_phase2_search_recent_context_applies_recency_and_salience_weights(self):
        older_id = self.manager.save_episode(
            {
                "session_id": "session-4",
                "user_id": "david",
                "event_type": "task_state",
                "content": "继续推进 DuckDB FTS 检索。",
                "salience_score": 0.95,
            }
        )
        newer_id = self.manager.save_episode(
            {
                "session_id": "session-4",
                "user_id": "david",
                "event_type": "task_state",
                "content": "今天需要先实现检索排序与 token budget。",
                "salience_score": 0.80,
            }
        )
        self.manager.conn.execute(
            """
            UPDATE context_stream
            SET created_at = ?
            WHERE id = ?
            """,
            [
                datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                - datetime.timedelta(hours=12),
                older_id,
            ],
        )

        results = self.manager.search_recent_context_advanced(
            session_id="session-4",
            user_id="david",
            query="检索",
            top_k=2,
        )

        self.assertEqual([item["id"] for item in results], [newer_id, older_id])

    def test_phase2_build_context_packet_respects_budget_and_deduplicates(self):
        self.manager.upsert_core_contract("memory_principle", "L3 永远优先。", priority=900)
        self.manager.save_knowledge(
            {
                "user_id": "david",
                "knowledge_type": "architecture",
                "title": "混合检索",
                "canonical_text": "DuckDB FTS 与结构化过滤联合用于 memory retrieval。",
                "keywords": "duckdb,fts,retrieval,memory",
                "stability_score": 0.95,
            }
        )
        self.manager.save_episode(
            {
                "session_id": "session-5",
                "user_id": "david",
                "event_type": "task_state",
                "content": "今天要实现 DuckDB FTS 与结构化过滤联合用于 memory retrieval。",
                "summary": "与 L2 存在高重叠。",
                "salience_score": 0.9,
            }
        )

        packet = self.manager.build_context_packet(
            session_id="session-5",
            user_id="david",
            query="duckdb retrieval",
            l1_limit=5,
            l2_limit=5,
            max_chars=120,
            deduplicate=True,
        )

        self.assertLessEqual(packet["metadata"]["total_chars"], 120)
        self.assertEqual(len(packet["semantic_knowledge"]), 1)
        self.assertEqual(len(packet["recent_context"]), 0)


if __name__ == "__main__":
    unittest.main()

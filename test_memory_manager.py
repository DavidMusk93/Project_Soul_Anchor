import datetime
import unittest
from unittest.mock import patch

from MemoryManager import MemoryManager


class TestMemoryManager(unittest.TestCase):
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

    def test_save_episode_persists_l1_record_with_default_ttl(self):
        record_id = self.manager.save_episode(
            {
                "session_id": "session-1",
                "user_id": "david",
                "event_type": "idea",
                "content": "Allen 需要具备分层记忆。",
                "summary": "用户强调分层记忆方向。",
                "tags": ["memory", "architecture"],
            }
        )

        row = self.manager.conn.execute(
            """
            SELECT id, session_id, user_id, event_type, content, summary, tags,
                   importance_score, salience_score, expires_at IS NOT NULL
            FROM context_stream
            WHERE id = ?
            """,
            [record_id],
        ).fetchone()

        self.assertEqual(
            row,
            (
                record_id,
                "session-1",
                "david",
                "idea",
                "Allen 需要具备分层记忆。",
                "用户强调分层记忆方向。",
                "memory,architecture",
                0.5,
                0.5,
                True,
            ),
        )

    def test_search_recent_context_filters_expired_records(self):
        active_id = self.manager.save_episode(
            {
                "session_id": "session-2",
                "user_id": "david",
                "event_type": "task_state",
                "content": "当前在补 Phase 1 API。",
                "salience_score": 0.9,
            }
        )
        expired_id = self.manager.save_episode(
            {
                "session_id": "session-2",
                "user_id": "david",
                "event_type": "task_state",
                "content": "这是一条已经过期的旧记录。",
                "expires_at": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                - datetime.timedelta(days=1),
            }
        )

        results = self.manager.search_recent_context(
            session_id="session-2",
            user_id="david",
            query="Phase 1",
            top_k=5,
        )

        returned_ids = [item["id"] for item in results]
        self.assertIn(active_id, returned_ids)
        self.assertNotIn(expired_id, returned_ids)

    @patch.object(MemoryManager, "_embed_text", autospec=True)
    def test_recall_memory_returns_ranked_l2_matches_and_updates_access_stats(self, mock_embed_text):
        mock_embed_text.return_value = [0.1, 0.2, 0.3]

        best_id = self.manager.save_knowledge(
            {
                "user_id": "david",
                "knowledge_type": "workflow",
                "title": "Pytest fixtures",
                "canonical_text": "pytest fixtures help manage setup and teardown for TDD.",
                "keywords": "pytest,testing,tdd",
                "stability_score": 0.91,
                "confidence_score": 0.88,
            }
        )
        second_id = self.manager.save_knowledge(
            {
                "user_id": "david",
                "knowledge_type": "workflow",
                "title": "DuckDB memory",
                "canonical_text": "DuckDB supports isolated in-memory databases for tests.",
                "keywords": "duckdb,testing",
            }
        )
        self.manager.save_knowledge(
            {
                "user_id": "david",
                "knowledge_type": "life",
                "title": "Gardening note",
                "canonical_text": "Tomatoes need sunlight and regular watering.",
                "keywords": "garden,plants",
            }
        )

        results = self.manager.recall_memory(query="pytest", user_id="david", top_k=2)

        self.assertEqual([item["id"] for item in results], [best_id, second_id])
        self.assertEqual([item["title"] for item in results], ["Pytest fixtures", "DuckDB memory"])
        mock_embed_text.assert_called_once_with(self.manager, "pytest")

        access_row = self.manager.conn.execute(
            """
            SELECT access_count, last_accessed_at IS NOT NULL
            FROM semantic_knowledge
            WHERE id = ?
            """,
            [best_id],
        ).fetchone()
        self.assertEqual(access_row, (1, True))

    def test_build_context_packet_prioritizes_l3_then_l2_then_l1(self):
        self.manager.upsert_core_contract("identity", "Allen 是持续进化的协作智能体。", priority=100)
        self.manager.upsert_core_contract("memory_principle", "必须区分 L1/L2/L3。", priority=900)

        self.manager.save_knowledge(
            {
                "user_id": "david",
                "knowledge_type": "architecture",
                "title": "分层记忆原则",
                "canonical_text": "L3 优先，L2 按稳定知识召回，L1 补充近期上下文。",
                "keywords": "memory,layered,context",
                "stability_score": 0.95,
            }
        )
        self.manager.save_episode(
            {
                "session_id": "session-3",
                "user_id": "david",
                "event_type": "task_state",
                "content": "我们正在实现 context builder。",
                "salience_score": 0.8,
            }
        )

        packet = self.manager.build_context_packet(
            session_id="session-3",
            user_id="david",
            query="分层记忆 context",
            l1_limit=2,
            l2_limit=2,
        )

        self.assertEqual(
            [item["contract_key"] for item in packet["core_contract"]],
            ["memory_principle", "identity"],
        )
        self.assertEqual(packet["semantic_knowledge"][0]["title"], "分层记忆原则")
        self.assertEqual(packet["recent_context"][0]["content"], "我们正在实现 context builder。")


class TestPhase2RetrievalSpecs(unittest.TestCase):
    def setUp(self):
        self.manager = MemoryManager(db_path=":memory:")
        self.manager.connect()

    def tearDown(self):
        self.manager.close()

    @unittest.expectedFailure
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

    @unittest.expectedFailure
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
        self.assertIn("title", result["match_reasons"])
        self.assertIn("keywords", result["match_reasons"])

    @unittest.expectedFailure
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

    @unittest.expectedFailure
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

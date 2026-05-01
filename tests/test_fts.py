"""
FTS lifecycle tests.

Covers:
  - FTS index is built at connection time (setup_fts called from connect)
  - FTS index is refreshed after writes (save_episode / save_knowledge)
  - BM25 search returns results with correct scores
  - Newly written records are findable after refresh
  - Search without matching content returns empty
  - Multiple concurrent FTS issues don't break
"""

from __future__ import annotations

import unittest

from soul_anchor.manager import MemoryManager


class TestFTSLifecycle(unittest.TestCase):
    """FTS index build/refresh/search at integration level."""

    def setUp(self):
        self.manager = MemoryManager(db_path=":memory:")
        self.manager.connect()

    def tearDown(self):
        self.manager.close()

    # ------------------------------------------------------------------
    # L2: semantic_knowledge  FTS
    # ------------------------------------------------------------------

    def test_knowledge_fts_returns_matches(self):
        """Write a L2 record, then find it by BM25 keyword search."""
        kid = self.manager.save_knowledge({
            "user_id": "david",
            "knowledge_type": "preference",
            "title": "color preference",
            "canonical_text": "David prefers dark blue over light blue",
            "keywords": "color, blue, preference",
        })

        results = self.manager.search_knowledge_fts(
            query="blue",
            user_id="david",
            top_k=5,
        )

        ids = [r["id"] for r in results]
        self.assertIn(kid, ids, msg="Newly written record should be findable via FTS")
        self.assertGreater(results[0]["bm25_score"], 0.0, msg="BM25 score should be > 0")

    def test_knowledge_fts_filters_by_user(self):
        """FTS results should respect user_id filter."""
        self.manager.save_knowledge({
            "user_id": "david",
            "knowledge_type": "preference",
            "title": "coffee order",
            "canonical_text": "double espresso no sugar",
            "keywords": "coffee, espresso",
        })
        self.manager.save_knowledge({
            "user_id": "alice",
            "knowledge_type": "preference",
            "title": "coffee order",
            "canonical_text": "latte with oat milk",
            "keywords": "coffee, latte",
        })

        david_results = self.manager.search_knowledge_fts(
            query="coffee",
            user_id="david",
            top_k=10,
        )
        alice_results = self.manager.search_knowledge_fts(
            query="coffee",
            user_id="alice",
            top_k=10,
        )

        for r in david_results:
            self.assertEqual(r["id"] is not None, True)
        david_ids = {r["id"] for r in david_results}
        alice_ids = {r["id"] for r in alice_results}
        self.assertFalse(
            david_ids & alice_ids,
            msg="FTS results for different users should not overlap",
        )

    def test_knowledge_fts_empty_when_no_match(self):
        """Search for content that doesn't exist returns empty list."""
        self.manager.save_knowledge({
            "user_id": "david",
            "knowledge_type": "preference",
            "title": "color preference",
            "canonical_text": "David likes blue",
            "keywords": "color",
        })

        results = self.manager.search_knowledge_fts(
            query="zzzznotfound",
            user_id="david",
            top_k=5,
        )
        self.assertEqual(len(results), 0, msg="No-match query should return empty")

    def test_knowledge_fts_stale_index_needs_refresh(self):
        """
        Verify that FTS index must be refreshed to see new records.
        Directly insert without going through save_knowledge (no implicit refresh).
        """
        self.manager.conn.execute(
            """
            INSERT INTO semantic_knowledge
                (user_id, knowledge_type, title, canonical_text, keywords, is_active)
            VALUES ('david', 'note', 'secret', 'hidden content for fts test', 'hidden', TRUE)
            """
        )

        # Without refresh, FTS should not find new row
        before = self.manager.search_knowledge_fts(
            query="hidden",
            user_id="david",
            top_k=5,
        )
        self.assertEqual(len(before), 0, msg="Without refresh, new row should be invisible")

        # After explicit refresh, FTS should find it
        self.manager.refresh_fts()
        after = self.manager.search_knowledge_fts(
            query="hidden",
            user_id="david",
            top_k=5,
        )
        self.assertGreater(len(after), 0, msg="After refresh, new row should be findable")

    def test_knowledge_fts_respects_is_active(self):
        """FTS should skip inactive (soft-deleted) records."""
        self.manager.save_knowledge({
            "user_id": "david",
            "knowledge_type": "preference",
            "title": "deleted item",
            "canonical_text": "this should not appear in search",
            "keywords": "deleted",
            "is_active": False,
        })

        results = self.manager.search_knowledge_fts(
            query="deleted",
            user_id="david",
            top_k=5,
        )
        self.assertEqual(len(results), 0, msg="Inactive records should not be searchable")

    # ------------------------------------------------------------------
    # L1: context_stream  FTS
    # ------------------------------------------------------------------

    def test_context_fts_returns_matches(self):
        """Write a L1 record, then find it by BM25 keyword search."""
        eid = self.manager.save_episode({
            "session_id": "session-1",
            "user_id": "david",
            "event_type": "idea",
            "content": "We should use DuckDB for memory storage",
            "summary": "duckdb memory idea",
            "tags": ["database", "memory"],
        })

        results = self.manager.search_context_fts(
            query="duckdb",
            session_id="session-1",
            user_id="david",
            top_k=5,
        )

        ids = [r["id"] for r in results]
        self.assertIn(eid, ids, msg="New context record should be findable via FTS")
        self.assertGreater(results[0]["bm25_score"], 0.0, msg="BM25 score should be > 0")

    def test_context_fts_filters_expired(self):
        """FTS should skip expired context_stream records."""
        import datetime

        past = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=30)
        self.manager.save_episode({
            "session_id": "session-1",
            "user_id": "david",
            "event_type": "idea",
            "content": "old expired idea about fts testing",
            "summary": "expired",
            "tags": ["test"],
            "expires_at": past,
        })

        results = self.manager.search_context_fts(
            query="expired",
            session_id="session-1",
            user_id="david",
            top_k=5,
        )
        self.assertEqual(len(results), 0, msg="Expired records should not appear")

    # ------------------------------------------------------------------
    # Multiple writes & concurrent search
    # ------------------------------------------------------------------

    def test_multiple_writes_all_findable(self):
        """Write 5 knowledge records, verify all are findable."""
        ids = []
        for i in range(5):
            kid = self.manager.save_knowledge({
                "user_id": "david",
                "knowledge_type": "fact",
                "title": f"fact #{i}",
                "canonical_text": f"this is fact number {i} about programming",
                "keywords": f"fact, programming, {i}",
            })
            ids.append(kid)

        for kid in ids:
            results = self.manager.search_knowledge_fts(
                query="fact",
                user_id="david",
                top_k=10,
            )
            found_ids = {r["id"] for r in results}
            self.assertIn(kid, found_ids, msg=f"Record {kid} should be findable after batch writes")

    def test_refresh_twice_no_error(self):
        """Calling refresh_fts multiple times should not error."""
        self.manager.refresh_fts()
        self.manager.refresh_fts()
        # If we got here, no exception was raised
        self.assertTrue(True, msg="Multiple refresh_fts calls should succeed")


if __name__ == "__main__":
    unittest.main()

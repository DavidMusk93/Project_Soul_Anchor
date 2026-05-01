from __future__ import annotations

import contextlib
import datetime
import logging
import os
from typing import Any

import duckdb

from soul_anchor.db import init_schema
from soul_anchor.db.variant import variant_sql_literal
from soul_anchor.embedding.dummy import DUMMY_EMBEDDING_DIM, DUMMY_EMBEDDING_MODEL_ID, embed_text
from soul_anchor.retrieval import (
    build_context_packet as build_context_packet_impl,
    recall_memory as recall_memory_impl,
    search_knowledge as search_knowledge_impl,
    search_recent_context as search_recent_context_impl,
    search_recent_context_advanced as search_recent_context_advanced_impl,
)
from soul_anchor.retrieval.fts import (
    FtsTable,
    fts_search_knowledge as fts_search_knowledge_impl,
    fts_search_context as fts_search_context_impl,
    refresh_fts_indexes as refresh_fts_indexes_impl,
    setup_fts_index as setup_fts_index_impl,
)


logger = logging.getLogger(__name__)

class MemoryManager:
    """
    Soul Anchor 记忆系统核心基石 (Phase 1)
    提供与本地 DuckDB 单文件数据库的连接管理及严谨的 Schema 初始化。
    """
    
    def __init__(self, db_path="aime_evolution.duckdb"):
        self.db_path = db_path
        self.conn = None
        # Deferred FTS refresh state. When depth > 0, writes only mark which
        # tables became dirty; the actual rebuild happens when the outermost
        # defer_fts_refresh() block exits.
        self._fts_defer_depth: int = 0
        self._fts_dirty_tables: set[FtsTable] = set()

    def connect(self):
        """建立连接、初始化 Schema、构建 FTS 索引"""
        self.conn = duckdb.connect(":memory:")
        self.conn.execute(
            f"ATTACH '{self.db_path}' AS soul_anchor_db (STORAGE_VERSION 'v1.5.0')"
        )
        self.conn.execute("USE soul_anchor_db")
        self._init_schema()
        self.setup_fts()

    def _init_schema(self):
        init_schema(self.conn)

    def _ensure_connected(self) -> None:
        if self.conn is None:
            raise RuntimeError("MemoryManager is not connected. Call connect() first.")

    def _now_utc(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    def _default_l1_ttl(self) -> datetime.timedelta:
        return datetime.timedelta(days=7)

    def _variant_sql_literal(self, value: Any) -> str:
        return variant_sql_literal(value)

    def _embed_text(self, text: str) -> list[float]:
        """
        Phase 3.4: deterministic dummy embedding for local hybrid retrieval.
        This can be swapped to a real embedding service without changing call sites.
        """
        return embed_text(text, dim=DUMMY_EMBEDDING_DIM)

    def save_episode(self, event: dict[str, Any]) -> int:
        """
        Save a L1 episodic record into context_stream.
        Returns the inserted row id.
        """
        self._ensure_connected()

        session_id = event["session_id"]
        user_id = event["user_id"]
        event_type = event["event_type"]
        content = event["content"]

        topic = event.get("topic")
        summary = event.get("summary")

        tags_value = event.get("tags")
        if isinstance(tags_value, list):
            tags = ",".join([str(t) for t in tags_value])
        else:
            tags = tags_value

        importance_score = float(event.get("importance_score", 0.5))
        salience_score = float(event.get("salience_score", 0.5))
        source_turn_id = event.get("source_turn_id")
        metadata = event.get("metadata")
        embedding = event.get("embedding")

        expires_at = event.get("expires_at")
        if expires_at is None:
            expires_at = self._now_utc() + self._default_l1_ttl()

        is_archived = bool(event.get("is_archived", False))

        row = self.conn.execute(
            f"""
            INSERT INTO context_stream (
                session_id, user_id, topic, event_type, content, summary, tags,
                importance_score, salience_score, source_turn_id, metadata, embedding,
                expires_at, is_archived
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {self._variant_sql_literal(metadata)}, ?, ?, ?)
            RETURNING id
            """,
            [
                session_id,
                user_id,
                topic,
                event_type,
                content,
                summary,
                tags,
                importance_score,
                salience_score,
                source_turn_id,
                embedding,
                expires_at,
                is_archived,
            ],
        ).fetchone()

        self.refresh_fts(tables=("context_stream",))
        return int(row[0])

    def search_recent_context(
        self,
        *,
        session_id: str,
        user_id: str,
        query: str,
        top_k: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Search L1 records by simple keyword matching and recency/salience sorting.
        Filters expired/archived items.
        """
        self._ensure_connected()
        return search_recent_context_impl(
            self.conn,
            session_id=session_id,
            user_id=user_id,
            query=query,
            top_k=top_k,
            now=self._now_utc(),
        )

    def search_recent_context_advanced(
        self,
        *,
        session_id: str,
        user_id: str,
        query: str,
        top_k: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Phase 2: Recency-first ranking with salience tie-breaker.
        """
        self._ensure_connected()
        return search_recent_context_advanced_impl(
            self.conn,
            session_id=session_id,
            user_id=user_id,
            query=query,
            top_k=top_k,
            now=self._now_utc(),
        )

    def save_knowledge(self, knowledge: dict[str, Any]) -> int:
        """Insert a L2 semantic knowledge record. Returns inserted id."""
        self._ensure_connected()

        user_id = knowledge["user_id"]
        knowledge_type = knowledge["knowledge_type"]
        title = knowledge["title"]
        canonical_text = knowledge["canonical_text"]

        keywords = knowledge.get("keywords")
        source_refs = knowledge.get("source_refs")
        confidence_score = float(knowledge.get("confidence_score", 0.7))
        stability_score = float(knowledge.get("stability_score", 0.7))
        metadata = knowledge.get("metadata")
        embedding = knowledge.get("embedding")
        is_active = bool(knowledge.get("is_active", True))

        # Phase 3.4 (MVP): auto-generate L2 embeddings when not provided.
        if embedding is None:
            text_for_embedding = "\n".join(
                [
                    str(title or ""),
                    str(keywords or ""),
                    str(canonical_text or ""),
                ]
            ).strip()
            embedding = embed_text(text_for_embedding, dim=DUMMY_EMBEDDING_DIM)

            if metadata is None:
                metadata = {}
            if isinstance(metadata, dict):
                metadata.setdefault("embedding_model", DUMMY_EMBEDDING_MODEL_ID)
                metadata.setdefault("embedding_dim", DUMMY_EMBEDDING_DIM)

        row = self.conn.execute(
            f"""
            INSERT INTO semantic_knowledge (
                user_id, knowledge_type, title, canonical_text, keywords, source_refs,
                confidence_score, stability_score, metadata, embedding, is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, {self._variant_sql_literal(metadata)}, ?, ?)
            RETURNING id
            """,
            [
                user_id,
                knowledge_type,
                title,
                canonical_text,
                keywords,
                source_refs,
                confidence_score,
                stability_score,
                embedding,
                is_active,
            ],
        ).fetchone()

        self.refresh_fts(tables=("semantic_knowledge",))
        return int(row[0])

    def recall_memory(self, *, query: str, user_id: str, top_k: int = 10) -> list[dict[str, Any]]:
        """
        Recall L2 knowledge by lightweight ranking (Phase 1 compatibility).

        Keyword-only selection: LIMIT is applied on kw_score. A vector_score
        is attached to each returned row for observability but does NOT
        participate in row selection. For true hybrid retrieval use
        `search_knowledge(..., use_embedding=True)`.
        """
        self._ensure_connected()
        return recall_memory_impl(
            self.conn,
            query=query,
            user_id=user_id,
            top_k=top_k,
            now=self._now_utc(),
            embed_text=self._embed_text,
        )

    def search_knowledge(
        self,
        *,
        query: str,
        user_id: str,
        top_k: int = 10,
        use_embedding: bool = False,
        candidate_pool: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Search L2 semantic knowledge for a user.

        Notes:
        - Default behavior is keyword-based ranking (Phase 2) and is stable/backward compatible.
        - When `use_embedding=True`, the function performs a hybrid rerank:
          1) build a keyword-based candidate pool
          2) compute cosine similarity between query embedding and row embeddings
          3) rerank the pool by a hybrid score (keyword score + weighted vector score)

        Args:
            query: Natural language query.
            user_id: Target user id.
            top_k: Number of results to return.
            use_embedding:
                If True, enable embedding-based reranking. This improves robustness for
                input variants (e.g. whitespace/format changes, paraphrases) at the cost
                of extra CPU (cosine similarity over the candidate pool).
                If False, keep pure keyword ranking.
            candidate_pool:
                Only used when `use_embedding=True`.
                Controls how many keyword-ranked candidates are pulled into the pool
                before vector reranking. Larger pools can improve recall but increase
                CPU cost linearly.

                - None (default): uses an internal heuristic (roughly max(top_k * 10, 50)).
                - int: explicit pool size; will be clamped to at least top_k.

        Returns:
            A list of dicts. When `use_embedding=False`, items include:
            - id/title/canonical_text/keywords/confidence_score/stability_score/retrieval_score/match_reasons

            When `use_embedding=True`, items additionally include:
            - vector_score: cosine(query_vec, row_vec)
            - hybrid_score: retrieval_score + vector_score * weight

        Example:

        ```python
        mm.search_knowledge(query="abcxyz", user_id="david", top_k=5, use_embedding=True)
        ```
        """
        self._ensure_connected()
        return search_knowledge_impl(
            self.conn,
            query=query,
            user_id=user_id,
            top_k=top_k,
            use_embedding=use_embedding,
            candidate_pool=candidate_pool,
            now=self._now_utc(),
            embed_text=self._embed_text,
        )

    def upsert_core_contract(self, contract_key: str, contract_value: str, *, priority: int = 100) -> None:
        """Insert or update a L3 contract item."""
        self._ensure_connected()
        now = self._now_utc()
        self.conn.execute(
            """
            INSERT INTO core_contract (contract_key, contract_value, priority, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (contract_key) DO UPDATE
            SET contract_value = excluded.contract_value,
                priority = excluded.priority,
                updated_at = excluded.updated_at
            """,
            [contract_key, contract_value, int(priority), now],
        )

    def load_core_contract(self) -> list[dict[str, Any]]:
        """Load all L3 contract items ordered by priority desc."""
        self._ensure_connected()
        rows = self.conn.execute(
            """
            SELECT contract_key, contract_value, priority, updated_at
            FROM core_contract
            ORDER BY priority DESC, contract_key ASC
            """
        ).fetchall()

        return [
            {
                "contract_key": contract_key,
                "contract_value": contract_value,
                "priority": int(priority),
                "updated_at": updated_at,
            }
            for (contract_key, contract_value, priority, updated_at) in rows
        ]

    def build_context_packet(
        self,
        *,
        session_id: str,
        user_id: str,
        query: str,
        l1_limit: int = 10,
        l2_limit: int = 10,
        max_chars: int | None = None,
        deduplicate: bool = False,
    ) -> dict[str, Any]:
        """
        Assemble a structured context packet (L3 -> L2 -> L1).
        Phase 2: keeps Phase 1 behavior but adds optional budget and deduplication hooks.
        """
        return build_context_packet_impl(
            session_id=session_id,
            user_id=user_id,
            query=query,
            l1_limit=l1_limit,
            l2_limit=l2_limit,
            max_chars=max_chars,
            deduplicate=deduplicate,
            load_core_contract=self.load_core_contract,
            search_knowledge=self.search_knowledge,
            search_recent_context_advanced=self.search_recent_context_advanced,
        )

    # ------------------------------------------------------------------
    # FTS (Full Text Search) — DuckDB fts extension, BM25 ranking
    # https://duckdb.org/docs/extensions/full_text_search
    #
    # Index lifecycle:
    #   - Built once at connection time (setup_fts called from connect())
    #   - Refreshed per-table after each write (table scope = context_stream
    #     for save_episode, semantic_knowledge for save_knowledge)
    #   - Search methods never rebuild — zero read-path overhead
    #   - Batch writes can wrap calls in `defer_fts_refresh()` to rebuild
    #     dirty tables once at the end of the block
    # ------------------------------------------------------------------

    def setup_fts(self) -> None:
        """
        Load the FTS extension and build BM25 indexes on semantic_knowledge
        and context_stream. Called once at connection time. Safe to call
        multiple times. No-op in offline environments where the FTS
        extension cannot be installed.
        """
        logger.info("Building FTS indexes at connection time...")
        self._ensure_connected()
        setup_fts_index_impl(self.conn)

    def refresh_fts(
        self,
        *,
        tables: tuple[FtsTable, ...] = ("semantic_knowledge", "context_stream"),
    ) -> None:
        """
        Rebuild BM25 indexes for the given tables. Only the affected tables
        should be passed after a write. DuckDB FTS has no incremental
        update — full rebuild of the listed tables is required.

        When inside a `defer_fts_refresh()` block the rebuild is skipped
        and the listed tables are marked dirty; the outermost block exit
        rebuilds all dirty tables once.
        """
        self._ensure_connected()
        if self._fts_defer_depth > 0:
            self._fts_dirty_tables.update(tables)
            return
        refresh_fts_indexes_impl(self.conn, tables=tables)

    @contextlib.contextmanager
    def defer_fts_refresh(self):
        """
        Coalesce multiple FTS rebuilds during a batch write.

        Inside the block, writes mark the tables they touch as dirty instead
        of rebuilding immediately. On exit of the OUTERMOST block, every
        dirty table is rebuilt once. Nested usage is supported.

        Example:
            with manager.defer_fts_refresh():
                for item in batch:
                    manager.save_knowledge(item)
            # FTS rebuilt once here
        """
        self._fts_defer_depth += 1
        try:
            yield
        finally:
            self._fts_defer_depth -= 1
            if self._fts_defer_depth == 0 and self._fts_dirty_tables:
                dirty = tuple(sorted(self._fts_dirty_tables))
                self._fts_dirty_tables.clear()
                refresh_fts_indexes_impl(self.conn, tables=dirty)

    def search_knowledge_fts(
        self,
        *,
        query: str,
        user_id: str,
        top_k: int = 10,
    ) -> list[dict[str, Any]]:
        """
        BM25 full-text search over L2 semantic_knowledge.
        Index is kept fresh by setup_fts (connect) + refresh_fts (after writes).
        Returns BM25-ranked results with bm25_score.
        """
        self._ensure_connected()
        return fts_search_knowledge_impl(
            self.conn,
            query=query,
            user_id=user_id,
            top_k=top_k,
        )

    def search_context_fts(
        self,
        *,
        query: str,
        session_id: str,
        user_id: str,
        top_k: int = 10,
    ) -> list[dict[str, Any]]:
        """
        BM25 full-text search over L1 context_stream.
        Index is kept fresh by setup_fts (connect) + refresh_fts (after writes).
        """
        self._ensure_connected()
        return fts_search_context_impl(
            self.conn,
            query=query,
            session_id=session_id,
            user_id=user_id,
            top_k=top_k,
            now=self._now_utc(),
        )

    def close(self):
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    db_file = "aime_evolution.duckdb"
    print("=== 初始化 Project Soul Anchor 基础架构 ===")
    print(f"目标数据库: {os.path.abspath(db_file)}")
    
    manager = MemoryManager(db_file)
    
    try:
        manager.connect()
        print("[SUCCESS] Schema Initialization Complete: L1(context_stream), L2(semantic_knowledge), L3(core_contract) tables created successfully.")
    except Exception as e:
        print(f"[ERROR] Schema 初始化失败: {e}")
    finally:
        manager.close()

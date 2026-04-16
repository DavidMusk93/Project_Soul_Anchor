from __future__ import annotations

import datetime
import os
from typing import Any

import duckdb

from soul_anchor.db import init_schema
from soul_anchor.db.variant import variant_sql_literal
from soul_anchor.retrieval import (
    build_context_packet as build_context_packet_impl,
    recall_memory as recall_memory_impl,
    search_knowledge as search_knowledge_impl,
    search_recent_context as search_recent_context_impl,
    search_recent_context_advanced as search_recent_context_advanced_impl,
)

class MemoryManager:
    """
    Soul Anchor 记忆系统核心基石 (Phase 1)
    提供与本地 DuckDB 单文件数据库的连接管理及严谨的 Schema 初始化。
    """
    
    def __init__(self, db_path="aime_evolution.duckdb"):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """建立连接并确保核心 Schema 就绪"""
        self.conn = duckdb.connect(":memory:")
        self.conn.execute(
            f"ATTACH '{self.db_path}' AS soul_anchor_db (STORAGE_VERSION 'v1.5.0')"
        )
        self.conn.execute("USE soul_anchor_db")
        self._init_schema()

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
        Phase 1 placeholder.
        In Phase 2/3 this should call a real embedder and store vectors.
        """
        _ = text
        return []

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

        return int(row[0])

    def recall_memory(self, *, query: str, user_id: str, top_k: int = 10) -> list[dict[str, Any]]:
        """
        Recall L2 knowledge by lightweight ranking (Phase 1).
        Uses keyword matching; in Phase 2 this should be FTS/hybrid retrieval.
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
    ) -> list[dict[str, Any]]:
        """
        Phase 2: Search semantic_knowledge with relevance-first ranking and explanations.

        Notes:
        - This is still a lightweight keyword-based implementation.
        - FTS/hybrid retrieval can replace the scoring internals later without changing the API.
        """
        self._ensure_connected()
        return search_knowledge_impl(
            self.conn,
            query=query,
            user_id=user_id,
            top_k=top_k,
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

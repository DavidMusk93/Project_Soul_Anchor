from __future__ import annotations

import datetime
import json
import os
from typing import Any

import duckdb

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
        self.conn = duckdb.connect(self.db_path)
        self._init_schema()

    def _init_schema(self):
        # L3 Core Persona / Core Contract (key-value, highest priority constraints)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS core_contract (
                contract_key VARCHAR PRIMARY KEY,
                contract_value TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 100,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        # L1 Episodic Memory (short-lived event stream)
        self.conn.execute(
            """
            CREATE SEQUENCE IF NOT EXISTS seq_context_stream START 1;
            CREATE TABLE IF NOT EXISTS context_stream (
                id BIGINT PRIMARY KEY DEFAULT nextval('seq_context_stream'),
                session_id VARCHAR NOT NULL,
                user_id VARCHAR NOT NULL,
                topic VARCHAR,
                event_type VARCHAR NOT NULL,
                content TEXT NOT NULL,
                summary TEXT,
                tags TEXT,
                importance_score DOUBLE DEFAULT 0.5,
                salience_score DOUBLE DEFAULT 0.5,
                source_turn_id VARCHAR,
                metadata VARIANT,
                embedding FLOAT[],
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                is_archived BOOLEAN DEFAULT FALSE
            );
            """
        )

        # L2 Semantic Memory (stable knowledge distilled from events)
        self.conn.execute(
            """
            CREATE SEQUENCE IF NOT EXISTS seq_semantic_knowledge START 1;
            CREATE TABLE IF NOT EXISTS semantic_knowledge (
                id BIGINT PRIMARY KEY DEFAULT nextval('seq_semantic_knowledge'),
                user_id VARCHAR NOT NULL,
                knowledge_type VARCHAR NOT NULL,
                title VARCHAR NOT NULL,
                canonical_text TEXT NOT NULL,
                keywords TEXT,
                source_refs TEXT,
                confidence_score DOUBLE DEFAULT 0.7,
                stability_score DOUBLE DEFAULT 0.7,
                metadata VARIANT,
                embedding FLOAT[],
                access_count BIGINT DEFAULT 0,
                last_accessed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            );
            """
        )

    def _ensure_connected(self) -> None:
        if self.conn is None:
            raise RuntimeError("MemoryManager is not connected. Call connect() first.")

    def _now_utc(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    def _default_l1_ttl(self) -> datetime.timedelta:
        return datetime.timedelta(days=7)

    def _variant_sql_literal(self, value: Any) -> str:
        if value is None:
            return "NULL"
        payload = json.dumps(value, ensure_ascii=False).replace("'", "''")
        return f"json('{payload}')::VARIANT"

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
        now = self._now_utc()

        terms = [t.strip() for t in query.split() if t.strip()]
        if not terms:
            terms = [query]

        rows = self.conn.execute(
            """
            SELECT
                id, session_id, user_id, topic, event_type, content, summary, tags,
                importance_score, salience_score, created_at, expires_at
            FROM context_stream
            WHERE session_id = ?
              AND user_id = ?
              AND is_archived = FALSE
              AND (expires_at IS NULL OR expires_at > ?)
            """,
            [session_id, user_id, now],
        ).fetchall()

        def _contains(text: str | None, term: str) -> bool:
            if not text:
                return False
            return term.lower() in text.lower()

        scored: list[tuple[int, int, float, datetime.datetime | None, tuple[Any, ...]]] = []
        for row in rows:
            (
                row_id,
                r_session_id,
                r_user_id,
                topic,
                event_type,
                content,
                summary,
                tags,
                importance_score,
                salience_score,
                created_at,
                expires_at,
            ) = row
            score = 0
            for term in terms:
                if _contains(content, term):
                    score += 3
                if _contains(summary, term):
                    score += 2
                if _contains(tags, term):
                    score += 1
            if score > 0:
                scored.append((1, score, float(salience_score), created_at, row))

        scored.sort(key=lambda x: (x[0], x[1], x[2], x[3] or datetime.datetime.min), reverse=True)
        selected_rows = [x[4] for x in scored[: int(top_k)]]

        results: list[dict[str, Any]] = []
        for (
            row_id,
            r_session_id,
            r_user_id,
            topic,
            event_type,
            content,
            summary,
            tags,
            importance_score,
            salience_score,
            created_at,
            expires_at,
        ) in selected_rows:
            results.append(
                {
                    "id": int(row_id),
                    "session_id": r_session_id,
                    "user_id": r_user_id,
                    "topic": topic,
                    "event_type": event_type,
                    "content": content,
                    "summary": summary,
                    "tags": tags,
                    "importance_score": float(importance_score),
                    "salience_score": float(salience_score),
                    "created_at": created_at,
                    "expires_at": expires_at,
                }
            )
        return results

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
        _ = self._embed_text(query)
        terms = [t.strip() for t in query.split() if t.strip()]
        if not terms:
            terms = [query]

        rows = self.conn.execute(
            """
            SELECT
                id, title, canonical_text, keywords, confidence_score, stability_score
            FROM semantic_knowledge
            WHERE is_active = TRUE
              AND user_id = ?
            """,
            [user_id],
        ).fetchall()

        def _score(text: str | None, term: str) -> bool:
            if not text:
                return False
            return term.lower() in text.lower()

        scored: list[tuple[int, int, float, float, tuple[Any, ...]]] = []
        for row in rows:
            row_id, title, canonical_text, keywords, confidence_score, stability_score = row
            score = 0
            for term in terms:
                if _score(title, term):
                    score += 3
                if _score(keywords, term):
                    score += 2
                if _score(canonical_text, term):
                    score += 1
            scored.append((1 if score > 0 else 0, score, float(stability_score), float(confidence_score), row))

        # Matches first (has_match), then score, then stability/confidence.
        scored.sort(key=lambda x: (x[0], x[1], x[2], x[3]), reverse=True)
        selected_rows = [x[4] for x in scored[: int(top_k)]]

        now = self._now_utc()
        ids = [int(r[0]) for r in selected_rows]
        if ids:
            self.conn.execute(
                """
                UPDATE semantic_knowledge
                SET access_count = access_count + 1,
                    last_accessed_at = ?,
                    updated_at = ?
                WHERE id IN (SELECT unnest(?::BIGINT[]))
                """,
                [now, now, ids],
            )

        results: list[dict[str, Any]] = []
        for row_id, title, canonical_text, keywords, confidence_score, stability_score in selected_rows:
            results.append(
                {
                    "id": int(row_id),
                    "title": title,
                    "content": canonical_text,
                    "canonical_text": canonical_text,
                    "keywords": keywords,
                    "confidence_score": float(confidence_score),
                    "stability_score": float(stability_score),
                }
            )
        return results

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
    ) -> dict[str, Any]:
        """
        Assemble a structured context packet (L3 -> L2 -> L1).
        Phase 1: pure DB reads + light ranking.
        """
        core_contract = self.load_core_contract()
        semantic_knowledge = self.recall_memory(query=query, user_id=user_id, top_k=l2_limit)
        recent_context = self.search_recent_context(
            session_id=session_id, user_id=user_id, query=query, top_k=l1_limit
        )

        return {
            "core_contract": core_contract,
            "semantic_knowledge": semantic_knowledge,
            "recent_context": recent_context,
        }

    def close(self):
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    db_file = "aime_evolution.duckdb"
    print(f"=== 初始化 Project Soul Anchor 基础架构 ===")
    print(f"目标数据库: {os.path.abspath(db_file)}")
    
    manager = MemoryManager(db_file)
    
    try:
        manager.connect()
        print("[SUCCESS] Schema Initialization Complete: L1(context_stream), L2(semantic_knowledge), L3(core_contract) tables created successfully.")
    except Exception as e:
        print(f"[ERROR] Schema 初始化失败: {e}")
    finally:
        manager.close()

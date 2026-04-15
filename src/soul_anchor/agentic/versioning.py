from __future__ import annotations

import datetime
from typing import Any

from soul_anchor.manager import MemoryManager
from soul_anchor.db.variant import variant_sql_literal


class KnowledgeVersioning:
    """
    Phase 3.3 knowledge version management (MVP).

    - create_snapshot: persist a snapshot of semantic_knowledge into knowledge_version_snapshot
    - rollback_to_snapshot: restore semantic_knowledge fields from a snapshot
    """

    def __init__(self, manager: MemoryManager):
        self.manager = manager

    def _ensure_connected(self) -> None:
        if self.manager.conn is None:
            raise RuntimeError("MemoryManager is not connected. Call connect() first.")

    def _now_utc(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    def _variant_literal(self, value: Any) -> str:
        return variant_sql_literal(value)

    def _write_audit(self, *, action_type: str, user_id: str, tool_payload: dict[str, Any], result_summary: str):
        now = self._now_utc()
        tool_sql = self._variant_literal(tool_payload)
        self.manager.conn.execute(
            f"""
            INSERT INTO memory_audit_log (
                action_type, session_id, user_id, decision_payload, tool_payload, result_summary, created_at
            )
            VALUES (?, NULL, ?, NULL, {tool_sql}, ?, ?)
            """,
            [action_type, user_id, result_summary, now],
        )

    def create_snapshot(self, *, knowledge_id: int, reason: str | None = None) -> int:
        self._ensure_connected()
        row = self.manager.conn.execute(
            """
            SELECT
                id, user_id, knowledge_type, title, canonical_text, keywords, source_refs,
                confidence_score, stability_score, metadata, embedding, is_active
            FROM semantic_knowledge
            WHERE id = ?
            """,
            [int(knowledge_id)],
        ).fetchone()
        if row is None:
            raise ValueError(f"Knowledge not found: {knowledge_id}")

        (
            _id,
            user_id,
            knowledge_type,
            title,
            canonical_text,
            keywords,
            source_refs,
            confidence_score,
            stability_score,
            metadata,
            embedding,
            is_active,
        ) = row

        payload = {
            "knowledge_id": int(_id),
            "user_id": user_id,
            "knowledge_type": knowledge_type,
            "title": title,
            "canonical_text": canonical_text,
            "keywords": keywords,
            "source_refs": source_refs,
            "confidence_score": float(confidence_score),
            "stability_score": float(stability_score),
            "metadata": metadata,
            "embedding": list(embedding) if embedding is not None else None,
            "is_active": bool(is_active),
        }

        payload_sql = self._variant_literal(payload)
        now = self._now_utc()
        snap = self.manager.conn.execute(
            f"""
            INSERT INTO knowledge_version_snapshot (knowledge_id, snapshot_payload, reason, created_at)
            VALUES (?, {payload_sql}, ?, ?)
            RETURNING id
            """,
            [int(knowledge_id), reason, now],
        ).fetchone()
        snapshot_id = int(snap[0])

        self._write_audit(
            action_type="create_snapshot",
            user_id=str(user_id),
            tool_payload={"knowledge_id": int(knowledge_id), "snapshot_id": snapshot_id, "reason": reason},
            result_summary="ok",
        )

        return snapshot_id

    def rollback_to_snapshot(
        self,
        *,
        knowledge_id: int,
        snapshot_id: int,
        reason: str | None = None,
    ) -> None:
        self._ensure_connected()

        row = self.manager.conn.execute(
            """
            SELECT knowledge_id, snapshot_payload
            FROM knowledge_version_snapshot
            WHERE id = ?
            """,
            [int(snapshot_id)],
        ).fetchone()
        if row is None:
            raise ValueError(f"Snapshot not found: {snapshot_id}")
        if int(row[0]) != int(knowledge_id):
            raise ValueError("Snapshot does not belong to the given knowledge_id")

        payload = row[1]
        if not isinstance(payload, dict):
            raise ValueError("Snapshot payload must be a dict")

        user_id = payload.get("user_id")
        if user_id is None:
            # Fallback: read current row
            cur = self.manager.conn.execute(
                "SELECT user_id FROM semantic_knowledge WHERE id = ?",
                [int(knowledge_id)],
            ).fetchone()
            user_id = cur[0] if cur else None

        now = self._now_utc()
        metadata_sql = self._variant_literal(payload.get("metadata"))

        # Restore a conservative subset of fields (expandable later).
        self.manager.conn.execute(
            f"""
            UPDATE semantic_knowledge
            SET
                knowledge_type = ?,
                title = ?,
                canonical_text = ?,
                keywords = ?,
                source_refs = ?,
                confidence_score = ?,
                stability_score = ?,
                metadata = {metadata_sql},
                embedding = ?,
                is_active = ?,
                updated_at = ?
            WHERE id = ?
            """,
            [
                payload.get("knowledge_type"),
                payload.get("title"),
                payload.get("canonical_text"),
                payload.get("keywords"),
                payload.get("source_refs"),
                float(payload.get("confidence_score", 0.7)),
                float(payload.get("stability_score", 0.7)),
                payload.get("embedding"),
                bool(payload.get("is_active", True)),
                now,
                int(knowledge_id),
            ],
        )

        self._write_audit(
            action_type="rollback_to_snapshot",
            user_id=str(user_id) if user_id is not None else "unknown",
            tool_payload={
                "knowledge_id": int(knowledge_id),
                "snapshot_id": int(snapshot_id),
                "reason": reason,
            },
            result_summary="ok",
        )

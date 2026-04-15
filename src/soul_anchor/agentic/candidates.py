from __future__ import annotations

from typing import Any

from soul_anchor.manager import MemoryManager
from soul_anchor.db.variant import variant_sql_literal
from soul_anchor.agentic.audit_writer import AuditWriter


class CandidateProcessor:
    """
    Phase 3.2 candidate processing:
    - merge pending knowledge_candidate into semantic_knowledge
    - mark duplicates (vs existing semantic_knowledge)
    - register conflicts into conflict_registry
    """

    def __init__(self, manager: MemoryManager):
        self.manager = manager
        self._audit = AuditWriter(manager)

    def _ensure_connected(self) -> None:
        if self.manager.conn is None:
            raise RuntimeError("MemoryManager is not connected. Call connect() first.")

    def _variant_literal(self, value: Any) -> str:
        return variant_sql_literal(value)

    def _update_candidate(self, *, candidate_id: int, status: str, payload: dict[str, Any]) -> None:
        now = self._audit.now_utc()
        payload_sql = self._variant_literal(payload)
        self.manager.conn.execute(
            f"""
            UPDATE knowledge_candidate
            SET status = ?,
                candidate_payload = {payload_sql},
                reviewed_at = ?
            WHERE id = ?
            """,
            [status, now, int(candidate_id)],
        )

    def process_pending(self, *, limit: int = 50) -> dict[str, int]:
        """
        Process pending candidates in id order.
        Returns counters: merged/duplicates/conflicts/skipped.
        """
        self._ensure_connected()
        rows = self.manager.conn.execute(
            """
            SELECT id
            FROM knowledge_candidate
            WHERE status = 'pending'
            ORDER BY id
            LIMIT ?
            """,
            [int(limit)],
        ).fetchall()

        counters = {"merged": 0, "duplicates": 0, "conflicts": 0, "skipped": 0}
        for (candidate_id,) in rows:
            outcome = self.process_one(candidate_id=int(candidate_id))
            if outcome == "merged":
                counters["merged"] += 1
            elif outcome == "duplicate":
                counters["duplicates"] += 1
            elif outcome == "conflict":
                counters["conflicts"] += 1
            else:
                counters["skipped"] += 1
        return counters

    def process_one(self, *, candidate_id: int) -> str:
        self._ensure_connected()

        row = self.manager.conn.execute(
            """
            SELECT id, user_id, knowledge_type, title, canonical_text, source_refs,
                   candidate_payload, confidence_score, status
            FROM knowledge_candidate
            WHERE id = ?
            """,
            [int(candidate_id)],
        ).fetchone()
        if row is None:
            return "skipped"

        (
            candidate_id,
            user_id,
            knowledge_type,
            title,
            canonical_text,
            source_refs,
            candidate_payload,
            confidence_score,
            status,
        ) = row

        if status != "pending":
            return "skipped"

        payload: dict[str, Any]
        if isinstance(candidate_payload, dict):
            payload = dict(candidate_payload)
        elif candidate_payload is None:
            payload = {}
        else:
            payload = {"raw_payload": candidate_payload}

        canonical_text = (canonical_text or "").strip()

        # 1) Duplicate vs existing semantic knowledge (exact canonical_text).
        dup = self.manager.conn.execute(
            """
            SELECT id
            FROM semantic_knowledge
            WHERE user_id = ?
              AND is_active = TRUE
              AND canonical_text = ?
            LIMIT 1
            """,
            [user_id, canonical_text],
        ).fetchone()
        if dup is not None:
            duplicate_of = int(dup[0])
            payload.update({"duplicate_of": duplicate_of, "duplicate_kind": "semantic"})
            self._update_candidate(candidate_id=int(candidate_id), status="duplicate", payload=payload)
            self._audit.write(
                action_type="candidate_duplicate",
                user_id=str(user_id),
                tool_payload={"candidate_id": int(candidate_id), "duplicate_of": duplicate_of},
                result_summary="duplicate_vs_semantic",
            )
            return "duplicate"

        # 2) Conflict vs existing semantic knowledge (same title/type but different text).
        existing = self.manager.conn.execute(
            """
            SELECT id, canonical_text
            FROM semantic_knowledge
            WHERE user_id = ?
              AND is_active = TRUE
              AND knowledge_type = ?
              AND title = ?
            LIMIT 1
            """,
            [user_id, knowledge_type, title],
        ).fetchone()
        if existing is not None:
            existing_id = int(existing[0])
            existing_text = str(existing[1] or "").strip()
            if existing_text != canonical_text:
                details = {
                    "candidate_id": int(candidate_id),
                    "existing_knowledge_id": existing_id,
                    "title": title,
                    "knowledge_type": knowledge_type,
                    "existing_text": existing_text,
                    "candidate_text": canonical_text,
                }
                details_sql = self._variant_literal(details)
                self.manager.conn.execute(
                    f"""
                    INSERT INTO conflict_registry (
                        user_id, candidate_id, existing_knowledge_id, conflict_type, status, details
                    )
                    VALUES (?, ?, ?, 'title_conflict', 'open', {details_sql})
                    """,
                    [user_id, int(candidate_id), existing_id],
                )
                payload.update({"conflict_with": existing_id, "conflict_type": "title_conflict"})
                self._update_candidate(candidate_id=int(candidate_id), status="conflict", payload=payload)
                self._audit.write(
                    action_type="candidate_conflict",
                    user_id=str(user_id),
                    tool_payload={"candidate_id": int(candidate_id), "existing_knowledge_id": existing_id},
                    result_summary="registered_conflict",
                )
                return "conflict"

        # 3) Merge into semantic_knowledge.
        metadata_sql = self._variant_literal(payload if payload else None)
        merged_row = self.manager.conn.execute(
            f"""
            INSERT INTO semantic_knowledge (
                user_id, knowledge_type, title, canonical_text, keywords, source_refs,
                confidence_score, stability_score, metadata, embedding, is_active
            )
            VALUES (?, ?, ?, ?, NULL, ?, ?, ?, {metadata_sql}, NULL, TRUE)
            RETURNING id
            """,
            [
                user_id,
                knowledge_type,
                title,
                canonical_text,
                source_refs,
                float(confidence_score),
                0.6,
            ],
        ).fetchone()
        merged_id = int(merged_row[0])

        payload.update({"merged_knowledge_id": merged_id})
        self._update_candidate(candidate_id=int(candidate_id), status="merged", payload=payload)
        self._audit.write(
            action_type="merge_candidate",
            user_id=str(user_id),
            tool_payload={"candidate_id": int(candidate_id), "merged_knowledge_id": merged_id},
            result_summary="merged",
        )
        return "merged"

from __future__ import annotations

from typing import Any, Literal

from soul_anchor.manager import MemoryManager
from soul_anchor.db.variant import variant_sql_literal
from soul_anchor.agentic.audit_writer import AuditWriter

ResolutionStrategy = Literal["keep_existing", "replace", "merge_text"]


class ConflictResolver:
    """
    Phase 3.2 conflict resolver.

    Resolves rows in conflict_registry by applying one of:
    - keep_existing: keep existing semantic_knowledge; reject candidate
    - replace: overwrite existing semantic_knowledge with candidate text; mark candidate applied
    - merge_text: merge texts into existing semantic_knowledge; mark candidate applied
    """

    def __init__(self, manager: MemoryManager):
        self.manager = manager
        self._audit = AuditWriter(manager)

    def _ensure_connected(self) -> None:
        if self.manager.conn is None:
            raise RuntimeError("MemoryManager is not connected. Call connect() first.")

    def _variant_literal(self, value: Any) -> str:
        return variant_sql_literal(value)

    def _write_audit(self, *, user_id: str, tool_payload: dict[str, Any], result_summary: str) -> None:
        self._audit.write(
            action_type="resolve_conflict",
            session_id=None,
            user_id=user_id,
            decision_payload=None,
            tool_payload=tool_payload,
            result_summary=result_summary,
        )

    def resolve(self, *, conflict_id: int, strategy: ResolutionStrategy) -> None:
        self._ensure_connected()

        row = self.manager.conn.execute(
            """
            SELECT id, user_id, candidate_id, existing_knowledge_id, conflict_type, status, details
            FROM conflict_registry
            WHERE id = ?
            """,
            [int(conflict_id)],
        ).fetchone()
        if row is None:
            raise ValueError(f"Conflict not found: {conflict_id}")

        _, user_id, candidate_id, existing_id, conflict_type, status, details = row
        if status != "open":
            raise ValueError(f"Conflict is not open: {conflict_id} status={status}")
        if candidate_id is None or existing_id is None:
            raise ValueError("Conflict must reference both candidate_id and existing_knowledge_id")

        candidate = self.manager.conn.execute(
            """
            SELECT id, canonical_text, candidate_payload
            FROM knowledge_candidate
            WHERE id = ?
            """,
            [int(candidate_id)],
        ).fetchone()
        if candidate is None:
            raise ValueError(f"Candidate not found: {candidate_id}")

        _, candidate_text, candidate_payload = candidate
        if isinstance(candidate_payload, dict):
            payload: dict[str, Any] = dict(candidate_payload)
        elif candidate_payload is None:
            payload = {}
        else:
            payload = {"raw_payload": candidate_payload}

        existing_row = self.manager.conn.execute(
            """
            SELECT id, canonical_text
            FROM semantic_knowledge
            WHERE id = ?
            """,
            [int(existing_id)],
        ).fetchone()
        if existing_row is None:
            raise ValueError(f"Existing knowledge not found: {existing_id}")

        _, existing_text = existing_row
        existing_text = str(existing_text or "")
        candidate_text = str(candidate_text or "")

        now = self._audit.now_utc()

        if strategy == "keep_existing":
            payload.update({"resolution": "keep_existing"})
            payload_sql = self._variant_literal(payload)
            self.manager.conn.execute(
                f"""
                UPDATE knowledge_candidate
                SET status = 'rejected',
                    reviewed_at = ?,
                    candidate_payload = {payload_sql}
                WHERE id = ?
                """,
                [now, int(candidate_id)],
            )
            self._write_audit(
                user_id=str(user_id),
                tool_payload={
                    "conflict_id": int(conflict_id),
                    "strategy": "keep_existing",
                    "candidate_id": int(candidate_id),
                    "existing_knowledge_id": int(existing_id),
                    "conflict_type": conflict_type,
                },
                result_summary="kept_existing",
            )

        elif strategy == "replace":
            self.manager.conn.execute(
                """
                UPDATE semantic_knowledge
                SET canonical_text = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                [candidate_text, now, int(existing_id)],
            )
            payload.update({"resolution": "replace", "applied_to": int(existing_id)})
            payload_sql = self._variant_literal(payload)
            self.manager.conn.execute(
                f"""
                UPDATE knowledge_candidate
                SET status = 'applied',
                    reviewed_at = ?,
                    candidate_payload = {payload_sql}
                WHERE id = ?
                """,
                [now, int(candidate_id)],
            )
            self._write_audit(
                user_id=str(user_id),
                tool_payload={
                    "conflict_id": int(conflict_id),
                    "strategy": "replace",
                    "candidate_id": int(candidate_id),
                    "existing_knowledge_id": int(existing_id),
                    "conflict_type": conflict_type,
                },
                result_summary="replaced_existing",
            )

        elif strategy == "merge_text":
            merged_text = f"{existing_text}\n\n---\n\n{candidate_text}".strip()
            self.manager.conn.execute(
                """
                UPDATE semantic_knowledge
                SET canonical_text = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                [merged_text, now, int(existing_id)],
            )
            payload.update({"resolution": "merge_text", "applied_to": int(existing_id)})
            payload_sql = self._variant_literal(payload)
            self.manager.conn.execute(
                f"""
                UPDATE knowledge_candidate
                SET status = 'applied',
                    reviewed_at = ?,
                    candidate_payload = {payload_sql}
                WHERE id = ?
                """,
                [now, int(candidate_id)],
            )
            self._write_audit(
                user_id=str(user_id),
                tool_payload={
                    "conflict_id": int(conflict_id),
                    "strategy": "merge_text",
                    "candidate_id": int(candidate_id),
                    "existing_knowledge_id": int(existing_id),
                    "conflict_type": conflict_type,
                },
                result_summary="merged_text",
            )

        else:
            raise ValueError(f"Unknown strategy: {strategy}")

        details_obj = details if isinstance(details, dict) else {"details": details}
        details_obj.update({"resolution": strategy, "resolved_by": "ConflictResolver"})
        details_sql = self._variant_literal(details_obj)
        self.manager.conn.execute(
            f"""
            UPDATE conflict_registry
            SET status = 'resolved',
                resolved_at = ?,
                details = {details_sql}
            WHERE id = ?
            """,
            [now, int(conflict_id)],
        )

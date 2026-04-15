from __future__ import annotations

import datetime
from typing import Any

from soul_anchor.manager import MemoryManager


class MemoryToolAPI:
    """
    Phase 3.1 Memory Tool API.

    This class provides a stable, auditable facade for memory operations.
    """

    def __init__(self, manager: MemoryManager):
        self.manager = manager

    def _now_utc(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    def _ensure_connected(self) -> None:
        # MemoryManager already raises on missing connection; keep this wrapper for clarity.
        if self.manager.conn is None:
            raise RuntimeError("MemoryManager is not connected. Call connect() first.")

    def _variant_literal(self, value: Any) -> str:
        # Reuse manager's encoding to keep VARIANT insertion consistent.
        return self.manager._variant_sql_literal(value)  # noqa: SLF001

    def _write_audit(
        self,
        *,
        action_type: str,
        session_id: str | None = None,
        user_id: str | None = None,
        decision_payload: Any | None = None,
        tool_payload: Any | None = None,
        result_summary: str | None = None,
    ) -> int:
        self._ensure_connected()

        decision_sql = self._variant_literal(decision_payload)
        tool_sql = self._variant_literal(tool_payload)

        row = self.manager.conn.execute(
            f"""
            INSERT INTO memory_audit_log (
                action_type, session_id, user_id, decision_payload, tool_payload, result_summary, created_at
            )
            VALUES (?, ?, ?, {decision_sql}, {tool_sql}, ?, ?)
            RETURNING id
            """,
            [action_type, session_id, user_id, result_summary, self._now_utc()],
        ).fetchone()
        return int(row[0])

    def search_context(self, *, session_id: str, user_id: str, query: str, top_k: int = 10):
        self._ensure_connected()
        results = self.manager.search_recent_context_advanced(
            session_id=session_id,
            user_id=user_id,
            query=query,
            top_k=top_k,
        )
        self._write_audit(
            action_type="search_context",
            session_id=session_id,
            user_id=user_id,
            decision_payload=None,
            tool_payload={"query": query, "top_k": int(top_k)},
            result_summary=f"n_results={len(results)}",
        )
        return results

    def search_knowledge(self, *, user_id: str, query: str, top_k: int = 10):
        self._ensure_connected()
        results = self.manager.search_knowledge(
            user_id=user_id,
            query=query,
            top_k=top_k,
        )
        self._write_audit(
            action_type="search_knowledge",
            session_id=None,
            user_id=user_id,
            decision_payload=None,
            tool_payload={"query": query, "top_k": int(top_k)},
            result_summary=f"n_results={len(results)}",
        )
        return results

    def load_core_contract(self):
        self._ensure_connected()
        results = self.manager.load_core_contract()
        self._write_audit(
            action_type="load_core_contract",
            session_id=None,
            user_id=None,
            decision_payload=None,
            tool_payload={},
            result_summary=f"n_items={len(results)}",
        )
        return results

    def save_episode(self, event: dict[str, Any]) -> int:
        self._ensure_connected()
        record_id = self.manager.save_episode(event)
        self._write_audit(
            action_type="save_episode",
            session_id=str(event.get("session_id")) if event.get("session_id") is not None else None,
            user_id=str(event.get("user_id")) if event.get("user_id") is not None else None,
            decision_payload=None,
            tool_payload={"event_type": event.get("event_type"), "id": record_id},
            result_summary="ok",
        )
        return int(record_id)

    def save_knowledge_candidate(self, candidate: dict[str, Any]) -> int:
        self._ensure_connected()

        user_id = candidate["user_id"]
        knowledge_type = candidate["knowledge_type"]
        title = candidate["title"]
        canonical_text = candidate["canonical_text"]

        source_refs = candidate.get("source_refs")
        payload = candidate.get("candidate_payload")
        confidence_score = float(candidate.get("confidence_score", 0.5))
        status = candidate.get("status", "pending")

        row = self.manager.conn.execute(
            f"""
            INSERT INTO knowledge_candidate (
                user_id, knowledge_type, title, canonical_text, source_refs, candidate_payload,
                confidence_score, status
            )
            VALUES (?, ?, ?, ?, ?, {self._variant_literal(payload)}, ?, ?)
            RETURNING id
            """,
            [user_id, knowledge_type, title, canonical_text, source_refs, confidence_score, status],
        ).fetchone()
        candidate_id = int(row[0])

        self._write_audit(
            action_type="save_knowledge_candidate",
            session_id=None,
            user_id=user_id,
            decision_payload=None,
            tool_payload={"id": candidate_id, "title": title, "knowledge_type": knowledge_type},
            result_summary="ok",
        )

        return candidate_id

    def audit_recent_actions(self, *, limit: int = 50) -> list[dict[str, Any]]:
        self._ensure_connected()
        rows = self.manager.conn.execute(
            """
            SELECT id, action_type, session_id, user_id, decision_payload, tool_payload, result_summary, created_at
            FROM memory_audit_log
            ORDER BY id DESC
            LIMIT ?
            """,
            [int(limit)],
        ).fetchall()

        results: list[dict[str, Any]] = []
        for (
            row_id,
            action_type,
            session_id,
            user_id,
            decision_payload,
            tool_payload,
            result_summary,
            created_at,
        ) in rows:
            results.append(
                {
                    "id": int(row_id),
                    "action_type": action_type,
                    "session_id": session_id,
                    "user_id": user_id,
                    "decision_payload": decision_payload,
                    "tool_payload": tool_payload,
                    "result_summary": result_summary,
                    "created_at": created_at,
                }
            )
        return results


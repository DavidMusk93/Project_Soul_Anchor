from __future__ import annotations

import datetime
from typing import Any

from soul_anchor.db.variant import variant_sql_literal
from soul_anchor.manager import MemoryManager


class AuditWriter:
    """
    Centralized writer for memory_audit_log.

    This avoids duplicated SQL snippets across agentic modules and keeps the
    payload encoding consistent (VARIANT via variant_sql_literal).
    """

    def __init__(self, manager: MemoryManager):
        self.manager = manager

    def _ensure_connected(self) -> None:
        if self.manager.conn is None:
            raise RuntimeError("MemoryManager is not connected. Call connect() first.")

    def now_utc(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    def write(
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

        decision_sql = variant_sql_literal(decision_payload)
        tool_sql = variant_sql_literal(tool_payload)

        row = self.manager.conn.execute(
            f"""
            INSERT INTO memory_audit_log (
                action_type, session_id, user_id, decision_payload, tool_payload, result_summary, created_at
            )
            VALUES (?, ?, ?, {decision_sql}, {tool_sql}, ?, ?)
            RETURNING id
            """,
            [action_type, session_id, user_id, result_summary, self.now_utc()],
        ).fetchone()
        return int(row[0])

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from soul_anchor.manager import MemoryManager


@dataclass(frozen=True)
class MemoryGateResult:
    """
    Phase 3.1 gating result.

    Keep this minimal and explicit so it is easy to test and audit.
    """

    accepted: bool
    target_layer: str
    reasons: list[str] = field(default_factory=list)
    requires_review: bool = False
    duplicate_of: int | None = None


class MemoryGating:
    """
    Phase 3.1 conservative gating rules.

    - Noise filtering for low-signal chat.
    - Duplicate detection for knowledge candidates.
    - L3 updates are never auto-written (requires review).
    """

    def __init__(self, manager: MemoryManager):
        self.manager = manager

    def _ensure_connected(self) -> None:
        if self.manager.conn is None:
            raise RuntimeError("MemoryManager is not connected. Call connect() first.")

    def gate_episode(self, event: dict[str, Any]) -> MemoryGateResult:
        """
        Gate a L1 episode write.
        Phase 3.1: only noise filtering is enforced here.
        """
        self._ensure_connected()

        event_type = str(event.get("event_type") or "")
        content = str(event.get("content") or "").strip()

        low_signal_exact = {
            "好的",
            "ok",
            "OK",
            "嗯",
            "嗯嗯",
            "收到",
            "谢谢",
            "thx",
        }

        if event_type == "user_message":
            if not content or content in low_signal_exact or len(content) <= 2:
                return MemoryGateResult(
                    accepted=False,
                    target_layer="drop",
                    reasons=["noise"],
                )

        return MemoryGateResult(
            accepted=True,
            target_layer="L1",
            reasons=[],
        )

    def gate_knowledge_candidate(self, candidate: dict[str, Any]) -> MemoryGateResult:
        """
        Gate a L2 candidate write.
        Phase 3.1: reject exact duplicates against existing semantic_knowledge.
        """
        self._ensure_connected()

        user_id = candidate["user_id"]
        canonical_text = str(candidate.get("canonical_text") or "").strip()

        if canonical_text:
            row = self.manager.conn.execute(
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
            if row is not None:
                return MemoryGateResult(
                    accepted=False,
                    target_layer="drop",
                    reasons=["duplicate"],
                    duplicate_of=int(row[0]),
                )

        return MemoryGateResult(
            accepted=True,
            target_layer="L2_candidate",
            reasons=[],
        )

    def gate_core_contract_update(self, update: dict[str, Any]) -> MemoryGateResult:
        """
        Gate an L3 update.
        Phase 3.1: never auto-write L3 updates; always require review.
        """
        _ = update
        self._ensure_connected()
        return MemoryGateResult(
            accepted=False,
            target_layer="L3",
            reasons=["l3_requires_review"],
            requires_review=True,
        )


from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class MemoryDecision:
    """
    Phase 3.1: structured decision output.
    Keep this minimal and testable; later phases can add scores and richer routing.
    """

    should_recall_context: bool = False
    should_recall_knowledge: bool = False
    should_write_episode: bool = False
    should_create_knowledge_candidate: bool = False
    reasons: list[str] = field(default_factory=list)


class DecisionEngine:
    """
    Phase 3.1 rule-based decision engine.

    This is intentionally conservative:
    - Prefer explicit user signals.
    - Avoid automatic long-term writes unless it looks like a stable preference/rule.
    """

    def decide(
        self,
        *,
        session_id: str,
        user_id: str,
        event_type: str,
        content: str,
    ) -> MemoryDecision:
        _ = session_id
        _ = user_id

        text = (content or "").strip()
        lower = text.lower()

        reasons: list[str] = []

        # Phase 3.1 default: user messages are stored as L1 episodes unless explicitly filtered later by gating.
        should_write_episode = event_type in {"user_message", "assistant_reply", "tool_result", "note", "task_state"}

        # History reference -> recall both L1 and L2.
        history_markers = ["上次", "之前", "还是按", "同样", "以前", "照旧"]
        if any(marker in text for marker in history_markers):
            reasons.append("user_referenced_history")
            should_recall_context = True
            should_recall_knowledge = True
        else:
            should_recall_context = False
            should_recall_knowledge = False

        # Preference / rule statement -> candidate knowledge.
        preference_markers = ["请记住", "偏好", "以后都", "必须", "不要", "总是", "每次"]
        if any(marker in text for marker in preference_markers):
            reasons.append("user_stated_preference")
            should_create_knowledge_candidate = True
        else:
            should_create_knowledge_candidate = False

        # Low-signal / empty messages: do nothing.
        if not lower:
            should_write_episode = False
            should_recall_context = False
            should_recall_knowledge = False
            should_create_knowledge_candidate = False
            reasons = ["empty_input"]

        return MemoryDecision(
            should_recall_context=should_recall_context,
            should_recall_knowledge=should_recall_knowledge,
            should_write_episode=should_write_episode,
            should_create_knowledge_candidate=should_create_knowledge_candidate,
            reasons=reasons,
        )


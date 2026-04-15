from __future__ import annotations

from dataclasses import asdict
from typing import Any

from soul_anchor.agentic.decision_engine import MemoryDecision
from soul_anchor.agentic.gating import MemoryGateResult, MemoryGating
from soul_anchor.agentic.tools import MemoryToolAPI
from soul_anchor.agentic.audit_writer import AuditWriter
from soul_anchor.manager import MemoryManager


class AuditRecorder:
    """
    Phase 3.1 audit recorder.

    Writes structured records into memory_audit_log. This is intentionally separate
    from MemoryToolAPI audit so we can record the "decision" step as well.
    """

    def __init__(self, manager: MemoryManager):
        self.manager = manager
        self._audit = AuditWriter(manager)

    def _ensure_connected(self) -> None:
        if self.manager.conn is None:
            raise RuntimeError("MemoryManager is not connected. Call connect() first.")

    def record_decision(
        self,
        *,
        session_id: str,
        user_id: str,
        decision: MemoryDecision,
        input_payload: dict[str, Any],
    ) -> int:
        decision_payload = asdict(decision)
        return self._audit.write(
            action_type="decision",
            session_id=session_id,
            user_id=user_id,
            decision_payload=decision_payload,
            tool_payload=input_payload,
            result_summary="ok",
        )


class AuditVerifier:
    """
    Phase 3.1 audit verifier.

    Ensures the actions executed by the loop are consistent with the DecisionEngine output.
    """

    def assert_consistent(self, *, decision: MemoryDecision, executed_actions: list[str]) -> None:
        actions = set(executed_actions)

        if "search_context" in actions and not decision.should_recall_context:
            raise ValueError("Decision does not allow recall_context but search_context was executed.")
        if "search_knowledge" in actions and not decision.should_recall_knowledge:
            raise ValueError("Decision does not allow recall_knowledge but search_knowledge was executed.")
        if "save_episode" in actions and not decision.should_write_episode:
            raise ValueError("Decision does not allow write_episode but save_episode was executed.")
        if "save_knowledge_candidate" in actions and not decision.should_create_knowledge_candidate:
            raise ValueError(
                "Decision does not allow knowledge candidate creation but save_knowledge_candidate was executed."
            )


class AgenticLoopRunner:
    """
    Phase 3.1 minimal Decide -> Gate -> Act -> Audit loop runner.

    This runner is intentionally narrow: it only executes a fixed set of tool actions
    driven by the DecisionEngine and guarded by the Gating layer.
    """

    def __init__(
        self,
        *,
        decision_engine: Any,
        gating: MemoryGating,
        tools: MemoryToolAPI,
        audit_recorder: AuditRecorder,
        audit_verifier: AuditVerifier,
    ):
        self.decision_engine = decision_engine
        self.gating = gating
        self.tools = tools
        self.audit_recorder = audit_recorder
        self.audit_verifier = audit_verifier

    def run_event(
        self,
        *,
        session_id: str,
        user_id: str,
        event_type: str,
        content: str,
    ) -> dict[str, Any]:
        decision: MemoryDecision = self.decision_engine.decide(
            session_id=session_id,
            user_id=user_id,
            event_type=event_type,
            content=content,
        )

        audit_ids: dict[str, int] = {}
        audit_ids["decision_audit_id"] = self.audit_recorder.record_decision(
            session_id=session_id,
            user_id=user_id,
            decision=decision,
            input_payload={"event_type": event_type, "content": content},
        )

        executed_actions: list[str] = []
        episode_id: int | None = None

        # Gate + write L1 episode if allowed.
        if decision.should_write_episode:
            gate_result: MemoryGateResult = self.gating.gate_episode(
                {
                    "session_id": session_id,
                    "user_id": user_id,
                    "event_type": event_type,
                    "content": content,
                }
            )
            if gate_result.accepted:
                episode_id = self.tools.save_episode(
                    {
                        "session_id": session_id,
                        "user_id": user_id,
                        "event_type": event_type,
                        "content": content,
                    }
                )
                executed_actions.append("save_episode")

        # Recall actions.
        if decision.should_recall_context:
            _ = self.tools.search_context(
                session_id=session_id,
                user_id=user_id,
                query=content,
                top_k=10,
            )
            executed_actions.append("search_context")

        if decision.should_recall_knowledge:
            _ = self.tools.search_knowledge(
                user_id=user_id,
                query=content,
                top_k=10,
            )
            executed_actions.append("search_knowledge")

        # Candidate creation (Phase 3.1): extract a conservative candidate from the user's message.
        if decision.should_create_knowledge_candidate:
            candidate = {
                "user_id": user_id,
                "knowledge_type": "preference",
                "title": "User Preference",
                "canonical_text": content,
                "source_refs": f"context_stream:{episode_id}" if episode_id is not None else None,
                "candidate_payload": {
                    "reasons": decision.reasons,
                    "event_type": event_type,
                    "content": content,
                },
                "confidence_score": 0.6,
                "status": "pending",
            }
            gate_result = self.gating.gate_knowledge_candidate(candidate)
            if gate_result.accepted:
                _ = self.tools.save_knowledge_candidate(candidate)
                executed_actions.append("save_knowledge_candidate")

        self.audit_verifier.assert_consistent(decision=decision, executed_actions=executed_actions)

        return {
            "decision": decision,
            "executed_actions": executed_actions,
            "audit_ids": audit_ids,
        }

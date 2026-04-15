from __future__ import annotations

from typing import Any, Callable


def build_context_packet(
    *,
    session_id: str,
    user_id: str,
    query: str,
    l1_limit: int,
    l2_limit: int,
    max_chars: int | None,
    deduplicate: bool,
    load_core_contract: Callable[[], list[dict[str, Any]]],
    search_knowledge: Callable[..., list[dict[str, Any]]],
    search_recent_context_advanced: Callable[..., list[dict[str, Any]]],
) -> dict[str, Any]:
    core_contract = load_core_contract()
    semantic_knowledge = search_knowledge(query=query, user_id=user_id, top_k=l2_limit)
    recent_context = search_recent_context_advanced(
        session_id=session_id,
        user_id=user_id,
        query=query,
        top_k=l1_limit,
    )

    if deduplicate and semantic_knowledge and recent_context:
        l2_texts = [str(item.get("canonical_text") or item.get("content") or "") for item in semantic_knowledge]
        l2_norm = [text.strip().lower() for text in l2_texts if text]
        filtered_l1 = []
        for item in recent_context:
            content = str(item.get("content") or "").strip().lower()
            if not content:
                continue
            if any(content in text or text in content for text in l2_norm):
                continue
            filtered_l1.append(item)
        recent_context = filtered_l1

    def _packet_chars() -> int:
        total = 0
        for item in core_contract:
            total += len(str(item.get("contract_value") or ""))
        for item in semantic_knowledge:
            total += len(str(item.get("canonical_text") or item.get("content") or ""))
        for item in recent_context:
            total += len(str(item.get("content") or ""))
        return total

    if max_chars is not None:
        while _packet_chars() > int(max_chars) and recent_context:
            recent_context.pop()
        while _packet_chars() > int(max_chars) and semantic_knowledge:
            semantic_knowledge.pop()

    return {
        "core_contract": core_contract,
        "semantic_knowledge": semantic_knowledge,
        "recent_context": recent_context,
        "metadata": {"total_chars": _packet_chars()},
    }

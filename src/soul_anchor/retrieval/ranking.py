from __future__ import annotations

import datetime
from typing import Any, Callable

import duckdb

from soul_anchor.embedding.similarity import cosine

def tokenize(text: str) -> list[str]:
    return [token.strip().lower() for token in text.split() if token.strip()]


def contains_any(text: str | None, terms: list[str]) -> bool:
    if not text:
        return False
    hay = text.lower()
    return any(term in hay for term in terms)


def search_recent_context(
    conn: duckdb.DuckDBPyConnection,
    *,
    session_id: str,
    user_id: str,
    query: str,
    top_k: int,
    now: datetime.datetime,
) -> list[dict[str, Any]]:
    terms = tokenize(query) or [query]
    rows = conn.execute(
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
        score = 0
        for term in terms:
            if _contains(row[5], term):
                score += 3
            if _contains(row[6], term):
                score += 2
            if _contains(row[7], term):
                score += 1
        if score > 0:
            scored.append((1, score, float(row[9]), row[10], row))

    scored.sort(key=lambda item: (item[0], item[1], item[2], item[3] or datetime.datetime.min), reverse=True)
    selected_rows = [item[4] for item in scored[: int(top_k)]]
    return _map_context_rows(selected_rows)


def search_recent_context_advanced(
    conn: duckdb.DuckDBPyConnection,
    *,
    session_id: str,
    user_id: str,
    query: str,
    top_k: int,
    now: datetime.datetime,
) -> list[dict[str, Any]]:
    terms = tokenize(query) or [query.lower()]
    rows = conn.execute(
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

    matched = []
    for row in rows:
        if contains_any(row[5], terms) or contains_any(row[6], terms) or contains_any(row[7], terms):
            matched.append(row)

    matched.sort(key=lambda row: (row[10] or datetime.datetime.min, float(row[9])), reverse=True)
    return _map_context_rows(matched[: int(top_k)])


def recall_memory(
    conn: duckdb.DuckDBPyConnection,
    *,
    query: str,
    user_id: str,
    top_k: int,
    now: datetime.datetime,
    embed_text: Callable[[str], list[float]],
) -> list[dict[str, Any]]:
    _ = embed_text(query)
    terms = tokenize(query) or [query]
    rows = conn.execute(
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
        score = 0
        for term in terms:
            if _score(row[1], term):
                score += 3
            if _score(row[3], term):
                score += 2
            if _score(row[2], term):
                score += 1
        scored.append((1 if score > 0 else 0, score, float(row[5]), float(row[4]), row))

    scored.sort(key=lambda item: (item[0], item[1], item[2], item[3]), reverse=True)
    selected_rows = [item[4] for item in scored[: int(top_k)]]
    _touch_semantic_rows(conn, now=now, ids=[int(row[0]) for row in selected_rows])

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


def search_knowledge(
    conn: duckdb.DuckDBPyConnection,
    *,
    query: str,
    user_id: str,
    top_k: int,
    use_embedding: bool = False,
    candidate_pool: int | None = None,
    now: datetime.datetime,
    embed_text: Callable[[str], list[float]],
) -> list[dict[str, Any]]:
    query_vec = embed_text(query) if use_embedding else []
    terms = tokenize(query) or [query.lower()]
    rows = conn.execute(
        """
        SELECT
            id, title, canonical_text, keywords, confidence_score, stability_score, embedding
        FROM semantic_knowledge
        WHERE is_active = TRUE
          AND user_id = ?
        """,
        [user_id],
    ).fetchall()

    def _hit(text: str | None, term: str) -> bool:
        if not text:
            return False
        return term in text.lower()

    scored: list[tuple[float, tuple[Any, ...], dict[str, bool], float]] = []
    for row in rows:
        title_hit = any(_hit(row[1], term) for term in terms)
        keywords_hit = any(_hit(row[3], term) for term in terms)
        text_hit = any(_hit(row[2], term) for term in terms)

        relevance = 0.0
        relevance += 3.0 if title_hit else 0.0
        relevance += 2.0 if keywords_hit else 0.0
        relevance += 1.0 if text_hit else 0.0

        retrieval_score = relevance * 1000.0 + float(row[5]) * 10.0 + float(row[4])
        vector_score = 0.0
        if use_embedding and row[6] is not None:
            vector_score = cosine(list(query_vec), list(row[6]))
        scored.append(
            (
                retrieval_score,
                row,
                {"title": title_hit, "keywords": keywords_hit, "canonical_text": text_hit},
                vector_score,
            )
        )

    scored.sort(key=lambda item: item[0], reverse=True)

    pool = int(candidate_pool) if candidate_pool is not None else max(int(top_k) * 10, 50)
    pool = max(pool, int(top_k))
    selected_pool = scored[:pool]

    if use_embedding:
        # Hybrid: use vector similarity to rerank a keyword-derived candidate pool.
        vector_weight = 2000.0
        selected_pool.sort(key=lambda item: (item[0] + item[3] * vector_weight), reverse=True)

    selected = selected_pool[: int(top_k)]
    _touch_semantic_rows(conn, now=now, ids=[int(item[1][0]) for item in selected])

    results: list[dict[str, Any]] = []
    for retrieval_score, row, reasons, vector_score in selected:
        row_id, title, canonical_text, keywords, confidence_score, stability_score, _embedding = row
        item: dict[str, Any] = {
            "id": int(row_id),
            "title": title,
            "content": canonical_text,
            "canonical_text": canonical_text,
            "keywords": keywords,
            "confidence_score": float(confidence_score),
            "stability_score": float(stability_score),
            "retrieval_score": float(retrieval_score),
            "match_reasons": reasons,
        }
        if use_embedding:
            vector_weight = 2000.0
            item["vector_score"] = float(vector_score)
            item["hybrid_score"] = float(retrieval_score + vector_score * vector_weight)
        results.append(
            item
        )
    return results


def _touch_semantic_rows(conn: duckdb.DuckDBPyConnection, *, now: datetime.datetime, ids: list[int]) -> None:
    if not ids:
        return
    conn.execute(
        """
        UPDATE semantic_knowledge
        SET access_count = access_count + 1,
            last_accessed_at = ?,
            updated_at = ?
        WHERE id IN (SELECT unnest(?::BIGINT[]))
        """,
        [now, now, ids],
    )


def _map_context_rows(rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for (
        row_id,
        session_id,
        user_id,
        topic,
        event_type,
        content,
        summary,
        tags,
        importance_score,
        salience_score,
        created_at,
        expires_at,
    ) in rows:
        results.append(
            {
                "id": int(row_id),
                "session_id": session_id,
                "user_id": user_id,
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

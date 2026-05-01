from __future__ import annotations

import datetime
from typing import Any, Callable

import duckdb

from soul_anchor.embedding.similarity import cosine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tokenize(text: str) -> list[str]:
    return [t.strip().lower() for t in text.split() if t.strip()]


def _match_any_ilike(hay: str | None, needles: list[str]) -> bool:
    if not hay:
        return False
    hay_lower = hay.lower()
    return any(n in hay_lower for n in needles)


def _build_any_term_filter(terms: list[str], *columns: str) -> tuple[str, list[str]]:
    """
    DuckDB ILIKE OR-chain: a row qualifies if ANY term matches ANY column.
    https://duckdb.org/docs/sql/expressions/comparison_operators
    """
    if not terms:
        return "1=1", []
    col_clauses: list[str] = []
    params: list[str] = []
    for col in columns:
        for term in terms:
            col_clauses.append(f"{col} ILIKE ?")
            params.append(f"%{term}%")
    return "(" + " OR ".join(col_clauses) + ")", params


def _build_kw_score_expr(terms: list[str]) -> tuple[str, list[str]]:
    """
    DuckDB CASE WHEN: title=3pt, keywords=2pt, canonical_text=1pt per term.
    https://duckdb.org/docs/sql/expressions/conditional
    """
    if not terms:
        return "0.0", []
    parts: list[str] = []
    params: list[str] = []
    for term in terms:
        parts.append(
            f"(CASE WHEN title ILIKE ? THEN 3.0 ELSE 0.0 END + "
            f"CASE WHEN keywords ILIKE ? THEN 2.0 ELSE 0.0 END + "
            f"CASE WHEN canonical_text ILIKE ? THEN 1.0 ELSE 0.0 END)"
        )
        params.extend([f"%{term}%"] * 3)
    return "+".join(parts), params


# ---------------------------------------------------------------------------
# L1: context_stream  search
# ---------------------------------------------------------------------------

def search_recent_context(
    conn: duckdb.DuckDBPyConnection,
    *,
    session_id: str,
    user_id: str,
    query: str,
    top_k: int,
    now: datetime.datetime,
) -> list[dict[str, Any]]:
    terms = _tokenize(query) or [query.lower()]
    where_extra, where_params = _build_any_term_filter(terms, "content", "summary", "tags")

    rows = conn.execute(
        f"""
        SELECT
            id, session_id, user_id, topic, event_type, content, summary, tags,
            importance_score, salience_score, created_at, expires_at
        FROM context_stream
        WHERE session_id = ?
          AND user_id = ?
          AND is_archived = FALSE
          AND (expires_at IS NULL OR expires_at > ?)
          AND {where_extra}
        ORDER BY salience_score DESC, created_at DESC
        LIMIT ?
        """,
        [session_id, user_id, now] + where_params + [int(top_k)],
    ).fetchall()

    return _map_context_rows(rows)


def search_recent_context_advanced(
    conn: duckdb.DuckDBPyConnection,
    *,
    session_id: str,
    user_id: str,
    query: str,
    top_k: int,
    now: datetime.datetime,
) -> list[dict[str, Any]]:
    terms = _tokenize(query) or [query.lower()]
    where_extra, where_params = _build_any_term_filter(terms, "content", "summary", "tags")

    rows = conn.execute(
        f"""
        SELECT
            id, session_id, user_id, topic, event_type, content, summary, tags,
            importance_score, salience_score, created_at, expires_at
        FROM context_stream
        WHERE session_id = ?
          AND user_id = ?
          AND is_archived = FALSE
          AND (expires_at IS NULL OR expires_at > ?)
          AND {where_extra}
        ORDER BY created_at DESC, salience_score DESC
        LIMIT ?
        """,
        [session_id, user_id, now] + where_params + [int(top_k)],
    ).fetchall()

    return _map_context_rows(rows)


# ---------------------------------------------------------------------------
# L2: semantic_knowledge  search
# ---------------------------------------------------------------------------

def recall_memory(
    conn: duckdb.DuckDBPyConnection,
    *,
    query: str,
    user_id: str,
    top_k: int,
    now: datetime.datetime,
    embed_text: Callable[[str], list[float]],
) -> list[dict[str, Any]]:
    query_vec = embed_text(query)
    terms = _tokenize(query) or [query.lower()]
    kw_expr, kw_params = _build_kw_score_expr(terms)

    rows = conn.execute(
        f"""
        SELECT
            id, title, canonical_text, keywords, confidence_score, stability_score, embedding,
            ({kw_expr}) AS kw_score
        FROM semantic_knowledge
        WHERE is_active = TRUE
          AND user_id = ?
        ORDER BY kw_score DESC, stability_score DESC, confidence_score DESC
        LIMIT ?
        """,
        kw_params + [user_id] + [int(top_k)],
    ).fetchall()

    _touch_semantic_rows(conn, now=now, ids=[int(r[0]) for r in rows])

    results: list[dict[str, Any]] = []
    for row_id, title, canonical_text, keywords, confidence_score, stability_score, embedding, kw_score in rows:
        _ = kw_score
        vec_score = 0.0
        if embedding is not None:
            vec_score = cosine(list(query_vec), list(embedding))
        results.append(
            {
                "id": int(row_id),
                "title": title,
                "content": canonical_text,
                "canonical_text": canonical_text,
                "keywords": keywords,
                "confidence_score": float(confidence_score),
                "stability_score": float(stability_score),
                "vector_score": float(vec_score),
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
    """
    Hybrid search over L2 semantic knowledge.

    Keyword mode: SQL WHERE ILIKE filter + ORDER BY kw_score DESC.
    Hybrid mode: no WHERE filter (vector similarity catches semantic matches),
                 computes list_cosine_similarity server-side.

    https://duckdb.org/docs/sql/functions/nested#list-functions
    """
    query_vec = embed_text(query)
    terms = _tokenize(query) or [query.lower()]

    pool = int(candidate_pool) if candidate_pool is not None else max(int(top_k) * 10, 50)
    pool = max(pool, int(top_k))

    kw_expr, kw_params = _build_kw_score_expr(terms)

    if use_embedding:
        rows = conn.execute(
            f"""
            SELECT
                id, title, canonical_text, keywords,
                confidence_score, stability_score, embedding,
                ({kw_expr}) AS kw_score,
                CASE WHEN embedding IS NOT NULL
                    THEN list_cosine_similarity(embedding, ?::FLOAT[{len(query_vec)}])
                    ELSE 0.0
                END AS vector_score
            FROM semantic_knowledge
            WHERE is_active = TRUE
              AND user_id = ?
            ORDER BY kw_score DESC, stability_score DESC, confidence_score DESC
            LIMIT ?
            """,
            kw_params + [query_vec, user_id] + [int(pool)],
        ).fetchall()
    else:
        where_extra, where_params = _build_any_term_filter(terms, "title", "keywords", "canonical_text")
        rows = conn.execute(
            f"""
            SELECT
                id, title, canonical_text, keywords,
                confidence_score, stability_score, embedding,
                ({kw_expr}) AS kw_score,
                0.0 AS vector_score
            FROM semantic_knowledge
            WHERE is_active = TRUE
              AND user_id = ?
              AND {where_extra}
            ORDER BY kw_score DESC, stability_score DESC, confidence_score DESC
            LIMIT ?
            """,
            kw_params + [user_id] + where_params + [int(pool)],
        ).fetchall()

    scored: list[tuple[float, tuple[Any, ...], dict[str, bool], float]] = []
    for row in rows:
        row_id, title, canonical_text, keywords, confidence_score, stability_score, embedding = row[:7]
        kw_score = float(row[7])
        vector_score = float(row[8])

        retrieval_score = kw_score * 1000.0 + float(stability_score) * 10.0 + float(confidence_score)

        scored.append((
            retrieval_score,
            row[:7],
            {
                "title": _match_any_ilike(title, terms),
                "keywords": _match_any_ilike(keywords, terms),
                "canonical_text": _match_any_ilike(canonical_text, terms),
            },
            vector_score,
        ))

    scored.sort(key=lambda item: item[0], reverse=True)

    if use_embedding:
        scored.sort(key=lambda item: (item[0] + item[3] * 2000.0), reverse=True)

    selected = scored[: int(top_k)]
    _touch_semantic_rows(conn, now=now, ids=[int(item[1][0]) for item in selected])

    results: list[dict[str, Any]] = []
    for retrieval_score, row, reasons, vector_score in selected:
        row_id, title, canonical_text, keywords, confidence_score, stability_score, _emb = row
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
            item["vector_score"] = float(vector_score)
            item["hybrid_score"] = float(retrieval_score + vector_score * 2000.0)
        results.append(item)
    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _touch_semantic_rows(conn: duckdb.DuckDBPyConnection, *, now: datetime.datetime, ids: list[int]) -> None:
    """
    Bump access_count + last_accessed_at for retrieved L2 rows.
    DuckDB: https://duckdb.org/docs/sql/query_syntax/unnest
    """
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
        results.append({
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
        })
    return results

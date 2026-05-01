from __future__ import annotations

import logging
import time
from typing import Any

import duckdb


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DuckDB FTS — Full Text Search via fts extension
#
# Built-in BM25 ranking with tokenization, stemming, stop words.
# https://duckdb.org/docs/extensions/full_text_search
#
# Design: FTS index is built at connection time and refreshed explicitly
# after writes. Search functions never rebuild the index — they assume
# the caller (manager) keeps it fresh.
#
# DuckDB FTS does not support incremental updates. Every rebuild is a
# full tokenization over the indexed table. This is acceptable because:
#   - rebuild is ~0.2s for 10K rows, ~0.7s for 50K rows
#   - writes are infrequent relative to reads in typical usage
#   - the alternative (rebuild on every search) burns latency on reads
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Build / rebuild helpers
# ---------------------------------------------------------------------------


def _build_fts_knowledge_index(conn: duckdb.DuckDBPyConnection) -> None:
    """Build or replace BM25 index on semantic_knowledge table."""
    conn.execute("INSTALL fts")
    conn.execute("LOAD fts")
    conn.execute(
        """
        PRAGMA create_fts_index(
            'semantic_knowledge', 'id', 'title', 'keywords', 'canonical_text',
            overwrite=1
        )
        """
    )


def _build_fts_context_index(conn: duckdb.DuckDBPyConnection) -> None:
    """Build or replace BM25 index on context_stream table."""
    conn.execute("INSTALL fts")
    conn.execute("LOAD fts")
    conn.execute(
        """
        PRAGMA create_fts_index(
            'context_stream', 'id', 'content', 'summary', 'tags',
            overwrite=1
        )
        """
    )


def setup_fts_index(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Initial FTS setup. Builds BM25 indexes on both semantic_knowledge
    and context_stream. Safe to call multiple times (overwrites existing).
    Called once at Manager connection time.
    """
    t0 = time.perf_counter()
    _build_fts_knowledge_index(conn)
    _build_fts_context_index(conn)
    elapsed = time.perf_counter() - t0
    logger.info("FTS indexes built (semantic_knowledge + context_stream) in %.3fs", elapsed)


def refresh_fts_indexes(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Refresh both BM25 indexes after writes. Caller should invoke this
    after every write to semantic_knowledge or context_stream.

    DuckDB FTS cannot incrementally update — a full rebuild is required.
    For small to medium datasets this is fast (~0.2s / 10K rows).
    """
    t0 = time.perf_counter()
    _build_fts_knowledge_index(conn)
    _build_fts_context_index(conn)
    elapsed = time.perf_counter() - t0
    logger.info("FTS indexes refreshed in %.3fs", elapsed)


# ---------------------------------------------------------------------------
# L2: semantic_knowledge  search
# ---------------------------------------------------------------------------


def fts_search_knowledge(
    conn: duckdb.DuckDBPyConnection,
    *,
    query: str,
    user_id: str,
    top_k: int,
) -> list[dict[str, Any]]:
    """
    BM25 full-text search over L2 semantic_knowledge.
    Assumes the FTS index is already built (caller manages freshness).
    """
    logger.debug("fts_search_knowledge: query=%r user_id=%s top_k=%d", query, user_id, top_k)
    rows = conn.execute(
        """
        SELECT
            sk.id, sk.title, sk.canonical_text, sk.keywords,
            sk.knowledge_type, sk.confidence_score, sk.stability_score,
            fts.score
        FROM (
            SELECT id, fts_main_semantic_knowledge.match_bm25(id, ?) AS score
            FROM semantic_knowledge
        ) fts
        JOIN semantic_knowledge sk ON sk.id = fts.id
        WHERE fts.score IS NOT NULL
          AND sk.is_active = TRUE
          AND sk.user_id = ?
        ORDER BY fts.score DESC
        LIMIT ?
        """,
        [query, user_id, int(top_k)],
    ).fetchall()

    results: list[dict[str, Any]] = []
    for (
        row_id, title, canonical_text, keywords,
        knowledge_type, confidence_score, stability_score, score,
    ) in rows:
        results.append({
            "id": int(row_id),
            "title": title,
            "content": canonical_text,
            "canonical_text": canonical_text,
            "keywords": keywords,
            "knowledge_type": knowledge_type,
            "confidence_score": float(confidence_score),
            "stability_score": float(stability_score),
            "bm25_score": float(score),
        })
    return results


# ---------------------------------------------------------------------------
# L1: context_stream  search
# ---------------------------------------------------------------------------


def fts_search_context(
    conn: duckdb.DuckDBPyConnection,
    *,
    query: str,
    session_id: str,
    user_id: str,
    top_k: int,
    now: Any,
) -> list[dict[str, Any]]:
    """
    BM25 full-text search over L1 context_stream.
    Assumes the FTS index is already built (caller manages freshness).
    Filters expired and archived rows.
    """
    logger.debug("fts_search_context: query=%r session_id=%s user_id=%s", query, session_id, user_id)
    rows = conn.execute(
        """
        SELECT
            cs.id, cs.session_id, cs.user_id, cs.topic, cs.event_type,
            cs.content, cs.summary, cs.tags, cs.importance_score,
            cs.salience_score, cs.created_at, cs.expires_at,
            fts.score
        FROM (
            SELECT id, fts_main_context_stream.match_bm25(id, ?) AS score
            FROM context_stream
        ) fts
        JOIN context_stream cs ON cs.id = fts.id
        WHERE fts.score IS NOT NULL
          AND cs.session_id = ?
          AND cs.user_id = ?
          AND cs.is_archived = FALSE
          AND (cs.expires_at IS NULL OR cs.expires_at > ?)
        ORDER BY fts.score DESC
        LIMIT ?
        """,
        [query, session_id, user_id, now, int(top_k)],
    ).fetchall()

    results: list[dict[str, Any]] = []
    for (
        row_id, session_id_val, user_id_val, topic, event_type,
        content, summary, tags, importance_score, salience_score,
        created_at, expires_at, score,
    ) in rows:
        results.append({
            "id": int(row_id),
            "session_id": session_id_val,
            "user_id": user_id_val,
            "topic": topic,
            "event_type": event_type,
            "content": content,
            "summary": summary,
            "tags": tags,
            "importance_score": float(importance_score),
            "salience_score": float(salience_score),
            "created_at": created_at,
            "expires_at": expires_at,
            "bm25_score": float(score),
        })
    return results

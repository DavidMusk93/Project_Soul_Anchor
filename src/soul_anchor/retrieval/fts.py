from __future__ import annotations

import logging
import time
from typing import Any, Literal

import duckdb


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DuckDB FTS — Full Text Search via fts extension
#
# Built-in BM25 ranking with tokenization, stemming, stop words.
# https://duckdb.org/docs/extensions/full_text_search
#
# Design:
#   * FTS is loaded once per process; INSTALL/LOAD are not repeated on refresh.
#   * When INSTALL fails (offline / air-gapped env) the module degrades
#     gracefully: search functions return [] and callers fall back to the
#     ILIKE-based ranking path.
#   * Refresh is table-scoped — callers rebuild only the affected table.
#   * Search queries go through the FTS index directly (single-layer SELECT)
#     so DuckDB can short-circuit non-matching rows via the inverted index.
# ---------------------------------------------------------------------------

FtsTable = Literal["semantic_knowledge", "context_stream"]

# Module-level flag: INSTALL/LOAD fts at most once per process.
_fts_loaded: bool = False
# Sticks True once INSTALL fails (e.g. offline env) so callers can fall back
# without paying the exception cost on every search.
_fts_unavailable: bool = False


def _ensure_fts_extension(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Load the FTS extension once per process. Returns True if available.
    On failure (offline env without cached extension) marks FTS unavailable
    and returns False so callers can degrade gracefully.
    """
    global _fts_loaded, _fts_unavailable
    if _fts_loaded:
        return True
    if _fts_unavailable:
        return False
    try:
        # Try LOAD first — if the extension is already installed locally this
        # avoids any network/disk INSTALL work. Fall through to INSTALL on
        # any error.
        try:
            conn.execute("LOAD fts")
        except duckdb.Error:
            conn.execute("INSTALL fts")
            conn.execute("LOAD fts")
        _fts_loaded = True
        return True
    except duckdb.Error as exc:
        _fts_unavailable = True
        logger.warning(
            "FTS extension unavailable (%s); falling back to ILIKE-only ranking.",
            exc,
        )
        return False


def is_fts_available() -> bool:
    """Whether the FTS extension has been loaded this process."""
    return _fts_loaded


# ---------------------------------------------------------------------------
# Build / rebuild helpers
# ---------------------------------------------------------------------------


_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "semantic_knowledge": ("title", "keywords", "canonical_text"),
    "context_stream": ("content", "summary", "tags"),
}


def _build_fts_index(conn: duckdb.DuckDBPyConnection, table: FtsTable) -> None:
    """Build or replace the BM25 index on a single table."""
    cols = _TABLE_COLUMNS[table]
    col_list = ", ".join(f"'{c}'" for c in cols)
    conn.execute(
        f"PRAGMA create_fts_index('{table}', 'id', {col_list}, overwrite=1)"
    )


def setup_fts_index(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Initial FTS setup. Loads the extension and builds BM25 indexes on both
    tables. Safe to call multiple times. Degrades to no-op when FTS is
    unavailable (offline env).
    """
    if not _ensure_fts_extension(conn):
        return
    t0 = time.perf_counter()
    _build_fts_index(conn, "semantic_knowledge")
    _build_fts_index(conn, "context_stream")
    elapsed = time.perf_counter() - t0
    logger.info("FTS indexes built (semantic_knowledge + context_stream) in %.3fs", elapsed)


def refresh_fts_indexes(
    conn: duckdb.DuckDBPyConnection,
    *,
    tables: tuple[FtsTable, ...] = ("semantic_knowledge", "context_stream"),
) -> None:
    """
    Rebuild the BM25 indexes for the specified tables. Callers should pass
    only the affected table(s) after a write; the default rebuilds both for
    backward compatibility.

    DuckDB FTS cannot incrementally update — a full rebuild is required.
    Degrades to no-op when FTS is unavailable.
    """
    if not _ensure_fts_extension(conn):
        return
    t0 = time.perf_counter()
    for table in tables:
        _build_fts_index(conn, table)
    elapsed = time.perf_counter() - t0
    logger.info("FTS indexes refreshed (%s) in %.3fs", ",".join(tables), elapsed)


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

    Uses the DuckDB-recommended single-layer SELECT so the FTS index can
    skip non-matching rows via the inverted index. When the extension is
    unavailable the function returns [] — callers should fall back to the
    ILIKE-based ranking path.
    """
    if not is_fts_available():
        return []
    logger.debug("fts_search_knowledge: query=%r user_id=%s top_k=%d", query, user_id, top_k)
    rows = conn.execute(
        """
        SELECT
            id, title, canonical_text, keywords,
            knowledge_type, confidence_score, stability_score,
            fts_main_semantic_knowledge.match_bm25(id, ?) AS score
        FROM semantic_knowledge
        WHERE score IS NOT NULL
          AND is_active = TRUE
          AND user_id = ?
        ORDER BY score DESC
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
    Single-layer SELECT so the FTS index drives candidate selection.
    Filters expired and archived rows. Degrades to [] when FTS is unavailable.
    """
    if not is_fts_available():
        return []
    logger.debug("fts_search_context: query=%r session_id=%s user_id=%s", query, session_id, user_id)
    rows = conn.execute(
        """
        SELECT
            id, session_id, user_id, topic, event_type,
            content, summary, tags, importance_score,
            salience_score, created_at, expires_at,
            fts_main_context_stream.match_bm25(id, ?) AS score
        FROM context_stream
        WHERE score IS NOT NULL
          AND session_id = ?
          AND user_id = ?
          AND is_archived = FALSE
          AND (expires_at IS NULL OR expires_at > ?)
        ORDER BY score DESC
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

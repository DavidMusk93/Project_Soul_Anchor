from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

# Make "src/" importable without requiring editable install.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from soul_anchor.embedding.dummy import DUMMY_EMBEDDING_DIM, DUMMY_EMBEDDING_MODEL_ID, embed_text  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="One-off embedding backfill for existing DuckDB files.")
    parser.add_argument("db_path", help="Path to the DuckDB file to update.")
    parser.add_argument("--l2", action="store_true", help="Backfill semantic_knowledge.embedding (L2).")
    parser.add_argument("--l1", action="store_true", help="Backfill context_stream.embedding (L1).")
    parser.add_argument("--dry-run", action="store_true", help="Compute counts only; do not update.")
    args = parser.parse_args()

    if not args.l1 and not args.l2:
        args.l2 = True

    conn = duckdb.connect(":memory:")
    conn.execute(f"ATTACH '{args.db_path}' AS db (STORAGE_VERSION 'v1.5.0')")
    conn.execute("USE db")

    l2_total, l2_null = conn.execute(
        "SELECT count(*), coalesce(sum(embedding IS NULL), 0) FROM semantic_knowledge"
    ).fetchone()
    l1_total, l1_null = conn.execute(
        "SELECT count(*), coalesce(sum(embedding IS NULL), 0) FROM context_stream"
    ).fetchone()

    print(f"[Before] L2 semantic_knowledge: total={l2_total}, embedding_null={l2_null}")
    print(f"[Before] L1 context_stream:   total={l1_total}, embedding_null={l1_null}")
    print(f"[Config] dim={DUMMY_EMBEDDING_DIM}, model={DUMMY_EMBEDDING_MODEL_ID}")

    if args.dry_run:
        return 0

    l2_updated = 0
    if args.l2:
        rows = conn.execute(
            """
            SELECT id, title, keywords, canonical_text
            FROM semantic_knowledge
            WHERE embedding IS NULL AND is_active = TRUE
            ORDER BY id
            """
        ).fetchall()
        for kid, title, keywords, canonical_text in rows:
            text = "\n".join([str(title or ""), str(keywords or ""), str(canonical_text or "")]).strip()
            vec = embed_text(text, dim=DUMMY_EMBEDDING_DIM)
            conn.execute("UPDATE semantic_knowledge SET embedding = ? WHERE id = ?", [vec, int(kid)])
            l2_updated += 1

    l1_updated = 0
    if args.l1:
        rows = conn.execute(
            """
            SELECT id, content, summary, tags
            FROM context_stream
            WHERE embedding IS NULL AND is_archived = FALSE
            ORDER BY id
            """
        ).fetchall()
        for eid, content, summary, tags in rows:
            text = "\n".join([str(content or ""), str(summary or ""), str(tags or "")]).strip()
            vec = embed_text(text, dim=DUMMY_EMBEDDING_DIM)
            conn.execute("UPDATE context_stream SET embedding = ? WHERE id = ?", [vec, int(eid)])
            l1_updated += 1

    l2_total2, l2_null2 = conn.execute(
        "SELECT count(*), coalesce(sum(embedding IS NULL), 0) FROM semantic_knowledge"
    ).fetchone()
    l1_total2, l1_null2 = conn.execute(
        "SELECT count(*), coalesce(sum(embedding IS NULL), 0) FROM context_stream"
    ).fetchone()

    print(f"[Update] L2 updated={l2_updated}")
    print(f"[Update] L1 updated={l1_updated}")
    print(f"[After]  L2 semantic_knowledge: total={l2_total2}, embedding_null={l2_null2}")
    print(f"[After]  L1 context_stream:   total={l1_total2}, embedding_null={l1_null2}")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


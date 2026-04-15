from __future__ import annotations

import duckdb


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS core_contract (
            contract_key VARCHAR PRIMARY KEY,
            contract_value TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 100,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS seq_context_stream START 1;
        CREATE TABLE IF NOT EXISTS context_stream (
            id BIGINT PRIMARY KEY DEFAULT nextval('seq_context_stream'),
            session_id VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            topic VARCHAR,
            event_type VARCHAR NOT NULL,
            content TEXT NOT NULL,
            summary TEXT,
            tags TEXT,
            importance_score DOUBLE DEFAULT 0.5,
            salience_score DOUBLE DEFAULT 0.5,
            source_turn_id VARCHAR,
            metadata VARIANT,
            embedding FLOAT[],
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP,
            is_archived BOOLEAN DEFAULT FALSE
        );
        """
    )

    conn.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS seq_semantic_knowledge START 1;
        CREATE TABLE IF NOT EXISTS semantic_knowledge (
            id BIGINT PRIMARY KEY DEFAULT nextval('seq_semantic_knowledge'),
            user_id VARCHAR NOT NULL,
            knowledge_type VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            canonical_text TEXT NOT NULL,
            keywords TEXT,
            source_refs TEXT,
            confidence_score DOUBLE DEFAULT 0.7,
            stability_score DOUBLE DEFAULT 0.7,
            metadata VARIANT,
            embedding FLOAT[],
            access_count BIGINT DEFAULT 0,
            last_accessed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        );
        """
    )

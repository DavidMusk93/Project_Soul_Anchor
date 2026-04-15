# Project Soul Anchor (AIME Guide)

This repository provides a local memory system built on DuckDB. It includes:

- L1 episodic memory (`context_stream`)
- L2 semantic memory (`semantic_knowledge`)
- L3 core contract (`core_contract`)
- Agentic layers (Phase 3.x): tools, decision, gating, audit, candidates, conflicts, versioning

This document is the practical guide for AIME to run and operate the project.

## Environment

### Requirements

- macOS or Linux
- Python: 3.11+
- DuckDB Python package (project uses DuckDB local file database)

### Virtualenv

Create and use a venv in repo root:

```bash
python3.11 -m venv .venv
./.venv/bin/python -m pip install --upgrade pip
```

Install runtime dependencies:

```bash
./.venv/bin/python -m pip install duckdb
```

Optional: install tooling (linter):

```bash
./.venv/bin/python -m pip install ruff
```

### ByteDance Internal PyPI (Optional)

If you want to use the internal index:

```bash
./.venv/bin/python -m pip install --index-url https://bytedpypi.byted.org/simple duckdb
```

## Project Layout

- `src/soul_anchor/manager.py`: main facade (`MemoryManager`)
- `src/soul_anchor/db/schema.py`: schema initialization (all phases)
- `src/soul_anchor/retrieval/`: ranking and context packet assembly
- `src/soul_anchor/agentic/`: Phase 3.x agentic modules
- `tests/`: unit tests (phase-based splits)
- `MemoryManager.py`: legacy shim for `from MemoryManager import MemoryManager`

## Quick Start

### Run Tests

```bash
./.venv/bin/python -m unittest -v
```

Run linter:

```bash
./.venv/bin/ruff check .
```

### Create / Connect Database

DuckDB runs as a single local file by default:

```python
from soul_anchor.manager import MemoryManager

mm = MemoryManager("aime_evolution.duckdb")
mm.connect()
```

Tables are created automatically on first connect (`init_schema`).

## Core APIs (Phase 1/2)

### L1: Save and Search Episodic Events

```python
from soul_anchor.manager import MemoryManager

mm = MemoryManager(":memory:")
mm.connect()

event_id = mm.save_episode(
    {
        "session_id": "s1",
        "user_id": "u1",
        "event_type": "user_message",
        "content": "Please keep commits short but with detail.",
        "metadata": {"channel": "chat"},
        "embedding": [0.1, 0.2, 0.3],
    }
)

recent = mm.search_recent_context_advanced(
    session_id="s1",
    user_id="u1",
    query="commits",
    top_k=5,
)
```

### L2: Save and Search Knowledge

```python
from soul_anchor.manager import MemoryManager

mm = MemoryManager(":memory:")
mm.connect()

kid = mm.save_knowledge(
    {
        "user_id": "u1",
        "knowledge_type": "workflow",
        "title": "Commit Discipline",
        "canonical_text": "Every change should be committed with a short subject and a detailed body.",
        "keywords": "commit,discipline",
        "metadata": {"source": "user_preference"},
    }
)

hits = mm.search_knowledge(user_id="u1", query="commit", top_k=5)
```

### Build Context Packet (L3 -> L2 -> L1)

```python
packet = mm.build_context_packet(
    session_id="s1",
    user_id="u1",
    query="commit discipline",
    l1_limit=10,
    l2_limit=10,
    max_chars=2000,
    deduplicate=True,
)
```

The return value is a dict:

- `core_contract`: ordered by priority desc
- `semantic_knowledge`: ranked list
- `recent_context`: ranked list
- `metadata.total_chars`: approximate payload size

## Agentic Usage (Phase 3.x)

### 1) Memory Tool API (Auditable Tools)

`MemoryToolAPI` is the recommended entry point for agentic calls (it writes audit logs):

```python
from soul_anchor.manager import MemoryManager
from soul_anchor.agentic.tools import MemoryToolAPI

mm = MemoryManager("aime_evolution.duckdb")
mm.connect()
tools = MemoryToolAPI(mm)

tools.save_episode(
    {
        "session_id": "s1",
        "user_id": "u1",
        "event_type": "user_message",
        "content": "Please remember my preferences.",
    }
)

ctx = tools.search_context(session_id="s1", user_id="u1", query="preferences", top_k=5)
know = tools.search_knowledge(user_id="u1", query="commit", top_k=5)
```

Every tool call inserts a row into `memory_audit_log`.

### 2) Decision -> Gate -> Act -> Audit (Minimal Closed Loop)

```python
from soul_anchor.agentic.audit import AgenticLoopRunner, AuditRecorder, AuditVerifier
from soul_anchor.agentic.decision_engine import DecisionEngine
from soul_anchor.agentic.gating import MemoryGating
from soul_anchor.agentic.tools import MemoryToolAPI
from soul_anchor.manager import MemoryManager

mm = MemoryManager(":memory:")
mm.connect()

runner = AgenticLoopRunner(
    decision_engine=DecisionEngine(),
    gating=MemoryGating(mm),
    tools=MemoryToolAPI(mm),
    audit_recorder=AuditRecorder(mm),
    audit_verifier=AuditVerifier(),
)

result = runner.run_event(
    session_id="s1",
    user_id="u1",
    event_type="user_message",
    content="Still use the previous approach: write tests first.",
)
```

`result` contains:

- `decision`: structured `MemoryDecision`
- `executed_actions`: executed tool action names
- `audit_ids.decision_audit_id`: audit row id for the decision step

### 3) Candidate Processing (Phase 3.2)

Candidates live in `knowledge_candidate` and are processed into:

- `merged`: inserted into `semantic_knowledge`
- `duplicate`: marked duplicate of existing semantic knowledge
- `conflict`: registered in `conflict_registry`

```python
from soul_anchor.agentic.candidates import CandidateProcessor

processor = CandidateProcessor(mm)
stats = processor.process_pending(limit=50)
```

### 4) Conflict Resolution (Phase 3.2)

Resolve a `conflict_registry` row with one of:

- `keep_existing`
- `replace`
- `merge_text`

```python
from soul_anchor.agentic.conflicts import ConflictResolver

resolver = ConflictResolver(mm)
resolver.resolve(conflict_id=123, strategy="merge_text")
```

All resolutions write an audit entry with `action_type="resolve_conflict"`.

### 5) Knowledge Versioning (Phase 3.3)

Snapshots are stored in `knowledge_version_snapshot`.

```python
from soul_anchor.agentic.versioning import KnowledgeVersioning

versioning = KnowledgeVersioning(mm)
snapshot_id = versioning.create_snapshot(knowledge_id=42, reason="before conflict resolution")
versioning.rollback_to_snapshot(knowledge_id=42, snapshot_id=snapshot_id, reason="bad merge")
```

Both operations write to `memory_audit_log` (`create_snapshot`, `rollback_to_snapshot`).

## Operations (Runbook)

### Database File Management

- Default DB file name used by examples: `aime_evolution.duckdb`
- DuckDB is a single local file. Back it up like a normal file.

Recommended:

- Keep DB files out of git (already ignored by `.gitignore`)
- Backup schedule: copy DB file daily or before risky operations (conflict resolution, batch merges)
- For incident response: restore the DB file from backup

### Safe Changes Workflow

If you are about to modify stable L2 knowledge in bulk:

1. Create a snapshot per affected knowledge row (Phase 3.3).
2. Apply conflict resolution / merge candidates.
3. If needed, rollback using the snapshot id.

### Monitoring / Observability

The main operational signal is `memory_audit_log`:

- What actions are being called (search, write, merge, resolve, rollback)
- Which user/session triggered them
- Payload and result summary

Example query:

```sql
SELECT action_type, count(*) AS n
FROM memory_audit_log
GROUP BY action_type
ORDER BY n DESC;
```

### Concurrency Notes

DuckDB is excellent for local analytics and single-process workloads. For multi-process writes:

- Prefer a single writer process
- If you need multi-writer concurrency, add an explicit queue / service layer in front of DuckDB

### Common Troubleshooting

- Import errors: ensure `./.venv` is active and you run tests from repo root.
- Failing tests: run `./.venv/bin/python -m unittest -v` and inspect the first failing module.
- Linter: run `./.venv/bin/ruff check .`


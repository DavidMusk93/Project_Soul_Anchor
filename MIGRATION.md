# Migration Guide

This document describes breaking changes and migration steps for consumers of this repository.

## 1) Import Path Change (MemoryManager)

### What Changed

The legacy root-level module `MemoryManager.py` has been removed.

### Why

- The project now uses a `src/`-based package layout (`src/soul_anchor`).
- Keeping a root-level shim creates duplicate entry points and makes refactors harder.

### How To Migrate

Replace:

```python
from MemoryManager import MemoryManager
```

With either:

```python
from soul_anchor.manager import MemoryManager
```

or:

```python
from soul_anchor import MemoryManager
```

## 2) Running From Repo Root

All examples and tests assume you run commands from the repository root:

```bash
./.venv/bin/python -m unittest -v
./.venv/bin/ruff check .
```


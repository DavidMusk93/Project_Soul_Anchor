from __future__ import annotations

import json
from typing import Any


def variant_sql_literal(value: Any) -> str:
    """
    Encode a Python object as a DuckDB VARIANT SQL literal.

    Notes:
    - We return a SQL expression (string) intended to be embedded into a query.
    - This avoids driver-specific parameter binding behavior for VARIANT.
    """
    if value is None:
        return "NULL"
    payload = json.dumps(value, ensure_ascii=False).replace("'", "''")
    return f"json('{payload}')::VARIANT"


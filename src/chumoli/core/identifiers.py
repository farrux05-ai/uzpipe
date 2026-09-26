"""chumoli.core.identifiers
==========================

Physical SQL identifier resolution across destinations.

dlt stores tables differently per destination:

- **ClickHouse** has no real schemas: every table lives in one database as
  ``{dataset}{separator}{table}`` (default separator ``___``), e.g.
  ``raw___orders``. See ``dlt.destinations.impl.clickhouse``.
- **Postgres / DuckDB / …** use the bare table name; the dataset is the
  schema and is resolved by dlt's ``sql_client`` search path.

Quality checks, row counts and preview all need the *physical* name, so
this module is the single place that knows the rule.
"""

from __future__ import annotations

from typing import Any

CLICKHOUSE_DEFAULT_SEPARATOR = "___"


def _clean(value: Any) -> str:
    return str(value or "").replace("`", "").replace('"', "").strip()


def clickhouse_separator(pipeline: Any) -> str:
    """dlt ClickHouse dataset/table separator (default ``___``)."""
    try:
        dest = getattr(pipeline, "destination", None)
        cfg = getattr(dest, "config", None) or getattr(dest, "client_config", None)
        sep = getattr(cfg, "dataset_table_separator", None) if cfg else None
        if sep:
            return str(sep)
    except Exception:
        pass
    return CLICKHOUSE_DEFAULT_SEPARATOR


def dataset_name(pipeline: Any) -> str:
    """Logical dlt dataset name (schema) used when the pipeline was built."""
    return _clean(getattr(pipeline, "dataset_name", None)) or "raw"


def physical_table_name(
    pipeline: Any,
    table: str,
    *,
    dest_key: str = "",
    dataset: str | None = None,
) -> str:
    """Unquoted physical table identifier for the destination.

    ClickHouse: ``{dataset}{separator}{table}``. Idempotent — an already
    prefixed/qualified name is returned unchanged. Other destinations:
    the bare table name.
    """
    tbl = _clean(table)
    if not tbl or dest_key != "clickhouse":
        return tbl
    sep = clickhouse_separator(pipeline)
    if sep in tbl or "." in tbl:
        return tbl
    ds = _clean(dataset) if dataset is not None else dataset_name(pipeline)
    return f"{ds or 'raw'}{sep}{tbl}"


def quote_identifier(identifier: str, dest_key: str = "") -> str:
    """Quote an identifier for the destination dialect (backticks vs double quotes)."""
    cleaned = _clean(identifier)
    if dest_key == "clickhouse":
        return f"`{cleaned}`"
    return f'"{cleaned}"'

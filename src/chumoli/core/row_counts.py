"""Row-count helpers that work for SQL and filesystem destinations."""

from __future__ import annotations

import structlog
from typing import Any

from chumoli.core.identifiers import physical_table_name, quote_identifier

log = structlog.get_logger("chumoli.row_counts")


def row_counts_from_trace(pipeline: Any) -> dict[str, int]:
    """dlt last_normalize_info.row_counts (preferred for all destinations)."""
    try:
        trace = getattr(pipeline, "last_trace", None)
        norm = getattr(trace, "last_normalize_info", None) if trace else None
        rc = getattr(norm, "row_counts", None) if norm else None
        if isinstance(rc, dict) and rc:
            return {
                str(k): int(v)
                for k, v in rc.items()
                if not str(k).startswith("_dlt")
            }
    except Exception:
        log.exception(
            "row_counts_from_trace_failed",
            pipeline=getattr(pipeline, "pipeline_name", "?"),
        )
    return {}


def row_counts_from_load_packages(load_info: Any) -> dict[str, int]:
    """Best-effort counts from completed load jobs when SQL is unavailable."""
    counts: dict[str, int] = {}
    if load_info is None:
        return counts
    packages = getattr(load_info, "load_packages", None) or []
    for package in packages:
        jobs = getattr(package, "jobs", None) or {}
        completed = jobs.get("completed_jobs") if isinstance(jobs, dict) else None
        if not completed:
            completed = getattr(package, "completed_jobs", None) or []
        for job in completed or []:
            table = getattr(job, "table_name", None)
            if table is None:
                jfi = getattr(job, "job_file_info", None)
                table = getattr(jfi, "table_name", None) if jfi else None
            if not table or str(table).startswith("_dlt"):
                continue
            rows = getattr(job, "rows_count", None)
            if rows is None:
                jfi = getattr(job, "job_file_info", None)
                rows = getattr(jfi, "rows_count", None) if jfi else None
            if rows is None:
                continue
            counts[str(table)] = counts.get(str(table), 0) + int(rows)
    return counts


def get_row_counts(
    pipeline: Any,
    *,
    dest_key: str | None = None,
    load_info: Any = None,
) -> dict[str, int]:
    """Row counts after load — never SQL-COUNT filesystem CSV (DuckDB bugs)."""
    from_trace = row_counts_from_trace(pipeline)
    if from_trace:
        return from_trace

    if dest_key in ("filesystem", "s3"):
        return row_counts_from_load_packages(load_info)

    user_tables = [
        table_name
        for table_name in getattr(
            getattr(pipeline, "default_schema", None), "tables", {}
        ).keys()
        if not table_name.startswith("_dlt")
    ]
    counts: dict[str, int] = {}
    try:
        with pipeline.sql_client() as client:
            for table_name in user_tables:
                physical = physical_table_name(
                    pipeline, table_name, dest_key=dest_key or ""
                )
                fqn = quote_identifier(physical, dest_key or "")
                result = client.execute_sql(f"SELECT COUNT(*) FROM {fqn}")
                counts[table_name] = int(result[0][0])
    except Exception:
        log.exception(
            "row_counts_sql_failed",
            pipeline=getattr(pipeline, "pipeline_name", "?"),
            dest=dest_key,
        )
        fallback = row_counts_from_load_packages(load_info)
        if fallback:
            return fallback
    return counts

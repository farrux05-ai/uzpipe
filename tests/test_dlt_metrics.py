"""dlt last_trace metrics extraction — unit tests (no real destination)."""

from __future__ import annotations

from types import SimpleNamespace

from chumoli.core.pipeline_runner import extract_dlt_metrics, _preview_fqn
from chumoli.core.quality import freshness_age_sql, _quote
from chumoli.store.run_store import RunStore


def test_extract_dlt_metrics_from_trace() -> None:
    tw = SimpleNamespace(items_count=7014)
    extract_info = SimpleNamespace(
        first_run=False,
        metrics={
            "lid": [
                {
                    "table_metrics": {
                        "orders": tw,
                        "_dlt_pipeline_state": SimpleNamespace(items_count=1),
                    }
                }
            ]
        },
    )
    pipeline = SimpleNamespace(
        last_trace=SimpleNamespace(
            steps=[
                SimpleNamespace(step="extract", step_info=extract_info),
            ]
        ),
        default_schema=SimpleNamespace(
            tables={
                "orders": {
                    "columns": {
                        "id": {},
                        "amount": {},
                        "_dlt_id": {},
                        "_dlt_load_id": {},
                    }
                },
                "_dlt_version": {"columns": {}},
            }
        ),
        state={
            "sources": {
                "src": {
                    "resources": {
                        "orders": {
                            "incremental": {
                                "updated_at": {"last_value": "2026-09-20T10:00:00"}
                            }
                        }
                    }
                }
            }
        },
    )
    load_info = SimpleNamespace(
        load_packages=[SimpleNamespace(schema_update={"orders": {"columns": {"created_by": {}}}})]
    )

    metrics = extract_dlt_metrics(pipeline, load_info)
    assert metrics["new_rows"] == 7014
    assert metrics["col_counts"] == {"orders": 2}
    assert metrics["schema_changes"] == ["orders"]
    assert metrics["cursor_last_value"] == "2026-09-20T10:00:00"
    assert metrics["is_first_run"] is False


def test_extract_dlt_metrics_empty_on_error() -> None:
    metrics = extract_dlt_metrics(SimpleNamespace(last_trace=None), None)
    assert metrics["new_rows"] == 0
    assert metrics["col_counts"] == {}
    assert metrics["is_first_run"] is True


def test_run_store_persists_metrics(tmp_path) -> None:
    store = RunStore(db_path=tmp_path / "runs.db")
    rid = store.record(
        pipeline_name="p1",
        success=True,
        quality_passed=True,
        row_counts={"orders": 128402},
        duration_seconds=4.3,
        total_rows=128402,
        rows_per_second=29860.0,
        new_rows=7014,
        col_counts={"orders": 12},
        schema_changes=["orders"],
        cursor_last_value="2026-09-20T10:00:00",
        is_first_run=False,
    )
    assert rid >= 1
    row = store.list_recent(limit=1)[0]
    assert row["new_rows"] == 7014
    assert row["col_counts"] == {"orders": 12}
    assert row["schema_changes"] == ["orders"]
    assert row["cursor_last_value"] == "2026-09-20T10:00:00"
    assert row["is_first_run"] is False


def test_run_store_migrates_old_schema(tmp_path) -> None:
    import sqlite3

    db = tmp_path / "old.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_name TEXT NOT NULL,
                success INTEGER NOT NULL,
                quality_passed INTEGER NOT NULL,
                row_counts_json TEXT NOT NULL DEFAULT '{}',
                quality_details_json TEXT NOT NULL DEFAULT '[]',
                error TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                trigger TEXT NOT NULL DEFAULT 'manual'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO runs (
                pipeline_name, success, quality_passed, started_at, finished_at
            ) VALUES ('legacy', 1, 1, 't0', 't1')
            """
        )
    store = RunStore(db_path=db)
    rows = store.list_recent(limit=5)
    assert len(rows) == 1
    assert rows[0]["new_rows"] == 0
    assert rows[0]["col_counts"] == {}
    assert rows[0]["is_first_run"] is True
    store.record(
        pipeline_name="p2",
        success=True,
        quality_passed=True,
        new_rows=10,
        is_first_run=False,
    )
    latest = store.list_recent(limit=1)[0]
    assert latest["pipeline_name"] == "p2"
    assert latest["new_rows"] == 10
    assert latest["is_first_run"] is False


def test_clickhouse_freshness_sql() -> None:
    sql = freshness_age_sql("orders", "updated_at", "clickhouse")
    assert "dateDiff('minute'" in sql
    assert "`orders`" in sql
    assert "`updated_at`" in sql
    assert "EXTRACT(EPOCH" not in sql

    pg = freshness_age_sql("orders", "updated_at", "postgres")
    assert "EXTRACT(EPOCH" in pg
    assert '"orders"' in pg


def test_clickhouse_preview_fqn() -> None:
    # ClickHouse has no real schemas — dlt stores tables as dataset___table
    assert _preview_fqn("clickhouse", "raw", "orders") == "`raw___orders`"
    assert _preview_fqn("duckdb", "raw", "orders") == '"raw"."orders"'
    assert _quote("orders", "clickhouse") == "`orders`"

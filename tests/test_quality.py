"""
Quality check executor tests.

Each check is tested against a REAL DuckDB pipeline (not mocked SQL),
in both a passing and a failing scenario, following the same
end-to-end rigor as test_e2e_rest_api_to_duckdb.py — the reasoning is
identical: a check implemented against a live destination is the only
way to be sure dlt's `sql_client()` and the SQL dialect actually work
as assumed, not just that the Python logic "looks right".
"""

from __future__ import annotations

import dlt
import pytest

from chumoli.core.config import QualityConfig
from chumoli.core.quality import run_quality_checks


@pytest.fixture
def loaded_pipeline(tmp_path):
    """A real dlt pipeline with a `people` table already loaded, for checks to run against."""
    pipeline = dlt.pipeline(
        pipeline_name="quality_test_pipeline",
        destination=dlt.destinations.duckdb(str(tmp_path / "warehouse.duckdb")),
        dataset_name="raw",
    )

    @dlt.resource(name="people", write_disposition="replace")
    def people():
        yield [
            {"id": 1, "name": "Aziz", "email": "aziz@example.com", "updated_at": "2026-09-17T10:00:00"},
            {"id": 2, "name": "Dilnoza", "email": None, "updated_at": "2026-09-17T09:00:00"},
            {"id": 3, "name": "Aziz", "email": "aziz2@example.com", "updated_at": "2026-09-17T08:00:00"},
        ]

    pipeline.run(people())
    return pipeline


def test_row_count_passes_when_minimum_met(loaded_pipeline) -> None:
    quality = QualityConfig(row_count_min=3)
    report = run_quality_checks(loaded_pipeline, quality, tables_written=["people"])

    assert report.all_passed
    assert len(report.outcomes) == 1
    assert report.outcomes[0].check_name == "row_count"


def test_row_count_fails_when_below_minimum(loaded_pipeline) -> None:
    quality = QualityConfig(row_count_min=10)
    report = run_quality_checks(loaded_pipeline, quality, tables_written=["people"])

    assert not report.all_passed
    assert "10" in report.failures[0].detail


def test_not_null_passes_for_fully_populated_column(loaded_pipeline) -> None:
    quality = QualityConfig(row_count_min=None, not_null_columns=["name"])
    report = run_quality_checks(loaded_pipeline, quality, tables_written=["people"])

    assert report.all_passed


def test_not_null_fails_when_nulls_present(loaded_pipeline) -> None:
    quality = QualityConfig(row_count_min=None, not_null_columns=["email"])
    report = run_quality_checks(loaded_pipeline, quality, tables_written=["people"])

    assert not report.all_passed
    assert "email" in report.failures[0].detail


def test_no_duplicates_fails_when_key_repeats(loaded_pipeline) -> None:
    # "name" repeats (Aziz appears twice) — this is intentionally the
    # wrong key to pick for a real primary key, used here specifically
    # to exercise the duplicate-detection path.
    quality = QualityConfig(row_count_min=None, no_duplicates_key="name")
    report = run_quality_checks(loaded_pipeline, quality, tables_written=["people"])

    assert not report.all_passed
    assert "takrorlangan" in report.failures[0].detail


def test_no_duplicates_passes_for_unique_key(loaded_pipeline) -> None:
    quality = QualityConfig(row_count_min=None, no_duplicates_key="id")
    report = run_quality_checks(loaded_pipeline, quality, tables_written=["people"])

    assert report.all_passed


def test_freshness_fails_when_data_is_old(loaded_pipeline) -> None:
    # All rows are dated 2026-09-17 in the fixture; the SLA below is
    # deliberately tiny so "now" (whenever the test runs) is always
    # far outside it, without hardcoding a specific "current time".
    quality = QualityConfig(row_count_min=None, freshness_max_minutes=1, freshness_column="updated_at")
    report = run_quality_checks(loaded_pipeline, quality, tables_written=["people"])

    assert not report.all_passed
    assert report.outcomes[0].check_name == "freshness"


def test_freshness_skipped_without_a_configured_column(loaded_pipeline) -> None:
    """freshness_max_minutes alone (no freshness_column) should not attempt the check."""
    quality = QualityConfig(
        row_count_min=None, freshness_max_minutes=1, freshness_column=None
    )
    report = run_quality_checks(loaded_pipeline, quality, tables_written=["people"])

    assert report.outcomes == []


def test_multiple_checks_combine_in_one_report(loaded_pipeline) -> None:
    quality = QualityConfig(row_count_min=3, not_null_columns=["name", "email"])
    report = run_quality_checks(loaded_pipeline, quality, tables_written=["people"])

    # row_count (1) + not_null for "name" (1) + not_null for "email" (1) = 3
    assert len(report.outcomes) == 3
    assert not report.all_passed  # email has a null
    assert len(report.failures) == 1


def test_no_checks_configured_returns_empty_report(loaded_pipeline) -> None:
    # Explicit None disables default row_count_min=1
    quality = QualityConfig(row_count_min=None)
    report = run_quality_checks(loaded_pipeline, quality, tables_written=["people"])

    assert report.outcomes == []
    assert report.all_passed  # vacuously true — no checks to fail


def test_explicit_table_name_overrides_tables_written(loaded_pipeline) -> None:
    """When QualityConfig.table_name is set, it's used instead of tables_written."""
    quality = QualityConfig(row_count_min=1, table_name="people")
    report = run_quality_checks(loaded_pipeline, quality, tables_written=["some_other_table"])

    assert report.all_passed
    assert report.outcomes[0].table_name == "people"


def test_no_duplicates_skipped_in_append_mode(loaded_pipeline) -> None:
    """append + no_duplicates would always fail on re-runs — skip as expected."""
    quality = QualityConfig(row_count_min=None, no_duplicates_key="name")
    report = run_quality_checks(
        loaded_pipeline,
        quality,
        tables_written=["people"],
        write_disposition="append",
    )
    assert report.all_passed
    assert report.outcomes[0].check_name == "no_duplicates"
    assert "append mode" in report.outcomes[0].detail


def test_freshness_bigint_milliseconds(tmp_path) -> None:
    """Payme/Click style millisecond bigint columns must not silent-fail."""
    import time

    pipeline = dlt.pipeline(
        pipeline_name="quality_ms_pipeline",
        destination=dlt.destinations.duckdb(str(tmp_path / "ms.duckdb")),
        dataset_name="raw",
    )
    now_ms = int(time.time() * 1000)

    @dlt.resource(name="receipts", write_disposition="replace")
    def receipts():
        yield [
            {"id": 1, "create_time": now_ms},
            {"id": 2, "create_time": now_ms - 60_000},
        ]

    pipeline.run(receipts())
    quality = QualityConfig(
        row_count_min=None,
        freshness_max_minutes=999999,
        freshness_column="create_time",
    )
    report = run_quality_checks(pipeline, quality, tables_written=["receipts"])
    assert report.all_passed, [o.detail for o in report.outcomes]
    assert report.outcomes[0].check_name == "freshness"
    assert "tekshirib bo'lmadi" not in report.outcomes[0].detail


def test_physical_table_prefixes_for_clickhouse(loaded_pipeline) -> None:
    """ClickHouse has no schemas — dlt stores tables as dataset___table."""
    from chumoli.core.quality import _physical_table

    assert _physical_table(loaded_pipeline, "orders", "clickhouse") == "raw___orders"
    # Already prefixed / qualified — do not double-apply
    assert _physical_table(loaded_pipeline, "raw___orders", "clickhouse") == "raw___orders"
    # Other destinations keep the logical name
    assert _physical_table(loaded_pipeline, "orders", "postgres") == "orders"
    assert _physical_table(loaded_pipeline, "orders", "duckdb") == "orders"


"""
chumoli.core.quality
======================

The content-level quality check executor: row_count, not_null,
no_duplicates, freshness.

WHY THIS FILE EXISTS (dlt-boundary.md has the full research)
------------------------------------------------------------------
dlt's free, open-source tier gives us `schema_contract` — a
STRUCTURAL guarantee (does the shape match: right tables, right
columns, right types). It does NOT give us CONTENT guarantees (is
the row count sane, are required columns actually populated, are
there duplicate keys, is the data fresh). Those checks
(`is_unique()`, `is_in()`, etc.) exist only in dltHub, a commercial
package requiring a license — confirmed by installing it, see
`docs/architecture/dlt-boundary.md`. So this module is the genuinely
new code this project needs to write; it isn't rebuilding something
dlt already gives away.

WHY THIS RUNS AS PLAIN SQL VIA `pipeline.sql_client()`, NOT A NEW
DEPENDENCY (Great Expectations, Soda, etc.)
------------------------------------------------------------------
`pipeline.sql_client()` is dlt's own, free, destination-agnostic SQL
execution API — the same `dlt.Pipeline` object we already have after
a run already exposes it, for every SQL destination dlt supports
(DuckDB, Postgres, ClickHouse, etc.). Four simple checks (COUNT,
NULL count, GROUP BY HAVING, MAX(date)) don't need a validation
framework — they need four SQL statements. Adding a dependency for
this would contradict the "zero heavy tool overhead" positioning
(docs/original-positioning/POSITIONING.md, "Batteries Included"
pillar: "Native SQL queries, no external dbt/Great Expectations
dependency").

WHY CHECKS RUN AFTER LOAD, AGAINST THE DESTINATION — NOT BEFORE,
AGAINST THE SOURCE DATA
------------------------------------------------------------------
See docs/architecture/quality-and-reliability.md, "Why quality checks
run AFTER load, not before" — intercepting data before it reaches the
destination would mean duplicating or hooking into dlt's own
extraction internals, which is exactly the kind of rewrite this
project avoids. Running checks as SQL against the already-loaded
table is simpler and destination-agnostic: the same SQL check string
works whether the destination is DuckDB or Postgres.

WHY A CHECK APPLIES TO "EVERY TABLE WRITTEN THIS RUN" WHEN
`QualityConfig.table_name` IS None
------------------------------------------------------------------
A single pipeline run can write to more than one table (e.g. a REST
API connector with multiple resources). Silently checking only one
arbitrary table would give a false sense of safety for the others.
Defaulting to "all tables this run touched" (using
`RunResult.row_counts.keys()` — already computed by pipeline_runner)
means quality checks are opt-in in aggregate ("turn this check on")
without forcing the user to know which specific table name to type,
matching the "a less technical person can operate it" requirement.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import dlt
import structlog

from chumoli.core.config import QualityConfig
from chumoli.core.identifiers import physical_table_name, quote_identifier

log = structlog.get_logger("chumoli.quality")


@dataclass
class CheckOutcome:
    """One check's result against one table."""

    check_name: str
    table_name: str
    passed: bool
    detail: str  # human-readable, plain Uzbek — shown directly in dashboard


@dataclass
class QualityReport:
    """All check outcomes for one pipeline run.

    Deliberately a flat list, not nested by table or check type —
    the dashboard's job is just to render this list (green/red per
    line), it shouldn't need to understand any grouping logic. Keep
    the grouping decision (if any) in the presentation layer, not
    baked into this data structure.
    """

    outcomes: list[CheckOutcome] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(o.passed for o in self.outcomes)

    @property
    def failures(self) -> list[CheckOutcome]:
        return [o for o in self.outcomes if not o.passed]


def _run_check_safely(check_name: str, table: str, fn, *args) -> CheckOutcome:
    """Run one quality check; on DB/SQL error return a failed outcome instead of crashing the pipeline.

    A missing table or bad column name must surface as a failed check
    (shown in the dashboard), not as an unhandled exception that marks
    the whole run as error.
    """
    try:
        return fn(*args)
    except Exception as e:
        return CheckOutcome(
            check_name=check_name,
            table_name=table,
            passed=False,
            detail=f"{table}: tekshirib bo'lmadi — {e}",
        )


def run_quality_checks(
    pipeline: dlt.Pipeline,
    quality: QualityConfig,
    tables_written: list[str],
    write_disposition: str = "replace",
) -> QualityReport:
    """Runs whichever checks are configured (non-None/non-empty) against the destination.

    `pipeline` is the SAME dlt.Pipeline object pipeline_runner just
    ran `.run()` on — reusing it (rather than reconnecting) avoids a
    second connection setup and keeps this function a pure
    "what to do with an already-run pipeline" step, callable
    independently of pipeline_runner for testing (see
    tests/test_quality.py).

    `tables_written` should be `list(RunResult.row_counts.keys())` —
    passed in explicitly rather than re-derived here, so this
    function stays destination/run-agnostic and easy to unit test
    with a hand-built list.
    """
    target_tables = [quality.table_name] if quality.table_name else tables_written
    report = QualityReport()
    dest = _destination_name(pipeline)

    if quality.row_count_min is not None:
        for table in target_tables:
            report.outcomes.append(
                _run_check_safely(
                    "row_count",
                    table,
                    _check_row_count,
                    pipeline,
                    table,
                    quality.row_count_min,
                    dest,
                )
            )

    if quality.not_null_columns:
        for table in target_tables:
            for column in quality.not_null_columns:
                report.outcomes.append(
                    _run_check_safely(
                        "not_null",
                        f"{table}.{column}",
                        _check_not_null,
                        pipeline,
                        table,
                        column,
                        dest,
                    )
                )

    if quality.no_duplicates_key:
        for table in target_tables:
            # append mode: each run re-inserts keys → aggregate looks duplicated
            if (write_disposition or "").lower() == "append":
                report.outcomes.append(
                    CheckOutcome(
                        check_name="no_duplicates",
                        table_name=f"{table}.{quality.no_duplicates_key}",
                        passed=True,
                        detail=(
                            f"{table}: append mode — takrorlanish tekshirilmadi "
                            "(kutilgan holat)"
                        ),
                    )
                )
                continue
            report.outcomes.append(
                _run_check_safely(
                    "no_duplicates",
                    f"{table}.{quality.no_duplicates_key}",
                    _check_no_duplicates,
                    pipeline,
                    table,
                    quality.no_duplicates_key,
                    dest,
                )
            )

    if quality.freshness_max_minutes is not None and quality.freshness_column:
        for table in target_tables:
            report.outcomes.append(
                _run_check_safely(
                    "freshness",
                    table,
                    _check_freshness,
                    pipeline,
                    table,
                    quality.freshness_column,
                    quality.freshness_max_minutes,
                    dest,
                )
            )

    return report


def _destination_name(pipeline: dlt.Pipeline) -> str:
    dest = getattr(pipeline, "destination", None)
    name = getattr(dest, "destination_name", None) or getattr(dest, "name", "") or ""
    return str(name).lower()


def _physical_table(pipeline: dlt.Pipeline, logical_table: str, dest: str = "") -> str:
    """Map a logical resource/table name to the physical SQL identifier.

    Thin wrapper over ``core.identifiers`` — the single source of truth for
    the ClickHouse ``dataset___table`` rule (shared with row counts and
    preview).
    """
    return physical_table_name(pipeline, logical_table, dest_key=dest)


def _quote(identifier: str, dest: str = "") -> str:
    """Minimal identifier quoting to avoid breaking on reserved words or mixed case.

    Not a defense against SQL injection — `identifier` values here
    come from PipelineConfig (table/column names the pipeline owner
    configured), never from external/untrusted data. If this
    function is ever fed a value from outside the trusted config
    chain, that call site needs its own validation first.
    """
    return quote_identifier(identifier, dest)


def freshness_age_sql(table: str, timestamp_column: str, dest: str = "") -> str:
    """Age of newest row in minutes.

    ``table`` must already be the *physical* SQL identifier (for ClickHouse
    that means ``dataset___table``). Callers use ``_physical_table`` first.

    Bigint (millisecond / second epoch) columns are supported — Payme
    ``create_time`` and similar APIs store ms since epoch as INTEGER.
    Heuristic: MAX > 1e12 → milliseconds; MAX > 1e9 → seconds; else TIMESTAMP.
    """
    tbl = _quote(table, dest)
    col = _quote(timestamp_column, dest)

    if dest == "clickhouse":
        return f"""
            SELECT dateDiff('minute',
                multiIf(
                    max({col}) > 1000000000000,
                    toDateTime(toInt64(max({col})) / 1000),
                    max({col}) > 1000000000,
                    toDateTime(toInt64(max({col}))),
                    toDateTime(max({col}))
                ),
                now()
            ) FROM {tbl}
        """

    # DuckDB / PostgreSQL
    return f"""
        SELECT EXTRACT(EPOCH FROM (
            CURRENT_TIMESTAMP -
            CASE
                WHEN MAX({col}) > 1000000000000
                    THEN to_timestamp(CAST(MAX({col}) AS DOUBLE) / 1000.0)
                WHEN MAX({col}) > 1000000000
                    THEN to_timestamp(CAST(MAX({col}) AS DOUBLE))
                ELSE CAST(MAX({col}) AS TIMESTAMP)
            END
        )) / 60 FROM {tbl}
    """


def _check_row_count(
    pipeline: dlt.Pipeline, table: str, minimum: int, dest: str = ""
) -> CheckOutcome:
    physical = _physical_table(pipeline, table, dest)
    count = _scalar(pipeline, f"SELECT COUNT(*) FROM {_quote(physical, dest)}")
    passed = count >= minimum
    detail = (
        f"{table}: {count} qator (kamida {minimum} kutilgan)"
        if passed
        else f"{table}: faqat {count} qator yuklandi, kamida {minimum} kerak edi"
    )
    log.info(
        "quality_check",
        check="row_count",
        table=table,
        physical_table=physical,
        count=count,
        minimum=minimum,
        passed=passed,
    )
    return CheckOutcome(check_name="row_count", table_name=table, passed=passed, detail=detail)


def _check_not_null(
    pipeline: dlt.Pipeline, table: str, column: str, dest: str = ""
) -> CheckOutcome:
    physical = _physical_table(pipeline, table, dest)
    null_count = _scalar(
        pipeline,
        f"SELECT COUNT(*) FROM {_quote(physical, dest)} WHERE {_quote(column, dest)} IS NULL",
    )
    passed = null_count == 0
    detail = (
        f"{table}.{column}: bo'sh qiymat yo'q"
        if passed
        else f"{table}.{column}: {null_count} ta qatorda bo'sh (NULL) qiymat topildi"
    )
    return CheckOutcome(
        check_name="not_null", table_name=f"{table}.{column}", passed=passed, detail=detail
    )


def _check_no_duplicates(
    pipeline: dlt.Pipeline, table: str, key_column: str, dest: str = ""
) -> CheckOutcome:
    physical = _physical_table(pipeline, table, dest)
    duplicate_count = _scalar(
        pipeline,
        f"""
        SELECT COUNT(*) FROM (
            SELECT {_quote(key_column, dest)}
            FROM {_quote(physical, dest)}
            GROUP BY {_quote(key_column, dest)}
            HAVING COUNT(*) > 1
        ) AS dupes
        """,
    )
    passed = duplicate_count == 0
    detail = (
        f"{table}.{key_column}: takrorlanish yo'q"
        if passed
        else f"{table}.{key_column}: {duplicate_count} ta takrorlangan qiymat topildi"
    )
    return CheckOutcome(
        check_name="no_duplicates", table_name=f"{table}.{key_column}", passed=passed, detail=detail
    )


def _check_freshness(
    pipeline: dlt.Pipeline,
    table: str,
    timestamp_column: str,
    max_minutes: int,
    dest: str = "",
) -> CheckOutcome:
    physical = _physical_table(pipeline, table, dest)
    age_minutes = _scalar(pipeline, freshness_age_sql(physical, timestamp_column, dest))
    if age_minutes is None:
        return CheckOutcome(
            check_name="freshness",
            table_name=table,
            passed=False,
            detail=f"{table}: {timestamp_column} ustunida ma'lumot topilmadi",
        )
    passed = age_minutes <= max_minutes
    detail = (
        f"{table}: eng so'nggi yozuv {int(age_minutes)} daqiqa oldin (SLA: {max_minutes} daqiqa)"
        if passed
        else f"{table}: ma'lumot {int(age_minutes)} daqiqa eski, SLA ({max_minutes} daqiqa) buzildi"
    )
    return CheckOutcome(check_name="freshness", table_name=table, passed=passed, detail=detail)


def _scalar(pipeline: dlt.Pipeline, sql: str) -> Any:
    """Runs a single-value SELECT and returns the scalar, via dlt's own sql_client.

    dlt's `execute_query` is the documented, destination-agnostic way
    to run arbitrary SQL against whatever the pipeline's destination
    is (docs/architecture/dlt-boundary.md confirms this is part of
    free dlt, not dltHub). We use it rather than reaching into
    destination-specific client libraries (e.g. duckdb.connect
    directly), so this module works unchanged regardless of which
    destination a pipeline uses.
    """
    with pipeline.sql_client() as client:
        with client.execute_query(sql) as cursor:
            row = cursor.fetchone()
            return row[0] if row else None

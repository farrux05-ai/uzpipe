"""Physical SQL identifier resolution (ClickHouse ``dataset___table``).

dlt stores ClickHouse tables as ``{dataset}{separator}{table}`` because
ClickHouse has no real schemas. Quality checks, row counts and preview
must all use that physical name — this is the shared rule.
"""

from __future__ import annotations

from types import SimpleNamespace

from chumoli.core.identifiers import (
    clickhouse_separator,
    dataset_name,
    physical_table_name,
    quote_identifier,
)


def _ch_pipeline(dataset: str = "raw", separator: str = "___") -> SimpleNamespace:
    return SimpleNamespace(
        dataset_name=dataset,
        destination=SimpleNamespace(
            config=SimpleNamespace(dataset_table_separator=separator)
        ),
    )


def test_physical_table_prefixes_for_clickhouse() -> None:
    p = _ch_pipeline()
    assert physical_table_name(p, "orders", dest_key="clickhouse") == "raw___orders"
    # Idempotent — already prefixed / qualified is returned unchanged
    assert physical_table_name(p, "raw___orders", dest_key="clickhouse") == "raw___orders"
    assert physical_table_name(p, "raw.orders", dest_key="clickhouse") == "raw.orders"


def test_physical_table_other_destinations_keep_bare_name() -> None:
    p = _ch_pipeline()
    assert physical_table_name(p, "orders", dest_key="duckdb") == "orders"
    assert physical_table_name(p, "orders", dest_key="postgres") == "orders"
    assert physical_table_name(p, "orders", dest_key="") == "orders"


def test_physical_table_honours_custom_separator() -> None:
    p = _ch_pipeline(dataset="ds", separator="__")
    assert physical_table_name(p, "orders", dest_key="clickhouse") == "ds__orders"


def test_physical_table_default_separator_when_config_missing() -> None:
    p = SimpleNamespace(dataset_name="raw", destination=None)
    assert clickhouse_separator(p) == "___"
    assert physical_table_name(p, "orders", dest_key="clickhouse") == "raw___orders"


def test_physical_table_explicit_dataset_overrides_pipeline() -> None:
    p = _ch_pipeline(dataset="ignored")
    assert (
        physical_table_name(p, "orders", dest_key="clickhouse", dataset="warehouse")
        == "warehouse___orders"
    )


def test_dataset_name_defaults_to_raw() -> None:
    assert dataset_name(SimpleNamespace(dataset_name=None)) == "raw"
    assert dataset_name(SimpleNamespace(dataset_name="  wh  ")) == "wh"


def test_quote_identifier_per_dialect() -> None:
    assert quote_identifier("orders", "clickhouse") == "`orders`"
    assert quote_identifier("orders", "duckdb") == '"orders"'
    # Backticks/quotes in the input are stripped, never passed through
    assert quote_identifier("or`ders", "clickhouse") == "`orders`"


def test_row_counts_uses_physical_name_for_clickhouse() -> None:
    """Regression: row counts must not query the bare table name on ClickHouse."""
    from chumoli.core import row_counts as rc

    executed: list[str] = []

    class _Client:
        def __enter__(self) -> "_Client":
            return self

        def __exit__(self, *_a) -> bool:
            return False

        def execute_sql(self, sql: str) -> list[tuple[int]]:
            executed.append(sql)
            return [(7,)]

    class _Pipeline:
        dataset_name = "raw"
        destination = SimpleNamespace(
            config=SimpleNamespace(dataset_table_separator="___")
        )
        default_schema = SimpleNamespace(tables={"orders": {}, "_dlt_loads": {}})

        def sql_client(self) -> _Client:
            return _Client()

    counts = rc.get_row_counts(_Pipeline(), dest_key="clickhouse")
    assert counts == {"orders": 7}
    assert executed == ["SELECT COUNT(*) FROM `raw___orders`"]

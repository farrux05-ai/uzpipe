"""Destination katalogi va connection shifrlash."""

from __future__ import annotations

from chumoli.connectors import register_builtin_connectors
from chumoli.connectors.base import registry
from chumoli.core.config import DestinationConfig, PipelineConfig
from chumoli.core.destinations import (
    DEST_CONNECTION_SECRET_KEY,
    all_destinations,
    get_destination,
)
from chumoli.security.crypto import CredentialCipher
from chumoli.store.control_store import ControlStore


def test_catalog_has_mvp_destinations() -> None:
    keys = {d["key"] for d in all_destinations()}
    assert keys >= {"duckdb", "postgresql", "filesystem", "s3", "clickhouse"}
    pg = get_destination("postgresql")
    assert pg is not None
    assert pg.needs_connection is True
    duck = get_destination("duckdb")
    assert duck is not None
    assert duck.needs_connection is False


def test_destination_connection_encrypted_not_in_config_json(tmp_path) -> None:
    register_builtin_connectors()
    cipher = CredentialCipher(key_path=tmp_path / "key")
    store = ControlStore(db_path=tmp_path / "control.db", cipher=cipher)
    manifest = registry.get_manifest("rest_api")

    config = PipelineConfig(
        name="dest_sec",
        connector_key="rest_api",
        source_params={
            "base_url": "https://example.com",
            "endpoint": "/x",
            "auth_type": "none",
        },
        destination=DestinationConfig(
            connector="postgresql",
            connection="postgresql://u:secretpass@host/db",
            dataset_name="raw",
        ),
    )
    store.save(
        config,
        raw_secrets={"secret_value": "", DEST_CONNECTION_SECRET_KEY: "postgresql://u:secretpass@host/db"},
        manifest=manifest,
    )

    # Diskdagi config_json da parol bo'lmasligi kerak
    import sqlite3

    con = sqlite3.connect(tmp_path / "control.db")
    row = con.execute("SELECT config_json, secrets_json FROM pipelines WHERE name='dest_sec'").fetchone()
    con.close()
    config_json, secrets_json = row
    assert "secretpass" not in config_json
    assert "secretpass" not in secrets_json  # shifrlangan

    loaded = store.load("dest_sec")
    assert loaded is not None
    assert loaded.config.destination.connection == "postgresql://u:secretpass@host/db"


def test_dlt_dest_alias_postgresql_to_postgres() -> None:
    """Catalog key postgresql must map to dlt.destinations.postgres."""
    from chumoli.core.pipeline_runner import _DLT_DEST_ALIASES

    assert _DLT_DEST_ALIASES.get("postgresql") == "postgres"
    # duckdb / filesystem / clickhouse use same name in dlt
    assert "duckdb" not in _DLT_DEST_ALIASES
    assert "filesystem" not in _DLT_DEST_ALIASES
    assert _DLT_DEST_ALIASES.get("s3") == "filesystem"
    assert "clickhouse" not in _DLT_DEST_ALIASES


def test_loader_file_format_per_destination() -> None:
    """ClickHouse uses parquet (not dlt's jsonl default); filesystem uses csv."""
    from chumoli.core.pipeline_runner import loader_file_format

    def cfg(connector: str, file_format: str | None = None) -> PipelineConfig:
        return PipelineConfig(
            name="fmt",
            connector_key="rest_api",
            source_params={"base_url": "https://x", "endpoint": "/", "auth_type": "none"},
            destination=DestinationConfig(
                connector=connector,
                connection="x",
                dataset_name="raw",
                file_format=file_format,
            ),
        )

    # ClickHouse: parquet by default; jsonl honoured; csv is invalid → parquet
    assert loader_file_format(cfg("clickhouse")) == "parquet"
    assert loader_file_format(cfg("clickhouse", "jsonl")) == "jsonl"
    assert loader_file_format(cfg("clickhouse", "csv")) == "parquet"
    # Filesystem / S3: csv by default, overridable
    assert loader_file_format(cfg("filesystem")) == "csv"
    assert loader_file_format(cfg("filesystem", "parquet")) == "parquet"
    # Others: dlt's own default
    assert loader_file_format(cfg("duckdb")) is None
    assert loader_file_format(cfg("postgresql")) is None

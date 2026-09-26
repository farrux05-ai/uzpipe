"""pipeline_runner — ControlStore + connector → dlt.run(); quality; notify; recovery."""

from __future__ import annotations

import uuid
import threading
import time
import tracemalloc
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import perf_counter
from typing import Any

import dlt
import structlog

from chumoli.connectors.base import BaseUZConnector, registry
from chumoli.core.config import PipelineConfig
from chumoli.core.errors import sanitize_error
from chumoli.core.quality import QualityReport, run_quality_checks
from chumoli.core.row_counts import get_row_counts
from chumoli.store.control_store import ControlStore, StoredPipeline

log = structlog.get_logger("chumoli.pipeline_runner")

# Parallel run guard: same pipeline_name + concurrent dlt.run → LoadPackageNotFound
# (dlt working dir shared). One in-process run per name at a time.
_RUNNING_PIPELINES: dict[str, dict[str, Any]] = {}
_RUNNING_LOCK = threading.Lock()


class PipelineAlreadyRunning(RuntimeError):
    """Same pipeline is already executing in this process."""


@dataclass
class RunResult:
    pipeline_name: str
    load_info: Any
    row_counts: dict[str, int]
    quality_report: QualityReport
    duration_seconds: float = 0.0
    peak_memory_mb: float = 0.0
    # dlt last_trace / schema / state — never computed by hand
    new_rows: int = 0
    col_counts: dict[str, int] = field(default_factory=dict)
    schema_changes: list[str] = field(default_factory=list)
    cursor_last_value: Any = None
    is_first_run: bool = True
    # Human-readable reason when the load itself failed (dlt failed jobs).
    # Exceptions raised before a RunResult exists are recorded separately.
    error: str | None = None

    @property
    def success(self) -> bool:
        return not self.load_info.has_failed_jobs

    @property
    def total_rows(self) -> int:
        return sum(self.row_counts.values())

    @property
    def rows_per_second(self) -> float:
        if self.duration_seconds <= 0:
            return 0.0
        return self.total_rows / self.duration_seconds


# UI/catalog keys → dlt.destinations attribute names (when they differ).
# Catalog keeps user-facing names (postgresql); dlt module uses postgres.
# s3 → filesystem (same dlt destination, different UI / validation).
_DLT_DEST_ALIASES: dict[str, str] = {
    "postgresql": "postgres",
    "s3": "filesystem",
}

# Rough scheme hints so a Postgres URL is never passed to ClickHouse (and vice versa).
_DEST_SCHEME_HINTS: dict[str, tuple[str, ...]] = {
    "postgresql": ("postgresql://", "postgres://", "postgresql+", "postgres+"),
    "clickhouse": ("clickhouse://", "clickhouses://", "http://", "https://"),
}


def _warn_connection_scheme(dest_key: str, connection: str) -> None:
    """Log if connection string clearly belongs to another destination type."""
    if not connection or not isinstance(connection, str):
        return
    low = connection.strip().lower()
    for other_key, prefixes in _DEST_SCHEME_HINTS.items():
        if other_key == dest_key:
            continue
        if any(low.startswith(p) for p in prefixes):
            # Likely UI cache leak: PG string saved under ClickHouse (or reverse)
            raise ValueError(
                f"Destination '{dest_key}' uchun connection boshqa turga o'xshaydi "
                f"({other_key}). Destination turini tekshiring yoki connection ni qayta kiriting."
            )


def build_dlt_pipeline(config: PipelineConfig) -> dlt.Pipeline:
    from chumoli.core.paths import (
        ensure_runtime_dirs,
        pipelines_dir,
        resolve_duckdb_path,
        resolve_s3_url,
    )

    ensure_runtime_dirs()
    dest_key = config.destination.connector
    connection = config.destination.connection
    dlt_key = _DLT_DEST_ALIASES.get(dest_key, dest_key)

    # DuckDB without an explicit path used to write <cwd>/<name>.duckdb — force under CHUMOLI_HOME/data
    if dest_key == "duckdb":
        connection = resolve_duckdb_path(connection or "", config.name)

    if dest_key == "filesystem":
        # dlt writes to HIDDEN staging (~/.chumoli/fs_staging/<pipeline>/)
        # including _dlt_* metadata. After load we publish only data tables to
        # the user-visible path (connection or ~/chumoli-data/exports/<pipeline>/).
        from chumoli.core.paths import fs_staging_dir

        staging = fs_staging_dir(config.name)
        bucket_url = str(staging)
        destination = dlt.destinations.filesystem(
            bucket_url=bucket_url,
            layout="{table_name}/{load_id}.{file_id}.{ext}",
        )
    elif dest_key == "s3":
        bucket_url = resolve_s3_url(connection)
        destination = dlt.destinations.filesystem(
            bucket_url=bucket_url,
            layout="{table_name}/{load_id}.{file_id}.{ext}",
        )
    elif connection:
        _warn_connection_scheme(dest_key, connection)
        try:
            destination_factory = getattr(dlt.destinations, dlt_key)
        except AttributeError as e:
            extra = dlt_key
            raise ValueError(
                f"dlt destination '{dest_key}' o'rnatilmagan (dlt key: '{dlt_key}'). "
                f"O'rnating: pip install \"dlt[{extra}]\"  "
                f"yoki boshqa destination tanlang (DuckDB / PostgreSQL / Fayl)."
            ) from e
        try:
            destination = destination_factory(credentials=connection)
        except Exception as e:
            # Surface missing extra / wrong credentials clearly
            msg = str(e).lower()
            if "no module" in msg or "not installed" in msg or "extra" in msg:
                raise ValueError(
                    f"Destination '{dest_key}' uchun dlt extra kerak: "
                    f'pip install "dlt[{dlt_key}]". '
                    f"Asl xato: {e}"
                ) from e
            raise
    else:
        destination = dlt_key

    pdir = pipelines_dir()
    return dlt.pipeline(
        pipeline_name=config.name,
        destination=destination,
        dataset_name=config.destination.dataset_name,
        pipelines_dir=str(pdir),
    )


def resolve_export_path(config: PipelineConfig) -> str | None:
    """Absolute local export folder for filesystem destination; None for remote/other."""
    from chumoli.core.paths import is_remote_url, resolve_filesystem_url

    if config.destination.connector != "filesystem":
        return None
    conn = config.destination.connection or ""
    if is_remote_url(conn):
        return None
    try:
        return resolve_filesystem_url(conn, config.name, allow_remote=False)
    except Exception:
        return None


def get_running_pipelines() -> dict[str, dict[str, Any]]:
    """Currently executing pipelines and coarse progress (for UI polling)."""
    with _RUNNING_LOCK:
        return {k: dict(v) for k, v in _RUNNING_PIPELINES.items()}


def _set_run_step(name: str, step: str, **extra: Any) -> None:
    with _RUNNING_LOCK:
        info = _RUNNING_PIPELINES.get(name)
        if info is None:
            return
        info["step"] = step
        info.update(extra)


def _as_mapping(obj: Any) -> dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "items"):
        try:
            return dict(obj.items())
        except Exception:
            return {}
    return {}


def _items_count(tw: Any) -> int:
    if tw is None:
        return 0
    if isinstance(tw, dict):
        return int(tw.get("items_count") or 0)
    return int(getattr(tw, "items_count", 0) or 0)


def _jsonable_cursor(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _user_col_counts(pipeline: Any) -> dict[str, int]:
    schema = getattr(pipeline, "default_schema", None)
    tables = _as_mapping(getattr(schema, "tables", None))
    out: dict[str, int] = {}
    for tname, tdef in tables.items():
        if str(tname).startswith("_dlt"):
            continue
        cols = _as_mapping(tdef.get("columns") if isinstance(tdef, dict) else getattr(tdef, "columns", None))
        out[str(tname)] = len([c for c in cols if not str(c).startswith("_dlt")])
    return out


def extract_dlt_metrics(pipeline: Any, load_info: Any = None) -> dict[str, Any]:
    """Read run metrics from dlt last_trace / schema / state. Never compute by hand.

    Failures return empty defaults so a run is never blocked by metrics.
    """
    metrics: dict[str, Any] = {
        "new_rows": 0,
        "col_counts": {},
        "schema_changes": [],
        "cursor_last_value": None,
        "is_first_run": True,
    }
    try:
        t = getattr(pipeline, "last_trace", None)
        steps = list(getattr(t, "steps", None) or [])
        if t is not None and steps:
            step0_info = getattr(steps[0], "step_info", None)
            if step0_info is not None and hasattr(step0_info, "first_run"):
                metrics["is_first_run"] = bool(step0_info.first_run)

            extract_step = next((s for s in steps if getattr(s, "step", None) == "extract"), None)
            if extract_step is not None:
                info = getattr(extract_step, "step_info", None)
                extract_metrics = _as_mapping(getattr(info, "metrics", None))
                new_rows = 0
                for metric_list in extract_metrics.values():
                    for m in metric_list or []:
                        table_metrics = (
                            m.get("table_metrics", {})
                            if isinstance(m, dict)
                            else getattr(m, "table_metrics", None)
                        )
                        for table, tw in _as_mapping(table_metrics).items():
                            if not str(table).startswith("_dlt"):
                                new_rows += _items_count(tw)
                metrics["new_rows"] = new_rows

        metrics["col_counts"] = _user_col_counts(pipeline)

        packages = list(getattr(load_info, "load_packages", None) or [])
        if packages:
            schema_update = _as_mapping(getattr(packages[0], "schema_update", None))
            metrics["schema_changes"] = [
                str(k) for k in schema_update if not str(k).startswith("_dlt")
            ]

        cursor_last_value = None
        state = _as_mapping(getattr(pipeline, "state", None))
        for source_data in _as_mapping(state.get("sources")).values():
            resources = _as_mapping(
                source_data.get("resources")
                if isinstance(source_data, dict)
                else getattr(source_data, "resources", None)
            )
            for res_data in resources.values():
                incremental = _as_mapping(
                    res_data.get("incremental")
                    if isinstance(res_data, dict)
                    else getattr(res_data, "incremental", None)
                )
                for col_state in incremental.values():
                    last = (
                        col_state.get("last_value")
                        if isinstance(col_state, dict)
                        else getattr(col_state, "last_value", None)
                    )
                    if last is not None:
                        cursor_last_value = last
        metrics["cursor_last_value"] = _jsonable_cursor(cursor_last_value)
    except Exception:
        log.warning("metrics_extraction_failed", exc_info=True)
    return metrics


def run_pipeline_by_name(name: str, store: ControlStore | None = None) -> RunResult:
    store = store or ControlStore()
    stored = store.load(name)
    if stored is None:
        raise KeyError(f"Pipeline '{name}' topilmadi")

    run_id = uuid.uuid4().hex[:12]
    structlog.contextvars.bind_contextvars(pipeline=name, run_id=run_id)
    log.info("pipeline_run_start", connector=stored.config.connector_key)

    with _RUNNING_LOCK:
        if name in _RUNNING_PIPELINES:
            structlog.contextvars.clear_contextvars()
            raise PipelineAlreadyRunning(
                f"Pipeline '{name}' hozir ishga tushgan. "
                "Tugaguncha kuting — parallel run dlt holatini buzadi."
            )
        _RUNNING_PIPELINES[name] = {
            "started_at": time.time(),
            "step": "starting",
            "run_id": run_id,
            "rows_so_far": 0,
        }

    try:
        connector = registry.get(stored.config.connector_key)
        result = _execute(stored, connector)
        log.info(
            "pipeline_run_done",
            success=result.success,
            total_rows=result.total_rows,
            duration_seconds=result.duration_seconds,
            rows_per_second=round(result.rows_per_second, 1),
        )
        try:
            from chumoli.core.notify import maybe_notify_run

            maybe_notify_run(
                store=store,
                notify_cfg=stored.config.notify,
                pipeline_name=result.pipeline_name,
                success=result.success,
                quality_passed=result.quality_report.all_passed,
                row_counts=result.row_counts,
                quality_details=[
                    {"passed": o.passed, "detail": o.detail}
                    for o in result.quality_report.outcomes
                ],
                error=result.error,
                duration_seconds=result.duration_seconds,
            )
        except Exception:
            log.exception("notify_failed", pipeline=name)
        return result
    finally:
        with _RUNNING_LOCK:
            _RUNNING_PIPELINES.pop(name, None)
        structlog.contextvars.clear_contextvars()


def record_run_result(
    runs: Any,
    result: RunResult,
    *,
    trigger: str,
    started_at: datetime | None = None,
) -> None:
    """Persist a completed run (load OK or dlt failed-jobs) to run history.

    Best-effort: a history write must never fail the run itself.
    """
    details = [
        {"passed": o.passed, "detail": o.detail} for o in result.quality_report.outcomes
    ]
    try:
        runs.record(
            pipeline_name=result.pipeline_name,
            success=result.success,
            quality_passed=result.quality_report.all_passed,
            row_counts=result.row_counts,
            quality_details=details,
            error=sanitize_error(result.error) if result.error else None,
            trigger=trigger,
            started_at=started_at,
            duration_seconds=result.duration_seconds,
            total_rows=result.total_rows,
            rows_per_second=result.rows_per_second,
            new_rows=result.new_rows,
            col_counts=result.col_counts,
            schema_changes=result.schema_changes,
            cursor_last_value=result.cursor_last_value,
            is_first_run=result.is_first_run,
            peak_memory_mb=result.peak_memory_mb,
        )
    except Exception:
        log.exception("run_record_failed", pipeline=result.pipeline_name, trigger=trigger)


def record_run_failure(
    runs: Any,
    pipeline_name: str,
    error: BaseException | str,
    *,
    trigger: str,
    started_at: datetime | None = None,
) -> None:
    """Persist a run that raised before producing a RunResult.

    This is what makes a failed manual/async run visible in the dashboard
    instead of only a transient toast. Best-effort.
    """
    try:
        runs.record(
            pipeline_name=pipeline_name,
            success=False,
            quality_passed=False,
            error=sanitize_error(error),
            trigger=trigger,
            started_at=started_at,
            finished_at=datetime.now(UTC),
        )
    except Exception:
        log.exception("run_record_failed", pipeline=pipeline_name, trigger=trigger)


def _execute(stored: StoredPipeline, connector: BaseUZConnector) -> RunResult:
    name = stored.config.name
    log.info("step_extract_start")
    _set_run_step(name, "extract")
    source = connector.build_dlt_source(stored.config.source_params, stored.secrets)
    pipeline = build_dlt_pipeline(stored.config)

    t0 = perf_counter()
    log.info("step_load_start")
    _set_run_step(name, "run")
    run_kwargs: dict[str, Any] = {
        "write_disposition": stored.config.write_disposition.value,
        "primary_key": stored.config.primary_key or None,
    }
    # Filesystem / S3: explicit loader format (csv default for local-friendly exports)
    dest = stored.config.destination
    if dest.connector in ("filesystem", "s3"):
        import os

        fmt = dest.file_format or "csv"
        run_kwargs["loader_file_format"] = fmt
        # CSV: uncompressed for Excel; other formats must not inherit this process env
        if fmt == "csv":
            os.environ["NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION"] = "true"
        else:
            os.environ.pop("NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION", None)

    # Stale pending packages from a previous failed load (e.g. _dlt_load_id
    # NOT NULL) block the next run — clear them before starting.
    _clear_pending_packages(pipeline, name, reason="pre_run")

    peak_memory_mb = 0.0
    tracemalloc.start()
    try:
        try:
            load_info = pipeline.run(source, **run_kwargs)
        except Exception as run_exc:
            # Recover once from broken pending package / NULL _dlt_load_id /
            # partially loaded packages that block every subsequent run.
            msg = str(run_exc)
            if _is_pending_or_constraint_error(msg):
                log.warning(
                    "load_constraint_retry",
                    detail=msg[:200],
                )
                _clear_pending_packages(pipeline, name, reason="retry", aggressive=True)
                load_info = pipeline.run(source, **run_kwargs)
            else:
                raise
        _, peak_mem = tracemalloc.get_traced_memory()
        peak_memory_mb = round(peak_mem / 1024 / 1024, 1)
    finally:
        tracemalloc.stop()
    duration = perf_counter() - t0

    log.info(
        "step_load_done",
        has_failed_jobs=load_info.has_failed_jobs,
        duration_seconds=round(duration, 3),
    )

    load_succeeded = not load_info.has_failed_jobs
    load_error: str | None = None
    if not load_succeeded:
        load_error = _summarize_failed_jobs(pipeline)
    _set_run_step(name, "count" if load_succeeded else "failed")
    row_counts = (
        get_row_counts(
            pipeline,
            dest_key=dest.connector,
            load_info=load_info,
        )
        if load_succeeded
        else {}
    )
    if row_counts:
        _set_run_step(name, "count", rows_so_far=sum(row_counts.values()))

    if load_succeeded and dest.connector == "filesystem":
        # Publish data-only tables to user-visible folder (no _dlt_* metadata)
        from pathlib import Path

        from chumoli.core.paths import (
            fs_staging_dir,
            publish_filesystem_export,
            resolve_filesystem_url,
        )

        try:
            staging_root = fs_staging_dir(name)
            dataset = (dest.dataset_name or "raw").strip() or "raw"
            # dlt layout: <bucket>/<dataset>/<table>/…
            staging_data = staging_root / dataset
            if not staging_data.is_dir():
                staging_data = staging_root
            visible = Path(
                resolve_filesystem_url(
                    dest.connection or "",
                    name,
                    allow_remote=False,
                )
            )
            # Put clean tables at top of user-chosen folder (not nested dataset+_dlt)
            replace = stored.config.write_disposition.value == "replace"
            published = publish_filesystem_export(
                staging_data, visible, replace=replace
            )
            log.info(
                "filesystem_published",
                staging=str(staging_data),
                visible=str(visible),
                tables=published,
            )
        except Exception as pub_exc:
            log.warning("filesystem_publish_failed", error=str(pub_exc)[:300])

    if load_succeeded:
        log.info("step_quality_start", tables=list(row_counts.keys()))
        _set_run_step(name, "quality")
        # Filesystem / S3 (CSV/Parquet files) — SQL quality checks don't apply reliably
        if dest.connector in ("filesystem", "s3"):
            quality_report = QualityReport()
        else:
            quality_report = run_quality_checks(
                pipeline,
                stored.config.quality,
                tables_written=list(row_counts.keys()),
                write_disposition=stored.config.write_disposition.value,
            )
        log.info(
            "step_quality_done",
            all_passed=quality_report.all_passed,
            failures=[
                o.detail for o in quality_report.outcomes if not o.passed
            ],
        )
    else:
        quality_report = QualityReport()

    dlt_metrics = extract_dlt_metrics(pipeline, load_info)

    _set_run_step(name, "done", rows_so_far=sum(row_counts.values()))
    return RunResult(
        pipeline_name=stored.config.name,
        load_info=load_info,
        row_counts=row_counts,
        quality_report=quality_report,
        duration_seconds=round(duration, 3),
        peak_memory_mb=peak_memory_mb,
        new_rows=int(dlt_metrics.get("new_rows") or 0),
        col_counts=dlt_metrics.get("col_counts") or {},
        schema_changes=list(dlt_metrics.get("schema_changes") or []),
        cursor_last_value=dlt_metrics.get("cursor_last_value"),
        is_first_run=bool(dlt_metrics.get("is_first_run", True)),
        error=load_error,
    )


def _load_stored(name: str, store: ControlStore | None = None):
    store = store or ControlStore()
    stored = store.load(name)
    if stored is None:
        raise KeyError(f"Pipeline '{name}' topilmadi")
    return store, stored


def _uz_step_fail_detail(step_name: str, exception_text: str) -> str:
    """dlt step failure → qisqa o'zbekcha tavsif.

    dlt ko'pincha multi-line xabar beradi: birinchi qator umumiy
    'Pipeline execution failed at step=...', keyinroq qatorlarda asl
    sabab ('caused an exception: ...'). Eng ma'noli qatorni tanlaymiz.
    """
    step_labels = {
        "extract": "ma'lumot olish (extract)",
        "normalize": "normalizatsiya",
        "load": "yuklash (load)",
        "run": "pipeline ishga tushirish",
        "sync": "destination bilan sinxronlash",
    }
    label = step_labels.get(step_name, step_name)
    text = (exception_text or "").strip()
    if not text:
        return f"{label} bosqichida xato: noma'lum xato"

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    # prefer the line with the actual root cause
    preferred = None
    for ln in lines:
        low = ln.lower()
        if "caused an exception:" in low or "connection refused" in low or "operationalerror" in low:
            preferred = ln
            break
    if preferred is None:
        # skip pure class-name lines like "<class '...'>"
        for ln in reversed(lines):
            if not (ln.startswith("<class ") and ln.endswith(">")):
                preferred = ln
                break
    if preferred is None:
        preferred = lines[0]

    if len(preferred) > 240:
        preferred = preferred[:237] + "..."
    return f"{label} bosqichida xato: {preferred}"


def _failed_jobs_from_pipeline(pipeline: Any) -> list[dict[str, str]]:
    """Real failed steps/jobs from the last trace (no placeholder).

    dlt 1.30: PipelineTrace.steps[*].step_exception faqat step yiqilganda
    to'ldiriladi. Muvaffaqiyatli run'da step_exception=None — ular bu yerga
    kirmaydi. "Oxirgi ish kuzatuvi mavjud" kabi mazmunsiz qator yozilmaydi.
    """
    out: list[dict[str, str]] = []
    try:
        last = pipeline.last_trace
        if last is not None:
            for step in getattr(last, "steps", []) or []:
                exc = getattr(step, "step_exception", None)
                if not exc:
                    continue
                step_name = str(getattr(step, "step", "unknown"))
                # step_exception odatda multi-line va asl sababni o'z ichiga oladi
                detail = _uz_step_fail_detail(step_name, str(exc))
                out.append({"job": f"step:{step_name}", "detail": detail})

            # package-level failed jobs (load_id ma'lum bo'lsa)
            fn = getattr(pipeline, "list_failed_jobs_in_package", None)
            if callable(fn):
                load_ids: list[str] = []
                load_info = getattr(last, "last_load_info", None)
                if load_info is not None:
                    loads = getattr(load_info, "loads_ids", None) or getattr(
                        load_info, "load_ids", None
                    )
                    if loads:
                        load_ids = list(loads)
                for lid in load_ids:
                    try:
                        jobs = fn(lid)
                    except TypeError as e:
                        log.warning("failed_jobs_lookup_unsupported", error=str(e))
                        break
                    except Exception as e:
                        log.warning("failed_jobs_lookup_error", error=str(e))
                        continue
                    if not jobs:
                        continue
                    for j in jobs:
                        out.append(
                            {
                                "job": str(getattr(j, "job_id", j)),
                                "detail": f"Yuklash job muvaffaqiyatsiz: {j}",
                            }
                        )
    except Exception as e:
        log.warning("get_failed_jobs_error", error=str(e))
        out.append({"job": "error", "detail": f"Failed jobs o'qib bo'lmadi: {e}"})
    return out


def _summarize_failed_jobs(pipeline: Any) -> str:
    """One-line, secret-free reason for a failed load (stored in run history)."""
    jobs = _failed_jobs_from_pipeline(pipeline)
    details = [j["detail"] for j in jobs if j.get("job") not in ("none", "error")]
    if not details:
        details = [j["detail"] for j in jobs]
    text = "; ".join(details) if details else "Load muvaffaqiyatsiz (sabab aniqlanmadi)"
    return sanitize_error(text, max_len=1000)


def get_failed_jobs(name: str, store: ControlStore | None = None) -> list[dict[str, str]]:
    """Faqat haqiqiy muvaffaqiyatsizliklarni qaytaradi (o'zbekcha detail)."""
    _, stored = _load_stored(name, store)
    pipeline = build_dlt_pipeline(stored.config)
    out = _failed_jobs_from_pipeline(pipeline)
    if not out:
        out.append(
            {
                "job": "none",
                "detail": "Hozircha muvaffaqiyatsiz ish topilmadi",
            }
        )
    return out


def _is_pending_or_constraint_error(msg: str) -> bool:
    """True when the failure is a stuck pending package or _dlt_load_id NOT NULL."""
    needles = (
        "_dlt_load_id",
        "Constraint Error",
        "NOT NULL constraint",
        "pending package",
        "LoadPackageAlreadyCompleted",
        "partially loaded",
        "Package with `load_id",
    )
    low = msg or ""
    return any(n in low for n in needles)


def _clear_pending_packages(
    pipeline: Any,
    name: str,
    *,
    reason: str = "manual",
    aggressive: bool = False,
) -> None:
    """Abort/drop pending dlt packages; on failure, wipe load/normalized + load/new.

    dlt 1.30 deprecates drop_pending_packages in favour of abort_packages. abort_packages
    may itself call load() and fail on a corrupt package — in that case we fall back to
    deleting the pending directories under the pipeline working dir.
    """
    import shutil
    from pathlib import Path

    cleared = False
    # Prefer abort_packages (dlt >= 1.30); fall back to deprecated alias.
    for method_name in ("abort_packages", "drop_pending_packages"):
        fn = getattr(pipeline, method_name, None)
        if not callable(fn):
            continue
        try:
            if method_name == "drop_pending_packages":
                fn(with_partial_loads=True)
            else:
                fn()
            cleared = True
            log.info(
                "pending_cleared",
                method=method_name,
                reason=reason,
            )
            break
        except Exception as e:
            log.warning(
                "pending_clear_failed",
                method=method_name,
                reason=reason,
                error=str(e)[:200],
            )

    if cleared and not aggressive:
        return

    # Aggressive / fallback: remove pending package dirs so the next extract is clean.
    try:
        working = Path(getattr(pipeline, "working_dir", "") or "")
        if not working.is_dir():
            return
        for sub in ("load/normalized", "load/new", "normalize/normalized", "extract"):
            target = working / sub
            if not target.exists():
                continue
            for child in list(target.iterdir()):
                try:
                    if child.is_dir():
                        shutil.rmtree(child, ignore_errors=True)
                    else:
                        child.unlink(missing_ok=True)
                except Exception:
                    log.exception(
                        "pending_dir_wipe_failed", path=str(child)
                    )
            log.info(
                "pending_dir_wiped",
                path=str(target),
                reason=reason,
            )
    except Exception:
        log.exception("pending_aggressive_clear_failed", reason=reason)


def _recovery_blocked_if_running(name: str) -> dict[str, str] | None:
    """Recovery must never run while the pipeline is loading.

    Wiping pending dirs or dropping a table mid-load corrupts dlt state.
    Returns an error payload to hand back to the caller, or None if safe.
    """
    if name in get_running_pipelines():
        return {
            "status": "error",
            "detail": (
                f"Pipeline '{name}' hozir ishlamoqda — tiklash uchun tugashini kuting."
            ),
        }
    return None


def drop_pending_packages(name: str, store: ControlStore | None = None) -> dict[str, str]:
    _, stored = _load_stored(name, store)
    blocked = _recovery_blocked_if_running(name)
    if blocked:
        return blocked
    pipeline = build_dlt_pipeline(stored.config)
    try:
        _clear_pending_packages(pipeline, name, reason="api", aggressive=True)
        log.info("recover_drop_pending", pipeline=name)
        return {"status": "ok", "detail": "Pending paketlar o'chirildi"}
    except Exception as e:
        log.exception("recover_drop_pending_failed", pipeline=name)
        return {
            "status": "error",
            "detail": sanitize_error(f"Pending paketlar o'chirilmadi: {e}"),
        }


def sync_from_destination(name: str, store: ControlStore | None = None) -> dict[str, str]:
    _, stored = _load_stored(name, store)
    blocked = _recovery_blocked_if_running(name)
    if blocked:
        return blocked
    pipeline = build_dlt_pipeline(stored.config)
    try:
        pipeline.sync_destination()
        log.info("recover_sync", pipeline=name)
        return {"status": "ok", "detail": "Destination bilan sinxronlashtirildi"}
    except Exception as e:
        log.exception("recover_sync_failed", pipeline=name)
        return {
            "status": "error",
            "detail": sanitize_error(f"Sinxronlash xatosi: {e}"),
        }


def _preview_fqn(dest_key: str, dataset: str, table_name: str) -> str:
    """Schema-qualified table name, dialect-aware (ClickHouse uses backticks)."""
    ds = str(dataset).replace("`", "").replace('"', "")
    tbl = str(table_name).replace("`", "").replace('"', "")
    if dest_key == "clickhouse":
        return f"`{ds}`.`{tbl}`"
    return f'"{ds}"."{tbl}"'


def _list_export_files(export_root: str, dataset: str) -> list[dict[str, Any]]:
    """List user data files under export path, skipping _dlt* metadata folders."""
    from pathlib import Path

    root = Path(export_root)
    # dlt writes under <bucket>/<dataset_name>/…
    candidates = [root / dataset, root]
    files: list[dict[str, Any]] = []
    seen: set[str] = set()
    for base in candidates:
        if not base.is_dir():
            continue
        for p in sorted(base.rglob("*")):
            if not p.is_file():
                continue
            # Skip dlt internal metadata tables/folders
            parts = {x.lower() for x in p.parts}
            if any(part.startswith("_dlt") for part in parts):
                continue
            if p.name.startswith(".") or p.name == "init":
                continue
            key = str(p.resolve())
            if key in seen:
                continue
            seen.add(key)
            try:
                rel = str(p.relative_to(root))
            except ValueError:
                rel = p.name
            files.append(
                {
                    "path": key,
                    "relative": rel,
                    "name": p.name,
                    "size": p.stat().st_size,
                    "suffix": p.suffix.lower(),
                }
            )
    return files


def _sample_csv_file(path: str, limit: int) -> dict[str, Any]:
    """Read header + first N data rows from a local CSV (no full load)."""
    import csv

    columns: list[str] = []
    rows: list[list[Any]] = []
    try:
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                return {"columns": [], "rows": []}
            columns = [str(c) for c in header]
            for i, row in enumerate(reader):
                if i >= limit:
                    break
                # pad / trim to header length
                cells = list(row) + [""] * max(0, len(columns) - len(row))
                rows.append(cells[: len(columns)])
    except Exception as e:
        return {"columns": [], "rows": [], "error": str(e)}
    return {"columns": columns, "rows": rows}


def get_preview_rows(
    name: str, store: ControlStore | None = None, limit: int = 10
) -> dict[str, Any]:
    """First N rows per loaded table (UI preview). Max 3 tables.

    For local filesystem: returns export_path + file list + CSV samples
    (no SQL). For S3: returns path hint only (no local files).
    """
    # DuckDB (and many warehouses) reject concurrent writers — never open
    # a second connection while this pipeline is still loading.
    running = get_running_pipelines()
    if name in running:
        step = running[name].get("step", "run")
        return {
            "tables": {},
            "error": f"Pipeline hozir ishlayapti ({step}). Tugaguncha kuting.",
        }
    _, stored = _load_stored(name, store)
    dest_key = stored.config.destination.connector
    limit = max(1, min(int(limit), 100))
    dataset = stored.config.destination.dataset_name or "raw"

    if dest_key == "s3":
        conn = (stored.config.destination.connection or "").strip()
        return {
            "tables": {},
            "export_path": conn or None,
            "files": [],
            "kind": "s3",
            "error": (
                "S3 destination — fayllar bulutda. "
                "Bucket URL: " + (conn or "(bo'sh)")
            ),
        }

    if dest_key == "filesystem":
        export_path = resolve_export_path(stored.config)
        if not export_path:
            return {
                "tables": {},
                "error": "Lokal export yo'li topilmadi.",
            }
        files = _list_export_files(export_path, dataset)
        tables: dict[str, Any] = {}
        # Sample up to 3 CSV files for inline preview
        csv_files = [f for f in files if f.get("suffix") == ".csv"][:3]
        for fmeta in csv_files:
            sample = _sample_csv_file(fmeta["path"], limit)
            # table key = parent folder (table_name) or file stem
            from pathlib import Path as _P

            parent = _P(fmeta["relative"]).parts[0] if fmeta.get("relative") else fmeta["name"]
            key = parent if parent and not parent.endswith(".csv") else fmeta["name"]
            if key in tables:
                key = fmeta["relative"] or fmeta["name"]
            tables[key] = {
                **sample,
                "file": fmeta["relative"] or fmeta["name"],
            }
        return {
            "tables": tables,
            "files": files,
            "export_path": export_path,
            "kind": "filesystem",
            "col_counts": {k: len(v.get("columns") or []) for k, v in tables.items()},
            "error": None if (files or tables) else "Hali hech qanday fayl yozilmagan — avval Run qiling.",
        }

    pipeline = build_dlt_pipeline(stored.config)

    try:
        col_counts = _user_col_counts(pipeline)
        user_tables = [t for t in col_counts if not t.startswith("_dlt")]
        if not user_tables:
            return {"tables": {}, "col_counts": {}, "error": "Hali hech qanday jadval yuklanmagan"}

        result: dict[str, Any] = {}
        with pipeline.sql_client() as client:
            for table_name in user_tables[:3]:
                try:
                    fqn = _preview_fqn(dest_key, dataset, table_name)
                    # LIMIT is an int we control (not user SQL) — portable across destinations
                    with client.execute_query(
                        f"SELECT * FROM {fqn} LIMIT {int(limit)}"
                    ) as cursor:
                        columns = [d[0] for d in (cursor.description or [])]
                        rows = [list(r) for r in cursor.fetchall()]
                    result[table_name] = {"columns": columns, "rows": rows}
                except Exception as e:
                    result[table_name] = {"error": str(e), "columns": [], "rows": []}
        return {"tables": result, "col_counts": col_counts}
    except Exception as e:
        return {"tables": {}, "col_counts": {}, "error": str(e)}


def drop_resource(name: str, resource: str, store: ControlStore | None = None) -> dict[str, str]:
    """Selectively reset one resource (table + state).

    NOTE: ``pipeline.drop()`` in dlt deletes the *entire* local pipeline
    working dir — it does NOT take a resource name. Selective drop is done
    via the supported CLI: ``dlt pipeline <name> drop <resource>``.
    """
    import subprocess
    import sys

    _, stored = _load_stored(name, store)
    res = (resource or "").strip()
    if not res:
        return {"status": "error", "detail": "Resource nomi bo'sh"}
    blocked = _recovery_blocked_if_running(name)
    if blocked:
        return blocked
    pipeline = build_dlt_pipeline(stored.config)
    try:
        cmd = [
            sys.executable,
            "-m",
            "dlt",
            "pipeline",
            name,
            "drop",
            res,
            "--pipelines-dir",
            str(pipeline.pipelines_dir),
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "").strip()[:400]
            msg = err or "noma'lum"
            log.warning("recover_drop_resource_failed", pipeline=name, resource=res)
            return {"status": "error", "detail": sanitize_error(f"Drop xatosi: {msg}")}
        log.info("recover_drop_resource", pipeline=name, resource=res)
        return {"status": "ok", "detail": f"Resource o'chirildi: {res}"}
    except subprocess.TimeoutExpired:
        return {"status": "error", "detail": "Drop timeout (60s)"}
    except Exception as e:
        log.exception("recover_drop_resource_failed", pipeline=name, resource=res)
        return {"status": "error", "detail": sanitize_error(f"Drop xatosi: {e}")}

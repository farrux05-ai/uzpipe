# Status

**Verified:** 2026-09-26

## Latest (recovery hardening)

- **`drop-resource` was completely broken** — the dlt CLI got `--pipelines-dir`
  after the subcommand (rejected) and no `-y` (could hang). Fixed to
  `dlt -y pipeline --pipelines-dir <dir> <name> drop <resource>`.
- Recovery (`sync` / `drop-pending` / `drop-resource`) is **blocked while the pipeline
  is running** — wiping pending dirs or dropping a table mid-load corrupts dlt state.
- Recovery error messages are **redacted** (`sanitize_error`) — no connection-string
  passwords in dashboard toasts.
- Dashboard recovery buttons disable during the action; failures surface as errors
  (an error payload was previously toasted as success).

## Prior (run error handling)

- **Failures are persisted** for manual (sync), async and demo runs — previously only
  scheduler runs recorded failures, so a dashboard-triggered failure left no trace.
- **Error reason captured:** `RunResult.error` / `RunResponse.error` carry the dlt
  failed-jobs reason; the dashboard run history renders it (red text under the badge).
- **Credential redaction:** `core/errors.py` (`sanitize_error`) strips passwords/tokens
  from connection strings, `Authorization`/`Bearer` and `key=value` secrets before
  anything is stored, returned or shown. `friendly_error` maps to plain Uzbek.
- `409 already running` is intentionally **not** recorded as a failure.

## Prior (bug fixes + docs)

- **Fixed:** Click connector auth raised `NameError` (`time` not imported) — connector was unusable.
- **Fixed:** filesystem export silently did nothing (`Path` undefined in `_execute`, swallowed by `except Exception`).
- **Fixed:** wheel build failed on duplicate `chumoli/static/index.html` (redundant `force-include`).
- Version is now single-sourced from `src/chumoli/__init__.py` (`pyproject.toml` + FastAPI app read it dynamically).
- Docs refreshed: architecture/strategy docs corrected; `docs/original-positioning/` marked historical.

## Prior (0.1.0 packaging)

- UZ connectors marked **beta** (`maturity` on manifest + UI badge).
- README: `pip install chumoli` primary path; PyPI classifiers.
- Filesystem: `_dlt_*` stays in `~/.chumoli/fs_staging/`; publish clean data only.

## Prior (filesystem / S3)

- Catalog: **`filesystem`** (local only) vs **`s3`** (object storage) — both use `dlt.destinations.filesystem`.
- Local exports: **`~/chumoli-data/exports/<pipeline>/`** (user-visible). dlt state remains under **`~/.chumoli/pipelines/`**.
- Preview for local filesystem: `export_path`, file list (skips `_dlt*`), CSV sample + copy/open in UI.
- Canonical UI is `src/chumoli/static/index.html` (no duplicate repo-root `static/`).
- Docs aligned: dashboard is static HTML (not marimo / not dltHub).

## Prior (dest connection isolation)

- **UI:** Destination connection cached **per destination key** (`_destConnByKey`).
- **API:** Reject create when destination extra is not installed.
- **Runner:** Scheme mismatch guard between destination types.

## Prior (still valid)

- Async run: `POST /api/pipelines/{name}/run/async` + `GET /api/runs/jobs/{id}`
- API key constant-time compare
- Telegram token via encrypted settings

## Tests

```bash
PYTHONPATH=src python -m pytest tests/ -q
```

# Changelog

## Unreleased

### Added
- Failed runs are now persisted to run history for manual (sync), async and demo
  runs. Previously only scheduler runs recorded failures, so a dashboard-triggered
  failure left no trace beyond a transient toast.
- `RunResult.error` / `RunResponse.error`: the dlt failed-jobs reason is captured
  and shown in the dashboard run history.
- `core/errors.py`: credential redaction (`sanitize_error`) and friendly mapping
  (`friendly_error`) so stored/displayed errors never leak passwords or tokens.

### Fixed
- Dashboard run stats: load-OK but quality-fail runs no longer counted as
  "Muvaffaqiyatli"; new `quality_warn` field so "Quality ogohlantirish" card is accurate
- Run history now shows the failure reason (redacted) instead of only a red badge
- Recovery (`sync` / `drop-pending` / `drop-resource`) now refuses to run while the
  pipeline is loading — wiping pending dirs or dropping a table mid-load corrupts
  dlt state
- Recovery error messages are redacted (no connection-string passwords in toasts)
- Dashboard recovery buttons disable during the action and surface failures as
  errors (an error payload was previously toasted as success)

## 0.1.0 — 2026-09-23

### Added
- PyPI packaging polish: classifiers, `pip install chumoli`, wheel static include
- Connector `maturity` (`stable` | `beta`) — UI badge
- Local filesystem: hidden dlt staging; user folder gets clean tables only
- Native folder picker for local filesystem destination
- CI: pytest 3.11/3.12 + Docker smoke

### Fixed
- Click connector auth raised `NameError` (`time` not imported) — connector was unusable
- Filesystem export silently failed: `Path` was undefined in `_execute` and the
  error was swallowed by `except Exception`
- Wheel build failed with duplicate `chumoli/static/index.html` (redundant
  `force-include`); static is now packaged via `packages = ["src/chumoli"]`
- Single source of truth for the version (`src/chumoli/__init__.py`);
  `pyproject.toml` and the FastAPI app read it dynamically

### Removed
- Unused imports across `src` and `tests`
- Stale `tests/static/` dashboard copies (canonical UI is `src/chumoli/static/index.html`)

### Notes
- UZ connectors (Click, Payme, Uzum Market, Didox) are **beta** — test with your own API keys
- Stable: SQL, REST, synthetic volume, DuckDB / filesystem / S3 destinations

# Phase 3–4 integration fix notes

This package remains **paper-only**. `ENABLE_STRATEGY_TRADING` defaults to `false`; no live broker adapter or exchange order API is present.

## What changed

- Strategy signals are now kept in `/api/state` and shown in the dashboard, including WAIT reasons and entry/stop/target levels.
- Entry fills from strategy orders are journaled with broker order ID, fee/slippage estimates, stop and target. Broker position-to-flat transitions reconcile a close record from observed paper fills and realized-P&L changes.
- Database trade creation checks broker order IDs to reduce duplicate journal rows after retries.
- The orchestrator now calls a public `PaperBroker.set_position_protection()` method instead of mutating `_lock` / `_positions` itself.
- Strategy order submission fails closed unless the feed is healthy, `FEED_EXCHANGE=deribit`, and `WALL_STRIKES` is configured.
- Default deployment feed changed to Deribit so option observations can be produced. Strategy order submission remains disabled by default.
- Health now runs `SELECT 1` against the configured database and returns HTTP 503 if the database is unavailable; an initialized engine alone is no longer treated as proof of connectivity.
- Deribit option polling now emits one complete snapshot per poll. The server replaces the previous snapshot instead of appending repeated observations, preventing repeated open-interest/gamma counting and avoiding per-contract repeated calculations.
- Gamma is marked with an update timestamp and ignored by strategy evaluation when stale; empty/invalid option snapshots clear the gamma state rather than leaving an old regime active.
- The internal paper-account symbol is aligned to `BTCUSDT` (the dashboard and API default); the public Deribit market-data instrument remains `BTC-PERPETUAL`.

## Environment configuration

Required for gamma/wall-driven signals:

- `FEED_EXCHANGE=deribit`
- `WALL_STRIKES` (optional comma-separated levels; set only after reviewing actual BTC price levels near current spot)
- `ENABLE_STRATEGY_TRADING=false` until the feed and wall classifications have been observed and validated

`WALL_STRIKES` are static configured levels. They can become stale as BTC moves and should be reviewed/updated. If omitted, the system intentionally has no wall tracker and cannot submit strategy orders. A healthy websocket alone does not validate the quality of gamma or wall observations.

For deployment, install the pinned dependencies from `requirements.txt`; SQLite uses `aiosqlite`, and PostgreSQL URLs require `asyncpg`.

## Checks performed in this environment

- Python compilation/import checks passed for Phase 3–4 modules and the updated broker.
- Dashboard inline JavaScript syntax check passed.
- `render.yaml` parsed successfully.
- Paper-broker fill plus public protective-level API smoke test passed.
- Unit tests in `test_integration_fixes.py` do not require a database driver or network.

## Not verified here

The environment did not have `aiosqlite` or `asyncpg`, and network/package installation was unavailable. Therefore SQL database integration, PostgreSQL operation, persistence across real process restart, live Deribit connectivity, and full dashboard-to-feed end-to-end behavior were **not** tested. The smoke tests are not evidence of profitability. Keep `ENABLE_STRATEGY_TRADING=false`. This package is paper-only and no live exchange order path is provided; do not enable strategy order submission just to work around missing gamma/wall inputs.

After installing dependencies, also run `python -m unittest -v test_database_integration.py`. This exercises SQLite trade idempotency, close/performance calculations, and broker snapshot recovery. It was included for the user to run locally but could not be executed in this environment because `aiosqlite` is missing.

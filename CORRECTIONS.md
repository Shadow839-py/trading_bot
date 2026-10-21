# Corrections in this build

## Corrected

1. **Repeated Deribit option rows were inflating modeled gamma.** The feed now emits one complete option snapshot per poll. The server indexes it by Deribit instrument name and replaces the prior snapshot, so repeated polls do not accumulate the same contract. Gamma is calculated once per poll instead of once for every contract in the poll.
2. **Option data could become stale while still appearing current.** Each gamma calculation records its update time. Strategy evaluation refuses to use gamma older than three option-poll intervals (with a 90-second minimum). The state API/WebSocket snapshot hides stale gamma, and the dashboard clears old gamma lines when data is missing/stale.
3. **Database health could report “connected” without testing the database.** `/api/health` now runs `SELECT 1`; database connection failures return HTTP 503.
4. **The configured paper symbol did not match the app/API defaults.** Render's `TRADING_SYMBOL` is now `BTCUSDT`. The data feed still explicitly subscribes to Deribit's `BTC-PERPETUAL`; the internal paper-account symbol is only a consistent journal/accounting label.
5. **Option snapshots could bloat the event journal.** The journal stores poll metadata and observation count rather than the full multi-contract snapshot payload every 30 seconds.
6. **Added regression tests** for option-snapshot deduplication and repeated instrument rows.

## Intentionally not auto-configured

- `WALL_STRIKES` remains unset rather than inventing liquidity levels. Without reviewed levels and observed wall classifications, wall confluence is unavailable. This is expected to keep the strategy from taking trades.
- `ENABLE_STRATEGY_TRADING=false` remains the default. This project is paper-only and has no live exchange-order adapter.

## Verification performed

- `python -m py_compile *.py`: passed.
- `python -m unittest -v test_option_snapshot.py test_integration_fixes.py`: 7 tests passed.
- `render.yaml` parsed successfully.
- Dashboard inline JavaScript passed `node --check`.

## Not yet verified

The runtime environment used for this patch lacks the database drivers (`aiosqlite`/`asyncpg`), so real SQLite/PostgreSQL persistence, database health checks against a live DB, restart recovery, and live Deribit WebSocket behavior were not end-to-end tested here. After installing `requirements.txt`, run `python -m unittest -v test_database_integration.py` and confirm `/api/health` plus the live Render logs. Passing these checks does not establish profitability.

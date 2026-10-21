# BTC Paper-Trading Dashboard

Paper-only BTC market-data dashboard and strategy prototype. The included broker is simulated; no live exchange-order adapter is included. Automated strategy order submission is disabled by default.

## Repository layout

Place these files directly in the GitHub repository root (do not upload the parent folder as an extra nested directory):

- `main.py` — application entry point
- `server.py`, `dashboard.html` — FastAPI server and web UI
- `analytics.py`, `feed.py` — market-data and analytics
- `strategy.py` — confluence strategy
- `broker.py` — simulated paper broker
- `database.py` — journal and state persistence
- `requirements.txt` — Python dependencies
- `render.yaml` — Render Blueprint configuration
- `test_integration_fixes.py`, `test_database_integration.py`, `test_option_snapshot.py` — tests
- `CORRECTIONS.md` — correction list and verification status
- `INTEGRATION_NOTES.md` — limitations and configuration notes

## Run locally

Use Python 3.12, create/activate a virtual environment, then:

```bash
python -m pip install -r requirements.txt
python -m unittest -v test_integration_fixes test_option_snapshot test_database_integration
python main.py
```

Open `http://127.0.0.1:8000` unless `PORT` is set differently.

## Deploy to Render

1. Push the contents of this directory to the root of a GitHub repository.
2. In Render, choose **New + → Blueprint** and connect that repository.
3. Render reads `render.yaml`; review the service and persistent-disk settings before applying.
4. Keep `ENABLE_STRATEGY_TRADING=false`. Set `WALL_STRIKES` only after checking current BTC levels and confirming the wall tracker works as intended.
5. Deploy and inspect `/api/health`, the feed status, gamma/wall observations, and the dashboard before doing anything else.

Do not commit `.env` files, API secrets, credentials, or local database files. No exchange API credentials are required for this paper-only configuration.

## Important limitations

Database persistence/recovery and live Deribit connectivity were not fully exercised in the packaging environment. Run the tests in an environment with dependencies installed and verify the deployment logs before relying on it. A passing test does not establish strategy profitability. Keep automated strategy order submission disabled until market-data inputs and journal/recovery behavior have been independently validated.

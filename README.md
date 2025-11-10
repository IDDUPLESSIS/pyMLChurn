pyMLChurn - SQL + ML churn predictions
=====================================

Python project that connects to SQL Server and pulls data from `[SAP].[dbo].[CustomerChurnCadence_v1]`, then trains a quick model and exports churn predictions with human-readable reasons. Supports Windows Integrated Auth or SQL login.

Quick Start
- Clone: `git clone https://github.com/IDDUPLESSIS/pyMLChurn && cd pyMLChurn`
- Install deps: `python -m venv .venv && .\.venv\Scripts\Activate.ps1 && pip install -r requirements.txt`
- Configure env: copy `.env.example` to `.env` and set `MSSQL_SERVER`, `MSSQL_DATABASE`, and auth
- Run once and keep CSVs:
  - `python pyMLChurn.py --output predictions.csv --raw-output raw.csv --keep-csv`
- Load into SQL (same run):
  - `python pyMLChurn.py --output predictions.csv --load-sql --load-if-exists append --keep-csv`
- Build EXE (optional): `.\build_exe.ps1`, then double‑click `dist\pyMLChurn\pyMLChurn.exe` (auto loads to SQL)

Requirements
- Python 3.9+
- Microsoft ODBC Driver for SQL Server (18 or 17)
- Python packages in `requirements.txt`

Setup
1) Install the ODBC driver: "ODBC Driver 18 for SQL Server" or "ODBC Driver 17 for SQL Server".
2) (Recommended) Create a virtual environment and activate it.
3) Install dependencies: `pip install -r requirements.txt`
4) Copy `.env.example` to `.env` and set at least:
   - `MSSQL_SERVER=your_sql_server,1533`
   - `MSSQL_DATABASE=SAP`
   - `MSSQL_AUTH=windows` (or `sql` plus `MSSQL_USERNAME`/`MSSQL_PASSWORD`)

Run
- Script (friendly headers by default):
  - `python pyMLChurn.py --output predictions.csv --raw-output raw.csv`
  - Limit rows for testing: `--top 100`
- Module form:
  - `python -m pymlchurn.cli --output predictions.csv`
- One-click runners:
  - `run_pyMLChurn.ps1` or `run_pyMLChurn.bat` (auto-creates venv, installs deps, runs)

Double‑click EXE
- The EXE auto‑loads predictions to SQL when run with no flags and searches for `.env` next to the EXE or in the repo root.
- To keep CSVs when running from a terminal: add `--keep-csv`.

Connection and refresh
- Pre-query stored procedure: runs `[dbo].[sp_build_customer_churn_cadence_v1]` once per 24h
  - Control with `--sp-ttl-hours 24`, `--force-sp`, `--skip-sp`
- Connectivity check: `--check` or `--check-only`

Query behavior
- Pulls only model features + label from `[SAP].[dbo].[CustomerChurnCadence_v1]`
- Snapshot handling:
  - Default keeps the latest snapshot per customer (by `as_of_date`)
  - Keep all snapshots: `--keep-all-rows`
  - Filter to a date: `--as-of YYYY-MM-DD`

Model + explanations
- LogisticRegression with imputation and scaling (class_weight balanced)
- SHAP explanations for per-row reasons (falls back to coefficients if needed)
- Default target (label) is `churned_hard90` (actual churn within 90 days)

Output columns
- Friendly headers (default): `--headers friendly`
  - Customer ID
  - Snapshot Date
  - Churned Within 90 Days (Actual)
  - Why They Churned (Actual)
  - Predicted to Churn (Next 90 Days)
  - Churn Probability % (Next 90 Days)
  - Churn Probability (Next 90 Days)
  - Why At Risk (Predicted)
- Technical headers: `--headers technical`
  - `customer_id`, `as_of_date_t0`, `actual_churned_90d_t0+90d`, `actual_churn_reason_t0`,
    `predicted_churn_90d_t0+90d`, `predicted_churn_probability_90d_t0+90d`,
    `predicted_churn_probability_90d_pct_t0+90d`, `predicted_churn_reason_t0`

Common flags
- `--raw-output raw.csv` save the raw query rows used by the model
- `--headers friendly|technical` choose column names
- `--target-col churned_hard90|churned_dynamic` change label
- `--as-of 2025-01-31` restrict to a date; `--keep-all-rows` keep all snapshots
- `--auth windows|sql`, `--username`, `--password`, `--driver`, `--no-encrypt`, `--no-trust-cert`

Notes
- ODBC 18 encrypts by default; for internal certs, `MSSQL_TRUST_CERT=yes` (default). For PKI, set `no`.
- Server can include port `host,port` (e.g., `server.local,1533`).
- Retries with exponential backoff are built-in for SP and queries.

Feature glossary (units)
- recency_days: days since last purchase (days)
- median_gap_days, p90_gap_days: typical/long gaps between purchases (days)
- cv_gap: irregularity of purchase cadence (unitless)
- in_renewal_grace: in renewal grace period (true/false)
- rev_180d: revenue in last 180 days (currency)
- rev_returns_90d: value of returns in last 90 days (currency)
- invoices_90d: invoices in last 90 days (count)
- credit_notes_90d: credit notes in last 90 days (count)
- orders_pos_30d: positive order value in last 30 days (currency)
- orders_neg_30d: negative order value in last 30 days (currency)
- backorder_qty_30d: backorder quantity in last 30 days (count/units)
- pct_change_3m, pct_change_6m, yoy_change_pct: sales change vs prior periods (percent)
- credit_notes_prev_month: credit notes last month (count)
- invoices_pos_prev_month: invoices last month (count)
- credit_notes_ma3: credit notes per month (3-month average) (count per month)
- threshold_days: days past expected purchase threshold (days)
- is_maintenance_heavy: maintenance-heavy profile (true/false)
- maint_cycle_days: maintenance cycle length (days)
- severity_score: issue severity score (unitless)
- lateness_component, credits_component, trend_component, mitigator_component: model signals (unitless)

Contributing
- See `CONTRIBUTING.md` for a short guide to setting up a dev environment and proposing changes.

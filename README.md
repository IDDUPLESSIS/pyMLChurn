pyMLChurn - SQL + ML churn predictions
=====================================

Python project that connects to SQL Server and pulls data from `[SAP].[chrn01].[v_train_dataset]`, then trains a quick model and exports churn predictions with human-readable reasons. Supports Windows Integrated Auth or SQL login.

Quick Start
- Clone: `git clone https://github.com/IDDUPLESSIS/pyMLChurn && cd pyMLChurn`
- Install deps: `python -m venv .venv && .\.venv\Scripts\Activate.ps1 && pip install -r requirements.txt`
- Configure env: copy `.env.example` to `.env` and set `MSSQL_SERVER`, `MSSQL_DATABASE`, and auth
- Run (loads to SQL):
    - `python pyMLChurn.py`
    - Limit rows for testing: `--top 100`
- Load mode:
    - Default is replace; use `--load-if-exists append` to append instead
- Build EXE (optional): `.\scripts\build_exe.ps1`, then doubleâ€‘click `dist\pyMLChurn\pyMLChurn.exe` (auto loads to SQL)

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
  - Script (friendly headers by default, loads to SQL):
    - `python pyMLChurn.py`
    - Limit rows for testing: `--top 100`
  - One-click runners:
    - `scripts\run_pyMLChurn.ps1` or `scripts\run_pyMLChurn.bat` (auto-creates venv, installs deps, runs)

Doubleâ€‘click EXE
- The EXE autoâ€‘loads predictions to SQL when run with no flags and searches for `.env` next to the EXE or in the repo root
- Default SQL load mode is replace (overwrites `[chrn01].[CustomerChurnPredictions]` on each run)
- Logs are written during the run and removed on exit
- Debug runner: `scripts\run_exe_debug.bat` captures console output to `dist\pyMLChurn\pyMLChurn_win_stdout_stderr.txt`

Connection
- Connectivity check: `--check` or `--check-only`

Query behavior
- Pulls engineered feature set + labels from `[SAP].[chrn01].[v_train_dataset]`
- Snapshot handling:
  - Default keeps the latest snapshot per customer (by `as_of_date`)
  - Keep all snapshots: `--keep-all-rows`
  - Filter to a date: `--as-of YYYY-MM-DD`

Model + explanations
- LogisticRegression with imputation and scaling (class_weight balanced)
- When database labels are missing or all zero, builds a pseudo-label from churn business rules (e.g. no orders in 90 days, no maint orders within grace, EoX not renewed, unpaid 90+ AR) and trains on that
- Applies temperature scaling so probabilities are realistic (not just 0/1)
- SHAP explanations for per-row reasons (falls back to coefficients if needed)
- Default target (label) is `Label_Churn_90d` when available

Output columns
- Friendly headers (default): `--headers friendly`
  - Customer ID
  - Snapshot Date
  - Recency Orders Days (Snapshot)
  - Churned Now (Business Rule)
  - Why (Business Rule)
  - Churned Within 90 Days (Actual)
  - Why They Churned (Actual)
  - Predicted to Churn (Next 90 Days)
  - Churn Probability % (Next 90 Days)
  - Churn Probability (Next 90 Days)
  - Why At Risk (Predicted)
  - Predicted Churn Month (Next 90 Days)
  - CreatedOn (timestamp)
- Technical headers: `--headers technical`
  - `CustomerId`, `as_of_date_t0`, `recency_orders_days_t0`, `business_churn_now`, `business_churn_reason`,
    `actual_churned_90d_t0+90d`, `actual_churn_reason_t0`, `predicted_churn_90d_t0+90d`,
    `predicted_churn_probability_90d_t0+90d`, `predicted_churn_probability_90d_pct_t0+90d`,
    `predicted_churn_reason_t0`, `predicted_churn_month_t0+90d`, `CreatedOn`

Common flags
  - `--top 100` limit rows for testing
  - `--headers friendly|technical` choose column names
  - `--target-col Label_Churn_90d|Label_Churn_180d|Label_HasAnyChurn` change label (when available)
  - `--as-of 2025-01-31` restrict to a date; `--keep-all-rows` keep all snapshots
  - `--load-schema chrn01`, `--load-table CustomerChurnPredictions`, `--load-if-exists append|replace|fail`
  - `--check` / `--check-only` connectivity check
  - `--auth windows|sql`, `--username`, `--password`, `--driver`, `--no-encrypt`, `--no-trust-cert`

Notes
- ODBC 18 encrypts by default; for internal certs, `MSSQL_TRUST_CERT=yes` (default). For PKI, set `no`.
- Server can include port `host,port` (e.g., `server.local,1533`).
- Retries with exponential backoff are built-in for SP and queries.

Feature glossary (high level)
- Recency + volume:
  - `Recency_Orders_Days`, `Recency_MaintOrders_Days`, `Recency_Invoices_Days`, `OrderValue_90d`, `OrderFreq_90d`, `MaintOrderValue_90d`, `ProdOrderValue_90d`
- Trend / change vs prior 90 days:
  - `OrderValue_Prev90d`, `OrderValue_3m_Change`, `OrderValue_3m_ChangePct`,
    `OrderFreq_Prev90d`, `OrderFreq_3m_Change`, `OrderFreq_3m_ChangePct`
- Mix / profile:
  - `ProdRatio_90d`, `MaintRatio_90d`, `IsProductHeavy_90d`, `IsMaintHeavy_90d`
- AR / credits / DSO:
  - `BackorderCount_180dPlus`, `UnpaidInv_OverTerms_Count`, `UnpaidInv_90plus_Count`,
    `ARBucket_*_Count`, `DSO_OpenInvoices`, `CreditValue_90d`, `CreditCount_90d`
- EoX risk:
  - `Eox_MinDaysToLDOS`, `Risk_EoX_*`, `PredChurn_EoXExpired`, `Eox_Rev_*`, `Eox_Rev*Pct_*`,
    `Eox_HasExpiredRevenue_12m`, `Eox_HasRiskRevenue_12m`, `Eox_MajorityExpired_12m`,
    `Eox_SKU_Count*`, `EoxExpired_*`, `EoxRisk_*`
- Rule flags:
  - `ChurnIf_NoOrd90`, `ChurnIf_NoInv90`, `ChurnIf_NoMaintOrd90_WithinGrace`,
    `PredChurn_Unpaid90plus`, `PredChurn_HighBackorders`, `KnownChurn_Effective`, `UpcomingChurn_90d`

Contributing
- See `docs\CONTRIBUTING.md` for a short guide to setting up a dev environment and proposing changes.

Troubleshooting
- .env not found
  - Python: place `.env` in the repo root
  - EXE (doubleâ€‘click): place `.env` either next to the EXE (`dist\pyMLChurn\.env`) or in the repo root â€” the EXE searches both and will chdir into the folder containing `.env`
  - Minimum keys: `MSSQL_SERVER`, `MSSQL_DATABASE`, `MSSQL_AUTH` (windows or sql). For SQL auth, also set `MSSQL_USERNAME` and `MSSQL_PASSWORD`
  - Certificates: keep `MSSQL_ENCRYPT=yes`. If you have an internal/selfâ€‘signed cert, set `MSSQL_TRUST_CERT=yes`
- ODBC driver missing
  - Install Microsoft ODBC Driver 18 or 17 for SQL Server. If both exist, you can pin with `MSSQL_ODBC_DRIVER="ODBC Driver 17 for SQL Server"`
- No console when doubleâ€‘clicking the EXE
  - Logs are written during the run and removed on exit
  - Use `scripts\run_exe_debug.bat` to capture all console output into `dist\pyMLChurn\pyMLChurn_win_stdout_stderr.txt`
- Table not updated / wrong DB
  - Default table: `[chrn01].[CustomerChurnPredictions]` in the database from your `.env`
  - Default load mode is replace (overwrites). To append, pass `--load-if-exists append`
  - Verify with:
    - `SELECT COUNT(*) FROM [chrn01].[CustomerChurnPredictions];`
    - `SELECT MAX(CreatedOn) FROM [chrn01].[CustomerChurnPredictions];`
- Connection timeouts (08001)
  - Verify server and port in `.env` (`MSSQL_SERVER=host,port`)
  - Ensure the port is reachable and remote connections are allowed
  - If you have proper CAâ€‘signed certs, consider `MSSQL_TRUST_CERT=no`
- Packaging errors with the EXE (SciPy/sklearn)
  - The EXE bundles `importlib.resources`, sklearn, scipy submodules, and SciPy data
  - If you still hit import errors, rebuild via `scripts\build_exe.ps1` (it uses the correct flags)

Project Structure
- `pyMLChurn.py` main script and CLI (friendly headers, logging, SQL load)
- `pymlchurn/`
  - `config.py` reads `.env` and builds connection config (with EXEâ€‘friendly search)
  - `db.py` ODBC/SQLAlchemy engine, retries, SP execution, query helpers
  - `query.py` selected columns + date handling for the churn dataset
  - `ml.py` model pipeline (LogisticRegression) + SHAPâ€‘based explanations
    - `load_sql.py` table creation and DataFrame loader (SQLAlchemy to_sql)
- `scripts\run_pyMLChurn.ps1` / `scripts\run_pyMLChurn.bat` oneâ€‘click runners for Python
- `scripts\build_exe.ps1` builds EXE (bundles sklearn/scipy + importlib.resources)
- `scripts\run_exe_debug.bat` runs EXE and captures console output to a text file
- `.vscode/` debug/run tasks for VS Code
- `requirements.txt`, `README.md`, `docs\CONTRIBUTING.md`, `.env.example`, `.gitignore`

Release Checklist
- Preflight
  - Verify `.env` is correct locally; ensure `.env` is not committed
  - Confirm ODBC Driver 17/18 installed and accessible
  - Smoke test Python run: `python pyMLChurn.py --check-only`
  - Full test with SQL load (replace):
    - `python pyMLChurn.py`
    - Check `[chrn01].[CustomerChurnPredictions]` row count and `MAX(CreatedOn)`
- Build EXE
  - `./scripts/build_exe.ps1` (produces `dist/pyMLChurn/pyMLChurn.exe`)
  - Double-click EXE; verify SQL load
  - Optional: `scripts\run_exe_debug.bat` to capture console to text
- Repo hygiene
  - Ensure `dist/`, `build/`, `*.spec`, `*.csv`, `.venv_build/`, `.state/` are gitâ€‘ignored
  - Update `README.md` and `docs\CONTRIBUTING.md` if behavior changed
- Publish
  - Commit and push to `main`
  - Create a GitHub release with short notes and (optional) EXE attached
  - Share the repo link (README contains Quick Start + Troubleshooting)


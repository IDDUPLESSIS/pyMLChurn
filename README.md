pyMLChurn - SQL + ML churn predictions
======================================

Python project that connects to SQL Server, pulls engineered churn data from `[SAP].[chrn01].[v_train_dataset]`, trains a churn model, applies a deterministic business-risk interpretation layer, and exports predictions to `[chrn01].[CustomerChurnPredictions]`.

The platform keeps ML probability separate from business-rule risk. Active maintenance contracts reduce interpreted risk severity, but they no longer hide obvious commercial inactivity.

Quick Start
-----------
- Clone: `git clone https://github.com/IDDUPLESSIS/pyMLChurn && cd pyMLChurn`
- Install deps: `python -m venv .venv && .\.venv\Scripts\Activate.ps1 && pip install -r requirements.txt`
- Configure env: copy `.env.example` to `.env` and set `MSSQL_SERVER`, `MSSQL_DATABASE`, and auth
- Run and load predictions to SQL: `python pyMLChurn.py`
- Limit rows for testing: `python pyMLChurn.py --top 100`
- Build EXE: `.\scripts\build_exe.ps1`, then run `dist\pyMLChurn\pyMLChurn.exe`

Requirements
------------
- Python 3.9+
- Microsoft ODBC Driver for SQL Server 18 or 17
- Python packages in `requirements.txt`

Setup
-----
1. Install the ODBC driver.
2. Create and activate a virtual environment.
3. Install dependencies: `pip install -r requirements.txt`
4. Copy `.env.example` to `.env` and set:
   - `MSSQL_SERVER=your_sql_server,1533`
   - `MSSQL_DATABASE=SAP`
   - `MSSQL_AUTH=windows` or `sql`
   - `MSSQL_USERNAME` and `MSSQL_PASSWORD` only when using SQL auth

Run
---
- Standard run: `python pyMLChurn.py`
- Connectivity check only: `python pyMLChurn.py --check-only`
- Score a specific snapshot: `python pyMLChurn.py --score-as-of 2026-05-31`
- Change target label: `--target-col Label_Churn_90d|Label_Churn_180d|Label_HasAnyChurn`
- SQL load options: `--load-schema chrn01 --load-table CustomerChurnPredictions --load-if-exists append|replace|fail`

Default load mode is `replace`, so each normal run overwrites `[chrn01].[CustomerChurnPredictions]` with the latest scored output.

Query Behavior
--------------
- Training data comes from `[SAP].[chrn01].[v_train_dataset]`.
- Scoring data uses the current/latest snapshot unless `--score-as-of` is supplied.
- The scoring query includes `LastOrderDate` so recency can be validated against `SnapshotDate` and `Recency_Orders_Days`.

Model And Business Risk
-----------------------
- `pymlchurn/ml.py` trains the ML model and produces the ML prediction score.
- `pymlchurn/risk.py` is deterministic business interpretation, not a second ML model.
- These concepts are kept separate:
  - ML prediction score / probability
  - raw commercial inactivity risk
  - maintenance protection modifier
  - adjusted business risk
  - final business status
- Business statuses include `Healthy`, `Protected`, `Protected But At Risk`, `Watchlist Risk`, `Churn Risk`, `Churned`, and `Data Quality Review`.
- Maintenance contracts are visible as protection, but protection decays as commercial inactivity grows.
- Recency validation flags mismatches for investigation instead of silently accepting inconsistent dates.

Output Columns
--------------
The output table preserves legacy columns and adds clearer business-risk fields.

Legacy compatibility columns include:
- `ChurnedNowBusinessRule`
- `WhyBusinessRule`
- `PredictedToChurnNext90Days`
- `ChurnRiskScoreNext90Days`
- `WhyAtRiskPredicted`

New business-risk columns include:
- `LastOrderDate`
- `RawCommercialInactivityRisk`
- `RawCommercialInactivityRiskScore`
- `ProtectedByMaintenanceContract`
- `MaintenanceProtectionScore`
- `AdjustedBusinessRiskScore`
- `WatchlistChurnRisk`
- `BusinessRiskStatus`
- `BusinessRiskExplanation`
- `MaintenanceProtectionLevel`
- `RawBusinessRiskReasons`
- `ProtectionModifierReasons`
- `RecencyValidationWarning`
- `RecencyValidationStatus`
- `MlPredictionScoreNext90Days`
- `MlPredictionScorePctNext90Days`
- `PredictionValueType`

Reporting View
--------------
`[chrn01].[v_CustomerChurnPredictions]` provides report-ready output and HTML helper columns.

- `RiskPillHtml` uses `BusinessRiskStatus`.
- `ProbBarHtml`, `ProbPct`, and `RiskSort` use `AdjustedBusinessRiskScore`.
- `DriversHtml` uses business-risk explanation, raw-risk reasons, protection reasons, and recency warnings.
- ML score columns remain available separately as `MlPredictionScoreNext90Days` and `MlPredictionScorePctNext90Days`.

SQL Schema Scripts
------------------
Exported SQL scripts for `[chrn01]` live under `sql/chrn01/`.

- `sql/chrn01/tables/`
- `sql/chrn01/views/`
- `sql/chrn01/procedures/`

These scripts are intended to keep the repository aligned with deployed SQL tables, views, and stored procedures.

Daily Automation
----------------
The SQL Agent job `chrn01_DailyChurnJob`, when enabled on SQL Server, runs:

```sql
EXEC chrn01.sp_RunDailyChurnJob;
```

That stored procedure refreshes customer snapshots and labels. It does not run the Python model or reload `[chrn01].[CustomerChurnPredictions]`.

To fully refresh reporting after new SAP rows arrive, run this after the SQL snapshot job:

```powershell
python pyMLChurn.py
```

or schedule the packaged EXE as a second job step.

Troubleshooting
---------------
- `.env` not found:
  - Place `.env` in the repo root.
  - For the EXE, place `.env` next to the EXE or in the repo root.
- ODBC driver missing:
  - Install Microsoft ODBC Driver 18 or 17 for SQL Server.
- Table not updated:
  - Verify `[chrn01].[CustomerChurnPredictions]` row count and `MAX(CreatedOn)`.
  - If snapshots refreshed but predictions did not, run `python pyMLChurn.py`.
  - Reporting HTML comes from `[chrn01].[v_CustomerChurnPredictions]`, which depends on the prediction table being refreshed.
- Connection timeouts:
  - Verify server and port in `.env`.
  - Keep `MSSQL_ENCRYPT=yes`.
  - Use `MSSQL_TRUST_CERT=yes` for internal/self-signed certificates when appropriate.

Project Structure
-----------------
- `pyMLChurn.py` main CLI script, logging, training/scoring orchestration, SQL load
- `pymlchurn/config.py` environment/config loader
- `pymlchurn/db.py` SQL Server connection and query helpers
- `pymlchurn/query.py` training/scoring SQL selectors
- `pymlchurn/ml.py` ML model pipeline and explanations
- `pymlchurn/risk.py` deterministic business-risk interpretation and recency validation
- `pymlchurn/load_sql.py` SQL table creation and DataFrame loading
- `scripts/` utility runners, EXE build script, SQL schema export script
- `sql/chrn01/` exported SQL schema scripts

Release Checklist
-----------------
- Verify `.env` is correct locally and not committed.
- Run `python pyMLChurn.py --check-only`.
- Run `python pyMLChurn.py`.
- Check `[chrn01].[CustomerChurnPredictions]` row count and `MAX(CreatedOn)`.
- Check `[chrn01].[v_CustomerChurnPredictions]` for `BusinessRiskStatus`, `AdjustedBusinessRiskScore`, and `MlPredictionScoreNext90Days`.
- Run `python -m compileall .`.
- Inspect `git diff` before committing.

Contributing to pyMLChurn
=========================

Thanks for your interest in improving pyMLChurn! This guide helps you set up a local dev environment and submit changes cleanly.

Prerequisites
- Windows with Microsoft ODBC Driver for SQL Server (17 or 18)
- Python 3.9+
- Access to a SQL Server with the `[SAP].[dbo].[CustomerChurnCadence_v1]` view and (optionally) the SP `[dbo].[sp_build_customer_churn_cadence_v1]`

Setup
1) Clone and create a virtualenv
   - `git clone https://github.com/IDDUPLESSIS/pyMLChurn`
   - `cd pyMLChurn`
   - `python -m venv .venv && .\.venv\Scripts\Activate.ps1`
2) Install dependencies
   - `pip install -r requirements.txt`
3) Configure environment
   - Copy `.env.example` to `.env` and set `MSSQL_SERVER`, `MSSQL_DATABASE`, and authentication

Run locally
- Script: `python pyMLChurn.py --output predictions.csv --raw-output raw.csv --keep-csv`
- Load to SQL: add `--load-sql --load-table CustomerChurnPredictions`
- Connectivity only: `python pyMLChurn.py --check-only`

Executable (optional)
- Build: `.\build_exe.ps1`
- Run EXE: `dist\pyMLChurn\pyMLChurn.exe` (auto loads to SQL when run without flags)

Coding guidelines
- Keep changes focused and minimal
- Match existing style and naming
- Favor friendly headers for user-facing CSV; technical headers remain available
- Avoid committing generated artifacts (`*.csv`, `dist/`, `build/`, `.venv_build/`)

Submitting changes
1) Create a feature branch: `git checkout -b feature/my-change`
2) Make changes; update README if user workflow changes
3) Commit: `git add -A && git commit -m "Describe the change succinctly"`
4) Push: `git push origin feature/my-change`
5) Open a pull request; describe the motivation, what changed, and any caveats

Security & secrets
- Never commit `.env` or credentials
- The loader uses parameterized connections via ODBC; TLS is on by default (ODBC 18)

Questions
- Open an issue or start a discussion in the repo


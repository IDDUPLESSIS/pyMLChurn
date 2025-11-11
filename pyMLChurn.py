"""
Run script to fetch data from [SAP].[dbo].[CustomerChurnCadence_v1] and export to CSV.

Usage examples:
  python pyMLChurn.py                    # TOP 1000, settings from .env
  python pyMLChurn.py --top 5000         # fetch 5000 rows
  python pyMLChurn.py --output out.csv   # custom output path
  python pyMLChurn.py --auth sql --username USER --password PASS

Environment defaults are loaded from .env (see .env.example).
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
import os
import time
from datetime import datetime
from typing import Optional
import sys
from dotenv import find_dotenv

from pymlchurn.config import Config
from pymlchurn.db import query_dataframe, pick_driver, connectivity_info
from pymlchurn.sp_runner import maybe_run_sp, SPRunPolicy
from pymlchurn.query import churn_query, feature_columns, target_column, CUSTOMER_ID_COL, DATE_COL
from pymlchurn.load_sql import create_table_if_missing, load_dataframe


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch data from [SAP].[dbo].[CustomerChurnCadence_v1] and export to CSV.",
    )
    p.add_argument("--top", type=int, default=None, help="Limit rows (TOP N). Default: all rows")
    p.add_argument(
        "--output",
        type=str,
        default=None,
        help="Predictions CSV path (default: customer_churn_predictions.csv)",
    )
    p.add_argument("--raw-output", type=str, default=None, help="Optional: also save raw query CSV here")
    p.add_argument("--target-col", type=str, default=target_column(), help="Target label column (default: churned_dynamic)")
    p.add_argument("--headers", choices=["friendly", "technical"], default="friendly", help="Column header style for output (default: friendly)")
    p.add_argument("--load-sql", action="store_true", help="Create table if missing and load predictions to SQL")
    p.add_argument("--load-schema", type=str, default="dbo", help="Target schema for SQL load (default: dbo)")
    p.add_argument("--load-table", type=str, default="CustomerChurnPredictions", help="Target table name for SQL load")
    p.add_argument("--load-if-exists", choices=["append","replace","fail"], default="replace", help="Behavior if table exists (default: replace)")
    p.add_argument("--keep-csv", action="store_true", help="Keep generated CSV files (default: delete after run)")
    p.add_argument("--auth", choices=["windows", "sql"], default=None, help="Override MSSQL_AUTH")
    p.add_argument("--username", type=str, default=None, help="SQL login username (if --auth sql)")
    p.add_argument("--password", type=str, default=None, help="SQL login password (if --auth sql)")
    p.add_argument(
        "--driver",
        type=str,
        default=None,
        help="ODBC driver name (e.g. 'ODBC Driver 18 for SQL Server')",
    )
    p.add_argument("--no-encrypt", action="store_true", help="Disable TLS encryption")
    p.add_argument("--no-trust-cert", action="store_true", help="Set TrustServerCertificate=no")
    p.add_argument("--sp-name", type=str, default="sp_build_customer_churn_cadence_v1", help="Stored procedure name to execute before query")
    p.add_argument("--sp-schema", type=str, default="dbo", help="Stored procedure schema")
    p.add_argument("--sp-ttl-hours", type=int, default=24, help="Min hours between SP runs (default 24)")
    p.add_argument("--force-sp", action="store_true", help="Force running the stored procedure")
    p.add_argument("--skip-sp", action="store_true", help="Skip running the stored procedure")
    p.add_argument("--check", action="store_true", help="Run connectivity check before querying")
    p.add_argument("--check-only", action="store_true", help="Only run connectivity check and exit")
    p.add_argument("--keep-all-rows", action="store_true", help="Do not deduplicate by latest as_of_date per customer")
    p.add_argument("--as-of", type=str, default=None, help="Filter to rows with this as_of date (YYYY-MM-DD)")
    return p.parse_args(argv)


def build_config(args: argparse.Namespace) -> Config:
    cfg = Config.from_env()
    if args.auth:
        cfg.auth = args.auth
    if args.username is not None:
        cfg.username = args.username
    if args.password is not None:
        cfg.password = args.password
    if args.driver is not None:
        cfg.odbc_driver = args.driver
    if args.no_encrypt:
        cfg.encrypt = False
    if args.no_trust_cert:
        cfg.trust_server_certificate = False
    return cfg


def main(argv: Optional[list[str]] = None) -> int:
    # Set up a persistent log file next to the EXE when frozen,
    # otherwise use the current working directory
    try:
        if getattr(sys, 'frozen', False):
            log_dir = Path(sys.executable).resolve().parent
        else:
            log_dir = Path.cwd()
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        log_dir = Path.cwd()
    log_file = str(log_dir / f"pyMLChurn_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    # Tee stdout/stderr to the log file to capture all prints
    try:
        class _Tee:
            def __init__(self, primary, fh):
                self.primary = primary
                self.fh = fh
            def write(self, s):
                try:
                    if self.primary:
                        self.primary.write(s)
                except Exception:
                    pass
                try:
                    self.fh.write(s)
                except Exception:
                    pass
            def flush(self):
                try:
                    if self.primary:
                        self.primary.flush()
                except Exception:
                    pass
                try:
                    self.fh.flush()
                except Exception:
                    pass
        _fh = open(log_file, 'a', encoding='utf-8', buffering=1)
        _orig_out, _orig_err = sys.stdout, sys.stderr
        sys.stdout = _Tee(_orig_out, _fh)
        sys.stderr = _Tee(_orig_err, _fh)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Logging initialized -> {log_file}")
    except Exception:
        pass
    def ts() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def log(msg: str) -> None:
        nonlocal log_file
        line = f"[{ts()}] {msg}"
        print(line)
        # Attempt primary log; on failure, fall back to Documents
        wrote = False
        try:
            if log_file:
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(line + "\n")
                wrote = True
        except Exception:
            wrote = False
        if not wrote:
            try:
                fallback_dir = Path.home() / "Documents" / "pyMLChurn"
                fallback_dir.mkdir(parents=True, exist_ok=True)
                fb = str(fallback_dir / f"pyMLChurn_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
                with open(fb, 'a', encoding='utf-8') as f:
                    f.write(line + "\n")
                log_file = fb
            except Exception:
                pass
        # Keep a simple rolling latest log next to the chosen log directory
        try:
            latest = Path(log_file).with_name("pyMLChurn_latest.log") if log_file else None
            if latest:
                with open(latest, 'a', encoding='utf-8') as f:
                    f.write(line + "\n")
        except Exception:
            pass

    args = parse_args(argv)
    # If running as frozen EXE with no args, default to SQL load and ephemeral CSVs
    try:
        if getattr(sys, 'frozen', False) and len(sys.argv) == 1:
            # Set sensible defaults for double-click runs
            args.load_sql = True
            if args.output is None:
                args.output = "predictions_auto.csv"
            args.raw_output = None
            args.keep_csv = False
            args.load_if_exists = 'replace'
            # Ensure working directory finds .env
            env_path = find_dotenv(usecwd=True)
            if not env_path:
                exe_dir = Path(sys.executable).resolve().parent
                for candidate in [exe_dir / ".env", exe_dir.parent / ".env", exe_dir.parent.parent / ".env"]:
                    if candidate.exists():
                        env_path = str(candidate)
                        break
            if env_path:
                os.chdir(str(Path(env_path).parent))
    except Exception:
        pass
    try:
        cfg = build_config(args)
    except Exception as e:
        log(f"ERROR loading configuration: {e}")
        if getattr(sys, 'frozen', False) and len(sys.argv) == 1:
            time.sleep(8)
        return 2

    # Surface the driver choice so it is visible in this script's output
    chosen_driver = pick_driver(cfg.odbc_driver)
    log(f"Using ODBC driver: {chosen_driver}")
    log(f"Logs: {log_file}")

    if args.check or args.check_only:
        log("Running connectivity check...")
        info = connectivity_info(cfg)
        log("Connectivity OK")
        log(f"Driver: {info['driver']}")
        log(f"Server: {info['server']} | Database: {info['database']}")
        log(f"User: {info['user']}")
        log("Version: " + str(info['sqlserver_version']).split("\n")[0])
        if args.check_only:
            return 0

    # Conditionally execute stored procedure prior to retrieval
    if not args.skip_sp:
        policy = SPRunPolicy(ttl_hours=args.sp_ttl_hours)
        log(f"Checking stored procedure TTL: {args.sp_schema}.{args.sp_name} (TTL {args.sp_ttl_hours}h)")
        _t0 = time.perf_counter()
        res = maybe_run_sp(cfg, args.sp_name, args.sp_schema, force=args.force_sp, policy=policy)
        dur = time.perf_counter() - _t0
        status = "executed" if res["ran"] else "skipped"
        log(f"Stored procedure {status}: {args.sp_schema}.{args.sp_name} ({res['reason']}) in {dur:.1f}s")

    # Build minimal SELECT for ML
    sql = churn_query(args.top, include_label=True, target=args.target_col)
    log("Running main query...")
    _t0 = time.perf_counter()
    try:
        df = query_dataframe(cfg, sql)
    except Exception as e:
        log(f"ERROR running query: {e}")
        if getattr(sys, 'frozen', False) and len(sys.argv) == 1:
            time.sleep(8)
        return 3
    log(f"Query returned {len(df):,} rows in {time.perf_counter()-_t0:.1f}s")

    # Normalize and optionally filter/deduplicate by date
    import pandas as pd
    if DATE_COL in df.columns:
        # Ensure clean string values and treat empties consistently
        df[DATE_COL] = df[DATE_COL].astype(str).str.strip()
        df[DATE_COL] = df[DATE_COL].replace({"None": pd.NA, "NaT": pd.NA, "nan": pd.NA, "": pd.NA})
        if args.as_of:
            df = df[df[DATE_COL] == args.as_of]
        if not args.keep_all_rows:
            # Keep latest non-null as_of_date per customer (yyyy-mm-dd sorts lexicographically by date)
            df = df.sort_values([DATE_COL], na_position='first').drop_duplicates(subset=[CUSTOMER_ID_COL], keep='last')

    generated_files: list[str] = []

    # Optionally save raw query
    if args.raw_output:
        raw_path = Path(args.raw_output).expanduser().resolve()
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        log(f"Writing raw CSV to {raw_path}...")
        df.to_csv(raw_path, index=False)
        print(f"Saved raw rows: {len(df):,} -> {raw_path}")
        generated_files.append(str(raw_path))

    # Train and predict
    log("Training model and generating predictions...")
    _t0 = time.perf_counter()
    try:
        from pymlchurn.ml import MLConfig, train_and_predict
        ml_cfg = MLConfig(customer_id_col=CUSTOMER_ID_COL, feature_cols=feature_columns(), target_col=args.target_col, date_col=DATE_COL)
        pred_df = train_and_predict(df, ml_cfg)
    except Exception as e:
        log(f"ERROR during model/predictions: {e}")
        if getattr(sys, 'frozen', False) and len(sys.argv) == 1:
            time.sleep(8)
        return 4
    log(f"Model + predictions completed in {time.perf_counter()-_t0:.1f}s")

    # Drop legacy columns if present
    if "actual_churned_hard90" in pred_df.columns:
        pred_df = pred_df.drop(columns=["actual_churned_hard90"])

    # Compute business-rule churn as of "today":
    # Churned if no purchases in last 90 days AND not in renewal grace period.
    try:
        # Correct interpretation: DATE_COL (t0) is the last purchase date
        as_of_dt = pd.to_datetime(df[DATE_COL], errors='coerce')
        days_since_purchase_today = (pd.Timestamp.today().normalize() - as_of_dt).dt.days.clip(lower=0)
        in_grace = df.get('in_renewal_grace').astype(bool).fillna(False)
        # Business rule: base 90 days; renewal grace adds +30 days to the threshold
        threshold = 90 + (in_grace.astype(int) * 30)
        business_churn_now = (days_since_purchase_today >= threshold).astype(int)
        reasons = []
        for i in range(len(df)):
            days_i = int(days_since_purchase_today.iloc[i]) if pd.notna(days_since_purchase_today.iloc[i]) else 0
            th_i = int(threshold.iloc[i]) if pd.notna(threshold.iloc[i]) else 90
            in_g = bool(in_grace.iloc[i])
            if business_churn_now.iloc[i] == 1:
                if in_g and th_i > 90:
                    reasons.append(f"No purchases for {days_i} days; Grace period exceeded")
                else:
                    reasons.append(f"No purchases for {days_i} days")
            else:
                if in_g and days_i < th_i:
                    reasons.append("In renewal grace period (extra 30 days)")
                elif days_i < 90:
                    reasons.append("Recent purchase within last 90 days")
                else:
                    reasons.append("Within adjusted threshold")
        pred_df['days_since_last_purchase_today'] = days_since_purchase_today.to_numpy()
        pred_df['business_churn_now'] = business_churn_now.to_numpy()
        pred_df['business_churn_reason'] = reasons
    except Exception:
        pass

    # (Reverted) no combined statuses or at-risk flags

    # Rename columns to indicate timeframe (_t0 for snapshot, _t0+90d for horizon)
    rename_map = {
        DATE_COL: "as_of_date_t0",
        "actual_churned_90d": "actual_churned_90d_t0+90d",
        "actual_churn_reason_90d": "actual_churn_reason_t0",
        "predicted_churn_90d": "predicted_churn_90d_t0+90d",
        "predicted_churn_probability_90d": "predicted_churn_probability_90d_t0+90d",
        "predicted_churn_probability_90d_pct": "predicted_churn_probability_90d_pct_t0+90d",
        "predicted_churn_reason_90d": "predicted_churn_reason_t0",
        "business_churn_now": "business_churn_now",
        "business_churn_reason": "business_churn_reason",
    }
    pred_df = pred_df.rename(columns=rename_map)

    # Apply header style
    if args.headers == "friendly":
        friendly_map = {
            CUSTOMER_ID_COL: "Customer ID",
            "as_of_date_t0": "Snapshot Date",
            "days_since_last_purchase_today": "Days Since Last Purchase (Today)",
            "business_churn_now": "Churned Now (Business Rule)",
            "business_churn_reason": "Why (Business Rule)",
            "actual_churned_90d_t0+90d": "Churned Within 90 Days (Actual)",
            "actual_churn_reason_t0": "Why They Churned (Actual)",
            "predicted_churn_90d_t0+90d": "Predicted to Churn (Next 90 Days)",
            "predicted_churn_probability_90d_t0+90d": "Churn Probability (Next 90 Days)",
            "predicted_churn_probability_90d_pct_t0+90d": "Churn Probability % (Next 90 Days)",
            "predicted_churn_reason_t0": "Why At Risk (Predicted)",
        }
        pred_df = pred_df.rename(columns=friendly_map)

        cols_pref = [
            "Customer ID",
            "Snapshot Date",
            "Days Since Last Purchase (Today)",
            "Churned Now (Business Rule)",
            "Why (Business Rule)",
            "Churned Within 90 Days (Actual)",
            "Why They Churned (Actual)",
            "Predicted to Churn (Next 90 Days)",
            "Churn Probability % (Next 90 Days)",
            "Churn Probability (Next 90 Days)",
            "Why At Risk (Predicted)",
        ]
        ordered = [c for c in cols_pref if c in pred_df.columns] + [c for c in pred_df.columns if c not in cols_pref]
        pred_df = pred_df[ordered]
    else:
        # Technical ordering
        cols_pref = [
            CUSTOMER_ID_COL,
            "as_of_date_t0",
            "business_churn_now",
            "business_churn_reason",
            "actual_churned_90d_t0+90d",
            "actual_churn_reason_t0",
            "predicted_churn_90d_t0+90d",
            "predicted_churn_probability_90d_t0+90d",
            "predicted_churn_probability_90d_pct_t0+90d",
            "predicted_churn_reason_t0",
        ]
        ordered = [c for c in cols_pref if c in pred_df.columns] + [c for c in pred_df.columns if c not in cols_pref]
        pred_df = pred_df[ordered]

    # Append CreatedOn timestamp (local time, seconds precision)
    try:
        import pandas as pd  # ensure available here
        pred_df['CreatedOn'] = pd.Timestamp.now().floor('s')
    except Exception:
        pass

    # Rename columns to PascalCase without spaces/underscores/hyphens
    def to_pascal(name: str) -> str:
        name = name.replace('%', ' Pct ')
        tokens = re.findall(r"[A-Za-z0-9]+", name)
        return "".join(t.capitalize() if not t.isnumeric() else t for t in tokens)

    pred_df = pred_df.rename(columns={c: to_pascal(c) for c in pred_df.columns})

    # Save predictions
    default_pred = (
        f"customer_churn_top{args.top}_predictions.csv" if args.top else "customer_churn_predictions.csv"
    )
    out_path = args.output or default_pred
    out = Path(out_path).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    log(f"Writing predictions CSV to {out}...")
    pred_df.to_csv(out, index=False)
    generated_files.append(str(out))

    print(f"Saved predictions for {len(pred_df):,} customers to: {out}")

    # Optional: create table and load to SQL
    if args.load_sql:
        log(f"Loading into SQL: [{args.load_schema}].[{args.load_table}] (if_exists={args.load_if_exists})...")
        _t0 = time.perf_counter()
        try:
            fq, dtypes = create_table_if_missing(cfg, pred_df, args.load_schema, args.load_table)
            load_dataframe(cfg, pred_df, args.load_schema, args.load_table, if_exists=args.load_if_exists)
            print(f"Loaded {len(pred_df):,} rows into {fq}")
            log(f"SQL load completed in {time.perf_counter()-_t0:.1f}s")
        except Exception as e:
            log(f"ERROR loading to SQL: {e}")
            if getattr(sys, 'frozen', False) and len(sys.argv) == 1:
                time.sleep(8)
            return 5
    # Cleanup generated CSVs unless requested to keep
    if not args.keep_csv:
        log("Cleaning up generated CSV files...")
        removed = []
        failed = []
        for fp in generated_files:
            try:
                os.remove(fp)
                removed.append(fp)
            except Exception as e:
                failed.append((fp, str(e)))
        if removed:
            log(f"Removed generated CSVs: {', '.join(removed)}")
        if failed:
            log("Could not remove some files (may be open):")
            for fp, err in failed:
                print(f"  {fp}: {err}")
    if getattr(sys, 'frozen', False) and len(sys.argv) == 1:
        log("Run complete. Closing in 5 seconds...")
        time.sleep(5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

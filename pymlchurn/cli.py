from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from .config import Config
from .db import query_dataframe, connectivity_info, pick_driver
from .sp_runner import maybe_run_sp, SPRunPolicy
from .query import churn_query, feature_columns, target_column, CUSTOMER_ID_COL, DATE_COL
from .ml import MLConfig, train_and_predict


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch data from [SAP].[dbo].[CustomerChurnCadence_v1] and export to CSV.",
    )
    p.add_argument(
        "--top",
        type=int,
        default=None,
        help="Limit rows (TOP N). Default: all rows",
    )
    p.add_argument(
        "--output",
        type=str,
        default=None,
        help="Predictions CSV path (default: customer_churn_predictions.csv)",
    )
    p.add_argument("--raw-output", type=str, default=None, help="Optional: also save raw query CSV here")
    p.add_argument("--target-col", type=str, default=target_column(), help="Target label column (default: churned_dynamic)")
    p.add_argument("--headers", choices=["friendly", "technical"], default="friendly", help="Column header style for output (default: friendly)")
    p.add_argument(
        "--auth",
        choices=["windows", "sql"],
        default=None,
        help="Override MSSQL_AUTH from environment",
    )
    p.add_argument("--username", type=str, default=None, help="SQL login username (if --auth sql)")
    p.add_argument("--password", type=str, default=None, help="SQL login password (if --auth sql)")
    p.add_argument(
        "--driver",
        type=str,
        default=None,
        help="ODBC driver name (e.g. 'ODBC Driver 18 for SQL Server'). Defaults to auto-detect.",
    )
    p.add_argument(
        "--no-encrypt",
        action="store_true",
        help="Disable TLS encryption in connection string",
    )
    p.add_argument(
        "--no-trust-cert",
        action="store_true",
        help="Set TrustServerCertificate=no",
    )
    p.add_argument(
        "--sp-name",
        type=str,
        default="sp_build_customer_churn_cadence_v1",
        help="Stored procedure name to execute before query (default: sp_build_customer_churn_cadence_v1)",
    )
    p.add_argument(
        "--sp-schema",
        type=str,
        default="dbo",
        help="Stored procedure schema (default: dbo)",
    )
    p.add_argument(
        "--sp-ttl-hours",
        type=int,
        default=24,
        help="Minimum hours between stored procedure runs (default: 24)",
    )
    p.add_argument(
        "--force-sp",
        action="store_true",
        help="Force running the stored procedure regardless of TTL",
    )
    p.add_argument(
        "--skip-sp",
        action="store_true",
        help="Skip running the stored procedure",
    )
    p.add_argument(
        "--check",
        action="store_true",
        help="Run connectivity check before querying",
    )
    p.add_argument(
        "--check-only",
        action="store_true",
        help="Only run connectivity check and exit",
    )
    p.add_argument(
        "--keep-all-rows",
        action="store_true",
        help="Do not deduplicate by latest as_of_date per customer",
    )
    p.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="Filter to rows with this as_of date (YYYY-MM-DD)",
    )
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
    args = parse_args(argv)
    cfg = build_config(args)

    if args.check or args.check_only:
        info = connectivity_info(cfg)
        print("Connectivity OK")
        print(f"  Driver:   {info['driver']}")
        print(f"  Server:   {info['server']}")
        print(f"  Database: {info['database']}")
        print(f"  User:     {info['user']}")
        print("  Version:  " + str(info['sqlserver_version']).split("\n")[0])
        if args.check_only:
            return 0

    # Run stored procedure if needed
    if not args.skip_sp:
        policy = SPRunPolicy(ttl_hours=args.sp_ttl_hours)
        res = maybe_run_sp(cfg, args.sp_name, args.sp_schema, force=args.force_sp, policy=policy)
        status = "executed" if res["ran"] else "skipped"
        print(f"Stored procedure {status}: {args.sp_schema}.{args.sp_name} ({res['reason']})")

    # Minimal query for ML
    sql = churn_query(args.top, include_label=True, target=args.target_col)
    df = query_dataframe(cfg, sql)

    # Normalize and optionally filter/deduplicate by date
    import pandas as pd
    if DATE_COL in df.columns:
        df[DATE_COL] = df[DATE_COL].astype(str).str.strip()
        df[DATE_COL] = df[DATE_COL].replace({"None": pd.NA, "NaT": pd.NA, "nan": pd.NA, "": pd.NA})
        if args.as_of:
            df = df[df[DATE_COL] == args.as_of]
        if not args.keep_all_rows:
            df = df.sort_values([DATE_COL], na_position='first').drop_duplicates(subset=[CUSTOMER_ID_COL], keep='last')

    # Optionally save raw query
    if args.raw_output:
        raw_path = Path(args.raw_output).expanduser().resolve()
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(raw_path, index=False)
        print(f"Saved raw rows: {len(df):,} -> {raw_path}")

    # Train and predict
    ml_cfg = MLConfig(customer_id_col=CUSTOMER_ID_COL, feature_cols=feature_columns(), target_col=args.target_col, date_col=DATE_COL)
    pred_df = train_and_predict(df, ml_cfg)

    # Drop legacy columns if present
    if "actual_churned_hard90" in pred_df.columns:
        pred_df = pred_df.drop(columns=["actual_churned_hard90"])

    # Compute business-rule churn as of "today":
    # Churned if no purchases in last 90 days AND not in renewal grace period.
    try:
        as_of_dt = pd.to_datetime(df[DATE_COL], errors='coerce')
        days_since_purchase_today = (pd.Timestamp.today().normalize() - as_of_dt).dt.days.clip(lower=0)
        in_grace = df.get('in_renewal_grace').astype(bool).fillna(False)
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

    # Rename columns to indicate timeframe
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

    # (Reverted) no combined statuses or at-risk flags

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

    # Append CreatedOn timestamp and rename to PascalCase
    try:
        import pandas as pd
        pred_df['CreatedOn'] = pd.Timestamp.now().floor('s')
    except Exception:
        pass

    import re as _re
    def _to_pascal(name: str) -> str:
        name = name.replace('%', ' Pct ')
        tokens = _re.findall(r"[A-Za-z0-9]+", name)
        return "".join(t.capitalize() if not t.isnumeric() else t for t in tokens)

    pred_df = pred_df.rename(columns={c: _to_pascal(c) for c in pred_df.columns})

    # Save predictions
    default_pred = (
        f"customer_churn_top{args.top}_predictions.csv" if args.top else "customer_churn_predictions.csv"
    )
    out_path = args.output or default_pred
    out = Path(out_path).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    pred_df.to_csv(out, index=False)

    print(f"Saved predictions for {len(pred_df):,} customers to: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



"""
Run script to train churn model on labeled historical data and score current snapshots.

Usage examples:
  python pyMLChurn.py
  python pyMLChurn.py --score-as-of 2026-03-01
  python pyMLChurn.py --train-cutoff-date 2025-12-31 --temporal-test-months 2 --temporal-validation-months 2
  python pyMLChurn.py --allow-pseudo-fallback
"""

from __future__ import annotations

import argparse
import atexit
import re
from pathlib import Path
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional
import sys
from dotenv import find_dotenv

from pymlchurn.config import Config
from pymlchurn.db import query_dataframe, pick_driver, connectivity_info
from pymlchurn.query import (
    CUSTOMER_ID_COL,
    DATE_COL,
    churn_scoring_query,
    churn_training_query,
    direct_rule_feature_columns,
    feature_columns,
    target_column,
)
from pymlchurn.load_sql import create_table_if_missing, load_dataframe


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Train churn model on labeled historical rows from [SAP].[chrn01].[v_train_dataset] "
            "and score current snapshot rows for SQL output."
        ),
    )
    p.add_argument("--top", type=int, default=None, help="Limit rows per query (TOP N). Default: all rows")
    p.add_argument("--target-col", type=str, default=target_column(), help="Target label column (default: Label_Churn_90d)")
    p.add_argument("--train-cutoff-date", type=str, default=None, help="Optional training cutoff date (YYYY-MM-DD)")
    p.add_argument("--score-as-of", type=str, default=None, help="Optional scoring snapshot date (YYYY-MM-DD)")
    p.add_argument("--as-of", type=str, default=None, help="Deprecated alias for --score-as-of")
    p.add_argument("--exclude-direct-rule-features", action="store_true", help="Exclude direct rule-like churn flags from model features")

    p.add_argument("--allow-pseudo-fallback", action="store_true", help="Allow pseudo-label fallback when supervised labels are insufficient")
    p.add_argument("--min-labeled-rows", type=int, default=500, help="Minimum labeled rows required for supervised training")
    p.add_argument("--min-positive-rows", type=int, default=25, help="Minimum positive rows required in training labels")
    p.add_argument("--temporal-test-months", type=int, default=2, help="Number of most recent months reserved for temporal test holdout")
    p.add_argument("--temporal-validation-months", type=int, default=2, help="Number of months before test reserved for temporal validation")

    p.add_argument("--output-dir", type=str, default=None, help="Directory to write metrics/artifacts (default: current run folder)")
    p.add_argument("--save-metrics-artifacts", dest="save_metrics_artifacts", action="store_true", help="Save metrics/explainability CSV artifacts")
    p.add_argument("--no-save-metrics-artifacts", dest="save_metrics_artifacts", action="store_false", help="Disable artifact CSV output")
    p.set_defaults(save_metrics_artifacts=True)

    p.add_argument(
        "--metrics-log-file",
        type=str,
        default=None,
        help="Append engineer-facing metrics history to this text file (default: pyMLChurn_model_metrics_history.txt in run folder)",
    )

    p.add_argument("--load-schema", type=str, default="chrn01", help="Target schema for SQL load (default: chrn01)")
    p.add_argument("--load-table", type=str, default="CustomerChurnPredictions", help="Target table name for SQL load")
    p.add_argument("--load-if-exists", choices=["append", "replace", "fail"], default="replace", help="Behavior if table exists (default: replace)")

    p.add_argument("--auth", choices=["windows", "sql"], default=None, help="Override MSSQL_AUTH")
    p.add_argument("--username", type=str, default=None, help="SQL login username (if --auth sql)")
    p.add_argument("--password", type=str, default=None, help="SQL login password (if --auth sql)")
    p.add_argument("--driver", type=str, default=None, help="ODBC driver name (e.g. 'ODBC Driver 18 for SQL Server')")
    p.add_argument("--no-encrypt", action="store_true", help="Disable TLS encryption")
    p.add_argument("--no-trust-cert", action="store_true", help="Set TrustServerCertificate=no")
    p.add_argument("--check", action="store_true", help="Run connectivity check before querying")
    p.add_argument("--check-only", action="store_true", help="Only run connectivity check and exit")
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


def _fmt_num(v: Any, digits: int = 6) -> str:
    if v is None:
        return "N/A"
    try:
        return f"{float(v):.{digits}f}"
    except Exception:
        return str(v)


def _fmt_pct(v: Any, digits: int = 2) -> str:
    if v is None:
        return "N/A"
    try:
        return f"{float(v) * 100.0:.{digits}f}%"
    except Exception:
        return str(v)


def _save_artifacts(artifacts: Dict[str, Any], output_dir: Path) -> Dict[str, str]:
    paths: Dict[str, str] = {}
    mapping = {
        "holdout_metrics_summary": "holdout_metrics_summary.csv",
        "validation_metrics_summary": "validation_metrics_summary.csv",
        "calibration_deciles": "calibration_by_decile.csv",
        "coefficients_standardized": "coefficients_standardized.csv",
        "odds_ratios": "odds_ratios.csv",
        "threshold_diagnostics": "threshold_diagnostics.csv",
        "class_weight_comparison": "class_weight_comparison.csv",
        "business_rule_holdout_metrics": "business_rule_holdout_metrics.csv",
    }
    for key, filename in mapping.items():
        if key not in artifacts:
            continue
        df = artifacts[key]
        path = output_dir / filename
        try:
            df.to_csv(path, index=False)
            paths[key] = str(path)
        except Exception:
            continue
    return paths


def _append_metrics_history(
    args: argparse.Namespace,
    log,
    run_dir: Path,
    metrics: Dict[str, Any],
    prediction_value_type: str,
    artifact_paths: Dict[str, str],
) -> None:
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        readable_path = Path(args.metrics_log_file).expanduser() if args.metrics_log_file else (run_dir / "pyMLChurn_model_metrics_history.txt")
        readable_path.parent.mkdir(parents=True, exist_ok=True)

        if readable_path.suffix.lower() == ".tsv":
            tsv_path = readable_path
            readable_path = readable_path.with_name(readable_path.stem + "_readable.txt")
        elif readable_path.suffix:
            tsv_path = readable_path.with_suffix(".tsv")
        else:
            tsv_path = readable_path.with_name(readable_path.name + ".tsv")

        split = metrics.get("temporal_split", {}) or {}
        train_split = split.get("train", {}) or {}
        val_split = split.get("validation", {}) or {}
        test_split = split.get("test", {}) or {}
        holdout = metrics.get("holdout_metrics", {}) or {}
        prod = metrics.get("production_scoring_summary", {}) or {}
        cal = metrics.get("calibration_summary", {}) or {}
        explain = metrics.get("explainability", {}) or {}
        model_selection = metrics.get("model_selection", {}) or {}
        baseline = metrics.get("business_rule_holdout_metrics", {}) or {}

        selected_class_weight = metrics.get("selected_class_weight", model_selection.get("selected_class_weight"))
        selected_threshold = metrics.get("selected_threshold", model_selection.get("selected_threshold"))

        top_pos = explain.get("top_positive_features", []) or []
        top_neg = explain.get("top_negative_features", []) or []
        top_pos_txt = ", ".join([f"{x.get('feature')}({_fmt_num(x.get('coefficient_standardized'), 4)})" for x in top_pos[:5]]) or "N/A"
        top_neg_txt = ", ".join([f"{x.get('feature')}({_fmt_num(x.get('coefficient_standardized'), 4)})" for x in top_neg[:5]]) or "N/A"

        report_lines = [
            "=" * 72,
            f"Run Timestamp: {ts}",
            f"Model Family: {metrics.get('model_family', 'N/A')}",
            f"Training Mode: {metrics.get('training_mode', 'N/A')}",
            f"Target Column: {metrics.get('target_col', 'N/A')}",
            f"Training Label Source: {metrics.get('train_label_source', 'N/A')}",
            "Scoring Population: current_snapshot",
            f"Evaluation Population: {metrics.get('evaluation_population', 'N/A')}",
            "Temporal Split:",
            f"  Train Range: {train_split.get('min_date', 'N/A')} to {train_split.get('max_date', 'N/A')}",
            f"  Validation Range: {val_split.get('min_date', 'N/A')} to {val_split.get('max_date', 'N/A')}",
            f"  Test Range: {test_split.get('min_date', 'N/A')} to {test_split.get('max_date', 'N/A')}",
            f"  Train Rows: {train_split.get('rows', 'N/A')}",
            f"  Validation Rows: {val_split.get('rows', 'N/A')}",
            f"  Test Rows: {test_split.get('rows', 'N/A')}",
            "Class Balance:",
            f"  Train Positive Rate: {_fmt_pct(train_split.get('positive_rate'))}",
            f"  Validation Positive Rate: {_fmt_pct(val_split.get('positive_rate'))}",
            f"  Test Positive Rate: {_fmt_pct(test_split.get('positive_rate'))}",
            "",
            "Model Selection (validation):",
            f"  Selected Class Weight: {selected_class_weight}",
            f"  Validation PR AUC (None): {_fmt_num(metrics.get('validation_pr_auc_none'))}",
            f"  Validation ROC AUC (None): {_fmt_num(metrics.get('validation_roc_auc_none'))}",
            f"  Validation PR AUC (Balanced): {_fmt_num(metrics.get('validation_pr_auc_balanced'))}",
            f"  Validation ROC AUC (Balanced): {_fmt_num(metrics.get('validation_roc_auc_balanced'))}",
            f"  Selected Threshold: {_fmt_num(selected_threshold, 4)}",
            "",
            "Feature Configuration:",
            f"  Feature Count Used: {metrics.get('feature_count_used', 'N/A')}",
            f"  Excluded Direct Rule Features: {metrics.get('excluded_direct_rule_features', False)}",
            f"  Excluded Feature Names: {', '.join(metrics.get('excluded_feature_names', [])) if metrics.get('excluded_feature_names') else 'N/A'}",
            "",
            "Holdout Classification Metrics (labeled temporal holdout):",
            f"  Accuracy: {_fmt_num(holdout.get('accuracy'))}",
            f"  Precision: {_fmt_num(holdout.get('precision'))}",
            f"  Recall: {_fmt_num(holdout.get('recall'))}",
            f"  F1: {_fmt_num(holdout.get('f1'))}",
            f"  ROC AUC: {_fmt_num(holdout.get('roc_auc'))}",
            f"  PR AUC: {_fmt_num(holdout.get('pr_auc'))}",
            f"  Brier Score: {_fmt_num(holdout.get('brier_score'))}",
            f"  Log Loss: {_fmt_num(holdout.get('log_loss'))}",
            f"  TP: {holdout.get('tp', 'N/A')}",
            f"  TN: {holdout.get('tn', 'N/A')}",
            f"  FP: {holdout.get('fp', 'N/A')}",
            f"  FN: {holdout.get('fn', 'N/A')}",
            "",
            "Ranking Quality (labeled temporal holdout):",
            f"  Precision@Top10%: {_fmt_num(holdout.get('precision_at_top10pct'))}",
            f"  Recall@Top10%: {_fmt_num(holdout.get('recall_at_top10pct'))}",
            f"  Lift@Top10%: {_fmt_num(holdout.get('lift_at_top10pct'))}",
            f"  Precision@Top5%: {_fmt_num(holdout.get('precision_at_top5pct'))}",
            f"  Recall@Top5%: {_fmt_num(holdout.get('recall_at_top5pct'))}",
            f"  Lift@Top5%: {_fmt_num(holdout.get('lift_at_top5pct'))}",
            "",
            "Holdout Business-Rule Baseline (labeled temporal holdout):",
            f"  Accuracy: {_fmt_num(baseline.get('accuracy'))}",
            f"  Precision: {_fmt_num(baseline.get('precision'))}",
            f"  Recall: {_fmt_num(baseline.get('recall'))}",
            f"  F1: {_fmt_num(baseline.get('f1'))}",
            f"  TP: {baseline.get('tp', 'N/A')}",
            f"  TN: {baseline.get('tn', 'N/A')}",
            f"  FP: {baseline.get('fp', 'N/A')}",
            f"  FN: {baseline.get('fn', 'N/A')}",
            f"  Predicted Positive Rate: {_fmt_pct(baseline.get('predicted_positive_rate'))}",
            "",
            "Calibration Summary (labeled temporal holdout):",
            f"  Avg Predicted Probability: {_fmt_num(cal.get('avg_predicted_probability'))}",
            f"  Actual Positive Rate: {_fmt_num(cal.get('actual_positive_rate'))}",
            f"  Calibration Gap: {_fmt_num(cal.get('calibration_gap'))}",
            f"  Expected Calibration Error: {_fmt_num(cal.get('expected_calibration_error'))}",
            f"  Decile Table File: {artifact_paths.get('calibration_deciles', 'N/A')}",
            "",
            "Model Explainability (model-level):",
            f"  Top Risk-Increasing Features: {top_pos_txt}",
            f"  Top Risk-Reducing Features: {top_neg_txt}",
            f"  Coefficient File: {artifact_paths.get('coefficients_standardized', 'N/A')}",
            f"  Odds Ratio File: {artifact_paths.get('odds_ratios', 'N/A')}",
            "",
            "Production Scoring Summary (unlabeled current scoring rows):",
            f"  Rows Scored: {prod.get('row_count', 'N/A')}",
            f"  Predicted Positives: {prod.get('predicted_positives', 'N/A')}",
            f"  Predicted Positive Rate: {_fmt_pct(prod.get('predicted_positive_rate'))}",
            f"  Average Score: {_fmt_num(prod.get('avg_predicted_probability'))}",
            f"  P90 Score: {_fmt_num(prod.get('p90_predicted_probability'))}",
            f"  Prediction Value Type: {prediction_value_type}",
            f"  Holdout Metrics File: {artifact_paths.get('holdout_metrics_summary', 'N/A')}",
            "=" * 72,
        ]

        block = "\n".join(report_lines)
        prefix = "\n\n" if readable_path.exists() and readable_path.stat().st_size > 0 else ""
        with open(readable_path, "a", encoding="utf-8") as f:
            f.write(prefix + block + "\n")

        headers = [
            "run_timestamp",
            "model_family",
            "training_mode",
            "target_col",
            "train_label_source",
            "evaluation_population",
            "prediction_value_type",
            "selected_class_weight",
            "selected_threshold",
            "feature_count_used",
            "excluded_direct_rule_features",
            "excluded_feature_names",
            "validation_pr_auc_none",
            "validation_roc_auc_none",
            "validation_pr_auc_balanced",
            "validation_roc_auc_balanced",
            "train_rows",
            "validation_rows",
            "test_rows",
            "train_positive_rate",
            "validation_positive_rate",
            "test_positive_rate",
            "accuracy",
            "precision",
            "recall",
            "f1",
            "roc_auc",
            "pr_auc",
            "brier_score",
            "log_loss",
            "tp",
            "tn",
            "fp",
            "fn",
            "precision_at_top10pct",
            "recall_at_top10pct",
            "lift_at_top10pct",
            "precision_at_top5pct",
            "recall_at_top5pct",
            "lift_at_top5pct",
            "business_rule_accuracy",
            "business_rule_precision",
            "business_rule_recall",
            "business_rule_f1",
            "business_rule_tp",
            "business_rule_tn",
            "business_rule_fp",
            "business_rule_fn",
            "business_rule_predicted_positive_rate",
            "expected_calibration_error",
            "prod_rows_scored",
            "prod_predicted_positives",
            "prod_predicted_positive_rate",
            "prod_avg_score",
            "prod_p90_score",
            "artifacts_holdout_metrics_summary",
            "artifacts_calibration_deciles",
            "artifacts_coefficients_standardized",
            "artifacts_odds_ratios",
            "artifacts_threshold_diagnostics",
            "artifacts_class_weight_comparison",
            "artifacts_business_rule_holdout_metrics",
        ]

        row = {
            "run_timestamp": ts,
            "model_family": metrics.get("model_family"),
            "training_mode": metrics.get("training_mode"),
            "target_col": metrics.get("target_col"),
            "train_label_source": metrics.get("train_label_source"),
            "evaluation_population": metrics.get("evaluation_population"),
            "prediction_value_type": prediction_value_type,
            "selected_class_weight": selected_class_weight,
            "selected_threshold": selected_threshold,
            "feature_count_used": metrics.get("feature_count_used"),
            "excluded_direct_rule_features": metrics.get("excluded_direct_rule_features"),
            "excluded_feature_names": ",".join(metrics.get("excluded_feature_names", [])),
            "validation_pr_auc_none": metrics.get("validation_pr_auc_none"),
            "validation_roc_auc_none": metrics.get("validation_roc_auc_none"),
            "validation_pr_auc_balanced": metrics.get("validation_pr_auc_balanced"),
            "validation_roc_auc_balanced": metrics.get("validation_roc_auc_balanced"),
            "train_rows": train_split.get("rows"),
            "validation_rows": val_split.get("rows"),
            "test_rows": test_split.get("rows"),
            "train_positive_rate": train_split.get("positive_rate"),
            "validation_positive_rate": val_split.get("positive_rate"),
            "test_positive_rate": test_split.get("positive_rate"),
            "accuracy": holdout.get("accuracy"),
            "precision": holdout.get("precision"),
            "recall": holdout.get("recall"),
            "f1": holdout.get("f1"),
            "roc_auc": holdout.get("roc_auc"),
            "pr_auc": holdout.get("pr_auc"),
            "brier_score": holdout.get("brier_score"),
            "log_loss": holdout.get("log_loss"),
            "tp": holdout.get("tp"),
            "tn": holdout.get("tn"),
            "fp": holdout.get("fp"),
            "fn": holdout.get("fn"),
            "precision_at_top10pct": holdout.get("precision_at_top10pct"),
            "recall_at_top10pct": holdout.get("recall_at_top10pct"),
            "lift_at_top10pct": holdout.get("lift_at_top10pct"),
            "precision_at_top5pct": holdout.get("precision_at_top5pct"),
            "recall_at_top5pct": holdout.get("recall_at_top5pct"),
            "lift_at_top5pct": holdout.get("lift_at_top5pct"),
            "business_rule_accuracy": metrics.get("business_rule_accuracy"),
            "business_rule_precision": metrics.get("business_rule_precision"),
            "business_rule_recall": metrics.get("business_rule_recall"),
            "business_rule_f1": metrics.get("business_rule_f1"),
            "business_rule_tp": metrics.get("business_rule_tp"),
            "business_rule_tn": metrics.get("business_rule_tn"),
            "business_rule_fp": metrics.get("business_rule_fp"),
            "business_rule_fn": metrics.get("business_rule_fn"),
            "business_rule_predicted_positive_rate": metrics.get("business_rule_predicted_positive_rate"),
            "expected_calibration_error": metrics.get("expected_calibration_error"),
            "prod_rows_scored": prod.get("row_count"),
            "prod_predicted_positives": prod.get("predicted_positives"),
            "prod_predicted_positive_rate": prod.get("predicted_positive_rate"),
            "prod_avg_score": prod.get("avg_predicted_probability"),
            "prod_p90_score": prod.get("p90_predicted_probability"),
            "artifacts_holdout_metrics_summary": artifact_paths.get("holdout_metrics_summary"),
            "artifacts_calibration_deciles": artifact_paths.get("calibration_deciles"),
            "artifacts_coefficients_standardized": artifact_paths.get("coefficients_standardized"),
            "artifacts_odds_ratios": artifact_paths.get("odds_ratios"),
            "artifacts_threshold_diagnostics": artifact_paths.get("threshold_diagnostics"),
            "artifacts_class_weight_comparison": artifact_paths.get("class_weight_comparison"),
            "artifacts_business_rule_holdout_metrics": artifact_paths.get("business_rule_holdout_metrics"),
        }

        write_header = not tsv_path.exists()
        with open(tsv_path, "a", encoding="utf-8") as f:
            if write_header:
                f.write("\t".join(headers) + "\n")
            f.write("\t".join([str(row.get(h, "")) for h in headers]) + "\n")

        log(f"Model metrics appended to: {readable_path} (readable), {tsv_path} (tabular)")
    except Exception as e:
        log(f"WARNING: could not write model metrics history: {e}")


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    log_fh = None
    orig_out = None
    orig_err = None
    try:
        if getattr(sys, "frozen", False):
            base_log_dir = Path(sys.executable).resolve().parent
        else:
            base_log_dir = Path.cwd()
        base_log_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        base_log_dir = Path.cwd()

    log_file = str(base_log_dir / f"pyMLChurn_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    def _cleanup_logs() -> None:
        nonlocal log_file, log_fh, orig_out, orig_err
        try:
            if orig_out is not None:
                sys.stdout = orig_out
            if orig_err is not None:
                sys.stderr = orig_err
        except Exception:
            pass
        try:
            if log_fh:
                log_fh.flush()
                log_fh.close()
        except Exception:
            pass

    atexit.register(_cleanup_logs)

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

        _fh = open(log_file, "a", encoding="utf-8", buffering=1)
        log_fh = _fh
        orig_out, orig_err = sys.stdout, sys.stderr
        sys.stdout = _Tee(orig_out, _fh)
        sys.stderr = _Tee(orig_err, _fh)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Logging initialized -> {log_file}")
    except Exception:
        pass

    def ts() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def log(msg: str) -> None:
        line = f"[{ts()}] {msg}"
        print(line)

    try:
        if getattr(sys, "frozen", False) and len(sys.argv) == 1:
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

    run_dir = Path(args.output_dir).expanduser() if args.output_dir else base_log_dir
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        log(f"ERROR creating output directory '{run_dir}': {e}")
        return 2

    score_as_of = args.score_as_of or args.as_of

    try:
        cfg = build_config(args)
    except Exception as e:
        log(f"ERROR loading configuration: {e}")
        if getattr(sys, "frozen", False) and len(sys.argv) == 1:
            time.sleep(8)
        return 2

    chosen_driver = pick_driver(cfg.odbc_driver)
    log(f"Using ODBC driver: {chosen_driver}")
    log(f"Run output directory: {run_dir}")

    if args.check or args.check_only:
        log("Running connectivity check...")
        info = connectivity_info(cfg)
        log("Connectivity OK")
        log(f"Driver: {info['driver']}")
        log(f"Server: {info['server']} | Database: {info['database']}")
        log(f"User: {info['user']}")
        log("Version: " + str(info["sqlserver_version"]).split("\n")[0])
        if args.check_only:
            return 0

    import pandas as pd

    train_sql = churn_training_query(target=args.target_col, top=args.top, train_cutoff_date=args.train_cutoff_date)
    score_sql = churn_scoring_query(score_as_of=score_as_of, top=args.top)

    log("Running training query (labeled historical)...")
    q0 = time.perf_counter()
    try:
        train_df = query_dataframe(cfg, train_sql)
    except Exception as e:
        log(f"ERROR running training query: {e}")
        return 3
    log(f"Training query returned {len(train_df):,} rows in {time.perf_counter() - q0:.1f}s")

    log("Running scoring query (current snapshot)...")
    q1 = time.perf_counter()
    try:
        score_df = query_dataframe(cfg, score_sql)
    except Exception as e:
        log(f"ERROR running scoring query: {e}")
        return 3
    log(f"Scoring query returned {len(score_df):,} rows in {time.perf_counter() - q1:.1f}s")

    if len(train_df) == 0:
        log("ERROR: training query returned 0 rows. Check label availability and train cutoff settings.")
        return 3
    if len(score_df) == 0:
        log("ERROR: scoring query returned 0 rows. Check score-as-of or source view freshness.")
        return 3
    if DATE_COL not in score_df.columns:
        log(f"ERROR: scoring dataframe is missing required date column '{DATE_COL}'.")
        return 3

    score_df[DATE_COL] = score_df[DATE_COL].astype(str).str.strip()
    score_df[DATE_COL] = score_df[DATE_COL].replace({"None": pd.NA, "NaT": pd.NA, "nan": pd.NA, "": pd.NA})

    dup_mask = score_df.duplicated(subset=[CUSTOMER_ID_COL], keep=False)
    dup_rows = int(dup_mask.sum())
    dup_customers = int(score_df.loc[dup_mask, CUSTOMER_ID_COL].nunique()) if dup_rows > 0 else 0
    if dup_rows > 0:
        log(
            "WARNING: scoring snapshot contains duplicate customers before dedupe. "
            f"duplicate_rows={dup_rows}, affected_customers={dup_customers}."
        )

    score_df = score_df.sort_values([DATE_COL], na_position="first").drop_duplicates(subset=[CUSTOMER_ID_COL], keep="last")
    score_df = score_df.reset_index(drop=True)

    log("Training supervised model with temporal split and scoring current snapshot...")
    t0 = time.perf_counter()
    try:
        from pymlchurn.ml import MLConfig, MLRunOptions, train_evaluate_and_score

        all_features = feature_columns()
        excluded_features = []
        model_features = list(all_features)
        if args.exclude_direct_rule_features:
            direct = set(direct_rule_feature_columns())
            excluded_features = [f for f in all_features if f in direct]
            model_features = [f for f in all_features if f not in direct]

        if not model_features:
            raise ValueError("No modeling features remain after exclusion settings.")

        log(f"Model feature count: {len(model_features)}")
        if excluded_features:
            log("Excluded direct rule features: " + ", ".join(excluded_features))

        ml_cfg = MLConfig(
            customer_id_col=CUSTOMER_ID_COL,
            feature_cols=model_features,
            target_col=args.target_col,
            date_col=DATE_COL,
        )
        ml_options = MLRunOptions(
            allow_pseudo_fallback=bool(args.allow_pseudo_fallback),
            min_labeled_rows=int(args.min_labeled_rows),
            min_positive_rows=int(args.min_positive_rows),
            temporal_test_months=int(args.temporal_test_months),
            temporal_validation_months=int(args.temporal_validation_months),
            excluded_direct_rule_features=bool(args.exclude_direct_rule_features),
            excluded_feature_names=excluded_features,
        )

        pred_df, model_metrics, metric_artifacts = train_evaluate_and_score(train_df, score_df, ml_cfg, ml_options)
    except Exception as e:
        log(f"ERROR during model training/scoring: {e}")
        return 4
    log(f"Model + scoring completed in {time.perf_counter() - t0:.1f}s")

    artifact_paths: Dict[str, str] = {}
    if args.save_metrics_artifacts:
        artifact_paths = _save_artifacts(metric_artifacts, run_dir)
        if artifact_paths:
            log("Saved metrics artifacts:")
            for k, v in artifact_paths.items():
                log(f"  {k}: {v}")

    is_proxy_score = model_metrics.get("training_mode") != "supervised_only"
    if is_proxy_score:
        if "predicted_churn_probability_90d" in pred_df.columns:
            pred_df["predicted_churn_score_90d"] = pred_df["predicted_churn_probability_90d"]
        if "predicted_churn_probability_90d_pct" in pred_df.columns:
            pred_df["predicted_churn_score_90d_pct"] = pred_df["predicted_churn_probability_90d_pct"]
        pred_df = pred_df.drop(
            columns=[c for c in ["predicted_churn_probability_90d", "predicted_churn_probability_90d_pct"] if c in pred_df.columns]
        )
        pred_df["prediction_value_type"] = "risk_score_proxy"
        log("Prediction values emitted as risk_score_proxy because pseudo fallback mode was used.")
    else:
        pred_df["prediction_value_type"] = "churn_probability_estimate"

    pred_df.attrs["model_metrics"] = model_metrics
    log(
        "Model selection: "
        f"class_weight={model_metrics.get('selected_class_weight')}, "
        f"threshold={_fmt_num(model_metrics.get('selected_threshold'), 4)}, "
        f"feature_count={model_metrics.get('feature_count_used')}, "
        f"excluded_features={','.join(model_metrics.get('excluded_feature_names', [])) if model_metrics.get('excluded_feature_names') else 'none'}"
    )
    _append_metrics_history(
        args=args,
        log=log,
        run_dir=run_dir,
        metrics=model_metrics,
        prediction_value_type=("risk_score_proxy" if is_proxy_score else "churn_probability_estimate"),
        artifact_paths=artifact_paths,
    )

    # Attach business-rule snapshot signals to scored rows for downstream comparability.
    try:
        churn_flag = pd.to_numeric(score_df.get("ChurnIf_NoOrd90"), errors="coerce").fillna(0).astype(int)
        maint_active = pd.to_numeric(score_df.get("MaintContractActive"), errors="coerce").fillna(0).astype(int)
        business_churn_now = ((churn_flag == 1) & (maint_active == 0)).astype(int)
        reasons = []
        for i in range(len(score_df)):
            if maint_active.iloc[i] == 1:
                reasons.append("Excluded: Active maintenance contract")
            elif churn_flag.iloc[i] == 1:
                reasons.append("No qualifying orders in last 90 days (snapshot rule)")
            else:
                reasons.append("Not churn by 90-day rule")
        pred_df["business_churn_now"] = business_churn_now.to_numpy()
        pred_df["business_churn_reason"] = reasons
        if "Recency_Orders_Days" in score_df.columns:
            pred_df["recency_orders_days_t0"] = pd.to_numeric(score_df["Recency_Orders_Days"], errors="coerce").to_numpy()
    except Exception:
        pass

    if is_proxy_score:
        pred_metric_src = "predicted_churn_score_90d"
        pred_metric_pct_src = "predicted_churn_score_90d_pct"
        pred_metric_tgt = "predicted_churn_risk_score_90d_t0+90d"
        pred_metric_pct_tgt = "predicted_churn_risk_score_90d_pct_t0+90d"
    else:
        pred_metric_src = "predicted_churn_probability_90d"
        pred_metric_pct_src = "predicted_churn_probability_90d_pct"
        pred_metric_tgt = "predicted_churn_probability_90d_t0+90d"
        pred_metric_pct_tgt = "predicted_churn_probability_90d_pct_t0+90d"

    rename_map = {
        DATE_COL: "as_of_date_t0",
        "actual_churned_90d": "actual_churned_90d_t0+90d",
        "actual_churn_reason_90d": "actual_churn_reason_t0",
        "predicted_churn_90d": "predicted_churn_90d_t0+90d",
        pred_metric_src: pred_metric_tgt,
        pred_metric_pct_src: pred_metric_pct_tgt,
        "predicted_churn_reason_90d": "predicted_churn_reason_t0",
        "business_churn_now": "business_churn_now",
        "business_churn_reason": "business_churn_reason",
        "prediction_value_type": "prediction_value_type",
    }
    pred_df = pred_df.rename(columns=rename_map)

    try:
        as_of_ts = pd.to_datetime(pred_df.get("as_of_date_t0"), errors="coerce")
        churn_month = (as_of_ts + pd.to_timedelta(90, unit="D")).dt.to_period("M").dt.to_timestamp()
        pred_df["predicted_churn_month_t0+90d"] = churn_month.dt.strftime("%Y-%m-01")
    except Exception:
        pass

    # Explicit stable SQL schema lock before persistence to prevent column drift.
    stable_columns = [
        CUSTOMER_ID_COL,
        "as_of_date_t0",
        "recency_orders_days_t0",
        "business_churn_now",
        "business_churn_reason",
        "actual_churned_90d_t0+90d",
        "actual_churn_reason_t0",
        "predicted_churn_90d_t0+90d",
        pred_metric_tgt,
        pred_metric_pct_tgt,
        "prediction_value_type",
        "predicted_churn_reason_t0",
        "predicted_churn_month_t0+90d",
    ]
    for c in stable_columns:
        if c not in pred_df.columns:
            pred_df[c] = pd.NA
    pred_df = pred_df[stable_columns]

    try:
        pred_df["CreatedOn"] = pd.Timestamp.now().floor("s")
    except Exception:
        pass

    def to_pascal(name: str) -> str:
        name = name.replace("%", " Pct ")
        tokens = re.findall(r"[A-Za-z0-9]+", name)
        return "".join(t.capitalize() if not t.isnumeric() else t for t in tokens)

    pred_df = pred_df.rename(columns={c: to_pascal(c) for c in pred_df.columns})

    log(f"Loading into SQL: [{args.load_schema}].[{args.load_table}] (if_exists={args.load_if_exists})...")
    l0 = time.perf_counter()
    try:
        fq, _dtypes = create_table_if_missing(cfg, pred_df, args.load_schema, args.load_table)
        load_dataframe(cfg, pred_df, args.load_schema, args.load_table, if_exists=args.load_if_exists)
        log(f"Loaded {len(pred_df):,} rows into {fq}")
        log(f"SQL load completed in {time.perf_counter() - l0:.1f}s")
    except Exception as e:
        log(f"ERROR loading to SQL: {e}")
        if getattr(sys, "frozen", False) and len(sys.argv) == 1:
            time.sleep(8)
        return 5

    if getattr(sys, "frozen", False) and len(sys.argv) == 1:
        log("Run complete. Closing in 5 seconds...")
        time.sleep(5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

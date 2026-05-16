# CHANGELOG

## Summary
Final cleanup pass focused on readability and maintenance safety without changing model behavior.

## What Changed
- Deprecated legacy backdoor entrypoint `train_and_predict(...)` with a hard error.
- Added direct-rule feature ablation support:
  - CLI: `--exclude-direct-rule-features`
  - Helper: `direct_rule_feature_columns()`
- Kept validation-driven selection logic in place:
  - class-weight benchmark (`none` vs `balanced`)
  - threshold tuning grid (`0.05` to `0.95`)
- Kept holdout business-rule baseline reporting and ECE calibration reporting.
- Locked SQL output to an explicit stable schema before load.
- Added duplicate-customer diagnostics before scoring dedupe.
- Expanded readable TXT and TSV history with model-selection, feature-config, baseline, ECE, and artifact-path fields.

## Why
- Remove misleading/dead code and improve operational observability.
- Keep outputs deterministic and production-safe while preserving current behavior.

## Command Examples
### 1) Default supervised temporal workflow
```bash
python pyMLChurn.py
```

### 2) Supervised run with direct-rule feature ablation
```bash
python pyMLChurn.py --exclude-direct-rule-features
```

### 3) Supervised run with explicit controls
```bash
python pyMLChurn.py \
  --train-cutoff-date 2025-12-31 \
  --score-as-of 2026-03-01 \
  --temporal-validation-months 2 \
  --temporal-test-months 2 \
  --min-labeled-rows 1000 \
  --min-positive-rows 50 \
  --output-dir ./runs
```

## Metrics Artifacts
- `holdout_metrics_summary.csv`: labeled temporal holdout model metrics.
- `validation_metrics_summary.csv`: selected-threshold validation metrics.
- `threshold_diagnostics.csv`: threshold grid diagnostics used for threshold selection.
- `class_weight_comparison.csv`: validation comparison for class-weight candidates.
- `business_rule_holdout_metrics.csv`: holdout baseline metrics from rule predictions.
- `calibration_by_decile.csv`: decile calibration table.
- `coefficients_standardized.csv`: standardized logistic coefficients.
- `odds_ratios.csv`: odds ratio table.
- `pyMLChurn_model_metrics_history.txt`: readable engineer-facing run report.
- `pyMLChurn_model_metrics_history.tsv`: tabular run history for tracking.

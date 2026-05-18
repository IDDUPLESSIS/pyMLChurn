# Changelog

## Current

### Changed
- Added a deterministic business-risk interpretation layer in `pymlchurn/risk.py`.
- Separated ML prediction score from raw business-rule risk, maintenance protection, adjusted business risk, and final business status.
- Changed maintenance contracts from a hard churn veto to a decaying protection modifier.
- Added watchlist and protected-at-risk classifications for commercially inactive accounts.
- Added recency validation for `SnapshotDate`, `LastOrderDate`, and `Recency_Orders_Days`.
- Updated `[chrn01].[v_CustomerChurnPredictions]` so report HTML uses adjusted business risk while preserving ML score columns separately.
- Exported canonical `[chrn01]` SQL objects under `sql/chrn01/`.
- Rewrote `README.md` for portfolio/recruiter readability and current operational behavior.

### Added Output Columns
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

### Cleanup
- Removed tracked `ChatGPT/` source snapshots.
- Removed generated model metrics history files from source control.
- Removed legacy duplicate SQL files now superseded by `sql/chrn01/`.
- Updated `.gitignore` to keep generated logs, metrics history, CSV artifacts, build outputs, and local scratch folders out of the repository.

## Earlier

- Added supervised temporal training and scoring workflow.
- Added class-weight comparison and threshold diagnostics.
- Added holdout metrics, validation metrics, calibration summaries, and model explainability artifacts.
- Added SQL load support for `[chrn01].[CustomerChurnPredictions]`.
- Added one-click Python and packaged EXE runners.

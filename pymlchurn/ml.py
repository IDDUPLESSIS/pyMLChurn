from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
try:
    import shap  # type: ignore
    _HAS_SHAP = True
except Exception:
    shap = None  # type: ignore
    _HAS_SHAP = False


@dataclass
class MLConfig:
    customer_id_col: str
    feature_cols: List[str]
    target_col: Optional[str] = None
    date_col: Optional[str] = None


def _coerce_types(df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for col in feature_cols:
        if col not in out.columns:
            continue
        # Convert booleans to 0/1 if present
        if out[col].dtype == bool:
            out[col] = out[col].astype(int)
        # Try numeric conversion for any column
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def build_pipeline(n_features: int) -> Pipeline:
    # Numeric pipeline: impute median, scale, then logistic regression
    numeric = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler(with_mean=True, with_std=True)),
        ]
    )

    pre = ColumnTransformer(
        transformers=[
            ("num", numeric, list(range(n_features))),
        ],
        remainder="drop",
    )

    clf = LogisticRegression(max_iter=1000, class_weight="balanced", solver="lbfgs")

    pipe = Pipeline(steps=[("pre", pre), ("clf", clf)])
    return pipe


def _risk_direction(col: str) -> str:
    """Return which direction increases churn risk for this feature.
    Values: 'high', 'low', 'neg' (negative values), 'pos' (positive values).
    """
    high_risk = {
        "recency_days",
        "median_gap_days",
        "p90_gap_days",
        "cv_gap",
        "rev_returns_90d",
        "credit_notes_90d",
        "orders_neg_30d",
        "backorder_qty_30d",
        "credit_notes_prev_month",
        "credit_notes_ma3",
        "threshold_days",
        "is_maintenance_heavy",
        "maint_cycle_days",
        "severity_score",
        "lateness_component",
        "credits_component",
        "trend_component",
    }
    low_risk = {
        "rev_180d",
        "invoices_90d",
        "orders_pos_30d",
        "invoices_pos_prev_month",
        "mitigator_component",
    }
    neg_risk = {"pct_change_3m", "pct_change_6m", "yoy_change_pct"}
    if col in high_risk:
        return "high"
    if col in low_risk:
        return "low"
    if col in neg_risk:
        return "neg"
    return "high"


def _friendly_label(col: str) -> str:
    mapping = {
        "recency_days": "No purchases for",
        "median_gap_days": "Typical gap between purchases",
        "p90_gap_days": "Long purchase gaps (90th percentile)",
        "cv_gap": "Irregular buying cadence",
        "in_renewal_grace": "In renewal grace period",
        "rev_180d": "Revenue in last 180 days",
        "rev_returns_90d": "Returns value in last 90 days",
        "invoices_90d": "Invoices in last 90 days",
        "credit_notes_90d": "Credit notes in last 90 days",
        "orders_pos_30d": "Positive order value in last 30 days",
        "orders_neg_30d": "Negative order value in last 30 days",
        "backorder_qty_30d": "Backorder quantity in last 30 days",
        "pct_change_3m": "Change vs prior 3 months",
        "pct_change_6m": "Change vs prior 6 months",
        "yoy_change_pct": "Year-over-year change",
        "credit_notes_prev_month": "Credit notes last month",
        "invoices_pos_prev_month": "Invoices last month",
        "credit_notes_ma3": "Credit notes per month (3-month average)",
        "threshold_days": "Days past expected purchase threshold",
        "is_maintenance_heavy": "Maintenance-heavy profile",
        "maint_cycle_days": "Maintenance cycle length",
        "severity_score": "Issue severity score",
        "lateness_component": "Late purchase signal",
        "credits_component": "Credits/returns signal",
        "trend_component": "Negative trend signal",
        "mitigator_component": "Mitigating signals",
    }
    return mapping.get(col, col)


def _format_value(col: str, value: float) -> str:
    if value is None or (isinstance(value, float) and (pd.isna(value))):
        return ""
    # Days-like features
    if col.endswith("_days") or col in {"recency_days", "maint_cycle_days", "threshold_days"}:
        try:
            return f"{int(round(float(value)))} days"
        except Exception:
            return ""
    # Counts
    if col in {
        "invoices_90d",
        "backorder_qty_30d",
        "invoices_pos_prev_month",
        "credit_notes_90d",
        "credit_notes_prev_month",
    }:
        try:
            return f"{int(round(float(value))):,}"
        except Exception:
            return ""
    if col == "credit_notes_ma3":
        try:
            return f"{float(value):.2f} per month"
        except Exception:
            return ""
    # Percentages
    if col in {"pct_change_3m", "pct_change_6m", "yoy_change_pct"}:
        try:
            return f"{float(value):+.1f}%"
        except Exception:
            return ""
    # Monetary-like: format with currency symbol and thousands separators
    if col in {"rev_180d", "rev_returns_90d", "orders_pos_30d", "orders_neg_30d"}:
        try:
            v = float(value)
            if v < 0:
                return f"-${abs(v):,.2f}"
            return f"${v:,.2f}"
        except Exception:
            return ""
    # Generic numeric
    try:
        v = float(value)
        if abs(v - round(v)) < 0.5:
            return f"{int(round(v)):,}"
        return f"{v:,.2f}"
    except Exception:
        return ""


def _describe(col: str, value: float, z: float) -> str:
    label = _friendly_label(col)
    direction = _risk_direction(col)
    val_txt = _format_value(col, value)
    if col == "is_maintenance_heavy":
        return "Maintenance‑heavy profile" if bool(value) else ""
    if col == "in_renewal_grace":
        return "In renewal grace period" if bool(value) else ""
    if direction == "neg":
        if value is not None and not (isinstance(value, float) and pd.isna(value)) and float(value) < 0:
            return f"{label} ({val_txt})"
        return ""
    if direction == "high":
        if z <= 0:
            return ""
        # Avoid numeric clutter for model signals
        if col in {"lateness_component", "credits_component", "trend_component", "mitigator_component"}:
            return label
        # Natural phrasing for "No purchases for"
        if label.startswith("No purchases for"):
            return f"{label} {val_txt}".strip()
        return f"{label} ({val_txt})" if val_txt else label
    if direction == "low":
        # For low-risk-direction features, emphasize deficiency
        base = label
        if label.startswith("Invoices"):
            base = "Few invoices in last 90 days"
        if label.startswith("Positive order value"):
            base = "Low positive order value (last 30 days)"
        if label.startswith("Revenue in"):
            base = "Low recent revenue"
        if label.startswith("Mitigating"):
            base = "Few mitigating signals"
        if z >= 0:
            return ""
        if col in {"lateness_component", "credits_component", "trend_component", "mitigator_component"}:
            return base
        return f"{base} ({val_txt})" if val_txt else base
    return f"{label} ({val_txt})"


def _shap_contributions(pipe: Pipeline, X: np.ndarray, feature_cols: List[str]) -> Optional[np.ndarray]:
    """Return SHAP contribution matrix (N x F) in model output space.

    Uses LinearExplainer for logistic regression on standardized features.
    Falls back to None if SHAP is unavailable or errors.
    """
    if not _HAS_SHAP:
        return None
    try:
        pre = pipe.named_steps["pre"]
        clf = pipe.named_steps["clf"]
        Xt = pre.transform(X)
        # Background sample for speed
        bg = Xt
        if Xt.shape[0] > 512:
            rng = np.random.default_rng(42)
            idx = rng.choice(Xt.shape[0], size=512, replace=False)
            bg = Xt[idx]
        # Prefer LinearExplainer for linear models
        try:
            explainer = shap.LinearExplainer(clf, bg, feature_names=feature_cols)
            phi = explainer.shap_values(Xt)
            # Older versions may return list; ensure ndarray
            phi = np.array(phi)
        except Exception:
            # Generic fallback
            explainer = shap.Explainer(clf, bg, feature_names=feature_cols)
            exp = explainer(Xt)
            phi = exp.values
        # Expected shape (N, F)
        if phi.ndim == 1:
            phi = phi.reshape(-1, 1)
        return phi
    except Exception:
        return None


def train_and_predict(df: pd.DataFrame, cfg: MLConfig) -> pd.DataFrame:
    # Ensure feature presence and types
    missing = [c for c in cfg.feature_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required feature columns: {missing}")

    work = _coerce_types(df, cfg.feature_cols)

    X = work[cfg.feature_cols].to_numpy()

    y = None
    if cfg.target_col and cfg.target_col in work.columns:
        # Coerce target to binary 0/1
        y = pd.to_numeric(work[cfg.target_col], errors="coerce").fillna(0).astype(int).to_numpy()

    pipe = build_pipeline(n_features=len(cfg.feature_cols))

    if y is not None:
        pipe.fit(X, y)
        proba = pipe.predict_proba(X)[:, 1]
        pred = (proba >= 0.5).astype(int)
    else:
        # If no target provided, we can't train supervised model; fall back to zeros
        proba = np.zeros(X.shape[0], dtype=float)
        pred = np.zeros(X.shape[0], dtype=int)

    result = pd.DataFrame(
        {
            cfg.customer_id_col: df[cfg.customer_id_col].to_numpy(),
            "predicted_churn_90d": pred,
            "predicted_churn_probability_90d": proba,
        }
    )
    # Add percent probability columns
    result["predicted_churn_probability_90d_pct"] = (
        result["predicted_churn_probability_90d"].astype(float) * 100.0
    ).round(2)
    if cfg.date_col and cfg.date_col in df.columns:
        # Prefer pass-through as string (SQL already returns yyyy-mm-dd)
        result["as_of_date"] = df[cfg.date_col].astype(str).to_numpy()

    # Build human-readable reasons for churned predictions using SHAP if available; fallback to coef method
    reasons = [""] * len(result)
    # Prepare transformed features and SHAP contributions
    pre = pipe.named_steps["pre"]
    Xt = pre.transform(X)
    phi = _shap_contributions(pipe, X, cfg.feature_cols)
    try:
        for i in range(len(result)):
            # Choose contribution vector (SHAP if available; else coef-based)
            if phi is not None:
                contrib = phi[i]
            else:
                coefs = pipe.named_steps["clf"].coef_.ravel()
                contrib = Xt[i] * coefs

            # For churned rows: focus on positive contributions (risk drivers), else fallback to abs
            # For non-churn rows: use top absolute contributions (strongest drivers either way)
            if int(pred[i]) == 1:
                order = np.argsort(contrib)[::-1]
            else:
                order = np.argsort(np.abs(contrib))[::-1]

            phrases = []
            for idx in order:
                if len(phrases) >= 3:
                    break
                if int(pred[i]) == 1 and contrib[idx] <= 0:
                    continue
                col = cfg.feature_cols[idx]
                val = df[col].iloc[i]
                try:
                    z = float(Xt[i, idx])
                except Exception:
                    z = 0.0
                phrase = _describe(col, float(val) if pd.notna(val) else np.nan, z)
                if phrase:
                    phrases.append(phrase)
            reasons[i] = "; ".join(phrases) if phrases else "elevated churn risk across multiple signals"
    except Exception:
        # On any unexpected error, keep reasons blank rather than failing
        pass

    # Predicted reasons column
    result["predicted_churn_reason_90d"] = reasons

    # Actual label-related fields
    if cfg.target_col and cfg.target_col in df.columns:
        actual = pd.to_numeric(df[cfg.target_col], errors="coerce").fillna(0).astype(int).to_numpy()
        result["actual_churned_90d"] = actual
        # Build reasons for actual churned rows (top positive contributors)
        actual_reasons = [""] * len(result)
        try:
            for i in range(len(result)):
                if actual[i] != 1:
                    continue
                if phi is not None:
                    contrib = phi[i]
                else:
                    coefs = pipe.named_steps["clf"].coef_.ravel()
                    contrib = Xt[i] * coefs
                order = np.argsort(contrib)[::-1]
                phrases = []
                for idx in order:
                    if len(phrases) >= 3:
                        break
                    if contrib[idx] <= 0:
                        continue
                    col = cfg.feature_cols[idx]
                    val = df[col].iloc[i]
                    try:
                        z = float(Xt[i, idx])
                    except Exception:
                        z = 0.0
                    phrase = _describe(col, float(val) if pd.notna(val) else np.nan, z)
                    if phrase:
                        phrases.append(phrase)
                actual_reasons[i] = "; ".join(phrases) if phrases else "observed churn within 90 days"
        except Exception:
            pass
        result["actual_churn_reason_90d"] = actual_reasons

    return result

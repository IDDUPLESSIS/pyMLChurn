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


def _temperature_scale(proba: np.ndarray, T: float = 8.0) -> np.ndarray:
    """
    Soften extreme probabilities from a very confident model by dividing
    logits by a temperature T > 1. Larger T => more moderate probabilities.
    """
    eps = 1e-9
    p = np.clip(proba.astype(float), eps, 1.0 - eps)
    logit = np.log(p / (1.0 - p))
    logit_scaled = logit / float(T)
    p_scaled = 1.0 / (1.0 + np.exp(-logit_scaled))
    return p_scaled


def _rule_based_pseudo_label(df: pd.DataFrame) -> Optional[np.ndarray]:
    """
    Build a pseudo-label for churn using current snapshot signals when no
    reliable supervised label is available.

    Rules (any of these imply churn=1):
      - KnownChurn_Effective == 1 (explicitly confirmed churn)
      - ChurnIf_NoOrd90 == 1  (no orders in last 90 days)
      - ChurnIf_NoInv90 == 1  (no invoices in last 90 days)
      - ChurnIf_NoMaintOrd90_WithinGrace == 1 (no maint orders within grace)
      - EoxExpired_NoOrders90d == 1
      - EoxRisk_NoMaintRenewal == 1
      - EoxExpired_UnpaidInv90plus == 1
    """
    if df.empty:
        return None

    y = np.zeros(len(df), dtype=int)

    # Known explicit churn
    if "KnownChurn_Effective" in df.columns:
        known = pd.to_numeric(df["KnownChurn_Effective"], errors="coerce").fillna(0.0)
        y = np.maximum(y, (known >= 1.0).astype(int))

    strong_flags = [
        "ChurnIf_NoOrd90",
        "ChurnIf_NoInv90",
        "ChurnIf_NoMaintOrd90_WithinGrace",
        "EoxExpired_NoOrders90d",
        "EoxRisk_NoMaintRenewal",
        "EoxExpired_UnpaidInv90plus",
    ]
    for col in strong_flags:
        if col not in df.columns:
            continue
        vals = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        y = np.maximum(y, (vals >= 1.0).astype(int))

    # Require at least two classes to be useful
    if np.unique(y).shape[0] < 2:
        return None
    return y


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
        "Recency_Orders_Days": "No orders for",
        "Recency_MaintOrders_Days": "No maintenance orders for",
        "Recency_Invoices_Days": "No invoices for",
        "OrderValue_90d": "Order value in last 90 days",
        "OrderValue_Prev90d": "Order value in previous 90 days",
        "OrderValue_3m_Change": "Change in order value vs prior 90 days",
        "OrderValue_3m_ChangePct": "Order value change vs prior 90 days",
        "OrderFreq_90d": "Orders in last 90 days",
        "OrderFreq_Prev90d": "Orders in previous 90 days",
        "OrderFreq_3m_Change": "Change in order frequency vs prior 90 days",
        "OrderFreq_3m_ChangePct": "Order frequency change vs prior 90 days",
        "MaintOrderValue_90d": "Maintenance order value in last 90 days",
        "ProdOrderValue_90d": "Product order value in last 90 days",
        "ProdRatio_90d": "Share of product revenue",
        "MaintRatio_90d": "Share of maintenance revenue",
        "IsProductHeavy_90d": "Product-heavy buying profile",
        "IsMaintHeavy_90d": "Maintenance-heavy buying profile",
        "BackorderCount_180dPlus": "Old backorders (180+ days)",
        "UnpaidInv_OverTerms_Count": "Invoices over terms",
        "UnpaidInv_90plus_Count": "Invoices 90+ days overdue",
        "ARBucket_Current_Count": "Invoices current",
        "ARBucket_1_30_Count": "Invoices 1-30 days overdue",
        "ARBucket_31_60_Count": "Invoices 31-60 days overdue",
        "ARBucket_61_90_Count": "Invoices 61-90 days overdue",
        "ARBucket_91_120_Count": "Invoices 91-120 days overdue",
        "ARBucket_121_180_Count": "Invoices 121-180 days overdue",
        "ARBucket_180Plus_Count": "Invoices 180+ days overdue",
        "DSO_OpenInvoices": "Days sales outstanding (open invoices)",
        "ReturnInvCount_90d": "Return invoices in last 90 days",
        "CreditValue_90d": "Credit value in last 90 days",
        "CreditCount_90d": "Credit notes in last 90 days",
        "ChurnIf_NoOrd90": "No orders in last 90 days",
        "ChurnIf_NoInv90": "No invoices in last 90 days",
        "ChurnIf_NoMaintOrd90_WithinGrace": "No maintenance orders within grace period",
        "PredChurn_Unpaid90plus": "Unpaid invoices 90+ days",
        "PredChurn_HighBackorders": "High backorders",
        "Eox_MinDaysToLDOS": "Days to least days-of-support",
        "Risk_EoX_12m": "End-of-support risk in next 12 months",
        "Risk_EoX_6m": "End-of-support risk in next 6 months",
        "Risk_EoX_3m": "End-of-support risk in next 3 months",
        "PredChurn_EoXExpired": "End-of-support already expired",
        "Eox_Rev_Expired_12m": "Expired revenue in last 12 months",
        "Eox_Rev_Risk12m": "At-risk revenue in next 12 months",
        "Eox_Rev_Total12m": "Total EoX-related revenue (12 months)",
        "Eox_RevExpiredPct_12m": "Share of revenue from expired items",
        "Eox_RevRiskPct_12m": "Share of revenue at EoX risk",
        "Eox_HasExpiredRevenue_12m": "Has expired-revenue items",
        "Eox_HasRiskRevenue_12m": "Has at-risk revenue items",
        "Eox_MajorityExpired_12m": "Most revenue on expired items",
        "Eox_SKU_CountExpired": "Expired SKUs count",
        "Eox_SKU_CountRisk12m": "At-risk SKUs count (12 months)",
        "EoxExpired_NoOrders90d": "No orders in 90 days on expired SKUs",
        "EoxRisk_NoMaintRenewal": "No maintenance renewal on at-risk SKUs",
        "EoxExpired_UnpaidInv90plus": "Unpaid invoices 90+ days on expired SKUs",
        "EoxExpired_CreditSpike90d": "Credit spike on expired SKUs (90 days)",
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

    # Handle cases where we cannot train a supervised model from the DB label:
    # fall back to rule-based pseudo labels; only if that also fails do we use
    # a simple heuristic risk score.
    if y is None or np.unique(y).shape[0] < 2:
        pseudo = _rule_based_pseudo_label(df)
        if pseudo is not None and np.unique(pseudo).shape[0] >= 2:
            y = pseudo
        else:
            # Fallback: rule-based risk scoring using engineered churn flags
            flag_candidates = [
                "ChurnIf_NoOrd90",
                "ChurnIf_NoInv90",
                "ChurnIf_NoMaintOrd90_WithinGrace",
                "PredChurn_Unpaid90plus",
                "PredChurn_HighBackorders",
            ]
            available_flags = [c for c in flag_candidates if c in df.columns]

            if available_flags:
                flags = df[available_flags].apply(pd.to_numeric, errors="coerce").fillna(0.0)
                for c in available_flags:
                    flags[c] = flags[c].clip(0, 1)
                risk_score = flags.mean(axis=1)
                proba = risk_score.to_numpy(dtype=float)
                pred = (proba >= 0.5).astype(int)

                reasons = []
                for i in range(len(df)):
                    phrases = []
                    for col in available_flags:
                        try:
                            if float(flags.iloc[i][col]) >= 0.5:
                                label = _friendly_label(col)
                                if label:
                                    phrases.append(label)
                        except Exception:
                            continue
                    reasons.append("; ".join(phrases) if phrases else "")
            else:
                # If no rule flags are present, fall back to zeros
                proba = np.zeros(X.shape[0], dtype=float)
                pred = np.zeros(X.shape[0], dtype=int)
                reasons = [""] * len(df)

            result = pd.DataFrame(
                {
                    cfg.customer_id_col: df[cfg.customer_id_col].to_numpy(),
                    "predicted_churn_90d": pred,
                    "predicted_churn_probability_90d": proba,
                }
            )
            result["predicted_churn_probability_90d_pct"] = (
                result["predicted_churn_probability_90d"].astype(float) * 100.0
            ).round(2)
            if cfg.date_col and cfg.date_col in df.columns:
                result["as_of_date"] = df[cfg.date_col].astype(str).to_numpy()

            # Rule-based reasons in this fallback path
            result["predicted_churn_reason_90d"] = reasons

            if cfg.target_col and cfg.target_col in df.columns:
                actual = pd.to_numeric(df[cfg.target_col], errors="coerce").fillna(0).astype(int).to_numpy()
                result["actual_churned_90d"] = actual
                result["actual_churn_reason_90d"] = [""] * len(result)

            return result

    # At this point y is guaranteed to be non-None with at least two classes
    if y is None or np.unique(y).shape[0] < 2:
        # Safety net; should be rare
        # Fallback: rule-based risk scoring using engineered churn flags
        flag_candidates = [
            "ChurnIf_NoOrd90",
            "ChurnIf_NoInv90",
            "ChurnIf_NoMaintOrd90_WithinGrace",
            "PredChurn_Unpaid90plus",
            "PredChurn_HighBackorders",
        ]
        available_flags = [c for c in flag_candidates if c in df.columns]

        if available_flags:
            flags = df[available_flags].apply(pd.to_numeric, errors="coerce").fillna(0.0)
            for c in available_flags:
                flags[c] = flags[c].clip(0, 1)
            risk_score = flags.mean(axis=1)
            proba = risk_score.to_numpy(dtype=float)
            pred = (proba >= 0.5).astype(int)

            reasons = []
            for i in range(len(df)):
                phrases = []
                for col in available_flags:
                    try:
                        if float(flags.iloc[i][col]) >= 0.5:
                            label = _friendly_label(col)
                            if label:
                                phrases.append(label)
                    except Exception:
                        continue
                reasons.append("; ".join(phrases) if phrases else "")
        else:
            # If no rule flags are present, fall back to zeros
            proba = np.zeros(X.shape[0], dtype=float)
            pred = np.zeros(X.shape[0], dtype=int)
            reasons = [""] * len(df)

        result = pd.DataFrame(
            {
                cfg.customer_id_col: df[cfg.customer_id_col].to_numpy(),
                "predicted_churn_90d": pred,
                "predicted_churn_probability_90d": proba,
            }
        )
        result["predicted_churn_probability_90d_pct"] = (
            result["predicted_churn_probability_90d"].astype(float) * 100.0
        ).round(2)
        if cfg.date_col and cfg.date_col in df.columns:
            result["as_of_date"] = df[cfg.date_col].astype(str).to_numpy()

        # Rule-based reasons in this fallback path
        result["predicted_churn_reason_90d"] = reasons

        if cfg.target_col and cfg.target_col in df.columns:
            actual = pd.to_numeric(df[cfg.target_col], errors="coerce").fillna(0).astype(int).to_numpy()
            result["actual_churned_90d"] = actual
            result["actual_churn_reason_90d"] = [""] * len(result)

        return result

    pipe = build_pipeline(n_features=len(cfg.feature_cols))
    pipe.fit(X, y)
    proba_raw = pipe.predict_proba(X)[:, 1]
    # Apply temperature scaling to avoid extreme 0/1 probabilities when
    # training from rule-based or noisy labels.
    proba = _temperature_scale(proba_raw, T=8.0)
    pred = (proba >= 0.5).astype(int)

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

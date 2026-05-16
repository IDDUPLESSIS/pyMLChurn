from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
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


@dataclass
class MLRunOptions:
    allow_pseudo_fallback: bool = False
    min_labeled_rows: int = 500
    min_positive_rows: int = 25
    temporal_test_months: int = 2
    temporal_validation_months: int = 2
    excluded_direct_rule_features: bool = False
    excluded_feature_names: List[str] = field(default_factory=list)


class MLWorkflowError(ValueError):
    pass


def _require_columns(df: pd.DataFrame, required: List[str], frame_name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise MLWorkflowError(f"{frame_name}: missing required columns: {missing}")


def _coerce_features(df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for col in feature_cols:
        if col not in out.columns:
            continue
        if out[col].dtype == bool:
            out[col] = out[col].astype(int)
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _parse_snapshot_dates(df: pd.DataFrame, date_col: str, frame_name: str) -> pd.Series:
    if date_col not in df.columns:
        raise MLWorkflowError(f"{frame_name}: required date column '{date_col}' is missing.")
    raw = df[date_col]
    parsed = pd.to_datetime(raw, errors="coerce")
    bad_mask = raw.notna() & parsed.isna()
    if bool(bad_mask.any()):
        sample = raw[bad_mask].astype(str).head(5).tolist()
        raise MLWorkflowError(
            f"{frame_name}: date column '{date_col}' contains malformed values; sample={sample}."
        )
    if not bool(parsed.notna().any()):
        raise MLWorkflowError(f"{frame_name}: no valid dates found in '{date_col}'.")
    return parsed


def build_pipeline(n_features: int, class_weight: Optional[str] = "balanced") -> Pipeline:
    numeric = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler(with_mean=True, with_std=True)),
        ]
    )
    pre = ColumnTransformer(
        transformers=[("num", numeric, list(range(n_features)))],
        remainder="drop",
    )
    clf = LogisticRegression(max_iter=2000, class_weight=class_weight, solver="lbfgs")
    return Pipeline(steps=[("pre", pre), ("clf", clf)])


def _rule_based_pseudo_label(df: pd.DataFrame) -> Optional[np.ndarray]:
    if df.empty:
        return None
    y = np.zeros(len(df), dtype=int)

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

    if np.unique(y).shape[0] < 2:
        return None
    return y


def _split_temporal_periods(
    dates: pd.Series,
    validation_months: int,
    test_months: int,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, Dict[str, Any]]:
    if validation_months < 1 or test_months < 1:
        raise MLWorkflowError("temporal_validation_months and temporal_test_months must both be >= 1.")

    periods = dates.dt.to_period("M")
    uniq = sorted(periods.dropna().unique())
    needed = validation_months + test_months + 1
    if len(uniq) < needed:
        raise MLWorkflowError(
            "Not enough monthly periods for temporal split. "
            f"Need at least {needed} distinct months, found {len(uniq)}."
        )

    split_train_end = len(uniq) - (validation_months + test_months)
    split_val_end = len(uniq) - test_months

    train_periods = set(uniq[:split_train_end])
    val_periods = set(uniq[split_train_end:split_val_end])
    test_periods = set(uniq[split_val_end:])

    train_mask = periods.isin(train_periods).to_numpy()
    val_mask = periods.isin(val_periods).to_numpy()
    test_mask = periods.isin(test_periods).to_numpy()

    meta = {
        "train_periods": [str(p) for p in sorted(train_periods)],
        "validation_periods": [str(p) for p in sorted(val_periods)],
        "test_periods": [str(p) for p in sorted(test_periods)],
    }
    return train_mask, val_mask, test_mask, meta


def _ensure_binary_split(name: str, y: np.ndarray) -> None:
    if y.shape[0] == 0:
        raise MLWorkflowError(f"Temporal split produced zero rows for {name} set.")
    uniq = np.unique(y)
    if uniq.shape[0] < 2:
        raise MLWorkflowError(
            f"{name} set contains only one class ({uniq.tolist()}). "
            "Adjust cutoff/split settings or label horizon to include both classes."
        )


def _binary_metrics_from_predictions(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, Any]:
    y_true = y_true.astype(int)
    y_pred = y_pred.astype(int)
    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    tn, fp, fn, tp = (int(cm[0, 0]), int(cm[0, 1]), int(cm[1, 0]), int(cm[1, 1]))

    return {
        "row_count": int(len(y_true)),
        "positive_count": int((y_true == 1).sum()),
        "class_prevalence": float(np.mean(y_true == 1)),
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "predicted_positive_rate": float(np.mean(y_pred == 1)),
    }


def _top_k_metrics(y_true: np.ndarray, y_prob: np.ndarray, frac: float) -> Dict[str, Optional[float]]:
    n = len(y_true)
    if n == 0:
        return {"precision": None, "recall": None, "lift": None}

    k = max(1, int(np.ceil(n * frac)))
    order = np.argsort(-y_prob)
    top_idx = order[:k]
    y_top = y_true[top_idx]

    positives_total = int((y_true == 1).sum())
    precision = float(y_top.mean()) if k > 0 else None
    recall = (float(y_top.sum()) / float(positives_total)) if positives_total > 0 else None
    baseline = float(positives_total) / float(n) if n > 0 else None
    lift = (precision / baseline) if (precision is not None and baseline and baseline > 0.0) else None

    return {"precision": precision, "recall": recall, "lift": lift}


def _calibration_by_decile(y_true: np.ndarray, y_prob: np.ndarray) -> pd.DataFrame:
    n = len(y_true)
    if n == 0:
        return pd.DataFrame(columns=["decile", "row_count", "avg_predicted_probability", "actual_rate"])

    bins = min(10, n)
    ranked = np.argsort(-y_prob)
    deciles = np.zeros(n, dtype=int)
    deciles[ranked] = np.floor(np.arange(n) * bins / n).astype(int) + 1

    d = pd.DataFrame(
        {
            "decile": deciles,
            "y_true": y_true.astype(int),
            "y_prob": y_prob.astype(float),
        }
    )

    out = (
        d.groupby("decile", as_index=False)
        .agg(
            row_count=("y_true", "size"),
            avg_predicted_probability=("y_prob", "mean"),
            actual_rate=("y_true", "mean"),
        )
        .sort_values("decile")
        .reset_index(drop=True)
    )
    return out


def _binary_metrics(y_true: np.ndarray, y_prob: np.ndarray, threshold: float = 0.5) -> Dict[str, Any]:
    y_prob = np.clip(y_prob.astype(float), 1e-9, 1.0 - 1e-9)
    y_pred = (y_prob >= float(threshold)).astype(int)

    base = _binary_metrics_from_predictions(y_true, y_pred)
    top10 = _top_k_metrics(y_true, y_prob, frac=0.10)
    top5 = _top_k_metrics(y_true, y_prob, frac=0.05)

    base.update(
        {
            "threshold": float(threshold),
            "roc_auc": float(roc_auc_score(y_true, y_prob)),
            "pr_auc": float(average_precision_score(y_true, y_prob)),
            "average_precision": float(average_precision_score(y_true, y_prob)),
            "brier_score": float(brier_score_loss(y_true, y_prob)),
            "log_loss": float(log_loss(y_true, y_prob, labels=[0, 1])),
            "avg_predicted_probability": float(np.mean(y_prob)),
            "p90_predicted_probability": float(np.quantile(y_prob, 0.90)),
            "precision_at_top10pct": top10["precision"],
            "recall_at_top10pct": top10["recall"],
            "lift_at_top10pct": top10["lift"],
            "precision_at_top5pct": top5["precision"],
            "recall_at_top5pct": top5["recall"],
            "lift_at_top5pct": top5["lift"],
        }
    )
    return base


def _evaluate_thresholds(y_true: np.ndarray, y_prob: np.ndarray, thresholds: np.ndarray) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for t in thresholds:
        y_pred = (y_prob >= float(t)).astype(int)
        m = _binary_metrics_from_predictions(y_true, y_pred)
        rows.append(
            {
                "threshold": float(t),
                "precision": m["precision"],
                "recall": m["recall"],
                "f1": m["f1"],
                "predicted_positive_rate": m["predicted_positive_rate"],
                "tp": m["tp"],
                "tn": m["tn"],
                "fp": m["fp"],
                "fn": m["fn"],
            }
        )
    return pd.DataFrame(rows)


def _select_best_threshold(threshold_df: pd.DataFrame) -> Tuple[float, Dict[str, Any]]:
    ranked = threshold_df.sort_values(
        ["f1", "precision", "predicted_positive_rate", "threshold"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)
    best = ranked.iloc[0].to_dict()
    return float(best["threshold"]), best


def _expected_calibration_error(y_true: np.ndarray, y_prob: np.ndarray, n_bins: int = 10) -> float:
    y_prob = np.clip(y_prob.astype(float), 0.0, 1.0)
    y_true = y_true.astype(int)
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    total = float(len(y_true))
    ece = 0.0
    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        if i == n_bins - 1:
            mask = (y_prob >= lo) & (y_prob <= hi)
        else:
            mask = (y_prob >= lo) & (y_prob < hi)
        if not bool(mask.any()):
            continue
        frac = float(mask.sum()) / total
        conf = float(np.mean(y_prob[mask]))
        acc = float(np.mean(y_true[mask]))
        ece += frac * abs(acc - conf)
    return float(ece)


def _risk_direction(col: str) -> str:
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
        "ARBucket_Current_Count",
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
    }
    return mapping.get(col, col)


def _format_value(col: str, value: float) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""

    days_like = {
        "recency_days",
        "maint_cycle_days",
        "threshold_days",
        "Recency_Orders_Days",
        "Recency_MaintOrders_Days",
        "Recency_Invoices_Days",
        "Eox_MinDaysToLDOS",
    }
    if col.lower().endswith("_days") or col in days_like:
        try:
            return f"{int(round(float(value)))} days"
        except Exception:
            return ""

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

    if col in {"recency_days", "Recency_Orders_Days"}:
        return f"No orders have been placed for {val_txt}." if val_txt else "Order recency data is unavailable."
    if col == "Recency_MaintOrders_Days":
        return f"No maintenance orders have been placed for {val_txt}." if val_txt else "Maintenance order recency data is unavailable."
    if col == "Recency_Invoices_Days":
        return f"No invoices have been issued for {val_txt}." if val_txt else "Invoice recency data is unavailable."

    if direction == "neg":
        if value is not None and not (isinstance(value, float) and pd.isna(value)) and float(value) < 0:
            return f"{label} is {val_txt}." if val_txt else f"{label} is negative."
        return ""

    if direction == "high" and z > 0:
        return f"{label} is {val_txt}." if val_txt else f"{label} is elevated."

    if direction == "low" and z < 0:
        return f"{label} is lower than baseline ({val_txt})." if val_txt else f"{label} is lower than baseline."

    return ""


def _compose_reason_sentence(phrases: List[str], default_text: str) -> str:
    cleaned: List[str] = []
    for p in phrases:
        s = (p or "").strip()
        if not s:
            continue
        if s.endswith("."):
            s = s[:-1]
        cleaned.append(s)

    if not cleaned:
        return default_text
    return "Risk signals include " + "; ".join(cleaned) + "."


def _shap_contributions(pipe: Pipeline, X: np.ndarray, feature_cols: List[str]) -> Optional[np.ndarray]:
    if not _HAS_SHAP:
        return None
    try:
        pre = pipe.named_steps["pre"]
        clf = pipe.named_steps["clf"]
        Xt = pre.transform(X)
        bg = Xt
        if Xt.shape[0] > 512:
            rng = np.random.default_rng(42)
            idx = rng.choice(Xt.shape[0], size=512, replace=False)
            bg = Xt[idx]
        try:
            explainer = shap.LinearExplainer(clf, bg, feature_names=feature_cols)
            phi = explainer.shap_values(Xt)
            phi = np.array(phi)
        except Exception:
            explainer = shap.Explainer(clf, bg, feature_names=feature_cols)
            exp = explainer(Xt)
            phi = exp.values
        if phi.ndim == 1:
            phi = phi.reshape(-1, 1)
        return phi
    except Exception:
        return None


def _build_reasons(df: pd.DataFrame, cfg: MLConfig, pipe: Pipeline, pred: np.ndarray) -> List[str]:
    reasons = [""] * len(df)
    if len(df) == 0:
        return reasons

    X = _coerce_features(df, cfg.feature_cols)[cfg.feature_cols].to_numpy()
    pre = pipe.named_steps["pre"]
    Xt = pre.transform(X)
    phi = _shap_contributions(pipe, X, cfg.feature_cols)
    coefs = pipe.named_steps["clf"].coef_.ravel()

    for i in range(len(df)):
        contrib = phi[i] if phi is not None else Xt[i] * coefs
        order = np.argsort(contrib)[::-1] if int(pred[i]) == 1 else np.argsort(np.abs(contrib))[::-1]

        phrases: List[str] = []
        for idx in order:
            if len(phrases) >= 3:
                break
            if int(pred[i]) == 1 and contrib[idx] <= 0:
                continue
            col = cfg.feature_cols[idx]
            val = df[col].iloc[i] if col in df.columns else np.nan
            try:
                z = float(Xt[i, idx])
            except Exception:
                z = 0.0
            phrase = _describe(col, float(val) if pd.notna(val) else np.nan, z)
            if phrase:
                phrases.append(phrase)

        reasons[i] = _compose_reason_sentence(phrases, "Churn risk is elevated across multiple signals.")

    return reasons


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
        if np.isnan(v):
            return None
        return v
    except Exception:
        return None


def _split_summary(name: str, df_part: pd.DataFrame, y: np.ndarray, date_col: str) -> Dict[str, Any]:
    date_series = pd.to_datetime(df_part[date_col], errors="coerce")
    dmin = date_series.min()
    dmax = date_series.max()
    return {
        "name": name,
        "rows": int(len(df_part)),
        "positives": int((y == 1).sum()),
        "positive_rate": float(np.mean(y == 1)),
        "min_date": dmin.strftime("%Y-%m-%d") if pd.notna(dmin) else None,
        "max_date": dmax.strftime("%Y-%m-%d") if pd.notna(dmax) else None,
    }


def train_evaluate_and_score(
    train_df: pd.DataFrame,
    score_df: pd.DataFrame,
    cfg: MLConfig,
    options: MLRunOptions,
) -> Tuple[pd.DataFrame, Dict[str, Any], Dict[str, pd.DataFrame]]:
    if cfg.target_col is None:
        raise MLWorkflowError("target_col is required for supervised training.")
    if cfg.date_col is None:
        raise MLWorkflowError("date_col is required for temporal splitting.")

    _require_columns(train_df, [cfg.customer_id_col, cfg.date_col] + cfg.feature_cols, "training dataframe")
    _require_columns(score_df, [cfg.customer_id_col, cfg.date_col] + cfg.feature_cols, "scoring dataframe")

    train_work = _coerce_features(train_df, cfg.feature_cols).copy()
    score_work = _coerce_features(score_df, cfg.feature_cols).copy()

    train_dates = _parse_snapshot_dates(train_work, cfg.date_col, "training dataframe")
    score_dates = _parse_snapshot_dates(score_work, cfg.date_col, "scoring dataframe")
    train_work[cfg.date_col] = train_dates
    score_work[cfg.date_col] = score_dates

    if cfg.target_col not in train_work.columns:
        raise MLWorkflowError(
            f"training dataframe: required target column '{cfg.target_col}' is missing for supervised training."
        )

    y_raw = pd.to_numeric(train_work[cfg.target_col], errors="coerce")
    labeled_mask = y_raw.notna()
    labeled_rows = int(labeled_mask.sum())

    training_mode = "supervised_only"
    train_label_source = "target"

    if labeled_rows < options.min_labeled_rows:
        if not options.allow_pseudo_fallback:
            raise MLWorkflowError(
                "Insufficient labeled rows for supervised training. "
                f"Found {labeled_rows}, required at least {options.min_labeled_rows}. "
                "Either relax thresholds or pass --allow-pseudo-fallback explicitly."
            )

    if labeled_rows >= options.min_labeled_rows:
        work = train_work[labeled_mask].copy().reset_index(drop=True)
        y_series = y_raw[labeled_mask].astype(int).reset_index(drop=True)
    else:
        pseudo = _rule_based_pseudo_label(train_work)
        if pseudo is None:
            raise MLWorkflowError(
                "Pseudo fallback was requested, but pseudo labels could not be generated with two classes."
            )
        work = train_work.copy().reset_index(drop=True)
        y_series = pd.Series(pseudo.astype(int))
        training_mode = "pseudo_fallback"
        train_label_source = "pseudo"

    bad_targets = (~y_series.isin([0, 1])).sum()
    if int(bad_targets) > 0:
        raise MLWorkflowError(
            f"Training labels contain non-binary values; found {int(bad_targets)} rows outside [0,1]."
        )

    positives_total = int((y_series == 1).sum())
    if positives_total < options.min_positive_rows:
        raise MLWorkflowError(
            "Insufficient positive rows for robust training. "
            f"Found {positives_total}, required at least {options.min_positive_rows}."
        )

    work["_target_y"] = y_series.to_numpy(dtype=int)
    work = work.sort_values(cfg.date_col).reset_index(drop=True)
    y = work["_target_y"].to_numpy(dtype=int)
    work = work.drop(columns=["_target_y"])

    train_mask, val_mask, test_mask, split_meta = _split_temporal_periods(
        dates=work[cfg.date_col],
        validation_months=options.temporal_validation_months,
        test_months=options.temporal_test_months,
    )

    df_train = work.loc[train_mask].reset_index(drop=True)
    df_val = work.loc[val_mask].reset_index(drop=True)
    df_test = work.loc[test_mask].reset_index(drop=True)

    y_train = y[train_mask]
    y_val = y[val_mask]
    y_test = y[test_mask]

    _ensure_binary_split("train", y_train)
    _ensure_binary_split("validation", y_val)
    _ensure_binary_split("test", y_test)

    x_train = df_train[cfg.feature_cols].to_numpy()
    x_val = df_val[cfg.feature_cols].to_numpy()
    x_train_val = work.loc[train_mask | val_mask, cfg.feature_cols].to_numpy()
    y_train_val = y[train_mask | val_mask]
    x_test = df_test[cfg.feature_cols].to_numpy()

    # Validation-driven model selection over class-weighting strategy.
    class_weight_candidates: List[Optional[str]] = [None, "balanced"]
    class_weight_rows: List[Dict[str, Any]] = []
    val_prob_by_weight: Dict[str, np.ndarray] = {}

    for cw in class_weight_candidates:
        label = "none" if cw is None else "balanced"
        model = build_pipeline(n_features=len(cfg.feature_cols), class_weight=cw)
        model.fit(x_train, y_train)
        val_prob = model.predict_proba(x_val)[:, 1]
        val_metrics_05 = _binary_metrics(y_val, val_prob, threshold=0.5)

        class_weight_rows.append(
            {
                "class_weight": label,
                "validation_pr_auc": val_metrics_05["pr_auc"],
                "validation_roc_auc": val_metrics_05["roc_auc"],
                "validation_f1_at_0_5": val_metrics_05["f1"],
            }
        )
        val_prob_by_weight[label] = val_prob

    class_weight_df = pd.DataFrame(class_weight_rows)
    class_weight_ranked = class_weight_df.sort_values(
        ["validation_pr_auc", "validation_roc_auc", "class_weight"],
        ascending=[False, False, True],
    ).reset_index(drop=True)
    selected_class_weight_label = str(class_weight_ranked.iloc[0]["class_weight"])
    selected_class_weight = None if selected_class_weight_label == "none" else "balanced"

    # Tune operating threshold on validation probabilities (binary flag only).
    selected_val_prob = val_prob_by_weight[selected_class_weight_label]
    threshold_grid = np.round(np.arange(0.05, 0.951, 0.05), 2)
    threshold_df = _evaluate_thresholds(y_val, selected_val_prob, threshold_grid)
    selected_threshold, selected_threshold_row = _select_best_threshold(threshold_df)
    threshold_df["selected"] = threshold_df["threshold"].round(6) == round(selected_threshold, 6)

    validation_metrics = _binary_metrics(y_val, selected_val_prob, threshold=selected_threshold)

    model_final = build_pipeline(n_features=len(cfg.feature_cols), class_weight=selected_class_weight)
    model_final.fit(x_train_val, y_train_val)
    test_prob = model_final.predict_proba(x_test)[:, 1]
    holdout_metrics = _binary_metrics(y_test, test_prob, threshold=selected_threshold)

    # Holdout business-rule baseline for side-by-side benchmark vs ML model.
    baseline_required = ["ChurnIf_NoOrd90", "MaintContractActive"]
    _require_columns(df_test, baseline_required, "temporal holdout baseline")
    churn_noord = pd.to_numeric(df_test["ChurnIf_NoOrd90"], errors="coerce").fillna(0).astype(int)
    maint_active = pd.to_numeric(df_test["MaintContractActive"], errors="coerce").fillna(0).astype(int)
    baseline_pred = ((churn_noord == 1) & (maint_active == 0)).astype(int).to_numpy()
    business_rule_metrics = _binary_metrics_from_predictions(y_test, baseline_pred)

    calibration_df = _calibration_by_decile(y_test, test_prob)
    calibration_summary = {
        "avg_predicted_probability": _to_float(np.mean(test_prob)) if len(test_prob) > 0 else None,
        "actual_positive_rate": _to_float(np.mean(y_test == 1)) if len(y_test) > 0 else None,
        "calibration_gap": _to_float(np.mean(test_prob) - np.mean(y_test == 1)) if len(test_prob) > 0 else None,
        "expected_calibration_error": _to_float(_expected_calibration_error(y_test, test_prob, n_bins=10)),
    }

    coef = model_final.named_steps["clf"].coef_.ravel()
    coef_df = pd.DataFrame(
        {
            "feature": cfg.feature_cols,
            "coefficient_standardized": coef.astype(float),
        }
    )
    coef_df["abs_coefficient"] = coef_df["coefficient_standardized"].abs()
    coef_df = coef_df.sort_values("coefficient_standardized", ascending=False).reset_index(drop=True)

    odds_df = coef_df[["feature", "coefficient_standardized"]].copy()
    odds_df["odds_ratio"] = np.exp(odds_df["coefficient_standardized"])
    odds_df = odds_df.sort_values("odds_ratio", ascending=False).reset_index(drop=True)

    top_pos = coef_df.sort_values("coefficient_standardized", ascending=False).head(10)
    top_neg = coef_df.sort_values("coefficient_standardized", ascending=True).head(10)

    x_score = score_work[cfg.feature_cols].to_numpy()
    score_prob = model_final.predict_proba(x_score)[:, 1]
    score_pred = (score_prob >= selected_threshold).astype(int)
    score_reasons = _build_reasons(score_work, cfg, model_final, score_pred)

    scored = pd.DataFrame(
        {
            cfg.customer_id_col: score_work[cfg.customer_id_col].to_numpy(),
            "predicted_churn_90d": score_pred.astype(int),
            "predicted_churn_probability_90d": score_prob.astype(float),
        }
    )
    scored["predicted_churn_probability_90d_pct"] = (scored["predicted_churn_probability_90d"] * 100.0).round(2)
    scored["predicted_churn_reason_90d"] = score_reasons
    scored["as_of_date"] = score_work[cfg.date_col].dt.strftime("%Y-%m-%d")

    if cfg.target_col in score_work.columns:
        actual_score = pd.to_numeric(score_work[cfg.target_col], errors="coerce")
        scored["actual_churned_90d"] = actual_score.to_numpy()

    production_summary = {
        "row_count": int(len(scored)),
        "predicted_positives": int((score_pred == 1).sum()),
        "predicted_positive_rate": float(np.mean(score_pred == 1)) if len(score_pred) > 0 else None,
        "avg_predicted_probability": float(np.mean(score_prob)) if len(score_prob) > 0 else None,
        "p90_predicted_probability": float(np.quantile(score_prob, 0.90)) if len(score_prob) > 0 else None,
    }

    split_train_summary = _split_summary("train", df_train, y_train, cfg.date_col)
    split_val_summary = _split_summary("validation", df_val, y_val, cfg.date_col)
    split_test_summary = _split_summary("test", df_test, y_test, cfg.date_col)

    class_weight_summary = {
        row["class_weight"]: {
            "validation_pr_auc": row["validation_pr_auc"],
            "validation_roc_auc": row["validation_roc_auc"],
            "validation_f1_at_0_5": row["validation_f1_at_0_5"],
        }
        for row in class_weight_rows
    }

    metrics: Dict[str, Any] = {
        "model_family": "Logistic Regression",
        "training_mode": training_mode,
        "model_mode": "logistic_regression" if training_mode == "supervised_only" else "logistic_regression_pseudo_fallback",
        "train_label_source": train_label_source,
        "target_col": cfg.target_col,
        "evaluation_population": "labeled_temporal_holdout" if training_mode == "supervised_only" else "pseudo_temporal_holdout",
        "metrics_available": True,
        "metric_status": "ok",
        "selected_class_weight": selected_class_weight_label,
        "selected_threshold": selected_threshold,
        "feature_count_used": int(len(cfg.feature_cols)),
        "excluded_direct_rule_features": bool(options.excluded_direct_rule_features),
        "excluded_feature_names": list(options.excluded_feature_names),
        "temporal_split": {
            "train": split_train_summary,
            "validation": split_val_summary,
            "test": split_test_summary,
            "train_periods": split_meta["train_periods"],
            "validation_periods": split_meta["validation_periods"],
            "test_periods": split_meta["test_periods"],
        },
        "model_selection": {
            "selected_class_weight": selected_class_weight_label,
            "selected_threshold": selected_threshold,
            "threshold_selection": selected_threshold_row,
            "class_weight_validation_comparison": class_weight_summary,
        },
        "validation_metrics": validation_metrics,
        "holdout_metrics": holdout_metrics,
        "business_rule_holdout_metrics": business_rule_metrics,
        "calibration_summary": calibration_summary,
        "production_scoring_summary": production_summary,
        "explainability": {
            "top_positive_features": top_pos[["feature", "coefficient_standardized"]].to_dict("records"),
            "top_negative_features": top_neg[["feature", "coefficient_standardized"]].to_dict("records"),
        },
        "validation_pr_auc_none": class_weight_summary.get("none", {}).get("validation_pr_auc"),
        "validation_roc_auc_none": class_weight_summary.get("none", {}).get("validation_roc_auc"),
        "validation_pr_auc_balanced": class_weight_summary.get("balanced", {}).get("validation_pr_auc"),
        "validation_roc_auc_balanced": class_weight_summary.get("balanced", {}).get("validation_roc_auc"),
        "business_rule_accuracy": business_rule_metrics.get("accuracy"),
        "business_rule_precision": business_rule_metrics.get("precision"),
        "business_rule_recall": business_rule_metrics.get("recall"),
        "business_rule_f1": business_rule_metrics.get("f1"),
        "business_rule_tp": business_rule_metrics.get("tp"),
        "business_rule_tn": business_rule_metrics.get("tn"),
        "business_rule_fp": business_rule_metrics.get("fp"),
        "business_rule_fn": business_rule_metrics.get("fn"),
        "business_rule_predicted_positive_rate": business_rule_metrics.get("predicted_positive_rate"),
        "expected_calibration_error": calibration_summary.get("expected_calibration_error"),
        "actual_positives": holdout_metrics.get("positive_count"),
        "predicted_positives": holdout_metrics.get("tp", 0) + holdout_metrics.get("fp", 0),
        "n_rows": holdout_metrics.get("row_count"),
        "accuracy": holdout_metrics.get("accuracy"),
        "precision": holdout_metrics.get("precision"),
        "recall": holdout_metrics.get("recall"),
        "f1": holdout_metrics.get("f1"),
        "roc_auc": holdout_metrics.get("roc_auc"),
        "brier_score": holdout_metrics.get("brier_score"),
        "tp": holdout_metrics.get("tp"),
        "tn": holdout_metrics.get("tn"),
        "fp": holdout_metrics.get("fp"),
        "fn": holdout_metrics.get("fn"),
    }

    artifacts: Dict[str, pd.DataFrame] = {
        "holdout_metrics_summary": pd.DataFrame([holdout_metrics]),
        "validation_metrics_summary": pd.DataFrame([validation_metrics]),
        "calibration_deciles": calibration_df,
        "coefficients_standardized": coef_df,
        "odds_ratios": odds_df,
        "threshold_diagnostics": threshold_df,
        "class_weight_comparison": class_weight_df,
        "business_rule_holdout_metrics": pd.DataFrame([business_rule_metrics]),
    }

    scored.attrs["model_metrics"] = metrics
    return scored, metrics, artifacts


def train_and_predict(df: pd.DataFrame, cfg: MLConfig) -> pd.DataFrame:
    raise MLWorkflowError(
        "train_and_predict is deprecated. Use train_evaluate_and_score with separate training and scoring dataframes."
    )

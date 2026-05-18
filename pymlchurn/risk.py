from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional

import math

import pandas as pd


RECENCY_TOLERANCE_DAYS = 31


@dataclass(frozen=True)
class RiskInterpretation:
    business_churn_now: int
    business_watchlist_risk: int
    business_risk_status: str
    raw_commercial_inactivity_risk: int
    protected_by_maintenance_contract: int
    raw_business_risk_score: float
    adjusted_business_risk_score: float
    protection_modifier_score: float
    maintenance_protection_level: str
    business_churn_reason: str
    raw_business_risk_reasons: str
    protection_modifier_reasons: str


def _num(row: Mapping[str, Any], name: str) -> Optional[float]:
    value = row.get(name)
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if math.isnan(out):
        return None
    return out


def _flag(row: Mapping[str, Any], name: str) -> bool:
    value = _num(row, name)
    return bool(value is not None and value >= 1.0)


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _join_reasons(reasons: Iterable[str], default: str) -> str:
    cleaned = [r.strip().rstrip(".") for r in reasons if r and r.strip()]
    if not cleaned:
        return default
    return "; ".join(cleaned) + "."


def _recency_weight(days: Optional[float], points: List[tuple[int, float]]) -> float:
    if days is None:
        return 0.0
    for threshold, weight in points:
        if days >= threshold:
            return weight
    return 0.0


def _maintenance_protection(row: Mapping[str, Any], reasons: List[str]) -> tuple[float, str, List[str]]:
    if not _flag(row, "MaintContractActive"):
        return 0.0, "None", []

    recency_orders = _num(row, "Recency_Orders_Days")
    recency_maint = _num(row, "Recency_MaintOrders_Days")
    protection_reasons: List[str] = ["Active maintenance contract is present"]

    if recency_orders is None:
        score = 0.20
        level = "Moderate"
        protection_reasons.append("commercial order recency is unavailable, so protection is capped")
    elif recency_orders <= 90:
        score = 0.40
        level = "Strong"
        protection_reasons.append("recent order activity keeps maintenance protection strong")
    elif recency_orders <= 180:
        score = 0.28
        level = "Moderate"
        protection_reasons.append("order inactivity is over 90 days, so protection is reduced")
    elif recency_orders <= 365:
        score = 0.15
        level = "Weak"
        protection_reasons.append("order inactivity is over 180 days, so protection is weak")
    else:
        score = 0.05
        level = "Minimal"
        protection_reasons.append("order inactivity is over 365 days, so protection is minimal")

    if recency_maint is not None and recency_maint <= 120 and score < 0.45:
        score = min(0.45, score + 0.07)
        protection_reasons.append("recent maintenance order activity adds limited protection")

    if any("No invoices" in r or "invoice" in r.lower() for r in reasons) and score > 0.10:
        score = max(0.10, score - 0.05)
        protection_reasons.append("invoice inactivity reduces maintenance protection")

    return _clip(score, 0.0, 0.45), level, protection_reasons


def interpret_business_risk(row: Mapping[str, Any]) -> RiskInterpretation:
    reasons: List[str] = []
    raw = 0.0

    recency_orders = _num(row, "Recency_Orders_Days")
    recency_invoices = _num(row, "Recency_Invoices_Days")
    order_change_pct = _num(row, "OrderValue_3m_ChangePct")
    order_freq_change_pct = _num(row, "OrderFreq_3m_ChangePct")

    order_weight = _recency_weight(recency_orders, [(365, 0.42), (180, 0.32), (90, 0.22), (60, 0.10)])
    raw += order_weight
    if recency_orders is not None and recency_orders > 90:
        reasons.append(f"No qualifying orders for {int(round(recency_orders))} days")

    invoice_weight = _recency_weight(recency_invoices, [(365, 0.20), (180, 0.15), (90, 0.10), (60, 0.05)])
    raw += invoice_weight
    if recency_invoices is not None and recency_invoices > 90:
        reasons.append(f"No invoices for {int(round(recency_invoices))} days")

    if _flag(row, "ChurnIf_NoOrd90"):
        raw += 0.15
        reasons.append("snapshot rule indicates no orders in the last 90 days")
    elif recency_orders is not None and recency_orders > 90:
        raw += 0.08
        reasons.append("raw recency still indicates no orders in the last 90 days")

    if _flag(row, "ChurnIf_NoInv90"):
        raw += 0.08
        reasons.append("snapshot rule indicates no invoices in the last 90 days")

    if _flag(row, "PredChurn_HighBackorders"):
        raw += 0.10
        reasons.append("backorders are elevated")
    elif (_num(row, "BackorderCount_180dPlus") or 0.0) > 0:
        raw += 0.05
        reasons.append("old backorders are present")

    if _flag(row, "PredChurn_Unpaid90plus") or (_num(row, "UnpaidInv_90plus_Count") or 0.0) > 0:
        raw += 0.08
        reasons.append("one or more invoices are at least 90 days overdue")

    if order_change_pct is not None and order_change_pct <= -0.50:
        raw += 0.06
        reasons.append("order value is down materially versus the prior 90 days")
    if order_freq_change_pct is not None and order_freq_change_pct <= -0.50:
        raw += 0.06
        reasons.append("order frequency is down materially versus the prior 90 days")

    eox_flags = [
        "PredChurn_EoXExpired",
        "Risk_EoX_3m",
        "EoxExpired_NoOrders90d",
        "EoxRisk_NoMaintRenewal",
        "EoxExpired_UnpaidInv90plus",
        "EoxExpired_CreditSpike90d",
    ]
    if any(_flag(row, col) for col in eox_flags):
        raw += 0.08
        reasons.append("end-of-support or renewal risk is present")

    raw = _clip(raw)
    protection, protection_level, protection_reasons = _maintenance_protection(row, reasons)
    adjusted = _clip(raw * (1.0 - protection))

    maint_active = _flag(row, "MaintContractActive")
    if raw >= 0.78 and adjusted >= 0.70:
        status = "Churned"
    elif adjusted >= 0.60:
        status = "Churn Risk"
    elif maint_active and raw >= 0.40:
        status = "Protected But At Risk"
    elif adjusted >= 0.35 or raw >= 0.45:
        status = "Watchlist Risk"
    elif maint_active:
        status = "Protected"
    else:
        status = "Healthy"

    business_churn_now = 1 if status == "Churned" else 0
    watchlist = 1 if status in {"Protected But At Risk", "Watchlist Risk", "Churn Risk"} else 0
    raw_commercial_inactivity_risk = 1 if raw >= 0.35 else 0

    if maint_active and raw >= 0.40:
        summary = (
            f"{status}: maintenance protection is {protection_level.lower()} and no longer "
            "hides commercial inactivity"
        )
    elif status in {"Churned", "Churn Risk", "Watchlist Risk"}:
        summary = f"{status}: commercial inactivity and risk signals are elevated"
    else:
        summary = f"{status}: no material commercial inactivity risk detected"

    return RiskInterpretation(
        business_churn_now=business_churn_now,
        business_watchlist_risk=watchlist,
        business_risk_status=status,
        raw_commercial_inactivity_risk=raw_commercial_inactivity_risk,
        protected_by_maintenance_contract=1 if maint_active else 0,
        raw_business_risk_score=round(raw, 6),
        adjusted_business_risk_score=round(adjusted, 6),
        protection_modifier_score=round(protection, 6),
        maintenance_protection_level=protection_level,
        business_churn_reason=summary + ".",
        raw_business_risk_reasons=_join_reasons(reasons, "No material raw inactivity signals were identified."),
        protection_modifier_reasons=_join_reasons(
            protection_reasons,
            "No active protection modifiers were identified.",
        ),
    )


def interpret_business_risk_frame(df: pd.DataFrame) -> pd.DataFrame:
    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        interpretation = interpret_business_risk(row.to_dict())
        records.append(
            {
                "business_churn_now": interpretation.business_churn_now,
                "watchlist_churn_risk": interpretation.business_watchlist_risk,
                "business_risk_status": interpretation.business_risk_status,
                "business_risk_explanation": interpretation.business_churn_reason,
                "raw_commercial_inactivity_risk": interpretation.raw_commercial_inactivity_risk,
                "raw_commercial_inactivity_risk_score": interpretation.raw_business_risk_score,
                "protected_by_maintenance_contract": interpretation.protected_by_maintenance_contract,
                "adjusted_business_risk_score": interpretation.adjusted_business_risk_score,
                "maintenance_protection_score": interpretation.protection_modifier_score,
                "maintenance_protection_level": interpretation.maintenance_protection_level,
                "business_churn_reason": interpretation.business_churn_reason,
                "raw_business_risk_reasons": interpretation.raw_business_risk_reasons,
                "protection_modifier_reasons": interpretation.protection_modifier_reasons,
            }
        )
    return pd.DataFrame.from_records(records, index=df.index)


def validate_recency_consistency(
    df: pd.DataFrame,
    snapshot_col: str = "as_of_date",
    last_order_col: str = "LastOrderDate",
    recency_col: str = "Recency_Orders_Days",
    tolerance_days: int = RECENCY_TOLERANCE_DAYS,
) -> pd.DataFrame:
    result = pd.DataFrame(index=df.index)
    result["recency_consistency_flag"] = "not_checked"
    result["recency_consistency_reason"] = "Last order date was not available for validation."

    required = {snapshot_col, last_order_col, recency_col}
    if not required.issubset(set(df.columns)):
        return result

    snapshot = pd.to_datetime(df[snapshot_col], errors="coerce")
    last_order = pd.to_datetime(df[last_order_col], errors="coerce")
    recency = pd.to_numeric(df[recency_col], errors="coerce")
    expected = (snapshot - last_order).dt.days

    checked = snapshot.notna() & last_order.notna() & recency.notna()
    result.loc[checked, "recency_consistency_flag"] = "ok"
    result.loc[checked, "recency_consistency_reason"] = "Stored order recency matches SnapshotDate minus LastOrderDate."

    mismatch = checked & ((expected - recency).abs() > int(tolerance_days))
    result.loc[mismatch, "recency_consistency_flag"] = "mismatch"
    result.loc[mismatch, "recency_consistency_reason"] = [
        f"Stored Recency_Orders_Days={int(round(float(stored)))} but SnapshotDate minus LastOrderDate={int(calc)} days."
        for stored, calc in zip(recency.loc[mismatch], expected.loc[mismatch])
    ]

    missing = ~(checked) & (snapshot.notna() | last_order.notna() | recency.notna())
    result.loc[missing, "recency_consistency_flag"] = "incomplete"
    result.loc[missing, "recency_consistency_reason"] = "One or more recency validation inputs were null or invalid."
    return result

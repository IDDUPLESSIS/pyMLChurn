from __future__ import annotations

from typing import Optional, List


CUSTOMER_ID_COL = "customer_id"
DATE_COL = "as_of_date"


def feature_columns() -> List[str]:
    return [
        "recency_days",
        "median_gap_days",
        "p90_gap_days",
        "cv_gap",
        "in_renewal_grace",
        "rev_180d",
        "rev_returns_90d",
        "invoices_90d",
        "credit_notes_90d",
        "orders_pos_30d",
        "orders_neg_30d",
        "backorder_qty_30d",
        "pct_change_3m",
        "pct_change_6m",
        "yoy_change_pct",
        "credit_notes_prev_month",
        "invoices_pos_prev_month",
        "credit_notes_ma3",
        "threshold_days",
        "is_maintenance_heavy",
        "maint_cycle_days",
        "severity_score",
        "lateness_component",
        "credits_component",
        "trend_component",
        "mitigator_component",
    ]


def target_column(default: str = "churned_hard90") -> str:
    return default


def churn_query(top: Optional[int] = None, include_label: bool = True, target: Optional[str] = None) -> str:
    # Build a SELECT with only the columns needed for the ML pipeline
    # Explicitly convert t0 to date for consistent values and alias as as_of_date
    feature_cols = feature_columns()
    parts: List[str] = [
        f"[{CUSTOMER_ID_COL}]",
        # Force yyyy-mm-dd string and fallback to current UTC date if somehow NULL
        "COALESCE(CONVERT(varchar(10), [t0], 23), CONVERT(varchar(10), SYSUTCDATETIME(), 23)) AS [as_of_date]",
    ] + [f"[{c}]" for c in feature_cols]
    if include_label:
        parts.append(f"[{target or target_column()}]")
    select_cols = ",\n      ".join(parts)

    top_clause = f"TOP ({int(top)}) " if (top is not None and int(top) > 0) else ""
    return f"""
SELECT {top_clause}
      {select_cols}
FROM [SAP].[dbo].[CustomerChurnCadence_v1];
"""

from __future__ import annotations

from typing import Optional, List

# Column names as they appear in your SQL view chrn01.v_train_dataset
CUSTOMER_ID_COL = "CustomerId"
DATE_COL = "as_of_date"  # will be an alias of SnapshotDate


def feature_columns() -> List[str]:
    """
    Feature list must exactly match the columns exposed by chrn01.v_train_dataset
    (excluding label columns).
    """
    return [
        # Core recency features
        "Recency_Orders_Days",
        "Recency_MaintOrders_Days",
        "Recency_Invoices_Days",

        # Basic 90d behaviour and mix (level + change)
        "OrderValue_90d",
        "OrderValue_Prev90d",
        "OrderValue_3m_Change",
        "OrderValue_3m_ChangePct",
        "OrderFreq_90d",
        "OrderFreq_Prev90d",
        "OrderFreq_3m_Change",
        "OrderFreq_3m_ChangePct",
        "MaintOrderValue_90d",
        "ProdOrderValue_90d",
        "ProdRatio_90d",
        "MaintRatio_90d",
        "IsProductHeavy_90d",
        "IsMaintHeavy_90d",

        # Risk / signal flags and counts
        "BackorderCount_180dPlus",
        "UnpaidInv_OverTerms_Count",
        "UnpaidInv_90plus_Count",
        "ARBucket_Current_Count",
        "ARBucket_1_30_Count",
        "ARBucket_31_60_Count",
        "ARBucket_61_90_Count",
        "ARBucket_91_120_Count",
        "ARBucket_121_180_Count",
        "ARBucket_180Plus_Count",
        "DSO_OpenInvoices",
        "ReturnInvCount_90d",
        "CreditValue_90d",
        "CreditCount_90d",

        # EoX / LDOS risk and revenue mix
        "Eox_MinDaysToLDOS",
        "Risk_EoX_12m",
        "Risk_EoX_6m",
        "Risk_EoX_3m",
        "PredChurn_EoXExpired",
        "Eox_Rev_Expired_12m",
        "Eox_Rev_Risk12m",
        "Eox_Rev_Total12m",
        "Eox_RevExpiredPct_12m",
        "Eox_RevRiskPct_12m",
        "Eox_HasExpiredRevenue_12m",
        "Eox_HasRiskRevenue_12m",
        "Eox_MajorityExpired_12m",
        "Eox_SKU_CountExpired",
        "Eox_SKU_CountRisk12m",
        "EoxExpired_NoOrders90d",
        "EoxRisk_NoMaintRenewal",
        "EoxExpired_UnpaidInv90plus",
        "EoxExpired_CreditSpike90d",

        # Rule-based churn flags (good engineered features)
        "ChurnIf_NoOrd90",
        "ChurnIf_NoInv90",
        "ChurnIf_NoMaintOrd90_WithinGrace",
        "PredChurn_Unpaid90plus",
        "PredChurn_HighBackorders",
    ]


def target_column(default: str = "Label_Churn_90d") -> str:
    """
    Default supervised target. We start with Label_Churn_90d to keep the
    semantics consistent with ml.py, which currently assumes a '90d' horizon
    in its output column names.
    """
    return default


def churn_query(
    top: Optional[int] = None,
    include_label: bool = True,
    target: Optional[str] = None,
) -> str:
    """
    Minimal SELECT from chrn01.v_train_dataset with only the columns needed
    by the ML pipeline. SnapshotDate is exposed to Python as 'as_of_date'.
    """
    feat_cols = feature_columns()

    # Always pull CustomerId + SnapshotDate (aliased to as_of_date)
    parts: List[str] = [
        "[CustomerId]",
        "CONVERT(varchar(10), [SnapshotDate], 23) AS [as_of_date]",
    ]
    # Feature columns
    parts += [f"[{c}]" for c in feat_cols]

    # Optional label
    if include_label:
        parts.append(f"[{target or target_column()}]")

    select_cols = ",\n      ".join(parts)
    top_clause = f"TOP ({int(top)}) " if (top is not None and int(top) > 0) else ""

    return f"""
SELECT {top_clause}
      {select_cols}
FROM [SAP].[chrn01].[v_train_dataset];
"""

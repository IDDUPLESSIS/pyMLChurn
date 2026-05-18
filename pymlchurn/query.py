from __future__ import annotations

from typing import List, Optional

# Column names as they appear in chrn01.v_train_dataset
CUSTOMER_ID_COL = "CustomerId"
DATE_COL = "as_of_date"  # alias for SnapshotDate


def feature_columns() -> List[str]:
    return [
        "Recency_Orders_Days",
        "Recency_MaintOrders_Days",
        "Recency_Invoices_Days",
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
        "MaintContractActive",
        "ChurnIf_NoOrd90",
        "ChurnIf_NoInv90",
        "ChurnIf_NoMaintOrd90_WithinGrace",
        "PredChurn_Unpaid90plus",
        "PredChurn_HighBackorders",
    ]


def direct_rule_feature_columns() -> List[str]:
    return [
        "ChurnIf_NoOrd90",
        "ChurnIf_NoInv90",
        "ChurnIf_NoMaintOrd90_WithinGrace",
    ]


def target_column(default: str = "Label_Churn_90d") -> str:
    return default


def _label_maturity_days(label_col: str) -> int:
    lookup = {
        "Label_Churn_90d": 90,
        "Label_Churn_180d": 180,
        "Label_Churn_365d": 365,
    }
    return lookup.get(label_col, 90)


def _safe_sql_date_literal(value: str) -> str:
    return value.replace("'", "''")


def _top_clause(top: Optional[int]) -> str:
    return f"TOP ({int(top)}) " if (top is not None and int(top) > 0) else ""


def _shared_select_columns(
    target: Optional[str] = None,
    include_target: bool = False,
    include_metadata: bool = False,
) -> List[str]:
    cols: List[str] = [
        f"[{CUSTOMER_ID_COL}]",
        "CONVERT(varchar(10), [SnapshotDate], 23) AS [as_of_date]",
    ]
    if include_metadata:
        cols.append("CONVERT(varchar(10), [LastOrderDate], 23) AS [LastOrderDate]")
    cols.extend([f"[{c}]" for c in feature_columns()])
    if include_target:
        cols.append(f"[{target or target_column()}]")
    return cols


def churn_training_query(
    target: Optional[str] = None,
    top: Optional[int] = None,
    train_cutoff_date: Optional[str] = None,
) -> str:
    target_col = target or target_column()
    maturity_days = _label_maturity_days(target_col)
    select_cols = ",\n      ".join(_shared_select_columns(target=target_col, include_target=True))

    if train_cutoff_date:
        cutoff_expr = f"CAST('{_safe_sql_date_literal(train_cutoff_date)}' AS date)"
    else:
        cutoff_expr = f"DATEADD(day, -{maturity_days}, CAST(GETDATE() AS date))"

    return f"""
SELECT {_top_clause(top)}
      {select_cols}
FROM [SAP].[chrn01].[v_train_dataset]
WHERE [{target_col}] IS NOT NULL
  AND [SnapshotDate] IS NOT NULL
  AND CAST([SnapshotDate] AS date) <= {cutoff_expr};
"""


def churn_scoring_query(
    score_as_of: Optional[str] = None,
    top: Optional[int] = None,
) -> str:
    select_cols = ",\n      ".join(_shared_select_columns(include_target=False, include_metadata=True))
    if score_as_of:
        score_expr = f"CAST('{_safe_sql_date_literal(score_as_of)}' AS date)"
    else:
        score_expr = "(SELECT MAX(CAST([SnapshotDate] AS date)) FROM [SAP].[chrn01].[v_train_dataset])"

    return f"""
SELECT {_top_clause(top)}
      {select_cols}
FROM [SAP].[chrn01].[v_train_dataset]
WHERE [SnapshotDate] IS NOT NULL
  AND CAST([SnapshotDate] AS date) = {score_expr};
"""


def churn_query(
    top: Optional[int] = None,
    include_label: bool = True,
    target: Optional[str] = None,
) -> str:
    """
    Deprecated compatibility helper.
    Prefer churn_training_query(...) and churn_scoring_query(...) explicitly.
    """
    if include_label:
        return churn_training_query(target=target, top=top, train_cutoff_date=None)
    return churn_scoring_query(score_as_of=None, top=top)

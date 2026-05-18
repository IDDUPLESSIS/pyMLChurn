SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID(N'chrn01.v_train_dataset', N'V') IS NOT NULL
    DROP VIEW [chrn01].[v_train_dataset];
GO

CREATE VIEW [chrn01].[v_train_dataset]
AS
SELECT
    s.SnapshotDate,
    s.CustomerId,
    s.LastOrderDate,

    ----------------------------------------------------------------
    -- 1) Recency + basic 90d windows (core churn signal = no product buys)
    ----------------------------------------------------------------
    s.Recency_Orders_Days,
    s.Recency_MaintOrders_Days,
    s.Recency_Invoices_Days,

    s.OrderValue_90d,
    s.OrderValue_Prev90d,
    s.OrderValue_3m_Change,
    s.OrderValue_3m_ChangePct,

    s.OrderFreq_90d,
    s.OrderFreq_Prev90d,
    s.OrderFreq_3m_Change,
    s.OrderFreq_3m_ChangePct,

    s.MaintOrderValue_90d,
    s.ProdOrderValue_90d,
    s.ProdRatio_90d,
    s.MaintRatio_90d,
    s.IsProductHeavy_90d,
    s.IsMaintHeavy_90d,

    ----------------------------------------------------------------
    -- 1b) Active maintenance contract protection feature
    -- If 1, maintenance can reduce interpreted risk severity but should not
    -- hide raw commercial inactivity signals.
    ----------------------------------------------------------------
    s.MaintContractActive,

    ----------------------------------------------------------------
    -- 2) AR / credit / returns / backorders (distress + friction predictors)
    ----------------------------------------------------------------
    s.BackorderCount_180dPlus,

    s.UnpaidInv_OverTerms_Count,
    s.UnpaidInv_90plus_Count,

    s.ARBucket_Current_Count,
    s.ARBucket_1_30_Count,
    s.ARBucket_31_60_Count,
    s.ARBucket_61_90_Count,
    s.ARBucket_91_120_Count,
    s.ARBucket_121_180_Count,
    s.ARBucket_180Plus_Count,
    s.DSO_OpenInvoices,

    s.ReturnInvCount_90d,
    s.CreditValue_90d,
    s.CreditCount_90d,

    ----------------------------------------------------------------
    -- 3) EoX / LDOS features
    ----------------------------------------------------------------
    s.Eox_MinDaysToLDOS,

    s.Risk_EoX_12m,
    s.Risk_EoX_6m,
    s.Risk_EoX_3m,
    s.PredChurn_EoXExpired,

    s.Eox_Rev_Expired_12m,
    s.Eox_Rev_Risk12m,
    s.Eox_Rev_Total12m,
    s.Eox_RevExpiredPct_12m,
    s.Eox_RevRiskPct_12m,

    s.Eox_HasExpiredRevenue_12m,
    s.Eox_HasRiskRevenue_12m,
    s.Eox_MajorityExpired_12m,

    s.Eox_SKU_CountExpired,
    s.Eox_SKU_CountRisk12m,
    s.Eox_MinLDOSDate,

    s.EoxExpired_NoOrders90d,
    s.EoxRisk_NoMaintRenewal,
    s.EoxExpired_UnpaidInv90plus,
    s.EoxExpired_CreditSpike90d,

    ----------------------------------------------------------------
    -- 4) Rule-based flags (engineered heuristics - OK as features)
    ----------------------------------------------------------------
    s.ChurnIf_NoOrd90,
    s.ChurnIf_NoInv90,
    s.ChurnIf_NoMaintOrd90_WithinGrace,
    s.PredChurn_Unpaid90plus,
    s.PredChurn_HighBackorders,

    ----------------------------------------------------------------
    -- 5) Labels (forward-looking targets already stored on CustomerSnapshot)
    -- Maintenance protection is modeled as context/modifier, not a hard veto
    -- against raw commercial inactivity visibility.
    ----------------------------------------------------------------
    s.Target_IsChurned_3mAhead  AS Label_Churn_90d,
    s.Target_IsChurned_6mAhead  AS Label_Churn_180d,
    s.Target_IsChurned_12mAhead AS Label_Churn_365d,

    CASE WHEN ISNULL(s.Target_IsChurned_3mAhead,0) = 1
           OR ISNULL(s.Target_IsChurned_6mAhead,0) = 1
           OR ISNULL(s.Target_IsChurned_12mAhead,0) = 1
         THEN 1 ELSE 0 END      AS Label_HasAnyChurn

FROM chrn01.CustomerSnapshot s;
GO

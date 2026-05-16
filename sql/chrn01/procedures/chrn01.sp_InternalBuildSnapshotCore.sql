SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID(N'chrn01.sp_InternalBuildSnapshotCore', N'P') IS NOT NULL
    DROP PROCEDURE [chrn01].[sp_InternalBuildSnapshotCore];
GO



CREATE PROCEDURE [chrn01].[sp_InternalBuildSnapshotCore]
(
    @AsOfDate date   -- build features using data up to this date
)
AS
BEGIN
    SET NOCOUNT ON;

    --------------------------------------------------------------------
    -- Derive the snapshot month-end we store in the table
    --------------------------------------------------------------------
    DECLARE @SnapshotDate date = EOMONTH(@AsOfDate);

    PRINT CONCAT('  → Building snapshot for AsOf=',
                 CONVERT(varchar(10), @AsOfDate, 120),
                 ' stored as SnapshotDate=',
                 CONVERT(varchar(10), @SnapshotDate, 120));

    --------------------------------------------------------------------
    -- 1) Base temp tables (cut off at @AsOfDate)
    --------------------------------------------------------------------

    --------------------------------------------------------------------
    -- 1.1 Orders: ZSD_REPORD_ORDER_INTAKE
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#Orders') IS NOT NULL DROP TABLE #Orders;

    SELECT
        [Sold To]                        AS CustomerId,
        [Order Number]                   AS SalesDoc,
        [Document Date]                  AS OrderDate,
        -1 * [Planned Revenue Rep#Curr#] AS OrderValue,        -- normalize to positive revenue
        [Contract Item End Date]         AS ContractEndDate,
        CASE
            WHEN LEN([Order Number]) = 8 THEN 1  -- maintenance contract
            WHEN LEN([Order Number]) = 6 THEN 0  -- product order
            ELSE NULL
        END AS IsMaintenance
    INTO #Orders
    FROM dbo.ZSD_REPORD_ORDER_INTAKE
    WHERE [Document Date] <= @AsOfDate
      AND [Company] = 'US01'
      AND ISNULL([Reject Reason Code], '') = '';

    --------------------------------------------------------------------
    -- 1.2 Backorders: ZSD_REPORD_BACKORDER
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#Backorders') IS NOT NULL DROP TABLE #Backorders;

    SELECT
        [Sold To]                       AS CustomerId,
        [Order Number]                  AS SalesDoc,
        [Document Date]                 AS OrderDate,
        [Qty OutStand]                  AS QtyOutstanding,
        [Billing Outstanding Rep#Curr#] AS BillingOutstanding,
        [Contract Item End Date]        AS ContractEndDate,
        CASE
            WHEN LEN([Order Number]) = 8 THEN 1
            WHEN LEN([Order Number]) = 6 THEN 0
            ELSE NULL
        END AS IsMaintenance
    INTO #Backorders
    FROM dbo.ZSD_REPORD_BACKORDER
    WHERE [Document Date] <= @AsOfDate
      AND [Company] = 'US01'
      AND ISNULL([Reject Reason Code], '') = '';

    --------------------------------------------------------------------
    -- 1.3 Invoices: ZSD_REPBILL (line-level)
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#Invoices') IS NOT NULL DROP TABLE #Invoices;

    SELECT
        [Sold-To]                                    AS CustomerId,
        [Billing Doc#]                               AS BillingDoc,
        [TxDate]                                     AS BillingDate,
        [Doc# Net Value] *
            CASE WHEN [+/-] = '-' THEN -1 ELSE 1 END AS DocNetValue,
        [Material]                                   AS Material,
        [Sales Doc#]                                 AS SalesDoc,
        CASE
            WHEN LEN(CAST([Sales Doc#] AS varchar(50))) = 8 THEN 1
            WHEN LEN(CAST([Sales Doc#] AS varchar(50))) = 6 THEN 0
            ELSE NULL
        END                                          AS IsMaintenance,
        CASE WHEN [Returns item] = 'X' THEN 1 ELSE 0 END AS IsReturn
    INTO #Invoices
    FROM dbo.ZSD_REPBILL
    WHERE [TxDate] <= @AsOfDate
      AND [Sales Org] = 'US01'
      AND ISNULL([Bill# Doc# Cancelled], '') = '';

    --------------------------------------------------------------------
    -- 1.4 AR Aging: ZFI_AGE joined to invoice-level Sold-To
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#ARAging') IS NOT NULL DROP TABLE #ARAging;

    ;WITH InvoiceSoldTo AS
    (
        SELECT
            [Billing Doc#] AS BillingDoc,
            MAX([Sold-To]) AS CustomerId
        FROM dbo.ZSD_REPBILL
        GROUP BY [Billing Doc#]
    ),
    AR_Raw AS
    (
        SELECT
            ist.CustomerId,
            a.[Billing Doc#] AS BillingDoc,
            a.[Doc Value]    AS DocValue,
            a.[Due Date]     AS DueDate,
            DATEDIFF(DAY, a.[Due Date], @AsOfDate) AS DaysPastDue
        FROM dbo.ZFI_AGE a
        JOIN InvoiceSoldTo ist
          ON ist.BillingDoc = a.[Billing Doc#]
    )
    SELECT
        CustomerId,
        BillingDoc,
        SUM(DocValue)    AS DocValue,
        MAX(DueDate)     AS DueDate,
        MAX(DaysPastDue) AS DaysPastDue
    INTO #ARAging
    FROM AR_Raw
    GROUP BY CustomerId, BillingDoc;

    --------------------------------------------------------------------
    -- 2) Customer list
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#Customers') IS NOT NULL DROP TABLE #Customers;

    SELECT DISTINCT CustomerId
    INTO #Customers
    FROM (
        SELECT CustomerId FROM #Orders
        UNION
        SELECT CustomerId FROM #Backorders
        UNION
        SELECT CustomerId FROM #Invoices
        UNION
        SELECT CustomerId FROM #ARAging
    ) c;

    --------------------------------------------------------------------
    -- 3) Aggregated features per customer
    --------------------------------------------------------------------

    --------------------------------------------------------------------
    -- 3.1 Orders features
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#FeatOrders') IS NOT NULL DROP TABLE #FeatOrders;

    SELECT
        c.CustomerId,

        MAX(o.OrderDate) AS LastOrderDate,
        CASE WHEN MAX(o.OrderDate) IS NULL THEN NULL
             ELSE DATEDIFF(DAY, MAX(o.OrderDate), @AsOfDate) END AS Recency_Orders_Days,

        MAX(CASE WHEN o.IsMaintenance = 1 THEN o.OrderDate END) AS LastMaintOrderDate,
        CASE WHEN MAX(CASE WHEN o.IsMaintenance = 1 THEN o.OrderDate END) IS NULL THEN NULL
             ELSE DATEDIFF(DAY, MAX(CASE WHEN o.IsMaintenance = 1 THEN o.OrderDate END), @AsOfDate) END AS Recency_MaintOrders_Days,

        SUM(CASE WHEN o.OrderDate >= DATEADD(DAY,-90,@AsOfDate) THEN ISNULL(o.OrderValue,0) ELSE 0 END) AS OrderValue_90d,
        COUNT(DISTINCT CASE WHEN o.OrderDate >= DATEADD(DAY,-90,@AsOfDate) THEN o.SalesDoc END)         AS OrderFreq_90d,

        SUM(CASE WHEN o.OrderDate >= DATEADD(DAY,-180,@AsOfDate)
                   AND o.OrderDate <  DATEADD(DAY,-90,@AsOfDate)
                 THEN ISNULL(o.OrderValue,0) ELSE 0 END) AS OrderValue_Prev90d,
        COUNT(DISTINCT CASE WHEN o.OrderDate >= DATEADD(DAY,-180,@AsOfDate)
                              AND o.OrderDate <  DATEADD(DAY,-90,@AsOfDate)
                            THEN o.SalesDoc END) AS OrderFreq_Prev90d,

        SUM(CASE WHEN o.IsMaintenance = 1 AND o.OrderDate >= DATEADD(DAY,-90,@AsOfDate) THEN ISNULL(o.OrderValue,0) ELSE 0 END) AS MaintOrderValue_90d,
        SUM(CASE WHEN o.IsMaintenance = 0 AND o.OrderDate >= DATEADD(DAY,-90,@AsOfDate) THEN ISNULL(o.OrderValue,0) ELSE 0 END) AS ProdOrderValue_90d
    INTO #FeatOrders
    FROM #Customers c
    LEFT JOIN #Orders o
      ON o.CustomerId = c.CustomerId
    GROUP BY c.CustomerId;

    --------------------------------------------------------------------
    -- 3.1b Maintenance contract active flag (as-of snapshot)
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#FeatMaintContract') IS NOT NULL DROP TABLE #FeatMaintContract;

    SELECT
        c.CustomerId,
        CASE WHEN EXISTS (
            SELECT 1
            FROM #Orders mo
            WHERE mo.CustomerId = c.CustomerId
              AND mo.IsMaintenance = 1
              AND mo.ContractEndDate IS NOT NULL
              AND mo.ContractEndDate >= @AsOfDate
        )
        THEN 1 ELSE 0 END AS MaintContractActive
    INTO #FeatMaintContract
    FROM #Customers c;

    --------------------------------------------------------------------
    -- 3.2 Invoice features
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#FeatInvoices') IS NOT NULL DROP TABLE #FeatInvoices;

    SELECT
        c.CustomerId,

        MAX(i.BillingDate) AS LastInvoiceDate,
        CASE WHEN MAX(i.BillingDate) IS NULL THEN NULL
             ELSE DATEDIFF(DAY, MAX(i.BillingDate), @AsOfDate) END AS Recency_Invoices_Days,

        SUM(CASE WHEN i.BillingDate >= DATEADD(DAY,-90,@AsOfDate) THEN ISNULL(i.DocNetValue,0) ELSE 0 END) AS InvValue_90d,
        SUM(CASE WHEN i.IsMaintenance = 1 AND i.BillingDate >= DATEADD(DAY,-90,@AsOfDate) THEN ISNULL(i.DocNetValue,0) ELSE 0 END) AS MaintInvValue_90d,

        COUNT(DISTINCT CASE WHEN i.IsReturn = 1 AND i.BillingDate >= DATEADD(DAY,-90,@AsOfDate) THEN i.BillingDoc END) AS ReturnInvCount_90d,

        SUM(CASE WHEN i.BillingDate >= DATEADD(DAY,-90,@AsOfDate) AND i.DocNetValue < 0 THEN -1 * ISNULL(i.DocNetValue,0) ELSE 0 END) AS CreditValue_90d,
        SUM(CASE WHEN i.BillingDate >= DATEADD(DAY,-90,@AsOfDate) AND i.DocNetValue < 0 THEN 1 ELSE 0 END) AS CreditCount_90d
    INTO #FeatInvoices
    FROM #Customers c
    LEFT JOIN #Invoices i
      ON i.CustomerId = c.CustomerId
    GROUP BY c.CustomerId;

    --------------------------------------------------------------------
    -- 3.3 AR features
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#FeatAR') IS NOT NULL DROP TABLE #FeatAR;

    SELECT
        c.CustomerId,
        SUM(CASE WHEN a.DaysPastDue > 0  THEN 1 ELSE 0 END)  AS UnpaidInv_OverTerms_Count,
        SUM(CASE WHEN a.DaysPastDue >= 90 THEN 1 ELSE 0 END) AS UnpaidInv_90plus_Count,

        SUM(CASE WHEN a.DaysPastDue < 0 THEN 1 ELSE 0 END)                           AS ARBucket_Current_Count,
        SUM(CASE WHEN a.DaysPastDue >= 0  AND a.DaysPastDue <= 30 THEN 1 ELSE 0 END) AS ARBucket_1_30_Count,
        SUM(CASE WHEN a.DaysPastDue > 30  AND a.DaysPastDue <= 60 THEN 1 ELSE 0 END) AS ARBucket_31_60_Count,
        SUM(CASE WHEN a.DaysPastDue > 60  AND a.DaysPastDue <= 90 THEN 1 ELSE 0 END) AS ARBucket_61_90_Count,
        SUM(CASE WHEN a.DaysPastDue > 90  AND a.DaysPastDue <= 120 THEN 1 ELSE 0 END) AS ARBucket_91_120_Count,
        SUM(CASE WHEN a.DaysPastDue > 120 AND a.DaysPastDue <= 180 THEN 1 ELSE 0 END) AS ARBucket_121_180_Count,
        SUM(CASE WHEN a.DaysPastDue > 180 THEN 1 ELSE 0 END)                         AS ARBucket_180Plus_Count,

        AVG(CASE WHEN a.DaysPastDue > 0 THEN CONVERT(decimal(9,2), a.DaysPastDue) END) AS DSO_OpenInvoices
    INTO #FeatAR
    FROM #Customers c
    LEFT JOIN #ARAging a
      ON a.CustomerId = c.CustomerId
    GROUP BY c.CustomerId;

    --------------------------------------------------------------------
    -- 3.4 Backorders
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#FeatBackorders') IS NOT NULL DROP TABLE #FeatBackorders;

    SELECT
        c.CustomerId,
        SUM(CASE WHEN DATEDIFF(DAY, b.OrderDate, @AsOfDate) >= 180 THEN 1 ELSE 0 END) AS BackorderCount_180dPlus
    INTO #FeatBackorders
    FROM #Customers c
    LEFT JOIN #Backorders b
      ON b.CustomerId = c.CustomerId
    GROUP BY c.CustomerId;

    --------------------------------------------------------------------
    -- 3.5 Revenue totals (12m)
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#FeatRev12m') IS NOT NULL DROP TABLE #FeatRev12m;

    SELECT
        CustomerId,
        SUM(CASE WHEN BillingDate >= DATEADD(DAY,-365,@AsOfDate) THEN ISNULL(DocNetValue,0) ELSE 0 END) AS RevTotal_12m
    INTO #FeatRev12m
    FROM #Invoices
    GROUP BY CustomerId;

    --------------------------------------------------------------------
    -- 3.6 EoX features (LDOS)
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#FeatEox') IS NOT NULL DROP TABLE #FeatEox;

    DECLARE @MinValidLDOS date = '2000-01-01';

    ;WITH EoxRaw AS
    (
        SELECT
            i.CustomerId,
            i.BillingDate,
            i.DocNetValue,
            mm.[ProductId],
            mm.[LastDateOfSupport] AS LDOS,
            DATEDIFF(DAY, @AsOfDate, mm.[LastDateOfSupport]) AS DaysToLDOS
        FROM #Invoices i
        JOIN [MASTERDATA].[dbo].[MSTR_Material] mm
          ON i.Material = mm.[ProductId]
        WHERE mm.[LastDateOfSupport] IS NOT NULL
          AND mm.[LastDateOfSupport] >= @MinValidLDOS
    )
    SELECT
        CustomerId,
        MIN(DaysToLDOS) AS MinDaysToLDOS,
        MIN(LDOS)       AS MinLDOSDate,

        SUM(CASE WHEN BillingDate >= DATEADD(DAY,-365,@AsOfDate) AND DaysToLDOS < 0
                 THEN ISNULL(DocNetValue,0) ELSE 0 END) AS Eox_Rev_Expired_12m,

        SUM(CASE WHEN BillingDate >= DATEADD(DAY,-365,@AsOfDate) AND DaysToLDOS BETWEEN 0 AND 365
                 THEN ISNULL(DocNetValue,0) ELSE 0 END) AS Eox_Rev_Risk12m,

        COUNT(DISTINCT CASE WHEN DaysToLDOS < 0 THEN [ProductId] END)                 AS Eox_SKU_CountExpired,
        COUNT(DISTINCT CASE WHEN DaysToLDOS BETWEEN 0 AND 365 THEN [ProductId] END)   AS Eox_SKU_CountRisk12m
    INTO #FeatEox
    FROM EoxRaw
    GROUP BY CustomerId;

    --------------------------------------------------------------------
    -- 4) Join features + derive flags
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#Snapshot') IS NOT NULL DROP TABLE #Snapshot;

    SELECT
        @SnapshotDate AS SnapshotDate,
        c.CustomerId,

        fo.Recency_Orders_Days,
        fo.Recency_MaintOrders_Days,
        fi.Recency_Invoices_Days,

        ISNULL(TRY_CONVERT(decimal(18,2), fo.OrderValue_90d),     0) AS OrderValue_90d,
        ISNULL(TRY_CONVERT(decimal(18,2), fo.OrderValue_Prev90d), 0) AS OrderValue_Prev90d,
        ISNULL(fo.OrderFreq_90d,0)                                   AS OrderFreq_90d,
        ISNULL(fo.OrderFreq_Prev90d,0)                               AS OrderFreq_Prev90d,
        ISNULL(TRY_CONVERT(decimal(18,2), fo.MaintOrderValue_90d), 0) AS MaintOrderValue_90d,
        ISNULL(TRY_CONVERT(decimal(18,2), fo.ProdOrderValue_90d),  0) AS ProdOrderValue_90d,

        ISNULL(fb.BackorderCount_180dPlus,0)    AS BackorderCount_180dPlus,
        ISNULL(far.UnpaidInv_OverTerms_Count,0) AS UnpaidInv_OverTerms_Count,
        ISNULL(far.UnpaidInv_90plus_Count,0)    AS UnpaidInv_90plus_Count,

        ISNULL(far.ARBucket_Current_Count,0)    AS ARBucket_Current_Count,
        ISNULL(far.ARBucket_1_30_Count,0)       AS ARBucket_1_30_Count,
        ISNULL(far.ARBucket_31_60_Count,0)      AS ARBucket_31_60_Count,
        ISNULL(far.ARBucket_61_90_Count,0)      AS ARBucket_61_90_Count,
        ISNULL(far.ARBucket_91_120_Count,0)     AS ARBucket_91_120_Count,
        ISNULL(far.ARBucket_121_180_Count,0)    AS ARBucket_121_180_Count,
        ISNULL(far.ARBucket_180Plus_Count,0)    AS ARBucket_180Plus_Count,
        ISNULL(far.DSO_OpenInvoices,0)          AS DSO_OpenInvoices,

        ISNULL(fi.ReturnInvCount_90d,0)         AS ReturnInvCount_90d,
        ISNULL(TRY_CONVERT(decimal(18,2), fi.CreditValue_90d),0) AS CreditValue_90d,
        ISNULL(fi.CreditCount_90d,0)            AS CreditCount_90d,

        ISNULL(mc.MaintContractActive,0)        AS MaintContractActive,

        ISNULL(TRY_CONVERT(decimal(18,2), ISNULL(fo.OrderValue_90d,0) - ISNULL(fo.OrderValue_Prev90d,0)), 0) AS OrderValue_3m_Change,
        CASE WHEN ISNULL(fo.OrderValue_Prev90d,0) > 0
             THEN TRY_CONVERT(decimal(9,4),
                  (ISNULL(fo.OrderValue_90d,0) - ISNULL(fo.OrderValue_Prev90d,0)) / NULLIF(fo.OrderValue_Prev90d,0))
             ELSE NULL END AS OrderValue_3m_ChangePct,

        ISNULL(fo.OrderFreq_90d,0) - ISNULL(fo.OrderFreq_Prev90d,0) AS OrderFreq_3m_Change,
        CASE WHEN ISNULL(fo.OrderFreq_Prev90d,0) > 0
             THEN TRY_CONVERT(decimal(9,4),
                  (ISNULL(fo.OrderFreq_90d,0) - ISNULL(fo.OrderFreq_Prev90d,0)) * 1.0 / NULLIF(fo.OrderFreq_Prev90d,0))
             ELSE NULL END AS OrderFreq_3m_ChangePct,

        CASE WHEN (ISNULL(fo.MaintOrderValue_90d,0) + ISNULL(fo.ProdOrderValue_90d,0)) > 0
             THEN TRY_CONVERT(decimal(9,4),
                  ISNULL(fo.ProdOrderValue_90d,0) * 1.0 /
                  (ISNULL(fo.MaintOrderValue_90d,0) + ISNULL(fo.ProdOrderValue_90d,0)))
             ELSE NULL END AS ProdRatio_90d,

        CASE WHEN (ISNULL(fo.MaintOrderValue_90d,0) + ISNULL(fo.ProdOrderValue_90d,0)) > 0
             THEN TRY_CONVERT(decimal(9,4),
                  ISNULL(fo.MaintOrderValue_90d,0) * 1.0 /
                  (ISNULL(fo.MaintOrderValue_90d,0) + ISNULL(fo.ProdOrderValue_90d,0)))
             ELSE NULL END AS MaintRatio_90d,

        CASE WHEN (ISNULL(fo.MaintOrderValue_90d,0) + ISNULL(fo.ProdOrderValue_90d,0)) > 0
               AND ISNULL(fo.ProdOrderValue_90d,0) * 1.0 /
                   (ISNULL(fo.MaintOrderValue_90d,0) + ISNULL(fo.ProdOrderValue_90d,0)) >= 0.7
             THEN 1 ELSE 0 END AS IsProductHeavy_90d,

        CASE WHEN (ISNULL(fo.MaintOrderValue_90d,0) + ISNULL(fo.ProdOrderValue_90d,0)) > 0
               AND ISNULL(fo.MaintOrderValue_90d,0) * 1.0 /
                   (ISNULL(fo.MaintOrderValue_90d,0) + ISNULL(fo.ProdOrderValue_90d,0)) >= 0.7
             THEN 1 ELSE 0 END AS IsMaintHeavy_90d,

        fe.MinDaysToLDOS AS Eox_MinDaysToLDOS,
        fe.MinLDOSDate   AS Eox_MinLDOSDate,
        ISNULL(TRY_CONVERT(decimal(18,2), fe.Eox_Rev_Expired_12m),0) AS Eox_Rev_Expired_12m,
        ISNULL(TRY_CONVERT(decimal(18,2), fe.Eox_Rev_Risk12m),0)     AS Eox_Rev_Risk12m,
        ISNULL(TRY_CONVERT(decimal(18,2), fr.RevTotal_12m),0)        AS Eox_Rev_Total12m,

        CASE WHEN ISNULL(fr.RevTotal_12m,0) > 0
             THEN TRY_CONVERT(decimal(9,4), ISNULL(fe.Eox_Rev_Expired_12m,0) / NULLIF(fr.RevTotal_12m,0))
             ELSE NULL END AS Eox_RevExpiredPct_12m,

        CASE WHEN ISNULL(fr.RevTotal_12m,0) > 0
             THEN TRY_CONVERT(decimal(9,4), ISNULL(fe.Eox_Rev_Risk12m,0) / NULLIF(fr.RevTotal_12m,0))
             ELSE NULL END AS Eox_RevRiskPct_12m,

        CASE WHEN ISNULL(fe.Eox_Rev_Expired_12m,0) > 0 THEN 1 ELSE 0 END AS Eox_HasExpiredRevenue_12m,
        CASE WHEN ISNULL(fe.Eox_Rev_Risk12m,0)   > 0 THEN 1 ELSE 0 END AS Eox_HasRiskRevenue_12m,
        CASE WHEN ISNULL(fr.RevTotal_12m,0) > 0
               AND ISNULL(fe.Eox_Rev_Expired_12m,0) >= 0.5 * ISNULL(fr.RevTotal_12m,0)
             THEN 1 ELSE 0 END AS Eox_MajorityExpired_12m,

        ISNULL(fe.Eox_SKU_CountExpired,0) AS Eox_SKU_CountExpired,
        ISNULL(fe.Eox_SKU_CountRisk12m,0) AS Eox_SKU_CountRisk12m,

        CASE WHEN fe.MinDaysToLDOS BETWEEN 0 AND 365 THEN 1 ELSE 0 END AS Risk_EoX_12m,
        CASE WHEN fe.MinDaysToLDOS BETWEEN 0 AND 182 THEN 1 ELSE 0 END AS Risk_EoX_6m,
        CASE WHEN fe.MinDaysToLDOS BETWEEN 0 AND 91  THEN 1 ELSE 0 END AS Risk_EoX_3m,
        CASE WHEN fe.MinDaysToLDOS < 0 THEN 1 ELSE 0 END AS PredChurn_EoXExpired,

        -- churn-event fields are not used; force safe defaults
        0 AS KnownChurn_Effective,
        0 AS UpcomingChurn_90d,
        0 AS KnownChurn_Confirmed,

        -- churn definition heuristic: no orders >90 AND no active maintenance contract
        CASE
            WHEN fo.Recency_Orders_Days IS NOT NULL
             AND fo.Recency_Orders_Days > 90
             AND ISNULL(mc.MaintContractActive,0) = 0
            THEN 1 ELSE 0
        END AS ChurnIf_NoOrd90,

        CASE WHEN fi.Recency_Invoices_Days IS NOT NULL AND fi.Recency_Invoices_Days > 90 THEN 1 ELSE 0 END AS ChurnIf_NoInv90,

        CASE
            WHEN fo.Recency_MaintOrders_Days IS NOT NULL
             AND fo.Recency_MaintOrders_Days > 90
             AND EXISTS (
                 SELECT 1
                 FROM #Orders mo
                 WHERE mo.CustomerId = c.CustomerId
                   AND mo.IsMaintenance = 1
                   AND mo.ContractEndDate IS NOT NULL
                   AND @AsOfDate BETWEEN mo.ContractEndDate AND DATEADD(DAY,30, mo.ContractEndDate)
             )
            THEN 1 ELSE 0
        END AS ChurnIf_NoMaintOrd90_WithinGrace,

        CASE WHEN ISNULL(far.UnpaidInv_90plus_Count,0) >= 1 THEN 1 ELSE 0 END AS PredChurn_Unpaid90plus,
        CASE WHEN ISNULL(fb.BackorderCount_180dPlus,0) >= 5 THEN 1 ELSE 0 END AS PredChurn_HighBackorders,

        CASE WHEN ISNULL(fe.Eox_Rev_Expired_12m,0) > 0 AND ISNULL(fo.Recency_Orders_Days,0) > 90 THEN 1 ELSE 0 END AS EoxExpired_NoOrders90d,

        CASE
            WHEN ISNULL(fe.Eox_Rev_Risk12m,0) > 0
             AND ISNULL(fo.Recency_MaintOrders_Days,0) > 90
             AND EXISTS (
                 SELECT 1
                 FROM #Orders mo2
                 WHERE mo2.CustomerId = c.CustomerId
                   AND mo2.IsMaintenance = 1
                   AND mo2.ContractEndDate IS NOT NULL
                   AND @AsOfDate BETWEEN mo2.ContractEndDate AND DATEADD(DAY,30, mo2.ContractEndDate)
             )
            THEN 1 ELSE 0
        END AS EoxRisk_NoMaintRenewal,

        CASE WHEN ISNULL(fe.Eox_Rev_Expired_12m,0) > 0 AND ISNULL(far.UnpaidInv_90plus_Count,0) >= 1 THEN 1 ELSE 0 END AS EoxExpired_UnpaidInv90plus,
        CASE WHEN ISNULL(fe.Eox_Rev_Expired_12m,0) > 0 AND ISNULL(TRY_CONVERT(decimal(18,2), fi.CreditValue_90d),0) > 0 THEN 1 ELSE 0 END AS EoxExpired_CreditSpike90d

    INTO #Snapshot
    FROM #Customers c
    LEFT JOIN #FeatOrders        fo  ON fo.CustomerId = c.CustomerId
    LEFT JOIN #FeatMaintContract mc  ON mc.CustomerId = c.CustomerId
    LEFT JOIN #FeatInvoices      fi  ON fi.CustomerId = c.CustomerId
    LEFT JOIN #FeatAR            far ON far.CustomerId = c.CustomerId
    LEFT JOIN #FeatBackorders    fb  ON fb.CustomerId = c.CustomerId
    LEFT JOIN #FeatRev12m        fr  ON fr.CustomerId = c.CustomerId
    LEFT JOIN #FeatEox           fe  ON fe.CustomerId = c.CustomerId;

    --------------------------------------------------------------------
    -- 5) Overwrite this month’s snapshot
    --------------------------------------------------------------------
    DELETE FROM chrn01.CustomerSnapshot
    WHERE SnapshotDate = @SnapshotDate;

    INSERT INTO chrn01.CustomerSnapshot
    (
        SnapshotDate,
        CustomerId,

        Recency_Orders_Days,
        Recency_MaintOrders_Days,
        Recency_Invoices_Days,

        OrderValue_90d,
        OrderValue_Prev90d,
        OrderFreq_90d,
        OrderFreq_Prev90d,
        MaintOrderValue_90d,
        ProdOrderValue_90d,

        BackorderCount_180dPlus,
        UnpaidInv_OverTerms_Count,
        UnpaidInv_90plus_Count,
        ARBucket_Current_Count,
        ARBucket_1_30_Count,
        ARBucket_31_60_Count,
        ARBucket_61_90_Count,
        ARBucket_91_120_Count,
        ARBucket_121_180_Count,
        ARBucket_180Plus_Count,
        DSO_OpenInvoices,
        ReturnInvCount_90d,
        CreditValue_90d,
        CreditCount_90d,

        MaintContractActive,

        OrderValue_3m_Change,
        OrderValue_3m_ChangePct,
        OrderFreq_3m_Change,
        OrderFreq_3m_ChangePct,

        ProdRatio_90d,
        MaintRatio_90d,
        IsProductHeavy_90d,
        IsMaintHeavy_90d,

        Eox_MinDaysToLDOS,
        Eox_MinLDOSDate,
        Eox_Rev_Expired_12m,
        Eox_Rev_Risk12m,
        Eox_Rev_Total12m,
        Eox_RevExpiredPct_12m,
        Eox_RevRiskPct_12m,
        Eox_HasExpiredRevenue_12m,
        Eox_HasRiskRevenue_12m,
        Eox_MajorityExpired_12m,
        Eox_SKU_CountExpired,
        Eox_SKU_CountRisk12m,
        Risk_EoX_12m,
        Risk_EoX_6m,
        Risk_EoX_3m,
        PredChurn_EoXExpired,

        KnownChurn_Effective,
        UpcomingChurn_90d,
        KnownChurn_Confirmed,

        ChurnIf_NoOrd90,
        ChurnIf_NoInv90,
        ChurnIf_NoMaintOrd90_WithinGrace,
        PredChurn_Unpaid90plus,
        PredChurn_HighBackorders,

        EoxExpired_NoOrders90d,
        EoxRisk_NoMaintRenewal,
        EoxExpired_UnpaidInv90plus,
        EoxExpired_CreditSpike90d
    )
    SELECT
        SnapshotDate,
        CustomerId,

        Recency_Orders_Days,
        Recency_MaintOrders_Days,
        Recency_Invoices_Days,

        OrderValue_90d,
        OrderValue_Prev90d,
        OrderFreq_90d,
        OrderFreq_Prev90d,
        MaintOrderValue_90d,
        ProdOrderValue_90d,

        BackorderCount_180dPlus,
        UnpaidInv_OverTerms_Count,
        UnpaidInv_90plus_Count,
        ARBucket_Current_Count,
        ARBucket_1_30_Count,
        ARBucket_31_60_Count,
        ARBucket_61_90_Count,
        ARBucket_91_120_Count,
        ARBucket_121_180_Count,
        ARBucket_180Plus_Count,
        DSO_OpenInvoices,
        ReturnInvCount_90d,
        CreditValue_90d,
        CreditCount_90d,

        MaintContractActive,

        OrderValue_3m_Change,
        OrderValue_3m_ChangePct,
        OrderFreq_3m_Change,
        OrderFreq_3m_ChangePct,

        ProdRatio_90d,
        MaintRatio_90d,
        IsProductHeavy_90d,
        IsMaintHeavy_90d,

        Eox_MinDaysToLDOS,
        Eox_MinLDOSDate,
        Eox_Rev_Expired_12m,
        Eox_Rev_Risk12m,
        Eox_Rev_Total12m,
        Eox_RevExpiredPct_12m,
        Eox_RevRiskPct_12m,
        Eox_HasExpiredRevenue_12m,
        Eox_HasRiskRevenue_12m,
        Eox_MajorityExpired_12m,
        Eox_SKU_CountExpired,
        Eox_SKU_CountRisk12m,
        Risk_EoX_12m,
        Risk_EoX_6m,
        Risk_EoX_3m,
        PredChurn_EoXExpired,

        KnownChurn_Effective,
        UpcomingChurn_90d,
        KnownChurn_Confirmed,

        ChurnIf_NoOrd90,
        ChurnIf_NoInv90,
        ChurnIf_NoMaintOrd90_WithinGrace,
        PredChurn_Unpaid90plus,
        PredChurn_HighBackorders,

        EoxExpired_NoOrders90d,
        EoxRisk_NoMaintRenewal,
        EoxExpired_UnpaidInv90plus,
        EoxExpired_CreditSpike90d
    FROM #Snapshot;

END;
GO

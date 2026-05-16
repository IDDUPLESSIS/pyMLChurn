SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID(N'chrn01.sp_LabelCustomerSnapshotTargets', N'P') IS NOT NULL
    DROP PROCEDURE [chrn01].[sp_LabelCustomerSnapshotTargets];
GO

CREATE PROCEDURE [chrn01].[sp_LabelCustomerSnapshotTargets]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Today date = CAST(GETDATE() AS date);

    DECLARE @MaxLabelDate_90  date = DATEADD(DAY, -90,  @Today);
    DECLARE @MaxLabelDate_180 date = DATEADD(DAY, -180, @Today);
    DECLARE @MaxLabelDate_365 date = DATEADD(DAY, -365, @Today);

    PRINT 'Labelling churn targets (3m, 6m, 12m) using sustained inactivity + maintenance protection + forward recovery logic...';
    PRINT CONCAT(
        'Today=', CONVERT(varchar(10), @Today, 120),
        ' | MaxLabelDate90=',  CONVERT(varchar(10), @MaxLabelDate_90, 120),
        ' | MaxLabelDate180=', CONVERT(varchar(10), @MaxLabelDate_180, 120),
        ' | MaxLabelDate365=', CONVERT(varchar(10), @MaxLabelDate_365, 120)
    );

    /*
        Business definition implemented here
        ------------------------------------
        A customer is churned at horizon H only when ALL of the following are true:

        1) Inactive at t0:
           - no qualifying PRODUCT invoice activity in the prior H days, including SnapshotDate
             window = (SnapshotDate - H, SnapshotDate]

        2) Not protected by maintenance at t0:
           - MaintContractActive = 0 on CustomerSnapshot

        3) No recovery after t0:
           - no qualifying PRODUCT invoice activity in the forward H days
             window = (SnapshotDate, SnapshotDate + H]
           - and no MAINTENANCE renewal / recovery in the forward H days
             window = (SnapshotDate, SnapshotDate + H]

        Notes
        -----
        - PRODUCT activity is inferred from ZSD_REPBILL rows where Sales Doc length = 6,
          non-cancelled, non-return, positive net value.
        - MAINTENANCE recovery is inferred from ZSD_REPORD_ORDER_INTAKE rows where
          Order Number length = 8, company US01, not rejected, document date in forward window,
          and Contract Item End Date extends beyond SnapshotDate.
        - Temporary inactivity is therefore NOT churn if the customer later recovers.
    */

    ;WITH BaseSnapshots AS
    (
        SELECT
            s.CustomerId,
            s.SnapshotDate,
            ISNULL(s.MaintContractActive, 0) AS MaintContractActive
        FROM chrn01.CustomerSnapshot s
    ),

    ----------------------------------------------------------------
    -- Prior product inactivity windows
    -- Product activity = positive, non-return, non-cancelled product invoice
    ----------------------------------------------------------------
    PriorProduct90 AS
    (
        SELECT
            bs.CustomerId,
            bs.SnapshotDate,
            COUNT_BIG(*) AS ProductActivityCount_Prior90
        FROM BaseSnapshots bs
        JOIN dbo.ZSD_REPBILL i
          ON i.[Sold-To] = bs.CustomerId
         AND i.[TxDate] >  DATEADD(DAY, -90, bs.SnapshotDate)
         AND i.[TxDate] <= bs.SnapshotDate
         AND i.[Sales Org] = 'US01'
         AND ISNULL(i.[Bill# Doc# Cancelled], '') = ''
         AND ISNULL(i.[Returns item], '') <> 'X'
         AND LEN(LTRIM(RTRIM(CAST(i.[Sales Doc#] AS varchar(50))))) = 6
         AND (
                i.[Doc# Net Value] *
                CASE WHEN i.[+/-] = '-' THEN -1 ELSE 1 END
             ) > 0
        GROUP BY
            bs.CustomerId,
            bs.SnapshotDate
    ),
    PriorProduct180 AS
    (
        SELECT
            bs.CustomerId,
            bs.SnapshotDate,
            COUNT_BIG(*) AS ProductActivityCount_Prior180
        FROM BaseSnapshots bs
        JOIN dbo.ZSD_REPBILL i
          ON i.[Sold-To] = bs.CustomerId
         AND i.[TxDate] >  DATEADD(DAY, -180, bs.SnapshotDate)
         AND i.[TxDate] <= bs.SnapshotDate
         AND i.[Sales Org] = 'US01'
         AND ISNULL(i.[Bill# Doc# Cancelled], '') = ''
         AND ISNULL(i.[Returns item], '') <> 'X'
         AND LEN(LTRIM(RTRIM(CAST(i.[Sales Doc#] AS varchar(50))))) = 6
         AND (
                i.[Doc# Net Value] *
                CASE WHEN i.[+/-] = '-' THEN -1 ELSE 1 END
             ) > 0
        GROUP BY
            bs.CustomerId,
            bs.SnapshotDate
    ),
    PriorProduct365 AS
    (
        SELECT
            bs.CustomerId,
            bs.SnapshotDate,
            COUNT_BIG(*) AS ProductActivityCount_Prior365
        FROM BaseSnapshots bs
        JOIN dbo.ZSD_REPBILL i
          ON i.[Sold-To] = bs.CustomerId
         AND i.[TxDate] >  DATEADD(DAY, -365, bs.SnapshotDate)
         AND i.[TxDate] <= bs.SnapshotDate
         AND i.[Sales Org] = 'US01'
         AND ISNULL(i.[Bill# Doc# Cancelled], '') = ''
         AND ISNULL(i.[Returns item], '') <> 'X'
         AND LEN(LTRIM(RTRIM(CAST(i.[Sales Doc#] AS varchar(50))))) = 6
         AND (
                i.[Doc# Net Value] *
                CASE WHEN i.[+/-] = '-' THEN -1 ELSE 1 END
             ) > 0
        GROUP BY
            bs.CustomerId,
            bs.SnapshotDate
    ),

    ----------------------------------------------------------------
    -- Forward product recovery windows
    ----------------------------------------------------------------
    RecoveryProduct90 AS
    (
        SELECT
            bs.CustomerId,
            bs.SnapshotDate,
            COUNT_BIG(*) AS ProductRecoveryCount_90
        FROM BaseSnapshots bs
        JOIN dbo.ZSD_REPBILL i
          ON i.[Sold-To] = bs.CustomerId
         AND i.[TxDate] >  bs.SnapshotDate
         AND i.[TxDate] <= DATEADD(DAY, 90, bs.SnapshotDate)
         AND i.[Sales Org] = 'US01'
         AND ISNULL(i.[Bill# Doc# Cancelled], '') = ''
         AND ISNULL(i.[Returns item], '') <> 'X'
         AND LEN(LTRIM(RTRIM(CAST(i.[Sales Doc#] AS varchar(50))))) = 6
         AND (
                i.[Doc# Net Value] *
                CASE WHEN i.[+/-] = '-' THEN -1 ELSE 1 END
             ) > 0
        GROUP BY
            bs.CustomerId,
            bs.SnapshotDate
    ),
    RecoveryProduct180 AS
    (
        SELECT
            bs.CustomerId,
            bs.SnapshotDate,
            COUNT_BIG(*) AS ProductRecoveryCount_180
        FROM BaseSnapshots bs
        JOIN dbo.ZSD_REPBILL i
          ON i.[Sold-To] = bs.CustomerId
         AND i.[TxDate] >  bs.SnapshotDate
         AND i.[TxDate] <= DATEADD(DAY, 180, bs.SnapshotDate)
         AND i.[Sales Org] = 'US01'
         AND ISNULL(i.[Bill# Doc# Cancelled], '') = ''
         AND ISNULL(i.[Returns item], '') <> 'X'
         AND LEN(LTRIM(RTRIM(CAST(i.[Sales Doc#] AS varchar(50))))) = 6
         AND (
                i.[Doc# Net Value] *
                CASE WHEN i.[+/-] = '-' THEN -1 ELSE 1 END
             ) > 0
        GROUP BY
            bs.CustomerId,
            bs.SnapshotDate
    ),
    RecoveryProduct365 AS
    (
        SELECT
            bs.CustomerId,
            bs.SnapshotDate,
            COUNT_BIG(*) AS ProductRecoveryCount_365
        FROM BaseSnapshots bs
        JOIN dbo.ZSD_REPBILL i
          ON i.[Sold-To] = bs.CustomerId
         AND i.[TxDate] >  bs.SnapshotDate
         AND i.[TxDate] <= DATEADD(DAY, 365, bs.SnapshotDate)
         AND i.[Sales Org] = 'US01'
         AND ISNULL(i.[Bill# Doc# Cancelled], '') = ''
         AND ISNULL(i.[Returns item], '') <> 'X'
         AND LEN(LTRIM(RTRIM(CAST(i.[Sales Doc#] AS varchar(50))))) = 6
         AND (
                i.[Doc# Net Value] *
                CASE WHEN i.[+/-] = '-' THEN -1 ELSE 1 END
             ) > 0
        GROUP BY
            bs.CustomerId,
            bs.SnapshotDate
    ),

    ----------------------------------------------------------------
    -- Forward maintenance recovery windows
    -- We treat a forward maintenance order / renewal as recovery when:
    --   - it is a maintenance doc (len = 8)
    --   - it occurs in the forward window
    --   - it is not rejected
    --   - its contract end date extends beyond SnapshotDate
    ----------------------------------------------------------------
    RecoveryMaint90 AS
    (
        SELECT
            bs.CustomerId,
            bs.SnapshotDate,
            COUNT_BIG(*) AS MaintRecoveryCount_90
        FROM BaseSnapshots bs
        JOIN dbo.ZSD_REPORD_ORDER_INTAKE o
          ON o.[Sold To] = bs.CustomerId
         AND o.[Document Date] >  bs.SnapshotDate
         AND o.[Document Date] <= DATEADD(DAY, 90, bs.SnapshotDate)
         AND o.[Company] = 'US01'
         AND ISNULL(o.[Reject Reason Code], '') = ''
         AND LEN(LTRIM(RTRIM(CAST(o.[Order Number] AS varchar(50))))) = 8
         AND o.[Contract Item End Date] IS NOT NULL
         AND o.[Contract Item End Date] > bs.SnapshotDate
        GROUP BY
            bs.CustomerId,
            bs.SnapshotDate
    ),
    RecoveryMaint180 AS
    (
        SELECT
            bs.CustomerId,
            bs.SnapshotDate,
            COUNT_BIG(*) AS MaintRecoveryCount_180
        FROM BaseSnapshots bs
        JOIN dbo.ZSD_REPORD_ORDER_INTAKE o
          ON o.[Sold To] = bs.CustomerId
         AND o.[Document Date] >  bs.SnapshotDate
         AND o.[Document Date] <= DATEADD(DAY, 180, bs.SnapshotDate)
         AND o.[Company] = 'US01'
         AND ISNULL(o.[Reject Reason Code], '') = ''
         AND LEN(LTRIM(RTRIM(CAST(o.[Order Number] AS varchar(50))))) = 8
         AND o.[Contract Item End Date] IS NOT NULL
         AND o.[Contract Item End Date] > bs.SnapshotDate
        GROUP BY
            bs.CustomerId,
            bs.SnapshotDate
    ),
    RecoveryMaint365 AS
    (
        SELECT
            bs.CustomerId,
            bs.SnapshotDate,
            COUNT_BIG(*) AS MaintRecoveryCount_365
        FROM BaseSnapshots bs
        JOIN dbo.ZSD_REPORD_ORDER_INTAKE o
          ON o.[Sold To] = bs.CustomerId
         AND o.[Document Date] >  bs.SnapshotDate
         AND o.[Document Date] <= DATEADD(DAY, 365, bs.SnapshotDate)
         AND o.[Company] = 'US01'
         AND ISNULL(o.[Reject Reason Code], '') = ''
         AND LEN(LTRIM(RTRIM(CAST(o.[Order Number] AS varchar(50))))) = 8
         AND o.[Contract Item End Date] IS NOT NULL
         AND o.[Contract Item End Date] > bs.SnapshotDate
        GROUP BY
            bs.CustomerId,
            bs.SnapshotDate
    ),

    Labels AS
    (
        SELECT
            bs.CustomerId,
            bs.SnapshotDate,

            ----------------------------------------------------------------
            -- 90d intermediate flags
            ----------------------------------------------------------------
            CASE WHEN ISNULL(pp90.ProductActivityCount_Prior90, 0) = 0 THEN 1 ELSE 0 END AS Target_Inactive_NoOrders_90d,
            CASE WHEN bs.MaintContractActive = 1 THEN 1 ELSE 0 END AS Target_MaintProtected_T0_90d,
            CASE WHEN ISNULL(rp90.ProductRecoveryCount_90, 0) > 0 THEN 1 ELSE 0 END AS Target_Recovered_ByOrder_90d,
            CASE WHEN ISNULL(rm90.MaintRecoveryCount_90, 0) > 0 THEN 1 ELSE 0 END AS Target_Recovered_ByMaint_90d,

            ----------------------------------------------------------------
            -- 180d intermediate flags
            ----------------------------------------------------------------
            CASE WHEN ISNULL(pp180.ProductActivityCount_Prior180, 0) = 0 THEN 1 ELSE 0 END AS Target_Inactive_NoOrders_180d,
            CASE WHEN bs.MaintContractActive = 1 THEN 1 ELSE 0 END AS Target_MaintProtected_T0_180d,
            CASE WHEN ISNULL(rp180.ProductRecoveryCount_180, 0) > 0 THEN 1 ELSE 0 END AS Target_Recovered_ByOrder_180d,
            CASE WHEN ISNULL(rm180.MaintRecoveryCount_180, 0) > 0 THEN 1 ELSE 0 END AS Target_Recovered_ByMaint_180d,

            ----------------------------------------------------------------
            -- 365d intermediate flags
            ----------------------------------------------------------------
            CASE WHEN ISNULL(pp365.ProductActivityCount_Prior365, 0) = 0 THEN 1 ELSE 0 END AS Target_Inactive_NoOrders_365d,
            CASE WHEN bs.MaintContractActive = 1 THEN 1 ELSE 0 END AS Target_MaintProtected_T0_365d,
            CASE WHEN ISNULL(rp365.ProductRecoveryCount_365, 0) > 0 THEN 1 ELSE 0 END AS Target_Recovered_ByOrder_365d,
            CASE WHEN ISNULL(rm365.MaintRecoveryCount_365, 0) > 0 THEN 1 ELSE 0 END AS Target_Recovered_ByMaint_365d

        FROM BaseSnapshots bs
        LEFT JOIN PriorProduct90    pp90  ON pp90.CustomerId  = bs.CustomerId AND pp90.SnapshotDate  = bs.SnapshotDate
        LEFT JOIN PriorProduct180   pp180 ON pp180.CustomerId = bs.CustomerId AND pp180.SnapshotDate = bs.SnapshotDate
        LEFT JOIN PriorProduct365   pp365 ON pp365.CustomerId = bs.CustomerId AND pp365.SnapshotDate = bs.SnapshotDate
        LEFT JOIN RecoveryProduct90 rp90  ON rp90.CustomerId  = bs.CustomerId AND rp90.SnapshotDate  = bs.SnapshotDate
        LEFT JOIN RecoveryProduct180 rp180 ON rp180.CustomerId = bs.CustomerId AND rp180.SnapshotDate = bs.SnapshotDate
        LEFT JOIN RecoveryProduct365 rp365 ON rp365.CustomerId = bs.CustomerId AND rp365.SnapshotDate = bs.SnapshotDate
        LEFT JOIN RecoveryMaint90   rm90  ON rm90.CustomerId  = bs.CustomerId AND rm90.SnapshotDate  = bs.SnapshotDate
        LEFT JOIN RecoveryMaint180  rm180 ON rm180.CustomerId = bs.CustomerId AND rm180.SnapshotDate = bs.SnapshotDate
        LEFT JOIN RecoveryMaint365  rm365 ON rm365.CustomerId = bs.CustomerId AND rm365.SnapshotDate = bs.SnapshotDate
    ),

    FinalLabels AS
    (
        SELECT
            l.CustomerId,
            l.SnapshotDate,

            CASE
                WHEN l.SnapshotDate > @MaxLabelDate_90 THEN NULL
                WHEN l.Target_Inactive_NoOrders_90d = 1
                 AND l.Target_MaintProtected_T0_90d = 0
                 AND (CASE WHEN l.Target_Recovered_ByOrder_90d = 1 OR l.Target_Recovered_ByMaint_90d = 1 THEN 1 ELSE 0 END) = 0
                THEN 1
                ELSE 0
            END AS Target_IsChurned_3mAhead,

            CASE
                WHEN l.SnapshotDate > @MaxLabelDate_180 THEN NULL
                WHEN l.Target_Inactive_NoOrders_180d = 1
                 AND l.Target_MaintProtected_T0_180d = 0
                 AND (CASE WHEN l.Target_Recovered_ByOrder_180d = 1 OR l.Target_Recovered_ByMaint_180d = 1 THEN 1 ELSE 0 END) = 0
                THEN 1
                ELSE 0
            END AS Target_IsChurned_6mAhead,

            CASE
                WHEN l.SnapshotDate > @MaxLabelDate_365 THEN NULL
                WHEN l.Target_Inactive_NoOrders_365d = 1
                 AND l.Target_MaintProtected_T0_365d = 0
                 AND (CASE WHEN l.Target_Recovered_ByOrder_365d = 1 OR l.Target_Recovered_ByMaint_365d = 1 THEN 1 ELSE 0 END) = 0
                THEN 1
                ELSE 0
            END AS Target_IsChurned_12mAhead

        FROM Labels l
    )

    UPDATE cs
    SET
        cs.Target_IsChurned_3mAhead  = fl.Target_IsChurned_3mAhead,
        cs.Target_IsChurned_6mAhead  = fl.Target_IsChurned_6mAhead,
        cs.Target_IsChurned_12mAhead = fl.Target_IsChurned_12mAhead
    FROM chrn01.CustomerSnapshot cs
    JOIN FinalLabels fl
      ON fl.CustomerId   = cs.CustomerId
     AND fl.SnapshotDate = cs.SnapshotDate;

    PRINT 'Labelling complete.';
END;
GO

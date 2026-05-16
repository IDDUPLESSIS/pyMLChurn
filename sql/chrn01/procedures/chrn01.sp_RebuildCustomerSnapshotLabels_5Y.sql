SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID(N'chrn01.sp_RebuildCustomerSnapshotLabels_5Y', N'P') IS NOT NULL
    DROP PROCEDURE [chrn01].[sp_RebuildCustomerSnapshotLabels_5Y];
GO

CREATE PROCEDURE [chrn01].[sp_RebuildCustomerSnapshotLabels_5Y]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @FiveYearsAgoStart date = DATEFROMPARTS(YEAR(GETDATE()) - 5, 1, 1);

    PRINT CONCAT('Rebuilding snapshot labels (purchase-gap churn) from ',
                 CONVERT(varchar(10), @FiveYearsAgoStart, 120), ' onwards.');

    --------------------------------------------------------------------
    -- Rebuild labels for the entire active window (definition changed)
    --------------------------------------------------------------------
    DELETE FROM chrn01.CustomerSnapshotLabels
    WHERE SnapshotDate >= @FiveYearsAgoStart;

    --------------------------------------------------------------------
    -- Build a compact purchase calendar (invoice-positive only)
    -- Rationale: "bought products" should be actual posted revenue, not orders.
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#Tx') IS NOT NULL DROP TABLE #Tx;

    SELECT
        b.[Sold-To]              AS CustomerId,
        CAST(b.[TxDate] AS date) AS TxDate
    INTO #Tx
    FROM dbo.ZSD_REPBILL b
    WHERE b.[Sales Org] = 'US01'
      AND ISNULL(b.[Bill# Doc# Cancelled], '') = ''
      AND b.[TxDate] >= @FiveYearsAgoStart
      AND (b.[Doc# Net Value] * CASE WHEN b.[+/-] = '-' THEN -1 ELSE 1 END) > 0;  -- positive revenue only

    CREATE INDEX IX_Tx_CustDate ON #Tx(CustomerId, TxDate);

    ;WITH Snap AS
    (
        SELECT s.SnapshotDate, s.CustomerId
        FROM chrn01.CustomerSnapshot s
        WHERE s.SnapshotDate >= @FiveYearsAgoStart
    )
    INSERT INTO chrn01.CustomerSnapshotLabels
    (
        SnapshotDate,
        CustomerId,
        Label_Churn_90d,
        Label_Churn_180d,
        Label_HasAnyChurn
    )
    SELECT
        s.SnapshotDate,
        s.CustomerId,

        ----------------------------------------------------------------
        -- Churn @ (t0+90): no purchases since t0
        -- (equivalently: no purchase in (t0, t0+90])
        ----------------------------------------------------------------
        CASE
            WHEN a1.FirstTxAfterT0 IS NULL THEN 1
            WHEN a1.FirstTxAfterT0 > DATEADD(DAY, 90, s.SnapshotDate) THEN 1
            ELSE 0
        END AS Label_Churn_90d,

        ----------------------------------------------------------------
        -- Churn @ (t0+180): no purchases since (t0+90)
        -- (equivalently: no purchase in (t0+90, t0+180])
        ----------------------------------------------------------------
        CASE
            WHEN a2.FirstTxAfterT0P90 IS NULL THEN 1
            WHEN a2.FirstTxAfterT0P90 > DATEADD(DAY, 180, s.SnapshotDate) THEN 1
            ELSE 0
        END AS Label_Churn_180d,

        ----------------------------------------------------------------
        -- Diagnostic: near-term churn in either horizon
        ----------------------------------------------------------------
        CASE
            WHEN
              (
                CASE
                    WHEN a1.FirstTxAfterT0 IS NULL THEN 1
                    WHEN a1.FirstTxAfterT0 > DATEADD(DAY, 90, s.SnapshotDate) THEN 1
                    ELSE 0
                END
              ) = 1
              OR
              (
                CASE
                    WHEN a2.FirstTxAfterT0P90 IS NULL THEN 1
                    WHEN a2.FirstTxAfterT0P90 > DATEADD(DAY, 180, s.SnapshotDate) THEN 1
                    ELSE 0
                END
              ) = 1
            THEN 1 ELSE 0
        END AS Label_HasAnyChurn

    FROM Snap s
    OUTER APPLY (
        SELECT MIN(t.TxDate) AS FirstTxAfterT0
        FROM #Tx t
        WHERE t.CustomerId = s.CustomerId
          AND t.TxDate > s.SnapshotDate
    ) a1
    OUTER APPLY (
        SELECT MIN(t.TxDate) AS FirstTxAfterT0P90
        FROM #Tx t
        WHERE t.CustomerId = s.CustomerId
          AND t.TxDate > DATEADD(DAY, 90, s.SnapshotDate)
    ) a2;

END;
GO

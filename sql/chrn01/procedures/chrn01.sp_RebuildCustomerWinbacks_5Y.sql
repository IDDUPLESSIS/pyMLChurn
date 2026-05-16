SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID(N'chrn01.sp_RebuildCustomerWinbacks_5Y', N'P') IS NOT NULL
    DROP PROCEDURE [chrn01].[sp_RebuildCustomerWinbacks_5Y];
GO
CREATE   PROCEDURE chrn01.sp_RebuildCustomerWinbacks_5Y
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @FiveYearsAgoStart date = DATEFROMPARTS(YEAR(GETDATE()) - 5, 1, 1);

    PRINT CONCAT('Rebuilding winback events from ', 
                 CONVERT(varchar(10), @FiveYearsAgoStart, 120), ' onwards...');

    -- Wipe old winbacks
    DELETE FROM chrn01.CustomerWinbackEvents
    WHERE ChurnCutoff < @FiveYearsAgoStart;

    ;WITH Churn AS (
        SELECT
            CustomerId,
            COALESCE(ChurnEffectiveDate, ChurnEventDate) AS ChurnCutoff
        FROM chrn01.CustomerChurnEvents
        WHERE COALESCE(ChurnEffectiveDate, ChurnEventDate) >= @FiveYearsAgoStart
    ),
    Tx AS (
        SELECT
            [Sold To]      AS CustomerId,
            [Document Date] AS TxDate
        FROM dbo.ZSD_REPORD_ORDER_INTAKE
        UNION ALL
        SELECT
            [Sold-To]      AS CustomerId,
            [TxDate]       AS TxDate
        FROM dbo.ZSD_REPBILL
    ),
    FirstReturn AS (
        SELECT
            c.CustomerId,
            c.ChurnCutoff,
            MIN(t.TxDate) AS ReturnDate
        FROM Churn c
        JOIN Tx t
            ON t.CustomerId = c.CustomerId
           AND t.TxDate    > c.ChurnCutoff
        GROUP BY c.CustomerId, c.ChurnCutoff
    )
    INSERT INTO chrn01.CustomerWinbackEvents (
        CustomerId,
        ChurnCutoff,
        ReturnDate,
        DaysOut
    )
    SELECT
        f.CustomerId,
        f.ChurnCutoff,
        f.ReturnDate,
        DATEDIFF(DAY, f.ChurnCutoff, f.ReturnDate) AS DaysOut
    FROM FirstReturn f
    -- avoid duplicates
    WHERE NOT EXISTS (
        SELECT 1
        FROM chrn01.CustomerWinbackEvents w
        WHERE w.CustomerId  = f.CustomerId
          AND w.ChurnCutoff = f.ChurnCutoff
    );

END;
GO

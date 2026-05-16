SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID(N'chrn01.sp_BuildCustomerSnapshot', N'P') IS NOT NULL
    DROP PROCEDURE [chrn01].[sp_BuildCustomerSnapshot];
GO
CREATE   PROCEDURE chrn01.sp_BuildCustomerSnapshot
(
    @AsOfDate date = NULL   -- Optional. NULL = full mode; NOT NULL = single month
)
AS
BEGIN
    SET NOCOUNT ON;

    -------------------------------------------------------------------------
    -- 0. Determine run mode and normalize @AsOfDate
    -------------------------------------------------------------------------
    DECLARE @RunMode varchar(20);

    IF @AsOfDate IS NOT NULL
    BEGIN
        SET @RunMode = 'single';             -- rebuild only that month
    END
    ELSE
    BEGIN
        SET @RunMode = 'full';               -- build history + current month
        SET @AsOfDate = CAST(GETDATE() AS date);
    END;

    DECLARE @CurrentMonthEnd date = EOMONTH(@AsOfDate);

    PRINT CONCAT('RUN MODE: ', @RunMode, 
                 ' | AsOf = ', CONVERT(varchar(10), @AsOfDate, 120),
                 ' | CurrentMonthEnd = ', CONVERT(varchar(10), @CurrentMonthEnd, 120));

    -------------------------------------------------------------------------
    -- 1. SINGLE MODE: just build the month for @AsOfDate and exit
    -------------------------------------------------------------------------
    IF @RunMode = 'single'
    BEGIN
        EXEC chrn01.sp_InternalBuildSnapshotCore @AsOfDate = @AsOfDate;
        RETURN;
    END;


    -------------------------------------------------------------------------
    -- 2. FULL MODE: 
    --    - build missing months up to last full month-end
    --    - only for the last 5 calendar years
    --    - purge older snapshots
    --    - then ALWAYS overwrite current month
    -------------------------------------------------------------------------

    -------------------------------------------------------------------------
    -- 2.1 Find earliest data date from SAP tables
    -------------------------------------------------------------------------
    DECLARE @MinOrderDate     date;
    DECLARE @MinBackorderDate date;
    DECLARE @MinInvoiceDate   date;
    DECLARE @MinDataDate      date;

    SELECT @MinOrderDate = MIN([Document Date])
    FROM dbo.ZSD_REPORD_ORDER_INTAKE;

    SELECT @MinBackorderDate = MIN([Document Date])
    FROM dbo.ZSD_REPORD_BACKORDER;

    SELECT @MinInvoiceDate = MIN([TxDate])
    FROM dbo.ZSD_REPBILL;

    SELECT @MinDataDate = MIN(d)
    FROM (VALUES (@MinOrderDate),
                 (@MinBackorderDate),
                 (@MinInvoiceDate)) AS v(d);

    IF @MinDataDate IS NULL
    BEGIN
        RAISERROR('No source data found in SAP tables.', 16, 1);
        RETURN;
    END;

    -------------------------------------------------------------------------
    -- 2.2 Apply 5-calendar-year clamp
    --      Earliest allowed date = 1 Jan of (current year - 5)
    -------------------------------------------------------------------------
    DECLARE @FiveYearsAgoStart date = DATEFROMPARTS(YEAR(GETDATE()) - 5, 1, 1);
    DECLARE @EffectiveMinDate  date;

    SET @EffectiveMinDate = CASE 
                                WHEN @MinDataDate < @FiveYearsAgoStart 
                                     THEN @FiveYearsAgoStart
                                ELSE @MinDataDate
                            END;

    PRINT CONCAT('Raw earliest data date     = ', CONVERT(varchar(10), @MinDataDate, 120));
    PRINT CONCAT('5-year window start        = ', CONVERT(varchar(10), @FiveYearsAgoStart, 120));
    PRINT CONCAT('Effective earliest snap dt = ', CONVERT(varchar(10), @EffectiveMinDate, 120));

    -------------------------------------------------------------------------
    -- 2.3 Purge snapshots older than the 5-year window
    -------------------------------------------------------------------------
    PRINT CONCAT('Purging snapshots older than ', 
                 CONVERT(varchar(10), @FiveYearsAgoStart, 120), ' ...');

    DELETE FROM chrn01.CustomerSnapshot
    WHERE SnapshotDate < @FiveYearsAgoStart;

    -------------------------------------------------------------------------
    -- 2.4 Determine historical range: from effective min month-end 
    --     up to last completed month-end (NOT including current month)
    -------------------------------------------------------------------------
    DECLARE @FirstSnapshot    date = EOMONTH(@EffectiveMinDate, 0);
    DECLARE @LastFullMonthEnd date = EOMONTH(GETDATE(), -1);
    DECLARE @HistSnapshotDate date = @FirstSnapshot;

    PRINT CONCAT('First historical snapshot  = ', CONVERT(varchar(10), @FirstSnapshot, 120));
    PRINT CONCAT('Last full month-end        = ', CONVERT(varchar(10), @LastFullMonthEnd, 120));

    -------------------------------------------------------------------------
    -- 2.5 Loop historical months: build ONLY missing ones
    -------------------------------------------------------------------------
    WHILE @HistSnapshotDate <= @LastFullMonthEnd
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM chrn01.CustomerSnapshot
            WHERE SnapshotDate = @HistSnapshotDate
        )
        BEGIN
            PRINT CONCAT('Building historical month: ', 
                         CONVERT(varchar(10), @HistSnapshotDate, 120));

            -- For historical months, AsOfDate = that month-end
            EXEC chrn01.sp_InternalBuildSnapshotCore 
                 @AsOfDate = @HistSnapshotDate;
        END
        ELSE
        BEGIN
            PRINT CONCAT('Skipping existing historical month: ', 
                         CONVERT(varchar(10), @HistSnapshotDate, 120));
        END;

        SET @HistSnapshotDate = EOMONTH(@HistSnapshotDate, 1);
    END;


    -------------------------------------------------------------------------
    -- 2.6 ALWAYS overwrite the current month snapshot (using today as cutoff)
    -------------------------------------------------------------------------
    PRINT CONCAT('Building current month snapshot as of ',
                 CONVERT(varchar(10), @AsOfDate, 120),
                 ' (SnapshotDate = ',
                 CONVERT(varchar(10), @CurrentMonthEnd, 120), ').');

    EXEC chrn01.sp_InternalBuildSnapshotCore @AsOfDate = @AsOfDate;

END
GO

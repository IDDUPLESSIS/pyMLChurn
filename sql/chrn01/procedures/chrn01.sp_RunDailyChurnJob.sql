SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID(N'chrn01.sp_RunDailyChurnJob', N'P') IS NOT NULL
    DROP PROCEDURE [chrn01].[sp_RunDailyChurnJob];
GO

CREATE PROCEDURE [chrn01].[sp_RunDailyChurnJob]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime datetime = GETDATE();

    PRINT '=====================================================';
    PRINT 'Daily churn job started at ' + CONVERT(varchar(19), @StartTime, 120);
    PRINT '=====================================================';

    BEGIN TRY

        ---------------------------------------------------------
        -- 1) Build / refresh customer snapshots (features)
        ---------------------------------------------------------
        PRINT 'Step 1: Building customer snapshots...';
        EXEC chrn01.sp_BuildCustomerSnapshot;
        PRINT 'Step 1: Snapshots build completed.';

        ---------------------------------------------------------
        -- 2) Label snapshots with churn targets
        --    Company definition:
        --      "Churn = customer has NO PRODUCT purchases in last 90 days"
        --    Labels should be forward-looking (90/180/365 ahead),
        --    and must NOT depend on CustomerChurnEvents.
        ---------------------------------------------------------
        PRINT 'Step 2: Labelling churn targets (product-only churn rule)...';
        EXEC chrn01.sp_LabelCustomerSnapshotTargets_ProductOnly;
        PRINT 'Step 2: Target labelling completed.';

        ---------------------------------------------------------
        -- Done
        ---------------------------------------------------------
        DECLARE @EndTime datetime = GETDATE();

        PRINT '=====================================================';
        PRINT 'Daily churn job completed at ' + CONVERT(varchar(19), @EndTime, 120);
        PRINT 'Elapsed seconds: ' + CONVERT(varchar(20), DATEDIFF(SECOND, @StartTime, @EndTime));
        PRINT '=====================================================';

    END TRY
    BEGIN CATCH
        PRINT '*** Daily churn job FAILED ***';
        PRINT ERROR_MESSAGE();
        THROW; -- lets SQL Agent see the real failure
    END CATCH;
END;
GO

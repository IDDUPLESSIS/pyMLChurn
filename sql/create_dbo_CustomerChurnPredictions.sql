-- Creates the destination table used by pyMLChurn loads
-- Default load mode in the app is REPLACE (overwrites table each run)

IF OBJECT_ID(N'[dbo].[CustomerChurnPredictions]', N'U') IS NOT NULL
BEGIN
    DROP TABLE [dbo].[CustomerChurnPredictions];
END
GO

CREATE TABLE [dbo].[CustomerChurnPredictions]
(
    [CustomerId]                     INT            NULL,
    [SnapshotDate]                   DATE           NULL,
    [DaysSinceLastPurchaseToday]     INT            NULL,
    [ChurnedNowBusinessRule]         BIT            NULL,
    [WhyBusinessRule]                NVARCHAR(MAX)  NULL,
    [ChurnedWithin90DaysActual]      BIT            NULL,
    [WhyTheyChurnedActual]           NVARCHAR(MAX)  NULL,
    [PredictedToChurnNext90Days]     BIT            NULL,
    [ChurnProbabilityPctNext90Days]  DECIMAL(5,2)   NULL,
    [ChurnProbabilityNext90Days]     DECIMAL(9,6)   NULL,
    [WhyAtRiskPredicted]             NVARCHAR(MAX)  NULL,
    [CreatedOn]                      DATETIME       NULL
);
GO

-- Helpful index for lookups by customer + snapshot
CREATE NONCLUSTERED INDEX IX_CustomerChurnPredictions_Customer_Snapshot
ON [dbo].[CustomerChurnPredictions] ([CustomerId], [SnapshotDate]);
GO

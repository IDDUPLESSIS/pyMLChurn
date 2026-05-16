IF OBJECT_ID(N'chrn01.CustomerSnapshotLabels', N'U') IS NULL
BEGIN
CREATE TABLE [chrn01].[CustomerSnapshotLabels]
(
    [SnapshotDate] date NOT NULL,
    [CustomerId] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Label_Churn_90d] bit NOT NULL,
    [Label_Churn_180d] bit NOT NULL,
    [Label_HasAnyChurn] bit NOT NULL,
    CONSTRAINT [PK_CustomerSnapshotLabels] PRIMARY KEY CLUSTERED ([SnapshotDate] ASC, [CustomerId] ASC)
);
END
GO

IF OBJECT_ID(N'chrn01.CustomerChurnScores', N'U') IS NULL
BEGIN
CREATE TABLE [chrn01].[CustomerChurnScores]
(
    [CustomerId] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [SnapshotDate] date NOT NULL,
    [ModelVersion] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Score] decimal(5,4) NOT NULL,
    [ScoredOn] datetime2(7) NOT NULL,
    CONSTRAINT [PK_CustomerChurnScores] PRIMARY KEY CLUSTERED ([CustomerId] ASC, [SnapshotDate] ASC, [ModelVersion] ASC)
);
END
GO

IF OBJECT_ID(N'chrn01.CustomerChurnPredictions_ExeSmoke', N'U') IS NULL
BEGIN
CREATE TABLE [chrn01].[CustomerChurnPredictions_ExeSmoke]
(
    [CustomerId] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [SnapshotDate] date NULL,
    [RecencyOrdersDaysSnapshot] float NULL,
    [ChurnedNowBusinessRule] bit NULL,
    [WhyBusinessRule] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ChurnedWithin90DaysActual] int NULL,
    [WhyTheyChurnedActual] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [PredictedToChurnNext90Days] bit NULL,
    [ChurnProbabilityPctNext90Days] numeric(5,2) NULL,
    [ChurnProbabilityNext90Days] numeric(9,6) NULL,
    [WhyAtRiskPredicted] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [PredictedChurnMonthNext90Days] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Createdon] datetime NULL
);
END
GO

IF OBJECT_ID(N'chrn01.CustomerChurnPredictions', N'U') IS NULL
BEGIN
CREATE TABLE [chrn01].[CustomerChurnPredictions]
(
    [CustomerId] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [SnapshotDate] date NULL,
    [RecencyOrdersDaysSnapshot] float NULL,
    [ChurnedNowBusinessRule] bit NULL,
    [WhyBusinessRule] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ChurnedWithin90DaysActual] float NULL,
    [WhyTheyChurnedActual] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [PredictedToChurnNext90Days] bit NULL,
    [ChurnRiskScorePctNext90Days] float NULL,
    [ChurnRiskScoreNext90Days] float NULL,
    [PredictionValueType] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [WhyAtRiskPredicted] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [PredictedChurnMonthNext90Days] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Createdon] datetime NULL
);
END
GO

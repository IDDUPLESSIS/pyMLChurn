IF OBJECT_ID(N'chrn01.CustomerChurnPredictions', N'U') IS NULL
BEGIN
CREATE TABLE [chrn01].[CustomerChurnPredictions]
(
    [CustomerId] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [SnapshotDate] date NULL,
    [RecencyOrdersDaysSnapshot] float NULL,
    [LastOrderDate] date NULL,
    [ChurnedNowBusinessRule] bit NULL,
    [WhyBusinessRule] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [RawCommercialInactivityRisk] bit NULL,
    [RawCommercialInactivityRiskScore] float NULL,
    [ProtectedByMaintenanceContract] bit NULL,
    [MaintenanceProtectionScore] float NULL,
    [AdjustedBusinessRiskScore] float NULL,
    [WatchlistChurnRisk] bit NULL,
    [BusinessRiskStatus] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [BusinessRiskExplanation] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [MaintenanceProtectionLevel] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [RawBusinessRiskReasons] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ProtectionModifierReasons] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [RecencyValidationWarning] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [RecencyValidationStatus] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ChurnedWithin90DaysActual] float NULL,
    [WhyTheyChurnedActual] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [PredictedToChurnNext90Days] bit NULL,
    [ChurnRiskScorePctNext90Days] float NULL,
    [ChurnRiskScoreNext90Days] float NULL,
    [MlPredictionScoreNext90Days] float NULL,
    [MlPredictionScorePctNext90Days] float NULL,
    [PredictionValueType] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [WhyAtRiskPredicted] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [PredictedChurnMonthNext90Days] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Createdon] datetime NULL
);
END
GO

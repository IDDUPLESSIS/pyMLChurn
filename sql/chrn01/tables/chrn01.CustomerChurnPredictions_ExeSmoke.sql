IF OBJECT_ID(N'chrn01.CustomerChurnPredictions_ExeSmoke', N'U') IS NULL
BEGIN
CREATE TABLE [chrn01].[CustomerChurnPredictions_ExeSmoke]
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
    [ChurnedWithin90DaysActual] int NULL,
    [WhyTheyChurnedActual] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [PredictedToChurnNext90Days] bit NULL,
    [ChurnProbabilityPctNext90Days] numeric(5,2) NULL,
    [ChurnProbabilityNext90Days] numeric(9,6) NULL,
    [MlPredictionScoreNext90Days] float NULL,
    [MlPredictionScorePctNext90Days] float NULL,
    [WhyAtRiskPredicted] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [PredictedChurnMonthNext90Days] nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Createdon] datetime NULL
);
END
GO

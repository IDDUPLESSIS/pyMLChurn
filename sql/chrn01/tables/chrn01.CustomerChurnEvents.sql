IF OBJECT_ID(N'chrn01.CustomerChurnEvents', N'U') IS NULL
BEGIN
CREATE TABLE [chrn01].[CustomerChurnEvents]
(
    [CustomerId] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [ChurnEventDate] date NOT NULL,
    [ChurnEffectiveDate] date NULL,
    [SourceSystem] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ReasonCode] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Notes] nvarchar(1000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    CONSTRAINT [PK_CustomerChurnEvents] PRIMARY KEY CLUSTERED ([CustomerId] ASC, [ChurnEventDate] ASC)
);
END
GO

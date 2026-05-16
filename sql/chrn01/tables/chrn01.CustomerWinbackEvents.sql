IF OBJECT_ID(N'chrn01.CustomerWinbackEvents', N'U') IS NULL
BEGIN
CREATE TABLE [chrn01].[CustomerWinbackEvents]
(
    [CustomerId] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [ChurnCutoff] date NOT NULL,
    [ReturnDate] date NOT NULL,
    [DaysOut] int NOT NULL,
    CONSTRAINT [PK_CustomerWinbackEvents] PRIMARY KEY CLUSTERED ([CustomerId] ASC, [ChurnCutoff] ASC)
);
END
GO

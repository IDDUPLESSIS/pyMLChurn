IF OBJECT_ID(N'chrn01.CustomerChurnCadence_v1', N'U') IS NULL
BEGIN
CREATE TABLE [chrn01].[CustomerChurnCadence_v1]
(
    [customer_id] bigint NOT NULL,
    [t0] date NOT NULL,
    [recency_days] int NULL,
    [median_gap_days] int NULL,
    [p90_gap_days] int NULL,
    [cv_gap] float NULL,
    [in_renewal_grace] bit NULL,
    [rev_180d] decimal(18,2) NULL,
    [rev_returns_90d] decimal(18,2) NULL,
    [invoices_90d] bigint NULL,
    [credit_notes_90d] bigint NULL,
    [orders_pos_30d] decimal(18,2) NULL,
    [orders_neg_30d] decimal(18,2) NULL,
    [backorder_qty_30d] float NULL,
    [pct_change_3m] float NULL,
    [pct_change_6m] float NULL,
    [yoy_change_pct] float NULL,
    [credit_notes_prev_month] int NULL,
    [invoices_pos_prev_month] int NULL,
    [credit_notes_ma3] float NULL,
    [churned_hard90] bit NOT NULL,
    [threshold_days] int NULL,
    [churned_dynamic] bit NOT NULL,
    [is_maintenance_heavy] bit NULL,
    [maint_cycle_days] int NULL,
    [churn_reason_dynamic] nvarchar(4000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [churn_reason_code_dynamic] nvarchar(1000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [churn_reason_hard90] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [severity_score] float NULL,
    [lateness_component] float NULL,
    [credits_component] float NULL,
    [trend_component] float NULL,
    [mitigator_component] float NULL,
    [last_tx_date_inv] date NULL,
    [last_tx_date_ord] date NULL,
    [recency_days_inv] int NULL,
    [recency_days_ord] int NULL,
    [recency_source] varchar(8) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [t0_from_invoice] bit NULL,
    [t0_from_order] bit NULL,
    CONSTRAINT [PK_CustomerChurnCadence_v1] PRIMARY KEY CLUSTERED ([customer_id] ASC, [t0] ASC)
);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_CCC_v1_churn_dynamic' AND object_id = OBJECT_ID(N'chrn01.CustomerChurnCadence_v1'))
    CREATE NONCLUSTERED INDEX [IX_CCC_v1_churn_dynamic] ON [chrn01].[CustomerChurnCadence_v1] ([churned_dynamic] ASC, [t0] ASC) INCLUDE ([recency_days], [threshold_days], [severity_score], [customer_id]);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_CCC_v1_churn_hard90' AND object_id = OBJECT_ID(N'chrn01.CustomerChurnCadence_v1'))
    CREATE NONCLUSTERED INDEX [IX_CCC_v1_churn_hard90] ON [chrn01].[CustomerChurnCadence_v1] ([churned_hard90] ASC, [t0] ASC) INCLUDE ([customer_id]);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_CCC_v1_recent_flags' AND object_id = OBJECT_ID(N'chrn01.CustomerChurnCadence_v1'))
    CREATE NONCLUSTERED INDEX [IX_CCC_v1_recent_flags] ON [chrn01].[CustomerChurnCadence_v1] ([t0] ASC, [in_renewal_grace] ASC) INCLUDE ([recency_days], [churned_dynamic], [churned_hard90], [severity_score]);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_CCC_v1_severity' AND object_id = OBJECT_ID(N'chrn01.CustomerChurnCadence_v1'))
    CREATE NONCLUSTERED INDEX [IX_CCC_v1_severity] ON [chrn01].[CustomerChurnCadence_v1] ([severity_score] DESC, [t0] ASC) INCLUDE ([customer_id], [recency_days], [threshold_days], [churned_dynamic]);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_CCC_v1_trends' AND object_id = OBJECT_ID(N'chrn01.CustomerChurnCadence_v1'))
    CREATE NONCLUSTERED INDEX [IX_CCC_v1_trends] ON [chrn01].[CustomerChurnCadence_v1] ([pct_change_3m] ASC, [pct_change_6m] ASC, [yoy_change_pct] ASC) INCLUDE ([customer_id], [t0], [churned_dynamic], [severity_score]);
GO

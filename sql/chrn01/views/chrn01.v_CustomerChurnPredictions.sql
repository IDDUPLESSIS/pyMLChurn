SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID(N'chrn01.v_CustomerChurnPredictions', N'V') IS NOT NULL
    DROP VIEW [chrn01].[v_CustomerChurnPredictions];
GO




CREATE   VIEW [chrn01].[v_CustomerChurnPredictions]
AS
WITH base AS (
    SELECT
        cnm.[Domain],

        -- Display name exactly like you wanted: "SoldToPartyName (ChildExternalId)"
        CONCAT(
            ro.[SoldToPartyName] COLLATE SQL_Latin1_General_CP1_CI_AS,
            ' (',
            cnm.[ChildExternalId] COLLATE SQL_Latin1_General_CP1_CI_AS,
            ')'
        ) AS [ChildExternalId],

        ccp.[SnapshotDate],
        ccp.[RecencyOrdersDaysSnapshot],
        COALESCE(ccp.[LastOrderDate], ro.[LastOrderDate]) AS [LastOrderDate],
        ccp.[ChurnedNowBusinessRule],
        ccp.[WhyBusinessRule],
        ccp.[RawCommercialInactivityRisk],
        ccp.[RawCommercialInactivityRiskScore],
        ccp.[ProtectedByMaintenanceContract],
        ccp.[MaintenanceProtectionScore],
        ccp.[AdjustedBusinessRiskScore],
        ccp.[WatchlistChurnRisk],
        ccp.[BusinessRiskStatus],
        ccp.[BusinessRiskExplanation],
        ccp.[MaintenanceProtectionLevel],
        ccp.[RawBusinessRiskReasons],
        ccp.[ProtectionModifierReasons],
        ccp.[RecencyValidationWarning],
        ccp.[RecencyValidationStatus],
        ccp.[PredictedToChurnNext90Days],
        ccp.[ChurnRiskScoreNext90Days],
        ccp.[MlPredictionScoreNext90Days],
        ccp.[MlPredictionScorePctNext90Days],
        ccp.[PredictionValueType],
        ccp.[WhyAtRiskPredicted],
        ccp.[PredictedChurnMonthNext90Days]

    FROM [SAP].[chrn01].[CustomerChurnPredictions] AS ccp

    LEFT JOIN [MASTERDATA].[dbo].[MSTR_CompanyNameMapping] AS cnm
      ON cnm.[ChildExternalId] COLLATE SQL_Latin1_General_CP1_CI_AS
       = ccp.[CustomerId]      COLLATE SQL_Latin1_General_CP1_CI_AS

    RIGHT JOIN [IBA].[dbo].[v_RAI_SP1-8] AS sm
      ON cnm.[Domain] COLLATE SQL_Latin1_General_CP1_CI_AS
       = sm.[Domain]  COLLATE SQL_Latin1_General_CP1_CI_AS

    CROSS APPLY (
        SELECT TOP (1)
            TRY_CONVERT(datetime2(0), oi.[Creation Date]) AS [LastOrderDate],
            oi.[Sold To Party Name]                      AS [SoldToPartyName]
        FROM [SAP].[dbo].[ZSD_REPORD_ORDER_INTAKE] AS oi
        WHERE oi.[Sold To] COLLATE SQL_Latin1_General_CP1_CI_AS
            = cnm.[ChildExternalId] COLLATE SQL_Latin1_General_CP1_CI_AS
          AND oi.[Company] <> 'MX01'
          AND TRY_CONVERT(datetime2(0), oi.[Creation Date]) >= DATEADD(year, -1, GETDATE())
        ORDER BY TRY_CONVERT(datetime2(0), oi.[Creation Date]) DESC
    ) AS ro

    WHERE cnm.[Domain] IS NOT NULL
),
calc AS (
    SELECT
        b.*,

        -- MM-DD-YYYY label
        CASE
            WHEN b.LastOrderDate IS NULL THEN ''
            ELSE CONCAT(
                RIGHT('0' + CAST(MONTH(b.LastOrderDate) AS varchar(2)), 2), '-',
                RIGHT('0' + CAST(DAY(b.LastOrderDate) AS varchar(2)), 2), '-',
                CAST(YEAR(b.LastOrderDate) AS varchar(4))
            )
        END AS LastOrderLabel,

        -- ProbPct: business-facing adjusted risk, if 0..1 => *100 else keep
        CASE
            WHEN b.[AdjustedBusinessRiskScore] IS NULL THEN NULL
            WHEN TRY_CONVERT(float, b.[AdjustedBusinessRiskScore]) <= 1
                THEN TRY_CONVERT(float, b.[AdjustedBusinessRiskScore]) * 100.0
            ELSE TRY_CONVERT(float, b.[AdjustedBusinessRiskScore])
        END AS ProbPct,

        -- RiskSort: business status severity first
        CASE
            WHEN b.[BusinessRiskStatus] = 'Churned' THEN 0
            WHEN b.[BusinessRiskStatus] = 'Churn Risk' THEN 1
            WHEN b.[BusinessRiskStatus] = 'Protected But At Risk' THEN 2
            WHEN b.[BusinessRiskStatus] = 'Watchlist Risk' THEN 3
            WHEN b.[BusinessRiskStatus] = 'Data Quality Review' THEN 4
            WHEN b.[BusinessRiskStatus] = 'Protected' THEN 5
            WHEN b.[BusinessRiskStatus] = 'Healthy' THEN 6
            ELSE 7
        END AS RiskSort,

        -- ProbSort: higher adjusted business risk first
        CASE
            WHEN b.[AdjustedBusinessRiskScore] IS NULL THEN -1
            WHEN TRY_CONVERT(float, b.[AdjustedBusinessRiskScore]) <= 1
                THEN TRY_CONVERT(float, b.[AdjustedBusinessRiskScore]) * 100.0
            ELSE TRY_CONVERT(float, b.[AdjustedBusinessRiskScore])
        END AS ProbSort,

        -- Business risk status pill HTML
        CASE
            WHEN b.[BusinessRiskStatus] = 'Churned' THEN
                '<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:#7f1d1d;color:#fff;font-weight:700;font-family:Arial,Helvetica,sans-serif;font-size:12px;">CHURNED</span>'
            WHEN b.[BusinessRiskStatus] = 'Churn Risk' THEN
                '<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:#fee2e2;color:#991b1b;font-weight:700;font-family:Arial,Helvetica,sans-serif;font-size:12px;">CHURN RISK</span>'
            WHEN b.[BusinessRiskStatus] = 'Protected But At Risk' THEN
                '<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:#ffedd5;color:#9a3412;font-weight:700;font-family:Arial,Helvetica,sans-serif;font-size:12px;">PROTECTED RISK</span>'
            WHEN b.[BusinessRiskStatus] = 'Watchlist Risk' THEN
                '<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:#fef3c7;color:#92400e;font-weight:700;font-family:Arial,Helvetica,sans-serif;font-size:12px;">WATCHLIST</span>'
            WHEN b.[BusinessRiskStatus] = 'Data Quality Review' THEN
                '<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:#e0e7ff;color:#3730a3;font-weight:700;font-family:Arial,Helvetica,sans-serif;font-size:12px;">DATA REVIEW</span>'
            WHEN b.[BusinessRiskStatus] = 'Protected' THEN
                '<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:#dbeafe;color:#1d4ed8;font-weight:700;font-family:Arial,Helvetica,sans-serif;font-size:12px;">PROTECTED</span>'
            WHEN b.[BusinessRiskStatus] = 'Healthy' THEN
                '<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:#dcfce7;color:#166534;font-weight:700;font-family:Arial,Helvetica,sans-serif;font-size:12px;">HEALTHY</span>'
            ELSE
                CONCAT(
                    '<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:#e5e7eb;color:#374151;font-weight:700;font-family:Arial,Helvetica,sans-serif;font-size:12px;">',
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(CAST(b.BusinessRiskStatus AS varchar(200)),''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;'),
                    '</span>'
                )
        END AS RiskPillHtml
    FROM base b
),
prob AS (
    SELECT
        c.*,

        -- Adjusted business risk bar HTML
        CASE
            WHEN c.ProbPct IS NULL THEN '<span style="color:#666;font-family:Arial,Helvetica,sans-serif;">—</span>'
            ELSE
                CONCAT(
                    '<div style="min-width:70px;font-family:Arial,Helvetica,sans-serif;">',
                      '<div style="background:#e5e7eb;border-radius:4px;height:8px;">',
                        '<div style="width:',
                            CAST(CASE WHEN c.ProbPct < 0 THEN 0 WHEN c.ProbPct > 100 THEN 100 ELSE CAST(c.ProbPct AS int) END AS varchar(10)),
                            '%;background:',
                            CASE
                                WHEN c.ProbPct >= 70 THEN '#dc2626'
                                WHEN c.ProbPct >= 40 THEN '#f59e0b'
                                ELSE '#16a34a'
                            END,
                            ';height:8px;border-radius:4px;"></div>',
                      '</div>',
                      '<div style="font-size:10px;color:#666;text-align:right;line-height:13px;">',
                        CAST(CASE WHEN c.ProbPct < 0 THEN 0 WHEN c.ProbPct > 100 THEN 100 ELSE CAST(c.ProbPct AS int) END AS varchar(10)),
                        '%</div>',
                    '</div>'
                )
        END AS ProbBarHtml
    FROM calc c
),
drivers AS (
    SELECT
        p.*,

        -- Business drivers HTML (bullet lines, max 6, +N more)
        CASE
            WHEN NULLIF(LTRIM(RTRIM(CONCAT(
                    COALESCE(p.BusinessRiskExplanation,''),
                    CASE WHEN NULLIF(LTRIM(RTRIM(COALESCE(p.RawBusinessRiskReasons,''))), '') IS NULL THEN '' ELSE '; ' + p.RawBusinessRiskReasons END,
                    CASE WHEN NULLIF(LTRIM(RTRIM(COALESCE(p.ProtectionModifierReasons,''))), '') IS NULL THEN '' ELSE '; ' + p.ProtectionModifierReasons END,
                    CASE WHEN NULLIF(LTRIM(RTRIM(COALESCE(p.RecencyValidationWarning,''))), '') IS NULL THEN '' ELSE '; ' + p.RecencyValidationWarning END
                ))), '') IS NULL THEN
                '<span style="color:#666;font-family:Arial,Helvetica,sans-serif;">—</span>'
            ELSE
            (
                SELECT
                    CONCAT(
                        '<div style="font-size:11px;line-height:14px;color:#111;font-family:Arial,Helvetica,sans-serif;">',
                        COALESCE(lines.LinesHtml,''),
                        CASE
                            WHEN cnt.TotalParts > 6 THEN CONCAT('<div style="margin-top:2px;color:#666;">+', CAST(cnt.TotalParts - 6 AS varchar(10)), ' more</div>')
                            ELSE ''
                        END,
                        '</div>'
                    )
                FROM
                (
                    SELECT COUNT(*) AS TotalParts
                    FROM OPENJSON(
                        '["' + REPLACE(
                            REPLACE(
                                REPLACE(CONCAT(
                                    COALESCE(p.BusinessRiskExplanation,''),
                                    CASE WHEN NULLIF(LTRIM(RTRIM(COALESCE(p.RawBusinessRiskReasons,''))), '') IS NULL THEN '' ELSE '; ' + p.RawBusinessRiskReasons END,
                                    CASE WHEN NULLIF(LTRIM(RTRIM(COALESCE(p.ProtectionModifierReasons,''))), '') IS NULL THEN '' ELSE '; ' + p.ProtectionModifierReasons END,
                                    CASE WHEN NULLIF(LTRIM(RTRIM(COALESCE(p.RecencyValidationWarning,''))), '') IS NULL THEN '' ELSE '; ' + p.RecencyValidationWarning END
                                ), '\', '\\'),
                            '"', '\"'),
                        ';', '","') + '"]'
                    )
                    WHERE NULLIF(LTRIM(RTRIM([value])), '') IS NOT NULL
                ) cnt
                CROSS APPLY
                (
                    SELECT STRING_AGG(
                        CONCAT(
                            '<div style="margin:0 0 2px 0;">• ',
                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM([value])),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;'),
                            '</div>'
                        ),
                        ''
                    ) WITHIN GROUP (ORDER BY [key]) AS LinesHtml
                    FROM (
                        SELECT TOP (6) [key], [value]
                        FROM OPENJSON(
                            '["' + REPLACE(
                                REPLACE(
                                    REPLACE(CONCAT(
                                        COALESCE(p.BusinessRiskExplanation,''),
                                        CASE WHEN NULLIF(LTRIM(RTRIM(COALESCE(p.RawBusinessRiskReasons,''))), '') IS NULL THEN '' ELSE '; ' + p.RawBusinessRiskReasons END,
                                        CASE WHEN NULLIF(LTRIM(RTRIM(COALESCE(p.ProtectionModifierReasons,''))), '') IS NULL THEN '' ELSE '; ' + p.ProtectionModifierReasons END,
                                        CASE WHEN NULLIF(LTRIM(RTRIM(COALESCE(p.RecencyValidationWarning,''))), '') IS NULL THEN '' ELSE '; ' + p.RecencyValidationWarning END
                                    ), '\', '\\'),
                                '"', '\"'),
                            ';', '","') + '"]'
                        )
                        WHERE NULLIF(LTRIM(RTRIM([value])), '') IS NOT NULL
                        ORDER BY [key]
                    ) t
                ) lines
            )
        END AS DriversHtml
    FROM prob p
),
final AS (
    SELECT
        d.*,

        COUNT(*) OVER (PARTITION BY d.Domain) AS TotalRows,
        SUM(CASE WHEN d.WatchlistChurnRisk = 1 OR d.BusinessRiskStatus IN ('Churned','Churn Risk','Protected But At Risk','Watchlist Risk') THEN 1 ELSE 0 END) OVER (PARTITION BY d.Domain) AS RiskCount,
        AVG(CASE WHEN d.ProbPct IS NULL THEN NULL ELSE d.ProbPct END) OVER (PARTITION BY d.Domain) AS AvgProbPct
    FROM drivers d
)
SELECT
    f.*,

    -- Business risk takeaway sentence
    CASE
        WHEN f.TotalRows = 0 THEN 'No churn prediction rows returned.'
        ELSE CONCAT(
            '<strong>', CAST(f.RiskCount AS varchar(10)),
            ' of ', CAST(f.TotalRows AS varchar(10)),
            ' customers (', CAST(CAST(ROUND(100.0 * f.RiskCount / NULLIF(f.TotalRows,0), 0) AS int) AS varchar(10)),
            '%)</strong> are watchlist, churn-risk, or churned customers',
            CASE
                WHEN f.AvgProbPct IS NULL THEN ''
                ELSE CONCAT(', with an average adjusted business risk of <strong>', CAST(CAST(ROUND(f.AvgProbPct,0) AS int) AS varchar(10)), '%</strong>')
            END,
            '.'
        )
    END AS TakeawayHtml

FROM final f;
GO

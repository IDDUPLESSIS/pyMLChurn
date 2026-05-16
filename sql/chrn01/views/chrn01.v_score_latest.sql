SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID(N'chrn01.v_score_latest', N'V') IS NOT NULL
    DROP VIEW [chrn01].[v_score_latest];
GO

CREATE   VIEW [chrn01].[v_score_latest]
AS
WITH LatestSnap AS
(
    SELECT
        CustomerId,
        MAX(SnapshotDate) AS SnapshotDate
    FROM chrn01.CustomerSnapshot
    GROUP BY CustomerId
)
SELECT
    t.*
FROM chrn01.v_train_dataset AS t
JOIN LatestSnap AS ls
  ON t.CustomerId  = ls.CustomerId
 AND t.SnapshotDate = ls.SnapshotDate;
GO

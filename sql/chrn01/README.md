# SQL export for [chrn01]

Source database: SAP

| Type | Object | File |
| --- | --- | --- |
| SQL_STORED_PROCEDURE | [chrn01].[sp_BuildCustomerSnapshot] | `procedures/chrn01.sp_BuildCustomerSnapshot.sql` |
| SQL_STORED_PROCEDURE | [chrn01].[sp_InternalBuildSnapshotCore] | `procedures/chrn01.sp_InternalBuildSnapshotCore.sql` |
| SQL_STORED_PROCEDURE | [chrn01].[sp_LabelCustomerSnapshotTargets] | `procedures/chrn01.sp_LabelCustomerSnapshotTargets.sql` |
| SQL_STORED_PROCEDURE | [chrn01].[sp_RebuildCustomerSnapshotLabels_5Y] | `procedures/chrn01.sp_RebuildCustomerSnapshotLabels_5Y.sql` |
| SQL_STORED_PROCEDURE | [chrn01].[sp_RebuildCustomerWinbacks_5Y] | `procedures/chrn01.sp_RebuildCustomerWinbacks_5Y.sql` |
| SQL_STORED_PROCEDURE | [chrn01].[sp_RunDailyChurnJob] | `procedures/chrn01.sp_RunDailyChurnJob.sql` |
| USER_TABLE | [chrn01].[CustomerChurnCadence_v1] | `tables/chrn01.CustomerChurnCadence_v1.sql` |
| USER_TABLE | [chrn01].[CustomerChurnEvents] | `tables/chrn01.CustomerChurnEvents.sql` |
| USER_TABLE | [chrn01].[CustomerChurnPredictions] | `tables/chrn01.CustomerChurnPredictions.sql` |
| USER_TABLE | [chrn01].[CustomerChurnPredictions_ExeSmoke] | `tables/chrn01.CustomerChurnPredictions_ExeSmoke.sql` |
| USER_TABLE | [chrn01].[CustomerChurnPredictions_MetricsSmoke] | `tables/chrn01.CustomerChurnPredictions_MetricsSmoke.sql` |
| USER_TABLE | [chrn01].[CustomerChurnScores] | `tables/chrn01.CustomerChurnScores.sql` |
| USER_TABLE | [chrn01].[CustomerSnapshot] | `tables/chrn01.CustomerSnapshot.sql` |
| USER_TABLE | [chrn01].[CustomerSnapshotLabels] | `tables/chrn01.CustomerSnapshotLabels.sql` |
| USER_TABLE | [chrn01].[CustomerWinbackEvents] | `tables/chrn01.CustomerWinbackEvents.sql` |
| VIEW | [chrn01].[v_CustomerChurnPredictions] | `views/chrn01.v_CustomerChurnPredictions.sql` |
| VIEW | [chrn01].[v_score_latest] | `views/chrn01.v_score_latest.sql` |
| VIEW | [chrn01].[v_train_dataset] | `views/chrn01.v_train_dataset.sql` |

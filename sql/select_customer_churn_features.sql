-- Mirrors the SELECT used by the app to fetch model features
-- Use this for validation or ad-hoc analysis

SELECT
      [customer_id],
      CONVERT(varchar(10), [t0], 23) AS [as_of_date], -- yyyy-mm-dd
      [recency_days],
      [median_gap_days],
      [p90_gap_days],
      [cv_gap],
      [in_renewal_grace],
      [rev_180d],
      [rev_returns_90d],
      [invoices_90d],
      [credit_notes_90d],
      [orders_pos_30d],
      [orders_neg_30d],
      [backorder_qty_30d],
      [pct_change_3m],
      [pct_change_6m],
      [yoy_change_pct],
      [credit_notes_prev_month],
      [invoices_pos_prev_month],
      [credit_notes_ma3],
      [churned_hard90],
      [threshold_days],
      [is_maintenance_heavy],
      [maint_cycle_days],
      [severity_score],
      [lateness_component],
      [credits_component],
      [trend_component],
      [mitigator_component]
FROM [SAP].[dbo].[CustomerChurnCadence_v1];

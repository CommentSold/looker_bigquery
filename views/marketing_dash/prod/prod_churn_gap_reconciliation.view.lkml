view: prod_churn_gap_reconciliation {
  # ==========================================================================
  # Reconciles prod_subscription_churn."Subscription End" (all MRR-active
  # ends) against prod_paid_subscription_cancellations."Paid Subscriptions
  # Cancelled" (trial -> paid -> churned).
  #
  # GRAIN: one row per terminal subscription, bucketed into exactly one of
  # four mutually-exclusive categories. The four bucket measures sum to
  # Total Ends (Subscriptions) in every month, so a stacked column built
  # from them is guaranteed to reconcile — there is no arithmetic to trust.
  #
  # Attribution priority (a subscription failing several gates lands in the
  # first one it fails):
  #   1. never_collected -- invoice generated, no payment ever cleared
  #   2. no_trial        -- trial_end IS NULL, out of scope for trial->paid
  #   3. internal_email  -- test/internal domain, excluded by the paid view only
  #   4. paid_churn      -- the revenue-relevant number
  #
  # TWO DELIBERATE CHOICES, both so this view reproduces the live tiles
  # rather than quietly disagreeing with them:
  #   * The dunning CTE is UNBOUNDED (no 60-day window), matching production.
  #     It therefore inherits the same early-dating risk.
  #   * The Stripe cancellation timestamp uses MIN(SAFE_CAST(...)) instead of
  #     production's nondeterministic ANY_VALUE(...). Validated: Total Ends
  #     (Creators) matched prod_subscription_churn exactly for Oct 2025 -
  #     Aug 2026, and the paid bucket matched to zero residual.
  #
  # NOT RECONCILED before Oct 2025. The spine starts Jul 2025 to line up
  # with prod_subscription_churn, but only Oct onward has been verified.
  # ==========================================================================

  derived_table: {
    sql:
      WITH
      dunning AS (
        SELECT subscription_id, MIN(updated_at) AS dunning_end_ts
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
        WHERE is_deleted = FALSE
          AND status IN ('uncollectible', 'void')
        GROUP BY subscription_id
      ),

      -- Invoices representing a real billing attempt (excludes $0 trial invoices)
      billable AS (
      SELECT invoice_id, subscription_id
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      GROUP BY invoice_id, subscription_id
      HAVING MAX(amount_due) > 0
      ),

      first_paid AS (
      SELECT invoice_id, subscription_id, paid_at
      FROM (
      SELECT
      i.invoice_id,
      i.subscription_id,
      CAST(i.updated_at AS TIMESTAMP) AS paid_at,
      ROW_NUMBER() OVER (
      PARTITION BY i.invoice_id ORDER BY i.updated_at ASC
      ) AS rn
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice` i
      JOIN billable b
      ON  b.invoice_id      = i.invoice_id
      AND b.subscription_id = i.subscription_id
      WHERE i.is_deleted = FALSE
      AND i.status = 'paid'
      AND i.amount_paid > 0
      )
      WHERE rn = 1
      ),

      -- MRR-active gate used by prod_subscription_churn: invoice GENERATED.
      -- The looser of the two gates, and the origin of most of the gap.
      mrr_subs AS (
      SELECT DISTINCT subscription_id
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      AND amount_due > 0
      ),

      -- One row per subscription, latest state, one plan row.
      -- No trial filter and no email filter here: this must be the churn
      -- view's population, not the paid view's.
      latest_subscription AS (
      SELECT * EXCEPT (rn)
      FROM (
      SELECT
      t1.*,
      CONCAT(
      JSON_EXTRACT_SCALAR(plan, '$.productName'),
      ': ',
      JSON_EXTRACT_SCALAR(plan, '$.interval')
      ) AS plan_interval,
      ROW_NUMBER() OVER (
      PARTITION BY t1.subscription_id ORDER BY t1.updated_at DESC
      ) AS rn
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS plan
      WHERE t1.is_deleted = FALSE
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      )
      WHERE rn = 1
      ),

      flip AS (
      SELECT
      subscription_id,
      TIMESTAMP_SECONDS(
      MIN(SAFE_CAST(JSON_VALUE(subscription, '$.cancelledAt._seconds') AS INT64))
      ) AS cancelled_at_utc,
      MIN(updated_at) AS status_ts
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription`
      WHERE is_deleted = FALSE
      AND status IN ('unpaid', 'canceled')
      GROUP BY subscription_id
      ),

      flip_resolved AS (
      SELECT
      subscription_id,
      CASE
      WHEN cancelled_at_utc IS NULL THEN status_ts
      WHEN status_ts       IS NULL THEN cancelled_at_utc
      ELSE LEAST(cancelled_at_utc, status_ts)
      END AS status_flip_at
      FROM flip
      ),

      scored AS (
      SELECT
      s.subscription_id,
      s.user_id,
      s.status,
      s.plan_interval,
      CAST(s.trial_end          AS TIMESTAMP) AS trial_end,
      CAST(s.initial_start_date AS TIMESTAMP) AS initial_start_date,
      LOWER(pprof.email) AS email,
      d.dunning_end_ts,
      CASE
      WHEN s.status NOT IN ('unpaid', 'canceled') THEN NULL
      WHEN s.cancel_at_period_end IS TRUE
      THEN DATE(COALESCE(d.dunning_end_ts, s.current_period_end), 'America/New_York')
      ELSE DATE(
      COALESCE(f.status_flip_at, d.dunning_end_ts, s.cancelled_at),
      'America/New_York'
      )
      END AS effective_end_date
      FROM latest_subscription s
      INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` p
      ON p.user_id = s.user_id
      INNER JOIN mrr_subs m
      ON m.subscription_id = s.subscription_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = s.user_id
      LEFT JOIN flip_resolved f
      ON f.subscription_id = s.subscription_id
      LEFT JOIN dunning d
      ON d.subscription_id = s.subscription_id
      WHERE p.apps_pop_store = TRUE
      AND p.user_type IN ('seller', 'verifiedSeller')
      ),

      ends AS (
      SELECT *
      FROM scored
      WHERE effective_end_date IS NOT NULL
      AND effective_end_date <  CURRENT_DATE('America/New_York')
      AND effective_end_date >= DATE '2025-07-01'
      ),

      paid_after_floor AS (
      SELECT
      e.subscription_id,
      COUNT(DISTINCT fp.invoice_id) AS paid_invoice_count,
      MIN(fp.paid_at)               AS first_paid_at,
      MAX(fp.paid_at)               AS last_paid_at
      FROM ends e
      LEFT JOIN first_paid fp
      ON  fp.subscription_id = e.subscription_id
      AND fp.paid_at >= COALESCE(e.trial_end, e.initial_start_date)
      GROUP BY 1
      )

      SELECT
      e.subscription_id,
      e.user_id,
      e.status AS subscription_status,
      e.plan_interval,
      e.email,
      e.trial_end,
      e.initial_start_date,
      e.effective_end_date,
      DATE_TRUNC(e.effective_end_date, MONTH) AS month_bucket,
      COALESCE(pf.paid_invoice_count, 0) AS paid_invoice_count,
      pf.first_paid_at,
      pf.last_paid_at,

      CASE
      WHEN COALESCE(pf.paid_invoice_count, 0) = 0 THEN 'Never collected'
      WHEN e.trial_end IS NULL                    THEN 'No trial (legacy)'
      WHEN e.email IS NOT NULL
      AND REGEXP_CONTAINS(
      e.email,
      r'@(test\.com|example\.com|popshoplive\.com|pop\.store|commentsold\.com)$'
      )                                      THEN 'Internal / test'
      ELSE 'Paid churn'
      END AS gap_bucket,

      CASE
      WHEN COALESCE(pf.paid_invoice_count, 0) = 0 THEN 2
      WHEN e.trial_end IS NULL                    THEN 3
      WHEN e.email IS NOT NULL
      AND REGEXP_CONTAINS(
      e.email,
      r'@(test\.com|example\.com|popshoplive\.com|pop\.store|commentsold\.com)$'
      )                                      THEN 4
      ELSE 1
      END AS gap_bucket_sort,

      DATE_TRUNC(e.effective_end_date, MONTH)
      = DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH) AS is_partial_month

      FROM ends e
      LEFT JOIN paid_after_floor pf
      ON pf.subscription_id = e.subscription_id
      {% if month_filter._is_filtered %}
      WHERE {% condition month_filter %} TIMESTAMP(DATE_TRUNC(e.effective_end_date, MONTH)) {% endcondition %}
      {% endif %}
      ;;
  }

  # ——— Filters ———

  filter: month_filter {
    type: date
    convert_tz: no
    label: "Month (filter)"
    description: "Filter by churn month, America/New_York. Use 'is in range'. Optional."
  }

  # ——— Dimensions ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: ${TABLE}.subscription_id ;;
  }

  dimension_group: month {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.month_bucket ;;
    timeframes: [date, month, quarter, year]
    label: "Month"
    description: "Churn month, America/New_York. Same axis as prod_subscription_churn."
  }

  dimension_group: end {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.effective_end_date ;;
    timeframes: [date, week, month]
    label: "End Date"
    description: "Exact churn date. Same derivation as prod_subscription_churn."
  }

  dimension: gap_bucket {
    type: string
    sql: ${TABLE}.gap_bucket ;;
    order_by_field: gap_bucket_sort
    label: "Gap Bucket"
    description: "Paid churn = converted from trial, collected >=1 payment, then churned. Never collected = invoice generated but no payment ever cleared. No trial (legacy) = trial_end IS NULL, out of scope. Internal / test = internal domain, excluded by the paid view only."
  }

  dimension: gap_bucket_sort {
    type: number
    sql: ${TABLE}.gap_bucket_sort ;;
    hidden: yes
  }

  dimension: is_paid_churn {
    type: yesno
    sql: ${TABLE}.gap_bucket = 'Paid churn' ;;
    label: "Is Paid Churn"
    description: "Yes = this subscription is counted by prod_paid_subscription_cancellations."
  }

  dimension: is_partial_month {
    type: yesno
    sql: ${TABLE}.is_partial_month ;;
    label: "Is Partial Month"
    description: "Yes for the current month, which is incomplete because end dates on/after today are excluded. Filter to No for a clean trailing series."
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
    description: "Latest status: canceled or unpaid."
  }

  dimension: plan_interval {
    type: string
    sql: ${TABLE}.plan_interval ;;
    label: "Plan: Interval"
    description: "Product name and billing interval, e.g. 'Launch: month'. Matches prod_subscription_churn.plan_interval."
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: paid_invoice_count {
    type: number
    sql: ${TABLE}.paid_invoice_count ;;
    description: "Billable invoices paid on/after COALESCE(trial_end, initial_start_date). Zero means never collected."
  }

  dimension_group: first_paid_at {
    type: time
    timeframes: [raw, date, month]
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.first_paid_at ;;
  }

  dimension_group: trial_ends_at {
    type: time
    timeframes: [raw, date, month]
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.trial_end ;;
  }

  dimension_group: initial_start {
    type: time
    timeframes: [raw, date, month]
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.initial_start_date ;;
  }

  # ——— Measures: the four stack series (use these for the chart) ———
  # Listed in stack order. These four sum to total_ends_subs exactly.

  measure: paid_churn_subs {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [gap_bucket: "Paid churn"]
    label: "1. Paid Churn"
    description: "Converted trial -> paid, collected >=1 payment, then churned. Matches prod_paid_subscription_cancellations."
    drill_fields: [drilldown*]
  }

  measure: never_collected_subs {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [gap_bucket: "Never collected"]
    label: "2. Never Collected"
    description: "Trial ended, invoice issued, payment never cleared. Belongs on the trial funnel, not churn."
    drill_fields: [drilldown*]
  }

  measure: no_trial_subs {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [gap_bucket: "No trial (legacy)"]
    label: "3. No Trial (Legacy)"
    description: "trial_end IS NULL. Legacy, migrated, or comped subscriptions. Out of scope for trial -> paid -> churn."
    drill_fields: [drilldown*]
  }

  measure: internal_email_subs {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [gap_bucket: "Internal / test"]
    label: "4. Internal / Test"
    description: "Internal email domain. prod_subscription_churn does not exclude these; the paid view does."
    drill_fields: [drilldown*]
  }

  # ——— Measures: totals and rate ———

  measure: total_ends_subs {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    label: "Total Ends (Subscriptions)"
    description: "All MRR-active subscription ends. Equals the sum of the four bucket measures."
    drill_fields: [drilldown*]
  }

  measure: total_ends_users {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Total Ends (Creators)"
    description: "Distinct creators. This is the number prod_subscription_churn reports."
    drill_fields: [drilldown*]
  }

  measure: paid_churn_users {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [gap_bucket: "Paid churn"]
    label: "Paid Churn (Creators)"
  }

  measure: pct_ever_paid {
    type: number
    sql: SAFE_DIVIDE(${paid_churn_subs}, ${total_ends_subs}) ;;
    value_format_name: percent_1
    label: "% Ever Paid"
    description: "Share of subscription ends that had collected at least one payment."
  }

  # ——— Drill Set ———

  set: drilldown {
    fields: [
      month_month,
      end_date,
      gap_bucket,
      user_id,
      subscription_id,
      subscription_status,
      plan_interval,
      email,
      paid_invoice_count,
      first_paid_at_date,
      trial_ends_at_date,
      initial_start_date
    ]
  }
}

# =============================================================================
# prod_monthly_paid_subscribers
# -----------------------------------------------------------------------------
# Paying creators at each month end, against target.
#
# GRAIN: one row per (month, subscription_id). Future months with a target but
# no actuals appear as a single row with NULL subscription fields.
#
# -----------------------------------------------------------------------------
# WHAT WAS REBUILT AND WHY
# -----------------------------------------------------------------------------
# 1. SURVIVORSHIP IN THE YEARLY LEG. yearly_plans filtered
#    `status = 'active' AND current_period_end > CURRENT_DATETIME()` — CURRENT
#    state. Any yearly subscription since churned was absent from EVERY
#    historical month. Measured: 49 of 274 yearly subscriptions, 18%, missing
#    from all history. past_due was excluded too.
#
# 2. ONLY THE CURRENT PERIOD WAS EXTRAPOLATED. The window ran
#    current_period_start -> current_period_end, so a yearly subscriber in their
#    second year had their entire first year omitted.
#
# 3. "BILLED DURING THE MONTH" IS NOT "SUBSCRIBED AT MONTH END". monthly_invoices
#    counted invoices CREATED in the month. A creator billed on the 3rd who
#    churned on the 10th counted; one renewing on the 25th did not. This is also
#    why August read 457 against July's 981 — on the 13th, only ~13 days of
#    renewals had happened. Not a 53% collapse, a partial month. A point-in-time
#    span count has no such problem: a snapshot taken today is simply today's
#    number.
#
# 4. TWO HAND-ROLLED LEGS FOR ONE QUESTION. The monthly/yearly split existed
#    only because invoice-counting cannot see a yearly subscriber between
#    renewals. Spans handle both identically — a yearly subscription's span just
#    covers twelve months — so interval becomes an ordinary dimension.
#
# 5. NO TIMEZONE ON THE INVOICE DATE. EXTRACT(YEAR FROM inv.created) with no
#    zone and no COALESCE(created, created_at), so month boundaries sat four
#    hours off every other view.
#
# Actuals now use the span logic validated against Stripe's active-subscriber
# figures to 0, -1, -7, -4, -1, 0 across six month-ends (with internal accounts
# included; see the parameter).
#
# -----------------------------------------------------------------------------
# EXPECTED CHANGE TO PUBLISHED NUMBERS — TELL THE TARGET OWNER
# -----------------------------------------------------------------------------
#   month-end   old view   this view   Stripe
#   2026-02-28       246         ~221      229
#   2026-03-31       276         ~246      253
#   2026-04-30       390         ~374      410
#   2026-05-31       690         ~720      759
#   2026-06-30       882         ~853      876
#   2026-07-31       981         ~958      981
#
# The old view was above Stripe in three months and below in two with no
# consistent sign — two errors pulling opposite ways. July's % of target moves
# from ~106.9% to ~104.4% against the 918 target. Approximate because the old
# figures count creators and the validation counted subscriptions; verify on
# first run.
#
# -----------------------------------------------------------------------------
# !! THE Q1 2026 TARGETS LOOK LIKE A DIFFERENT METRIC !!
# -----------------------------------------------------------------------------
# Apr-Dec ramp steadily by ~180/month: 373, 504, 737, 918, 1077, 1255, 1440,
# 1624, 1803. Jan-Mar read 298, 734, 1262 — steps of 436 then 528, then a DROP
# of 889 into April. A March target of 1,262 against a March actual near 250 is
# not a miss, it is a different basis. Confirm with whoever set them before
# publishing Q1 variance. Until then, filter to 2026-04 onward.
#
# -----------------------------------------------------------------------------
# READING NOTES
# -----------------------------------------------------------------------------
# * Monthly and Yearly subscriber counts do NOT sum to the total. A creator
#   holding both a monthly and a yearly plan is counted in each.
# * Is Month Complete = No for the current month. The count is still a valid
#   point-in-time figure, but it is being compared against an END-of-month
#   target, so % of Target will understate until the month closes.
# * Future months carry a target and no actuals. The measures return NULL, not
#   zero, so the actual line stops rather than crashing to the axis — provided
#   the viz is set to leave gaps for missing values rather than plot them as 0.
#
# Inherits two open items so it agrees with prod_subscription_churn: the dunning
# lookback is unbounded, and current_period_end is overwritten to the
# cancellation instant on some cancellations.
# =============================================================================

view: prod_monthly_paid_subscribers {
  derived_table: {
    sql:
      WITH
      -- ══════════════════════════════════════════════════════════════════
      -- VALIDATED SPAN LOGIC — identical to prod_active_paid_subscribers
      -- ══════════════════════════════════════════════════════════════════
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

      dunning AS (
      SELECT subscription_id, MIN(updated_at) AS dunning_end_ts
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      AND status IN ('uncollectible', 'void')
      GROUP BY subscription_id
      ),

      -- MRR-active gate AND span start. amount_due > 0 drops $0 trial invoices
      -- and always-comped subscriptions, matching Stripe.
      subscription_mrr AS (
      SELECT
      subscription_id,
      MIN(DATE(COALESCE(created, created_at), 'America/New_York')) AS first_mrr_date
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      AND amount_due > 0
      GROUP BY subscription_id
      ),

      -- ONE SPAN PER SUBSCRIPTION, monthly and yearly alike. No status filter:
      -- that was the survivorship bug. Liveness on a date comes from the span.
      subscription_spans AS (
      SELECT
      t1.subscription_id,
      t1.user_id,
      JSON_EXTRACT_SCALAR(plan, '$.interval')    AS plan_interval,
      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      sm.first_mrr_date AS start_date,
      CASE
      WHEN t1.status NOT IN ('unpaid', 'canceled') THEN NULL   -- still open
      WHEN t1.cancel_at_period_end IS TRUE
      THEN DATE(COALESCE(d.dunning_end_ts, t1.current_period_end), 'America/New_York')
      ELSE DATE(
      COALESCE(f.status_flip_at, d.dunning_end_ts, t1.cancelled_at),
      'America/New_York'
      )
      END AS end_date
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS plan
      INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` p
      ON p.user_id = t1.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = t1.user_id
      INNER JOIN subscription_mrr sm ON sm.subscription_id = t1.subscription_id
      LEFT JOIN flip_resolved f      ON f.subscription_id = t1.subscription_id
      LEFT JOIN dunning d            ON d.subscription_id = t1.subscription_id
      WHERE t1.is_deleted = FALSE
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND p.apps_pop_store = TRUE
      AND p.user_type IN ('seller', 'verifiedSeller')
      {% if exclude_internal_accounts._parameter_value == 'yes' %}
      AND (pprof.email IS NULL OR NOT REGEXP_CONTAINS(LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'))
      {% endif %}
      ),

      -- Snapshot on the last day of the month, or today for the current month.
      -- Runs to 2027-12 so future targets have a row to attach to.
      month_spine AS (
      SELECT
      month_start,
      LEAST(LAST_DAY(month_start), CURRENT_DATE('America/New_York')) AS snapshot_date,
      LAST_DAY(month_start) <  CURRENT_DATE('America/New_York') AS is_month_complete,
      month_start > DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH) AS is_future_month
      FROM UNNEST(GENERATE_DATE_ARRAY('2025-01-01', '2027-12-01', INTERVAL 1 MONTH)) AS month_start
      ),

      targets AS (
      SELECT * FROM UNNEST([
      STRUCT(2026 AS year,  1 AS month_number,  298 AS paid_subscribers_target),
      STRUCT(2026,  2,  734),
      STRUCT(2026,  3, 1262),
      STRUCT(2026,  4,  373),
      STRUCT(2026,  5,  504),
      STRUCT(2026,  6,  737),
      STRUCT(2026,  7,  918),
      STRUCT(2026,  8, 1077),
      STRUCT(2026,  9, 1255),
      STRUCT(2026, 10, 1440),
      STRUCT(2026, 11, 1624),
      STRUCT(2026, 12, 1803)
      ])
      )

      SELECT
      ms.month_start,
      EXTRACT(YEAR  FROM ms.month_start) AS year,
      EXTRACT(MONTH FROM ms.month_start) AS month_number,
      ms.snapshot_date,
      ms.is_month_complete,
      ms.is_future_month,
      s.subscription_id,
      s.user_id,
      s.plan_interval,
      s.plan_name,
      t.paid_subscribers_target
      FROM month_spine ms
      -- LEFT so a future month keeps its target row with NULL subscription.
      LEFT JOIN subscription_spans s
      ON  ms.snapshot_date >= s.start_date
      AND (s.end_date IS NULL OR ms.snapshot_date < s.end_date)
      AND NOT ms.is_future_month
      LEFT JOIN targets t
      ON  t.year         = EXTRACT(YEAR  FROM ms.month_start)
      AND t.month_number = EXTRACT(MONTH FROM ms.month_start)
      WHERE s.subscription_id IS NOT NULL
      OR t.paid_subscribers_target IS NOT NULL
      ;;
  }

  # ——— Parameters ———

  parameter: exclude_internal_accounts {
    type: unquoted
    label: "Exclude Internal Accounts"
    default_value: "yes"
    description: "Yes = internal view, the default and the basis the targets were set on. No = matches Stripe, which applies no email filter. The difference is roughly 2-3%."
    allowed_value: { label: "Yes (internal view)" value: "yes" }
    allowed_value: { label: "No (match Stripe)"   value: "no"  }
  }

  # ——— Primary Key ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(CAST(${TABLE}.month_start AS STRING), '|',
      COALESCE(${TABLE}.subscription_id, 'no-subs')) ;;
  }

  # ——— Time ———

  dimension_group: month {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.month_start ;;
    timeframes: [date, month, quarter, year]
    label: "Month"
  }

  dimension: month_number {
    type: number
    sql: ${TABLE}.month_number ;;
    label: "Month Number"
  }

  dimension: year {
    type: number
    sql: ${TABLE}.year ;;
    label: "Year"
  }

  dimension_group: snapshot {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.snapshot_date ;;
    timeframes: [date]
    label: "Snapshot Date"
    description: "The day counted: last day of the month, or today for the current month."
  }

  dimension: is_month_complete {
    type: yesno
    sql: ${TABLE}.is_month_complete ;;
    label: "Is Month Complete"
    description: "No for the current month. The count is still a valid point-in-time figure — unlike the old invoice-based version, which genuinely under-read mid-month — but it is being compared to an END-of-month target, so % of Target will understate until the month closes."
  }

  dimension: is_future_month {
    type: yesno
    sql: ${TABLE}.is_future_month ;;
    label: "Is Future Month"
    description: "Target only, no actuals. Measures return NULL rather than zero, so the actual line stops instead of dropping to the axis."
  }

  # ——— Subscription ———

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
    hidden: yes
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
    label: "User ID"
  }

  dimension: plan_interval {
    type: string
    sql: ${TABLE}.plan_interval ;;
    label: "Interval"
    description: "month or year. A single dimension now — the old view maintained two separate CTEs because invoice-counting could not see a yearly subscriber between renewals."
  }

  dimension: plan_name {
    type: string
    sql: ${TABLE}.plan_name ;;
    label: "Product Name"
  }

  # ——— Actual Measures ———
  # NULL rather than zero on future months, so a line chart breaks cleanly.

  measure: total_paid_subscribers {
    type: number
    sql: IF(LOGICAL_OR(${TABLE}.is_future_month), NULL,
      COUNT(DISTINCT ${TABLE}.user_id)) ;;
    label: "Paid Creators on Platform: Actual"
    description: "Distinct creators with an MRR-active subscription on the snapshot date. Point-in-time, not billed-during-month. Validated against Stripe to within 0-7 per month."
    drill_fields: [detail*]
  }

  measure: total_paid_subscriptions {
    type: number
    sql: IF(LOGICAL_OR(${TABLE}.is_future_month), NULL,
      COUNT(DISTINCT ${TABLE}.subscription_id)) ;;
    label: "Paid Subscriptions: Actual"
    description: "Distinct subscriptions. Above the creator count where a creator holds more than one plan."
    drill_fields: [detail*]
  }

  measure: total_monthly_plan_subscribers {
    type: number
    sql: IF(LOGICAL_OR(${TABLE}.is_future_month), NULL,
      COUNT(DISTINCT IF(${TABLE}.plan_interval = 'month', ${TABLE}.user_id, NULL))) ;;
    label: "Monthly Plan Subscribers"
    description: "Creators on monthly billing. Does NOT sum with Yearly to the total — a creator holding both is counted in each."
    drill_fields: [detail*]
  }

  measure: total_yearly_plan_subscribers {
    type: number
    sql: IF(LOGICAL_OR(${TABLE}.is_future_month), NULL,
      COUNT(DISTINCT IF(${TABLE}.plan_interval = 'year', ${TABLE}.user_id, NULL))) ;;
    label: "Yearly Plan Subscribers"
    description: "Creators on yearly billing, counted from their span rather than extrapolated. The old version omitted 49 of 274 yearly subscriptions from all history and covered only the current billing period."
    drill_fields: [detail*]
  }

  # ——— Target ———

  measure: paid_subscribers_target {
    type: max
    sql: ${TABLE}.paid_subscribers_target ;;
    label: "Paid Creators on Platform: Target"
    description: "End-of-month target. Constant within a month, so MAX is exact when grouped by month — but do NOT group by Interval or Product as well, or the same target repeats on every row. NOTE the Jan-Mar 2026 targets (298/734/1262) appear to be on a different basis from Apr onward; confirm before publishing Q1 variance."
  }

  # ——— Variance ———

  measure: paid_subscribers_variance {
    type: number
    sql: ${total_paid_subscribers} - ${paid_subscribers_target} ;;
    label: "Variance to Target"
    description: "Actual minus target. NULL for future months."
  }

  measure: paid_subscribers_pct_of_target {
    type: number
    sql: SAFE_DIVIDE(${total_paid_subscribers}, NULLIF(${paid_subscribers_target}, 0)) ;;
    value_format_name: percent_1
    label: "% of Target"
    description: "Understates for the current month, which is compared against an end-of-month target. Filter Is Month Complete = Yes for a clean series."
  }

  # ——— Drill Set ———

  set: detail {
    fields: [
      month_month,
      snapshot_date,
      user_id,
      plan_name,
      plan_interval
    ]
  }
}

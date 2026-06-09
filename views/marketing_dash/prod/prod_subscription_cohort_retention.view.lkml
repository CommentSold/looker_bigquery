# =============================================================================
# prod_subscription_cohort_retention
# -----------------------------------------------------------------------------
# Cohort retention triangle: of the users who first became paying (MRR-active)
# in month X, how many are still paying in X+1, X+2, ... ?
#
# Built on the SAME definitions as prod_subscription_churn:
#   * A "start" = first invoice with amount_due > 0 (trial-conversion / first
#     bill), NOT initial_start_date (trial start).
#   * A subscription is MRR-active if it ever issued an amount_due > 0 invoice
#     (includes uncollectible/dunning; excludes $0-only trials).
#   * Churn = cancelled_at (the cancellation/MRR-drop date).
#
# COHORT      = month of a user's FIRST MRR-active subscription.
# RETAINED in month M = the user had >=1 MRR-active subscription that was active
#               in month M, where active spans [start_month .. churn_month - 1]
#               (STRICT / end-of-month: a user who churns in month M is NOT
#               counted active in M). This makes the diagonal (latest month of
#               each cohort, summed) equal the current active-subscriber count
#               and reconcile with Stripe (~734 vs Stripe 783; remaining gap is
#               never-billed trials Stripe also excludes).
#
#               Consequence of STRICT spans: month 0 can be < 100% for users who
#               start and churn in the same calendar month, and the cohort drop
#               aligns to the churn month (does NOT lag it). This was a
#               deliberate choice to match Stripe's active-subscriber count over
#               a clean 100% month-0. To revert to "active through churn month"
#               (clean 100% month-0, drop lags by one month), change the
#               user_active_months upper bound back to the churn month inclusive.
#
# GRAIN of the derived table: one row per (cohort_month, months_since).
# Reactivations resurrect a user: the DISTINCT union of spans means a user who
# churns then restarts shows inactive in the gap month(s) and active again
# after -- retention can legitimately tick back up.
# =============================================================================

view: prod_subscription_cohort_retention {
  derived_table: {
    sql:
      WITH base_subscriptions AS (
        SELECT
          t1.subscription_id,
          t1.user_id,
          t1.cancelled_at
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
        UNNEST(t1.plans) AS plan
        INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` p
          ON p.user_id = t1.user_id
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
          ON pprof.user_id = t1.user_id
        WHERE
          t1.is_deleted = FALSE
          AND p.apps_pop_store = TRUE
          AND p.user_type IN ('seller', 'verifiedSeller')
          AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
          -- Test-email exclusion ENABLED (matches the dashboard's current state).
          -- Comment out for a Stripe-matching (no-filter) view.
          AND (pprof.email IS NULL OR (
            LOWER(pprof.email) NOT LIKE '%@test.com'
            AND LOWER(pprof.email) NOT LIKE '%@example.com'
            AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
            AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
            AND LOWER(pprof.email) NOT LIKE '%@pop.store'
          ))
      ),

      -- First billable-invoice date per subscription (= MRR-active set).
      subscription_mrr AS (
      SELECT
      subscription_id,
      MIN(DATE(created_at)) AS first_mrr_date
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE amount_due > 0
      GROUP BY subscription_id
      ),

      -- Active span per subscription, in month buckets.
      -- end_month = churn month if cancelled, else NULL (open / still active).
      sub_spans AS (
      SELECT
      b.user_id,
      DATE_TRUNC(sm.first_mrr_date, MONTH) AS start_month,
      CASE
      WHEN b.cancelled_at IS NOT NULL
      AND DATE(b.cancelled_at) < CURRENT_DATE()
      THEN DATE_TRUNC(DATE(b.cancelled_at), MONTH)
      END AS churn_month
      FROM base_subscriptions b
      INNER JOIN subscription_mrr sm
      ON sm.subscription_id = b.subscription_id
      ),

      -- Each user's cohort = the month of their EARLIEST MRR-active start.
      user_cohort AS (
      SELECT
      user_id,
      MIN(start_month) AS cohort_month
      FROM sub_spans
      GROUP BY user_id
      ),

      cohort_sizes AS (
      SELECT
      cohort_month,
      COUNT(DISTINCT user_id) AS cohort_size
      FROM user_cohort
      GROUP BY cohort_month
      ),

      -- Explode every subscription span into the months it was active, then
      -- DISTINCT to the user level so overlapping subs don't double-count.
      -- STRICT spans: a cancelled sub is active in [start_month .. churn_month-1].
      --   * If churned: upper bound = churn_month minus 1 month (exclusive).
      --   * If open: upper bound = current month (still active now).
      -- LEAST() caps a future current period at the current month; GREATEST()
      -- guards against churn in the same month as start (yields an empty span,
      -- i.e. not counted active that month -> month-0 can dip below 100%).
      user_active_months AS (
      SELECT DISTINCT
      s.user_id,
      active_month
      FROM sub_spans s,
      UNNEST(GENERATE_DATE_ARRAY(
      s.start_month,
      CASE
      WHEN s.churn_month IS NULL
      THEN DATE_TRUNC(CURRENT_DATE(), MONTH)
      ELSE DATE_SUB(s.churn_month, INTERVAL 1 MONTH)
      END,
      INTERVAL 1 MONTH
      )) AS active_month
      ),

      -- One row per (cohort_month, active_month) the user was present in,
      -- only for active months at or after the cohort month.
      cohort_activity AS (
      SELECT
      uc.cohort_month,
      uam.active_month,
      DATE_DIFF(uam.active_month, uc.cohort_month, MONTH) AS months_since,
      uam.user_id
      FROM user_cohort uc
      INNER JOIN user_active_months uam
      ON uam.user_id = uc.user_id
      WHERE uam.active_month >= uc.cohort_month
      )

      SELECT
      ca.cohort_month,
      ca.months_since,
      DATE_ADD(ca.cohort_month, INTERVAL ca.months_since MONTH) AS activity_month,
      cs.cohort_size,
      COUNT(DISTINCT ca.user_id) AS retained_users,
      SAFE_DIVIDE(COUNT(DISTINCT ca.user_id), cs.cohort_size) AS retention_rate
      FROM cohort_activity ca
      INNER JOIN cohort_sizes cs
      ON cs.cohort_month = ca.cohort_month
      GROUP BY ca.cohort_month, ca.months_since, cs.cohort_size
      ORDER BY ca.cohort_month, ca.months_since
      ;;
  }

  # ——— Primary Key ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(CAST(${TABLE}.cohort_month AS STRING), '|', CAST(${TABLE}.months_since AS STRING)) ;;
  }

  # ——— Dimensions ———

  dimension_group: cohort {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.cohort_month ;;
    timeframes: [month, quarter, year]
    label: "Cohort (Signup Month)"
    description: "Month of the user's first MRR-active subscription"
  }

  dimension_group: activity {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.activity_month ;;
    timeframes: [month, quarter, year]
    label: "Activity Month"
    description: "Calendar month being measured (cohort month + months since)"
  }

  dimension: months_since {
    type: number
    sql: ${TABLE}.months_since ;;
    label: "Months Since Signup"
    description: "0 = the cohort/signup month itself, 1 = the next month, ..."
  }

  dimension: months_since_label {
    type: string
    order_by_field: months_since
    sql: ${TABLE}.months_since ;;
    html:
      {% if value == 0 %}Month 0 (signup){% else %}Month {{ value }}{% endif %} ;;
    label: "Cohort Age"
    description: "Worded version of Months Since Signup for a self-explaining pivot legend"
  }

  dimension: cohort_age_band {
    type: string
    order_by_field: months_since
    sql:
      CASE
        WHEN ${TABLE}.months_since = 0 THEN '0: signup month'
        WHEN ${TABLE}.months_since BETWEEN 1 AND 3 THEN '1-3 months'
        WHEN ${TABLE}.months_since BETWEEN 4 AND 6 THEN '4-6 months'
        WHEN ${TABLE}.months_since BETWEEN 7 AND 12 THEN '7-12 months'
        ELSE '13+ months'
      END ;;
    label: "Cohort Age Band"
  }

  dimension: cohort_size {
    type: number
    sql: ${TABLE}.cohort_size ;;
    label: "Cohort Size"
    description: "Distinct users who became MRR-active in the cohort month (constant within a cohort)"
  }

  dimension: retention_rate {
    type: number
    value_format_name: percent_1
    sql: ${TABLE}.retention_rate ;;
    label: "Retention Rate (row)"
    description: "Retained users / cohort size, precomputed per (cohort, months_since) row"
  }

  # ——— Measures ———
  # The natural cohort viz pivots rows = Cohort (Signup Month), columns =
  # Cohort Age / Months Since Signup, so each cell maps to exactly ONE derived-
  # table row and the measures below are exact. If you aggregate ACROSS
  # months_since without pivoting, retained_users sums incorrectly -- always
  # pivot or filter the age dimension.

  measure: retained_users {
    type: sum
    sql: ${TABLE}.retained_users ;;
    label: "Retained Users"
    description: "Distinct cohort users still MRR-active in the activity month"
  }

  measure: cohort_size_measure {
    type: max
    sql: ${TABLE}.cohort_size ;;
    label: "Cohort Size"
    description: "Constant per cohort; MAX is safe across a single cohort's rows"
  }

  measure: retention_rate_measure {
    type: average
    value_format_name: percent_1
    sql: ${TABLE}.retention_rate ;;
    label: "Retention Rate"
    description: "Use in the pivot (rows=cohort, cols=age); each cell is one row so AVG returns that row's exact rate"
  }
}

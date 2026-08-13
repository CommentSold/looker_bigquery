# =============================================================================
# prod_current_saas_subscriptions
# -----------------------------------------------------------------------------
# Live (non-terminal) subscriptions by plan and status, as of now.
#
# GRAIN: one row per subscription. fact_seller_subscription is UPSERT per
# subscription_id — one row, current state only, no history. Verified 1.0 rows
# per subscription, so no dedup is needed.
#
# Companion to prod_active_paid_subscribers, which is the same population as a
# daily time series. The two should tie; see RECONCILING THE TWO VIEWS below.
#
# -----------------------------------------------------------------------------
# STRIPE'S "ACTIVE SUBSCRIBERS" DEFINITION, established empirically
# -----------------------------------------------------------------------------
#   status IN ('active','past_due')  AND  MRR > 0  AND  NO email filter
#
# Trials excluded (zero MRR in Stripe's model), fully-discounted excluded,
# internal accounts INCLUDED. Measured 2026-08-12:
#
#   non-internal, active/past_due, MRR > 0     985
#   internal,     active/past_due, MRR > 0    + 24
#   -----------------------------------------------
#   Stripe-comparable                          1,009
#   Stripe "Active subscribers"                1,010
#
# To reproduce: Exclude Internal Accounts = No, and use the Stripe-Comparable
# Subscribers measure (or filter Is MRR Active = Yes and Is Zero MRR = No).
#
# -----------------------------------------------------------------------------
# TWO BUGS FIXED FROM THE ORIGINAL VERSION
# -----------------------------------------------------------------------------
# 1. `cancelled_at IS NULL` as the liveness test EXCLUDED 124 non-internal
#    subscriptions (142 including internal) with status = 'active',
#    cancel_at_period_end = true and a future period end. Those are billing now:
#    five were checked directly in Stripe, all showing the Active badge with a
#    "Cancels <date>" note. A ~14% undercount. Liveness is now by status.
#
# 2. My earlier `is_mrr_active` dimension was defined as status IN
#    ('active','past_due'), which only removes trials. Stripe also removes
#    fully-discounted subscriptions, so that dimension could not reach Stripe's
#    definition. Now split into Is MRR Active (status) and Is Zero MRR (price).
#
# !! THE OLD 1,010 = 1,010 MATCH WAS COINCIDENCE !!
# The original view read 1,010 and so did Stripe. Two different errors happened
# to land on the same number: the view dropped the scheduled cancellations while
# including trials and comped subscriptions. Do not treat that agreement as
# evidence the old logic was right.
#
# -----------------------------------------------------------------------------
# RECONCILING THE TWO VIEWS
# -----------------------------------------------------------------------------
# prod_active_paid_subscribers gates on "ever billed more than zero" (an
# invoice-history test) because prices are not retained historically. This view
# gates on TODAY's price, which is closer to Stripe's live definition.
#
# The two disagree for a subscription that paid full price and was later comped:
# ever-billed says yes, current price says zero. Both dimensions are exposed
# here so the gap is measurable rather than mysterious.
#
# Confirmed: no NULL-status rows exist in this population, so the status
# whitelist drops nothing the old cancelled_at filter would have kept.
#
# -----------------------------------------------------------------------------
# EXPECTED FIGURES (2026-08-12, Exclude Internal Accounts = Yes)
# -----------------------------------------------------------------------------
#   active, not scheduled to cancel      893
#   active, scheduled to cancel          124
#   past_due                              21
#   trialing                              93
#   ------------------------------------------
#   Subscription Count                 1,131
#   of which zero MRR (active+past_due)   53
#   Stripe-Comparable Subscribers        985   (1,009 with internal included)
# =============================================================================

view: prod_current_saas_subscriptions {
  derived_table: {
    sql:
      WITH ever_billed AS (
        -- Matches the gate in prod_active_paid_subscribers and
        -- prod_subscription_churn: at least one invoice that actually asked for
        -- money. Excludes $0 trial invoices.
        SELECT DISTINCT subscription_id
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
        WHERE is_deleted = FALSE
          AND amount_due > 0
      )

      SELECT
      t1.subscription_id,
      t1.user_id,
      t1.status,
      t1.cancel_at_period_end,
      t1.cancelled_at,
      t1.current_period_end,
      COALESCE(t1.discounted_price, t1.price + t1.tax_amount)      AS price,
      COALESCE(t1.discounted_price, t1.price + t1.tax_amount) = 0  AS is_zero_mrr,
      eb.subscription_id IS NOT NULL                               AS has_ever_billed,
      JSON_EXTRACT_SCALAR(plan, '$.productName') AS subscription_product_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval')    AS subscription_interval
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS plan
      INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` p
      ON p.user_id = t1.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = t1.user_id
      LEFT JOIN ever_billed eb
      ON eb.subscription_id = t1.subscription_id
      WHERE
      t1.is_deleted = FALSE
      -- Liveness by STATUS, not by cancelled_at. A scheduled cancellation has
      -- cancelled_at populated but keeps billing until the period ends.
      -- Terminal: canceled, unpaid. No NULL statuses exist here.
      AND t1.status IN ('active', 'trialing', 'past_due')
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND p.apps_pop_store = TRUE
      AND p.user_type IN ('seller', 'verifiedSeller')
      {% if exclude_internal_accounts._parameter_value == 'yes' %}
      AND (
      pprof.email IS NULL
      OR NOT REGEXP_CONTAINS(
      LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'
      )
      )
      {% endif %}
      ;;
  }

  # ——— Parameters ———

  parameter: exclude_internal_accounts {
    type: unquoted
    label: "Exclude Internal Accounts"
    default_value: "yes"
    description: "Yes = internal view, the default. No = required to match Stripe, which applies no email filter and counts internal accounts. Internal accounts are 49 live subscriptions as of 2026-08-12, of which 24 are MRR-active."
    allowed_value: { label: "Yes (internal view)" value: "yes" }
    allowed_value: { label: "No (match Stripe)"   value: "no"  }
  }

  # ——— Primary Key ———

  dimension: subscription_id {
    type: string
    primary_key: yes
    hidden: yes
    sql: ${TABLE}.subscription_id ;;
  }

  # ——— Dimensions ———

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
    hidden: yes
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    label: "Status"
    description: "active (includes scheduled cancellations still billing), trialing ($0 MRR), or past_due (live until explicitly cancelled). Terminal statuses — canceled, unpaid — are excluded."
  }

  dimension: is_scheduled_to_cancel {
    type: yesno
    sql: ${TABLE}.cancel_at_period_end IS TRUE ;;
    label: "Scheduled to Cancel"
    description: "Yes when the subscriber has cancelled but the paid period has not ended. Still billing and still counted — Stripe shows these as Active with a 'Cancels <date>' note. 124 non-internal as of 2026-08-12, and they were entirely missing from the original version of this tile."
  }

  dimension: is_mrr_active {
    type: yesno
    sql: ${TABLE}.status IN ('active', 'past_due') ;;
    label: "Is MRR Active"
    description: "No for trialing, which carries $0 MRR. NOT sufficient on its own to match Stripe — combine with Is Zero MRR = No, since Stripe also excludes fully-discounted subscriptions."
  }

  dimension: is_zero_mrr {
    type: yesno
    sql: ${TABLE}.is_zero_mrr ;;
    label: "Is Zero MRR"
    description: "Fully discounted at today's price. Stripe excludes these from its active-subscriber count. 53 non-internal among active/past_due as of 2026-08-12, 18 internal."
  }

  dimension: has_ever_billed {
    type: yesno
    sql: ${TABLE}.has_ever_billed ;;
    label: "Has Ever Billed"
    description: "At least one invoice with amount_due > 0. This is the gate prod_active_paid_subscribers uses, because historical prices are not retained. Differs from Is Zero MRR for a subscription that paid full price and was later comped — compare the two to reconcile this tile against the time series."
  }

  dimension: subscription_product_name {
    type: string
    sql: ${TABLE}.subscription_product_name ;;
    label: "Product Name"
  }

  dimension: subscription_interval {
    type: string
    sql: ${TABLE}.subscription_interval ;;
    label: "Interval"
  }

  dimension: plan_interval {
    type: string
    sql: CONCAT(${TABLE}.subscription_product_name, ': ', ${TABLE}.subscription_interval) ;;
    label: "Plan: Interval"
    description: "Product name and billing interval, e.g. 'Launch: month'"
  }

  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
    value_format_name: decimal_2
    label: "Price"
    description: "Current price including tax, after any discount. Point-in-time only — historical prices are not retained, so do not build a revenue trend on this."
  }

  dimension_group: cancelled_at {
    type: time
    sql: ${TABLE}.cancelled_at ;;
    timeframes: [date, week, month, year]
    label: "Cancelled"
    description: "When the subscriber requested cancellation. Populated only for scheduled cancellations — always NULL in the original version of this view, where those rows were filtered out entirely."
  }

  dimension_group: current_period_end {
    type: time
    sql: ${TABLE}.current_period_end ;;
    timeframes: [date, week, month, year]
    label: "Period Ends"
    description: "End of the paid period. For a scheduled cancellation this is when the subscription actually lapses, so it forecasts near-term churn. Caution: current_period_end is overwritten to the cancellation instant on some cancellations — an open issue on prod_subscription_churn — so treat it as indicative for terminal subscriptions."
  }

  dimension: status_detail {
    type: string
    sql:
      CASE
        WHEN ${TABLE}.status = 'active' AND ${TABLE}.cancel_at_period_end IS TRUE
          THEN 'active (cancelling)'
        ELSE ${TABLE}.status
      END ;;
    label: "Status Detail"
    description: "Splits active into renewing vs already scheduled to cancel. Same population as Status — 'active (cancelling)' subscriptions are still billing."
  }

  # ——— Measures ———

  measure: count_distinct_user_id {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Distinct Subscribers"
    description: "Distinct creators with a live subscription. Verified 1:1 with subscriptions as of 2026-08-12, but recheck before summing across a stacked chart."
    drill_fields: [detail*]
  }

  measure: count_subscriptions {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    label: "Subscription Count"
    description: "Distinct live subscriptions, all statuses. 1,131 non-internal as of 2026-08-12."
    drill_fields: [detail*]
  }

  measure: stripe_comparable_subscribers {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_mrr_active: "yes", is_zero_mrr: "no"]
    label: "Stripe-Comparable Subscribers"
    description: "MRR-active and not fully discounted — Stripe's definition. ONLY matches Stripe when Exclude Internal Accounts = No: 1,009 against Stripe's 1,010 on 2026-08-12. With the internal filter on it reads 985."
    drill_fields: [detail*]
  }

  measure: scheduled_to_cancel_count {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_scheduled_to_cancel: "yes"]
    label: "Scheduled to Cancel"
    description: "Live subscriptions that will lapse at period end — churn already committed, with known dates. Arguably the most actionable number on this tile."
    drill_fields: [detail*]
  }

  measure: trialing_count {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [status: "trialing"]
    label: "Trialing"
    description: "Live trials, $0 MRR. Excluded from Stripe's active-subscriber count."
    drill_fields: [detail*]
  }

  # ——— Drill Set ———

  set: detail {
    fields: [
      user_id,
      subscription_id,
      status,
      plan_interval,
      price,
      is_zero_mrr,
      has_ever_billed,
      is_scheduled_to_cancel,
      cancelled_at_date,
      current_period_end_date
    ]
  }
}

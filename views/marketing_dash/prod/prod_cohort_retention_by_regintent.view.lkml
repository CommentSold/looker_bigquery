# =============================================================================
# prod_cohort_retention_by_regintent
# -----------------------------------------------------------------------------
# PURPOSE: answer "which registration intent retains best?"
#
# Companion to prod_subscription_cohort_retention, NOT a replacement. That view
# reproduces Stripe's cohort grid to 0.47pp mean absolute error and should stay
# untouched. This one adds utm_regintent to the grain, which Stripe does not
# publish and therefore cannot be validated against.
#
# GRAIN: one row per (cohort_month, utm_regintent, months_since).
#
# -----------------------------------------------------------------------------
# INTENDED USE -- aggregate ACROSS cohorts
# -----------------------------------------------------------------------------
# Per-cohort segmentation is not viable: ~2,877 first-time entrants across
# twelve cohorts split ~11 ways gives single-digit denominators for most
# segments (2025-09 has 32 users total, 2026-03 has 63). At n=6 one churn moves
# retention 17pp.
#
# So build the tile WITHOUT the cohort dimension. Denominators become hundreds.
#
# THEN FILTER Cohort Maturity Months >= N, where N is the highest Cohort Age you
# display. That keeps the panel BALANCED -- every column draws on the same set
# of cohorts. Without it, month 6 is computed from old cohorts only while
# month 1 includes everything, so a curve can bend purely from changing cohort
# mix rather than from actual churn.
#
# Built-in check: with a balanced panel, Cohort Size is IDENTICAL across every
# Cohort Age column. If it varies, the maturity filter is too loose for the
# range being shown.
#
# -----------------------------------------------------------------------------
# READING THIS DATA HONESTLY
# -----------------------------------------------------------------------------
# These are OBSERVATIONAL segments, not an experiment. Users self-select their
# intent at onboarding, so a difference describes WHO signed up, not what the
# intent caused. A segment skewed toward recent cohorts will also look
# different for reasons unrelated to intent -- keep Cohort Size on the tile.
#
# -----------------------------------------------------------------------------
# REGINTENT RESOLUTION
# -----------------------------------------------------------------------------
# Same COALESCE(onboarding event, marketing capture) pattern as the agent trial
# reporting views. It is a USER attribute, stable per user, so segments
# partition each cohort exactly and no user is double-counted.
#
#   * NO NORMALISATION. Every raw value stays distinct, including
#     near-duplicates like 'realestate' / 'realestateevent' / 'real_estate'. If
#     they should ever be combined, do it in a deliberate mapping table so the
#     decision is visible and reversible.
#   * NO BUCKETING. There is no 'Other' group; every intent appears under its
#     own name. To keep a chart readable, filter on Cohort Size — that is
#     transparent about what is being hidden, unlike a rank cutoff.
#   * NULL is labelled '(not set)' (no onboarding event captured at all) and
#     stays distinct from an explicit 'generic'. Both always appear as their own
#     rows; Is Specific Intent is an optional convenience filter, not a merge.
#
# -----------------------------------------------------------------------------
# MODEL -- identical to prod_subscription_cohort_retention
# -----------------------------------------------------------------------------
#   1. Merge each customer's subscriptions into continuous ACTIVE PERIODS.
#   2. COHORT = month of the customer's FIRST period; later periods belong to
#      that same cohort.
#   3. DENOMINATOR = first-time entrants (period_seq = 1).
#   4. NUMERATOR = running SUM(starts - ends), including returning customers.
#
# Also inherits two open production issues, deliberately, so the two views
# agree: the dunning CTE is unbounded, and current_period_end is overwritten to
# the cancellation instant on some cancellations.
# =============================================================================

view: prod_cohort_retention_by_regintent {
  derived_table: {
    sql:
      WITH
      latest_subscription AS (
        SELECT * EXCEPT (rn)
        FROM (
          SELECT
            t1.subscription_id,
            t1.user_id,
            t1.status,
            t1.cancel_at_period_end,
            t1.current_period_end,
            t1.cancelled_at,
            ROW_NUMBER() OVER (
              PARTITION BY t1.subscription_id ORDER BY t1.updated_at DESC
            ) AS rn
          FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1
          WHERE t1.is_deleted = FALSE
            AND EXISTS (
              SELECT 1 FROM UNNEST(t1.plans) AS plan
              WHERE JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
            )
        )
        WHERE rn = 1
      ),

      -- MIN(SAFE_CAST(...)) not ANY_VALUE(...): ANY_VALUE is nondeterministic
      -- across history rows and returns NULL when it lands on a row with no
      -- cancelledAt, silently falling through to MIN(updated_at).
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

      test_email_users AS (
      SELECT
      user_id,
      LOGICAL_OR(REGEXP_CONTAINS(LOWER(email),
      r'@(test\.com|example\.com|popshoplive\.com|pop\.store|commentsold\.com)$')
      ) AS is_test_email
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      GROUP BY user_id
      ),

      -- ══════════════════════════════════════════════════════════════════
      -- UTM REGINTENT PER USER
      -- ══════════════════════════════════════════════════════════════════
      onboarding_regintent AS (
      SELECT user_id, utm_regintent
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      WHERE (scene = 'onboarding' OR scene IS NULL)
      AND (step_name = 'onboarding_complete' OR step_name IS NULL)
      AND user_id IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY
      CASE WHEN utm_regintent IS NOT NULL AND utm_regintent != 'generic'
      THEN 0 ELSE 1 END,
      `timestamp` DESC
      ) = 1
      ),

      marketing_regintent AS (
      SELECT
      user_id,
      ANY_VALUE(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent')) AS utm_regintent
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      WHERE user_id IS NOT NULL
      GROUP BY user_id
      ),

      user_regintent AS (
      SELECT
      COALESCE(oe.user_id, mc.user_id) AS user_id,
      -- NO NORMALISATION. Every raw value stays distinct, including
      -- near-duplicates like 'realestate' / 'realestateevent' /
      -- 'real_estate'. If they should ever be combined, do it in a
      -- deliberate mapping table, not silently here.
      COALESCE(oe.utm_regintent, mc.utm_regintent) AS utm_regintent
      FROM onboarding_regintent oe
      FULL OUTER JOIN marketing_regintent mc
      ON mc.user_id = oe.user_id
      ),

      scoped AS (
      SELECT
      s.subscription_id,
      s.user_id,
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
      LEFT JOIN flip_resolved f     ON f.subscription_id = s.subscription_id
      LEFT JOIN dunning d           ON d.subscription_id = s.subscription_id
      LEFT JOIN test_email_users te ON te.user_id = s.user_id
      WHERE s.user_id IS NOT NULL
      -- Semi-join: immune to duplicate profile rows.
      AND EXISTS (
      SELECT 1
      FROM `popshoplive-26f81.dbt_popshop.dim_profiles` p
      WHERE p.user_id = s.user_id
      AND p.apps_pop_store = TRUE
      AND p.user_type IN ('seller', 'verifiedSeller')
      )
      {% if exclude_test_emails._parameter_value == 'yes' %}
      AND COALESCE(te.is_test_email, FALSE) = FALSE
      {% endif %}
      ),

      subscription_mrr AS (
      SELECT
      subscription_id,
      MIN(DATE(COALESCE(created, created_at), 'America/New_York')) AS first_mrr_date
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      AND amount_due > 0
      GROUP BY subscription_id
      ),

      sub_spans AS (
      SELECT
      s.user_id,
      sm.first_mrr_date    AS start_date,
      s.effective_end_date AS end_date
      FROM scoped s
      INNER JOIN subscription_mrr sm
      ON sm.subscription_id = s.subscription_id
      ),

      -- ══════════════════════════════════════════════════════════════════
      -- MERGE INTO CUSTOMER-LEVEL ACTIVE PERIODS
      -- ══════════════════════════════════════════════════════════════════
      ordered AS (
      SELECT
      user_id,
      start_date,
      COALESCE(end_date, DATE '9999-12-31') AS end_date_filled,
      MAX(COALESCE(end_date, DATE '9999-12-31')) OVER (
      PARTITION BY user_id ORDER BY start_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ) AS prev_max_end
      FROM sub_spans
      ),

      flagged AS (
      SELECT
      *,
      CASE
      WHEN prev_max_end IS NULL THEN 1
      -- Subtract from start_date rather than adding to prev_max_end:
      -- prev_max_end is 9999-12-31 for an open subscription and adding
      -- days to that overflows.
      WHEN DATE_SUB(start_date, INTERVAL {% parameter merge_gap_days %} DAY) > prev_max_end THEN 1
      ELSE 0
      END AS is_new_period
      FROM ordered
      ),

      grouped AS (
      SELECT
      *,
      SUM(is_new_period) OVER (
      PARTITION BY user_id ORDER BY start_date
      ) AS period_id
      FROM flagged
      ),

      merged AS (
      SELECT
      user_id,
      MIN(start_date) AS period_start,
      NULLIF(MAX(end_date_filled), DATE '9999-12-31') AS period_end
      FROM grouped
      GROUP BY user_id, period_id
      ),

      periods AS (
      SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY period_start) AS period_seq
      FROM merged
      ),

      user_cohort AS (
      SELECT
      p.user_id,
      DATE_TRUNC(MIN(p.period_start), MONTH) AS cohort_month,
      COALESCE(ANY_VALUE(ur.utm_regintent), '(not set)') AS utm_regintent
      FROM periods p
      LEFT JOIN user_regintent ur ON ur.user_id = p.user_id
      GROUP BY p.user_id
      ),

      events AS (
      SELECT
      uc.cohort_month,
      uc.utm_regintent,
      DATE_TRUNC(p.period_start, MONTH) AS event_month,
      p.period_seq,
      1     AS delta,
      TRUE  AS is_start
      FROM periods p
      INNER JOIN user_cohort uc ON uc.user_id = p.user_id

      UNION ALL

      SELECT
      uc.cohort_month,
      uc.utm_regintent,
      DATE_TRUNC(p.period_end, MONTH),
      p.period_seq,
      -1,
      FALSE
      FROM periods p
      INNER JOIN user_cohort uc ON uc.user_id = p.user_id
      WHERE p.period_end IS NOT NULL
      AND p.period_end < CURRENT_DATE('America/New_York')
      ),

      monthly AS (
      SELECT
      cohort_month,
      utm_regintent,
      event_month,
      SUM(delta)                           AS net_change,
      COUNTIF(is_start AND period_seq = 1) AS new_starts,
      COUNTIF(is_start AND period_seq > 1) AS returning_starts,
      COUNTIF(NOT is_start)                AS period_ends
      FROM events
      GROUP BY 1, 2, 3
      ),

      -- Total paying entrants per intent, across all cohorts. Exposed as a
      -- dimension so sample size is visible without a second query. No ranking
      -- and no bucketing -- every intent keeps its own identity.
      regintent_volume AS (
      SELECT
      utm_regintent,
      SUM(new_starts) AS regintent_total_users
      FROM monthly
      GROUP BY utm_regintent
      ),

      -- One spine per (cohort, segment) so a segment with no events in a month
      -- still carries its balance forward.
      spine AS (
      SELECT c.cohort_month, c.utm_regintent, m AS event_month
      FROM (SELECT DISTINCT cohort_month, utm_regintent FROM monthly) c
      CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(
      c.cohort_month,
      DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH),
      INTERVAL 1 MONTH
      )) AS m
      ),

      running AS (
      SELECT
      sp.cohort_month,
      sp.utm_regintent,
      sp.event_month,
      DATE_DIFF(sp.event_month, sp.cohort_month, MONTH) AS months_since,
      SUM(COALESCE(m.net_change, 0)) OVER (
      PARTITION BY sp.cohort_month, sp.utm_regintent ORDER BY sp.event_month
      ) AS active_subscribers,
      COALESCE(m.new_starts, 0)       AS new_starts,
      COALESCE(m.returning_starts, 0) AS returning_starts,
      COALESCE(m.period_ends, 0)      AS period_ends
      FROM spine sp
      LEFT JOIN monthly m
      ON  m.cohort_month  = sp.cohort_month
      AND m.utm_regintent = sp.utm_regintent
      AND m.event_month   = sp.event_month
      ),

      sizes AS (
      SELECT cohort_month, utm_regintent, SUM(new_starts) AS cohort_size
      FROM monthly
      GROUP BY 1, 2
      )

      SELECT
      r.cohort_month,
      r.utm_regintent,
      rv.regintent_total_users,
      r.utm_regintent NOT IN ('(not set)', 'generic') AS is_specific_intent,
      r.event_month,
      r.months_since,
      -- Complete months between the cohort month and the last CLOSED month.
      -- Filter on this to keep the panel balanced across columns.
      DATE_DIFF(
      DATE_SUB(DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH), INTERVAL 1 MONTH),
      r.cohort_month, MONTH
      ) AS cohort_maturity_months,
      z.cohort_size,
      r.active_subscribers,
      r.new_starts,
      r.returning_starts,
      r.period_ends,
      r.event_month = DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH) AS is_partial_month
      FROM running r
      INNER JOIN sizes z
      ON  z.cohort_month  = r.cohort_month
      AND z.utm_regintent = r.utm_regintent
      INNER JOIN regintent_volume rv
      ON rv.utm_regintent = r.utm_regintent
      ORDER BY r.cohort_month, r.utm_regintent, r.months_since
      ;;
  }

  # ——— Parameters ———

  parameter: exclude_test_emails {
    type: unquoted
    label: "Exclude Test Emails"
    default_value: "no"
    description: "No = consistent with the Stripe-reconciled cohort view, which applies no email filter. Yes = matches the older internal dashboard."
    allowed_value: { label: "No (match Stripe)"     value: "no"  }
    allowed_value: { label: "Yes (match dashboard)" value: "yes" }
  }

  parameter: merge_gap_days {
    type: number
    label: "Merge Gap Days"
    default_value: "1"
    description: "Two of a user's subscriptions separated by no more than this many days count as one continuous active period. Measured immaterial on the unsegmented view (0.473pp vs 0.467pp mean absolute error). Leave at 1 unless you have a reason."
  }

  # ——— Primary Key ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(
           CAST(${TABLE}.cohort_month AS STRING), '|',
           ${TABLE}.utm_regintent, '|',
           CAST(${TABLE}.months_since AS STRING)
         ) ;;
  }

  # ——— Segment dimensions ———

  dimension: utm_regintent {
    type: string
    sql: ${TABLE}.utm_regintent ;;
    label: "Reg Intent"
    description: "Registration intent captured at onboarding, exactly as recorded. No normalisation and no bucketing — near-duplicates such as 'realestate', 'realestateevent' and 'real_estate' stay distinct, and 'generic' stays distinct from '(not set)' (which means no onboarding event was captured at all). To keep a chart readable, filter on Cohort Size rather than collapsing values."
  }

  dimension: is_specific_intent {
    type: yesno
    sql: ${TABLE}.is_specific_intent ;;
    label: "Is Specific Intent"
    description: "No for '(not set)' and 'generic'. Optional convenience filter — both values are always available as their own rows."
  }

  dimension: regintent_total_users {
    type: number
    sql: ${TABLE}.regintent_total_users ;;
    label: "Reg Intent Total Users"
    description: "Paying first-time entrants for this intent across ALL cohorts, unaffected by tile filters. The true population behind the segment — check it before reading a retention difference as real."
  }

  # ——— Cohort and time dimensions ———

  dimension_group: cohort {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.cohort_month ;;
    timeframes: [month, quarter, year]
    label: "Cohort (First Paid Month)"
    description: "Month the customer FIRST became MRR-active. Leave OFF the segmented tile — including it collapses denominators to single digits."
  }

  dimension: cohort_maturity_months {
    type: number
    sql: ${TABLE}.cohort_maturity_months ;;
    label: "Cohort Maturity Months"
    description: "Complete months of history available for this cohort. Filter to >= the highest Cohort Age displayed, so every column draws on the same cohorts. Without it a curve can bend from changing cohort mix rather than from churn."
  }

  dimension_group: event {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.event_month ;;
    timeframes: [month, quarter, year]
    label: "Event Month"
  }

  dimension: months_since {
    type: number
    sql: ${TABLE}.months_since ;;
    label: "Months Since Cohort Start"
    description: "0 = the cohort month itself."
  }

  dimension: months_since_label {
    type: string
    order_by_field: months_since
    sql: ${TABLE}.months_since ;;
    html: {% if value == 0 %}Month 0 (start){% else %}Month {{ value }}{% endif %} ;;
    label: "Cohort Age"
  }

  dimension: is_partial_month {
    type: yesno
    sql: ${TABLE}.is_partial_month ;;
    label: "Is Partial Month"
    description: "Yes for the current, incomplete month. Always filter to No."
  }

  # ——— Measures ———
  # Retention Rate is a WEIGHTED ratio, so it aggregates correctly across
  # intents and across cohorts. It does NOT aggregate across Cohort Age —
  # always pivot or filter that dimension.

  measure: retention_rate {
    type: number
    sql: SAFE_DIVIDE(SUM(${TABLE}.active_subscribers), SUM(${TABLE}.cohort_size)) ;;
    value_format_name: percent_1
    label: "Retention Rate"
    description: "Active Subscribers / Cohort Size, weighted by cohort size."
  }

  measure: cohort_size {
    type: sum
    sql: ${TABLE}.cohort_size ;;
    label: "Cohort Size"
    description: "First-time entrants. On a balanced panel this is identical across every Cohort Age column — if it varies, the maturity filter is too loose for the range shown."
  }

  measure: active_subscribers {
    type: sum
    sql: ${TABLE}.active_subscribers ;;
    label: "Active Subscribers"
    description: "Cumulative period starts minus ends. Already a running total; never aggregate across Cohort Age."
  }

  measure: returning_starts {
    type: sum
    sql: ${TABLE}.returning_starts ;;
    label: "Returning Starts"
    description: "Cohort members who restarted after a lapse. Why a balance can tick back up."
  }

  measure: period_ends {
    type: sum
    sql: ${TABLE}.period_ends ;;
    label: "Period Ends"
    description: "Cohort members whose active period ended in this month."
  }
}

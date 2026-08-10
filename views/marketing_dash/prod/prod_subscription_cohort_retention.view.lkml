# =============================================================================
# prod_subscription_cohort_retention
# -----------------------------------------------------------------------------
# Reproduces Stripe's Customer Cohort Retention chart
# (dashboard.stripe.com/billing/churn) to a mean absolute error of 0.47pp
# across 78 cells, against targets Stripe rounds to whole percent.
#
# -----------------------------------------------------------------------------
# THE MODEL, as validated empirically
# -----------------------------------------------------------------------------
#   1. Merge each customer's subscriptions into continuous ACTIVE PERIODS.
#      Subscriptions separated by <= merge_gap_days are one period. This is why
#      the chain-replacement pattern (cancel 08:07, restart 08:09, new
#      subscription_id) produces no churn on Stripe's side.
#   2. COHORT = month of the customer's FIRST period. Later periods belong to
#      that same cohort, NOT to the month they start in.
#   3. DENOMINATOR = first-time entrants only (period_seq = 1).
#   4. NUMERATOR = running SUM(period starts - period ends) for the cohort,
#      including returning customers' later periods.
#
# Points 2 and 3 together are the whole thing, and they are asymmetric on
# purpose. Stripe's template makes this look otherwise: active_start_count
# subtracts REACTIVATE (so the denominator is new customers only) while
# active_count counts every ACTIVE_START (so the numerator includes returns).
#
# EVIDENCE for point 2 -- the 2025-10 cohort. Stripe's Mo 8/9/10 read 25%, 24%,
# 25% of 83, i.e. 20.75, 19.92, 20.75 subscribers. The balance OSCILLATES. This
# view now returns 21, 20, 21. A cohort balance can only rise if returning
# customers rejoin their original cohort. Assigning each period its own cohort
# gives a strictly monotone balance, flat at 18, which cannot reproduce that
# shape -- and also produced impossible >100% cells (2025-09 at 103.1%).
#
# -----------------------------------------------------------------------------
# COLUMN ALIGNMENT WITH STRIPE'S UI  (verified against the Sigma export)
# -----------------------------------------------------------------------------
#   Stripe "Start value" = Cohort Size
#   Stripe "Mo 1"        = months_since 0  (the cohort month itself)
#   Stripe "Mo N"        = months_since N-1
#   Stripe omits the current partial month -- filter Is Partial Month = No.
# Proof: cohort 2026-02, event 2026-07, 29/78 = 37.2%; Stripe shows 37% under
# Mo 6, and Feb -> Jul is five months.
#
# -----------------------------------------------------------------------------
# VALIDATED COHORT SIZES  (Exclude Test Emails = No, Merge Gap Days = 1)
# -----------------------------------------------------------------------------
#   cohort    ours   Stripe Start   Stripe New
#   2025-08     55            61            -     <- see KNOWN OUTLIERS
#   2025-09     32            32            -
#   2025-10     83            83            -
#   2025-11     92            90            -
#   2025-12    139           141            -
#   2026-01     82            81            -
#   2026-02     76            78            -
#   2026-03     63            63           63
#   2026-04    341           340          341
#   2026-05    568           565          568
#   2026-06    731           734          731
#   2026-07    615           616          615
# Where Stripe publishes both, ours matches New exactly. The +-3 against Start
# value is Stripe's own inconsistency between the two figures, not ours.
#
# -----------------------------------------------------------------------------
# KNOWN OUTLIERS -- do not tune these away
# -----------------------------------------------------------------------------
# 2026-04 Mo 1 runs -2.2pp and is the single worst cell. It persists at every
#   gap value and under both denominators, so it is not a modelling choice.
#   April is also the month with 7 duplicate rows in stripe_analytical_data and
#   the worst row (4.2%) in prod_stripe_bq_reconciliation. Something specific
#   happened in April; investigate it rather than absorbing it into tolerance.
# 2025-08 has cohort_size 55 against Stripe's 61 and is the only cohort where
#   Stripe is materially higher, with errors turning positive at Mo 7-8 (+1.5,
#   +1.7). Consistent with truncated history at the start of
#   fact_seller_subscription. No modelling change will fix it.
#
# -----------------------------------------------------------------------------
# STILL INHERITED FROM PRODUCTION, DELIBERATELY
# -----------------------------------------------------------------------------
#   * The dunning CTE is unbounded (no 60-day window), so a stale void from an
#     earlier billing period can pull an end date early.
#   * current_period_end is overwritten to the cancellation instant on some
#     cancellations, mis-dating the cancel_at_period_end IS TRUE branch.
# Both are open items on prod_subscription_churn. Kept identical here so the
# two views agree; fix in both or neither.
#
# GRAIN: one row per (cohort_month, event_month).
# =============================================================================

view: prod_subscription_cohort_retention {
  derived_table: {
    sql:
      WITH
      -- ══════════════════════════════════════════════════════════════════
      -- 1) ONE ROW PER SUBSCRIPTION, LATEST STATE
      --    EXISTS rather than UNNEST so the plan filter does not fan the row
      --    out per plan. The previous version read the history table raw,
      --    which meant cancelled_at was NULL on pre-cancellation rows and
      --    every cancelled subscription also emitted an open-ended span.
      -- ══════════════════════════════════════════════════════════════════
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

      -- Pre-aggregated so a duplicated profile row cannot fan out the spans.
      test_email_users AS (
      SELECT
      user_id,
      LOGICAL_OR(REGEXP_CONTAINS(LOWER(email),
      r'@(test\.com|example\.com|popshoplive\.com|pop\.store|commentsold\.com)$')
      ) AS is_test_email
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      GROUP BY user_id
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
      -- Semi-join rather than INNER JOIN: immune to duplicate profile rows.
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

      -- Identical to prod_subscription_churn: is_deleted filter,
      -- COALESCE(created, created_at), America/New_York. The previous version
      -- omitted all three, which shifted cohort assignment.
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
      s.effective_end_date AS end_date      -- NULL = still active
      FROM scoped s
      INNER JOIN subscription_mrr sm
      ON sm.subscription_id = s.subscription_id
      ),

      -- ══════════════════════════════════════════════════════════════════
      -- 2) MERGE INTO CUSTOMER-LEVEL ACTIVE PERIODS
      --    Gaps-and-islands. 9999-12-31 stands in for "open" so MAX() works.
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
      -- days to that overflows. Algebraically identical, and correct at
      -- the sentinel -- nothing is later than 9999-12-31, so a start
      -- following an open subscription always merges.
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

      -- ══════════════════════════════════════════════════════════════════
      -- 3) COHORT = MONTH OF THE USER'S FIRST PERIOD
      --    Every later period is attributed here too, which is what lets a
      --    cohort balance recover -- the behaviour Stripe's 2025-10 row shows
      --    and a per-period cohort cannot reproduce.
      -- ══════════════════════════════════════════════════════════════════
      user_cohort AS (
      SELECT
      user_id,
      DATE_TRUNC(MIN(period_start), MONTH) AS cohort_month
      FROM periods
      GROUP BY user_id
      ),

      events AS (
      SELECT
      uc.cohort_month,
      DATE_TRUNC(p.period_start, MONTH) AS event_month,
      p.period_seq,
      1     AS delta,
      TRUE  AS is_start
      FROM periods p
      INNER JOIN user_cohort uc ON uc.user_id = p.user_id

      UNION ALL

      SELECT
      uc.cohort_month,
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
      event_month,
      SUM(delta)                                    AS net_change,
      COUNTIF(is_start AND period_seq = 1)          AS new_starts,
      COUNTIF(is_start AND period_seq > 1)          AS returning_starts,
      COUNTIF(NOT is_start)                         AS period_ends
      FROM events
      GROUP BY 1, 2
      ),

      -- Dense spine so a cohort with no events in a month still carries the
      -- balance forward.
      spine AS (
      SELECT c.cohort_month, m AS event_month
      FROM (SELECT DISTINCT cohort_month FROM monthly) c
      CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(
      c.cohort_month,
      DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH),
      INTERVAL 1 MONTH
      )) AS m
      ),

      running AS (
      SELECT
      sp.cohort_month,
      sp.event_month,
      DATE_DIFF(sp.event_month, sp.cohort_month, MONTH) AS months_since,
      SUM(COALESCE(m.net_change, 0)) OVER (
      PARTITION BY sp.cohort_month ORDER BY sp.event_month
      ) AS active_subscribers,
      COALESCE(m.new_starts, 0)       AS new_starts,
      COALESCE(m.returning_starts, 0) AS returning_starts,
      COALESCE(m.period_ends, 0)      AS period_ends
      FROM spine sp
      LEFT JOIN monthly m
      ON  m.cohort_month = sp.cohort_month
      AND m.event_month  = sp.event_month
      ),

      -- First-time entrants only. A user's first period starts in their cohort
      -- month by definition, so these all land in months_since 0.
      sizes AS (
      SELECT cohort_month, SUM(new_starts) AS cohort_size
      FROM monthly
      GROUP BY cohort_month
      )

      SELECT
      r.cohort_month,
      r.event_month,
      r.months_since,
      z.cohort_size,
      r.active_subscribers,
      SAFE_DIVIDE(r.active_subscribers, z.cohort_size) AS retention_rate,
      r.new_starts,
      r.returning_starts,
      r.period_ends,
      r.event_month = DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH) AS is_partial_month
      FROM running r
      INNER JOIN sizes z
      ON z.cohort_month = r.cohort_month
      ORDER BY r.cohort_month, r.months_since
      ;;
  }

  # ——— Parameters ———

  parameter: exclude_test_emails {
    type: unquoted
    label: "Exclude Test Emails"
    default_value: "no"
    description: "No = Stripe-comparable; Stripe applies no email filter, and turning this on was the dominant cause of the previous version's cohort-size undercount (April -28, May -24). Yes = matches the old internal dashboard."
    allowed_value: { label: "No (match Stripe)"      value: "no"  }
    allowed_value: { label: "Yes (match dashboard)"  value: "yes" }
  }

  parameter: merge_gap_days {
    type: number
    label: "Merge Gap Days"
    default_value: "1"
    description: "Two of a user's subscriptions separated by no more than this many days count as one continuous active period. Measured immaterial: 0.473pp mean absolute error at 1 vs 0.467pp at 7 across 78 cells. 1 is the default because Stripe's model has no tolerance at all and same-day cancel-and-recreate is the only pattern with direct evidence."
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
    label: "Cohort (First Paid Month)"
    description: "Month the customer FIRST became MRR-active. A customer who lapses and returns stays in this cohort — matching Stripe, whose cohort balances recover."
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
    description: "0 = the cohort month itself. Stripe's UI labels this Mo 1, so Stripe's Mo N = months_since N-1."
  }

  dimension: months_since_label {
    type: string
    order_by_field: months_since
    sql: ${TABLE}.months_since ;;
    html: {% if value == 0 %}Month 0 (start){% else %}Month {{ value }}{% endif %} ;;
    label: "Cohort Age"
  }

  dimension: stripe_column_label {
    type: string
    order_by_field: months_since
    sql: CONCAT('Mo ', CAST(${TABLE}.months_since + 1 AS STRING)) ;;
    label: "Stripe Column"
    description: "Stripe's own column heading, for reading straight across from dashboard.stripe.com/billing/churn."
  }

  dimension: cohort_age_band {
    type: string
    order_by_field: months_since
    sql:
      CASE
        WHEN ${TABLE}.months_since = 0 THEN '0: start month'
        WHEN ${TABLE}.months_since BETWEEN 1 AND 3  THEN '1-3 months'
        WHEN ${TABLE}.months_since BETWEEN 4 AND 6  THEN '4-6 months'
        WHEN ${TABLE}.months_since BETWEEN 7 AND 12 THEN '7-12 months'
        ELSE '13+ months'
      END ;;
    label: "Cohort Age Band"
  }

  dimension: is_partial_month {
    type: yesno
    sql: ${TABLE}.is_partial_month ;;
    label: "Is Partial Month"
    description: "Yes for the current, incomplete month. Filter to No to reproduce Stripe's trailing edge — Stripe omits it."
  }

  dimension: cohort_size {
    type: number
    sql: ${TABLE}.cohort_size ;;
    label: "Cohort Size"
    description: "First-time entrants in the cohort month. Matches Stripe's Start value within ±3 and its New-subscriber count exactly. Constant within a cohort."
  }

  # ——— Measures ———
  # Grain is one row per (cohort_month, event_month), so with rows = Cohort and
  # columns = Cohort Age each cell maps to exactly one row and MAX is exact.
  # Active Subscribers is ALREADY a running total: never SUM it across
  # months_since. Always pivot or filter the age dimension.

  measure: active_subscribers {
    type: max
    sql: ${TABLE}.active_subscribers ;;
    label: "Active Subscribers"
    description: "Cumulative period starts minus ends for this cohort through this month — Stripe's active_subscribers. Already a running total; do not sum across months."
  }

  measure: cohort_size_measure {
    type: max
    sql: ${TABLE}.cohort_size ;;
    label: "Cohort Size"
  }

  measure: retention_rate {
    type: max
    value_format_name: percent_1
    sql: ${TABLE}.retention_rate ;;
    label: "Retention Rate"
    description: "Active Subscribers / Cohort Size. Use with rows = Cohort, columns = Cohort Age. Mean absolute error vs Stripe 0.47pp across 78 cells."
  }

  # Diagnostics: these explain WHY a balance moved in a given month.

  measure: returning_starts {
    type: max
    sql: ${TABLE}.returning_starts ;;
    label: "Returning Starts"
    description: "Cohort members who restarted a subscription in this month after a lapse. Non-zero values are why the balance can tick back up."
  }

  measure: period_ends {
    type: max
    sql: ${TABLE}.period_ends ;;
    label: "Period Ends"
    description: "Cohort members whose active period ended in this month."
  }
}

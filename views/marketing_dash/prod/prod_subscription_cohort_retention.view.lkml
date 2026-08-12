# =============================================================================
# prod_subscription_cohort_retention
# -----------------------------------------------------------------------------
# Reproduces Stripe's Customer Cohort Retention chart
# (dashboard.stripe.com/billing/churn) to a mean absolute error of 0.47pp
# across 78 cells, against targets Stripe rounds to whole percent.
#
# GRAIN: one row per (cohort_month, event_month, utm_regintent).
#
# -----------------------------------------------------------------------------
# WHAT CHANGED WHEN utm_regintent WAS ADDED
# -----------------------------------------------------------------------------
# utm_regintent is in the grain purely so a DASHBOARD FILTER can slice the grid.
# With the filter cleared, every number is byte-identical to the validated
# version, because each creator carries exactly one intent (NULL -> '(not set)')
# so the intents PARTITION each cohort and summing across them returns the total.
#
# !! THE MEASURES HAD TO CHANGE FROM type: max TO WEIGHTED SUMS !!
# With utm_regintent in the grain, a cell that does not include the intent
# dimension covers MULTIPLE rows. type: max would return the largest single
# segment instead of the cohort total, so every number on the heatmap would
# silently drop. At cell level nothing changes: one row per cell means SUM
# equals MAX equals the value.
#
# Also removed: the pre-computed retention_rate column and the cohort_size
# DIMENSION. A pre-divided rate cannot be re-aggregated across segments, and a
# cohort_size dimension would force a GROUP BY that splits every row. Both are
# now measures. If your tile referenced the cohort_size DIMENSION, swap it for
# the Cohort Size MEASURE — same label, one click.
#
# REGRESSION TEST BEFORE PUBLISHING (filter cleared):
#   cohort   size   Mo 1
#   2025-09    32  100.0%     2026-03    63  100.0%
#   2025-10    83   72.3%     2026-04   341   54.8%
#   2025-11    92   81.5%     2026-05   568   79.2%
#   2025-12   139   46.8%     2026-06   731   55.5%
#   2026-01    82   80.5%     2026-07   615   62.8%
#   2026-02    76   84.2%
# If any of these move, stop — the intent join is duplicating or dropping
# creators and the Stripe reconciliation is broken.
#
# -----------------------------------------------------------------------------
# WHAT THE FILTER DOES TO THE DENOMINATOR  (say this out loud to management)
# -----------------------------------------------------------------------------
# Filtering to an intent changes BOTH numerator and denominator. Cohort Size
# becomes "creators with that intent who first paid in that month", not the whole
# cohort. So the grid answers "of the pdf_creator creators who first paid in
# April, how many still pay?" — which is the intended question, but it means:
#
#   * Cohort Size shrinks, sometimes to single digits. A row built on 4 creators
#     will read 0% or 100%. Check Cohort Size before believing a cell.
#   * Rows can vanish. Intent capture began around April 2026, so filtering to
#     an agent intent empties the 2025 cohorts entirely.
#   * THE STRIPE RECONCILIATION ONLY HOLDS WITH THE FILTER CLEARED. Stripe does
#     not segment by intent, so there is nothing to reconcile a filtered view
#     against. Do not cite the 0.47pp figure on a filtered screenshot.
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
# Points 2 and 3 are asymmetric on purpose. Stripe's template makes this look
# otherwise: active_start_count subtracts REACTIVATE (so the denominator is new
# customers only) while active_count counts every ACTIVE_START (so the numerator
# includes returns).
#
# EVIDENCE for point 2 -- the 2025-10 cohort. Stripe's Mo 8/9/10 read 25%, 24%,
# 25% of 83, i.e. 20.75, 19.92, 20.75 subscribers. The balance OSCILLATES. This
# view returns 21, 20, 21. A cohort balance can only rise if returning customers
# rejoin their original cohort. Assigning each period its own cohort gives a
# strictly monotone balance, flat at 18, which cannot reproduce that shape --
# and also produced impossible >100% cells (2025-09 at 103.1%).
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
# VALIDATED COHORT SIZES  (Exclude Test Emails = No, Merge Gap Days = 1,
#                          intent filter CLEARED)
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
# REGINTENT RESOLUTION
# -----------------------------------------------------------------------------
# COALESCE(onboarding event, marketing capture), the same pattern as the agent
# trial reporting views. A USER attribute, stable per creator.
#   * NO NORMALISATION. 'realestate', 'realestateevent' and 'real_estate' stay
#     distinct -- their retention differs sharply, so merging destroys signal.
#   * NO BUCKETING. No 'Other' group.
#   * NULL is labelled '(not set)' and stays distinct from an explicit 'generic'.
# COST: this adds a scan of popstore_onboarding_screen_action. If the tile gets
# slow, add a datagroup or persist_for on the view.
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
# =============================================================================

view: prod_subscription_cohort_retention {
  derived_table: {
    sql:
      WITH
      -- ══════════════════════════════════════════════════════════════════
      -- 1) ONE ROW PER SUBSCRIPTION, LATEST STATE
      --    EXISTS rather than UNNEST so the plan filter does not fan the row
      --    out per plan.
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

      -- ══════════════════════════════════════════════════════════════════
      -- UTM REGINTENT PER CREATOR
      --    Prefer a non-generic onboarding event, fall back to the marketing
      --    capture blob. Exactly one value per creator, which is what makes
      --    the intents partition each cohort so a cleared filter reproduces
      --    the validated totals.
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
      -- NO NORMALISATION. Near-duplicates stay distinct on purpose.
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
      -- COALESCE(created, created_at), America/New_York.
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
      -- 3) COHORT = MONTH OF THE USER'S FIRST PERIOD, plus their intent.
      --    Every later period is attributed here too, which is what lets a
      --    cohort balance recover -- the behaviour Stripe's 2025-10 row shows
      --    and a per-period cohort cannot reproduce.
      -- ══════════════════════════════════════════════════════════════════
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

      -- One spine per (cohort, intent) so a segment with no events in a month
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
      -- PARTITION must include utm_regintent or balances bleed across
      -- segments. Summing these across segments gives the cohort total.
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

      -- First-time entrants only. A user's first period starts in their cohort
      -- month by definition, so these all land in months_since 0.
      sizes AS (
      SELECT cohort_month, utm_regintent, SUM(new_starts) AS cohort_size
      FROM monthly
      GROUP BY 1, 2
      )

      SELECT
      r.cohort_month,
      r.utm_regintent,
      r.event_month,
      r.months_since,
      z.cohort_size,
      r.active_subscribers,
      r.new_starts,
      r.returning_starts,
      r.period_ends,
      r.utm_regintent NOT IN ('(not set)', 'generic') AS is_specific_intent,
      r.event_month = DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH) AS is_partial_month
      FROM running r
      INNER JOIN sizes z
      ON  z.cohort_month  = r.cohort_month
      AND z.utm_regintent = r.utm_regintent
      ORDER BY r.cohort_month, r.utm_regintent, r.months_since
      ;;
  }

  # ——— Parameters ———

  parameter: exclude_test_emails {
    type: unquoted
    label: "Exclude Test Emails"
    default_value: "no"
    description: "No = Stripe-comparable; Stripe applies no email filter, and turning this on caused a large cohort-size undercount in an earlier version (April -28, May -24). Yes = matches the old internal dashboard."
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
    sql: CONCAT(
           CAST(${TABLE}.cohort_month AS STRING), '|',
           ${TABLE}.utm_regintent, '|',
           CAST(${TABLE}.months_since AS STRING)
         ) ;;
  }

  # ——— Dimensions ———

  dimension: utm_regintent {
    type: string
    sql: ${TABLE}.utm_regintent ;;
    label: "Reg Intent"
    description: "Signup intent captured at onboarding, exactly as recorded — no normalisation, no bucketing. USE AS A DASHBOARD FILTER, not as a row or column dimension: adding it to the grid splits every cohort into single-digit segments. Filtering changes the DENOMINATOR too, so Cohort Size becomes creators with that intent only. Note intent capture began around April 2026, so filtering to an agent intent empties the 2025 cohorts. The Stripe reconciliation holds only with this filter cleared."
  }

  dimension: is_specific_intent {
    type: yesno
    sql: ${TABLE}.is_specific_intent ;;
    label: "Is Specific Intent"
    description: "No for '(not set)' and 'generic'. Optional convenience filter — both remain available as their own values."
  }

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

  # ——— Measures ———
  # WEIGHTED, not type: max. With utm_regintent in the grain a cell without the
  # intent dimension covers multiple rows, and max would return the largest
  # single segment rather than the cohort total. At cell level SUM == MAX.
  #
  # Active Subscribers is ALREADY a running total: summing it across intents is
  # correct (the segments are disjoint), summing it across months_since is NOT.
  # Always pivot or filter the age dimension.

  measure: cohort_size_measure {
    type: sum
    sql: ${TABLE}.cohort_size ;;
    label: "Cohort Size"
    description: "DENOMINATOR — first-time entrants. Never shrinks from churn; that is Active Subscribers. With the intent filter cleared this matches Stripe's Start value within ±3 and its New-subscriber count exactly."
  }

  measure: active_subscribers {
    type: sum
    sql: ${TABLE}.active_subscribers ;;
    label: "Active Subscribers"
    description: "NUMERATOR — cumulative period starts minus ends for this cohort through this month, Stripe's active_subscribers. Already a running total; never sum across Cohort Age."
  }

  measure: retention_rate {
    type: number
    sql: SAFE_DIVIDE(SUM(${TABLE}.active_subscribers), SUM(${TABLE}.cohort_size)) ;;
    value_format_name: percent_1
    label: "Retention Rate"
    description: "Active Subscribers / Cohort Size, weighted by cohort size — so it aggregates correctly across intents and a totals row gives a size-weighted average. Use with rows = Cohort, columns = Cohort Age or Stripe Column. Mean absolute error vs Stripe 0.47pp across 78 cells with the intent filter cleared."
  }

  # Diagnostics: these explain WHY a balance moved in a given month.

  measure: returning_starts {
    type: sum
    sql: ${TABLE}.returning_starts ;;
    label: "Returning Starts"
    description: "Cohort members who restarted a subscription in this month after a lapse, credited back to their original cohort. Non-zero values are why a retention figure can tick back UP."
  }

  measure: period_ends {
    type: sum
    sql: ${TABLE}.period_ends ;;
    label: "Period Ends"
    description: "Cohort members whose active period ended in this month."
  }
}

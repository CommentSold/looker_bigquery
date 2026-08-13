# =============================================================================
# prod_cumulative_creator_signups
# -----------------------------------------------------------------------------
# Powers three tiles:
#   1. Cumulative New Creator Sign-ups (actual vs target)
#   2. Monthly New Creator Sign-ups (actual vs target)
#   3. Monthly New Paid Subscribers (actual vs target)
#
# GRAIN: one row per month. Deliberately an aggregate view — see DRILLING below.
#
# -----------------------------------------------------------------------------
# WHAT WAS FIXED
# -----------------------------------------------------------------------------
# 1. EMAIL FILTER WAS MISSING pop.store. The regex listed test.com,
#    example.com, popshoplive.com and commentsold.com but not pop.store, so this
#    view counted internal accounts every other view excludes. Measured by month:
#    10, 0, 4, 5, 112, 19, 0, 1 extra signups — May alone was 112.
#
# 2. NO TIMEZONE ANYWHERE. Signup used bare DATE(s.created_at) and first payment
#    used DATE(fss.first_subscription_date), both UTC, while every other view
#    buckets in America/New_York. Measured 2-14 creators per month landing in a
#    different signup month, and 8-58 per month for first payment.
#
# 3. MIN(created_at) ON INVOICES is now MIN(COALESCE(created, created_at)),
#    matching prod_subscription_churn. `created` is Stripe's invoice date;
#    created_at is the pipeline write time.
#
# 4. FUTURE MONTHS PLOTTED AS ZERO. September-December had targets and no
#    actuals, and both bar charts drew the actual at 0 — reading as a collapse
#    rather than as no data. Actual measures now return NULL for future months.
#
# 5. THE CURRENT MONTH IS PARTIAL. August read 600 against a 3,586 full-month
#    target, a 6x apparent miss on 13 days of data. Added Is Partial Month plus
#    pro-rated target measures so the comparison can be made honestly.
#
# 6. DUPLICATE MEASURES. cumulative_profiles.monthly_signups and
#    monthly_creator_signups.actual_creator_signups were the same expression on
#    the same CTE, exposed as two measures. One removed.
#
# 7. CONVERSION RATE REMOVED. It divided creators whose FIRST PAYMENT was in
#    month M by creators who SIGNED UP in month M — different people. A creator
#    who signed up in March and first paid in June was in June's numerator and
#    March's denominator, so it was a ratio of two independent monthly flows, not
#    a conversion rate. prod_signup_conversion_funnel does this cohort-correctly
#    and validated; use that. (The filter mismatch between the two populations
#    turned out to be only 7 creators of 1,887 — the design was the problem, not
#    the filtering.)
#
# 8. THE 2025-01-13 CUTOFF REMOVED. An undocumented hardcoded floor meant
#    "cumulative" was not all-time. It omitted 10 signups of 26,667, so removing
#    it raises every cumulative point by 10. Noise against targets in the tens of
#    thousands, and worth it for a figure that means what it says.
#
# -----------------------------------------------------------------------------
# CROSS-VIEW CHECK — run this before publishing
# -----------------------------------------------------------------------------
# After the email and timezone fixes this view's signup population is IDENTICAL
# to prod_signup_conversion_funnel: same store requirement, same vidcon
# exclusion, same email filter, same timezone.
#
# Monthly Creator Sign-ups: Actual must equal that view's Total Sign-ups exactly:
#   2026-01   478      2026-05  2,376
#   2026-02  1,140     2026-06  2,310
#   2026-03  2,096     2026-07  1,950
#   2026-04  2,044     2026-08    592   (partial month, no maturity filter)
#
# If they differ, one of the two has drifted and both tiles are suspect.
#
# -----------------------------------------------------------------------------
# TARGETS — supplied by management, kept as given
# -----------------------------------------------------------------------------
# Two things to be aware of when presenting, neither of which is a data issue:
#
# * The cumulative signup target reaches 51,642 by December against an all-time
#   actual near 26,700 — roughly 25,000 more in four months at a run rate near
#   2,000/month.
# * The monthly paid-subscriber target steps 213, 436, 528 then DROPS to 112 in
#   April and stays at 145-251. Actuals move the opposite way: 64, 64, 61 then
#   149, 366, 392, 360. Both series break at April, in opposite directions. That
#   is the same boundary as the 7-day trial launch, so Q1 targets were probably
#   set on a pre-trial funnel model. "Beating target by 250%" from May onward is
#   partly comparing against a plan for a different product.
#
# -----------------------------------------------------------------------------
# DRILLING
# -----------------------------------------------------------------------------
# This view is month-grain, so it cannot drill to creators. The old drill_fields
# listed only the month dimensions, which drilled to a single row.
# For creator-level detail use prod_signup_conversion_funnel — same signup
# population, one row per creator, with email and subscription IDs.
# =============================================================================

view: prod_cumulative_creator_signups {
  derived_table: {
    sql:
      WITH
      onboarding_events_dedup AS (
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

      marketing_capture AS (
      SELECT
      user_id,
      ANY_VALUE(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent')) AS utm_regintent
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      WHERE user_id IS NOT NULL
      GROUP BY user_id
      ),

      -- Signup = store creation, America/New_York. Same population as
      -- prod_signup_conversion_funnel, including pop.store in the email filter.
      signups AS (
      SELECT
      p.user_id,
      MIN(DATE(s.created_at, 'America/New_York')) AS signup_date
      FROM `popshoplive-26f81.dbt_popshop.dim_profiles` p
      INNER JOIN `popshoplive-26f81.dbt_popshop.dim_stores` s
      ON p.user_id = s.store_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = p.user_id
      LEFT JOIN marketing_capture mc         ON mc.user_id = p.user_id
      LEFT JOIN onboarding_events_dedup oe   ON oe.user_id = p.user_id
      WHERE p.apps_pop_store = TRUE
      AND p.user_type IN ('seller', 'verifiedSeller')
      -- COALESCE around the comparison: without it a creator with no
      -- regintent yields NULL, NOT NULL is NULL, and they are silently
      -- dropped. That mistake cut a test population from 26,667 to 11,077.
      AND NOT COALESCE(COALESCE(oe.utm_regintent, mc.utm_regintent) = 'vidcon', FALSE)
      AND (pprof.email IS NULL
      OR NOT REGEXP_CONTAINS(LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'))
      GROUP BY p.user_id
      ),

      -- First real payment per creator. COALESCE(created, created_at) and
      -- America/New_York, matching prod_subscription_churn.
      first_payment AS (
      SELECT
      user_id,
      MIN(DATE(COALESCE(created, created_at), 'America/New_York')) AS first_paid_date
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      AND status = 'paid'
      AND amount_due  > 0
      AND amount_paid > 0
      GROUP BY user_id
      ),

      -- New paid creators restricted to the SAME population as signups, so the
      -- two series on the dashboard describe the same set of people.
      paid_creators AS (
      SELECT
      s.user_id,
      fp.first_paid_date
      FROM signups s
      INNER JOIN first_payment fp ON fp.user_id = s.user_id
      ),

      monthly_signups AS (
      SELECT DATE_TRUNC(signup_date, MONTH) AS month_start, COUNT(*) AS n
      FROM signups
      GROUP BY 1
      ),

      monthly_paid AS (
      SELECT DATE_TRUNC(first_paid_date, MONTH) AS month_start, COUNT(*) AS n
      FROM paid_creators
      GROUP BY 1
      ),

      -- Dense spine from the earliest signup so the running totals cannot skip
      -- a month, out to 2027-12 so future targets have somewhere to attach.
      month_spine AS (
      SELECT month_start
      FROM UNNEST(GENERATE_DATE_ARRAY(
      (SELECT DATE_TRUNC(MIN(signup_date), MONTH) FROM signups),
      '2027-12-01',
      INTERVAL 1 MONTH
      )) AS month_start
      ),

      targets AS (
      SELECT * FROM UNNEST([
      STRUCT(2026 AS year,  1 AS month_number, 14000 AS cumulative_signups_target,
      1254 AS monthly_signups_target,  213 AS monthly_paid_target),
      STRUCT(2026,  2, 16000, 2008, 436),
      STRUCT(2026,  3, 18000, 2008, 528),
      STRUCT(2026,  4, 20979, 2979, 112),
      STRUCT(2026,  5, 24202, 3224, 145),
      STRUCT(2026,  6, 28852, 4650, 251),
      STRUCT(2026,  7, 32727, 3875, 209),
      STRUCT(2026,  8, 36313, 3586, 194),
      STRUCT(2026,  9, 39940, 3627, 218),
      STRUCT(2026, 10, 43781, 3840, 230),
      STRUCT(2026, 11, 47711, 3931, 236),
      STRUCT(2026, 12, 51642, 3931, 236)
      ])
      ),

      joined AS (
      SELECT
      ms.month_start,
      COALESCE(msu.n, 0) AS monthly_signups,
      COALESCE(mp.n, 0)  AS monthly_new_paid
      FROM month_spine ms
      LEFT JOIN monthly_signups msu ON msu.month_start = ms.month_start
      LEFT JOIN monthly_paid    mp  ON mp.month_start  = ms.month_start
      ),

      running AS (
      SELECT
      month_start,
      monthly_signups,
      monthly_new_paid,
      SUM(monthly_signups)  OVER (ORDER BY month_start) AS cumulative_signups,
      SUM(monthly_new_paid) OVER (ORDER BY month_start) AS cumulative_paid
      FROM joined
      )

      SELECT
      r.month_start,
      EXTRACT(YEAR  FROM r.month_start) AS year,
      EXTRACT(MONTH FROM r.month_start) AS month_number,

      r.monthly_signups,
      r.monthly_new_paid,
      r.cumulative_signups,
      r.cumulative_paid,

      t.cumulative_signups_target,
      t.monthly_signups_target,
      t.monthly_paid_target,

      -- Future months carry a target and no actuals. Flagged so the measures
      -- can return NULL rather than zero.
      r.month_start > DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH) AS is_future_month,
      r.month_start = DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH) AS is_partial_month,

      -- For pro-rating a full-month target against a part-month actual.
      CASE
      WHEN r.month_start = DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH)
      THEN EXTRACT(DAY FROM CURRENT_DATE('America/New_York'))
      WHEN r.month_start < DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH)
      THEN EXTRACT(DAY FROM LAST_DAY(r.month_start))
      ELSE 0
      END AS days_elapsed,
      EXTRACT(DAY FROM LAST_DAY(r.month_start)) AS days_in_month

      FROM running r
      LEFT JOIN targets t
      ON  t.year         = EXTRACT(YEAR  FROM r.month_start)
      AND t.month_number = EXTRACT(MONTH FROM r.month_start)
      WHERE r.month_start <= DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH)
      OR t.cumulative_signups_target IS NOT NULL
      ;;
  }

  # ——— Primary Key ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CAST(${TABLE}.month_start AS STRING) ;;
  }

  # ——— Time ———

  # Name kept as first_day_of_month (not renamed to `month`) so the three
  # existing tiles keep working — they reference first_day_of_month_date and
  # first_day_of_month_month. The LABEL is "Month", which is what users see.
  dimension_group: first_day_of_month {
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

  dimension: is_future_month {
    type: yesno
    sql: ${TABLE}.is_future_month ;;
    label: "Is Future Month"
    description: "Target only, no actuals. All actual measures return NULL rather than zero, so a chart leaves a gap instead of drawing a drop to the axis. Set the visualization's missing-value handling to leave gaps, not plot as zero."
  }

  dimension: is_partial_month {
    type: yesno
    sql: ${TABLE}.is_partial_month ;;
    label: "Is Partial Month"
    description: "Yes for the month in progress. Its actual is real but incomplete, and it is being compared against a FULL-month target — August read 600 against 3,586 on 13 days of data. Use the pro-rated target measures, or filter this out for a clean series."
  }

  dimension: days_elapsed {
    type: number
    sql: ${TABLE}.days_elapsed ;;
    label: "Days Elapsed"
    description: "Days of the month that have completed. Equals Days in Month for closed months."
  }

  dimension: days_in_month {
    type: number
    sql: ${TABLE}.days_in_month ;;
    label: "Days in Month"
  }

  # ——— Actual measures ———
  # type: number with an explicit NULL for future months. A plain type: sum
  # renders those as 0, which is what made September-December look like a
  # collapse on both bar charts.

  measure: monthly_creator_signups {
    type: number
    sql: IF(LOGICAL_OR(${TABLE}.is_future_month), NULL,
      SUM(${TABLE}.monthly_signups)) ;;
    label: "Monthly Creator Sign-ups: Actual"
    description: "New creator sign-ups, by store-creation month, America/New_York. Must equal Total Sign-ups in prod_signup_conversion_funnel exactly — 478, 1140, 2096, 2044, 2376, 2310, 1950, 592 for Jan-Aug 2026."
  }

  measure: cumulative_creator_signups {
    type: number
    sql: IF(LOGICAL_OR(${TABLE}.is_future_month), NULL,
      MAX(${TABLE}.cumulative_signups)) ;;
    label: "Cumulative Creator Sign-ups: Actual"
    description: "Running total of all sign-ups from the first on record. MAX is correct because the value is already cumulative — never SUM it across months."
  }

  measure: monthly_new_paid_creators {
    type: number
    sql: IF(LOGICAL_OR(${TABLE}.is_future_month), NULL,
      SUM(${TABLE}.monthly_new_paid)) ;;
    label: "Monthly New Paid Subscribers: Actual"
    description: "Creators whose FIRST billable paid invoice fell in this month, restricted to the same population as the signup series. Not a conversion rate — these creators may have signed up in any earlier month."
  }

  measure: cumulative_paid_creators {
    type: number
    sql: IF(LOGICAL_OR(${TABLE}.is_future_month), NULL,
      MAX(${TABLE}.cumulative_paid)) ;;
    label: "Cumulative Paid Creators: Actual"
    description: "Running total of creators who have ever made a first payment. Already cumulative — never SUM across months."
  }

  # ——— Targets ———
  # Supplied by management and kept as given. See the header for two
  # discontinuities worth mentioning when presenting.

  measure: cumulative_signups_target {
    type: max
    sql: ${TABLE}.cumulative_signups_target ;;
    label: "Cumulative Creator Sign-ups: Target"
    description: "Reaches 51,642 by December 2026 against an all-time actual near 26,700."
  }

  measure: monthly_signups_target {
    type: sum
    sql: ${TABLE}.monthly_signups_target ;;
    label: "Monthly Creator Sign-ups: Target"
  }

  measure: monthly_paid_target {
    type: sum
    sql: ${TABLE}.monthly_paid_target ;;
    label: "Monthly New Paid Subscribers: Target"
    description: "Steps 213, 436, 528 then DROPS to 112 in April. Actuals step the opposite way at the same boundary — the 7-day trial launch. Q1 targets appear to be on a pre-trial basis."
  }

  # ——— Pro-rated targets, for the month in progress ———

  measure: monthly_signups_target_prorated {
    type: number
    sql: SAFE_DIVIDE(SUM(${TABLE}.monthly_signups_target)
                     * SUM(${TABLE}.days_elapsed),
                     SUM(${TABLE}.days_in_month)) ;;
    value_format_name: decimal_0
    label: "Monthly Sign-ups Target (Pro-rated)"
    description: "Full-month target scaled to the days elapsed. Identical to the target for closed months. For August: 3,586 x 13/31 = about 1,504, which is the honest comparison against a 592 actual instead of the full 3,586."
  }

  measure: monthly_paid_target_prorated {
    type: number
    sql: SAFE_DIVIDE(SUM(${TABLE}.monthly_paid_target)
                     * SUM(${TABLE}.days_elapsed),
                     SUM(${TABLE}.days_in_month)) ;;
    value_format_name: decimal_0
    label: "Monthly Paid Target (Pro-rated)"
    description: "Full-month target scaled to the days elapsed. Identical to the target for closed months."
  }

  # ——— Variance ———

  measure: cumulative_signups_variance {
    type: number
    sql: ${cumulative_creator_signups} - ${cumulative_signups_target} ;;
    label: "Cumulative Sign-ups Variance"
  }

  measure: monthly_signups_variance {
    type: number
    sql: ${monthly_creator_signups} - ${monthly_signups_target} ;;
    label: "Monthly Sign-ups Variance"
    description: "Against the FULL-month target. For the month in progress compare against the pro-rated version instead."
  }

  measure: monthly_paid_variance {
    type: number
    sql: ${monthly_new_paid_creators} - ${monthly_paid_target} ;;
    label: "Monthly Paid Subscribers Variance"
    description: "Against the FULL-month target. For the month in progress compare against the pro-rated version instead."
  }

  measure: pct_of_signups_target {
    type: number
    sql: SAFE_DIVIDE(${monthly_creator_signups}, ${monthly_signups_target}) ;;
    value_format_name: percent_1
    label: "% of Monthly Sign-ups Target"
  }

  measure: pct_of_paid_target {
    type: number
    sql: SAFE_DIVIDE(${monthly_new_paid_creators}, ${monthly_paid_target}) ;;
    value_format_name: percent_1
    label: "% of Monthly Paid Target"
  }
}

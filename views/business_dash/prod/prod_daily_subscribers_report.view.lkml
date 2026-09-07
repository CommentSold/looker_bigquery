# =============================================================================
# prod_daily_subscribers_report
# -----------------------------------------------------------------------------
# New subscribers per DAY, split four ways. Built for the period after the
# 2026-09-02 change that removed the free trial.
#
# GRAIN: one row per SUBSCRIPTION, at that subscription's first billable
# collected payment. A creator with three paid subscriptions has three rows.
#
# -----------------------------------------------------------------------------
# THE ANCHOR
# -----------------------------------------------------------------------------
# Payment Sequence numbers each creator's paid subscriptions in order. Sequence 1
# is the same population as prod_new_paid_subscribers_by_plan, so New Subscribers
# grouped by month MUST reproduce Monthly New Paid Subscribers: Actual.
# Verified 2026-09-05:
#
#   2026-06  388     2026-08  236
#   2026-07  357     2026-09   42   (partial)
#
# Total Subscribers counts every row and reads HIGHER — 396 / 370 / 248 / 43.
# That is the point of the grain, not a reconciliation failure.
#
# Population exclusions were audited the same day. Among first payments, the
# filters drop only internal emails (11-13 a month), vidcon (1-2) and one
# apps_pop_store case. NOTHING is lost to the dim_stores join, so no paying
# creator is dropped for lacking a store row.
#
# -----------------------------------------------------------------------------
# !! THE TRIAL IS A COUPON, NOT A PLAN FEATURE !!
# -----------------------------------------------------------------------------
# This is the single most important thing in this file. plans[].trialPeriodDays
# still reads 7 on every monthly plan and always will — the plan was never the
# source of the trial. Read trial_end off the SUBSCRIPTION. Never read the plan.
#
# Coupon history, from fact_seller_subscription:
#   4fb50491  "Trial Coupon - 14days"   80 subs   2025-09-24 to 2025-10-15
#   4fb50491  "Trial Coupon - 30days"  323 subs   2025-10-15 to 2026-01-16
#             (SAME coupon id, renamed — never filter on coupon NAME)
#   [no trial at all: 2026-01-16 to 2026-03-24]
#   4d7gad9f  "Trial Coupon - 7days" 4,179 subs   2026-03-24 to 2026-09-02 13:48
#   GtvSA4Ch  "firstmonth50%off"                  2026-09-02 12:20 onward
#
# So "the 7-day trial launched around April 2026", repeated in several view
# headers including prod_signup_conversion_funnel, is wrong twice over: it
# launched 2026-03-24, and 14- and 30-day trials ran through 2025.
#
# The trial coupon is once per creator. That is why a creator who abandons a
# trial and comes back has to pay upfront — true long before any deploy, and the
# reason the fourth segment exists.
#
# Exceptions exist and are manual: about ten "ECHO Retention Trial - <userid>"
# coupons, plus "Extra trial for Frances". One of these is why a trial appears
# at 18:56 UTC on 2026-09-02, after the cutover.
#
# -----------------------------------------------------------------------------
# !! firstmonth50%off DISTORTS REVENUE, NOT MRR !!
# -----------------------------------------------------------------------------
# GtvSA4Ch is percent_off 0.5, duration ONCE. It halves the FIRST invoice only.
# First Payment Revenue and Average Payment therefore drop across 2026-09-02
# with no change in plan mix and no change to recurring revenue from month two.
# Anyone reading that drop as "direct buyers pick cheaper plans" is wrong. Use
# Has First-Month 50% Discount to split it.
#
# One post-deploy subscription carries SIRYxL8K "1 month free" at 100% off. A
# 100% coupon produces a $0 invoice, which the billable filter excludes, so that
# creator is invisible to this view and to every other subscriber tile.
#
# -----------------------------------------------------------------------------
# THE FOUR SEGMENTS
# -----------------------------------------------------------------------------
#   New - converted from trial          trial_end later than the start
#   New - direct after abandoned trial  no trial here, but started one on an
#                                       EARLIER subscription that never billed,
#                                       so the coupon was already spent
#   New - direct purchase               no trial, never had one
#   Returning subscriber                sequence 2+
#
# Measured (eligible population only):
#   month  trial  after-abandoned  direct  returning  total
#   Jun     371        14             3         8      396
#   Jul     332        25             0        13      370
#   Aug     210        18             8        12      248
#   Sep      27         2            13         1       43
#
# July's 0 in Direct is real: pre-deploy almost nobody paid upfront without
# having spent a trial first. September inverts it, 13 against 2. That inversion
# is the deploy.
#
# The trial segment drains to 2026-10-02: 87 in-flight trials resolve by 09-09,
# then stragglers on 09-19, 09-21, 09-23, 09-24 and 10-02.
#
# -----------------------------------------------------------------------------
# !! RETURNING vs UPGRADE — the weak point !!
# -----------------------------------------------------------------------------
# Sequence 2+ is labelled "Returning subscriber", but this view cannot tell a
# reactivation from a plan upgrade. Handoff open item #5 records a creator with
# three sequential subscription_ids, each starting seconds after the previous
# was cancelled. Every one of those looks like a return here.
#
# Return Gap separates them: an upgrade lands within hours, a reactivation after
# weeks. No threshold is hardcoded — picking one silently is how a plausible
# wrong number gets shipped.
#
# -----------------------------------------------------------------------------
# DEPLOY TIMESTAMP
# -----------------------------------------------------------------------------
# 2026-09-02 15:30 UTC (21:00 IST, per QA). Corroborated: among trial-eligible
# monthly signups the last trial coupon was issued 13:48:04 UTC and the new
# coupon takes over from 18:37:48 UTC, so 15:30 sits inside the window. The two
# coupons interleave between 12:20 and 13:48, which looks like a staged rollout
# rather than an instant switch.
#
# Only Is Post Trial Removal depends on this constant. The four-way split reads
# trial_end and is unaffected.
# =============================================================================

view: prod_daily_subscribers_report {
  derived_table: {
    sql:
      WITH
      -- Change the deploy instant here and nowhere else.
      deploy_marker AS (
        SELECT TIMESTAMP('2026-09-02 15:30:00+00') AS trial_removal_ts
      ),

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

      -- Population IDENTICAL to prod_new_paid_subscribers_by_plan and
      -- prod_cumulative_creator_signups. Copied deliberately, not paraphrased:
      -- if these drift apart the anchor breaks and nothing in Looker says so.
      -- Audited 2026-09-05 against first payments: excludes internal emails
      -- (11-13/month), vidcon (1-2/month) and one apps_pop_store case. The
      -- dim_stores INNER JOIN drops no payers at all.
      eligible_creators AS (
      SELECT
      p.user_id,
      MIN(DATE(s.created_at, 'America/New_York')) AS signup_date
      FROM `popshoplive-26f81.dbt_popshop.dim_profiles` p
      INNER JOIN `popshoplive-26f81.dbt_popshop.dim_stores` s
      ON p.user_id = s.store_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = p.user_id
      LEFT JOIN marketing_capture mc       ON mc.user_id = p.user_id
      LEFT JOIN onboarding_events_dedup oe ON oe.user_id = p.user_id
      WHERE p.apps_pop_store = TRUE
      AND p.user_type IN ('seller', 'verifiedSeller')
      -- COALESCE around the comparison, never NOT (COALESCE(...) = 'x'):
      -- the latter is NULL when both sides are NULL and drops those rows.
      AND NOT COALESCE(COALESCE(oe.utm_regintent, mc.utm_regintent) = 'vidcon', FALSE)
      AND (pprof.email IS NULL
      OR NOT REGEXP_CONTAINS(LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'))
      GROUP BY p.user_id
      ),

      -- Every trial this creator has ever started, including on subscriptions
      -- that never billed and so appear nowhere else in this view. The trial
      -- coupon is once per creator, so a prior trial means the next
      -- subscription had to pay upfront.
      prior_trials AS (
      SELECT user_id, MIN(initial_start_date) AS first_trial_start_ts
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription`
      WHERE is_deleted = FALSE
      AND trial_end > initial_start_date
      AND EXISTS (SELECT 1 FROM UNNEST(plans) AS pl
      WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') = 'plan')
      GROUP BY user_id
      ),

      -- First billable collected payment PER SUBSCRIPTION.
      -- COALESCE(created, created_at) is the Stripe epoch. Do NOT use
      -- updated_at: measured 124 days late on average for January invoices,
      -- because every historical row was rewritten in one pass around May 2026.
      sub_first_payment AS (
      SELECT
      user_id,
      subscription_id,
      invoice_id,
      COALESCE(created, created_at) AS paid_ts,
      -- CENTS in the source. 2354 = $23.54, confirmed against Stripe.
      amount_paid / 100 AS payment_amount
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      AND status = 'paid'
      AND amount_due  > 0
      AND amount_paid > 0
      QUALIFY ROW_NUMBER() OVER (
      PARTITION BY subscription_id
      ORDER BY COALESCE(created, created_at) ASC, invoice_id ASC
      ) = 1
      ),

      sequenced AS (
      SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY user_id
      ORDER BY paid_ts ASC, subscription_id ASC) AS payment_sequence,
      LAG(paid_ts) OVER (PARTITION BY user_id
      ORDER BY paid_ts ASC, subscription_id ASC) AS prev_paid_ts,
      LAG(subscription_id) OVER (PARTITION BY user_id
      ORDER BY paid_ts ASC, subscription_id ASC) AS prev_subscription_id
      FROM sub_first_payment
      ),

      subscription_plan AS (
      SELECT
      t1.subscription_id,
      t1.initial_start_date,
      t1.trial_end,
      t1.status AS subscription_status_now,
      JSON_VALUE(t1.subscription, '$.coupon.id')   AS coupon_id,
      JSON_VALUE(t1.subscription, '$.coupon.name') AS coupon_name,
      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval')    AS plan_interval,
      SAFE_CAST(JSON_EXTRACT_SCALAR(plan, '$.amount') AS NUMERIC) AS current_base_price,
      COUNT(*) OVER (PARTITION BY t1.subscription_id) AS plan_entries
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS plan
      WHERE t1.is_deleted = FALSE
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      QUALIFY ROW_NUMBER() OVER (
      PARTITION BY t1.subscription_id
      ORDER BY JSON_EXTRACT_SCALAR(plan, '$.productName'),
      JSON_EXTRACT_SCALAR(plan, '$.interval')
      ) = 1
      )

      SELECT
      sq.user_id,
      sq.subscription_id,
      sq.invoice_id,
      CONCAT(sq.user_id, '-', sq.subscription_id) AS row_key,

      DATE(sq.paid_ts, 'America/New_York') AS paid_date,
      DATETIME(sq.paid_ts, 'America/New_York') AS paid_at_et,
      sq.payment_amount,
      sq.payment_sequence,
      sq.prev_subscription_id,
      DATE_DIFF(DATE(sq.paid_ts, 'America/New_York'),
      DATE(sq.prev_paid_ts, 'America/New_York'), DAY) AS days_since_previous_payment,

      ec.signup_date,
      DATE_DIFF(DATE(sq.paid_ts, 'America/New_York'), ec.signup_date, DAY) AS days_signup_to_payment,
      DATE_DIFF(DATE(sq.paid_ts, 'America/New_York'),
      DATE(sp.initial_start_date, 'America/New_York'), DAY) AS days_sub_start_to_payment,

      sp.plan_name,
      sp.plan_interval,
      sp.current_base_price,
      sp.subscription_status_now,
      sp.coupon_id,
      sp.coupon_name,
      COALESCE(sp.plan_entries, 0) AS plan_entries,

      DATE(sp.initial_start_date, 'America/New_York') AS subscription_start_date,
      DATE(sp.trial_end, 'America/New_York')          AS trial_end_date,
      DATE_DIFF(DATE(sp.trial_end, 'America/New_York'),
      DATE(sp.initial_start_date, 'America/New_York'), DAY) AS trial_length_days,

      sp.trial_end IS NOT NULL
      AND sp.trial_end > sp.initial_start_date AS came_via_trial,

      COALESCE(pt.first_trial_start_ts < sp.initial_start_date, FALSE) AS had_prior_trial,
      DATE(pt.first_trial_start_ts, 'America/New_York') AS first_trial_start_date,

      sq.payment_sequence > 1 AS is_returning,

      -- Order matters: the trial branch must precede the abandoned-trial
      -- branch, because a creator can have both an earlier abandoned trial
      -- and a trial on the subscription that paid.
      CASE
      WHEN sq.payment_sequence > 1 THEN 'Returning subscriber'
      WHEN sp.trial_end IS NOT NULL AND sp.trial_end > sp.initial_start_date
      THEN 'New - converted from trial'
      WHEN COALESCE(pt.first_trial_start_ts < sp.initial_start_date, FALSE)
      THEN 'New - direct after abandoned trial'
      ELSE 'New - direct purchase'
      END AS subscriber_type,

      sq.paid_ts >= dm.trial_removal_ts AS is_post_trial_removal

      FROM sequenced sq
      CROSS JOIN deploy_marker dm
      INNER JOIN eligible_creators ec ON ec.user_id = sq.user_id
      LEFT JOIN subscription_plan sp  ON sp.subscription_id = sq.subscription_id
      LEFT JOIN prior_trials pt       ON pt.user_id = sq.user_id
      ;;
  }

  # ——— Primary Key ———

  dimension: row_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: ${TABLE}.row_key ;;
  }

  dimension: user_id         { type: string sql: ${TABLE}.user_id ;;         label: "User ID" }
  dimension: subscription_id { type: string sql: ${TABLE}.subscription_id ;; label: "Subscription ID" }
  dimension: invoice_id      { type: string sql: ${TABLE}.invoice_id ;;      label: "Invoice ID" }

  # ——— Time ———

  dimension_group: paid {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.paid_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Paid"
    description: "Day of this subscription's first billable collected payment, America/New_York. The x-axis."
  }

  dimension_group: paid_at {
    type: time
    timeframes: [time, hour, minute, hour_of_day, day_of_week]
    datatype: datetime
    convert_tz: no
    sql: ${TABLE}.paid_at_et ;;
    label: "Paid At (ET)"
    description: "Exact moment the first billable payment was collected, in America/New_York. Converted in SQL, not by Looker: this model sets no query_timezone, so convert_tz would fall back to a connection default that an admin can change and that may resolve per-user. Stripe's own timestamp, not our ingestion time."
  }

  dimension_group: signup {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.signup_date ;;
    timeframes: [date, week, month]
    label: "Signup"
    description: "Store creation date. A DIFFERENT axis from Paid — do not mix the two on one chart."
  }

  dimension_group: subscription_start {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.subscription_start_date ;;
    timeframes: [date, week, month]
    label: "Subscription Start"
  }

  dimension_group: trial_end {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.trial_end_date ;;
    timeframes: [date, week, month]
    label: "Trial End"
  }

  dimension_group: first_trial_start {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.first_trial_start_date ;;
    timeframes: [date, week, month]
    label: "First Trial Start"
    description: "When this creator first started ANY trial, including on subscriptions that never billed."
  }

  # ——— The split ———

  dimension: subscriber_type {
    type: string
    sql: ${TABLE}.subscriber_type ;;
    label: "Subscriber Type"
    description: "Four mutually exclusive values. PIVOT ON THIS. 'Direct after abandoned trial' was almost the whole direct segment before 2026-09-02, because the trial coupon is once per creator; after the deploy 'Direct purchase' takes over. Post-deploy the two mean the same thing operationally, since nobody gets a trial."
  }

  dimension: came_via_trial {
    type: yesno
    sql: ${TABLE}.came_via_trial ;;
    label: "Came Via Trial"
    description: "Reads this subscription's own trial_end. NEVER read plans[].trialPeriodDays — it still says 7 on every monthly plan because the trial was a coupon, not a plan feature."
  }

  dimension: had_prior_trial {
    type: yesno
    sql: ${TABLE}.had_prior_trial ;;
    label: "Had Prior Trial"
    description: "Started a trial on an EARLIER subscription that never billed. Coupon 4d7gad9f is once per creator, so they had to pay upfront. True long before the 2026-09-02 deploy."
  }

  dimension: is_returning {
    type: yesno
    sql: ${TABLE}.is_returning ;;
    label: "Is Returning"
    description: "Not this creator's first paid subscription. Check Return Gap before calling it a reactivation — plan upgrades mint a new subscription_id and land here too."
  }

  dimension: payment_sequence {
    type: number
    sql: ${TABLE}.payment_sequence ;;
    label: "Payment Sequence"
    description: "1 for a creator's first paid subscription, 2 for their second. Sequence 1 is exactly the population prod_new_paid_subscribers_by_plan counts."
  }

  dimension: days_since_previous_payment {
    type: number
    sql: ${TABLE}.days_since_previous_payment ;;
    label: "Days Since Previous Payment"
    description: "Gap to this creator's previous paid subscription. NULL on sequence 1."
  }

  dimension: return_gap {
    type: tier
    tiers: [1, 7, 30, 90, 365]
    style: integer
    sql: ${TABLE}.days_since_previous_payment ;;
    label: "Return Gap"
    description: "Banded gap to the previous paid subscription. Under a day is almost certainly a plan upgrade, not a return. Check this distribution before quoting Returning Subscribers."
  }

  dimension: prev_subscription_id {
    type: string
    sql: ${TABLE}.prev_subscription_id ;;
    label: "Previous Subscription ID"
  }

  dimension: is_post_trial_removal {
    type: yesno
    sql: ${TABLE}.is_post_trial_removal ;;
    label: "Is Post Trial Removal"
    description: "Payment landed after 2026-09-02 15:30 UTC. The four-way split does not depend on this — it reads trial_end."
  }

  # ——— Coupon ———

  dimension: coupon_name {
    type: string
    sql: ${TABLE}.coupon_name ;;
    label: "Coupon"
    description: "The trial was a COUPON, never a plan feature. 4d7gad9f 'Trial Coupon - 7days', 4,179 subs, 2026-03-24 to 2026-09-02 13:48 UTC. Before it, 4fb50491 ran as 14-day then 30-day through 2026-01-16 — same id, renamed, which is why you must filter on Coupon ID and not on this field. Replaced 2026-09-02 by GtvSA4Ch 'firstmonth50%off'."
  }

  dimension: coupon_id {
    type: string
    sql: ${TABLE}.coupon_id ;;
    label: "Coupon ID"
    description: "Filter on this, never on Coupon name — 4fb50491 has already been renamed once, and a string filter on a renamed value silently returns 0."
  }

  dimension: is_first_month_discount {
    type: yesno
    sql: ${TABLE}.coupon_id = 'GtvSA4Ch' ;;
    label: "Has First-Month 50% Discount"
    description: "Post-deploy replacement for the trial. percent_off 0.5, duration ONCE, so it halves the FIRST invoice only. First Payment Revenue and Average Payment drop across 2026-09-02 with no change in plan mix and no change to MRR from month two. Do not read that drop as customers buying cheaper plans."
  }

  # ——— Maturity / verification ———

  dimension: trial_length_days {
    type: number
    sql: ${TABLE}.trial_length_days ;;
    label: "Trial Length (Days)"
    description: "7 for standard trial converters, NULL for direct purchases. A handful of manually granted extended trials run to 2026-10-02."
  }

  dimension: days_sub_start_to_payment {
    type: number
    sql: ${TABLE}.days_sub_start_to_payment ;;
    label: "Days Subscription Start to Payment"
    description: "7 for a trial conversion, 0 for a direct purchase. THE deploy indicator. Independent of signup date, unlike Days Signup to Payment, which on the direct segment is dominated by long-dormant creators and reads in the hundreds."
  }

  dimension: days_signup_to_payment {
    type: number
    sql: ${TABLE}.days_signup_to_payment ;;
    label: "Days Signup to Payment"
    description: "From signup, not from subscription start. Reads in the hundreds for dormant creators who return and buy, so it is NOT a deploy indicator — use Days Subscription Start to Payment for that."
  }

  # ——— Plan ———

  dimension: plan_name {
    type: string
    sql: COALESCE(${TABLE}.plan_name, '(unknown)') ;;
    label: "Plan Name"
    description: "CURRENT state from plans[], not the plan as at payment. Payment Amount is the historical figure — trust it where the two disagree."
  }

  dimension: plan_interval {
    type: string
    sql: ${TABLE}.plan_interval ;;
    label: "Interval"
  }

  dimension: plan_interval_combined {
    type: string
    sql: CONCAT(COALESCE(${TABLE}.plan_name, '(unknown)'), ': ',
      COALESCE(${TABLE}.plan_interval, '(unknown)')) ;;
    label: "Plan: Interval"
    description: "Matches the pivot in prod_new_paid_subscribers_by_plan, so plan colours stay consistent across tiles."
  }

  dimension: payment_amount {
    type: number
    sql: ${TABLE}.payment_amount ;;
    value_format_name: usd
    label: "Payment Amount"
    description: "Collected on this subscription's first billable invoice, in DOLLARS (source is cents). Real history. Halved from 2026-09-02 for anyone on firstmonth50%off."
  }

  dimension: subscription_status_now {
    type: string
    sql: ${TABLE}.subscription_status_now ;;
    label: "Subscription Status Now"
    description: "Status TODAY, not at payment. NEVER filter the historical series on this — it reintroduces the survivorship bug."
  }

  dimension: plan_entries {
    type: number
    sql: ${TABLE}.plan_entries ;;
    label: "Plan Entries"
    description: "planType='plan' entries on the subscription. Should be 1. 0 means the subscription row could not be resolved."
  }

  # ——— Measures ———
  # Every segment measure filters on BOOLEAN dimensions, never on
  # subscriber_type strings. A string filter must match the CASE branch
  # character for character, so relabelling a segment silently returns 0.

  measure: total_subscribers {
    type: count_distinct
    sql: ${TABLE}.row_key ;;
    label: "Total Subscribers"
    description: "Every subscription that started paying on this day, new and returning. The full height of the stacked bar. HIGHER than New Subscribers by design — 396 / 370 / 248 for Jun-Aug 2026."
    drill_fields: [detail*]
  }

  measure: new_subscribers {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [payment_sequence: "1"]
    label: "New Subscribers"
    description: "First-time paying creators only. THE ANCHOR: group by month and this must reproduce Monthly New Paid Subscribers: Actual — 388, 357, 236 for Jun-Aug 2026. If it does not, the population filters have drifted."
    drill_fields: [detail*]
  }

  measure: new_trial_converters {
    type: count_distinct
    sql: ${TABLE}.row_key ;;
    filters: [payment_sequence: "1", came_via_trial: "yes"]
    label: "New - Converted From Trial"
    description: "Draining. In-flight trials resolve through 2026-09-09, with stragglers to 2026-10-02."
    drill_fields: [detail*]
  }

  measure: new_direct_after_abandoned_trial {
    type: count_distinct
    sql: ${TABLE}.row_key ;;
    filters: [payment_sequence: "1", came_via_trial: "no", had_prior_trial: "yes"]
    label: "New - Direct After Abandoned Trial"
    description: "First-time payer who had already spent their trial coupon on a subscription that never billed. Not a returning subscriber — they have never paid before. 14 / 25 / 18 for Jun-Aug 2026."
    drill_fields: [detail*]
  }

  measure: new_direct_subscribers {
    type: count_distinct
    sql: ${TABLE}.row_key ;;
    filters: [payment_sequence: "1", came_via_trial: "no", had_prior_trial: "no"]
    label: "New - Direct Purchase"
    description: "Paid upfront, never had a trial at all. Near zero before the deploy (3 / 0 / 8 for Jun-Aug) and the dominant segment after it."
    drill_fields: [detail*]
  }

  measure: returning_subscribers {
    type: count_distinct
    sql: ${TABLE}.row_key ;;
    filters: [is_returning: "yes"]
    label: "Returning Subscribers"
    description: "Second or later paid subscription for this creator. Includes plan upgrades, which mint a new subscription_id and are indistinguishable from reactivations here — split by Return Gap before quoting it. 8 / 13 / 12 for Jun-Aug 2026."
    drill_fields: [detail*]
  }

  measure: returning_within_a_day {
    type: count_distinct
    sql: ${TABLE}.row_key ;;
    filters: [is_returning: "yes", days_since_previous_payment: "<1"]
    label: "Returning Within a Day (Likely Upgrades)"
    description: "Paid again within a day of the previous subscription. Almost certainly upgrades. Watch as a share of Returning Subscribers — if large, that segment is mislabelled."
    drill_fields: [detail*]
  }

  measure: payment_revenue {
    type: sum
    sql: ${TABLE}.payment_amount ;;
    value_format_name: usd
    label: "First Payment Revenue"
    description: "Collected on these first invoices only. Not recurring revenue and not MRR. Depressed from 2026-09-02 by firstmonth50%off, which halves the first invoice."
  }

  measure: avg_payment {
    type: average
    sql: ${TABLE}.payment_amount ;;
    value_format_name: usd
    label: "Average Payment"
    description: "Split by Has First-Month 50% Discount before comparing across 2026-09-02, or the coupon will look like a shift in plan mix."
  }

  measure: median_days_sub_start_to_payment {
    type: median
    sql: ${TABLE}.days_sub_start_to_payment ;;
    label: "Median Days Subscription Start to Payment"
    description: "THE deploy indicator. 7 while trials existed, 0 after. Filter to Payment Sequence = 1 and do not pivot."
  }

  # ——— Guards ———

  measure: subscriber_type_accounting_check {
    type: number
    sql: ${total_subscribers}
         - ${new_trial_converters}
         - ${new_direct_after_abandoned_trial}
         - ${new_direct_subscribers}
         - ${returning_subscribers} ;;
    label: "Subscriber Type Accounting Check"
    description: "Must always be 0. Verifies the four stack segments sum to Total Subscribers."
  }

  measure: unknown_plan {
    type: count_distinct
    sql: ${TABLE}.row_key ;;
    filters: [plan_entries: "0"]
    label: "Unknown Plan"
    description: "Subscription has no resolvable plan row. Should be 0."
    drill_fields: [detail*]
  }

  measure: returning_with_a_trial {
    type: count_distinct
    sql: ${TABLE}.row_key ;;
    filters: [is_returning: "yes", came_via_trial: "yes", is_post_trial_removal: "yes"]
    label: "Returning With a Trial (Should Be 0)"
    description: "A returning subscriber granted a trial after 2026-09-02. Should be impossible. Non-zero means either the change missed the reactivation path or someone issued a manual ECHO Retention Trial coupon."
    drill_fields: [detail*]
  }

  # ——— Drill Set ———

  set: detail {
    fields: [
      user_id,
      subscription_id,
      paid_at_time,
      subscriber_type,
      payment_sequence,
      days_since_previous_payment,
      prev_subscription_id,
      coupon_name,
      plan_name,
      plan_interval,
      payment_amount,
      signup_date,
      days_signup_to_payment,
      subscription_start_date,
      days_sub_start_to_payment,
      trial_end_date,
      trial_length_days,
      first_trial_start_date,
      subscription_status_now
    ]
  }
}

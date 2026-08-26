# =============================================================================
# prod_signup_conversion_funnel
# -----------------------------------------------------------------------------
# What happened to each signup cohort: did they subscribe, and did they pay?
#
# GRAIN: one row per creator (user_id).
#
# -----------------------------------------------------------------------------
# TERMINOLOGY -- "TRIAL START", NOT "SUBSCRIBE"
# -----------------------------------------------------------------------------
# Field labels use TRIAL language because that is how the business talks:
# "subscribed" means PAYING, and a free trial is a "trial".
#
#   Trial Start = a subscription record was created. Free, no money moves.
#   Paid        = an invoice was actually charged and collected.
#
# Paid creators are a SUBSET of trial starts — you cannot pay without a
# subscription existing first. That is why paid creators legitimately appear in
# the trial-start drill-down.
#
# !! CAVEAT: BEFORE APRIL 2026 THERE WAS NO TRIAL !!
# For Jan-Mar 2026 cohorts, "Trial Start" actually means "subscribed and paid
# immediately". February shows 47 trial starts and 47 paid — zero gap — because
# starting a subscription WAS paying. The label is anachronistic for those
# cohorts, and the blue line changes meaning partway across the chart. Say which
# side of the launch you are describing.
#
# The underlying LookML field NAMES still say subscribe (subscribed_within_7d,
# signup_to_subscribe_7d, and so on). Only labels and descriptions changed, so
# existing tiles keep working. Renaming the fields would break them.
#
# -----------------------------------------------------------------------------
# !! THE 7-DAY TRIAL LAUNCHED AROUND APRIL 2026 !!
# -----------------------------------------------------------------------------
# median_days_to_pay by signup cohort: 0, 0, 1, 7, 7, 7, 7, 7
# (Jan, Feb, Mar, Apr, May, Jun, Jul, Aug 2026)
#
# Before April, creators paid immediately. From April, they take a 7-day trial
# first. That changes the funnel's shape, not just its level:
#
#   cohort  trial start   paid    paid/trial start
#   Jan          16.9%   12.3%          73%
#   Feb           7.4%    5.4%          73%
#   Mar           8.6%    4.2%          49%
#   Apr          27.1%   10.1%          37%
#   May          36.8%   16.3%          44%
#   Jun          54.1%   12.3%          23%
#   Jul          41.0%   17.6%          43%
#
# Pre-trial: few started, most paid. Post-trial: many start a trial, fewer
# convert. A trend line spanning both is plotting two different funnels, and
# "trial start rate rose from 17% to 54%" is largely "we added a free trial".
#
# Pre-April cohorts are deliberately NOT filtered out — the history is real and
# excluding it silently would be worse. But say which side of the change you are
# describing, and consider splitting the chart at April.
#
# -----------------------------------------------------------------------------
# WHAT WAS FIXED
# -----------------------------------------------------------------------------
# 1. PAID-EVER MADE COHORTS INCOMPARABLE. The old view reported "has ever paid",
#    which favours old cohorts. August read 8.1% against July's 17.6% and looked
#    like a collapse; it was creators who signed up in the last week and whose
#    trial has not ended yet.
#
#    Conversion turns out to complete almost entirely within 7 days:
#      cohort    7d     30d    60d    ever
#      May     14.9%   15.9%  16.1%  16.3%
#      Jun     10.9%   11.9%  12.3%  12.3%
#      Jul     16.7%   17.6%  17.6%  17.6%
#    So the fix is a 7-day window with PER-CREATOR maturity (Is Observable 7d),
#    not a 30- or 60-day one with per-cohort maturity. A partial month now
#    reports on its mature portion instead of reading artificially low.
#
# 2. "PAID IN SIGNUP MONTH" vs "PAID IN A LATER MONTH" measured the day of the
#    month, not behaviour. With a 7-day median, anyone signing up after roughly
#    the 24th lands in "later month" by calendar position alone. Replaced with
#    Days to Pay bands.
#
# 3. NO TIMEZONE on any date conversion, so month boundaries sat four hours off
#    every other view. Measured: 153 creators of 29,096 changed signup month;
#    5,120 changed signup day. The 17.6% day-change rate is almost exactly
#    4/24 hours, confirming created_at is a genuine UTC instant and the
#    conversion is correct here (unlike stripe_analytical_data.event_date,
#    which is a local wall clock and must NOT be converted).
#
# 4. MIN(created_at) on invoices is now MIN(COALESCE(created, created_at)),
#    matching prod_subscription_churn.
#
# -----------------------------------------------------------------------------
# TWO THINGS I EXPECTED TO BE BUGS AND WERE NOT — verified, do not "fix"
# -----------------------------------------------------------------------------
# * The payment side joins on user_id with no restriction to plan subscriptions.
#   Checked: 2,006 creators have a paid invoice, and all 2,006 have one on a PLAN
#   subscription. Zero misclassified, zero invoices with an unresolvable
#   subscription_id. The user_id join is safe.
# * INNER JOIN dim_stores ON user_id = store_id drops creators with no store.
#   Checked: 1 creator of 27,158, and no user has two store rows. Store creation
#   is effectively coincident with signup.
#
# -----------------------------------------------------------------------------
# KNOWN ANOMALY — the week of 2026-06-22
# -----------------------------------------------------------------------------
#   1,207 signups against ~580 in neighbouring weeks
#   78.1% subscribed against 38-48%
#   6.6% paid against 15-21%
# Double the volume, double the subscribe rate, a third of the paid rate. A
# campaign that filled the trial funnel with creators who did not convert. This
# single week is most of why June's cohort has the worst subscribe-to-paid ratio
# in the series (23%). Not a denominator artifact — the volume is real.
# =============================================================================

view: prod_signup_conversion_funnel {
  derived_table: {
    sql:
      WITH
      -- EXISTS cannot sit in a JOIN predicate in BigQuery, so the planType
      -- filter lives here and other CTEs join to it.
      plan_subs AS (
        SELECT
          subscription_id,
          user_id,
          status,
          DATE(created_at, 'America/New_York') AS sub_created_date
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription`
        WHERE is_deleted = FALSE
          AND EXISTS (SELECT 1 FROM UNNEST(plans) AS pl
                      WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') = 'plan')
      ),

      onboarding_events AS (
      SELECT
      context_campaign_campaign AS marketing_campaign,
      context_campaign_onboarding_path AS onboarding_path,
      context_campaign_planlevel AS plan_level,
      context_user_agent AS user_agent,
      utm_regintent,
      business_type,
      `timestamp`,
      user_id,
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      WHERE (scene = 'onboarding' OR scene IS NULL)
      AND (step_name = 'onboarding_complete' OR step_name IS NULL)
      ),

      onboarding_events_dedup AS (
      SELECT
      user_id,
      marketing_campaign,
      onboarding_path,
      plan_level,
      utm_regintent,
      business_type,
      `timestamp`,
      user_agent
      FROM onboarding_events
      QUALIFY ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY
      CASE
      WHEN marketing_campaign IS NOT NULL
      OR (utm_regintent IS NOT NULL AND utm_regintent != 'generic')
      OR (business_type IS NOT NULL AND business_type != 'generic')
      THEN 0 ELSE 1
      END,
      `timestamp` DESC
      ) = 1
      ),

      marketing_capture AS (
        SELECT
        user_id,
        JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_campaign') AS utm_campaign,
        JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_source') AS utm_source,
        JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent') AS utm_regintent,
        JSON_VALUE(private_profile, '$.onboardingMarketingCapture.url') AS url,
        JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent') AS user_agent,
        JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_onboarding_path') AS onboarding_path,
        JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_planlevel') AS plan_level,
        JSON_VALUE(private_profile, '$.email') AS profile_email,
        JSON_VALUE(private_profile, '$.sellerShippingAddress.firstName') AS first_name,
        JSON_VALUE(private_profile, '$.sellerShippingAddress.lastName')  AS last_name,
        FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      ),

      signups AS (
      SELECT
      p.user_id,
      p.username,
      p.url_code,
      -- dim_private_profiles is one row per user (verified), so ANY_VALUE
      -- cannot pick arbitrarily between differing values here.
      ANY_VALUE(pprof.email) AS email,
      MIN(DATE(s.created_at, 'America/New_York')) AS signup_date
      FROM `popshoplive-26f81.dbt_popshop.dim_profiles` p
      INNER JOIN `popshoplive-26f81.dbt_popshop.dim_stores` s
      ON p.user_id = s.store_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = p.user_id
      LEFT JOIN marketing_capture mc ON mc.user_id = p.user_id
      LEFT JOIN onboarding_events_dedup oe ON oe.user_id = p.user_id
      WHERE p.apps_pop_store = TRUE
      AND p.user_type IN ('seller', 'verifiedSeller')
      AND (
        COALESCE(oe.utm_regintent, mc.utm_regintent) IS NULL
        OR COALESCE(oe.utm_regintent, mc.utm_regintent) NOT IN ("vidcon")
      )
      AND (pprof.email IS NULL
      OR NOT REGEXP_CONTAINS(LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'))
      GROUP BY p.user_id, p.username, p.url_code
      ),

      user_subscriptions AS (
      SELECT
      user_id,
      COUNT(DISTINCT subscription_id) AS total_subs,
      MIN(sub_created_date) AS first_sub_date,
      STRING_AGG(DISTINCT subscription_id, ', ' ORDER BY subscription_id) AS all_subscription_ids,
      STRING_AGG(DISTINCT status, ', ' ORDER BY status) AS distinct_sub_statuses,
      -- Explicit booleans rather than CONTAINS_SUBSTR on a concatenated
      -- string. None of the statuses is currently a substring of another,
      -- but the string test would break silently if one ever were.
      LOGICAL_OR(status = 'active')   AS has_active,
      LOGICAL_OR(status = 'trialing') AS has_trialing,
      LOGICAL_OR(status = 'past_due') AS has_past_due,
      LOGICAL_OR(status = 'canceled') AS has_canceled,
      LOGICAL_OR(status = 'unpaid')   AS has_unpaid
      FROM plan_subs
      GROUP BY user_id
      ),

      user_payment_history AS (
      SELECT
      i.user_id,
      COUNT(DISTINCT i.invoice_id) AS total_billable_paid_invoices,
      MIN(DATE(COALESCE(i.created, i.created_at), 'America/New_York')) AS first_paid_date,
      STRING_AGG(DISTINCT i.subscription_id, ', ' ORDER BY i.subscription_id) AS paid_subscription_ids
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice` i
      WHERE i.is_deleted = FALSE
      AND i.status = 'paid'
      AND i.amount_due  > 0
      AND i.amount_paid > 0
      GROUP BY i.user_id
      )

      SELECT
      s.user_id,
      s.username AS sign_up_user_username,
      s.email    AS sign_up_user_email,
      'https://pop.store/' || s.url_code AS sign_up_user_url,
      s.signup_date,

      COALESCE(us.total_subs, 0) AS total_subscriptions,
      us.all_subscription_ids,
      us.distinct_sub_statuses AS subscription_statuses,
      us.first_sub_date,

      COALESCE(uph.total_billable_paid_invoices, 0) AS total_paid_invoices,
      uph.first_paid_date,
      uph.paid_subscription_ids,

      -- ══════════════════════════════════════════════════════════════
      -- MATURITY, per creator rather than per cohort. A creator who
      -- signed up four days ago cannot have converted yet; counting them
      -- as a non-converter is what made the current month read low.
      -- ══════════════════════════════════════════════════════════════
      DATE_DIFF(CURRENT_DATE('America/New_York'), s.signup_date, DAY) AS days_since_signup,
      DATE_DIFF(CURRENT_DATE('America/New_York'), s.signup_date, DAY) >= 7  AS is_observable_7d,
      DATE_DIFF(CURRENT_DATE('America/New_York'), s.signup_date, DAY) >= 30 AS is_observable_30d,

      DATE_DIFF(uph.first_paid_date, s.signup_date, DAY) AS days_to_pay,
      DATE_DIFF(us.first_sub_date,   s.signup_date, DAY) AS days_to_subscribe,

      -- Fixed-window conversion. Only meaningful alongside the matching
      -- Is Observable filter.
      COALESCE(DATE_DIFF(uph.first_paid_date, s.signup_date, DAY) <= 7,  FALSE) AS paid_within_7d,
      COALESCE(DATE_DIFF(uph.first_paid_date, s.signup_date, DAY) <= 30, FALSE) AS paid_within_30d,
      COALESCE(DATE_DIFF(us.first_sub_date,   s.signup_date, DAY) <= 7,  FALSE) AS subscribed_within_7d,
      COALESCE(DATE_DIFF(us.first_sub_date,   s.signup_date, DAY) <= 30, FALSE) AS subscribed_within_30d,
      COALESCE(DATE_DIFF(uph.first_paid_date, us.first_sub_date, DAY) <= 8, FALSE)
        AND COALESCE(DATE_DIFF(us.first_sub_date, s.signup_date, DAY) <= 7, FALSE)
        AS paid_within_8d_of_trial,

      -- TRUE (not FALSE) when first_sub_date IS NULL. A creator who never
      -- started a trial sits on neither side of a trial-denominated ratio, so
      -- excluding them would silently gut every signup-denominated measure on
      -- the same tile — Signup → Trial Start would read ~100%.
      COALESCE(DATE_DIFF(CURRENT_DATE('America/New_York'), us.first_sub_date, DAY) >= 7, TRUE)
        AS is_trial_observable_7d,

      DATE_DIFF(uph.first_paid_date, us.first_sub_date, DAY) AS days_trial_to_pay,

      uph.total_billable_paid_invoices > 0 AS ever_paid,
      us.total_subs > 0                    AS ever_subscribed,

      -- Replaces "Paid in signup month" / "Paid in a later month", which
      -- sorted creators by the day of the month they happened to sign up.
      CASE
      WHEN uph.first_paid_date IS NULL THEN 'Never paid'
      WHEN DATE_DIFF(uph.first_paid_date, s.signup_date, DAY) <= 0  THEN 'Same day'
      WHEN DATE_DIFF(uph.first_paid_date, s.signup_date, DAY) <= 7  THEN 'Within 7 days (trial end)'
      WHEN DATE_DIFF(uph.first_paid_date, s.signup_date, DAY) <= 30 THEN '8-30 days'
      WHEN DATE_DIFF(uph.first_paid_date, s.signup_date, DAY) <= 90 THEN '31-90 days'
      ELSE '5. Over 90 days'
      END AS days_to_pay_band,

      -- ══════════════════════════════════════════════════════════════
      -- COMPLETE 7-DAY OUTCOME. Four mutually exclusive buckets that sum
      -- to Total Sign-ups, so nothing is unaccounted for. Unlike
      -- funnel_bucket, which reads CURRENT subscription status, this is
      -- fixed at the 7-day window and therefore comparable across cohorts.
      -- ══════════════════════════════════════════════════════════════
      CASE
      WHEN COALESCE(DATE_DIFF(uph.first_paid_date, s.signup_date, DAY) <= 7, FALSE)
      THEN 'Paid within 7 days'
      WHEN COALESCE(DATE_DIFF(us.first_sub_date, s.signup_date, DAY) <= 7, FALSE)
      THEN 'Trial started, not converted'
      WHEN us.total_subs > 0
      THEN 'Trial started after 7 days'
      ELSE 'Never started a trial'
      END AS outcome_7d,

      CASE
      WHEN uph.total_billable_paid_invoices > 0 THEN 'Paid'
      WHEN us.has_trialing                      THEN 'Currently in trial'
      WHEN us.has_active OR us.has_past_due     THEN 'Active, not yet billed'
      WHEN us.has_unpaid                        THEN 'Payment failed'
      WHEN us.has_canceled                      THEN 'Trial cancelled without paying'
      WHEN us.total_subs > 0                    THEN 'Other status'
      ELSE                                           'Never started a trial'
      END AS funnel_bucket,

      CASE
      WHEN uph.total_billable_paid_invoices > 0 THEN 'Paid'
      WHEN us.has_trialing                      THEN 'In trial'
      WHEN us.has_active OR us.has_past_due     THEN 'Active no payment'
      WHEN us.total_subs > 0                    THEN 'Trial started, then churned'
      ELSE                                           'Never started a trial'
      END AS funnel_bucket_coarse

      FROM signups s
      LEFT JOIN user_subscriptions   us  ON us.user_id  = s.user_id
      LEFT JOIN user_payment_history uph ON uph.user_id = s.user_id
      WHERE 1 = 1
      {% if date_range._is_filtered %}
      AND {% condition date_range %} TIMESTAMP(s.signup_date) {% endcondition %}
      {% endif %}
      ;;
  }

  # ——— Filters ———

  filter: date_range {
    type: date
    description: "Filter by signup date. Use 'is in range' in the UI. Optional."
  }

  # ——— Primary Key ———

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
    primary_key: yes
  }

  # ——— Signup date ———

  dimension_group: signup {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.signup_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Signup"
    description: "Signup date, America/New_York. Use as the chart x-axis. NOTE the 7-day trial launched around April 2026, so cohorts either side of that are different funnels."
  }

  # ——— Maturity ———

  dimension: days_since_signup {
    type: number
    sql: ${TABLE}.days_since_signup ;;
    label: "Days Since Signup"
  }

  dimension: is_observable_7d {
    type: yesno
    sql: ${TABLE}.is_observable_7d ;;
    label: "Is Observable 7d"
    description: "Yes when the creator signed up at least 7 days ago and has therefore had time to convert. FILTER TO YES alongside the 7-day conversion measures — otherwise the current month includes creators mid-trial and reads artificially low. August 2026 read 8.1% against July's 17.6% for exactly this reason."
  }

  dimension: is_observable_30d {
    type: yesno
    sql: ${TABLE}.is_observable_30d ;;
    label: "Is Observable 30d"
    description: "Yes when the creator signed up at least 30 days ago. Pair with the 30-day measures. Rarely needed — conversion is within 1pp of final by day 7."
  }

  # ——— Outcome dimensions ———

  dimension: outcome_7d {
    type: string
    sql: ${TABLE}.outcome_7d ;;
    label: "7-Day Outcome"
    description: "Four mutually exclusive buckets summing exactly to Total Sign-ups: paid within 7 days, trial started but not converted, trial started after 7 days, never started a trial. Pivot on this for a stacked bar with no unexplained remainder. Unlike Funnel Bucket, which reads current status, this is fixed at the 7-day window and so is comparable across cohorts."
  }

  dimension: funnel_bucket {
    type: string
    sql: ${TABLE}.funnel_bucket ;;
    label: "Funnel Bucket (Detailed)"
    description: "Seven-way outcome by CURRENT status, evaluated in order: Paid beats In trial beats Active beats Payment failed beats Cancelled. A creator with an old cancelled trial and a current active subscription counts as Active. For cohort comparison prefer 7-Day Outcome, which is unaffected by how long ago the cohort signed up."
  }

  dimension: funnel_bucket_coarse {
    type: string
    sql: ${TABLE}.funnel_bucket_coarse ;;
    label: "Funnel Bucket (Coarse)"
    description: "Five-way version for top-level charts: Paid, In trial, Active no payment, Trial started then churned, Never started a trial."
  }

  dimension: days_to_pay_band {
    type: string
    sql: ${TABLE}.days_to_pay_band ;;
    label: "Days to Pay"
    description: "How long from signup to first payment. 'Within 7 days' is trial-end conversion. Replaces the old 'Paid in signup month' vs 'later month' split, which sorted creators by which day of the month they signed up rather than by behaviour — with a 7-day trial, anyone signing up after about the 24th fell into 'later month' automatically."
  }

  dimension: days_to_pay {
    type: number
    sql: ${TABLE}.days_to_pay ;;
    label: "Days to Pay (Number)"
    description: "Days from signup to first real payment. NULL if never paid. Median is 7 from April 2026 onward (the trial length) and 0-1 before, when there was no trial. That step is the cleanest evidence of the launch."
  }

  dimension: days_to_subscribe {
    type: number
    sql: ${TABLE}.days_to_subscribe ;;
    label: "Days to Trial Start"
  }

  dimension: ever_paid {
    type: yesno
    sql: ${TABLE}.ever_paid ;;
    label: "Ever Paid"
    description: "Ever had a real invoice paid. Biased toward older cohorts — prefer the 7-day measures for anything compared across cohorts."
  }

  dimension: ever_subscribed {
    type: yesno
    sql: ${TABLE}.ever_subscribed ;;
    label: "Ever Started a Trial"
  }

  dimension: paid_within_7d {
    type: yesno
    sql: ${TABLE}.paid_within_7d ;;
    hidden: yes
  }

  dimension: paid_within_30d {
    type: yesno
    sql: ${TABLE}.paid_within_30d ;;
    hidden: yes
  }

  dimension: subscribed_within_7d {
    type: yesno
    sql: ${TABLE}.subscribed_within_7d ;;
    hidden: yes
  }

  dimension: subscribed_within_30d {
    type: yesno
    sql: ${TABLE}.subscribed_within_30d ;;
    hidden: yes
  }

  # ——— Detail ———

  dimension: total_subscriptions {
    type: number
    sql: ${TABLE}.total_subscriptions ;;
    label: "Trials Started"
  }

  dimension: subscription_statuses {
    type: string
    sql: ${TABLE}.subscription_statuses ;;
    label: "Trial/Subscription Statuses"
    description: "All distinct statuses across this creator's trial/subscription records. 'trialing' means the trial is still running; 'active' means it converted and is billing."
  }

  dimension: all_subscription_ids {
    type: string
    sql: ${TABLE}.all_subscription_ids ;;
    label: "Subscription IDs (for Stripe)"
    description: "For cross-referencing with Stripe. Each ID is one trial/subscription record."
  }

  dimension: total_paid_invoices {
    type: number
    sql: ${TABLE}.total_paid_invoices ;;
    label: "Total Paid Invoices"
  }

  dimension: paid_subscription_ids {
    type: string
    sql: ${TABLE}.paid_subscription_ids ;;
    label: "Paid Subscription IDs"
  }

  dimension_group: first_paid {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.first_paid_date ;;
    timeframes: [date, week, month]
    label: "First Paid"
  }

  dimension_group: first_sub {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.first_sub_date ;;
    timeframes: [date, week, month]
    label: "First Trial Start"
  }

  dimension: sign_up_user_username {
    type: string
    sql: ${TABLE}.sign_up_user_username ;;
  }

  dimension: sign_up_user_email {
    type: string
    sql: ${TABLE}.sign_up_user_email ;;
    label: "Email"
  }

  dimension: sign_up_user_url {
    type: string
    sql: ${TABLE}.sign_up_user_url ;;
    link: {
      label: "Open storefront"
      url: "{{ value }}"
    }
  }

  dimension: is_trial_observable_7d {
    type: yesno
    sql: ${TABLE}.is_trial_observable_7d ;;
    label: "Is Trial Observable 7d"
    description: "Yes when the creator's FIRST TRIAL started at least 7 days ago, OR they never started one. Is Observable 7d keys on SIGNUP date and does not protect trial-denominated measures — a creator can be 20 days past signup and 1 day into a trial. Filter to Yes on any Trial Start → Paid tile. Never-trialers are Yes by design so this filter does not distort Total Sign-ups or Signup → Trial Start on the same tile."
  }

  dimension: paid_within_8d_of_trial {
    type: yesno
    sql: ${TABLE}.paid_within_8d_of_trial ;;
    hidden: yes
    # Strict subset of subscribed_within_7d by construction. 8 not 7: a trial
    # starting day D bills day D+7 and the extra day absorbs a boundary slip.
    # Quantified by pays_on_day_8 / pays_after_day_8 in the debug SQL.
  }

  dimension: days_trial_to_pay {
    type: number
    sql: ${TABLE}.days_trial_to_pay ;;
    label: "Days Trial Start → Pay"
    description: "Days from first trial start to first payment. Should cluster at 7. Distinct from Days to Pay, which counts from signup and so is inflated for creators who signed up and trialled later."
  }

  # ——— Measures ———

  measure: paid_within_8d_of_trial_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [paid_within_8d_of_trial: "yes"]
    label: "Converted on Trial Clock (Count)"
    description: "Numerator of Trial Start → Paid, 7 Days. Ship this beside the rate — rate measures drill to the denominator."
    drill_fields: [drill_details*]
  }

  measure: trial_to_paid_7d {
    type: number
    sql: SAFE_DIVIDE(
           COUNT(DISTINCT IF(${TABLE}.paid_within_8d_of_trial, ${TABLE}.user_id, NULL)),
           COUNT(DISTINCT IF(${TABLE}.subscribed_within_7d,    ${TABLE}.user_id, NULL))) ;;
    value_format_name: percent_1
    label: "Trial Start → Paid, 7 Days (%)"
    description: "Of creators who started a trial within 7 days of signup, the share who paid within 8 days OF TRIAL START. Cohort-stable — unlike Trial Start → Paid (%), which is ever/ever and restates history every week. Numerator is a strict subset of the denominator, so this cannot exceed 100%. REQUIRES Is Trial Observable 7d = Yes."
    drill_fields: [drill_details*]
  }

  measure: trial_clock_subset_check {
    type: number
    sql: ${paid_within_8d_of_trial_count} - LEAST(${paid_within_8d_of_trial_count}, ${subscribed_within_7d_count}) ;;
    label: "Trial Clock Subset Check"
    description: "Must always be 0. Non-zero means the trial-clock numerator has escaped its denominator and Trial Start → Paid, 7 Days can read above 100%."
  }

  measure: total_signups {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Total Sign-ups"
    drill_fields: [drill_details*]
  }

  measure: paid_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [ever_paid: "yes"]
    label: "Paid"
    drill_fields: [drill_details*]
  }

  measure: trialing_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [funnel_bucket: "2. Currently in trial"]
    label: "Currently Trialing"
    drill_fields: [drill_details*]
  }

  measure: active_no_payment_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [funnel_bucket: "3. Active, not yet billed"]
    label: "Active Sub, Not Yet Billed"
    drill_fields: [drill_details*]
  }

  measure: payment_failed_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [funnel_bucket: "4. Payment failed"]
    label: "Payment Failed"
    drill_fields: [drill_details*]
  }

  measure: cancelled_without_paying_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [funnel_bucket: "5. Trial cancelled without paying"]
    label: "Cancelled Without Paying"
    drill_fields: [drill_details*]
  }

  measure: never_subscribed_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    # Boolean rather than filters: [funnel_bucket: "7. Never started a trial"].
    # A string filter has to match the CASE branch in the derived table
    # character for character, so renaming a bucket label silently returns 0 —
    # which is exactly what happened, and what Signup Accounting Check caught.
    # ever_subscribed = no is the same population and cannot drift.
    filters: [ever_subscribed: "no"]
    label: "Never Started a Trial"
    description: "Signed up and never created a subscription record at all. The largest group by far — around 91% of March 2026 signups. Drill here for creators who never entered the funnel."
    drill_fields: [drill_details*]
  }

  # ——— Drillable numerators ———
  # The rate measures below are type: number, built from other aggregates, so
  # Looker may not offer a drill link on them. These counts are plain
  # count_distinct and always drill cleanly — put one beside each rate on the
  # tile and people click the count rather than the percentage.

  measure: paid_within_7d_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [paid_within_7d: "yes"]
    label: "Paid Within 7 Days (Count)"
    description: "Numerator of Signup → Paid, 7 Days. Drill here for the creators who converted."
    drill_fields: [drill_details*]
  }

  measure: subscribed_within_7d_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [subscribed_within_7d: "yes"]
    label: "Trial Starts Within 7 Days (Count)"
    description: "Creators who started a trial within 7 days. Numerator of Signup → Trial Start, 7 Days. Includes those who went on to pay — drill here for everyone who entered the funnel."
    drill_fields: [drill_details*]
  }

  measure: subscribed_not_paid_7d_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [subscribed_within_7d: "yes", paid_within_7d: "no"]
    label: "Trial Started, Not Converted (Count)"
    description: "Started a trial within 7 days and did NOT convert to paid within 7 days. The gap between the two lines on the chart — drill here to see who the trial attracts that does not convert. The week of 2026-06-22 is where these concentrate: 1,207 signups, 78% starting a trial, 5.2% paying."
    drill_fields: [drill_details*]
  }

  # ——— What happened to everyone else ———
  # Total Sign-ups splits into exactly four groups with no remainder:
  #   Paid Within 7 Days
  # + Trial Started, Not Converted
  # + Trial Started After 7 Days
  # + Never Started a Trial
  # = Total Sign-ups
  # Signup Accounting Check verifies it and must always be 0.

  measure: not_subscribed_within_7d_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [subscribed_within_7d: "no"]
    label: "No Trial Start in 7 Days (Count)"
    description: "Total Sign-ups minus Trial Starts Within 7 Days — the creators the two conversion columns do not account for. July 2026: 1,960 - 792 = 1,168. Splits into Trial Started After 7 Days and Never Started a Trial."
    drill_fields: [drill_details*]
  }

  measure: subscribed_after_7d_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [ever_subscribed: "yes", subscribed_within_7d: "no"]
    label: "Trial Started After 7 Days (Count)"
    description: "Came back and started a trial later than 7 days after signing up. Real entries into the funnel that the 7-day window does not credit — check Days to Trial Start in the drill to see how much later."
    drill_fields: [drill_details*]
  }

  measure: signup_accounting_check {
    type: number
    sql: ${total_signups}
         - ${paid_within_7d_count}
         - ${subscribed_not_paid_7d_count}
         - ${subscribed_after_7d_count}
         - ${never_subscribed_count} ;;
    label: "Signup Accounting Check"
    description: "Must always be 0. Verifies the four 7-Day Outcome buckets sum to Total Sign-ups: Paid + Trial Started Not Converted + Trial Started After 7 Days + Never Started a Trial. A non-zero value means a creator falls into none of them, or into two."
  }

  # ——— Rates ———
  # The 7-day pair is the headline. Filter Is Observable 7d = Yes on any tile
  # using them, or the newest cohort includes creators still mid-trial.

  measure: signup_to_paid_7d {
    type: number
    sql: SAFE_DIVIDE(
           COUNT(DISTINCT IF(${TABLE}.paid_within_7d, ${TABLE}.user_id, NULL)),
           COUNT(DISTINCT ${TABLE}.user_id)) ;;
    value_format_name: percent_1
    label: "Signup → Paid, 7 Days (%)"
    description: "Share of sign-ups who paid within 7 days. Comparable across cohorts, unlike paid-ever. Conversion is within 1pp of final by day 7 (May 14.9% at 7d vs 16.3% ever), so this is the right headline. REQUIRES Is Observable 7d = Yes."
    drill_fields: [drill_details*]
  }

  measure: signup_to_subscribe_7d {
    type: number
    sql: SAFE_DIVIDE(
           COUNT(DISTINCT IF(${TABLE}.subscribed_within_7d, ${TABLE}.user_id, NULL)),
           COUNT(DISTINCT ${TABLE}.user_id)) ;;
    value_format_name: percent_1
    label: "Signup → Trial Start, 7 Days (%)"
    description: "Share of sign-ups who STARTED A TRIAL within 7 days. No money moves at this point — the trial is free. Paid creators are a SUBSET of these, since you cannot pay without first starting a subscription. NOTE: before April 2026 there was no trial, so for Jan-Mar cohorts this means 'subscribed and paid immediately' — February shows 47 trial starts and 47 paid, zero gap. REQUIRES Is Observable 7d = Yes."
    drill_fields: [drill_details*]
  }

  measure: signup_to_paid_30d {
    type: number
    sql: SAFE_DIVIDE(
           COUNT(DISTINCT IF(${TABLE}.paid_within_30d, ${TABLE}.user_id, NULL)),
           COUNT(DISTINCT ${TABLE}.user_id)) ;;
    value_format_name: percent_1
    label: "Signup → Paid, 30 Days (%)"
    description: "Pair with Is Observable 30d = Yes. Rarely worth the extra month of lag — it runs about 1pp above the 7-day figure."
    drill_fields: [drill_details*]
  }

  measure: signup_to_paid_ever {
    type: number
    sql: SAFE_DIVIDE(
           COUNT(DISTINCT IF(${TABLE}.ever_paid, ${TABLE}.user_id, NULL)),
           COUNT(DISTINCT ${TABLE}.user_id)) ;;
    value_format_name: percent_1
    label: "Signup → Paid, Ever (%)"
    description: "What the old view reported. Kept for continuity, but NOT comparable across cohorts — an older cohort has had longer to convert. Use the 7-day version for trends."
    drill_fields: [drill_details*]
  }

  measure: signup_to_subscribe_ever {
    type: number
    sql: SAFE_DIVIDE(
           COUNT(DISTINCT IF(${TABLE}.ever_subscribed, ${TABLE}.user_id, NULL)),
           COUNT(DISTINCT ${TABLE}.user_id)) ;;
    value_format_name: percent_1
    label: "Signup → Trial Start, Ever (%)"
    description: "Not comparable across cohorts — an older cohort has had longer. See Signup → Trial Start, 7 Days."
    drill_fields: [drill_details*]
  }

  measure: subscribe_to_paid_rate {
    type: number
    sql: SAFE_DIVIDE(
           COUNT(DISTINCT IF(${TABLE}.ever_paid, ${TABLE}.user_id, NULL)),
           COUNT(DISTINCT IF(${TABLE}.ever_subscribed, ${TABLE}.user_id, NULL))) ;;
    value_format_name: percent_1
    label: "Trial Start → Paid (%)"
    description: "Of creators who started a trial, the share who converted to paid. THE key funnel metric. Where the April 2026 trial launch shows up most clearly: 73% for Jan-Feb (when there was no trial and starting a subscription meant paying), then 37-44% from April, and 23% for June. A free trial raises trial starts and lowers the conversion rate — both are expected."
    drill_fields: [drill_details*]
  }

  measure: median_days_to_pay {
    type: median
    sql: ${TABLE}.days_to_pay ;;
    label: "Median Days to Pay"
    description: "0-1 before April 2026, 7 after. That step is the trial launch and is the cleanest single indicator of it."
  }

  # ——— Drill Set ———

  set: drill_details {
    fields: [
      user_id,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      signup_date,
      days_since_signup,
      outcome_7d,
      funnel_bucket,
      days_to_pay_band,
      days_to_pay,
      days_to_subscribe,
      total_subscriptions,
      subscription_statuses,
      all_subscription_ids,
      total_paid_invoices,
      first_paid_date,
      first_sub_date,
      paid_subscription_ids
    ]
  }
}

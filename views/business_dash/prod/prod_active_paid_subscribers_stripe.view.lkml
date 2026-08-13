# =============================================================================
# prod_active_paid_subscribers_stripe
# -----------------------------------------------------------------------------
# Point-in-time count of MRR-active subscriptions, by day.
#
# GRAIN: one row per (report_date, subscription_id).
#
# -----------------------------------------------------------------------------
# WHAT WAS WRONG BEFORE
# -----------------------------------------------------------------------------
# 1. SURVIVORSHIP. daily_active filtered `WHERE sp.status IN ('active',
#    'past_due')`, which is CURRENT status. Every subscription that has since
#    churned was therefore absent from EVERY historical date, so the curve could
#    only rise and was correct only at today's edge. The end_date CASE for
#    canceled/unpaid statuses was dead code — those rows never survived the
#    filter. Measured against Stripe:
#
#      date        old view   this view   Stripe
#      2026-02-28       129         221      229
#      2026-03-31       168         246      253
#      2026-04-30       232         374      410
#      2026-05-31       368         720      759
#      2026-06-30       544         853      876
#      2026-07-31       917         958      981
#
#    Growth Feb->Jul read 7.1x on the old view against Stripe's 4.3x — a 65%
#    overstatement, entirely from survivorship.
#
# 2. NO MRR-ACTIVE GATE. Never-billed trials and non-seller accounts were
#    included. Removing the status filter alone made things WORSE, not better:
#    it produced 1,644 at Jun 30 falling to 1,140 at Jul 31, which is impossible
#    for a point-in-time count. Adding the `amount_due > 0` invoice gate fixed
#    the shape.
#
# 3. UNRELIABLE START AND END DATES.
#      start was DATE(created_at) — the TRIAL start, so a converting
#        subscription was counted days before it ever paid.
#      end was COALESCE(cancellation_applied_at, current_period_end,
#        created_at). cancellation_applied_at is when the subscriber CLICKED
#        cancel, not when access lapsed.
#    Now both come from prod_subscription_churn: first billable invoice for the
#    start, effective_end_date for the end.
#
#    Worth noting the end dates were closer than expected — for the 239
#    subscriptions where the two definitions disagreed, the old end date averaged
#    only 4.8 days EARLIER than the validated one. Items 1 and 2 did nearly all
#    the damage.
#
# -----------------------------------------------------------------------------
# WHY A POINT-IN-TIME SERIES IS RECONSTRUCTABLE AT ALL
# -----------------------------------------------------------------------------
# fact_seller_subscription is UPSERT-per-subscription-id: no history, only
# current state. But invoices ARE historical, so first_mrr_date is a real
# record, and effective_end_date is the expression reconciled to +-5/month
# against Stripe churn. Each subscription has exactly one span, and chain
# replacement mints new subscription_ids rather than reusing them, so "how many
# spans cover date D" is answerable.
#
# WHAT IS NOT RECONSTRUCTABLE, and must not be built on this view:
#   * MRR-weighted history. Only today's price exists, so an upgrade or a
#     discount applied six months ago is invisible. Any revenue series must come
#     from invoice history instead.
#   * Historical past_due episodes that later recovered. Only the current status
#     survives, so a subscription that went past_due in March and recovered
#     looks continuously active.
#
# -----------------------------------------------------------------------------
# STRIPE'S DEFINITION, as established empirically
# -----------------------------------------------------------------------------
#   status IN ('active','past_due')  AND  MRR > 0  AND  NO email filter
# Trials excluded (zero MRR in Stripe's model), fully-discounted excluded,
# internal accounts INCLUDED. Verified 2026-08-12: 985 non-internal with MRR
# plus 24 internal with MRR = 1,009 against Stripe's 1,010.
#
# This view's `amount_due > 0` gate excludes never-billed trials and
# always-comped subscriptions, matching Stripe. The remaining difference is the
# email filter: set Exclude Internal Accounts = No for a Stripe-comparable
# series. With it on, expect a small systematic undercount (-8 to -39 per month,
# roughly 2-9%).
#
# CAUTION: the MRR gate is "ever billed more than zero", not "currently billing
# more than zero". A subscription that paid full price and was later comped still
# qualifies — which is right for a historical series, and matches
# prod_subscription_churn.
#
# -----------------------------------------------------------------------------
# INHERITED OPEN ITEMS, kept so this view agrees with prod_subscription_churn
# -----------------------------------------------------------------------------
#   * The dunning lookback is unbounded, so a stale void from an earlier billing
#     period can pull an end date early.
#   * current_period_end is overwritten to the cancellation instant on some
#     cancellations, mis-dating the cancel_at_period_end branch.
# Fix in both views or neither.
#
# PERFORMANCE: a daily spine from 2025-01-01 crossed with ~1,000 concurrent
# spans is several hundred thousand rows. Add a datagroup or persist_for if the
# tile is slow; switch the spine to weekly if a daily grain is not needed.
# =============================================================================

view: prod_active_paid_subscribers_stripe {
  derived_table: {
    sql:
    WITH date_spine AS (
      SELECT d AS report_date
      FROM UNNEST(GENERATE_DATE_ARRAY('2025-01-01', CURRENT_DATE('America/New_York'))) AS d
    ),

      -- ══════════════════════════════════════════════════════════════════
      -- VALIDATED END DATE -- identical to prod_subscription_churn.
      -- MIN(SAFE_CAST(...)) not ANY_VALUE(...): ANY_VALUE is nondeterministic
      -- and returns NULL when it lands on a row with no cancelledAt.
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

      -- MRR-active gate AND the span start. amount_due > 0 excludes $0 trial
      -- invoices and always-comped subscriptions, which is what Stripe does.
      subscription_mrr AS (
      SELECT
      subscription_id,
      MIN(DATE(COALESCE(created, created_at), 'America/New_York')) AS first_mrr_date
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      AND amount_due > 0
      GROUP BY subscription_id
      ),

      -- ══════════════════════════════════════════════════════════════════
      -- ONE SPAN PER SUBSCRIPTION. No status filter here — that was the
      -- survivorship bug. Liveness on any given date comes from the span.
      -- ══════════════════════════════════════════════════════════════════
      subscription_spans AS (
      SELECT
      t1.subscription_id,
      t1.user_id,
      t1.status,
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
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1
      INNER JOIN subscription_mrr sm ON sm.subscription_id = t1.subscription_id
      LEFT JOIN flip_resolved f     ON f.subscription_id = t1.subscription_id
      LEFT JOIN dunning d           ON d.subscription_id = t1.subscription_id
      WHERE t1.is_deleted = FALSE
      AND EXISTS (SELECT 1 FROM UNNEST(t1.plans) AS pl
      WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') = 'plan')
      ),

      daily_active AS (
      SELECT
      ds.report_date,
      sp.subscription_id
      FROM date_spine ds
      JOIN subscription_spans sp
      ON ds.report_date >= sp.start_date
      AND (sp.end_date IS NULL OR ds.report_date < sp.end_date)
      ),

      onboarding_events AS (
      SELECT
      context_campaign_campaign        AS marketing_campaign,
      context_campaign_onboarding_path AS onboarding_path,
      context_campaign_planlevel       AS plan_level,
      context_user_agent               AS user_agent,
      utm_regintent,
      business_type,
      `timestamp`,
      user_id,
      scene,
      step_name,
      onboarding_session_id,
      CASE
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(bot|crawler|spider|crawl|slurp|googlebot|bingpreview|facebookexternalhit|twitterbot|linkedinbot|discordbot|telegrambot|google-read-aloud)') THEN 'BOT'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(wv|webview|meta-iab|metaiab|facebook|fban|fbav|instagram|iabmv/1|whatsapp|line|linkedinapp|snapchat|gsa/|googleapp/|youtube|tiktok|reddit)') THEN 'WEBVIEW'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(iphone|ipad|ipod|cpu iphone os|cpu os)') THEN 'IOS'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'android') THEN 'ANDROID'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(windows nt|win64|wow64)') THEN 'WINDOWS_DESKTOP'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(macintosh|mac os x)') AND NOT REGEXP_CONTAINS(LOWER(context_user_agent), r'(iphone|ipad)') THEN 'MACOS_DESKTOP'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(linux|x11)') AND NOT REGEXP_CONTAINS(LOWER(context_user_agent), r'android') THEN 'LINUX_DESKTOP'
      ELSE 'OTHER'
      END AS device_category
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      WHERE (scene = 'onboarding' OR scene IS NULL)
      AND (step_name = 'onboarding_complete' OR step_name IS NULL)
      ),

      -- Prefer rows where marketing/intent/business_type are actually
      -- populated, then most recent, so a 'generic' row is not picked
      -- arbitrarily.
      onboarding_events_dedup AS (
      SELECT
      user_id,
      marketing_campaign,
      onboarding_path,
      plan_level,
      utm_regintent,
      business_type,
      `timestamp`,
      onboarding_session_id,
      device_category,
      user_agent
      FROM onboarding_events
      QUALIFY ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY
      CASE
      WHEN marketing_campaign IS NOT NULL
      OR (utm_regintent IS NOT NULL AND utm_regintent != 'generic')
      OR (business_type  IS NOT NULL AND business_type  != 'generic')
      THEN 0 ELSE 1
      END,
      `timestamp` DESC
      ) = 1
      ),

      marketing_capture AS (
      SELECT
      user_id,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_campaign')        AS utm_campaign,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_source')          AS utm_source,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent')       AS utm_regintent,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_onboarding_path') AS onboarding_path,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_planlevel')       AS plan_level,
      JSON_VALUE(private_profile, '$.email')                                         AS profile_email,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.firstName')                AS first_name,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.lastName')                 AS last_name
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      )

      SELECT
      da.report_date,
      da.subscription_id,
      fs.user_id,
      fs.status AS subscription_status,

      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email   AS sign_up_user_email,
      mc.profile_email,
      mc.first_name,
      mc.last_name,
      COALESCE(oe.marketing_campaign, mc.utm_campaign)                            AS marketing_campaign,
      COALESCE(oe.utm_regintent,      mc.utm_regintent)                           AS utm_regintent,
      COALESCE(oe.business_type,      JSON_VALUE(prof.profile, '$.businessType')) AS business_type,

      -- CURRENT price, not the price on report_date. fact_seller_subscription
      -- is upsert-only, so historical pricing does not exist. Do not build a
      -- revenue series from this.
      COALESCE(fs.discounted_price, fs.price + fs.tax_amount) AS price,
      COALESCE(fs.discounted_price, fs.price + fs.tax_amount) = 0 AS is_currently_zero_mrr,

      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval')    AS plan_interval,
      fs.initial_start_date AS trial_starts,
      fs.trial_end          AS trial_ends,

      COALESCE(
      CASE
      WHEN fs.cancellation_applied_at IS NOT NULL
      AND fs.cancellation_applied_at < fs.trial_end
      THEN fs.cancellation_applied_at
      END,
      fs.trial_end
      ) AS effective_trial_end,

      CASE
      WHEN COALESCE(oe.marketing_campaign, mc.utm_campaign) IS NOT NULL THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source

      FROM daily_active da

      JOIN `popshoplive-26f81.dbt_popshop.fact_seller_subscription` fs
      ON fs.subscription_id = da.subscription_id
      AND fs.is_deleted = FALSE

      CROSS JOIN UNNEST(fs.plans) AS plan

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = fs.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = fs.user_id
      LEFT JOIN marketing_capture mc
      ON mc.user_id = fs.user_id
      LEFT JOIN onboarding_events_dedup oe
      ON oe.user_id = fs.user_id

      WHERE
      JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND prof.apps_pop_store = TRUE
      AND prof.user_type IN ('seller', 'verifiedSeller')
      {% if exclude_internal_accounts._parameter_value == 'yes' %}
      AND (pprof.email IS NULL OR NOT REGEXP_CONTAINS(LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'))
      {% endif %}
      {% if date_range._is_filtered %}
      AND {% condition date_range %} TIMESTAMP(da.report_date) {% endcondition %}
      {% endif %}
      ;;
  }

  # ——— Parameters ———

  parameter: exclude_internal_accounts {
    type: unquoted
    label: "Exclude Internal Accounts"
    default_value: "no"
    description: "Yes = internal view. No = Stripe-comparable; Stripe applies no email filter and counts internal accounts, so leaving this on produces a systematic undercount of roughly 2-9% per month (-8 to -39 measured Feb-Jul 2026)."
    allowed_value: { label: "Yes (internal view)"    value: "yes" }
    allowed_value: { label: "No (match Stripe)"      value: "no"  }
  }

  # ——— Filters ———

  filter: date_range {
    type: date
    description: "Filter by report date. Use 'is in range' in the UI. Optional."
  }

  # ——— Dimensions ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(CAST(${TABLE}.report_date AS STRING), '-', ${TABLE}.subscription_id) ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension_group: report {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.report_date ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
    label: "Current Status"
    description: "Status TODAY, not on the report date — fact_seller_subscription keeps no history. A subscription active on an old report date may show 'canceled' here. Never filter a historical series on this: doing so is the survivorship bug this view was rebuilt to remove."
  }

  dimension: is_currently_zero_mrr {
    type: yesno
    sql: ${TABLE}.is_currently_zero_mrr ;;
    label: "Is Currently Zero MRR"
    description: "Fully discounted at CURRENT prices. Informational only — the amount_due > 0 invoice gate already excludes always-comped subscriptions. A subscription that paid full price and was later comped is correctly still counted."
  }

  dimension: sign_up_user_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.sign_up_url_code ;;
  }

  dimension: sign_up_user_username {
    type: string
    sql: ${TABLE}.sign_up_user_username ;;
  }

  dimension: sign_up_user_email {
    type: string
    sql: ${TABLE}.sign_up_user_email ;;
  }

  dimension: marketing_campaign {
    type: string
    sql: ${TABLE}.marketing_campaign ;;
  }

  dimension: utm_regintent {
    type: string
    sql: ${TABLE}.utm_regintent ;;
  }

  dimension: business_type {
    type: string
    sql: ${TABLE}.business_type ;;
  }

  dimension: acquisition_source {
    type: string
    sql: ${TABLE}.acquisition_source ;;
  }

  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
    value_format_name: decimal_2
    label: "Price (Current)"
    description: "CURRENT price, not the price on the report date. Historical pricing does not exist in this table — do not build a revenue series from this field."
  }

  dimension: plan_name {
    type: string
    sql: ${TABLE}.plan_name ;;
  }

  dimension: plan_interval {
    type: string
    sql: ${TABLE}.plan_interval ;;
    label: "Interval"
  }

  dimension_group: trial_starts_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.trial_starts ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension_group: trial_ends_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.trial_ends ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension_group: effective_trial_ends_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.effective_trial_end ;;
    timeframes: [date, week, month, quarter, year]
  }

  # ——— Measures ———

  measure: active_subscribers {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    label: "Active Paid Subscribers"
    description: "Distinct MRR-active subscriptions on the report date. Validated against Stripe: 221/246/374/720/853/958 vs Stripe's 229/253/410/759/876/981 for month-ends Feb-Jul 2026, with the internal filter ON. Set Exclude Internal Accounts = No to close that gap."
    drill_fields: [drilldown_details*]
  }

  measure: active_creators {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Active Paid Creators"
    description: "Distinct creators. Below the subscription count where a creator holds two concurrent subscriptions."
    drill_fields: [drilldown_details*]
  }

  # ——— Drill Set ———

  set: drilldown_details {
    fields: [
      user_id,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      subscription_id,
      subscription_status,
      plan_name,
      plan_interval,
      price,
      is_currently_zero_mrr,
      trial_starts_at_date,
      trial_ends_at_date,
      effective_trial_ends_at_date,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type
    ]
  }
}

# =============================================================================
# prod_active_paid_subscribers
# -----------------------------------------------------------------------------
# Point-in-time count of live subscriptions by day, segmented so a stacked bar
# can be toggled to reach every published definition.
#
# GRAIN: one row per (report_date, subscription_id).
#
# -----------------------------------------------------------------------------
# THE FOUR SEGMENTS — mutually exclusive, so they stack correctly
# -----------------------------------------------------------------------------
#   Paying subscribers      external, has been billed more than $0    985
#   100% discounted         external, never billed anything            52
#   Internal - paying       internal, has been billed                  24
#   Internal - discounted   internal, never billed                     18
#                                                          TOTAL     1,079
# (2026-08-12)
#
# FOUR SEGMENTS, NOT THREE. Some subscriptions are BOTH internal AND fully
# discounted — 18 of them. A three-way split (paying / discounted / internal)
# would either double-count or drop those, and Stripe's figure would become
# unreachable: 985 + all 42 internal = 1,027, not 1,011.
#
# TOGGLING IS ADDITIVE. Switching a legend entry off removes that segment; it
# never subtracts. Every published number is a sum of included segments:
#
#   Paying                                        =   985  actual revenue-contributing
#   Paying + Internal-paying                      = 1,009  ~= Stripe's 1,011
#   Paying + Discounted                           = 1,037  = the old view
#   all four                                      = 1,079  everyone on a plan
#
# The 2-subscription gap to Stripe's 1,011 is snapshot timing between two reads
# minutes apart, not a definitional difference.
#
# -----------------------------------------------------------------------------
# HOW "DISCOUNTED" IS DETERMINED — and why not by price
# -----------------------------------------------------------------------------
# A subscription counts as discounted when it has NEVER had an invoice with
# amount_due > 0. That is invoice history, so it is stable over time.
#
# The alternative — current price = 0 — is CURRENT state, so a subscription
# comped last week would be recategorised as discounted across its whole
# history, including months when it genuinely paid. The two agree closely today
# (52 by invoice, 53 by price) but only the invoice test is safe historically.
#
# Consequence worth knowing: a subscription that paid full price and was LATER
# comped stays in Paying. It did generate revenue. Use Is Currently Zero MRR to
# find those.
#
# -----------------------------------------------------------------------------
# WHY THE GATE IS TWO-TIER
# -----------------------------------------------------------------------------
# `amount_due > 0` does three jobs at once: it excludes comped subscriptions, it
# excludes never-converted trials, and it supplies the span start. Dropping it to
# admit comped subscriptions would also admit every trial — that is what
# produced the impossible 1,644-at-Jun-30 figure during testing.
#
# So the span start is LEAST of two tiers:
#   tier 1  first invoice with amount_due > 0
#   tier 2  first $0 PAID invoice at or after COALESCE(trial_end,
#           initial_start_date) — a post-trial billing cycle on a 100%
#           discount, not the trial invoice itself
# LEAST rather than COALESCE: a subscription comped for three months and then
# billed was a live subscriber from month one.
#
# !! VERIFY BEFORE SHIPPING: no subscription with status 'trialing' should reach
# tier 2. Run Output 3 of test_include_comped_subscriptions.sql — it must return
# zero rows. If trials leak through, every segment total is inflated.
#
# -----------------------------------------------------------------------------
# THE HISTORICAL FIX THAT MATTERS MORE THAN ANY OF THIS
# -----------------------------------------------------------------------------
# The original view filtered daily_active on CURRENT status, so every
# subscription since churned was absent from EVERY historical date. The curve
# could only rise.
#
#   date        old view   Paying+Internal-paying   Stripe
#   2026-02-28       129                      229      229
#   2026-03-31       168                      252      253
#   2026-04-30       232                      403      410
#   2026-05-31       368                      755      759
#   2026-06-30       544                      875      876
#   2026-07-31       917                      981      981
#
# Growth Feb->Jul read 7.1x on the old view against Stripe's 4.4x. A 3%
# endpoint difference is trivial beside a growth multiple wrong by 60%.
#
# Start and end dates also now come from prod_subscription_churn: first billable
# invoice rather than DATE(created_at) (which was the TRIAL start), and
# effective_end_date rather than COALESCE(cancellation_applied_at,
# current_period_end, created_at) (cancellation_applied_at is when the
# subscriber CLICKED cancel, not when access lapsed).
#
# -----------------------------------------------------------------------------
# WHAT THIS VIEW CANNOT DO
# -----------------------------------------------------------------------------
# fact_seller_subscription is UPSERT per subscription_id — current state only.
# Invoices are historical, which is what makes a point-in-time COUNT possible.
# But there is no historical price, so:
#   * no MRR-weighted series. An upgrade or discount from six months ago is
#     invisible. Revenue trends must come from invoice history.
#   * no historical past_due episodes. A subscription that went past_due in
#     March and recovered looks continuously active.
#
# -----------------------------------------------------------------------------
# INHERITED OPEN ITEMS, kept so this agrees with prod_subscription_churn
# -----------------------------------------------------------------------------
#   * The dunning lookback is unbounded, so a stale void from an earlier billing
#     period can pull an end date early.
#   * current_period_end is overwritten to the cancellation instant on some
#     cancellations, mis-dating the cancel_at_period_end branch.
#
# PERFORMANCE: a daily spine from 2025-01-01 crossed with ~1,000 concurrent
# spans is several hundred thousand rows. Add a datagroup or persist_for if the
# tile is slow; switch the spine to weekly if a daily grain is not needed.
# =============================================================================

view: prod_active_paid_subscribers {
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

      -- ══════════════════════════════════════════════════════════════════
      -- TWO-TIER SPAN START. See "WHY THE GATE IS TWO-TIER" in the header.
      -- ══════════════════════════════════════════════════════════════════
      sub_floor AS (
      SELECT
      subscription_id,
      DATE(COALESCE(CAST(trial_end AS TIMESTAMP),
      CAST(initial_start_date AS TIMESTAMP)), 'America/New_York') AS floor_date
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription`
      WHERE is_deleted = FALSE
      ),

      -- TIER 1: real money was asked for.
      billed_start AS (
      SELECT
      subscription_id,
      MIN(DATE(COALESCE(created, created_at), 'America/New_York')) AS start_date
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      AND amount_due > 0
      GROUP BY subscription_id
      ),

      -- TIER 2: a $0 invoice at or after the payment floor is a post-trial
      -- billing cycle on a 100% discount, not the trial invoice.
      comped_start AS (
      SELECT
      i.subscription_id,
      MIN(DATE(COALESCE(i.created, i.created_at), 'America/New_York')) AS start_date
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice` i
      JOIN sub_floor sf ON sf.subscription_id = i.subscription_id
      WHERE i.is_deleted = FALSE
      AND i.status = 'paid'
      AND i.amount_due = 0
      AND DATE(COALESCE(i.created, i.created_at), 'America/New_York') >= sf.floor_date
      GROUP BY i.subscription_id
      ),

      span_start AS (
      SELECT
      COALESCE(b.subscription_id, c.subscription_id) AS subscription_id,
      LEAST(COALESCE(b.start_date, DATE '9999-12-31'),
      COALESCE(c.start_date, DATE '9999-12-31')) AS start_date,
      b.subscription_id IS NOT NULL AS has_ever_billed
      FROM billed_start b
      FULL OUTER JOIN comped_start c ON c.subscription_id = b.subscription_id
      ),

      -- ══════════════════════════════════════════════════════════════════
      -- ONE SPAN PER SUBSCRIPTION. No status filter — that was the
      -- survivorship bug. Liveness on a date comes from the span.
      -- ══════════════════════════════════════════════════════════════════
      subscription_spans AS (
      SELECT
      t1.subscription_id,
      t1.user_id,
      t1.status,
      ss.start_date,
      ss.has_ever_billed,
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
      INNER JOIN span_start ss      ON ss.subscription_id = t1.subscription_id
      LEFT JOIN flip_resolved f     ON f.subscription_id = t1.subscription_id
      LEFT JOIN dunning d           ON d.subscription_id = t1.subscription_id
      WHERE t1.is_deleted = FALSE
      AND ss.start_date < DATE '9999-12-31'
      AND EXISTS (SELECT 1 FROM UNNEST(t1.plans) AS pl
      WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') = 'plan')
      ),

      daily_active AS (
      SELECT
      ds.report_date,
      sp.subscription_id,
      sp.has_ever_billed
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

      onboarding_events_dedup AS (
      SELECT
      user_id, marketing_campaign, onboarding_path, plan_level,
      utm_regintent, business_type, device_category, user_agent
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
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_campaign')  AS utm_campaign,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_source')    AS utm_source,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent') AS utm_regintent,
      JSON_VALUE(private_profile, '$.email')                                   AS profile_email,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.firstName')          AS first_name,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.lastName')           AS last_name
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      )

      SELECT
      da.report_date,
      da.subscription_id,
      da.has_ever_billed,
      fs.user_id,
      fs.status AS subscription_status,

      -- Internal is a SEGMENT here, never a filter — you cannot both remove
      -- internal accounts and display them as a toggleable series.
      COALESCE(REGEXP_CONTAINS(LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'), FALSE) AS is_internal,

      CASE
      WHEN NOT COALESCE(REGEXP_CONTAINS(LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'), FALSE)
      AND da.has_ever_billed          THEN 'Paying subscribers'
      WHEN NOT COALESCE(REGEXP_CONTAINS(LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'), FALSE)
      THEN '100% discounted'
      WHEN da.has_ever_billed              THEN 'Internal - paying'
      ELSE                                      'Internal - discounted'
      END AS subscriber_category,

      CASE
      WHEN NOT COALESCE(REGEXP_CONTAINS(LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'), FALSE)
      AND da.has_ever_billed          THEN 1
      WHEN NOT COALESCE(REGEXP_CONTAINS(LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'), FALSE)
      THEN 2
      WHEN da.has_ever_billed              THEN 3
      ELSE                                      4
      END AS subscriber_category_sort,

      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email   AS sign_up_user_email,
      mc.first_name,
      mc.last_name,
      COALESCE(oe.marketing_campaign, mc.utm_campaign)                            AS marketing_campaign,
      COALESCE(oe.utm_regintent,      mc.utm_regintent)                           AS utm_regintent,
      COALESCE(oe.business_type,      JSON_VALUE(prof.profile, '$.businessType')) AS business_type,

      -- CURRENT price, not the price on report_date.
      COALESCE(fs.discounted_price, fs.price + fs.tax_amount)     AS price,
      COALESCE(fs.discounted_price, fs.price + fs.tax_amount) = 0 AS is_currently_zero_mrr,

      -- ADDED, nothing above changed. fs.price is plan + add-ons, so the
      -- plan's own list price is read from the plan entry instead. 7 live
      -- subscriptions carry an add-on today, and add-ons recur.
      -- is_currently_zero_mrr is DELIBERATELY untouched — it is informational
      -- here (the four segments run off has_ever_billed, which is invoice-
      -- based) and changing its basis risks the Stripe-validated segmentation.
      SAFE_CAST(JSON_EXTRACT_SCALAR(plan, '$.amount') AS NUMERIC) AS plan_price,
      (SELECT COALESCE(SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(pl, '$.amount') AS NUMERIC)), 0)
      FROM UNNEST(fs.plans) AS pl
      WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') NOT IN ('plan', 'taxProduct')) AS addon_amount,

      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval')    AS plan_interval,
      fs.initial_start_date AS trial_starts,
      fs.trial_end          AS trial_ends,

      CASE
      WHEN COALESCE(oe.marketing_campaign, mc.utm_campaign) IS NOT NULL THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source,

      -- ══════════════════════════════════════════════════════════════
      -- PERIOD-END SNAPSHOT FLAGS
      -- A point-in-time count CANNOT be aggregated by counting distinct
      -- over a period: group seven days together and a subscription
      -- active on ANY of them counts once, so the weekly bar reads
      -- "active at some point this week" and inflates. Measured: the week
      -- of Jan 5 read 233 against ~180 on any single day.
      --
      -- Filter to one of these and the period contains exactly ONE
      -- report_date, so the count is a true snapshot at any grain.
      -- CURRENT_DATE is included so the in-progress period has a point.
      -- ══════════════════════════════════════════════════════════════
      (da.report_date = LAST_DAY(da.report_date)
      OR da.report_date = CURRENT_DATE('America/New_York')) AS is_month_end,

      -- WEEK(MONDAY) assumes the model's week_start_day is monday, which is
      -- Looker's default. If the model overrides it, change this to match or
      -- the flagged day will fall in the wrong Looker week.
      (da.report_date = DATE_ADD(DATE_TRUNC(da.report_date, WEEK(MONDAY)), INTERVAL 6 DAY)
      OR da.report_date = CURRENT_DATE('America/New_York')) AS is_week_end

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
      {% if date_range._is_filtered %}
      AND {% condition date_range %} TIMESTAMP(da.report_date) {% endcondition %}
      {% endif %}
      ;;
  }

  # ——— Filters ———

  filter: date_range {
    type: date
    description: "Filter by report date. Use 'is in range' in the UI. Optional."
  }

  # ——— Primary Key ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(CAST(${TABLE}.report_date AS STRING), '-', ${TABLE}.subscription_id) ;;
  }

  # ——— The segment dimension ———

  dimension: subscriber_category {
    type: string
    sql: ${TABLE}.subscriber_category ;;
    order_by_field: subscriber_category_sort
    label: "Subscriber Category"
    description: "Four mutually exclusive segments — pivot on this for the stacked bar. Paying subscribers = external, has been billed. 100% discounted = external, never billed anything. Internal - paying / Internal - discounted = internal accounts split the same way. Four rather than three because 18 subscriptions are both internal AND discounted; a three-way split would double-count them and make Stripe's figure unreachable."
  }

  dimension: subscriber_category_sort {
    type: number
    sql: ${TABLE}.subscriber_category_sort ;;
    hidden: yes
  }

  dimension: is_internal {
    type: yesno
    sql: ${TABLE}.is_internal ;;
    label: "Is Internal Account"
    description: "Internal or test email domain. Stripe applies no email filter and counts these, so they are a segment here rather than a filter."
  }

  dimension: has_ever_billed {
    type: yesno
    sql: ${TABLE}.has_ever_billed ;;
    label: "Has Ever Billed"
    description: "At least one invoice with amount_due > 0. No means a 100% discount. Uses invoice HISTORY, not current price, so a subscription comped last week is not retroactively recategorised across months when it genuinely paid."
  }

  # ——— Time ———

  dimension_group: report {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.report_date ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension: is_month_end {
    type: yesno
    sql: ${TABLE}.is_month_end ;;
    label: "Is Month End"
    description: "Yes on the last day of each month, plus today for the month in progress. FILTER TO YES when grouping by Month — otherwise the count unions every day in the month and inflates, because a point-in-time metric cannot be aggregated by counting distinct over a period."
  }

  dimension: is_week_end {
    type: yesno
    sql: ${TABLE}.is_week_end ;;
    label: "Is Week End"
    description: "Yes on the last day of each week, plus today for the week in progress. FILTER TO YES when grouping by Week, for the same reason as Is Month End. Assumes the model's week_start_day is monday."
  }

  # ——— Subscription ———

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
    label: "Current Status"
    description: "Status TODAY, not on the report date — fact_seller_subscription keeps no history. A subscription active on an old report date may show 'canceled' here. NEVER filter a historical series on this: that is the survivorship bug this view was rebuilt to remove."
  }

  dimension: is_currently_zero_mrr {
    type: yesno
    sql: ${TABLE}.is_currently_zero_mrr ;;
    label: "Is Currently Zero MRR"
    description: "Fully discounted at TODAY's price. Differs from Has Ever Billed = No for a subscription that paid full price and was later comped — those sit in Paying subscribers, correctly, because they did generate revenue."
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

  dimension: plan_price {
    type: number
    sql: ${TABLE}.plan_price ;;
    value_format_name: decimal_2
    label: "Plan Price"
    description: "The plan's own list price, from the plan entry — excludes add-ons, tax and discounts. Price is plan + add-ons + tax minus discount, so the two differ for anyone holding an add-on. Both are CURRENT state; neither is historical."
  }

  dimension: addon_amount {
    type: number
    sql: ${TABLE}.addon_amount ;;
    value_format_name: decimal_2
    label: "Add-on Amount"
    description: "Monthly add-on value. Three types exist: commentChatAddOn, popStoreAiEchoMeAddOn (Credit Pack) and modelMeAddOn. 7 live subscriptions carry one as of 2026-08-24."
  }

  dimension: has_addon {
    type: yesno
    sql: ${TABLE}.addon_amount > 0 ;;
    label: "Has Add-on"
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
    timeframes: [date, month]
  }

  dimension_group: trial_ends_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.trial_ends ;;
    timeframes: [date, month]
  }

  # ——— Measures ———
  # Use total_subscribers on the stacked chart with Subscriber Category pivoted.
  # The four measures below are for single-value tiles, each reproducing one
  # published figure exactly.

  measure: total_subscribers {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    label: "Subscribers"
    description: "Distinct live subscriptions. Pivot on Subscriber Category for the stacked bar — the four segments are mutually exclusive, so the stack total is correct. 1,079 as of 2026-08-12. !! ONLY VALID AT DAILY GRAIN unless you filter Is Week End or Is Month End to Yes: grouping by week or month unions every day in the period, so a subscription live on any day counts once and the bar reads high."
    drill_fields: [drilldown_details*]
  }

  measure: paying_subscribers {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_internal: "no", has_ever_billed: "yes"]
    label: "Paying Subscribers"
    description: "External and has been billed — the figure that actually contributes revenue. 985 as of 2026-08-12."
    drill_fields: [drilldown_details*]
  }

  measure: stripe_comparable_subscribers {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [has_ever_billed: "yes"]
    label: "Stripe-Comparable Subscribers"
    description: "Paying plus Internal-paying — Stripe's definition, which excludes discounts but includes internal accounts. 1,009 as of 2026-08-12 against Stripe's 1,011; the 2 is snapshot timing. Validated to 0/-1/-7/-4/-1/0 across Feb-Jul 2026 month-ends."
    drill_fields: [drilldown_details*]
  }

  measure: old_view_equivalent {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_internal: "no"]
    label: "Old View Equivalent"
    description: "Paying plus 100% discounted, internal excluded — what the previous version of this tile counted. 1,037 as of 2026-08-12 against the old tile's 1,045. Provided for the transition; it reconciles to nothing external."
    drill_fields: [drilldown_details*]
  }

  measure: discounted_subscribers {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_internal: "no", has_ever_billed: "no"]
    label: "100% Discounted"
    description: "External, never billed anything. 52 as of 2026-08-12."
    drill_fields: [drilldown_details*]
  }

  measure: internal_subscribers {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_internal: "yes"]
    label: "Internal Accounts"
    description: "Both internal segments combined. 42 as of 2026-08-12 (24 paying, 18 discounted)."
    drill_fields: [drilldown_details*]
  }

  measure: active_creators {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Distinct Creators"
    description: "Below the subscription count where a creator holds two concurrent subscriptions. Do NOT sum across a pivoted chart — a creator with subscriptions in two categories appears in both."
    drill_fields: [drilldown_details*]
  }

  # ——— Drill Set ———

  set: drilldown_details {
    fields: [
      report_date,
      subscriber_category,
      user_id,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      subscription_id,
      subscription_status,
      plan_name,
      plan_interval,
      price,
      has_ever_billed,
      is_currently_zero_mrr,
      is_internal,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type
    ]
  }
}

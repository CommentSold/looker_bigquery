view: qa_trial_report {
  filter: date_range {
    type: date
    description: "Filter events by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql:
      WITH base AS (
        SELECT
          t1.*,
          plan,

      COALESCE(
      CASE
      WHEN t1.cancellation_applied_at IS NOT NULL AND t1.cancellation_applied_at < t1.trial_end
      THEN t1.cancellation_applied_at
      END,
      t1.trial_end
      ) AS effective_trial_end

      FROM dbt_popshop.fact_seller_subscription t1,
      UNNEST(t1.plans) AS plan

      WHERE
      t1.trial_end IS NOT NULL
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND {% condition date_range %} TIMESTAMP(t1.initial_start_date) {% endcondition %}
      ),

      -- ✅ Mirror qa_onboarding_funnel: filter to onboarding_complete events so we pick up
      -- the row that actually carries marketing_campaign / utm_regintent / business_type.
      onboarding_events AS (
      SELECT
      context_campaign_campaign AS marketing_campaign,
      utm_regintent,
      business_type,
      `timestamp`,
      user_id,
      scene,
      step_name,
      onboarding_session_id
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      WHERE (scene = 'onboarding' OR scene IS NULL)
      AND (step_name = 'onboarding_complete' OR step_name IS NULL)
      ),

      -- ✅ Deduplicate to the most relevant onboarding event per user.
      -- Prefer rows where the marketing/intent/business_type fields are actually populated,
      -- and among those pick the most recent by timestamp. This prevents picking an
      -- arbitrary 'generic' row.
      onboarding_events_dedup AS (
      SELECT
      user_id,
      marketing_campaign,
      utm_regintent,
      business_type,
      `timestamp`,
      onboarding_session_id
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
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent') AS utm_regintent
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      ),

      -- ✅ Deduplicate ai_pdf_generations to most recent record per user_id
      ai_pdf_latest AS (
      SELECT
      user_id,
      session_id,
      created_at,
      status
      FROM `popshoplive-26f81.commentsold.ai_pdf_generations`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) = 1
      )

      SELECT
      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email AS sign_up_user_email,
      COALESCE(oe.marketing_campaign, mc.utm_campaign) AS marketing_campaign,
      COALESCE(oe.utm_regintent, mc.utm_regintent) AS utm_regintent,
      oe.business_type,

      CASE
      WHEN DATETIME(base.effective_trial_end) <= CURRENT_DATETIME()
      AND DATETIME_DIFF(DATETIME(base.effective_trial_end), DATETIME(base.initial_start_date), DAY) <= 7
      THEN 'Cancelled within 7 days'
      WHEN DATETIME(base.effective_trial_end) <= CURRENT_DATETIME()
      THEN 'Cancelled after 7 days'
      END AS cancellation_status,

      CASE
      WHEN DATETIME(base.effective_trial_end) <= CURRENT_DATETIME()
      AND DATETIME_DIFF(DATETIME(base.effective_trial_end), DATETIME(base.initial_start_date), DAY) <= 7
      AND base.effective_trial_end = base.trial_end
      THEN 1 ELSE 0
      END AS within_7_days,

      CASE
      WHEN DATETIME(base.effective_trial_end) <= CURRENT_DATETIME()
      AND DATETIME_DIFF(DATETIME(base.effective_trial_end), DATETIME(base.initial_start_date), DAY) > 7
      AND base.status = 'active'
      THEN 1 ELSE 0
      END AS after_7_days,

      mc.utm_campaign AS marketing_utm_campaign,
      mc.utm_source AS marketing_utm_source,
      mc.utm_regintent AS marketing_utm_regintent,

      CASE
      WHEN COALESCE(oe.marketing_campaign, mc.utm_campaign) IS NOT NULL
      THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL
      THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source,

      base.id,
      base.status,
      base.initial_start_date AS trial_starts,
      base.trial_end AS trial_ends,
      base.cancellation_applied_at,
      base.effective_trial_end,
      base.subscription_id,
      base.user_id,

      COALESCE(base.discounted_price, base.price + base.tax_amount) AS price,

      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval') AS plan_interval,

      CASE
      WHEN base.trial_end IS NULL THEN 'No trial'
      WHEN DATETIME(base.effective_trial_end) <= CURRENT_DATETIME() THEN 'Ended'
      ELSE 'Started'
      END AS trial_status,

      CASE
      WHEN base.trial_end IS NULL THEN 3
      WHEN DATETIME(base.effective_trial_end) <= CURRENT_DATETIME() THEN 2
      ELSE 1
      END AS trial_type,

      CASE
      WHEN base.initial_start_date IS NOT NULL THEN 1
      ELSE 0
      END AS is_trial_started,

      CASE
      WHEN DATETIME(base.effective_trial_end) <= CURRENT_DATETIME() THEN 1
      ELSE 0
      END AS is_trial_ended,

      base.current_period_start,
      base.current_period_end,

      CASE
      WHEN base.current_period_start IS NOT NULL
      AND DATETIME(base.current_period_start) >= DATETIME(base.effective_trial_end)
      AND base.status = 'active'
      THEN 1 ELSE 0
      END AS is_subscription_start,

      CASE
      WHEN base.cancellation_applied_at IS NOT NULL
      AND base.current_period_end IS NOT NULL
      AND DATETIME(base.current_period_end) <= CURRENT_DATETIME()
      THEN 1 ELSE 0
      END AS is_subscription_manually_ended,

      CASE
      WHEN base.cancellation_applied_at IS NULL
      AND base.current_period_end IS NOT NULL
      AND DATETIME(base.current_period_end) <= CURRENT_DATETIME()
      THEN 1 ELSE 0
      END AS is_subscription_ended,

      -- ✅ ai_pdf_generations fields — "No Record" when no match
      COALESCE(aipdf.session_id, 'No Record')    AS ai_pdf_session_id,
      COALESCE(CAST(aipdf.created_at AS STRING), 'No Record') AS ai_pdf_created_at,
      COALESCE(aipdf.status, 'No Record')        AS ai_pdf_status

      FROM base

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = base.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = base.user_id

      -- ✅ Use deduplicated CTE instead of raw table
      LEFT JOIN ai_pdf_latest aipdf
      ON aipdf.user_id = base.user_id

      LEFT JOIN marketing_capture mc
      ON mc.user_id = base.user_id

      -- ✅ Join to the filtered + properly-prioritized onboarding event
      LEFT JOIN onboarding_events_dedup oe
      ON oe.user_id = base.user_id

      WHERE
      (pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      ))

      ORDER BY base.initial_start_date DESC;;
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
    primary_key: yes
    hidden: yes
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

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
    value_format_name: decimal_2
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

  dimension: trial_status {
    type: string
    sql: ${TABLE}.trial_status ;;
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

  dimension: within_7_days {
    type: number
    sql: ${TABLE}.within_7_days ;;
    hidden: yes
  }

  dimension: after_7_days {
    type: number
    sql: ${TABLE}.after_7_days ;;
    hidden: yes
  }

  dimension: cancellation_status {
    type: string
    sql: ${TABLE}.cancellation_status ;;
  }

  dimension_group: current_period_start_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.current_period_start ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension_group: current_period_end_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.current_period_end ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension: is_subscription_start {
    type: number
    sql: ${TABLE}.is_subscription_start ;;
    hidden: yes
  }

  dimension: is_subscription_manually_ended {
    type: number
    sql: ${TABLE}.is_subscription_manually_ended ;;
    hidden: yes
  }

  dimension: is_subscription_ended {
    type: number
    sql: ${TABLE}.is_subscription_ended ;;
    hidden: yes
  }

  # ✅ ai_pdf_generations drill-down dimensions
  dimension: ai_pdf_session_id {
    type: string
    sql: ${TABLE}.ai_pdf_session_id ;;
    label: "AI PDF Session ID"
    description: "Most recent session_id from ai_pdf_generations. 'No Record' if none found."
  }

  dimension: ai_pdf_created_at {
    type: string
    sql: ${TABLE}.ai_pdf_created_at ;;
    label: "AI PDF Created At"
    description: "Most recent created_at from ai_pdf_generations. 'No Record' if none found."
  }

  dimension: ai_pdf_status {
    type: string
    sql: ${TABLE}.ai_pdf_status ;;
    label: "AI PDF Status"
    description: "Most recent status from ai_pdf_generations. 'No Record' if none found."
  }

  measure: sum_total_trials {
    type: count_distinct
    sql: ${id} ;;
    label: "Total Trials"
    drill_fields: [onboarding_details*]
  }

  measure: sum_total_trials_started {
    type: sum
    sql: ${TABLE}.is_trial_started ;;
    label: "Started Trials"
    drill_fields: [onboarding_details*]
  }

  measure: sum_total_trials_ended {
    type: sum
    sql: ${TABLE}.is_trial_ended ;;
    label: "Ended Trials"
    drill_fields: [onboarding_details*]
  }

  measure: cancelled_within_7_days {
    type: count
    filters: [within_7_days: "1"]
    label: "Within 7 days"
    drill_fields: [onboarding_details*]
  }

  measure: cancelled_after_7_days {
    type: count
    filters: [after_7_days: "1"]
    label: "After 7 days"
    drill_fields: [onboarding_details*]
  }

  measure: total_subscriptions_cancelled {
    type: sum
    sql: ${TABLE}.is_subscription_manually_ended ;;
    label: "Subscriptions Cancelled"
    drill_fields: [onboarding_details*]
  }

  measure: total_subscriptions_started {
    type: sum
    sql: ${TABLE}.is_subscription_start ;;
    label: "Subscriptions Started"
    drill_fields: [onboarding_details*]
  }

  measure: total_subscriptions_ended {
    type: sum
    sql: ${TABLE}.is_subscription_ended ;;
    label: "Subscriptions Ended (Natural)"
    drill_fields: [onboarding_details*]
  }

  measure: net_active_subscriptions {
    type: number
    sql: ${total_subscriptions_started} - ${total_subscriptions_cancelled} - ${total_subscriptions_ended} ;;
    label: "Net Active Subscriptions"
    drill_fields: [onboarding_details*]
  }

  set: onboarding_details {
    fields: [
      user_id,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      subscription_id,
      plan_name,
      plan_interval,
      price,
      trial_status,
      trial_starts_at_date,
      trial_ends_at_date,
      effective_trial_ends_at_date,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type,
      ai_pdf_session_id,
      ai_pdf_created_at,
      ai_pdf_status
    ]
  }
}

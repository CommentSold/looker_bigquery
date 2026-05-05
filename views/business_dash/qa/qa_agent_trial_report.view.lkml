view: qa_agent_trial_report {
  filter: date_range {
    type: date
    description: "Filter trials by start date. Use 'is in range' in the UI. Optional."
  }

  derived_table: {
    sql:
    WITH base AS (
      SELECT
        t1.*,
        plan,
        COALESCE(
          CASE
            WHEN t1.cancellation_applied_at IS NOT NULL
              AND t1.cancellation_applied_at < t1.trial_end
            THEN t1.cancellation_applied_at
          END,
          t1.trial_end
        ) AS effective_trial_end
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS plan
      WHERE
        t1.trial_end IS NOT NULL
        AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
        AND {% condition date_range %} TIMESTAMP(t1.initial_start_date) {% endcondition %}
    ),

      -- Onboarding events (mirrors qa_trial_report logic)
      onboarding_events AS (
      SELECT
      context_campaign_campaign AS marketing_campaign,
      utm_regintent,
      business_type,
      `timestamp`,
      user_id
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      WHERE (scene = 'onboarding' OR scene IS NULL)
      AND (step_name = 'onboarding_complete' OR step_name IS NULL)
      ),

      onboarding_events_dedup AS (
      SELECT
      user_id,
      marketing_campaign,
      utm_regintent,
      business_type,
      `timestamp`
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
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_campaign')  AS utm_campaign,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_source')    AS utm_source,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent') AS utm_regintent
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      ),

      -- Most recent echo_me_agents row per user — surfaces is_meta_setup_valid
      echo_me_latest AS (
      SELECT
      user_id,
      is_meta_setup_valid,
      created_at AS meta_setup_last_seen_at
      FROM `popshoplive-26f81.commentchat.echo_me_agents`
      WHERE is_meta_setup_valid IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY created_at DESC
      ) = 1
      )

      SELECT
      base.id,
      base.subscription_id,
      base.user_id,
      base.status,

      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email AS sign_up_user_email,

      -- Effective utm_regintent (with fallback) — used both for filtering and display
      COALESCE(oe.utm_regintent, mc.utm_regintent) AS utm_regintent,
      COALESCE(oe.marketing_campaign, mc.utm_campaign) AS marketing_campaign,
      oe.business_type,

      CASE
      WHEN COALESCE(oe.marketing_campaign, mc.utm_campaign) IS NOT NULL THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source,

      base.initial_start_date AS trial_starts,
      base.trial_end AS trial_ends,
      base.effective_trial_end,
      base.cancellation_applied_at,

      COALESCE(base.discounted_price, base.price + base.tax_amount) AS price,
      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval')    AS plan_interval,

      CASE
      WHEN base.trial_end IS NULL THEN 'No trial'
      WHEN DATETIME(base.effective_trial_end) <= CURRENT_DATETIME() THEN 'Ended'
      ELSE 'Started'
      END AS trial_status,

      CASE
      WHEN base.initial_start_date IS NOT NULL THEN 1 ELSE 0
      END AS is_trial_started,

      -- Meta setup flag display: 'Valid' / 'Invalid' / 'Not Set Up'
      CASE
      WHEN ema.user_id IS NULL THEN 'Not Set Up'
      WHEN ema.is_meta_setup_valid = TRUE THEN 'Valid'
      ELSE 'Invalid'
      END AS meta_setup_status,

      ema.is_meta_setup_valid,
      ema.meta_setup_last_seen_at

      FROM base

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = base.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = base.user_id

      LEFT JOIN onboarding_events_dedup oe
      ON oe.user_id = base.user_id

      LEFT JOIN marketing_capture mc
      ON mc.user_id = base.user_id

      LEFT JOIN echo_me_latest ema
      ON ema.user_id = base.user_id

      WHERE
        -- Restrict to agent-related regintents only
        COALESCE(oe.utm_regintent, mc.utm_regintent) IN (
          'brand_deals_agent',
          'auto_selling_agent',
          'engagement_agent',
          'comment_to_dm_agent',
          'ai_team'
        )
        AND (pprof.email IS NULL OR (
          LOWER(pprof.email) NOT LIKE '%@test.com'
          AND LOWER(pprof.email) NOT LIKE '%@example.com'
          AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
          AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
        ))
      ORDER BY base.initial_start_date DESC
      ;;
  }

  # ——— Identity / Hidden ———

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
    primary_key: yes
    hidden: yes
  }

  dimension: is_trial_started {
    type: number
    sql: ${TABLE}.is_trial_started ;;
    hidden: yes
  }

  # ——— Profile / Drill-down dimensions ———

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: sign_up_user_username {
    type: string
    sql: ${TABLE}.sign_up_user_username ;;
  }

  dimension: sign_up_user_email {
    type: string
    sql: ${TABLE}.sign_up_user_email ;;
  }

  dimension: sign_up_user_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.sign_up_url_code ;;
  }

  # ——— Trial / Subscription dimensions ———

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    label: "Subscription Status"
  }

  dimension: trial_status {
    type: string
    sql: ${TABLE}.trial_status ;;
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

  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
    value_format_name: decimal_2
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

  # ——— Attribution dimensions ———

  dimension: utm_regintent {
    type: string
    sql: ${TABLE}.utm_regintent ;;
    label: "UTM Regintent"
    description: "Restricted to agent regintents: brand_deals_agent, auto_selling_agent, engagement_agent, comment_to_dm_agent, ai_team"
  }

  dimension: marketing_campaign {
    type: string
    sql: ${TABLE}.marketing_campaign ;;
  }

  dimension: business_type {
    type: string
    sql: ${TABLE}.business_type ;;
  }

  dimension: acquisition_source {
    type: string
    sql: ${TABLE}.acquisition_source ;;
  }

  # ——— Meta setup (echo_me_agents) ———

  dimension: meta_setup_status {
    type: string
    sql: ${TABLE}.meta_setup_status ;;
    label: "Meta Setup Status"
    description: "Valid / Invalid / Not Set Up — based on most recent echo_me_agents record"
  }

  dimension: is_meta_setup_valid {
    type: yesno
    sql: ${TABLE}.is_meta_setup_valid ;;
    label: "Is Meta Setup Valid"
    description: "Raw boolean flag from latest echo_me_agents row. NULL when user has no record."
  }

  dimension_group: meta_setup_last_seen_at {
    type: time
    sql: ${TABLE}.meta_setup_last_seen_at ;;
    timeframes: [date, week, month, quarter, year]
    label: "Meta Setup Last Seen"
  }

  # ——— Measures ———

  measure: total_agent_trials {
    type: count_distinct
    sql: ${id} ;;
    label: "Agent Trials"
    description: "Distinct trial subscriptions where utm_regintent is an agent regintent"
    drill_fields: [agent_drill_details*]
  }

  measure: agent_trials_started {
    type: sum
    sql: ${TABLE}.is_trial_started ;;
    label: "Agent Trials Started"
    description: "Count of started agent trials"
    drill_fields: [agent_drill_details*]
  }

  measure: meta_setup_valid_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [meta_setup_status: "Valid"]
    label: "Users w/ Valid Meta Setup"
    drill_fields: [agent_drill_details*]
  }

  measure: meta_setup_invalid_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [meta_setup_status: "Invalid"]
    label: "Users w/ Invalid Meta Setup"
    drill_fields: [agent_drill_details*]
  }

  measure: meta_setup_not_setup_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [meta_setup_status: "Not Set Up"]
    label: "Users w/o Meta Setup"
    drill_fields: [agent_drill_details*]
  }

  measure: pct_meta_setup_valid {
    type: number
    sql: SAFE_DIVIDE(${meta_setup_valid_count}, NULLIF(${total_agent_trials}, 0)) * 100 ;;
    label: "% Agent Trials w/ Valid Meta"
    value_format_name: decimal_1
  }

  # ——— Drill set (includes Meta setup flag for ALL users) ———

  set: agent_drill_details {
    fields: [
      user_id,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      subscription_id,
      status,
      plan_name,
      plan_interval,
      price,
      trial_status,
      trial_starts_at_date,
      trial_ends_at_date,
      effective_trial_ends_at_date,
      utm_regintent,
      marketing_campaign,
      business_type,
      acquisition_source,
      meta_setup_status,
      is_meta_setup_valid,
      meta_setup_last_seen_at_date
    ]
  }
}

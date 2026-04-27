view: prod_ai_echo_me {
  filter: date_range {
    type: date
    description: "Filter by trial start date (initial_start_date). Use 'is in range' in the UI. Optional."
  }

  derived_table: {
    sql:
      WITH base_raw AS (
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
          ) AS effective_trial_end,
          JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
          JSON_EXTRACT_SCALAR(plan, '$.interval') AS plan_interval
        FROM dbt_popshop.fact_seller_subscription t1,
        UNNEST(t1.plans) AS plan
        WHERE
          t1.trial_end IS NOT NULL
          AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      ),

      -- Deduplicate to one row per user (handles multiple 'plan' type entries)
      base AS (
      SELECT *
      FROM base_raw
      QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY initial_start_date DESC) = 1
      ),

      -- Marketing capture fallback for utm_regintent
      marketing_capture AS (
      SELECT
      user_id,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_campaign') AS utm_campaign,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_source') AS utm_source,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent') AS utm_regintent
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      ),

      -- Echo Me agent stats per user: count of agents activated, total rows
      -- Actual status values: null, 'enabled', 'disabled', 'preview', 'connected'
      echo_me_user_stats AS (
      SELECT
      user_id,
      COUNT(*) AS total_echo_me_agents,
      COUNTIF(sea_status IN ('enabled', 'connected')) AS total_sea_active,
      COUNTIF(coda_status IN ('enabled', 'connected')) AS total_coda_active,
      COUNTIF(dma_status IN ('enabled', 'connected')) AS total_dma_active,
      COUNTIF(asa_status IN ('enabled', 'connected')) AS total_asa_active,
      COUNTIF(raca_status IN ('enabled', 'connected')) AS total_raca_active,
      -- Count rows where at least one agent is enabled or connected
      COUNTIF(
      sea_status IN ('enabled', 'connected')
      OR coda_status IN ('enabled', 'connected')
      OR dma_status IN ('enabled', 'connected')
      OR asa_status IN ('enabled', 'connected')
      OR raca_status IN ('enabled', 'connected')
      ) AS total_with_any_agent_active,
      CASE
        WHEN COUNTIF(ai_echo_setup_complete = TRUE) > 0 THEN 'Yes'
        ELSE 'No'
      END AS onboarding_complete
      FROM `popshoplive-26f81.commentchat.echo_me_agents`
      GROUP BY user_id
      ),

      -- Most recent echo_me_agents record per user
      echo_me_latest AS (
        SELECT
          user_id,

          -- Per-agent latest status (independent of other agents)
          ARRAY_AGG(sea_status IGNORE NULLS ORDER BY sea_status_last_changed DESC LIMIT 1)[SAFE_OFFSET(0)] AS sea_status,
          ARRAY_AGG(coda_status IGNORE NULLS ORDER BY coda_status_last_changed DESC LIMIT 1)[SAFE_OFFSET(0)] AS coda_status,
          ARRAY_AGG(dma_status IGNORE NULLS ORDER BY dma_status_last_changed DESC LIMIT 1)[SAFE_OFFSET(0)] AS dma_status,
          ARRAY_AGG(asa_status IGNORE NULLS ORDER BY asa_status_last_changed DESC LIMIT 1)[SAFE_OFFSET(0)] AS asa_status,
          ARRAY_AGG(raca_status IGNORE NULLS ORDER BY raca_status_last_changed DESC LIMIT 1)[SAFE_OFFSET(0)] AS raca_status,

          -- Representative record fields — pick from the most recently updated row
          ARRAY_AGG(STRUCT(
            id AS echo_me_id,
            session_id,
            agent,
            created_at,
            updated_at,
            external_channel_id,
            external_channel_name,
            channel_type,
            linked_meta_app,
            linked_meta_app_id
          ) ORDER BY updated_at DESC LIMIT 1)[SAFE_OFFSET(0)].* ,

          -- Overall status derived AFTER collapsing
          CASE
            WHEN ARRAY_AGG(sea_status  IGNORE NULLS ORDER BY sea_status_last_changed  DESC LIMIT 1)[SAFE_OFFSET(0)] IN ('enabled','connected')
              OR ARRAY_AGG(coda_status IGNORE NULLS ORDER BY coda_status_last_changed DESC LIMIT 1)[SAFE_OFFSET(0)] IN ('enabled','connected')
              OR ARRAY_AGG(dma_status  IGNORE NULLS ORDER BY dma_status_last_changed  DESC LIMIT 1)[SAFE_OFFSET(0)] IN ('enabled','connected')
              OR ARRAY_AGG(asa_status  IGNORE NULLS ORDER BY asa_status_last_changed  DESC LIMIT 1)[SAFE_OFFSET(0)] IN ('enabled','connected')
              OR ARRAY_AGG(raca_status IGNORE NULLS ORDER BY raca_status_last_changed DESC LIMIT 1)[SAFE_OFFSET(0)] IN ('enabled','connected')
            THEN 'active'
            ELSE 'inactive'
          END AS overall_agent_status

        FROM `popshoplive-26f81.commentchat.echo_me_agents`
        GROUP BY user_id
      ),

      -- Trial users with Echo Me activity (INNER JOIN — only trial users appear, grouped by trial_start_date)
      trial_echo_me_users AS (
      SELECT
      -- echo_me fields
      em.echo_me_id,
      em.session_id,
      em.agent,
      em.created_at AS echo_me_created_at,
      em.updated_at AS echo_me_updated_at,
      CASE WHEN em.sea_status  = 'disabled' THEN 'paused' ELSE em.sea_status  END AS sea_status,
      em.coda_status,
      em.dma_status,
      em.asa_status,
      em.raca_status,
      em.overall_agent_status,
      em.external_channel_id,
      em.external_channel_name,
      em.channel_type,
      em.linked_meta_app,
      em.linked_meta_app_id,

      -- Echo Me stats per user
      stats.total_echo_me_agents,
      stats.total_sea_active,
      stats.total_coda_active,
      stats.total_dma_active,
      stats.total_asa_active,
      stats.total_raca_active,
      stats.onboarding_complete,
      stats.total_with_any_agent_active,

      -- subscription / trial fields
      base.id,
      base.subscription_id,
      base.user_id,
      base.status AS subscription_status,
      base.initial_start_date AS trial_starts,
      base.trial_end AS trial_ends,
      base.effective_trial_end,
      base.cancellation_applied_at,

      COALESCE(base.discounted_price, base.price + base.tax_amount) AS price,
      base.plan_name,
      base.plan_interval,

      CASE
      WHEN base.trial_end IS NULL THEN 'No trial'
      WHEN DATE(base.effective_trial_end) <= CURRENT_DATE() THEN 'Ended'
      ELSE 'Started'
      END AS trial_status,

      -- User's current status: Subscriber or Cancelled
      CASE
      WHEN base.status = 'active' THEN 'Subscriber'
      WHEN base.status IN ('canceled', 'cancelled') THEN 'Cancelled'
      WHEN base.cancellation_applied_at IS NOT NULL THEN 'Cancelled'
      ELSE COALESCE(base.status, 'Unknown')
      END AS user_current_status,

      -- Boolean flag for paying subscriber (for drill down)
      CASE
      WHEN base.status = 'active' THEN 'Yes'
      ELSE 'No'
      END AS is_subscriber,

      -- profile fields
      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email AS sign_up_user_email,

      -- acquisition fields with fallback for utm_regintent
      COALESCE(NULLIF(oe.context_campaign_campaign, ''), 'generic') AS marketing_campaign,
      COALESCE(NULLIF(oe.utm_regintent, ''), NULLIF(mc.utm_regintent, ''), 'generic') AS utm_regintent,
      COALESCE(NULLIF(oe.business_type, ''), 'generic') AS business_type,

      CASE
      WHEN COALESCE(oe.context_campaign_campaign, mc.utm_campaign) IS NOT NULL
      THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL
      THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source,

      FROM base

      -- INNER JOIN to echo_me_latest — only users who started a trial AND have Echo Me activity
      INNER JOIN echo_me_latest em
      ON em.user_id = base.user_id

      -- Join to user stats for counts
      LEFT JOIN echo_me_user_stats stats
      ON stats.user_id = base.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = base.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = base.user_id

      -- Marketing capture fallback
      LEFT JOIN marketing_capture mc
      ON mc.user_id = base.user_id

      LEFT JOIN (
      SELECT *
      FROM (
      SELECT *,
      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp DESC) AS rn
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      )
      WHERE rn = 1
      ) oe ON oe.user_id = base.user_id

      WHERE
      (pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      ))
      )

      SELECT *
      FROM trial_echo_me_users
      WHERE 1=1
      {% if date_range._is_filtered %}
      AND {% condition date_range %} trial_starts {% endcondition %}
      {% endif %}
      ORDER BY echo_me_created_at DESC
      ;;
  }

  # ——— Primary Key ———

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
    primary_key: yes
  }

  dimension: echo_me_id {
    type: string
    sql: ${TABLE}.echo_me_id ;;
    label: "Latest Echo Me Record ID"
    description: "Most recent echo_me_agents record ID for this user"
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
    label: "Latest Echo Me Session ID"
    description: "Most recent Echo Me session_id for this user"
  }

  # ——— Echo Me Dimensions ———

  dimension_group: echo_me_created {
    type: time
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.echo_me_created_at ;;
    timeframes: [time, date, week, month, quarter, year]
    label: "Echo Me Created"
  }

  dimension_group: echo_me_updated {
    type: time
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.echo_me_updated_at ;;
    timeframes: [time, date, week, month, quarter, year]
    label: "Echo Me Updated"
  }

  dimension: agent {
    type: string
    sql: ${TABLE}.agent ;;
    label: "Agent"
    description: "Agent identifier for the most recent Echo Me record"
  }

  dimension: overall_agent_status {
    type: string
    sql: ${TABLE}.overall_agent_status ;;
    label: "Overall Agent Status"
    description: "Status values: 'active' (at least one agent enabled/connected), 'inactive' (no agents enabled/connected). Raw statuses: null, enabled, disabled, preview, connected."
  }

  dimension: sea_status {
    type: string
    sql: ${TABLE}.sea_status ;;
    label: "Social Engagement Agent Status"
    description: "Values: null, enabled, disabled, preview, connected"
  }

  dimension: coda_status {
    type: string
    sql: ${TABLE}.coda_status ;;
    label: "Comment to DM Agent Status"
    description: "Values: null, enabled, disabled, preview, connected"
  }

  dimension: dma_status {
    type: string
    sql: ${TABLE}.dma_status ;;
    label: "Deal Monitoring Agent Status"
    description: "Values: null, enabled, disabled, preview, connected"
  }

  dimension: asa_status {
    type: string
    sql: ${TABLE}.asa_status ;;
    label: "Auto Selling Agent Status"
    description: "Values: null, enabled, disabled, preview, connected"
  }

  dimension: raca_status {
    type: string
    sql: ${TABLE}.raca_status ;;
    label: "Real Estate Concierge Agent Status"
    description: "Values: null, enabled, disabled, preview, connected"
  }

  # ——— Echo Me Stats Dimensions ———

  dimension: total_echo_me_agents {
    type: number
    sql: ${TABLE}.total_echo_me_agents ;;
    label: "Total Echo Me Agent Records"
    description: "Total number of echo_me_agents rows for this user"
  }

  dimension: total_with_any_agent_active {
    type: number
    sql: ${TABLE}.total_with_any_agent_active ;;
    label: "Records With Any Agent Active"
    description: "Number of echo_me_agents rows where at least one agent is enabled or connected"
  }

  # ——— Channel Dimensions ———

  dimension: external_channel_id {
    type: string
    sql: ${TABLE}.external_channel_id ;;
    label: "External Channel ID"
  }

  dimension: external_channel_name {
    type: string
    sql: ${TABLE}.external_channel_name ;;
    label: "External Channel Name"
  }

  dimension: channel_type {
    type: string
    sql: ${TABLE}.channel_type ;;
    label: "Channel Type"
  }

  dimension: linked_meta_app {
    type: string
    sql: ${TABLE}.linked_meta_app ;;
    label: "Linked Meta App"
  }

  dimension: linked_meta_app_id {
    type: string
    sql: ${TABLE}.linked_meta_app_id ;;
    label: "Linked Meta App ID"
  }

  # ——— User / Subscription Dimensions ———

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: user_current_status {
    type: string
    sql: ${TABLE}.user_current_status ;;
    label: "User Current Status"
    description: "Whether the user is currently a Subscriber or Cancelled"
  }

  dimension: is_subscriber {
    type: string
    sql: ${TABLE}.is_subscriber ;;
    label: "Subscriber"
    description: "Yes if the user is a paying subscriber, No otherwise"
  }

  dimension: trial_status {
    type: string
    sql: ${TABLE}.trial_status ;;
  }

  dimension_group: trial_starts_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.trial_starts ;;
    timeframes: [date, week, month, quarter, year]
    label: "Trial Start"
    description: "Use this date dimension as x-axis to match trial_start graph cohorts"
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

  # ——— Profile / Acquisition Dimensions ———

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
    description: "Defaults to 'generic' if null"
  }

  dimension: utm_regintent {
    type: string
    sql: ${TABLE}.utm_regintent ;;
    label: "UTM Regintent"
    description: "Use as the Color dimension in chart config. Falls back to onboardingMarketingCapture if missing, then to 'generic' if null."
  }

  dimension: business_type {
    type: string
    sql: ${TABLE}.business_type ;;
    description: "Defaults to 'generic' if null"
  }

  dimension: acquisition_source {
    type: string
    sql: ${TABLE}.acquisition_source ;;
  }

  dimension: onboarding_complete {
    type: string
    sql: ${TABLE}.onboarding_complete ;;
    label: "Onboarding Complete"
    description: "Yes if the user has at least one echo_me_agents row with ai_echo_setup_complete set. No otherwise."
  }

  # ——— Measures ———

  measure: total_echo_me_setups {
    type: count_distinct
    sql: ${TABLE}.echo_me_id ;;
    label: "Total Echo Me Setups"
    description: "Count of distinct Echo Me agent setup records"
    drill_fields: [drill_details*]
  }

  measure: total_unique_users {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Unique Users"
    description: "Count of distinct trial users who set up an Echo Me agent"
    drill_fields: [drill_details*]
  }

  measure: total_active {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [overall_agent_status: "active"]
    label: "Active Agent Users"
    description: "Trial users with at least one agent enabled or connected"
    drill_fields: [drill_details*]
  }

  measure: total_inactive {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [overall_agent_status: "inactive"]
    label: "Inactive Agent Users"
    description: "Trial users with no agents enabled or connected"
    drill_fields: [drill_details*]
  }

  measure: activation_rate {
    type: number
    sql: SAFE_DIVIDE(${total_active}, NULLIF(${total_unique_users}, 0)) * 100 ;;
    label: "Activation Rate (%)"
    value_format_name: decimal_1
    drill_fields: [drill_details*]
  }

  measure: count_subscribers {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [user_current_status: "Subscriber"]
    label: "Current Subscribers"
    description: "Count of users who are currently subscribed"
    drill_fields: [drill_details*]
  }

  measure: count_cancelled {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [user_current_status: "Cancelled"]
    label: "Cancelled Users"
    description: "Count of users who have cancelled"
    drill_fields: [drill_details*]
  }

  measure: sum_echo_me_agents {
    type: sum
    sql: ${TABLE}.total_echo_me_agents ;;
    label: "Total Echo Me Agent Records (Sum)"
    description: "Sum of all echo_me_agents rows across users"
    drill_fields: [drill_details*]
  }

  measure: sum_with_any_agent_active {
    type: sum
    sql: ${TABLE}.total_with_any_agent_active ;;
    label: "Total Records With Active Agents (Sum)"
    description: "Sum of echo_me_agents rows with at least one enabled/connected agent across users"
    drill_fields: [drill_details*]
  }

  measure: count_setup_complete {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [onboarding_complete: "Yes"]
    label: "AI Echo Setup Complete Users"
    description: "Count of users who have completed AI Echo setup"
    drill_fields: [drill_details*]
  }

  # ——— Drill Set ———

  set: drill_details {
    fields: [
      user_id,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      is_subscriber,
      onboarding_complete,
      session_id,
      echo_me_created_date,
      overall_agent_status,
      sea_status,
      coda_status,
      dma_status,
      asa_status,
      raca_status,
      total_echo_me_agents,
      total_with_any_agent_active,
      channel_type,
      external_channel_name,
      linked_meta_app,
      user_current_status,
      subscription_id,
      subscription_status,
      plan_name,
      plan_interval,
      price,
      trial_status,
      trial_starts_at_date,
      trial_ends_at_date,
      effective_trial_ends_at_date,
      marketing_campaign,
      utm_regintent,
      business_type,
      acquisition_source
    ]
  }
}

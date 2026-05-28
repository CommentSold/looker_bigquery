view: prod_monthly_new_trials_started {
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

      -- Deduplicate to one row per user
      base AS (
      SELECT *
      FROM base_raw
      QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY initial_start_date DESC) = 1
      ),

      -- Monthly targets hardcoded
      targets AS (
      SELECT * FROM UNNEST([
      STRUCT(2026 AS year, 1 AS month_number, 0 AS new_trial_started_target),
      STRUCT(2026 AS year, 2 AS month_number, 0 AS new_trial_started_target),
      STRUCT(2026 AS year, 3 AS month_number, 0 AS new_trial_started_target),
      STRUCT(2026 AS year, 4 AS month_number, 447 AS new_trial_started_target),
      STRUCT(2026 AS year, 5 AS month_number, 484 AS new_trial_started_target),
      STRUCT(2026 AS year, 6 AS month_number, 837 AS new_trial_started_target),
      STRUCT(2026 AS year, 7 AS month_number, 698 AS new_trial_started_target),
      STRUCT(2026 AS year, 8 AS month_number, 645 AS new_trial_started_target),
      STRUCT(2026 AS year, 9 AS month_number, 725 AS new_trial_started_target),
      STRUCT(2026 AS year, 10 AS month_number, 768 AS new_trial_started_target),
      STRUCT(2026 AS year, 11 AS month_number, 786 AS new_trial_started_target),
      STRUCT(2026 AS year, 12 AS month_number, 786 AS new_trial_started_target)
      ])
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

      -- ✅ Deduplicate to the most relevant onboarding event per user.
      -- Prefer rows where the marketing/intent/business_type fields are actually populated,
      -- and among those pick the most recent by timestamp. This prevents picking an
      -- arbitrary 'generic' row.
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
        COALESCE(
          CASE
            WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(bot|crawler|spider|crawl|slurp|googlebot|bingpreview|facebookexternalhit|twitterbot|linkedinbot|discordbot|telegrambot|google-read-aloud)') THEN 'BOT'
            WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'instagram') THEN 'WEBVIEW_INSTAGRAM'
            WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(fban|fbav|facebook)') THEN 'WEBVIEW_FACEBOOK'
            WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'tiktok') THEN 'WEBVIEW_TIKTOK'
            WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'snapchat') THEN 'WEBVIEW_SNAPCHAT'
            WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(linkedin|linkedinapp)') THEN 'WEBVIEW_LINKEDIN'
            WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(wv|webview|meta-iab|metaiab|iabmv/1|whatsapp|line|gsa/|googleapp/|youtube|reddit)') THEN 'WEBVIEW_OTHER'
            WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(iphone|ipad|ipod|cpu iphone os|cpu os)') THEN 'IOS'
            WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'android') THEN 'ANDROID'
            WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(windows nt|win64|wow64)') THEN 'WINDOWS_DESKTOP'
            WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(macintosh|mac os x)') AND NOT REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(iphone|ipad)') THEN 'MACOS_DESKTOP'
            WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(linux|x11)') AND NOT REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'android') THEN 'LINUX_DESKTOP'
            ELSE 'OTHER'
          END,
          "No Onboarding Event"
        ) AS device_category
        FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      ),

      -- Trial-level data with acquisition fields
      trial_data AS (
      SELECT
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

      -- Month key for joining to targets
      EXTRACT(YEAR FROM base.initial_start_date) AS trial_start_year,
      EXTRACT(MONTH FROM base.initial_start_date) AS trial_start_month,

      CASE
      WHEN base.trial_end IS NULL THEN 'No trial'
      WHEN DATE(base.effective_trial_end) <= CURRENT_DATE() THEN 'Ended'
      ELSE 'Started'
      END AS trial_status,

      CASE
      WHEN base.status = 'active' THEN 'Subscriber'
      WHEN base.status IN ('canceled', 'cancelled') THEN 'Cancelled'
      WHEN base.cancellation_applied_at IS NOT NULL THEN 'Cancelled'
      ELSE COALESCE(base.status, 'Unknown')
      END AS user_current_status,

      CASE
      WHEN base.status = 'active' THEN 'Yes'
      ELSE 'No'
      END AS is_subscriber,

      -- profile fields
      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email AS sign_up_user_email,

      COALESCE(oe.marketing_campaign, mc.utm_campaign) AS marketing_campaign,
      COALESCE(oe.utm_regintent, mc.utm_regintent) AS utm_regintent,
      COALESCE(oe.business_type, JSON_VALUE(prof.profile, '$.businessType')) AS business_type,

      CASE
      WHEN COALESCE(oe.marketing_campaign, mc.utm_campaign) IS NOT NULL
      THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL
      THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source

      FROM base

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = base.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = base.user_id

      LEFT JOIN marketing_capture mc
      ON mc.user_id = base.user_id

      LEFT JOIN onboarding_events_dedup oe
      ON oe.user_id = base.user_id

      WHERE
      (pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      AND LOWER(pprof.email) NOT LIKE '%@pop.store'
      ))
      ),

      -- Actual trial rows joined to their monthly target
      actual_rows AS (
      SELECT
      td.*,
      t.new_trial_started_target,
      TIMESTAMP(DATE(td.trial_start_year, td.trial_start_month, 1)) AS month_start_date
      FROM trial_data td
      INNER JOIN targets t
      ON td.trial_start_year = t.year
      AND td.trial_start_month = t.month_number
      ),

      -- Synthetic placeholder rows for target-only months (no actuals yet)
      target_only_months AS (
      SELECT
      CAST(NULL AS STRING) AS id,
      CAST(NULL AS STRING) AS subscription_id,
      '__target_placeholder__' AS user_id,
      CAST(NULL AS STRING) AS subscription_status,
      TIMESTAMP(DATE(t.year, t.month_number, 1)) AS trial_starts,
      CAST(NULL AS TIMESTAMP) AS trial_ends,
      CAST(NULL AS TIMESTAMP) AS effective_trial_end,
      CAST(NULL AS TIMESTAMP) AS cancellation_applied_at,
      CAST(NULL AS FLOAT64) AS price,
      CAST(NULL AS STRING) AS plan_name,
      CAST(NULL AS STRING) AS plan_interval,
      t.year AS trial_start_year,
      t.month_number AS trial_start_month,
      CAST(NULL AS STRING) AS trial_status,
      CAST(NULL AS STRING) AS user_current_status,
      'No' AS is_subscriber,
      CAST(NULL AS STRING) AS sign_up_url_code,
      CAST(NULL AS STRING) AS sign_up_user_username,
      CAST(NULL AS STRING) AS sign_up_user_email,
      'generic' AS marketing_campaign,
      'generic' AS utm_regintent,
      'generic' AS business_type,
      'organic_walk-in' AS acquisition_source,
      t.new_trial_started_target,
      TIMESTAMP(DATE(t.year, t.month_number, 1)) AS month_start_date
      FROM targets t
      WHERE NOT EXISTS (
      SELECT 1 FROM trial_data td
      WHERE td.trial_start_year = t.year
      AND td.trial_start_month = t.month_number
      )
      ),

      -- Union actual rows + target-only placeholder rows
      combined AS (
      SELECT * FROM actual_rows
      UNION ALL
      SELECT * FROM target_only_months
      )

      SELECT *
      FROM combined
      WHERE 1=1
      {% if date_range._is_filtered %}
      AND {% condition date_range %} trial_starts {% endcondition %}
      {% endif %}
      ORDER BY month_start_date DESC, trial_starts DESC
      ;;
  }

  # ——— Primary Key ———

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
    primary_key: yes
    hidden: yes
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  # ——— Trial Date Dimensions ———

  dimension_group: trial_starts_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.trial_starts ;;
    timeframes: [date, week, month, quarter, year]
    label: "Trial Start"
    description: "Use month timeframe as x-axis for the monthly chart"
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

  # ——— Subscription Dimensions ———

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

  # ——— Target Dimension ———

  dimension: new_trial_started_target {
    type: number
    sql: ${TABLE}.new_trial_started_target ;;
    label: "Monthly New Trials Started: Target"
    description: "Hardcoded monthly target for new trials started"
    hidden: yes
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

  # ——— Measures ———

  measure: monthly_new_trials_started_actual {
    type: count_distinct
    sql: CASE WHEN ${TABLE}.user_id != '__target_placeholder__' THEN ${TABLE}.user_id END ;;
    label: "Monthly New Trials Started: Actual"
    description: "Count of distinct users who started a trial in the month (excludes placeholder rows)"
    drill_fields: [drill_details*]
  }

  measure: monthly_new_trials_started_target {
    type: max
    sql: ${TABLE}.new_trial_started_target ;;
    label: "Monthly New Trials Started: Target"
    description: "Hardcoded monthly target for new trials started"
    drill_fields: [drill_details*]
  }

  measure: target_achievement_rate {
    type: number
    sql: SAFE_DIVIDE(${monthly_new_trials_started_actual}, NULLIF(${monthly_new_trials_started_target}, 0)) * 100 ;;
    label: "Target Achievement Rate (%)"
    value_format_name: decimal_1
    drill_fields: [drill_details*]
  }

  measure: count_subscribers {
    type: count_distinct
    sql: CASE WHEN ${TABLE}.user_id != '__target_placeholder__' THEN ${TABLE}.user_id END ;;
    filters: [user_current_status: "Subscriber"]
    label: "Current Subscribers"
    drill_fields: [drill_details*]
  }

  measure: count_cancelled {
    type: count_distinct
    sql: CASE WHEN ${TABLE}.user_id != '__target_placeholder__' THEN ${TABLE}.user_id END ;;
    filters: [user_current_status: "Cancelled"]
    label: "Cancelled Users"
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
      subscription_id,
      subscription_status,
      user_current_status,
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

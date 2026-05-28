view: prod_trial_subscription_summary {
  derived_table: {
    sql:
      WITH total_active_sub AS (
        -- ── Total Active Subscriptions (yesterday signups) ─────────
        SELECT
          prof.user_id,
          prof.url_code         AS sign_up_url_code,
          prof.username         AS sign_up_user_username,
          pprof.email           AS sign_up_user_email,
          DATE(prof.created_at) AS signup_date,
          'paid'                AS subscription_type,
          -- Join subscription to get sub-level fields
          fs.subscription_id,
          DATE(fs.initial_start_date) AS trial_start_date,
          DATE(fs.trial_end)          AS trial_end_date
        FROM `popshoplive-26f81.dbt_popshop.dim_profiles` prof
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
          ON pprof.user_id = prof.user_id
        -- Join latest active subscription per user
        LEFT JOIN (
          SELECT
            subscription_id,
            user_id,
            initial_start_date,
            trial_end
          FROM `dbt_popshop.fact_seller_subscription`,
          UNNEST(plans) AS plan
          WHERE is_deleted = FALSE
            AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
          -- ✅ Dedupe by subscription_id not user_id
          -- so users with multiple subs get all their rows
          QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY updated_at DESC) = 1
        ) fs ON fs.user_id = prof.user_id
        WHERE prof.user_type IN ('seller', 'verifiedSeller')
          AND prof.apps_pop_store = TRUE
          AND DATE(prof.created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          AND (pprof.email IS NULL OR (
            LOWER(pprof.email) NOT LIKE '%@test.com'
            AND LOWER(pprof.email) NOT LIKE '%@example.com'
            AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
            AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
            AND LOWER(pprof.email) NOT LIKE '%@pop.store'
          ))
      ),

      -- ── Total Active Trials (yesterday trial starts) ───────────
      total_active_trials AS (
      SELECT
      t1.user_id,
      DATE(t1.initial_start_date) AS trial_start_date,
      DATE(t1.trial_end)          AS trial_end_date,
      t1.subscription_id,
      'trial'                     AS subscription_type
      FROM `dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS plan
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = t1.user_id
      WHERE t1.trial_end IS NOT NULL
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND DATE(t1.initial_start_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND (pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      AND LOWER(pprof.email) NOT LIKE '%@pop.store'
      ))
      QUALIFY ROW_NUMBER() OVER (PARTITION BY t1.subscription_id ORDER BY t1.updated_at DESC) = 1
      ),

      -- ── Union both with shared shape ───────────────────────────
      combined AS (
      SELECT
      user_id,
      subscription_id,
      trial_start_date,
      trial_end_date,
      subscription_type,
      sign_up_url_code,
      sign_up_user_username,
      sign_up_user_email
      FROM total_active_sub

      UNION ALL

      SELECT
      tat.user_id,
      tat.subscription_id,
      tat.trial_start_date,
      tat.trial_end_date,
      tat.subscription_type,
      prof.url_code     AS sign_up_url_code,
      prof.username     AS sign_up_user_username,
      pprof.email       AS sign_up_user_email
      FROM total_active_trials tat
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = tat.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = tat.user_id
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
      )

      SELECT
      c.user_id,
      c.subscription_id,
      c.trial_start_date,
      c.trial_end_date,
      c.subscription_type,
      c.sign_up_url_code,
      c.sign_up_user_username,
      c.sign_up_user_email,

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

      FROM combined c
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = c.user_id
      LEFT JOIN marketing_capture mc
      ON mc.user_id = c.user_id

      -- ✅ Join to the filtered + properly-prioritized onboarding event
      LEFT JOIN onboarding_events_dedup oe
      ON oe.user_id = c.user_id
      ;;
  }

  # ——— Dimensions ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(${TABLE}.user_id, '-', ${TABLE}.subscription_type) ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: subscription_type {
    type: string
    sql: ${TABLE}.subscription_type ;;
    description: "trial = started trial yesterday | paid = signed up yesterday"
  }

  dimension_group: trial_start {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.trial_start_date ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension_group: trial_end {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.trial_end_date ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension: acquisition_source {
    type: string
    sql: ${TABLE}.acquisition_source ;;
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

  # ——— Measures ———

  # Card 1 — green: yesterday's trial starts
  measure: total_active_trials {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [subscription_type: "trial"]
    label: "Yesterday's Total Active Trials"
    description: "Distinct users who started a trial yesterday"
    drill_fields: [drilldown_details*]
  }

  # Card 2 — orange: yesterday's new signups
  measure: total_active_subscriptions {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [subscription_type: "paid"]
    label: "Total Active Subscriptions"
    description: "Distinct seller/verifiedSeller profiles created yesterday"
    drill_fields: [drilldown_details*]
  }

  # Card 3 — conversion rate: (trials / subscriptions) * 100
  measure: trial_to_paid_conversion_rate {
    type: number
    sql: SAFE_DIVIDE(${total_active_trials}, NULLIF(${total_active_subscriptions}, 0));;
    label: "Trial → Paid Conversion Rate"
    description: "(Yesterday's Total Active Trials / Total Active Subscriptions)"
    value_format_name: percent_1
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
      subscription_type,
      trial_start_date,
      trial_end_date,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type
    ]
  }
}

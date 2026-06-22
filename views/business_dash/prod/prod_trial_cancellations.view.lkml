view: prod_trial_cancellations {
  derived_table: {
    sql:
      WITH onboarding_events AS (
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
      )
      SELECT
        fs.subscription_id,
        fs.user_id,
        DATE(COALESCE(fs.cancelled_at, fs.updated_at)) AS cancellation_date,
        fs.initial_start_date,

      CASE
      WHEN DATE(fs.initial_start_date) = DATE(COALESCE(fs.cancelled_at, fs.updated_at))
      THEN 'same_day'
      ELSE 'later'
      END AS cancellation_timing,

      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email AS sign_up_user_email,
      mc.profile_email,
      mc.first_name,
      mc.last_name,
      COALESCE(oe.marketing_campaign, mc.utm_campaign) AS marketing_campaign,
      COALESCE(oe.utm_regintent, mc.utm_regintent) AS utm_regintent,
      COALESCE(oe.business_type, JSON_VALUE(prof.profile, '$.businessType')) AS business_type,
      COALESCE(oe.onboarding_path, mc.onboarding_path) AS onboarding_path,
      COALESCE(oe.plan_level, mc.plan_level) AS plan_level,
      COALESCE(oe.device_category, mc.device_category) AS device_category,
      COALESCE(oe.user_agent, mc.user_agent) AS user_agent,

      COALESCE(fs.discounted_price, fs.price + fs.tax_amount) AS price,
      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval') AS plan_interval,
      fs.initial_start_date AS trial_starts,
      fs.trial_end AS trial_ends,

      COALESCE(
      CASE
      WHEN fs.cancellation_applied_at IS NOT NULL
      AND fs.cancellation_applied_at < fs.trial_end
      THEN fs.cancellation_applied_at
      END,
      fs.trial_end
      ) AS effective_trial_end,

      CASE
      WHEN fs.trial_end IS NULL THEN 'No trial'
      WHEN DATE(COALESCE(
      CASE
      WHEN fs.cancellation_applied_at IS NOT NULL
      AND fs.cancellation_applied_at < fs.trial_end
      THEN fs.cancellation_applied_at
      END,
      fs.trial_end
      )) <= CURRENT_DATE() THEN 'Ended'
      ELSE 'Started'
      END AS trial_status,

      -- Inferred subscription status.
      -- 'payment retrying' : status='past_due' — trial ended, the post-trial
      --   charge failed and Stripe is still retrying (dunning). Not cancelled.
      -- 'unpaid'           : status='unpaid' (legacy, pre 2026-05-10) OR
      --   status='canceled' with NULL cancellation_applied_at. Upstream stopped
      --   emitting 'unpaid' on 2026-05-10, so those now arrive as 'canceled'
      --   with a NULL cancellation_applied_at.
      -- 'canceled'         : status='canceled' with a non-NULL
      --   cancellation_applied_at — a genuine member-initiated cancellation.
      CASE
      WHEN fs.status = 'past_due'
      THEN 'payment retrying'
      WHEN fs.status = 'canceled' AND fs.cancellation_applied_at IS NOT NULL
      THEN 'canceled'
      WHEN (fs.status = 'canceled' AND fs.cancellation_applied_at IS NULL)
      OR fs.status = 'unpaid'
      THEN 'unpaid'
      END AS subscription_status,

      CASE
      WHEN COALESCE(oe.marketing_campaign, mc.utm_campaign) IS NOT NULL
      THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL
      THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source

      FROM `dbt_popshop.fact_seller_subscription` fs

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
      fs.trial_end IS NOT NULL
      AND fs.status IN ('canceled', 'unpaid', 'past_due')
      AND (
      -- Branch 1: cancelled during or before trial
      fs.cancelled_at <= fs.trial_end
      -- Branch 2: subscription period never advanced past trial (Stripe didn't bill)
      OR fs.current_period_end = fs.trial_end
      -- Branch 3: status went unpaid without an explicit cancel.
      -- Post 2026-05-10 the upstream no longer emits status='unpaid', so an
      -- unpaid trial cancellation now surfaces as status='canceled' with a
      -- NULL cancellation_applied_at. Match both the legacy and inferred forms.
      OR (
      (fs.status = 'unpaid'
      OR (fs.status = 'canceled' AND fs.cancellation_applied_at IS NULL))
      AND fs.cancelled_at IS NULL
      AND fs.trial_end <= CURRENT_TIMESTAMP()
      )
      -- Branch 4: NEW. Catches the ~10 edge-case subs where Stripe advanced
      -- current_period_end after trial, billed once, all retries failed, and
      -- cancelled_at landed in the post-trial billing period. We're confident
      -- these are trial cancellations because of the NOT EXISTS guard below.
      OR (fs.cancelled_at IS NOT NULL AND fs.cancelled_at > fs.trial_end)
      -- Branch 5: NEW. Trial ended, the first post-trial charge failed and
      -- Stripe is still retrying — status='past_due', not cancelled yet.
      -- Surfaced as 'payment retrying'. The NOT EXISTS guard below keeps out
      -- anyone who already had a successful post-trial payment.
      OR (fs.status = 'past_due' AND fs.trial_end <= CURRENT_TIMESTAMP())
      )
      AND NOT EXISTS (
      -- Excludes the paid-then-failed subs currently caught by Branch 3.
      -- Anyone with a successful post-trial payment belongs in
      -- prod_paid_subscription_cancellations, not here.
      SELECT 1
      FROM `dbt_popshop.fact_seller_subscription_invoice` inv
      WHERE inv.subscription_id = fs.subscription_id
      AND inv.is_deleted = FALSE
      AND inv.status = 'paid'
      AND inv.amount_due > 0
      AND inv.amount_paid > 0
      AND inv.updated_at >= fs.trial_end
      )
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND (pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      AND LOWER(pprof.email) NOT LIKE '%@pop.store'
      ))
      {% if date_range._is_filtered %}
      AND {% condition date_range %} TIMESTAMP(fs.initial_start_date) {% endcondition %}
      {% endif %}
      ;;
  }

  # ——— Filters ———

  filter: date_range {
    type: date
    description: "Filter by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  # ——— Dimensions ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension_group: cancellation {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.cancellation_date ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension: cancellation_timing {
    type: string
    sql: ${TABLE}.cancellation_timing ;;
    description: "Whether cancellation happened on the same day as trial start (same_day) or later. Pivot on this for stacked bars."
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
    description: "Inferred status used as a pivot. 'canceled' = member-initiated (status='canceled' with non-NULL cancellation_applied_at); 'unpaid' = status='unpaid' legacy rows or status='canceled' with NULL cancellation_applied_at (upstream stopped emitting 'unpaid' on 2026-05-10); 'payment retrying' = status='past_due', trial ended with the post-trial charge still in Stripe retry/dunning (not cancelled)."
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: sign_up_user_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.sign_up_url_code ;;
  }

  # ✅ New profile JSON dimensions
  dimension: profile_email {
    type: string
    sql: ${TABLE}.profile_email ;;
    label: "Profile Email (JSON)"
    description: "Email pulled from private_profile JSON ($.email). May differ from sign_up_user_email."
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
    label: "First Name"
    description: "From private_profile.sellerShippingAddress.firstName. NULL if user has not set a shipping address."
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
    label: "Last Name"
    description: "From private_profile.sellerShippingAddress.lastName. NULL if user has not set a shipping address."
  }

  dimension: full_name {
    type: string
    sql: TRIM(CONCAT(COALESCE(${TABLE}.first_name, ''), ' ', COALESCE(${TABLE}.last_name, ''))) ;;
    label: "Full Name"
    description: "Concatenated first_name + last_name from shipping address. Empty when neither is set."
  }

  dimension: sign_up_user_username {
    type: string
    sql: ${TABLE}.sign_up_user_username ;;
  }

  dimension: sign_up_user_email {
    type: string
    sql: ${TABLE}.sign_up_user_email ;;
    description: "Email from dim_private_profiles.email column"
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

  dimension: onboarding_path {
    type: string
    sql: ${TABLE}.onboarding_path ;;
  }

  dimension: plan_level {
    type: string
    sql: ${TABLE}.plan_level ;;
  }

  dimension: device_category {
    type: string
    sql: ${TABLE}.device_category ;;
  }

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
  }

  dimension: acquisition_source {
    type: string
    sql: ${TABLE}.acquisition_source ;;
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

  dimension_group: trial_starts_at {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.trial_starts ;;
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

  measure: trial_cancellations {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    label: "Trial Cancellations"
    description: "Total distinct subscriptions cancelled during trial"
    drill_fields: [drilldown_details*]
  }

  measure: same_day_cancellations {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [cancellation_timing: "same_day"]
    label: "Same Day Cancellations"
    description: "Cancellations where trial start date = cancellation date"
    drill_fields: [drilldown_details*]
  }

  measure: later_cancellations {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [cancellation_timing: "later"]
    label: "Later Cancellations"
    description: "Cancellations where trial start date != cancellation date"
    drill_fields: [drilldown_details*]
  }

  measure: unpaid_cancellations {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [subscription_status: "unpaid"]
    label: "Unpaid Cancellations"
    description: "Inferred unpaid trial cancellations: status='unpaid' (legacy) or status='canceled' with NULL cancellation_applied_at."
    drill_fields: [drilldown_details*]
  }

  measure: member_cancellations {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [subscription_status: "canceled"]
    label: "Member Cancellations"
    description: "Member-initiated trial cancellations: status='canceled' with a non-NULL cancellation_applied_at."
    drill_fields: [drilldown_details*]
  }

  measure: payment_retrying {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [subscription_status: "payment retrying"]
    label: "Payment Retrying"
    description: "Trials where the post-trial charge failed and Stripe is still retrying (status='past_due'). Not yet cancelled."
    drill_fields: [drilldown_details*]
  }

  # ——— Drill Set ———

  set: drilldown_details {
    fields: [
      user_id,
      first_name,
      last_name,
      profile_email,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      subscription_id,
      subscription_status,
      cancellation_timing,
      plan_name,
      plan_interval,
      price,
      trial_status,
      trial_starts_at_time,
      trial_ends_at_date,
      effective_trial_ends_at_date,
      cancellation_date,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type,
      onboarding_path,
      plan_level,
      device_category,
      user_agent
    ]
  }
}

view: prod_trial_conversions {
  derived_table: {
    sql:
      WITH subscriptions AS (
        SELECT
          subscription_id,
          trial_end AS trial_end_ts,
          status AS sub_status
        FROM `dbt_popshop.fact_seller_subscription`
        WHERE is_deleted = FALSE
      ),

      invoice_history AS (
      SELECT
      invoice_id,
      subscription_id,
      status,
      amount_due,
      amount_paid,
      created_at,
      updated_at
      FROM `dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      ),

      invoice_rollup AS (
      SELECT
      invoice_id,
      subscription_id,
      MIN(created_at) AS invoice_created_at,
      MAX(amount_due) AS max_amount_due,
      MAX(amount_paid) AS max_amount_paid
      FROM invoice_history
      GROUP BY 1, 2
      ),

      -- Only invoices where amount_due > 0 (excludes $0 trial invoices)
      billable_invoices AS (
      SELECT
      invoice_id,
      subscription_id,
      invoice_created_at
      FROM invoice_rollup
      WHERE max_amount_due > 0
      ),

      -- PAID: first paid event per billable invoice
      first_paid_event_per_invoice AS (
      SELECT
      ih.invoice_id,
      ih.subscription_id,
      ih.updated_at AS paid_at,
      ROW_NUMBER() OVER (
      PARTITION BY ih.invoice_id
      ORDER BY ih.updated_at ASC
      ) AS rn
      FROM invoice_history ih
      JOIN billable_invoices bi
      ON ih.invoice_id = bi.invoice_id
      AND ih.subscription_id = bi.subscription_id
      WHERE ih.status = 'paid'
      AND ih.amount_paid > 0
      ),

      paid_billable_invoices AS (
      SELECT
      invoice_id,
      subscription_id,
      paid_at
      FROM first_paid_event_per_invoice
      WHERE rn = 1
      ),

      first_paid_conversion_per_sub AS (
      SELECT
      s.subscription_id,
      p.paid_at,
      ROW_NUMBER() OVER (
      PARTITION BY s.subscription_id
      ORDER BY p.paid_at ASC
      ) AS rn
      FROM subscriptions s
      JOIN paid_billable_invoices p
      ON s.subscription_id = p.subscription_id
      AND p.paid_at >= s.trial_end_ts
      ),

      -- UNPAID: subscriptions with status='unpaid' that have a billable invoice
      -- but did NOT appear in paid_conversions
      unpaid_subs AS (
      SELECT
      s.subscription_id,
      bi.invoice_created_at AS event_at,
      'unpaid' AS invoice_status,
      ROW_NUMBER() OVER (
      PARTITION BY s.subscription_id
      ORDER BY bi.invoice_created_at ASC
      ) AS rn
      FROM subscriptions s
      JOIN billable_invoices bi
      ON bi.subscription_id = s.subscription_id
      AND bi.invoice_created_at >= s.trial_end_ts
      WHERE s.sub_status = 'unpaid'
      AND s.subscription_id NOT IN (
      SELECT subscription_id FROM first_paid_conversion_per_sub WHERE rn = 1
      )
      ),

      unpaid_conversions AS (
      SELECT
      subscription_id,
      event_at,
      invoice_status
      FROM unpaid_subs
      WHERE rn = 1
      ),

      paid_conversions AS (
      SELECT
      subscription_id,
      paid_at AS event_at,
      'paid' AS invoice_status
      FROM first_paid_conversion_per_sub
      WHERE rn = 1
      ),

      -- UNION paid + unpaid only
      combined AS (
      SELECT * FROM paid_conversions
      UNION ALL
      SELECT * FROM unpaid_conversions
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
      ELSE ""
      END,
      ""
      ) AS device_category
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      )
      SELECT
      combined.subscription_id,
      combined.event_at,
      combined.invoice_status,
      CASE
      WHEN combined.invoice_status = 'paid' AND fs.status != 'active'
      THEN TRUE
      ELSE FALSE
      END AS did_paid_sub_cancel,
      fs.user_id,
      fs.status AS current_subscription_status,
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
      COALESCE(oe.user_agent, mc.user_agent) AS user_agent,
      COALESCE(oe.device_category, mc.device_category) AS device_category,
      COALESCE(fs.discounted_price, fs.price + fs.tax_amount) AS price,
      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval') AS plan_interval,
      fs.initial_start_date AS trial_starts,
      fs.trial_end AS trial_ends,
      COALESCE(
      CASE WHEN fs.cancellation_applied_at IS NOT NULL AND fs.cancellation_applied_at < fs.trial_end
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

      CASE
      WHEN COALESCE(oe.marketing_campaign, mc.utm_campaign) IS NOT NULL
      THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL
      THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source

      FROM combined

      JOIN `dbt_popshop.fact_seller_subscription` fs
      ON fs.subscription_id = combined.subscription_id
      AND fs.is_deleted = FALSE

      CROSS JOIN UNNEST(fs.plans) AS plan
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof ON prof.user_id = fs.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof ON pprof.user_id = fs.user_id
      LEFT JOIN marketing_capture mc ON mc.user_id = fs.user_id
      LEFT JOIN onboarding_events_dedup oe ON oe.user_id = fs.user_id

      WHERE
      JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
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
    description: "Filter by trial start date. Use 'is in range' in the UI to pick start and end. Optional."
  }

  # ——— Dimensions ———

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(${TABLE}.subscription_id, '-', ${TABLE}.invoice_status) ;;
  }

  dimension_group: event {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    datatype: timestamp
    sql: ${TABLE}.event_at ;;
    description: "Timestamp of the conversion event (paid_at for paid, invoice_created_at for unpaid)"
  }

  dimension: invoice_status {
    type: string
    sql: ${TABLE}.invoice_status ;;
    description: "Invoice status: paid or unpaid only."
  }

  # ✅ NEW: paid-sub churn flag
  dimension: did_paid_sub_cancel {
    type: yesno
    sql: ${TABLE}.did_paid_sub_cancel ;;
    description: "TRUE if this is a paid converter (invoice_status='paid') whose subscription is now non-active. Used for cohort churn rate."
  }

  dimension: current_subscription_status {
    type: string
    sql: ${TABLE}.current_subscription_status ;;
    description: "Current status of the underlying subscription (active, canceled, unpaid, etc.)."
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

  measure: trial_conversion_count {
    type: count
    description: "Count of subscriptions by invoice status (paid or unpaid)"
    drill_fields: [drilldown_details*]
  }

  measure: paid_converted_trials {
    type: count
    filters: [invoice_status: "paid"]
    description: "Count of subscriptions that converted from trial to paid"
    drill_fields: [drilldown_details*]
  }

  measure: unpaid_trials {
    type: count
    filters: [invoice_status: "unpaid"]
    description: "Count of subscriptions marked unpaid at subscription level (payment failed)"
    drill_fields: [drilldown_details*]
  }

  measure: paid_converted_trials_last_28_days {
    type: count
    filters: [invoice_status: "paid", trial_starts_at_date: "28 days"]
    description: "Count of trial-to-paid conversions in the last 28 days, by trial start date"
    drill_fields: [drilldown_details*]
  }

  # ✅ NEW: Cohort churn measures
  # By trial start cohort: of trials that started in month X, what percentage of
  # the paid converters are now non-active (cancelled / payment failed)?

  measure: paid_subs_cancelled_from_cohort {
    type: count
    filters: [
      invoice_status: "paid",
      did_paid_sub_cancel: "yes"
    ]
    label: "Paid Subs Cancelled From Cohort"
    description: "Of the paid converters in this trial-start cohort, how many are now non-active. Numerator for cohort paid-churn rate."
    drill_fields: [drilldown_details*]
  }

  measure: paid_subs_still_active_from_cohort {
    type: count
    filters: [
      invoice_status: "paid",
      did_paid_sub_cancel: "no"
    ]
    label: "Paid Subs Still Active From Cohort"
    description: "Of the paid converters in this trial-start cohort, how many are still active."
    drill_fields: [drilldown_details*]
  }

  measure: paid_sub_cohort_churn_rate {
    type: number
    sql:
      SAFE_DIVIDE(
        ${paid_subs_cancelled_from_cohort},
        ${paid_converted_trials}
      ) ;;
    value_format_name: percent_1
    label: "Paid Sub Cohort Churn Rate"
    description: "paid_subs_cancelled_from_cohort / paid_converted_trials. By trial-start cohort: percentage of paid converters who are now non-active. Lower is better."
  }

  measure: paid_sub_cohort_retention_rate {
    type: number
    sql:
      SAFE_DIVIDE(
        ${paid_subs_still_active_from_cohort},
        ${paid_converted_trials}
      ) ;;
    value_format_name: percent_1
    label: "Paid Sub Cohort Retention Rate"
    description: "paid_subs_still_active_from_cohort / paid_converted_trials. Complement of churn rate. Higher is better."
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
      invoice_status,
      current_subscription_status,
      did_paid_sub_cancel,
      plan_name,
      plan_interval,
      price,
      trial_status,
      trial_starts_at_time,
      trial_ends_at_date,
      effective_trial_ends_at_date,
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

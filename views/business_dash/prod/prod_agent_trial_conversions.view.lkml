view: prod_agent_trial_conversions {
  derived_table: {
    sql:
    WITH subscriptions AS (
      SELECT
        subscription_id,
        user_id,
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
      MAX(amount_due)  AS max_amount_due,
      MAX(amount_paid) AS max_amount_paid
      FROM invoice_history
      GROUP BY 1, 2
      ),

      -- Only invoices where amount_due > 0 (excludes $0 trial invoices)
      billable_invoices AS (
      SELECT invoice_id, subscription_id, invoice_created_at
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
      SELECT invoice_id, subscription_id, paid_at
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
      SELECT subscription_id, event_at, invoice_status
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

      combined AS (
      SELECT * FROM paid_conversions
      UNION ALL
      SELECT * FROM unpaid_conversions
      ),

      -- Onboarding events (mirrors qa_agent_trial_report logic)
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

      -- Most recent meta_setup_* row per user (matches qa_agent_trial_report scoping)
      echo_me_latest AS (
      SELECT
      id,
      user_id,
      is_meta_setup_valid,
      created_at AS meta_setup_last_seen_at
      FROM `popshoplive-26f81.commentchat.echo_me_agents`
      WHERE id LIKE '%meta_setup_%'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) = 1
      ),

      social_button_clicks AS (
      SELECT
      creator_id AS user_id,
      COUNTIF(action = 'fb_connection_initiated') > 0 AS clicked_fb_connect,
      COUNTIF(action = 'ig_connection_initiated') > 0 AS clicked_ig_connect,
      COUNTIF(action IN ('fb_connection_initiated', 'ig_connection_initiated')) > 0 AS clicked_social_connect,
      MAX(CASE WHEN action IN ('fb_connection_initiated', 'ig_connection_initiated') THEN created_at END) AS social_clicked_last_at
      FROM `popshoplive-26f81.commentchat.user_activity`
      WHERE action IN ('fb_connection_initiated', 'ig_connection_initiated')
      GROUP BY creator_id
      ),

      -- ✅ Connection-link email/SMS engagement, aggregated per creator.
      -- Same source + join key as social_button_clicks (commentchat.user_activity.creator_id = fs.user_id),
      -- and the same event taxonomy used by the prod_connection_link_email_activity view.
      -- One row per creator; counts default to 0 via COALESCE in the final SELECT for creators
      -- with no connection-link events.
      connection_link_activity AS (
      SELECT
      creator_id AS user_id,
      COUNTIF(action = 'connection_link_email_sent')   AS email_sent_count,
      COUNTIF(action = 'connection_link_email_opened') AS email_opened_count,
      COUNTIF(action = 'connection_link_sms_sent')     AS sms_sent_count,
      COUNTIF(action = 'connection_link_sms_opened')   AS sms_opened_count
      FROM `popshoplive-26f81.commentchat.user_activity`
      WHERE action IN (
      'connection_link_email_sent',
      'connection_link_email_opened',
      'connection_link_sms_sent',
      'connection_link_sms_opened'
      )
      GROUP BY creator_id
      )

      SELECT
      combined.subscription_id,
      combined.event_at,
      combined.invoice_status,

      fs.user_id,
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

      CASE
      WHEN COALESCE(oe.marketing_campaign, mc.utm_campaign) IS NOT NULL THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source,

      COALESCE(fs.discounted_price, fs.price + fs.tax_amount) AS price,
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

      -- Meta setup label (matches qa_agent_trial_report values for chart consistency)
      CASE
      WHEN ema.user_id IS NULL THEN 'Not Set Up'
      WHEN ema.is_meta_setup_valid = TRUE THEN 'Has Valid Meta Connection'
      ELSE 'Invalid'
      END AS meta_setup_status,

      ema.is_meta_setup_valid,
      ema.meta_setup_last_seen_at,

      COALESCE(sbc.clicked_social_connect, FALSE) AS clicked_social_connect,
      COALESCE(sbc.clicked_fb_connect, FALSE)     AS clicked_fb_connect,
      COALESCE(sbc.clicked_ig_connect, FALSE)     AS clicked_ig_connect,
      CASE
      WHEN sbc.clicked_fb_connect AND sbc.clicked_ig_connect THEN 'FB + IG'
      WHEN sbc.clicked_fb_connect THEN 'FB only'
      WHEN sbc.clicked_ig_connect THEN 'IG only'
      ELSE 'None'
      END AS social_button_clicked_status,
      sbc.social_clicked_last_at,

      -- ✅ Connection-link engagement counts (per creator / user_id)
      COALESCE(cla.email_sent_count, 0)   AS email_sent,
      COALESCE(cla.email_opened_count, 0) AS email_opened,
      COALESCE(cla.sms_sent_count, 0)     AS sms_sent,
      COALESCE(cla.sms_opened_count, 0)   AS sms_opened

      FROM combined

      JOIN `dbt_popshop.fact_seller_subscription` fs
      ON fs.subscription_id = combined.subscription_id
      AND fs.is_deleted = FALSE

      CROSS JOIN UNNEST(fs.plans) AS plan

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = fs.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = fs.user_id

      LEFT JOIN onboarding_events_dedup oe
      ON oe.user_id = fs.user_id

      LEFT JOIN marketing_capture mc
      ON mc.user_id = fs.user_id

      LEFT JOIN echo_me_latest ema
      ON ema.user_id = fs.user_id

      LEFT JOIN social_button_clicks sbc
      ON sbc.user_id = fs.user_id

      LEFT JOIN connection_link_activity cla
      ON cla.user_id = fs.user_id

      WHERE
      JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      -- Restrict to agent regintents (same list as qa_agent_trial_report)
      AND COALESCE(oe.utm_regintent, mc.utm_regintent) IN (
      'brand_deals_agent',
      'auto_selling_agent',
      'engagement_agent',
      'comment_to_dm_agent',
      'ai_team',
      'vidcon'
      )
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
    description: "Filter by trial start date. Use 'is in range' in the UI. Optional."
  }

  # ——— Identity ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(${TABLE}.subscription_id, '-', ${TABLE}.invoice_status) ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  # ——— Conversion event ———

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

  # ——— Profile / Drill-down ———

  dimension: sign_up_user_username {
    type: string
    sql: ${TABLE}.sign_up_user_username ;;
  }

  dimension: sign_up_user_email {
    type: string
    sql: ${TABLE}.sign_up_user_email ;;
    description: "Email from dim_private_profiles.email column"
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

  # ——— Plan / Trial dims ———

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

  # ——— Attribution ———

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

  # ——— Meta setup ———

  dimension: meta_setup_status {
    type: string
    sql: ${TABLE}.meta_setup_status ;;
    label: "Meta Setup Status"
    description: "Has Valid Meta Connection / Invalid / Not Set Up — based on most recent meta_setup_* row in echo_me_agents"
  }

  dimension: is_meta_setup_valid {
    type: yesno
    sql: ${TABLE}.is_meta_setup_valid ;;
    label: "Is Meta Setup Valid"
    description: "Raw boolean from latest meta_setup_* row. NULL when user has no record."
  }

  dimension_group: meta_setup_last_seen_at {
    type: time
    sql: ${TABLE}.meta_setup_last_seen_at ;;
    timeframes: [date, week, month, quarter, year]
    label: "Meta Setup Last Seen"
  }

  # ——— Social button clicks (commentchat.user_activity) ———

  dimension: social_button_clicked_status {
    type: string
    sql: ${TABLE}.social_button_clicked_status ;;
    label: "Social Button Clicked"
    description: "Whether the user clicked the FB and/or IG connect button. Values: 'FB + IG', 'FB only', 'IG only', 'None'."
  }

  dimension: clicked_social_connect {
    type: yesno
    sql: ${TABLE}.clicked_social_connect ;;
    label: "Clicked Social Connect (Any)"
    description: "TRUE if user has any fb_connection_initiated or ig_connection_initiated event in commentchat.user_activity."
  }

  dimension: clicked_fb_connect {
    type: yesno
    sql: ${TABLE}.clicked_fb_connect ;;
    label: "Clicked FB Connect"
  }

  dimension: clicked_ig_connect {
    type: yesno
    sql: ${TABLE}.clicked_ig_connect ;;
    label: "Clicked IG Connect"
  }

  dimension_group: social_clicked_last_at {
    type: time
    sql: ${TABLE}.social_clicked_last_at ;;
    timeframes: [date, week, month, quarter, year]
    label: "Social Button Last Clicked"
  }

  dimension: meta_setup_with_social_status {
    type: string
    sql:
    CASE
      WHEN ${meta_setup_status} = 'Has Valid Meta Connection'
        THEN 'Has Valid Meta Connection'
      WHEN ${meta_setup_status} = 'Not Set Up'
        AND ${clicked_social_connect} = TRUE
        THEN 'Not Set Up (Clicked Social)'
      WHEN ${meta_setup_status} = 'Not Set Up'
        AND ${clicked_social_connect} = FALSE
        THEN 'Not Set Up (No Social Click)'
      ELSE 'Unknown'
    END ;;
    description: "Meta setup status combined with whether the user clicked any social connect option."
  }

  # ——— Connection-link email/SMS engagement (commentchat.user_activity) ———

  dimension: email_sent {
    type: number
    sql: ${TABLE}.email_sent ;;
    label: "Email Sent (count)"
    description: "Number of connection_link_email_sent events for this user. 0 if none. Use email_sent > 0 to answer 'was an email ever sent?'."
  }

  dimension: email_opened {
    type: number
    sql: ${TABLE}.email_opened ;;
    label: "Email Opened (count)"
    description: "Number of connection_link_email_opened events for this user. 0 if none."
  }

  dimension: sms_sent {
    type: number
    sql: ${TABLE}.sms_sent ;;
    label: "SMS Sent (count)"
    description: "Number of connection_link_sms_sent events for this user. 0 if none."
  }

  dimension: sms_opened {
    type: number
    sql: ${TABLE}.sms_opened ;;
    label: "SMS Opened (count)"
    description: "Number of connection_link_sms_opened events for this user. 0 if none."
  }

  # ✅ Convenience yes/no flags for the "did it happen at all?" question
  dimension: has_email_sent {
    type: yesno
    sql: ${TABLE}.email_sent > 0 ;;
    label: "Email Sent?"
  }

  dimension: has_email_opened {
    type: yesno
    sql: ${TABLE}.email_opened > 0 ;;
    label: "Email Opened?"
  }

  dimension: has_sms_sent {
    type: yesno
    sql: ${TABLE}.sms_sent > 0 ;;
    label: "SMS Sent?"
  }

  dimension: has_sms_opened {
    type: yesno
    sql: ${TABLE}.sms_opened > 0 ;;
    label: "SMS Opened?"
  }

  # ——— Measures ———

  measure: trial_conversion_count {
    type: count
    label: "Trial Conversions (Paid + Unpaid)"
    description: "Count of agent trial conversions by invoice status"
    drill_fields: [agent_drill_details*]
  }

  measure: paid_converted_trials {
    type: count
    filters: [invoice_status: "paid"]
    label: "Paid Conversions"
    description: "Count of agent subscriptions that converted from trial to paid"
    drill_fields: [agent_drill_details*]
  }

  measure: unpaid_trials {
    type: count
    filters: [invoice_status: "unpaid"]
    label: "Unpaid Trials"
    description: "Count of agent subscriptions marked unpaid (payment failed)"
    drill_fields: [agent_drill_details*]
  }

  # — Meta setup measures (paid conversions only — used by Daily Trial Conversions chart) —

  measure: paid_conversions_meta_valid {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [invoice_status: "paid", meta_setup_status: "Has Valid Meta Connection"]
    label: "Paid Conversions w/ Valid Meta Connection"
    drill_fields: [agent_drill_details*]
  }

  measure: paid_conversions_meta_invalid {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [invoice_status: "paid", meta_setup_status: "Invalid"]
    label: "Paid Conversions w/ Invalid Meta"
    drill_fields: [agent_drill_details*]
  }

  measure: paid_conversions_meta_not_setup {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [invoice_status: "paid", meta_setup_status: "Not Set Up"]
    label: "Paid Conversions w/o Meta Setup"
    drill_fields: [agent_drill_details*]
  }

  measure: pct_paid_conversions_meta_valid {
    type: number
    sql: SAFE_DIVIDE(${paid_conversions_meta_valid}, NULLIF(${paid_converted_trials}, 0)) * 100 ;;
    label: "% Paid Conversions w/ Valid Meta"
    value_format_name: decimal_1
  }

  measure: social_connect_clicked_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [clicked_social_connect: "Yes"]
    label: "Users Who Clicked Social Connect"
    drill_fields: [agent_drill_details*]
  }

  # ✅ Distinct-user counts for connection-link engagement (handy for dashboard tiles)
  measure: users_with_email_sent {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [has_email_sent: "Yes"]
    label: "Users w/ Email Sent"
    drill_fields: [agent_drill_details*]
  }

  measure: users_with_email_opened {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [has_email_opened: "Yes"]
    label: "Users w/ Email Opened"
    drill_fields: [agent_drill_details*]
  }

  measure: users_with_sms_sent {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [has_sms_sent: "Yes"]
    label: "Users w/ SMS Sent"
    drill_fields: [agent_drill_details*]
  }

  measure: users_with_sms_opened {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [has_sms_opened: "Yes"]
    label: "Users w/ SMS Opened"
    drill_fields: [agent_drill_details*]
  }

  # ——— Drill set ———

  set: agent_drill_details {
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
      event_date,
      plan_name,
      plan_interval,
      price,
      trial_status,
      trial_starts_at_time,
      trial_ends_at_date,
      effective_trial_ends_at_date,
      utm_regintent,
      marketing_campaign,
      business_type,
      onboarding_path,
      plan_level,
      acquisition_source,
      meta_setup_status,
      is_meta_setup_valid,
      meta_setup_last_seen_at_date,
      social_button_clicked_status,
      clicked_fb_connect,
      clicked_ig_connect,
      social_clicked_last_at_date,
      email_sent,
      email_opened,
      sms_sent,
      sms_opened,
      device_category,
      user_agent
    ]
  }
}

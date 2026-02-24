view: onboarding_funnel {
  filter: date_range {
    type: date
    description: "Filter events by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql: WITH stores AS (
      SELECT
        user_id
      FROM `popshoplive-26f81.dbt_popshop.dim_profiles`
      WHERE user_type IN ('seller', 'verifiedSeller')
        AND apps_pop_store = TRUE
    ),
    onboarding_events AS (
      SELECT
        a.`timestamp`,
        a.user_id,
        a.utm_regintent,
        a.onboarding_session_id,
        a.context_campaign_campaign AS utm_campaign,
        a.context_user_agent,
        CASE
          WHEN REGEXP_CONTAINS(LOWER(a.context_user_agent), r'(bot|crawler|spider|crawl|slurp|googlebot|bingpreview|facebookexternalhit|twitterbot|linkedinbot|discordbot|telegrambot|google-read-aloud)') THEN 'BOT'
          WHEN REGEXP_CONTAINS(LOWER(a.context_user_agent), r'(wv|webview|meta-iab|metaiab|facebook|fban|fbav|instagram|iabmv/1|whatsapp|line|linkedinapp|snapchat|gsa/|googleapp/|youtube|tiktok|reddit)') THEN 'WEBVIEW'
          WHEN REGEXP_CONTAINS(LOWER(a.context_user_agent), r'(iphone|ipad|ipod|cpu iphone os|cpu os)') THEN 'IOS'
          WHEN REGEXP_CONTAINS(LOWER(a.context_user_agent), r'android') THEN 'ANDROID'
          WHEN REGEXP_CONTAINS(LOWER(a.context_user_agent), r'(windows nt|win64|wow64)') THEN 'WINDOWS_DESKTOP'
          WHEN REGEXP_CONTAINS(LOWER(a.context_user_agent), r'(macintosh|mac os x)') AND NOT REGEXP_CONTAINS(LOWER(a.context_user_agent), r'(iphone|ipad)') THEN 'MACOS_DESKTOP'
          WHEN REGEXP_CONTAINS(LOWER(a.context_user_agent), r'(linux|x11)') AND NOT REGEXP_CONTAINS(LOWER(a.context_user_agent), r'android') THEN 'LINUX_DESKTOP'
          ELSE 'OTHER'
        END AS device_category,
        CASE
          WHEN a.step_name IN ('onboarding_headshot_entered','onboarding_headshot_auto_skipped','onboarding_headshot_manual_skipped') THEN 'onboarding_headshot_entered'
          WHEN a.step_name IN ('onboarding_niche_entered','onboarding_niche_auto_skipped') THEN 'onboarding_niche_entered'
          WHEN a.step_name IN ('onboarding_user_otp_verified','onboarding_email_login_verified') THEN 'onboarding_auth_verified'
          WHEN a.step_name IN ('onboarding_ai_echo_prompt_entered','onboarding_ai_echo_template_selected') THEN 'onboarding_ai_echo_prompt_entered'
          ELSE a.step_name
        END AS step_name_canonical,
        CASE
          WHEN a.step_name = 'onboarding_headshot_auto_skipped' THEN 'auto_skipped'
          WHEN a.step_name = 'onboarding_headshot_manual_skipped' THEN 'manual_skipped'
          WHEN a.step_name = 'onboarding_niche_auto_skipped' THEN 'auto_skipped'
          WHEN a.step_name = 'onboarding_user_otp_verified' THEN 'otp_verified'
          WHEN a.step_name = 'onboarding_email_login_verified' THEN 'email_login_verified'
          ELSE 'completed'
        END AS step_variant,
        a.business_type
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action` a
      WHERE a.scene = 'onboarding'
        AND a.step_name IN (
          'onboarding_ai_echo_prompt_entered',
          'onboarding_ai_echo_template_selected',
          'onboarding_socials_entered',
          'onboarding_headshot_entered',
          'onboarding_headshot_auto_skipped',
          'onboarding_headshot_manual_skipped',
          'onboarding_niche_entered',
          'onboarding_niche_auto_skipped',
          'onboarding_intent_entered',
          'onboarding_preview_shown',
          'onboarding_url_confirmed',
          'onboarding_contact_info_submitted',
          'onboarding_user_otp_verified',
          'onboarding_email_login_verified',
          'onboarding_complete'
        )
        AND {% condition date_range %} a.`timestamp` {% endcondition %}
        AND {% condition utm_regintent %} a.utm_regintent {% endcondition %}
        AND {% condition onboarding_session_id %} a.onboarding_session_id {% endcondition %}
        AND NOT (
          a.step_name = 'onboarding_niche_auto_skipped'
          AND (
            NOT EXISTS (
              SELECT 1
              FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action` b
              WHERE b.onboarding_session_id = a.onboarding_session_id
                AND b.scene = 'onboarding'
                AND b.step_name = 'onboarding_socials_entered'
            )
            OR NOT EXISTS (
              SELECT 1
              FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action` c
              WHERE c.onboarding_session_id = a.onboarding_session_id
                AND c.scene = 'onboarding'
                AND c.step_name IN ('onboarding_headshot_entered','onboarding_headshot_auto_skipped','onboarding_headshot_manual_skipped')
            )
          )
        )
        QUALIFY
          CASE
            WHEN a.utm_regintent = 'aiecho' THEN TRUE
            ELSE ROW_NUMBER() OVER (PARTITION BY a.onboarding_session_id, step_name_canonical ORDER BY a.`timestamp`) = 1
          END = TRUE
    )
    SELECT
        oe.`timestamp`,
        COALESCE(oe.user_id, s.user_id) AS user_id,
        oe.utm_regintent,
        oe.onboarding_session_id,
        oe.step_name_canonical AS step_name,
        oe.step_variant,
        oe.utm_campaign,
        oe.context_user_agent,
        oe.device_category,
        t3.email AS user_email,
        t4.store_id AS sign_up_user_id,
        t4.created_at AS sign_up_store_created_at,
        t5.url_code AS sign_up_url_code,
        t5.username AS sign_up_user_username,
        t3.email AS sign_up_user_email,
        CASE
          WHEN oe.utm_campaign IS NOT NULL THEN 'marketing_campaign'
          WHEN oe.user_id IS NULL THEN 'event_not_fired'
          ELSE 'organic_walk-in'
        END AS acquisition_source,
        CASE oe.step_name_canonical
          WHEN 'onboarding_ai_echo_prompt_entered' THEN 4
          WHEN 'onboarding_socials_entered' THEN 5
          WHEN 'onboarding_headshot_entered' THEN 6
          WHEN 'onboarding_niche_entered' THEN 7
          WHEN 'onboarding_intent_entered' THEN 8
          WHEN 'onboarding_preview_shown' THEN 9
          WHEN 'onboarding_url_confirmed' THEN 10
          WHEN 'onboarding_contact_info_submitted' THEN 11
          WHEN 'onboarding_auth_verified' THEN 12
          WHEN 'onboarding_complete' THEN 13
        END AS step_ordinality,
        CASE oe.step_name_canonical
          WHEN 'onboarding_ai_echo_prompt_entered' THEN 'AI Echo Prompt Entered'
          WHEN 'onboarding_socials_entered' THEN 'Social Accounts Entered'
          WHEN 'onboarding_headshot_entered' THEN 'Headshot Step Completed'
          WHEN 'onboarding_niche_entered' THEN 'Niche Selection Completed'
          WHEN 'onboarding_intent_entered' THEN 'Intent Selected'
          WHEN 'onboarding_preview_shown' THEN 'Preview Shown'
          WHEN 'onboarding_url_confirmed' THEN 'URL Confirmed'
          WHEN 'onboarding_contact_info_submitted' THEN 'Contact Information Submitted'
          WHEN 'onboarding_auth_verified' THEN 'Account Verified'
          WHEN 'onboarding_complete' THEN 'Onboarding Completed'
        END AS bar_name,
        CASE oe.step_name_canonical
          WHEN 'onboarding_headshot_entered' THEN TRUE
          WHEN 'onboarding_niche_entered' THEN TRUE
          WHEN 'onboarding_auth_verified' THEN TRUE
          ELSE FALSE
        END AS is_combined_step,
        oe.business_type
      FROM stores s
      LEFT JOIN onboarding_events oe ON oe.user_id = s.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` t2 ON t2.user_id = COALESCE(oe.user_id, s.user_id)
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` t3 ON t3.user_id = COALESCE(oe.user_id, s.user_id)
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_stores` t4 ON t4.store_id = COALESCE(oe.user_id, s.user_id)
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` t5 ON t5.user_id = COALESCE(oe.user_id, s.user_id)
        AND t5.user_type IN ('seller', 'verifiedSeller')
        AND t5.apps_pop_store = TRUE
      WHERE (t3.email IS NULL OR (
        LOWER(t3.email) NOT LIKE '%@test.com'
        AND LOWER(t3.email) NOT LIKE '%@example.com'
        AND LOWER(t3.email) NOT LIKE '%@popshoplive.com'
        AND LOWER(t3.email) NOT LIKE '%@commentsold.com'
      ));;
  }

  # --- Dimensions ---
  dimension: step_name {
    type: string
    sql: ${TABLE}.step_name ;;
    description: "Canonical key (reporting_step_key) for the funnel step. Use for grouping; order by step_ordinality."
  }

  dimension: step_variant {
    type: string
    sql: ${TABLE}.step_variant ;;
    description: "Segment label for combined steps: completed | auto_skipped | manual_skipped | otp_verified | email_login_verified."
  }

  dimension: step_ordinality {
    type: number
    sql: ${TABLE}.step_ordinality ;;
    description: "Funnel order 1-13. Sort by this for correct step order."
  }

  dimension: acquisition_source {
    type: string
    sql: ${TABLE}.acquisition_source ;;
  }

  dimension: bar_name {
    type: string
    sql: ${TABLE}.bar_name ;;
    description: "Display-friendly funnel bar label. Use for chart axis labels; order by step_ordinality."
  }

  dimension: is_combined_step {
    type: yesno
    sql: ${TABLE}.is_combined_step ;;
    description: "TRUE for steps 6 (Headshot), 7 (Niche), 12 (Auth Verified) which combine multiple events with segment coloring."
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: utm_regintent {
    type: string
    sql: ${TABLE}.utm_regintent ;;
  }

  dimension: onboarding_session_id {
    type: string
    sql: ${TABLE}.onboarding_session_id ;;
  }

  dimension: business_type {
    type: string
    sql: ${TABLE}.business_type ;;
  }

  dimension: utm_campaign {
    type: string
    sql: ${TABLE}.utm_campaign ;;
  }

  dimension: context_user_agent {
    type: string
    sql: ${TABLE}.context_user_agent ;;
  }

  dimension: device_category {
    type: string
    sql: ${TABLE}.device_category ;;
  }

  dimension: user_email {
    type: string
    sql: ${TABLE}.user_email ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
    timeframes: [time, date, week, month, quarter, year]
  }

  dimension: sign_up_user_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.sign_up_user_id ;;
  }

  dimension_group: sign_up_store_created_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.sign_up_store_created_at ;;
  }

  dimension: sign_up_user_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.sign_up_url_code ;;
  }

  dimension: sign_up_user_email {
    type: string
    sql: ${TABLE}.sign_up_user_email ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
    description: "Event count per step. Use count_distinct_sessions for unique sessions (funnel)."
  }

  measure: count_onboarding_complete {
    type: count
    filters: [step_name: "onboarding_complete"]
    drill_fields: [onboarding_details*]
  }

  measure: count_distinct_sessions {
    type: count_distinct
    sql: ${onboarding_session_id} ;;
    value_format_name: decimal_0
    drill_fields: [detail*]
    description: "Unique onboarding sessions per step. Use for funnel; pre-signup steps typically have NULL user_id."
  }

  set: onboarding_details {
    fields: [
      sign_up_store_created_at_time,
      sign_up_user_id,
      sign_up_user_url,
      sign_up_user_email,
      acquisition_source,
      utm_campaign,
      utm_regintent,
    ]
  }

  set: detail {
    fields: [
      step_name,
      bar_name,
      step_variant,
      step_ordinality,
      acquisition_source,
      is_combined_step,
      user_id,
      utm_regintent,
      onboarding_session_id,
      business_type,
      timestamp_time,
      utm_campaign,
      context_user_agent,
      device_category
    ]
  }
}

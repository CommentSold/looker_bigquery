view: onboarding_funnel {
  filter: date_range {
    type: date
    description: "Filter events by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql: SELECT
        t1.timestamp,
        t1.user_id,
        t1.utm_regintent,
        t1.onboarding_session_id,
        t1.step_name_canonical AS step_name,
        t1.step_variant,
        t2.first_name,
        t3.last_name,
        t3.email,
        t4.phone_number,
        t1.instagram_handle,
        t1.instagram_followers,
        t1.tiktok_handle,
        t1.tiktok_followers,
        t1.utm_campaign,
        t1.context_user_agent,
        t1.device_category,
        CASE t1.step_name_canonical
          WHEN 'onboarding_started' THEN 1
          WHEN 'onboarding_intro_video_seen' THEN 2
          WHEN 'onboarding_ai_echo_intro_seen' THEN 3
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
        CASE t1.step_name_canonical
          WHEN 'onboarding_started' THEN 'Onboarding Started'
          WHEN 'onboarding_intro_video_seen' THEN 'Intro Video Seen'
          WHEN 'onboarding_ai_echo_intro_seen' THEN 'AI Echo Introduction'
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
        CASE t1.step_name_canonical
          WHEN 'onboarding_headshot_entered' THEN TRUE
          WHEN 'onboarding_niche_entered' THEN TRUE
          WHEN 'onboarding_auth_verified' THEN TRUE
          ELSE FALSE
        END AS is_combined_step,
        t1.business_type
      FROM (
        SELECT
          a.`timestamp`,
          a.user_id,
          a.utm_regintent,
          a.onboarding_session_id,
          a.tiktok_handle,
          a.instagram_handle,
          a.instagram_followers,
          a.tiktok_followers,
          a.utm_campaign,
          a.context_user_agent,
          CASE
            WHEN REGEXP_CONTAINS(LOWER(a.context_user_agent), r'(bot|crawler|spider|crawl|slurp|googlebot|bingpreview|facebookexternalhit|twitterbot|linkedinbot|whatsapp|telegrambot|discordbot)') THEN 'BOT'
            WHEN REGEXP_CONTAINS(LOWER(a.context_user_agent), r'wv') THEN 'WEBVIEW'
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
            'onboarding_started',
            'onboarding_intro_video_seen',
            'onboarding_ai_echo_intro_seen',
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
          QUALIFY ROW_NUMBER() OVER (PARTITION BY a.onboarding_session_id, step_name_canonical ORDER BY a.`timestamp`) = 1
      ) AS t1
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` t2 ON t2.user_id = t1.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` t3 ON t3.user_id = t1.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_users` t4 ON t4.user_id = t1.user_id
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

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: phone_number {
    type: string
    sql: ${TABLE}.phone_number ;;
  }

  dimension: instagram_handle {
    type: string
    sql: ${TABLE}.instagram_handle ;;
  }

  dimension: instagram_followers {
    type: string
    sql: ${TABLE}.instagram_followers ;;
  }

  dimension: tiktok_handle {
    type: string
    sql: ${TABLE}.tiktok_handle ;;
  }

  dimension: tiktok_followers {
    type: string
    sql: ${TABLE}.tiktok_followers ;;
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

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
    timeframes: [time, date, week, month, quarter, year]
  }

  measure: count {
    type: count
    drill_fields: [detail*]
    description: "Event count per step. Use count_distinct_sessions for unique sessions (funnel)."
  }

  measure: count_distinct_sessions {
    type: count_distinct
    sql: ${onboarding_session_id} ;;
    value_format_name: decimal_0
    drill_fields: [detail*]
    description: "Unique onboarding sessions per step. Use for funnel; pre-signup steps typically have NULL user_id."
  }

  set: detail {
    fields: [
      step_name,
      bar_name,
      step_variant,
      step_ordinality,
      is_combined_step,
      user_id,
      utm_regintent,
      onboarding_session_id,
      business_type,
      timestamp_time,
      first_name,
      last_name,
      email,
      phone_number,
      instagram_handle,
      instagram_followers,
      tiktok_handle,
      tiktok_followers,
      utm_campaign,
      context_user_agent,
      device_category
    ]
  }
}

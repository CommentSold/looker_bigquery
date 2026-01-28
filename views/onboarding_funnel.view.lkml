view: onboarding_funnel {
  filter: start_date {
    type: date
    description: "Filter events from this date (inclusive). Optional."
  }

  filter: end_date {
    type: date
    description: "Filter events through this date (inclusive). Optional."
  }

  filter: filter_utm_regintent {
    type: string
    description: "Filter by utm_regintent. Optional."
  }

  filter: filter_onboarding_session_id {
    type: string
    description: "Filter by onboarding_session_id. Optional."
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
          `timestamp`,
          user_id,
          utm_regintent,
          onboarding_session_id,
          tiktok_handle,
          instagram_handle,
          instagram_followers,
          tiktok_followers,
          CASE
            WHEN step_name IN ('onboarding_headshot_entered','onboarding_headshot_auto_skipped','onboarding_headshot_manual_skipped') THEN 'onboarding_headshot_entered'
            WHEN step_name IN ('onboarding_niche_entered','onboarding_niche_auto_skipped') THEN 'onboarding_niche_entered'
            WHEN step_name IN ('onboarding_user_otp_verified','onboarding_email_login_verified') THEN 'onboarding_auth_verified'
            ELSE step_name
          END AS step_name_canonical,
          CASE
            WHEN step_name = 'onboarding_headshot_auto_skipped' THEN 'auto_skipped'
            WHEN step_name = 'onboarding_headshot_manual_skipped' THEN 'manual_skipped'
            WHEN step_name = 'onboarding_niche_auto_skipped' THEN 'auto_skipped'
            WHEN step_name = 'onboarding_user_otp_verified' THEN 'otp_verified'
            WHEN step_name = 'onboarding_email_login_verified' THEN 'email_login_verified'
            ELSE 'completed'
          END AS step_variant,
          business_type
        FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
        WHERE scene = 'onboarding'
          AND step_name IN (
            'onboarding_started',
            'onboarding_intro_video_seen',
            'onboarding_ai_echo_intro_seen',
            'onboarding_ai_echo_prompt_entered',
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
          AND {% condition start_date %} `timestamp` {% endcondition %}
          AND {% condition end_date %} `timestamp` {% endcondition %}
          AND {% condition filter_utm_regintent %} utm_regintent {% endcondition %}
          AND {% condition filter_onboarding_session_id %} onboarding_session_id {% endcondition %}
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

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
    timeframes: [time, date, week, month, quarter, year]
  }

  measure: count {
    type: count
    drill_fields: [detail*]
    description: "Event count per step. Use count_distinct_users for unique users when a user can emit multiple events per step."
  }

  measure: count_distinct_users {
    type: count_distinct
    sql: ${user_id} ;;
    value_format_name: decimal_0
    drill_fields: [detail*]
    description: "Unique user count per step. Use instead of count when measuring users (not raw events)."
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
    ]
  }
}

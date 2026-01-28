view: onboarding_funnel {

  # =====================
  # PARAMETERS (Dashboard Filters)
  # =====================
  parameter: start_date {
    type: date
    description: "Filter events from this date (inclusive)."
  }

  parameter: end_date {
    type: date
    description: "Filter events through this date (inclusive)."
  }

  parameter: filter_utm_regintent {
    type: string
    description: "Filter by utm_regintent (e.g. generic, paid, creator)."
  }

  parameter: filter_onboarding_session_id {
    type: string
    description: "Filter by a single onboarding_session_id (debugging)."
  }

  # =====================
  # DERIVED TABLE (CANONICAL FUNNEL EVENTS)
  # =====================
  derived_table: {
    sql: WITH base_events AS (
        SELECT
          `timestamp`,
          user_id,
          utm_regintent,
          onboarding_session_id,
          tiktok_handle,
          instagram_handle,
          instagram_followers,
          tiktok_followers,
          business_type,

      -- Canonical step name
      CASE
      WHEN step_name IN ('onboarding_headshot_entered','onboarding_headshot_auto_skipped','onboarding_headshot_manual_skipped') THEN 'onboarding_headshot_entered'
      WHEN step_name IN ('onboarding_niche_entered','onboarding_niche_auto_skipped') THEN 'onboarding_niche_entered'
      WHEN step_name IN ('onboarding_user_otp_verified','onboarding_email_login_verified') THEN 'onboarding_auth_verified'
      ELSE step_name
      END AS step_name_canonical,

      -- Variant classification for combined steps
      CASE
      WHEN step_name = 'onboarding_headshot_auto_skipped' THEN 'auto_skipped'
      WHEN step_name = 'onboarding_headshot_manual_skipped' THEN 'manual_skipped'
      WHEN step_name = 'onboarding_niche_auto_skipped' THEN 'auto_skipped'
      WHEN step_name = 'onboarding_user_otp_verified' THEN 'otp_verified'
      WHEN step_name = 'onboarding_email_login_verified' THEN 'email_login_verified'
      ELSE 'completed'
      END AS step_variant
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
      )

      SELECT
      b.*,

      -- Step order
      CASE step_name_canonical
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

      -- Display label
      CASE step_name_canonical
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

      -- Combined-step flag
      step_name_canonical IN (
      'onboarding_headshot_entered',
      'onboarding_niche_entered',
      'onboarding_auth_verified'
      ) AS is_combined_step

      FROM base_events b
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` p ON p.user_id = b.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pp ON pp.user_id = b.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_users` u ON u.user_id = b.user_id
      WHERE pp.email IS NULL OR (
        LOWER(pp.email) NOT LIKE '%@test.com'
        AND LOWER(pp.email) NOT LIKE '%@example.com'
        AND LOWER(pp.email) NOT LIKE '%@popshoplive.com'
        AND LOWER(pp.email) NOT LIKE '%@commentsold.com'
      );;
  }

  # =====================
  # DIMENSIONS
  # =====================
  dimension: step_name { sql: ${TABLE}.step_name_canonical ;; }
  dimension: bar_name { sql: ${TABLE}.bar_name ;; order_by_field: step_ordinality }
  dimension: step_variant { sql: ${TABLE}.step_variant ;; }
  dimension: step_ordinality { type: number sql: ${TABLE}.step_ordinality ;; }
  dimension: is_combined_step { type: yesno sql: ${TABLE}.is_combined_step ;; }

  dimension: user_id { sql: ${TABLE}.user_id ;; }
  dimension: utm_regintent { sql: ${TABLE}.utm_regintent ;; }
  dimension: onboarding_session_id { sql: ${TABLE}.onboarding_session_id ;; }
  dimension: business_type { sql: ${TABLE}.business_type ;; }

  dimension_group: timestamp { type: time sql: ${TABLE}.timestamp ;; timeframes: [date, week, month] }

  # =====================
  # MEASURES (FUNNEL CORE)
  # =====================
  measure: users {
    type: count_distinct
    sql: ${user_id} ;;
    value_format_name: decimal_0
    description: "Distinct users reaching this step"
  }

  measure: users_prev_step {
    type: number
    sql: LAG(${users}) OVER (ORDER BY ${step_ordinality}) ;;
    value_format_name: decimal_0
    description: "Users at previous funnel step"
  }

  measure: step_conversion_rate {
    type: number
    sql: SAFE_DIVIDE(${users}, ${users_prev_step}) ;;
    value_format_name: percent_1
    description: "Conversion from previous step"
  }

  measure: funnel_conversion_rate {
    type: number
    sql: SAFE_DIVIDE(${users}, FIRST_VALUE(${users}) OVER (ORDER BY ${step_ordinality})) ;;
    value_format_name: percent_1
    description: "Conversion vs onboarding start"
  }
}

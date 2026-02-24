view: onboarding_funnel {
  filter: date_range {
    type: date
    description: "Filter events by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql: WITH onboarding_events AS (
      SELECT
        a.`timestamp`,
        a.user_id,
        a.utm_regintent,
        a.onboarding_session_id,
        a.context_campaign_campaign AS marketing_campaign,
        a.context_user_agent,
        a.scene,
        a.step_name,
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
          /*'onboarding_started',
            'onboarding_intro_video_seen',
            'onboarding_ai_echo_intro_seen',*/
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
      pprof.email as user_email,
      st.store_id AS sign_up_user_id,
      st.created_at AS sign_up_store_created_at,
      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email AS sign_up_user_email,
      oe.marketing_campaign,
      oe.utm_regintent,
      oe.business_type,
      oe.`timestamp`,
      oe.step_name,
      CASE
        WHEN oe.marketing_campaign IS NOT NULL THEN 'marketing_campaign'
        WHEN oe.user_id IS NULL THEN 'event_not_fired'
        ELSE 'organic_walk-in'
      END AS acquisition_source
    FROM `popshoplive-26f81.dbt_popshop.dim_profiles` prof
    LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_stores` st ON st.store_id = prof.user_id
    LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof ON pprof.user_id = prof.user_id
    LEFT JOIN onboarding_events oe
      ON oe.user_id = prof.user_id
      AND (oe.scene = 'onboarding' OR oe.scene IS NULL)
      AND (oe.step_name = 'onboarding_complete' OR oe.step_name IS NULL)
    WHERE
      user_type IN ('seller', 'verifiedSeller')
      AND apps_pop_store = TRUE
      AND {% condition date_range %} prof.created_at {% endcondition %}
      AND (pprof.email IS NULL OR (
        LOWER(pprof.email) NOT LIKE '%@test.com'
        AND LOWER(pprof.email) NOT LIKE '%@example.com'
        AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
        AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      ))
    ORDER BY acquisition_source DESC;;
  }

  dimension_group: sign_up_store_created_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.sign_up_store_created_at ;;
    timeframes: [time, date, week, month, quarter, year]
  }

  dimension: onboarding_session_id {
    type: string
    sql: ${TABLE}.onboarding_session_id ;;
  }

  dimension: step_name {
    type: string
    sql: ${TABLE}.step_name ;;
    description: "Canonical key (reporting_step_key) for the funnel step. Use for grouping; order by step_ordinality."
  }

  dimension: sign_up_user_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.sign_up_user_id ;;
  }

  dimension: sign_up_user_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.sign_up_url_code ;;
  }

  dimension: sign_up_user_email {
    type: string
    sql: ${TABLE}.sign_up_user_email ;;
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

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
    timeframes: [time, date, week, month, quarter, year]
  }

  measure: count_onboarding_complete {
    type: count
    filters: [step_name: "onboarding_complete"]
    drill_fields: [onboarding_details*]
  }

  set: onboarding_details {
    fields: [
      sign_up_store_created_at_time,
      sign_up_user_id,
      sign_up_user_url,
      sign_up_user_email,
      acquisition_source,
      marketing_campaign,
      utm_regintent,
      business_type,
    ]
  }
}

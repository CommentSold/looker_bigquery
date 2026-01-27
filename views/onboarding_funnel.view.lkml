# onboarding_funnel.view.lkml
# Onboarding funnel: 9 canonical steps (Headshot/Niche rolled up) with step_variant for segment coloring.
# JIRA: utm_regintent, onboarding_session_id filters; step_name (x), count, step_variant (series).
# Chart: step_name on x-axis, count or count_distinct_users as measure, step_variant as series/pivot for colors.

view: onboarding_funnel {
  # Optional: start_date/end_date push into DT; utm_regintent and onboarding_session_id are filtered via dimensions in the explore.
  parameter: start_date {
    type: date
    description: "Filter events from this date (inclusive). Optional."
  }

  parameter: end_date {
    type: date
    description: "Filter events through this date (inclusive). Optional."
  }

  derived_table: {
    sql: SELECT
        base.timestamp,
        base.user_id,
        base.utm_regintent,
        base.onboarding_session_id,
        base.step_name_canonical AS step_name,
        base.step_variant,
        CASE base.step_name_canonical
          WHEN 'onboarding_started' THEN 1
          WHEN 'onboarding_intro_video_seen' THEN 2
          WHEN 'onboarding_ai_echo_intro_seen' THEN 3
          WHEN 'onboarding_ai_echo_prompt_entered' THEN 4
          WHEN 'onboarding_socials_entered' THEN 5
          WHEN 'onboarding_headshot_entered' THEN 6
          WHEN 'onboarding_niche_entered' THEN 7
          WHEN 'onboarding_intent_entered' THEN 8
          WHEN 'onboarding_preview_shown' THEN 9
        END AS step_ordinality,
        base.businessType
      FROM (
        SELECT
          `timestamp`,
          user_id,
          utm_regintent,
          onboarding_session_id,
          CASE
            WHEN step_name IN ('onboarding_headshot_entered','onboarding_headshot_auto_skipped','onboarding_headshot_manual_skipped') THEN 'onboarding_headshot_entered'
            WHEN step_name IN ('onboarding_niche_entered','onboarding_niche_auto_skipped') THEN 'onboarding_niche_entered'
            ELSE step_name
          END AS step_name_canonical,
          CASE
            WHEN step_name = 'onboarding_headshot_auto_skipped' THEN 'auto_skipped'
            WHEN step_name = 'onboarding_headshot_manual_skipped' THEN 'manual_skipped'
            WHEN step_name = 'onboarding_niche_auto_skipped' THEN 'auto_skipped'
            ELSE 'completed'
          END AS step_variant,
          CAST(NULL AS STRING) AS businessType
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
            'onboarding_preview_shown'
          )
          {% if start_date %} AND `timestamp` >= TIMESTAMP('{{ start_date }}') {% endif %}
          {% if end_date %} AND `timestamp` <= TIMESTAMP('{{ end_date }}') {% endif %}
      ) AS base
    ;;

  }

  # --- Dimensions ---
  dimension: step_name {
    type: string
    sql: ${TABLE}.step_name ;;
    description: "Canonical funnel step (Headshot/Niche rolled up). Use on x-axis; order by step_ordinality."
  }

  dimension: step_variant {
    type: string
    sql: ${TABLE}.step_variant ;;
    description: "completed | auto_skipped | manual_skipped. Use as series/pivot for segment coloring."
  }

  dimension: step_ordinality {
    type: number
    sql: ${TABLE}.step_ordinality ;;
    description: "Funnel order 1-9. Sort by this for correct step order."
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

  # Placeholder until businessType exists in popstore_onboarding_screen_action. Replace with real column when available.
  dimension: business_type {
    type: string
    sql: ${TABLE}.businessType ;;
    description: "Placeholder. Wire to businessType column in source when available."
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
    timeframes: [time, date, week, month, quarter, year]
  }

  # --- Measures ---
  # Raw event count; default for funnel.
  measure: count {
    type: count
    drill_fields: [detail*]
    description: "Event count per step. Use count_distinct_users for unique users when a user can emit multiple events per step."
  }

  # Optional (JIRA): unique users per step; prefer when multiple events per user per step.
  measure: count_distinct_users {
    type: count_distinct
    sql: ${user_id} ;;
    value_format_name: decimal_0
    drill_fields: [detail*]
    description: "Unique user count per step. Use instead of count when measuring users (not raw events)."
  }

  set: detail {
    fields: [
      timestamp_time,
      user_id,
      step_name,
      step_variant,
      step_ordinality,
      utm_regintent,
      onboarding_session_id,
      business_type
    ]
  }
}

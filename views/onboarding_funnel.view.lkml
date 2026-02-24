view: onboarding_funnel {

  filter: date_range {
    type: date
    description: "Filter events by date range."
  }

  derived_table: {
    sql:

    SELECT
      pprof.email AS user_email,
      st.store_id AS sign_up_user_id,
      st.created_at AS sign_up_store_created_at,
      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email AS sign_up_user_email,

      oe.user_id AS oe_user_id,
      oe.marketing_campaign,
      oe.utm_regintent,
      oe.business_type,
      oe.timestamp,
      oe.step_name,
      oe.onboarding_session_id,

      CASE
      WHEN oe.user_id IS NULL THEN 'event_not_fired'
      WHEN oe.marketing_campaign IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source

      FROM `popshoplive-26f81.dbt_popshop.dim_profiles` prof

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_stores` st
      ON st.store_id = prof.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = prof.user_id

      LEFT JOIN (
      SELECT
      context_campaign_campaign AS marketing_campaign,
      utm_regintent,
      business_type,
      timestamp,
      user_id,
      step_name,
      onboarding_session_id,
      scene
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      WHERE
      (scene = 'onboarding' OR scene IS NULL)
      AND (step_name = 'onboarding_complete' OR step_name IS NULL)
      AND {% condition date_range %} timestamp {% endcondition %}
      AND {% condition utm_regintent %} utm_regintent {% endcondition %}
      AND {% condition onboarding_session_id %} onboarding_session_id {% endcondition %}
      ) oe
      ON oe.user_id = prof.user_id

      WHERE
      user_type IN ('seller','verifiedSeller')
      AND apps_pop_store = TRUE
      AND {% condition date_range %} prof.created_at {% endcondition %}
      AND (
      pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      )
      )

      ;;
  }

  ################################
  # TIME DIMENSIONS
  ################################

  dimension_group: sign_up_store_created_at {
    type: time
    sql: ${TABLE}.sign_up_store_created_at ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
    timeframes: [date, week, month, quarter, year]
  }

  ################################
  # DIMENSIONS
  ################################

  dimension: sign_up_user_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.sign_up_user_id ;;
  }

  dimension: acquisition_source {
    type: string
    sql: ${TABLE}.acquisition_source ;;
  }

  dimension: step_name {
    type: string
    sql: ${TABLE}.step_name ;;
  }

  dimension: utm_regintent {
    type: string
    sql: ${TABLE}.utm_regintent ;;
  }

  dimension: onboarding_session_id {
    type: string
    sql: ${TABLE}.onboarding_session_id ;;
  }

  ################################
  # ✅ NEW CORRECT ACQUISITION MEASURE
  ################################

  measure: popstore_creations {
    label: "Popstore Creations"
    type: count_distinct
    sql: ${sign_up_user_id} ;;
  }

  ################################
  # EXISTING ONBOARDING MEASURE
  ################################

  measure: count_onboarding_complete {
    type: count
    filters: [step_name: "onboarding_complete"]
  }

}

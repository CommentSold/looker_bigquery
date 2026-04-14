view: qa_onboarding_funnel {
  filter: date_range {
    type: date
    description: "Filter events by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql: WITH onboarding_events AS (
      SELECT
        context_campaign_campaign AS marketing_campaign,
        utm_regintent,
        business_type,
        `timestamp`,
        user_id,
        scene,
        step_name,
        onboarding_session_id
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      WHERE (scene = 'onboarding' OR scene IS NULL)
        AND (step_name = 'onboarding_complete' OR step_name IS NULL)
        AND {% condition date_range %} `timestamp` {% endcondition %}
        AND {% condition utm_regintent %} utm_regintent {% endcondition %}
        AND {% condition onboarding_session_id %} onboarding_session_id {% endcondition %}
    ),
    marketing_capture AS (
      SELECT
      user_id,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_campaign') AS utm_campaign,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_source') AS utm_source,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent') AS utm_regintent
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
    )
    SELECT
      pprof.email as user_email,
      st.store_id AS sign_up_user_id,
      st.created_at AS sign_up_store_created_at,
      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email AS sign_up_user_email,
      oe.marketing_campaign,
      COALESCE(oe.utm_regintent, mc.utm_regintent) AS utm_regintent,
      oe.business_type,
      oe.`timestamp`,
      oe.step_name,
      oe.onboarding_session_id,
      CASE
      WHEN COALESCE(oe.marketing_campaign, mc.utm_campaign) IS NOT NULL
      THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL
      THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source
    FROM `popshoplive-26f81.dbt_popshop.dim_profiles` prof
    LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_stores` st ON st.store_id = prof.user_id
    LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof ON pprof.user_id = prof.user_id
    LEFT JOIN marketing_capture mc ON mc.user_id = prof.user_id
    LEFT JOIN onboarding_events oe ON oe.user_id = prof.user_id
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

  measure: count_onboard {
    type: count
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

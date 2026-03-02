view: creator_contest_action {
  filter: date_range {
    type: date
    description: "Filter events by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql: SELECT
        cca.anonymous_id,
        cca.user_id,
        cca.step_name,
        cca.business_type,
        cca.loaded_at,
        cca.`timestamp` as creator_contest_created_at,
        cca.stage,
        pprof.email as user_email,
        prof.username as user_name,
        prof.url_code
      FROM `popshoplive-26f81.popstore.popstore_creator_contest_action_view` as cca
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof ON pprof.user_id = cca.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof ON prof.user_id = cca.user_id
      WHERE cca.stage IN ('stage1', 'stage2', 'stage3', 'stage4')
        AND cca.step_name IN (
          'contest_stage1_entered',
          'contest_stage2_entered',
          'contest_stage3_entered',
          'contest_stage4_entered'
        )
        AND {% condition date_range %} cca.loaded_at {% endcondition %}
      ORDER BY cca.loaded_at DESC;;
  }

  dimension_group: creator_contest_created_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.creator_contest_created_at ;;
    timeframes: [time, date, week, month, quarter, year]
  }

  dimension_group: loaded_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.loaded_at ;;
    timeframes: [time, date, week, month, quarter, year]
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: step_name {
    type: string
    sql: ${TABLE}.step_name ;;
  }

  dimension: business_type {
    type: string
    sql: ${TABLE}.business_type ;;
  }

  dimension: stage {
    type: string
    sql: ${TABLE}.stage;;
  }

  dimension: user_email {
    type: string
    sql: ${TABLE}.user_email;;
  }

  dimension: user_name {
    type: string
    sql: ${TABLE}.user_name;;
  }

  dimension: url_code {
    type: string
    sql: ${TABLE}.url_code;;
  }

  measure: count_contest {
    type: count
    drill_fields: [creator_contest_details*]
  }

  set: creator_contest_details {
    fields: [
      creator_contest_created_at_time,
      loaded_at_time,
      anonymous_id,
      user_id,
      step_name,
      business_type,
      stage,
      user_name,
      user_email,
      url_code
    ]
  }
}

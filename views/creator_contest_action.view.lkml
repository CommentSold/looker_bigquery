view: creator_contest_action {
  filter: date_range {
    type: date
    description: "Filter events by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql: SELECT
        DATE_TRUNC(DATE(loaded_at, 'UTC'), WEEK(MONDAY)) AS week_start_utc,
        DATE_ADD(DATE_TRUNC(DATE(loaded_at, 'UTC'), WEEK(MONDAY)), INTERVAL 6 DAY) AS week_end_utc,
        stage,
        COUNT(*) AS total_events
      FROM `popshoplive-26f81.popstore.popstore_creator_contest_action_view`
      WHERE stage IN ('stage1', 'stage2', 'stage3', 'stage4')
        AND step_name IN (
          'contest_stage1_entered',
          'contest_stage2_entered',
          'contest_stage3_entered',
          'contest_stage4_entered'
        )
        AND {% condition date_range %} loaded_at {% endcondition %}
      GROUP BY
        week_start_utc,
        week_end_utc,
        stage
      ORDER BY
        week_start_utc DESC,
        stage;;
  }

  dimension: week_start_utc {
    type: string
    sql: ${TABLE}.week_start_utc ;;
  }

  dimension: week_end_utc {
    type: string
    sql: ${TABLE}.week_end_utc ;;
  }

  dimension: stage {
    type: string
    sql: ${TABLE}.stage;;
  }

  dimension: total_events {
    type: string
    sql: ${TABLE}.total_events ;;
  }

  set: onboarding_details {
    fields: [
      week_start_utc,
      week_end_utc,
      stage,
      total_events
    ]
  }
}

view: trial_report {
  filter: date_range {
    type: date
    description: "Filter events by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql: SELECT
      DATE(current_period_start) AS start_date,
      DATE(trial_end) AS end_date,
      DATE_TRUNC(DATE(current_period_start), MONTH) AS report_month,
      COUNT(*) AS total_trials,
      COUNT(CASE WHEN DATE(trial_end) <= CURRENT_DATE() THEN 1 END) AS total_ended_trials,
      COUNT(CASE WHEN DATE(trial_end) > CURRENT_DATE() THEN 1 END) AS total_active_trials
    FROM dbt_popshop.fact_seller_subscription
    WHERE
      trial_end IS NOT NULL
      AND {% condition date_range %} current_period_start {% endcondition %}
    GROUP BY 1, 2, 3
    ORDER BY start_date;;
  }

  dimension_group: start_date_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.start_date ;;
    timeframes: [time, date, week, month, quarter, year]
  }

  dimension_group: end_date_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.end_date ;;
    timeframes: [time, date, week, month, quarter, year]
  }

  dimension_group: report_month_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.report_month ;;
    timeframes: [month]
  }

  dimension: total_trials {
    type: number
    sql: ${TABLE}.total_trials ;;
  }

  dimension: total_active_trials {
    type: number
    sql: ${TABLE}.total_active_trials ;;
  }

  dimension: total_ended_trials {
    type: number
    sql: ${TABLE}.total_ended_trials ;;
  }

  measure: sum_total_trials {
    type: sum
    sql: ${TABLE}.total_trials ;;
    label: "Total Trials"
    drill_fields: [onboarding_details*]
  }

  measure: sum_total_active_trials {
    type: sum
    sql: ${TABLE}.total_active_trials ;;
    label: "Active Trials"
    drill_fields: [onboarding_details*]
  }

  measure: sum_total_ended_trials {
    type: sum
    sql: ${TABLE}.total_ended_trials ;;
    label: "Ended Trials"
    drill_fields: [onboarding_details*]
  }

  set: onboarding_details {
    fields: [
      start_date_at_date,
      end_date_at_date,
      report_month_at_month,
      total_trials,
      total_active_trials,
      total_ended_trials
    ]
  }
}

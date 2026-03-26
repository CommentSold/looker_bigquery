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

  measure: sum_total_trials {
    type: sum
    sql: ${TABLE}.total_trials ;;
    label: "Total Trials"
    drill_fields: [trial_detail.detail*]
  }

  measure: sum_total_active_trials {
    type: sum
    sql: ${TABLE}.total_active_trials ;;
    label: "Active Trials"
    drill_fields: [trial_detail.detail*]
  }

  measure: sum_total_ended_trials {
    type: sum
    sql: ${TABLE}.total_ended_trials ;;
    label: "Ended Trials"
    drill_fields: [trial_detail.detail*]
  }
}


view: trial_detail {
  derived_table: {
    sql:
      SELECT
        DATE_TRUNC(DATE(current_period_start), MONTH) AS report_month,
        operation,
        status,
        subscription_id,
        user_id,
        CASE
          WHEN discounted_price IS NULL THEN (price + tax_amount)
          ELSE discounted_price
        END AS price,
        JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
        JSON_EXTRACT_SCALAR(plan, '$.interval') AS plan_interval,
        CASE
          WHEN trial_end IS NULL THEN 'No trial'
          WHEN DATE(trial_end) < CURRENT_DATE() THEN 'Trial ended'
          ELSE 'Trialing'
        END AS trial_status,
        trial_end
      FROM dbt_popshop.fact_seller_subscription t1,
      UNNEST(t1.plans) AS plan
      WHERE trial_end IS NOT NULL;;
  }

  dimension_group: report_month_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.report_month ;;
    timeframes: [month]
    hidden: yes
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: operation {
    type: string
    sql: ${TABLE}.operation ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
    value_format_name: decimal_2
  }

  dimension: plan_name {
    type: string
    sql: ${TABLE}.plan_name ;;
  }

  dimension: plan_interval {
    type: string
    sql: ${TABLE}.plan_interval ;;
    label: "Interval"
  }

  dimension: trial_status {
    type: string
    sql: ${TABLE}.trial_status ;;
  }

  dimension_group: trial_end_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.trial_end ;;
    timeframes: [date]
  }

  set: detail {
    fields: [
      subscription_id,
      user_id,
      operation,
      status,
      price,
      plan_name,
      plan_interval,
      trial_status,
      trial_end_at_date
    ]
  }
}

view: trial_report {
  filter: date_range {
    type: date
    description: "Filter events by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql:
      WITH base AS (
        SELECT
          id,
          DATE(current_period_start) AS start_date,
          DATE(trial_end) AS end_date,
          COUNT(*) AS total_trials,
          COUNT(CASE WHEN DATE(trial_end) <= CURRENT_DATE() THEN 1 END) AS total_ended_trials,
          COUNT(CASE WHEN DATE(trial_end) > CURRENT_DATE() THEN 1 END) AS total_active_trials
        FROM dbt_popshop.fact_seller_subscription
        WHERE
          trial_end IS NOT NULL
          AND {% condition date_range %} TIMESTAMP(current_period_start) {% endcondition %}
        GROUP BY 1, 2, 3
      ),
      detail AS (
        SELECT
          t1.id,
          t1.status,
          t1.subscription_id,
          t1.user_id,
          CASE
            WHEN t1.discounted_price IS NULL THEN (t1.price + t1.tax_amount)
            ELSE t1.discounted_price
          END AS price,
          JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
          JSON_EXTRACT_SCALAR(plan, '$.interval') AS plan_interval,
          CASE
            WHEN t1.trial_end IS NULL THEN 'No trial'
            WHEN DATE(t1.trial_end) < CURRENT_DATE() THEN 'Trial ended'
            ELSE 'Trialing'
          END AS trial_status,
          t1.trial_end
        FROM dbt_popshop.fact_seller_subscription t1,
        UNNEST(t1.plans) AS plan
      )
      SELECT
        b.id,
        b.start_date,
        b.end_date,
        b.total_trials,
        b.total_ended_trials,
        b.total_active_trials,
        d.status,
        d.subscription_id,
        d.user_id,
        d.price,
        d.plan_name,
        d.plan_interval,
        d.trial_status,
        d.trial_end
      FROM base b
      LEFT JOIN detail d ON b.id = d.id;;
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
    primary_key: yes
    hidden: yes
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

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
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
      subscription_id,
      user_id,
      status,
      price,
      plan_name,
      plan_interval,
      trial_status,
      trial_end_at_date,
      start_date_at_date,
      end_date_at_date
    ]
  }
}

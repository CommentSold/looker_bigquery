view: trial_report {
  filter: date_range {
    type: date
    description: "Filter events by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql:
      SELECT
        t1.id,
        t1.current_period_start AS start_date,
        t1.trial_end AS end_date,
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
        END AS trial_status
      FROM dbt_popshop.fact_seller_subscription t1,
      UNNEST(t1.plans) AS plan
      WHERE
        t1.trial_end IS NOT NULL
        AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
        AND {% condition date_range %} TIMESTAMP(t1.current_period_start) {% endcondition %}
      ORDER BY t1.created_at DESC;;
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
    timeframes: [date, week, month, quarter, year]
  }

  dimension_group: end_date_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.end_date ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
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

  measure: sum_total_trials {
    type: count_distinct
    sql: ${id} ;;
    label: "Total Trials"
    drill_fields: [onboarding_details*]
  }

  measure: sum_total_active_trials {
    type: count_distinct
    sql: CASE WHEN ${trial_status} = 'Trialing' THEN ${id} END ;;
    label: "Active Trials"
    drill_fields: [onboarding_details*]
  }

  measure: sum_total_ended_trials {
    type: count_distinct
    sql: CASE WHEN ${trial_status} = 'Trial ended' THEN ${id} END ;;
    label: "Ended Trials"
    drill_fields: [onboarding_details*]
  }

  set: onboarding_details {
    fields: [
      user_id,
      subscription_id,
      plan_name,
      plan_interval,
      price,
      trial_status,
      start_date_at_date,
      end_date_at_date
    ]
  }
}

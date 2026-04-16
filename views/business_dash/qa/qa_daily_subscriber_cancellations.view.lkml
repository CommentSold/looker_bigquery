view: qa_daily_subscriber_cancellations {
  derived_table: {
    sql:
      WITH cancellations AS (
        SELECT
          t1.subscription_id,
          t1.user_id,
          DATE(t1.initial_start_date) AS trial_start_date,
          DATE(COALESCE(t1.cancelled_at, t1.cancellation_applied_at)) AS cancellation_date,
          DATE(t1.trial_end) AS trial_end_date,
          DATE_DIFF(DATE(COALESCE(t1.cancelled_at, t1.cancellation_applied_at)), DATE(t1.trial_end), DAY) AS days_as_subscriber,
          t1.status,
          t1.cancel_at_period_end,
          COALESCE(t1.discounted_price, t1.price + t1.tax_amount) AS price,
          JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
          JSON_EXTRACT_SCALAR(plan, '$.interval') AS plan_interval,
          -- Profile fields
          prof.url_code AS sign_up_url_code,
          prof.username AS sign_up_user_username,
          pprof.email AS sign_up_user_email,
          -- Onboarding/Marketing fields
          oe.context_campaign_campaign AS marketing_campaign,
          oe.utm_regintent,
          oe.business_type,
          CASE
            WHEN oe.user_id IS NULL THEN 'event_not_fired'
            WHEN oe.context_campaign_campaign IS NOT NULL THEN 'marketing_campaign'
            ELSE 'organic_walk-in'
          END AS acquisition_source
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
        UNNEST(t1.plans) AS plan
        INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof ON prof.user_id = t1.user_id
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof ON pprof.user_id = t1.user_id
        LEFT JOIN (
          SELECT *
          FROM (
            SELECT *,
              ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp DESC) AS rn
            FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
          )
          WHERE rn = 1
        ) oe ON oe.user_id = t1.user_id
        WHERE
          JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
          AND (t1.cancelled_at IS NOT NULL OR t1.cancellation_applied_at IS NOT NULL)
          -- Exclude trial cancellations: only count cancellations after trial ended
          AND (
            t1.trial_end IS NULL
            OR COALESCE(t1.cancelled_at, t1.cancellation_applied_at) > t1.trial_end
          )
          AND prof.apps_pop_store = TRUE
          AND prof.user_type IN ('seller', 'verifiedSeller')
          AND (pprof.email IS NULL OR NOT REGEXP_CONTAINS(LOWER(pprof.email), r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com)$'))
      )

      SELECT * FROM cancellations
      ;;
  }

  # ——— Primary Key ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: ${TABLE}.subscription_id ;;
  }

  # ——— Core Identifiers ———

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
    label: "Subscription ID"
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
    label: "User ID"
  }

  # ——— User Profile Dimensions ———

  dimension: sign_up_user_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.sign_up_url_code ;;
    label: "Pop Store URL"
  }

  dimension: sign_up_user_username {
    type: string
    sql: ${TABLE}.sign_up_user_username ;;
    label: "Username"
  }

  dimension: sign_up_user_email {
    type: string
    sql: ${TABLE}.sign_up_user_email ;;
    label: "Email"
  }

  # ——— Date Dimensions ———

  dimension_group: trial_start {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.trial_start_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Trial Start"
    description: "Trial start date cohort - matches with Daily Trial Conversions graph"
  }

  dimension_group: cancellation {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.cancellation_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Cancellation"
  }

  dimension_group: trial_end {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.trial_end_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Trial End"
  }

  dimension: days_as_subscriber {
    type: number
    sql: ${TABLE}.days_as_subscriber ;;
    label: "Days as Subscriber"
    description: "Number of days between trial end and cancellation"
  }

  # ——— Subscription Dimensions ———

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    label: "Status"
  }

  dimension: cancel_at_period_end {
    type: yesno
    sql: ${TABLE}.cancel_at_period_end ;;
    label: "Cancel at Period End"
  }

  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
    label: "Price"
    value_format_name: decimal_2
  }

  dimension: plan_name {
    type: string
    sql: ${TABLE}.plan_name ;;
    label: "Plan Name"
  }

  dimension: plan_interval {
    type: string
    sql: ${TABLE}.plan_interval ;;
    label: "Plan Interval"
  }

  # ——— Marketing/Acquisition Dimensions ———

  dimension: marketing_campaign {
    type: string
    sql: ${TABLE}.marketing_campaign ;;
    label: "Marketing Campaign"
  }

  dimension: utm_regintent {
    type: string
    sql: ${TABLE}.utm_regintent ;;
    label: "UTM Reg Intent"
  }

  dimension: business_type {
    type: string
    sql: ${TABLE}.business_type ;;
    label: "Business Type"
  }

  dimension: acquisition_source {
    type: string
    sql: ${TABLE}.acquisition_source ;;
    label: "Acquisition Source"
  }

  # ——— Measures ———

  measure: total_cancellations {
    type: count
    label: "Total Cancellations"
    description: "Total subscription cancellations by trial start date cohort"
    drill_fields: [drilldown_details*]
  }

  measure: total_monthly_plan_cancellations {
    type: count
    filters: [plan_interval: "month"]
    label: "Monthly Plan Cancellations"
    description: "Cancellations from monthly billing plans"
    drill_fields: [drilldown_details*]
  }

  measure: total_yearly_plan_cancellations {
    type: count
    filters: [plan_interval: "year"]
    label: "Yearly Plan Cancellations"
    description: "Cancellations from yearly billing plans"
    drill_fields: [drilldown_details*]
  }

  measure: count_users {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Unique Users Cancelled"
    drill_fields: [drilldown_details*]
  }

  measure: avg_days_as_subscriber {
    type: average
    sql: ${TABLE}.days_as_subscriber ;;
    label: "Avg Days as Subscriber"
    description: "Average days between trial end and cancellation"
    value_format_name: decimal_1
  }

  measure: avg_price {
    type: average
    sql: ${TABLE}.price ;;
    label: "Avg Price"
    value_format_name: decimal_2
  }

  # ——— Drill Set ———

  set: drilldown_details {
    fields: [
      user_id,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      subscription_id,
      plan_name,
      plan_interval,
      price,
      status,
      trial_start_date,
      trial_end_date,
      cancellation_date,
      days_as_subscriber,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type
    ]
  }
}

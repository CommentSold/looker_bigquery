view: qa_trial_cancellations {
  derived_table: {
    sql:
      SELECT
        fs.subscription_id,
        fs.user_id,
        DATE(COALESCE(fs.cancelled_at, fs.updated_at)) AS cancellation_date,
        fs.initial_start_date,

      CASE
      WHEN DATE(fs.initial_start_date) = DATE(COALESCE(fs.cancelled_at, fs.updated_at))
      THEN 'same_day'
      ELSE 'later'
      END AS cancellation_timing,

      -- Drilldown fields
      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email AS sign_up_user_email,
      oe.context_campaign_campaign AS marketing_campaign,
      oe.utm_regintent,
      oe.business_type,

      COALESCE(fs.discounted_price, fs.price + fs.tax_amount) AS price,
      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval') AS plan_interval,
      fs.initial_start_date AS trial_starts,
      fs.trial_end AS trial_ends,

      COALESCE(
      CASE
      WHEN fs.cancellation_applied_at IS NOT NULL
      AND fs.cancellation_applied_at < fs.trial_end
      THEN fs.cancellation_applied_at
      END,
      fs.trial_end
      ) AS effective_trial_end,

      CASE
      WHEN fs.trial_end IS NULL THEN 'No trial'
      WHEN DATE(COALESCE(
      CASE
      WHEN fs.cancellation_applied_at IS NOT NULL
      AND fs.cancellation_applied_at < fs.trial_end
      THEN fs.cancellation_applied_at
      END,
      fs.trial_end
      )) <= CURRENT_DATE() THEN 'Ended'
      ELSE 'Started'
      END AS trial_status,

      fs.status AS subscription_status,

      CASE
      WHEN oe.user_id IS NULL THEN 'event_not_fired'
      WHEN oe.context_campaign_campaign IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source

      FROM `dbt_popshop.fact_seller_subscription` fs

      CROSS JOIN UNNEST(fs.plans) AS plan

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = fs.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = fs.user_id

      LEFT JOIN (
      SELECT *
      FROM (
      SELECT *,
      ROW_NUMBER() OVER (PARTITION BY user_id) AS rn
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      )
      WHERE rn = 1
      ) oe
      ON oe.user_id = fs.user_id

      WHERE
      fs.trial_end IS NOT NULL
      AND fs.status IN ('canceled', 'unpaid')
      AND (
      fs.cancelled_at <= fs.trial_end
      OR fs.current_period_end = fs.trial_end
      OR (fs.status = 'unpaid' AND fs.cancelled_at IS NULL AND fs.trial_end <= CURRENT_TIMESTAMP())
      )
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND (pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      ))
      {% if date_range._is_filtered %}
      AND {% condition date_range %} TIMESTAMP(fs.initial_start_date) {% endcondition %}
      {% endif %}
      ;;
  }

  # ——— Filters ———

  filter: date_range {
    type: date
    description: "Filter by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  # ——— Dimensions ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension_group: cancellation {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.cancellation_date ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension: cancellation_timing {
    type: string
    sql: ${TABLE}.cancellation_timing ;;
    description: "Whether cancellation happened on the same day as trial start (same_day) or later. Pivot on this for stacked bars."
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
    description: "Subscription status: canceled or unpaid"
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: sign_up_user_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.sign_up_url_code ;;
  }

  dimension: sign_up_user_username {
    type: string
    sql: ${TABLE}.sign_up_user_username ;;
  }

  dimension: sign_up_user_email {
    type: string
    sql: ${TABLE}.sign_up_user_email ;;
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

  dimension: acquisition_source {
    type: string
    sql: ${TABLE}.acquisition_source ;;
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

  dimension_group: trial_starts_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.trial_starts ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension_group: trial_ends_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.trial_ends ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension_group: effective_trial_ends_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.effective_trial_end ;;
    timeframes: [date, week, month, quarter, year]
  }

  # ——— Measures ———

  measure: trial_cancellations {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    label: "Trial Cancellations"
    description: "Total distinct subscriptions cancelled during trial"
    drill_fields: [drilldown_details*]
  }

  measure: same_day_cancellations {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [cancellation_timing: "same_day"]
    label: "Same Day Cancellations"
    description: "Cancellations where trial start date = cancellation date"
    drill_fields: [drilldown_details*]
  }

  measure: later_cancellations {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [cancellation_timing: "later"]
    label: "Later Cancellations"
    description: "Cancellations where trial start date != cancellation date"
    drill_fields: [drilldown_details*]
  }

  # ——— Drill Set ———

  set: drilldown_details {
    fields: [
      user_id,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      subscription_id,
      subscription_status,
      cancellation_timing,
      plan_name,
      plan_interval,
      price,
      trial_status,
      trial_starts_at_date,
      trial_ends_at_date,
      effective_trial_ends_at_date,
      cancellation_date,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type
    ]
  }
}

view: daily_new_trials {
  derived_table: {
    sql:
      WITH new_trials AS (
        SELECT
          fs.subscription_id,
          fs.user_id,
          DATE(fs.initial_start_date) AS trial_start_date,
          COALESCE(fs.discounted_price, fs.price + fs.tax_amount) AS price,
          JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
          JSON_EXTRACT_SCALAR(plan, '$.interval') AS plan_interval,
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
      ELSE 'Active'
      END AS trial_status,

      CASE
      WHEN oe.user_id IS NULL THEN 'event_not_fired'
      WHEN oe.context_campaign_campaign IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source,

      oe.context_campaign_campaign AS marketing_campaign,
      oe.utm_regintent,
      oe.business_type,

      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email AS sign_up_user_email,

      ROW_NUMBER() OVER (PARTITION BY fs.subscription_id ORDER BY fs.updated_at DESC) AS rn

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

      WHERE fs.is_deleted = FALSE
      -- Only subscriptions that actually have a trial
      AND fs.trial_end IS NOT NULL
      AND fs.initial_start_date IS NOT NULL
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND (pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      ))
      )

      SELECT * FROM new_trials WHERE rn = 1
      {% if date_range._is_filtered %}
      AND {% condition date_range %} TIMESTAMP(trial_start_date) {% endcondition %}
      {% endif %}
      ;;
  }

  # ——— Filters ———

  filter: date_range {
    type: date
    description: "Filter by trial start date. Optional."
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

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension_group: trial_start {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.trial_start_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Trial Start"
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

  dimension: trial_status {
    type: string
    sql: ${TABLE}.trial_status ;;
    description: "No trial / Active / Ended"
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

  dimension: plan_name {
    type: string
    sql: ${TABLE}.plan_name ;;
  }

  dimension: plan_interval {
    type: string
    sql: ${TABLE}.plan_interval ;;
    label: "Interval"
  }

  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
    value_format_name: decimal_2
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

  # ——— Measures ———

  measure: new_trials {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    label: "New Trials"
    description: "Count of distinct new trial subscriptions started on a given date"
    drill_fields: [drilldown_details*]
  }

  measure: new_trials_marketing {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [acquisition_source: "marketing_campaign"]
    label: "New Trials (Marketing)"
    description: "New trials attributed to a marketing campaign"
    drill_fields: [drilldown_details*]
  }

  measure: new_trials_organic {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [acquisition_source: "organic_walk-in"]
    label: "New Trials (Organic)"
    description: "New trials from organic walk-in"
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
      plan_name,
      plan_interval,
      price,
      trial_status,
      trial_start_date,
      trial_ends_at_date,
      effective_trial_ends_at_date,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type
    ]
  }
}

view: new_trial_report {
  filter: date_range {
    type: date
    description: "Filter events by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql:
      WITH base AS (
        SELECT
          t1.*,
          plan,

          COALESCE(
            CASE
              WHEN t1.cancellation_applied_at IS NOT NULL
                   AND t1.cancellation_applied_at < t1.trial_end
              THEN t1.cancellation_applied_at
            END,
            t1.trial_end
          ) AS effective_trial_end

        FROM dbt_popshop.fact_seller_subscription t1,
        UNNEST(t1.plans) AS plan

        WHERE
          t1.trial_end IS NOT NULL
          AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
          AND {% condition date_range %} TIMESTAMP(t1.initial_start_date) {% endcondition %}
      )
      SELECT
        prof.url_code AS sign_up_url_code,
        prof.username AS sign_up_user_username,
        pprof.email AS sign_up_user_email,
        oe.context_campaign_campaign as marketing_campaign,
        oe.utm_regintent,
        oe.business_type,

        CASE
          WHEN DATE(base.effective_trial_end) <= CURRENT_DATE()
               AND DATE_DIFF(DATE(base.effective_trial_end), DATE(base.initial_start_date), DAY) <= 6
            THEN 'Cancelled within 6 days'
          WHEN DATE(base.effective_trial_end) <= CURRENT_DATE()
            THEN 'Cancelled after 6 days'
        END AS cancellation_status,

        CASE
          WHEN DATE(base.effective_trial_end) <= CURRENT_DATE()
               AND DATE_DIFF(DATE(base.effective_trial_end), DATE(base.initial_start_date), DAY) <= 6
            THEN 1 ELSE 0
        END AS within_7_days,

        CASE
          WHEN DATE(base.effective_trial_end) <= CURRENT_DATE()
            AND DATE_DIFF(DATE(base.effective_trial_end), DATE(base.initial_start_date), DAY) > 6
          THEN 1 ELSE 0
        END AS after_7_days,

        CASE
          WHEN oe.user_id IS NULL THEN 'event_not_fired'
          WHEN oe.context_campaign_campaign IS NOT NULL THEN 'marketing_campaign'
          ELSE 'organic_walk-in'
        END AS acquisition_source,

        base.id,
        base.status,
        base.initial_start_date AS trial_starts,
        base.trial_end AS trial_ends,
        base.cancellation_applied_at,
        base.effective_trial_end,
        base.subscription_id,
        base.user_id,

        COALESCE(base.discounted_price, base.price + base.tax_amount) AS price,

        JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
        JSON_EXTRACT_SCALAR(plan, '$.interval') AS plan_interval,

        CASE
          WHEN base.trial_end IS NULL THEN 'No trial'
          WHEN DATE(base.effective_trial_end) <= CURRENT_DATE() THEN 'Ended'
          ELSE 'Started'
        END AS trial_status,

        CASE
          WHEN base.trial_end IS NULL THEN 3
          WHEN DATE(base.effective_trial_end) <= CURRENT_DATE() THEN 2
          ELSE 1
        END AS trial_type,

        CASE
          WHEN base.initial_start_date IS NOT NULL THEN 1
          ELSE 0
        END AS is_trial_started,

        CASE
          WHEN DATE(base.effective_trial_end) <= CURRENT_DATE() THEN 1
          ELSE 0
        END AS is_trial_ended,

      FROM base

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
        ON prof.user_id = base.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
        ON pprof.user_id = base.user_id

      LEFT JOIN (
        SELECT *
        FROM (
          SELECT * ,
          ROW_NUMBER() OVER (PARTITION BY user_id) rn
          FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
        )
        WHERE rn = 1
      ) oe
      ON oe.user_id = base.user_id

      WHERE
        (pprof.email IS NULL OR (
          LOWER(pprof.email) NOT LIKE '%@test.com'
          AND LOWER(pprof.email) NOT LIKE '%@example.com'
          AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
          AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
        ))

      ORDER BY base.initial_start_date DESC;;
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
    primary_key: yes
    hidden: yes
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

  dimension: within_7_days {
    type: number
    sql: ${TABLE}.within_7_days ;;
    hidden: yes
  }

  dimension: after_7_days {
    type: number
    sql: ${TABLE}.after_7_days ;;
    hidden: yes
  }

  dimension: cancellation_status {
    type: string
    sql: ${TABLE}.cancellation_status ;;
  }

  measure: sum_total_trials {
    type: count_distinct
    sql: ${id} ;;
    label: "Total Trials"
    drill_fields: [onboarding_details*]
  }

  measure: sum_total_trials_started {
    type: sum
    sql: ${TABLE}.is_trial_started ;;
    label: "Started Trials"
    drill_fields: [onboarding_details*]
  }

  measure: sum_total_trials_ended {
    type: sum
    sql: ${TABLE}.is_trial_ended ;;
    label: "Ended Trials"
    drill_fields: [onboarding_details*]
  }

  measure: cancelled_within_7_days {
    type: count
    filters: [within_7_days: "1"]
    label: "Within 7 days"
    drill_fields: [onboarding_details*]
  }

  measure: cancelled_after_7_days {
    type: count
    filters: [after_7_days: "1"]
    label: "After 7 days"
    drill_fields: [onboarding_details*]
  }

  set: onboarding_details {
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
      trial_starts_at_date,
      trial_ends_at_date,
      effective_trial_ends_at_date,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type
    ]
  }
}

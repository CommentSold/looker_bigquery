view: qa_trial_conversion_rate {
  derived_table: {
    sql:
      WITH all_trials AS (
        SELECT
          fs.subscription_id,
          fs.user_id,
          DATE(fs.initial_start_date) AS trial_start_date,
          DATE(fs.trial_end)          AS trial_end_date,
          fs.trial_end                AS trial_end_ts,
          COALESCE(fs.discounted_price, fs.price + fs.tax_amount) AS price,
          JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
          JSON_EXTRACT_SCALAR(plan, '$.interval')    AS plan_interval,

      CASE
      WHEN oe.context_campaign_campaign IS NOT NULL THEN 'marketing_campaign'
      WHEN oe.user_id IS NOT NULL               THEN 'organic_walk-in'
      ELSE 'event_not_fired'
      END AS acquisition_source,

      oe.context_campaign_campaign AS marketing_campaign,
      oe.utm_regintent,
      oe.business_type,
      prof.url_code       AS sign_up_url_code,
      prof.username       AS sign_up_user_username,
      pprof.email         AS sign_up_user_email,

      ROW_NUMBER() OVER (
      PARTITION BY fs.subscription_id ORDER BY fs.updated_at DESC
      ) AS rn

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
      ) oe ON oe.user_id = fs.user_id

      WHERE fs.is_deleted = FALSE
      AND fs.trial_end IS NOT NULL
      AND fs.initial_start_date IS NOT NULL
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND (pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      ))
      ),

      -- Deduplicated trials
      trials AS (
      SELECT * FROM all_trials WHERE rn = 1
      ),

      -- First paid invoice after trial end per subscription
      paid_invoices AS (
      SELECT
      ih.subscription_id,
      MIN(DATE(ih.updated_at)) AS converted_date
      FROM `dbt_popshop.fact_seller_subscription_invoice` ih
      JOIN trials t
      ON t.subscription_id = ih.subscription_id
      WHERE ih.is_deleted = FALSE
      AND ih.status     = 'paid'
      AND ih.amount_paid > 0
      AND DATE(ih.updated_at) >= t.trial_end_date
      GROUP BY 1
      ),

      -- Join conversion back to trials
      trial_with_conversion AS (
      SELECT
      t.*,
      pi.converted_date,
      CASE WHEN pi.subscription_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_converted
      FROM trials t
      LEFT JOIN paid_invoices pi
      ON pi.subscription_id = t.subscription_id
      )

      SELECT
      twc.subscription_id,
      twc.user_id,
      twc.trial_start_date,
      twc.trial_end_date,
      twc.converted_date,
      twc.is_converted,
      twc.price,
      twc.plan_name,
      twc.plan_interval,
      twc.acquisition_source,
      twc.marketing_campaign,
      twc.utm_regintent,
      twc.business_type,
      twc.sign_up_url_code,
      twc.sign_up_user_username,
      twc.sign_up_user_email
      FROM trial_with_conversion twc
      {% if date_range._is_filtered %}
      WHERE {% condition date_range %} TIMESTAMP(twc.trial_start_date) {% endcondition %}
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

  dimension: is_converted {
    type: yesno
    sql: ${TABLE}.is_converted ;;
    label: "Converted to Paid"
    description: "TRUE if the trial converted to a paid subscription"
  }

  dimension_group: trial_start {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.trial_start_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Trial Start"
  }

  dimension_group: trial_end {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.trial_end_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Trial End"
  }

  dimension_group: converted {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.converted_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Conversion"
    description: "Date the trial converted to paid (NULL if not yet converted)"
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

  measure: total_trials {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    label: "Total Trials"
    description: "Total number of trials started"
    drill_fields: [drilldown_details*]
  }

  # Stripe denominator: only trials whose trial_end has passed (i.e. reached trial end)
  measure: trials_reached_end {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [trial_end_date: "before today"]
    label: "Trials That Reached Trial End"
    description: "Trials where trial_end_date < today — the correct denominator per Stripe's formula"
    drill_fields: [drilldown_details*]
  }

  measure: converted_trials {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_converted: "yes"]
    label: "Converted Trials"
    description: "Number of trials that converted to paid"
    drill_fields: [drilldown_details*]
  }

  # Matches Stripe: converted / trials_that_reached_trial_end
  measure: trial_conversion_rate {
    type: number
    sql: SAFE_DIVIDE(${converted_trials}, NULLIF(${trials_reached_end}, 0)) ;;
    label: "Trial Conversion Rate"
    description: "converted_trials / trials_that_reached_trial_end — matches Stripe's 12.6% formula"
    value_format_name: percent_1
    drill_fields: [drilldown_details*]
  }

  # Daily version: same Stripe logic scoped per day
  measure: daily_trial_conversion_rate {
    type: number
    sql: SAFE_DIVIDE(
           COUNTIF(${TABLE}.is_converted = TRUE AND ${TABLE}.trial_end_date < CURRENT_DATE()),
           NULLIF(COUNTIF(${TABLE}.trial_end_date < CURRENT_DATE()), 0)
         ) ;;
    label: "Daily Trial Conversion Rate"
    description: "Per-day: converted / trials_that_reached_trial_end. Matches Stripe's dashed line."
    value_format_name: percent_1
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
      is_converted,
      plan_name,
      plan_interval,
      price,
      trial_start_date,
      trial_end_date,
      converted_date,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type
    ]
  }
}

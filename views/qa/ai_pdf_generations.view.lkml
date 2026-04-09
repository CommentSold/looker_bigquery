view: ai_pdf_generations {
  filter: date_range {
    type: date
    description: "Filter by AI PDF created_at date. Use 'is in range' in the UI. Optional."
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
      ),

      -- Most recent ai_pdf_generations record per user + total count
      ai_pdf_latest AS (
      SELECT
      user_id,
      session_id,
      created_at,
      status,
      COUNT(*) OVER (PARTITION BY user_id) AS total_pdfs_generated
      FROM `popshoplive-26f81.commentsold.ai_pdf_generations`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) = 1
      )

      SELECT
      -- ai_pdf fields
      aipdf.session_id,
      aipdf.created_at        AS ai_pdf_created_at,
      aipdf.status            AS ai_pdf_status,
      aipdf.total_pdfs_generated,

      -- subscription / trial fields
      base.id,
      base.subscription_id,
      base.user_id,
      base.status       AS subscription_status,
      base.initial_start_date AS trial_starts,
      base.trial_end          AS trial_ends,
      base.effective_trial_end,
      base.cancellation_applied_at,

      COALESCE(base.discounted_price, base.price + base.tax_amount) AS price,
      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval')    AS plan_interval,

      CASE
      WHEN base.trial_end IS NULL THEN 'No trial'
      WHEN DATE(base.effective_trial_end) <= CURRENT_DATE() THEN 'Ended'
      ELSE 'Started'
      END AS trial_status,

      -- profile fields
      prof.url_code  AS sign_up_url_code,
      prof.username  AS sign_up_user_username,
      pprof.email    AS sign_up_user_email,

      -- acquisition fields
      oe.context_campaign_campaign AS marketing_campaign,
      oe.utm_regintent,
      oe.business_type,

      CASE
      WHEN oe.user_id IS NULL                       THEN 'event_not_fired'
      WHEN oe.context_campaign_campaign IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source

      FROM ai_pdf_latest aipdf

      -- Join to subscription (LEFT — keep pdf records even if no sub)
      LEFT JOIN base
      ON base.user_id = aipdf.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = aipdf.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = aipdf.user_id

      LEFT JOIN (
      SELECT *
      FROM (
      SELECT *,
      ROW_NUMBER() OVER (PARTITION BY user_id) AS rn
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      )
      WHERE rn = 1
      ) oe ON oe.user_id = aipdf.user_id

      WHERE
      (pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      ))
      {% if date_range._is_filtered %}
      AND {% condition date_range %} aipdf.created_at {% endcondition %}
      {% endif %}

      ORDER BY aipdf.created_at DESC
      ;;
  }

  # ——— Primary Key ———

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
    primary_key: yes
    hidden: yes
  }

  # ——— AI PDF Dimensions ———

  dimension_group: ai_pdf_created {
    type: time
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.ai_pdf_created_at ;;
    timeframes: [time, date, week, month, quarter, year]
    label: "AI PDF Created"
  }

  dimension: total_pdfs_generated {
    type: number
    sql: ${TABLE}.total_pdfs_generated ;;
    label: "Total PDFs Generated"
    description: "Total number of AI PDF generation attempts by this user across all time"
  }

  dimension: ai_pdf_status {
    type: string
    sql: ${TABLE}.ai_pdf_status ;;
    label: "AI PDF Status"
    # ✅ Color by utm_regintent is applied at the chart level in Looker
    # but we expose utm_regintent as the pivot/color dimension below
  }

  # ——— User / Subscription Dimensions ———

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
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

  # ——— Profile / Acquisition Dimensions ———

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

  # ✅ utm_regintent — use this as the "Color" dimension in Looker chart config
  dimension: utm_regintent {
    type: string
    sql: ${TABLE}.utm_regintent ;;
    label: "UTM Regintent"
    description: "Use as the Color dimension in chart config to color bars/lines by intent."
  }

  dimension: business_type {
    type: string
    sql: ${TABLE}.business_type ;;
  }

  dimension: acquisition_source {
    type: string
    sql: ${TABLE}.acquisition_source ;;
  }

  # ——— Measures ———

  measure: total_ai_pdf_generations {
    type: count_distinct
    sql: ${TABLE}.session_id ;;
    label: "Total AI PDF Generations"
    description: "Count of distinct AI PDF generation sessions"
    drill_fields: [drill_details*]
  }

  measure: total_unique_users {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Unique Users"
    description: "Count of distinct users who generated an AI PDF"
    drill_fields: [drill_details*]
  }

  measure: total_completed {
    type: count_distinct
    sql: ${TABLE}.session_id ;;
    filters: [ai_pdf_status: "completed"]
    label: "Completed Generations"
    drill_fields: [drill_details*]
  }

  measure: total_failed {
    type: count_distinct
    sql: ${TABLE}.session_id ;;
    filters: [ai_pdf_status: "failed"]
    label: "Failed Generations"
    drill_fields: [drill_details*]
  }

  measure: completion_rate {
    type: number
    sql: SAFE_DIVIDE(${total_completed}, NULLIF(${total_ai_pdf_generations}, 0)) * 100 ;;
    label: "Completion Rate (%)"
    value_format_name: decimal_1
    drill_fields: [drill_details*]
  }

  # ——— Drill Set ———

  set: drill_details {
    fields: [
      user_id,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      session_id,
      ai_pdf_created_date,
      ai_pdf_status,
      total_pdfs_generated,
      subscription_id,
      subscription_status,
      plan_name,
      plan_interval,
      price,
      trial_status,
      trial_starts_at_date,
      trial_ends_at_date,
      effective_trial_ends_at_date,
      marketing_campaign,
      utm_regintent,
      business_type,
      acquisition_source
    ]
  }
}

view: ai_pdf_generations {
  filter: date_range {
    type: date
    description: "Filter by trial start date (initial_start_date). Use 'is in range' in the UI. Optional."
  }

  derived_table: {
    sql:
      WITH base_raw AS (
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
          ) AS effective_trial_end,
          JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
          JSON_EXTRACT_SCALAR(plan, '$.interval') AS plan_interval
        FROM dbt_popshop.fact_seller_subscription t1,
        UNNEST(t1.plans) AS plan
        WHERE
          t1.trial_end IS NOT NULL
          AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      ),

      -- Deduplicate to one row per user (handles multiple 'plan' type entries)
      base AS (
      SELECT *
      FROM base_raw
      QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY initial_start_date DESC) = 1
      ),

      -- Marketing capture fallback for utm_regintent
      marketing_capture AS (
      SELECT
      user_id,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_campaign') AS utm_campaign,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_source') AS utm_source,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent') AS utm_regintent
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      ),

      -- AI PDF stats per user: count completed, count started (all attempts)
      ai_pdf_user_stats AS (
      SELECT
      user_id,
      COUNT(*) AS total_pdf_generations_started,
      COUNTIF(status = 'success') AS total_pdfs_completed
      FROM `popshoplive-26f81.commentsold.ai_pdf_generations`
      GROUP BY user_id
      ),

      -- Most recent ai_pdf_generations record per user
      ai_pdf_latest AS (
      SELECT
      user_id,
      session_id,
      created_at,
      status
      FROM `popshoplive-26f81.commentsold.ai_pdf_generations`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) = 1
      ),

      -- Trial users with any AI PDF activity (all statuses: success, failed, pdf_summary_ready)
      trial_pdf_users AS (
      SELECT
      -- ai_pdf fields
      aipdf.session_id,
      aipdf.created_at AS ai_pdf_created_at,
      aipdf.status AS ai_pdf_status,

      -- PDF generation stats per user
      stats.total_pdfs_completed,
      stats.total_pdf_generations_started,

      -- subscription / trial fields
      base.id,
      base.subscription_id,
      base.user_id,
      base.status AS subscription_status,
      base.initial_start_date AS trial_starts,
      base.trial_end AS trial_ends,
      base.effective_trial_end,
      base.cancellation_applied_at,

      COALESCE(base.discounted_price, base.price + base.tax_amount) AS price,
      base.plan_name,
      base.plan_interval,

      CASE
      WHEN base.trial_end IS NULL THEN 'No trial'
      WHEN DATE(base.effective_trial_end) <= CURRENT_DATE() THEN 'Ended'
      ELSE 'Started'
      END AS trial_status,

      -- User's current status: Subscriber or Cancelled
      CASE
      WHEN base.status = 'active' THEN 'Subscriber'
      WHEN base.status IN ('canceled', 'cancelled') THEN 'Cancelled'
      WHEN base.cancellation_applied_at IS NOT NULL THEN 'Cancelled'
      ELSE COALESCE(base.status, 'Unknown')
      END AS user_current_status,

      -- Boolean flag for paying subscriber (for drill down)
      CASE
      WHEN base.status = 'active' THEN 'Yes'
      ELSE 'No'
      END AS is_subscriber,

      -- profile fields
      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email AS sign_up_user_email,

      -- acquisition fields with fallback for utm_regintent (COALESCE nulls to 'generic')
      COALESCE(NULLIF(oe.context_campaign_campaign, ''), 'generic') AS marketing_campaign,
      COALESCE(NULLIF(oe.utm_regintent, ''), NULLIF(mc.utm_regintent, ''), 'generic') AS utm_regintent,
      COALESCE(NULLIF(oe.business_type, ''), 'generic') AS business_type,

      CASE
      WHEN COALESCE(oe.context_campaign_campaign, mc.utm_campaign) IS NOT NULL
      THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL
      THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source,

      FROM base

      -- Join to ai_pdf_latest (INNER — only users who started a trial AND have PDF activity)
      INNER JOIN ai_pdf_latest aipdf
      ON aipdf.user_id = base.user_id

      -- Join to user stats for counts
      LEFT JOIN ai_pdf_user_stats stats
      ON stats.user_id = base.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = base.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = base.user_id

      -- Marketing capture fallback
      LEFT JOIN marketing_capture mc
      ON mc.user_id = base.user_id

      LEFT JOIN (
      SELECT *
      FROM (
      SELECT *,
      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp DESC) AS rn
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      )
      WHERE rn = 1
      ) oe ON oe.user_id = base.user_id

      WHERE
      (pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      ))
      )

      SELECT *
      FROM trial_pdf_users
      WHERE 1=1
      -- Filter to only pdf_creator and generic rows (nulls/empties are already converted to 'generic')
      AND utm_regintent IN ('pdf_creator', 'generic')
      {% if date_range._is_filtered %}
      AND {% condition date_range %} trial_starts {% endcondition %}
      {% endif %}
      ORDER BY ai_pdf_created_at DESC
      ;;
  }

  # ——— Primary Key ———

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
    primary_key: yes
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
    label: "Latest AI PDF Session ID"
    description: "Most recent AI PDF session_id for this user"
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

  dimension: total_pdfs_completed {
    type: number
    sql: ${TABLE}.total_pdfs_completed ;;
    label: "PDFs Successfully Generated"
    description: "Number of PDFs successfully generated (completed) by this user"
  }

  dimension: total_pdf_generations_started {
    type: number
    sql: ${TABLE}.total_pdf_generations_started ;;
    label: "PDF Generations Started"
    description: "Total number of PDF generation attempts started by this user"
  }

  dimension: ai_pdf_status {
    type: string
    sql: ${TABLE}.ai_pdf_status ;;
    label: "AI PDF Status"
    description: "Status values: 'success' (completed), 'pdf_summary_ready' (started/in progress), 'failed'"
  }

  # ——— User / Subscription Dimensions ———

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: user_current_status {
    type: string
    sql: ${TABLE}.user_current_status ;;
    label: "User Current Status"
    description: "Whether the user is currently a Subscriber or Cancelled"
  }

  dimension: is_subscriber {
    type: string
    sql: ${TABLE}.is_subscriber ;;
    label: "Subscriber"
    description: "Yes if the user is a paying subscriber, No otherwise"
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
    label: "Trial Start"
    description: "Use this date dimension to compare with trial_start graph"
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
    description: "Defaults to 'generic' if null"
  }

  dimension: utm_regintent {
    type: string
    sql: ${TABLE}.utm_regintent ;;
    label: "UTM Regintent"
    description: "Use as the Color dimension in chart config. Falls back to onboardingMarketingCapture if missing, then to 'generic' if null."
  }

  dimension: business_type {
    type: string
    sql: ${TABLE}.business_type ;;
    description: "Defaults to 'generic' if null"
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
    filters: [ai_pdf_status: "success"]
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

  measure: total_in_progress {
    type: count_distinct
    sql: ${TABLE}.session_id ;;
    filters: [ai_pdf_status: "pdf_summary_ready"]
    label: "In Progress (Started)"
    description: "PDF generations that started but haven't completed yet"
    drill_fields: [drill_details*]
  }

  measure: completion_rate {
    type: number
    sql: SAFE_DIVIDE(${total_completed}, NULLIF(${total_ai_pdf_generations}, 0)) * 100 ;;
    label: "Completion Rate (%)"
    value_format_name: decimal_1
    drill_fields: [drill_details*]
  }

  measure: sum_pdfs_completed {
    type: sum
    sql: ${TABLE}.total_pdfs_completed ;;
    label: "Total PDFs Completed (Sum)"
    description: "Sum of all successfully generated PDFs across users"
    drill_fields: [drill_details*]
  }

  measure: sum_pdf_generations_started {
    type: sum
    sql: ${TABLE}.total_pdf_generations_started ;;
    label: "Total PDF Generations Started (Sum)"
    description: "Sum of all PDF generation attempts across users"
    drill_fields: [drill_details*]
  }

  measure: count_subscribers {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [user_current_status: "Subscriber"]
    label: "Current Subscribers"
    description: "Count of users who are currently subscribed"
    drill_fields: [drill_details*]
  }

  measure: count_cancelled {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [user_current_status: "Cancelled"]
    label: "Cancelled Users"
    description: "Count of users who have cancelled"
    drill_fields: [drill_details*]
  }

  # ——— Drill Set ———

  set: drill_details {
    fields: [
      user_id,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      is_subscriber,
      session_id,
      ai_pdf_created_date,
      ai_pdf_status,
      total_pdfs_completed,
      total_pdf_generations_started,
      user_current_status,
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

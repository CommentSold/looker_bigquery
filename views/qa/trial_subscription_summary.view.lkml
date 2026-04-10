view: trial_subscription_summary {
  derived_table: {
    sql:
      WITH

            -- ── Total Active Subscriptions (yesterday signups) ─────────
            total_active_sub AS (
              SELECT
                prof.user_id,
                prof.url_code         AS sign_up_url_code,
                prof.username         AS sign_up_user_username,
                pprof.email           AS sign_up_user_email,
                DATE(prof.created_at) AS signup_date,
                'paid'                AS subscription_type,
                -- Join subscription to get sub-level fields
                fs.subscription_id,
                DATE(fs.initial_start_date) AS trial_start_date,
                DATE(fs.trial_end)          AS trial_end_date
              FROM `popshoplive-26f81.dbt_popshop.dim_profiles` prof
              LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
                ON pprof.user_id = prof.user_id
              -- Join latest active subscription per user
              LEFT JOIN (
                SELECT
                  subscription_id,
                  user_id,
                  initial_start_date,
                  trial_end
                FROM `dbt_popshop.fact_seller_subscription`,
                UNNEST(plans) AS plan
                WHERE is_deleted = FALSE
                  AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
                -- ✅ Dedupe by subscription_id not user_id
                -- so users with multiple subs get all their rows
                QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY updated_at DESC) = 1
              ) fs ON fs.user_id = prof.user_id
              WHERE prof.user_type IN ('seller', 'verifiedSeller')
                AND prof.apps_pop_store = TRUE
                AND DATE(prof.created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                AND (pprof.email IS NULL OR (
                  LOWER(pprof.email) NOT LIKE '%@test.com'
                  AND LOWER(pprof.email) NOT LIKE '%@example.com'
                  AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
                  AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
                ))
            ),

      -- ── Total Active Trials (yesterday trial starts) ───────────
      total_active_trials AS (
      SELECT
      t1.user_id,
      DATE(t1.initial_start_date) AS trial_start_date,
      DATE(t1.trial_end)          AS trial_end_date,
      t1.subscription_id,
      'trial'                     AS subscription_type
      FROM `dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS plan
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = t1.user_id
      WHERE t1.trial_end IS NOT NULL
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND DATE(t1.initial_start_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND (pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      ))
      QUALIFY ROW_NUMBER() OVER (PARTITION BY t1.subscription_id ORDER BY t1.updated_at DESC) = 1
      ),

      -- ── Union both with shared shape ───────────────────────────
      combined AS (
      SELECT
      user_id,
      subscription_id,
      trial_start_date,
      trial_end_date,
      subscription_type,
      sign_up_url_code,
      sign_up_user_username,
      sign_up_user_email
      FROM total_active_sub

      UNION ALL

      SELECT
      tat.user_id,
      tat.subscription_id,
      tat.trial_start_date,
      tat.trial_end_date,
      tat.subscription_type,
      prof.url_code     AS sign_up_url_code,
      prof.username     AS sign_up_user_username,
      pprof.email       AS sign_up_user_email
      FROM total_active_trials tat
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = tat.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = tat.user_id
      )

      SELECT
      c.user_id,
      c.subscription_id,
      c.trial_start_date,
      c.trial_end_date,
      c.subscription_type,
      c.sign_up_url_code,
      c.sign_up_user_username,
      c.sign_up_user_email,
      oe.context_campaign_campaign AS marketing_campaign,
      oe.utm_regintent,
      oe.business_type,
      CASE
      WHEN oe.user_id IS NULL                       THEN 'event_not_fired'
      WHEN oe.context_campaign_campaign IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source
      FROM combined c
      LEFT JOIN (
      SELECT *
      FROM (
      SELECT *,
      ROW_NUMBER() OVER (PARTITION BY user_id) AS rn
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      )
      WHERE rn = 1
      ) oe ON oe.user_id = c.user_id
      ;;
  }

  # ——— Dimensions ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(${TABLE}.user_id, '-', ${TABLE}.subscription_type) ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: subscription_type {
    type: string
    sql: ${TABLE}.subscription_type ;;
    description: "trial = started trial yesterday | paid = signed up yesterday"
  }

  dimension_group: trial_start {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.trial_start_date ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension_group: trial_end {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.trial_end_date ;;
    timeframes: [date, week, month, quarter, year]
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

  # Card 1 — green: yesterday's trial starts
  measure: total_active_trials {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [subscription_type: "trial"]
    label: "Yesterday's Total Active Trials"
    description: "Distinct users who started a trial yesterday"
    drill_fields: [drilldown_details*]
  }

  # Card 2 — orange: yesterday's new signups
  measure: total_active_subscriptions {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [subscription_type: "paid"]
    label: "Total Active Subscriptions"
    description: "Distinct seller/verifiedSeller profiles created yesterday"
    drill_fields: [drilldown_details*]
  }

  # Card 3 — conversion rate: (trials / subscriptions) * 100
  measure: trial_to_paid_conversion_rate {
    type: number
    sql: SAFE_DIVIDE(${total_active_trials}, NULLIF(${total_active_subscriptions}, 0));;
    label: "Trial → Paid Conversion Rate"
    description: "(Yesterday's Total Active Trials / Total Active Subscriptions)"
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
      subscription_type,
      trial_start_date,
      trial_end_date,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type
    ]
  }
}

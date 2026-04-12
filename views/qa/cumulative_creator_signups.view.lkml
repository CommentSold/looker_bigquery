view: cumulative_creator_signups {
  derived_table: {
    sql:
      WITH profiles AS (
        SELECT
          p.user_id,
          MIN(DATE(s.created_at)) AS profile_created_at
        FROM `popshoplive-26f81.dbt_popshop.dim_profiles` p
        INNER JOIN `popshoplive-26f81.dbt_popshop.dim_stores` s
          ON p.user_id = s.store_id
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
          ON pprof.user_id = p.user_id
        WHERE
          p.apps_pop_store = TRUE
          AND p.user_type IN ('seller', 'verifiedSeller')
          AND (pprof.email IS NULL OR NOT REGEXP_CONTAINS(LOWER(pprof.email), r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com)$'))
        GROUP BY p.user_id
      ),

      profiles_with_dates AS (
      SELECT
      user_id,
      EXTRACT(YEAR FROM profile_created_at) AS yr_number,
      EXTRACT(MONTH FROM profile_created_at) AS mn_number,
      profile_created_at
      FROM profiles
      WHERE profile_created_at >= '2025-01-13'
      ),

      cumulative_profiles AS (
      SELECT
      yr_number,
      mn_number,
      COUNT(DISTINCT user_id) AS monthly_signups,
      SUM(COUNT(DISTINCT user_id)) OVER (ORDER BY yr_number, mn_number ROWS UNBOUNDED PRECEDING) AS actual_cumulative_creator_signups
      FROM profiles_with_dates
      GROUP BY 1, 2
      ),

      first_saas_base AS (
      SELECT
      user_id,
      MIN(current_period_start) AS first_subscription_date
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS plan
      WHERE
      JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND current_period_start IS NOT NULL
      AND status = 'active'
      AND (
      trial_end IS NULL
      OR DATETIME(current_period_start) >= DATETIME(
      COALESCE(
      CASE
      WHEN cancellation_applied_at IS NOT NULL AND cancellation_applied_at < trial_end
      THEN cancellation_applied_at
      END,
      trial_end
      )
      )
      )
      GROUP BY user_id
      ),

      first_saas AS (
      SELECT
      p.user_id,
      EXTRACT(YEAR FROM fss.first_subscription_date) AS yr_number,
      EXTRACT(MONTH FROM fss.first_subscription_date) AS mn_number,
      DATE(fss.first_subscription_date) AS first_saas_invoice
      FROM `popshoplive-26f81.dbt_popshop.dim_profiles` p
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = p.user_id
      INNER JOIN first_saas_base fss
      ON p.user_id = fss.user_id
      WHERE
      p.apps_pop_store = TRUE
      AND p.user_type IN ('seller', 'verifiedSeller')
      AND (pprof.email IS NULL OR NOT REGEXP_CONTAINS(LOWER(pprof.email), r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com)$'))
      ),

      monthly_paid_creators AS (
      SELECT
      yr_number,
      mn_number,
      COUNT(DISTINCT user_id) AS actual_paid_creators,
      SUM(COUNT(DISTINCT user_id)) OVER (ORDER BY yr_number, mn_number ROWS UNBOUNDED PRECEDING) AS cumulative_paid_creators
      FROM first_saas
      WHERE yr_number IS NOT NULL
      GROUP BY 1, 2
      ),

      monthly_creator_signups AS (
      SELECT
      yr_number,
      mn_number,
      COUNT(DISTINCT user_id) AS actual_creator_signups
      FROM profiles_with_dates
      GROUP BY 1, 2
      ),

      month_spine AS (
      SELECT
      EXTRACT(YEAR FROM month_date) AS year,
      EXTRACT(MONTH FROM month_date) AS month_number,
      month_date AS first_day_of_month
      FROM UNNEST(GENERATE_DATE_ARRAY('2025-01-01', '2027-12-01', INTERVAL 1 MONTH)) AS month_date
      )

      SELECT
      ms.first_day_of_month,
      ms.month_number,
      ms.year,
      cp.monthly_signups AS monthly_creator_signups,
      cp.actual_cumulative_creator_signups,
      mcs.actual_creator_signups,
      mpc.actual_paid_creators,
      mpc.cumulative_paid_creators
      FROM month_spine ms
      LEFT JOIN cumulative_profiles cp
      ON ms.year = cp.yr_number
      AND ms.month_number = cp.mn_number
      LEFT JOIN monthly_creator_signups mcs
      ON ms.year = mcs.yr_number
      AND ms.month_number = mcs.mn_number
      LEFT JOIN monthly_paid_creators mpc
      ON ms.year = mpc.yr_number
      AND ms.month_number = mpc.mn_number
      WHERE ms.year >= 2025
      AND cp.actual_cumulative_creator_signups IS NOT NULL  -- Only include months with actual data
      ORDER BY ms.year, ms.month_number
      ;;
  }

  # ——— Primary Key ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(CAST(${TABLE}.year AS STRING), '-', CAST(${TABLE}.month_number AS STRING)) ;;
  }

  # ——— Date Dimensions ———

  dimension_group: first_day_of_month {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.first_day_of_month ;;
    timeframes: [date, month, quarter, year]
    label: "Month"
  }

  dimension: month_number {
    type: number
    sql: ${TABLE}.month_number ;;
    label: "Month Number"
  }

  dimension: year {
    type: number
    sql: ${TABLE}.year ;;
    label: "Year"
  }

  # ——— Creator Sign-up Dimensions ———

  dimension: monthly_creator_signups_dim {
    type: number
    sql: ${TABLE}.monthly_creator_signups ;;
    label: "Monthly Creator Sign-ups"
    description: "Number of new creator sign-ups in this month"
    hidden: yes
  }

  dimension: actual_cumulative_creator_signups_dim {
    type: number
    sql: ${TABLE}.actual_cumulative_creator_signups ;;
    label: "Cumulative Creator Sign-ups"
    description: "Running total of creator sign-ups since Jan 13, 2025"
    hidden: yes
  }

  dimension: actual_creator_signups_dim {
    type: number
    sql: ${TABLE}.actual_creator_signups ;;
    label: "Creator Sign-ups"
    hidden: yes
  }

  # ——— Paid Creator Dimensions ———

  dimension: actual_paid_creators_dim {
    type: number
    sql: ${TABLE}.actual_paid_creators ;;
    label: "Monthly Paid Creators"
    description: "Number of creators who made their first payment this month"
    hidden: yes
  }

  dimension: cumulative_paid_creators_dim {
    type: number
    sql: ${TABLE}.cumulative_paid_creators ;;
    label: "Cumulative Paid Creators"
    description: "Running total of paid creators"
    hidden: yes
  }

  # ——— Measures ———

  measure: total_monthly_creator_signups {
    type: sum
    sql: ${TABLE}.monthly_creator_signups ;;
    label: "Monthly Creator Sign-ups"
    description: "Total new creator sign-ups in the selected period"
    drill_fields: [first_day_of_month_date, month_number, year, total_monthly_creator_signups]
  }

  measure: cumulative_creator_signups {
    type: max
    sql: ${TABLE}.actual_cumulative_creator_signups ;;
    label: "Cumulative Creator Sign-ups: Actual"
    description: "Running total of creator sign-ups (use with single month selection)"
    drill_fields: [first_day_of_month_date, month_number, year, cumulative_creator_signups]
  }

  measure: total_creator_signups {
    type: sum
    sql: ${TABLE}.actual_creator_signups ;;
    label: "Total Creator Sign-ups"
    description: "Sum of creator sign-ups in the selected period"
    drill_fields: [first_day_of_month_date, month_number, year, total_creator_signups]
  }

  measure: total_monthly_paid_creators {
    type: sum
    sql: ${TABLE}.actual_paid_creators ;;
    label: "Monthly Paid Creators"
    description: "Total creators who made their first payment in the selected period"
    drill_fields: [first_day_of_month_date, month_number, year, total_monthly_paid_creators]
  }

  measure: cumulative_paid_creators {
    type: max
    sql: ${TABLE}.cumulative_paid_creators ;;
    label: "Cumulative Paid Creators"
    description: "Running total of paid creators (use with single month selection)"
    drill_fields: [first_day_of_month_date, month_number, year, cumulative_paid_creators]
  }

  # ——— Calculated Measures ———

  measure: conversion_rate {
    type: number
    sql: SAFE_DIVIDE(${total_monthly_paid_creators}, NULLIF(${total_monthly_creator_signups}, 0)) * 100 ;;
    label: "Conversion Rate (%)"
    description: "Percentage of sign-ups that converted to paid"
    value_format_name: decimal_1
  }

  measure: cumulative_conversion_rate {
    type: number
    sql: SAFE_DIVIDE(${cumulative_paid_creators}, NULLIF(${cumulative_creator_signups}, 0)) * 100 ;;
    label: "Cumulative Conversion Rate (%)"
    description: "Overall percentage of sign-ups that converted to paid"
    value_format_name: decimal_1
  }
}

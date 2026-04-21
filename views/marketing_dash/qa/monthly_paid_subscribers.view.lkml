view: monthly_paid_subscribers {
  derived_table: {
    sql:
      -- Yearly plans: active subscriptions with yearly interval
      WITH yearly_plans AS (
        SELECT
          t1.user_id,
          t1.subscription_id,
          DATE(t1.current_period_start) AS period_start_date,
          DATE(t1.current_period_end) AS period_end_date
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
        UNNEST(t1.plans) AS plan
        INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` p ON p.user_id = t1.user_id
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof ON pprof.user_id = t1.user_id
        WHERE
          JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
          AND JSON_EXTRACT_SCALAR(plan, '$.interval') = 'year'
          AND DATETIME(t1.current_period_end) > CURRENT_DATETIME()
          AND t1.status = 'active'
          AND p.apps_pop_store = TRUE
          AND p.user_type IN ('seller', 'verifiedSeller')
          AND (pprof.email IS NULL OR NOT REGEXP_CONTAINS(LOWER(pprof.email), r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com)$'))
      ),

      -- Extrapolate yearly plans across months
      yearly_extrapolate AS (
      SELECT DISTINCT
      EXTRACT(YEAR FROM month_start) AS invoice_year,
      EXTRACT(MONTH FROM month_start) AS invoice_month,
      yp.user_id,
      'yearly' AS invoice_type
      FROM yearly_plans yp
      CROSS JOIN (
      SELECT DATE_TRUNC(month_date, MONTH) AS month_start
      FROM UNNEST(GENERATE_DATE_ARRAY('2025-01-01', '2027-12-01', INTERVAL 1 MONTH)) AS month_date
      ) ms
      WHERE
      ms.month_start >= DATE_TRUNC(yp.period_start_date, MONTH)
      AND ms.month_start < DATE_TRUNC(yp.period_end_date, MONTH)
      AND ms.month_start <= DATE_TRUNC(CURRENT_DATE(), MONTH)  -- Only up to current month
      ),

      -- Monthly invoices from actual invoice records
      monthly_invoices AS (
        SELECT
          EXTRACT(YEAR  FROM inv.created) AS invoice_year,
          EXTRACT(MONTH FROM inv.created) AS invoice_month,
          inv.user_id,
          'monthly' AS invoice_type
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice` inv
        INNER JOIN `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1
          ON t1.subscription_id = inv.subscription_id,
        UNNEST(t1.plans) AS plan
        INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` p ON p.user_id = inv.user_id
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof ON pprof.user_id = inv.user_id
        WHERE JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
          AND JSON_EXTRACT_SCALAR(plan, '$.interval') = 'month'
          AND inv.status    = 'paid'
          AND inv.amount_due  > 0    -- ← NEW
          AND inv.amount_paid > 0    -- ← NEW
          AND inv.is_deleted = FALSE -- ← NEW (consistency with other views)
          AND p.apps_pop_store = TRUE
          AND p.user_type IN ('seller', 'verifiedSeller')
          AND (pprof.email IS NULL
               OR NOT REGEXP_CONTAINS(LOWER(pprof.email),
                    r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com)$'))
      ),

      -- Combine monthly and yearly
      combined AS (
      SELECT invoice_year, invoice_month, user_id, invoice_type FROM yearly_extrapolate
      UNION ALL
      SELECT invoice_year, invoice_month, user_id, invoice_type FROM monthly_invoices
      ),

      -- Aggregate by month
      monthly_summary AS (
      SELECT
      invoice_year,
      invoice_month,
      COUNT(DISTINCT user_id) AS actual_paid_subscribers,
      COUNT(DISTINCT CASE WHEN invoice_type = 'monthly' THEN user_id END) AS monthly_plan_subscribers,
      COUNT(DISTINCT CASE WHEN invoice_type = 'yearly' THEN user_id END) AS yearly_plan_subscribers
      FROM combined
      GROUP BY 1, 2
      ),

      -- Month spine for output
      output_months AS (
      SELECT
      EXTRACT(YEAR FROM month_date) AS year,
      EXTRACT(MONTH FROM month_date) AS month_number,
      month_date AS first_day_of_month
      FROM UNNEST(GENERATE_DATE_ARRAY('2025-01-01', '2027-12-01', INTERVAL 1 MONTH)) AS month_date
      ),

      -- Target/Quota data for 2026
      targets AS (
      SELECT * FROM UNNEST([
  STRUCT(2026 AS year, 1 AS month_number, 298 AS paid_subscribers_target),
  STRUCT(2026 AS year, 2 AS month_number, 734 AS paid_subscribers_target),
  STRUCT(2026 AS year, 3 AS month_number, 1262 AS paid_subscribers_target),
  STRUCT(2026 AS year, 4 AS month_number, 373 AS paid_subscribers_target),
  STRUCT(2026 AS year, 5 AS month_number, 504 AS paid_subscribers_target),
  STRUCT(2026 AS year, 6 AS month_number, 737 AS paid_subscribers_target),
  STRUCT(2026 AS year, 7 AS month_number, 918 AS paid_subscribers_target),
  STRUCT(2026 AS year, 8 AS month_number, 1077 AS paid_subscribers_target),
  STRUCT(2026 AS year, 9 AS month_number, 1255 AS paid_subscribers_target),
  STRUCT(2026 AS year, 10 AS month_number, 1440 AS paid_subscribers_target),
  STRUCT(2026 AS year, 11 AS month_number, 1624 AS paid_subscribers_target),
  STRUCT(2026 AS year, 12 AS month_number, 1803 AS paid_subscribers_target)
])
      )

      SELECT
      om.first_day_of_month,
      om.month_number,
      om.year,
      CASE
        WHEN om.first_day_of_month <= DATE_TRUNC(CURRENT_DATE(), MONTH)
          THEN ms.actual_paid_subscribers
        ELSE NULL
      END AS actual_paid_subscribers,
      CASE
        WHEN om.first_day_of_month <= DATE_TRUNC(CURRENT_DATE(), MONTH)
          THEN ms.monthly_plan_subscribers
        ELSE NULL
      END AS monthly_plan_subscribers,
      CASE
        WHEN om.first_day_of_month <= DATE_TRUNC(CURRENT_DATE(), MONTH)
          THEN ms.yearly_plan_subscribers
        ELSE NULL
      END AS yearly_plan_subscribers,
      t.paid_subscribers_target
      FROM output_months om
      LEFT JOIN monthly_summary ms
      ON om.year = ms.invoice_year
      AND om.month_number = ms.invoice_month
      LEFT JOIN targets t
      ON om.year = t.year
      AND om.month_number = t.month_number
      WHERE om.year >= 2025
      AND (ms.actual_paid_subscribers IS NOT NULL OR t.paid_subscribers_target IS NOT NULL)
      ORDER BY om.year, om.month_number
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

  # ——— Subscriber Dimensions (hidden, used for measures) ———

  dimension: actual_paid_subscribers_dim {
    type: number
    sql: ${TABLE}.actual_paid_subscribers ;;
    hidden: yes
  }

  dimension: monthly_plan_subscribers_dim {
    type: number
    sql: ${TABLE}.monthly_plan_subscribers ;;
    hidden: yes
  }

  dimension: yearly_plan_subscribers_dim {
    type: number
    sql: ${TABLE}.yearly_plan_subscribers ;;
    hidden: yes
  }

  dimension: paid_subscribers_target_dim {
    type: number
    sql: ${TABLE}.paid_subscribers_target ;;
    hidden: yes
  }

  # ——— Actual Measures ———

  measure: total_paid_subscribers {
    type: sum
    sql: ${TABLE}.actual_paid_subscribers ;;
    label: "Paid Creators on Platform: Actual"
    description: "Total paying subscribers at end of month (monthly + yearly plans)"
    drill_fields: [first_day_of_month_date, month_number, year, total_paid_subscribers]
  }

  measure: total_monthly_plan_subscribers {
    type: sum
    sql: ${TABLE}.monthly_plan_subscribers ;;
    label: "Monthly Plan Subscribers"
    description: "Subscribers on monthly billing"
    drill_fields: [first_day_of_month_date, month_number, year, total_monthly_plan_subscribers]
  }

  measure: total_yearly_plan_subscribers {
    type: sum
    sql: ${TABLE}.yearly_plan_subscribers ;;
    label: "Yearly Plan Subscribers"
    description: "Subscribers on yearly billing (extrapolated)"
    drill_fields: [first_day_of_month_date, month_number, year, total_yearly_plan_subscribers]
  }

  measure: max_paid_subscribers {
    type: max
    sql: ${TABLE}.actual_paid_subscribers ;;
    label: "Paid Subscribers (Latest)"
    description: "Use for single month view - shows the actual count"
    drill_fields: [first_day_of_month_date, month_number, year, max_paid_subscribers]
  }

  # ——— Target Measures ———

  measure: paid_subscribers_target {
    type: max
    sql: ${TABLE}.paid_subscribers_target ;;
    label: "Paid Creators on Platform: Target"
    description: "Target for paying subscribers at end of month"
    drill_fields: [first_day_of_month_date, month_number, year, paid_subscribers_target]
  }

  # ——— Variance Measures ———

  measure: paid_subscribers_variance {
    type: number
    sql: ${total_paid_subscribers} - ${paid_subscribers_target} ;;
    label: "Paid Subscribers Variance"
    description: "Actual - Target for paying subscribers"
  }

  measure: paid_subscribers_pct_of_target {
    type: number
    sql: SAFE_DIVIDE(${total_paid_subscribers}, NULLIF(${paid_subscribers_target}, 0)) * 100 ;;
    label: "% of Target"
    description: "Actual as percentage of target"
    value_format_name: decimal_1
  }
}

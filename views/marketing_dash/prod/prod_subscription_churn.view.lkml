view: prod_subscription_churn {
  derived_table: {
    sql:
      WITH base_subscriptions AS (
        SELECT
          t1.subscription_id,
          t1.user_id,
          DATE(t1.initial_start_date) AS initial_start_date,
          DATE(t1.current_period_end) AS current_period_end,
          t1.cancelled_at,
          CONCAT(
            JSON_EXTRACT_SCALAR(plan, '$.productName'),
            ': ',
            JSON_EXTRACT_SCALAR(plan, '$.interval')
          ) AS plan_interval
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
        UNNEST(t1.plans) AS plan
        INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` p
          ON p.user_id = t1.user_id
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
          ON pprof.user_id = t1.user_id
        WHERE
          t1.is_deleted = FALSE
          AND p.apps_pop_store = TRUE
          AND p.user_type IN ('seller', 'verifiedSeller')
          AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
          AND (pprof.email IS NULL OR (
            LOWER(pprof.email) NOT LIKE '%@test.com'
            AND LOWER(pprof.email) NOT LIKE '%@example.com'
            AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
            AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
            AND LOWER(pprof.email) NOT LIKE '%@pop.store'
          ))
      ),

      subscription_mrr AS (
        SELECT
          subscription_id,
          MIN(DATE(created_at)) AS first_mrr_date
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
        WHERE amount_due > 0
        GROUP BY subscription_id
      ),

      starts AS (
        SELECT
          DATE_TRUNC(sm.first_mrr_date, MONTH) AS month_bucket,
          b.plan_interval,
          b.user_id,
          'start' AS event_type
        FROM base_subscriptions b
        INNER JOIN subscription_mrr sm
        ON sm.subscription_id = b.subscription_id
      ),

      ends AS (
        SELECT
          DATE_TRUNC(DATE(b.cancelled_at), MONTH) AS month_bucket,
          b.plan_interval,
          b.user_id,
          'end' AS event_type
        FROM base_subscriptions b
        INNER JOIN subscription_mrr sm
        ON sm.subscription_id = b.subscription_id
        WHERE b.cancelled_at IS NOT NULL
        AND DATE(b.cancelled_at) < CURRENT_DATE()
      ),

      combined AS (
        SELECT * FROM starts
        UNION ALL
        SELECT * FROM ends
      ),

      plan_intervals AS (
        SELECT DISTINCT plan_interval FROM combined
      ),

      month_spine AS (
        SELECT
        month_start AS month_bucket,
        plan_interval
        FROM UNNEST(GENERATE_DATE_ARRAY(
          DATE('2025-07-01'),
          DATE_TRUNC(CURRENT_DATE(), MONTH),
          INTERVAL 1 MONTH
        )) AS month_start
        CROSS JOIN plan_intervals
      )

      SELECT
        ms.month_bucket,
        ms.plan_interval,
        COUNT(DISTINCT CASE WHEN c.event_type = 'start' THEN c.user_id END) AS subscription_starts,
        COUNT(DISTINCT CASE WHEN c.event_type = 'end'   THEN c.user_id END) AS subscription_ends
      FROM month_spine ms
      LEFT JOIN combined c
      ON c.month_bucket = ms.month_bucket
      AND c.plan_interval = ms.plan_interval
      {% if date_range._is_filtered %}
      WHERE {% condition date_range %} ms.month_bucket {% endcondition %}
      {% endif %}
      GROUP BY ms.month_bucket, ms.plan_interval
      ORDER BY ms.month_bucket, ms.plan_interval
      ;;
  }

  # ——— Filters ———

  filter: date_range {
    type: date
    description: "Filter by month. Use 'is in range' in the UI to pick start and end. Optional."
  }

  # ——— Primary Key ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(CAST(${TABLE}.month_bucket AS STRING), '|', COALESCE(${TABLE}.plan_interval, 'null')) ;;
  }

  # ——— Dimensions ———

  dimension_group: month {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.month_bucket ;;
    timeframes: [date, month, quarter, year]
    label: "Month"
  }

  dimension: plan_interval {
    type: string
    sql: ${TABLE}.plan_interval ;;
    label: "Plan: Interval"
    description: "Product name and billing interval, e.g. 'Launch: month'"
  }

  # ——— Measures ———

  measure: subscription_starts {
    type: sum
    sql: ${TABLE}.subscription_starts ;;
    label: "Subscription Start"
    description: "Count of distinct users who started an MRR-active subscription this month (New + Reactivation), bucketed by first billable-invoice date. Reconciled to within ~±13/month of Stripe New + Reactivation."
  }

  measure: subscription_ends {
    type: sum
    sql: -1 * ${TABLE}.subscription_ends ;;
    label: "Subscription End"
    description: "Count of distinct users with an MRR-active subscription cancelled in this month, bucketed by cancellation date (negated for waterfall display). Reconciled to within ~±5/month of Stripe churn."
  }

  measure: net_subscription_change {
    type: number
    sql: ${subscription_starts} + ${subscription_ends} ;;
    label: "Net Change"
    description: "Subscription Start - Subscription End for the month"
  }
}

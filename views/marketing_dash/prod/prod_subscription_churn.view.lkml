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
          ) AS plan_interval,
          ROW_NUMBER() OVER (
            PARTITION BY t1.user_id
            ORDER BY t1.initial_start_date ASC
          ) AS earliest_sub_rank,
          ROW_NUMBER() OVER (
            PARTITION BY t1.user_id
            ORDER BY t1.initial_start_date DESC
          ) AS most_recent_sub_rank
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
          AND (pprof.email IS NULL OR (
            LOWER(pprof.email) NOT LIKE '%@test.com'
            AND LOWER(pprof.email) NOT LIKE '%@example.com'
            AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
            AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
            AND LOWER(pprof.email) NOT LIKE '%@pop.store'
          ))
      ),

      -- Subscription starts: one row per user, their FIRST subscription
      starts AS (
      SELECT
      DATE_TRUNC(initial_start_date, MONTH) AS month_bucket,
      plan_interval,
      user_id,
      'start' AS event_type
      FROM base_subscriptions
      WHERE earliest_sub_rank = 1
      AND initial_start_date >= DATE('2025-07-01')
      ),

      -- Subscription ends: one row per user, their MOST RECENT subscription,
      -- counted only if cancelled and period ended in the past
      ends AS (
      SELECT
      DATE_TRUNC(DATE(current_period_end), MONTH) AS month_bucket,
      plan_interval,
      user_id,
      'end' AS event_type
      FROM base_subscriptions
      WHERE most_recent_sub_rank = 1
      AND cancelled_at IS NOT NULL
      AND current_period_end < CURRENT_DATE()
      AND initial_start_date >= DATE('2025-07-01')
      ),

      combined AS (
      SELECT * FROM starts
      UNION ALL
      SELECT * FROM ends
      ),

      -- Month spine so months with zero events still appear.
      -- Cross-joined with plan_intervals seen in `combined` so each
      -- (month, plan) pair is present even with zero events.
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
    description: "Count of distinct users whose first-ever subscription started in this month"
  }

  measure: subscription_ends {
    type: sum
    sql: -1 * ${TABLE}.subscription_ends ;;
    label: "Subscription End"
    description: "Count of distinct users whose most recent subscription ended in this month (negated for waterfall display)"
  }

  measure: net_subscription_change {
    type: number
    sql: ${subscription_starts} + ${subscription_ends} ;;
    label: "Net Change"
    description: "Subscription Start - Subscription End for the month"
  }
}

view: prod_subscription_churn {
  derived_table: {
    sql:
      WITH
      extracted_timestamps AS (
        SELECT
          subscription_id,
          TIMESTAMP_SECONDS(
            SAFE_CAST(
              ANY_VALUE(JSON_VALUE(subscription, '$.cancelledAt._seconds'))
              AS INT64
            )
          ) AS cancelled_at_utc,
          MIN(updated_at) AS canceled_at_ts
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription`
        WHERE is_deleted = FALSE
          AND status IN ("unpaid", "canceled")
        GROUP BY subscription_id
      ),
      canceled_status_at AS (
        SELECT
          subscription_id,
          cancelled_at_utc,
          canceled_at_ts,
          CASE
            WHEN cancelled_at_utc IS NULL THEN canceled_at_ts
            WHEN canceled_at_ts IS NULL THEN cancelled_at_utc
            ELSE LEAST(cancelled_at_utc, canceled_at_ts)
          END AS cancelled_at
        FROM extracted_timestamps
      ),

      -- Independent witness for the dunning path, used only as a fallback.
      dunning AS (
        SELECT
          subscription_id,
          MIN(updated_at) AS dunning_end_ts
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
        WHERE is_deleted = FALSE
          AND status IN ('uncollectible', 'void')
        GROUP BY subscription_id
      ),

      latest_subscription AS (
        SELECT * EXCEPT (rn)
        FROM (
          SELECT
            t1.*,
            CONCAT(
              JSON_EXTRACT_SCALAR(plan, '$.productName'),
              ': ',
              JSON_EXTRACT_SCALAR(plan, '$.interval')
            ) AS plan_interval,
            ROW_NUMBER() OVER (
              PARTITION BY t1.subscription_id
              ORDER BY t1.updated_at DESC
            ) AS rn
          FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
          UNNEST(t1.plans) AS plan
          WHERE
            t1.is_deleted = FALSE
            AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
        )
        WHERE rn = 1
      ),

      base_subscriptions AS (
        SELECT
          s.subscription_id,
          s.user_id,
          s.status,
          s.cancelled_at,
          s.cancel_at_period_end,
          s.current_period_end,
          s.plan_interval,
          c.cancelled_at,
          d.dunning_end_ts,

          -- ---- Stripe ended_at -------------------------------------
          CASE
            WHEN s.status NOT IN ("unpaid", "canceled") THEN NULL   -- not terminated yet
            WHEN s.cancel_at_period_end IS TRUE THEN DATE(COALESCE(d.dunning_end_ts, s.current_period_end), 'America/New_York')
            ELSE DATE(
                   COALESCE(
                     c.cancelled_at,   -- authoritative: the status flip
                     d.dunning_end_ts,   -- fallback: invoice went uncollectible
                     s.cancelled_at      -- last resort
                   ),
                   'America/New_York'
                 )
          END AS effective_end_date
          -- ----------------------------------------------------------

        FROM latest_subscription s
        INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` p
          ON p.user_id = s.user_id
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
          ON pprof.user_id = s.user_id
        LEFT JOIN canceled_status_at c
          ON c.subscription_id = s.subscription_id
        LEFT JOIN dunning dn
          ON dn.subscription_id = s.subscription_id
        LEFT JOIN dunning d
          ON d.subscription_id = s.subscription_id
        WHERE p.apps_pop_store = TRUE
          AND p.user_type IN ('seller', 'verifiedSeller')
          AND EXISTS (
            SELECT 1
            FROM UNNEST(s.plans) AS plan
            WHERE JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
          )
          -- AND (pprof.email IS NULL OR NOT REGEXP_CONTAINS(LOWER(pprof.email),
          --      r'@(test\.com|example\.com|popshoplive\.com|pop\.store|commentsold\.com)$'))
      ),

      subscription_mrr AS (
        SELECT
          subscription_id,
          MIN(DATE(COALESCE(created, created_at), 'America/New_York')) AS first_mrr_date
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
        WHERE is_deleted = FALSE
          AND amount_due > 0
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
          DATE_TRUNC(b.effective_end_date, MONTH) AS month_bucket,
          b.plan_interval,
          b.user_id,
          'end' AS event_type
        FROM base_subscriptions b
        INNER JOIN subscription_mrr sm
          ON sm.subscription_id = b.subscription_id
        WHERE b.effective_end_date IS NOT NULL
          AND b.effective_end_date < CURRENT_DATE('America/New_York')
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

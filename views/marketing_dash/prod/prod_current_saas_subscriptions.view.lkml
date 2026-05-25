view: prod_current_saas_subscriptions {
  derived_table: {
    sql:
      SELECT
        t1.subscription_id,
        t1.user_id,
        t1.status,
        JSON_EXTRACT_SCALAR(plan, '$.productName') AS subscription_product_name,
        JSON_EXTRACT_SCALAR(plan, '$.interval')    AS subscription_interval,
        JSON_EXTRACT_SCALAR(plan, '$.planType')    AS plan_type,
        t1.cancelled_at
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS plan
      INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` p
        ON p.user_id = t1.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
        ON pprof.user_id = t1.user_id
      WHERE
        t1.cancelled_at IS NULL
        AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
        AND p.apps_pop_store = TRUE
        AND p.user_type IN ('seller', 'verifiedSeller')
        AND (
          pprof.email IS NULL
          OR NOT REGEXP_CONTAINS(
            LOWER(pprof.email),
            r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'
          )
        )
      ;;
  }

  # ——— Primary Key ———

  dimension: subscription_id {
    type: string
    primary_key: yes
    hidden: yes
    sql: ${TABLE}.subscription_id ;;
  }

  # ——— Dimensions ———

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
    hidden: yes
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    label: "Status"
    description: "Subscription status (active, trialing, past_due, unpaid, etc.)"
  }

  dimension: subscription_product_name {
    type: string
    sql: ${TABLE}.subscription_product_name ;;
    label: "Product Name"
  }

  dimension: subscription_interval {
    type: string
    sql: ${TABLE}.subscription_interval ;;
    label: "Interval"
  }

  dimension: plan_interval {
    type: string
    sql: CONCAT(${TABLE}.subscription_product_name, ': ', ${TABLE}.subscription_interval) ;;
    label: "Plan: Interval"
    description: "Product name and billing interval, e.g. 'Launch: month'"
  }

  dimension_group: cancelled_at {
    type: time
    sql: ${TABLE}.cancelled_at ;;
    timeframes: [date, week, month, year]
    label: "Cancelled"
  }

  # ——— Measures ———

  measure: count_distinct_user_id {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Distinct Subscribers"
    description: "Count of distinct users with a non-cancelled subscription"
    drill_fields: [user_id, status, plan_interval]
  }

  measure: count_subscriptions {
    type: count
    label: "Subscription Count"
    description: "Total subscription rows (one user may have multiple)"
  }
}

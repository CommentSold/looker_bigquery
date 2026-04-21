view: prod_signup_conversion_funnel {
  filter: date_range {
    type: date
    description: "Filter by signup date (month the creator signed up). Use 'is in range' in the UI. Optional."
  }

  derived_table: {
    sql:
      WITH signups AS (
        SELECT
          p.user_id,
          p.username,
          p.url_code,
          MIN(DATE(s.created_at)) AS signup_date
        FROM `popshoplive-26f81.dbt_popshop.dim_profiles` p
        INNER JOIN `popshoplive-26f81.dbt_popshop.dim_stores` s
          ON p.user_id = s.store_id
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
          ON pprof.user_id = p.user_id
        WHERE p.apps_pop_store = TRUE
          AND p.user_type IN ('seller', 'verifiedSeller')
          AND (pprof.email IS NULL
               OR NOT REGEXP_CONTAINS(LOWER(pprof.email),
                    r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com)$'))
        GROUP BY p.user_id, p.username, p.url_code
      ),

      user_subscriptions AS (
      SELECT
      fs.user_id,
      STRING_AGG(DISTINCT fs.subscription_id, ', ' ORDER BY fs.subscription_id) AS all_subscription_ids,
      COUNT(DISTINCT fs.subscription_id) AS total_subs,
      STRING_AGG(DISTINCT fs.status, ', ' ORDER BY fs.status) AS distinct_sub_statuses,
      MAX(fs.trial_end) AS latest_trial_end
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` fs,
      UNNEST(fs.plans) AS plan
      WHERE fs.is_deleted = FALSE
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      GROUP BY fs.user_id
      ),

      user_payment_history AS (
      SELECT
      ssi.user_id,
      COUNT(DISTINCT ssi.invoice_id) AS total_billable_paid_invoices,
      MIN(ssi.created_at) AS first_paid_invoice_at,
      STRING_AGG(DISTINCT ssi.subscription_id, ', ' ORDER BY ssi.subscription_id) AS paid_subscription_ids
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice` ssi
      WHERE ssi.is_deleted = FALSE
      AND ssi.status = 'paid'
      AND ssi.amount_due  > 0
      AND ssi.amount_paid > 0
      GROUP BY ssi.user_id
      )

      SELECT
      s.user_id,
      s.username AS sign_up_user_username,
      'https://pop.store/' || s.url_code AS sign_up_user_url,
      s.signup_date,

      COALESCE(us.total_subs, 0) AS total_subscriptions,
      us.all_subscription_ids,
      us.distinct_sub_statuses AS subscription_statuses,
      DATE(us.latest_trial_end) AS latest_trial_end_date,

      COALESCE(uph.total_billable_paid_invoices, 0) AS total_paid_invoices,
      DATE(uph.first_paid_invoice_at) AS first_paid_date,
      uph.paid_subscription_ids,

      CASE
      WHEN uph.total_billable_paid_invoices > 0
      AND DATE_TRUNC(DATE(uph.first_paid_invoice_at), MONTH) = DATE_TRUNC(s.signup_date, MONTH)
      THEN '1. Paid in signup month'
      WHEN uph.total_billable_paid_invoices > 0
      THEN '2. Paid in a later month'
      WHEN us.total_subs > 0 AND CONTAINS_SUBSTR(us.distinct_sub_statuses, 'active')
      THEN '3. Active sub, no billable invoice yet'
      WHEN us.total_subs > 0 AND CONTAINS_SUBSTR(us.distinct_sub_statuses, 'trialing')
      THEN '4. Currently trialing'
      WHEN us.total_subs > 0 AND CONTAINS_SUBSTR(us.distinct_sub_statuses, 'canceled')
      THEN '5. Cancelled without paying'
      WHEN us.total_subs > 0 AND CONTAINS_SUBSTR(us.distinct_sub_statuses, 'unpaid')
      THEN '6. Payment failed (unpaid)'
      WHEN us.total_subs > 0
      THEN CONCAT('7. Other sub status: ', us.distinct_sub_statuses)
      ELSE '8. Never created a subscription'
      END AS funnel_bucket,

      -- Coarser grouping for cleaner top-level charts
      CASE
      WHEN uph.total_billable_paid_invoices > 0 THEN 'Paid'
      WHEN us.total_subs > 0 AND CONTAINS_SUBSTR(us.distinct_sub_statuses, 'trialing') THEN 'In trial'
      WHEN us.total_subs > 0 AND CONTAINS_SUBSTR(us.distinct_sub_statuses, 'active') THEN 'Active no payment'
      WHEN us.total_subs > 0 THEN 'Subscribed but churned'
      ELSE 'Never subscribed'
      END AS funnel_bucket_coarse

      FROM signups s
      LEFT JOIN user_subscriptions us   ON us.user_id  = s.user_id
      LEFT JOIN user_payment_history uph ON uph.user_id = s.user_id
      WHERE 1=1
      {% if date_range._is_filtered %}
      AND {% condition date_range %} TIMESTAMP(s.signup_date) {% endcondition %}
      {% endif %}
      ;;
  }

  # ——— Primary Key ———

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
    primary_key: yes
  }

  # ——— Signup Date Dimensions ———

  dimension_group: signup {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.signup_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Signup"
    description: "Month the creator signed up — use this as the x-axis"
  }

  # ——— Bucket Dimensions ———

  dimension: funnel_bucket {
    type: string
    sql: ${TABLE}.funnel_bucket ;;
    label: "Funnel Bucket (Detailed)"
    description: "8-bucket classification of what happened after sign-up. Use as Pivot to see breakdown by month."
  }

  dimension: funnel_bucket_coarse {
    type: string
    sql: ${TABLE}.funnel_bucket_coarse ;;
    label: "Funnel Bucket (Coarse)"
    description: "5-bucket simplified version for cleaner top-level charts: Paid, In trial, Active no payment, Subscribed but churned, Never subscribed."
  }

  # ——— Subscription / Payment Detail ———

  dimension: total_subscriptions {
    type: number
    sql: ${TABLE}.total_subscriptions ;;
    label: "Total Subscriptions"
  }

  dimension: subscription_statuses {
    type: string
    sql: ${TABLE}.subscription_statuses ;;
    label: "Subscription Statuses"
    description: "Comma-separated list of all distinct subscription statuses for this user"
  }

  dimension: all_subscription_ids {
    type: string
    sql: ${TABLE}.all_subscription_ids ;;
    label: "All Subscription IDs"
    description: "Use this to cross-reference with Stripe"
  }

  dimension: total_paid_invoices {
    type: number
    sql: ${TABLE}.total_paid_invoices ;;
    label: "Total Paid Invoices"
  }

  dimension_group: first_paid {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.first_paid_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "First Paid"
  }

  dimension_group: latest_trial_end {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.latest_trial_end_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Latest Trial End"
  }

  dimension: paid_subscription_ids {
    type: string
    sql: ${TABLE}.paid_subscription_ids ;;
    label: "Paid Subscription IDs"
  }

  # ——— Profile Dimensions ———

  dimension: sign_up_user_username {
    type: string
    sql: ${TABLE}.sign_up_user_username ;;
  }

  dimension: sign_up_user_url {
    type: string
    sql: ${TABLE}.sign_up_user_url ;;
  }

  # ——— Measures ———

  measure: total_signups {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Total Sign-ups"
    drill_fields: [drill_details*]
  }

  measure: paid_in_signup_month {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [funnel_bucket: "1. Paid in signup month"]
    label: "Paid in Signup Month"
    drill_fields: [drill_details*]
  }

  measure: paid_in_later_month {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [funnel_bucket: "2. Paid in a later month"]
    label: "Paid in a Later Month"
    drill_fields: [drill_details*]
  }

  measure: active_no_invoice_yet {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [funnel_bucket: "3. Active sub, no billable invoice yet"]
    label: "Active Sub, No Invoice Yet"
    drill_fields: [drill_details*]
  }

  measure: currently_trialing {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [funnel_bucket: "4. Currently trialing"]
    label: "Currently Trialing"
    drill_fields: [drill_details*]
  }

  measure: cancelled_without_paying {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [funnel_bucket: "5. Cancelled without paying"]
    label: "Cancelled Without Paying"
    drill_fields: [drill_details*]
  }

  measure: payment_failed {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [funnel_bucket: "6. Payment failed (unpaid)"]
    label: "Payment Failed"
    drill_fields: [drill_details*]
  }

  measure: other_sub_status {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [funnel_bucket: "7. Other sub status: %"]
    label: "Other Sub Status"
    drill_fields: [drill_details*]
  }

  measure: never_subscribed {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [funnel_bucket: "8. Never created a subscription"]
    label: "Never Created a Subscription"
    drill_fields: [drill_details*]
  }

  # ——— Rate measures ———

  measure: signup_to_subscribe_rate {
    type: number
    sql: SAFE_DIVIDE(${total_signups} - ${never_subscribed}, NULLIF(${total_signups}, 0)) * 100 ;;
    label: "Signup → Subscribe Rate (%)"
    description: "Percent of sign-ups who created any subscription"
    value_format_name: decimal_1
  }

  measure: signup_to_paid_rate {
    type: number
    sql: SAFE_DIVIDE(${paid_in_signup_month} + ${paid_in_later_month}, NULLIF(${total_signups}, 0)) * 100 ;;
    label: "Signup → Paid Rate (%)"
    description: "Percent of sign-ups who ever paid a real invoice"
    value_format_name: decimal_1
  }

  # ——— Drill Set ———

  set: drill_details {
    fields: [
      user_id,
      sign_up_user_username,
      sign_up_user_url,
      signup_date,
      funnel_bucket,
      funnel_bucket_coarse,
      total_subscriptions,
      subscription_statuses,
      all_subscription_ids,
      total_paid_invoices,
      first_paid_date,
      latest_trial_end_date,
      paid_subscription_ids
    ]
  }
}

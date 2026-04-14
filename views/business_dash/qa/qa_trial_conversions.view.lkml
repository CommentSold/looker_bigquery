view: qa_trial_conversions {
  derived_table: {
    sql:
      WITH subscriptions AS (
        SELECT
          subscription_id,
          trial_end AS trial_end_ts,
          status AS sub_status
        FROM `dbt_popshop.fact_seller_subscription`
        WHERE is_deleted = FALSE
      ),

      invoice_history AS (
      SELECT
      invoice_id,
      subscription_id,
      status,
      amount_due,
      amount_paid,
      created_at,
      updated_at
      FROM `dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      ),

      invoice_rollup AS (
      SELECT
      invoice_id,
      subscription_id,
      MIN(created_at) AS invoice_created_at,
      MAX(amount_due) AS max_amount_due,
      MAX(amount_paid) AS max_amount_paid
      FROM invoice_history
      GROUP BY 1, 2
      ),

      -- Only invoices where amount_due > 0 (excludes $0 trial invoices)
      billable_invoices AS (
      SELECT
      invoice_id,
      subscription_id,
      invoice_created_at
      FROM invoice_rollup
      WHERE max_amount_due > 0
      ),

      -- PAID: first paid event per billable invoice
      first_paid_event_per_invoice AS (
      SELECT
      ih.invoice_id,
      ih.subscription_id,
      ih.updated_at AS paid_at,
      ROW_NUMBER() OVER (
      PARTITION BY ih.invoice_id
      ORDER BY ih.updated_at ASC
      ) AS rn
      FROM invoice_history ih
      JOIN billable_invoices bi
      ON ih.invoice_id = bi.invoice_id
      AND ih.subscription_id = bi.subscription_id
      WHERE ih.status = 'paid'
      AND ih.amount_paid > 0
      ),

      paid_billable_invoices AS (
      SELECT
      invoice_id,
      subscription_id,
      paid_at
      FROM first_paid_event_per_invoice
      WHERE rn = 1
      ),

      first_paid_conversion_per_sub AS (
      SELECT
      s.subscription_id,
      p.paid_at,
      ROW_NUMBER() OVER (
      PARTITION BY s.subscription_id
      ORDER BY p.paid_at ASC
      ) AS rn
      FROM subscriptions s
      JOIN paid_billable_invoices p
      ON s.subscription_id = p.subscription_id
      AND p.paid_at >= s.trial_end_ts
      ),

      -- UNPAID: subscriptions with status='unpaid' that have a billable invoice
      -- but did NOT appear in paid_conversions
      unpaid_subs AS (
      SELECT
      s.subscription_id,
      bi.invoice_created_at AS event_at,
      'unpaid' AS invoice_status,
      ROW_NUMBER() OVER (
      PARTITION BY s.subscription_id
      ORDER BY bi.invoice_created_at ASC
      ) AS rn
      FROM subscriptions s
      JOIN billable_invoices bi
      ON bi.subscription_id = s.subscription_id
      AND bi.invoice_created_at >= s.trial_end_ts
      WHERE s.sub_status = 'unpaid'
      AND s.subscription_id NOT IN (
      SELECT subscription_id FROM first_paid_conversion_per_sub WHERE rn = 1
      )
      ),

      unpaid_conversions AS (
      SELECT
      subscription_id,
      event_at,
      invoice_status
      FROM unpaid_subs
      WHERE rn = 1
      ),

      paid_conversions AS (
      SELECT
      subscription_id,
      paid_at AS event_at,
      'paid' AS invoice_status
      FROM first_paid_conversion_per_sub
      WHERE rn = 1
      ),

      -- UNION paid + unpaid only
      combined AS (
      SELECT * FROM paid_conversions
      UNION ALL
      SELECT * FROM unpaid_conversions
      )

      SELECT
      combined.subscription_id,
      combined.event_at,
      combined.invoice_status,

      -- Drilldown fields from related tables
      fs.user_id,
      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email AS sign_up_user_email,
      oe.context_campaign_campaign AS marketing_campaign,
      oe.utm_regintent,
      oe.business_type,

      COALESCE(fs.discounted_price, fs.price + fs.tax_amount) AS price,
      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval') AS plan_interval,
      fs.initial_start_date AS trial_starts,
      fs.trial_end AS trial_ends,

      COALESCE(
      CASE
      WHEN fs.cancellation_applied_at IS NOT NULL
      AND fs.cancellation_applied_at < fs.trial_end
      THEN fs.cancellation_applied_at
      END,
      fs.trial_end
      ) AS effective_trial_end,

      CASE
      WHEN fs.trial_end IS NULL THEN 'No trial'
      WHEN DATE(COALESCE(
      CASE
      WHEN fs.cancellation_applied_at IS NOT NULL
      AND fs.cancellation_applied_at < fs.trial_end
      THEN fs.cancellation_applied_at
      END,
      fs.trial_end
      )) <= CURRENT_DATE() THEN 'Ended'
      ELSE 'Started'
      END AS trial_status,

      CASE
      WHEN oe.user_id IS NULL THEN 'event_not_fired'
      WHEN oe.context_campaign_campaign IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source

      FROM combined

      JOIN `dbt_popshop.fact_seller_subscription` fs
      ON fs.subscription_id = combined.subscription_id
      AND fs.is_deleted = FALSE

      CROSS JOIN UNNEST(fs.plans) AS plan

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = fs.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = fs.user_id

      LEFT JOIN (
      SELECT *
      FROM (
      SELECT *,
      ROW_NUMBER() OVER (PARTITION BY user_id) AS rn
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      )
      WHERE rn = 1
      ) oe
      ON oe.user_id = fs.user_id

      WHERE
      JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND (pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      ))
      {% if date_range._is_filtered %}
      AND {% condition date_range %} TIMESTAMP(fs.initial_start_date) {% endcondition %}
      {% endif %}
      ;;
  }

  # ——— Filters ———

  filter: date_range {
    type: date
    description: "Filter by trial start date. Use 'is in range' in the UI to pick start and end. Optional."
  }

  # ——— Dimensions ———

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(${TABLE}.subscription_id, '-', ${TABLE}.invoice_status) ;;
  }

  dimension_group: event {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    datatype: timestamp
    sql: ${TABLE}.event_at ;;
    description: "Timestamp of the conversion event (paid_at for paid, invoice_created_at for unpaid)"
  }

  dimension: invoice_status {
    type: string
    sql: ${TABLE}.invoice_status ;;
    description: "Invoice status: paid or unpaid only."
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
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

  dimension: acquisition_source {
    type: string
    sql: ${TABLE}.acquisition_source ;;
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

  # ——— Measures ———

  measure: trial_conversion_count {
    type: count
    description: "Count of subscriptions by invoice status (paid or unpaid)"
    drill_fields: [drilldown_details*]
  }

  measure: paid_converted_trials {
    type: count
    filters: [invoice_status: "paid"]
    description: "Count of subscriptions that converted from trial to paid"
    drill_fields: [drilldown_details*]
  }

  measure: unpaid_trials {
    type: count
    filters: [invoice_status: "unpaid"]
    description: "Count of subscriptions marked unpaid at subscription level (payment failed)"
    drill_fields: [drilldown_details*]
  }

  measure: paid_converted_trials_last_28_days {
    type: count
    filters: [invoice_status: "paid", trial_starts_at_date: "28 days"]
    description: "Count of trial-to-paid conversions in the last 28 days, by trial start date"
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
      invoice_status,
      plan_name,
      plan_interval,
      price,
      trial_status,
      trial_starts_at_date,
      trial_ends_at_date,
      effective_trial_ends_at_date,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type
    ]
  }
}

view: prod_paid_subscription_cancellations {
  derived_table: {
    sql:
      WITH subscriptions AS (
        SELECT
          subscription_id,
          s.user_id,
          initial_start_date,
          trial_end,
          cancelled_at,
          cancellation_applied_at,
          s.updated_at,
          current_period_start,
          current_period_end,
          status,
          discounted_price,
          price,
          tax_amount,
          plans
        FROM `dbt_popshop.fact_seller_subscription` s
        INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` p
          ON p.user_id = s.user_id
        WHERE s.is_deleted = FALSE
          AND s.trial_end IS NOT NULL
          AND p.apps_pop_store = TRUE
          AND p.user_type IN ('seller', 'verifiedSeller')
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

      -- Only invoices that represent real billing attempts (excludes $0 trial invoices)
      billable_invoices AS (
      SELECT
      invoice_id,
      subscription_id,
      invoice_created_at
      FROM invoice_rollup
      WHERE max_amount_due > 0
      ),

      -- For each billable invoice, find the first 'paid' event
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

      -- Per subscription: did they ever successfully pay a billable invoice
      -- AFTER trial_end? Track count and first/last paid timestamps.
      sub_paid_history AS (
      SELECT
      s.subscription_id,
      COUNT(DISTINCT p.invoice_id) AS successful_paid_invoice_count,
      MIN(p.paid_at) AS first_paid_at,
      MAX(p.paid_at) AS last_paid_at
      FROM subscriptions s
      JOIN paid_billable_invoices p
      ON p.subscription_id = s.subscription_id
      AND p.paid_at >= s.trial_end
      GROUP BY 1
      ),

      -- The candidate set: any non-active subscription that had >=1 successful
      -- post-trial paid invoice. This is the "paid then cancelled" cohort,
      -- whether stripe marked them 'unpaid' (still retrying / gave up) or the
      -- subscription eventually flipped to 'canceled'.
      paid_then_cancelled AS (
      SELECT
      s.subscription_id,
      s.user_id,
      s.initial_start_date,
      s.trial_end,
      s.cancelled_at,
      s.cancellation_applied_at,
      s.updated_at,
      s.current_period_start,
      s.current_period_end,
      s.status,
      s.discounted_price,
      s.price,
      s.tax_amount,
      s.plans,
      ph.successful_paid_invoice_count,
      ph.first_paid_at,
      ph.last_paid_at
      FROM subscriptions s
      JOIN sub_paid_history ph
      ON ph.subscription_id = s.subscription_id
      WHERE s.status != 'active'
      AND ph.successful_paid_invoice_count >= 1
      )

      SELECT
      ptc.subscription_id,
      ptc.user_id,
      ptc.status AS subscription_status,
      ptc.successful_paid_invoice_count,
      ptc.first_paid_at,
      ptc.last_paid_at,

      -- Anchor on trial_starts (initial_start_date) per requirement: aligns with
      -- prod_trial_conversions and prod_trial_cancellations on the same time axis.
      ptc.initial_start_date AS trial_starts,
      ptc.trial_end AS trial_ends,

      COALESCE(
      CASE
      WHEN ptc.cancellation_applied_at IS NOT NULL
      AND ptc.cancellation_applied_at < ptc.trial_end
      THEN ptc.cancellation_applied_at
      END,
      ptc.trial_end
      ) AS effective_trial_end,

      -- Date we treat the paid subscription as having "ended". Prefer the
      -- explicit cancelled_at; fall back to current_period_end (for unpaid
      -- subs that are still in dunning the period_end is when access lapsed);
      -- finally fall back to updated_at.
      DATE(COALESCE(
        ptc.cancelled_at,
        CASE
          WHEN ptc.current_period_end < CURRENT_TIMESTAMP()
          THEN ptc.current_period_end
        END,
        ptc.updated_at
      )) AS subscription_cancellation_date,

      CASE
      WHEN ptc.status = 'unpaid' THEN 'payment_failed'
      WHEN ptc.status = 'past_due' THEN 'payment_retrying'
      WHEN ptc.status = 'canceled' THEN 'cancelled'
      WHEN ptc.status = 'incomplete_expired' THEN 'payment_failed'
      ELSE ptc.status
      END AS cancellation_reason,

      -- Drilldown fields
      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      pprof.email AS sign_up_user_email,
      JSON_VALUE(pprof.private_profile, '$.email') AS profile_email,
      JSON_VALUE(pprof.private_profile, '$.sellerShippingAddress.firstName') AS first_name,
      JSON_VALUE(pprof.private_profile, '$.sellerShippingAddress.lastName')  AS last_name,

      oe.context_campaign_campaign AS marketing_campaign,
      oe.utm_regintent,
      oe.business_type,
      oe.context_campaign_onboarding_path AS onboarding_path,
      oe.context_campaign_planlevel AS plan_level,
      oe.context_user_agent AS user_agent,
      CASE
      WHEN REGEXP_CONTAINS(LOWER(oe.context_user_agent), r'(bot|crawler|spider|crawl|slurp|googlebot|bingpreview|facebookexternalhit|twitterbot|linkedinbot|discordbot|telegrambot|google-read-aloud)') THEN 'BOT'
      WHEN REGEXP_CONTAINS(LOWER(oe.context_user_agent), r'(wv|webview|meta-iab|metaiab|facebook|fban|fbav|instagram|iabmv/1|whatsapp|line|linkedinapp|snapchat|gsa/|googleapp/|youtube|tiktok|reddit)') THEN 'WEBVIEW'
      WHEN REGEXP_CONTAINS(LOWER(oe.context_user_agent), r'(iphone|ipad|ipod|cpu iphone os|cpu os)') THEN 'IOS'
      WHEN REGEXP_CONTAINS(LOWER(oe.context_user_agent), r'android') THEN 'ANDROID'
      WHEN REGEXP_CONTAINS(LOWER(oe.context_user_agent), r'(windows nt|win64|wow64)') THEN 'WINDOWS_DESKTOP'
      WHEN REGEXP_CONTAINS(LOWER(oe.context_user_agent), r'(macintosh|mac os x)') AND NOT REGEXP_CONTAINS(LOWER(oe.context_user_agent), r'(iphone|ipad)') THEN 'MACOS_DESKTOP'
      WHEN REGEXP_CONTAINS(LOWER(oe.context_user_agent), r'(linux|x11)') AND NOT REGEXP_CONTAINS(LOWER(oe.context_user_agent), r'android') THEN 'LINUX_DESKTOP'
      ELSE 'OTHER'
      END AS device_category,

      COALESCE(ptc.discounted_price, ptc.price + ptc.tax_amount) AS price,
      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval') AS plan_interval,

      CASE
      WHEN oe.user_id IS NULL THEN 'event_not_fired'
      WHEN oe.context_campaign_campaign IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source

      FROM paid_then_cancelled ptc

      CROSS JOIN UNNEST(ptc.plans) AS plan

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = ptc.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = ptc.user_id

      LEFT JOIN (
        SELECT
          user_id,
          context_campaign_campaign,
          context_campaign_onboarding_path,
          context_campaign_planlevel,
          context_user_agent,
          utm_regintent,
          business_type
        FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
        WHERE (scene = 'onboarding' OR scene IS NULL)
          AND (step_name = 'onboarding_complete' OR step_name IS NULL)
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY user_id
          ORDER BY
            CASE
              WHEN context_campaign_campaign IS NOT NULL
                OR (utm_regintent IS NOT NULL AND utm_regintent != 'generic')
                OR (business_type IS NOT NULL AND business_type != 'generic')
              THEN 0 ELSE 1
            END,
            `timestamp` DESC
        ) = 1
      ) oe
      ON oe.user_id = ptc.user_id

      WHERE
      JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND (pprof.email IS NULL OR (
      LOWER(pprof.email) NOT LIKE '%@test.com'
      AND LOWER(pprof.email) NOT LIKE '%@example.com'
      AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
      AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
      AND LOWER(pprof.email) NOT LIKE '%@pop.store'
      ))
      {% if date_range._is_filtered %}
      AND {% condition date_range %} TIMESTAMP(ptc.initial_start_date) {% endcondition %}
      {% endif %}
      ;;
  }

  # ——— Filters ———

  filter: date_range {
    type: date
    description: "Filter by trial start date (initial_start_date). Use 'is in range' in the UI to pick start and end. Optional."
  }

  # ——— Dimensions ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
    description: "Current subscription status (unpaid, canceled, etc.). Excludes 'active'."
  }

  dimension: cancellation_reason {
    type: string
    sql: ${TABLE}.cancellation_reason ;;
    description: "payment_failed = status 'unpaid' (Stripe retries exhausted / insufficient funds / declined). cancelled = explicitly canceled after at least one successful payment."
  }

  dimension: successful_paid_invoice_count {
    type: number
    sql: ${TABLE}.successful_paid_invoice_count ;;
    description: "Number of successful billable invoices paid after trial_end. Always >= 1 in this view."
  }

  dimension_group: first_paid_at {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.first_paid_at ;;
    description: "Timestamp of the first successful post-trial payment."
  }

  dimension_group: last_paid_at {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.last_paid_at ;;
    description: "Timestamp of the most recent successful post-trial payment."
  }

  dimension_group: subscription_cancelled {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.subscription_cancellation_date ;;
    timeframes: [date, week, month, quarter, year]
    description: "Date the paid subscription ended (cancelled_at, else current_period_end, else updated_at)."
  }

  # Primary time axis (matches prod_trial_conversions / prod_trial_cancellations)
  dimension_group: trial_starts_at {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.trial_starts ;;
    description: "Trial start date (initial_start_date). Use this as the chart x-axis to align with the trial conversions / cancellations views."
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

  dimension: sign_up_user_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.sign_up_url_code ;;
  }

  dimension: profile_email {
    type: string
    sql: ${TABLE}.profile_email ;;
    label: "Profile Email (JSON)"
    description: "Email pulled from private_profile JSON ($.email). May differ from sign_up_user_email."
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
    label: "First Name"
    description: "From private_profile.sellerShippingAddress.firstName. NULL if user has not set a shipping address."
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
    label: "Last Name"
    description: "From private_profile.sellerShippingAddress.lastName. NULL if user has not set a shipping address."
  }

  dimension: full_name {
    type: string
    sql: TRIM(CONCAT(COALESCE(${TABLE}.first_name, ''), ' ', COALESCE(${TABLE}.last_name, ''))) ;;
    label: "Full Name"
    description: "Concatenated first_name + last_name from shipping address. Empty when neither is set."
  }

  dimension: sign_up_user_username {
    type: string
    sql: ${TABLE}.sign_up_user_username ;;
  }

  dimension: sign_up_user_email {
    type: string
    sql: ${TABLE}.sign_up_user_email ;;
    description: "Email from dim_private_profiles.email column"
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

  dimension: onboarding_path {
    type: string
    sql: ${TABLE}.onboarding_path ;;
  }

  dimension: plan_level {
    type: string
    sql: ${TABLE}.plan_level ;;
  }

  dimension: device_category {
    type: string
    sql: ${TABLE}.device_category ;;
  }

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
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

  # ——— Measures ———

  measure: paid_subscriptions_cancelled {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    label: "Paid Subscriptions Cancelled"
    description: "Distinct subscriptions that had >=1 successful post-trial payment and are now non-active (payment failed / cancelled)."
    drill_fields: [drilldown_details*]
  }

  measure: payment_failed_count {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [cancellation_reason: "payment_failed"]
    label: "Payment Failed (Stripe Unpaid)"
    description: "Paid subs that flipped to 'unpaid' after Stripe exhausted retry attempts (insufficient funds, declined, etc.)."
    drill_fields: [drilldown_details*]
  }

  measure: explicitly_cancelled_count {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [cancellation_reason: "cancelled"]
    label: "Explicitly Cancelled (Post-Paid)"
    description: "Paid subs that were explicitly cancelled (status = 'canceled') after at least one successful payment."
    drill_fields: [drilldown_details*]
  }

  measure: payment_retrying_count {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [cancellation_reason: "payment_retrying"]
    label: "Payment Retrying (Past Due)"
    description: "Paid subs in Stripe dunning — payment failed but retries haven't exhausted yet. May recover or roll into payment_failed."
    drill_fields: [drilldown_details*]
  }

  # ——— Drill Set ———

  set: drilldown_details {
    fields: [
      user_id,
      first_name,
      last_name,
      profile_email,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      subscription_id,
      subscription_status,
      cancellation_reason,
      successful_paid_invoice_count,
      first_paid_at_time,
      last_paid_at_time,
      subscription_cancelled_date,
      plan_name,
      plan_interval,
      price,
      trial_starts_at_time,
      trial_ends_at_date,
      effective_trial_ends_at_date,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type,
      onboarding_path,
      plan_level,
      device_category,
      user_agent
    ]
  }
}

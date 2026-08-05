view: prod_paid_subscription_cancellations {
  derived_table: {
    sql:
      WITH
      -- ============================================================
      -- 1) ONE ROW PER SUBSCRIPTION (latest state), ONE PLAN ROW.
      --    fact_seller_subscription stores change history, so reading it
      --    raw fans out and lets stale status/timestamp rows through.
      --    This mirrors `latest_subscription` in prod_subscription_churn.
      -- ============================================================
      latest_subscription AS (
        SELECT * EXCEPT (rn)
        FROM (
          SELECT
            t1.*,
            JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
            JSON_EXTRACT_SCALAR(plan, '$.interval')    AS plan_interval,
            CONCAT(
              JSON_EXTRACT_SCALAR(plan, '$.productName'),
              ': ',
              JSON_EXTRACT_SCALAR(plan, '$.interval')
            ) AS plan_interval_label,
            ROW_NUMBER() OVER (
              PARTITION BY t1.subscription_id
              ORDER BY t1.updated_at DESC
            ) AS rn
          FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
          UNNEST(t1.plans) AS plan
          WHERE t1.is_deleted = FALSE
            AND trial_end IS NOT NULL
            AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
        )
        WHERE rn = 1
      ),

      -- ============================================================
      -- 2) AUTHORITATIVE CHURN TIMESTAMP.
      --    Identical to prod_subscription_churn: the embedded Stripe
      --    timestamp ($.cancelledAt._seconds) LEAST-ed against the first
      --    updated_at on which the row carried a terminal status.
      -- ============================================================
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
      AND trial_end IS NOT NULL
      AND status IN ('unpaid', 'canceled')
      GROUP BY subscription_id
      ),

      canceled_status_at AS (
      SELECT
      subscription_id,
      CASE
      WHEN cancelled_at_utc IS NULL THEN canceled_at_ts
      WHEN canceled_at_ts   IS NULL THEN cancelled_at_utc
      ELSE LEAST(cancelled_at_utc, canceled_at_ts)
      END AS status_flip_cancelled_at
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

      subscriptions AS (
      SELECT
      s.subscription_id,
      s.user_id,
      -- initial_start_date / trial_end / cancellation_applied_at do not all
      -- share a type upstream (one is DATE, the others TIMESTAMP), which
      -- breaks COALESCE and any comparison against invoice timestamps.
      -- Normalise here once; CAST is a no-op for columns already TIMESTAMP.
      CAST(s.initial_start_date AS TIMESTAMP)      AS initial_start_date,
      CAST(s.trial_end AS TIMESTAMP)               AS trial_end,
      s.cancelled_at AS raw_cancelled_at,
      CAST(s.cancellation_applied_at AS TIMESTAMP) AS cancellation_applied_at,
      s.updated_at,
      s.current_period_start,
      s.current_period_end,
      s.cancel_at_period_end,
      s.status,
      s.discounted_price,
      s.price,
      s.tax_amount,
      s.plan_name,
      s.plan_interval,
      s.plan_interval_label,
      c.status_flip_cancelled_at,
      d.dunning_end_ts
      FROM latest_subscription s
      INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` p
      ON p.user_id = s.user_id
      LEFT JOIN canceled_status_at c
      ON c.subscription_id = s.subscription_id
      LEFT JOIN dunning d
      ON d.subscription_id = s.subscription_id
      WHERE p.apps_pop_store = TRUE
      AND p.user_type IN ('seller', 'verifiedSeller')
      ),

      -- ============================================================
      -- 3) "EVER SUCCESSFULLY PAID" GATE
      --    Payment floor is COALESCE(trial_end, initial_start_date) so
      --    non-trial subscriptions are no longer silently dropped.
      -- ============================================================
      invoice_history AS (
      SELECT
      invoice_id,
      subscription_id,
      status,
      amount_due,
      amount_paid,
      -- Normalised to TIMESTAMP so the payment-floor comparison below can
      -- never hit a DATE/TIMESTAMP supertype error. CAST is a no-op when
      -- the column is already a TIMESTAMP.
      CAST(created_at AS TIMESTAMP) AS created_at,
      CAST(updated_at AS TIMESTAMP) AS updated_at
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      ),

      invoice_rollup AS (
      SELECT
      invoice_id,
      subscription_id,
      MIN(created_at)  AS invoice_created_at,
      MAX(amount_due)  AS max_amount_due,
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
      ON ih.invoice_id     = bi.invoice_id
      AND ih.subscription_id = bi.subscription_id
      WHERE ih.status = 'paid'
      AND ih.amount_paid > 0
      ),

      paid_billable_invoices AS (
      SELECT invoice_id, subscription_id, paid_at
      FROM first_paid_event_per_invoice
      WHERE rn = 1
      ),

      sub_paid_history AS (
      SELECT
      s.subscription_id,
      COUNT(DISTINCT p.invoice_id) AS successful_paid_invoice_count,
      MIN(p.paid_at) AS first_paid_at,
      MAX(p.paid_at) AS last_paid_at
      FROM subscriptions s
      JOIN paid_billable_invoices p
      ON p.subscription_id = s.subscription_id
      AND p.paid_at >= COALESCE(s.trial_end, s.initial_start_date)
      GROUP BY 1
      ),

      -- ============================================================
      -- 4) CANDIDATE SET + EFFECTIVE END DATE
      --    effective_end_date is byte-for-byte the prod_subscription_churn
      --    expression. past_due deliberately resolves to NULL: the
      --    subscription stays open until an explicit cancellation.
      -- ============================================================
      paid_then_cancelled AS (
      SELECT
      s.*,
      ph.successful_paid_invoice_count,
      ph.first_paid_at,
      ph.last_paid_at,
      CASE
      WHEN s.status NOT IN ('unpaid', 'canceled') THEN NULL
      WHEN s.cancel_at_period_end IS TRUE
      THEN DATE(COALESCE(s.dunning_end_ts, s.current_period_end), 'America/New_York')
      ELSE DATE(
      COALESCE(
      s.status_flip_cancelled_at, -- authoritative: the status flip
      s.dunning_end_ts,           -- fallback: invoice went uncollectible
      s.raw_cancelled_at          -- last resort
      ),
      'America/New_York'
      )
      END AS effective_end_date
      FROM subscriptions s
      JOIN sub_paid_history ph
      ON ph.subscription_id = s.subscription_id
      WHERE s.status IN ('canceled', 'unpaid', 'past_due', 'incomplete_expired')
      AND ph.successful_paid_invoice_count >= 1
      ),

      onboarding_events AS (
      SELECT
      context_campaign_campaign        AS marketing_campaign,
      context_campaign_onboarding_path AS onboarding_path,
      context_campaign_planlevel       AS plan_level,
      context_user_agent               AS user_agent,
      utm_regintent,
      business_type,
      `timestamp`,
      user_id,
      scene,
      step_name,
      onboarding_session_id,
      CASE
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(bot|crawler|spider|crawl|slurp|googlebot|bingpreview|facebookexternalhit|twitterbot|linkedinbot|discordbot|telegrambot|google-read-aloud)') THEN 'BOT'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(wv|webview|meta-iab|metaiab|facebook|fban|fbav|instagram|iabmv/1|whatsapp|line|linkedinapp|snapchat|gsa/|googleapp/|youtube|tiktok|reddit)') THEN 'WEBVIEW'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(iphone|ipad|ipod|cpu iphone os|cpu os)') THEN 'IOS'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'android') THEN 'ANDROID'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(windows nt|win64|wow64)') THEN 'WINDOWS_DESKTOP'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(macintosh|mac os x)') AND NOT REGEXP_CONTAINS(LOWER(context_user_agent), r'(iphone|ipad)') THEN 'MACOS_DESKTOP'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(linux|x11)') AND NOT REGEXP_CONTAINS(LOWER(context_user_agent), r'android') THEN 'LINUX_DESKTOP'
      ELSE 'OTHER'
      END AS device_category
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      WHERE (scene = 'onboarding' OR scene IS NULL)
      AND (step_name = 'onboarding_complete' OR step_name IS NULL)
      ),

      onboarding_events_dedup AS (
      SELECT
      user_id,
      marketing_campaign,
      onboarding_path,
      plan_level,
      utm_regintent,
      business_type,
      `timestamp`,
      onboarding_session_id,
      device_category,
      user_agent
      FROM onboarding_events
      QUALIFY ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY
      CASE
      WHEN marketing_campaign IS NOT NULL
      OR (utm_regintent IS NOT NULL AND utm_regintent != 'generic')
      OR (business_type  IS NOT NULL AND business_type  != 'generic')
      THEN 0 ELSE 1
      END,
      `timestamp` DESC
      ) = 1
      ),

      marketing_capture AS (
      SELECT
      user_id,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_campaign')       AS utm_campaign,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_source')         AS utm_source,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent')      AS utm_regintent,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.url')                AS url,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')         AS user_agent,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_onboarding_path') AS onboarding_path,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_planlevel')      AS plan_level,
      JSON_VALUE(private_profile, '$.email')                                        AS profile_email,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.firstName')               AS first_name,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.lastName')                AS last_name,
      COALESCE(
      CASE
      WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(bot|crawler|spider|crawl|slurp|googlebot|bingpreview|facebookexternalhit|twitterbot|linkedinbot|discordbot|telegrambot|google-read-aloud)') THEN 'BOT'
      WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'instagram') THEN 'WEBVIEW_INSTAGRAM'
      WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(fban|fbav|facebook)') THEN 'WEBVIEW_FACEBOOK'
      WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'tiktok') THEN 'WEBVIEW_TIKTOK'
      WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'snapchat') THEN 'WEBVIEW_SNAPCHAT'
      WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(linkedin|linkedinapp)') THEN 'WEBVIEW_LINKEDIN'
      WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(wv|webview|meta-iab|metaiab|iabmv/1|whatsapp|line|gsa/|googleapp/|youtube|reddit)') THEN 'WEBVIEW_OTHER'
      WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(iphone|ipad|ipod|cpu iphone os|cpu os)') THEN 'IOS'
      WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'android') THEN 'ANDROID'
      WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(windows nt|win64|wow64)') THEN 'WINDOWS_DESKTOP'
      WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(macintosh|mac os x)') AND NOT REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(iphone|ipad)') THEN 'MACOS_DESKTOP'
      WHEN REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'(linux|x11)') AND NOT REGEXP_CONTAINS(LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')), r'android') THEN 'LINUX_DESKTOP'
      ELSE 'OTHER'
      END,
      "No Onboarding Event"
      ) AS device_category
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      )

      SELECT
      ptc.subscription_id,
      ptc.user_id,
      ptc.status AS subscription_status,
      ptc.successful_paid_invoice_count,
      ptc.first_paid_at,
      ptc.last_paid_at,

      -- PRIMARY TIME AXIS: the churn date, computed exactly as in
      -- prod_subscription_churn (America/New_York, dunning-aware).
      ptc.effective_end_date AS subscription_cancellation_date,
      ptc.effective_end_date IS NOT NULL AS is_churned,

      -- Secondary / cohort dimensions
      ptc.initial_start_date AS trial_starts,
      ptc.trial_end          AS trial_ends,

      COALESCE(
      CASE
      WHEN ptc.cancellation_applied_at IS NOT NULL
      AND ptc.cancellation_applied_at < COALESCE(ptc.trial_end, ptc.initial_start_date)
      THEN ptc.cancellation_applied_at
      END,
      ptc.trial_end,
      ptc.initial_start_date
      ) AS effective_trial_end,

      CASE
      WHEN ptc.status = 'unpaid' THEN 'payment_failed'
      WHEN ptc.status = 'incomplete_expired' THEN 'payment_failed'
      WHEN ptc.status = 'past_due' THEN 'payment_retrying'
      WHEN ptc.status = 'canceled'
      AND ptc.cancellation_applied_at IS NULL
      AND ptc.dunning_end_ts IS NOT NULL THEN 'payment_failed'
      WHEN ptc.status = 'canceled' THEN 'cancelled'
      ELSE ptc.status
      END AS cancellation_reason,

      -- Drilldown fields
      prof.url_code  AS sign_up_url_code,
      prof.username  AS sign_up_user_username,
      pprof.email    AS sign_up_user_email,
      mc.profile_email,
      mc.first_name,
      mc.last_name,
      COALESCE(oe.marketing_campaign, mc.utm_campaign)                            AS marketing_campaign,
      COALESCE(oe.utm_regintent,      mc.utm_regintent)                           AS utm_regintent,
      COALESCE(oe.business_type,      JSON_VALUE(prof.profile, '$.businessType')) AS business_type,
      COALESCE(oe.onboarding_path,    mc.onboarding_path)                         AS onboarding_path,
      COALESCE(oe.plan_level,         mc.plan_level)                              AS plan_level,
      COALESCE(oe.device_category,    mc.device_category)                         AS device_category,
      COALESCE(oe.user_agent,         mc.user_agent)                              AS user_agent,

      COALESCE(ptc.discounted_price, ptc.price + ptc.tax_amount) AS price,
      ptc.plan_name,
      ptc.plan_interval,
      ptc.plan_interval_label,

      CASE
      WHEN COALESCE(oe.marketing_campaign, mc.utm_campaign) IS NOT NULL THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source

      FROM paid_then_cancelled ptc

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = ptc.user_id

      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = ptc.user_id

      LEFT JOIN marketing_capture mc
      ON mc.user_id = ptc.user_id

      LEFT JOIN onboarding_events_dedup oe
      ON oe.user_id = ptc.user_id

      WHERE
      -- Match prod_subscription_churn: never bucket a future-dated end.
      (ptc.effective_end_date IS NULL
      OR ptc.effective_end_date < CURRENT_DATE('America/New_York'))
      AND (pprof.email IS NULL OR NOT REGEXP_CONTAINS(LOWER(pprof.email),
        r'@(test\.com|example\.com|popshoplive\.com|pop\.store|commentsold\.com)$'))
      {% if date_range._is_filtered %}
      -- effective_end_date is a DATE (America/New_York); Looker emits
      -- TIMESTAMP literals for a date filter, so cast to match. Paired with
      -- convert_tz: no on the filter so both sides are UTC-midnight and the
      -- month boundaries line up exactly.
      AND {% condition date_range %} TIMESTAMP(ptc.effective_end_date) {% endcondition %}
      {% endif %}
      ;;
  }

  # ——— Filters ———

  filter: date_range {
    type: date
    convert_tz: no
    description: "Filter by SUBSCRIPTION CANCELLATION DATE (the churn date, America/New_York). Use 'is in range' in the UI to pick start and end. Optional."
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
    description: "Latest subscription status: canceled, unpaid, past_due, incomplete_expired."
  }

  dimension: is_churned {
    type: yesno
    sql: ${TABLE}.is_churned ;;
    description: "Yes when the subscription has a terminal status (canceled/unpaid) and therefore a cancellation date. past_due subs are No — they stay open until explicit cancellation."
  }

  dimension: cancellation_reason {
    type: string
    sql: ${TABLE}.cancellation_reason ;;
    description: "payment_failed = 'unpaid'/'incomplete_expired', or 'canceled' with a dunning (void/uncollectible) invoice and no user-initiated cancellation. cancelled = user-initiated cancellation after at least one successful payment. payment_retrying = still in dunning (no cancellation date yet)."
  }

  dimension: successful_paid_invoice_count {
    type: number
    sql: ${TABLE}.successful_paid_invoice_count ;;
    description: "Number of successful billable invoices paid on/after COALESCE(trial_end, initial_start_date). Always >= 1 in this view."
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

  # PRIMARY TIME AXIS — use this as the chart x-axis.
  dimension_group: subscription_cancelled {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.subscription_cancellation_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Cancellation Date"
    description: "Date the paid subscription churned, America/New_York. Same derivation as prod_subscription_churn: cancel_at_period_end -> COALESCE(dunning_end, current_period_end); otherwise COALESCE(Stripe $.cancelledAt, dunning_end, cancelled_at). NULL for past_due."
  }

  dimension_group: trial_starts_at {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.trial_starts ;;
    description: "Trial / subscription start (initial_start_date). Cohort dimension only — do NOT use as the churn x-axis."
  }

  dimension_group: trial_ends_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.trial_ends ;;
    timeframes: [date, week, month, quarter, year]
    description: "NULL for subscriptions that never had a trial."
  }

  dimension_group: effective_trial_ends_at {
    type: time
    convert_tz: no
    sql: ${TABLE}.effective_trial_end ;;
    timeframes: [date, week, month, quarter, year]
    description: "Payment floor: cancellation_applied_at when it precedes the trial end, else trial_end, else initial_start_date."
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
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
    label: "Last Name"
  }

  dimension: full_name {
    type: string
    sql: TRIM(CONCAT(COALESCE(${TABLE}.first_name, ''), ' ', COALESCE(${TABLE}.last_name, ''))) ;;
    label: "Full Name"
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

  dimension: plan_interval_label {
    type: string
    sql: ${TABLE}.plan_interval_label ;;
    label: "Plan: Interval"
    description: "Product name and billing interval, e.g. 'Launch: month'. Matches prod_subscription_churn.plan_interval for side-by-side comparison."
  }

  # ——— Measures ———

  measure: paid_subscriptions_cancelled {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_churned: "yes"]
    label: "Paid Subscriptions Cancelled"
    description: "Distinct subscriptions with >=1 successful billable payment that have since churned (canceled/unpaid). Bucketed by Cancellation Date."
    drill_fields: [drilldown_details*]
  }

  measure: payment_failed_count {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [cancellation_reason: "payment_failed", is_churned: "yes"]
    label: "Payment Failed (Stripe Unpaid)"
    description: "Paid subs that churned via failed payment — 'unpaid'/'incomplete_expired', or 'canceled' with a void/uncollectible invoice and no user-initiated cancellation."
    drill_fields: [drilldown_details*]
  }

  measure: explicitly_cancelled_count {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [cancellation_reason: "cancelled", is_churned: "yes"]
    label: "Explicitly Cancelled (Post-Paid)"
    description: "Paid subs cancelled by the seller after at least one successful payment."
    drill_fields: [drilldown_details*]
  }

  measure: payment_retrying_count {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [cancellation_reason: "payment_retrying"]
    label: "Payment Retrying (Past Due)"
    description: "Paid subs currently in Stripe dunning. These have NO cancellation date (not yet churned), so they will not appear on a Cancellation Date axis — query them without a date dimension."
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

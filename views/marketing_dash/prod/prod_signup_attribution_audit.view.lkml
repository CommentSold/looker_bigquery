# =============================================================================
# prod_signup_attribution_audit
# -----------------------------------------------------------------------------
# Creator signups with their registration attribution, segmented by whether the
# creator ever paid. Built for the marketing team to work through signups whose
# Reg Intent is "(not set)" without exporting a CSV.
#
# GRAIN: one row per (creator, subscription). A creator with two subscriptions
# gets two rows and appears once in any creator count; a creator with none gets
# one row with NULL subscription fields. Count creators with the Creators
# measure, never with a row count.
#
# -----------------------------------------------------------------------------
# "(NOT SET)" vs "generic"
# -----------------------------------------------------------------------------
# "(not set)" means NEITHER attribution source carries a registration intent.
# 'generic' is a real captured value meaning the creator took the default path.
# They are different things and the tile filter should say which it means.
#
# -----------------------------------------------------------------------------
# popstore_onboarding_screen_action IS DECOMMISSIONED
# -----------------------------------------------------------------------------
# It is still joined because historical rows remain useful for creators who
# signed up before it was retired, but it records nothing new. For recent
# signups the marketing-capture blob on dim_private_profiles is the ONLY source,
# which is why every attribution field here is a COALESCE of the two rather
# than two separate columns.
#
# If that table is eventually dropped, delete the onboarding_legacy CTE and its
# LEFT JOIN. Every COALESCE then degrades cleanly to the marketing-capture side.
#
# Consequence for the audit: "no onboarding event" is EXPECTED for recent
# signups and is not evidence of a tracking failure. Why Not Set is built around
# the marketing-capture blob for that reason.
#
# -----------------------------------------------------------------------------
# OTHER LANDING-URL PARAMETERS ARE SURFACED RAW, NOT INTERPRETED
# -----------------------------------------------------------------------------
# Sample landing URL from a "(not set)" creator:
#   /signup/finalize?reg=creator&instagram=...&businessType=beautyfashionaffiliate
#   &inferredCategoryLabel=Travel+%26+Lifestyle+Creator&nicheSlug=travel_and_lifestyle_creator
#
# URL Reg Param and URL Niche Slug pull `reg=` and `nicheSlug=` out of that URL
# so they can be inspected alongside the record.
#
# THESE ARE SEPARATE FIELDS FROM utm_regintent. `reg`, `businessType` and
# `nicheSlug` each mean their own thing, and this view makes no claim that any of
# them substitutes for a registration intent or that the capture is reading the
# wrong parameter. Whether they relate at all is for the marketing team to
# decide from the data. Reg Intent stays "(not set)" regardless of what these
# contain, and Why Not Set describes only which attribution FIELDS are present,
# never what their values imply.
#
# business_type is populated from the businessType field on its own merits. It is
# not a fallback for utm_regintent and is never used as one.
# =============================================================================

view: prod_signup_attribution_audit {
  derived_table: {
    sql:
      WITH
      -- DECOMMISSIONED SOURCE. Historical rows only; records nothing new.
      onboarding_legacy AS (
        SELECT
          user_id,
          utm_regintent,
          context_campaign_campaign        AS campaign,
          context_campaign_onboarding_path AS onboarding_path,
          context_campaign_planlevel       AS plan_level,
          context_user_agent               AS user_agent,
          business_type
        FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
        WHERE (scene = 'onboarding' OR scene IS NULL)
          AND (step_name = 'onboarding_complete' OR step_name IS NULL)
          AND user_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY user_id
          ORDER BY
            CASE WHEN context_campaign_campaign IS NOT NULL
                   OR (utm_regintent IS NOT NULL AND utm_regintent != 'generic')
                   OR (business_type IS NOT NULL AND business_type != 'generic')
                 THEN 0 ELSE 1 END,
            `timestamp` DESC
        ) = 1
      ),

      -- LIVE SOURCE for anything recent.
      capture AS (
      SELECT
      user_id,
      email AS account_email,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent')       AS utm_regintent,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_campaign')        AS campaign,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_source')          AS utm_source,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.url')                 AS landing_url,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')          AS user_agent,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_onboarding_path') AS onboarding_path,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_planlevel')       AS plan_level,
      JSON_QUERY(private_profile, '$.onboardingMarketingCapture') IS NOT NULL         AS has_capture_blob,
      JSON_VALUE(private_profile, '$.email')                           AS profile_json_email,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.firstName') AS first_name,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.lastName')  AS last_name,
      -- Guessed paths. JSON_VALUE returns NULL on a missing path, so all are
      -- safe to attempt; confirm the real keys if phone stays empty.
      COALESCE(
      JSON_VALUE(private_profile, '$.sellerShippingAddress.phone'),
      JSON_VALUE(private_profile, '$.sellerShippingAddress.phoneNumber'),
      JSON_VALUE(private_profile, '$.phoneNumber'),
      JSON_VALUE(private_profile, '$.phone')
      ) AS phone_number,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.address1') AS address_line_1,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.city')     AS city,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.state')    AS state,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.country')  AS country,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.zip')      AS postal_code
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      ),

      -- Validated churn date, identical to prod_subscription_churn.
      flip AS (
      SELECT
      subscription_id,
      TIMESTAMP_SECONDS(
      MIN(SAFE_CAST(JSON_VALUE(subscription, '$.cancelledAt._seconds') AS INT64))
      ) AS cancelled_at_utc,
      MIN(updated_at) AS status_ts
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription`
      WHERE is_deleted = FALSE AND status IN ('unpaid', 'canceled')
      GROUP BY subscription_id
      ),

      flip_resolved AS (
      SELECT
      subscription_id,
      CASE
      WHEN cancelled_at_utc IS NULL THEN status_ts
      WHEN status_ts       IS NULL THEN cancelled_at_utc
      ELSE LEAST(cancelled_at_utc, status_ts)
      END AS status_flip_at
      FROM flip
      ),

      dunning AS (
      SELECT subscription_id, MIN(updated_at) AS dunning_end_ts
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE AND status IN ('uncollectible', 'void')
      GROUP BY subscription_id
      ),

      invoice_summary AS (
      SELECT
      subscription_id,
      COUNTIF(status = 'paid' AND amount_paid > 0)     AS paid_invoices,
      -- !! CENTS. 2354 = $23.54, confirmed against Stripe. /100 to dollars.
      -- The `> 0` filters are comparisons and correct in either unit.
      SUM(IF(status = 'paid', amount_paid, 0)) / 100   AS total_amount_paid,
      COUNTIF(status IN ('uncollectible', 'void'))  AS failed_or_void_invoices,
      MIN(IF(status = 'paid' AND amount_due > 0 AND amount_paid > 0,
      DATE(COALESCE(created, created_at), 'America/New_York'), NULL)) AS first_billable_paid_date,
      MAX(IF(status = 'paid' AND amount_due > 0 AND amount_paid > 0,
      DATE(COALESCE(created, created_at), 'America/New_York'), NULL)) AS last_billable_paid_date
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      GROUP BY subscription_id
      ),

      -- Creator-level payment flag. Drives the Paid / Not Paid segment, so it
      -- must be per creator: a creator with one paid and one unpaid
      -- subscription is Paid, and both their rows carry that.
      ever_paid AS (
      SELECT DISTINCT user_id
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      AND status = 'paid'
      AND amount_due  > 0
      AND amount_paid > 0
      ),

      subscriptions AS (
      SELECT
      t1.subscription_id,
      t1.user_id,
      t1.status,
      t1.cancel_at_period_end,
      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval')    AS plan_interval,
      -- Plan list price, from the plan entry. t1.price is plan + add-ons.
      SAFE_CAST(JSON_EXTRACT_SCALAR(plan, '$.amount') AS NUMERIC)  AS plan_price,
      -- Total billed: plan + add-ons + tax, minus discount.
      COALESCE(t1.discounted_price, t1.price + t1.tax_amount)     AS price,
      COALESCE(t1.discounted_price, t1.price + t1.tax_amount) = 0 AS is_fully_discounted,
      DATE(t1.initial_start_date,      'America/New_York') AS trial_started,
      DATE(t1.trial_end,               'America/New_York') AS trial_ended,
      DATE(t1.current_period_start,    'America/New_York') AS current_period_start,
      DATE(t1.current_period_end,      'America/New_York') AS current_period_end,
      DATE(t1.cancelled_at,            'America/New_York') AS cancelled_at,
      DATE(t1.cancellation_applied_at, 'America/New_York') AS cancellation_requested_at,
      CASE
      WHEN t1.status NOT IN ('unpaid', 'canceled') THEN NULL
      WHEN t1.cancel_at_period_end IS TRUE
      THEN DATE(COALESCE(d.dunning_end_ts, t1.current_period_end), 'America/New_York')
      ELSE DATE(COALESCE(f.status_flip_at, d.dunning_end_ts, t1.cancelled_at), 'America/New_York')
      END AS churn_date,
      CASE
      WHEN t1.status = 'canceled' AND d.dunning_end_ts IS NOT NULL   THEN 'Churned - payment failed'
      WHEN t1.status = 'canceled'                                    THEN 'Churned - cancelled'
      WHEN t1.status = 'unpaid'                                      THEN 'Churned - payment failed'
      WHEN t1.status = 'past_due'                                    THEN 'Live - in dunning'
      WHEN t1.status = 'active' AND t1.cancel_at_period_end IS TRUE   THEN 'Live - cancels at period end'
      WHEN t1.status = 'active'                                      THEN 'Live - active'
      WHEN t1.status = 'trialing'                                    THEN 'Live - in trial'
      ELSE CONCAT('Other: ', COALESCE(t1.status, 'NULL'))
      END AS subscription_state
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS plan
      LEFT JOIN flip_resolved f ON f.subscription_id = t1.subscription_id
      LEFT JOIN dunning d       ON d.subscription_id = t1.subscription_id
      WHERE t1.is_deleted = FALSE
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      )

      SELECT
      -- ---- identity ----
      p.user_id,
      p.username,
      COALESCE(
      JSON_VALUE(p.profile, '$.storeName'),
      JSON_VALUE(p.profile, '$.displayName'),
      JSON_VALUE(p.profile, '$.name'),
      JSON_VALUE(p.profile, '$.businessName')
      ) AS store_name,
      'https://pop.store/' || p.url_code AS storefront_url,
      NULLIF(TRIM(CONCAT(COALESCE(c.first_name, ''), ' ', COALESCE(c.last_name, ''))), '') AS full_name,
      COALESCE(c.account_email, c.profile_json_email) AS email,
      c.phone_number,
      COALESCE(ol.business_type, JSON_VALUE(p.profile, '$.businessType')) AS business_type,
      NULLIF(TRIM(REGEXP_REPLACE(CONCAT(
      COALESCE(c.address_line_1, ''), ' ',
      COALESCE(c.city, ''),           ' ',
      COALESCE(c.state, ''),          ' ',
      COALESCE(c.postal_code, ''),    ' ',
      COALESCE(c.country, '')
      ), r'\s+', ' ')), '') AS address,
      DATE(st.created_at, 'America/New_York') AS signup_date,

      -- ---- attribution, COALESCEd across the two sources ----
      COALESCE(ol.utm_regintent,  c.utm_regintent)  AS reg_intent_raw,
      COALESCE(ol.campaign,       c.campaign)       AS utm_campaign,
      c.utm_source,
      c.landing_url,
      COALESCE(ol.onboarding_path, c.onboarding_path) AS onboarding_path,
      COALESCE(ol.plan_level,      c.plan_level)      AS plan_level,
      COALESCE(ol.user_agent,      c.user_agent)      AS user_agent,
      c.has_capture_blob,
      ol.user_id IS NOT NULL AS has_legacy_onboarding_row,

      -- The signup link carries `reg=`, not `utm_regintent=`. If this is
      -- populated while Reg Intent is "(not set)", the attribution exists in
      -- the URL and is not being stored.
      REGEXP_EXTRACT(c.landing_url, r'[?&]reg=([^&]+)')       AS url_reg_param,
      REGEXP_EXTRACT(c.landing_url, r'[?&]nicheSlug=([^&]+)') AS url_niche_slug,

      -- ---- segment ----
      IF(ep.user_id IS NOT NULL, 'Paid', 'Not Paid') AS payment_status,

      -- ---- subscription ----
      s.subscription_id,
      s.plan_name,
      s.plan_interval,
      s.price,
      s.plan_price,
      s.is_fully_discounted,
      s.status AS subscription_status_raw,
      s.subscription_state,
      s.trial_started,
      s.trial_ended,
      s.current_period_start,
      s.current_period_end,
      s.cancel_at_period_end,
      s.cancellation_requested_at,
      s.cancelled_at,
      s.churn_date,

      -- ---- payment ----
      COALESCE(inv.paid_invoices, 0)           AS paid_invoices,
      COALESCE(inv.total_amount_paid, 0)       AS total_amount_paid,
      inv.first_billable_paid_date,
      inv.last_billable_paid_date,
      COALESCE(inv.failed_or_void_invoices, 0) AS failed_or_void_invoices,
      DATE_DIFF(inv.first_billable_paid_date,
      DATE(st.created_at, 'America/New_York'), DAY) AS days_signup_to_first_payment

      FROM `popshoplive-26f81.dbt_popshop.dim_profiles` p
      INNER JOIN `popshoplive-26f81.dbt_popshop.dim_stores` st ON st.store_id = p.user_id
      LEFT JOIN capture c            ON c.user_id  = p.user_id
      LEFT JOIN onboarding_legacy ol ON ol.user_id = p.user_id
      LEFT JOIN ever_paid ep         ON ep.user_id = p.user_id
      LEFT JOIN subscriptions s      ON s.user_id  = p.user_id
      LEFT JOIN invoice_summary inv  ON inv.subscription_id = s.subscription_id
      WHERE p.apps_pop_store = TRUE
      AND p.user_type IN ('seller', 'verifiedSeller')
      AND (c.account_email IS NULL
      OR NOT REGEXP_CONTAINS(LOWER(c.account_email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'))
      ;;
  }

  # ——— Primary Key ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(${TABLE}.user_id, '|', COALESCE(${TABLE}.subscription_id, 'no-sub')) ;;
  }

  # ——— Segment ———

  dimension: payment_status {
    type: string
    sql: ${TABLE}.payment_status ;;
    label: "Payment Status"
    description: "Paid = the creator has at least one collected invoice above $0. Creator-level, so a creator with one paid and one unpaid subscription is Paid on every row. Pivot on this for the stacked bar."
  }

  dimension: reg_intent {
    type: string
    sql: COALESCE(${TABLE}.reg_intent_raw, '(not set)') ;;
    label: "Reg Intent"
    description: "Registration intent, COALESCEd across the decommissioned onboarding events table and the live marketing-capture blob. '(not set)' means NEITHER source has a value — different from 'generic', which is a real captured value meaning the creator took the default path."
  }

  dimension: is_not_set {
    type: yesno
    sql: ${TABLE}.reg_intent_raw IS NULL ;;
    label: "Is Reg Intent Not Set"
    description: "Convenience filter for the audit. Excludes 'generic', which IS attributed."
  }

  # ——— Why is it not set ———

  dimension: why_not_set {
    type: string
    sql:
      CASE
        WHEN ${TABLE}.reg_intent_raw IS NOT NULL             THEN 'Attributed'
        WHEN NOT ${TABLE}.has_capture_blob
             AND NOT ${TABLE}.has_legacy_onboarding_row      THEN '1. No attribution data at all'
        WHEN NOT ${TABLE}.has_capture_blob                   THEN '2. Legacy event only, no capture blob'
        WHEN ${TABLE}.utm_source IS NOT NULL
             OR ${TABLE}.utm_campaign IS NOT NULL            THEN '3. Capture blob has other UTMs, no regintent'
        WHEN ${TABLE}.landing_url IS NOT NULL                THEN '4. Capture blob has landing URL only'
        ELSE                                                      '5. Capture blob exists but empty'
      END ;;
    label: "Why Not Set"
    description: "Describes which attribution FIELDS are present on the record — nothing about what their values mean. Bucket 1 is expected for creators predating the marketing capture. Buckets 3 and 4 are records where the capture ran and stored something but utm_regintent specifically was empty."
  }

  dimension: url_reg_param {
    type: string
    sql: ${TABLE}.url_reg_param ;;
    label: "URL Reg Param"
    description: "The raw `reg=` value from the landing URL, surfaced for inspection. A SEPARATE field from utm_regintent — this view makes no claim they are equivalent or interchangeable."
  }

  dimension: url_niche_slug {
    type: string
    sql: ${TABLE}.url_niche_slug ;;
    label: "URL Niche Slug"
    description: "The raw `nicheSlug=` value from the landing URL, surfaced for inspection. A SEPARATE field from utm_regintent."
  }

  dimension: has_capture_blob {
    type: yesno
    sql: ${TABLE}.has_capture_blob ;;
    label: "Has Marketing Capture"
  }

  dimension: has_legacy_onboarding_row {
    type: yesno
    sql: ${TABLE}.has_legacy_onboarding_row ;;
    label: "Has Legacy Onboarding Row"
    description: "From the decommissioned popstore_onboarding_screen_action. No for anything recent by definition — not a tracking failure."
  }

  # ——— Identity ———

  dimension: user_id     { type: string  sql: ${TABLE}.user_id ;;     label: "User ID" }
  dimension: username    { type: string  sql: ${TABLE}.username ;;    label: "Username" }
  dimension: store_name  { type: string  sql: ${TABLE}.store_name ;;  label: "Store Name" }
  dimension: full_name   { type: string  sql: ${TABLE}.full_name ;;   label: "Full Name" }
  dimension: email       { type: string  sql: ${TABLE}.email ;;       label: "Email" }
  dimension: phone_number{ type: string  sql: ${TABLE}.phone_number ;; label: "Phone Number" }
  dimension: business_type {
    type: string
    sql: ${TABLE}.business_type ;;
    label: "Business Type"
    description: "The businessType field, COALESCEd across the legacy onboarding row and the profile. Its own field with its own meaning — NOT a fallback for utm_regintent, and it does not affect Reg Intent or Why Not Set."
  }
  dimension: address     { type: string  sql: ${TABLE}.address ;;     label: "Address" }

  dimension: storefront_url {
    type: string
    sql: ${TABLE}.storefront_url ;;
    label: "Storefront URL"
    link: { label: "Open storefront" url: "{{ value }}" }
  }

  # ——— Attribution detail ———

  dimension: utm_campaign    { type: string sql: ${TABLE}.utm_campaign ;;    label: "UTM Campaign" }
  dimension: utm_source      { type: string sql: ${TABLE}.utm_source ;;      label: "UTM Source" }
  dimension: onboarding_path { type: string sql: ${TABLE}.onboarding_path ;; label: "Onboarding Path" }
  dimension: plan_level      { type: string sql: ${TABLE}.plan_level ;;      label: "Plan Level" }
  dimension: user_agent      { type: string sql: ${TABLE}.user_agent ;;      label: "User Agent" }

  dimension: landing_url {
    type: string
    sql: ${TABLE}.landing_url ;;
    label: "Landing URL"
    link: { label: "Open landing URL" url: "{{ value }}" }
  }

  # ——— Time ———

  dimension_group: signup {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.signup_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Signup"
    description: "Store creation date, America/New_York. Matches prod_signup_conversion_funnel and prod_cumulative_creator_signups."
  }

  # ——— Subscription ———

  dimension: subscription_id        { type: string sql: ${TABLE}.subscription_id ;;        label: "Subscription ID" }
  dimension: plan_name              { type: string sql: ${TABLE}.plan_name ;;              label: "Plan Name" }
  dimension: plan_interval          { type: string sql: ${TABLE}.plan_interval ;;          label: "Interval" }
  dimension: subscription_status_raw{ type: string sql: ${TABLE}.subscription_status_raw ;; label: "Subscription Status (Raw)" }
  dimension: subscription_state     { type: string sql: ${TABLE}.subscription_state ;;     label: "Subscription State" }

  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
    value_format_name: decimal_2
    label: "Total Billed"
    description: "Plan + add-ons + tax, minus any discount. For the plan's own list price use Plan Price."
  }

  dimension: plan_price {
    type: number
    sql: ${TABLE}.plan_price ;;
    value_format_name: decimal_2
    label: "Plan Price"
    description: "The plan's own list price, read from the plan entry — excludes add-ons, tax and discounts."
  }

  dimension: is_fully_discounted {
    type: yesno
    sql: ${TABLE}.is_fully_discounted ;;
    label: "Is Fully Discounted"
    description: "100% coupon at current price. Stripe does not count these as paying."
  }

  dimension: cancel_at_period_end {
    type: yesno
    sql: ${TABLE}.cancel_at_period_end ;;
    label: "Cancels at Period End"
  }

  dimension_group: trial_started  { type: time convert_tz: no datatype: date sql: ${TABLE}.trial_started ;;  timeframes: [date, month] label: "Trial Started" }
  dimension_group: trial_ended    { type: time convert_tz: no datatype: date sql: ${TABLE}.trial_ended ;;    timeframes: [date, month] label: "Trial Ended" }
  dimension_group: period_start   { type: time convert_tz: no datatype: date sql: ${TABLE}.current_period_start ;; timeframes: [date] label: "Current Period Start" }
  dimension_group: period_end     { type: time convert_tz: no datatype: date sql: ${TABLE}.current_period_end ;;   timeframes: [date] label: "Current Period End" }
  dimension_group: cancel_requested { type: time convert_tz: no datatype: date sql: ${TABLE}.cancellation_requested_at ;; timeframes: [date] label: "Cancellation Requested" }
  dimension_group: cancelled_at   { type: time convert_tz: no datatype: date sql: ${TABLE}.cancelled_at ;;   timeframes: [date] label: "Cancelled At" }
  dimension_group: churn          { type: time convert_tz: no datatype: date sql: ${TABLE}.churn_date ;;     timeframes: [date, month] label: "Churn Date"
    description: "Validated churn date, same derivation as prod_subscription_churn. NULL means still live." }

  # ——— Payment detail ———

  dimension: paid_invoices           { type: number sql: ${TABLE}.paid_invoices ;;           label: "Paid Invoices" }
  dimension: failed_or_void_invoices { type: number sql: ${TABLE}.failed_or_void_invoices ;; label: "Failed / Void Invoices" }

  dimension: total_amount_paid {
    type: number
    sql: ${TABLE}.total_amount_paid ;;
    value_format_name: decimal_2
    label: "Total Amount Paid"
    description: "Lifetime collected on this subscription, in DOLLARS. The source column is cents and is divided by 100 — before this fix every figure here read 100x too high."
  }

  dimension: days_signup_to_first_payment {
    type: number
    sql: ${TABLE}.days_signup_to_first_payment ;;
    label: "Days Signup to First Payment"
    description: "Median is 7 from April 2026 (the trial length) and 0-1 before, when there was no trial."
  }

  dimension_group: first_paid { type: time convert_tz: no datatype: date sql: ${TABLE}.first_billable_paid_date ;; timeframes: [date, month] label: "First Paid" }
  dimension_group: last_paid  { type: time convert_tz: no datatype: date sql: ${TABLE}.last_billable_paid_date ;;  timeframes: [date, month] label: "Last Paid" }

  # ——— Measures ———

  measure: creators {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Creators"
    description: "Distinct creators. USE THIS for the stacked bar — a row count would double-count creators who hold more than one subscription."
    drill_fields: [audit_detail*]
  }

  measure: paid_creators {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [payment_status: "Paid"]
    label: "Paid Creators"
    drill_fields: [audit_detail*]
  }

  measure: not_paid_creators {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [payment_status: "Not Paid"]
    label: "Not Paid Creators"
    drill_fields: [audit_detail*]
  }

  measure: subscriptions_count {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    label: "Subscriptions"
    drill_fields: [audit_detail*]
  }

  measure: total_revenue {
    type: sum
    sql: ${TABLE}.total_amount_paid ;;
    value_format_name: decimal_2
    label: "Total Revenue Collected"
    description: "Summed at subscription grain, so safe to aggregate. In DOLLARS — the source column is cents and is divided by 100. Shows what the unattributed segment is actually worth."
  }

  measure: pct_paid {
    type: number
    sql: SAFE_DIVIDE(${paid_creators}, ${creators}) ;;
    value_format_name: percent_1
    label: "% Paid"
  }

  # ——— Drill Set ———

  set: audit_detail {
    fields: [
      user_id, username, store_name, storefront_url, full_name, email,
      phone_number, business_type, address, signup_date,
      reg_intent, why_not_set, url_reg_param, url_niche_slug,
      utm_campaign, utm_source, landing_url, onboarding_path, plan_level, user_agent,
      payment_status,
      subscription_id, plan_name, plan_interval, plan_price, price, is_fully_discounted,
      subscription_status_raw, subscription_state,
      trial_started_date, trial_ended_date,
      period_start_date, period_end_date,
      cancel_at_period_end, cancel_requested_date, cancelled_at_date, churn_date,
      paid_invoices, total_amount_paid, first_paid_date, last_paid_date,
      failed_or_void_invoices, days_signup_to_first_payment
    ]
  }
}

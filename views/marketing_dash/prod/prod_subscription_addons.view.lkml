# =============================================================================
# prod_subscription_addons
# -----------------------------------------------------------------------------
# Add-ons attached to subscriptions: who has them, which ones, and what they are
# worth per month.
#
# GRAIN: one row per (subscription, add-on). A subscription holding two add-ons
# produces two rows, which is what makes a per-add-on breakdown possible.
# Count subscriptions and creators with count_distinct, never with a row count.
#
# -----------------------------------------------------------------------------
# WHAT AN ADD-ON IS HERE
# -----------------------------------------------------------------------------
# Every entry in fact_seller_subscription.plans whose planType is NEITHER 'plan'
# NOR 'taxProduct'. Exclusion-based on purpose, so a new add-on type appears
# automatically rather than being silently dropped.
#
# Types present as of 2026-08-24, across all subscriptions ever:
#   commentChatAddOn        24 entries   $4 - $39
#   popStoreAiEchoMeAddOn    9 entries   $10 - $22   (Credit Pack, sold in-app)
#   modelMeAddOn             2 entries   $10
#
# Of those, 7 sit on LIVE subscriptions. Add-ons recur until cancelled
# separately from the plan, so this grows.
#
# -----------------------------------------------------------------------------
# WHY THIS IS A SEPARATE VIEW
# -----------------------------------------------------------------------------
# prod_subscription_price_points has an Add-on Amount dimension, but it is one
# row per SUBSCRIPTION with the add-ons summed. That answers "does this
# subscriber have add-ons and what are they worth in total"; it cannot answer
# "how many people have the Credit Pack". This view can, because the add-on is
# the grain.
#
# -----------------------------------------------------------------------------
# WHAT THIS CANNOT TELL YOU
# -----------------------------------------------------------------------------
# WHEN an add-on was purchased. The plans array is current state, and the
# `updatedAt` inside each entry is the PRODUCT definition's timestamp, shared
# across every subscriber — not that subscriber's adoption date. Subscription
# Started is the subscription's start, which for an add-on bought later is
# earlier than the adoption.
#
# The closest available proxy is the first invoice whose billed amount jumped by
# the add-on's value; prod_subscription_price_points.Price Changed flags those
# subscriptions, though it also fires on coupon changes.
#
# The `creditAddonAmount` field on invoices does record the AI-credit add-on
# specifically (11 invoices carry it), but there is no equivalent for the other
# two types, so it is not a general answer.
# =============================================================================

view: prod_subscription_addons {
  derived_table: {
    sql:
      SELECT
        t1.subscription_id,
        t1.user_id,
        t1.status,
        t1.cancel_at_period_end,

      -- ---- the add-on ----
      JSON_EXTRACT_SCALAR(addon, '$.productName') AS addon_name,
      JSON_EXTRACT_SCALAR(addon, '$.planType')    AS addon_type,
      JSON_EXTRACT_SCALAR(addon, '$.interval')    AS addon_interval,
      SAFE_CAST(JSON_EXTRACT_SCALAR(addon, '$.amount') AS NUMERIC) AS addon_amount,
      SAFE_CAST(JSON_EXTRACT_SCALAR(addon, '$.aiEchoMeCredit') AS INT64) AS ai_credits,
      JSON_EXTRACT_SCALAR(addon, '$.description') AS addon_description,

      -- Annual add-ons divided by 12 so they can be summed with monthly ones.
      CASE
      WHEN JSON_EXTRACT_SCALAR(addon, '$.interval') = 'year'
      THEN SAFE_CAST(JSON_EXTRACT_SCALAR(addon, '$.amount') AS NUMERIC) / 12
      ELSE SAFE_CAST(JSON_EXTRACT_SCALAR(addon, '$.amount') AS NUMERIC)
      END AS monthly_equivalent_addon,

      -- ---- the plan it is attached to ----
      (SELECT JSON_EXTRACT_SCALAR(pl, '$.productName')
      FROM UNNEST(t1.plans) AS pl
      WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') = 'plan' LIMIT 1) AS plan_name,
      (SELECT JSON_EXTRACT_SCALAR(pl, '$.interval')
      FROM UNNEST(t1.plans) AS pl
      WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') = 'plan' LIMIT 1) AS plan_interval,
      (SELECT SAFE_CAST(JSON_EXTRACT_SCALAR(pl, '$.amount') AS NUMERIC)
      FROM UNNEST(t1.plans) AS pl
      WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') = 'plan' LIMIT 1) AS plan_price,

      -- How many add-ons this subscription holds in total.
      (SELECT COUNT(*)
      FROM UNNEST(t1.plans) AS pl
      WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') NOT IN ('plan', 'taxProduct')) AS addons_on_subscription,

      -- ---- totals, for context ----
      t1.price                                                AS subscription_total_price,
      t1.tax_amount,
      COALESCE(t1.discounted_price, t1.price + t1.tax_amount) AS total_billed,

      DATE(t1.initial_start_date,   'America/New_York') AS subscription_started,
      DATE(t1.current_period_end,   'America/New_York') AS current_period_end,

      COALESCE(REGEXP_CONTAINS(LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'), FALSE) AS is_internal,

      pprof.email,
      prof.username,
      prof.url_code

      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS addon
      INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = t1.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = t1.user_id
      WHERE t1.is_deleted = FALSE
      -- Exclusion-based, so a new add-on type is picked up automatically.
      AND JSON_EXTRACT_SCALAR(addon, '$.planType') NOT IN ('plan', 'taxProduct')
      AND prof.apps_pop_store = TRUE
      AND prof.user_type IN ('seller', 'verifiedSeller')
      ;;
  }

  # ——— Primary Key ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(${TABLE}.subscription_id, '|', ${TABLE}.addon_name) ;;
  }

  # ——— The add-on ———

  dimension: addon_name {
    type: string
    sql: ${TABLE}.addon_name ;;
    label: "Add-on"
    description: "Product name, e.g. 'Credit Pack 250'. The dimension to group by."
  }

  dimension: addon_type {
    type: string
    sql: ${TABLE}.addon_type ;;
    label: "Add-on Type"
    description: "planType from the plans array: commentChatAddOn, popStoreAiEchoMeAddOn or modelMeAddOn. Several products can share a type."
  }

  dimension: addon_interval {
    type: string
    sql: ${TABLE}.addon_interval ;;
    label: "Add-on Interval"
  }

  dimension: addon_amount {
    type: number
    sql: ${TABLE}.addon_amount ;;
    value_format_name: usd
    label: "Add-on Price"
    description: "List price of this add-on, per its own interval."
  }

  dimension: ai_credits {
    type: number
    sql: ${TABLE}.ai_credits ;;
    label: "AI Credits"
    description: "Credits granted per period, for the AI credit add-ons. NULL for other types."
  }

  dimension: addon_description {
    type: string
    sql: ${TABLE}.addon_description ;;
    label: "Add-on Description"
  }

  dimension: addons_on_subscription {
    type: number
    sql: ${TABLE}.addons_on_subscription ;;
    label: "Add-ons on This Subscription"
    description: "How many add-ons the subscription holds. Above 1 means it contributes more than one row here — count subscriptions with the measure, never with a row count."
  }

  # ——— The plan it sits on ———

  dimension: plan_name {
    type: string
    sql: ${TABLE}.plan_name ;;
    label: "Plan Name"
    description: "The subscription's underlying plan. Shows which plans attract which add-ons."
  }

  dimension: plan_interval {
    type: string
    sql: ${TABLE}.plan_interval ;;
    label: "Plan Interval"
  }

  dimension: plan_price {
    type: number
    sql: ${TABLE}.plan_price ;;
    value_format_name: usd
    label: "Plan Price"
    description: "The plan's own list price, excluding this and any other add-on."
  }

  dimension: addon_share_of_bill {
    type: number
    sql: SAFE_DIVIDE(${TABLE}.addon_amount, NULLIF(${TABLE}.total_billed, 0)) ;;
    value_format_name: percent_1
    label: "Add-on Share of Bill"
    description: "This add-on as a proportion of the total billed. The Credit Pack at $22 on a $5.99 Creator plan is most of the bill."
  }

  # ——— Status ———

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    label: "Subscription Status"
    description: "Status of the SUBSCRIPTION, not the add-on. An add-on can be cancelled independently of the plan, and that cancellation is not recorded here — the entry simply disappears from the array."
  }

  dimension: is_live {
    type: yesno
    sql: ${TABLE}.status IN ('active', 'trialing', 'past_due') ;;
    label: "Is Live"
    description: "Filter to Yes for currently-attached add-ons. Without it this includes add-ons on cancelled subscriptions."
  }

  dimension: is_scheduled_to_cancel {
    type: yesno
    sql: ${TABLE}.cancel_at_period_end IS TRUE ;;
    label: "Scheduled to Cancel"
  }

  dimension: is_internal {
    type: yesno
    sql: ${TABLE}.is_internal ;;
    label: "Is Internal Account"
  }

  # ——— Amounts ———

  dimension: subscription_total_price {
    type: number
    sql: ${TABLE}.subscription_total_price ;;
    value_format_name: usd
    label: "Subscription Total Price"
    description: "Plan plus every add-on, excluding tax."
  }

  dimension: total_billed {
    type: number
    sql: ${TABLE}.total_billed ;;
    value_format_name: usd
    label: "Total Billed"
    description: "Plan + add-ons + tax, minus any discount."
  }

  dimension: monthly_equivalent_addon {
    type: number
    sql: ${TABLE}.monthly_equivalent_addon ;;
    value_format_name: usd
    label: "Monthly Equivalent Add-on"
    hidden: yes
  }

  # ——— Creator ———

  dimension: user_id         { type: string sql: ${TABLE}.user_id ;;         label: "User ID" }
  dimension: subscription_id { type: string sql: ${TABLE}.subscription_id ;; label: "Subscription ID" }
  dimension: email           { type: string sql: ${TABLE}.email ;;           label: "Email" }
  dimension: username        { type: string sql: ${TABLE}.username ;;        label: "Username" }

  dimension: storefront_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.url_code ;;
    label: "Storefront URL"
    link: { label: "Open storefront" url: "{{ value }}" }
  }

  dimension_group: subscription_started {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.subscription_started ;;
    timeframes: [date, month, quarter, year]
    label: "Subscription Started"
    description: "When the SUBSCRIPTION started, NOT when the add-on was bought. Adoption date is not recorded anywhere — see the view header."
  }

  dimension_group: current_period_end {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.current_period_end ;;
    timeframes: [date, month]
    label: "Period Ends"
  }

  # ——— Measures ———

  measure: subscriptions_with_addon {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    label: "Subscriptions"
    description: "Distinct subscriptions holding this add-on. Do NOT use a row count — a subscription with two add-ons produces two rows."
    drill_fields: [addon_detail*]
  }

  measure: creators_with_addon {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Creators"
    description: "Distinct creators. Below the subscription count where a creator holds add-ons on more than one subscription."
    drill_fields: [addon_detail*]
  }

  measure: monthly_addon_revenue {
    type: sum
    sql: ${TABLE}.monthly_equivalent_addon ;;
    value_format_name: usd
    label: "Monthly Add-on Revenue"
    description: "Add-on list prices summed, annual divided by 12. List price — it excludes tax and does not account for a discount applied to the subscription as a whole."
    drill_fields: [addon_detail*]
  }

  measure: annual_addon_run_rate {
    type: number
    sql: SUM(${TABLE}.monthly_equivalent_addon) * 12 ;;
    value_format_name: usd
    label: "Annual Add-on Run Rate"
    description: "Monthly add-on revenue x 12. A run rate at today's attachment, not a forecast."
  }

  measure: avg_addon_price {
    type: average
    sql: ${TABLE}.addon_amount ;;
    value_format_name: usd
    label: "Average Add-on Price"
  }

  measure: total_ai_credits {
    type: sum
    sql: ${TABLE}.ai_credits ;;
    label: "AI Credits Granted per Period"
    description: "Credits committed per billing period across the AI credit add-ons."
  }

  # ——— Drill Set ———

  set: addon_detail {
    fields: [
      user_id,
      username,
      email,
      storefront_url,
      subscription_id,
      addon_name,
      addon_type,
      addon_amount,
      addon_interval,
      ai_credits,
      plan_name,
      plan_interval,
      plan_price,
      addon_share_of_bill,
      total_billed,
      status,
      is_scheduled_to_cancel,
      is_internal,
      subscription_started_date,
      current_period_end_date
    ]
  }
}

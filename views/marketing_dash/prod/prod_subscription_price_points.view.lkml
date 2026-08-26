# =============================================================================
# prod_subscription_price_points
# -----------------------------------------------------------------------------
# Live subscriptions by plan AND the price point each one is actually on.
# Built for the Real Estate grandfathering question — some subscribers are still
# on the old Starter price while new ones pay the current price — but the
# Is Real Estate flag is a filter, not a hard restriction, so it works for any
# plan family.
#
# GRAIN: one row per subscription.
#
# -----------------------------------------------------------------------------
# TWO PRICES, AND WHY BOTH ARE HERE
# -----------------------------------------------------------------------------
#   Base Price       price on the plan, BEFORE any coupon. This is the
#                    grandfathering axis — group by it to answer the question.
#   Effective Price  COALESCE(discounted_price, price + tax_amount), what the
#                    subscriber actually pays after a coupon.
#
# Group by Effective Price and a subscriber on the CURRENT price with a 50%
# coupon looks like a THIRD price point. Coupons and grandfathering are separate
# things; keep them on separate axes.
#
# -----------------------------------------------------------------------------
# PRICE HISTORY EXISTS, BUT ONLY IN THE INVOICES
# -----------------------------------------------------------------------------
# fact_seller_subscription is upsert-per-subscription — current state only. If a
# subscriber was migrated from the old price to the new one, the table shows only
# the new one and the migration is invisible.
#
# Invoices ARE historical. So:
#   Distinct Billed Amounts  how many different amounts this subscription has
#                            been charged. > 1 means the price CHANGED.
#   First / Latest Billed    the amounts at each end.
#   Price Changed            convenience flag for > 1.
#
# That is the only reliable way to identify who was actually migrated. Anyone
# with Price Changed = Yes has moved between price points at some point; anyone
# with No has been on one price throughout.
#
# CAUTION: invoice amounts include tax and reflect coupons, so a subscriber whose
# COUPON changed also shows Price Changed = Yes. Read it as "the amount charged
# changed", not "the plan price changed". Cross-check against Has Discount.
#
# -----------------------------------------------------------------------------
# WHO COUNTS AS LIVE
# -----------------------------------------------------------------------------
# status IN ('active', 'trialing', 'past_due'), matching
# prod_current_saas_subscriptions:
#   * active   — includes scheduled cancellations, which keep billing to period end
#   * past_due — stays live until an explicit cancellation, and Stripe counts it
#   * trialing — live but NOT yet paying. A trial has a price point assigned but
#                has never been charged it, so for "who pays what today" filter
#                Status to active and past_due.
# Terminal: canceled, unpaid.
#
# Internal accounts and 100%-discount subscriptions are FLAGS here, not filters,
# so the tile can include or exclude them deliberately.
# =============================================================================

view: prod_subscription_price_points {
  derived_table: {
    sql:
      WITH invoice_history AS (
        SELECT
          subscription_id,
          -- !! amount_due and amount_paid are CENTS. 2354 = $23.54, confirmed
          -- against Stripe. fact_seller_subscription.price is DOLLARS. Every
          -- money output below is /100 so the two are comparable.
          -- COUNT(DISTINCT) is unit-agnostic and needs no conversion, and the
          -- `amount_due > 0` filter is a comparison so it is correct either way.
          COUNT(DISTINCT amount_due)                       AS distinct_billed_amounts,
          MIN(amount_due) / 100                            AS min_billed_amount,
          MAX(amount_due) / 100                            AS max_billed_amount,
          COUNTIF(status = 'paid' AND amount_paid > 0)     AS paid_invoice_count,
          SUM(IF(status = 'paid', amount_paid, 0)) / 100   AS lifetime_amount_paid,
          MIN(DATE(COALESCE(created, created_at), 'America/New_York')) AS first_billed_date,
          -- Amount on the most recent billable invoice: what they are paying now
          -- according to the billing system rather than the subscription record.
          ARRAY_AGG(amount_due ORDER BY COALESCE(created, created_at) DESC LIMIT 1)[OFFSET(0)] / 100 AS latest_billed_amount,
          ARRAY_AGG(amount_due ORDER BY COALESCE(created, created_at) ASC  LIMIT 1)[OFFSET(0)] / 100 AS first_billed_amount
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
        WHERE is_deleted = FALSE
          AND amount_due > 0
        GROUP BY subscription_id
      ),

      enriched AS (
      SELECT
      t1.subscription_id,
      t1.user_id,
      t1.status,
      t1.cancel_at_period_end,

      JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval')    AS plan_interval,

      -- ---- price ----
      -- !! base_price comes from the PLAN ENTRY, not t1.price.
      -- t1.price is the sum of every non-tax plan entry, so for a subscriber
      -- holding an add-on it is plan + add-on and lands them in a phantom
      -- price point ($39 Starter + $22 Credit Pack would read as $61).
      -- Measured: 7 live subscriptions affected, and root_price_wrong = 7
      -- exactly matches with_addon = 7, so add-ons are the only cause.
      SAFE_CAST(JSON_EXTRACT_SCALAR(plan, '$.amount') AS NUMERIC) AS base_price,
      SAFE_CAST(JSON_EXTRACT_SCALAR(plan, '$.discountAmount') AS NUMERIC) AS plan_discount_amount,

      -- The subscription root, kept for comparison and for revenue.
      t1.price                                                AS subscription_total_price,
      t1.tax_amount,
      t1.discounted_price,

      -- Add-ons: anything that is neither the plan nor tax. Exclusion-based
      -- so it survives new add-on types — there are already three:
      -- commentChatAddOn (24), popStoreAiEchoMeAddOn (9), modelMeAddOn (2).
      (SELECT COALESCE(SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(pl, '$.amount') AS NUMERIC)), 0)
      FROM UNNEST(t1.plans) AS pl
      WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') NOT IN ('plan', 'taxProduct')) AS addon_amount,
      (SELECT STRING_AGG(DISTINCT JSON_EXTRACT_SCALAR(pl, '$.productName'), ', ')
      FROM UNNEST(t1.plans) AS pl
      WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') NOT IN ('plan', 'taxProduct')) AS addon_names,

      -- Total actually billed: plan + add-ons + tax, minus any discount.
      -- VERIFIED: of 1,122 live subscriptions, 802 have discounted_price
      -- exactly equal to price + tax, 204 have it LOWER (a genuine discount),
      -- 116 have it NULL and fall back to price + tax. NONE are higher, so
      -- this COALESCE is a correct "what they pay" figure.
      COALESCE(t1.discounted_price, t1.price + t1.tax_amount) AS effective_price,
      t1.discounted_price IS NOT NULL
      AND t1.discounted_price < (t1.price + t1.tax_amount)  AS has_discount,
      COALESCE(t1.discounted_price, t1.price + t1.tax_amount) = 0 AS is_zero_mrr,

      -- Normalised so annual and monthly plans can be summed together.
      -- Total billed, normalised to monthly. INCLUDES tax and add-ons.
      CASE
      WHEN JSON_EXTRACT_SCALAR(plan, '$.interval') = 'year'
      THEN COALESCE(t1.discounted_price, t1.price + t1.tax_amount) / 12
      ELSE COALESCE(t1.discounted_price, t1.price + t1.tax_amount)
      END AS monthly_equivalent_price,

      -- Plan list price only, normalised to monthly. EXCLUDES tax, add-ons
      -- and discounts. Use this for grandfathering arithmetic so the answer
      -- is not distorted by tax or an unrelated add-on.
      CASE
      WHEN JSON_EXTRACT_SCALAR(plan, '$.interval') = 'year'
      THEN SAFE_CAST(JSON_EXTRACT_SCALAR(plan, '$.amount') AS NUMERIC) / 12
      ELSE SAFE_CAST(JSON_EXTRACT_SCALAR(plan, '$.amount') AS NUMERIC)
      END AS monthly_equivalent_plan_price,

      -- ---- invoice-derived history ----
      ih.distinct_billed_amounts,
      ih.min_billed_amount,
      ih.max_billed_amount,
      ih.first_billed_amount,
      ih.latest_billed_amount,
      COALESCE(ih.distinct_billed_amounts, 0) > 1 AS price_changed,
      COALESCE(ih.paid_invoice_count, 0)          AS paid_invoice_count,
      COALESCE(ih.lifetime_amount_paid, 0)        AS lifetime_amount_paid,
      ih.first_billed_date,

      -- ---- segmentation ----
      COALESCE(REGEXP_CONTAINS(LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'), FALSE) AS is_internal,

      REGEXP_CONTAINS(LOWER(JSON_EXTRACT_SCALAR(plan, '$.productName')), r'^real estate') AS is_real_estate,

      -- How many planType='plan' entries this subscription carries. The
      -- QUALIFY below keeps only one; anything above 1 means a second plan
      -- entry was discarded and is worth looking at.
      COUNT(*) OVER (PARTITION BY t1.subscription_id) AS plan_entries,

      DATE(t1.initial_start_date,   'America/New_York') AS subscription_started,
      DATE(t1.current_period_start, 'America/New_York') AS current_period_start,
      DATE(t1.current_period_end,   'America/New_York') AS current_period_end,

      pprof.email  AS email,
      prof.username,
      prof.url_code

      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS plan
      INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = t1.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = t1.user_id
      LEFT JOIN invoice_history ih
      ON ih.subscription_id = t1.subscription_id
      WHERE t1.is_deleted = FALSE
      AND t1.status IN ('active', 'trialing', 'past_due')
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      AND prof.apps_pop_store = TRUE
      AND prof.user_type IN ('seller', 'verifiedSeller')
      -- ══════════════════════════════════════════════════════════════════
      -- ONE ROW PER SUBSCRIPTION.
      --
      -- UNNEST(plans) emits a row per plan entry, and a subscription can carry
      -- more than one with planType = 'plan'. Without this, such a subscription
      -- appears twice: count_distinct measures survive it, but SUM measures
      -- (Monthly Revenue) double-count and window functions over rows
      -- (Starts Last 90d) overstate.
      --
      -- Caught on Real Estate Starter $374/year, where Starts Last 90d read 4
      -- against 3 Active Subscriptions — impossible, since the window only
      -- counts rows already in this live-only set.
      --
      -- Plan Entries is kept below so any subscription where this discards a
      -- second, genuinely different plan product is visible rather than silent.
      -- ══════════════════════════════════════════════════════════════════
      QUALIFY ROW_NUMBER() OVER (
      PARTITION BY t1.subscription_id
      ORDER BY JSON_EXTRACT_SCALAR(plan, '$.productName'),
      JSON_EXTRACT_SCALAR(plan, '$.interval')
      ) = 1
      ),

      -- ══════════════════════════════════════════════════════════════════
      -- WHICH PRICE IS CURRENT? Derived, not hardcoded.
      --
      -- The current price for a plan is the one new subscriptions are still
      -- being created at. So within each (plan, interval), rank the distinct
      -- base prices by the most recent subscription start. Rank 1 is current;
      -- everything else is legacy.
      --
      -- Self-maintaining — no price list to keep updated when pricing changes.
      --
      -- CAVEAT: if someone is deliberately placed on an OLD price today (a
      -- retention save, a negotiated deal), that price's latest start moves to
      -- today and it would be mislabelled Current. Starts Last 90d is exposed
      -- so this is visible: a genuine current price has a steady stream of new
      -- starts, a one-off exception has one or two.
      -- ══════════════════════════════════════════════════════════════════
      windowed AS (
      SELECT
      e.*,
      MAX(e.subscription_started) OVER (
      PARTITION BY e.plan_name, e.plan_interval, e.base_price
      ) AS latest_start_at_price,
      SUM(IF(e.subscription_started >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 90 DAY), 1, 0)) OVER (
      PARTITION BY e.plan_name, e.plan_interval, e.base_price
      ) AS starts_last_90d,
      COUNT(DISTINCT e.base_price) OVER (
      PARTITION BY e.plan_name, e.plan_interval
      ) AS distinct_prices_in_plan
      FROM enriched e
      )

      SELECT
      w.*,
      DENSE_RANK() OVER (
      PARTITION BY w.plan_name, w.plan_interval
      ORDER BY w.latest_start_at_price DESC
      ) AS price_recency_rank
      FROM windowed w
      ;;
  }

  # ——— Primary Key ———

  dimension: subscription_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.subscription_id ;;
    label: "Subscription ID"
  }

  # ——— Plan ———

  dimension: plan_name {
    type: string
    sql: ${TABLE}.plan_name ;;
    label: "Plan Name"
  }

  dimension: plan_interval {
    type: string
    sql: ${TABLE}.plan_interval ;;
    label: "Interval"
  }

  dimension: plan_interval_combined {
    type: string
    sql: CONCAT(${TABLE}.plan_name, ': ', ${TABLE}.plan_interval) ;;
    label: "Plan: Interval"
  }

  dimension: is_real_estate {
    type: yesno
    sql: ${TABLE}.is_real_estate ;;
    label: "Is Real Estate Plan"
    description: "Plan name begins with 'Real Estate'. A filter rather than a hard restriction, so this view also works for other plan families."
  }

  # ——— Price ———

  dimension: price_tier {
    type: string
    sql:
      CASE
        WHEN ${TABLE}.distinct_prices_in_plan = 1 THEN 'Only price'
        WHEN ${TABLE}.price_recency_rank = 1      THEN 'Current price'
        ELSE CONCAT('Legacy price (', CAST(${TABLE}.price_recency_rank - 1 AS STRING), ' back)')
      END ;;
    label: "Price Tier"
    description: "Current versus legacy, derived from which price new subscriptions are still starting at within the same plan and interval — not from a hardcoded price list, so it keeps working when pricing changes. Verify with Starts Last 90d: a genuine current price has ongoing new starts. 'Only price' means the plan has never changed price."
  }

  dimension: price_recency_rank {
    type: number
    sql: ${TABLE}.price_recency_rank ;;
    label: "Price Recency Rank"
    description: "1 = the price with the most recent new subscription in this plan and interval. 2 = the one before it, and so on."
  }

  dimension: starts_last_90d {
    type: number
    sql: ${TABLE}.starts_last_90d ;;
    label: "Starts Last 90d at This Price (Unfiltered)"
    description: "!! IGNORES THE TILE'S FILTERS. Computed as a window function inside the derived table, which runs BEFORE Looker applies any filter — so it counts trialing and internal subscriptions even when the tile excludes them. That is why it can exceed Active Subscriptions. Use New Starts Last 90d for a filter-respecting count; keep this one only as the evidence behind Price Tier, which is deliberately unfiltered so the current price is identified from ALL new subscriptions rather than whichever subset a tile happens to show."
  }

  dimension: started_last_90d {
    type: yesno
    sql: ${TABLE}.subscription_started >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 90 DAY) ;;
    hidden: yes
  }

  dimension: plan_entries {
    type: number
    sql: ${TABLE}.plan_entries ;;
    label: "Plan Entries"
    description: "How many planType='plan' entries this subscription carries in its plans array. Should be 1. Above 1 means a second plan entry exists and was discarded to prevent double-counting — worth investigating, since the subscription may genuinely hold two products."
  }

  dimension: distinct_prices_in_plan {
    type: number
    sql: ${TABLE}.distinct_prices_in_plan ;;
    label: "Distinct Prices in Plan"
    description: "How many base prices exist for this plan and interval. 1 means no grandfathering."
  }

  dimension_group: latest_start_at_price {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.latest_start_at_price ;;
    timeframes: [date, month]
    label: "Latest Start at This Price"
    description: "Most recent subscription start at this price point. What Price Tier is ranked on — surfaced so the ranking can be checked rather than trusted."
  }

  dimension: base_price {
    type: number
    sql: ${TABLE}.base_price ;;
    value_format_name: usd
    label: "Base Price"
    description: "The PLAN's own list price, read from the plan entry — excludes add-ons, tax and discounts. THIS is the grandfathering axis. Previously taken from the subscription root, which is plan + add-ons and put 7 subscribers into phantom price points."
  }

  dimension: plan_discount_amount {
    type: number
    sql: ${TABLE}.plan_discount_amount ;;
    value_format_name: usd
    label: "Plan Discount Amount"
    description: "Discount recorded on the plan entry itself, if any."
  }

  dimension: subscription_total_price {
    type: number
    sql: ${TABLE}.subscription_total_price ;;
    value_format_name: usd
    label: "Subscription Total Price (Plan + Add-ons)"
    description: "The subscription root price: every non-tax plan entry summed. Equals Base Price unless the subscriber holds an add-on. Exposed so the difference is visible rather than silent — this is the field that used to drive Price Point."
  }

  dimension: addon_amount {
    type: number
    sql: ${TABLE}.addon_amount ;;
    value_format_name: usd
    label: "Add-on Amount"
    description: "Monthly value of add-ons attached to this subscription. Excludes the plan and tax."
  }

  dimension: addon_names {
    type: string
    sql: ${TABLE}.addon_names ;;
    label: "Add-ons"
    description: "Which add-ons are attached, e.g. 'Credit Pack 250'. Three types exist: commentChatAddOn, popStoreAiEchoMeAddOn and modelMeAddOn."
  }

  dimension: has_addon {
    type: yesno
    sql: ${TABLE}.addon_amount > 0 ;;
    label: "Has Add-on"
    description: "7 live subscriptions as of 2026-08-24, but add-ons recur and are sold in-app, so this grows. IMPORTANT for Price Changed: buying or cancelling an add-on changes the billed amount and trips that flag without the PLAN price having moved. Exclude add-on holders before reading Price Changed as a price migration."
  }

  dimension: monthly_equivalent_plan_price {
    type: number
    sql: ${TABLE}.monthly_equivalent_plan_price ;;
    value_format_name: usd
    label: "Monthly Equivalent (Plan Price)"
    description: "Plan list price normalised to monthly, annual divided by 12. Excludes tax, add-ons and discounts — the clean basis for sizing a grandfathering gap."
  }

  dimension: price_point {
    type: string
    sql: CONCAT('$', FORMAT('%.2f', ${TABLE}.base_price), ' / ', ${TABLE}.plan_interval) ;;
    label: "Price Point"
    description: "Formatted base price and interval, e.g. '$49.00 / month'. The dimension to group by for the old-price-vs-new-price table. Coupons do NOT move a subscriber between price points here — see Effective Price for what they actually pay."
  }

  dimension: effective_price {
    type: number
    sql: ${TABLE}.effective_price ;;
    value_format_name: usd
    label: "Total Billed"
    description: "What the subscriber actually pays: plan + add-ons + tax, minus any discount. Verified across 1,122 live subscriptions — 802 equal price+tax, 204 lower (a real discount), 116 NULL and falling back to price+tax, none higher. Do NOT group by this for grandfathering: tax, a coupon or an add-on would each split one price point into several."
  }

  dimension: tax_amount {
    type: number
    sql: ${TABLE}.tax_amount ;;
    value_format_name: usd
    label: "Tax Amount"
  }

  dimension: has_discount {
    type: yesno
    sql: ${TABLE}.has_discount ;;
    label: "Has Discount"
    description: "A coupon is reducing the price below the plan's list price. Separate from being on an older price point."
  }

  dimension: is_zero_mrr {
    type: yesno
    sql: ${TABLE}.is_zero_mrr ;;
    label: "Is Zero MRR"
    description: "Fully discounted. Stripe excludes these from its active-subscriber count."
  }

  dimension: monthly_equivalent_price {
    type: number
    sql: ${TABLE}.monthly_equivalent_price ;;
    value_format_name: usd
    label: "Monthly Equivalent (Total Billed)"
    description: "Total billed normalised to monthly, annual divided by 12. INCLUDES tax and add-ons, so it is the revenue figure, not the plan price. For grandfathering arithmetic use Monthly Equivalent (Plan Price)."
  }

  # ——— Price history, from invoices ———

  dimension: price_changed {
    type: yesno
    sql: ${TABLE}.price_changed ;;
    label: "Price Changed"
    description: "This subscription has been charged more than one distinct amount. The subscription table shows current state only, so this is the only way to spot a price migration. TWO OTHER THINGS ALSO TRIP IT: a changed coupon, and buying or cancelling an ADD-ON (the Credit Pack alone moves a bill by $22). Cross-check Has Discount and Has Add-on before reading this as a plan price change."
  }

  dimension: distinct_billed_amounts {
    type: number
    sql: ${TABLE}.distinct_billed_amounts ;;
    label: "Distinct Billed Amounts"
    description: "How many different amounts this subscription has been charged. 1 means one price throughout."
  }

  dimension: first_billed_amount {
    type: number
    sql: ${TABLE}.first_billed_amount ;;
    value_format_name: usd
    label: "First Billed Amount"
    description: "The amount on the earliest billable invoice — the price they originally signed up at."
  }

  dimension: latest_billed_amount {
    type: number
    sql: ${TABLE}.latest_billed_amount ;;
    value_format_name: usd
    label: "Latest Billed Amount"
    description: "The amount on the most recent billable invoice — what the billing system last actually charged. Compare against Base Price: a disagreement means the subscription record and the invoices are out of step."
  }

  dimension: min_billed_amount {
    type: number
    sql: ${TABLE}.min_billed_amount ;;
    value_format_name: usd
    label: "Lowest Billed Amount"
  }

  dimension: max_billed_amount {
    type: number
    sql: ${TABLE}.max_billed_amount ;;
    value_format_name: usd
    label: "Highest Billed Amount"
  }

  dimension: paid_invoice_count {
    type: number
    sql: ${TABLE}.paid_invoice_count ;;
    label: "Paid Invoices"
  }

  dimension: lifetime_amount_paid {
    type: number
    sql: ${TABLE}.lifetime_amount_paid ;;
    value_format_name: usd
    label: "Lifetime Amount Paid"
  }

  # ——— Status ———

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    label: "Status"
    description: "active (includes scheduled cancellations still billing), trialing (assigned a price point but never charged it), past_due (live until explicitly cancelled). For 'who pays what today', filter to active and past_due."
  }

  dimension: is_scheduled_to_cancel {
    type: yesno
    sql: ${TABLE}.cancel_at_period_end IS TRUE ;;
    label: "Scheduled to Cancel"
    description: "Cancelled but still billing until the period ends. Counted as live."
  }

  dimension: is_internal {
    type: yesno
    sql: ${TABLE}.is_internal ;;
    label: "Is Internal Account"
    description: "A flag rather than a filter, so the tile decides. Stripe includes these; internal reporting usually does not."
  }

  # ——— Creator ———

  dimension: user_id  { type: string sql: ${TABLE}.user_id ;;  label: "User ID" }
  dimension: email    { type: string sql: ${TABLE}.email ;;    label: "Email" }
  dimension: username { type: string sql: ${TABLE}.username ;; label: "Username" }

  dimension: storefront_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.url_code ;;
    label: "Storefront URL"
    link: { label: "Open storefront" url: "{{ value }}" }
  }

  # ——— Dates ———

  dimension_group: subscription_started {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.subscription_started ;;
    timeframes: [date, month, quarter, year]
    label: "Subscription Started"
    description: "Useful for grandfathering: subscribers on an older price generally started earlier. Group price points by this to see when each was in effect."
  }

  dimension_group: first_billed {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.first_billed_date ;;
    timeframes: [date, month]
    label: "First Billed"
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

  measure: subscriptions {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    label: "Active Subscriptions"
    description: "Distinct live subscriptions. The headline count for the price-point table."
    drill_fields: [price_detail*]
  }

  measure: creators {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Distinct Creators"
    description: "Below the subscription count where a creator holds more than one plan."
    drill_fields: [price_detail*]
  }

  measure: new_starts_last_90d {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [started_last_90d: "yes"]
    label: "New Starts Last 90d"
    description: "Subscriptions started at this price in the last 90 days, RESPECTING the tile's filters. Use this rather than the unfiltered dimension of a similar name — that one is a window function and counts trialing and internal subscriptions regardless of what the tile excludes."
    drill_fields: [price_detail*]
  }

  measure: paying_subscriptions {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_zero_mrr: "no"]
    label: "Paying Subscriptions"
    description: "Excludes fully-discounted subscriptions, which sit in a price-point bucket while contributing nothing. For 'how many subscribers are on each price', this is usually the honest number — 8 of the 65 legacy Real Estate Pro annual subscribers pay $0."
    drill_fields: [price_detail*]
  }

  measure: zero_mrr_subscriptions {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_zero_mrr: "yes"]
    label: "Zero MRR Subscriptions"
    description: "Fully discounted. Assigned a price point but paying nothing."
    drill_fields: [price_detail*]
  }

  measure: total_monthly_revenue {
    type: sum
    sql: ${TABLE}.monthly_equivalent_price ;;
    value_format_name: usd
    label: "Monthly Revenue (Total Billed)"
    description: "Monthly-equivalent TOTAL BILLED summed, annual divided by 12. Includes tax and add-ons, so it will not equal Base Price x subscriber count — that gap is tax, plus add-ons for the 7 subscribers holding one. For plan-only arithmetic use Monthly Plan Revenue."
  }

  measure: total_monthly_plan_revenue {
    type: sum
    sql: ${TABLE}.monthly_equivalent_plan_price ;;
    value_format_name: usd
    label: "Monthly Plan Revenue"
    description: "Monthly-equivalent PLAN list price summed — excludes tax, add-ons and discounts. Equals Base Price x ACTIVE subscriber count, which INCLUDES fully-discounted subscriptions paying nothing. For a revenue figure use Monthly Plan Revenue (Paying Only)."
  }

  measure: total_monthly_plan_revenue_paying {
    type: sum
    sql: IF(${TABLE}.is_zero_mrr, 0, ${TABLE}.monthly_equivalent_plan_price) ;;
    value_format_name: usd
    label: "Monthly Plan Revenue (Paying Only)"
    description: "Plan list price summed across PAYING subscriptions only, excluding fully-discounted ones. Monthly Plan Revenue counts every subscription at its list price, including those paying nothing — for Real Estate Pro $500/year that is 56 subscriptions at $2,333/month when 8 of them pay $0, so the real figure is nearer $2,000. Use this one for anything revenue-facing."
  }

  measure: total_monthly_addon_revenue {
    type: sum
    sql: ${TABLE}.addon_amount ;;
    value_format_name: usd
    label: "Monthly Add-on Revenue"
    description: "Add-on value attached to these subscriptions, monthly. Small today but recurring and sold in-app."
  }

  measure: subscriptions_with_addon {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [has_addon: "yes"]
    label: "Subscriptions With Add-on"
    description: "7 as of 2026-08-24. Watch this alongside Price Changed — a rise means more of that flag is add-on activity rather than price migration."
    drill_fields: [price_detail*]
  }

  measure: avg_base_price {
    type: average
    sql: ${TABLE}.base_price ;;
    value_format_name: usd
    label: "Average Base Price"
  }

  measure: migrated_subscriptions {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [price_changed: "yes"]
    label: "Price Changed (Count)"
    description: "Subscriptions charged more than one distinct amount over their life — moved between price points, or had a coupon change."
    drill_fields: [price_detail*]
  }

  measure: revenue_gap_vs_max_price {
    type: number
    sql: (MAX(${TABLE}.base_price) * COUNT(DISTINCT ${TABLE}.subscription_id))
      - SUM(${TABLE}.base_price) ;;
    value_format_name: usd
    label: "Gap vs Highest Price in Group"
    description: "What this group would bill at the highest base price present in it, minus what it bills now. Now built on PLAN price, so tax and add-ons no longer distort it. Group by Plan Name to size the grandfathering discount per plan. Only meaningful within a single plan and interval — across mixed plans the highest price is arbitrary."
  }

  # ——— Drill Set ———

  set: price_detail {
    fields: [
      user_id,
      username,
      email,
      storefront_url,
      subscription_id,
      plan_name,
      plan_interval,
      price_point,
      base_price,
      addon_amount,
      addon_names,
      subscription_total_price,
      effective_price,
      has_discount,
      status,
      is_scheduled_to_cancel,
      is_internal,
      subscription_started_date,
      first_billed_date,
      first_billed_amount,
      latest_billed_amount,
      distinct_billed_amounts,
      price_changed,
      paid_invoice_count,
      lifetime_amount_paid
    ]
  }
}

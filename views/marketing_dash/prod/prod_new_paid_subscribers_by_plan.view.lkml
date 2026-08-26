# =============================================================================
# prod_new_paid_subscribers_by_plan
# -----------------------------------------------------------------------------
# New paid subscribers by the plan they first paid for. The plan breakdown that
# sits UNDERNEATH the "Monthly New Paid Subscribers" tile in
# prod_cumulative_creator_signups.
#
# GRAIN: one row per creator, at their FIRST billable collected payment.
#
# -----------------------------------------------------------------------------
# WHY A SEPARATE VIEW RATHER THAN ADDING PLAN TO THE EXISTING ONE
# -----------------------------------------------------------------------------
# prod_cumulative_creator_signups is month-grain and carries month-level targets.
# Adding plan to it would break three things:
#
# 1. TARGETS. At (month, plan) grain, type: sum multiplies the target by the
#    number of plans — August's 194 becomes roughly 2,100. type: max repeats 194
#    on every plan row. There is no correct aggregation for a month-level target
#    at plan grain. That view's own header already warns: "do NOT group by
#    Interval as well, or the same target repeats on every row."
#
# 2. CUMULATIVE MEASURES. cumulative_paid is a window over months. Partition by
#    plan and MAX() across plans returns the largest single plan, not the total.
#    Silently wrong.
#
# 3. SIGNUPS HAVE NO PLAN. A creator signs up before choosing one, so
#    Monthly Creator Sign-ups cannot be broken down at all.
#
# This view therefore carries NO targets and NO cumulative measures. It answers
# one question: of the new paid subscribers in month M, which plan did each buy?
#
# -----------------------------------------------------------------------------
# GRAIN IS PER CREATOR, NOT PER SUBSCRIPTION — this is what makes it tie out
# -----------------------------------------------------------------------------
# The tile it must match counts CREATORS whose first billable invoice fell in the
# month. One creator, one plan, so a stacked bar sums exactly to that figure.
#
# Count subscriptions instead and a creator who first paid two subscriptions on
# the same day appears twice, and the totals stop matching. Measured: 2,075
# creators paid a single subscription on their first paying day, 2 paid two.
# Those 2 are resolved deterministically (earliest invoice, then lowest
# subscription_id) and flagged by Is First Payment Ambiguous, so they are
# visible rather than silently assigned.
#
# -----------------------------------------------------------------------------
# !! THE PLAN IS CURRENT STATE, THE PAYMENT AMOUNT IS HISTORICAL !!
# -----------------------------------------------------------------------------
# Plan Name and Interval come from fact_seller_subscription.plans, which is
# UPSERT per subscription — current state only. So a subscription whose plan was
# swapped IN PLACE would be attributed to its plan today, not the plan it was on
# when it first paid.
#
# Mitigating evidence: upgrades appear to mint a NEW subscription_id rather than
# swap the plan on an existing one (the chain-replacement finding — a creator was
# observed with three sequential subscription_ids, each starting seconds after
# the previous was cancelled). If that holds universally, in-place plan swaps do
# not happen and the attribution is exact. It has not been proven universally.
#
# First Payment Amount is DIFFERENT: it is the amount on the actual invoice, so
# it is real history and cannot be retroactively rewritten. Where the two
# disagree, trust the payment amount.
#
# POSSIBLE UPGRADE: fact_seller_subscription_invoice has an `invoice` STRING
# column, almost certainly Stripe's raw invoice JSON. Stripe invoices carry
# lines[].price with product and nickname. If that is present, plan can be read
# from the INVOICE — genuine history, immune to later plan changes. Check with:
#
# --   SELECT invoice
# --   FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
# --   WHERE is_deleted = FALSE AND status = 'paid' AND amount_due > 0
# --   LIMIT 1;
#
# If lines[].price.nickname or lines[].price.product is there, replace the
# plan_name / plan_interval expressions with JSON_VALUE reads off that blob and
# delete the UNNEST(plans) join entirely.
#
# -----------------------------------------------------------------------------
# VALIDATION — must pass before publishing
# -----------------------------------------------------------------------------
# Sum New Paid Subscribers per month, no plan dimension. It must equal
# Monthly New Paid Subscribers: Actual in prod_cumulative_creator_signups
# exactly:
#
#   2026-01   65      2026-05  368
#   2026-02   63      2026-06  388
#   2026-03   61      2026-07  357
#   2026-04  150      2026-08  191   (partial month)
#
# Any gap means a creator was dropped or double-attributed. The population
# filters here are deliberately IDENTICAL to that view: store required, vidcon
# excluded, pop.store in the email filter, America/New_York throughout.
# =============================================================================

view: prod_new_paid_subscribers_by_plan {
  derived_table: {
    sql:
      WITH
      onboarding_events_dedup AS (
        SELECT user_id, utm_regintent
        FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
        WHERE (scene = 'onboarding' OR scene IS NULL)
          AND (step_name = 'onboarding_complete' OR step_name IS NULL)
          AND user_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY user_id
          ORDER BY
            CASE WHEN utm_regintent IS NOT NULL AND utm_regintent != 'generic'
                 THEN 0 ELSE 1 END,
            `timestamp` DESC
        ) = 1
      ),

      marketing_capture AS (
        SELECT
          user_id,
          ANY_VALUE(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent')) AS utm_regintent
        FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
        WHERE user_id IS NOT NULL
        GROUP BY user_id
      ),

      -- Population IDENTICAL to prod_cumulative_creator_signups. If these
      -- filters drift apart the two tiles stop reconciling.
      eligible_creators AS (
        SELECT
          p.user_id,
          MIN(DATE(s.created_at, 'America/New_York')) AS signup_date
        FROM `popshoplive-26f81.dbt_popshop.dim_profiles` p
        INNER JOIN `popshoplive-26f81.dbt_popshop.dim_stores` s
          ON p.user_id = s.store_id
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
          ON pprof.user_id = p.user_id
        LEFT JOIN marketing_capture mc       ON mc.user_id = p.user_id
        LEFT JOIN onboarding_events_dedup oe ON oe.user_id = p.user_id
        WHERE p.apps_pop_store = TRUE
          AND p.user_type IN ('seller', 'verifiedSeller')
          -- COALESCE around the comparison: without it a creator with no
          -- regintent yields NULL, NOT NULL is NULL, and they are silently
          -- dropped. That mistake once cut a test population from 26,667 to
          -- 11,077.
          AND NOT COALESCE(COALESCE(oe.utm_regintent, mc.utm_regintent) = 'vidcon', FALSE)
          AND (pprof.email IS NULL
               OR NOT REGEXP_CONTAINS(LOWER(pprof.email),
                    r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'))
        GROUP BY p.user_id
      ),

      -- How many subscriptions a creator first paid on the SAME day. 2 for two
      -- creators, 1 for the other 2,075. Kept so the tie-break is visible.
      first_day_spread AS (
        SELECT
          i.user_id,
          COUNT(DISTINCT i.subscription_id) AS subs_on_first_paid_day
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice` i
        JOIN (
          SELECT user_id,
                 MIN(DATE(COALESCE(created, created_at), 'America/New_York')) AS d
          FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
          WHERE is_deleted = FALSE AND status = 'paid'
            AND amount_due > 0 AND amount_paid > 0
          GROUP BY user_id
        ) f
          ON f.user_id = i.user_id
         AND DATE(COALESCE(i.created, i.created_at), 'America/New_York') = f.d
        WHERE i.is_deleted = FALSE AND i.status = 'paid'
          AND i.amount_due > 0 AND i.amount_paid > 0
        GROUP BY i.user_id
      ),

      -- ONE invoice per creator: their first billable collected payment.
      -- ORDER BY includes subscription_id so the two same-day ties resolve
      -- deterministically rather than arbitrarily.
      first_payment AS (
        SELECT
          user_id,
          subscription_id,
          invoice_id,
          DATE(COALESCE(created, created_at), 'America/New_York') AS first_paid_date,
          -- !! CENTS. 2354 = $23.54, confirmed against Stripe. Converted to
          -- dollars so this is comparable with subscription prices, which are
          -- already dollars. The `> 0` filters below are comparisons and are
          -- correct in either unit.
          amount_paid / 100 AS first_payment_amount,
          amount_due  / 100 AS first_amount_due
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
        WHERE is_deleted = FALSE
          AND status = 'paid'
          AND amount_due  > 0
          AND amount_paid > 0
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY user_id
          ORDER BY COALESCE(created, created_at) ASC, subscription_id ASC
        ) = 1
      ),

      -- One plan row per subscription. Verified 1 planType='plan' entry per
      -- subscription across all 1,112 live subscriptions, but this view also
      -- covers CHURNED ones, so the guard stays and Plan Entries is exposed.
      subscription_plan AS (
        SELECT
          t1.subscription_id,
          JSON_EXTRACT_SCALAR(plan, '$.productName') AS plan_name,
          JSON_EXTRACT_SCALAR(plan, '$.interval')    AS plan_interval,
          -- The PLAN's own price. t1.price is the sum of every non-tax plan
          -- entry, so it is plan + add-ons for the 7 subscribers holding one.
          SAFE_CAST(JSON_EXTRACT_SCALAR(plan, '$.amount') AS NUMERIC) AS current_base_price,
          (SELECT COALESCE(SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(pl, '$.amount') AS NUMERIC)), 0)
           FROM UNNEST(t1.plans) AS pl
           WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') NOT IN ('plan', 'taxProduct')) AS addon_amount,
          t1.status                                  AS subscription_status_now,
          COUNT(*) OVER (PARTITION BY t1.subscription_id) AS plan_entries
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
        UNNEST(t1.plans) AS plan
        WHERE t1.is_deleted = FALSE
          AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY t1.subscription_id
          ORDER BY JSON_EXTRACT_SCALAR(plan, '$.productName'),
                   JSON_EXTRACT_SCALAR(plan, '$.interval')
        ) = 1
      )

      SELECT
        ec.user_id,
        ec.signup_date,
        fp.subscription_id,
        fp.invoice_id,
        fp.first_paid_date,
        fp.first_payment_amount,
        fp.first_amount_due,

        sp.plan_name,
        sp.plan_interval,
        sp.current_base_price,
        COALESCE(sp.addon_amount, 0) AS addon_amount,
        sp.subscription_status_now,
        COALESCE(sp.plan_entries, 0) AS plan_entries,

        COALESCE(fds.subs_on_first_paid_day, 1) > 1 AS is_first_payment_ambiguous,

        DATE_DIFF(fp.first_paid_date, ec.signup_date, DAY) AS days_signup_to_first_payment,

        DATE_TRUNC(fp.first_paid_date, MONTH)
          = DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH) AS is_partial_month,

        prof.username,
        prof.url_code,
        pprof.email

      FROM eligible_creators ec
      INNER JOIN first_payment fp      ON fp.user_id = ec.user_id
      LEFT JOIN subscription_plan sp   ON sp.subscription_id = fp.subscription_id
      LEFT JOIN first_day_spread fds   ON fds.user_id = ec.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
        ON prof.user_id = ec.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
        ON pprof.user_id = ec.user_id
      ;;
  }

  # ——— Primary Key ———

  dimension: user_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.user_id ;;
    label: "User ID"
  }

  # ——— Time ———

  dimension_group: first_paid {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.first_paid_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "First Paid"
    description: "Month of the creator's first billable collected invoice, America/New_York. The x-axis. Matches the axis of Monthly New Paid Subscribers in prod_cumulative_creator_signups."
  }

  dimension_group: signup {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.signup_date ;;
    timeframes: [date, month, quarter, year]
    label: "Signup"
    description: "Store creation date. A DIFFERENT axis from First Paid — a creator who signed up in March and first paid in June sits in March here and June there. Do not mix the two on one chart."
  }

  dimension: is_partial_month {
    type: yesno
    sql: ${TABLE}.is_partial_month ;;
    label: "Is Partial Month"
    description: "Yes for the month in progress. Its bar is real but incomplete — filter it out for a clean trend, or label it."
  }

  dimension: days_signup_to_first_payment {
    type: number
    sql: ${TABLE}.days_signup_to_first_payment ;;
    label: "Days Signup to First Payment"
    description: "Median is 7 from April 2026 (the trial length) and 0-1 before, when there was no trial."
  }

  # ——— Plan ———

  dimension: plan_name {
    type: string
    sql: ${TABLE}.plan_name ;;
    label: "Plan Name"
    description: "Plan on the subscription that generated the first payment. CURRENT state, not the plan as at that payment — see Plan Attribution Caveat in the view header. First Payment Amount is the historical figure and should be trusted where the two disagree."
  }

  dimension: plan_interval {
    type: string
    sql: ${TABLE}.plan_interval ;;
    label: "Interval"
  }

  dimension: plan_interval_combined {
    type: string
    sql: CONCAT(COALESCE(${TABLE}.plan_name, '(unknown)'), ': ',
                COALESCE(${TABLE}.plan_interval, '(unknown)')) ;;
    label: "Plan: Interval"
    description: "Pivot on this for the stacked bar."
  }

  dimension: is_real_estate {
    type: yesno
    sql: REGEXP_CONTAINS(LOWER(${TABLE}.plan_name), r'^real estate') ;;
    label: "Is Real Estate Plan"
  }

  # ——— Amounts ———

  dimension: first_payment_amount {
    type: number
    sql: ${TABLE}.first_payment_amount ;;
    value_format_name: usd
    label: "First Payment Amount"
    description: "Amount actually collected on the first billable invoice, in DOLLARS (the source column is cents and is divided by 100). Real history — it cannot be rewritten by a later plan or price change, unlike Current Base Price. Includes tax and any add-on billed on that invoice, and reflects the coupon in force at the time."
  }

  dimension: current_base_price {
    type: number
    sql: ${TABLE}.current_base_price ;;
    value_format_name: usd
    label: "Current Base Price"
    description: "The PLAN's list price today, before coupon, tax and add-ons — read from the plan entry, not the subscription root. Shown for comparison only: a gap against First Payment Amount means tax, a coupon, an add-on, or a genuine price change since."
  }

  dimension: addon_amount {
    type: number
    sql: ${TABLE}.addon_amount ;;
    value_format_name: usd
    label: "Add-on Amount"
    description: "Monthly add-on value on the subscription today. Non-zero means First Payment Amount may include an add-on as well as the plan."
  }

  dimension: has_addon {
    type: yesno
    sql: ${TABLE}.addon_amount > 0 ;;
    label: "Has Add-on"
  }

  dimension: first_payment_price_band {
    type: tier
    tiers: [0, 20, 50, 100, 250, 500, 1000]
    style: integer
    sql: ${TABLE}.first_payment_amount ;;
    value_format_name: usd
    label: "First Payment Band"
    description: "Banded first payment in dollars, for grouping plans of similar value without an exact price list. The tiers assume dollars — before the cents fix every row fell into the top band."
  }

  # ——— Data quality ———

  dimension: is_first_payment_ambiguous {
    type: yesno
    sql: ${TABLE}.is_first_payment_ambiguous ;;
    label: "Is First Payment Ambiguous"
    description: "The creator first paid TWO subscriptions on the same day, so the plan credited here was chosen by tie-break (lowest subscription_id) rather than determined. 2 creators of 2,077. Flagged rather than hidden."
  }

  dimension: plan_entries {
    type: number
    sql: ${TABLE}.plan_entries ;;
    label: "Plan Entries"
    description: "planType='plan' entries on the subscription. Should be 1. Above 1 means a second was discarded to prevent double-counting. 0 means the subscription row could not be found at all — those creators appear as '(unknown)' plan and are worth investigating."
  }

  dimension: subscription_status_now {
    type: string
    sql: ${TABLE}.subscription_status_now ;;
    label: "Subscription Status Now"
    description: "Status of that subscription TODAY, not at first payment. A creator who first paid in March may show 'canceled' here — that is correct and expected. NEVER filter the historical series on this: doing so reintroduces the survivorship bug."
  }

  # ——— Creator ———

  dimension: subscription_id { type: string sql: ${TABLE}.subscription_id ;; label: "Subscription ID" }
  dimension: invoice_id      { type: string sql: ${TABLE}.invoice_id ;;      label: "First Invoice ID" }
  dimension: username        { type: string sql: ${TABLE}.username ;;        label: "Username" }
  dimension: email           { type: string sql: ${TABLE}.email ;;           label: "Email" }

  dimension: storefront_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.url_code ;;
    label: "Storefront URL"
    link: { label: "Open storefront" url: "{{ value }}" }
  }

  # ——— Measures ———

  measure: new_paid_subscribers {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "New Paid Subscribers"
    description: "Creators whose first billable collected invoice fell in this month. One creator counts once, on one plan, so a stacked bar sums exactly to Monthly New Paid Subscribers: Actual in prod_cumulative_creator_signups — 65, 63, 61, 150, 368, 388, 357, 191 for Jan-Aug 2026."
    drill_fields: [detail*]
  }

  measure: first_payment_revenue {
    type: sum
    sql: ${TABLE}.first_payment_amount ;;
    value_format_name: usd
    label: "First Payment Revenue"
    description: "Total collected on these creators' FIRST invoices only — not recurring revenue and not MRR. Answers 'what did this month's new subscribers bring in on day one'."
  }

  measure: avg_first_payment {
    type: average
    sql: ${TABLE}.first_payment_amount ;;
    value_format_name: usd
    label: "Average First Payment"
  }

  measure: ambiguous_attributions {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [is_first_payment_ambiguous: "yes"]
    label: "Ambiguous Attributions"
    description: "Creators whose plan was assigned by tie-break. Should stay at 2. A rise means concurrent first payments are becoming common and the tie-break needs revisiting."
  }

  measure: unknown_plan {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [plan_entries: "0"]
    label: "Unknown Plan"
    description: "Creators whose first-payment subscription has no resolvable plan row. Should be 0. Above 0 means an invoice references a subscription that is deleted or has no planType='plan' entry."
  }

  # ——— Drill Set ———

  set: detail {
    fields: [
      user_id,
      username,
      email,
      storefront_url,
      first_paid_date,
      plan_name,
      plan_interval,
      first_payment_amount,
      current_base_price,
      subscription_id,
      invoice_id,
      subscription_status_now,
      signup_date,
      days_signup_to_first_payment,
      is_first_payment_ambiguous
    ]
  }
}

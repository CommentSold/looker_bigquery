view: prod_stripe_bq_reconciliation {
  # ==========================================================================
  # Monthly reconciliation between BigQuery subscription events and Stripe's
  # own event table (stripe_analytical_data), for starts and ends.
  #
  # GRAIN: one row per (month, event_type, subscription_id).
  #
  # ------------------------------------------------------------------------
  # WHY SUBSCRIPTION GRAIN
  # ------------------------------------------------------------------------
  # Stripe reports at subscription-event grain. Verified: July 2026
  # new_subscribers = 644 rows, matching Stripe's dashboard New 615 +
  # Reactivation 29 exactly. Counting distinct creators instead gave 625,
  # because ~19 creators had two subscription events that month.
  #
  # Three different "BigQuery July starts" figures all exist and all are
  # internally correct:
  #     632  distinct creators
  #     635  prod_subscription_churn (distinct creators per plan_interval,
  #          then SUMmed -- so a creator with a monthly AND a yearly plan
  #          counts twice, but one with two monthly plans counts once)
  #     644  subscription events (Stripe's grain)
  # This view compares at subscription grain and additionally exposes
  # creator-grain measures, clearly labelled, so the two are never read
  # against each other by accident.
  #
  # ------------------------------------------------------------------------
  # THE IDENTITY  (Reconciliation Check must always be 0)
  # ------------------------------------------------------------------------
  #   BigQuery Total - Missing in Stripe + Missing in BigQuery = Stripe Total
  # Note this is NOT  BigQuery + Missing in BigQuery = Stripe, which only
  # holds when Missing in Stripe is zero.
  #
  # ------------------------------------------------------------------------
  # TIMEZONE -- RESOLVED, DO NOT "FIX"
  # ------------------------------------------------------------------------
  # event_date is Sigma's local_event_timestamp (New York wall clock) that
  # the loader stamped 'UTC' onto. The stored instant is wrong by the NY
  # offset but the rendered wall clock is right, so the correct local date is
  # DATE(event_date) with NO timezone argument. Confirmed: this reproduces
  # Stripe's published July totals to the digit.
  #
  # IF the backfill in backfill_stripe_event_date_timezone.sql is applied,
  # event_date becomes a true instant and this must change to
  # DATE(event_date, 'America/New_York'). Search STRIPE_LOCAL_DATE below.
  # Both forms yield identical buckets; mismatching them is silently wrong
  # by 4-5 hours.
  #
  # ------------------------------------------------------------------------
  # COVERAGE GUARD
  # ------------------------------------------------------------------------
  # A previous presence-only guard let March 2026 through on 7 stray rows
  # against Stripe's real 65, producing a nonsense 871% variance. The guard
  # now requires the Stripe month to look fully loaded:
  #     - the month has closed
  #     - events on >= 10 distinct days
  #     - first event on or before the 5th
  #     - last event within 3 days of month end
  # These are heuristics. A per-month load manifest emitted by the loader
  # would be strictly better than inferring completeness from the data;
  # until then, use prod_stripe_load_coverage (below) to audit exclusions.
  # ==========================================================================

  derived_table: {
    sql:
      WITH
      -- ══════════════════════════════════════════════════════════════════
      -- BIGQUERY SIDE  (logic mirrors prod_subscription_churn)
      -- ══════════════════════════════════════════════════════════════════
      flip AS (
        SELECT
          subscription_id,
          TIMESTAMP_SECONDS(
            MIN(SAFE_CAST(JSON_VALUE(subscription, '$.cancelledAt._seconds') AS INT64))
          ) AS cancelled_at_utc,
          MIN(updated_at) AS status_ts
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription`
        WHERE is_deleted = FALSE
          AND status IN ('unpaid', 'canceled')
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

      -- Unbounded, matching production. Inherits the same early-dating risk;
      -- deliberate so this view reconciles against the live tiles.
      dunning AS (
      SELECT subscription_id, MIN(updated_at) AS dunning_end_ts
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
      PARTITION BY t1.subscription_id ORDER BY t1.updated_at DESC
      ) AS rn
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS plan
      WHERE t1.is_deleted = FALSE
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      )
      WHERE rn = 1
      ),

      base_subscriptions AS (
      SELECT
      s.subscription_id,
      s.user_id,
      s.status,
      s.plan_interval,
      CASE
      WHEN s.status NOT IN ('unpaid', 'canceled') THEN NULL
      WHEN s.cancel_at_period_end IS TRUE
      THEN DATE(COALESCE(d.dunning_end_ts, s.current_period_end), 'America/New_York')
      ELSE DATE(
      COALESCE(f.status_flip_at, d.dunning_end_ts, s.cancelled_at),
      'America/New_York'
      )
      END AS effective_end_date
      FROM latest_subscription s
      INNER JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` p
      ON p.user_id = s.user_id
      LEFT JOIN flip_resolved f
      ON f.subscription_id = s.subscription_id
      LEFT JOIN dunning d
      ON d.subscription_id = s.subscription_id
      WHERE p.apps_pop_store = TRUE
      AND p.user_type IN ('seller', 'verifiedSeller')
      AND s.subscription_id IS NOT NULL
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

      -- One row per subscription per event. No creator-level collapse.
      bq_side AS (
      SELECT
      DATE_TRUNC(sm.first_mrr_date, MONTH) AS month_bucket,
      'Subscription Starts'                AS event_type,
      b.subscription_id,
      b.user_id,
      b.plan_interval
      FROM base_subscriptions b
      INNER JOIN subscription_mrr sm
      ON sm.subscription_id = b.subscription_id

      UNION ALL

      SELECT
      DATE_TRUNC(b.effective_end_date, MONTH) AS month_bucket,
      'Subscription Ends'                     AS event_type,
      b.subscription_id,
      b.user_id,
      b.plan_interval
      FROM base_subscriptions b
      INNER JOIN subscription_mrr sm
      ON sm.subscription_id = b.subscription_id
      WHERE b.effective_end_date IS NOT NULL
      AND b.effective_end_date < CURRENT_DATE('America/New_York')
      ),

      -- ══════════════════════════════════════════════════════════════════
      -- STRIPE SIDE
      -- ══════════════════════════════════════════════════════════════════
      stripe_events AS (
      SELECT
      -- STRIPE_LOCAL_DATE -- see TIMEZONE note in the header.
      -- event_date is a local wall clock labelled UTC -> no conversion:
      DATE(event_date)                        AS event_local_date,
      DATE_TRUNC(DATE(event_date), MONTH)     AS month_bucket,
      -- After the backfill, use instead:
      -- DATE(event_date, 'America/New_York')                    AS event_local_date,
      -- DATE_TRUNC(DATE(event_date, 'America/New_York'), MONTH) AS month_bucket,

      CASE row_type
      WHEN 'new_subscribers'     THEN 'Subscription Starts'
      WHEN 'churned_subscribers' THEN 'Subscription Ends'
      END AS event_type,
      subscription_id,
      user_id
      FROM `popshoplive-26f81.dbt_popshop.stripe_analytical_data`
      WHERE row_type IN ('new_subscribers', 'churned_subscribers')
      AND subscription_id IS NOT NULL
      AND event_date IS NOT NULL
      ),

      stripe_side AS (
      SELECT
      month_bucket,
      event_type,
      subscription_id,
      MAX(user_id) AS user_id,     -- MAX ignores NULLs from the LEFT JOIN upstream
      COUNT(*)     AS stripe_row_count
      FROM stripe_events
      GROUP BY 1, 2, 3
      ),

      -- Does Stripe's month look fully loaded? See COVERAGE GUARD in header.
      stripe_coverage AS (
      SELECT
      month_bucket,
      event_type,
      COUNT(DISTINCT event_local_date)          AS stripe_distinct_days,
      EXTRACT(DAY FROM MIN(event_local_date))   AS first_event_day,
      EXTRACT(DAY FROM MAX(event_local_date))   AS last_event_day,
      EXTRACT(DAY FROM LAST_DAY(month_bucket))  AS days_in_month
      FROM stripe_events
      GROUP BY 1, 2
      ),

      comparable AS (
      SELECT month_bucket, event_type
      FROM stripe_coverage
      WHERE month_bucket < DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH)
      AND stripe_distinct_days >= 10
      AND first_event_day <= 5
      AND last_event_day >= days_in_month - 3
      ),

      -- Month lists per subscription, used to tell a date disagreement apart
      -- from a subscription that is genuinely absent from the other system.
      stripe_months AS (
      SELECT
      event_type,
      subscription_id,
      STRING_AGG(DISTINCT FORMAT_DATE('%Y-%m', month_bucket), ', '
      ORDER BY FORMAT_DATE('%Y-%m', month_bucket)) AS months
      FROM stripe_side
      GROUP BY 1, 2
      ),

      bq_months AS (
      SELECT
      event_type,
      subscription_id,
      STRING_AGG(DISTINCT FORMAT_DATE('%Y-%m', month_bucket), ', '
      ORDER BY FORMAT_DATE('%Y-%m', month_bucket)) AS months
      FROM bq_side
      GROUP BY 1, 2
      ),

      comparison AS (
      SELECT
      COALESCE(b.month_bucket,    s.month_bucket)    AS month_bucket,
      COALESCE(b.event_type,      s.event_type)      AS event_type,
      COALESCE(b.subscription_id, s.subscription_id) AS subscription_id,
      COALESCE(b.user_id,         s.user_id)         AS user_id,
      b.plan_interval,
      s.stripe_row_count,
      b.subscription_id IS NOT NULL AS in_bigquery,
      s.subscription_id IS NOT NULL AS in_stripe
      FROM bq_side b
      FULL OUTER JOIN stripe_side s
      ON  s.month_bucket    = b.month_bucket
      AND s.event_type      = b.event_type
      AND s.subscription_id = b.subscription_id
      )

      SELECT
      c.month_bucket,
      c.event_type,
      c.subscription_id,
      c.user_id,
      c.plan_interval,
      COALESCE(c.stripe_row_count, 0) AS stripe_row_count,
      c.in_bigquery,
      c.in_stripe,

      CASE
      WHEN c.in_bigquery AND c.in_stripe THEN 'Matched'
      WHEN c.in_bigquery                 THEN 'Missing in Stripe'
      ELSE                                    'Missing in BigQuery'
      END AS match_status,

      CASE
      WHEN c.in_bigquery AND c.in_stripe THEN 3
      WHEN c.in_bigquery                 THEN 1
      ELSE                                    2
      END AS match_status_sort,

      -- Splits real absence from a month-boundary or churn-date
      -- disagreement, which is the far more common cause.
      CASE
      WHEN c.in_bigquery AND c.in_stripe                    THEN 'Matched'
      WHEN c.in_bigquery AND sm.subscription_id IS NOT NULL THEN 'Date disagreement'
      WHEN c.in_bigquery                                    THEN 'Absent from Stripe'
      WHEN bm.subscription_id IS NOT NULL                   THEN 'Date disagreement'
      ELSE                                                       'Absent from BigQuery'
      END AS mismatch_class,

      CASE
      WHEN c.in_bigquery AND NOT c.in_stripe THEN sm.months
      WHEN c.in_stripe AND NOT c.in_bigquery THEN bm.months
      END AS other_side_months

      FROM comparison c
      INNER JOIN comparable cm
      ON  cm.month_bucket = c.month_bucket
      AND cm.event_type   = c.event_type
      LEFT JOIN stripe_months sm
      ON  sm.event_type      = c.event_type
      AND sm.subscription_id = c.subscription_id
      LEFT JOIN bq_months bm
      ON  bm.event_type      = c.event_type
      AND bm.subscription_id = c.subscription_id
      {% if month_filter._is_filtered %}
      WHERE {% condition month_filter %} TIMESTAMP(c.month_bucket) {% endcondition %}
      {% endif %}
      ;;
  }

  # ——— Filters ———

  filter: month_filter {
    type: date
    convert_tz: no
    label: "Month (filter)"
    description: "Filter by event month, America/New_York. Use 'is in range'. Optional."
  }

  # ——— Dimensions ———

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(
           CAST(${TABLE}.month_bucket AS STRING), '|',
           ${TABLE}.event_type, '|',
           ${TABLE}.subscription_id
         ) ;;
  }

  dimension_group: month {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.month_bucket ;;
    timeframes: [date, month, quarter, year]
    label: "Month"
    description: "Event month, America/New_York. Only months that pass the coverage guard appear. See prod_stripe_load_coverage for exclusions."
  }

  dimension: event_type {
    type: string
    sql: ${TABLE}.event_type ;;
    label: "Event Type"
    description: "Subscription Starts (Stripe new_subscribers, includes reactivations) or Subscription Ends (Stripe churned_subscribers)."
  }

  dimension: match_status {
    type: string
    sql: ${TABLE}.match_status ;;
    order_by_field: match_status_sort
    label: "Match Status"
    description: "Missing in Stripe = BigQuery counted a subscription Stripe did not. Missing in BigQuery = the reverse. Matched = both agree on subscription and month."
  }

  dimension: match_status_sort {
    type: number
    sql: ${TABLE}.match_status_sort ;;
    hidden: yes
  }

  dimension: mismatch_class {
    type: string
    sql: ${TABLE}.mismatch_class ;;
    label: "Mismatch Class"
    description: "Date disagreement = the subscription exists in the other system but in a different month (month boundary, or a churn-date derivation difference). Absent = it does not exist there at all, which is the only class implying real data loss."
  }

  dimension: other_side_months {
    type: string
    sql: ${TABLE}.other_side_months ;;
    label: "Other Side Months"
    description: "For a mismatch, the month(s) the other system placed this subscription in. NULL when matched or genuinely absent."
  }

  dimension: is_date_disagreement {
    type: yesno
    sql: ${TABLE}.mismatch_class = 'Date disagreement' ;;
    label: "Is Date Disagreement"
  }

  dimension: is_mismatch {
    type: yesno
    sql: ${TABLE}.match_status != 'Matched' ;;
    label: "Is Mismatch"
    description: "Yes for any subscription the two systems disagree about. Use as a tile filter to build the exception list."
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
    description: "Creator. NULL for Stripe rows whose customer_id did not resolve to a user_id upstream."
  }

  dimension: plan_interval {
    type: string
    sql: ${TABLE}.plan_interval ;;
    label: "Plan: Interval"
    description: "From BigQuery. NULL for subscriptions present only in Stripe."
  }

  dimension: stripe_row_count {
    type: number
    sql: ${TABLE}.stripe_row_count ;;
    label: "Stripe Row Count"
    description: "Rows Stripe emitted for this subscription-month. Normally 1; higher means a repeated event."
  }

  dimension: in_bigquery {
    type: yesno
    sql: ${TABLE}.in_bigquery ;;
    label: "In BigQuery"
  }

  dimension: in_stripe {
    type: yesno
    sql: ${TABLE}.in_stripe ;;
    label: "In Stripe"
  }

  # ——— Measures: subscription grain (Stripe's grain — use these) ———

  measure: stripe_subscriptions {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [in_stripe: "yes"]
    label: "Stripe Total (Subscriptions)"
    description: "Distinct subscriptions Stripe reports. This is the figure on Stripe's dashboard: New + Reactivation for starts, Churn for ends."
    drill_fields: [drilldown*]
  }

  measure: bq_subscriptions {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [in_bigquery: "yes"]
    label: "BigQuery Total (Subscriptions)"
    description: "Distinct subscriptions BigQuery reports. Compare against Stripe Total (Subscriptions) — same grain."
    drill_fields: [drilldown*]
  }

  measure: stripe_rows {
    type: sum
    sql: ${TABLE}.stripe_row_count ;;
    label: "Stripe Total (Rows)"
    description: "Raw row count in stripe_analytical_data. Equals Stripe Total (Subscriptions) unless a subscription emitted the same event twice in one month."
  }

  measure: matched_subscriptions {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [match_status: "Matched"]
    label: "Matched"
    drill_fields: [drilldown*]
  }

  measure: missing_in_stripe {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [match_status: "Missing in Stripe"]
    label: "Missing in Stripe"
    description: "Subscriptions BigQuery counted that Stripe did not, in this month."
    drill_fields: [drilldown*]
  }

  measure: missing_in_bigquery {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [match_status: "Missing in BigQuery"]
    label: "Missing in BigQuery"
    description: "Subscriptions Stripe counted that BigQuery did not, in this month."
    drill_fields: [drilldown*]
  }

  measure: missing_in_bigquery_negated {
    type: number
    sql: -1 * ${missing_in_bigquery} ;;
    label: "Missing in BigQuery (Negated)"
    description: "For diverging-bar display only. Do not use in tables."
  }

  # ——— Measures: creator grain (for comparing to prod_subscription_churn) ———

  measure: stripe_creators {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [in_stripe: "yes"]
    label: "Stripe Total (Creators)"
    description: "Distinct creators. DO NOT compare against Stripe's dashboard, which reports subscriptions. Provided only for grain analysis."
  }

  measure: bq_creators {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [in_bigquery: "yes"]
    label: "BigQuery Total (Creators)"
    description: "Distinct creators. Closest analogue to prod_subscription_churn, though that view SUMs distinct creators per plan_interval and so sits between this and the subscription count."
  }

  measure: multi_subscription_creators {
    type: number
    sql: ${bq_subscriptions} - ${bq_creators} ;;
    label: "Creators With Extra Subscriptions"
    description: "Subscription count minus creator count. This is the size of the grain gap and explains most of the difference between this view and prod_subscription_churn."
  }

  measure: churn_tile_replica {
    type: number
    sql: ${bq_creators} + ${multi_subscription_creators} ;;
    label: "BigQuery Total (Churn Tile Grain)"
    description: "Approximates prod_subscription_churn's hybrid grain. Diagnostic only — do not compare to Stripe."
  }

  # ——— Measures: variance and verification ———

  measure: net_variance {
    type: number
    sql: ${bq_subscriptions} - ${stripe_subscriptions} ;;
    label: "Net Variance"
    description: "BigQuery minus Stripe, at subscription grain. Positive = BigQuery overcounts. Hold this to a tolerance, not the sum of the two directions."
  }

  measure: reconciliation_check {
    type: number
    sql: (${bq_subscriptions} - ${missing_in_stripe} + ${missing_in_bigquery})
      - ${stripe_subscriptions} ;;
    label: "Reconciliation Check"
    description: "Must always be 0. Verifies BigQuery - Missing in Stripe + Missing in BigQuery = Stripe. A non-zero value means the join grain is broken, not that the data disagrees."
  }

  measure: total_disagreements {
    type: number
    sql: ${missing_in_stripe} + ${missing_in_bigquery} ;;
    label: "Total Disagreements"
    description: "Both directions summed. Offsetting pairs here usually mean a date disagreement rather than missing data — check Mismatch Class."
  }

  measure: date_disagreements {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [mismatch_class: "Date disagreement"]
    label: "Date Disagreements"
    description: "Mismatches where the subscription exists in both systems but in different months. Not data loss."
    drill_fields: [drilldown*]
  }

  measure: genuinely_absent {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [mismatch_class: "Absent from Stripe, Absent from BigQuery"]
    label: "Genuinely Absent"
    description: "Mismatches where the subscription does not exist in the other system at all. This is the only class that implies real data loss."
    drill_fields: [drilldown*]
  }

  measure: pct_net_variance {
    type: number
    sql: SAFE_DIVIDE(${net_variance}, ${stripe_subscriptions}) ;;
    value_format_name: percent_1
    label: "% Net Variance"
    description: "Net Variance as a share of Stripe's count. The tolerance metric."
  }

  measure: match_rate {
    type: number
    sql: SAFE_DIVIDE(${matched_subscriptions},
      ${matched_subscriptions} + ${missing_in_stripe} + ${missing_in_bigquery}) ;;
    value_format_name: percent_1
    label: "Match Rate"
  }

  measure: latest_comparable_month {
    type: date
    convert_tz: no
    sql: MAX(${TABLE}.month_bucket) ;;
    label: "Latest Comparable Month"
    description: "Most recent month passing the coverage guard. Put this on the dashboard so the comparison window is visible."
  }

  # ——— Drill Set ———

  set: drilldown {
    fields: [
      month_month,
      event_type,
      match_status,
      mismatch_class,
      other_side_months,
      subscription_id,
      user_id,
      plan_interval,
      stripe_row_count
    ]
  }
}


view: prod_stripe_load_coverage {
  # ==========================================================================
  # Audit companion. One row per (month, event_type) in stripe_analytical_data
  # with the coverage diagnostics and the pass/fail verdict, INCLUDING months
  # the main view excludes. Use this to answer "why is March missing?" without
  # letting a partially-loaded month leak a wrong variance figure into a tile.
  # ==========================================================================

  derived_table: {
    sql:
      WITH stripe_events AS (
        SELECT
          -- Keep in lockstep with STRIPE_LOCAL_DATE in the main view.
          DATE(event_date)                    AS event_local_date,
          DATE_TRUNC(DATE(event_date), MONTH) AS month_bucket,
          CASE row_type
            WHEN 'new_subscribers'     THEN 'Subscription Starts'
            WHEN 'churned_subscribers' THEN 'Subscription Ends'
          END AS event_type,
          subscription_id,
          user_id
        FROM `popshoplive-26f81.dbt_popshop.stripe_analytical_data`
        WHERE row_type IN ('new_subscribers', 'churned_subscribers')
          AND event_date IS NOT NULL
      )
      SELECT
        month_bucket,
        event_type,
        COUNT(*)                                 AS stripe_rows,
        COUNT(DISTINCT subscription_id)          AS stripe_subscriptions,
        COUNT(DISTINCT user_id)                  AS stripe_creators,
        COUNT(DISTINCT event_local_date)         AS distinct_days,
        MIN(event_local_date)                    AS first_event_date,
        MAX(event_local_date)                    AS last_event_date,
        EXTRACT(DAY FROM LAST_DAY(month_bucket)) AS days_in_month,
        (
          month_bucket < DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH)
          AND COUNT(DISTINCT event_local_date) >= 10
          AND EXTRACT(DAY FROM MIN(event_local_date)) <= 5
          AND EXTRACT(DAY FROM MAX(event_local_date))
              >= EXTRACT(DAY FROM LAST_DAY(month_bucket)) - 3
        ) AS is_comparable
      FROM stripe_events
      GROUP BY 1, 2
      ;;
  }

  dimension: primary_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: CONCAT(CAST(${TABLE}.month_bucket AS STRING), '|', ${TABLE}.event_type) ;;
  }

  dimension_group: month {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.month_bucket ;;
    timeframes: [date, month, quarter, year]
    label: "Month"
  }

  dimension: event_type {
    type: string
    sql: ${TABLE}.event_type ;;
    label: "Event Type"
  }

  dimension: is_comparable {
    type: yesno
    sql: ${TABLE}.is_comparable ;;
    label: "Is Comparable"
    description: "Yes when the month has closed and Stripe's load looks complete: events on >=10 distinct days, first by the 5th, last within 3 days of month end. Only Yes months appear in prod_stripe_bq_reconciliation."
  }

  dimension: distinct_days {
    type: number
    sql: ${TABLE}.distinct_days ;;
    label: "Distinct Days With Events"
  }

  dimension: days_in_month {
    type: number
    sql: ${TABLE}.days_in_month ;;
  }

  dimension_group: first_event {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.first_event_date ;;
    timeframes: [date]
    label: "First Event"
  }

  dimension_group: last_event {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.last_event_date ;;
    timeframes: [date]
    label: "Last Event"
  }

  measure: stripe_rows {
    type: sum
    sql: ${TABLE}.stripe_rows ;;
    label: "Stripe Rows"
  }

  measure: stripe_subscriptions {
    type: sum
    sql: ${TABLE}.stripe_subscriptions ;;
    label: "Stripe Subscriptions"
  }

  measure: stripe_creators {
    type: sum
    sql: ${TABLE}.stripe_creators ;;
    label: "Stripe Creators"
    description: "Summed across event types, so not a distinct count across the whole table."
  }
}

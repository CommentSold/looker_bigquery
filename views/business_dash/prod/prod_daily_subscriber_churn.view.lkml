# =============================================================================
# prod_daily_subscriber_churn
# -----------------------------------------------------------------------------
# Paid subscriptions that CHURNED, per day, by reason and by how they were
# acquired. Counterpart to prod_daily_subscribers_report, and the replacement
# for prod_paid_subscription_cancellations.
#
# GRAIN: one row per SUBSCRIPTION that collected at least one billable payment
# and has since reached a terminal or dunning status.
#
# -----------------------------------------------------------------------------
# !! LIVE INCIDENT AS AT 2026-09-08: DUNNING WRITE-OFF HAS STOPPED !!
# -----------------------------------------------------------------------------
# Payment-failure churn is being UNDERCOUNTED from 2026-09-01. Not a reporting
# bug — the upstream process that marks a failed invoice `uncollectible` has
# stopped running, and that mark is what this view keys on.
#
# Evidence gathered 2026-09-08:
#   * Failures continue at the normal rate. Billable invoices not collected ran
#     23-35% of volume through August and 23-35% through September. Unchanged.
#   * The write-off sweep ran daily through 2026-08-31, marking 10-18 invoices
#     a day, always invoices 1-2 days old. From 2026-09-01 it stops. The only
#     three markings since hit invoices from 4 Jul, 21 Jul and 19 Jun — a
#     cleanup pass, not the routine sweep.
#   * Retries still run. September invoices reach attempt_count 2, the same
#     ceiling that triggered write-off in August, then sit at `open`. Invoices
#     from 1-4 September are 4-7 days old with attempts exhausted and unmoved.
#   * 38 subscriptions are stuck in past_due: none marked unpaid, none written
#     off.
#
# Those 38 are real churn this view cannot yet see. They appear under
# Payment Retrying (Not Yet Churned) and will land on their write-off date once
# the process resumes, so early-September bars will GROW retroactively.
#
# Dunning Stalled (Should Be Near 0) monitors this. It counts subscriptions in
# dunning for more than 5 days without resolution — against an August baseline
# of about 2 days. If it climbs again, the write-off process has stopped again.
#
# Note the start date: 2026-09-01, a day BEFORE the trial-removal deploy at
# 2026-09-02 15:30 UTC. Probably unrelated to it.
#
# -----------------------------------------------------------------------------
# WHY THE SUBSCRIBERS TILE NEVER DECREASES
# -----------------------------------------------------------------------------
# prod_daily_subscribers_report exists because an invoice was collected, and
# churning does not delete the invoice, so its past bars are frozen. Verified
# 2026-09-08: trial_end is never overwritten on cancellation (0 cases across
# 1,042 canceled / 918 active / 156 unpaid / 38 past_due), so its SEGMENTATION
# is frozen too, not just its totals.
#
# Net = New Subscribers minus Subscriptions Churned, as a merged query. Valid
# only because the eligibility filters below are character-for-character
# identical to that view. If they drift, the net is wrong and nothing in Looker
# will say so.
#
# -----------------------------------------------------------------------------
# !! THE CLASSIFIER BUG THAT WAS FIXED HERE — do not reintroduce it !!
# -----------------------------------------------------------------------------
# The payment-failure branch tests `cancellation_applied_at IS NULL`. An earlier
# draft of this view tested `status_flip_cancelled_at IS NULL` instead. That
# column is LEAST(cancelledAt, MIN(updated_at)), and the source table holds one
# row per subscription (6,336 rows, 6,336 subscriptions), so MIN(updated_at) is
# always populated and the column is NEVER NULL for a canceled or unpaid row.
# The branch could not fire and every dunning write-off was labelled Voluntary.
#
# Measured cost of that bug, churns misclassified per month:
#   Apr 12   May 23   Jun 101   Jul 129   Aug 140
# From June onward it hid EVERY payment failure that arrived via write-off.
#
# The two fields are not interchangeable. cancellation_applied_at is populated
# only when someone actually cancelled; its absence alongside a dunning invoice
# is what distinguishes an involuntary write-off from a voluntary exit.
#
# -----------------------------------------------------------------------------
# !! WHAT prod_paid_subscription_cancellations MISSES !!
# -----------------------------------------------------------------------------
# It filters trial_end IS NOT NULL in both latest_subscription and
# extracted_timestamps, so any subscription without a trial is invisible to it.
#
# Measured over 934 churned paid subscriptions: 151 invisible, 16%.
#   by first-paid month:  Jan 63%  Feb 85%  Mar 96%   (the no-trial window,
#                                                      2026-01-16 to 2026-03-24)
#                         Apr 8%   May 7%   Jun 6%   Jul 12%   Aug 16%
#
# The Apr-Aug remainder is annual plans plus REACTIVATIONS. The trial is a
# once-per-creator coupon, so a returning subscriber never got one, never had a
# trial_end, and their churn has never appeared in that view. Nobody knew that
# because nobody knew the trial was a coupon.
#
# From 2026-09-02 no new subscription has a trial, so that view will show
# near-zero churn from roughly October and the drop will read as a retention
# win. THIS VIEW HAS NO trial_end FILTER. To reproduce the old tile for
# comparison, filter Came Via Trial = Yes. Reconciliation targets, trial-only
# churn split payment-failed / voluntary:
#   Jun 86/99    Jul 115/135    Aug 120/127
#
# Two more corrections to that file, both measured:
#   * its header says fact_seller_subscription "stores change history" and it
#     dedups on updated_at DESC. It does not: 6,336 rows for 6,336
#     subscriptions. The dedup is a no-op. This view does not dedup.
#   * it reads paid_at from invoice.updated_at, measured 124 days late on
#     average for January 2026 invoices. This view uses
#     COALESCE(created, created_at), matching prod_subscription_churn.
#
# -----------------------------------------------------------------------------
# THE CHURN DATE
# -----------------------------------------------------------------------------
# Reused byte-for-byte from prod_subscription_churn, validated to within about
# 5 a month against Stripe. NOT re-derived — a second derivation of the same
# concept is how two views end up disagreeing.
#
#   cancel_at_period_end TRUE -> COALESCE(dunning_end, current_period_end)
#   otherwise                 -> COALESCE(Stripe $.cancelledAt, dunning_end,
#                                         cancelled_at)
#
# past_due resolves to NULL on purpose: still open, not yet churn. Those rows
# are kept and flagged Is Churned = No so the dunning pipeline stays visible —
# which is how the incident above became findable.
#
# !! FUTURE-DATED CHURN !! With cancel_at_period_end the churn date can be in
# the future: cancelled now, paid period runs on. prod_subscription_churn drops
# those; this view exposes them as Is Future Dated and every churn measure
# excludes them. Any reconciliation against that view needs the same exclusion.
#
# KNOWN DEFECTS, inherited, sized 2026-09-08:
#   * current_period_end overwritten to the cancellation instant on 27
#     subscriptions (handoff item #4). Where cancel_at_period_end is also TRUE
#     the churn is dated at cancellation rather than at the true period end —
#     too early. Is Period End Overwritten flags them.
#   * unbounded dunning lookback (handoff item #3): 590 subscriptions carry
#     dunning and in ZERO does the dunning timestamp precede the last successful
#     payment. Real in principle, no current exposure. Churn Before First
#     Payment monitors it.
#   * 105 subscriptions have a trial length other than 7, 14 or 30 days —
#     manual extensions. They are trials and count as such.
#
# -----------------------------------------------------------------------------
# VALIDATION — run before publishing
# -----------------------------------------------------------------------------
# 1. Subscriptions Churned by month within about 5 of prod_subscription_churn.
#    A bigger gap is a population difference, not a date-logic difference.
# 2. Churn Reason Accounting Check = 0 and Trial Split Accounting Check = 0 on
#    every row.
# 3. Churn Before First Payment = 0 on every row.
# 4. Dunning Stalled near 0 — currently it is NOT, see the incident above.
# =============================================================================

view: prod_daily_subscriber_churn {
  derived_table: {
    sql:
      WITH
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
      user_id, marketing_campaign, onboarding_path, plan_level,
      utm_regintent, business_type, device_category, user_agent
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
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_campaign')        AS utm_campaign,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_source')          AS utm_source,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent')       AS utm_regintent,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_onboarding_path') AS onboarding_path,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_planlevel')       AS plan_level,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')          AS user_agent,
      JSON_VALUE(private_profile, '$.email')                                          AS profile_email,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.firstName')                AS first_name,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.lastName')                 AS last_name
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      ),

      -- Regintent only, for eligibility. Kept separate from marketing_capture
      -- so the eligibility block stays byte-identical to the other prod views.
      regintent_capture AS (
      SELECT
      user_id,
      ANY_VALUE(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent')) AS utm_regintent
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      WHERE user_id IS NOT NULL
      GROUP BY user_id
      ),

      -- IDENTICAL to prod_daily_subscribers_report. Netting the two is only
      -- valid if these match exactly. Audited 2026-09-05: drops internal emails
      -- (11-13/month), vidcon (1-2) and one apps_pop_store case, and nothing at
      -- all to the dim_stores join.
      eligible_creators AS (
      SELECT
      p.user_id,
      MIN(DATE(s.created_at, 'America/New_York')) AS signup_date
      FROM `popshoplive-26f81.dbt_popshop.dim_profiles` p
      INNER JOIN `popshoplive-26f81.dbt_popshop.dim_stores` s
      ON p.user_id = s.store_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = p.user_id
      LEFT JOIN regintent_capture rc       ON rc.user_id = p.user_id
      LEFT JOIN onboarding_events_dedup oe ON oe.user_id = p.user_id
      WHERE p.apps_pop_store = TRUE
      AND p.user_type IN ('seller', 'verifiedSeller')
      -- COALESCE around the comparison, never NOT (COALESCE(...) = 'x').
      AND NOT COALESCE(COALESCE(oe.utm_regintent, rc.utm_regintent) = 'vidcon', FALSE)
      AND (pprof.email IS NULL
      OR NOT REGEXP_CONTAINS(LOWER(pprof.email),
      r'@(test\.com|example\.com|popshoplive\.com|commentsold\.com|pop\.store)$'))
      GROUP BY p.user_id
      ),

      prior_trials AS (
      SELECT user_id, MIN(initial_start_date) AS first_trial_start_ts
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription`
      WHERE is_deleted = FALSE
      AND trial_end > initial_start_date
      AND EXISTS (SELECT 1 FROM UNNEST(plans) AS pl
      WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') = 'plan')
      GROUP BY user_id
      ),

      -- Gate: at least one collected billable payment. Same gate as the
      -- subscribers view, so the two cover the same subscriptions.
      -- Deliberately NO payment floor against trial_end — that reintroduces a
      -- trial-shaped filter and drops direct purchases.
      payments AS (
      SELECT
      subscription_id,
      user_id,
      COUNT(DISTINCT invoice_id)         AS payments_collected,
      MIN(COALESCE(created, created_at)) AS first_paid_ts,
      MAX(COALESCE(created, created_at)) AS last_paid_ts,
      SUM(amount_paid) / 100             AS revenue_collected
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      AND status = 'paid'
      AND amount_due  > 0
      AND amount_paid > 0
      GROUP BY subscription_id, user_id
      ),

      sequenced AS (
      SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY user_id
      ORDER BY first_paid_ts ASC, subscription_id ASC) AS payment_sequence
      FROM payments
      ),

      -- First invoice that was DUE and never collected: when trouble started.
      -- Feeds the dunning-stall monitor.
      first_failure AS (
      SELECT subscription_id, MIN(COALESCE(created, created_at)) AS first_unpaid_invoice_ts
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      AND amount_due > 0
      AND COALESCE(amount_paid, 0) = 0
      GROUP BY subscription_id
      ),

      -- No trial_end filter here, unlike prod_paid_subscription_cancellations.
      extracted_timestamps AS (
      SELECT
      subscription_id,
      TIMESTAMP_SECONDS(SAFE_CAST(
      ANY_VALUE(JSON_VALUE(subscription, '$.cancelledAt._seconds')) AS INT64)
      ) AS cancelled_at_utc,
      MIN(updated_at) AS canceled_at_ts
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription`
      WHERE is_deleted = FALSE
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

      dunning AS (
      SELECT subscription_id, MIN(updated_at) AS dunning_end_ts
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      AND status IN ('uncollectible', 'void')
      GROUP BY subscription_id
      ),

      -- No ROW_NUMBER dedup: verified 6,336 rows for 6,336 subscriptions.
      subscription_plan AS (
      SELECT
      t1.subscription_id,
      t1.user_id,
      t1.status,
      t1.cancel_at_period_end,
      CAST(t1.initial_start_date AS TIMESTAMP)      AS initial_start_date,
      CAST(t1.trial_end AS TIMESTAMP)               AS trial_end,
      CAST(t1.cancellation_applied_at AS TIMESTAMP) AS cancellation_applied_at,
      t1.cancelled_at,
      t1.current_period_end,
      COALESCE(t1.discounted_price, t1.price + t1.tax_amount) AS price,
      JSON_VALUE(t1.subscription, '$.coupon.id')   AS coupon_id,
      JSON_VALUE(t1.subscription, '$.coupon.name') AS coupon_name,
      JSON_EXTRACT_SCALAR(plan, '$.productName')   AS plan_name,
      JSON_EXTRACT_SCALAR(plan, '$.interval')      AS plan_interval,
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
      sp.subscription_id,
      sp.user_id,
      sp.status AS subscription_status,
      sp.cancel_at_period_end,

      sq.payments_collected,
      sq.revenue_collected,
      sq.payment_sequence,
      DATE(sq.first_paid_ts, 'America/New_York') AS first_paid_date,
      DATE(sq.last_paid_ts,  'America/New_York') AS last_paid_date,

      -- PRIMARY AXIS. Byte-for-byte prod_subscription_churn.
      CASE
      WHEN sp.status NOT IN ('unpaid', 'canceled') THEN NULL
      WHEN sp.cancel_at_period_end IS TRUE
      THEN DATE(COALESCE(d.dunning_end_ts, sp.current_period_end), 'America/New_York')
      ELSE DATE(
      COALESCE(c.status_flip_cancelled_at, d.dunning_end_ts, sp.cancelled_at),
      'America/New_York')
      END AS churn_date,

      -- Coarse, for the pivot.
      -- cancellation_applied_at IS NULL, NOT status_flip_cancelled_at —
      -- the latter is never null and silences this branch entirely.
      CASE
      WHEN sp.status IN ('unpaid', 'incomplete_expired') THEN 'Payment failed'
      WHEN sp.status = 'past_due' THEN 'Payment retrying'
      WHEN sp.status = 'canceled'
      AND d.dunning_end_ts IS NOT NULL
      AND sp.cancellation_applied_at IS NULL THEN 'Payment failed'
      WHEN sp.status = 'canceled' THEN 'Voluntary cancellation'
      ELSE sp.status
      END AS churn_reason,

      -- Detailed, for drills and support questions.
      CASE
      WHEN sp.status = 'unpaid'             THEN 'Payment failed - Stripe unpaid'
      WHEN sp.status = 'incomplete_expired' THEN 'Payment failed - incomplete expired'
      WHEN sp.status = 'past_due'           THEN 'Payment retrying - in dunning'
      WHEN sp.status = 'canceled'
      AND d.dunning_end_ts IS NOT NULL
      AND sp.cancellation_applied_at IS NULL
      THEN 'Payment failed - dunning write-off'
      WHEN sp.status = 'canceled' AND sp.cancel_at_period_end IS TRUE
      THEN 'Voluntary - cancelled at period end'
      WHEN sp.status = 'canceled'           THEN 'Voluntary - cancelled immediately'
      ELSE sp.status
      END AS churn_reason_detail,

      -- Same four values as prod_daily_subscribers_report.
      CASE
      WHEN sq.payment_sequence > 1 THEN 'Reactivated (paid before)'
      WHEN sp.trial_end IS NOT NULL AND sp.trial_end > sp.initial_start_date
      THEN 'New - trial converted'
      WHEN COALESCE(pt.first_trial_start_ts < sp.initial_start_date, FALSE)
      THEN 'New - paid upfront (trial already used)'
      ELSE 'New - paid upfront (no trial)'
      END AS acquisition_segment,

      sp.trial_end IS NOT NULL AND sp.trial_end > sp.initial_start_date AS came_via_trial,

      sp.current_period_end = sp.cancelled_at AS is_period_end_overwritten,

      ff.first_unpaid_invoice_ts,
      DATE(ff.first_unpaid_invoice_ts, 'America/New_York') AS first_unpaid_date,
      DATE_DIFF(CURRENT_DATE('America/New_York'),
      DATE(ff.first_unpaid_invoice_ts, 'America/New_York'), DAY) AS days_since_first_failure,

      DATE(sp.initial_start_date, 'America/New_York') AS subscription_start_date,
      DATE(sp.trial_end,          'America/New_York') AS trial_end_date,
      DATE_DIFF(DATE(sp.trial_end, 'America/New_York'),
      DATE(sp.initial_start_date, 'America/New_York'), DAY) AS trial_length_days,

      ec.signup_date,

      COALESCE(oe.marketing_campaign, mc.utm_campaign)                            AS marketing_campaign,
      COALESCE(oe.utm_regintent,      mc.utm_regintent)                           AS utm_regintent,
      COALESCE(oe.business_type,      JSON_VALUE(prof.profile, '$.businessType')) AS business_type,
      COALESCE(oe.onboarding_path,    mc.onboarding_path)                         AS onboarding_path,
      COALESCE(oe.plan_level,         mc.plan_level)                              AS plan_level,
      COALESCE(oe.device_category,    'No Onboarding Event')                      AS device_category,
      COALESCE(oe.user_agent,         mc.user_agent)                              AS user_agent,
      mc.utm_source                                                               AS utm_source,
      CASE
      WHEN COALESCE(oe.marketing_campaign, mc.utm_campaign) IS NOT NULL THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source,

      prof.username  AS sign_up_user_username,
      prof.url_code  AS sign_up_url_code,
      pprof.email    AS sign_up_user_email,
      mc.profile_email,
      mc.first_name,
      mc.last_name,

      sp.plan_name,
      sp.plan_interval,
      sp.price,
      sp.coupon_id,
      sp.coupon_name,
      COALESCE(sp.plan_entries, 0) AS plan_entries

      FROM subscription_plan sp
      INNER JOIN eligible_creators ec ON ec.user_id = sp.user_id
      INNER JOIN sequenced sq         ON sq.subscription_id = sp.subscription_id
      LEFT JOIN canceled_status_at c  ON c.subscription_id = sp.subscription_id
      LEFT JOIN dunning d             ON d.subscription_id = sp.subscription_id
      LEFT JOIN first_failure ff      ON ff.subscription_id = sp.subscription_id
      LEFT JOIN prior_trials pt       ON pt.user_id = sp.user_id
      LEFT JOIN onboarding_events_dedup oe ON oe.user_id = sp.user_id
      LEFT JOIN marketing_capture mc  ON mc.user_id = sp.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof
      ON prof.user_id = sp.user_id
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
      ON pprof.user_id = sp.user_id
      WHERE sp.status IN ('canceled', 'unpaid', 'past_due', 'incomplete_expired')
      ;;
  }

  # ——— Primary Key ———

  dimension: subscription_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.subscription_id ;;
    label: "Subscription ID"
  }

  dimension: user_id { type: string sql: ${TABLE}.user_id ;; label: "User ID" }

  # ——— Time ———

  dimension_group: churn {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.churn_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Churn"
    description: "Day the paid subscription churned, America/New_York. THE x-axis. NULL for past_due, which is still open. Same derivation as prod_subscription_churn, validated to within about 5 a month against Stripe."
  }

  dimension_group: first_paid {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.first_paid_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "First Paid"
    description: "When this subscription first collected. The COHORT axis — use it to ask how a given month's intake has since churned. Matches Paid Date in prod_daily_subscribers_report."
  }

  dimension_group: last_paid {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.last_paid_date ;;
    timeframes: [date, week, month]
    label: "Last Paid"
  }

  dimension_group: first_unpaid {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.first_unpaid_date ;;
    timeframes: [date, week, month]
    label: "First Failed Invoice"
    description: "First billable invoice that was raised and never collected — when trouble started. Feeds the dunning-stall monitor."
  }

  dimension_group: subscription_start {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.subscription_start_date ;;
    timeframes: [date, week, month]
    label: "Subscription Start"
  }

  dimension_group: trial_end {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.trial_end_date ;;
    timeframes: [date, week, month]
    label: "Trial End"
    description: "NULL for subscriptions that never had a trial — everything created after 2026-09-02, and annual plans before it."
  }

  dimension_group: signup {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.signup_date ;;
    timeframes: [date, week, month]
    label: "Signup"
  }

  dimension: lifetime_days {
    type: number
    sql: DATE_DIFF(${TABLE}.churn_date, ${TABLE}.first_paid_date, DAY) ;;
    label: "Paid Lifetime (Days)"
  }

  dimension: lifetime_band {
    type: tier
    tiers: [1, 31, 62, 93, 186, 365]
    style: integer
    sql: DATE_DIFF(${TABLE}.churn_date, ${TABLE}.first_paid_date, DAY) ;;
    label: "Paid Lifetime Band"
    description: "Under a month means they churned inside their first billing cycle. Worth watching across 2026-09-02: firstmonth50%off halves the first invoice, so a first-cycle churn now recovers less."
  }

  # ——— Churn shape ———

  dimension: is_churned {
    type: yesno
    sql: ${TABLE}.churn_date IS NOT NULL ;;
    label: "Is Churned"
    description: "No for past_due, which is still open. Those rows are kept so the dunning pipeline stays visible — FILTER TO YES on any churn tile."
  }

  dimension: is_future_dated {
    type: yesno
    sql: ${TABLE}.churn_date >= CURRENT_DATE('America/New_York') ;;
    label: "Is Future Dated"
    description: "Yes when cancel_at_period_end pushes the churn date beyond today. prod_subscription_churn drops these; this view exposes them and every churn measure excludes them."
  }

  dimension: is_period_end_overwritten {
    type: yesno
    sql: ${TABLE}.is_period_end_overwritten ;;
    label: "Is Period End Overwritten"
    description: "current_period_end equals the cancellation instant — handoff item #4, 27 subscriptions as at 2026-09-08. Where cancel_at_period_end is also TRUE the churn is dated at cancellation rather than at the true period end, so it is recorded too early."
  }

  dimension: days_since_first_failure {
    type: number
    sql: ${TABLE}.days_since_first_failure ;;
    label: "Days Since First Failed Invoice"
    description: "Days from the first uncollected invoice to today. Through August, write-off followed within about 2 days. Anything much larger on an unresolved subscription means dunning has stalled."
  }

  dimension: is_dunning_stalled {
    type: yesno
    sql: ${TABLE}.churn_date IS NULL
      AND ${TABLE}.days_since_first_failure > 5 ;;
    label: "Is Dunning Stalled"
    description: "Unresolved for more than 5 days after its first failed invoice. The 5 is not arbitrary: through August the write-off sweep marked invoices 1-2 days old, daily. As at 2026-09-08 this is NON-ZERO — the write-off process stopped on 2026-09-01 and 38 subscriptions are stuck in past_due. See the view header."
  }

  dimension: churn_reason {
    type: string
    sql: ${TABLE}.churn_reason ;;
    label: "Churn Reason"
    description: "Three values, for pivoting: Payment failed, Voluntary cancellation, Payment retrying. USE THIS on the stacked bar. Churn Reason (Detailed) has six and belongs on tables."
  }

  dimension: churn_reason_detail {
    type: string
    sql: ${TABLE}.churn_reason_detail ;;
    label: "Churn Reason (Detailed)"
    description: "Six values. Splits payment failure into Stripe unpaid, incomplete expired and dunning write-off, and voluntary into cancelled immediately versus cancelled at period end."
  }

  dimension: acquisition_segment {
    type: string
    sql: ${TABLE}.acquisition_segment ;;
    label: "Acquisition Segment"
    description: "How this subscription was acquired, same four values as prod_daily_subscribers_report. Pivot on this to compare retention between trial converters and direct purchasers."
  }

  dimension: came_via_trial {
    type: yesno
    sql: ${TABLE}.came_via_trial ;;
    label: "Came Via Trial"
    description: "Filter to Yes to reproduce the old Daily Paid Subscriber Churn (Post-Trial) tile. That view requires this and so misses 16% of churn — annual plans and, crucially, reactivations, which never received a trial because the trial was a once-per-creator coupon."
  }

  dimension: payment_sequence {
    type: number
    sql: ${TABLE}.payment_sequence ;;
    label: "Payment Sequence"
    description: "1 for the creator's first paid subscription. Above 1 means this churn is a reactivation that has lapsed again."
  }

  dimension: payments_collected {
    type: number
    sql: ${TABLE}.payments_collected ;;
    label: "Payments Collected"
    description: "Billable invoices collected before churn. 1 means they never renewed."
  }

  dimension: revenue_collected {
    type: number
    sql: ${TABLE}.revenue_collected ;;
    value_format_name: usd
    label: "Revenue Collected"
  }

  dimension: trial_length_days {
    type: number
    sql: ${TABLE}.trial_length_days ;;
    label: "Trial Length (Days)"
    description: "7, 14 or 30 for standard coupons. 105 subscriptions carry other lengths — manual extensions. They are trials and count as such."
  }

  # ——— Marketing attribution ———

  dimension: marketing_campaign { type: string sql: ${TABLE}.marketing_campaign ;; label: "Marketing Campaign" }
  dimension: utm_regintent      { type: string sql: ${TABLE}.utm_regintent ;;      label: "UTM Regintent" }
  dimension: utm_source         { type: string sql: ${TABLE}.utm_source ;;         label: "UTM Source" }
  dimension: business_type      { type: string sql: ${TABLE}.business_type ;;      label: "Business Type" }
  dimension: onboarding_path    { type: string sql: ${TABLE}.onboarding_path ;;    label: "Onboarding Path" }
  dimension: plan_level         { type: string sql: ${TABLE}.plan_level ;;         label: "Plan Level" }
  dimension: device_category    { type: string sql: ${TABLE}.device_category ;;    label: "Device Category" }
  dimension: user_agent         { type: string sql: ${TABLE}.user_agent ;;         label: "User Agent" }

  dimension: acquisition_source {
    type: string
    sql: ${TABLE}.acquisition_source ;;
    label: "Acquisition Source"
    description: "marketing_campaign when a campaign or utm_source was captured, otherwise organic_walk-in. Same derivation as prod_trial_report, so the two pivot identically."
  }

  # ——— Creator ———

  dimension: sign_up_user_username { type: string sql: ${TABLE}.sign_up_user_username ;; label: "Username" }
  dimension: sign_up_user_email    { type: string sql: ${TABLE}.sign_up_user_email ;;    label: "Email" }
  dimension: profile_email         { type: string sql: ${TABLE}.profile_email ;;         label: "Profile Email (JSON)" }
  dimension: first_name            { type: string sql: ${TABLE}.first_name ;;            label: "First Name" }
  dimension: last_name             { type: string sql: ${TABLE}.last_name ;;             label: "Last Name" }

  dimension: full_name {
    type: string
    sql: TRIM(CONCAT(COALESCE(${TABLE}.first_name, ''), ' ', COALESCE(${TABLE}.last_name, ''))) ;;
    label: "Full Name"
  }

  dimension: sign_up_user_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.sign_up_url_code ;;
    label: "Storefront URL"
    link: { label: "Open storefront" url: "{{ value }}" }
  }

  # ——— Plan and coupon ———

  dimension: plan_name {
    type: string
    sql: COALESCE(${TABLE}.plan_name, '(unknown)') ;;
    label: "Plan Name"
    description: "CURRENT state from plans[], not the plan as at churn."
  }

  dimension: plan_interval { type: string sql: ${TABLE}.plan_interval ;; label: "Interval" }

  dimension: plan_interval_combined {
    type: string
    sql: CONCAT(COALESCE(${TABLE}.plan_name, '(unknown)'), ': ',
      COALESCE(${TABLE}.plan_interval, '(unknown)')) ;;
    label: "Plan: Interval"
  }

  dimension: coupon_name { type: string sql: ${TABLE}.coupon_name ;; label: "Coupon" }

  dimension: coupon_id {
    type: string
    sql: ${TABLE}.coupon_id ;;
    label: "Coupon ID"
    description: "Filter on this, never on Coupon name — 4fb50491 has already been renamed once, from 'Trial Coupon - 14days' to 'Trial Coupon - 30days'."
  }

  dimension: price { type: number sql: ${TABLE}.price ;; value_format_name: usd label: "Price" }

  dimension: plan_entries {
    type: number
    sql: ${TABLE}.plan_entries ;;
    label: "Plan Entries"
    description: "Should be 1. 0 means the plan row could not be resolved."
  }

  dimension: subscription_status { type: string sql: ${TABLE}.subscription_status ;; label: "Subscription Status" }

  # ——— Measures ———
  # Segment measures filter on booleans wherever possible. Where a string filter
  # is unavoidable it is on churn_reason, whose values are defined in this file
  # — never on a label a chart could rename.

  measure: subscriptions_churned {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_churned: "yes", is_future_dated: "no"]
    label: "Subscriptions Churned"
    description: "Paid subscriptions that reached a terminal status on or before today, bucketed by Churn Date. Excludes past_due and future-dated period-end cancellations. Must sit within about 5 a month of prod_subscription_churn. UNDERCOUNTED from 2026-09-01 while dunning write-off is stalled — see the view header."
    drill_fields: [detail*]
  }

  measure: churned_payment_failed {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_churned: "yes", is_future_dated: "no", churn_reason: "Payment failed"]
    label: "Churned - Payment Failed"
    description: "Roughly half of all churn: 101 / 129 / 140 for Jun / Jul / Aug 2026."
    drill_fields: [detail*]
  }

  measure: churned_voluntary {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_churned: "yes", is_future_dated: "no", churn_reason: "Voluntary cancellation"]
    label: "Churned - Voluntary"
    drill_fields: [detail*]
  }

  measure: churned_trial_converted {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_churned: "yes", is_future_dated: "no", came_via_trial: "yes"]
    label: "Churned - Came Via Trial"
    description: "The only churn prod_paid_subscription_cancellations can see. 185 / 250 / 247 for Jun / Jul / Aug 2026."
    drill_fields: [detail*]
  }

  measure: churned_direct {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_churned: "yes", is_future_dated: "no", came_via_trial: "no"]
    label: "Churned - Direct (No Trial)"
    description: "Invisible to prod_paid_subscription_cancellations. 16% of all churn to date; effectively all of it from October onward."
    drill_fields: [detail*]
  }

  measure: churned_reactivations {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_churned: "yes", is_future_dated: "no", payment_sequence: ">1"]
    label: "Churned - Reactivations"
    description: "A returning subscriber who has lapsed again. Never visible in the old view: reactivations get no trial, so they have no trial_end."
    drill_fields: [detail*]
  }

  measure: payment_retrying {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_churned: "no"]
    label: "Payment Retrying (Not Yet Churned)"
    description: "In dunning, no churn date, so these do NOT appear on a Churn Date axis — query them without a date dimension. 38 as at 2026-09-08, abnormally high because write-off has stalled."
    drill_fields: [detail*]
  }

  measure: churned_first_cycle {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_churned: "yes", is_future_dated: "no", payments_collected: "1"]
    label: "Churned After One Payment"
    description: "Collected once and never renewed. The sharpest read on whether removing the trial changed customer quality."
    drill_fields: [detail*]
  }

  measure: revenue_collected_before_churn {
    type: sum
    sql: ${TABLE}.revenue_collected ;;
    value_format_name: usd
    label: "Revenue Collected Before Churn"
    description: "Lifetime collected on the churned subscriptions. NOT forward revenue lost, and not MRR."
  }

  measure: median_paid_lifetime_days {
    type: median
    sql: DATE_DIFF(${TABLE}.churn_date, ${TABLE}.first_paid_date, DAY) ;;
    label: "Median Paid Lifetime (Days)"
    description: "Split by Acquisition Segment to compare retention across cohorts. Not comparable across recent months — a cohort cannot show a long lifetime until enough time has passed."
  }

  # ——— Guards ———

  measure: churn_reason_accounting_check {
    type: number
    sql: ${subscriptions_churned} - ${churned_payment_failed} - ${churned_voluntary} ;;
    label: "Churn Reason Accounting Check"
    description: "Must always be 0."
  }

  measure: trial_split_accounting_check {
    type: number
    sql: ${subscriptions_churned} - ${churned_trial_converted} - ${churned_direct} ;;
    label: "Trial Split Accounting Check"
    description: "Must always be 0. Verifies the trial / direct split covers every churn."
  }

  measure: unknown_plan {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [plan_entries: "0"]
    label: "Unknown Plan"
    description: "Should be 0."
    drill_fields: [detail*]
  }

  measure: churn_before_first_payment {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_churned: "yes", lifetime_days: "<0"]
    label: "Churn Before First Payment (Should Be 0)"
    description: "A churn date earlier than the first collected payment is impossible. Non-zero is the signature of the unbounded dunning lookback pulling an end date back past a payment that clearly succeeded — handoff item #3. Measured 0 across 590 dunning subscriptions on 2026-09-08."
    drill_fields: [detail*]
  }

  measure: dunning_stalled {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_dunning_stalled: "yes"]
    label: "Dunning Stalled (Should Be Near 0)"
    description: "Subscriptions unresolved more than 5 days after their first failed invoice. Through August the write-off sweep ran daily on invoices 1-2 days old, so this sat near zero. It is currently NON-ZERO: the sweep stopped on 2026-09-01 and payment-failure churn is being undercounted. Put this on a dashboard alert — it is the early warning for this class of incident."
    drill_fields: [detail*]
  }

  measure: future_dated_churns {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_churned: "yes", is_future_dated: "yes"]
    label: "Future Dated Churns (Excluded)"
    description: "Cancelled with the paid period still running. Excluded from every churn measure above. Not an error — they land on their own date — but useful as a forward indicator."
    drill_fields: [detail*]
  }

  # ——— Drill Set ———

  set: detail {
    fields: [
      user_id,
      first_name,
      last_name,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      subscription_id,
      churn_date,
      churn_reason_detail,
      acquisition_segment,
      first_paid_date,
      last_paid_date,
      lifetime_days,
      payments_collected,
      revenue_collected,
      first_unpaid_date,
      days_since_first_failure,
      plan_name,
      plan_interval,
      price,
      coupon_name,
      trial_end_date,
      trial_length_days,
      subscription_status,
      payment_sequence,
      marketing_campaign,
      acquisition_source,
      utm_regintent,
      business_type,
      onboarding_path,
      plan_level,
      device_category
    ]
  }
}

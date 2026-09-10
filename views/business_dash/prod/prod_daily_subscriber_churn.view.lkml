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
# !! PAYMENT-FAILURE CHURN IS UNDERCOUNTED RIGHT NOW (as at 2026-09-08) !!
# -----------------------------------------------------------------------------
# September shows almost no payment-failure churn. That is a recognition delay,
# not an improvement in retention, and not a defect in this view.
#
# VERIFIED:
#   * Payments are still failing at the normal rate. Billable invoices not
#     collected: 32.0% for 2026-08-01..30, 31.4% for 2026-08-31 onward.
#   * 84 eligible subscriptions sit in past_due with unresolved failed invoices,
#     $2,433.16 outstanding. 83 of the 84 have a retry scheduled in Stripe; the
#     latest is 2026-09-16.
#   * A subscription is only written off once retries are exhausted, and this
#     view keys payment-failure churn off that write-off. So those 84 will land
#     on the chart as they resolve, and early-September bars WILL GROW.
#
# NOT ESTABLISHED — do not repeat as fact:
#   * WHY the backlog is larger than usual. A lengthened Stripe retry schedule
#     is the leading theory but has not been demonstrated. The obvious test —
#     comparing days-to-next-retry before and after 2026-08-30 — does not work:
#     August invoices are almost all written off already and a written-off
#     invoice has no next retry, so the comparison measures resolution status
#     rather than schedule. Restricted to live subscriptions, August returns no
#     rows at all and there is no baseline. Settle it from Stripe Dashboard ->
#     Settings -> Billing -> Manage failed payments, which shows the schedule
#     and its edit history.
#
# ALSO CHECKED AND CLEARED:
#   * The loader is healthy: 55-85 invoice rows updated every day through the
#     period, uncollectible still appearing.
#   * Cancelled subscriptions carrying a nextPaymentAttempt are NOT pending
#     charges. Stripe turns automatic collection off at cancellation and leaves
#     the field populated; the invoice stays `open` forever. Confirmed in the
#     Stripe UI on sub_1U9hcv and sub_1TNqmO.
#
# Dunning Stalled (Should Be Near 0) monitors this. It fires only when Stripe
# has NO further retry planned and the row still has not been written off, so it
# survives retry-schedule changes.
#
# KNOWN GAP, roughly 30 a month: a subscription cancelled within a day of a
# failed payment, with no dunning record, classifies as Voluntary rather than
# Payment failed. Real but unquantified as to cause — the gap between failure
# and cancellation has not been profiled, so no branch has been added for it.
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
# TWO AXES — pick deliberately
# -----------------------------------------------------------------------------
# Churn Date          churned subscriptions only. Stable: once a bar is in the
#                     past it does not change. Use this for anything reported,
#                     reconciled or screenshotted.
# Churn or Pending    adds subscriptions still in dunning, dated from their
#                     first failed invoice, so the pipeline is visible. NOT
#                     stable: a pending row moves to Payment failed AND to a
#                     different date when it is written off.
#
# Measures pair with axes: Subscriptions Churned with the first, Churned or
# Pending with the second. Crossing them silently drops or double-counts rows.
#
# prod_paid_subscription_cancellations cannot show the pending population at
# all — past_due resolves to a NULL effective_end_date there and it exposes no
# first-failure date to fall back on. That is why the pending series lives here.
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

      -- GROUP BY + ANY_VALUE rather than a bare SELECT: a bare SELECT fans out
      -- the whole view if dim_private_profiles ever holds two rows for a user.
      -- Revenue Collected Before Churn is a type: sum and would double
      -- silently. Fanout Check guards it.
      marketing_capture AS (
      SELECT
      user_id,
      ANY_VALUE(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_campaign'))        AS utm_campaign,
      ANY_VALUE(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_source'))          AS utm_source,
      ANY_VALUE(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent'))       AS utm_regintent,
      ANY_VALUE(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_onboarding_path')) AS onboarding_path,
      ANY_VALUE(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_planlevel'))       AS plan_level,
      ANY_VALUE(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent'))          AS user_agent,
      ANY_VALUE(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.signup_provider'))     AS signup_provider,
      ANY_VALUE(JSON_VALUE(private_profile, '$.email'))                                          AS profile_email,
      ANY_VALUE(JSON_VALUE(private_profile, '$.sellerShippingAddress.firstName'))                AS first_name,
      ANY_VALUE(JSON_VALUE(private_profile, '$.sellerShippingAddress.lastName'))                 AS last_name
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      WHERE user_id IS NOT NULL
      GROUP BY user_id
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
      SELECT
      subscription_id,
      MIN(COALESCE(created, created_at)) AS first_unpaid_invoice_ts,
      -- Stripe's own next retry. nextPaymentAttempt is an OBJECT
      -- ({_seconds: ...}), not a scalar: reading it with
      -- JSON_VALUE(..., '$.nextPaymentAttempt') alone returns NULL on every
      -- row and makes it look as though Stripe has stopped retrying. Both
      -- paths are tried.
      MAX(SAFE.TIMESTAMP_SECONDS(SAFE_CAST(COALESCE(
      JSON_VALUE(invoice, '$.nextPaymentAttempt._seconds'),
      JSON_VALUE(invoice, '$.nextPaymentAttempt')) AS INT64)))
      AS next_retry_ts
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

      -- Combined axis so subscriptions still in dunning can be plotted.
      -- Churn date where one exists, otherwise the date the first invoice
      -- failed. MIXED SEMANTICS — see the dimension description.
      COALESCE(
      CASE
      WHEN sp.status NOT IN ('unpaid', 'canceled') THEN NULL
      WHEN sp.cancel_at_period_end IS TRUE
      THEN DATE(COALESCE(d.dunning_end_ts, sp.current_period_end), 'America/New_York')
      ELSE DATE(
      COALESCE(c.status_flip_cancelled_at, d.dunning_end_ts, sp.cancelled_at),
      'America/New_York')
      END,
      DATE(ff.first_unpaid_invoice_ts, 'America/New_York')
      ) AS churn_or_pending_date,

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
      ff.next_retry_ts,
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
      CASE
      WHEN mc.signup_provider = 'instagram' THEN 'Instagram'
      WHEN mc.signup_provider = 'facebook'  THEN 'Facebook'
      ELSE 'Phone'
      END AS signup_type,

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

  dimension_group: churn_or_pending {
    type: time
    convert_tz: no
    datatype: date
    sql: ${TABLE}.churn_or_pending_date ;;
    timeframes: [date, week, month, quarter, year]
    label: "Churn or Pending"
    description: "Churn date where one exists, otherwise the date the first invoice failed. Lets subscriptions still in dunning appear on a chart, which Churn Date cannot do because past_due has no churn date. !! MIXED SEMANTICS !! churned rows are dated when they churned, pending rows when their trouble started. A pending row MOVES when it is written off — its series changes AND its date changes — so bars on this axis are not stable and a screenshot will not reproduce next week. Use Churn Date for anything that must be reproducible."
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

  dimension: is_plottable {
    type: yesno
    sql: COALESCE(${TABLE}.churn_date < CURRENT_DATE('America/New_York'), TRUE) ;;
    hidden: yes
    # TRUE when the row either has a churn date in the past, or has no churn
    # date at all (still in dunning). Excludes only future-dated period-end
    # cancellations. Used by Churned or Pending, which must keep past_due rows
    # that Is Churned = Yes would drop.
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
         AND ${TABLE}.days_since_first_failure > 2
         AND (${TABLE}.next_retry_ts IS NULL
              OR ${TABLE}.next_retry_ts < CURRENT_TIMESTAMP()) ;;
    label: "Is Dunning Stalled"
    description: "Unresolved, older than 2 days, and Stripe has NO further retry planned — so it should have been written off and was not. Deliberately keyed off Stripe's own next-retry field rather than elapsed days: a days-based threshold fires en masse whenever the retry schedule changes, which produces a false alarm rather than a signal. A subscription simply waiting for its next scheduled retry is NOT stalled."
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

  dimension: signup_type {
    type: string
    sql: ${TABLE}.signup_type ;;
    label: "Signup Type"
    description: "Instagram, Facebook or Phone, from onboardingMarketingCapture.signup_provider. Anything not instagram or facebook — including a missing value — falls to Phone, matching prod_trial_report. Phone therefore means 'phone or unknown'."
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
    description: "Paid subscriptions that reached a terminal status on or before today, bucketed by Churn Date. Excludes past_due and future-dated period-end cancellations. Must sit within about 5 a month of prod_subscription_churn. Payment-failure churn is UNDERCOUNTED for roughly the last two weeks at any given moment: a failure is only recognised once retries are exhausted and the invoice is written off, so recent bars grow. See the view header."
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

  measure: churned_or_pending {
    type: count_distinct
    sql: ${TABLE}.subscription_id ;;
    filters: [is_plottable: "yes"]
    label: "Churned or Pending"
    description: "Churned subscriptions PLUS those still in dunning, on the Churn or Pending axis. Higher than Subscriptions Churned by the size of the dunning pipeline. The pending portion is provisional: those rows move to Payment failed, and to a different date, once written off. For a stable series use Subscriptions Churned."
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

  measure: row_count {
    type: count
    hidden: yes
  }

  measure: fanout_check {
    type: number
    sql: ${row_count} - ${subscriptions_churned} - ${payment_retrying} - ${future_dated_churns} ;;
    label: "Fanout Check (Should Be 0)"
    description: "Rows minus the three mutually exclusive states every row must be in. Non-zero means an attribution join is duplicating rows, which would silently double Revenue Collected Before Churn. The profile CTEs are deduped so this should stay 0."
  }

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
    description: "Subscriptions with no retry left that were never written off. Should be near 0. Non-zero means the write-off step is genuinely failing, as opposed to a backlog sitting in a longer retry cycle — the two look identical on a churn chart and this measure is what separates them. Worth a dashboard alert."
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
      profile_email,
      sign_up_user_username,
      sign_up_user_email,
      sign_up_user_url,
      subscription_id,
      churn_date,
      churn_or_pending_date,
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
      signup_type,
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

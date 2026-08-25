# =============================================================================
# Echo Me agent analytics — v3, rebuilt on the true entity grain
# =============================================================================
#
# THE GRAIN
# ---------
# The entity is (user_id, agent, channel), NOT (user_id, agent). Verified:
#
#   agent   entities   users   channel-scoped?
#   SEA        3,257   1,752   yes (up to 73 channels/user)
#   CODA       2,043     843   yes (up to 62)
#   DMA          972     624   yes (up to 15)
#   ASA          893     893   no
#   GRX          136     136   no
#
# `id` is an EVENT key, not an entity key:
#   ASA   {user}_asa_{epoch_ms}
#   CODA  {user}_{channel}_coda[_{epoch_ms}]
#   DMA   {user}_{channel}_dma_{epoch_ms}_{channel_index}
#   GRX   {user}_grx_{created|started}_{epoch_ms}
#   SEA   {user}_{channel}_sea[_{epoch_ms}]
#
# The table is append-only. 501 duplicate ids exist, all SEA, all carrying
# distinct status_last_changed values (0 identical rewrites) — real events.
# Never dedupe on id. Dedupe on (entity_key, event_ts) only.
#
#
# THREE LEVELS OF COUNTING — pick deliberately
# --------------------------------------------
#   Active Connections   user x agent x channel   operational load
#   Active Agents        user x agent             product adoption  (max 5/user)
#   Active Users         user                     reach
#
# All three come from the same view. A tile that mixes them will look wrong.
# "Active Agents" is the number most product questions want.
#
#
# NEVER-ENABLED vs PAUSED  (replaces the old ghost/phantom filter)
# ----------------------------------------------------------------
# The system writes a `disabled` row per connected channel when a user first
# lands on an agent page. At channel grain these are 80% of CODA entities and
# 23% of SEA. Deleting them destroys the denominator — that row is the only
# evidence the channel exists. So they are RECLASSIFIED, not dropped:
#
#   Active         is_active
#   Paused         not active AND entity has >1 event (a human acted)
#   Never enabled  single `disabled` event, system default
#
# Two activation rates fall out, and both are honest:
#   Activation Rate (engaged) = active / (active + paused)
#   Activation Rate (all)     = active / all connections
#
# Set `include_never_enabled` to "no" to reproduce the old delete behaviour.
#
#
# AGENTS THAT ARE NOT TOGGLES
# ---------------------------
# DMA: 1 never-enabled entity out of 972; 617/624 users active. Rows appear
#      only once provisioned. Activation rate is ~100% by construction.
# GRX: a funnel, not a switch. Stages: plan_creation_started -> enabled
#      (Plan Creation Completed) -> active (Plan Launched). Use `grx_stage`.
# Exclude both from activation-rate tiles or they will flatten the comparison.
#
#
# DMA BATCHING
# ------------
# One DMA user action writes one row per channel at the same epoch. A single
# toggle by a 4-channel user = 4 connection deactivations, 1 agent
# deactivation. Use the agent-level measure for churn reporting.
#
#
# DATA CAVEATS
# ------------
# * Channel name/type/meta-app are populated on SEA rows only. Resolved here
#   per (user, channel) across all agents so DMA/CODA inherit them.
# * Internal accounts (@commentsold.com etc., incl. channel ids that are
#   emails) dominated the old numbers. Excluded by default; see
#   `include_internal_users`.
# * 2024-08 -> 2025-12 CODA history is REAL, migrated 2026-04-22. 1,385 of
#   1,386 rows post-date the user's own signup. created_at/updated_at are
#   warehouse load timestamps; only status_last_changed is event time.
#   Use `history_floor_date` to hide it from tiles rather than deleting it.
# =============================================================================


# -----------------------------------------------------------------------------
# 0. echo_me_agent_events — one row per source event, keyed to the real entity
# -----------------------------------------------------------------------------
view: echo_me_agent_events {
  derived_table: {
    # datagroup_trigger: echo_me_default_datagroup   # enable once validated
    sql:
      WITH internal_users AS (
        SELECT user_id
        FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
        WHERE LOWER(email) LIKE '%@test.com'
           OR LOWER(email) LIKE '%@example.com'
           OR LOWER(email) LIKE '%@popshoplive.com'
           OR LOWER(email) LIKE '%@commentsold.com'
           OR LOWER(email) LIKE '%@pop.store'
      ),

      raw AS (
      SELECT
      id      AS source_row_id,
      user_id,
      session_id,
      agent   AS agent_raw,
      created_at,
      updated_at,
      external_channel_id,
      external_channel_name,
      channel_type,
      linked_meta_app,
      linked_meta_app_id,
      ai_echo_setup_complete,

      CASE agent
      WHEN 'echo-me-sea'  THEN 'sea'   WHEN 'echo-me-coda' THEN 'coda'
      WHEN 'echo-me-dma'  THEN 'dma'   WHEN 'echo-me-asa'  THEN 'asa'
      WHEN 'echo-me-grx'  THEN 'grx'
      END AS agent_key,

      CASE agent
      WHEN 'echo-me-sea'  THEN sea_status   WHEN 'echo-me-coda' THEN coda_status
      WHEN 'echo-me-dma'  THEN dma_status   WHEN 'echo-me-asa'  THEN asa_status
      WHEN 'echo-me-grx'  THEN grx_status
      END AS status,

      CASE agent
      WHEN 'echo-me-sea'  THEN sea_status_last_changed
      WHEN 'echo-me-coda' THEN coda_status_last_changed
      WHEN 'echo-me-dma'  THEN dma_status_last_changed
      WHEN 'echo-me-asa'  THEN asa_status_last_changed
      WHEN 'echo-me-grx'  THEN grx_status_last_changed
      END AS status_last_changed
      FROM `popshoplive-26f81.commentchat.echo_me_agents`
      WHERE agent IS NOT NULL
      ),

      keyed AS (
      SELECT
      r.*,
      COALESCE(r.external_channel_id, '_none') AS channel_key,
      CONCAT(r.user_id, '|', r.agent_key, '|',
      COALESCE(r.external_channel_id, '_none')) AS entity_key,
      CONCAT(r.user_id, '|', r.agent_key)               AS user_agent_key,
      r.external_channel_id IS NOT NULL                 AS is_channel_scoped,

      -- status_last_changed is populated on 100% of rows today. If this
      -- ever fires we are dating an event by its warehouse load time.
      r.status_last_changed IS NULL                     AS event_ts_imputed,
      COALESCE(r.status_last_changed, r.updated_at, r.created_at) AS event_ts,

      COALESCE(
      CASE
      WHEN r.agent_key = 'grx' THEN r.status IN ('enabled', 'active')
      ELSE r.status IN ('enabled', 'connected')
      END, FALSE) AS is_active,

      CASE agent_key
      WHEN 'sea'  THEN 'Social Engagement (SEA)'
      WHEN 'coda' THEN 'Comment to DM (CODA)'
      WHEN 'dma'  THEN 'Deal Monitoring (DMA)'
      WHEN 'asa'  THEN 'Auto Selling (ASA)'
      WHEN 'grx'  THEN 'Growth RX (GRX)'
      END AS agent_name,

      CASE agent_key
      WHEN 'sea' THEN 1 WHEN 'coda' THEN 2 WHEN 'dma' THEN 3
      WHEN 'asa' THEN 4 WHEN 'grx'  THEN 5
      END AS agent_sort_order,

      -- DMA/GRX are provisioned or funnel-shaped, not user toggles.
      agent_key IN ('sea', 'coda', 'asa') AS is_toggle_agent

      FROM raw r
      WHERE r.agent_key IS NOT NULL
      ),

      counted AS (
      SELECT
      k.*,
      COUNT(*) OVER (PARTITION BY k.entity_key) AS entity_event_count
      FROM keyed k
      ),

      -- Channel name/type/app live on SEA rows only. Resolve per (user,
      -- channel) across every agent so DMA and CODA inherit them.
      channel_meta AS (
      SELECT
      user_id,
      external_channel_id,
      ARRAY_AGG(external_channel_name IGNORE NULLS ORDER BY event_ts DESC LIMIT 1)[SAFE_OFFSET(0)] AS resolved_channel_name,
      ARRAY_AGG(channel_type          IGNORE NULLS ORDER BY event_ts DESC LIMIT 1)[SAFE_OFFSET(0)] AS resolved_channel_type,
      ARRAY_AGG(linked_meta_app       IGNORE NULLS ORDER BY event_ts DESC LIMIT 1)[SAFE_OFFSET(0)] AS resolved_meta_app,
      ARRAY_AGG(linked_meta_app_id    IGNORE NULLS ORDER BY event_ts DESC LIMIT 1)[SAFE_OFFSET(0)] AS resolved_meta_app_id
      FROM counted
      WHERE external_channel_id IS NOT NULL
      GROUP BY 1, 2
      )

      SELECT
      c.* EXCEPT (external_channel_name, channel_type, linked_meta_app, linked_meta_app_id),

      COALESCE(cm.resolved_channel_name, c.external_channel_name) AS external_channel_name,
      COALESCE(cm.resolved_channel_type, c.channel_type)          AS channel_type,
      COALESCE(cm.resolved_meta_app,     c.linked_meta_app)       AS linked_meta_app,
      COALESCE(cm.resolved_meta_app_id,  c.linked_meta_app_id)    AS linked_meta_app_id,

      -- System default: the entity's only event is a `disabled` write with
      -- no user action behind it. NOT deleted — reclassified downstream.
      COALESCE(c.entity_event_count = 1 AND c.status = 'disabled', FALSE)
      AS is_system_default,

      iu.user_id IS NOT NULL AS is_internal_user,

      -- Channel ids that are email addresses are internal test fixtures.
      COALESCE(REGEXP_CONTAINS(c.external_channel_id, r'@'), FALSE)
      AS channel_id_looks_internal,

      CASE
      WHEN c.agent_key = 'grx' AND c.status = 'plan_creation_started' THEN 'Plan Creation Started'
      WHEN c.agent_key = 'grx' AND c.status = 'enabled'               THEN 'Plan Creation Completed'
      WHEN c.agent_key = 'grx' AND c.status = 'active'                THEN 'Plan Launched'
      WHEN c.status = 'enabled'   THEN 'Enabled'
      WHEN c.status = 'connected' THEN 'Connected'
      WHEN c.status = 'disabled'  THEN 'Disabled'
      WHEN c.status = 'preview'   THEN 'Preview'
      WHEN c.status IS NULL       THEN 'Not Set'
      ELSE c.status
      END AS status_display,

      CASE c.status
      WHEN 'plan_creation_started' THEN 1
      WHEN 'enabled'               THEN 2
      WHEN 'active'                THEN 3
      END AS grx_stage_order

      FROM counted c
      LEFT JOIN channel_meta cm
      ON  cm.user_id            = c.user_id
      AND cm.external_channel_id = c.external_channel_id
      LEFT JOIN internal_users iu
      ON iu.user_id = c.user_id
      ;;
  }

  # Composite PK — `id` alone is not unique (501 duplicate SEA rows).
  dimension: event_pk {
    type: string
    sql: CONCAT(${TABLE}.entity_key, '|',
                CAST(UNIX_MICROS(${TABLE}.event_ts) AS STRING), '|',
                ${TABLE}.source_row_id) ;;
    primary_key: yes
    hidden: yes
  }

  dimension: entity_key      { type: string sql: ${TABLE}.entity_key ;; hidden: yes }
  dimension: user_agent_key  { type: string sql: ${TABLE}.user_agent_key ;; hidden: yes }
  dimension: user_id         { type: string sql: ${TABLE}.user_id ;; }
  dimension: agent_key       { type: string sql: ${TABLE}.agent_key ;; }
  dimension: agent_name      { type: string sql: ${TABLE}.agent_name ;; order_by_field: agent_sort_order }
  dimension: agent_sort_order { type: number sql: ${TABLE}.agent_sort_order ;; hidden: yes }
  dimension: channel_key     { type: string sql: ${TABLE}.channel_key ;; }
  dimension: status          { type: string sql: ${TABLE}.status ;; }
  dimension: status_display  { type: string sql: ${TABLE}.status_display ;; label: "Status" }
  dimension: is_active       { type: yesno  sql: ${TABLE}.is_active ;; }
  dimension: is_system_default { type: yesno sql: ${TABLE}.is_system_default ;; }
  dimension: is_internal_user  { type: yesno sql: ${TABLE}.is_internal_user ;; }
  dimension: is_toggle_agent   { type: yesno sql: ${TABLE}.is_toggle_agent ;; }
  dimension: event_ts_imputed  { type: yesno sql: ${TABLE}.event_ts_imputed ;; }

  dimension_group: event {
    type: time
    datatype: timestamp
    convert_tz: no
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.event_ts ;;
  }

  measure: event_count { type: count }

  measure: imputed_ts_count {
    type: count
    filters: [event_ts_imputed: "yes"]
    description: "Should be 0. Non-zero means events are being dated by warehouse load time."
  }
}


# -----------------------------------------------------------------------------
# 1. echo_me_user_attrs — one row per user (unchanged from v2)
# -----------------------------------------------------------------------------
view: echo_me_user_attrs {
  derived_table: {
    sql:
      WITH agent_users AS (
        SELECT DISTINCT user_id
        FROM `popshoplive-26f81.commentchat.echo_me_agents`
        WHERE agent IS NOT NULL
      ),

      pp_raw AS (
      SELECT
      user_id,
      email AS account_email,
      JSON_VALUE(private_profile, '$.email')                                    AS profile_email,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.firstName')          AS first_name,
      JSON_VALUE(private_profile, '$.sellerShippingAddress.lastName')           AS last_name,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_campaign')  AS utm_campaign,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_source')    AS utm_source,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.utm_regintent') AS utm_regintent,
      JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')    AS user_agent,
      LOWER(JSON_VALUE(private_profile, '$.onboardingMarketingCapture.user_agent')) AS ua_lower
      FROM `popshoplive-26f81.dbt_popshop.dim_private_profiles`
      ),

      marketing_capture AS (
      SELECT
      p.*,
      COALESCE(
      CASE
      WHEN REGEXP_CONTAINS(ua_lower, r'(bot|crawler|spider|crawl|slurp|googlebot|bingpreview|facebookexternalhit|twitterbot|linkedinbot|discordbot|telegrambot|google-read-aloud)') THEN 'BOT'
      WHEN REGEXP_CONTAINS(ua_lower, r'instagram')            THEN 'WEBVIEW_INSTAGRAM'
      WHEN REGEXP_CONTAINS(ua_lower, r'(fban|fbav|facebook)') THEN 'WEBVIEW_FACEBOOK'
      WHEN REGEXP_CONTAINS(ua_lower, r'tiktok')               THEN 'WEBVIEW_TIKTOK'
      WHEN REGEXP_CONTAINS(ua_lower, r'snapchat')             THEN 'WEBVIEW_SNAPCHAT'
      WHEN REGEXP_CONTAINS(ua_lower, r'(linkedin|linkedinapp)') THEN 'WEBVIEW_LINKEDIN'
      WHEN REGEXP_CONTAINS(ua_lower, r'(wv|webview|meta-iab|metaiab|iabmv/1|whatsapp|line|gsa/|googleapp/|youtube|reddit)') THEN 'WEBVIEW_OTHER'
      WHEN REGEXP_CONTAINS(ua_lower, r'(iphone|ipad|ipod|cpu iphone os|cpu os)') THEN 'IOS'
      WHEN REGEXP_CONTAINS(ua_lower, r'android')              THEN 'ANDROID'
      WHEN REGEXP_CONTAINS(ua_lower, r'(windows nt|win64|wow64)') THEN 'WINDOWS_DESKTOP'
      WHEN REGEXP_CONTAINS(ua_lower, r'(macintosh|mac os x)')
      AND NOT REGEXP_CONTAINS(ua_lower, r'(iphone|ipad)')   THEN 'MACOS_DESKTOP'
      WHEN REGEXP_CONTAINS(ua_lower, r'(linux|x11)')
      AND NOT REGEXP_CONTAINS(ua_lower, r'android')         THEN 'LINUX_DESKTOP'
      ELSE 'OTHER'
      END,
      'No Onboarding Event'
      ) AS device_category
      FROM pp_raw p
      ),

      onboarding_events AS (
      SELECT
      user_id,
      context_campaign_campaign        AS marketing_campaign,
      context_campaign_onboarding_path AS onboarding_path,
      context_campaign_planlevel       AS plan_level,
      context_user_agent               AS user_agent,
      utm_regintent,
      business_type,
      `timestamp`,
      onboarding_session_id,
      CASE
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(bot|crawler|spider|crawl|slurp|googlebot|bingpreview|facebookexternalhit|twitterbot|linkedinbot|discordbot|telegrambot|google-read-aloud)') THEN 'BOT'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(wv|webview|meta-iab|metaiab|facebook|fban|fbav|instagram|iabmv/1|whatsapp|line|linkedinapp|snapchat|gsa/|googleapp/|youtube|tiktok|reddit)') THEN 'WEBVIEW'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(iphone|ipad|ipod|cpu iphone os|cpu os)') THEN 'IOS'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'android') THEN 'ANDROID'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(windows nt|win64|wow64)') THEN 'WINDOWS_DESKTOP'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(macintosh|mac os x)')
      AND NOT REGEXP_CONTAINS(LOWER(context_user_agent), r'(iphone|ipad)') THEN 'MACOS_DESKTOP'
      WHEN REGEXP_CONTAINS(LOWER(context_user_agent), r'(linux|x11)')
      AND NOT REGEXP_CONTAINS(LOWER(context_user_agent), r'android') THEN 'LINUX_DESKTOP'
      ELSE 'OTHER'
      END AS device_category
      FROM `popshoplive-26f81.popstore.popstore_onboarding_screen_action`
      WHERE (scene = 'onboarding' OR scene IS NULL)
      AND (step_name = 'onboarding_complete' OR step_name IS NULL)
      ),

      -- Prefer rows where marketing/intent fields are actually populated, then
      -- most recent, so we never land on an arbitrary 'generic' row.
      onboarding_events_dedup AS (
      SELECT *
      FROM onboarding_events
      QUALIFY ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY
      CASE
      WHEN marketing_campaign IS NOT NULL
      OR (utm_regintent IS NOT NULL AND utm_regintent != 'generic')
      OR (business_type   IS NOT NULL AND business_type   != 'generic')
      THEN 0 ELSE 1
      END,
      `timestamp` DESC
      ) = 1
      ),

      subs AS (
      SELECT
      t1.user_id,
      t1.subscription_id,
      t1.status                  AS subscription_status,
      t1.initial_start_date,
      t1.trial_end,
      t1.cancellation_applied_at,
      COALESCE(t1.discounted_price, t1.price + t1.tax_amount) AS price,
      JSON_EXTRACT_SCALAR(pl, '$.productName') AS plan_name,
      JSON_EXTRACT_SCALAR(pl, '$.interval')    AS plan_interval
      FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1,
      UNNEST(t1.plans) AS pl
      WHERE JSON_EXTRACT_SCALAR(pl, '$.planType') = 'plan'
      QUALIFY ROW_NUMBER() OVER (
      PARTITION BY t1.user_id ORDER BY t1.initial_start_date DESC
      ) = 1
      )

      SELECT
      u.user_id,

      prof.url_code AS sign_up_url_code,
      prof.username AS sign_up_user_username,
      mc.account_email AS sign_up_user_email,
      mc.profile_email,
      mc.first_name,
      mc.last_name,

      COALESCE(oe.marketing_campaign, mc.utm_campaign) AS marketing_campaign,
      -- 'generic' is a real captured value; a missing capture is 'unknown'.
      -- The old COALESCE(..., 'generic') merged the two and hid how much of
      -- the funnel is simply untracked.
      CASE COALESCE(NULLIF(TRIM(LOWER(oe.utm_regintent)), ''),
      NULLIF(TRIM(LOWER(mc.utm_regintent)), ''))
      WHEN 'realestate' THEN 'real_estate'
      ELSE COALESCE(NULLIF(TRIM(LOWER(oe.utm_regintent)), ''),
      NULLIF(TRIM(LOWER(mc.utm_regintent)), ''), 'unknown')
      END AS utm_regintent,
      COALESCE(oe.business_type, JSON_VALUE(prof.profile, '$.businessType'), 'generic') AS business_type,
      COALESCE(oe.device_category, mc.device_category) AS device_category,
      COALESCE(oe.user_agent, mc.user_agent) AS user_agent,
      CASE
      WHEN COALESCE(oe.marketing_campaign, mc.utm_campaign) IS NOT NULL THEN 'marketing_campaign'
      WHEN mc.utm_source IS NOT NULL THEN 'marketing_campaign'
      ELSE 'organic_walk-in'
      END AS acquisition_source,

      s.subscription_id,
      s.subscription_status,
      s.initial_start_date AS trial_starts,
      s.trial_end          AS trial_ends,
      s.cancellation_applied_at,
      s.price,
      s.plan_name,
      s.plan_interval,
      s.trial_end IS NOT NULL AS has_trial,
      COALESCE(
      CASE
      WHEN s.cancellation_applied_at IS NOT NULL
      AND s.cancellation_applied_at < s.trial_end
      THEN s.cancellation_applied_at
      END,
      s.trial_end
      ) AS effective_trial_end,

      CASE
      WHEN s.subscription_status = 'active' THEN 'Subscriber'
      WHEN s.subscription_status IN ('canceled', 'cancelled') THEN 'Cancelled'
      WHEN s.cancellation_applied_at IS NOT NULL THEN 'Cancelled'
      ELSE COALESCE(s.subscription_status, 'Unknown')
      END AS user_current_status

      FROM agent_users u
      LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_profiles` prof ON prof.user_id = u.user_id
      LEFT JOIN marketing_capture mc ON mc.user_id = u.user_id
      LEFT JOIN onboarding_events_dedup oe ON oe.user_id = u.user_id
      LEFT JOIN subs s ON s.user_id = u.user_id
      WHERE (mc.account_email IS NULL OR (
      LOWER(mc.account_email) NOT LIKE '%@test.com'
      AND LOWER(mc.account_email) NOT LIKE '%@example.com'
      AND LOWER(mc.account_email) NOT LIKE '%@popshoplive.com'
      AND LOWER(mc.account_email) NOT LIKE '%@commentsold.com'
      AND LOWER(mc.account_email) NOT LIKE '%@pop.store'
      ))
      ;;
  }

  dimension: user_id { type: string sql: ${TABLE}.user_id ;; primary_key: yes }

  dimension: utm_regintent {
    type: string
    sql: COALESCE(${TABLE}.utm_regintent, 'unknown') ;;
    label: "Signup intent"
    description: "What the seller said they came to do, from the signup URL. 'generic' = arrived with no stated intent. 'unknown' = no capture recorded, or the user predates intent tracking. The COALESCE also catches users with no attribute row at all, so no active agent can silently drop out of a cohort chart."
  }

  dimension: marketing_campaign { type: string sql: ${TABLE}.marketing_campaign ;; }
  dimension: business_type      { type: string sql: ${TABLE}.business_type ;; }
  dimension: acquisition_source { type: string sql: ${TABLE}.acquisition_source ;; }
  dimension: device_category    { type: string sql: ${TABLE}.device_category ;; }
  dimension: user_agent         { type: string sql: ${TABLE}.user_agent ;; }

  dimension: first_name  { type: string sql: ${TABLE}.first_name ;; }
  dimension: last_name   { type: string sql: ${TABLE}.last_name ;; }
  dimension: full_name {
    type: string
    sql: TRIM(CONCAT(COALESCE(${TABLE}.first_name, ''), ' ', COALESCE(${TABLE}.last_name, ''))) ;;
  }
  dimension: sign_up_user_username { type: string sql: ${TABLE}.sign_up_user_username ;; }
  dimension: sign_up_user_email    { type: string sql: ${TABLE}.sign_up_user_email ;; }
  dimension: profile_email         { type: string sql: ${TABLE}.profile_email ;; }
  dimension: sign_up_user_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.sign_up_url_code ;;
  }

  dimension: has_trial            { type: yesno  sql: ${TABLE}.has_trial ;; }
  dimension: subscription_id      { type: string sql: ${TABLE}.subscription_id ;; }
  dimension: subscription_status  { type: string sql: ${TABLE}.subscription_status ;; }
  dimension: user_current_status  { type: string sql: ${TABLE}.user_current_status ;; }
  dimension: is_subscriber {
    type: yesno
    sql: ${TABLE}.subscription_status = 'active' ;;
  }
  dimension: plan_name     { type: string sql: ${TABLE}.plan_name ;; }
  dimension: plan_interval { type: string sql: ${TABLE}.plan_interval ;; label: "Interval" }
  dimension: price         { type: number sql: ${TABLE}.price ;; value_format_name: decimal_2 }

  dimension_group: trial_starts_at {
    type: time
    datatype: timestamp
    convert_tz: no
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.trial_starts ;;
    label: "Trial Start"
  }

  dimension_group: trial_ends_at {
    type: time
    datatype: timestamp
    convert_tz: no
    timeframes: [date, week, month, quarter, year]
    sql: ${TABLE}.trial_ends ;;
  }

  measure: user_count { type: count_distinct sql: ${TABLE}.user_id ;; label: "Users" }
}


# -----------------------------------------------------------------------------
# 2. echo_me_agent_status — CURRENT state, one row per user x agent x channel
#    Counts at all three levels. Read the measure labels carefully.
# -----------------------------------------------------------------------------
view: echo_me_agent_status {
  parameter: as_of_date {
    type: date
    label: "State As Of Date"
    description: "Rewinds every connection to its state on this date. Blank = today."
  }

  parameter: include_never_enabled {
    type: unquoted
    label: "Include Never-Enabled Connections"
    description: "Yes keeps system-default rows as available-but-off. No reproduces the old delete behaviour."
    allowed_value: { label: "Yes" value: "yes" }
    allowed_value: { label: "No"  value: "no"  }
    default_value: "yes"
  }

  parameter: include_internal_users {
    type: unquoted
    label: "Include Internal Users"
    allowed_value: { label: "No"  value: "no"  }
    allowed_value: { label: "Yes" value: "yes" }
    default_value: "no"
  }

  derived_table: {
    sql:
      WITH ev AS (
        SELECT *
        FROM ${echo_me_agent_events.SQL_TABLE_NAME}
        WHERE 1=1
        {% if include_internal_users._parameter_value == 'no' %}
          AND NOT is_internal_user
          AND NOT channel_id_looks_internal
        {% endif %}
        {% if include_never_enabled._parameter_value == 'no' %}
          AND NOT is_system_default
        {% endif %}
        {% if as_of_date._is_filtered %}
          AND event_ts <= TIMESTAMP({% parameter as_of_date %})
        {% endif %}
      ),

      latest AS (
      SELECT *
      FROM ev
      QUALIFY ROW_NUMBER() OVER (
      PARTITION BY entity_key ORDER BY event_ts DESC, source_row_id DESC
      ) = 1
      ),

      hist AS (
      SELECT
      entity_key,
      COUNT(*)                           AS events_for_entity,
      MIN(event_ts)                      AS first_event_ts,
      MIN(IF(is_active, event_ts, NULL)) AS first_activated_ts,
      COUNTIF(is_active) > 0             AS ever_activated
      FROM ev
      GROUP BY 1
      ),

      -- Agent is active for a user if ANY of their channels is active.
      user_agent_rollup AS (
      SELECT
      user_agent_key,
      LOGICAL_OR(is_active)          AS agent_active_for_user,
      COUNTIF(is_active)             AS channels_active_for_agent,
      COUNTIF(NOT is_system_default) AS channels_engaged_for_agent,
      COUNT(*)                       AS channels_total_for_agent
      FROM latest
      GROUP BY 1
      ),

      user_rollup AS (
      SELECT
      SPLIT(user_agent_key, '|')[OFFSET(0)] AS user_id,
      COUNTIF(agent_active_for_user)        AS user_active_agent_count,
      COUNT(*)                              AS user_agents_touched_count,
      -- A seller has "acted" only if at least one connection is something
      -- other than a system-written page-load row.
      SUM(channels_engaged_for_agent) > 0   AS user_has_engaged
      FROM user_agent_rollup
      GROUP BY 1
      )

      SELECT
      l.entity_key,
      l.user_agent_key,
      l.user_id,
      l.agent_key,
      l.agent_name,
      l.agent_sort_order,
      l.channel_key,
      l.is_channel_scoped,
      l.is_toggle_agent,
      l.status,
      l.status_display,
      l.grx_stage_order,
      l.is_active,
      l.is_system_default,
      l.is_internal_user,
      l.event_ts AS status_last_changed,
      l.source_row_id AS echo_me_id,
      l.session_id,
      l.external_channel_id,
      l.external_channel_name,
      l.channel_type,
      l.linked_meta_app,
      l.linked_meta_app_id,
      l.ai_echo_setup_complete,

      h.events_for_entity,
      h.first_event_ts,
      h.first_activated_ts,
      h.ever_activated,

      -- Five-state classification. The old three-state version collapsed
      -- 'preview' into 'Paused', which hid the largest actionable segment
      -- in the product: for SEA, 848 connections sit in preview against 805
      -- running. Setup abandonment and churn are different problems.
      --   Active          switched on
      --   In setup        reached preview / plan-creation, never went live
      --   Switched off    was live at some point, is not now  (true churn)
      --   Abandoned setup acted, never went live, not in preview
      --   Never enabled   system wrote one `disabled` row on page load
      CASE
      WHEN l.is_active         THEN 'Active'
      WHEN l.is_system_default THEN 'Never enabled'
      WHEN h.ever_activated    THEN 'Switched off'
      WHEN l.status IN ('preview', 'plan_creation_started') THEN 'In setup'
      ELSE 'Abandoned setup'
      END AS connection_state,

      CASE
      WHEN l.is_active         THEN 1
      WHEN l.status IN ('preview', 'plan_creation_started')
      AND NOT h.ever_activated THEN 2
      WHEN h.ever_activated    THEN 3
      WHEN l.is_system_default THEN 5
      ELSE 4
      END AS connection_state_order,

      uar.agent_active_for_user,
      uar.channels_active_for_agent,
      uar.channels_engaged_for_agent,
      uar.channels_total_for_agent,
      ur.user_active_agent_count,
      ur.user_agents_touched_count,
      ur.user_has_engaged

      FROM latest l
      LEFT JOIN hist h              ON h.entity_key     = l.entity_key
      LEFT JOIN user_agent_rollup uar ON uar.user_agent_key = l.user_agent_key
      LEFT JOIN user_rollup ur      ON ur.user_id       = l.user_id
      ;;
  }

  dimension: entity_key {
    type: string
    sql: ${TABLE}.entity_key ;;
    primary_key: yes
    hidden: yes
  }

  dimension: user_agent_key { type: string sql: ${TABLE}.user_agent_key ;; hidden: yes }
  dimension: user_id        { type: string sql: ${TABLE}.user_id ;; }

  # ——— Agent ———

  dimension: agent_key  { type: string sql: ${TABLE}.agent_key ;; label: "Agent Key" }
  dimension: agent_name {
    type: string
    sql: ${TABLE}.agent_name ;;
    label: "Agent"
    order_by_field: agent_sort_order
  }
  dimension: agent_sort_order { type: number sql: ${TABLE}.agent_sort_order ;; hidden: yes }

  dimension: is_toggle_agent {
    type: yesno
    sql: ${TABLE}.is_toggle_agent ;;
    label: "Is User-Toggled Agent"
    description: "SEA, CODA, ASA. DMA is provisioned and GRX is a funnel, so activation rate is meaningless for those two — filter to Yes on rate tiles."
  }

  # ——— State ———

  dimension: connection_state {
    type: string
    sql: ${TABLE}.connection_state ;;
    label: "Connection State"
    order_by_field: connection_state_order
    description: "Active | In setup (reached preview, never went live) | Switched off (was live, now isn't) | Abandoned setup | Never enabled (system default, no user action)."
  }

  dimension: connection_state_order {
    type: number
    sql: ${TABLE}.connection_state_order ;;
    hidden: yes
  }

  dimension: is_active {
    type: yesno
    sql: ${TABLE}.is_active ;;
    label: "Connection Active"
  }

  dimension: agent_active_for_user {
    type: yesno
    sql: ${TABLE}.agent_active_for_user ;;
    label: "Agent Active For User"
    description: "True if ANY of this user's channels has this agent active."
  }

  dimension: is_system_default {
    type: yesno
    sql: ${TABLE}.is_system_default ;;
    label: "Never Enabled (system default)"
  }

  dimension: is_engaged {
    type: yesno
    sql: NOT ${TABLE}.is_system_default ;;
    label: "Engaged Connection"
    description: "A human has acted on this connection at least once."
  }

  dimension: ever_activated { type: yesno sql: ${TABLE}.ever_activated ;; }

  dimension: churned_connection {
    type: yesno
    sql: ${TABLE}.ever_activated AND NOT ${TABLE}.is_active ;;
    label: "Connection Churned"
  }

  dimension: status         { type: string sql: ${TABLE}.status ;; label: "Raw Status" }
  dimension: status_display { type: string sql: ${TABLE}.status_display ;; label: "Status" order_by_field: grx_stage_order }
  dimension: grx_stage_order { type: number sql: ${TABLE}.grx_stage_order ;; hidden: yes }

  dimension: grx_stage {
    type: string
    sql: IF(${TABLE}.agent_key = 'grx', ${TABLE}.status_display, NULL) ;;
    label: "GRX Stage"
    description: "Plan Creation Started -> Plan Creation Completed -> Plan Launched. Null for other agents."
    order_by_field: grx_stage_order
  }

  dimension: events_for_entity { type: number sql: ${TABLE}.events_for_entity ;; label: "Status Events" }
  dimension: is_internal_user  { type: yesno  sql: ${TABLE}.is_internal_user ;; }

  # ——— Time ———

  dimension_group: status_last_changed {
    type: time
    datatype: timestamp
    convert_tz: no
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.status_last_changed ;;
    label: "Status Last Changed"
    description: "Cohort dimension: when this connection reached its current state. NOT a point-in-time stock filter — use echo_me_agent_monthly for trends."
  }

  dimension_group: first_activated {
    type: time
    datatype: timestamp
    convert_tz: no
    timeframes: [date, week, month, quarter, year]
    sql: ${TABLE}.first_activated_ts ;;
    label: "First Activated"
  }

  # ——— Channel ———

  dimension: is_channel_scoped {
    type: yesno
    sql: ${TABLE}.is_channel_scoped ;;
    description: "SEA, CODA and DMA are per-channel. ASA and GRX are per-user."
  }

  dimension: external_channel_id   { type: string sql: ${TABLE}.external_channel_id ;;   label: "External Channel ID" }
  dimension: external_channel_name { type: string sql: ${TABLE}.external_channel_name ;; label: "External Channel Name" }
  dimension: channel_type          { type: string sql: ${TABLE}.channel_type ;;          label: "Channel Type" }
  dimension: linked_meta_app       { type: string sql: ${TABLE}.linked_meta_app ;;       label: "Linked Meta App" }
  dimension: linked_meta_app_id    { type: string sql: ${TABLE}.linked_meta_app_id ;;    label: "Linked Meta App ID" }
  dimension: echo_me_id            { type: string sql: ${TABLE}.echo_me_id ;;            label: "Latest Event ID" }
  dimension: session_id            { type: string sql: ${TABLE}.session_id ;; }

  # ——— Per-user rollups, replicated onto every row ———

  dimension: channels_active_for_agent { type: number sql: ${TABLE}.channels_active_for_agent ;; label: "Active Channels (this user+agent)" }
  dimension: channels_total_for_agent  { type: number sql: ${TABLE}.channels_total_for_agent ;;  label: "Total Channels (this user+agent)" }
  dimension: user_active_agent_count   { type: number sql: ${TABLE}.user_active_agent_count ;;   label: "Active Agents (this user)" }

  dimension: user_has_engaged {
    type: yesno
    sql: ${TABLE}.user_has_engaged ;;
    label: "Seller Has Taken An Action"
    description: "No means every connection this seller has is a system-written page-load row — they opened an agent but never clicked anything."
  }
  dimension: user_agents_touched_count { type: number sql: ${TABLE}.user_agents_touched_count ;; label: "Agents Touched (this user)" }

  dimension: user_adoption_depth {
    type: string
    sql: CASE ${TABLE}.user_active_agent_count
           WHEN 0 THEN 'None active'
           WHEN 1 THEN '1 agent'
           WHEN 2 THEN '2 agents'
           WHEN 3 THEN '3 agents'
           WHEN 4 THEN '4 agents'
           ELSE '5 agents'
         END ;;
    order_by_field: user_active_agent_count
    label: "Agents running per seller"
    description: "How many distinct agents this seller currently has switched on, across all their channels."
  }

  dimension: user_active_agent_bucket {
    type: tier
    tiers: [1, 2, 3, 4, 5]
    style: integer
    sql: ${TABLE}.user_active_agent_count ;;
    label: "Active Agent Count Bucket"
  }

  # ===========================================================================
  # MEASURES — three counting levels. Do not mix them in one tile.
  # ===========================================================================

  # --- Level 1: connections (user x agent x channel) ---

  measure: connections {
    type: count
    label: "Connections"
    drill_fields: [connection_drill*]
  }

  measure: active_connections {
    type: count
    filters: [is_active: "yes"]
    label: "Active Connections"
    description: "user x agent x channel. Larger than Active Agents for multi-channel merchants."
    drill_fields: [connection_drill*]
  }

  measure: in_setup_connections {
    type: count
    filters: [connection_state: "In setup"]
    label: "Connections In Setup"
    description: "Reached preview or plan-creation and never went live. The onboarding gap."
    drill_fields: [connection_drill*]
  }

  measure: switched_off_connections {
    type: count
    filters: [connection_state: "Switched off"]
    label: "Connections Switched Off"
    description: "Was live at some point, is not now. This is real churn — do not confuse with In setup."
    drill_fields: [connection_drill*]
  }

  measure: abandoned_setup_connections {
    type: count
    filters: [connection_state: "Abandoned setup"]
    label: "Connections Abandoned In Setup"
  }

  measure: setup_completion_rate {
    type: number
    sql: SAFE_DIVIDE(${active_connections},
      NULLIF(${active_connections} + ${in_setup_connections}, 0)) ;;
    value_format_name: percent_1
    label: "Setup Completion Rate"
    description: "Of connections that got as far as preview, how many went live."
  }

  measure: connection_churn_rate {
    type: number
    sql: SAFE_DIVIDE(${switched_off_connections},
      NULLIF(${active_connections} + ${switched_off_connections}, 0)) ;;
    value_format_name: percent_1
    label: "Connection Churn Rate"
    description: "Of connections that ever went live, how many are off now."
  }

  measure: never_enabled_connections {
    type: count
    filters: [connection_state: "Never enabled"]
    label: "Never-Enabled Connections"
  }

  measure: engaged_connections {
    type: count
    filters: [is_engaged: "yes"]
    label: "Engaged Connections"
  }

  # --- Level 2: agents (user x agent) — the product adoption number ---

  measure: agents_touched {
    type: count_distinct
    sql: ${TABLE}.user_agent_key ;;
    label: "Agents Touched"
    drill_fields: [connection_drill*]
  }

  measure: active_agents {
    type: count_distinct
    sql: IF(${TABLE}.agent_active_for_user, ${TABLE}.user_agent_key, NULL) ;;
    label: "Active Agents"
    description: "Distinct user x agent pairs with at least one active channel. Max 5 per user."
    drill_fields: [connection_drill*]
  }

  # --- Level 3: users ---

  measure: users_touched {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Sellers Reached"
    description: "Every seller who has loaded at least one agent page, including those who never took an action. This is reach, not customers."
    drill_fields: [connection_drill*]
  }

  measure: users_with_active_agent {
    type: count_distinct
    sql: IF(${TABLE}.is_active, ${TABLE}.user_id, NULL) ;;
    label: "Users With Active Agent"
    description: "Deduped across agents and channels — will NOT sum across the Agent dimension."
    drill_fields: [connection_drill*]
  }

  # --- Rates ---

  measure: activation_rate_engaged {
    type: number
    sql: SAFE_DIVIDE(${active_connections}, NULLIF(${engaged_connections}, 0)) ;;
    value_format_name: percent_1
    label: "Activation Rate (engaged)"
    description: "Of connections a human actually touched, how many are on. Filter to Is User-Toggled Agent = Yes."
  }

  measure: activation_rate_all {
    type: number
    sql: SAFE_DIVIDE(${active_connections}, NULLIF(${connections}, 0)) ;;
    value_format_name: percent_1
    label: "Activation Rate (all connections)"
    description: "Of every available connection including never-enabled, how many are on."
  }

  measure: users_without_active_agent {
    type: count_distinct
    sql: IF(${TABLE}.user_active_agent_count = 0, ${TABLE}.user_id, NULL) ;;
    label: "Sellers With No Active Agent"
    description: "Opened at least one agent page but currently has nothing switched on."
    drill_fields: [connection_drill*]
  }

  measure: agents_per_active_seller {
    type: number
    sql: SAFE_DIVIDE(${active_agents}, NULLIF(${users_with_active_agent}, 0)) ;;
    value_format_name: decimal_2
    label: "Agents per Active Seller"
    description: "How the agent count and the seller count relate. Above 1.0 means sellers run more than one agent."
  }

  measure: sellers_engaged {
    type: count_distinct
    sql: IF(${TABLE}.user_has_engaged, ${TABLE}.user_id, NULL) ;;
    label: "Sellers Who Took An Action"
    description: "Opened an agent AND did something — enabled, paused, or connected a channel."
    drill_fields: [connection_drill*]
  }

  measure: sellers_reached_only {
    type: count_distinct
    sql: IF(NOT ${TABLE}.user_has_engaged, ${TABLE}.user_id, NULL) ;;
    label: "Sellers Who Only Looked"
    description: "Loaded an agent page and never clicked. These sit in the denominator of the all-reached rate below and are why it reads low."
    drill_fields: [connection_drill*]
  }

  # TWO RATES, TWO QUESTIONS. Always present the qualifier with the number.
  #   of sellers who tried  -> "when a seller tries an agent, does it stick?"
  #   of all sellers reached -> "of everyone who saw an agent, how many run one?"

  measure: seller_activation_rate_engaged {
    type: number
    sql: SAFE_DIVIDE(${users_with_active_agent}, NULLIF(${sellers_engaged}, 0)) ;;
    value_format_name: percent_1
    label: "Activation Rate (of sellers who tried)"
    description: "Sellers with an agent running, over sellers who took any action on an agent. The product-quality number."
  }

  measure: user_activation_rate {
    type: number
    sql: SAFE_DIVIDE(${users_with_active_agent}, NULLIF(${users_touched}, 0)) ;;
    value_format_name: percent_1
    label: "Activation Rate (of all sellers reached)"
    description: "Sellers with an agent running, over EVERY seller who has loaded an agent page — including those who never clicked. A funnel number, not an activation number."
  }

  measure: avg_channels_per_agent {
    type: number
    sql: SAFE_DIVIDE(${connections}, NULLIF(${agents_touched}, 0)) ;;
    value_format_name: decimal_2
    label: "Avg Channels per Agent"
  }

  set: connection_drill {
    fields: [
      user_id,
      echo_me_user_attrs.full_name,
      echo_me_user_attrs.sign_up_user_email,
      echo_me_user_attrs.utm_regintent,
      agent_name,
      connection_state,
      status_display,
      status_last_changed_time,
      events_for_entity,
      channel_type,
      external_channel_name,
      external_channel_id,
      linked_meta_app,
      user_active_agent_count,
      echo_me_user_attrs.user_current_status
    ]
  }
}


# -----------------------------------------------------------------------------
# 3. echo_me_agent_monthly — month x user x agent x channel, span-based
#    Point-in-time state at each month end. Use for every trend line.
# -----------------------------------------------------------------------------
view: echo_me_agent_monthly {
  parameter: include_internal_users {
    type: unquoted
    allowed_value: { label: "No"  value: "no"  }
    allowed_value: { label: "Yes" value: "yes" }
    default_value: "no"
    label: "Include Internal Users"
  }

  derived_table: {
    sql:
      WITH ev AS (
        SELECT
          entity_key, user_agent_key, user_id, agent_key, agent_name,
          agent_sort_order, channel_key, is_toggle_agent,
          event_ts, is_active, source_row_id
        FROM ${echo_me_agent_events.SQL_TABLE_NAME}
        WHERE 1=1
        {% if include_internal_users._parameter_value == 'no' %}
          AND NOT is_internal_user
          AND NOT channel_id_looks_internal
        {% endif %}
        -- Dedupe on (entity, timestamp) ONLY. Never on id: the 501 duplicate
        -- SEA ids all carry distinct timestamps and are real events.
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY entity_key, event_ts ORDER BY source_row_id DESC
        ) = 1
      ),

      spans AS (
      SELECT
      e.*,
      LEAD(event_ts) OVER (PARTITION BY entity_key ORDER BY event_ts) AS next_event_ts,
      LAG(is_active) OVER (PARTITION BY entity_key ORDER BY event_ts) AS prev_is_active
      FROM ev e
      ),

      entities AS (
      SELECT
      entity_key,
      ANY_VALUE(user_agent_key)   AS user_agent_key,
      ANY_VALUE(user_id)          AS user_id,
      ANY_VALUE(agent_key)        AS agent_key,
      ANY_VALUE(agent_name)       AS agent_name,
      ANY_VALUE(agent_sort_order) AS agent_sort_order,
      ANY_VALUE(channel_key)      AS channel_key,
      ANY_VALUE(is_toggle_agent)  AS is_toggle_agent,
      MIN(event_ts)               AS first_event_ts
      FROM ev
      GROUP BY 1
      ),

      months AS (
      SELECT
      m AS month_start,
      TIMESTAMP_SUB(
      TIMESTAMP(DATE_ADD(m, INTERVAL 1 MONTH), 'America/New_York'),
      INTERVAL 1 MICROSECOND) AS month_end_ts
      FROM UNNEST(GENERATE_DATE_ARRAY(
      (SELECT DATE_TRUNC(MIN(DATE(event_ts, 'America/New_York')), MONTH) FROM ev),
      DATE_TRUNC(CURRENT_DATE('America/New_York'), MONTH),
      INTERVAL 1 MONTH)) AS m
      ),

      entity_months AS (
      SELECT e.*, m.month_start, m.month_end_ts
      FROM entities e
      JOIN months m
      ON m.month_start >= DATE_TRUNC(DATE(e.first_event_ts, 'America/New_York'), MONTH)
      ),

      state AS (
      SELECT
      em.entity_key, em.user_agent_key, em.user_id, em.agent_key,
      em.agent_name, em.agent_sort_order, em.channel_key,
      em.is_toggle_agent, em.month_start,
      COALESCE(s.is_active, FALSE) AS is_active_at_month_end
      FROM entity_months em
      LEFT JOIN spans s
      ON  s.entity_key = em.entity_key
      AND s.event_ts  <= em.month_end_ts
      AND (s.next_event_ts IS NULL OR s.next_event_ts > em.month_end_ts)
      ),

      transitions AS (
      SELECT
      entity_key,
      DATE_TRUNC(DATE(event_ts, 'America/New_York'), MONTH) AS month_start,
      COUNTIF(is_active AND NOT COALESCE(prev_is_active, FALSE))  AS activations,
      COUNTIF(NOT is_active AND COALESCE(prev_is_active, FALSE))  AS deactivations
      FROM spans
      GROUP BY 1, 2
      ),

      -- Agent-grain flow. Derived from month-end STOCK, not from raw events,
      -- so it reconciles exactly to the trend line: for any month,
      -- agent_activations - agent_deactivations = active_agents(M) - active_agents(M-1).
      -- Connection-grain activations do NOT reconcile to that delta, because
      -- one user action fans out across every channel they have connected.
      agent_state AS (
      SELECT
      user_agent_key,
      month_start,
      LOGICAL_OR(is_active_at_month_end) AS agent_active
      FROM state
      GROUP BY 1, 2
      ),

      agent_flow AS (
      SELECT
      user_agent_key,
      month_start,
      agent_active,
      COALESCE(LAG(agent_active) OVER (
      PARTITION BY user_agent_key ORDER BY month_start), FALSE) AS prev_agent_active
      FROM agent_state
      )

      SELECT
      CONCAT(st.entity_key, '|', CAST(st.month_start AS STRING)) AS pk,
      st.entity_key,
      st.user_agent_key,
      st.user_id,
      st.agent_key,
      st.agent_name,
      st.agent_sort_order,
      st.channel_key,
      st.is_toggle_agent,
      st.month_start,
      st.is_active_at_month_end,
      -- agent-level state for this user in this month, so churn tiles are
      -- not multiplied by the DMA per-channel fan-out
      LOGICAL_OR(st.is_active_at_month_end) OVER (
      PARTITION BY st.user_agent_key, st.month_start) AS agent_active_at_month_end,
      COALESCE(tr.activations, 0)   AS activations,
      COALESCE(tr.deactivations, 0) AS deactivations,

      -- Replicated across the user's channels; the measures below use
      -- COUNT DISTINCT on user_agent_key so the fan-out cancels out.
      COALESCE(af.agent_active AND NOT af.prev_agent_active, FALSE) AS agent_activated_in_month,
      COALESCE(NOT af.agent_active AND af.prev_agent_active, FALSE) AS agent_deactivated_in_month

      FROM state st
      LEFT JOIN transitions tr
      ON tr.entity_key = st.entity_key AND tr.month_start = st.month_start
      LEFT JOIN agent_flow af
      ON af.user_agent_key = st.user_agent_key AND af.month_start = st.month_start
      ;;
  }

  dimension: pk { type: string sql: ${TABLE}.pk ;; primary_key: yes hidden: yes }

  dimension: user_id        { type: string sql: ${TABLE}.user_id ;; }
  dimension: user_agent_key { type: string sql: ${TABLE}.user_agent_key ;; hidden: yes }
  dimension: agent_key      { type: string sql: ${TABLE}.agent_key ;; }
  dimension: agent_name {
    type: string
    sql: ${TABLE}.agent_name ;;
    label: "Agent"
    order_by_field: agent_sort_order
  }
  dimension: agent_sort_order { type: number sql: ${TABLE}.agent_sort_order ;; hidden: yes }
  dimension: channel_key      { type: string sql: ${TABLE}.channel_key ;; }
  dimension: is_toggle_agent  { type: yesno  sql: ${TABLE}.is_toggle_agent ;; }

  dimension_group: month {
    type: time
    datatype: date
    convert_tz: no
    timeframes: [month, quarter, year]
    sql: ${TABLE}.month_start ;;
    label: "Snapshot"
    description: "Bucketed America/New_York. State measured at month end."
  }

  dimension: is_active_at_month_end {
    type: yesno
    sql: ${TABLE}.is_active_at_month_end ;;
    label: "Connection Active At Month End"
  }

  dimension: agent_active_at_month_end {
    type: yesno
    sql: ${TABLE}.agent_active_at_month_end ;;
    label: "Agent Active At Month End"
  }

  # ——— Measures, same three levels ———

  measure: active_connections {
    type: count
    filters: [is_active_at_month_end: "yes"]
    label: "Active Connections (month end)"
  }

  measure: inactive_connections {
    type: count
    filters: [is_active_at_month_end: "no"]
    label: "Inactive Connections (month end)"
  }

  measure: active_agents {
    type: count_distinct
    sql: IF(${TABLE}.agent_active_at_month_end, ${TABLE}.user_agent_key, NULL) ;;
    label: "Active Agents (month end)"
    description: "Distinct user x agent. This is the adoption trend line."
  }

  measure: active_users {
    type: count_distinct
    sql: IF(${TABLE}.is_active_at_month_end, ${TABLE}.user_id, NULL) ;;
    label: "Active Sellers (month end)"
    description: "Distinct sellers with at least one agent switched on. Plot alongside Active Agents to show adoption depth."
  }

  measure: agents_per_active_seller {
    type: number
    sql: SAFE_DIVIDE(${active_agents}, NULLIF(${active_users}, 0)) ;;
    value_format_name: decimal_2
    label: "Agents per Active Seller (month end)"
  }

  measure: connections_available {
    type: count
    label: "Connections Available (cumulative to month end)"
  }

  measure: activations {
    type: sum
    sql: ${TABLE}.activations ;;
    label: "Connection Activations In Month"
  }

  measure: deactivations {
    type: sum
    sql: ${TABLE}.deactivations ;;
    label: "Connection Deactivations In Month"
  }

  measure: net_activations {
    type: number
    sql: ${activations} - ${deactivations} ;;
    label: "Net Connection Activations"
  }

  # --- Agent-grain flow. Use THESE against the Active Agents trend line. ---

  dimension: agent_activated_in_month {
    type: yesno
    sql: ${TABLE}.agent_activated_in_month ;;
    hidden: yes
  }

  dimension: agent_deactivated_in_month {
    type: yesno
    sql: ${TABLE}.agent_deactivated_in_month ;;
    hidden: yes
  }

  measure: agent_activations {
    type: count_distinct
    sql: IF(${TABLE}.agent_activated_in_month, ${TABLE}.user_agent_key, NULL) ;;
    label: "Agent Activations In Month"
    description: "Users who had this agent off at the end of last month and on at the end of this one."
  }

  measure: agent_deactivations {
    type: count_distinct
    sql: IF(${TABLE}.agent_deactivated_in_month, ${TABLE}.user_agent_key, NULL) ;;
    label: "Agent Deactivations In Month"
  }

  measure: agent_deactivations_negative {
    type: number
    sql: -1 * ${agent_deactivations} ;;
    label: "Agent Deactivations In Month (negative)"
  }

  measure: net_agent_activations {
    type: number
    sql: ${agent_activations} - ${agent_deactivations} ;;
    label: "Net Agent Activations"
    description: "Reconciles exactly to the month-over-month change in Active Agents (month end)."
  }

  measure: month_end_activation_rate {
    type: number
    sql: SAFE_DIVIDE(${active_connections}, NULLIF(${connections_available}, 0)) ;;
    value_format_name: percent_1
    label: "Activation Rate (month end)"
  }
}


# -----------------------------------------------------------------------------
# 4. prod_ai_echo_me — user-grain view, rebuilt on the entity grain
#    All previous field names preserved. Semantics that changed:
#      sea_status / coda_status / dma_status  now the user's BEST channel state
#                                             (was: an arbitrary channel)
#      total_with_any_agent_active            now distinct agents active (0-5)
#      total_echo_me_agents                   now events excl. system defaults
#      external_channel_*                     representative only; use
#                                             echo_me_agent_status per channel
# -----------------------------------------------------------------------------
view: prod_ai_echo_me {
  filter: date_range {
    type: date
    description: "Filter by trial start date (initial_start_date). Optional."
  }

  derived_table: {
    sql:
      WITH ev AS (
        SELECT *
        FROM ${echo_me_agent_events.SQL_TABLE_NAME}
        WHERE NOT is_internal_user
          AND NOT channel_id_looks_internal
      ),

      latest_entity AS (
      SELECT *
      FROM ev
      QUALIFY ROW_NUMBER() OVER (
      PARTITION BY entity_key ORDER BY event_ts DESC, source_row_id DESC
      ) = 1
      ),

      -- Collapse channels: an agent is active for the user if ANY channel is.
      user_agent AS (
      SELECT
      user_id,
      agent_key,
      LOGICAL_OR(is_active)      AS agent_active,
      LOGICAL_OR(NOT is_system_default) AS agent_engaged,
      COUNTIF(is_active)         AS channels_active,
      COUNT(*)                   AS channels_total,
      -- best available status: prefer an active channel's label
      ARRAY_AGG(status_display ORDER BY is_active DESC, event_ts DESC LIMIT 1)[SAFE_OFFSET(0)] AS best_status
      FROM latest_entity
      GROUP BY 1, 2
      ),

      user_latest_row AS (
      SELECT *
      FROM ev
      QUALIFY ROW_NUMBER() OVER (
      PARTITION BY user_id ORDER BY event_ts DESC, source_row_id DESC
      ) = 1
      ),

      agent_wide AS (
      SELECT
      user_id,
      MAX(IF(agent_key = 'sea',  best_status, NULL)) AS sea_status,
      MAX(IF(agent_key = 'coda', best_status, NULL)) AS coda_status,
      MAX(IF(agent_key = 'dma',  best_status, NULL)) AS dma_status,
      MAX(IF(agent_key = 'asa',  best_status, NULL)) AS asa_status,
      MAX(IF(agent_key = 'grx',  best_status, NULL)) AS grx_status,

      COUNTIF(agent_key = 'sea'  AND agent_active) AS total_sea_active,
      COUNTIF(agent_key = 'coda' AND agent_active) AS total_coda_active,
      COUNTIF(agent_key = 'dma'  AND agent_active) AS total_dma_active,
      COUNTIF(agent_key = 'asa'  AND agent_active) AS total_asa_active,
      COUNTIF(agent_key = 'grx'  AND agent_active) AS total_grx_active,

      COUNTIF(agent_active)                        AS active_agent_count,
      COUNTIF(agent_engaged AND NOT agent_active)  AS paused_agent_count,
      COUNTIF(NOT agent_engaged)                   AS never_enabled_agent_count,
      COUNT(*)                                     AS agents_touched_count,
      SUM(channels_active)                         AS active_connection_count,
      SUM(channels_total)                          AS total_connection_count,
      IF(COUNTIF(agent_active) > 0, 'active', 'inactive') AS overall_agent_status
      FROM user_agent
      GROUP BY user_id
      ),

      raw_counts AS (
      SELECT
      user_id,
      COUNTIF(NOT is_system_default) AS total_echo_me_agents,
      IF(COUNTIF(ai_echo_setup_complete = TRUE) > 0, 'Yes', 'No') AS onboarding_complete
      FROM ev
      GROUP BY user_id
      )

      SELECT
      aw.user_id,

      ulr.source_row_id AS echo_me_id,
      ulr.session_id,
      ulr.agent_raw     AS agent,
      ulr.created_at    AS echo_me_created_at,
      ulr.updated_at    AS echo_me_updated_at,
      ulr.external_channel_id,
      ulr.external_channel_name,
      ulr.channel_type,
      ulr.linked_meta_app,
      ulr.linked_meta_app_id,

      aw.sea_status, aw.coda_status, aw.dma_status,
      aw.asa_status, aw.grx_status,
      aw.overall_agent_status,
      aw.total_sea_active, aw.total_coda_active, aw.total_dma_active,
      aw.total_asa_active, aw.total_grx_active,
      aw.active_agent_count,
      aw.paused_agent_count,
      aw.never_enabled_agent_count,
      aw.agents_touched_count,
      aw.active_connection_count,
      aw.total_connection_count,
      aw.active_agent_count AS total_with_any_agent_active,
      aw.agents_touched_count - aw.active_agent_count AS inactive_agent_count,

      rc.total_echo_me_agents,
      rc.onboarding_complete,

      ua.sign_up_url_code, ua.sign_up_user_username, ua.sign_up_user_email,
      ua.profile_email, ua.first_name, ua.last_name,
      ua.marketing_campaign, ua.utm_regintent, ua.business_type,
      ua.acquisition_source, ua.device_category, ua.user_agent,
      ua.subscription_id, ua.subscription_status, ua.user_current_status,
      ua.plan_name, ua.plan_interval, ua.price,
      ua.trial_starts, ua.trial_ends, ua.effective_trial_end,
      ua.cancellation_applied_at,
      IF(ua.subscription_status = 'active', 'Yes', 'No') AS is_subscriber,

      CASE
      WHEN ua.trial_ends IS NULL THEN 'No trial'
      WHEN DATE(ua.effective_trial_end) <= CURRENT_DATE() THEN 'Ended'
      ELSE 'Started'
      END AS trial_status

      FROM agent_wide aw
      JOIN raw_counts rc      ON rc.user_id  = aw.user_id
      JOIN user_latest_row ulr ON ulr.user_id = aw.user_id
      JOIN ${echo_me_user_attrs.SQL_TABLE_NAME} ua ON ua.user_id = aw.user_id
      WHERE ua.trial_ends IS NOT NULL
      {% if date_range._is_filtered %}
      AND {% condition date_range %} ua.trial_starts {% endcondition %}
      {% endif %}
      ;;
  }

  dimension: user_id    { type: string sql: ${TABLE}.user_id ;; primary_key: yes }
  dimension: echo_me_id { type: string sql: ${TABLE}.echo_me_id ;; label: "Latest Event ID" }
  dimension: session_id { type: string sql: ${TABLE}.session_id ;; label: "Latest Session ID" }

  dimension_group: echo_me_created {
    type: time
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.echo_me_created_at ;;
    timeframes: [time, date, week, month, quarter, year]
    label: "Echo Me Created"
    description: "Warehouse load timestamp, not event time. Use Status Last Changed for event dating."
  }

  dimension_group: echo_me_updated {
    type: time
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.echo_me_updated_at ;;
    timeframes: [time, date, week, month, quarter, year]
    label: "Echo Me Updated"
  }

  dimension: agent { type: string sql: ${TABLE}.agent ;; label: "Agent (latest event)" }

  dimension: overall_agent_status {
    type: string
    sql: ${TABLE}.overall_agent_status ;;
    label: "Overall Agent Status"
  }

  dimension: sea_status  { type: string sql: ${TABLE}.sea_status ;;  label: "Social Engagement Agent Status" description: "Best state across the user's channels." }
  dimension: coda_status { type: string sql: ${TABLE}.coda_status ;; label: "Comment to DM Agent Status"      description: "Best state across the user's channels." }
  dimension: dma_status  { type: string sql: ${TABLE}.dma_status ;;  label: "Deal Monitoring Agent Status"    description: "Best state across the user's channels." }
  dimension: asa_status  { type: string sql: ${TABLE}.asa_status ;;  label: "Auto Selling Agent Status" }
  dimension: grx_status  { type: string sql: ${TABLE}.grx_status ;;  label: "Growth RX Agent Status" }

  dimension: active_agent_count {
    type: number
    sql: ${TABLE}.active_agent_count ;;
    label: "Active Agents"
    description: "Distinct agents with at least one active channel (0-5)."
  }

  dimension: paused_agent_count        { type: number sql: ${TABLE}.paused_agent_count ;;        label: "Paused Agents" }
  dimension: never_enabled_agent_count { type: number sql: ${TABLE}.never_enabled_agent_count ;; label: "Never-Enabled Agents" }
  dimension: inactive_agent_count      { type: number sql: ${TABLE}.inactive_agent_count ;;      label: "Inactive Agents" }
  dimension: agents_touched_count      { type: number sql: ${TABLE}.agents_touched_count ;;      label: "Agents Touched" }
  dimension: active_connection_count   { type: number sql: ${TABLE}.active_connection_count ;;   label: "Active Connections" }
  dimension: total_connection_count    { type: number sql: ${TABLE}.total_connection_count ;;    label: "Total Connections" }

  dimension: active_agent_bucket {
    type: tier
    tiers: [1, 2, 3, 4, 5]
    style: integer
    sql: ${TABLE}.active_agent_count ;;
    label: "Active Agent Count Bucket"
    description: "X-axis for the agents-per-user distribution tile."
  }

  dimension: is_multi_agent_user {
    type: yesno
    sql: ${TABLE}.active_agent_count >= 2 ;;
    label: "Multi-Agent User"
  }

  dimension: is_multi_channel_user {
    type: yesno
    sql: ${TABLE}.total_connection_count > ${TABLE}.agents_touched_count ;;
    label: "Multi-Channel User"
  }

  dimension: total_echo_me_agents {
    type: number
    sql: ${TABLE}.total_echo_me_agents ;;
    label: "Total Echo Me Events"
  }

  dimension: total_with_any_agent_active {
    type: number
    sql: ${TABLE}.total_with_any_agent_active ;;
    label: "Distinct Agents Active"
  }

  dimension: external_channel_id {
    type: string
    sql: ${TABLE}.external_channel_id ;;
    label: "External Channel ID"
    description: "From the user's most recent event only. Users can have up to 73 channels — use the Agent Status explore for per-channel truth."
  }
  dimension: external_channel_name { type: string sql: ${TABLE}.external_channel_name ;; label: "External Channel Name" }
  dimension: channel_type          { type: string sql: ${TABLE}.channel_type ;;          label: "Channel Type" }
  dimension: linked_meta_app       { type: string sql: ${TABLE}.linked_meta_app ;;       label: "Linked Meta App" }
  dimension: linked_meta_app_id    { type: string sql: ${TABLE}.linked_meta_app_id ;;    label: "Linked Meta App ID" }

  dimension: subscription_id     { type: string sql: ${TABLE}.subscription_id ;; }
  dimension: subscription_status { type: string sql: ${TABLE}.subscription_status ;; }
  dimension: user_current_status { type: string sql: ${TABLE}.user_current_status ;; label: "User Current Status" }
  dimension: is_subscriber       { type: string sql: ${TABLE}.is_subscriber ;; label: "Subscriber" }
  dimension: trial_status        { type: string sql: ${TABLE}.trial_status ;; }

  dimension_group: trial_starts_at {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.trial_starts ;;
    label: "Trial Start"
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

  dimension: price           { type: number sql: ${TABLE}.price ;; value_format_name: decimal_2 }
  dimension: plan_name       { type: string sql: ${TABLE}.plan_name ;; }
  dimension: plan_interval   { type: string sql: ${TABLE}.plan_interval ;; label: "Interval" }
  dimension: device_category { type: string sql: ${TABLE}.device_category ;; }
  dimension: user_agent      { type: string sql: ${TABLE}.user_agent ;; }

  dimension: sign_up_user_url {
    type: string
    sql: 'https://pop.store/' || ${TABLE}.sign_up_url_code ;;
  }
  dimension: profile_email { type: string sql: ${TABLE}.profile_email ;; label: "Profile Email (JSON)" }
  dimension: first_name    { type: string sql: ${TABLE}.first_name ;; label: "First Name" }
  dimension: last_name     { type: string sql: ${TABLE}.last_name ;;  label: "Last Name" }
  dimension: full_name {
    type: string
    sql: TRIM(CONCAT(COALESCE(${TABLE}.first_name, ''), ' ', COALESCE(${TABLE}.last_name, ''))) ;;
    label: "Full Name"
  }
  dimension: sign_up_user_username { type: string sql: ${TABLE}.sign_up_user_username ;; }
  dimension: sign_up_user_email    { type: string sql: ${TABLE}.sign_up_user_email ;; }
  dimension: marketing_campaign    { type: string sql: ${TABLE}.marketing_campaign ;; }
  dimension: utm_regintent         { type: string sql: ${TABLE}.utm_regintent ;; label: "UTM Regintent" }
  dimension: business_type         { type: string sql: ${TABLE}.business_type ;; }
  dimension: acquisition_source    { type: string sql: ${TABLE}.acquisition_source ;; }
  dimension: onboarding_complete   { type: string sql: ${TABLE}.onboarding_complete ;; label: "Onboarding Complete" }

  # ——— Measures ———

  measure: total_unique_users {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Unique Users"
    drill_fields: [drill_details*]
  }

  measure: total_active {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [overall_agent_status: "active"]
    label: "Active Agent Users"
    drill_fields: [drill_details*]
  }

  measure: total_inactive {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [overall_agent_status: "inactive"]
    label: "Inactive Agent Users"
    drill_fields: [drill_details*]
  }

  measure: activation_rate {
    type: number
    sql: SAFE_DIVIDE(${total_active}, NULLIF(${total_unique_users}, 0)) * 100 ;;
    label: "Activation Rate (%)"
    value_format_name: decimal_1
  }

  measure: sum_active_agents {
    type: sum
    sql: ${TABLE}.active_agent_count ;;
    label: "Total Active Agents"
  }

  measure: sum_active_connections {
    type: sum
    sql: ${TABLE}.active_connection_count ;;
    label: "Total Active Connections"
  }

  measure: avg_active_agents_per_user {
    type: average
    sql: ${TABLE}.active_agent_count ;;
    value_format_name: decimal_2
    label: "Avg Active Agents per User"
  }

  measure: avg_connections_per_user {
    type: average
    sql: ${TABLE}.total_connection_count ;;
    value_format_name: decimal_2
    label: "Avg Connections per User"
  }

  measure: count_subscribers {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [user_current_status: "Subscriber"]
    label: "Current Subscribers"
    drill_fields: [drill_details*]
  }

  measure: count_cancelled {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [user_current_status: "Cancelled"]
    label: "Cancelled Users"
    drill_fields: [drill_details*]
  }

  measure: count_setup_complete {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [onboarding_complete: "Yes"]
    label: "AI Echo Setup Complete Users"
    drill_fields: [drill_details*]
  }

  set: drill_details {
    fields: [
      user_id, first_name, last_name, profile_email,
      sign_up_user_username, sign_up_user_email, sign_up_user_url,
      is_subscriber, onboarding_complete,
      active_agent_count, paused_agent_count, never_enabled_agent_count,
      agents_touched_count, active_connection_count, total_connection_count,
      overall_agent_status,
      sea_status, coda_status, dma_status, asa_status, grx_status,
      user_current_status, subscription_id, subscription_status,
      plan_name, plan_interval, price, trial_status,
      trial_starts_at_time, trial_ends_at_date, effective_trial_ends_at_date,
      marketing_campaign, utm_regintent, business_type, acquisition_source,
      device_category
    ]
  }
}


# =============================================================================
# EXPLORES — paste into your model file
# =============================================================================
#
# explore: echo_me_agent_status {
#   label: "Echo Me — Connections (current state)"
#   view_label: "Connection"
#   description: "One row per user x agent x channel."
#   join: echo_me_user_attrs {
#     view_label: "User"
#     type: left_outer
#     relationship: many_to_one
#     sql_on: ${echo_me_agent_status.user_id} = ${echo_me_user_attrs.user_id} ;;
#   }
# }
#
# explore: echo_me_agent_monthly {
#   label: "Echo Me — Agents Over Time"
#   view_label: "Snapshot"
#   join: echo_me_user_attrs {
#     view_label: "User"
#     type: left_outer
#     relationship: many_to_one
#     sql_on: ${echo_me_agent_monthly.user_id} = ${echo_me_user_attrs.user_id} ;;
#   }
# }
#
# explore: prod_ai_echo_me {
#   label: "Echo Me — Trial Users"
#   description: "One row per trial user. Do NOT join the connection view here
#                 unless you want deliberate fan-out."
# }
#
# datagroup: echo_me_default_datagroup {
#   sql_trigger: SELECT MAX(updated_at) FROM `popshoplive-26f81.commentchat.echo_me_agents` ;;
#   max_cache_age: "1 hour"
# }
# =============================================================================

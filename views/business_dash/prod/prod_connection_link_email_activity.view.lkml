view: prod_connection_link_email_activity {
  filter: date_range {
    type: date
    description: "Filter events by when they occurred. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql:
      -- ✅ Event grain: one row per connection-link event (sent OR opened),
      -- across BOTH channels (email + sms). The drill-down needs individual
      -- events, and sent/opened events DO NOT share a key (each has its own
      -- session_id), so the only reliable rollup axis is creator_id.
      WITH base AS (
        SELECT
          id,
          creator_id,
          session_id,
          action,
          location,
          action_details,
          occured_at,
          -- ✅ Channel: Email vs SMS, derived from the action name.
          CASE
            WHEN action LIKE 'connection_link_email_%' THEN 'Email'
            WHEN action LIKE 'connection_link_sms_%'   THEN 'SMS'
          END AS channel,
          -- ✅ Event type is channel-agnostic: Sent vs Opened.
          CASE
            WHEN action LIKE '%_sent'   THEN 'Sent'
            WHEN action LIKE '%_opened' THEN 'Opened'
            WHEN action LIKE '%_failed' THEN 'Failed'
          END AS event_type
        FROM `popshoplive-26f81.commentchat.user_activity`
        WHERE action IN (
            'connection_link_email_sent',
            'connection_link_email_opened',
            'connection_link_sms_sent',
            'connection_link_sms_opened',
            'connection_link_email_failed',
            'connection_link_sms_failed'
          )
          AND {% condition date_range %} occured_at {% endcondition %}
      ),

      -- ✅ Roll up each creator's totals onto every one of their event rows.
      -- Counts are by event_type (not by raw action) so they include BOTH
      -- channels. This lets us classify engagement at creator level while
      -- staying at event grain for drill-downs.
      with_creator_rollup AS (
      SELECT
      base.*,
      COUNTIF(event_type = 'Sent')   OVER (PARTITION BY creator_id) AS creator_sent_count,
      COUNTIF(event_type = 'Opened') OVER (PARTITION BY creator_id) AS creator_opened_count,
      COUNTIF(event_type = 'Failed') OVER (PARTITION BY creator_id) AS creator_failed_count
      FROM base
      )

      SELECT
      *,
      -- ✅ Creator-level engagement bucket (any channel). The
      -- "Opened, No Send Recorded" bucket is expected while capture is
      -- incomplete — we started logging these events recently.
      CASE
      WHEN creator_failed_count > 0 THEN 'Failed'
      WHEN creator_sent_count > 0 AND creator_opened_count > 0 THEN 'Sent & Opened'
      WHEN creator_sent_count > 0 AND creator_opened_count = 0 THEN 'Sent, Not Opened'
      WHEN creator_sent_count = 0 AND creator_opened_count > 0 THEN 'Opened, No Send Recorded'
      ELSE 'Unknown'
      END AS creator_engagement_status
      FROM with_creator_rollup ;;
  }

  # ───────────────────────────── Dimensions ─────────────────────────────

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
    primary_key: yes
    hidden: yes
  }

  dimension: creator_id {
    type: string
    sql: ${TABLE}.creator_id ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: action {
    type: string
    sql: ${TABLE}.action ;;
    description: "Raw action: connection_link_{email|sms}_{sent|opened}."
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
    label: "Channel"
    description: "Email or SMS, derived from the action name."
  }

  dimension: event_type {
    type: string
    sql: ${TABLE}.event_type ;;
    label: "Event Type"
    description: "Channel-agnostic event type: 'Sent' or 'Opened'."
  }

  dimension: location {
    type: string
    sql: ${TABLE}.location ;;
  }

  dimension: action_details {
    type: string
    sql: ${TABLE}.action_details ;;
    label: "Action Details (JSON)"
  }

  dimension_group: occured_at {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    datatype: timestamp
    convert_tz: no
    sql: ${TABLE}.occured_at ;;
    label: "Occurred"
  }

  # ✅ Per-creator rollups (same value repeated on every row for that creator)
  dimension: creator_sent_count {
    type: number
    sql: ${TABLE}.creator_sent_count ;;
    label: "Creator: Total Sent"
    description: "Number of 'sent' events (any channel) this creator has in the selected range."
  }

  dimension: creator_opened_count {
    type: number
    sql: ${TABLE}.creator_opened_count ;;
    label: "Creator: Total Opened"
    description: "Number of 'opened' events (any channel) this creator has in the selected range."
  }

  dimension: creator_failed_count {
    type: number
    sql: ${TABLE}.creator_failed_count ;;
    label: "Creator: Total Failed"
    description: "Number of 'failed' events (any channel) this creator has in the selected range."
  }

  dimension: creator_engagement_status {
    type: string
    sql: ${TABLE}.creator_engagement_status ;;
    label: "Creator Engagement Status"
    description: "Failed, Sent & Opened / Sent, Not Opened / Opened, No Send Recorded. Computed across both channels."
  }

  # ───────────────────────────── Measures ─────────────────────────────

  measure: total_events {
    type: count
    label: "Total Events"
    drill_fields: [event_details*]
  }

  # ── Channel-agnostic totals ──
  measure: total_sent {
    type: count
    filters: [event_type: "Sent"]
    label: "Sent (all channels)"
    drill_fields: [event_details*]
  }

  measure: total_opened {
    type: count
    filters: [event_type: "Opened"]
    label: "Opened (all channels)"
    drill_fields: [event_details*]
  }

  measure: total_failed {
    type: count
    filters: [event_type: "Failed"]
    label: "Failed (all channels)"
    drill_fields: [event_details*]
  }

  # ── Email-only ──
  measure: emails_sent {
    type: count
    filters: [event_type: "Sent", channel: "Email"]
    label: "Emails Sent (events)"
    drill_fields: [event_details*]
  }

  measure: emails_opened {
    type: count
    filters: [event_type: "Opened", channel: "Email"]
    label: "Emails Opened (events)"
    drill_fields: [event_details*]
  }

  measure: emails_failed {
    type: count
    filters: [event_type: "Failed", channel: "Email"]
    label: "Emails Failed (events)"
    drill_fields: [event_details*]
  }

  # ── SMS-only ──
  measure: sms_sent {
    type: count
    filters: [event_type: "Sent", channel: "SMS"]
    label: "SMS Sent (events)"
    drill_fields: [event_details*]
  }

  measure: sms_opened {
    type: count
    filters: [event_type: "Opened", channel: "SMS"]
    label: "SMS Opened (events)"
    drill_fields: [event_details*]
  }

  measure: sms_failed {
    type: count
    filters: [event_type: "Failed", channel: "SMS"]
    label: "SMS Failed (events)"
    drill_fields: [event_details*]
  }

  # ── Creator-level distinct counts ──
  measure: distinct_creators {
    type: count_distinct
    sql: ${creator_id} ;;
    label: "Creators (any event)"
    drill_fields: [creator_summary*]
  }

  measure: creators_who_sent {
    type: count_distinct
    sql: ${creator_id} ;;
    filters: [event_type: "Sent"]
    label: "Creators Who Sent"
    drill_fields: [creator_summary*]
  }

  measure: creators_who_opened {
    type: count_distinct
    sql: ${creator_id} ;;
    filters: [event_type: "Opened"]
    label: "Creators Who Opened"
    drill_fields: [creator_summary*]
  }

  measure: creators_who_failed {
    type: count_distinct
    sql: ${creator_id} ;;
    filters: [event_type: "Failed"]
    label: "Creators Who Failed"
    drill_fields: [creator_summary*]
  }

  measure: creators_sent_and_opened {
    type: count_distinct
    sql: ${creator_id} ;;
    filters: [creator_engagement_status: "Sent & Opened"]
    label: "Creators Sent & Opened"
    drill_fields: [creator_summary*]
  }

  # ✅ Open rate at CREATOR level among creators with a recorded send.
  measure: creator_open_rate {
    type: number
    sql: SAFE_DIVIDE(${creators_sent_and_opened}, ${creators_who_sent}) ;;
    value_format_name: percent_1
    label: "Open Rate (creator-level)"
    drill_fields: [creator_summary*]
  }

  # ───────────────────────────── Drill sets ─────────────────────────────

  set: event_details {
    fields: [
      creator_id,
      channel,
      event_type,
      occured_at_time,
      session_id,
      location,
      action_details,
      creator_sent_count,
      creator_opened_count,
      creator_engagement_status
    ]
  }

  set: creator_summary {
    fields: [
      creator_id,
      creator_engagement_status,
      creator_sent_count,
      creator_opened_count
    ]
  }
}

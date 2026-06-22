view: prod_connection_link_email_activity {
  filter: date_range {
    type: date
    description: "Filter events by when they occurred. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql:
    -- ✅ Event grain: one row per email event (sent OR opened).
    -- We keep event grain because the drill-down needs to show individual
    -- sends/opens, AND because sent/opened events DO NOT share a key
    -- (each event has its own session_id), so the only reliable rollup
    -- axis is creator_id.
    WITH base AS (
      SELECT
        id,
        creator_id,
        session_id,
        action,
        location,
        action_details,
        occured_at,
        CASE action
          WHEN 'connection_link_email_sent'   THEN 'Sent'
          WHEN 'connection_link_email_opened' THEN 'Opened'
        END AS event_type
      FROM `popshoplive-26f81.commentchat.user_activity`
      WHERE action IN ('connection_link_email_sent', 'connection_link_email_opened')
        AND {% condition date_range %} occured_at {% endcondition %}
    ),

      -- ✅ Roll up each creator's totals onto every one of their event rows.
      -- This lets us classify engagement at the creator level while staying
      -- at event grain for drill-downs.
      with_creator_rollup AS (
      SELECT
      base.*,
      COUNTIF(action = 'connection_link_email_sent')   OVER (PARTITION BY creator_id) AS creator_sent_count,
      COUNTIF(action = 'connection_link_email_opened') OVER (PARTITION BY creator_id) AS creator_opened_count
      FROM base
      )

      SELECT
      *,
      -- ✅ Creator-level engagement bucket. The "Opened, No Send Recorded"
      -- bucket is expected while capture is incomplete (we started logging
      -- these events recently and don't have all sent/opened pairs yet).
      CASE
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
    description: "Raw action: connection_link_email_sent or connection_link_email_opened."
  }

  dimension: event_type {
    type: string
    sql: ${TABLE}.event_type ;;
    label: "Event Type"
    description: "Human-friendly event type: 'Sent' or 'Opened'."
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
    description: "Number of 'sent' events this creator has in the selected range."
  }

  dimension: creator_opened_count {
    type: number
    sql: ${TABLE}.creator_opened_count ;;
    label: "Creator: Total Opened"
    description: "Number of 'opened' events this creator has in the selected range."
  }

  dimension: creator_engagement_status {
    type: string
    sql: ${TABLE}.creator_engagement_status ;;
    label: "Creator Engagement Status"
    description: "Sent & Opened / Sent, Not Opened / Opened, No Send Recorded. The last bucket is a data-capture gap, not real behavior."
  }

  # ───────────────────────────── Measures ─────────────────────────────

  measure: total_events {
    type: count
    label: "Total Events"
    drill_fields: [event_details*]
  }

  measure: emails_sent {
    type: count
    filters: [event_type: "Sent"]
    label: "Emails Sent (events)"
    drill_fields: [event_details*]
  }

  measure: emails_opened {
    type: count
    filters: [event_type: "Opened"]
    label: "Emails Opened (events)"
    drill_fields: [event_details*]
  }

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

  measure: creators_sent_and_opened {
    type: count_distinct
    sql: ${creator_id} ;;
    filters: [creator_engagement_status: "Sent & Opened"]
    label: "Creators Sent & Opened"
    drill_fields: [creator_summary*]
  }

  # ✅ Open rate defined at CREATOR level among creators with a recorded send.
  # Numerator = creators who both sent and opened; denominator = creators who sent.
  # This avoids the meaningless event-level ratio (sends/opens don't pair up).
  measure: creator_open_rate {
    type: number
    sql: SAFE_DIVIDE(${creators_sent_and_opened}, ${creators_who_sent}) ;;
    value_format_name: percent_1
    label: "Open Rate (creator-level)"
    drill_fields: [creator_summary*]
  }

  # ───────────────────────────── Drill sets ─────────────────────────────

  # Drill into the raw individual events (sends/opens).
  set: event_details {
    fields: [
      creator_id,
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

  # Drill into a per-creator rollup (one conceptual row per creator).
  set: creator_summary {
    fields: [
      creator_id,
      creator_engagement_status,
      creator_sent_count,
      creator_opened_count
    ]
  }
}

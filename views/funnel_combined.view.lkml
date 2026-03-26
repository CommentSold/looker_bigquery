view: funnel_combined {
  filter: date_range {
    type: date
    description: "Filter events by date range. Use 'is in range' in the UI to pick start and end. Optional."
  }

  derived_table: {
    sql: WITH base_acquisition AS (
        SELECT
          DATE(st.created_at) AS event_date,
          'Acquisition' AS stage,
          CASE
            WHEN oe.user_id IS NULL THEN 'event_not_fired'
            WHEN oe.context_campaign_campaign IS NOT NULL THEN 'marketing_campaign'
            ELSE 'organic_walk-in'
          END AS acquisition_source,
          COUNT(DISTINCT st.store_id) AS value
        FROM `popshoplive-26f81.dbt_popshop.dim_profiles` prof
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_stores` st
          ON st.store_id = prof.user_id
        LEFT JOIN `popshoplive-26f81.popstore.popstore_onboarding_screen_action` oe
          ON oe.user_id = prof.user_id
        WHERE
          user_type IN ('seller', 'verifiedSeller')
          AND apps_pop_store = TRUE
          AND {% condition date_range %} st.created_at {% endcondition %}
        GROUP BY 1,2,3
      ),

      base_trials_started AS (
        SELECT
          DATE(t1.current_period_start) AS event_date,
          'Trials Started' AS stage,
          CASE
            WHEN oe.user_id IS NULL THEN 'event_not_fired'
            WHEN oe.context_campaign_campaign IS NOT NULL THEN 'marketing_campaign'
            ELSE 'organic_walk-in'
          END AS acquisition_source,
          COUNT(DISTINCT t1.id) AS value
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1
        LEFT JOIN `popshoplive-26f81.popstore.popstore_onboarding_screen_action` oe
          ON oe.user_id = t1.user_id
        WHERE
          t1.trial_end IS NOT NULL
          AND {% condition date_range %} t1.current_period_start {% endcondition %}
        GROUP BY 1,2,3
      ),

      base_trials_ended AS (
        SELECT
          DATE(t1.trial_end) AS event_date,
          'Trials Ended' AS stage,
          CASE
            WHEN oe.user_id IS NULL THEN 'event_not_fired'
            WHEN oe.context_campaign_campaign IS NOT NULL THEN 'marketing_campaign'
            ELSE 'organic_walk-in'
          END AS acquisition_source,
          COUNT(DISTINCT t1.id) AS value
        FROM `popshoplive-26f81.dbt_popshop.fact_seller_subscription` t1
        LEFT JOIN `popshoplive-26f81.popstore.popstore_onboarding_screen_action` oe
          ON oe.user_id = t1.user_id
        WHERE
          t1.trial_end IS NOT NULL
          AND {% condition date_range %} t1.trial_end {% endcondition %}
        GROUP BY 1,2,3
      )

      SELECT * FROM base_acquisition
      UNION ALL
      SELECT * FROM base_trials_started
      UNION ALL
      SELECT * FROM base_trials_ended
      ;;
  }

  # =========================
  # ⏱️ Date Dimension
  # =========================
  dimension_group: event_date {
    type: time
    timeframes: [date, week, month, quarter, year]
    sql: ${TABLE}.event_date ;;
    convert_tz: no
  }

  # =========================
  # 🧭 Core Dimensions
  # =========================
  dimension: stage {
    type: string
    sql: ${TABLE}.stage ;;
    description: "Funnel stage (Acquisition, Trials Started, Trials Ended)"
  }

  dimension: acquisition_source {
    type: string
    sql: ${TABLE}.acquisition_source ;;
  }

  # Optional ordering helper
  dimension: stage_order {
    type: number
    hidden: yes
    sql:
      CASE
        WHEN ${stage} = 'Acquisition' THEN 1
        WHEN ${stage} = 'Trials Started' THEN 2
        WHEN ${stage} = 'Trials Ended' THEN 3
        ELSE 99
      END ;;
  }

  # =========================
  # 📊 Measures
  # =========================
  measure: total_value {
    type: sum
    sql: ${TABLE}.value ;;
    label: "Total Count"
  }

  measure: acquisition_count {
    type: sum
    sql: CASE WHEN ${stage} = 'Acquisition' THEN ${TABLE}.value END ;;
  }

  measure: trials_started_count {
    type: sum
    sql: CASE WHEN ${stage} = 'Trials Started' THEN ${TABLE}.value END ;;
  }

  measure: trials_ended_count {
    type: sum
    sql: CASE WHEN ${stage} = 'Trials Ended' THEN ${TABLE}.value END ;;
  }

  # =========================
  # 📈 Conversion Metrics
  # =========================
  measure: trial_start_rate {
    type: number
    value_format_name: percent_2
    sql:
      SAFE_DIVIDE(
        SUM(CASE WHEN ${stage} = 'Trials Started' THEN ${TABLE}.value END),
        SUM(CASE WHEN ${stage} = 'Acquisition' THEN ${TABLE}.value END)
      ) ;;
  }

  measure: trial_end_rate {
    type: number
    value_format_name: percent_2
    sql:
      SAFE_DIVIDE(
        SUM(CASE WHEN ${stage} = 'Trials Ended' THEN ${TABLE}.value END),
        SUM(CASE WHEN ${stage} = 'Trials Started' THEN ${TABLE}.value END)
      ) ;;
  }

  # =========================
  # 🔍 Drill Set
  # =========================
  set: funnel_details {
    fields: [
      event_date_date,
      stage,
      acquisition_source,
      total_value
    ]
  }
}

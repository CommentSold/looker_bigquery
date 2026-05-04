view: qa_acquisition_funnel_summary {
  derived_table: {
    sql:
      WITH
      -- ============================================================
      -- 1. DENOMINATOR: Total Acquisition of Popstores
      --    Mirrors qa_onboarding_funnel base: sellers with pop store enabled.
      -- ============================================================
      acquisition AS (
        SELECT
          prof.user_id,
          prof.created_at AS acquired_at
        FROM `popshoplive-26f81.dbt_popshop.dim_profiles` prof
        LEFT JOIN `popshoplive-26f81.dbt_popshop.dim_private_profiles` pprof
          ON pprof.user_id = prof.user_id
        WHERE prof.user_type IN ('seller', 'verifiedSeller')
          AND prof.apps_pop_store = TRUE
          AND (pprof.email IS NULL OR (
            LOWER(pprof.email) NOT LIKE '%@test.com'
            AND LOWER(pprof.email) NOT LIKE '%@example.com'
            AND LOWER(pprof.email) NOT LIKE '%@popshoplive.com'
            AND LOWER(pprof.email) NOT LIKE '%@commentsold.com'
          ))
      ),

      -- ============================================================
      -- 2. TRIALS STARTED: distinct users with a 'plan'-type subscription
      --    that has an initial_start_date. Mirrors qa_trial_report base.
      -- ============================================================
      trials_started AS (
      SELECT DISTINCT fs.user_id
      FROM `dbt_popshop.fact_seller_subscription` fs,
      UNNEST(fs.plans) AS plan
      WHERE fs.initial_start_date IS NOT NULL
      AND fs.trial_end IS NOT NULL
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      ),

      -- ============================================================
      -- 3. TRIAL CANCELLATIONS: mirrors qa_trial_cancellations logic.
      --    Cancelled during trial OR unpaid with elapsed trial.
      -- ============================================================
      trial_cancellations AS (
      SELECT DISTINCT fs.user_id
      FROM `dbt_popshop.fact_seller_subscription` fs,
      UNNEST(fs.plans) AS plan
      WHERE fs.trial_end IS NOT NULL
      AND fs.status IN ('canceled', 'unpaid')
      AND (
      fs.cancelled_at <= fs.trial_end
      OR fs.current_period_end = fs.trial_end
      OR (fs.status = 'unpaid' AND fs.cancelled_at IS NULL AND fs.trial_end <= CURRENT_TIMESTAMP())
      )
      AND JSON_EXTRACT_SCALAR(plan, '$.planType') = 'plan'
      ),

      -- ============================================================
      -- 4. TRIAL CONVERSIONS: mirrors qa_trial_conversions paid logic.
      --    Users with first paid billable invoice after trial_end.
      -- ============================================================
      subs_for_conv AS (
      SELECT subscription_id, user_id, trial_end AS trial_end_ts
      FROM `dbt_popshop.fact_seller_subscription`
      WHERE is_deleted = FALSE
      ),
      invoice_rollup AS (
      SELECT
      invoice_id,
      subscription_id,
      MAX(amount_due) AS max_amount_due
      FROM `dbt_popshop.fact_seller_subscription_invoice`
      WHERE is_deleted = FALSE
      GROUP BY 1, 2
      ),
      billable_invoices AS (
      SELECT invoice_id, subscription_id
      FROM invoice_rollup
      WHERE max_amount_due > 0
      ),
      first_paid_per_invoice AS (
      SELECT
      ih.invoice_id,
      ih.subscription_id,
      ih.updated_at AS paid_at,
      ROW_NUMBER() OVER (PARTITION BY ih.invoice_id ORDER BY ih.updated_at ASC) AS rn
      FROM `dbt_popshop.fact_seller_subscription_invoice` ih
      JOIN billable_invoices bi
      ON ih.invoice_id = bi.invoice_id
      AND ih.subscription_id = bi.subscription_id
      WHERE ih.status = 'paid'
      AND ih.amount_paid > 0
      AND ih.is_deleted = FALSE
      ),
      trial_conversions AS (
      SELECT DISTINCT s.user_id
      FROM subs_for_conv s
      JOIN first_paid_per_invoice p
      ON s.subscription_id = p.subscription_id
      AND p.paid_at >= s.trial_end_ts
      WHERE p.rn = 1
      ),

      -- ============================================================
      -- 5. PDF GENERATIONS: any user with at least one PDF gen record.
      --    Two flavors so the chart can show "trial users who generated"
      --    vs "all users who generated".
      -- ============================================================
      pdf_users_all AS (
      SELECT DISTINCT user_id
      FROM `popshoplive-26f81.commentsold.ai_pdf_generations`
      ),
      pdf_users_trial AS (
      SELECT DISTINCT a.user_id
      FROM pdf_users_all a
      JOIN trials_started t ON t.user_id = a.user_id
      ),

      -- ============================================================
      -- 6. PER-USER FLAGS: one row per acquired user with bucket flags.
      --    Lets us count distinct users per bucket and intersect.
      -- ============================================================
      user_flags AS (
      SELECT
      a.user_id,
      a.acquired_at,
      IF(ts.user_id IS NOT NULL, 1, 0) AS is_trial_started,
      IF(tc.user_id IS NOT NULL, 1, 0) AS is_trial_cancelled,
      IF(cv.user_id IS NOT NULL, 1, 0) AS is_trial_converted,
      IF(pa.user_id IS NOT NULL, 1, 0) AS is_pdf_generator_all,
      IF(pt.user_id IS NOT NULL, 1, 0) AS is_pdf_generator_trial
      FROM acquisition a
      LEFT JOIN trials_started      ts ON ts.user_id = a.user_id
      LEFT JOIN trial_cancellations tc ON tc.user_id = a.user_id
      LEFT JOIN trial_conversions   cv ON cv.user_id = a.user_id
      LEFT JOIN pdf_users_all       pa ON pa.user_id = a.user_id
      LEFT JOIN pdf_users_trial     pt ON pt.user_id = a.user_id
      )

      SELECT * FROM user_flags
      ;;
  }

  # ——— Filters ———

  filter: date_range {
    type: date
    description: "Filter by acquisition (popstore created) date. Optional."
    sql: {% condition %} ${TABLE}.acquired_at {% endcondition %} ;;
  }

  # ——— Dimensions ———

  dimension: user_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.user_id ;;
  }

  dimension_group: acquired_at {
    type: time
    sql: ${TABLE}.acquired_at ;;
    timeframes: [date, week, month, quarter, year]
  }

  dimension: is_trial_started {
    type: number
    sql: ${TABLE}.is_trial_started ;;
    hidden: yes
  }

  dimension: is_trial_cancelled {
    type: number
    sql: ${TABLE}.is_trial_cancelled ;;
    hidden: yes
  }

  dimension: is_trial_converted {
    type: number
    sql: ${TABLE}.is_trial_converted ;;
    hidden: yes
  }

  dimension: is_pdf_generator_all {
    type: number
    sql: ${TABLE}.is_pdf_generator_all ;;
    hidden: yes
  }

  dimension: is_pdf_generator_trial {
    type: number
    sql: ${TABLE}.is_pdf_generator_trial ;;
    hidden: yes
  }

  # ——— Core Measures (counts) ———

  measure: total_acquisition {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    label: "Total Acquisition"
    description: "Distinct popstore sellers acquired (denominator for all funnel stages)"
  }

  measure: total_trials_started {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [is_trial_started: "1"]
    label: "Trials Started"
    description: "Distinct users who started a plan trial"
  }

  measure: total_trials_cancelled {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [is_trial_cancelled: "1"]
    label: "Trials Cancelled"
    description: "Distinct users whose trial was cancelled (canceled or unpaid with elapsed trial)"
  }

  measure: total_trials_converted {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [is_trial_converted: "1"]
    label: "Trials Converted (Paid)"
    description: "Distinct users who paid a billable invoice after trial end"
  }

  measure: total_pdf_generators_all {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [is_pdf_generator_all: "1"]
    label: "PDF Generators (All)"
    description: "Distinct acquired users who generated at least one AI PDF"
  }

  measure: total_pdf_generators_trial {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [is_pdf_generator_trial: "1"]
    label: "PDF Generators (Trial Users Only)"
    description: "Distinct trial users who generated at least one AI PDF"
  }

  # ——— Complement measures (for 2-slice pie charts: bucket vs rest) ———

  measure: rest_not_trial_started {
    type: number
    sql: ${total_acquisition} - ${total_trials_started} ;;
    label: "Other Acquisition (No Trial)"
  }

  measure: rest_not_trial_cancelled {
    type: number
    sql: ${total_acquisition} - ${total_trials_cancelled} ;;
    label: "Other Acquisition (Not Cancelled)"
  }

  measure: rest_not_trial_converted {
    type: number
    sql: ${total_acquisition} - ${total_trials_converted} ;;
    label: "Other Acquisition (Not Converted)"
  }

  measure: rest_not_pdf_generator_all {
    type: number
    sql: ${total_acquisition} - ${total_pdf_generators_all} ;;
    label: "Other Acquisition (No PDF)"
  }

  measure: rest_not_pdf_generator_trial {
    type: number
    sql: ${total_trials_started} - ${total_pdf_generators_trial} ;;
    label: "Trial Users (No PDF)"
    description: "Trial users who did NOT generate a PDF (denominator here is Trials Started, not Total Acquisition)"
  }

  # ——— Percentage measures ———

  measure: pct_trials_started {
    type: number
    sql: SAFE_DIVIDE(${total_trials_started}, NULLIF(${total_acquisition}, 0)) * 100 ;;
    label: "% Trials Started"
    value_format_name: decimal_1
  }

  measure: pct_trials_cancelled {
    type: number
    sql: SAFE_DIVIDE(${total_trials_cancelled}, NULLIF(${total_acquisition}, 0)) * 100 ;;
    label: "% Trials Cancelled (of Acquisition)"
    value_format_name: decimal_1
  }

  measure: pct_trials_converted {
    type: number
    sql: SAFE_DIVIDE(${total_trials_converted}, NULLIF(${total_acquisition}, 0)) * 100 ;;
    label: "% Trials Converted (of Acquisition)"
    value_format_name: decimal_1
  }

  measure: pct_pdf_generators_all {
    type: number
    sql: SAFE_DIVIDE(${total_pdf_generators_all}, NULLIF(${total_acquisition}, 0)) * 100 ;;
    label: "% PDF Generators (of Acquisition)"
    value_format_name: decimal_1
  }

  measure: pct_pdf_generators_trial {
    type: number
    sql: SAFE_DIVIDE(${total_pdf_generators_trial}, NULLIF(${total_trials_started}, 0)) * 100 ;;
    label: "% PDF Generators (of Trials Started)"
    value_format_name: decimal_1
  }

  # ——— Trial-cohort percentages (denominator = Trials Started) ———
  # Useful when you want "of users who started a trial, what % cancelled / converted"

  measure: pct_cancelled_of_trials {
    type: number
    sql: SAFE_DIVIDE(${total_trials_cancelled}, NULLIF(${total_trials_started}, 0)) * 100 ;;
    label: "% Cancelled (of Trials Started)"
    value_format_name: decimal_1
  }

  measure: pct_converted_of_trials {
    type: number
    sql: SAFE_DIVIDE(${total_trials_converted}, NULLIF(${total_trials_started}, 0)) * 100 ;;
    label: "% Converted (of Trials Started)"
    value_format_name: decimal_1
  }
}

connection: "bigquery"

include: "/views/business_dash/prod/*.view.lkml"
include: "/views/business_dash/qa/*.view.lkml"

include: "/views/marketing_dash/prod/*.view.lkml"
include: "/views/marketing_dash/qa/*.view.lkml"

include: "/views/qa/*.view.lkml"             # include all views in the views/qa/ folder in this project
# include: "/**/*.view.lkml"                 # include all views in this project

# Business Dash -> PROD #

explore: prod_active_paid_subscribers {
  label: "Active Paid Subscribers Prod"
  group_label: "Trial"
}
explore: prod_active_paid_subscribers_stripe {
  label: "Active Paid Subscribers Stripe Prod"
  group_label: "Subscriptions"
}
explore: prod_ai_pdf_generations {
  label: "AI PDF Generations Prod"
  group_label: "AI PDF"
}
explore: prod_onboarding_funnel {
  label: "Onboarding Funnel Prod"
  group_label: "Onboarding"
}
explore: prod_trial_cancellations {
  label: "Trial Cancellations Prod"
  group_label: "Trial"
}
explore: prod_paid_subscription_cancellations {
  label: "Paid Subscription Cancellations Prod"
  group_label: "Trial"
}
explore: prod_trial_conversions {
  label: "Trial Conversions Prod"
  group_label: "Trial"
}
explore: prod_agent_trial_conversions {
  label: "Agent Trial Conversions Prod"
  group_label: "Trial"
}
explore: prod_trial_report {
  label: "Trial Report Prod"
  group_label: "Trial"
}
explore: prod_agent_trial_report {
  label: "Agent Trial Report Prod"
  group_label: "Trial"
}
explore: prod_daily_subscribers_report {
  label: "Daily Subscribers Report"
  group_label: "Subscriptions"
}
explore: prod_daily_subscriber_churn {
  label: "Daily Subscriber Churn"
  group_label: "Subscriptions"
}
explore: echo_me_agent_status {
  label: "Echo Me — Agents (current state)"
  view_label: "Agent"
  description: "One row per user x agent. Use for agent counts, rankings, channel detail."
  join: echo_me_user_attrs {
    view_label: "User"
    type: left_outer
    relationship: many_to_one
    sql_on: ${echo_me_agent_status.user_id} = ${echo_me_user_attrs.user_id} ;;
  }
}
explore: echo_me_agent_monthly {
  label: "Echo Me — Agents Over Time"
  view_label: "Agent Month"
  description: "Month x user x agent snapshots. Use for every trend line."
  join: echo_me_user_attrs {
    view_label: "User"
    type: left_outer
    relationship: many_to_one
    sql_on: ${echo_me_agent_monthly.user_id} = ${echo_me_user_attrs.user_id} ;;
  }
}
explore: prod_ai_echo_me {
  label: "Echo Me — Trial Users"
  description: "One row per trial user. Use for user counts and per-user distributions."
  # Optional: lets you build a user-grain tile that filters on a specific
  # agent's state, e.g. "trial users whose CODA is active".
  join: echo_me_agent_status {
    view_label: "Agent"
    type: left_outer
    relationship: one_to_many
    sql_on: ${prod_ai_echo_me.user_id} = ${echo_me_agent_status.user_id} ;;
  }
}
datagroup: echo_me_default_datagroup {
  sql_trigger: SELECT MAX(updated_at) FROM `popshoplive-26f81.commentchat.echo_me_agents` ;;
  max_cache_age: "1 hour"
}
explore: prod_connection_link_email_activity {
  label: "Connection Link Email Activity Prod"
  group_label: "Email Activity"
}

# Business Dash -> QA #

explore: qa_active_paid_subscribers {
  label: "Active Paid Subscribers QA"
  group_label: "Trial"
}
explore: qa_ai_pdf_generations {
  label: "AI PDF Generations QA"
  group_label: "AI PDF"
}
explore: qa_onboarding_funnel {
  label: "Onboarding Funnel QA"
  group_label: "Onboarding"
}
explore: qa_trial_cancellations {
  label: "Trial Cancellations QA"
  group_label: "Trial"
}
explore: qa_trial_conversion_rate {
  label: "Trial Conversion Rate QA"
  group_label: "Trial"
}
explore: qa_trial_conversions {
  label: "Trial Conversions QA"
  group_label: "Trial"
}
explore: qa_agent_trial_conversions {
  label: "Agent Trial Conversions QA"
  group_label: "Trial"
}
explore: qa_trial_report {
  label: "Trial Report QA"
  group_label: "Trial"
}
explore: qa_agent_trial_report {
  label: "Agent Trial Report QA"
  group_label: "Trial"
}
explore: daily_new_trials {
  label: "Cumulative New Trials"
  group_label: "Trial"
}
explore: qa_daily_subscriber_cancellations {
  label: "Daily Subscriber Cancellations QA"
  group_label: "Subscriptions"
}
explore: qa_ai_echo_me {
  label: "AI Echo Me QA"
  group_label: "AI Echo Me"
}
explore: qa_acquisition_funnel_summary {
  label: "Acquisition Funnel Summary"
  group_label: "Onboarding"
}
explore: qa_connection_link_email_activity {
  label: "Connection Link Email Activity QA"
  group_label: "Email Activity"
}

# Marketing Dash -> PROD #

explore: prod_cumulative_creator_signups {
  label: "Cumulative Creator Signups Prod"
  group_label: "Subscriptions"
}
explore: prod_new_paid_subscribers_by_plan {
  label: "New Paid Subscribers by Plan"
  group_label: "Subscriptions"
}
explore: prod_monthly_paid_subscribers {
  label: "Monthly Paid Subscribers Prod"
  group_label: "Subscriptions"
}
explore: prod_trial_subscription_summary {
  label: "Trial Subscription Summary Prod"
  group_label: "Subscriptions"
}
explore: prod_monthly_new_trials_started {
  label: "Monthly New Trials Started Prod"
  group_label: "Trial"
}
explore: prod_signup_conversion_funnel {
  label: "Signup Conversion Funnel Prod"
  group_label: "Signup"
}
explore: prod_current_saas_subscriptions {
  label: "Current SAAS Subscriptions Prod"
  group_label: "Subscriptions"
}
explore: prod_subscription_churn {
  label: "Subscription Churn Prod"
  group_label: "Subscriptions"
}
explore: prod_subscription_cohort_retention {
  label: "Subscription Cohort Retention Prod"
  group_label: "Subscriptions"
}
explore: prod_churn_gap_reconciliation {
  label: "Churn Gap Reconciliation"
  group_label: "Subscriptions"
  description: "Reconciles total subscription ends against paid post-trial churn."
}
explore: prod_signup_attribution_audit {
  label: "Signup Attribution Audit"
  group_label: "Signup"
}
explore: prod_subscription_price_points {
  label: "Subscription Price Points"
  group_label: "Subscriptions"
}
explore: prod_subscription_addons {
  label: "Subscription Addons"
  group_label: "Subscriptions"
}

# Marketing Dash -> QA #

explore: trial_subscription_summary {
  label: "Trial Subscription Summary"
  group_label: "Trial"
}
explore: cumulative_creator_signups {
  label: "Cumulative Creator Signups"
  group_label: "Subscriptions"
}
explore: monthly_paid_subscribers {
  label: "Monthly Paid Subscribers"
  group_label: "Subscriptions"
}
explore: qa_monthly_new_trials_started {
  label: "Monthly New Trials Started QA"
  group_label: "Trial"
}
explore: qa_signup_conversion_funnel {
  label: "Signup Conversion Funnel QA"
  group_label: "Signup"
}

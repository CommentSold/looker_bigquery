connection: "bigquery"

include: "/views/business_dash/prod/*.view.lkml"
include: "/views/business_dash/qa/*.view.lkml"

include: "/views/marketing_dash/prod/*.view.lkml"
# include: "/views/marketing_dash/qa/*.view.lkml"

include: "/views/qa/*.view.lkml"             # include all views in the views/qa/ folder in this project
# include: "/**/*.view.lkml"                 # include all views in this project

# Business Dash -> PROD #

explore: prod_onboarding_funnel {
  label: "Onboarding Funnel Prod"
  group_label: "Onboarding"
}
explore: prod_ai_pdf_generations {
  label: "AI PDF Generations Prod"
  group_label: "AI PDF"
}
explore: prod_trial_report {
  label: "Trial Report Prod"
  group_label: "Trial"
}
explore: prod_trial_cancellations {
  label: "Trial Cancellations Prod"
  group_label: "Trial"
}
explore: prod_trial_conversions {
  label: "Trial Conversions Prod"
  group_label: "Trial"
}
explore: prod_active_paid_subscribers {
  label: "Active Paid Subscribers Prod"
  group_label: "Trial"
}

# Business Dash -> QA #

explore: qa_onboarding_funnel {
  label: "Onboarding Funnel QA"
  group_label: "Onboarding"
}
explore: trial_report {
  label: "Trial Report"
  group_label: "Trial"
}
explore: trial_cancellations {
  label: "Trial Cancellations"
  group_label: "Trial"
}
explore: test_trial {
  label: "Test Trial"
  group_label: "Trial"
}
explore: daily_new_trials {
  label: "Cumulative New Trials"
  group_label: "Trial"
}
explore: trial_conversion_rate {
  label: "Trial Conversion Rate"
  group_label: "Trial"
}
explore: ai_pdf_generations {
  label: "AI PDF Generations"
  group_label: "AI PDF"
}
explore: qa_active_paid_subscribers {
  label: "Active Paid Subscribers QA"
  group_label: "Trial"
}

# Marketing Dash -> PROD #

explore: prod_cumulative_creator_signups {
  label: "Cumulative Creator Signups Prod"
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

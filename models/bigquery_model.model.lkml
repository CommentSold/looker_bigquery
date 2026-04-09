connection: "bigquery"

include: "/views/*.view.lkml"                # include all views in the views/ folder in this project
include: "/views/qa/*.view.lkml"             # include all views in the views/qa/ folder in this project
# include: "/**/*.view.lkml"                 # include all views in this project
# include: "my_dashboard.dashboard.lookml"   # include a LookML dashboard called my_dashboard

# PROD #

explore: onboarding_funnel {
  label: "Onboarding Funnel"
  group_label: "Onboarding"
}
explore: creator_contest_action {
  label: "Creator Contest Action"
  group_label: "Contest"
}
explore: new_trial_report {
  label: "New Trial Report"
  group_label: "Trial"
}
explore: trial_conversions {
  label: "Trial Conversions"
  group_label: "Trial"
}
explore: active_paid_subscribers {
  label: "Active Paid Subscribers"
  group_label: "Trial"
}

# QA #

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
explore: trial_subscription_summary {
  label: "Trial Subscription Summary"
  group_label: "Trial"
}
explore: ai_pdf_generations {
  label: "AI PDF Generations"
  group_label: "AI PDF"
}

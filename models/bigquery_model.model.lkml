connection: "bigquery"

include: "/views/*.view.lkml"                # include all views in the views/ folder in this project
# include: "/**/*.view.lkml"                 # include all views in this project
# include: "my_dashboard.dashboard.lookml"   # include a LookML dashboard called my_dashboard

# # Select the views that should be a part of this model,
# # and define the joins that connect them together.
#
explore: onboarding_steps_funnel {
  label: "Onboarding Steps Funnel"
  group_label: "Onboarding"
}
explore: onboarding_funnel {
  label: "Onboarding Funnel"
  group_label: "Onboarding"
}
explore: creator_contest_action {
  label: "Creator Contest Action"
  group_label: "Contest"
}
explore: trial_report {
  label: "Trial Report"
  group_label: "Trial"
  join: trial_detail {
    sql_on: DATE(${trial_report.report_month_at_month}) = DATE(${trial_detail.report_month_at_month}) ;;
    relationship: many_to_one
    type: left_outer
  }
}

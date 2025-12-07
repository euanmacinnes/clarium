use super::plan::ExplainPlan;

pub fn explain_text(plan: &ExplainPlan) -> String {
    let mut out = String::new();
    out.push_str("EXPLAIN (text)\n");
    out.push_str(&format!("stmt: {}\n", plan.stmt));
    for st in &plan.stages {
        out.push_str(&format!("- {}: {}\n", st.name, st.details));
    }
    out
}

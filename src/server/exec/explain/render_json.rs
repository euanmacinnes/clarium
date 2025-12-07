use super::plan::ExplainPlan;

pub fn explain_json(plan: &ExplainPlan) -> serde_json::Value {
    let stages: Vec<serde_json::Value> = plan.stages.iter().map(|s| {
        serde_json::json!({"name": s.name, "details": s.details})
    }).collect();
    serde_json::json!({
        "format": "json",
        "stmt": plan.stmt,
        "stages": stages,
    })
}

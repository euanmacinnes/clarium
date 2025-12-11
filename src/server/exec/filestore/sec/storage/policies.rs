use anyhow::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRecord {
    pub policy_id: String,
    pub role_id: String,
    pub actions: Vec<String>,
    pub resource_selector: String,
    pub predicate_json: Option<String>,
    pub effect: String,
    pub priority: i32,
}

/// Fetch policies for a set of role IDs. Empty input returns empty.
pub async fn list_policies_for_roles(store: &crate::storage::SharedStore, role_ids: &[String]) -> Result<Vec<PolicyRecord>> {
    if role_ids.is_empty() { return Ok(Vec::new()); }
    // Build simple IN (...) clause; inputs already come from trusted storage.
    let mut inlist = String::new();
    for (i, r) in role_ids.iter().enumerate() {
        if i > 0 { inlist.push(','); }
        inlist.push('\'');
        inlist.push_str(&r.replace("'", "''"));
        inlist.push('\'');
    }
    let sql = format!(
        "SELECT policy_id, role_id, actions, resource_selector, predicate_json, effect, priority FROM security.policies WHERE role_id IN ({}) ORDER BY priority DESC",
        inlist
    );
    let val = crate::server::exec::execute_query_safe(store, &sql).await?;
    let mut out = Vec::new();
    if let serde_json::Value::Array(rows) = val {
        for r in rows {
            if let serde_json::Value::Object(m) = r {
                let policy_id = m.get("policy_id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                if policy_id.is_empty() { continue; }
                let role_id = m.get("role_id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                let actions_str = m.get("actions").and_then(|v| v.as_str()).unwrap_or("");
                let actions = actions_str.split(',').map(|s| s.trim().to_ascii_lowercase()).filter(|s| !s.is_empty()).collect();
                let resource_selector = m.get("resource_selector").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                let predicate_json = m.get("predicate_json").and_then(|v| v.as_str()).map(|s| s.to_string());
                let effect = m.get("effect").and_then(|v| v.as_str()).unwrap_or("deny").to_ascii_lowercase();
                let priority = m.get("priority").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                out.push(PolicyRecord { policy_id, role_id, actions, resource_selector, predicate_json, effect, priority });
            }
        }
    }
    Ok(out)
}

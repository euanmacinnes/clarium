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

/// Create a policy
pub async fn create_policy(
    store: &crate::storage::SharedStore,
    policy_id: &str,
    role_id: &str,
    actions_csv: &str,
    resource_selector: &str,
    predicate_json: Option<&str>,
    effect: &str,
    priority: i32,
) -> Result<()> {
    let now = chrono::Utc::now().timestamp_millis();
    let pred_sql = predicate_json
        .map(|s| format!("'{}'", s.replace("'", "''")))
        .unwrap_or_else(|| "NULL".into());
    let sql = format!(
        "INSERT INTO security.policies (policy_id, role_id, actions, resource_selector, predicate_json, effect, priority, created_at, updated_at) VALUES ('{}','{}','{}','{}',{},'{}',{}, {}, {})",
        policy_id.replace("'", "''"),
        role_id.replace("'", "''"),
        actions_csv.replace("'", "''"),
        resource_selector.replace("'", "''"),
        pred_sql,
        effect.replace("'", "''").to_ascii_lowercase(),
        priority,
        now,
        now
    );
    let _ = crate::server::exec::execute_query_safe(store, &sql).await?;
    super::epochs::bump_epoch_global(store).await.ok();
    Ok(())
}

/// Update a policy (mutable fields only)
pub async fn update_policy(
    store: &crate::storage::SharedStore,
    policy_id: &str,
    role_id: Option<&str>,
    actions_csv: Option<&str>,
    resource_selector: Option<&str>,
    predicate_json: Option<Option<&str>>,
    effect: Option<&str>,
    priority: Option<i32>,
) -> Result<bool> {
    let now = chrono::Utc::now().timestamp_millis();
    let mut sets: Vec<String> = Vec::new();
    if let Some(v) = role_id { sets.push(format!("role_id='{}'", v.replace("'", "''"))); }
    if let Some(v) = actions_csv { sets.push(format!("actions='{}'", v.replace("'", "''"))); }
    if let Some(v) = resource_selector { sets.push(format!("resource_selector='{}'", v.replace("'", "''"))); }
    if let Some(v) = predicate_json {
        match v {
            Some(s) => sets.push(format!("predicate_json='{}'", s.replace("'", "''"))),
            None => sets.push("predicate_json=NULL".into()),
        }
    }
    if let Some(v) = effect { sets.push(format!("effect='{}'", v.replace("'", "''").to_ascii_lowercase())); }
    if let Some(v) = priority { sets.push(format!("priority={}", v)); }
    sets.push(format!("updated_at={}", now));
    if sets.is_empty() { return Ok(false); }
    let sql = format!(
        "UPDATE security.policies SET {} WHERE policy_id='{}'",
        sets.join(", "),
        policy_id.replace("'", "''")
    );
    let _ = crate::server::exec::execute_query_safe(store, &sql).await?;
    super::epochs::bump_epoch_global(store).await.ok();
    Ok(true)
}

/// Delete a policy
pub async fn delete_policy(store: &crate::storage::SharedStore, policy_id: &str) -> Result<()> {
    let sql = format!(
        "DELETE FROM security.policies WHERE policy_id='{}'",
        policy_id.replace("'", "''")
    );
    let _ = crate::server::exec::execute_query_safe(store, &sql).await?;
    super::epochs::bump_epoch_global(store).await.ok();
    Ok(())
}

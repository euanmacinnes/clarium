use anyhow::Result;
use chrono::Utc;

/// Return role IDs granted to the given user, considering validity windows.
pub async fn list_roles_for_user(store: &crate::storage::SharedStore, user_id: &str) -> Result<Vec<String>> {
    let now = Utc::now().timestamp_millis();
    let sql = format!(
        "SELECT role_id FROM security.role_memberships WHERE user_id = '{}' AND (valid_from IS NULL OR valid_from <= {}) AND (valid_to IS NULL OR valid_to >= {})",
        user_id.replace("'", "''"), now, now
    );
    let val = crate::server::exec::execute_query_safe(store, &sql).await?;
    let mut out = Vec::new();
    if let serde_json::Value::Array(rows) = val {
        for r in rows {
            if let serde_json::Value::Object(m) = r {
                if let Some(rid) = m.get("role_id").and_then(|v| v.as_str()) {
                    out.push(rid.to_string());
                }
            }
        }
    }
    Ok(out)
}

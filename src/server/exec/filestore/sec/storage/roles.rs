use anyhow::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleRecord {
    pub role_id: String,
    pub description: Option<String>,
}

/// List all roles from security.roles
pub async fn list_roles(store: &crate::storage::SharedStore) -> Result<Vec<RoleRecord>> {
    let sql = "SELECT role_id, description FROM security.roles";
    let val = crate::server::exec::execute_query_safe(store, sql).await?;
    let mut out = Vec::new();
    if let serde_json::Value::Array(rows) = val {
        for r in rows {
            if let serde_json::Value::Object(m) = r {
                let role_id = m.get("role_id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                let description = m.get("description").and_then(|v| v.as_str()).map(|s| s.to_string());
                if !role_id.is_empty() { out.push(RoleRecord { role_id, description }); }
            }
        }
    }
    Ok(out)
}

/// Get a single role by id
pub async fn get_role(store: &crate::storage::SharedStore, role_id: &str) -> Result<Option<RoleRecord>> {
    let sql = format!("SELECT role_id, description FROM security.roles WHERE role_id = '{}'", role_id.replace("'", "''"));
    let val = crate::server::exec::execute_query_safe(store, &sql).await?;
    if let serde_json::Value::Array(mut rows) = val {
        if let Some(serde_json::Value::Object(m)) = rows.into_iter().next() {
            let rid = m.get("role_id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let description = m.get("description").and_then(|v| v.as_str()).map(|s| s.to_string());
            if !rid.is_empty() { return Ok(Some(RoleRecord { role_id: rid, description })); }
        }
    }
    Ok(None)
}

/// Create a new role (idempotent if already exists)
pub async fn create_role(store: &crate::storage::SharedStore, role_id: &str, description: Option<&str>) -> Result<()> {
    let now = chrono::Utc::now().timestamp_millis();
    // Try insert; if fails (exists), do nothing
    let desc_sql = description.map(|s| format!("'{}'", s.replace("'", "''"))).unwrap_or_else(|| "NULL".into());
    let sql = format!(
        "INSERT INTO security.roles (role_id, description, created_at, updated_at) VALUES ('{}', {}, {}, {})",
        role_id.replace("'", "''"), desc_sql, now, now
    );
    let _ = crate::server::exec::execute_query_safe(store, &sql).await?;
    super::epochs::bump_epoch_global(store).await.ok();
    Ok(())
}

/// Update role description
pub async fn update_role(store: &crate::storage::SharedStore, role_id: &str, description: Option<&str>) -> Result<bool> {
    let now = chrono::Utc::now().timestamp_millis();
    let desc_sql = description.map(|s| format!("'{}'", s.replace("'", "''"))).unwrap_or_else(|| "NULL".into());
    let sql = format!(
        "UPDATE security.roles SET description = {}, updated_at = {} WHERE role_id = '{}'",
        desc_sql, now, role_id.replace("'", "''")
    );
    let _ = crate::server::exec::execute_query_safe(store, &sql).await?;
    super::epochs::bump_epoch_global(store).await.ok();
    // Our engine doesn't return affected rows; return true optimistically
    Ok(true)
}

/// Delete role
pub async fn delete_role(store: &crate::storage::SharedStore, role_id: &str) -> Result<()> {
    // Remove memberships first to avoid orphans
    let rm = format!("DELETE FROM security.role_memberships WHERE role_id = '{}'", role_id.replace("'", "''"));
    let _ = crate::server::exec::execute_query_safe(store, &rm).await?;
    let sql = format!("DELETE FROM security.roles WHERE role_id = '{}'", role_id.replace("'", "''"));
    let _ = crate::server::exec::execute_query_safe(store, &sql).await?;
    super::epochs::bump_epoch_global(store).await.ok();
    Ok(())
}

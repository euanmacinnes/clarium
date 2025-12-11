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

/// Grant a role to a user with optional validity window (epoch ms).
pub async fn grant_role(
    store: &crate::storage::SharedStore,
    user_id: &str,
    role_id: &str,
    valid_from_ms: Option<i64>,
    valid_to_ms: Option<i64>,
) -> Result<()> {
    let now = Utc::now().timestamp_millis();
    let vf = valid_from_ms.map(|v| v.to_string()).unwrap_or_else(|| "NULL".into());
    let vt = valid_to_ms.map(|v| v.to_string()).unwrap_or_else(|| "NULL".into());
    let sql = format!(
        "INSERT INTO security.role_memberships (user_id, role_id, valid_from, valid_to, created_at, updated_at) VALUES ('{}','{}', {}, {}, {}, {})",
        user_id.replace("'", "''"),
        role_id.replace("'", "''"),
        vf,
        vt,
        now,
        now
    );
    let _ = crate::server::exec::execute_query_safe(store, &sql).await?;
    // Bump global epoch for policy caches
    super::epochs::bump_epoch_global(store).await.ok();
    Ok(())
}

/// Revoke a role from a user
pub async fn revoke_role(
    store: &crate::storage::SharedStore,
    user_id: &str,
    role_id: &str,
) -> Result<()> {
    let sql = format!(
        "DELETE FROM security.role_memberships WHERE user_id='{}' AND role_id='{}'",
        user_id.replace("'", "''"),
        role_id.replace("'", "''")
    );
    let _ = crate::server::exec::execute_query_safe(store, &sql).await?;
    super::epochs::bump_epoch_global(store).await.ok();
    Ok(())
}

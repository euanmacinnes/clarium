use crate::security;
use crate::storage::SharedStore;

/// Map CommandKind to a coarse database-scoped privilege label used in security.grants
fn required_db_priv_for(cmd: security::CommandKind) -> Option<&'static [&'static str]> {
    match cmd {
        security::CommandKind::Select => Some(&["DB READ", "DB WRITE", "DB OWNER"]),
        security::CommandKind::Insert => Some(&["DB WRITE", "DB OWNER"]),
        security::CommandKind::Calculate => Some(&["DB READ", "DB WRITE", "DB OWNER"]),
        security::CommandKind::DeleteRows | security::CommandKind::DeleteColumns => Some(&["DB WRITE", "DB OWNER"]),
        security::CommandKind::Schema | security::CommandKind::Database | security::CommandKind::Other => None,
    }
}

/// RBAC gate using SQL catalogs under security.*. Async because it queries the store via SQL engine.
pub async fn check_command_allowed_async(
    store: &SharedStore,
    username: &str,
    cmd: security::CommandKind,
    db: Option<&str>,
) -> bool {
    // Admin shortcut: membership in role 'admin'
    let q_admin = format!(
        "SELECT COUNT(1) AS c FROM security.role_memberships WHERE LOWER(user_id)=LOWER('{}') AND LOWER(role_id)='admin'",
        username.replace("'", "''")
    );
    if let Ok(val) = crate::server::exec::execute_query_safe(store, &q_admin).await {
        let is_admin = val.get("results").and_then(|r| r.get(0)).and_then(|row| row.get("c")).and_then(|v| v.as_i64()).unwrap_or(0) > 0;
        if is_admin { return true; }
    }
    // Database & DDL ops require admin
    match cmd {
        security::CommandKind::Schema | security::CommandKind::Database => { return false; }
        _ => {}
    }
    // If no db provided, deny non-admin operations by default
    let Some(db_name) = db else { return false; };

    // Collect user roles
    let q_roles = format!(
        "SELECT role_id FROM security.role_memberships WHERE LOWER(user_id)=LOWER('{}')",
        username.replace("'", "''")
    );
    let mut roles: Vec<String> = Vec::new();
    if let Ok(val) = crate::server::exec::execute_query_safe(store, &q_roles).await {
        if let Some(arr) = val.get("results").and_then(|v| v.as_array()) {
            for row in arr.iter() {
                if let Some(r) = row.get("role_id").and_then(|v| v.as_str()) { roles.push(r.to_string()); }
            }
        }
    }
    if roles.is_empty() { return false; }

    // Map command to required privileges
    let Some(req_privs) = required_db_priv_for(cmd) else { return false; };
    // Check grants at DATABASE scope for any of the user's roles
    // Build IN list safely by escaping single quotes
    let role_list = roles
        .iter()
        .map(|r| format!("'{}'", r.replace("'", "''")))
        .collect::<Vec<_>>()
        .join(",");
    // Compare case-insensitively: lower both column and constants
    let priv_list = req_privs
        .iter()
        .map(|p| format!("LOWER('{}')", p.to_lowercase()))
        .collect::<Vec<_>>()
        .join(",");
    let q = format!(
        "SELECT COUNT(1) AS c FROM security.grants \
         WHERE scope_kind='DATABASE' AND LOWER(db_name)=LOWER('{db}') \
           AND LOWER(role_id) IN ({roles}) AND LOWER(privilege) IN ({privs})",
        db = db_name.replace("'", "''"), roles = role_list, privs = priv_list
    );
    if let Ok(val) = crate::server::exec::execute_query_safe(store, &q).await {
        let c = val.get("results").and_then(|r| r.get(0)).and_then(|row| row.get("c")).and_then(|v| v.as_i64()).unwrap_or(0);
        return c > 0;
    }
    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    User,
    Admin,
    // Tenancy roles (placeholders for future enforcement; used in debug builds only for now)
    TenancyAdmin,
    TenancyDDL,
    TenancyRead,
    // Database-scoped roles inferred from legacy perms
    DbReader,
    DbWriter,
    DbDeleter,
    Compute,
}

/// Derive roles for a user by consulting the existing parquet-backed authorizer.
/// This keeps storage unchanged while enabling a richer RBAC surface in debug builds.
pub fn roles_for_user(db_root: &str, username: &str, db: Option<&str>) -> Vec<Role> {
    let mut roles = vec![Role::User];
    // Admin: any DDL/database privilege implies admin under the legacy model
    if security::authorize(db_root, username, security::CommandKind::Schema, None).unwrap_or(false) {
        roles.push(Role::Admin);
    }
    // Database-scoped roles (best-effort from legacy permissions)
    if security::authorize(db_root, username, security::CommandKind::Select, db).unwrap_or(false) {
        roles.push(Role::DbReader);
    }
    if security::authorize(db_root, username, security::CommandKind::Insert, db).unwrap_or(false) {
        roles.push(Role::DbWriter);
    }
    if security::authorize(db_root, username, security::CommandKind::Calculate, db).unwrap_or(false) {
        roles.push(Role::Compute);
    }
    if security::authorize(db_root, username, security::CommandKind::DeleteRows, db).unwrap_or(false) {
        roles.push(Role::DbDeleter);
    }

    // Tenancy roles: map to admin for now until tenancy control-plane is implemented
    // This allows debug builds to experiment with IAM without changing release behavior.
    // Future: resolve tenant-scoped ACLs here.
    if roles.contains(&Role::Admin) {
        roles.push(Role::TenancyAdmin);
        roles.push(Role::TenancyDDL);
        roles.push(Role::TenancyRead);
    }
    roles
}

/// Check whether a command is allowed for the given user under the enhanced RBAC model.
/// Internally derives roles from the legacy authorizer to preserve existing semantics.
pub fn check_command_allowed(db_root: &str, username: &str, cmd: security::CommandKind, db: Option<&str>) -> bool {
    let roles = roles_for_user(db_root, username, db);
    let is_admin = roles.contains(&Role::Admin);
    if is_admin {
        return true;
    }
    match cmd {
        security::CommandKind::Select => roles.contains(&Role::DbReader),
        security::CommandKind::Insert => roles.contains(&Role::DbWriter),
        security::CommandKind::Calculate => roles.contains(&Role::Compute),
        security::CommandKind::DeleteRows | security::CommandKind::DeleteColumns => roles.contains(&Role::DbDeleter),
        // DDL/database operations reserved to admin under legacy model
        security::CommandKind::Schema | security::CommandKind::Database => false,
        // Other commands: deny by default unless admin
        security::CommandKind::Other => false,
    }
}

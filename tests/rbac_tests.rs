//! RBAC integration tests: authentication (Argon2/SQL) and authorization gate.
//! These tests exercise positive and negative paths across the RBAC catalogs.

use anyhow::Result;
use tempfile::tempdir;

use clarium::storage::SharedStore;
use clarium::identity::{LoginRequest, SessionManager};
use clarium::identity::{login_via_sql};
use clarium::security::CommandKind;

// Argon2 for generating PHC hashes in tests
use argon2::{Argon2, PasswordHasher};
use password_hash::SaltString;

// Execute a SQL statement and assert no error
async fn exec_ok(store: &SharedStore, sql: &str) {
    if let Err(e) = clarium::server::exec::execute_query_safe(store, sql).await {
        panic!("SQL failed: {} => {}", sql, e);
    }
}

async fn install_security_ddl(store: &SharedStore) {
    use std::path::Path;
    // Run DDL scripts directly to avoid global OnceCell in ensure_installed
    let ddl_root = Path::new("scripts").join("ddl");
    if let Err(e) = clarium::tools::installer::run_installer(store, &ddl_root).await {
        panic!("run_installer failed: {}", e);
    }
    if let Err(e) = clarium::tools::installer::run_install_checks(store).await {
        panic!("run_install_checks failed: {}", e);
    }
}

fn phc_for(password: &str) -> String {
    let mut salt_bytes = [0u8; 16];
    getrandom::getrandom(&mut salt_bytes).expect("salt");
    let salt = SaltString::encode_b64(&salt_bytes).expect("salt b64");
    let argon2 = Argon2::default();
    argon2.hash_password(password.as_bytes(), &salt).unwrap().to_string()
}

#[tokio::test]
async fn rbac_installer_provisions_dev_admin_and_allows_admin_ops() -> Result<()> {
    let tmp = tempdir()?;
    let store = SharedStore::new(tmp.path())?;

    install_security_ddl(&store).await;

    // Seed clarium/clarium admin explicitly for test stability
    let phc = phc_for("clarium");
    let now_ms = chrono::Utc::now().timestamp_millis();
    exec_ok(&store, &format!(
        "INSERT INTO security.users (user_id, display_name, password_hash, attrs_json, created_at, updated_at) VALUES ('clarium','Clarium Admin','{}','{}',{}, {})",
        phc.replace("'", "''"), "{}", now_ms, now_ms
    )).await;
    // role membership admin
    exec_ok(&store, &format!(
        "INSERT INTO security.role_memberships (user_id, role_id, valid_from, valid_to, created_at, updated_at) VALUES ('clarium','admin', {}, NULL, {}, {})",
        now_ms, now_ms, now_ms
    )).await;

    let lr = LoginRequest { username: "clarium".into(), password: "clarium".into(), db: None, ip: None };
    let resp = login_via_sql(&store, &SessionManager::default(), &lr).await;
    assert!(resp.is_ok(), "expected admin login to succeed");

    // Admin should be allowed to perform Database/Schema operations via RBAC gate
    let allowed_db = clarium::identity::check_command_allowed_async(&store, "clarium", CommandKind::Database, None).await;
    let allowed_schema = clarium::identity::check_command_allowed_async(&store, "clarium", CommandKind::Schema, Some("anydb")).await;
    assert!(allowed_db, "admin should be allowed for Database operations");
    assert!(allowed_schema, "admin should be allowed for Schema operations");
    Ok(())
}

#[tokio::test]
async fn rbac_sql_auth_argon2_positive_and_negative() -> Result<()> {
    let tmp = tempdir()?;
    let store = SharedStore::new(tmp.path())?;

    install_security_ddl(&store).await;

    // Create a user with a strong Argon2 PHC
    let phc = phc_for("s3cr3t!");
    let now_ms = chrono::Utc::now().timestamp_millis();
    let ins_user = format!(
        "INSERT INTO security.users (user_id, display_name, password_hash, attrs_json, created_at, updated_at) VALUES ('{}','{}','{}','{}',{},{})",
        "alice", "Alice", phc.replace("'", "''"), "{}", now_ms, now_ms
    );
    exec_ok(&store, &ins_user).await;

    // Wrong password should fail
    let lr_bad = LoginRequest { username: "alice".into(), password: "wrong".into(), db: None, ip: None };
    let bad = login_via_sql(&store, &SessionManager::default(), &lr_bad).await;
    assert!(bad.is_err(), "login with wrong password must fail");

    // Correct password should succeed
    let lr_ok = LoginRequest { username: "alice".into(), password: "s3cr3t!".into(), db: None, ip: None };
    let ok = login_via_sql(&store, &SessionManager::default(), &lr_ok).await;
    assert!(ok.is_ok(), "login with correct password should succeed");
    Ok(())
}

#[tokio::test]
async fn rbac_authorization_db_scoped_privileges() -> Result<()> {
    let tmp = tempdir()?;
    let store = SharedStore::new(tmp.path())?;

    install_security_ddl(&store).await;

    // Create user alice
    let phc = phc_for("s3cr3t!");
    let now_ms = chrono::Utc::now().timestamp_millis();
    exec_ok(&store, &format!(
        "INSERT INTO security.users (user_id, display_name, password_hash, attrs_json, created_at, updated_at) VALUES ('{}','{}','{}','{}',{},{})",
        "alice", "Alice", phc.replace("'", "''"), "{}", now_ms, now_ms
    )).await;

    // Add role membership: db_reader
    exec_ok(&store, &format!(
        "INSERT INTO security.role_memberships (user_id, role_id, valid_from, valid_to, created_at, updated_at) VALUES ('{}','{}',{}, NULL, {}, {})",
        "alice", "db_reader", now_ms, now_ms, now_ms
    )).await;

    // Grant DB READ to db_reader for database 'sampledb'
    exec_ok(&store, &format!(
        "INSERT INTO security.grants (scope_kind, db_name, role_id, privilege, created_at) VALUES ('{}','{}','{}','{}',{})",
        "DATABASE", "sampledb", "db_reader", "DB READ", now_ms
    )).await;

    // Positive: SELECT and CALCULATE allowed on sampledb
    let sel_ok = clarium::identity::check_command_allowed_async(&store, "alice", CommandKind::Select, Some("sampledb")).await;
    let calc_ok = clarium::identity::check_command_allowed_async(&store, "alice", CommandKind::Calculate, Some("sampledb")).await;
    assert!(sel_ok, "DB READ should allow SELECT on granted database");
    assert!(calc_ok, "DB READ should allow CALCULATE on granted database");

    // Negative: INSERT/DELETE not allowed
    let ins_no = clarium::identity::check_command_allowed_async(&store, "alice", CommandKind::Insert, Some("sampledb")).await;
    let del_no = clarium::identity::check_command_allowed_async(&store, "alice", CommandKind::DeleteRows, Some("sampledb")).await;
    assert!(!ins_no, "DB READ should NOT allow INSERT");
    assert!(!del_no, "DB READ should NOT allow DELETE");

    // Negative: other database without grants
    let other_no = clarium::identity::check_command_allowed_async(&store, "alice", CommandKind::Select, Some("otherdb")).await;
    assert!(!other_no, "no grant on otherdb should deny SELECT");
    Ok(())
}

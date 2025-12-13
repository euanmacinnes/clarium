//! RBAC object-scope privilege label seeding tests for GRAPH, FILESTORE, VECTOR
use anyhow::Result;
use tempfile::tempdir;

use clarium::storage::SharedStore;

// Execute a SQL statement and return JSON value result or error
async fn exec_sql(store: &SharedStore, sql: &str) -> anyhow::Result<serde_json::Value> {
    clarium::server::exec::execute_query_safe(store, sql).await
}

async fn install_security_ddl(store: &SharedStore) {
    use std::path::Path;
    let ddl_root = Path::new("scripts").join("ddl");
    if let Err(e) = clarium::tools::installer::run_installer(store, &ddl_root).await {
        panic!("run_installer failed: {}", e);
    }
    if let Err(e) = clarium::tools::installer::run_install_checks(store).await {
        panic!("run_install_checks failed: {}", e);
    }
}

#[tokio::test]
async fn rbac_seeds_graph_filestore_vector_labels_and_is_idempotent() -> Result<()> {
    let tmp = tempdir()?;
    let store = SharedStore::new(tmp.path())?;

    install_security_ddl(&store).await;

    // Expected labels grouped by class
    let labels = vec![
        // GRAPH
        "GRAPH READ", "GRAPH WRITE", "GRAPH ALTER", "GRAPH DROP",
        // FILESTORE
        "FILE READ", "FILE WRITE", "FILE ALTER", "FILE DELETE",
        // VECTOR
        "VECTOR READ", "VECTOR WRITE", "VECTOR ALTER", "VECTOR DROP",
    ];

    // Verify each label exists as a GLOBAL grant to admin exactly once
    for p in &labels {
        let q = format!(
            "SELECT COUNT(1) AS c FROM security.grants WHERE scope_kind='GLOBAL' AND LOWER(privilege)=LOWER('{}') AND LOWER(role_id)='admin'",
            p.replace("'", "''")
        );
        let val = exec_sql(&store, &q).await?;
        let c = val
            .get("results")
            .and_then(|r| r.get(0))
            .and_then(|row| row.get("c"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        assert_eq!(c, 1, "expected exactly one GLOBAL admin grant for {}", p);
    }

    // Re-run installer to check idempotency
    install_security_ddl(&store).await;
    for p in &labels {
        let q = format!(
            "SELECT COUNT(1) AS c FROM security.grants WHERE scope_kind='GLOBAL' AND LOWER(privilege)=LOWER('{p}') AND LOWER(role_id)='admin'",
            p = p.replace("'", "''")
        );
        let val = exec_sql(&store, &q).await?;
        let c = val
            .get("results")
            .and_then(|r| r.get(0))
            .and_then(|row| row.get("c"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        assert_eq!(c, 1, "installer should be idempotent for {}", p);
    }

    Ok(())
}

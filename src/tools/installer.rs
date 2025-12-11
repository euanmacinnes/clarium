use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};
use chrono::Utc;
use xxhash_rust::xxh3::xxh3_128;

use crate::tprintln;
use crate::storage::SharedStore;

fn collect_sql_files_recursive(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    fn walk(acc: &mut Vec<PathBuf>, p: &Path) {
        if let Ok(rd) = fs::read_dir(p) {
            let mut entries: Vec<_> = rd.filter_map(|e| e.ok()).collect();
            // stable ordering: by file name
            entries.sort_by_key(|e| e.path());
            for ent in entries {
                let path = ent.path();
                if path.is_dir() { walk(acc, &path); }
                else if let Some(ext) = path.extension() { if ext == "sql" { acc.push(path); } }
            }
        }
    }
    walk(&mut out, dir);
    out
}

async fn ensure_install_tables(store: &SharedStore) -> Result<()> {
    // Minimal ensures so we can log even if other DDLs fail
    let _ = crate::server::exec::execute_query_safe(store, "CREATE SCHEMA IF NOT EXISTS security").await;
    let _ = crate::server::exec::execute_query_safe(store, "CREATE TABLE IF NOT EXISTS security.install_log (script_path TEXT, checksum TEXT, started_at BIGINT, finished_at BIGINT, status TEXT, statements INT, error_text TEXT)").await;
    Ok(())
}

fn split_statements(sql: &str) -> Vec<String> {
    // Very simple splitter: split on semicolons; ignore empty; does not handle quoted semicolons.
    sql.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).map(|s| s.to_string()).collect()
}

pub async fn run_installer(store: &SharedStore, ddl_root: &Path) -> Result<()> {
    ensure_install_tables(store).await?;
    let files = collect_sql_files_recursive(ddl_root);
    tprintln!("installer: discovered {} SQL files under {}", files.len(), ddl_root.display());
    for f in files {
        let path_str = f.to_string_lossy().to_string();
        let src = fs::read_to_string(&f)?;
        let chk = xxh3_128(src.as_bytes());
        let checksum = format!("{:032x}", chk);
        let started = Utc::now().timestamp_millis();
        let mut status = "ok".to_string();
        let mut err: Option<String> = None;
        let stmts = split_statements(&src);
        let mut ran = 0usize;
        for s in stmts.iter() {
            match crate::server::exec::execute_query_safe(store, s).await {
                Ok(_) => { ran += 1; }
                Err(e) => { status = "error".into(); err = Some(e.to_string()); break; }
            }
        }
        let finished = Utc::now().timestamp_millis();
        let log_sql = format!(
            "INSERT INTO security.install_log (script_path, checksum, started_at, finished_at, status, statements, error_text) VALUES ('{}','{}',{},{} ,'{}', {}, '{}')",
            path_str.replace("'", "''"), checksum, started, finished, status, ran, err.clone().unwrap_or_default().replace("'", "''")
        );
        let _ = crate::server::exec::execute_query_safe(store, &log_sql).await;
        tprintln!("installer: {} status={} stmts={} err={}", path_str, status, ran, err.unwrap_or_default());
    }
    Ok(())
}

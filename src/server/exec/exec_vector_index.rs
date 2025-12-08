//! exec_vector_index
//! ------------------
//! VECTOR INDEX catalog management: CREATE/DROP/SHOW for sidecar `.vindex` files
//! stored alongside tables/views under `<db>/<schema>/<name>.vindex`.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;

use crate::server::query;
use crate::storage::SharedStore;
use crate::error::AppError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VIndexFile {
    pub version: i32,
    pub name: String,
    pub qualified: String,
    pub table: String,
    pub column: String,
    pub algo: String,
    pub metric: Option<String>,
    pub dim: Option<i32>,
    pub params: Option<serde_json::Map<String, serde_json::Value>>, // M, ef_build, ef_search (optional)
    pub status: Option<serde_json::Map<String, serde_json::Value>>,  // state/last_built_at/rows_indexed
    pub mode: Option<String>, // IMMEDIATE | BATCHED | ASYNC | REBUILD_ONLY
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

fn qualify_name(name: &str) -> String {
    let d = crate::system::current_query_defaults();
    crate::ident::qualify_regular_ident(name, &d)
}

pub(crate) fn path_for_vindex(store: &SharedStore, qualified: &str) -> std::path::PathBuf {
    let mut p = store.0.lock().root_path().clone();
    let local = qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
    p.push(local);
    p.set_extension("vindex");
    p
}

pub(crate) fn read_vindex_file(store: &SharedStore, qualified: &str) -> Result<Option<VIndexFile>> {
    let path = path_for_vindex(store, qualified);
    if !path.exists() { return Ok(None); }
    let text = std::fs::read_to_string(&path)?;
    let v: VIndexFile = serde_json::from_str(&text)?;
    Ok(Some(v))
}

pub(crate) fn write_vindex_file(store: &SharedStore, qualified: &str, vf: &VIndexFile) -> Result<()> {
    let path = path_for_vindex(store, qualified);
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent).ok(); }
    std::fs::write(&path, serde_json::to_string_pretty(vf)?)?;
    Ok(())
}

pub(crate) fn delete_vindex_file(store: &SharedStore, qualified: &str) -> Result<()> {
    let path = path_for_vindex(store, qualified);
    if path.exists() { std::fs::remove_file(&path).ok(); }
    Ok(())
}

pub(crate) fn now_iso() -> String { chrono::Utc::now().to_rfc3339() }

fn table_exists(store: &SharedStore, qualified_table: &str) -> bool {
    // Resolve to local path via storage helpers to ensure consistent layout handling
    let guard = store.0.lock();
    let dir = guard.db_dir(qualified_table);
    let has_dir = dir.is_dir();
    let has_schema = dir.join("schema.json").exists();
    let data_parquet = dir.join("data.parquet");
    let has_data_file = data_parquet.exists();
    // Also consider chunked parquet files like data-<min>-<max>-<ts>.parquet
    let mut has_chunk = false;
    if has_dir && !has_schema && !has_data_file {
        if let Ok(rd) = std::fs::read_dir(&dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                    if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                        if name == "data.parquet" || name.starts_with("data-") { has_chunk = true; break; }
                    }
                }
            }
        }
    }
    let exists = has_dir && (has_schema || has_data_file || has_chunk);
    crate::tprintln!(
        "[DDL] table_exists: table='{}' dir='{}' has_dir={} has_schema={} has_data_file={} has_chunk={} -> {}",
        qualified_table, dir.display(), has_dir, has_schema, has_data_file, has_chunk, exists
    );
    exists
}

fn list_vector_indexes(store: &SharedStore) -> Result<Value> {
    let root = store.0.lock().root_path().clone();
    let mut out: Vec<serde_json::Value> = Vec::new();
    if let Ok(dbs) = std::fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
            if let Ok(sd) = std::fs::read_dir(&db_path) {
                for schema_dir in sd.flatten().filter(|e| e.path().is_dir()) {
                    let sp = schema_dir.path();
                    if let Ok(td) = std::fs::read_dir(&sp) {
                        for tentry in td.flatten() {
                            let tp = tentry.path();
                            if tp.is_file() && tp.extension().and_then(|s| s.to_str()) == Some("vindex") {
                                if let Ok(text) = std::fs::read_to_string(&tp) {
                                    if let Ok(v) = serde_json::from_str::<VIndexFile>(&text) {
                                        out.push(serde_json::json!({
                                            "name": v.name,
                                            "table": v.table,
                                            "column": v.column,
                                            "algo": v.algo,
                                            "metric": v.metric,
                                            "dim": v.dim,
                                            "mode": v.mode
                                        }));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(Value::Array(out))
}

pub fn execute_vector_index(store: &SharedStore, cmd: query::Command) -> Result<Value> {
    match cmd {
        query::Command::CreateVectorIndex { name, table, column, algo, options } => {
            if algo.to_lowercase() != "hnsw" { return Err(AppError::Ddl { code: "vector_algo".into(), message: format!("Only HNSW is supported for now, got '{}'.", algo) }.into()); }
            let qualified = qualify_name(&name);
            crate::tprintln!("[VINDEX] CREATE name='{}' table='{}' column='{}' algo='{}' opts={:?}", qualified, table, column, algo, options);
            if read_vindex_file(store, &qualified)?.is_some() {
                return Err(AppError::Conflict { code: "name_conflict".into(), message: format!("Vector index already exists: {}", qualified) }.into());
            }
            let qtable = crate::ident::qualify_regular_ident(&table, &crate::system::current_query_defaults());
            if !table_exists(store, &qtable) {
                return Err(AppError::NotFound { code: "table_not_found".into(), message: format!("Table not found: {}", qtable) }.into());
            }
            // Build params map from options
            let mut params = serde_json::Map::new();
            let mut metric: Option<String> = None;
            let mut dim: Option<i32> = None;
            let mut mode: Option<String> = None;
            for (k, v) in options.into_iter() {
                let kl = k.to_lowercase();
                if kl == "metric" { metric = Some(v.trim_matches('\'').to_string()); continue; }
                if kl == "dim" { dim = v.parse::<i32>().ok(); continue; }
                if kl == "mode" { mode = Some(v.trim_matches('\'').to_ascii_uppercase()); continue; }
                params.insert(k, serde_json::Value::String(v));
            }
            // Validate/normalize mode; default to REBUILD_ONLY if absent
            let allowed = ["IMMEDIATE", "BATCHED", "ASYNC", "REBUILD_ONLY"];
            let mode = match mode {
                Some(m) => {
                    if !allowed.contains(&m.as_str()) {
                        return Err(AppError::Ddl { code: "vector_mode".into(), message: format!("Invalid vector index mode '{}'; expected one of IMMEDIATE|BATCHED|ASYNC|REBUILD_ONLY", m) }.into());
                    }
                    Some(m)
                }
                None => Some("REBUILD_ONLY".to_string()),
            };
            let vf = VIndexFile {
                version: 1,
                name: qualified.clone(),
                qualified: qualified.clone(),
                table: qtable,
                column,
                algo: "hnsw".to_string(),
                metric,
                dim,
                params: if params.is_empty() { None } else { Some(params) },
                status: None,
                mode,
                created_at: Some(now_iso()),
                updated_at: None,
            };
            write_vindex_file(store, &qualified, &vf)?;
            crate::tprintln!("[VINDEX] CREATE saved name='{}' mode={:?} metric={:?} dim={:?}", vf.name, vf.mode, vf.metric, vf.dim);
            info!(target: "clarium::ddl", "CREATE VECTOR INDEX saved '{}.vindex'", qualified);
            Ok(serde_json::json!({"status":"ok"}))
        }
        query::Command::DropVectorIndex { name } => {
            let qualified = qualify_name(&name);
            if read_vindex_file(store, &qualified)?.is_none() {
                return Err(AppError::NotFound { code: "not_found".into(), message: format!("Vector index not found: {}", qualified) }.into());
            }
            delete_vindex_file(store, &qualified)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        query::Command::ShowVectorIndex { name } => {
            let qualified = qualify_name(&name);
            crate::tprintln!("[VINDEX] SHOW name='{}'", qualified);
            if let Some(vf) = read_vindex_file(store, &qualified)? {
                crate::tprintln!("[VINDEX] SHOW found: table='{}' column='{}' mode={:?}", vf.table, vf.column, vf.mode);
                let row = serde_json::json!({
                    "name": vf.name,
                    "table": vf.table,
                    "column": vf.column,
                    "algo": vf.algo,
                    "metric": vf.metric,
                    "dim": vf.dim,
                    "params": vf.params,
                    "status": vf.status,
                    "mode": vf.mode
                });
                return Ok(serde_json::json!([row]));
            }
            return Err(AppError::NotFound { code: "not_found".into(), message: format!("Vector index not found: {}", qualified) }.into());
        }
        query::Command::ShowVectorIndexes => {
            let out = list_vector_indexes(store)?;
            if let Some(arr) = out.as_array() {
                crate::tprintln!("[VINDEX] SHOW INDEXES count={}", arr.len());
            }
            Ok(out)
        }
        query::Command::AlterVectorIndexSetMode { name, mode } => {
            let qualified = qualify_name(&name);
            if let Some(mut vf) = read_vindex_file(store, &qualified)? {
                let up = mode.to_ascii_uppercase();
                let allowed = ["IMMEDIATE", "BATCHED", "ASYNC", "REBUILD_ONLY"];
                if !allowed.contains(&up.as_str()) {
                    return Err(AppError::Ddl { code: "vector_mode".into(), message: format!("Invalid vector index mode '{}'; expected one of IMMEDIATE|BATCHED|ASYNC|REBUILD_ONLY", mode) }.into());
                }
                let old_mode = vf.mode.clone();
                vf.mode = Some(up);
                vf.updated_at = Some(now_iso());
                write_vindex_file(store, &qualified, &vf)?;
                crate::tprintln!("[VINDEX] ALTER SET MODE name='{}' old={:?} new={:?}", qualified, old_mode, vf.mode);
                Ok(serde_json::json!({"status":"ok"}))
            } else {
                Err(AppError::NotFound { code: "not_found".into(), message: format!("Vector index not found: {}", qualified) }.into())
            }
        }
        query::Command::BuildVectorIndex { name, options } => {
            let qualified = qualify_name(&name);
            crate::tprintln!("[VINDEX] BUILD name='{}' options={:?}", qualified, options);
            if let Some(mut vf) = read_vindex_file(store, &qualified)? {
                let out = crate::server::exec::exec_vector_runtime::build_vector_index(store, &mut vf, &options)?;
                // persist updated status into .vindex
                write_vindex_file(store, &qualified, &vf)?;
                crate::tprintln!("[VINDEX] BUILD completed: status state={:?} rows_indexed={:?}", vf.status.as_ref().and_then(|m| m.get("state")), vf.status.as_ref().and_then(|m| m.get("rows_indexed")));
                Ok(out)
            } else {
                Err(AppError::NotFound { code: "not_found".into(), message: format!("Vector index not found: {}", qualified) }.into())
            }
        }
        query::Command::ReindexVectorIndex { name } => {
            let qualified = qualify_name(&name);
            if let Some(mut vf) = read_vindex_file(store, &qualified)? {
                let out = crate::server::exec::exec_vector_runtime::reindex_vector_index(store, &mut vf)?;
                write_vindex_file(store, &qualified, &vf)?;
                crate::tprintln!("[VINDEX] REINDEX name='{}' state={:?}", qualified, vf.status.as_ref().and_then(|m| m.get("state")));
                Ok(out)
            } else {
                Err(AppError::NotFound { code: "not_found".into(), message: format!("Vector index not found: {}", qualified) }.into())
            }
        }
        query::Command::ShowVectorIndexStatus { name } => {
            crate::tprintln!("[VINDEX] SHOW STATUS name={:?}", name);
            let out = crate::server::exec::exec_vector_runtime::show_vector_index_status(store, name.as_deref())?;
            if let Some(arr) = out.as_array() { crate::tprintln!("[VINDEX] STATUS rows={} first_row={:?}", arr.len(), arr.get(0)); }
            Ok(out)
        }
        _ => Err(AppError::Ddl { code: "unsupported_vector_index".into(), message: "unsupported vector index command".into() }.into()),
    }
}

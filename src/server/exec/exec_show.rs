use anyhow::Result;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::Path;

use crate::server::query::Command;
use crate::scripts::scripts_dir_for;
use crate::storage::SharedStore;
use crate::server::graphstore::graphstore_status_df;
use crate::lua_bc::DEFAULT_DB;

pub async fn execute_show(store: &SharedStore, cmd: Command) -> Result<Value> {
    match cmd {
        Command::ShowGraphStatus { name } => {
            // Determine graph: explicit arg wins; else fall back to session default graph
            let graph = if let Some(n) = name { n } else {
                crate::system::get_current_graph_opt()
                    .ok_or_else(|| anyhow::anyhow!("SHOW GRAPH STATUS: missing graph name; set it with USE GRAPH or provide explicitly"))?
            };
            let df = graphstore_status_df(store, &graph)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowTransactionIsolation => single_kv("transaction_isolation", "read committed"),
        Command::ShowStandardConformingStrings => single_kv("standard_conforming_strings", "on"),
        Command::ShowServerVersion => single_kv("server_version", "14.0"),
        Command::ShowClientEncoding => single_kv("client_encoding", "UTF8"),
        Command::ShowServerEncoding => single_kv("server_encoding", "UTF8"),
        Command::ShowDateStyle => single_kv("DateStyle", "ISO, MDY"),
        Command::ShowIntegerDateTimes => single_kv("integer_datetimes", "on"),
        Command::ShowTimeZone => single_kv("TimeZone", "UTC"),
        Command::ShowSearchPath => single_kv("search_path", "public"),
        Command::ShowDefaultTransactionIsolation => single_kv("default_transaction_isolation", "read committed"),
        Command::ShowTransactionReadOnly => single_kv("transaction_read_only", "off"),
        Command::ShowApplicationName => single_kv("application_name", "clarium"),
        Command::ShowExtraFloatDigits => single_kv("extra_float_digits", "3"),
        Command::ShowAll => show_all(),
        Command::ShowSchemas => show_schemas(store),
        Command::ShowTables => show_tables(store),
        Command::ShowObjects => show_objects(store),
        Command::ShowScripts => show_scripts(store),
        // -------------------------------------------------
        // FILESTORE SHOW commands → delegate to filestore::show
        Command::ShowFilestores { database } => {
            let db = database.unwrap_or_else(|| DEFAULT_DB.to_string());
            let df = crate::server::exec::filestore::show_filestores_df(store, &db)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowFilestoreConfig { filestore, folder_prefix } => {
            let df = crate::server::exec::filestore::show_filestore_config_df(store, DEFAULT_DB, &filestore, folder_prefix.as_deref())?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowFilesInFilestore { filestore, prefix, limit, offset } => {
            let off = offset.unwrap_or(0).max(0) as usize;
            let lim = limit.and_then(|n| if n > 0 { Some(n as usize) } else { None });
            let df = crate::server::exec::filestore::show_files_df_paged(store, DEFAULT_DB, &filestore, prefix.as_deref(), off, lim)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowTreesInFilestore { filestore } => {
            let df = crate::server::exec::filestore::show_trees_df(store, DEFAULT_DB, &filestore)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowCommitsInFilestore { filestore } => {
            let df = crate::server::exec::filestore::show_commits_df(store, DEFAULT_DB, &filestore)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowDiffInFilestore { filestore, left_tree_id, right_tree_id, live_prefix } => {
            let df = crate::server::exec::filestore::show_diff_df(store, DEFAULT_DB, &filestore, &left_tree_id, right_tree_id.as_deref(), live_prefix.as_deref())?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowChunksInFilestore { filestore } => {
            let df = crate::server::exec::filestore::show_chunks_df(store, DEFAULT_DB, &filestore)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowAliasesInFilestore { filestore } => {
            let df = crate::server::exec::filestore::show_aliases_df(store, DEFAULT_DB, &filestore)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowAdminInFilestore { filestore } => {
            let df = crate::server::exec::filestore::show_admin_counts_df(store, DEFAULT_DB, &filestore)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        Command::ShowHealthInFilestore { filestore } => {
            let df = crate::server::exec::filestore::show_health_df(store, DEFAULT_DB, &filestore)?;
            return Ok(crate::server::exec::dataframe_to_json(&df));
        }
        // -------------------------------------------------
        other => anyhow::bail!(format!("unsupported SHOW variant in exec_show: {:?}", other)),
    }
}

fn single_kv(key: &str, val: &str) -> Result<Value> {
    Ok(serde_json::json!([{ key: val }]))
}

fn show_all() -> Result<Value> {
    let rows = vec![
        kv("server_version", "14.0"),
        kv("client_encoding", "UTF8"),
        kv("standard_conforming_strings", "on"),
        kv("TimeZone", "UTC"),
        kv("search_path", "public"),
        kv("default_transaction_isolation", "read committed"),
        kv("transaction_read_only", "off"),
        kv("extra_float_digits", "3"),
    ];
    Ok(Value::Array(rows))
}

fn kv(k: &str, v: &str) -> Value { serde_json::json!({ k: v }) }

fn root_path(store: &SharedStore) -> std::path::PathBuf { let g = store.0.lock(); g.root_path().clone() }

fn show_schemas(store: &SharedStore) -> Result<Value> {
    let mut schemas: BTreeSet<String> = BTreeSet::new();
    let root = root_path(store);
    if let Ok(dbs) = std::fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
            if let Ok(sd) = std::fs::read_dir(&db_path) {
                for sch_ent in sd.flatten() {
                    let p = sch_ent.path(); if p.is_dir() {
                        if let Some(name) = p.file_name().and_then(|s| s.to_str()) { if !name.starts_with('.') { schemas.insert(name.to_string()); } }
                    }
                }
            }
        }
    }
    let rows: Vec<Value> = schemas.into_iter().map(|s| serde_json::json!({"schema_name": s})).collect();
    Ok(Value::Array(rows))
}

fn show_tables(store: &SharedStore) -> Result<Value> {
    let root = root_path(store);
    let mut out: Vec<Value> = Vec::new();
    if let Ok(dbs) = std::fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
            if let Ok(sd) = std::fs::read_dir(&db_path) {
                for schema_dir in sd.flatten().filter(|e| e.path().is_dir()) {
                    let sp = schema_dir.path();
                    if let Ok(td) = std::fs::read_dir(&sp) {
                        for tentry in td.flatten() {
                            let tp = tentry.path();
                            if tp.is_dir() && tp.join("schema.json").exists() {
                                let tname_os = tentry.file_name();
                                let mut tname = tname_os.to_string_lossy().to_string();
                                if let Some(stripped) = tname.strip_suffix(".time") { tname = stripped.to_string(); }
                                out.push(serde_json::json!({"table_name": tname}));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(Value::Array(out))
}

fn show_objects(store: &SharedStore) -> Result<Value> {
    let root = root_path(store);
    let mut out: Vec<Value> = Vec::new();
    if let Ok(dbs) = std::fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
            if let Ok(sd) = std::fs::read_dir(&db_path) {
                for schema_dir in sd.flatten().filter(|e| e.path().is_dir()) {
                    let sp = schema_dir.path();
                    if let Ok(td) = std::fs::read_dir(&sp) {
                        for tentry in td.flatten() {
                            let tp = tentry.path();
                            if tp.is_dir() && tp.join("schema.json").exists() {
                                let mut name = tentry.file_name().to_string_lossy().to_string();
                                if let Some(stripped) = name.strip_suffix(".time") { name = stripped.to_string(); }
                                out.push(serde_json::json!({"name": name, "type": "table"}));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(Value::Array(out))
}

fn show_scripts(store: &SharedStore) -> Result<Value> {
    use std::fs;
    let root = root_path(store);
    let mut out: Vec<Value> = Vec::new();
    if let Ok(dbs) = fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let dbname = db_ent.file_name().to_string_lossy().to_string();
            let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
            if let Ok(sd) = fs::read_dir(&db_path) {
                for sch_ent in sd.flatten() {
                    let sname = sch_ent.file_name().to_string_lossy().to_string();
                    let sdir = scripts_dir_for(Path::new(&root), &dbname, &sname);
                    if sdir.exists() {
                        // look in scalars and aggregates
                        for sub in ["scalars", "aggregates"] {
                            let subd = sdir.join(sub);
                            if subd.exists() {
                                if let Ok(listing) = fs::read_dir(&subd) {
                                    for f in listing.flatten() {
                                        let p = f.path();
                                        if p.extension().and_then(|e| e.to_str()).unwrap_or("").eq_ignore_ascii_case("lua") {
                                            let name = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                                            let kind = if sub == "aggregates" { "aggregate" } else { "scalar" };
                                            out.push(serde_json::json!({"db": dbname, "schema": sname, "name": name, "kind": kind}));
                                        }
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


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_root() -> std::path::PathBuf {
        let mut base = std::env::temp_dir();
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        let pid = std::process::id();
        base.push(format!("clarium_tests_{}_{}", pid, ts));
        base
    }

    fn write_file(path: &std::path::Path, text: &str) {
        if let Some(parent) = path.parent() { fs::create_dir_all(parent).unwrap(); }
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(text.as_bytes()).unwrap();
    }

    #[test]
    fn show_scripts_lists_scalar_and_aggregate() {
        // Arrange: temp db root with one db/schema and two scripts
        let root = unique_temp_root();
        fs::create_dir_all(&root).unwrap();
        let db = "db1";
        let schema = "public";
        let scripts_root = scripts_dir_for(&root, db, schema);
        let scalar_path = scripts_root.join("scalars").join("hello.lua");
        let agg_path = scripts_root.join("aggregates").join("sum.lua");
        write_file(&scalar_path, "function hello(x) return x end");
        write_file(&agg_path, "function sum(x,y) return x+y end");

        let store = crate::storage::SharedStore::new(root.to_string_lossy().as_ref()).unwrap();

        // Act: execute SHOW SCRIPTS through the public API
        let rt = tokio::runtime::Runtime::new().unwrap();
        let val = rt.block_on(crate::server::exec::execute_query(&store, "SHOW SCRIPTS")).unwrap();

        // Assert: find both entries
        let arr = match val { serde_json::Value::Array(a) => a, _ => panic!("SHOW SCRIPTS did not return an array: {:?}", val) };
        let mut has_scalar = false;
        let mut has_agg = false;
        for row in &arr {
            if let serde_json::Value::Object(m) = row {
                let dbv = m.get("db").and_then(|v| v.as_str()).unwrap_or("");
                let scv = m.get("schema").and_then(|v| v.as_str()).unwrap_or("");
                let name = m.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let kind = m.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                if dbv == db && scv == schema && name == "hello" && kind == "scalar" { has_scalar = true; }
                if dbv == db && scv == schema && name == "sum" && kind == "aggregate" { has_agg = true; }
            }
        }

        if !(has_scalar && has_agg) {
            // Fallback diagnostics: scan filesystem directly as the SHOW implementation would
            let sdir = scripts_dir_for(&root, db, schema);
            let mut expected: Vec<serde_json::Value> = Vec::new();
            for sub in ["scalars", "aggregates"] {
                let subd = sdir.join(sub);
                if subd.exists() {
                    for ent in fs::read_dir(&subd).unwrap().flatten() {
                        let p = ent.path();
                        if p.extension().and_then(|e| e.to_str()).unwrap_or("").eq_ignore_ascii_case("lua") {
                            let name = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                            let kind = if sub == "aggregates" { "aggregate" } else { "scalar" };
                            expected.push(serde_json::json!({"db": db, "schema": schema, "name": name, "kind": kind}));
                        }
                    }
                }
            }
            panic!(
                "SHOW SCRIPTS mismatch.\nExpected entries (from filesystem): {:#?}\nActual rows: {:#?}",
                expected, arr
            );
        }

        // Cleanup best-effort
        let _ = fs::remove_dir_all(&root);
    }
}

//! exec_create
//! -----------
//! Regular table DDL handling extracted from exec.rs to keep the dispatcher thin.
//! Contains CREATE TABLE (regular), DROP TABLE, RENAME TABLE, and SQL parsing helper
//! `do_create_table`. Future contributors: keep new DDL logic here.

use anyhow::{Context, Result};
use crate::error::AppError;
use tracing::{debug, info};
use crate::tprintln;

use crate::storage::SharedStore;

/// Handle CREATE TABLE for regular (non-time) tables.
pub fn handle_create_table(store: &SharedStore, table: &str, primary_key: &Option<Vec<String>>, partitions: &Option<Vec<String>>) -> Result<serde_json::Value> {
    use std::{fs, path::PathBuf};
    debug!(target: "clarium::exec", "CreateTable: begin table='{}' pk={:?} partitions={:?}", table, primary_key, partitions);

    // Qualify with current session defaults if not already fully qualified
    let qd = crate::system::current_query_defaults();
    let table = crate::ident::qualify_regular_ident(table, &qd);

    // Resolve filesystem paths for diagnostics (before creating)
    let (_dir_path_before, exists_before) = {
        let g = store.0.lock();
        let root = g.root_path().clone();
        let dir: PathBuf = root.join(table.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
        let ex = dir.exists();
        (dir, ex)
    };
    info!(target: "clarium::ddl", "CREATE TABLE requested table='{}' existed_before={}", table, exists_before);

    if table.ends_with(".time") { return Err(AppError::Ddl { code: "ddl_error".into(), message: "CREATE TABLE cannot target a .time table".into() }.into()); }

    // Enforce uniqueness with views: a table cannot be created if a view with the same base name exists
    {
        let root = store.root_path().clone();
        let mut vp = root.join(table.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
        // For regular table, vp points to .../db/schema/table — convert to .view file
        vp.set_extension("view");
        if vp.exists() {
            return Err(AppError::Conflict { code: "name_conflict".into(), message: format!("A VIEW exists with name '{}'. Table names must be unique across views.", table) }.into());
        }
    }
    // Create the table directory and initial schema via store
    {
        let guard = store.0.lock();
        guard.create_table(&table)?;
        if primary_key.is_some() || partitions.is_some() {
            guard.set_table_metadata(&table, primary_key.clone(), partitions.clone())?;
        }
    }

    // Post-create diagnostics
    let (exists_after, schema_path, schema_summary) = {
        let g = store.0.lock();
        let root = g.root_path().clone();
        let dir: PathBuf = root.join(table.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
        let sp = dir.join("schema.json");
        let mut summary = String::new();
        if sp.exists() {
            if let Ok(text) = fs::read_to_string(&sp) {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(obj) = json.as_object() {
                        let mut cols: Vec<String> = obj
                            .iter()
                            .filter_map(|(k, v)| { let ty = v.as_str().unwrap_or(""); if k != "PRIMARY" { Some(format!("{}:{}", k, ty)) } else { None } })
                            .collect();
                        cols.sort();
                        summary = format!("cols={} [{}]", cols.len(), cols.join(", "));
                    }
                }
            }
        }
        (dir.exists(), sp, summary)
    };

    info!(target: "clarium::ddl", "CREATE TABLE finalized table='{}' existed_after={} schema='{}' {}",
        table, exists_after, schema_path.display(), schema_summary);
    debug!(target: "clarium::exec", "CreateTable: success table='{}'", table);
    Ok(serde_json::json!({"status":"ok"}))
}

pub fn handle_drop_table(store: &SharedStore, table: &str, if_exists: bool) -> Result<serde_json::Value> {
    let guard = store.0.lock();
    if table.ends_with(".time") { return Err(AppError::Ddl { code: "ddl_error".into(), message: "DROP TABLE cannot target a .time table".into() }.into()); }
    // Qualify with session defaults
    let qd = crate::system::current_query_defaults();
    let tableq = crate::ident::qualify_regular_ident(table, &qd);
    // Check if table exists
    let table_path = guard.root_path().join(tableq.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
    let exists = table_path.exists();
    // If IF EXISTS is used and table doesn't exist, return success without error
    if if_exists && !exists {
        return Ok(serde_json::json!({"status":"ok"}));
    }
    // If table doesn't exist and IF EXISTS is not used, return error
    if !exists {
        return Err(AppError::NotFound { code: "not_found".into(), message: format!("Table not found: {}", tableq) }.into());
    }
    // Otherwise proceed with normal deletion
    guard.delete_table(&tableq)?;
    Ok(serde_json::json!({"status":"ok"}))
}

pub fn handle_rename_table(store: &SharedStore, from: &str, to: &str) -> Result<serde_json::Value> {
    use std::fs;
    if from.ends_with(".time") || to.ends_with(".time") { anyhow::bail!("RENAME TABLE cannot rename .time tables; use RENAME TIME TABLE"); }
    let qd = crate::system::current_query_defaults();
    let fromq = crate::ident::qualify_regular_ident(from, &qd);
    let toq = crate::ident::qualify_regular_ident(to, &qd);
    let src = store.root_path().join(fromq.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
    let dst = store.root_path().join(toq.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
    if !src.exists() { return Err(AppError::NotFound { code: "not_found".into(), message: format!("Source table not found: {}", from) }.into()); }
    if let Some(parent) = dst.parent() { fs::create_dir_all(parent).ok(); }
    fs::rename(&src, &dst)?;
    Ok(serde_json::json!({"status":"ok"}))
}

/// Parse and execute a CREATE TABLE statement with full SQL syntax (columns and types).
/// This function is used by pgwire to support CREATE TABLE commands from SQL clients.
/// Syntax: CREATE TABLE [IF NOT EXISTS] <db>/<schema>/<table> (col type, ...)
pub fn do_create_table(store: &SharedStore, q: &str) -> Result<()> {
    
    // Parse: CREATE TABLE [IF NOT EXISTS] <db>/<schema>/<table> (col type, ...)
    tprintln!("[CREATE] do_create_table: CALLED with q='{}'", q);
    debug!(target: "clarium::exec", "do_create_table: begin q='{}'", q);
    let mut s = q.trim();
    let up = s.to_uppercase();
    if !up.starts_with("CREATE TABLE ") { return Err(AppError::Ddl { code: "unsupported_create".into(), message: "Only CREATE TABLE is supported".into() }.into()); }
    s = s["CREATE TABLE ".len()..].trim();
    let s_up = s.to_uppercase();
    if s_up.starts_with("IF NOT EXISTS ") { s = s["IF NOT EXISTS ".len()..].trim(); }
    // Extract identifier up to '(' and the column list inside (...)
    let p_open = s.find('(').ok_or_else(|| AppError::Ddl { code: "syntax".into(), message: "expected ( in CREATE TABLE".into() })?;
    let ident = s[..p_open].trim();
    let p_close = s.rfind(')').ok_or_else(|| AppError::Ddl { code: "syntax".into(), message: "expected ) in CREATE TABLE".into() })?;
    let cols_str = &s[p_open+1 .. p_close];
    // Parse columns and detect constraints
    let mut cols: Vec<(String, String)> = Vec::new();
    let mut cur = String::new();
    let mut depth = 0i32;
    for ch in cols_str.chars() {
        match ch {
            '(' => { depth += 1; cur.push(ch); }
            ')' => { depth -= 1; cur.push(ch); }
            ',' if depth == 0 => { if !cur.trim().is_empty() { cols.push(split_col_def(cur.trim())); } cur.clear(); }
            _ => cur.push(ch),
        }
    }
    if !cur.trim().is_empty() { cols.push(split_col_def(cur.trim())); }

    // Detect PRIMARY KEY quickly
    let mut has_primary_key = false;
    let cols_str_up = cols_str.to_uppercase();
    if cols_str_up.contains("PRIMARY KEY") { has_primary_key = true; }

    // Map SQL types to internal type keys
    let mut schema_entries: Vec<(String, String)> = Vec::new();
    tprintln!("[CREATE] do_create_table: parsed {} columns from SQL", cols.len());
    for (name, ty) in cols.into_iter() {
        tprintln!("[CREATE] do_create_table: processing col='{}' type='{}'", name, ty);
        let n = name.trim_matches('"').to_string();
        // Skip table-level constraint rows
        let n_up = n.to_uppercase();
        if n_up == "PRIMARY" || n_up == "FOREIGN" || n_up == "UNIQUE" || n_up == "CHECK" || n_up == "CONSTRAINT" { 
            tprintln!("[CREATE] do_create_table: skipping constraint keyword '{}'", n);
            continue; 
        }
        if n == "_time" { 
            tprintln!("[CREATE] do_create_table: skipping _time column");
            continue; 
        }
        let t_up = ty.to_ascii_lowercase();
        let key = if t_up.contains("char") || t_up.contains("text") || t_up.contains("json") || t_up.contains("bool") { "string".to_string() }
            else if t_up.contains("int") { "int64".to_string() }
            else if t_up.contains("double") || t_up.contains("real") || t_up.contains("float") || t_up.contains("numeric") || t_up.contains("decimal") { "float64".to_string() }
            else if t_up.contains("time") || t_up.contains("date") { "int64".to_string() }
            else if t_up.contains("vector") { "vector".to_string() }
            else { "string".to_string() };
        tprintln!("[CREATE] do_create_table: adding col='{}' mapped_type='{}'", n, key);
        schema_entries.push((n, key));
    }
    tprintln!("[CREATE] do_create_table: final schema_entries count={}", schema_entries.len());
    // Create directory and schema.json
    let ident_norm = ident.trim().trim_matches('"');
    // Qualify with current session defaults (like handle_create_table does)
    let qd = crate::system::current_query_defaults();
    let db_path = crate::ident::qualify_regular_ident(ident_norm, &qd);
    tprintln!("[CREATE] do_create_table: qualified table name: '{}' -> '{}'", ident_norm, db_path);
    let root = store.root_path();
    let dir = std::path::Path::new(&root).join(db_path.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
    debug!(target: "clarium::exec", "do_create_table: dir='{}' (db_path='{}')", dir.display(), db_path);
    std::fs::create_dir_all(&dir).with_context(|| format!("create table dir {}", dir.display()))?;
    let mut map = serde_json::Map::new();
    for (k, t) in schema_entries.into_iter() { 
        tprintln!("[CREATE] do_create_table: inserting into map key='{}' value='{}'", k, t);
        map.insert(k, serde_json::Value::String(t)); 
    }
    if has_primary_key { map.insert("PRIMARY".to_string(), serde_json::Value::String("marker".to_string())); }
    let sj = dir.join("schema.json");
    let json_str = serde_json::to_string_pretty(&serde_json::Value::Object(map.clone()))?;
    tprintln!("[CREATE] do_create_table: writing schema.json to '{}' content='{}'", sj.display(), json_str);
    std::fs::write(&sj, &json_str)?;
    debug!(target: "clarium::exec", "do_create_table: wrote schema.json at '{}'", sj.display());
    Ok(())
}

fn split_col_def(s: &str) -> (String, String) {
    let mut parts = s.split_whitespace();
    let name = parts.next().unwrap_or("").to_string();
    let mut ty = String::new();
    for p in parts { if !ty.is_empty() { ty.push(' '); } ty.push_str(p); }
    (name, ty)
}

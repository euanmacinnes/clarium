//! exec_create
//! -----------
//! Regular table DDL handling extracted from exec.rs to keep the dispatcher thin.
//! Contains CREATE TABLE (regular), DROP TABLE, RENAME TABLE, and SQL parsing helper
//! `do_create_table`. Future contributors: keep new DDL logic here.

use anyhow::{Context, Result};
use tracing::{debug, info};

use crate::storage::SharedStore;

/// Handle CREATE TABLE for regular (non-time) tables.
pub fn handle_create_table(store: &SharedStore, table: &str, primary_key: &Option<Vec<String>>, partitions: &Option<Vec<String>>) -> Result<serde_json::Value> {
    use std::{fs, path::PathBuf};
    debug!(target: "clarium::exec", "CreateTable: begin table='{}' pk={:?} partitions={:?}", table, primary_key, partitions);

    // Resolve filesystem paths for diagnostics (before creating)
    let (_dir_path_before, exists_before) = {
        let g = store.0.lock();
        let root = g.root_path().clone();
        let dir: PathBuf = root.join(table.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
        let ex = dir.exists();
        (dir, ex)
    };
    info!(target: "clarium::ddl", "CREATE TABLE requested table='{}' existed_before={}", table, exists_before);

    if table.ends_with(".time") { anyhow::bail!("CREATE TABLE cannot target a .time table"); }

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
    if table.ends_with(".time") { anyhow::bail!("DROP TABLE cannot target a .time table"); }
    // Check if table exists
    let table_path = guard.root_path().join(table.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
    let exists = table_path.exists();
    // If IF EXISTS is used and table doesn't exist, return success without error
    if if_exists && !exists {
        return Ok(serde_json::json!({"status":"ok"}));
    }
    // If table doesn't exist and IF EXISTS is not used, return error
    if !exists {
        anyhow::bail!("Table not found: {}", table);
    }
    // Otherwise proceed with normal deletion
    guard.delete_table(&table)?;
    Ok(serde_json::json!({"status":"ok"}))
}

pub fn handle_rename_table(store: &SharedStore, from: &str, to: &str) -> Result<serde_json::Value> {
    use std::fs;
    if from.ends_with(".time") || to.ends_with(".time") { anyhow::bail!("RENAME TABLE cannot rename .time tables; use RENAME TIME TABLE"); }
    let src = store.root_path().join(from.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
    let dst = store.root_path().join(to.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
    if !src.exists() { anyhow::bail!("Source table not found: {}", from); }
    if let Some(parent) = dst.parent() { fs::create_dir_all(parent).ok(); }
    fs::rename(&src, &dst)?;
    Ok(serde_json::json!({"status":"ok"}))
}

/// Parse and execute a CREATE TABLE statement with full SQL syntax (columns and types).
/// This function is used by pgwire to support CREATE TABLE commands from SQL clients.
/// Syntax: CREATE TABLE [IF NOT EXISTS] <db>/<schema>/<table> (col type, ...)
pub fn do_create_table(store: &SharedStore, q: &str) -> Result<()> {
    use anyhow::anyhow;
    // Parse: CREATE TABLE [IF NOT EXISTS] <db>/<schema>/<table> (col type, ...)
    debug!(target: "clarium::exec", "do_create_table: begin q='{}'", q);
    let mut s = q.trim();
    let up = s.to_uppercase();
    if !up.starts_with("CREATE TABLE ") { return Err(anyhow!("unsupported CREATE TABLE")); }
    s = s["CREATE TABLE ".len()..].trim();
    let s_up = s.to_uppercase();
    if s_up.starts_with("IF NOT EXISTS ") { s = s["IF NOT EXISTS ".len()..].trim(); }
    // Extract identifier up to '(' and the column list inside (...)
    let p_open = s.find('(').ok_or_else(|| anyhow!("expected ( in CREATE TABLE"))?;
    let ident = s[..p_open].trim();
    let p_close = s.rfind(')').ok_or_else(|| anyhow!("expected ) in CREATE TABLE"))?;
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
    for (name, ty) in cols.into_iter() {
        let n = name.trim_matches('"').to_string();
        // Skip table-level constraint rows
        let n_up = n.to_uppercase();
        if n_up == "PRIMARY" || n_up == "FOREIGN" || n_up == "UNIQUE" || n_up == "CHECK" || n_up == "CONSTRAINT" { continue; }
        if n == "_time" { continue; }
        let t_up = ty.to_ascii_lowercase();
        let key = if t_up.contains("char") || t_up.contains("text") || t_up.contains("json") || t_up.contains("bool") { "string".to_string() }
            else if t_up.contains("int") { "int64".to_string() }
            else if t_up.contains("double") || t_up.contains("real") || t_up.contains("float") || t_up.contains("numeric") || t_up.contains("decimal") { "float64".to_string() }
            else if t_up.contains("time") || t_up.contains("date") { "int64".to_string() }
            else { "string".to_string() };
        schema_entries.push((n, key));
    }
    // Create directory and schema.json
    let ident_norm = ident.trim().trim_matches('"');
    // If normalized earlier, we expect slashes; otherwise convert dots to slashes
    let db_path = if ident_norm.contains('/') { ident_norm.to_string() } else { ident_norm.replace('.', "/") };
    let root = store.root_path();
    let dir = std::path::Path::new(&root).join(db_path.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
    debug!(target: "clarium::exec", "do_create_table: dir='{}' (db_path='{}')", dir.display(), db_path);
    std::fs::create_dir_all(&dir).with_context(|| format!("create table dir {}", dir.display()))?;
    let mut map = serde_json::Map::new();
    for (k, t) in schema_entries.into_iter() { map.insert(k, serde_json::Value::String(t)); }
    if has_primary_key { map.insert("PRIMARY".to_string(), serde_json::Value::String("marker".to_string())); }
    let sj = dir.join("schema.json");
    std::fs::write(&sj, serde_json::to_string_pretty(&serde_json::Value::Object(map))?)?;
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

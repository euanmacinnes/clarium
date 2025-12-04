//! exec_describe
//! -------------
//! Implements `DESCRIBE <object>` for tables and views.

use anyhow::{Context, Result};
use polars::prelude::*;

use crate::storage::SharedStore;

fn qualify_name(name: &str) -> String {
    let d = crate::system::current_query_defaults();
    crate::ident::qualify_regular_ident(name, &d)
}

fn view_path(store: &SharedStore, qualified: &str) -> std::path::PathBuf {
    let mut p = store.0.lock().root_path().clone();
    let local = qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
    p.push(local);
    p.set_extension("view");
    p
}

fn table_dir_path(store: &SharedStore, qualified: &str) -> std::path::PathBuf {
    let mut p = store.0.lock().root_path().clone();
    let local = qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
    p.push(local);
    p
}

fn time_table_dir_path(store: &SharedStore, qualified: &str) -> std::path::PathBuf {
    let mut p = table_dir_path(store, qualified);
    // append .time as a directory
    let ext = format!("{}time", std::path::MAIN_SEPARATOR);
    let s = p.to_string_lossy().to_string() + &ext;
    std::path::PathBuf::from(s)
}

fn read_table_schema_json(dir: &std::path::Path) -> Result<Option<serde_json::Value>> {
    let sj = dir.join("schema.json");
    if !sj.exists() { return Ok(None); }
    let text = std::fs::read_to_string(&sj)?;
    let v: serde_json::Value = serde_json::from_str(&text)?;
    Ok(Some(v))
}

fn headers() -> Vec<&'static str> {
    vec![
        "Primary Key",
        "Column",
        "Foreign Keys",
        "Type",
        "Nullable",
        "Default",
        "Autoincrement",
        "Check",
        "Unique",
        "Index",
        "comment",
    ]
}

fn to_dtype_key_str(dt: &DataType) -> String {
    crate::storage::Store::dtype_to_str(dt)
}

pub fn execute_describe(store: &SharedStore, name: &str) -> Result<serde_json::Value> {
    // Qualify name with session defaults
    let qualified = qualify_name(name);

    // 1) Try view first
    if let Some(vf) = crate::server::exec::exec_views::read_view_file(store, &qualified)? {
        let mut pk_set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        // Views do not currently carry PK metadata
        let mut out_rows: Vec<[String; 11]> = Vec::new();
        for (col, ty_key) in vf.columns.into_iter() {
            let pk = if pk_set.contains(&col) { "*".to_string() } else { String::new() };
            out_rows.push([
                pk,
                col,
                String::new(),
                ty_key,
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
            ]);
        }
        return rows_to_json(out_rows);
    }

    // 2) Else treat as table; use pg_catalog simulators for columns/PK to align with SQLAlchemy
    // Compute stable OID for this object
    let qd = crate::system::current_query_defaults();
    let table_oid = crate::server::exec::exec_common::regclass_oid_with_defaults(
        &qualified,
        Some(&qd.current_database),
        Some(&qd.current_schema),
    );

    // Load schema.json once to map types; also use it to confirm table existence on disk
    let reg_dir = table_dir_path(store, &qualified);
    let time_dir = time_table_dir_path(store, &qualified);
    let dir = if reg_dir.is_dir() && reg_dir.join("schema.json").exists() {
        Some(reg_dir)
    } else if time_dir.is_dir() && time_dir.join("schema.json").exists() {
        Some(time_dir)
    } else { None };
    if dir.is_none() { anyhow::bail!(format!("Object not found: {} (neither view nor table)", qualified)); }
    let dir = dir.unwrap();
    let json = read_table_schema_json(&dir).context("failed to read schema.json")?;
    let json = json.ok_or_else(|| anyhow::anyhow!("schema.json missing for table"))?;
    let mut type_map: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    if let Some(obj) = json.as_object() {
        for (k, v) in obj.iter() {
            if k == "PRIMARY" || k == "primaryKey" || k == "partitions" || k == "tableType" { continue; }
            let ty_key = if let Some(s) = v.as_str() { s.to_string() }
                else if let Some(m) = v.as_object() { m.get("type").and_then(|x| x.as_str()).unwrap_or("").to_string() }
                else { String::new() };
            type_map.insert(k.clone(), ty_key);
        }
    }

    // Query pg_attribute for columns of this table. If system catalogs are unavailable
    // (e.g., in lightweight/temp stores), fall back to schema.json contents.
    let attr_df_opt = crate::system::system_table_df("pg_catalog.pg_attribute", store);
    let mut columns: Vec<(i32, String, String)> = Vec::new(); // (attnum, name, type_key)
    if let Some(attr_df) = attr_df_opt {
        let attrelid = attr_df.column("attrelid").ok().and_then(|c| c.i32().ok());
        let attname = attr_df.column("attname").ok().and_then(|c| c.str().ok());
        let attnum = attr_df.column("attnum").ok().and_then(|c| c.i32().ok());
        let attisdropped = attr_df.column("attisdropped").ok().and_then(|c| c.bool().ok());
        if let (Some(attrelid), Some(attname), Some(attnum), Some(attisdropped)) = (attrelid, attname, attnum, attisdropped) {
            let len = attrelid.len();
            for i in 0..len {
                if attrelid.get(i).unwrap_or(0) == table_oid && !attisdropped.get(i).unwrap_or(false) {
                    let num = attnum.get(i).unwrap_or(0);
                    let name = attname.get(i).unwrap_or("");
                    // Lookup type from schema map
                    let ty = type_map.get(name).cloned().unwrap_or_default();
                    columns.push((num, name.to_string(), ty));
                }
            }
        }
    }
    // Fallback: if no columns were found via pg_attribute, synthesize from schema.json
    if columns.is_empty() {
        let mut keys: Vec<String> = type_map.keys().cloned().collect();
        keys.sort();
        for (i, k) in keys.into_iter().enumerate() {
            let ty = type_map.get(&k).cloned().unwrap_or_default();
            columns.push(((i as i32) + 1, k, ty));
        }
    }
    // Sort by attnum to preserve natural order
    columns.sort_by_key(|t| t.0);

    // Query pg_constraint_columns for PK membership (best-effort). If unavailable, leave empty.
    let mut pk_attnums: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();
    if let Some(cc_df) = crate::system::system_table_df("pg_catalog.pg_constraint_columns", store) {
        let conrelid = cc_df.column("conrelid").ok().and_then(|c| c.i32().ok());
        let contype = cc_df.column("contype").ok().and_then(|c| c.str().ok());
        let attnum = cc_df.column("attnum").ok().and_then(|c| c.i32().ok());
        if let (Some(conrelid), Some(contype), Some(attnum)) = (conrelid, contype, attnum) {
            let len = conrelid.len();
            for i in 0..len {
                if conrelid.get(i).unwrap_or(0) == table_oid {
                    if contype.get(i) == Some("p") {
                        if let Some(n) = attnum.get(i) { pk_attnums.insert(n); }
                    }
                }
            }
        }
    }

    // Build output rows
    let mut out_rows: Vec<[String; 11]> = Vec::new();
    for (num, col, ty_key) in columns.into_iter() {
        let pk = if pk_attnums.contains(&num) { "*".to_string() } else { String::new() };
        out_rows.push([
            pk,
            col,
            String::new(),
            ty_key,
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ]);
    }
    rows_to_json(out_rows)
}

fn rows_to_json(rows: Vec<[String; 11]>) -> Result<serde_json::Value> {
    // Build DataFrame with required header order
    let hdrs = headers();
    // Prepare per-column vectors
    let mut cols: Vec<Vec<String>> = vec![Vec::new(); hdrs.len()];
    for r in rows.into_iter() {
        for (i, v) in r.into_iter().enumerate() { cols[i].push(v); }
    }
    let mut cols_vec: Vec<Column> = Vec::new();
    for (i, h) in hdrs.into_iter().enumerate() {
        cols_vec.push(Series::new(h.into(), cols[i].clone()).into());
    }
    let df = DataFrame::new(cols_vec)?;
    Ok(crate::server::exec::exec_helpers::dataframe_to_json(&df))
}

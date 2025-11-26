use crate::storage::SharedStore;
use polars::prelude::*;
use tracing::debug;
use std::cell::Cell;
use std::path::{Path, PathBuf};

// JSON helpers for stable OID persistence
fn read_json(path: &Path) -> Option<serde_json::Value> {
    std::fs::read_to_string(path).ok().and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
}

fn write_json(path: &Path, val: &serde_json::Value) {
    // Best-effort write; ignore errors to avoid breaking queries on read-only stores
    if let Ok(text) = serde_json::to_string_pretty(val) { let _ = std::fs::write(path, text); }
}

fn stable_hash_u32(s: &str) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    (h.finish() & 0x7FFF_FFFF) as u32
}

/// Obtain a stable, synthesized OID for a table, persisted in schema.json.
/// Returns a positive 32-bit integer OID in a user-space range.
fn get_or_assign_table_oid(table_dir: &Path, db: &str, schema: &str, table: &str) -> i32 {
    let sj = table_dir.join("schema.json");
    // Reserve range starting at 16384 and spread by hash
    let seed = format!("{}.{}/{}", db, schema, table);
    let default_oid = 16384 + (stable_hash_u32(&seed) % 1_000_000) as i32;
    if let Some(mut json) = read_json(&sj) {
        let obj = json.as_object_mut();
        if let Some(obj) = obj {
            // nested object to avoid clashing with user-defined keys
            let o = obj.entry("__timeline_oids__").or_insert(serde_json::json!({}));
            if let Some(map) = o.as_object() {
                if let Some(v) = map.get("class_oid").and_then(|v| v.as_i64()) {
                    return v as i32;
                }
            }
            // write back
            let mut new_map = o.as_object().cloned().unwrap_or_default();
            new_map.insert("class_oid".to_string(), serde_json::json!(default_oid));
            *o = serde_json::Value::Object(new_map);
            write_json(&sj, &serde_json::Value::Object(obj.clone()));
            return default_oid;
        }
    }
    default_oid
}

// Thread-local execution flags to avoid cross-test and cross-session interference.
// These replace the previous process-wide AtomicBools, which could cause
// intermittent failures when tests run in parallel and flip the flags.
thread_local! {
    static TLS_NULL_ON_ERROR: Cell<bool> = const { Cell::new(true) };
}
pub fn get_null_on_error() -> bool { TLS_NULL_ON_ERROR.with(|c| c.get()) }
pub fn set_null_on_error(v: bool) { TLS_NULL_ON_ERROR.with(|c| c.set(v)); }

// Projection/ORDER BY strictness flag
// When true (default), ORDER BY columns must be present in the result set at sort time.
// When false, engine may temporarily inject missing ORDER BY columns to perform sorting
// and drop them afterward in non-aggregate projection paths.
thread_local! {
    static TLS_STRICT_PROJECTION: Cell<bool> = const { Cell::new(true) };
}
pub fn get_strict_projection() -> bool { TLS_STRICT_PROJECTION.with(|c| c.get()) }
pub fn set_strict_projection(v: bool) { TLS_STRICT_PROJECTION.with(|c| c.set(v)); }

fn strip_time_ext(name: &str) -> String {
    if let Some(stripped) = name.strip_suffix(".time") { stripped.to_string() } else { name.to_string() }
}

#[derive(Debug, Clone)]
struct TableMeta { 
    db: String,
    schema: String, 
    table: String, 
    cols: Vec<(String, String)>,
    has_primary_marker: bool,  // True if schema.json contains "PRIMARY" column
    dir: PathBuf,              // full path to the table directory
}

fn enumerate_tables(store: &SharedStore) -> Vec<TableMeta> {
    let mut out: Vec<TableMeta> = Vec::new();
    let root = store.root_path();
    if let Ok(dbs) = std::fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
            let dbname = match db_path.file_name().and_then(|s| s.to_str()) { Some(n) => n.to_string(), None => continue };
            if dbname.starts_with('.') { continue; }
            if let Ok(schemas) = std::fs::read_dir(&db_path) {
                for sch_ent in schemas.flatten() {
                    let sch_path = sch_ent.path(); if !sch_path.is_dir() { continue; }
                    let schema_name = sch_path.file_name().and_then(|s| s.to_str()).unwrap_or("").to_string();
                    if schema_name.starts_with('.') { continue; }
                    if let Ok(tbls) = std::fs::read_dir(&sch_path) {
                        for tentry in tbls.flatten() {
                            let tp = tentry.path();
                            if tp.is_dir() {
                                let sj = tp.join("schema.json");
                                if sj.exists() {
                                    let tname = strip_time_ext(&tentry.file_name().to_string_lossy());
                                    let mut cols: Vec<(String, String)> = Vec::new();
                                    let mut has_primary_marker = false;
                                    if let Ok(text) = std::fs::read_to_string(&sj) {
                                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                            if let serde_json::Value::Object(obj) = json {
                                                for (k, v) in obj.into_iter() {
                                                    if k == "PRIMARY" { has_primary_marker = true; }
                                                    if let serde_json::Value::String(s) = v { cols.push((k, s)); }
                                                    else if let serde_json::Value::Object(m) = v {
                                                        if let Some(serde_json::Value::String(t)) = m.get("type") { cols.push((k, t.clone())); }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if !cols.iter().any(|(n, _)| n == "_time") { cols.insert(0, ("_time".into(), "int64".into())); }
                                    out.push(TableMeta { db: dbname.clone(), schema: schema_name.clone(), table: tname, cols, has_primary_marker, dir: tp.clone() });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    out
}


// Core-facing: materialize known system tables as DataFrame for use in queries
pub fn system_table_df(name: &str, store: &SharedStore) -> Option<DataFrame> {
    debug!(target: "timeline::system", "system_table_df: input='{}'", name);
    // Strip alias/trailing tokens after first whitespace (e.g., "pg_type t")
    let mut base = name.trim().to_string();
    if let Some(idx) = base.find(|c: char| c.is_whitespace()) { base = base[..idx].to_string(); }
    // Strip trailing semicolon if present (clients may send `... FROM pg_type;`)
    if base.ends_with(';') { base.pop(); }
    // Remove surrounding quotes if present (e.g., "dbs\\pg_type" or 'pg_type')
    if (base.starts_with('"') && base.ends_with('"')) || (base.starts_with('\'') && base.ends_with('\'')) {
        base = base[1..base.len()-1].to_string();
    }
    // Normalize name, allow arbitrary prefixes like <db>/<schema>/information_schema.tables or dbs\\pg_type
    let ident = base.replace('\\', "/").to_lowercase();
    // Convert slashes to dots for simpler suffix checks
    let dotted = ident.replace('/', ".");
    let parts: Vec<&str> = dotted.split('.').collect();
    let last1 = parts.last().copied().unwrap_or("");
    let last2 = if parts.len() >= 2 { format!("{}.{}", parts[parts.len()-2], parts[parts.len()-1]) } else { String::new() };
    debug!(target: "timeline::system", "system_table_df: normalized base='{}' dotted='{}' last1='{}' last2='{}'", base, dotted, last1, last2);

    // Helper closures to test suffix equality
    let is = |s: &str| last2 == s || last1 == s;

    // information_schema
    if is("information_schema.schemata") {
        let mut schemas: Vec<String> = Vec::new();
        let root = store.root_path();
        if let Ok(dbs) = std::fs::read_dir(&root) {
            for db_ent in dbs.flatten() {
                let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
                if let Ok(sd) = std::fs::read_dir(&db_path) {
                    for sch_ent in sd.flatten() {
                        let sch_path = sch_ent.path(); if !sch_path.is_dir() { continue; }
                        if let Some(name) = sch_path.file_name().and_then(|s| s.to_str()) {
                            if !name.starts_with('.') { schemas.push(name.to_string()); }
                        }
                    }
                }
            }
        }
        schemas.sort(); schemas.dedup();
        let series = Series::new("schema_name".into(), schemas);
        let out = DataFrame::new(vec![series.into()]).ok();
        if let Some(ref df) = out { debug!(target: "timeline::system", "system_table_df: matched information_schema.schemata rows={}", df.height()); } else { debug!(target: "timeline::system", "system_table_df: schemata build failed"); }
        return out;
    }

    if is("information_schema.tables") {
        let mut schema_col: Vec<String> = Vec::new();
        let mut table_col: Vec<String> = Vec::new();
        let mut type_col: Vec<String> = Vec::new();
        let root = store.root_path();
        if let Ok(dbs) = std::fs::read_dir(&root) {
            for db_ent in dbs.flatten() {
                let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
                if let Ok(sd) = std::fs::read_dir(&db_path) {
                    for schema_dir in sd.flatten().filter(|e| e.path().is_dir()) {
                        let schema_name = schema_dir.file_name().to_string_lossy().to_string();
                        if schema_name.starts_with('.') { continue; }
                        if let Ok(td) = std::fs::read_dir(schema_dir.path()) {
                            for tentry in td.flatten() {
                                let tp = tentry.path();
                                if tp.is_dir() {
                                    let has_schema = tp.join("schema.json").exists();
                                    let has_data = tp.join("data.parquet").exists();
                                    // Also consider chunked parquet directories as existing tables
                                    let has_chunks = std::fs::read_dir(&tp)
                                        .ok()
                                        .and_then(|iter| {
                                            for e in iter.flatten() {
                                                if let Some(name) = e.file_name().to_str() {
                                                    if name.starts_with("data-") && name.ends_with(".parquet") { return Some(true); }
                                                }
                                            }
                                            None
                                        })
                                        .unwrap_or(false);
                                    if has_schema || has_data || has_chunks {
                                        let tname = strip_time_ext(&tentry.file_name().to_string_lossy());
                                        schema_col.push(schema_name.clone());
                                        table_col.push(tname);
                                        type_col.push("BASE TABLE".to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        let df = DataFrame::new(vec![
            Series::new("table_schema".into(), schema_col).into(),
            Series::new("table_name".into(), table_col).into(),
            Series::new("table_type".into(), type_col).into(),
        ]).ok();
        return df;
    }

    if is("information_schema.columns") {
        let metas = enumerate_tables(store);
        let mut schema_col: Vec<String> = Vec::new();
        let mut table_col: Vec<String> = Vec::new();
        let mut col_name: Vec<String> = Vec::new();
        let mut ord_pos: Vec<i32> = Vec::new();
        let mut data_type: Vec<String> = Vec::new();
        let mut is_null: Vec<String> = Vec::new();
        let mut udt_name: Vec<String> = Vec::new();
        for m in metas {
            let mut ord = 1i32;
            for (cname, dtype_key) in m.cols.iter() {
                let dt = match dtype_key.as_str() { "string" | "utf8" => "text", "int64" => "bigint", _ => "double precision" };
                let udt = match dtype_key.as_str() { "string" | "utf8" => "text", "int64" => "int8", _ => "float8" };
                schema_col.push(m.schema.clone());
                table_col.push(m.table.clone());
                col_name.push(cname.clone());
                ord_pos.push(ord);
                data_type.push(dt.to_string());
                is_null.push("YES".to_string());
                udt_name.push(udt.to_string());
                ord += 1;
            }
        }
        return DataFrame::new(vec![
            Series::new("table_schema".into(), schema_col).into(),
            Series::new("table_name".into(), table_col).into(),
            Series::new("column_name".into(), col_name).into(),
            Series::new("ordinal_position".into(), ord_pos).into(),
            Series::new("data_type".into(), data_type).into(),
            Series::new("is_nullable".into(), is_null).into(),
            Series::new("udt_name".into(), udt_name).into(),
        ]).ok();
    }

    if is("information_schema.views") {
        return DataFrame::new(vec![Series::new("table_name".into(), Vec::<String>::new()).into()]).ok();
    }

    // pg_catalog.pg_type can also be referred to as just pg_type
    if last1 == "pg_type" || last2 == "pg_catalog.pg_type" {
        // Provide a richer pg_type compatible with common client expectations.
        // Columns included: oid, typname, typarray, typnamespace, typelem, typrelid, typbasetype,
        // typtypmod, typcategory, typtype.
        // Built-in types use PostgreSQL OIDs for stability; element/type relation fields are neutral.
        let names: Vec<String> = vec![
            "int4".into(),
            "int8".into(),
            "float8".into(),
            "text".into(),
            "bool".into(),
            "timestamp".into(),
            "timestamptz".into(),
        ];
        let oids: Vec<i32> = vec![23, 20, 701, 25, 16, 1114, 1184];
        let arrays: Vec<i32> = vec![1007, 1016, 1022, 1009, 1000, 1115, 1185];
        let pg_catalog_oid: i32 = 11;
        let typnamespace: Vec<i32> = vec![pg_catalog_oid; names.len()];
        // Element type OID for array types; for built-in scalar types set to 0.
        let typelem: Vec<i32> = vec![0; names.len()];
        // Relation OID for composite types; 0 for base types
        let typrelid: Vec<i32> = vec![0; names.len()];
        // Base type OID for domains; 0 for base types
        let typbasetype: Vec<i32> = vec![0; names.len()];
        // Type-specific typmod; -1 when not specified (PostgreSQL convention)
        let typtypmod: Vec<i32> = vec![-1; names.len()];
        // Category codes approximating PostgreSQL
        let typcategory: Vec<String> = vec![
            "N".into(), // int4 numeric
            "N".into(), // int8 numeric
            "N".into(), // float8 numeric
            "S".into(), // text string
            "B".into(), // bool boolean
            "D".into(), // timestamp datetime
            "D".into(), // timestamptz datetime
        ];
        // Type type: 'b' for base types
        let typtype: Vec<String> = vec!["b".into(); names.len()];

        let df = DataFrame::new(vec![
            Series::new("typname".into(), names).into(),
            Series::new("oid".into(), oids).into(),
            Series::new("typarray".into(), arrays).into(),
            Series::new("typnamespace".into(), typnamespace).into(),
            Series::new("typelem".into(), typelem).into(),
            Series::new("typrelid".into(), typrelid).into(),
            Series::new("typbasetype".into(), typbasetype).into(),
            Series::new("typtypmod".into(), typtypmod).into(),
            Series::new("typcategory".into(), typcategory).into(),
            Series::new("typtype".into(), typtype).into(),
        ]).ok();
        if let Some(ref df) = df {
            debug!(target: "timeline::system", "system_table_df: matched pg_type rows={}, cols={:?}", df.height(), df.get_column_names());
        } else {
            debug!(target: "timeline::system", "system_table_df: pg_type build failed");
        }
        return df;
    }

    if last1 == "pg_namespace" || last2 == "pg_catalog.pg_namespace" {
        // Provide minimal pg_namespace with OIDs for pg_catalog and public
        let nspname: Vec<String> = vec!["pg_catalog".into(), "public".into()];
        let oid: Vec<i32> = vec![11, 2200];
        return DataFrame::new(vec![
            Series::new("oid".into(), oid).into(),
            Series::new("nspname".into(), nspname).into(),
        ]).ok();
    }

    if last1 == "pg_attribute" || last2 == "pg_catalog.pg_attribute" {
        // Provide pg_attribute with columns needed by SQLAlchemy for constraint queries
        // attrelid: OID of the table this column belongs to
        // attname: column name
        // attnum: column number (1-based)
        let metas = enumerate_tables(store);
        let mut attrelid: Vec<i32> = Vec::new();
        let mut attname: Vec<String> = Vec::new();
        let mut attnum: Vec<i32> = Vec::new();
        
        for m in metas.iter() {
            let table_oid = get_or_assign_table_oid(&m.dir, &m.db, &m.schema, &m.table);
            let mut col_num = 1i32;
            for (cname, _dtype) in m.cols.iter() {
                // Skip PRIMARY marker column (it's metadata, not a real column)
                if cname == "PRIMARY" {
                    continue;
                }
                attrelid.push(table_oid);
                attname.push(cname.clone());
                attnum.push(col_num);
                col_num += 1;
            }
        }
        return DataFrame::new(vec![
            Series::new("attrelid".into(), attrelid).into(),
            Series::new("attname".into(), attname).into(),
            Series::new("attnum".into(), attnum).into(),
        ]).ok();
    }
    
    if last1 == "pg_constraint" || last2 == "pg_catalog.pg_constraint" {
        // Provide pg_constraint for primary key constraints
        // Timeline tables with 'primary-key': True have a PRIMARY marker column in schema.json
        let metas = enumerate_tables(store);
        let mut conrelid: Vec<i32> = Vec::new();
        let mut conname: Vec<String> = Vec::new();
        let mut contype: Vec<String> = Vec::new();
        let mut conkey: Vec<Vec<i32>> = Vec::new();
        let mut conindid: Vec<i32> = Vec::new();
        let mut oid: Vec<i32> = Vec::new();
        
        let mut constraint_oid = 20000i32; // Start constraint OIDs at 20000
        
        for m in metas.iter() {
            let table_oid = get_or_assign_table_oid(&m.dir, &m.db, &m.schema, &m.table);
            
            // If table has PRIMARY marker, identify the actual primary key column
            if m.has_primary_marker {
                let mut pk_columns: Vec<i32> = Vec::new();
                
                // Find the primary key column (typically 'id', 'record_id', or first non-system column)
                for (col_idx, (cname, _dtype)) in m.cols.iter().enumerate() {
                    // Skip _time and PRIMARY marker columns
                    if cname == "_time" || cname == "PRIMARY" {
                        continue;
                    }
                    // Look for typical primary key column names
                    if cname == "id" || cname == "record_id" || cname.ends_with("_id") {
                        pk_columns.push((col_idx + 1) as i32); // 1-based column numbering
                        break;
                    }
                }
                // If no typical PK column found, use first non-system column
                if pk_columns.is_empty() {
                    for (col_idx, (cname, _dtype)) in m.cols.iter().enumerate() {
                        if cname != "_time" && cname != "PRIMARY" {
                            pk_columns.push((col_idx + 1) as i32);
                            break;
                        }
                    }
                }
                
                // Add constraint if we found primary key columns
                if !pk_columns.is_empty() {
                    conrelid.push(table_oid);
                    conname.push(format!("{}_pkey", m.table));
                    contype.push("p".to_string()); // 'p' = primary key
                    conkey.push(pk_columns);
                    conindid.push(0); // 0 = no index (Timeline doesn't have separate index tracking)
                    oid.push(constraint_oid);
                    constraint_oid += 1;
                }
            }
        }
        
        // Convert Vec<Vec<i32>> to PostgreSQL array string format: {1,2,3}
        // This is what PostgreSQL returns for array columns and what SQLAlchemy expects
        let conkey_strings: Vec<String> = conkey.into_iter()
            .map(|v| {
                let nums: Vec<String> = v.iter().map(|n| n.to_string()).collect();
                format!("{{{}}}", nums.join(","))
            })
            .collect();
        
        return DataFrame::new(vec![
            Series::new("oid".into(), oid).into(),
            Series::new("conrelid".into(), conrelid).into(),
            Series::new("conname".into(), conname).into(),
            Series::new("contype".into(), contype).into(),
            Series::new("conkey".into(), conkey_strings).into(),
            Series::new("conindid".into(), conindid).into(),
        ]).ok();
    }
    
    // pg_constraint_columns: pre-expanded version of pg_constraint for SQLAlchemy compatibility
    // This table simulates unnest(conkey) and generate_subscripts() by providing one row per constraint column
    if last1 == "pg_constraint_columns" || last2 == "pg_catalog.pg_constraint_columns" {
        let metas = enumerate_tables(store);
        let mut oid: Vec<i32> = Vec::new();
        let mut conrelid: Vec<i32> = Vec::new();
        let mut conname: Vec<String> = Vec::new();
        let mut contype: Vec<String> = Vec::new();
        let mut attnum: Vec<i32> = Vec::new();
        let mut ord: Vec<i32> = Vec::new();
        let mut conindid: Vec<i32> = Vec::new();
        
        let mut constraint_oid = 20000i32;
        
        for m in metas.iter() {
            let table_oid = get_or_assign_table_oid(&m.dir, &m.db, &m.schema, &m.table);
            
            if m.has_primary_marker {
                let mut pk_columns: Vec<i32> = Vec::new();
                
                // Find primary key columns (same logic as pg_constraint)
                for (col_idx, (cname, _dtype)) in m.cols.iter().enumerate() {
                    if cname == "_time" || cname == "PRIMARY" { continue; }
                    if cname == "id" || cname == "record_id" || cname.ends_with("_id") {
                        pk_columns.push((col_idx + 1) as i32);
                        break;
                    }
                }
                if pk_columns.is_empty() {
                    for (col_idx, (cname, _dtype)) in m.cols.iter().enumerate() {
                        if cname != "_time" && cname != "PRIMARY" {
                            pk_columns.push((col_idx + 1) as i32);
                            break;
                        }
                    }
                }
                
                // Generate one row per column in the constraint
                if !pk_columns.is_empty() {
                    let con_name = format!("{}_pkey", m.table);
                    for (position, col_num) in pk_columns.iter().enumerate() {
                        oid.push(constraint_oid);
                        conrelid.push(table_oid);
                        conname.push(con_name.clone());
                        contype.push("p".to_string());
                        attnum.push(*col_num);
                        ord.push((position + 1) as i32); // 1-based ordering like generate_subscripts
                        conindid.push(0);
                    }
                    constraint_oid += 1;
                }
            }
        }
        
        return DataFrame::new(vec![
            Series::new("oid".into(), oid).into(),
            Series::new("conrelid".into(), conrelid).into(),
            Series::new("conname".into(), conname).into(),
            Series::new("contype".into(), contype).into(),
            Series::new("attnum".into(), attnum).into(),
            Series::new("ord".into(), ord).into(),
            Series::new("conindid".into(), conindid).into(),
        ]).ok();
    }
    
    if last1 == "pg_description" || last2 == "pg_catalog.pg_description" {
        // Provide empty pg_description with expected columns so JOINs can resolve field names.
        // Columns: objoid OID, classoid OID, objsubid int4, description text
        return DataFrame::new(vec![
            Series::new("objoid".into(), Vec::<i32>::new()).into(),
            Series::new("classoid".into(), Vec::<i32>::new()).into(),
            Series::new("objsubid".into(), Vec::<i32>::new()).into(),
            Series::new("description".into(), Vec::<String>::new()).into(),
        ]).ok();
    }

    if last1 == "pg_class" || last2 == "pg_catalog.pg_class" {
        let metas = enumerate_tables(store);
        let mut relname: Vec<String> = Vec::new();
        let mut nspname: Vec<String> = Vec::new();
        let mut relkind: Vec<String> = Vec::new();
        let mut oid: Vec<i32> = Vec::new();
        let mut relnamespace: Vec<i32> = Vec::new();
        let mut relpartbound: Vec<Option<String>> = Vec::new();
        
        // Map schema names to namespace OIDs (matching pg_namespace)
        let pg_catalog_oid: i32 = 11;
        let public_oid: i32 = 2200;
        
        for m in metas.iter() {
            relname.push(m.table.clone());
            nspname.push(m.schema.clone());
            relkind.push("r".to_string());
            // Assign/reuse synthetic OID persisted in schema.json for stability
            oid.push(get_or_assign_table_oid(&m.dir, &m.db, &m.schema, &m.table));
            // Map schema name to namespace OID
            let ns_oid = match m.schema.as_str() {
                "pg_catalog" => pg_catalog_oid,
                "public" => public_oid,
                _ => public_oid, // default to public
            };
            relnamespace.push(ns_oid);
            // relpartbound is NULL for non-partitioned tables
            relpartbound.push(None);
        }
        return DataFrame::new(vec![
            Series::new("relname".into(), relname).into(),
            Series::new("nspname".into(), nspname).into(),
            Series::new("relkind".into(), relkind).into(),
            Series::new("oid".into(), oid).into(),
            Series::new("relnamespace".into(), relnamespace).into(),
            Series::new("relpartbound".into(), relpartbound).into(),
        ]).ok();
    }

    debug!(target: "timeline::system", "system_table_df: no match for '{}' (base='{}')", name, base);
    None
}

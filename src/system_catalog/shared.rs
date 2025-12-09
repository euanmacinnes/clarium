use crate::storage::SharedStore;
use std::path::{Path, PathBuf};

/// Stable non-cryptographic hash to derive IDs deterministically
pub fn stable_hash_u32(s: &str) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    (h.finish() & 0x7FFF_FFFF) as u32
}

fn read_json(path: &Path) -> Option<serde_json::Value> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
}

fn write_json(path: &Path, val: &serde_json::Value) {
    if let Ok(text) = serde_json::to_string_pretty(val) {
        let _ = std::fs::write(path, text);
    }
}


#[derive(Debug, Clone)]
pub struct TableMeta {
    pub db: String,
    pub schema: String,
    pub table: String,
    pub cols: Vec<(String, String)>,
    pub has_primary_marker: bool,
    pub dir: PathBuf,
}

/// Enumerate user tables on disk by scanning the store root for schema.json files.
pub fn enumerate_tables(store: &SharedStore) -> Vec<TableMeta> {
    let mut out: Vec<TableMeta> = Vec::new();
    let root = store.root_path();
    if let Ok(dbs) = std::fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let db_path = db_ent.path();
            if !db_path.is_dir() { continue; }
            let dbname = match db_path.file_name().and_then(|s| s.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
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
                                    let tname = tentry.file_name().to_string_lossy().to_string();
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

#[derive(Debug, Clone)]
pub struct ViewMeta {
    pub db: String,
    pub schema: String,
    pub view: String,
    pub def_sql: String,
    pub file: PathBuf,
}

pub fn enumerate_views(store: &SharedStore) -> Vec<ViewMeta> {
    let mut out: Vec<ViewMeta> = Vec::new();
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
                    if let Ok(entries) = std::fs::read_dir(&sch_path) {
                        for e in entries.flatten() {
                            let p = e.path();
                            if p.is_file() {
                                if let Some(ext) = p.extension().and_then(|s| s.to_str()) {
                                    if ext.eq_ignore_ascii_case("view") {
                                        let vname = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                                        let mut def = String::new();
                                        if let Some(json) = read_json(&p) {
                                            if let Some(s) = json.get("definition_sql").and_then(|v| v.as_str()) { def = s.to_string(); }
                                        }
                                        out.push(ViewMeta { db: dbname.clone(), schema: schema_name.clone(), view: vname, def_sql: def, file: p.clone() });
                                    }
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

#[derive(Debug, Clone)]
pub struct SidecarMeta {
    pub db: String,
    pub schema: String,
    pub name: String,
    pub file: PathBuf,
}

pub fn enumerate_vector_indexes(store: &SharedStore) -> Vec<SidecarMeta> {
    let mut out: Vec<SidecarMeta> = Vec::new();
    let root = store.root_path();
    if let Ok(dbs) = std::fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
            let dbname = match db_path.file_name().and_then(|s| s.to_str()) { Some(n) => n.to_string(), None => continue };
            if let Ok(schemas) = std::fs::read_dir(&db_path) {
                for sch_ent in schemas.flatten() {
                    let sch_path = sch_ent.path(); if !sch_path.is_dir() { continue; }
                    let schema_name = sch_path.file_name().and_then(|s| s.to_str()).unwrap_or("").to_string();
                    if let Ok(entries) = std::fs::read_dir(&sch_path) {
                        for e in entries.flatten() {
                            let p = e.path();
                            if p.is_file() && p.extension().and_then(|s| s.to_str()).map(|x| x.eq_ignore_ascii_case("vindex")).unwrap_or(false) {
                                let name = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                                out.push(SidecarMeta { db: dbname.clone(), schema: schema_name.clone(), name, file: p.clone() });
                            }
                        }
                    }
                }
            }
        }
    }
    out
}

#[derive(Debug, Clone)]
pub struct GraphTxnCtx {
    pub graph: String,
    pub root: PathBuf,
    pub partitions: u32,
    pub hash_seed: u64,
}

pub fn enumerate_graphs(store: &SharedStore) -> Vec<SidecarMeta> {
    let mut out: Vec<SidecarMeta> = Vec::new();
    let root = store.root_path();
    if let Ok(dbs) = std::fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
            let dbname = match db_path.file_name().and_then(|s| s.to_str()) { Some(n) => n.to_string(), None => continue };
            if let Ok(schemas) = std::fs::read_dir(&db_path) {
                for sch_ent in schemas.flatten() {
                    let sch_path = sch_ent.path(); if !sch_path.is_dir() { continue; }
                    let schema_name = sch_path.file_name().and_then(|s| s.to_str()).unwrap_or("").to_string();
                    if let Ok(entries) = std::fs::read_dir(&sch_path) {
                        for e in entries.flatten() {
                            let p = e.path();
                            if p.is_file() && p.extension().and_then(|s| s.to_str()).map(|x| x.eq_ignore_ascii_case("graph")).unwrap_or(false) {
                                let name = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                                out.push(SidecarMeta { db: dbname.clone(), schema: schema_name.clone(), name, file: p.clone() });
                            }
                        }
                    }
                }
            }
        }
    }
    out
}

/// Obtain a stable, synthesized OID for a table, persisted in schema.json.
/// Returns a positive 32-bit integer OID in a user-space range.
pub fn get_or_assign_table_oid(table_dir: &Path, db: &str, schema: &str, table: &str) -> i32 {
    let sj = table_dir.join("schema.json");
    // Reserve range starting at 16384 and spread by hash
    let seed = format!("{}.{}/{}", db, schema, table);
    let default_oid = 16384 + (stable_hash_u32(&seed) % 1_000_000) as i32;
    if let Some(mut json) = read_json(&sj) {
        let obj = json.as_object_mut();
        if let Some(obj) = obj {
            // nested object to avoid clashing with user-defined keys
            let o = obj.entry("__clarium_oids__").or_insert(serde_json::json!({}));
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

/// Obtain a stable, synthesized OID for a view, persisted in the `.view` JSON file
/// under a nested `__clarium_oids__` map to avoid clashing with user keys.
pub fn get_or_assign_view_oid(view_file: &Path, db: &str, schema: &str, view: &str) -> i32 {
    // Reserve a different range for views to minimize accidental collision with tables
    // Range start 18000
    let seed = format!("view:{}.{}/{}", db, schema, view);
    let default_oid = 18000 + (stable_hash_u32(&seed) % 1_000_000) as i32;
    if let Some(mut json) = read_json(view_file) {
        if let Some(obj) = json.as_object_mut() {
            let o = obj.entry("__clarium_oids__").or_insert(serde_json::json!({}));
            if let Some(map) = o.as_object() {
                if let Some(v) = map.get("class_oid").and_then(|v| v.as_i64()) {
                    return v as i32;
                }
            }
            // write back
            let mut new_map = o.as_object().cloned().unwrap_or_default();
            new_map.insert("class_oid".to_string(), serde_json::json!(default_oid));
            *o = serde_json::Value::Object(new_map);
            write_json(view_file, &serde_json::Value::Object(obj.clone()));
            return default_oid;
        }
    }
    default_oid
}

/// Obtain a stable OID for a vector index, persisted inside the `.vindex` JSON file
pub fn get_or_assign_vindex_oid(vindex_file: &Path, db: &str, schema: &str, name: &str) -> i32 {
    // Reserve a separate range for vector indexes to avoid collision
    // Range start 22000
    let seed = format!("vindex:{}.{}/{}", db, schema, name);
    let default_oid = 22000 + (stable_hash_u32(&seed) % 1_000_000) as i32;
    if let Some(mut json) = read_json(vindex_file) {
        if let Some(obj) = json.as_object_mut() {
            let o = obj.entry("__clarium_oids__").or_insert(serde_json::json!({}));
            if let Some(map) = o.as_object() {
                if let Some(v) = map.get("class_oid").and_then(|v| v.as_i64()) { return v as i32; }
            }
            let mut new_map = o.as_object().cloned().unwrap_or_default();
            new_map.insert("class_oid".to_string(), serde_json::json!(default_oid));
            *o = serde_json::Value::Object(new_map);
            write_json(vindex_file, &serde_json::Value::Object(obj.clone()));
            return default_oid;
        }
    }
    default_oid
}

/// Obtain a stable OID for a graph catalog, persisted inside the `.graph` JSON file
pub fn get_or_assign_graph_oid(graph_file: &Path, db: &str, schema: &str, name: &str) -> i32 {
    // Reserve a separate range for graphs
    // Range start 23000
    let seed = format!("graph:{}.{}/{}", db, schema, name);
    let default_oid = 23000 + (stable_hash_u32(&seed) % 1_000_000) as i32;
    if let Some(mut json) = read_json(graph_file) {
        if let Some(obj) = json.as_object_mut() {
            let o = obj.entry("__clarium_oids__").or_insert(serde_json::json!({}));
            if let Some(map) = o.as_object() {
                if let Some(v) = map.get("class_oid").and_then(|v| v.as_i64()) { return v as i32; }
            }
            let mut new_map = o.as_object().cloned().unwrap_or_default();
            new_map.insert("class_oid".to_string(), serde_json::json!(default_oid));
            *o = serde_json::Value::Object(new_map);
            write_json(graph_file, &serde_json::Value::Object(obj.clone()));
            return default_oid;
        }
    }
    default_oid
}


/// Lookup a view definition by its OID. Returns Some(definition_sql) or None
/// if OID is not found or maps to a non-view object.
pub fn lookup_view_definition_by_oid(store: &SharedStore, oid: i32) -> Option<String> {
    let views = enumerate_views(store);
    for v in views {
        let vid = get_or_assign_view_oid(&v.file, &v.db, &v.schema, &v.view);
        if vid == oid {
            return Some(v.def_sql);
        }
    }
    None
}
use crate::storage::SharedStore;
use polars::prelude::*;
use tracing::debug;
use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::cell::RefCell;
use crate::tprintln;

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
pub(crate) fn get_or_assign_view_oid(view_file: &Path, db: &str, schema: &str, view: &str) -> i32 {
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
fn get_or_assign_vindex_oid(vindex_file: &Path, db: &str, schema: &str, name: &str) -> i32 {
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
fn get_or_assign_graph_oid(graph_file: &Path, db: &str, schema: &str, name: &str) -> i32 {
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

// ----------------------------
// Vector search configuration
// ----------------------------
// Defaults (can be overridden via CLI/config in future; for now use thread-local with sensible defaults)
thread_local! {
    static TLS_VECTOR_EF_SEARCH: Cell<i32> = const { Cell::new(64) };      // query-time HNSW ef_search
    static TLS_VECTOR_HNSW_M: Cell<i32> = const { Cell::new(32) };         // HNSW M (graph degree)
    static TLS_VECTOR_HNSW_EF_BUILD: Cell<i32> = const { Cell::new(200) }; // HNSW ef_build
    static TLS_VECTOR_PRESELECT_ALPHA: Cell<i32> = const { Cell::new(8) }; // ANN preselect alpha (W = alpha * k)
}

pub fn get_vector_ef_search() -> i32 { TLS_VECTOR_EF_SEARCH.with(|c| c.get()) }
pub fn set_vector_ef_search(v: i32) { TLS_VECTOR_EF_SEARCH.with(|c| c.set(v)); }

pub fn get_vector_hnsw_m() -> i32 { TLS_VECTOR_HNSW_M.with(|c| c.get()) }
pub fn set_vector_hnsw_m(v: i32) { TLS_VECTOR_HNSW_M.with(|c| c.set(v)); }

pub fn get_vector_hnsw_ef_build() -> i32 { TLS_VECTOR_HNSW_EF_BUILD.with(|c| c.get()) }
pub fn set_vector_hnsw_ef_build(v: i32) { TLS_VECTOR_HNSW_EF_BUILD.with(|c| c.set(v)); }

/// ANN preselect alpha knob (W = alpha * k). Used by ORDER BY ANN two-phase path when LIMIT is absent.
pub fn get_vector_preselect_alpha() -> i32 { TLS_VECTOR_PRESELECT_ALPHA.with(|c| c.get()) }
pub fn set_vector_preselect_alpha(v: i32) { TLS_VECTOR_PRESELECT_ALPHA.with(|c| c.set(v.max(1))); }

/// Helper to accept common SET variable aliases (case-insensitive) for vector knobs
pub fn apply_vector_setting(var: &str, val: &str) -> bool {
    let up = var.to_ascii_lowercase();
    // Accept both dots and underscores as separators
    match up.as_str() {
        "vector.search.ef_search" | "vector_ef_search" | "vector.search.efsearch" => {
            if let Ok(n) = val.parse::<i32>() { set_vector_ef_search(n); return true; }
            return false;
        }
        "vector.hnsw.m" | "vector_hnsw_m" | "vector.m" => {
            if let Ok(n) = val.parse::<i32>() { set_vector_hnsw_m(n); return true; }
            return false;
        }
        "vector.hnsw.ef_build" | "vector_hnsw_ef_build" | "vector.ef_build" => {
            if let Ok(n) = val.parse::<i32>() { set_vector_hnsw_ef_build(n); return true; }
            return false;
        }
        // Two-phase ANN preselect multiplier
        "vector.preselect_alpha" | "vector.preselect.alpha" | "vector_preselect_alpha" | "vector.ann.preselect_alpha" => {
            if let Ok(n) = val.parse::<i32>() { set_vector_preselect_alpha(n); return true; }
            return false;
        }
        _ => false,
    }
}

// Thread-local current database/schema for session-aware qualification (per-thread/session)
thread_local! {
    static TLS_CURRENT_DB: Cell<Option<String>> = const { Cell::new(None) };
}
thread_local! {
    static TLS_CURRENT_SCHEMA: Cell<Option<String>> = const { Cell::new(None) };
}

// Thread-local current GRAPH (qualified path db/schema/name)
thread_local! {
    static TLS_CURRENT_GRAPH: Cell<Option<String>> = const { Cell::new(None) };
}

/// Get current database name for this thread/session, or default configured database
pub fn get_current_database() -> String {
    TLS_CURRENT_DB.with(|c| c.take()).map(|s| { TLS_CURRENT_DB.with(|c2| c2.set(Some(s.clone()))); s })
        .unwrap_or_else(|| crate::ident::DEFAULT_DB.to_string())
}

/// Optional getter: returns None when current database is unset for this thread/session
pub fn get_current_database_opt() -> Option<String> {
    TLS_CURRENT_DB.with(|c| c.take()).map(|s| { TLS_CURRENT_DB.with(|c2| c2.set(Some(s.clone()))); s })
}

/// Get current schema name for this thread/session, or default configured schema
pub fn get_current_schema() -> String {
    TLS_CURRENT_SCHEMA.with(|c| c.take()).map(|s| { TLS_CURRENT_SCHEMA.with(|c2| c2.set(Some(s.clone()))); s })
        .unwrap_or_else(|| crate::ident::DEFAULT_SCHEMA.to_string())
}

/// Optional getter: returns None when current schema is unset for this thread/session
pub fn get_current_schema_opt() -> Option<String> {
    TLS_CURRENT_SCHEMA.with(|c| c.take()).map(|s| { TLS_CURRENT_SCHEMA.with(|c2| c2.set(Some(s.clone()))); s })
}

/// Set current database for this thread/session
pub fn set_current_database(db: &str) { tprintln!("[system] setting current database to {}", db); TLS_CURRENT_DB.with(|c| c.set(Some(db.to_string()))); }

/// Set current schema for this thread/session
pub fn set_current_schema(schema: &str) { tprintln!("[system] setting current schema database to {}", schema); TLS_CURRENT_SCHEMA.with(|c| c.set(Some(schema.to_string()))); }

/// Unset current database (and by extension, schema) for this thread/session (so helpers can treat it as NONE)
pub fn unset_current_database() {
    unset_current_schema();
    TLS_CURRENT_DB.with(|c| c.set(None));
}

/// Unset current schema for this thread/session (so helpers can treat it as NONE)
pub fn unset_current_schema() { TLS_CURRENT_SCHEMA.with(|c| c.set(None)); }

/// Set current graph (qualified: db/schema/name) for this thread/session
pub fn set_current_graph(graph: &str) { TLS_CURRENT_GRAPH.with(|c| c.set(Some(graph.to_string()))); }

/// Unset current graph for this thread/session
pub fn unset_current_graph() { TLS_CURRENT_GRAPH.with(|c| c.set(None)); }

/// Get current graph if set for this thread/session
pub fn get_current_graph_opt() -> Option<String> {
    TLS_CURRENT_GRAPH.with(|c| c.take()).map(|s| { TLS_CURRENT_GRAPH.with(|c2| c2.set(Some(s.clone()))); s })
}

// ----------------------------
// GraphStore transactional TLS
// ----------------------------

#[derive(Debug, Clone)]
pub struct GraphTxnCtx {
    pub graph: String,
    pub root: PathBuf,
    pub partitions: u32,
    pub hash_seed: u64,
}

thread_local! {
    static TLS_GRAPH_TXN: RefCell<Option<crate::server::graphstore::txn::GraphTxn>> = const { RefCell::new(None) };
}
thread_local! {
    static TLS_GRAPH_TXN_CTX: RefCell<Option<GraphTxnCtx>> = const { RefCell::new(None) };
}

pub fn set_graph_txn(tx: crate::server::graphstore::txn::GraphTxn, ctx: GraphTxnCtx) {
    TLS_GRAPH_TXN.with(|c| *c.borrow_mut() = Some(tx));
    TLS_GRAPH_TXN_CTX.with(|c| *c.borrow_mut() = Some(ctx));
}

pub fn take_graph_txn() -> Option<crate::server::graphstore::txn::GraphTxn> {
    TLS_GRAPH_TXN.with(|c| c.borrow_mut().take())
}

pub fn peek_graph_txn_active() -> bool {
    TLS_GRAPH_TXN.with(|c| c.borrow().is_some())
}

pub fn get_graph_txn_ctx() -> Option<GraphTxnCtx> {
    TLS_GRAPH_TXN_CTX.with(|c| c.borrow().clone())
}

pub fn clear_graph_txn() {
    TLS_GRAPH_TXN.with(|c| *c.borrow_mut() = None);
    TLS_GRAPH_TXN_CTX.with(|c| *c.borrow_mut() = None);
}

/// Helper to obtain QueryDefaults from current thread-local session values
pub fn current_query_defaults() -> crate::ident::QueryDefaults {
    let db = get_current_database();
    let schema = get_current_schema();
    crate::ident::QueryDefaults::new(db, schema)
}

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

#[derive(Debug, Clone)]
struct ViewMeta {
    db: String,
    schema: String,
    view: String,
    def_sql: String,
    file: PathBuf,
}

fn enumerate_views(store: &SharedStore) -> Vec<ViewMeta> {
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
                                        // read def
                                        let mut def = String::new();
                                        if let Some(json) = read_json(&p) {
                                            if let Some(s) = json.get("definition_sql").and_then(|v| v.as_str()) {
                                                def = s.to_string();
                                            }
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
struct SidecarMeta {
    db: String,
    schema: String,
    name: String,
    file: PathBuf,
}

fn enumerate_vector_indexes(store: &SharedStore) -> Vec<SidecarMeta> {
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

fn enumerate_graphs(store: &SharedStore) -> Vec<SidecarMeta> {
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


// Core-facing: materialize known system tables as DataFrame for use in queries
pub fn system_table_df(name: &str, store: &SharedStore) -> Option<DataFrame> {
    debug!(target: "clarium::system", "system_table_df: input='{}'", name);
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
    debug!(target: "clarium::system", "system_table_df: normalized base='{}' dotted='{}' last1='{}' last2='{}'", base, dotted, last1, last2);

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
        if let Some(ref df) = out { debug!(target: "clarium::system", "system_table_df: matched information_schema.schemata rows={}", df.height()); } else { debug!(target: "clarium::system", "system_table_df: schemata build failed"); }
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
        // List views by scanning for .view files under db/schema folders
        let root = store.root_path();
        let mut schemas: Vec<String> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        let mut defs: Vec<String> = Vec::new();
        if let Ok(dbs) = std::fs::read_dir(&root) {
            for db_ent in dbs.flatten() {
                let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
                if db_ent.file_name().to_string_lossy().starts_with('.') { continue; }
                if let Ok(schemas_dir) = std::fs::read_dir(&db_path) {
                    for sch_ent in schemas_dir.flatten() {
                        let sch_path = sch_ent.path(); if !sch_path.is_dir() { continue; }
                        let schema_name = sch_ent.file_name().to_string_lossy().to_string();
                        if schema_name.starts_with('.') { continue; }
                        if let Ok(entries) = std::fs::read_dir(&sch_path) {
                            for e in entries.flatten() {
                                let p = e.path();
                                if p.is_file() {
                                    if let Some(ext) = p.extension().and_then(|s| s.to_str()) {
                                        if ext.eq_ignore_ascii_case("view") {
                                            let tname = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                                            // Read definition for convenience
                                            let mut def = String::new();
                                            if let Ok(text) = std::fs::read_to_string(&p) {
                                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                    if let Some(s) = json.get("definition_sql").and_then(|v| v.as_str()) {
                                                        def = s.to_string();
                                                    }
                                                }
                                            }
                                            schemas.push(schema_name.clone());
                                            names.push(tname);
                                            defs.push(def);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return DataFrame::new(vec![
            Series::new("table_schema".into(), schemas).into(),
            Series::new("table_name".into(), names).into(),
            Series::new("view_definition".into(), defs).into(),
        ]).ok();
    }

    // pg_catalog.pg_views compatibility
    if last1 == "pg_views" || last2 == "pg_catalog.pg_views" {
        let views = enumerate_views(store);
        let mut schemaname: Vec<String> = Vec::new();
        let mut viewname: Vec<String> = Vec::new();
        let mut definition: Vec<String> = Vec::new();
        for v in views {
            schemaname.push(v.schema);
            viewname.push(v.view);
            definition.push(v.def_sql);
        }
        return DataFrame::new(vec![
            Series::new("schemaname".into(), schemaname).into(),
            Series::new("viewname".into(), viewname).into(),
            Series::new("definition".into(), definition).into(),
        ]).ok();
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
            "hstore".into(),
            "vector".into(),
        ];
        let oids: Vec<i32> = vec![23, 20, 701, 25, 16, 1114, 1184, 16414, 70400];
        let arrays: Vec<i32> = vec![1007, 1016, 1022, 1009, 1000, 1115, 1185, 16415, 70401];
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
            "U".into(), // hstore user-defined
            "U".into(), // vector user-defined
        ];
        // Type type: 'b' for base types
        let typtype: Vec<String> = vec!["b".into(); names.len()];
        // Type delimiter used in arrays; comma for most types
        let typdelim: Vec<String> = vec![",".into(); names.len()];

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
            Series::new("typdelim".into(), typdelim).into(),
        ]).ok();
        if let Some(ref df) = df {
            debug!(target: "clarium::system", "system_table_df: matched pg_type rows={}, cols={:?}", df.height(), df.get_column_names());
        } else {
            debug!(target: "clarium::system", "system_table_df: pg_type build failed");
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

    // pg_catalog.pg_roles compatibility (a view in PostgreSQL)
    if last1 == "pg_roles" || last2 == "pg_catalog.pg_roles" {
        // Minimal subset of columns commonly used by clients and our tests
        let oid: Vec<i32> = vec![10];
        let rolname: Vec<String> = vec!["postgres".into()];
        let rolsuper: Vec<bool> = vec![true];
        let rolinherit: Vec<bool> = vec![true];
        let rolcreaterole: Vec<bool> = vec![true];
        let rolcreatedb: Vec<bool> = vec![true];
        let rolcanlogin: Vec<bool> = vec![true];
        let rolreplication: Vec<bool> = vec![false];
        let rolconnlimit: Vec<i32> = vec![-1];
        let rolpassword: Vec<Option<String>> = vec![None];
        let rolvaliduntil: Vec<Option<String>> = vec![None];
        let rolbypassrls: Vec<bool> = vec![true];

        return DataFrame::new(vec![
            Series::new("oid".into(), oid).into(),
            Series::new("rolname".into(), rolname).into(),
            Series::new("rolsuper".into(), rolsuper).into(),
            Series::new("rolinherit".into(), rolinherit).into(),
            Series::new("rolcreaterole".into(), rolcreaterole).into(),
            Series::new("rolcreatedb".into(), rolcreatedb).into(),
            Series::new("rolcanlogin".into(), rolcanlogin).into(),
            Series::new("rolreplication".into(), rolreplication).into(),
            Series::new("rolconnlimit".into(), rolconnlimit).into(),
            Series::new("rolpassword".into(), rolpassword).into(),
            Series::new("rolvaliduntil".into(), rolvaliduntil).into(),
            Series::new("rolbypassrls".into(), rolbypassrls).into(),
        ]).ok();
    }

    // pg_catalog.pg_database compatibility
    if last1 == "pg_database" || last2 == "pg_catalog.pg_database" {
        // Enumerate databases by listing first-level directories under the db root.
        let root = store.root_path();
        let mut names: Vec<String> = Vec::new();
        if let Ok(dbs) = std::fs::read_dir(&root) {
            for ent in dbs.flatten() {
                let p = ent.path();
                if p.is_dir() {
                    if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                        if !name.starts_with('.') {
                            names.push(name.to_string());
                        }
                    }
                }
            }
        }
        // Fallback: if no dbs found, expose a default logical database "clarium"
        if names.is_empty() { names.push("clarium".to_string()); }

        // Synthesize columns commonly used by clients/ORMs
        // oid: stable positive OID derived from name
        let oids: Vec<i32> = names.iter().map(|n| {
            // Reserve a range starting at 20000 for database OIDs
            20000 + (stable_hash_u32(&format!("db:{}", n)) % 1_000_000) as i32
        }).collect();
        // datdba: arbitrary stable owner OID (10)
        let datdba: Vec<i32> = vec![10; names.len()];
        // encoding: 6 corresponds to UTF8 in PostgreSQL catalogs
        let encoding: Vec<i32> = vec![6; names.len()];
        // locale/collation placeholders
        let datcollate: Vec<String> = vec!["en_US.UTF-8".into(); names.len()];
        let datctype: Vec<String> = vec!["en_US.UTF-8".into(); names.len()];
        // template/connection flags
        let datistemplate: Vec<bool> = vec![false; names.len()];
        let datallowconn: Vec<bool> = vec![true; names.len()];
        // connection limit: -1 = no limit
        let datconnlimit: Vec<i32> = vec![-1; names.len()];

        let df = DataFrame::new(vec![
            Series::new("oid".into(), oids).into(),
            Series::new("datname".into(), names).into(),
            Series::new("datdba".into(), datdba).into(),
            Series::new("encoding".into(), encoding).into(),
            Series::new("datcollate".into(), datcollate).into(),
            Series::new("datctype".into(), datctype).into(),
            Series::new("datistemplate".into(), datistemplate).into(),
            Series::new("datallowconn".into(), datallowconn).into(),
            Series::new("datconnlimit".into(), datconnlimit).into(),
        ]).ok();
        if let Some(ref df) = df {
            debug!(target: "clarium::system", "system_table_df: matched pg_database rows={}", df.height());
        } else {
            debug!(target: "clarium::system", "system_table_df: pg_database build failed");
        }
        return df;
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
        let mut attisdropped: Vec<bool> = Vec::new();
        
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
                attisdropped.push(false);
                col_num += 1;
            }
        }
        return DataFrame::new(vec![
            Series::new("attrelid".into(), attrelid).into(),
            Series::new("attname".into(), attname).into(),
            Series::new("attnum".into(), attnum).into(),
            Series::new("attisdropped".into(), attisdropped).into(),
        ]).ok();
    }

    // Minimal support for pg_attrdef so JOINs in client metadata queries can resolve
    if last1 == "pg_attrdef" || last2 == "pg_catalog.pg_attrdef" {
        // Columns commonly used by clients: adrelid (OID), adnum (int4), adbin (text)
        return DataFrame::new(vec![
            Series::new("adrelid".into(), Vec::<i32>::new()).into(),
            Series::new("adnum".into(), Vec::<i32>::new()).into(),
            Series::new("adbin".into(), Vec::<String>::new()).into(),
        ]).ok();
    }
    
    if last1 == "pg_constraint" || last2 == "pg_catalog.pg_constraint" {
        // Provide pg_constraint for primary key constraints
        // clarium tables with 'primary-key': True have a PRIMARY marker column in schema.json
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
                    conindid.push(0); // 0 = no index (clarium doesn't have separate index tracking)
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

    // Minimal support for pg_depend used by some DBeaver queries
    if last1 == "pg_depend" || last2 == "pg_catalog.pg_depend" {
        // Provide empty table with expected columns referenced by queries
        // refobjid OID, refobjsubid int4, classid OID, refclassid OID, objid OID, deptype char/text
        return DataFrame::new(vec![
            Series::new("refobjid".into(), Vec::<i32>::new()).into(),
            Series::new("refobjsubid".into(), Vec::<i32>::new()).into(),
            Series::new("classid".into(), Vec::<i32>::new()).into(),
            Series::new("refclassid".into(), Vec::<i32>::new()).into(),
            Series::new("objid".into(), Vec::<i32>::new()).into(),
            Series::new("deptype".into(), Vec::<String>::new()).into(),
        ]).ok();
    }

    if last1 == "pg_class" || last2 == "pg_catalog.pg_class" {
        let metas = enumerate_tables(store);
        let vmetas = enumerate_views(store);
        let idxs = enumerate_vector_indexes(store);
        let graphs = enumerate_graphs(store);
        let mut relname: Vec<String> = Vec::new();
        let mut nspname: Vec<String> = Vec::new();
        let mut relkind: Vec<String> = Vec::new();
        let mut oid: Vec<i32> = Vec::new();
        let mut relnamespace: Vec<i32> = Vec::new();
        let mut relpartbound: Vec<Option<String>> = Vec::new();
        
        // Map schema names to namespace OIDs (matching pg_namespace)
        let pg_catalog_oid: i32 = 11;
        let public_oid: i32 = 2200;
        let ns_oid_for = |schema: &str| -> i32 {
            match schema {
                "pg_catalog" => pg_catalog_oid,
                "public" => public_oid,
                _ => public_oid,
            }
        };
        
        for m in metas.iter() {
            relname.push(m.table.clone());
            nspname.push(m.schema.clone());
            relkind.push("r".to_string());
            oid.push(get_or_assign_table_oid(&m.dir, &m.db, &m.schema, &m.table));
            relnamespace.push(ns_oid_for(&m.schema));
            relpartbound.push(None);
        }
        for v in vmetas.iter() {
            relname.push(v.view.clone());
            nspname.push(v.schema.clone());
            relkind.push("v".to_string());
            oid.push(get_or_assign_view_oid(&v.file, &v.db, &v.schema, &v.view));
            relnamespace.push(ns_oid_for(&v.schema));
            relpartbound.push(None);
        }
        // Vector indexes as relkind 'i'
        for x in idxs.iter() {
            relname.push(x.name.clone());
            nspname.push(x.schema.clone());
            relkind.push("i".to_string());
            oid.push(get_or_assign_vindex_oid(&x.file, &x.db, &x.schema, &x.name));
            relnamespace.push(ns_oid_for(&x.schema));
            relpartbound.push(None);
        }
        // Graph catalogs – expose as views (relkind 'v') for client compatibility
        for g in graphs.iter() {
            relname.push(g.name.clone());
            nspname.push(g.schema.clone());
            relkind.push("v".to_string());
            oid.push(get_or_assign_graph_oid(&g.file, &g.db, &g.schema, &g.name));
            relnamespace.push(ns_oid_for(&g.schema));
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

    // Minimal pg_catalog.pg_shdescription (shared descriptions) to support joins from role/database
    // Columns: objoid OID, classoid OID, description text
    if last1 == "pg_shdescription" || last2 == "pg_catalog.pg_shdescription" {
        return DataFrame::new(vec![
            Series::new("objoid".into(), Vec::<i32>::new()).into(),
            Series::new("classoid".into(), Vec::<i32>::new()).into(),
            Series::new("description".into(), Vec::<String>::new()).into(),
        ]).ok();
    }

    debug!(target: "clarium::system", "system_table_df: no match for '{}' (base='{}')", name, base);
    None
}

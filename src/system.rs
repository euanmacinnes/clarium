use crate::storage::SharedStore;
use polars::prelude::*;
use tracing::debug;
use std::cell::Cell;
use crate::system_catalog::shared::*;
use std::cell::RefCell;
use crate::tprintln;


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

    // New registry-based router: allow class-based system tables to handle requests first.
    // Keep existing debug! logs; new code uses tprintln! inside modules.
    {
        use crate::system_catalog::registry as sysreg;
        sysreg::ensure_registered();
        // Determine schema and table names
        let (schema, table) = if last2.contains('.') {
            let mut it = last2.split('.');
            (it.next().unwrap_or(""), it.next().unwrap_or(""))
        } else {
            // If only a bare name, try pg_catalog first, then information_schema
            ("pg_catalog", last1)
        };
        if let Some(cls) = sysreg::find(schema, table) {
            if let Some(df) = cls.build(store) {
                return Some(df);
            }
        }
    }

    // Registry is the single dispatch path for system catalog tables now.

    // Helper closures to test suffix equality
    let is = |s: &str| last2 == s || last1 == s;

    // Schema-only compatibility tables for pg_catalog
    if last1 == "pg_am" || last2 == "pg_catalog.pg_am" {
        tprintln!("[loader] pg_am schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("amname".into(), Vec::<String>::new()).into(),
            Series::new("amhandler".into(), Vec::<i32>::new()).into(),
            Series::new("amtype".into(), Vec::<String>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_amop" || last2 == "pg_catalog.pg_amop" {
        tprintln!("[loader] pg_amop schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("amopfamily".into(), Vec::<i32>::new()).into(),
            Series::new("amoplefttype".into(), Vec::<i32>::new()).into(),
            Series::new("amoprighttype".into(), Vec::<i32>::new()).into(),
            Series::new("amopstrategy".into(), Vec::<i32>::new()).into(),
            Series::new("amoppurpose".into(), Vec::<String>::new()).into(),
            Series::new("amopopr".into(), Vec::<i32>::new()).into(),
            Series::new("amopmethod".into(), Vec::<i32>::new()).into(),
            Series::new("amopsortfamily".into(), Vec::<i32>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_amproc" || last2 == "pg_catalog.pg_amproc" {
        tprintln!("[loader] pg_amproc schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("amprocfamily".into(), Vec::<i32>::new()).into(),
            Series::new("amproclefttype".into(), Vec::<i32>::new()).into(),
            Series::new("amprocrighttype".into(), Vec::<i32>::new()).into(),
            Series::new("amprocnum".into(), Vec::<i32>::new()).into(),
            Series::new("amproc".into(), Vec::<i32>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_operator" || last2 == "pg_catalog.pg_operator" {
        tprintln!("[loader] pg_operator schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("oprname".into(), Vec::<String>::new()).into(),
            Series::new("oprnamespace".into(), Vec::<i32>::new()).into(),
            Series::new("oprleft".into(), Vec::<i32>::new()).into(),
            Series::new("oprright".into(), Vec::<i32>::new()).into(),
            Series::new("oprresult".into(), Vec::<i32>::new()).into(),
            Series::new("oprcom".into(), Vec::<i32>::new()).into(),
            Series::new("oprnegate".into(), Vec::<i32>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_opclass" || last2 == "pg_catalog.pg_opclass" {
        tprintln!("[loader] pg_opclass schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("opcname".into(), Vec::<String>::new()).into(),
            Series::new("opcnamespace".into(), Vec::<i32>::new()).into(),
            Series::new("opcmethod".into(), Vec::<i32>::new()).into(),
            Series::new("opcintype".into(), Vec::<i32>::new()).into(),
            Series::new("opckeytype".into(), Vec::<i32>::new()).into(),
            Series::new("opcdefault".into(), Vec::<String>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_opfamily" || last2 == "pg_catalog.pg_opfamily" {
        tprintln!("[loader] pg_opfamily schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("opfname".into(), Vec::<String>::new()).into(),
            Series::new("opfnamespace".into(), Vec::<i32>::new()).into(),
            Series::new("opfmethod".into(), Vec::<i32>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_collation" || last2 == "pg_catalog.pg_collation" {
        tprintln!("[loader] pg_collation schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("collname".into(), Vec::<String>::new()).into(),
            Series::new("collnamespace".into(), Vec::<i32>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_conversion" || last2 == "pg_catalog.pg_conversion" {
        tprintln!("[loader] pg_conversion schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("conname".into(), Vec::<String>::new()).into(),
            Series::new("connamespace".into(), Vec::<i32>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_language" || last2 == "pg_catalog.pg_language" {
        tprintln!("[loader] pg_language schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("lanname".into(), Vec::<String>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_index" || last2 == "pg_catalog.pg_index" {
        tprintln!("[loader] pg_index schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("indexrelid".into(), Vec::<i32>::new()).into(),
            Series::new("indrelid".into(), Vec::<i32>::new()).into(),
            Series::new("indisunique".into(), Vec::<bool>::new()).into(),
            Series::new("indisprimary".into(), Vec::<bool>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_inherits" || last2 == "pg_catalog.pg_inherits" {
        tprintln!("[loader] pg_inherits schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("inhrelid".into(), Vec::<i32>::new()).into(),
            Series::new("inhparent".into(), Vec::<i32>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_rewrite" || last2 == "pg_catalog.pg_rewrite" {
        tprintln!("[loader] pg_rewrite schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("ev_class".into(), Vec::<i32>::new()).into(),
            Series::new("rulename".into(), Vec::<String>::new()).into(),
            Series::new("ev_type".into(), Vec::<String>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_trigger" || last2 == "pg_catalog.pg_trigger" {
        tprintln!("[loader] pg_trigger schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("tgrelid".into(), Vec::<i32>::new()).into(),
            Series::new("tgname".into(), Vec::<String>::new()).into(),
            Series::new("tgenabled".into(), Vec::<String>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_tablespace" || last2 == "pg_catalog.pg_tablespace" {
        tprintln!("[loader] pg_tablespace schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("spcname".into(), Vec::<String>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_cast" || last2 == "pg_catalog.pg_cast" {
        tprintln!("[loader] pg_cast schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("castsource".into(), Vec::<i32>::new()).into(),
            Series::new("casttarget".into(), Vec::<i32>::new()).into(),
            Series::new("castfunc".into(), Vec::<i32>::new()).into(),
            Series::new("castcontext".into(), Vec::<String>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_enum" || last2 == "pg_catalog.pg_enum" {
        tprintln!("[loader] pg_enum schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("enumtypid".into(), Vec::<i32>::new()).into(),
            Series::new("enumlabel".into(), Vec::<String>::new()).into(),
        ]).ok();
    }
    if last1 == "pg_range" || last2 == "pg_catalog.pg_range" {
        tprintln!("[loader] pg_range schema requested; returning empty set");
        return DataFrame::new(vec![
            Series::new("rngtypid".into(), Vec::<i32>::new()).into(),
            Series::new("rngsubtype".into(), Vec::<i32>::new()).into(),
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

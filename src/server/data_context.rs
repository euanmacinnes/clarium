//! DataContext: tracks sources (FROM/USING/JOIN) and resolves column names

use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::cell::RefCell;

use anyhow::Result;
use polars::prelude::DataFrame;
use tracing::debug;

use crate::query::TableRef;

/// Execution pipeline stages for SELECT processing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SelectStage {
    FromWhere,
    ByOrGroupBy,
    Rolling,
    ProjectSelect,
    OrderLimit,
    Having,
}

#[derive(Clone)]
pub struct DataContext {
    /// All sources participating in the current query or clause
    pub sources: Vec<TableRef>,
    /// alias -> canonical name
    pub alias_to_name: HashMap<String, String>,
    /// Parent query sources for nested subquery correlation.
    /// When a subquery executes, this holds the sources from all outer query levels,
    /// allowing proper identification of which columns should be correlated.
    pub parent_sources: Vec<TableRef>,
    /// Current defaults for resolving unqualified table identifiers
    pub current_database: Option<String>,
    pub current_schema: Option<String>,
    /// Current session user and database user for PostgreSQL compatibility
    pub current_user: Option<String>,
    pub session_user: Option<String>,
    /// Query execution timestamps (set once at query start)
    pub transaction_timestamp: Option<std::time::SystemTime>,
    pub statement_timestamp: Option<std::time::SystemTime>,
    /// Per-stage materialized column names
    pub stage_columns: HashMap<SelectStage, HashSet<String>>,
    /// User-generated columns (e.g., from UDFs) per stage
    pub stage_user_columns: HashMap<SelectStage, HashSet<String>>,
    /// Query-scoped script registry snapshot.
    /// Captured at query start to provide stable, isolated UDF resolution
    /// throughout execution, immune to global registry changes.
    pub script_registry: Option<crate::scripts::ScriptRegistry>,
    /// Query-scoped prepared Lua VM reused for all UDF calls in this query.
    /// Stored here to make the Lua state explicitly query-specific.
    pub query_lua: Rc<RefCell<Option<crate::scripts::PreparedLua>>>,
    /// Optional handle to storage so certain scalar helpers can resolve metadata
    /// (e.g., pg_get_viewdef) during expression compilation.
    pub store: Option<crate::storage::SharedStore>,
    /// CTE (Common Table Expression) results indexed by name
    pub cte_tables: HashMap<String, DataFrame>,
    /// Temporary ORDER BY columns added for sorting but not in original SELECT projection
    pub temp_order_by_columns: HashSet<String>,
}

impl Default for DataContext {
    fn default() -> Self {
        Self::new()
    }
}

impl DataContext {
    pub fn new() -> Self { Self::with_defaults("clarium", "public") }

    /// Register materialized columns for a stage from a DataFrame
    pub fn register_df_columns_for_stage(&mut self, stage: SelectStage, df: &DataFrame) {
        let entry = self.stage_columns.entry(stage).or_default();
        for c in df.get_column_names() { entry.insert(c.to_string()); }
    }

    /// Resolve a PostgreSQL regclass cast input (text relation name) to a stable OID (Int32).
    /// Rules:
    /// - Recognize common system catalogs with fixed OIDs: pg_class(1259), pg_type(1247), pg_namespace(2615).
    /// - Accept optional schema qualification; ignore leading pg_catalog. for system catalogs.
    /// - For regular tables, resolve to a canonical storage path using current defaults and hash it via FNV-1a 32-bit.
    /// - Always return a non-zero positive OID (map hash 0 to 1).
    pub fn resolve_regclass_oid(&self, name: &str) -> i32 {
        fn fnv1a32(data: &[u8]) -> u32 {
            const FNV_OFFSET: u32 = 0x811C9DC5;
            const FNV_PRIME: u32 = 0x01000193;
            let mut hash = FNV_OFFSET;
            for b in data {
                hash ^= *b as u32;
                hash = hash.wrapping_mul(FNV_PRIME);
            }
            if hash == 0 { 1 } else { hash }
        }
        // Normalize identifier: handle quoting and case folding like Postgres for unquoted identifiers
        let raw = name.trim();
        let (folded, _was_quoted) = if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
            (raw[1..raw.len()-1].to_string(), true)
        } else {
            (raw.to_ascii_lowercase(), false)
        };
        let mut ident = folded.as_str();
        // Strip leading pg_catalog. for convenience
        if ident.starts_with("pg_catalog.") { ident = &ident[11..]; }
        // Known system catalogs with stable OIDs
        match ident {
            "pg_class" => return 1259,
            "pg_type" => return 1247,
            "pg_namespace" => return 2615,
            _ => {}
        }
        // For other names, resolve to a canonical table ident and hash
        // If input was schema-qualified like schema.table, keep it; otherwise use defaults
        let effective = self.resolve_table_ident(ident);
        // Normalize time suffix by removing trailing .time for hashing stability
        let eff = if effective.ends_with(".time") { &effective[..effective.len()-5] } else { effective.as_str() };
        let h = fnv1a32(eff.as_bytes());
        (h as i32).abs().max(1)
    }

    /// Register user-generated columns (e.g., UDF outputs) for a stage
    pub fn register_user_columns_for_stage<I: IntoIterator<Item=String>>(&mut self, stage: SelectStage, cols: I) {
        let entry = self.stage_user_columns.entry(stage).or_default();
        for c in cols { entry.insert(c); }
    }

    /// Returns all visible columns up to (and including) a given stage
    /// Includes both materialized DataFrame columns and user-generated columns registered per stage
    pub fn visible_columns_until(&self, stage: SelectStage) -> HashSet<String> {
        use SelectStage::*;
        let mut out: HashSet<String> = HashSet::new();
        let order = [FromWhere, ByOrGroupBy, Rolling, ProjectSelect, OrderLimit, Having];
        for st in order.iter() {
            if let Some(set) = self.stage_columns.get(st) { out.extend(set.iter().cloned()); }
            if let Some(uset) = self.stage_user_columns.get(st) { out.extend(uset.iter().cloned()); }
            if *st == stage { break; }
        }
        out
    }

    /// Returns only the final SELECT output columns (ProjectSelect stage)
    pub fn final_select_columns(&self) -> HashSet<String> {
        self.stage_columns.get(&SelectStage::ProjectSelect).cloned().unwrap_or_default()
    }

    /// Helper to format a specific Column Not Found error with clause context and alias suggestion
    pub fn column_not_found_error(name: &str, clause: &str, df: &DataFrame) -> anyhow::Error {
        // Suggest an alias by checking for unique suffix matches
        let needle = if name.contains('.') { name.to_string() } else { format!(".{}", name) };
        let mut suggestion: Option<String> = None;
        let mut matches: Vec<String> = Vec::new();
        for c in df.get_column_names() {
            if c.ends_with(&needle) { matches.push(c.to_string()); }
        }
        if matches.len() == 1 { suggestion = Some(matches.remove(0)); }
        let mut msg = format!("Column not found in {}: '{}'", clause, name);
        if let Some(s) = suggestion { msg.push_str(&format!(". Did you mean to reference it as '{}' and/or add an alias?", s)); }
        // Append available columns for better diagnostics
        let cols = df.get_column_names();
        if !cols.is_empty() {
            let mut list: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
            list.sort_unstable();
            // Limit the list to avoid overly long messages
            let shown: Vec<&str> = list.into_iter().take(50).collect();
            msg.push_str(&format!(". Available columns: [{}]{}",
                                  shown.join(", "),
                                  if cols.len() > 50 { " (truncated)" } else { "" }));
        }
        anyhow::anyhow!(msg)
    }

    /// Construct with explicit defaults for current database and schema
    pub fn with_defaults(db: impl Into<String>, schema: impl Into<String>) -> Self {
        let now = std::time::SystemTime::now();
        DataContext {
            sources: Vec::new(),
            alias_to_name: HashMap::new(),
            parent_sources: Vec::new(),
            current_database: Some(db.into()),
            current_schema: Some(schema.into()),
            current_user: Some("postgres".to_string()),
            session_user: Some("postgres".to_string()),
            transaction_timestamp: Some(now),
            statement_timestamp: Some(now),
            stage_columns: HashMap::new(),
            stage_user_columns: HashMap::new(),
            script_registry: None,
            query_lua: Rc::new(RefCell::new(None)),
            store: None,
            cte_tables: HashMap::new(),
            temp_order_by_columns: HashSet::new(),
        }
    }

    /// Set the script registry for this query context (builder pattern)
    pub fn with_registry(mut self, reg: crate::scripts::ScriptRegistry) -> Self {
        self.script_registry = Some(reg);
        self
    }

    /// Register a FROM/JOIN table reference (and its optional alias)
    pub fn add_source(&mut self, t: &TableRef) {
        self.sources.push(t.clone());
        if let Some(a) = t.alias() {
            if let Some(n) = t.table_name() {
                self.alias_to_name.insert(a.to_string(), n.to_string());
            }
        }
        // Also allow referencing by canonical name as its own alias for convenience (only for Table variant)
        if let Some(n) = t.table_name() {
            self.alias_to_name.entry(n.to_string()).or_insert_with(|| n.to_string());
        }
    }

    /// Register a USING source (identifier string). We map it as a TableRef with no alias.
    pub fn add_using_ident(&mut self, ident: &str) {
        let t = TableRef::Table { name: ident.to_string(), alias: None };
        self.add_source(&t);
    }

    /// Resolve a column name against a DataFrame, considering aliases present in the context.
    /// Supports: fully-qualified alias ("a.col"), unqualified ("col"), and exact matches.
    pub fn resolve_column(&self, df: &DataFrame, name: &str) -> Result<String> {
        debug!(target: "clarium::exec", "DataContext::resolve_column: '{}' cols={:?}", name, df.get_column_names());
        let cols = df.get_column_names();
        let def_db = self.current_database.as_deref();
        let def_schema = self.current_schema.as_deref();
        // Helper: check exact and suffix matches in df
        let exact_in_df = |n: &str| cols.iter().any(|c| c.as_str() == n);
        let suffix_matches_in_df = |suffix: &str| -> Vec<String> {
            let needle = format!(".{}", suffix);
            cols.iter().filter_map(|c| {
                let s = c.as_str();
                if s.ends_with(&needle) { Some(s.to_string()) } else { None }
            }).collect()
        };
        // 1) If name contains a dot, it may be fully or partially qualified (e.g., alias.col or schema/table.col)
        if name.contains('.') {
            // If it's already an exact column in df, return it
            if exact_in_df(name) { return Ok(name.to_string()); }
            // If formatted as alias.col, only allow suffix fallback when alias is known in this context.
            if let Some((maybe_alias, col_part)) = name.rsplit_once('.') {
                // If alias is not known to this context, do NOT resolve by suffix here — it may belong to an outer query.
                // Leave such names to be handled by correlated subquery substitution instead of hijacking them locally.
                let alias_known = self.alias_to_name.contains_key(maybe_alias);
                if alias_known {
                    // Try exact with defaults, then unique suffix within current df
                    if let (Some(db), Some(schema)) = (def_db, def_schema) {
                        let candidate = format!("{}/{}/{}", db, schema, name);
                        if exact_in_df(&candidate) { return Ok(candidate); }
                    }
                    let mut matches = suffix_matches_in_df(col_part);
                    if matches.len() > 1 {
                        if let (Some(db), Some(schema)) = (def_db, def_schema) {
                            matches.retain(|m| m.starts_with(&format!("{}/{}/", db, schema)));
                        }
                    }
                    if matches.len() == 1 { return Ok(matches.remove(0)); }
                } else {
                    // Alias unknown: bail early to avoid accidentally mapping to inner columns like o.customer_id == o.customer_id
                    anyhow::bail!(format!("Column not found: {} (unknown alias '{}')", name, maybe_alias));
                }
            }
            // If missing db/schema and not in alias.col form, try to prepend defaults
            // Cases:
            //   table.col -> {db}/{schema}/table.col
            //   schema/table.col -> {db}/schema/table.col
            //   db/schema/table.col -> use as-is (already handled by exact), but also try suffix-in-df mapping
            let has_slash_count = name.matches('/').count();
            if let (Some(db), Some(schema)) = (def_db, def_schema) {
                let candidate = match has_slash_count {
                    0 => format!("{}/{}/{}", db, schema, name),
                    1 => format!("{}/{}", db, name),
                    _ => name.to_string(),
                };
                if exact_in_df(&candidate) { return Ok(candidate); }
                // As a fallback, if df has only one suffix match for the column part, use it
                if let Some((_tbl, col_part)) = name.rsplit_once('.') {
                    let mut matches = suffix_matches_in_df(col_part);
                    if matches.len() > 1 {
                        // Prefer those under default db/schema
                        matches.retain(|m| m.starts_with(&format!("{}/{}/", db, schema)));
                    }
                    if matches.len() == 1 { return Ok(matches.remove(0)); }
                }
            }
            anyhow::bail!(format!("Column not found: {}", name));
        }
        // 2) Unqualified name: try exact match first
        if exact_in_df(name) { return Ok(name.to_string()); }
        // 3) Suffix matches
        let mut matches = suffix_matches_in_df(name);
        if matches.is_empty() { anyhow::bail!(format!("Column not found: {}", name)); }
        if matches.len() > 1 {
            // Prefer default db/schema scope if available
            if let (Some(db), Some(schema)) = (def_db, def_schema) {
                let scoped: Vec<String> = matches.iter().filter(|&m| m.starts_with(&format!("{}/{}/", db, schema))).cloned().collect();
                if scoped.len() == 1 { return Ok(scoped[0].clone()); }
                if !scoped.is_empty() { matches = scoped; }
            }
            // Still ambiguous with multiple matches
            anyhow::bail!(format!("Ambiguous column '{}'; qualify with table alias", name));
        }
        Ok(matches.remove(0))
    }

    /// Resolve a column name with awareness of stage-registered columns.
    /// Strategy:
    /// 1) Try resolving against the provided DataFrame (exact, then suffix-qualified) — same as `resolve_column`.
    /// 2) If not found, consult `visible_columns_until(stage)` to allow resolving names introduced by prior stages.
    ///    We still map the result back to an actual column present in `df` by repeating the resolution against `df` using the outcome.
    ///    If resolution remains impossible in `df`, return a not found/ambiguous error.
    pub fn resolve_column_at_stage(&self, df: &DataFrame, name: &str, stage: SelectStage) -> Result<String> {
        // Step 1: normal resolution first
        if let Ok(n) = self.resolve_column(df, name) { return Ok(n); }
        // Step 2: consult stage-visible names
        let visible = self.visible_columns_until(stage);
        let def_db = self.current_database.as_deref();
        let def_schema = self.current_schema.as_deref();
        let prefer_scope = |c: &str| -> bool {
            if let (Some(db), Some(schema)) = (def_db, def_schema) {
                c.starts_with(&format!("{}/{}/", db, schema))
            } else { false }
        };
        // If the incoming name is already fully-qualified, see if it's visible
        if name.contains('.') {
            if visible.contains(name) {
                // If df has it, return; otherwise try suffix match into df
                if df.get_column_names().iter().any(|n| n.as_str() == name) { return Ok(name.to_string()); }
                let needle = format!(".{}", name.split('.').next_back().unwrap_or(name));
                let mut df_matches: Vec<String> = df.get_column_names().iter().filter_map(|c| {
                    let s = c.as_str(); if s.ends_with(&needle) { Some(s.to_string()) } else { None }
                }).collect();
                if df_matches.len() == 1 { return Ok(df_matches[0].clone()); }
                if df_matches.len() > 1 {
                    // Prefer matches under current db/schema
                    let scoped: Vec<String> = df_matches.iter().filter(|&m| prefer_scope(m)).cloned().collect();
                    if scoped.len() == 1 { return Ok(scoped[0].clone()); }
                    if !scoped.is_empty() { df_matches = scoped; }
                }
            }
            anyhow::bail!(format!("Column not found: {}", name));
        }
        // Unqualified: try exact visible first
        if visible.contains(name) {
            // If df has exact, use it
            if df.get_column_names().iter().any(|n| n.as_str() == name) { return Ok(name.to_string()); }
            // Otherwise disambiguate by suffix against df
            let needle = format!(".{}", name);
            let mut df_matches: Vec<String> = df.get_column_names().iter().filter_map(|c| {
                let s = c.as_str(); if s.ends_with(&needle) { Some(s.to_string()) } else { None }
            }).collect();
            if df_matches.len() == 1 { return Ok(df_matches[0].clone()); }
            if df_matches.len() > 1 {
                // Prefer default db/schema scope if available
                let scoped: Vec<String> = df_matches.iter().filter(|&m| prefer_scope(m)).cloned().collect();
                if scoped.len() == 1 { return Ok(scoped[0].clone()); }
                if !scoped.is_empty() { df_matches = scoped; }
            }
            if df_matches.is_empty() { anyhow::bail!(format!("Column not found: {}", name)); }
            anyhow::bail!(format!("Ambiguous column '{}'; qualify with table alias", name));
        }
        // Not visible and not in df — final attempt: suffix within visible set, then map into df
        let needle = format!(".{}", name);
        let mut vis_matches: Vec<&String> = visible.iter().filter(|c| c.ends_with(&needle)).collect();
        if !vis_matches.is_empty() {
            // Prefer visible matches under current default scope
            let scoped_vis: Vec<&String> = vis_matches.iter().cloned().filter(|m| prefer_scope(m)).collect();
            if !scoped_vis.is_empty() { vis_matches = scoped_vis; }
            if vis_matches.len() == 1 {
                let candidate = vis_matches[0];
                // Map candidate into df: if df has it, return it; else suffix in df
                if df.get_column_names().iter().any(|n| n.as_str() == candidate.as_str()) { return Ok(candidate.clone()); }
                let last = candidate.split('.').next_back().unwrap_or(candidate.as_str());
                let needle2 = format!(".{}", last);
                let mut df_matches: Vec<String> = df.get_column_names().iter().filter_map(|c| {
                    let s = c.as_str(); if s.ends_with(&needle2) { Some(s.to_string()) } else { None }
                }).collect();
                if df_matches.len() == 1 { return Ok(df_matches[0].clone()); }
                if df_matches.len() > 1 {
                    let scoped: Vec<String> = df_matches.iter().filter(|&m| prefer_scope(m)).cloned().collect();
                    if scoped.len() == 1 { return Ok(scoped[0].clone()); }
                    if !scoped.is_empty() { df_matches = scoped; }
                }
            }
        }
        anyhow::bail!(format!("Column not found: {}", name));
    }

    /// Resolve a table identifier to a canonical storage path using current defaults
    /// Rules:
    /// - If identifier contains '/' or '\\' treat as already fully-qualified path; normalize '\\' to '/'.
    /// - If identifier contains ".store.", return as-is (KV addressing) — caller may still need current db; we only expand bare store paths when defaults exist.
    /// - If identifier has one dot segment (schema.table), prepend current database.
    /// - If identifier has no dots (table), prepend current database and schema.
    /// - If identifier has two or more dots (db.schema.table), return db/schema/table.
    pub fn resolve_table_ident(&self, ident: &str) -> String {
        if ident.contains(".store.") { return ident.to_string(); }
        let d = crate::ident::QueryDefaults::from_options(self.current_database.as_deref(), self.current_schema.as_deref());
        // If identifier denotes a time table, qualify accordingly
        if ident.contains(".time") || ident.trim_end_matches('/').ends_with(".time") {
            return crate::ident::qualify_time_ident(ident, &d);
        }
        crate::ident::qualify_regular_ident(ident, &d)
    }

    /// Resolve a time-series table identifier, ensuring the resulting path ends with '.time'.
    /// Supports path-like (db/schema/table[.time]) and dotted forms (db.schema.table[.time], schema.table, table).
    pub fn resolve_time_table_ident(&self, ident: &str) -> String {
        if ident.contains(".store.") { return ident.to_string(); }
        let d = crate::ident::QueryDefaults::from_options(self.current_database.as_deref(), self.current_schema.as_deref());
        crate::ident::qualify_time_ident(ident, &d)
    }

    /// Load a source DataFrame given a TableRef, applying alias/name prefixing to columns.
    /// Supports CTEs, system tables, KV addresses, regular tables resolved via defaults, and subqueries.
    pub fn load_source_df(&self, store: &crate::storage::SharedStore, t: &TableRef) -> anyhow::Result<DataFrame> {
        match t {
            TableRef::Table { name, alias } => {
                // Check CTEs first - they take precedence over everything
                if let Some(cte_df) = self.cte_tables.get(name) {
                    tracing::debug!(target: "clarium::exec", "load_source_df: CTE hit name='{}' alias={:?}", name, alias);
                    return Self::prefix_columns(cte_df.clone(), t);
                }
                // Try system tables using the raw name, so system schemas like information_schema.* work
                if let Some(sys) = crate::system::system_table_df(name, store) {
                    tracing::debug!(target: "clarium::exec", "load_source_df: system table hit name='{}' alias={:?}", name, alias);
                    return Self::prefix_columns(sys, t);
                }
                // Resolve to a canonical path for regular tables or KV
                let effective = self.resolve_table_ident(name);
                tracing::debug!(target: "clarium::exec", "load_source_df: resolving name='{}' -> effective='{}' alias={:?}", name, effective, alias);
                // If this resolves to a VIEW file (<db>/<schema>/<name>.view), execute its definition as a subquery
                if !effective.contains(".store.") {
                    if let Some(vf) = crate::server::exec::exec_views::read_view_file(store, &effective).ok().flatten() {
                        tracing::debug!(target: "clarium::exec", "load_source_df: view hit name='{}' -> executing definition", effective);
                        // Parse and execute the stored definition SQL
                        let cmd = crate::query::parse(&vf.definition_sql)?;
                        let subquery_df = match cmd {
                            crate::query::Command::Select(q) => crate::server::exec::exec_select::run_select_with_context(store, &q, Some(self))?,
                            crate::query::Command::SelectUnion { queries, all } => crate::server::exec::exec_select::handle_select_union(store, &queries, all)?,
                            _ => anyhow::bail!("View definition must be SELECT or SELECT UNION"),
                        };
                        // Prefix columns with alias or view name
                        let prefixed = Self::prefix_columns(subquery_df, t)?;
                        tracing::debug!(target: "clarium::exec", "load_source_df: view prefixed -> cols={:?}", prefixed.get_column_names());
                        return Ok(prefixed);
                    }
                }
                let df = if effective.contains(".store.") {
                    // KV addressing
                    let out = Self::read_df_or_kv(store, &effective)?;
                    tracing::debug!(target: "clarium::exec", "load_source_df: KV df cols={:?} rows={}", out.get_column_names(), out.height());
                    out
                } else {
                    let guard = store.0.lock();
                    match guard.read_df(&effective) {
                        Ok(out) => {
                            tracing::debug!(target: "clarium::exec", "load_source_df: read_df('{}') -> cols={:?} rows={}", effective, out.get_column_names(), out.height());
                            out
                        }
                        Err(e) => {
                            // If the path is time-like but missing ".time" suffix, try with the suffix as a fallback
                            if !effective.ends_with(".time") {
                                let candidate = format!("{}.time", effective);
                                match guard.read_df(&candidate) {
                                    Ok(out2) => {
                                        tracing::debug!(target: "clarium::exec", "load_source_df: fallback read_df('{}') -> cols={:?} rows={}", candidate, out2.get_column_names(), out2.height());
                                        out2
                                    }
                                    Err(_) => return Err(e),
                                }
                            } else {
                                return Err(e);
                            }
                        }
                    }
                };
                let pref = alias.as_deref().unwrap_or(name.as_str());
                let prefixed = Self::prefix_columns(df, t)?;
                tracing::debug!(target: "clarium::exec", "load_source_df: prefixed with '{}' -> cols={:?}", pref, prefixed.get_column_names());
                Ok(prefixed)
            }
            TableRef::Subquery { query, alias } => {
                // Execute the subquery using run_select_with_context to allow nested correlation
                tracing::debug!(target: "clarium::exec", "load_source_df: executing subquery with alias '{}'", alias);
                let subquery_df = crate::server::exec::exec_select::run_select_with_context(store, query, Some(self))?;
                tracing::debug!(target: "clarium::exec", "load_source_df: subquery result cols={:?} rows={}", subquery_df.get_column_names(), subquery_df.height());
                // Prefix columns with the subquery alias
                Self::prefix_columns(subquery_df, t)
            }
        }
    }

    fn prefix_columns(df: DataFrame, t: &TableRef) -> anyhow::Result<DataFrame> {
        let pref = t.effective_name();
        let mut cols: Vec<polars::prelude::Column> = Vec::new();
        for cname in df.get_column_names() {
            let new_name = format!("{}.{}", pref, cname);
            let mut s = df.column(cname.as_str())?.clone();
            s.rename(new_name.into());
            cols.push(s);
        }
        Ok(DataFrame::new(cols)?)
    }

    fn read_df_or_kv(store: &crate::storage::SharedStore, name: &str) -> anyhow::Result<DataFrame> {
        // Duplicate of exec.rs helper to avoid module coupling
        // Detect pattern: <database>.store.<store>.<key>
        if name.contains(".store.") {
            let parts: Vec<&str> = name.split('.').collect();
            if parts.len() < 4 {
                anyhow::bail!(format!("Invalid store address '{}'. Expected <database>.store.<store>.<key>", name));
            }
            if parts[1].to_lowercase() != "store" {
                anyhow::bail!(format!("Invalid store address '{}'. Expected literal 'store' segment", name));
            }
            let db = parts[0];
            let store_name = parts[2];
            let key = parts[3..].join(".");
            let kv = store.kv_store(db, store_name);
            if let Some(val) = kv.get(&key) {
                match val {
                    crate::storage::KvValue::ParquetDf(df) => Ok(df),
                    crate::storage::KvValue::Json(_) => anyhow::bail!("JSON key cannot be used in FROM yet; JSON querying is not implemented"),
                    crate::storage::KvValue::Str(_) | crate::storage::KvValue::Int(_) => anyhow::bail!("Scalar key cannot be used in FROM; expected a table"),
                }
            } else {
                anyhow::bail!(format!("KV key not found: {}.store.{}.{}", db, store_name, key));
            }
        } else {
            let guard = store.0.lock();
            guard.read_df(name)
        }
    }
}

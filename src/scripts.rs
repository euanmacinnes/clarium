//! Simple Lua script registry for UDFs.
//!
//! This module provides a minimal registry that can store and reload Lua scripts
//! from the filesystem. For now, it only tracks script sources by name and does
//! not expose execution hooks into the query engine beyond presence in the
//! registry. This is sufficient for compile-time wiring and DDL operations in
//! tests.

use anyhow::{anyhow, Result};
use parking_lot::Mutex;
use std::{collections::HashMap, path::{Path, PathBuf}, fs};
use std::sync::atomic::{AtomicU64, Ordering};
use polars::prelude::{DataType, Series, DataFrame, NamedFrom};
use std::cell::RefCell;
use std::hash::Hash;
use tracing::debug;
use std::io::Write as _;
use std::fs::OpenOptions;

#[derive(Clone, Default)]
pub struct ScriptRegistry {
    inner: std::sync::Arc<Mutex<HashMap<String, String>>>, // name -> source
    meta: std::sync::Arc<Mutex<HashMap<String, ScriptMeta>>>, // name -> metadata (kind, return types)
}

#[derive(Clone, Debug)]
#[derive(Default)]
pub enum ScriptKind {
    #[default]
    Scalar,
    Aggregate,
    Constraint,
    Tvf,
}


#[derive(Clone, Debug, Default)]
pub struct ScriptMeta {
    pub kind: ScriptKind,
    pub returns: Vec<DataType>, // empty means unknown; single means scalar; >1 means multi-return
    pub nullable: bool,
    pub version: u64, // bump when code changes
    // For TVFs, describe output columns (name + dtype). If empty, engine will infer from Lua data.
    pub tvf_columns: Vec<(String, DataType)>,
}

impl ScriptRegistry {
    pub fn new() -> Result<Self> { Ok(Self::default()) }

    #[inline]
    pub(crate) fn norm(name: &str) -> String { name.to_ascii_lowercase() }

    /// Load or reload a script by logical name with the provided source text.
    pub fn load_script_text(&self, name: &str, code: &str) -> Result<()> {
        let key = Self::norm(name);
        let mut g = self.inner.lock();
        g.insert(key.clone(), code.to_string());
        // bump version to invalidate caches
        let mut m = self.meta.lock();
        let entry = m.entry(key).or_default();
        entry.version = entry.version.saturating_add(1);
        Ok(())
    }

    /// Set or update metadata for a function.
    pub fn set_meta(&self, name: &str, meta: ScriptMeta) {
        let key = Self::norm(name);
        let mut m = self.meta.lock();
        m.insert(key, meta);
    }

    /// Get metadata if recorded.
    pub fn get_meta(&self, name: &str) -> Option<ScriptMeta> {
        let key = Self::norm(name);
        self.meta.lock().get(&key).cloned()
    }

    /// Remove a function from the registry if present.
    pub fn unload_function(&self, name: &str) {
        let key = Self::norm(name);
        let mut g = self.inner.lock();
        g.remove(&key);
        let mut m = self.meta.lock();
        m.remove(&key);
    }

    /// Rename a function key in the registry if present.
    pub fn rename_function(&self, old: &str, newn: &str) -> Result<()> {
        let oldk = Self::norm(old);
        let newk = Self::norm(newn);
        let mut g = self.inner.lock();
        if let Some(code) = g.remove(&oldk) { g.insert(newk.clone(), code); }
        let mut m = self.meta.lock();
        if let Some(meta) = m.remove(&oldk) { m.insert(newk, meta); }
        Ok(())
    }

    /// Check if a function name is present in the registry.
    pub fn has_function(&self, name: &str) -> bool {
        let key = Self::norm(name);
        let g = self.inner.lock();
        let exists = g.contains_key(&key);
        debug!("[UDF REGISTRY] has_function: checking '{}' (normalized: '{}'), exists={}, total functions={}", name, key, exists, g.len());
        exists
    }

    /// Execute a registered Lua function by name with JSON-compatible arguments.
    /// NOTE: This path is kept for legacy use but not used by the engine anymore.
    pub fn call_function_json(&self, name: &str, args: &[serde_json::Value]) -> Result<serde_json::Value> {
        use mlua::{Value as LVal, MultiValue};
        debug!("[UDF CALL] call_function_json: attempting to call function '{}' with {} args", name, args.len());
        debug!("[UDF CALL] call_function_json: registry has_function('{}')={}", name, self.has_function(name));
        let result = self.with_prepared_lua(|lua| {
            let globals = lua.globals();
            let lname = Self::norm(name);
            debug!("[UDF CALL] call_function_json: normalized name='{}', looking up in Lua globals", lname);
            let func: mlua::Function = match globals.get(lname.as_str()) {
                Ok(f) => {
                    debug!("[UDF CALL] call_function_json: successfully retrieved function '{}'", lname);
                    f
                },
                Err(e) => {
                    debug!("[UDF CALL] call_function_json: failed to get function '{}' from globals: {}", lname, e);
                    return Err(e.into());
                }
            };
            let mut mvals = MultiValue::new();
            for a in args.iter().rev() {
                let lv = json_to_lua_mode(lua, a, NullMode::RealNil)?;
                mvals.push_front(lv);
            }
            let out: LVal = func.call(mvals)?;
            let j = lua_to_json(out)?;
            debug!("[UDF CALL] call_function_json: successfully called '{}', result type: {:?}", name, j);
            Ok(j)
        });
        if let Err(ref e) = result {
            debug!("[UDF CALL] call_function_json: error calling '{}': {}", name, e);
        }
        result
    }

    /// Execute a registered Lua function by name with JSON-compatible arguments,
    /// mapping JSON nulls to real Lua nil values. This is intended for aggregate
    /// UDFs which often perform arithmetic over arrays and must treat missing
    /// values as nil, not the string "nil".
    pub fn call_function_json_aggregate(&self, name: &str, args: &[serde_json::Value]) -> Result<serde_json::Value> {
        use mlua::{Value as LVal, MultiValue};
        self.with_prepared_lua(|lua| {
            let globals = lua.globals();
            let lname = Self::norm(name);
            let func: mlua::Function = globals.get(lname.as_str())?;
            let mut mvals = MultiValue::new();
            for a in args.iter().rev() {
                let lv = json_to_lua_mode(lua, a, NullMode::RealNil)?;
                // Preserve original argument order into Lua by pushing to front in reverse iteration
                mvals.push_front(lv);
            }
            let out: LVal = func.call(mvals)?;
            let j = lua_to_json(out)?;
            Ok(j)
        })
    }

    /// Load all .lua scripts in subfolders `scalars`, `aggregates`, and `constraints` into the registry
    /// and fetch optional metadata from one of (in order):
    /// 1) Sidecar JSON file `<name>.meta.json` next to the Lua file
    /// 2) Embedded JSON docstring at the top of the Lua file inside a block comment `--[[ { ... } ]]`
    /// 3) Calling a `<name>__meta()` Lua function if present in any loaded script
    /// 4) Fallback default metadata
    pub fn load_from_schema_root(&self, dir: &Path) -> Result<()> {
        if !dir.exists() { return Ok(()); }
        let scalars = dir.join("scalars");
        let aggregates = dir.join("aggregates");
        let constraints = dir.join("constraints");
        let tvfs = dir.join("tvfs");
        let load_dir = |folder: &Path, kind: ScriptKind| -> Result<()> {
            if !folder.exists() { return Ok(()); }
            let rd = match fs::read_dir(folder) {
                Ok(rd) => rd,
                Err(e) => {
                    tracing::error!(target: "clarium::udf", "[UDF LOAD] Failed to read directory '{}': {}", folder.display(), e);
                    return Ok(());
                }
            };
            for ent_res in rd {
                let ent = match ent_res {
                    Ok(e) => e,
                    Err(e) => { tracing::error!(target: "clarium::udf", "[UDF LOAD] Failed to read a directory entry in '{}': {}", folder.display(), e); continue; }
                };
                let p = ent.path();
                if p.is_file() && p.extension().and_then(|e| e.to_str()).unwrap_or("").eq_ignore_ascii_case("lua") {
                    let name = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                    let code = match fs::read_to_string(&p) {
                        Ok(c) => c,
                        Err(e) => { 
                            tracing::error!(target: "clarium::udf", "[UDF LOAD] Failed to read Lua script '{}': {}", p.display(), e);
                            // Write adjacent .log with details
                            let _ = write_script_error_log_adjacent(&p, "read", &name, &format!("Failed to read Lua script: {}", e));
                            continue; 
                        }
                    };
                    // Load under unqualified name
                    if let Err(e) = self.load_script_text(&name, &code) {
                        tracing::error!(target: "clarium::udf", "[UDF LOAD] Failed to register script '{}': {}", name, e);
                        let _ = write_script_error_log_adjacent(&p, "register", &name, &format!("Failed to register script in registry: {}", e));
                        // Do not attempt to set metadata if we couldn't register
                        continue;
                    }
                    // Also expose all globally provided functions under pg_catalog.<name>
                    // so clients using schema-qualified calls can resolve them.
                    let qualified = format!("pg_catalog.{}", name);
                    if let Err(e) = self.load_script_text(&qualified, &code) {
                        tracing::error!(target: "clarium::udf", "[UDF LOAD] Failed to register qualified script '{}': {}", qualified, e);
                    }
                    // Try sidecar .meta.json
                    let mut applied_meta = false;
                    let sidecar = p.with_extension("meta.json");
                    if sidecar.exists() {
                        match fs::read_to_string(&sidecar) {
                            Ok(txt) => {
                                match Self::parse_meta_json(&txt, &kind) {
                                    Ok(meta) => { self.set_meta(&name, meta.clone()); self.set_meta(&qualified, meta); applied_meta = true; }
                                    Err(e) => { 
                                        tracing::error!(target: "clarium::udf", "[UDF LOAD] Invalid meta sidecar for '{}': {}", p.display(), e);
                                        let _ = write_script_error_log_adjacent(&p, "meta", &name, &format!("Invalid .meta.json sidecar '{}': {}", sidecar.display(), e));
                                    }
                                }
                            }
                            Err(e) => { 
                                tracing::error!(target: "clarium::udf", "[UDF LOAD] Failed to read meta sidecar for '{}': {}", p.display(), e);
                                let _ = write_script_error_log_adjacent(&p, "meta", &name, &format!("Failed to read .meta.json sidecar '{}': {}", sidecar.display(), e));
                            }
                        }
                    }
                    // Try embedded JSON docstring at top of Lua file: --[[ { ... } ]]
                    if !applied_meta {
                        if let Some(meta) = Self::parse_embedded_meta(&code, &kind) {
                            self.set_meta(&name, meta.clone());
                            self.set_meta(&qualified, meta);
                            applied_meta = true;
                        }
                    }
                    // Try to read metadata via meta function in a fresh Lua state
                    if !applied_meta {
                        match self.fetch_meta_via_lua(&name, &kind) {
                            Ok(meta) => { self.set_meta(&name, meta.clone()); self.set_meta(&qualified, meta); applied_meta = true; }
                            Err(e) => { 
                                tracing::error!(target: "clarium::udf", "[UDF LOAD] Failed to fetch meta via Lua for '{}': {}", name, e);
                                let _ = write_script_error_log_adjacent(&p, "meta", &name, &format!("Failed to fetch meta via Lua: {}", e));
                            }
                        }
                    }
                    // default meta when not provided
                    if !applied_meta {
                        let meta = ScriptMeta { kind: kind.clone(), returns: Vec::new(), nullable: true, version: 0, tvf_columns: Vec::new() };
                        self.set_meta(&name, meta.clone());
                        self.set_meta(&qualified, meta);
                    }
                }
            }
            Ok(())
        };
        load_dir(&scalars, ScriptKind::Scalar)?;
        load_dir(&aggregates, ScriptKind::Aggregate)?;
        load_dir(&constraints, ScriptKind::Constraint)?;
        load_dir(&tvfs, ScriptKind::Tvf)?;
        Ok(())
    }

    fn parse_meta_json(txt: &str, default_kind: &ScriptKind) -> Result<ScriptMeta> {
        let v: serde_json::Value = serde_json::from_str(txt)?;
        Self::meta_from_json_value(v, default_kind)
    }

    fn meta_from_json_value(v: serde_json::Value, default_kind: &ScriptKind) -> Result<ScriptMeta> {
        let mut meta = ScriptMeta { kind: default_kind.clone(), returns: Vec::new(), nullable: true, version: 0, tvf_columns: Vec::new() };
        if let Some(k) = v.get("kind").and_then(|x| x.as_str()) {
            meta.kind = if k.eq_ignore_ascii_case("aggregate") { ScriptKind::Aggregate }
                else if k.eq_ignore_ascii_case("constraint") { ScriptKind::Constraint }
                else if k.eq_ignore_ascii_case("tvf") { ScriptKind::Tvf }
                else { ScriptKind::Scalar };
        }
        if let Some(nul) = v.get("nullable").and_then(|x| x.as_bool()) { meta.nullable = nul; }
        if let Some(arr) = v.get("returns").and_then(|x| x.as_array()) {
            let mut outs = Vec::new();
            for s in arr.iter().filter_map(|e| e.as_str()) { outs.push(str_to_dtype(s)?); }
            meta.returns = outs;
        }
        // TVF output schema: columns: [ { name: "col", type: "int64" }, ... ]
        if let Some(cols) = v.get("columns").and_then(|x| x.as_array()) {
            let mut out_cols: Vec<(String, DataType)> = Vec::with_capacity(cols.len());
            for c in cols {
                let name = c.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string();
                let ty = c.get("type").and_then(|x| x.as_str()).ok_or_else(|| anyhow!("TVF column missing type"))?;
                out_cols.push((name, str_to_dtype(ty)?));
            }
            meta.tvf_columns = out_cols;
        }
        if let Some(vv) = v.get("version").and_then(|x| x.as_u64()) { meta.version = vv; }
        Ok(meta)
    }

    fn parse_embedded_meta(code: &str, default_kind: &ScriptKind) -> Option<ScriptMeta> {
        // Look for a top-of-file block comment containing a JSON object, e.g.:
        // --[[
        // { "kind": "scalar", "returns": ["string"], "nullable": false }
        // ]]
        let trimmed = code.trim_start();
        if let Some(rest) = trimmed.strip_prefix("--[[") {
            if let Some(end_idx) = rest.find("]]" ) {
                let json_txt = &rest[..end_idx];
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(json_txt) {
                    if let Ok(meta) = Self::meta_from_json_value(v, default_kind) { return Some(meta); }
                }
            }
        }
        None
    }

    fn fetch_meta_via_lua(&self, name: &str, default_kind: &ScriptKind) -> Result<ScriptMeta> {
        use mlua::Lua;
        let snapshot: std::collections::HashMap<String, String> = { self.inner.lock().clone() };
        let lua = Lua::new();
        for (n, code) in snapshot.iter() {
            if let Err(e) = lua.load(code.as_str()).exec() {
                // Do not abort metadata fetch for other scripts; log and continue
                tracing::error!(target: "clarium::udf", "[UDF META] Failed to load script '{}' while fetching meta for '{}': {}", n, name, e);
                // Attempt to write an adjacent .log for the failing script if we can resolve its file
                let _ = write_script_error_log_for_name(n, "meta-load", &format!("Failed to load script while fetching meta for '{}': {}", name, e));
                continue;
            }
        }
        let globals = lua.globals();
        let meta_fn: Option<mlua::Function> = globals.get(format!("{}__meta", name).as_str()).ok();
        let mut meta = ScriptMeta { kind: default_kind.clone(), returns: Vec::new(), nullable: true, version: 0, tvf_columns: Vec::new() };
        if let Some(mf) = meta_fn {
            let v: mlua::Value = match mf.call(()) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(target: "clarium::udf", "[UDF META] '{}'__meta() call failed: {}", name, e);
                    let _ = write_script_error_log_for_name(name, "meta-call", &format!("'{}__meta' call failed: {}", name, e));
                    return Ok(meta);
                }
            };
            // Expect a table with fields: kind, returns (array of strings), nullable (bool)
            if let mlua::Value::Table(t) = v {
                if let Ok(k) = t.get::<_, String>("kind") { meta.kind = if k.eq_ignore_ascii_case("aggregate") { ScriptKind::Aggregate } else if k.eq_ignore_ascii_case("constraint") { ScriptKind::Constraint } else { ScriptKind::Scalar }; }
                if let Ok(nul) = t.get::<_, bool>("nullable") { meta.nullable = nul; }
                if let Ok(arr) = t.get::<_, mlua::Table>("returns") {
                    let mut outs: Vec<DataType> = Vec::new();
                    for s in arr.sequence_values::<String>().flatten() { outs.push(str_to_dtype(&s)?); }
                    meta.returns = outs;
                }
            }
        }
        Ok(meta)
    }

    /// Produce a diagnostic snapshot of the registry state: lists all registered
    /// function names with basic metadata (kind, returns, nullable, version).
    /// Intended for test/harness logging to help investigate intermittent UDF issues.
    pub fn debug_snapshot(&self) -> String {
        let g = self.inner.lock();
        let m = self.meta.lock();
        let mut names: Vec<String> = g.keys().cloned().collect();
        names.sort();
        let mut out = String::new();
        use std::fmt::Write as _;
        let _ = writeln!(out, "ScriptRegistry snapshot: total_functions={}", names.len());
        for name in names {
            let meta = m.get(&name);
            match meta {
                Some(meta) => {
                    let kind = match meta.kind { ScriptKind::Scalar => "scalar", ScriptKind::Aggregate => "aggregate", ScriptKind::Constraint => "constraint", ScriptKind::Tvf => "tvf" };
                    let returns: Vec<&'static str> = meta.returns.iter().map(|dt| match dt {
                        DataType::Boolean => "bool",
                        DataType::Int64 => "int64",
                        DataType::Float64 => "float64",
                        DataType::String => "string",
                        DataType::Null => "null",
                        DataType::Datetime(_, _) => "timestamp",
                        DataType::Date => "date",
                        DataType::Time => "time",
                        DataType::Duration(polars::prelude::TimeUnit::Nanoseconds) => "interval(ns)",
                        DataType::Duration(polars::prelude::TimeUnit::Microseconds) => "interval(us)",
                        DataType::Duration(polars::prelude::TimeUnit::Milliseconds) => "interval(ms)",
                        DataType::List(inner) => {
                            if matches!(**inner, DataType::Float64) { "vector" } else { "list" }
                        }
                        _ => "other",
                    }).collect();
                    let _ = writeln!(out, "  - {}: kind={}, returns={:?}, nullable={}, version={}", name, kind, returns, meta.nullable, meta.version);
                }
                None => {
                    let _ = writeln!(out, "  - {}: <no meta>", name);
                }
            }
        }
        out
    }

    /// Create an immutable snapshot of this registry.
    /// The returned ScriptRegistry contains deep-copied maps wrapped in new Arc<Mutex<_>>
    /// instances so that subsequent mutations to the original do not affect the snapshot
    /// (and vice versa). This is useful to bind a stable view for the duration of a query.
    pub fn snapshot(&self) -> Result<Self> {
        let scripts = {
            let g = self.inner.lock();
            (*g).clone()
        };
        let metas = {
            let m = self.meta.lock();
            (*m).clone()
        };
        Ok(ScriptRegistry {
            inner: std::sync::Arc::new(Mutex::new(scripts)),
            meta: std::sync::Arc::new(Mutex::new(metas)),
        })
    }

    /// Try to evaluate a table-valued Lua function given a raw call string like
    /// "my_tvf(arg1, 'arg2', [1,2,3])". Returns Ok(Some(DataFrame)) on success when
    /// the function exists and is of kind Tvf, Ok(None) if the function name is not
    /// registered as a TVF, and Err on execution/conversion errors.
    pub fn try_eval_tvf_call(
        &self,
        call: &str,
        ctx: Option<&crate::server::data_context::DataContext>,
    ) -> Result<Option<DataFrame>> {
        // Extract name and args as strings first
        let s = call.trim();
        let fname = s.split('(').next().unwrap_or("").trim();
        let lname = Self::norm(fname);
        // Check metadata to ensure TVF
        if let Some(meta) = self.get_meta(&lname) {
            if !matches!(meta.kind, ScriptKind::Tvf) {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        fn extract_args(fcall: &str) -> Option<Vec<String>> {
            let open = fcall.find('(')?;
            if !fcall.ends_with(')') { return Some(vec![]); }
            let inside = &fcall[open+1..fcall.len()-1];
            let mut args: Vec<String> = Vec::new();
            let mut cur = String::new();
            let mut in_sq = false; let mut in_dq = false; let mut depth_br = 0usize; let mut prev_bs = false;
            for ch in inside.chars() {
                if ch == '\\' { prev_bs = !prev_bs; cur.push(ch); continue; } else { prev_bs = false; }
                if !in_dq && ch=='\'' { in_sq = !in_sq; cur.push(ch); continue; }
                if !in_sq && ch=='"' { in_dq = !in_dq; cur.push(ch); continue; }
                if !in_sq && !in_dq {
                    if ch=='[' || ch=='{' { depth_br += 1; }
                    if ch==']' || ch=='}' { if depth_br>0 { depth_br -= 1; } }
                    if ch==',' && depth_br==0 { args.push(cur.trim().to_string()); cur.clear(); continue; }
                }
                cur.push(ch);
            }
            if !cur.is_empty() { args.push(cur.trim().to_string()); }
            Some(args)
        }

        fn strip_quotes(x: &str) -> String {
            let t = x.trim();
            if (t.starts_with('\'') && t.ends_with('\'')) || (t.starts_with('"') && t.ends_with('"')) {
                if t.len() >= 2 { return t[1..t.len()-1].to_string(); }
            }
            t.to_string()
        }

        // Prepare Lua and call function
        let df = self.with_prepared_lua(|lua| {
            // Optionally register context accessor
            if let Some(dc) = ctx { Self::register_context_accessor(lua, &ContextInfo::from_data_context(dc))?; }
            let globals = lua.globals();
            let func: mlua::Function = globals.get(lname.as_str())
                .map_err(|e| anyhow!("TVF '{}' not found: {}", lname, e))?;
            // Build argument list
            use mlua::{Value as LVal, MultiValue};
            let mut mvals = MultiValue::new();
            if let Some(arg_strs) = extract_args(s) {
                for a in arg_strs {
                    let lv: LVal = if a.is_empty() { LVal::Nil }
                        else if (a.starts_with('[') && a.ends_with(']')) || (a.starts_with('{') && a.ends_with('}')) {
                            // Try parse as JSON
                            match serde_json::from_str::<serde_json::Value>(&a) {
                                Ok(j) => json_to_lua_mode(lua, &j, NullMode::RealNil)?,
                                Err(_) => LVal::String(lua.create_string(strip_quotes(&a))?),
                            }
                        } else if a.starts_with('\'') || a.starts_with('"') {
                            LVal::String(lua.create_string(strip_quotes(&a))?)
                        } else {
                            // Try number or boolean
                            if let Ok(i) = a.parse::<i64>() { LVal::Integer(i) }
                            else if let Ok(f) = a.parse::<f64>() { LVal::Number(f) }
                            else if a.eq_ignore_ascii_case("true") { LVal::Boolean(true) }
                            else if a.eq_ignore_ascii_case("false") { LVal::Boolean(false) }
                            else { LVal::String(lua.create_string(&a)?) }
                        };
                    mvals.push_front(lv);
                }
            }
            let outv: LVal = func.call(mvals)
                .map_err(|e| anyhow!("TVF '{}' execution error: {}", lname, e))?;
            let j = lua_to_json(outv)?;
            // Convert JSON to DataFrame
            Self::json_to_df(&j, self.get_meta(&lname))
        })?;
        Ok(Some(df))
    }

    fn json_to_df(j: &serde_json::Value, meta: Option<ScriptMeta>) -> Result<DataFrame> {
        match j {
            serde_json::Value::Array(rows) => {
                // Expect array of row objects or arrays
                if rows.is_empty() {
                    return Ok(DataFrame::new(vec![])?);
                }
                // Determine columns
                let (col_names, col_values_per_row): (Vec<String>, Vec<Vec<serde_json::Value>>) = match &rows[0] {
                    serde_json::Value::Object(obj0) => {
                        // Union of keys across rows; prefer meta.tvf_columns order if provided
                        let mut names: Vec<String> = if let Some(m) = &meta { if !m.tvf_columns.is_empty() { m.tvf_columns.iter().map(|(n, _)| n.clone()).collect() } else { Vec::new() } } else { Vec::new() };
                        if names.is_empty() {
                            names = obj0.keys().cloned().collect();
                            names.sort();
                        }
                        let mut per_row: Vec<Vec<serde_json::Value>> = Vec::with_capacity(rows.len());
                        for r in rows {
                            let mut rowvals: Vec<serde_json::Value> = Vec::with_capacity(names.len());
                            let robj = r.as_object().unwrap();
                            for n in &names {
                                rowvals.push(robj.get(n).cloned().unwrap_or(serde_json::Value::Null));
                            }
                            per_row.push(rowvals);
                        }
                        (names, per_row)
                    }
                    serde_json::Value::Array(arr0) => {
                        // Positional columns; use meta names or c0..cN
                        let names: Vec<String> = if let Some(m) = &meta { if !m.tvf_columns.is_empty() { m.tvf_columns.iter().map(|(n, _)| n.clone()).collect() } else { (0..arr0.len()).map(|i| format!("c{}", i)).collect() } } else { (0..arr0.len()).map(|i| format!("c{}", i)).collect() };
                        let mut per_row: Vec<Vec<serde_json::Value>> = Vec::with_capacity(rows.len());
                        for r in rows { let rarr = r.as_array().unwrap(); per_row.push(rarr.clone()); }
                        (names, per_row)
                    }
                    _ => { return Err(anyhow!("Unsupported TVF row format")); }
                };
                // Build Series per column using dtype inference or meta
                let mut cols: Vec<Series> = Vec::with_capacity(col_names.len());
                for (ci, cname) in col_names.iter().enumerate() {
                    // Collect values for this column
                    let mut vals: Vec<serde_json::Value> = Vec::with_capacity(col_values_per_row.len());
                    for r in &col_values_per_row { vals.push(r.get(ci).cloned().unwrap_or(serde_json::Value::Null)); }
                    let dtype_hint = meta.as_ref().and_then(|m| m.tvf_columns.get(ci)).map(|(_, dt)| dt.clone());
                    cols.push(Self::json_values_to_series(cname, &vals, dtype_hint)?);
                }
                Ok(DataFrame::new(cols.into_iter().map(|s| s.into()).collect())?)
            }
            serde_json::Value::Object(cols_obj) => {
                // Columnar format: object of arrays
                let mut names: Vec<String> = if let Some(m) = &meta { if !m.tvf_columns.is_empty() { m.tvf_columns.iter().map(|(n, _)| n.clone()).collect() } else { cols_obj.keys().cloned().collect() } } else { cols_obj.keys().cloned().collect() };
                if meta.as_ref().map(|m| m.tvf_columns.is_empty()).unwrap_or(true) { names.sort(); }
                let mut cols: Vec<Series> = Vec::with_capacity(names.len());
                for (i, n) in names.iter().enumerate() {
                    let arr = cols_obj.get(n).cloned().unwrap_or(serde_json::Value::Array(vec![]));
                    let list = match arr { serde_json::Value::Array(v) => v, _ => Vec::new() };
                    let dtype_hint = meta.as_ref().and_then(|m| m.tvf_columns.get(i)).map(|(_, dt)| dt.clone());
                    cols.push(Self::json_values_to_series(n, &list, dtype_hint)?);
                }
                Ok(DataFrame::new(cols.into_iter().map(|s| s.into()).collect())?)
            }
            _ => Err(anyhow!("TVF must return an array of rows or object of arrays")),
        }
    }

    fn json_values_to_series(name: &str, vals: &Vec<serde_json::Value>, hint: Option<DataType>) -> Result<Series> {
        // Detect vectors (array of numbers) and other types
        let inferred = if let Some(dt) = hint { if matches!(dt, DataType::Null) { Self::infer_dtype(vals) } else { dt } } else { Self::infer_dtype(vals) };
        use serde_json::Value as JV;
        let s = match inferred {
            DataType::Datetime(_, _) => {
                // Build epoch milliseconds then cast to Datetime[ms]
                let mut vms: Vec<Option<i64>> = Vec::with_capacity(vals.len());
                for x in vals {
                    let ms = match x {
                        JV::Null => None,
                        JV::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                // Heuristic: if looks like seconds (< 1e12), convert to ms
                                let abs = i.unsigned_abs();
                                let as_ms = if abs < 1_000_000_000_000 { i.saturating_mul(1000) } else { i };
                                Some(as_ms)
                            } else if let Some(f) = n.as_f64() {
                                Some((f * 1000.0).round() as i64)
                            } else { None }
                        }
                        JV::String(s) => {
                            if let Some(ms) = crate::server::query::query_common::parse_iso8601_to_ms(s) { Some(ms) }
                            else if let Ok(i) = s.parse::<i64>() { Some(i) } else { None }
                        }
                        _ => None,
                    };
                    vms.push(ms);
                }
                let s = Series::new(name.into(), vms);
                s.cast(&DataType::Datetime(polars::prelude::TimeUnit::Milliseconds, None)).map_err(|e| anyhow!(e.to_string()))?
            }
            DataType::Date => {
                // Polars Date = days since epoch (Int32)
                let mut vdays: Vec<Option<i32>> = Vec::with_capacity(vals.len());
                for x in vals {
                    let d = match x {
                        JV::Null => None,
                        JV::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                // Heuristic: if large (ms), convert to days
                                let days = if i.abs() > 10_000 { i / 86_400_000 } else { i };
                                Some(days as i32)
                            } else { None }
                        }
                        JV::String(s) => {
                            if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                Some((nd - epoch).num_days() as i32)
                            } else if let Some(ms) = crate::server::query::query_common::parse_iso8601_to_ms(s) {
                                Some((ms / 86_400_000) as i32)
                            } else { None }
                        }
                        _ => None,
                    };
                    vdays.push(d);
                }
                Series::new(name.into(), vdays)
            }
            DataType::Time => {
                // Polars Time = nanoseconds since midnight (Int64)
                let mut vns: Vec<Option<i64>> = Vec::with_capacity(vals.len());
                for x in vals {
                    let ns = match x {
                        JV::Null => None,
                        JV::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                // Treat as milliseconds if small; else assume already nanoseconds
                                let v = if i < 86_400_000 { i * 1_000_000 } else { i };
                                Some(v)
                            } else if let Some(f) = n.as_f64() {
                                // seconds → ns
                                Some((f * 1_000_000_000.0).round() as i64)
                            } else { None }
                        }
                        JV::String(s) => {
                            if let Some(ns) = Self::parse_time_str_to_ns(s) { Some(ns) }
                            else if let Ok(i) = s.parse::<i64>() { Some(i) } else { None }
                        }
                        _ => None,
                    };
                    vns.push(ns);
                }
                let s = Series::new(name.into(), vns);
                s.cast(&DataType::Time).map_err(|e| anyhow!(e.to_string()))?
            }
            DataType::Duration(tu) => {
                // Build integer values in the same base unit, prefer ms
                let mut vals_i64: Vec<Option<i64>> = Vec::with_capacity(vals.len());
                for x in vals {
                    let ms = match x {
                        JV::Null => None,
                        JV::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f.round() as i64)),
                        JV::String(s) => {
                            if let Some(ms) = Self::parse_iso8601_duration_to_ms(s) { Some(ms) }
                            else if let Ok(i) = s.parse::<i64>() { Some(i) } else { None }
                        }
                        _ => None,
                    };
                    // Convert ms to the requested unit
                    let v = ms.map(|m| match tu { polars::prelude::TimeUnit::Nanoseconds => m.saturating_mul(1_000_000), polars::prelude::TimeUnit::Microseconds => m.saturating_mul(1_000), polars::prelude::TimeUnit::Milliseconds => m });
                    vals_i64.push(v);
                }
                let s = Series::new(name.into(), vals_i64);
                s.cast(&DataType::Duration(tu)).map_err(|e| anyhow!(e.to_string()))?
            }
            DataType::Boolean => {
                let mut v: Vec<Option<bool>> = Vec::with_capacity(vals.len());
                for x in vals { v.push(match x { JV::Bool(b)=>Some(*b), JV::Null=>None, JV::Number(n)=>Some(n.as_i64().unwrap_or(0)!=0), JV::String(s)=>Some(!s.is_empty()), _=>None }); }
                Series::new(name.into(), v)
            }
            DataType::Int64 => {
                let mut v: Vec<Option<i64>> = Vec::with_capacity(vals.len());
                for x in vals { v.push(match x { JV::Number(n)=>n.as_i64(), JV::String(s)=>s.parse::<i64>().ok(), JV::Bool(b)=>Some(if *b {1} else {0}), JV::Null=>None, _=>None }); }
                Series::new(name.into(), v)
            }
            DataType::Float64 => {
                let mut v: Vec<Option<f64>> = Vec::with_capacity(vals.len());
                for x in vals { v.push(match x { JV::Number(n)=>n.as_f64(), JV::String(s)=>s.parse::<f64>().ok(), JV::Bool(b)=>Some(if *b {1.0} else {0.0}), JV::Null=>None, _=>None }); }
                Series::new(name.into(), v)
            }
            DataType::List(inner) if matches!(*inner, DataType::Float64) => {
                // VECTOR: each value is an array of numbers
                let mut avs: Vec<polars::prelude::AnyValue> = Vec::with_capacity(vals.len());
                for x in vals {
                    match x {
                        JV::Array(a) => {
                            let mut vv: Vec<f64> = Vec::with_capacity(a.len());
                            for e in a { if let Some(f)=e.as_f64() { vv.push(f); } else if let Some(i)=e.as_i64(){ vv.push(i as f64);} }
                            avs.push(polars::prelude::AnyValue::List(Series::new("".into(), vv)));
                        }
                        JV::String(s) if s.starts_with('[') && s.ends_with(']') => {
                            if let Ok(arr) = serde_json::from_str::<Vec<f64>>(s) {
                                avs.push(polars::prelude::AnyValue::List(Series::new("".into(), arr)));
                            } else {
                                avs.push(polars::prelude::AnyValue::Null);
                            }
                        }
                        JV::Null => avs.push(polars::prelude::AnyValue::Null),
                        _ => avs.push(polars::prelude::AnyValue::Null),
                    }
                }
                Series::from_any_values_and_dtype(
                    name.into(),
                    &avs,
                    &DataType::List(Box::new(DataType::Float64)),
                    false,
                ).map_err(|e| anyhow!(e.to_string()))?
            }
            _ => {
                // Default to String
                let mut v: Vec<Option<String>> = Vec::with_capacity(vals.len());
                for x in vals { v.push(match x { JV::String(s)=>Some(s.clone()), JV::Number(n)=>Some(n.to_string()), JV::Bool(b)=>Some(b.to_string()), JV::Null=>None, JV::Array(_)|JV::Object(_)=>Some(x.to_string()) }); }
                Series::new(name.into(), v)
            }
        };
        Ok(s)
    }

    fn infer_dtype(vals: &Vec<serde_json::Value>) -> DataType {
        use serde_json::Value as JV;
        for x in vals {
            match x {
                JV::Null => continue,
                JV::Bool(_) => return DataType::Boolean,
                JV::Number(n) => { if n.is_i64() { return DataType::Int64; } else { return DataType::Float64; } }
                JV::Array(a) => {
                    if a.iter().all(|e| e.is_number()) { return DataType::List(Box::new(DataType::Float64)); }
                    return DataType::String;
                }
                JV::Object(_) => return DataType::String,
                JV::String(_) => return DataType::String,
            }
        }
        DataType::Null
    }
}

impl ScriptRegistry {
    fn parse_time_str_to_ns(s: &str) -> Option<i64> {
        // Accept HH:MM, HH:MM:SS, HH:MM:SS.mmm[uuu][nnn]
        let t = s.trim();
        let parts: Vec<&str> = t.split(':').collect();
        if parts.len() < 2 { return None; }
        let h: i64 = parts[0].parse().ok()?;
        let m: i64 = parts[1].parse().ok()?;
        let mut sec_f: f64 = 0.0;
        if parts.len() >= 3 {
            sec_f = parts[2].parse::<f64>().ok().unwrap_or(0.0);
        }
        let total_ns = (((h * 60 + m) as f64 * 60.0) + sec_f) * 1_000_000_000.0;
        Some(total_ns.round() as i64)
    }

    fn parse_iso8601_duration_to_ms(s: &str) -> Option<i64> {
        // Minimal ISO8601 duration parser supporting PnDTnHnMnS with fractional seconds
        let mut txt = s.trim();
        if txt.is_empty() { return None; }
        if !txt.starts_with('P') && !txt.starts_with('p') { return None; }
        txt = &txt[1..];
        let mut in_time = false;
        let mut num = String::new();
        let mut total_ms: f64 = 0.0;
        for ch in txt.chars() {
            if ch == 'T' || ch == 't' { in_time = true; continue; }
            if ch.is_ascii_digit() || ch == '.' || ch == ',' {
                let c = if ch == ',' { '.' } else { ch };
                num.push(c);
                continue;
            }
            if !num.is_empty() {
                let val: f64 = num.parse().unwrap_or(0.0);
                match ch {
                    'D' | 'd' => { total_ms += val * 86_400_000.0; }
                    'H' | 'h' => { total_ms += val * 3_600_000.0; }
                    'M' | 'm' => {
                        if in_time { total_ms += val * 60_000.0; } else { /* months unsupported */ }
                    }
                    'S' | 's' => { total_ms += val * 1000.0; }
                    'W' | 'w' => { total_ms += val * 7.0 * 86_400_000.0; }
                    _ => {}
                }
                num.clear();
            }
        }
        // If dangling number without designator, ignore
        let ms = total_ms.round() as i64;
        if ms == 0 { None } else { Some(ms) }
    }
}

// --- Prepared Lua VM cache for ScriptRegistry snapshots ---
// This is used both in a query-scoped cache (preferred) and as a
// thread-local fallback when no query context is bound.
pub(crate) struct PreparedLua {
    stamp: u64,
    lua: mlua::Lua,
}

impl std::fmt::Debug for PreparedLua {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedLua").field("stamp", &self.stamp).finish_non_exhaustive()
    }
}

thread_local! {
    static TLS_PREPARED_LUA: RefCell<Option<PreparedLua>> = const { RefCell::new(None) };
}

impl ScriptRegistry {
    // Compute a stable stamp for the current registry contents (scripts + meta versions).
    // This should change whenever any script text or version changes.
    fn scripts_stamp(&self) -> u64 {
        use std::hash::Hasher as _;
        let g = self.inner.lock();
        let m = self.meta.lock();
        let mut names: Vec<&String> = g.keys().collect();
        names.sort();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        // Include addresses of the underlying Arc targets so different registries
        // (even with identical contents) don't collide across sessions/threads.
        let iptr = std::sync::Arc::as_ptr(&self.inner) as usize;
        let mptr = std::sync::Arc::as_ptr(&self.meta) as usize;
        iptr.hash(&mut hasher);
        mptr.hash(&mut hasher);
        // Include number of functions
        names.len().hash(&mut hasher);
        for n in names {
            // Function name
            n.hash(&mut hasher);
            // Hash full script contents (not just length) to avoid collisions across registries
            if let Some(code) = g.get(n) {
                // Hash both length and bytes for extra safety
                code.len().hash(&mut hasher);
                hasher.write(code.as_bytes());
            }
            // Hash full metadata if present (kind, returns schema, nullable, version)
            if let Some(meta) = m.get(n) {
                // kind
                match meta.kind {
                    ScriptKind::Scalar => 0u8.hash(&mut hasher),
                    ScriptKind::Aggregate => 1u8.hash(&mut hasher),
                    ScriptKind::Constraint => 2u8.hash(&mut hasher),
                    ScriptKind::Tvf => 3u8.hash(&mut hasher),
                }
                // returns: hash Debug representation for stability across Polars versions
                meta.returns.len().hash(&mut hasher);
                for dt in &meta.returns {
                    use std::fmt::Write as _;
                    let mut s = String::new();
                    let _ = write!(&mut s, "{:?}", dt);
                    s.hash(&mut hasher);
                }
                // Hash tvf columns schema
                meta.tvf_columns.len().hash(&mut hasher);
                for (n, dt) in &meta.tvf_columns {
                    n.hash(&mut hasher);
                    use std::fmt::Write as _;
                    let mut s = String::new();
                    let _ = write!(&mut s, "{:?}", dt);
                    s.hash(&mut hasher);
                }
                meta.nullable.hash(&mut hasher);
                meta.version.hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    // Run a closure with a prepared Lua VM for this registry snapshot using a per-thread cache.
    fn with_prepared_lua<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mlua::Lua) -> Result<R>,
    {
        use mlua::Lua;
        let stamp = self.scripts_stamp();
        debug!("[UDF LUA] with_prepared_lua: current stamp={}", stamp);
        TLS_PREPARED_LUA.with(|cell| -> Result<R> {
            let need_new = {
                let opt = cell.borrow();
                match *opt { 
                    Some(PreparedLua { stamp: s, .. }) if s == stamp => {
                        debug!("[UDF LUA] with_prepared_lua: reusing cached Lua VM (stamp matches)");
                        false
                    }, 
                    Some(PreparedLua { stamp: s, .. }) => {
                        debug!("[UDF LUA] with_prepared_lua: cached Lua VM outdated (stamp {} != {}), will rebuild", s, stamp);
                        true
                    },
                    None => {
                        debug!("[UDF LUA] with_prepared_lua: no cached Lua VM, will create new");
                        true
                    }
                }
            };
            if need_new {
                let snapshot: std::collections::HashMap<String, String> = { self.inner.lock().clone() };
                debug!("[UDF LUA] with_prepared_lua: creating new Lua VM and loading {} scripts", snapshot.len());
                let lua = Lua::new();
                // Configure Lua package.path to include packages from all known roots
                if let Err(e) = configure_lua_package_paths(&lua) {
                    tracing::debug!(target: "clarium::udf", "[UDF LUA] configure package paths failed: {}", e);
                }
                for (n, code) in snapshot.iter() { 
                    debug!("[UDF LUA] with_prepared_lua: loading script '{}' into Lua VM", n);
                    if let Err(e) = lua.load(code.as_str()).exec() {
                        // Do not abort the whole VM build; log and continue with remaining scripts
                        tracing::error!(target: "clarium::udf", "[UDF LUA] Failed to load script '{}' into Lua VM: {}", n, e);
                        let _ = write_script_error_log_for_name(n, "vm-load", &format!("Failed to load script into Lua VM: {}", e));
                        continue;
                    }
                }
                debug!("[UDF LUA] with_prepared_lua: all scripts loaded, caching Lua VM with stamp={}", stamp);
                *cell.borrow_mut() = Some(PreparedLua { stamp, lua });
            }
            let opt = cell.borrow();
            let lua_ref: &Lua = &opt.as_ref().unwrap().lua;
            f(lua_ref)
        })
    }

    /// Run a closure with the prepared Lua VM and resolved function handle for this
    /// registry snapshot on the current thread. This is optimized for per-chunk execution
    /// where the same function is called repeatedly for many rows.
    pub fn with_lua_function<F, R>(&self, name: &str, f: F) -> Result<R>
    where
        F: FnOnce(&mlua::Lua, mlua::Function) -> Result<R>,
    {
        self.with_prepared_lua(|lua| {
            let globals = lua.globals();
            let lname = Self::norm(name);
            // Check if the global exists and is a function
            let value: mlua::Value = globals.get(lname.as_str())
                .map_err(|e| anyhow!("UDF '{}' error: {}", name, e))?;
            if value.is_nil() {
                // Log current registry snapshot to aid debugging when function is missing
                if cfg!(debug_assertions) {
                    let snap = self.debug_snapshot();
                    tracing::debug!(target: "clarium::udf", "UDF lookup miss: '{}' registry-snapshot: {}", name, snap);
                }
                // First attempt: the registry may contain the function source but the current
                // prepared Lua VM was created before it was added. Try to inject from registry.
                if let Some(code) = { self.inner.lock().get(&lname).cloned() } {
                    if let Err(e) = lua.load(code.as_str()).exec() {
                        tracing::debug!(target: "clarium::udf", "UDF '{}' inject-from-registry failed: {}", name, e);
                    } else {
                        // Retry lookup after injecting
                        let value2: mlua::Value = globals.get(lname.as_str())
                            .map_err(|e| anyhow!("UDF '{}' error: {}", name, e))?;
                        if !value2.is_nil() {
                            let func: mlua::Function = match value2 {
                                mlua::Value::Function(f) => f,
                                _ => return Err(anyhow!("UDF '{}' is not a function", name)),
                            };
                            return f(lua, func).map_err(|e| anyhow!("UDF '{}' error: {}", name, e));
                        }
                    }
                }
                // Attempt to auto-load from disk if present in any global scripts folder.
                if let Some(code) = self.try_load_script_from_global_scripts(name)? {
                    // Load into the current Lua VM for immediate availability
                    lua.load(code.as_str()).exec()
                        .map_err(|e| anyhow!("UDF '{}' load error: {}", name, e))?;
                    // Try again to fetch the function
                    let value2: mlua::Value = globals.get(lname.as_str())
                        .map_err(|e| anyhow!("UDF '{}' error: {}", name, e))?;
                    if value2.is_nil() {
                        tracing::debug!(target: "clarium::udf", "UDF still nil after autoload: '{}'", name);
                        return Err(anyhow!("UDF '{}' not found after loading script", name));
                    }
                    let func: mlua::Function = match value2 {
                        mlua::Value::Function(f) => f,
                        _ => return Err(anyhow!("UDF '{}' is not a function", name)),
                    };
                    // Wrap any execution errors with UDF context
                    return f(lua, func).map_err(|e| anyhow!("UDF '{}' error: {}", name, e));
                } else {
                    // Auto-load did not find a file; report the exact paths we attempted
                    let candidates = candidate_udf_script_paths(name);
                    let hint = candidates
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>()
                        .join("; ");
                    tracing::debug!(target: "clarium::udf", "UDF autoload miss: '{}' searched=[{}]", name, hint);
                    return Err(anyhow!(
                        "UDF '{}' not found in registry. Auto-load looked for a script at: {}",
                        name, hint
                    ));
                }
            }
            let func: mlua::Function = match value {
                mlua::Value::Function(f) => f,
                _ => return Err(anyhow!("UDF '{}' is not a function", name)),
            };
            // Wrap any execution errors with UDF context
            f(lua, func).map_err(|e| anyhow!("UDF '{}' error: {}", name, e))
        })
    }

    /// Register a Rust function accessible from Lua that returns context values on demand.
    /// This allows Lua functions to call get_context(key) to retrieve values like current_database, etc.
    /// This is more efficient than setting global tables every time, as values are only accessed when needed.
    pub fn register_context_accessor(lua: &mlua::Lua, ctx_info: &ContextInfo) -> Result<()> {
        use mlua::Value as LuaValue;
        
        // Clone the context info for the closure (it's lightweight and Clone + Send + Sync)
        let ctx = ctx_info.clone();
        
        // Create a Lua function that takes a key and returns the corresponding context value
        let get_context = lua.create_function(move |lua, key: String| {
            let key_lower = key.to_ascii_lowercase();
            let result: mlua::Result<LuaValue> = match key_lower.as_str() {
                "current_database" => Ok(match ctx.current_database {
                    Some(ref s) => LuaValue::String(lua.create_string(s)?),
                    None => LuaValue::Nil,
                }),
                "current_schema" => Ok(match ctx.current_schema {
                    Some(ref s) => LuaValue::String(lua.create_string(s)?),
                    None => LuaValue::Nil,
                }),
                "current_user" => Ok(match ctx.current_user {
                    Some(ref s) => LuaValue::String(lua.create_string(s)?),
                    None => LuaValue::Nil,
                }),
                "session_user" => Ok(match ctx.session_user {
                    Some(ref s) => LuaValue::String(lua.create_string(s)?),
                    None => LuaValue::Nil,
                }),
                "transaction_timestamp" => Ok(match ctx.transaction_timestamp_secs {
                    Some(v) => LuaValue::Number(v),
                    None => LuaValue::Nil,
                }),
                "statement_timestamp" => Ok(match ctx.statement_timestamp_secs {
                    Some(v) => LuaValue::Number(v),
                    None => LuaValue::Nil,
                }),
                _ => Ok(LuaValue::Nil),
            };
            result
        })?;
        
        // Register the function as a global
        lua.globals().set("get_context", get_context)?;
        Ok(())
    }
}

/// Lightweight context info that is Send + Sync for passing to Lua execution closures.
#[derive(Clone, Debug)]
pub struct ContextInfo {
    pub current_database: Option<String>,
    pub current_schema: Option<String>,
    pub current_user: Option<String>,
    pub session_user: Option<String>,
    pub transaction_timestamp_secs: Option<f64>,
    pub statement_timestamp_secs: Option<f64>,
}

impl ContextInfo {
    /// Extract context info from DataContext for use in Send closures.
    pub fn from_data_context(ctx: &crate::server::data_context::DataContext) -> Self {
        let transaction_timestamp_secs = ctx.transaction_timestamp.and_then(|ts| {
            ts.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_secs() as f64)
        });
        let statement_timestamp_secs = ctx.statement_timestamp.and_then(|ts| {
            ts.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_secs() as f64)
        });
        
        ContextInfo {
            current_database: ctx.current_database.clone(),
            current_schema: ctx.current_schema.clone(),
            current_user: ctx.current_user.clone(),
            session_user: ctx.session_user.clone(),
            transaction_timestamp_secs,
            statement_timestamp_secs,
        }
    }
}

// Note: Query-scoped Lua cache was removed. The system now relies solely on
// the per-thread TLS_PREPARED_LUA cache keyed by the registry snapshot stamp.

#[derive(Clone, Copy)]
enum NullMode { StringNil, RealNil }

fn json_to_lua_mode<'lua>(lua: &'lua mlua::Lua, v: &serde_json::Value, mode: NullMode) -> Result<mlua::Value<'lua>> {
    use mlua::Value as LVal;
    let lv = match v {
        serde_json::Value::Null => match mode { NullMode::StringNil => LVal::String(lua.create_string("nil")?), NullMode::RealNil => LVal::Nil },
        serde_json::Value::Bool(b) => LVal::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() { LVal::Integer(i) } else { LVal::Number(n.as_f64().unwrap_or(0.0)) }
        }
        serde_json::Value::String(s) => LVal::String(lua.create_string(s)?),
        serde_json::Value::Array(arr) => {
            let tbl = lua.create_table()?;
            for (i, item) in arr.iter().enumerate() { tbl.set((i+1) as i64, json_to_lua_mode(lua, item, mode)?)?; }
            LVal::Table(tbl)
        }
        serde_json::Value::Object(map) => {
            let tbl = lua.create_table()?;
            for (k, val) in map.iter() { tbl.set(k.as_str(), json_to_lua_mode(lua, val, mode)?)?; }
            LVal::Table(tbl)
        }
    };
    Ok(lv)
}

fn str_to_dtype(s: &str) -> Result<DataType> {
    let u = s.to_ascii_lowercase();
    Ok(match u.as_str() {
        "int64" | "integer" | "bigint" => DataType::Int64,
        "float64" | "double" | "float" | "number" => DataType::Float64,
        "boolean" | "bool" => DataType::Boolean,
        "utf8" | "string" | "str" | "text" => DataType::String,
        "vector" => DataType::List(Box::new(DataType::Float64)),
        "null" => DataType::Null,
        // timestamp / datetime family
        "timestamp" | "timestamptz" | "timestampz" => DataType::Datetime(polars::prelude::TimeUnit::Milliseconds, None),
        v if v.starts_with("datetime") => DataType::Datetime(polars::prelude::TimeUnit::Milliseconds, None),
        // date/time
        "date" => DataType::Date,
        "time" => DataType::Time,
        // interval/duration (store as Duration[ms])
        "interval" | "duration" => DataType::Duration(polars::prelude::TimeUnit::Milliseconds),
        // any → leave to inference (we'll treat as no hint)
        "any" => DataType::Null,
        _ => return Err(anyhow!(format!("Unknown data type hint: {}", s))),
    })
}

fn lua_to_json(v: mlua::Value) -> Result<serde_json::Value> {
    use mlua::Value as LVal;
    let j = match v {
        LVal::Nil => serde_json::Value::Null,
        LVal::Boolean(b) => serde_json::Value::Bool(b),
        LVal::Integer(i) => serde_json::json!(i),
        LVal::Number(f) => serde_json::json!(f),
        LVal::String(s) => serde_json::Value::String(s.to_str()?.to_string()),
        LVal::Table(t) => {
            // Try array-like first
            let mut arr: Vec<serde_json::Value> = Vec::new();
            let mut is_array = true;
            let mut max_idx = 0i64;
            for pair in t.clone().pairs::<mlua::Value, mlua::Value>() {
                let (k, _v) = pair?;
                match k { LVal::Integer(i) => { if i > max_idx { max_idx = i; } }, _ => { is_array = false; break; } }
            }
            if is_array && max_idx > 0 {
                for i in 1..=max_idx { let val = t.get::<i64, mlua::Value>(i).unwrap_or(mlua::Value::Nil); arr.push(lua_to_json(val)?); }
                serde_json::Value::Array(arr)
            } else {
                let mut map = serde_json::Map::new();
                for pair in t.pairs::<mlua::Value, mlua::Value>() { let (k, v) = pair?; if let LVal::String(s) = k { map.insert(s.to_str()?.to_string(), lua_to_json(v)?); } }
                serde_json::Value::Object(map)
            }
        }
        _ => serde_json::Value::Null,
    };
    Ok(j)
}

/// Write an error log file adjacent to the given script path.
/// The log file name will be `<script_stem>.error.log` and will contain stage, function name,
/// timestamp (unix seconds), and the full error message.
fn write_script_error_log_adjacent(script_path: &Path, stage: &str, func_name: &str, err_msg: &str) -> Result<()> {
    let stem = script_path.file_stem().and_then(|s| s.to_str()).unwrap_or("script");
    let log_path = script_path.with_file_name(format!("{}.error.log", stem));
    // Compose log content
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let _ = writeln!(file, "--- clarium lua script error ---");
    let _ = writeln!(file, "time_unix={} stage={} function={} file={}", ts, stage, func_name, script_path.display());
    let _ = writeln!(file, "error: {}", err_msg);
    let _ = writeln!(file, "");
    Ok(())
}

/// Resolve a script path by function name (handles schema-qualified names) and write an adjacent error log.
fn write_script_error_log_for_name(func_name: &str, stage: &str, err_msg: &str) -> Result<()> {
    // Strip schema qualification if present (e.g., "pg_catalog.name" -> "name")
    let base = func_name.rsplit('.').next().unwrap_or(func_name);
    if let Some((path, _kind)) = find_function_script_in_global_scripts(base) {
        let _ = write_script_error_log_adjacent(&path, stage, base, err_msg);
    }
    Ok(())
}

/// Compute the scripts directory for a given database and schema under root.
pub fn scripts_dir_for(root: &Path, db: &str, schema: &str) -> PathBuf {
    root.join(db).join(schema).join("scripts")
}

/// Configure Lua `package.path` and `package.cpath` to include known packages folders.
/// Adds entries for both global scripts roots and extra per-database roots:
/// - <root>/packages/?.lua
/// - <root>/packages/?/init.lua
fn configure_lua_package_paths(lua: &mlua::Lua) -> Result<()> {
    use std::path::PathBuf;
    // Build list of unique package roots
    let mut roots: Vec<PathBuf> = Vec::new();
    for r in global_scripts_roots() { roots.push(r); }
    for r in extra_script_roots() { roots.push(r); }
    // Deduplicate
    let mut uniq: Vec<PathBuf> = Vec::new();
    for r in roots.into_iter() {
        if !uniq.iter().any(|x| x == &r) { uniq.push(r); }
    }
    // Read existing package.path
    let pkg: mlua::Table = lua.globals().get("package")?;
    let cur_path: String = pkg.get("path").unwrap_or_else(|_| String::new());
    let mut path = cur_path;
    // Windows backslashes; Lua accepts both, but we follow OS paths
    for root in uniq.iter() {
        let p1 = root.join("packages").join("?.lua");
        let p2 = root.join("packages").join("?").join("init.lua");
        let s1 = p1.to_string_lossy().replace("\\", "/");
        let s2 = p2.to_string_lossy().replace("\\", "/");
        if !path.is_empty() { path.push(';'); }
        path.push_str(&s1);
        path.push(';');
        path.push_str(&s2);
    }
    pkg.set("path", path)?;
    // Leave cpath unchanged for now
    Ok(())
}

/// Return a list of candidate global scripts roots to auto-load on startup.
/// Order:
/// 1) <exe_dir>/scripts
/// 2) ./scripts under current working directory
pub fn global_scripts_roots() -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            v.push(dir.join("scripts"));
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        v.push(cwd.join("scripts"));
    }
    // Include any extra roots registered by active stores, typically `<db_root>/.system/udf`.
    // These already contain subfolders like `scalars/`, `aggregates/`, etc.
    for r in extra_script_roots() { v.push(r); }
    v
}

/// Check if a function's script file exists under any global scripts root.
/// Looks in subfolders `scalars`, `aggregates`, `constraints`, and `tvfs` for `<name>.lua` (case-insensitive logical name).
fn function_exists_in_global_scripts(name: &str) -> bool {
    let lname = name.to_ascii_lowercase();
    for root in global_scripts_roots() {
        // scalars/<name>.lua
        let p1 = root.join("scalars").join(format!("{}.lua", lname));
        if p1.exists() { return true; }
        // aggregates/<name>.lua
        let p2 = root.join("aggregates").join(format!("{}.lua", lname));
        if p2.exists() { return true; }
        // constraints/<name>.lua
        let p3 = root.join("constraints").join(format!("{}.lua", lname));
        if p3.exists() { return true; }
        // tvfs/<name>.lua
        let p4 = root.join("tvfs").join(format!("{}.lua", lname));
        if p4.exists() { return true; }
        // packages/<name>.lua
        let p5 = root.join("packages").join(format!("{}.lua", lname));
        if p5.exists() { return true; }
    }
    false
}

/// Find a function script path under global roots and return its path and kind.
fn find_function_script_in_global_scripts(name: &str) -> Option<(PathBuf, ScriptKind)> {
    let lname = name.to_ascii_lowercase();
    for root in global_scripts_roots() {
        let p1 = root.join("scalars").join(format!("{}.lua", lname));
        if p1.exists() { return Some((p1, ScriptKind::Scalar)); }
        let p2 = root.join("aggregates").join(format!("{}.lua", lname));
        if p2.exists() { return Some((p2, ScriptKind::Aggregate)); }
        let p3 = root.join("constraints").join(format!("{}.lua", lname));
        if p3.exists() { return Some((p3, ScriptKind::Constraint)); }
        let p4 = root.join("tvfs").join(format!("{}.lua", lname));
        if p4.exists() { return Some((p4, ScriptKind::Tvf)); }
        let p5 = root.join("packages").join(format!("{}.lua", lname));
        if p5.exists() { return Some((p5, ScriptKind::Scalar)); }
    }
    None
}

/// Return all candidate full paths where the auto-loader will look for the given UDF name.
/// This includes `scalars/<name>.lua`, `aggregates/<name>.lua` and `constraints/<name>.lua` under each global
/// scripts root returned by `global_scripts_roots()`.
fn candidate_udf_script_paths(name: &str) -> Vec<PathBuf> {
    let lname = name.to_ascii_lowercase();
    let mut v = Vec::new();
    for root in global_scripts_roots() {
        v.push(root.join("scalars").join(format!("{}.lua", lname)));
        v.push(root.join("aggregates").join(format!("{}.lua", lname)));
        v.push(root.join("constraints").join(format!("{}.lua", lname)));
        v.push(root.join("tvfs").join(format!("{}.lua", lname)));
    }
    v
}

impl ScriptRegistry {
    /// Try to load a single script by name from global script roots into the registry.
    /// Returns the script code if loaded, or None if not found on disk.
    fn try_load_script_from_global_scripts(&self, name: &str) -> Result<Option<String>> {
        if let Some((path, kind)) = find_function_script_in_global_scripts(name) {
            let code = fs::read_to_string(&path)?;
            // Debug-print which script file is being auto-loaded to aid diagnostics
            debug!(
                "[scripts] auto-loaded UDF '{}' from {}",
                name,
                path.display()
            );
            // Update registry (bumps version/stamp)
            self.load_script_text(name, &code)?;
            let qualified = format!("pg_catalog.{}", name);
            self.load_script_text(&qualified, &code)?;

            // Attempt to load metadata similar to bulk loader
            let mut applied_meta = false;
            let sidecar = path.with_extension("meta.json");
            if sidecar.exists() {
                if let Ok(txt) = fs::read_to_string(&sidecar) {
                    if let Ok(meta) = Self::parse_meta_json(&txt, &kind) {
                        self.set_meta(name, meta.clone());
                        self.set_meta(&qualified, meta);
                        applied_meta = true;
                    }
                }
            }
            if !applied_meta {
                if let Some(meta) = Self::parse_embedded_meta(&code, &kind) {
                    self.set_meta(name, meta.clone());
                    self.set_meta(&qualified, meta);
                    applied_meta = true;
                }
            }
            if !applied_meta {
                if let Ok(meta) = self.fetch_meta_via_lua(name, &kind) {
                    self.set_meta(name, meta.clone());
                    self.set_meta(&qualified, meta);
                    applied_meta = true;
                }
            }
            if !applied_meta {
                let meta = ScriptMeta { kind, returns: Vec::new(), nullable: true, version: 0, tvf_columns: Vec::new() };
                self.set_meta(name, meta.clone());
                self.set_meta(&qualified, meta);
            }

            Ok(Some(code))
        } else {
            Ok(None)
        }
    }
}

/// Load global default scripts from well-known locations. This is intended to
/// provide built-in UDFs that are always present, independent of any database/schema.
///
/// It will attempt each candidate root; missing folders are ignored.
pub fn load_global_default_scripts(reg: &ScriptRegistry) -> Result<()> {
    debug!("[UDF LOAD] load_global_default_scripts: starting to load global scripts");
    let roots = global_scripts_roots();
    debug!("[UDF LOAD] load_global_default_scripts: checking {} script root(s)", roots.len());
    for root in roots {
        debug!("[UDF LOAD] load_global_default_scripts: attempting to load from root: {:?}", root);
        // Reuse schema loader semantics: expect subfolders scalars/ and aggregates/
        match reg.load_from_schema_root(&root) {
            Ok(_) => debug!("[UDF LOAD] load_global_default_scripts: successfully loaded from root: {:?}", root),
            Err(e) => debug!("[UDF LOAD] load_global_default_scripts: failed to load from root {:?}: {}", root, e),
        }
    }
    debug!("[UDF LOAD] load_global_default_scripts: finished loading, registry has {} functions", reg.inner.lock().len());
    Ok(())
}

/// Load scripts for a schema directory. Backward-compatible wrapper that now
/// loads from `scalars` and `aggregates` subfolders and fetches metadata.
pub fn load_all_scripts_for_schema(reg: &ScriptRegistry, dir: &Path) -> Result<()> {
    reg.load_from_schema_root(dir)
}

// Session-local registry override removed. Use the global registry directly.
pub fn with_session_registry<F, R>(_reg: &ScriptRegistry, f: F) -> R
where
    F: FnOnce() -> R,
{
    // No-op wrapper retained for compatibility
    f()
}

// Global script registry used by all execution paths. This registry is thread-safe
// and does not hold or reference any Lua state directly.
static GLOBAL_REG: once_cell::sync::Lazy<parking_lot::RwLock<Option<ScriptRegistry>>> = once_cell::sync::Lazy::new(|| parking_lot::RwLock::new(None));

/// Initialize or update the global script registry.
/// If the registry is already initialized, merge/update scripts in the existing registry
/// instead of replacing it. This ensures the registry is only initialized once.
pub fn init_script_registry(reg: ScriptRegistry) {
    debug!("[UDF REGISTRY] init_script_registry: starting, incoming registry has {} functions", reg.inner.lock().len());
    let mut w = GLOBAL_REG.write();
    if let Some(existing) = w.as_ref() {
        // Registry already exists - merge scripts from new registry into existing one
        debug!("[UDF REGISTRY] init_script_registry: merging into existing registry");
        let scripts = reg.inner.lock();
        debug!("[UDF REGISTRY] init_script_registry: merging {} scripts", scripts.len());
        for (name, code) in scripts.iter() {
            debug!("[UDF REGISTRY] init_script_registry: merging script '{}'", name);
            let _ = existing.load_script_text(name, code);
        }
        let metas = reg.meta.lock();
        for (name, meta) in metas.iter() {
            debug!("[UDF REGISTRY] init_script_registry: merging meta for '{}', kind={:?}", name, meta.kind);
            existing.set_meta(name, meta.clone());
        }
        // Bump generation to invalidate any prepared Lua caches that may hold
        // an older snapshot of the registry contents.
        GLOBAL_REG_GEN.fetch_add(1, Ordering::Relaxed);
        debug!("[UDF REGISTRY] init_script_registry: merge complete, global registry now has {} functions", existing.inner.lock().len());
    } else {
        // First initialization
        debug!("[UDF REGISTRY] init_script_registry: first initialization with {} functions", reg.inner.lock().len());
        *w = Some(reg);
        GLOBAL_REG_GEN.fetch_add(1, Ordering::Relaxed);
    }
}

/// Initialize the global registry only if it hasn't been set yet.
/// Returns true if the registry was set by this call, false if an instance
/// was already present and is kept intact.
pub fn init_script_registry_once(reg: ScriptRegistry) -> bool {
    let mut w = GLOBAL_REG.write();
    if w.is_none() {
        *w = Some(reg);
        GLOBAL_REG_GEN.fetch_add(1, Ordering::Relaxed);
        true
    } else {
        // Already initialized: merge new scripts/meta into the existing registry
        if let Some(existing) = w.as_ref() {
            let scripts = reg.inner.lock();
            for (name, code) in scripts.iter() {
                let _ = existing.load_script_text(name, code);
            }
            let metas = reg.meta.lock();
            for (name, meta) in metas.iter() {
                existing.set_meta(name, meta.clone());
            }
            GLOBAL_REG_GEN.fetch_add(1, Ordering::Relaxed);
        }
        false
    }
}

pub fn get_script_registry() -> Option<ScriptRegistry> {
    // Always use the global registry; session-local override removed
    let reg = GLOBAL_REG.read().clone();
    if let Some(ref r) = reg {
        debug!("[UDF REGISTRY] get_script_registry: returning registry with {} functions", r.inner.lock().len());
    } else {
        debug!("[UDF REGISTRY] get_script_registry: registry not initialized, returning None");
    }
    reg
}

/// Return a diagnostic snapshot of the current global registry, if initialized.
pub fn debug_script_registry_snapshot() -> Option<String> {
    get_script_registry().map(|r| r.debug_snapshot())
}

// --- Concurrency diagnostics and safer global initialization ---
// A monotonic generation counter for the global registry. It changes when the
// global registry is initialized or merged into, allowing tests to detect
// concurrent modifications.
static GLOBAL_REG_GEN: once_cell::sync::Lazy<AtomicU64> = once_cell::sync::Lazy::new(|| AtomicU64::new(0));

/// Return the current global registry generation (monotonic counter).
pub fn script_registry_generation() -> u64 { GLOBAL_REG_GEN.load(Ordering::Relaxed) }

// --- Additional script roots (database-root based) ---
// Some tests and runtime code seed UDFs into per-database roots under `<db_root>/.system/udf`.
// To allow late auto-loading when a function is first referenced, we keep a list of extra
// script roots contributed by active stores. These are probed alongside the standard
// global candidates (exe_dir/scripts, cwd/scripts).
static EXTRA_SCRIPT_ROOTS: once_cell::sync::Lazy<parking_lot::RwLock<Vec<PathBuf>>> =
    once_cell::sync::Lazy::new(|| parking_lot::RwLock::new(Vec::new()));

/// Register an additional scripts root (typically `<db_root>/.system/udf`).
/// Idempotent: duplicate registrations are ignored.
pub fn register_udf_root(dir: &Path) {
    if dir.as_os_str().is_empty() { return; }
    let mut w = EXTRA_SCRIPT_ROOTS.write();
    if !w.iter().any(|p| p == dir) {
        w.push(dir.to_path_buf());
        debug!("[UDF LOAD] registered extra UDF root: {}", dir.display());
    }
}

/// Snapshot the currently registered extra script roots.
fn extra_script_roots() -> Vec<PathBuf> { EXTRA_SCRIPT_ROOTS.read().clone() }


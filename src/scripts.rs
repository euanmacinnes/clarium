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
use polars::prelude::DataType;
use std::cell::RefCell;
use std::hash::Hash;
use tracing::debug;

#[derive(Clone, Default)]
pub struct ScriptRegistry {
    inner: std::sync::Arc<Mutex<HashMap<String, String>>>, // name -> source
    meta: std::sync::Arc<Mutex<HashMap<String, ScriptMeta>>>, // name -> metadata (kind, return types)
}

#[derive(Clone, Debug)]
#[derive(Default)]
pub enum ScriptKind { #[default]
Scalar, Aggregate }


#[derive(Clone, Debug, Default)]
pub struct ScriptMeta {
    pub kind: ScriptKind,
    pub returns: Vec<DataType>, // empty means unknown; single means scalar; >1 means multi-return
    pub nullable: bool,
    pub version: u64, // bump when code changes
}

impl ScriptRegistry {
    pub fn new() -> Result<Self> { Ok(Self::default()) }

    #[inline]
    fn norm(name: &str) -> String { name.to_ascii_lowercase() }

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

    /// Load all .lua scripts in subfolders `scalars` and `aggregates` into the registry
    /// and fetch optional metadata from one of (in order):
    /// 1) Sidecar JSON file `<name>.meta.json` next to the Lua file
    /// 2) Embedded JSON docstring at the top of the Lua file inside a block comment `--[[ { ... } ]]`
    /// 3) Calling a `<name>__meta()` Lua function if present in any loaded script
    /// 4) Fallback default metadata
    pub fn load_from_schema_root(&self, dir: &Path) -> Result<()> {
        if !dir.exists() { return Ok(()); }
        let scalars = dir.join("scalars");
        let aggregates = dir.join("aggregates");
        let load_dir = |folder: &Path, kind: ScriptKind| -> Result<()> {
            if !folder.exists() { return Ok(()); }
            for ent in fs::read_dir(folder)? {
                let ent = ent?;
                let p = ent.path();
                if p.is_file() && p.extension().and_then(|e| e.to_str()).unwrap_or("").eq_ignore_ascii_case("lua") {
                    let name = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                    let code = fs::read_to_string(&p)?;
                    // Load under unqualified name
                    self.load_script_text(&name, &code)?;
                    // Also expose all globally provided functions under pg_catalog.<name>
                    // so clients using schema-qualified calls can resolve them.
                    let qualified = format!("pg_catalog.{}", name);
                    self.load_script_text(&qualified, &code)?;
                    // Try sidecar .meta.json
                    let mut applied_meta = false;
                    let sidecar = p.with_extension("meta.json");
                    if sidecar.exists() {
                        if let Ok(txt) = fs::read_to_string(&sidecar) {
                            if let Ok(meta) = Self::parse_meta_json(&txt, &kind) {
                                self.set_meta(&name, meta.clone());
                                self.set_meta(&qualified, meta);
                                applied_meta = true;
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
                        if let Ok(meta) = self.fetch_meta_via_lua(&name, &kind) {
                            self.set_meta(&name, meta.clone());
                            self.set_meta(&qualified, meta);
                            applied_meta = true;
                        }
                    }
                    // default meta when not provided
                    if !applied_meta {
                        let meta = ScriptMeta { kind: kind.clone(), returns: Vec::new(), nullable: true, version: 0 };
                        self.set_meta(&name, meta.clone());
                        self.set_meta(&qualified, meta);
                    }
                }
            }
            Ok(())
        };
        load_dir(&scalars, ScriptKind::Scalar)?;
        load_dir(&aggregates, ScriptKind::Aggregate)?;
        Ok(())
    }

    fn parse_meta_json(txt: &str, default_kind: &ScriptKind) -> Result<ScriptMeta> {
        let v: serde_json::Value = serde_json::from_str(txt)?;
        Self::meta_from_json_value(v, default_kind)
    }

    fn meta_from_json_value(v: serde_json::Value, default_kind: &ScriptKind) -> Result<ScriptMeta> {
        let mut meta = ScriptMeta { kind: default_kind.clone(), returns: Vec::new(), nullable: true, version: 0 };
        if let Some(k) = v.get("kind").and_then(|x| x.as_str()) {
            meta.kind = if k.eq_ignore_ascii_case("aggregate") { ScriptKind::Aggregate } else { ScriptKind::Scalar };
        }
        if let Some(nul) = v.get("nullable").and_then(|x| x.as_bool()) { meta.nullable = nul; }
        if let Some(arr) = v.get("returns").and_then(|x| x.as_array()) {
            let mut outs = Vec::new();
            for s in arr.iter().filter_map(|e| e.as_str()) { outs.push(str_to_dtype(s)?); }
            meta.returns = outs;
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
        for (_n, code) in snapshot.iter() { lua.load(code.as_str()).exec()?; }
        let globals = lua.globals();
        let meta_fn: Option<mlua::Function> = globals.get(format!("{}__meta", name).as_str()).ok();
        let mut meta = ScriptMeta { kind: default_kind.clone(), returns: Vec::new(), nullable: true, version: 0 };
        if let Some(mf) = meta_fn {
            let v: mlua::Value = mf.call(())?;
            // Expect a table with fields: kind, returns (array of strings), nullable (bool)
            if let mlua::Value::Table(t) = v {
                if let Ok(k) = t.get::<_, String>("kind") { meta.kind = if k.eq_ignore_ascii_case("aggregate") { ScriptKind::Aggregate } else { ScriptKind::Scalar }; }
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
                    let kind = match meta.kind { ScriptKind::Scalar => "scalar", ScriptKind::Aggregate => "aggregate" };
                    let returns: Vec<&'static str> = meta.returns.iter().map(|dt| match dt {
                        DataType::Boolean => "bool",
                        DataType::Int64 => "int64",
                        DataType::Float64 => "float64",
                        DataType::String => "string",
                        DataType::Null => "null",
                        DataType::Datetime(_, _) => "datetime",
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
                }
                // returns: hash Debug representation for stability across Polars versions
                meta.returns.len().hash(&mut hasher);
                for dt in &meta.returns {
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
                for (n, code) in snapshot.iter() { 
                    debug!("[UDF LUA] with_prepared_lua: loading script '{}' into Lua VM", n);
                    lua.load(code.as_str()).exec()?; 
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
        "null" => DataType::Null,
        // datetime types could be specified as datetime[ms]
        v if v.starts_with("datetime") => DataType::Datetime(polars::prelude::TimeUnit::Milliseconds, None),
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

/// Compute the scripts directory for a given database and schema under root.
pub fn scripts_dir_for(root: &Path, db: &str, schema: &str) -> PathBuf {
    root.join(db).join(schema).join("scripts")
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
    v
}

/// Check if a function's script file exists under any global scripts root.
/// Looks in subfolders `scalars` and `aggregates` for `<name>.lua` (case-insensitive logical name).
fn function_exists_in_global_scripts(name: &str) -> bool {
    let lname = name.to_ascii_lowercase();
    for root in global_scripts_roots() {
        // scalars/<name>.lua
        let p1 = root.join("scalars").join(format!("{}.lua", lname));
        if p1.exists() { return true; }
        // aggregates/<name>.lua
        let p2 = root.join("aggregates").join(format!("{}.lua", lname));
        if p2.exists() { return true; }
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
    }
    None
}

/// Return all candidate full paths where the auto-loader will look for the given UDF name.
/// This includes both `scalars/<name>.lua` and `aggregates/<name>.lua` under each global
/// scripts root returned by `global_scripts_roots()`.
fn candidate_udf_script_paths(name: &str) -> Vec<PathBuf> {
    let lname = name.to_ascii_lowercase();
    let mut v = Vec::new();
    for root in global_scripts_roots() {
        v.push(root.join("scalars").join(format!("{}.lua", lname)));
        v.push(root.join("aggregates").join(format!("{}.lua", lname)));
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
                let meta = ScriptMeta { kind, returns: Vec::new(), nullable: true, version: 0 };
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


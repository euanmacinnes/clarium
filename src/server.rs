//!
//! clarium HTTP/WS server
//! -----------------------
//! This module defines the Axum-based HTTP API and WebSocket interface for clarium.
//! It also optionally launches a pgwire endpoint when compiled with the `pgwire` feature.
//!
//! Responsibilities:
//! - Session management with a simple cookie + CSRF token model.
//! - Login/logout endpoints backed by the `security` module.
//! - Data write and query endpoints delegating to the query engine.
//! - Per-session defaults for current database and schema (default: clarium/public).
//! - WebSocket endpoint for interactive queries.
//! - First-run demo dataset creation and startup inventory logs.

use std::{net::SocketAddr, collections::HashMap};

use axum::{routing::{get, post}, Router, extract::{State, ws::{WebSocketUpgrade, Message}, Path}, Json};
use axum::response::IntoResponse;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use serde::{Serialize, Deserialize};
use tracing::{info, error};
use tokio::sync::RwLock;
use tokio::sync::watch;
use getrandom::getrandom;
use anyhow::Context;
use std::panic::{AssertUnwindSafe};
use futures_util::FutureExt; // for catch_unwind on async blocks
use std::time::{Duration, Instant};

use crate::{storage::{SharedStore, Record}, security};
pub mod query;
pub mod exec;
pub mod data_context;
pub mod graphstore; // direct graph storage engine (scaffolding)
use serde_json::json;
use polars::prelude::*;
use crate::scripts::{ScriptRegistry, scripts_dir_for, load_all_scripts_for_schema, load_global_default_scripts};
use crate::ident::{DEFAULT_DB, DEFAULT_SCHEMA};

const SESSION_COOKIE: &str = "clarium_session";

/// Returns a normalized transaction command kind if the text is a transaction control statement.
/// Supported commands: BEGIN, START TRANSACTION, COMMIT, END, ROLLBACK (case-insensitive; optional trailing semicolon).
fn detect_transaction_cmd(text: &str) -> Option<&'static str> {
    let up = text.trim();
    if up.is_empty() { return None; }
    // Strip a single trailing semicolon if present
    let up = up.strip_suffix(';').unwrap_or(up).trim();
    let up = up.to_ascii_uppercase();
    match up.as_str() {
        "BEGIN" => Some("BEGIN"),
        "START TRANSACTION" => Some("BEGIN"),
        "COMMIT" => Some("COMMIT"),
        // PostgreSQL accepts END as an alias for COMMIT
        "END" => Some("COMMIT"),
        "ROLLBACK" => Some("ROLLBACK"),
        _ => None,
    }
}

/// Shared server state injected into all handlers.
///
/// Holds the global `SharedStore` handle, session maps (usernames and CSRF tokens),
/// and per-session defaults for current (database, schema). The defaults are used
/// when handling unqualified table names over HTTP/WS.
#[derive(Clone)]
pub struct AppState {
    pub store: SharedStore,
    pub db_root: String,
    /// Lua script registry for UDFs
    pub scripts: ScriptRegistry,
    /// Session id -> username mapping
    pub sessions: std::sync::Arc<RwLock<HashMap<String, String>>>,
    /// Session id -> CSRF token mapping
    pub csrf_tokens: std::sync::Arc<RwLock<HashMap<String, String>>>,
    /// Session id -> (database, schema) mapping
    pub session_defaults: std::sync::Arc<RwLock<HashMap<String, (String, String)>>>,
    /// Session id -> metadata (issued_at, last_seen)
    pub session_meta: std::sync::Arc<RwLock<HashMap<String, SessionMeta>>>,
    /// Brute-force protection: (ip, username) -> attempt state (login)
    pub login_attempts: std::sync::Arc<RwLock<HashMap<(String, String), AttemptState>>>,
}

#[derive(Debug, Clone, Copy)]
pub struct SessionMeta {
    pub issued_at: Instant,
    pub last_seen: Instant,
}

#[derive(Debug, Clone, Copy)]
pub struct AttemptState {
    pub window_start: Instant,
    pub count_in_window: u32,
    pub failures: u32,
    pub backoff_until: Option<Instant>,
}

fn session_idle_timeout() -> Duration {
    let secs = std::env::var("CLARIUM_SESSION_IDLE_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(30 * 60);
    Duration::from_secs(secs)
}

fn session_absolute_lifetime() -> Duration {
    let secs = std::env::var("CLARIUM_SESSION_ABS_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(24 * 60 * 60);
    Duration::from_secs(secs)
}

/// Start the clarium HTTP server (and optional pgwire) bound to the given ports.
///
/// This sets up the store, ensures an admin user exists, creates a demo dataset on
/// first run (when the store is empty), prints installed databases/schemas, and
/// mounts all HTTP and WebSocket routes.
fn log_startup_folders(db_root: &str) {
    // Gather basic environment and folder info
    let cwd = std::env::current_dir().ok();
    let exe = std::env::current_exe().ok();
    let user = std::env::var("USER").or_else(|_| std::env::var("USERNAME")).ok();
    let home = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")).ok();
    let work_dir = std::env::var("PWD").ok();
    let db_env = std::env::var("clarium_DB_FOLDER").ok();

    info!(
        target: "startup",
        "clarium starting. Folder configuration: cwd={:?}, exe={:?}, user={:?}, home={:?}, work_dir_env={:?}, db_root_param={:?}, clarium_DB_FOLDER_env={:?}",
        cwd, exe, user, home, work_dir, db_root, db_env
    );

    // Check existence of key directories
    let etc_clarama = std::path::Path::new("/etc/clarama");
    let cwd_exists = cwd.as_ref().map(|p| p.exists()).unwrap_or(false);
    let etc_exists = etc_clarama.exists();
    let db_path = std::path::Path::new(db_root);
    let db_exists = db_path.exists();
    info!(
        target: "startup",
        "Path existence: cwd_exists={}, /etc/clarama_exists={}, db_root_exists={}",
        cwd_exists, etc_exists, db_exists
    );
}

#[inline]
fn env_default_db() -> String {
    std::env::var("CLARIUM_DEFAULT_DB").unwrap_or_else(|_| DEFAULT_DB.to_string())
}

#[inline]
fn env_default_schema() -> String {
    std::env::var("CLARIUM_DEFAULT_SCHEMA").unwrap_or_else(|_| DEFAULT_SCHEMA.to_string())
}

pub async fn run_with_ports(http_port: u16, pg_port: Option<u16>, db_root: &str) -> anyhow::Result<()> {
    // Print folder configuration as the very first thing on startup
    log_startup_folders(db_root);

    // Ensure the database root exists
    std::fs::create_dir_all(db_root)
        .with_context(|| format!("Failed to create or access database root: {}", db_root))?;
    // Ensure security default admin exists
    crate::security::ensure_default_admin(db_root)
        .with_context(|| format!("While ensuring default admin under db_root: {}", db_root))?;
    let store = SharedStore::new(db_root)
        .with_context(|| format!("While creating SharedStore with root: {}", db_root))?;

    // On first startup with an empty store (no tables), create a demo table with 1 week of per-second sine data.
    if is_store_completely_empty_three_level(db_root) {
        if let Err(e) = create_demo_dataset(&store) {
            tracing::warn!("Failed to create demo dataset: {}", e);
        }
    }

    // Print installed databases and schemas on startup
    print_installed_db_and_schemas(db_root);

    // Initialize scripts registry and load global default scripts, then per-db/schema scripts
    let scripts = ScriptRegistry::new()?;
    // Load globally bundled scripts (e.g., ./scripts and <exe>/scripts)
    let _ = load_global_default_scripts(&scripts);
    if let Ok(db_dirs) = std::fs::read_dir(db_root) {
        for dbent in db_dirs.flatten() {
            if dbent.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                let dbname = dbent.file_name().to_string_lossy().to_string();
                if let Ok(schema_dirs) = std::fs::read_dir(dbent.path()) {
                    for schent in schema_dirs.flatten() {
                        if schent.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                            let sname = schent.file_name().to_string_lossy().to_string();
                            let sdir = scripts_dir_for(std::path::Path::new(db_root), &dbname, &sname);
                            let _ = load_all_scripts_for_schema(&scripts, &sdir);
                        }
                    }
                }
            }
        }
    }

    // Make registry globally accessible for executor paths
    crate::scripts::init_script_registry(scripts.clone());

    // Shutdown signal (Ctrl-C) broadcaster
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // Start background KV sweeper (shutdown-aware)
    {
        let store_for_sweep = store.clone();
        let mut rx = shutdown_rx.clone();
        tokio::spawn(async move {
            use std::time::Duration;
            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        if *rx.borrow() { crate::tprintln!("[shutdown] kv_sweeper exiting on shutdown signal"); break; }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        // Sweep expired keys across all stores
                        let reg = store_for_sweep.kv_registry();
                        let removed = reg.sweep_all();
                        if removed > 0 { tracing::debug!(removed = removed, "kv_sweep"); }
                    }
                }
            }
        });
    }

    // Background GraphStore GC ticker (optional, shutdown-aware)
    {
        let store_for_gc = store.clone();
        let mut rx = shutdown_rx.clone();
        // Interval in seconds; default 60s; set to 0 or negative to disable
        let interval_sec: i64 = std::env::var("CLARIUM_GRAPH_GC_INTERVAL_SEC").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(60);
        if interval_sec > 0 {
            tokio::spawn(async move {
                use std::time::Duration;
                loop {
                    tokio::select! {
                        _ = rx.changed() => {
                            if *rx.borrow() { crate::tprintln!("[shutdown] graph_gc_ticker exiting on shutdown signal"); break; }
                        }
                        _ = tokio::time::sleep(Duration::from_secs(interval_sec as u64)) => {
                            crate::server::graphstore::gc_scan_all_graphs(&store_for_gc);
                        }
                    }
                }
            });
        } else {
            tracing::info!("graph_gc_ticker" = false, "GraphStore background GC ticker disabled");
        }
    }

    let app_state = AppState {
        store: store.clone(),
        db_root: db_root.to_string(),
        scripts,
        sessions: std::sync::Arc::new(RwLock::new(HashMap::new())),
        csrf_tokens: std::sync::Arc::new(RwLock::new(HashMap::new())),
        session_defaults: std::sync::Arc::new(RwLock::new(HashMap::new())),
        session_meta: std::sync::Arc::new(RwLock::new(HashMap::new())),
        login_attempts: std::sync::Arc::new(RwLock::new(HashMap::new())),
    };

    // Optionally start a basic pgwire listener on the provided port
    #[cfg(feature = "pgwire")]
    {
        if let Some(port) = pg_port.or(Some(5433)) {
            let store_clone = store.clone();
            let mut rx = shutdown_rx.clone();
            tokio::spawn(async move {
                let addr_pg: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
                if let Err(e) = crate::pgwire_server::start_pgwire(store_clone, &addr_pg.to_string(), rx).await {
                    tracing::error!("pgwire server error: {}", e);
                }
            });
        }
    }

    let app = Router::new()
        .route("/", get(|| async { "clarium ok" }))
        .route("/login", post(login))
        .route("/logout", post(logout))
        .route("/csrf", get(get_csrf))
        .route("/write/{database}", post(write))
        .route("/query", post(query_handler))
        .route("/use/database", post(use_database))
        .route("/use/schema", post(use_schema))
        .route("/ws", get(ws_handler))
        .with_state(app_state);

    let addr: SocketAddr = format!("0.0.0.0:{}", http_port).parse()?;
    info!("Starting server on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    // Prepare graceful shutdown future (Ctrl-C)
    let mut http_rx = shutdown_rx.clone();
    let shutdown_fut = async move {
        // Wait for either Ctrl-C or an external shutdown signal
        let ctrl_c = async {
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::error!("failed to listen for ctrl_c: {}", e);
            }
        };
        tokio::select! {
            _ = ctrl_c => { tracing::info!(target="shutdown", "Ctrl-C received, initiating graceful shutdown"); }
            _ = http_rx.changed() => { if *http_rx.borrow() { tracing::info!(target="shutdown", "Shutdown signal received, stopping HTTP server"); } }
        }
    };

    // Run server with graceful shutdown and spawn a Ctrl-C notifier
    let http_server = axum::serve(listener, app).with_graceful_shutdown(shutdown_fut);

    // Ctrl-C handler task: broadcast shutdown and perform cleanup
    {
        let store_for_cleanup = store.clone();
        let shutdown_tx2 = shutdown_tx.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::info!(target="shutdown", "Ctrl-C detected, broadcasting shutdown and cleaning up");
                let _ = shutdown_tx2.send(true);
                graceful_cleanup(&store_for_cleanup, http_port).await;
            }
        });
    }

    // Await the HTTP server (it will exit after shutdown signal)
    http_server.await?;

    Ok(())
}

/// Best-effort cleanup logic invoked on shutdown:
/// - sweep KV registries
/// - clear sessions and CSRF maps
/// - emit debug counters
async fn graceful_cleanup(store: &SharedStore, http_port: u16) {
    crate::tprintln!("[shutdown] starting graceful cleanup for http_port={}", http_port);
    // Sweep KV to drop expired keys before exit
    let reg = store.kv_registry();
    let removed = reg.sweep_all();
    tracing::info!(target="shutdown", removed = removed, "KV sweep completed during shutdown");
    // Nothing else to explicitly free; data is persisted. Session maps will be dropped with process.
    crate::tprintln!("[shutdown] cleanup complete");
}

/// Return true if the database root contains no three-level tables of the form
/// <db>/<schema>/<table>.time (i.e., contains no schema.json under any table directory).
fn is_store_completely_empty_three_level(db_root: &str) -> bool {
    use std::path::Path;
    let root = Path::new(db_root);
    if !root.exists() { return true; }
    if let Ok(rd) = std::fs::read_dir(root) {
        for db_entry in rd.flatten() {
            let dbp = db_entry.path();
            if !dbp.is_dir() { continue; }
            let db_name = db_entry.file_name().to_string_lossy().to_string();
            if db_name.starts_with('.') { continue; }
            if let Ok(sd) = std::fs::read_dir(&dbp) {
                for schema_entry in sd.flatten() {
                    let sp = schema_entry.path();
                    if !sp.is_dir() { continue; }
                    if let Ok(td) = std::fs::read_dir(&sp) {
                        for tentry in td.flatten() {
                            let tp = tentry.path();
                            if tp.is_dir() {
                                let sj = tp.join("schema.json");
                                if sj.exists() { return false; }
                            }
                        }
                    }
                }
            }
        }
    }
    true
}

/// Scan and print the list of installed databases and their schemas.
///
/// Only three-level layouts are considered; results are logged and printed.
fn print_installed_db_and_schemas(db_root: &str) {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;
    let root = Path::new(db_root);
    let mut db_to_schemas: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    if let Ok(rd) = std::fs::read_dir(root) {
        for db_entry in rd.flatten() {
            let dbp = db_entry.path();
            if !dbp.is_dir() { continue; }
            let db_name = db_entry.file_name().to_string_lossy().to_string();
            if db_name.starts_with('.') { continue; }
            if let Ok(sd) = std::fs::read_dir(&dbp) {
                for schema_entry in sd.flatten() {
                    let sp = schema_entry.path();
                    if !sp.is_dir() { continue; }
                    // Look for three-level layout: <db>/<schema>/<table>.time/schema.json
                    if let Ok(td) = std::fs::read_dir(&sp) {
                        for tentry in td.flatten() {
                            let tp = tentry.path();
                            if tp.is_dir() {
                                let sj = tp.join("schema.json");
                                if sj.exists() {
                                    let schema_name = schema_entry.file_name().to_string_lossy().to_string();
                                    db_to_schemas.entry(db_name.clone()).or_default().insert(schema_name);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if db_to_schemas.is_empty() {
        println!("No databases/schemas found under {}", db_root);
        tracing::info!("No databases/schemas found under {}", db_root);
        return;
    }
    println!("Installed databases and schemas:");
    tracing::info!("Installed databases and schemas:");
    for (db, schemas) in db_to_schemas.iter() {
        let list: Vec<String> = schemas.iter().cloned().collect();
        let joined = list.join(", ");
        println!("- {}: {}", db, joined);
        tracing::info!("- {}: {}", db, joined);
    }
}

/// Generate a one-week, per-second sine-wave dataset and write it to
/// clarium/public/demo.time. Used on first run when the store is empty.
fn create_demo_dataset(store: &SharedStore) -> anyhow::Result<()> {
    println!("Empty startup detected, creating demo dataset");
    use std::time::{SystemTime, UNIX_EPOCH};
    let now_ms: i64 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
    let week_ms: i64 = 7 * 24 * 3600 * 1000;
    let start_ms: i64 = now_ms - week_ms;
    let seconds: i64 = 7 * 24 * 3600; // 604800
    let period_secs: f64 = 2.0 * 3600.0; // 2 hours
    let two_pi: f64 = std::f64::consts::PI * 2.0;

    let mut recs: Vec<Record> = Vec::with_capacity(seconds as usize + 1);
    // Also compute positive/negative half-wave intervals while generating
    let mut cur_sign: i8 = 0; // 1 = positive, -1 = negative, 0 = unknown
    let mut run_start: Option<i64> = None;
    let mut pos_starts: Vec<i64> = Vec::new();
    let mut pos_ends: Vec<i64> = Vec::new();
    let mut pos_labels: Vec<String> = Vec::new();
    let mut neg_starts: Vec<i64> = Vec::new();
    let mut neg_ends: Vec<i64> = Vec::new();
    let mut neg_labels: Vec<String> = Vec::new();
    let mut pos_idx: i64 = 0; // will label WAVE{pos_idx*2}
    let mut neg_idx: i64 = 0; // will label WAVE{neg_idx*2 + 1}

    for i in 0..=seconds {
        let t = start_ms + i * 1000;
        let angle = two_pi * (i as f64) / period_secs;
        let val = angle.sin();
        let mut sensors = serde_json::Map::new();
        sensors.insert("value".into(), json!(val));
        recs.push(Record { _time: t, sensors });

        // Determine sign (treat exact zero as continuation of current run)
        let s = if val > 0.0 { 1 } else if val < 0.0 { -1 } else { 0 };
        let eff_sign = if s == 0 { cur_sign } else { s };
        if cur_sign == 0 {
            if eff_sign != 0 { cur_sign = eff_sign; run_start = Some(t); }
        } else if eff_sign != cur_sign && eff_sign != 0 {
            // Close previous run at previous timestamp (t - 1000)
            if let Some(rs) = run_start.take() {
                let end_t = t - 1000;
                if cur_sign > 0 {
                    pos_starts.push(rs);
                    pos_ends.push(end_t);
                    pos_labels.push(format!("WAVE{}", pos_idx * 2));
                    pos_idx += 1;
                } else {
                    neg_starts.push(rs);
                    neg_ends.push(end_t);
                    neg_labels.push(format!("WAVE{}", neg_idx * 2 + 1));
                    neg_idx += 1;
                }
            }
            // Start new run
            cur_sign = eff_sign;
            run_start = Some(t);
        }
    }
    // Close final run if open
    if cur_sign != 0 {
        if let Some(rs) = run_start.take() {
            let end_t = start_ms + seconds * 1000;
            if cur_sign > 0 {
                pos_starts.push(rs);
                pos_ends.push(end_t);
                pos_labels.push(format!("WAVE{}", pos_idx * 2));
            } else {
                neg_starts.push(rs);
                neg_ends.push(end_t);
                neg_labels.push(format!("WAVE{}", neg_idx * 2 + 1));
            }
        }
    }

    println!("Demo sine wave created, saving...");

    // Write to default database/schema/table: clarium/public/demo.time
    let demo = "clarium/public/demo.time";
    let guard = store.0.lock();
    guard.write_records(demo, &recs)?;

    println!("time table saved. Now saving events");


    // Create positive_events and negative_events regular tables
    let pos_table = "clarium/public/demo_positive_events";
    let neg_table = "clarium/public/demo_negative_events";
    // Ensure directories exist and schema.json initialized as regular
    guard.create_table(pos_table)?;
    guard.create_table(neg_table)?;

    // Build DataFrames for the intervals and persist
    if !pos_starts.is_empty() {
        let s_start = Series::new("_start_date".into(), pos_starts);
        let s_end = Series::new("_end_date".into(), pos_ends);
        let s_label = Series::new("label".into(), pos_labels);
        let df = DataFrame::new(vec![s_start.into(), s_end.into(), s_label.into()])?;
        guard.rewrite_table_df(pos_table, df)?;
    } else {
        // still initialize empty schema with locked columns
        guard.schema_add(pos_table, &vec![
            ("_start_date".to_string(), polars::prelude::DataType::Int64),
            ("_end_date".to_string(), polars::prelude::DataType::Int64),
            ("label".to_string(), polars::prelude::DataType::String),
        ])?;
    }
    if !neg_starts.is_empty() {
        let s_start = Series::new("_start_date".into(), neg_starts);
        let s_end = Series::new("_end_date".into(), neg_ends);
        let s_label = Series::new("label".into(), neg_labels);
        let df = DataFrame::new(vec![s_start.into(), s_end.into(), s_label.into()])?;
        guard.rewrite_table_df(neg_table, df)?;
    } else {
        guard.schema_add(neg_table, &vec![
            ("_start_date".to_string(), polars::prelude::DataType::Int64),
            ("_end_date".to_string(), polars::prelude::DataType::Int64),
            ("label".to_string(), polars::prelude::DataType::String),
        ])?;
    }

    println!("Created clarium/public/demo.time");
    println!("Created clarium/public/demo_positive_events");
    println!("Created clarium/public/demo_negative_events");

    Ok(())
}

// Backward-compatible entry that uses defaults
/// Convenience entry point using default ports (7878 HTTP, 5433 pgwire) and db root "dbs".
pub async fn run() -> anyhow::Result<()> {
    run_with_ports(7878, Some(5433), "dbs").await
}

#[derive(Debug, Deserialize)]
struct WritePayload {
    records: Vec<Record>,
}

#[derive(Debug, Deserialize)]
struct LoginPayload { username: String, password: String }

#[derive(Debug, Deserialize)]
struct UsePayload { name: String }

fn parse_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    let cookie = headers.get("cookie").or_else(|| headers.get("Cookie"))?;
    let s = cookie.to_str().ok()?;
    for part in s.split(';') {
        let p = part.trim();
        if let Some(eq) = p.find('=') {
            let (k, v) = p.split_at(eq);
            if k == name { return Some(v[1..].to_string()); }
        }
    }
    None
}

async fn get_username_from_headers(state: &AppState, headers: &HeaderMap) -> Option<String> {
    let sid = parse_cookie(headers, SESSION_COOKIE)?;
    // Enforce session timeouts
    {
        let mut meta_map = state.session_meta.write().await;
        if let Some(meta) = meta_map.get_mut(&sid) {
            let now = Instant::now();
            if now.duration_since(meta.issued_at) > session_absolute_lifetime()
                || now.duration_since(meta.last_seen) > session_idle_timeout()
            {
                // expire session: remove from all maps
                drop(meta_map);
                let mut s = state.sessions.write().await; s.remove(&sid);
                let mut c = state.csrf_tokens.write().await; c.remove(&sid);
                let mut d = state.session_defaults.write().await; d.remove(&sid);
                let mut m = state.session_meta.write().await; m.remove(&sid);
                return None;
            } else {
                meta.last_seen = now;
            }
        } else {
            // No metadata -> treat as invalid
            let mut s = state.sessions.write().await; s.remove(&sid);
            let mut c = state.csrf_tokens.write().await; c.remove(&sid);
            let mut d = state.session_defaults.write().await; d.remove(&sid);
            let mut m = state.session_meta.write().await; m.remove(&sid);
            return None;
        }
    }
    let map = state.sessions.read().await;
    map.get(&sid).cloned()
}

fn get_sid_from_headers(headers: &HeaderMap) -> Option<String> {
    parse_cookie(headers, SESSION_COOKIE)
}

async fn validate_csrf(state: &AppState, headers: &HeaderMap) -> bool {
    let Some(sid) = get_sid_from_headers(headers) else { return false; };
    let Some(provided) = headers.get("x-csrf-token").and_then(|v| v.to_str().ok()).map(|s| s.to_string()) else { return false; };
    let cmap = state.csrf_tokens.read().await;
    match cmap.get(&sid) {
        Some(expected) => expected == &provided,
        None => false,
    }
}

fn set_session_cookie(sid: &str) -> HeaderValue {
    // Secure, HttpOnly cookie scoped to path / with SameSite=Strict
    HeaderValue::from_str(&format!("{}={}; HttpOnly; Secure; SameSite=Strict; Path=/", SESSION_COOKIE, sid)).unwrap()
}

fn clear_session_cookie() -> HeaderValue {
    HeaderValue::from_str(&format!("{}=deleted; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; Secure; SameSite=Strict; Path=/", SESSION_COOKIE)).unwrap()
}

async fn login(State(state): State<AppState>, Json(payload): Json<LoginPayload>) -> impl IntoResponse {
    // Basic rate limiting
    let client_ip = extract_client_ip(None);
    if !check_login_rate_limit(&state, &client_ip, &payload.username).await {
        return (StatusCode::TOO_MANY_REQUESTS, HeaderMap::new(), Json(serde_json::json!({"status":"rate_limited"})));
    }
    // Use unified SQL-backed login used by pgwire as well
    use std::fmt::Write as _;
    let lr = crate::identity::LoginRequest {
        username: payload.username.clone(),
        password: payload.password.clone(),
        db: None,
        ip: Some(client_ip.clone()),
    };
    match crate::identity::login_via_sql(&state.store, &crate::identity::SessionManager::default(), &lr).await {
        Ok(resp) => {
            let sid = resp.session.session_id.clone();
            // generate CSRF token
            let mut csrf_bytes = [0u8; 32];
            let _ = getrandom(&mut csrf_bytes);
            let mut csrf = String::with_capacity(64);
            for b in &csrf_bytes { let _ = write!(&mut csrf, "{:02x}", b); }
            {
                let mut map = state.sessions.write().await;
                map.insert(sid.clone(), payload.username.clone());
            }
            {
                let mut cmap = state.csrf_tokens.write().await;
                cmap.insert(sid.clone(), csrf.clone());
            }
            // initialize session defaults (database, schema)
            {
                let mut dmap = state.session_defaults.write().await;
                dmap.insert(sid.clone(), (env_default_db(), env_default_schema()));
            }
            // initialize session meta
            {
                let mut mmap = state.session_meta.write().await;
                let now = Instant::now();
                mmap.insert(sid.clone(), SessionMeta { issued_at: now, last_seen: now });
            }
            // record success and reset rate limiter
            record_login_success(&state, &client_ip, &payload.username).await;
            let mut headers = HeaderMap::new();
            headers.insert("Set-Cookie", set_session_cookie(&sid));
            (StatusCode::OK, headers, Json(serde_json::json!({"status":"ok"})))
        }
        Err(_e) => {
            // invalid credentials or lookup failure
            record_login_failure(&state, &client_ip, &payload.username).await;
            (StatusCode::UNAUTHORIZED, HeaderMap::new(), Json(serde_json::json!({"status":"unauthorized"})))
        }
    }
}

async fn logout(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    // Require CSRF token
    if !validate_csrf(&state, &headers).await {
        return (StatusCode::FORBIDDEN, HeaderMap::new(), Json(serde_json::json!({"status":"forbidden","error":"invalid csrf"})));
    }
    if let Some(sid) = parse_cookie(&headers, SESSION_COOKIE) {
        let mut map = state.sessions.write().await;
        map.remove(&sid);
        // also remove csrf token
        let mut cmap = state.csrf_tokens.write().await;
        cmap.remove(&sid);
        // and defaults + meta
        let mut dmap = state.session_defaults.write().await; dmap.remove(&sid);
        let mut mmap = state.session_meta.write().await; mmap.remove(&sid);
    }
    let mut h = HeaderMap::new();
    h.insert("Set-Cookie", clear_session_cookie());
    (StatusCode::OK, h, Json(serde_json::json!({"status":"ok"})))
}

async fn write(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(database): Path<String>,
    Json(payload): Json<WritePayload>,
) -> impl IntoResponse {
    let Some(username) = get_username_from_headers(&state, &headers).await else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"status":"unauthorized"})));
    };
    if !validate_csrf(&state, &headers).await {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"status":"forbidden","error":"invalid csrf"})));
    }
    // authorize insert (enhanced RBAC in debug builds; legacy parquet authorizer in release)
    let allowed = crate::identity::check_command_allowed_async(&state.store, &username, security::CommandKind::Insert, Some(&database)).await;
    if !allowed {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"status":"forbidden"})));
    }
    let guard = state.store.0.lock();
    match guard.write_records(&database, &payload.records) {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"status":"ok","written": payload.records.len()}))),
        Err(e) => {
            error!("write failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"status":"error","error": e.to_string()})))
        }
    }
}

#[derive(Debug, Deserialize)]
struct QueryPayload { query: String }

async fn get_csrf(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    // Must be logged in to fetch CSRF token
    let Some(_username) = get_username_from_headers(&state, &headers).await else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"status":"unauthorized"})));
    };
    let Some(sid) = get_sid_from_headers(&headers) else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"status":"unauthorized"})));
    };
    let cmap = state.csrf_tokens.read().await;
    if let Some(token) = cmap.get(&sid) {
        return (StatusCode::OK, Json(serde_json::json!({"status":"ok","csrf": token})));
    }
    (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"status":"error","error":"csrf not available"})))
}

fn to_ck_and_db(cmd: &query::Command) -> (security::CommandKind, Option<String>) {
    match cmd {
        query::Command::Select(q) => (security::CommandKind::Select, q.base_table.as_ref().and_then(|t| t.table_name().map(|s| s.to_string()))),
        query::Command::Calculate { query: q, .. } => (security::CommandKind::Calculate, q.base_table.as_ref().and_then(|t| t.table_name().map(|s| s.to_string()))),
        query::Command::Update { table, .. } => {
            let db_name = if table.contains('/') { table.split('/').next().map(|s| s.to_string()) } else { None };
            (security::CommandKind::Other, db_name)
        }
        // Views
        query::Command::CreateView { .. } | query::Command::DropView { .. } | query::Command::ShowView { .. } => (security::CommandKind::Database, None),
        query::Command::DeleteRows { database, .. } => (security::CommandKind::DeleteRows, Some(database.clone())),
        query::Command::DeleteColumns { database, .. } => (security::CommandKind::DeleteColumns, Some(database.clone())),
        query::Command::SchemaShow { database } => (security::CommandKind::Schema, Some(database.clone())),
        query::Command::SchemaAdd { database, .. } => (security::CommandKind::Schema, Some(database.clone())),
        // Legacy
        query::Command::DatabaseAdd { .. } => (security::CommandKind::Database, None),
        query::Command::DatabaseDelete { .. } => (security::CommandKind::Database, None),
        // New DDL
        query::Command::CreateDatabase { .. } | query::Command::DropDatabase { .. } | query::Command::RenameDatabase { .. } => (security::CommandKind::Database, None),
        query::Command::CreateSchema { .. } | query::Command::DropSchema { .. } | query::Command::RenameSchema { .. } => (security::CommandKind::Schema, None),
        query::Command::CreateTimeTable { .. } | query::Command::DropTimeTable { .. } | query::Command::RenameTimeTable { .. } => (security::CommandKind::Database, None),
        query::Command::CreateTable { .. } | query::Command::DropTable { .. } | query::Command::RenameTable { .. } => (security::CommandKind::Database, None),
        query::Command::AlterTable { table, .. } => {
            let db_name = if table.contains('/') { table.split('/').next().map(|s| s.to_string()) } else { None };
            (security::CommandKind::Database, db_name)
        }
        query::Command::UserAdd { .. } | query::Command::UserDelete { .. } | query::Command::UserAlter { .. } => (security::CommandKind::Other, None),
        query::Command::CreateScript { .. } | query::Command::DropScript { .. } | query::Command::RenameScript { .. } | query::Command::LoadScript { .. } => (security::CommandKind::Other, None),
        // KV store/key commands
        query::Command::CreateStore { database, .. } => (security::CommandKind::Database, Some(database.clone())),
        query::Command::DropStore { database, .. } => (security::CommandKind::Database, Some(database.clone())),
        query::Command::RenameStore { database, .. } => (security::CommandKind::Database, Some(database.clone())),
        query::Command::WriteKey { database, .. } => (security::CommandKind::Other, Some(database.clone())),
        query::Command::ReadKey { database, .. } => (security::CommandKind::Other, Some(database.clone())),
        query::Command::DropKey { database, .. } => (security::CommandKind::Other, Some(database.clone())),
        query::Command::RenameKey { database, .. } => (security::CommandKind::Other, Some(database.clone())),
        query::Command::ListStores { database, .. } => (security::CommandKind::Other, Some(database.clone())),
        query::Command::ListKeys { database, .. } => (security::CommandKind::Other, Some(database.clone())),
        query::Command::DescribeKey { database, .. } => (security::CommandKind::Other, Some(database.clone())),
        query::Command::DescribeObject { .. } => (security::CommandKind::Other, None),
        // Vector index catalog and lifecycle
        query::Command::CreateVectorIndex { .. }
        | query::Command::DropVectorIndex { .. }
        | query::Command::ShowVectorIndex { .. }
        | query::Command::ShowVectorIndexes
        | query::Command::BuildVectorIndex { .. }
        | query::Command::ReindexVectorIndex { .. }
        | query::Command::ShowVectorIndexStatus { .. }
        | query::Command::AlterVectorIndexSetMode { .. }
        => (security::CommandKind::Database, None),
        // Graph catalog and graph-related commands
        query::Command::CreateGraph { .. }
        | query::Command::DropGraph { .. }
        | query::Command::ShowGraph { .. }
        | query::Command::ShowGraphs
        | query::Command::ShowGraphStatus { .. }
        | query::Command::UseGraph { .. }
        | query::Command::UnsetGraph
        | query::Command::ShowCurrentGraph
        | query::Command::BeginGraphTxn { .. }
        | query::Command::CommitGraphTxn
        | query::Command::AbortGraphTxn
        | query::Command::InsertNodeTxn { .. }
        | query::Command::InsertEdgeTxn { .. }
        | query::Command::GcGraph { .. }
        | query::Command::MatchRewrite { .. } => (security::CommandKind::Other, None),
        // Global session-affecting and SHOW
        query::Command::UseDatabase { .. } | query::Command::UseSchema { .. } | query::Command::Set { .. } => (security::CommandKind::Other, None),
        query::Command::ShowTransactionIsolation
        | query::Command::ShowStandardConformingStrings
        | query::Command::ShowServerVersion
        | query::Command::ShowClientEncoding
        | query::Command::ShowServerEncoding
        | query::Command::ShowDateStyle
        | query::Command::ShowIntegerDateTimes
        | query::Command::ShowTimeZone
        | query::Command::ShowSearchPath
        | query::Command::ShowDefaultTransactionIsolation
        | query::Command::ShowTransactionReadOnly
        | query::Command::ShowApplicationName
        | query::Command::ShowExtraFloatDigits
        | query::Command::ShowAll
        | query::Command::ShowSchemas
        | query::Command::ShowTables
        | query::Command::ShowObjects
        | query::Command::ShowScripts => (security::CommandKind::Other, None),
        query::Command::ClearScriptCache { .. } => (security::CommandKind::Other, None),
        // FILESTORE commands: treat as Other with no specific database context here
        query::Command::ShowFilestores { .. }
        | query::Command::ShowFilestoreConfig { .. }
        | query::Command::ShowFilesInFilestore { .. }
        | query::Command::ShowTreesInFilestore { .. }
        | query::Command::ShowCommitsInFilestore { .. }
        | query::Command::ShowDiffInFilestore { .. }
        | query::Command::ShowChunksInFilestore { .. }
        | query::Command::ShowAliasesInFilestore { .. }
        | query::Command::ShowAdminInFilestore { .. }
        | query::Command::ShowHealthInFilestore { .. }
        | query::Command::CreateFilestoreCmd { .. }
        | query::Command::AlterFilestoreCmd { .. }
        | query::Command::DropFilestoreCmd { .. }
        | query::Command::IngestFileFromBytesCmd { .. }
        | query::Command::IngestFileFromHostPathCmd { .. }
        | query::Command::UpdateFileFromBytesCmd { .. }
        | query::Command::RenameFilePathCmd { .. }
        | query::Command::DeleteFilePathCmd { .. }
        | query::Command::CreateTreeCmd { .. }
        | query::Command::CommitTreeCmd { .. }
        => (security::CommandKind::Other, None),
        query::Command::Explain { .. } => (security::CommandKind::Other, None),
        query::Command::SelectUnion { .. } => (security::CommandKind::Select, None),
        query::Command::Slice(_) => (security::CommandKind::Select, None),
        query::Command::Insert { table, .. } => {
            // Extract database from table path (format: db/schema/table or just table)
            let db_name = if table.contains('/') {
                table.split('/').next().map(|s| s.to_string())
            } else {
                None
            };
            (security::CommandKind::Other, db_name)
        }
    }
}

async fn query_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<QueryPayload>,
) -> impl IntoResponse {
    let Some(username) = get_username_from_headers(&state, &headers).await else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"status":"unauthorized"}))).into_response();
    };
    if !validate_csrf(&state, &headers).await {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"status":"forbidden","error":"invalid csrf"}))).into_response();
    }
    // Transaction control statements: accept as no-ops for client compatibility
    if let Some(_tx) = detect_transaction_cmd(&payload.query) {
        return (StatusCode::OK, Json(serde_json::json!({"status":"ok","results": {"transaction":"ok"} }))).into_response();
    }
    // Parse and authorize
    let cmd = match query::parse(&payload.query) {
        Ok(c) => c,
        Err(e) => { return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"status":"error","error": e.to_string()}))).into_response(); }
    };
    let (ck, db_opt) = to_ck_and_db(&cmd);
    let allowed = crate::identity::check_command_allowed_async(&state.store, &username, ck, db_opt.as_deref()).await;
    if !allowed {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"status":"forbidden"}))).into_response();
    }
    // Determine per-session defaults
    let (cur_db, cur_schema) = {
        let sid_opt = get_sid_from_headers(&headers);
        if let Some(sid) = sid_opt {
            let dmap = state.session_defaults.read().await;
            if let Some((db, sc)) = dmap.get(&sid) { (db.clone(), sc.clone()) } else { (env_default_db(), env_default_schema()) }
        } else { (env_default_db(), env_default_schema()) }
    };
    let defaults = crate::ident::QueryDefaults { current_database: cur_db, current_schema: cur_schema };
    let exec_fut = async {
        crate::server::exec::execute_query_with_defaults(&state.store, &payload.query, &defaults).await
    };
    let exec_result = AssertUnwindSafe(exec_fut).catch_unwind().await;
    match exec_result {
        Ok(Ok(value)) => {
            // If this was a self privilege change, rotate session id for safety
            if let Ok(parsed_cmd) = query::parse(&payload.query) {
                if let query::Command::UserAlter { username: u, .. } = parsed_cmd {
                    if u == username {
                        if let Some(old_sid) = get_sid_from_headers(&headers) {
                            if let Some(new_cookie) = rotate_session_id(&state, &old_sid).await {
                                let mut h = HeaderMap::new();
                                h.insert("Set-Cookie", new_cookie);
                                let resp = (StatusCode::OK, h, Json(serde_json::json!({"status":"ok","results": value})) ).into_response();
                                return resp;
                            }
                        }
                    }
                }
            }
            return (StatusCode::OK, Json(serde_json::json!({"status":"ok","results": value})) ).into_response();
        }
        Ok(Err(e)) => {
            // Prefer AppError mapping when available
            if let Some(app) = e.downcast_ref::<crate::error::AppError>() {
                let status = app.http_status();
                return (StatusCode::from_u16(status).unwrap_or(StatusCode::UNPROCESSABLE_ENTITY), Json(serde_json::json!({
                    "status":"error",
                    "code": app.code_str(),
                    "message": app.message()
                }))).into_response();
            }
            // Treat other execution failures as semantic errors (unprocessable)
            error!("query failed: {e}");
            return (StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::json!({"status":"error","code":"exec_error","message": e.to_string()}))).into_response();
        }
        Err(panic_payload) => {
            // Convert panics to a 500 error response without crashing the server task
            let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() { *s }
                      else if let Some(s) = panic_payload.downcast_ref::<String>() { s.as_str() }
                      else { "panic" };
            error!(target: "panic", "HTTP query_handler panic: {}", msg);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "status":"error",
                "code":"internal_panic",
                "message":"internal server error"
            }))).into_response();
        }
    }
}

async fn ws_handler(State(state): State<AppState>, headers: HeaderMap, ws: WebSocketUpgrade) -> impl IntoResponse {
    // Require login
    let Some(username) = get_username_from_headers(&state, &headers).await else {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };
    // Require CSRF token header too
    if !validate_csrf(&state, &headers).await {
        return (StatusCode::FORBIDDEN, "forbidden: invalid csrf").into_response();
    }
    ws.on_upgrade(move |mut socket| {
        let state = state.clone();
        let username = username.clone();
        async move {
            use futures_util::StreamExt;
            while let Some(Ok(msg)) = socket.next().await {
                match msg {
                    Message::Text(text) => {
                        // Transaction control statements over WS: accept as no-ops
                        if detect_transaction_cmd(&text).is_some() {
                            let _ = socket.send(Message::Text(serde_json::json!({"status":"ok","results": {"transaction":"ok"}}).to_string().into())).await;
                            continue;
                        }
                        // authorize per message using unified async RBAC gate
                        let auth_ok = if let Ok(cmd) = query::parse(&text) {
                            let (ck, db_opt) = to_ck_and_db(&cmd);
                            crate::identity::check_command_allowed_async(&state.store, &username, ck, db_opt.as_deref()).await
                        } else { false };
                        if !auth_ok {
                            let _ = socket.send(Message::Text(serde_json::json!({"status":"forbidden","error":"forbidden"}).to_string().into())).await;
                            continue;
                        }
                        // Per-session defaults
                        let (cur_db, cur_schema) = {
                            let sid_opt = get_sid_from_headers(&headers);
                            if let Some(sid) = sid_opt {
                                let dmap = state.session_defaults.read().await;
                                if let Some((db, sc)) = dmap.get(&sid) { (db.clone(), sc.clone()) } else { (env_default_db(), env_default_schema()) }
                            } else { (env_default_db(), env_default_schema()) }
                        };
                        let defaults = crate::ident::QueryDefaults { current_database: cur_db, current_schema: cur_schema };
                        let fut = async {
                            crate::server::exec::execute_query_with_defaults(&state.store, &text, &defaults).await
                        };
                        match AssertUnwindSafe(fut).catch_unwind().await {
                            Ok(Ok(val)) => {
                                let _ = socket.send(Message::Text(serde_json::json!({"status":"ok","results": val}).to_string().into())).await;
                            }
                            Ok(Err(e)) => {
                                // Keep socket alive; prefer AppError mapping when available
                                if let Some(app) = e.downcast_ref::<crate::error::AppError>() {
                                    let _ = socket.send(Message::Text(serde_json::json!({
                                        "status":"error",
                                        "code": app.code_str(),
                                        "message": app.message()
                                    }).to_string().into())).await;
                                } else {
                                    let _ = socket.send(Message::Text(serde_json::json!({"status":"error","code":"exec_error","message": e.to_string()}).to_string().into())).await;
                                }
                            }
                            Err(panic_payload) => {
                                let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() { *s }
                                          else if let Some(s) = panic_payload.downcast_ref::<String>() { s.as_str() }
                                          else { "panic" };
                                error!(target: "panic", "WS handler panic: {}", msg);
                                let _ = socket.send(Message::Text(serde_json::json!({
                                    "status":"error",
                                    "code":"internal_panic",
                                    "message":"internal server error"
                                }).to_string().into())).await;
                                // Decide to keep the socket open; if subsequent operations fail due to state, client may close.
                            }
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        }
    })
}

// --- Helpers: session rotation and login rate limiting ---

async fn rotate_session_id(state: &AppState, old_sid: &str) -> Option<HeaderValue> {
    // Generate new SID and CSRF
    let mut bytes = [0u8; 16];
    let _ = getrandom(&mut bytes);
    let mut new_sid = String::with_capacity(32);
    use std::fmt::Write as _;
    for b in &bytes { let _ = write!(&mut new_sid, "{:02x}", b); }
    let mut csrf_bytes = [0u8; 32];
    let _ = getrandom(&mut csrf_bytes);
    let mut new_csrf = String::with_capacity(64);
    for b in &csrf_bytes { let _ = write!(&mut new_csrf, "{:02x}", b); }

    let mut sessions = state.sessions.write().await;
    if let Some(username) = sessions.remove(old_sid) {
        sessions.insert(new_sid.clone(), username);
        drop(sessions);
        // Move defaults
        let mut d = state.session_defaults.write().await;
        if let Some(val) = d.remove(old_sid) { d.insert(new_sid.clone(), val); }
        drop(d);
        // Replace CSRF
        let mut c = state.csrf_tokens.write().await;
        c.remove(old_sid);
        c.insert(new_sid.clone(), new_csrf);
        drop(c);
        // Update meta
        let mut m = state.session_meta.write().await;
        let now = Instant::now();
        m.insert(new_sid.clone(), SessionMeta { issued_at: now, last_seen: now });
        m.remove(old_sid);
        drop(m);
        Some(set_session_cookie(&new_sid))
    } else {
        None
    }
}

fn extract_client_ip(headers: Option<&HeaderMap>) -> String {
    if let Some(h) = headers {
        if let Some(v) = h.get("x-forwarded-for").or_else(|| h.get("X-Forwarded-For")) {
            if let Ok(s) = v.to_str() { return s.split(',').next().unwrap_or("").trim().to_string(); }
        }
        if let Some(v) = h.get("x-real-ip").or_else(|| h.get("X-Real-IP")) {
            if let Ok(s) = v.to_str() { return s.to_string(); }
        }
    }
    // Fallback: unknown
    "unknown".to_string()
}

async fn check_login_rate_limit(state: &AppState, ip: &str, username: &str) -> bool {
    let mut map = state.login_attempts.write().await;
    let key = (ip.to_string(), username.to_string());
    let now = Instant::now();
    let entry = map.entry(key).or_insert(AttemptState { window_start: now, count_in_window: 0, failures: 0, backoff_until: None });
    // Backoff enforcement
    if let Some(until) = entry.backoff_until {
        if now < until { return false; }
        entry.backoff_until = None;
    }
    // Sliding window of 60 seconds
    if now.duration_since(entry.window_start) > Duration::from_secs(60) {
        entry.window_start = now;
        entry.count_in_window = 0;
    }
    entry.count_in_window += 1;
    // Hard cap attempts per minute
    if entry.count_in_window > 20 {
        return false;
    }
    true
}

async fn record_login_failure(state: &AppState, ip: &str, username: &str) {
    let mut map = state.login_attempts.write().await;
    let key = (ip.to_string(), username.to_string());
    let now = Instant::now();
    let entry = map.entry(key).or_insert(AttemptState { window_start: now, count_in_window: 0, failures: 0, backoff_until: None });
    entry.failures = entry.failures.saturating_add(1);
    let backoff_secs = 2u64.saturating_pow(entry.failures.min(8));
    entry.backoff_until = Some(now + Duration::from_secs(backoff_secs));
}

async fn record_login_success(state: &AppState, ip: &str, username: &str) {
    let mut map = state.login_attempts.write().await;
    let key = (ip.to_string(), username.to_string());
    map.remove(&key);
}


#[derive(Debug, Serialize)]
struct UseResult { status: &'static str }

async fn use_database(State(state): State<AppState>, headers: HeaderMap, Json(payload): Json<UsePayload>) -> impl IntoResponse {
    let Some(_username) = get_username_from_headers(&state, &headers).await else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"status":"unauthorized"})));
    };
    if !validate_csrf(&state, &headers).await {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"status":"forbidden","error":"invalid csrf"})));
    }
    let Some(sid) = get_sid_from_headers(&headers) else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"status":"unauthorized"})));
    };
    let mut dmap = state.session_defaults.write().await;
    let entry = dmap.entry(sid).or_insert(("clarium".to_string(), "user".to_string()));
    if !payload.name.trim().is_empty() { entry.0 = payload.name.trim().to_string(); }
    (StatusCode::OK, Json(serde_json::json!(UseResult{ status: "ok" })))
}

async fn use_schema(State(state): State<AppState>, headers: HeaderMap, Json(payload): Json<UsePayload>) -> impl IntoResponse {
    let Some(_username) = get_username_from_headers(&state, &headers).await else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"status":"unauthorized"})));
    };
    if !validate_csrf(&state, &headers).await {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"status":"forbidden","error":"invalid csrf"})));
    }
    let Some(sid) = get_sid_from_headers(&headers) else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"status":"unauthorized"})));
    };
    let mut dmap = state.session_defaults.write().await;
    let entry = dmap.entry(sid).or_insert(("clarium".to_string(), "user".to_string()));
    if !payload.name.trim().is_empty() { entry.1 = payload.name.trim().to_string(); }
    (StatusCode::OK, Json(serde_json::json!(UseResult{ status: "ok" })))
}

// Submodules implementing parts of exec
// NOTE: This module is intentionally kept thin. Add new logic in exec_*.rs files.
pub mod exec_select;
pub mod exec_show;
pub mod select_stages;
pub mod exec_slice;
pub mod exec_common;
pub mod where_subquery;
pub mod exec_helpers; // shared helpers (dataframe conversions, select df)
pub mod exec_create;  // regular table DDL and CREATE TABLE parser
pub mod exec_insert;  // INSERT INTO handling
pub mod df_utils;     // dataframe helpers (read_df_or_kv, etc.)
pub mod exec_calculate; // CALCULATE handling
pub mod exec_keys;      // KV key operations
pub mod exec_update;    // UPDATE handling
pub mod exec_delete;    // DELETE COLUMNS handling
pub mod exec_scripts;   // SCRIPT management (create/drop/rename/load)
pub mod exec_views;     // VIEW management (create/drop/show)
pub mod exec_describe;  // DESCRIBE <object> (tables/views)
pub mod exec_vector_index; // VECTOR INDEX management
pub mod exec_graph;        // GRAPH catalog management
pub mod exec_graph_runtime; // Graph TVFs runtime (neighbors/paths)

use anyhow::Result;
use polars::prelude::*;
use crate::storage::{SharedStore, KvValue};
use crate::ident::QueryDefaults;
use crate::scripts::get_script_registry;
use tracing::debug;
// Bring frequently used helpers from submodules into scope
use crate::server::exec::exec_select::run_select;
use crate::server::exec::exec_slice::run_slice;
use crate::server::exec::where_subquery::{eval_where_mask, where_contains_subquery};
use crate::server::exec::exec_common::build_where_expr;
use std::ops::Not;
use crate::server::exec::exec_helpers::dataframe_to_json;
use crate::server::exec::df_utils::read_df_or_kv;
// Re-export common helpers so external callers can keep using crate::server::exec::*
pub use crate::server::exec::exec_helpers::{execute_select_df, dataframe_to_tabular, normalize_query_with_defaults};
pub use crate::server::exec::exec_create::do_create_table;

use crate::server::query::query_common::*;
use crate::server::query::*;

/// Returns true if the provided SQL text is a transaction control statement
/// that we treat as a no-op for compatibility (BEGIN/START TRANSACTION/COMMIT/END/ROLLBACK).
fn is_transaction_control(text: &str) -> bool {
    let s = text.trim();
    if s.is_empty() { return false; }
    let s = s.strip_suffix(';').unwrap_or(s).trim();
    let up = s.to_ascii_uppercase();
    matches!(
        up.as_str(),
        "BEGIN" | "START TRANSACTION" | "COMMIT" | "END" | "ROLLBACK"
    )
}

pub async fn execute_query(store: &SharedStore, text: &str) -> Result<serde_json::Value> {
    // Accept transaction control statements as no-ops globally so all frontends
    // (HTTP/WS/pgwire) behave consistently even without real transactional storage.
    if is_transaction_control(text) {
        return Ok(serde_json::json!({"status":"ok"}));
    }
    let cmd = parse(text)?;
    match cmd {
        Command::Slice(plan) => {
            // Create DataContext with registry snapshot for SLICE query
            let registry_snapshot = crate::scripts::get_script_registry()
                .and_then(|r| r.snapshot().ok());
            let mut ctx = crate::server::data_context::DataContext::with_defaults("clarium", "public");
            if let Some(reg) = registry_snapshot {
                ctx.script_registry = Some(reg);
            }
            let df = run_slice(store, &plan, &ctx)?;
            Ok(dataframe_to_json(&df))
        }
        // SHOW commands (global)
        Command::ShowTransactionIsolation
        | Command::ShowStandardConformingStrings
        | Command::ShowServerVersion
        | Command::ShowClientEncoding
        | Command::ShowServerEncoding
        | Command::ShowDateStyle
        | Command::ShowIntegerDateTimes
        | Command::ShowTimeZone
        | Command::ShowSearchPath
        | Command::ShowDefaultTransactionIsolation
        | Command::ShowTransactionReadOnly
        | Command::ShowApplicationName
        | Command::ShowExtraFloatDigits
        | Command::ShowAll
        | Command::ShowSchemas
        | Command::ShowTables
        | Command::ShowObjects
        | Command::ShowScripts
        | Command::ShowGraphStatus { .. } => {
            self::exec_show::execute_show(store, cmd).await
        }
        // DESCRIBE <object>
        Command::DescribeObject { name } => {
            self::exec_describe::execute_describe(store, &name)
        }
        // Vector index catalog
        Command::CreateVectorIndex { .. }
        | Command::DropVectorIndex { .. }
        | Command::ShowVectorIndex { .. }
        | Command::ShowVectorIndexes => {
            self::exec_vector_index::execute_vector_index(store, cmd)
        }
        // Graph catalogs
        Command::CreateGraph { .. }
        | Command::DropGraph { .. }
        | Command::ShowGraph { .. }
        | Command::ShowGraphs => {
            self::exec_graph::execute_graph(store, cmd)
        }
        // Graph GC command
        Command::GcGraph { name } => {
            // Determine target: explicit name > session default > all graphs
            if let Some(g) = name {
                // Per-graph GC
                let mut handle = crate::server::graphstore::GraphHandle::open(store, &g)?;
                let did = handle.run_gc_if_needed()?;
                Ok(serde_json::json!({"status":"ok","graph": g, "compacted": did}))
            } else if let Some(gdef) = crate::system::get_current_graph_opt() {
                let mut handle = crate::server::graphstore::GraphHandle::open(store, &gdef)?;
                let did = handle.run_gc_if_needed()?;
                Ok(serde_json::json!({"status":"ok","graph": gdef, "compacted": did}))
            } else {
                crate::server::graphstore::gc_scan_all_graphs(store);
                Ok(serde_json::json!({"status":"ok","scope":"all"}))
            }
        }
        // MATCH rewrite execution
        Command::MatchRewrite { sql } => {
            // Replace session default placeholder if present
            let mut sql2 = sql.clone();
            if sql2.contains("__SESSION_DEFAULT__") {
                let g = crate::system::get_current_graph_opt()
                    .ok_or_else(|| anyhow::anyhow!("MATCH: no graph specified and no session default set; use USING GRAPH or USE GRAPH."))?;
                let quoted = if g.starts_with('\'') && g.ends_with('\'') { g } else { format!("'{}'", g.replace('\'', "''")) };
                sql2 = sql2.replace("__SESSION_DEFAULT__", &quoted);
            }
            // Parse the rewritten SELECT and execute
            match crate::server::query::parse(&sql2)? {
                crate::server::query::Command::Select(q) => {
                    let df = run_select(store, &q)?;
                    Ok(dataframe_to_json(&df))
                }
                crate::server::query::Command::SelectUnion { queries, all } => {
                    let out = crate::server::exec::exec_select::handle_select_union(store, &queries, all)?;
                    Ok(dataframe_to_json(&out))
                }
                other => {
                    anyhow::bail!(format!("MATCH rewrite did not produce a SELECT: {:?}", other));
                }
            }
        }
        // USE and SET commands affect only session defaults; update thread-local defaults
        Command::UseDatabase { name } => {
            if name.eq_ignore_ascii_case("none") {
                crate::system::unset_current_database();
            } else {
                crate::system::set_current_database(&name);
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::UseGraph { name } => {
            if name.eq_ignore_ascii_case("none") {
                crate::system::unset_current_graph();
            } else {
                crate::system::set_current_graph(&name);
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        // GraphStore transactional DDL
        Command::BeginGraphTxn { graph } => {
            use crate::server::graphstore::GraphHandle;
            use crate::server::graphstore::txn::GraphTxn;
            // Resolve graph name: explicit > session default
            let gname = if let Some(g) = graph { g } else {
                crate::system::get_current_graph_opt().ok_or_else(|| anyhow::anyhow!("BEGIN: no graph specified and no session default; use BEGIN GRAPH <db/schema/name> or USE GRAPH"))?
            };
            // Open handle and seed txn/ctx
            let handle = GraphHandle::open(store, &gname)?;
            // Prevent nested txn
            if crate::system::peek_graph_txn_active() {
                anyhow::bail!("a graph transaction is already active; COMMIT or ABORT before BEGIN");
            }
            let mut tx = GraphTxn::begin(&handle.root, 0)?;
            let seed = handle.manifest.partitioning.as_ref().and_then(|p| p.hash_seed).unwrap_or(0);
            let ctx = crate::system::GraphTxnCtx { graph: gname.clone(), root: handle.root.clone(), partitions: handle.manifest.partitions, hash_seed: seed };
            crate::system::set_graph_txn(tx, ctx);
            Ok(serde_json::json!({"status":"ok","graph": gname}))
        }
        Command::CommitGraphTxn => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let ctx = crate::system::get_graph_txn_ctx().ok_or_else(|| anyhow::anyhow!("no active graph transaction"))?;
            if let Some(tx) = crate::system::take_graph_txn() {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64;
                tx.commit(now)?;
                crate::system::clear_graph_txn();
                Ok(serde_json::json!({"status":"ok","graph": ctx.graph}))
            } else {
                anyhow::bail!("no active graph transaction");
            }
        }
        Command::AbortGraphTxn => {
            if let Some(tx) = crate::system::take_graph_txn() {
                let _ = tx.abort();
                crate::system::clear_graph_txn();
                Ok(serde_json::json!({"status":"ok"}))
            } else {
                anyhow::bail!("no active graph transaction");
            }
        }
        Command::InsertNodeTxn { graph, label, key, node_id } => {
            // Ensure active txn and same graph if provided
            let ctx = crate::system::get_graph_txn_ctx().ok_or_else(|| anyhow::anyhow!("no active graph transaction; issue BEGIN first"))?;
            if let Some(g) = &graph { if *g != ctx.graph { anyhow::bail!(format!("INSERT NODE GRAPH '{}' does not match active transaction graph '{}'", g, ctx.graph)); } }
            let mut tx = crate::system::take_graph_txn().ok_or_else(|| anyhow::anyhow!("no active graph transaction; issue BEGIN first"))?;
            tx.insert_node(&label, &key, node_id);
            // place back
            crate::system::set_graph_txn(tx, ctx);
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::InsertEdgeTxn { graph, src, dst, etype_id, part } => {
            let ctx = crate::system::get_graph_txn_ctx().ok_or_else(|| anyhow::anyhow!("no active graph transaction; issue BEGIN first"))?;
            if let Some(g) = &graph { if *g != ctx.graph { anyhow::bail!(format!("INSERT EDGE GRAPH '{}' does not match active transaction graph '{}'", g, ctx.graph)); } }
            let mut tx = crate::system::take_graph_txn().ok_or_else(|| anyhow::anyhow!("no active graph transaction; issue BEGIN first"))?;
            let part_id: u32 = if let Some(p) = part { p } else {
                // default routing: hash_mod by src with seed
                let v = (src ^ ctx.hash_seed) % (ctx.partitions as u64);
                v as u32
            };
            let et = etype_id.unwrap_or(0);
            tx.insert_edge(part_id, src, dst, et);
            crate::system::set_graph_txn(tx, ctx);
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::UnsetGraph => {
            crate::system::unset_current_graph();
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::ShowCurrentGraph => {
            let g = crate::system::get_current_graph_opt();
            Ok(serde_json::json!([{ "graph": g.unwrap_or_default() }]))
        }
        Command::UseSchema { name } => {
            if name.eq_ignore_ascii_case("none") {
                crate::system::unset_current_schema();
            } else {
                crate::system::set_current_schema(&name);
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::Set { variable, value } => {
            // Apply known vector/search settings; ignore unknowns for forward compatibility
            let mut applied = false;
            if crate::system::apply_vector_setting(&variable, &value) { applied = true; }
            // Allow toggling strict projection via SET strict.projection = on|off
            let vlow = variable.to_ascii_lowercase();
            if vlow == "strict.projection" || vlow == "projection.strict" {
                let on = matches!(value.to_ascii_lowercase().as_str(), "on" | "true" | "1");
                crate::system::set_strict_projection(on);
                applied = true;
            }
            let status = if applied { "ok" } else { "ignored" };
            Ok(serde_json::json!({"status": status}))
        }
        Command::Insert { table, columns, values } => {
            crate::server::exec::exec_insert::handle_insert(store, table, columns, values)
        }
        // Script management
        Command::CreateScript { .. }
        | Command::DropScript { .. }
        | Command::RenameScript { .. }
        | Command::LoadScript { .. } => {
            self::exec_scripts::execute_scripts(store, cmd)
        }
        // View management
        Command::CreateView { .. }
        | Command::DropView { .. }
        | Command::ShowView { .. } => {
            self::exec_views::execute_views(store, cmd)
        }
        Command::Select(q) => {
            let (df, into) = crate::server::exec::exec_select::handle_select(store, &q)?;
            if let Some((dest, mode)) = into {
                let dest = dest.trim();
                let guard = store.0.lock();
                guard.create_table(dest).ok();
                if dest.ends_with(".time") {
                    // Expect exactly one _time column and ensure uniqueness
                    let time_cols = df.get_column_names().into_iter().filter(|n| n.as_str() == "_time").count();
                    if time_cols != 1 { anyhow::bail!("INTO time table requires exactly one _time column in the projection"); }
                    let time_col = df.column("_time").ok();
                    let time = time_col.and_then(|c| c.i64().ok()).ok_or_else(|| anyhow::anyhow!("_time not in result for INTO time table"))?;
                    let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::with_capacity(df.height());
                    for i in 0..df.height() {
                        let tval = time.get(i).ok_or_else(|| anyhow::anyhow!("null _time value not allowed for INTO time table"))?;
                        if !seen.insert(tval) { anyhow::bail!("_time must be unique for INTO time table"); }
                    }
                    let mut records: Vec<crate::storage::Record> = Vec::with_capacity(df.height());
                    let names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
                    for i in 0..df.height() {
                        let t = time.get(i).ok_or_else(|| anyhow::anyhow!("bad time index"))?;
                        let mut map = serde_json::Map::new();
                        for name in &names {
                            if name.as_str() == "_time" { continue; }
                            let s = df.column(name.as_str())?;
                            let v = s.get(i);
                            let jv = match v {
                                Ok(polars::prelude::AnyValue::Null) => None,
                                Ok(polars::prelude::AnyValue::Int64(v)) => Some(serde_json::json!(v)),
                                Ok(polars::prelude::AnyValue::Float64(v)) => Some(serde_json::json!(v)),
                                Ok(polars::prelude::AnyValue::String(s)) => Some(serde_json::json!(s)),
                                Ok(polars::prelude::AnyValue::StringOwned(s)) => Some(serde_json::json!(s)),
                                Ok(_) => None,
                                Err(_) => None,
                            };
                            if let Some(val) = jv { map.insert(name.clone(), val); }
                        }
                        records.push(crate::storage::Record { _time: t, sensors: map });
                    }
                    guard.write_records(dest, &records)?;
                } else {
                    match mode {
                        IntoMode::Replace => { guard.rewrite_table_df(dest, df.clone())?; }
                        IntoMode::Append => {
                            let combined = match guard.read_df(dest) { Ok(existing) => { existing.vstack(&df)? } Err(_) => df.clone(), };
                            guard.rewrite_table_df(dest, combined)?;
                        }
                    }
                }
            }
            Ok(dataframe_to_json(&df))
        }
        Command::SelectUnion { queries, all } => {
            let out = crate::server::exec::exec_select::handle_select_union(store, &queries, all)?;
            Ok(dataframe_to_json(&out))
        }
        Command::Calculate { target_sensor, query } => {
            crate::server::exec::exec_calculate::handle_calculate(store, &target_sensor, &query)
        }
        // KV STORE/KEY operations
        Command::CreateStore { database, store: st } => {
            // Creating a store is idempotent; obtaining it will create dir+config if missing
            let _ = store.clone(); // shadow
            let _kv = store.kv_store(&database, &st);
            // ensure settings saved
            // ignore error
            let _ = _kv.save_settings();
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::DropStore { database, store: st } => {
            let reg = store.kv_registry();
            let existed = reg.drop_store(&database, &st)?;
            Ok(serde_json::json!({"status":"ok","dropped": existed}))
        }
        Command::RenameStore { database, from, to } => {
            let reg = store.kv_registry();
            reg.rename_store(&database, &from, &to)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::ListStores { database } => {
            let reg = store.kv_registry();
            let stores = reg.list_stores(&database);
            Ok(serde_json::json!({"stores": stores}))
        }
        Command::ListKeys { database, store: st } => {
            let kv = store.kv_store(&database, &st);
            let mut keys = kv.keys();
            keys.sort();
            Ok(serde_json::json!({"keys": keys}))
        }
        Command::DescribeKey { database, store: st, key } => {
            let kv = store.kv_store(&database, &st);
            if let Some(val) = kv.get(&key) {
                match val {
                    KvValue::Str(s) => Ok(serde_json::json!({"key": key, "type":"string","value": s})),
                    KvValue::Int(n) => Ok(serde_json::json!({"key": key, "type":"int","value": n})),
                    KvValue::Json(j) => Ok(serde_json::json!({"key": key, "type":"json","value": j})),
                    KvValue::ParquetDf(df) => {
                        let cols_meta: Vec<serde_json::Value> = df.get_column_names().iter().map(|name| {
                            let dt = df.column(name.as_str()).ok().map(|c| format!("{:?}", c.dtype())).unwrap_or_else(|| "Unknown".into());
                            serde_json::json!({"name": name, "dtype": dt})
                        }).collect();
                        Ok(serde_json::json!({
                            "key": key,
                            "type": "table",
                            "rows": df.height(),
                            "cols": df.width(),
                            "columns": cols_meta
                        }))
                    }
                }
            } else {
                anyhow::bail!(format!("Key not found: {}.store.{}.{}", database, st, key));
            }
        }
        Command::WriteKey { database, store: st, key, value, ttl_ms, reset_on_access } => {
            crate::server::exec::exec_keys::handle_write_key(store, &database, &st, &key, &value, ttl_ms, reset_on_access)
        }
        Command::ReadKey { database, store: st, key } => {
            crate::server::exec::exec_keys::handle_read_key(store, &database, &st, &key)
        }
        Command::DropKey { database, store: st, key } => {
            crate::server::exec::exec_keys::handle_drop_key(store, &database, &st, &key)
        }
        Command::RenameKey { database, store: st, from, to } => {
            crate::server::exec::exec_keys::handle_rename_key(store, &database, &st, &from, &to)
        }
        Command::DeleteRows { database, where_clause } => {
            // Load full dataframe
            let df_all = read_df_or_kv(store, &database)?;
            // If no WHERE, truncate database
            let new_df = if let Some(w) = &where_clause {
                // Create DataContext with registry snapshot for WHERE clause evaluation
                let registry_snapshot = crate::scripts::get_script_registry()
                    .and_then(|r| r.snapshot().ok());
                let mut ctx = crate::server::data_context::DataContext::with_defaults("clarium", "public");
                if let Some(reg) = registry_snapshot {
                    ctx.script_registry = Some(reg);
                }
                // Build mask series (with subquery support)
                let mask = if where_contains_subquery(w) {
                    eval_where_mask(&df_all, &ctx, store, w)?
                } else {
                    let mask_df = df_all.clone().lazy().select([build_where_expr(w, &ctx).alias("__m__")]).collect()?;
                    mask_df.column("__m__")?.bool()?.clone()
                };
                let keep = mask.not();
                df_all.filter(&keep)?
            } else {
                // Empty df with only _time column
                DataFrame::new(vec![Series::new("_time".into(), Vec::<i64>::new()).into()])?
            };
            let guard = store.0.lock();
            guard.rewrite_table_df(&database, new_df)?;
            Ok(serde_json::json!({"status": "ok"}))
        }
        Command::Update { table, assignments, where_clause } => {
            crate::server::exec::exec_update::handle_update(store, table, assignments, where_clause)
        }
        Command::DeleteColumns { database, columns, where_clause } => {
            crate::server::exec::exec_delete::handle_delete_columns(store, database, columns, where_clause)
        }
        // script commands are delegated earlier to exec_scripts
        Command::SchemaShow { database } => {
            let guard = store.0.lock();
            // access private load function via public? Not available; so rebuild via filter_df expected? We'll implement via reading schema.json
            let (schema, locks) = guard.load_schema_with_locks(&database)?;
            let mut rows: Vec<serde_json::Value> = Vec::new();
            let mut names: Vec<String> = schema.keys().cloned().collect();
            names.sort();
            for name in names {
                if let Some(dt) = schema.get(&name) {
                    let ty = crate::storage::Store::dtype_to_str(dt);
                    rows.push(serde_json::json!({"name": name, "type": ty, "locked": locks.contains(&name)}));
                } else {
                    // If schema entry is unexpectedly missing, skip gracefully
                    continue;
                }
            }
            Ok(serde_json::json!({"schema": rows}))
        }
        Command::SchemaAdd { database, entries, primary_key, partitions } => {
            // Map type words to DataType
            let map_type = |s: &str| -> Result<DataType> {
                let up = s.to_uppercase();
                match up.as_str() {
                    "SENSOR" | "FLOAT64" | "FLOAT" | "DOUBLE" => Ok(DataType::Float64),
                    "DISCRETE" | "INT64" | "INTEGER" | "INT" => Ok(DataType::Int64),
                    "LABEL" | "STRING" | "UTF8" => Ok(DataType::String),
                    "DATETIME" | "DATE" | "TIMESTAMP" => Ok(DataType::Int64), // epoch ms
                    // Native vector column type backed by Polars List(Float64)
                    "VECTOR" => Ok(DataType::List(Box::new(DataType::Float64))),
                    _ => Err(anyhow::anyhow!(format!("Unknown type: {}", s))),
                }
            };
            let mut typed: Vec<(String, DataType)> = Vec::new();
            for (name, ty) in entries {
                let dt = map_type(&ty)?;
                typed.push((name, dt));
            }
            let guard = store.0.lock();
            if !typed.is_empty() { guard.schema_add(&database, &typed)?; }
            if primary_key.is_some() || partitions.is_some() { guard.set_table_metadata(&database, primary_key, partitions)?; }
            Ok(serde_json::json!({"status": "ok", "added": typed.len()}))
        }
        Command::DatabaseAdd { database } => {
            let guard = store.0.lock();
            guard.create_table(&database)?;
            Ok(serde_json::json!({"status": "ok"}))
        }
        Command::DatabaseDelete { database } => {
            let guard = store.0.lock();
            guard.delete_table(&database)?;
            Ok(serde_json::json!({"status": "ok"}))
        }
        // New DDL commands
        Command::CreateDatabase { name } => {
            use std::fs;
            let dir = store.root_path().join(name.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            fs::create_dir_all(&dir)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::DropDatabase { name } => {
            use std::fs;
            let dir = store.root_path().join(name.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            if dir.exists() { let _ = fs::remove_dir_all(&dir); }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::RenameDatabase { from, to } => {
            use std::fs;
            let src = store.root_path().join(from.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            let dst = store.root_path().join(to.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            if !src.exists() { anyhow::bail!("Source database not found: {}", from); }
            // Ensure parent of dst exists
            if let Some(parent) = dst.parent() { fs::create_dir_all(parent).ok(); }
            fs::rename(&src, &dst)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::CreateSchema { path } => {
            use std::fs;
            // If unqualified (no '/'), prepend current database
            let full = if path.contains('/') || path.contains('\\') { path.clone() } else { format!("{}/{}", crate::system::get_current_database(), path) };
            let dir = store.root_path().join(full.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            fs::create_dir_all(&dir)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::DropSchema { path } => {
            use std::fs;
            let full = if path.contains('/') || path.contains('\\') { path.clone() } else { format!("{}/{}", crate::system::get_current_database(), path) };
            let dir = store.root_path().join(full.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            if dir.exists() { let _ = fs::remove_dir_all(&dir); }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::RenameSchema { from, to } => {
            use std::fs;
            let from_full = if from.contains('/') || from.contains('\\') { from.clone() } else { format!("{}/{}", crate::system::get_current_database(), from) };
            let to_full = if to.contains('/') || to.contains('\\') { to.clone() } else { format!("{}/{}", crate::system::get_current_database(), to) };
            let src = store.root_path().join(from_full.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            let dst = store.root_path().join(to_full.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            if !src.exists() { anyhow::bail!("Source schema not found: {}", from); }
            if let Some(parent) = dst.parent() { fs::create_dir_all(parent).ok(); }
            fs::rename(&src, &dst)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::CreateTimeTable { table } => {
            // Qualify identifier with current session defaults
            let d = crate::system::current_query_defaults();
            let table = crate::ident::qualify_time_ident(&table, &d);
            // Prevent name collision with existing views
            {
                let root = store.root_path().clone();
                let mut vp = root.join(table.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
                // The table ident here is expected to end with .time, so the base directory is already with .time suffix
                // We want to check for a view that uses the base name without .time
                let base_no_time = if let Some(stripped) = table.strip_suffix(".time") { stripped } else { &table };
                vp = root.join(base_no_time.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
                vp.set_extension("view");
                if vp.exists() {
                    anyhow::bail!(format!("Object name conflict: a VIEW exists with name '{}'. Time table names must be unique across views.", base_no_time));
                }
            }
            let guard = store.0.lock();
            guard.create_table(&table)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::DropTimeTable { table } => {
            let d = crate::system::current_query_defaults();
            let table = crate::ident::qualify_time_ident(&table, &d);
            let guard = store.0.lock();
            guard.delete_table(&table)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::RenameTimeTable { from, to } => {
            use std::fs;
            let d = crate::system::current_query_defaults();
            let fromq = crate::ident::qualify_time_ident(&from, &d);
            let toq = crate::ident::qualify_time_ident(&to, &d);
            let src = store.root_path().join(fromq.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            let dst = store.root_path().join(toq.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            if !src.exists() { anyhow::bail!("Source time table not found: {}", from); }
            if let Some(parent) = dst.parent() { fs::create_dir_all(parent).ok(); }
            fs::rename(&src, &dst)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::CreateTable { table, primary_key, partitions } => {
            crate::server::exec::exec_create::handle_create_table(store, &table, &primary_key, &partitions)
        }
        Command::DropTable { table, if_exists } => {
            crate::server::exec::exec_create::handle_drop_table(store, &table, if_exists)
        }
        Command::RenameTable { from, to } => {
            crate::server::exec::exec_create::handle_rename_table(store, &from, &to)
        }
        Command::UserAdd { username, password, is_admin, perms, scope_db } => {
            // Build permissions
            let mut p = crate::security::Perms { is_admin, select: false, insert: false, calculate: false, delete: false };
            if is_admin { p.select = true; p.insert = true; p.calculate = true; p.delete = true; }
            for perm in perms.iter().map(|s| s.to_uppercase()) {
                match perm.as_str() {
                    "SELECT" => p.select = true,
                    "INSERT" => p.insert = true,
                    "CALCULATE" => p.calculate = true,
                    "DELETE" => p.delete = true,
                    _ => {}
                }
            }
            let root = store.root_path();
            let scope = match scope_db.as_deref() { Some(db) => crate::security::Scope::Database(db), None => crate::security::Scope::Global };
            crate::security::add_user(root.to_string_lossy().as_ref(), scope, &username, &password, p)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::UserDelete { username, scope_db } => {
            let root = store.root_path();
            let scope = match scope_db.as_deref() { Some(db) => crate::security::Scope::Database(db), None => crate::security::Scope::Global };
            crate::security::delete_user(root.to_string_lossy().as_ref(), scope, &username)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::UserAlter { username, new_password, is_admin, perms, scope_db } => {
            let root = store.root_path();
            let scope = match scope_db.as_deref() { Some(db) => crate::security::Scope::Database(db), None => crate::security::Scope::Global };
            // Build optional perms if provided
            let perms_opt = if let Some(v) = perms {
                let mut p = crate::security::Perms { is_admin: false, select: false, insert: false, calculate: false, delete: false };
                for perm in v.iter().map(|s| s.to_uppercase()) {
                    match perm.as_str() {
                        "SELECT" => p.select = true,
                        "INSERT" => p.insert = true,
                        "CALCULATE" => p.calculate = true,
                        "DELETE" => p.delete = true,
                        _ => {}
                    }
                }
                Some(p)
            } else { None };
            crate::security::alter_user(
                root.to_string_lossy().as_ref(),
                scope,
                &username,
                new_password.as_deref(),
                is_admin,
                perms_opt,
            )?;
            Ok(serde_json::json!({"status":"ok"}))
        }
    }
}

/// Panic-safe wrapper around `execute_query`.
///
/// Executes the query on a spawned task so that any internal panic is captured by the runtime
/// and converted to an error value instead of unwinding the current thread/task. This ensures
/// DDL and other execution errors never terminate the serving thread and can be reported back
/// to the user gracefully.
pub async fn execute_query_safe(store: &SharedStore, text: &str) -> Result<serde_json::Value> {
    use tracing::debug;
    let text_owned = text.to_string();
    let store_cloned = store.clone();
    let handle = tokio::spawn(async move {
        // Delegate to the regular executor; propagate its Result
        execute_query(&store_cloned, &text_owned).await
    });
    match handle.await {
        Ok(res) => res,
        Err(join_err) => {
            // Convert panic or cancellation into a user-visible error, without unwinding here
            if join_err.is_panic() {
                debug!(target: "exec", "execute_query_safe captured panic inside query task");
                Err(anyhow::anyhow!("query execution failed due to an internal panic"))
            } else if join_err.is_cancelled() {
                Err(anyhow::anyhow!("query execution was cancelled"))
            } else {
                Err(anyhow::anyhow!(format!("query execution task join error: {}", join_err)))
            }
        }
    }
}


// dataframe_to_tabular and execute_select_df are provided by exec_helpers and re-exported above.

// Convenience: normalize with defaults then execute
pub async fn execute_query_with_defaults(store: &SharedStore, text: &str, defaults: &QueryDefaults) -> Result<serde_json::Value> {
    let effective = crate::server::exec::exec_helpers::normalize_query_with_defaults(
        text,
        &defaults.current_database,
        &defaults.current_schema,
    );
    execute_query_safe(store, &effective).await
}

#[cfg(test)]
mod tests;
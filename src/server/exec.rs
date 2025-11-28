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

use anyhow::Result;
use polars::prelude::*;
use crate::{query, query::Command};
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

pub async fn execute_query(store: &SharedStore, text: &str) -> Result<serde_json::Value> {
    let cmd = query::parse(text)?;
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
        | Command::ShowScripts => {
            self::exec_show::execute_show(store, cmd).await
        }
        // USE and SET commands affect only session defaults; update thread-local defaults
        Command::UseDatabase { name } => {
            crate::system::set_current_database(&name);
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::UseSchema { name } => {
            crate::system::set_current_schema(&name);
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::Set { .. } => { Ok(serde_json::json!({"status":"ok"})) }
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
                        crate::query::IntoMode::Replace => { guard.rewrite_table_df(dest, df.clone())?; }
                        crate::query::IntoMode::Append => {
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
                let dt = schema.get(&name).unwrap();
                let ty = crate::storage::Store::dtype_to_str(dt);
                rows.push(serde_json::json!({"name": name, "type": ty, "locked": locks.contains(&name)}));
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


// dataframe_to_tabular and execute_select_df are provided by exec_helpers and re-exported above.

// Convenience: normalize with defaults then execute
pub async fn execute_query_with_defaults(store: &SharedStore, text: &str, defaults: &QueryDefaults) -> Result<serde_json::Value> {
    let effective = crate::server::exec::exec_helpers::normalize_query_with_defaults(
        text,
        &defaults.current_database,
        &defaults.current_schema,
    );
    execute_query(store, &effective).await
}

#[cfg(test)]
mod tests;



// Tracing macros for diagnostics (debug is already imported earlier in this module)
use tracing::{info, warn, error};
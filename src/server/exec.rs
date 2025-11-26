use anyhow::{Context, Result};
use serde_json::Value;
use polars::prelude::*;
use std::ops::Not;

use crate::{storage::{SharedStore, KvValue}, query::{self, Command}};
use crate::scripts::{get_script_registry, scripts_dir_for};
use std::path::Path;
use tracing::{debug};
// Wrapper that supports ROLLING BY by delegating to rolling or standard paths
// Shared defaults for database and schema qualification across protocols
use crate::ident::QueryDefaults;

mod df_utils;
mod exec_common;
pub(crate) mod exec_select;
mod exec_slice;
mod exec_show;
mod where_subquery;
mod select_stages;
use self::df_utils::{read_df_or_kv, dataframe_to_json};
use self::exec_select::run_select;
use self::exec_common::{build_where_expr};
use self::exec_slice::{run_slice};
use self::where_subquery::{where_contains_subquery, eval_where_mask};

pub async fn execute_query(store: &SharedStore, text: &str) -> Result<Value> {
    debug!(target: "clarium::exec", "execute_query: text='{}'", text);
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
        // USE and SET commands affect only session defaults; return ok here
        Command::UseDatabase { .. } | Command::UseSchema { .. } | Command::Set { .. } => {
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::Select(q) => {
            let df = run_select(store, &q)?;
            // Handle SELECT ... INTO <table> [APPEND|REPLACE]
            if let Some(dest) = &q.into_table {
                let dest = dest.trim();
                let mode = q.into_mode.clone().unwrap_or(crate::query::IntoMode::Append);
                let guard = store.0.lock();
                // Ensure destination exists (create if missing)
                // create_table works for both .time and regular tables; .time is implied by suffix
                guard.create_table(dest).ok();
                if dest.ends_with(".time") {
                    // Expect exactly one _time column and ensure uniqueness
                    let time_cols = df.get_column_names().into_iter().filter(|n| n.as_str() == "_time").count();
                    if time_cols != 1 { anyhow::bail!("INTO time table requires exactly one _time column in the projection"); }
                    let time_col = df.column("_time").ok();
                    let time = time_col.and_then(|c| c.i64().ok()).ok_or_else(|| anyhow::anyhow!("_time not in result for INTO time table"))?;
                    // Uniqueness and non-null check
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
                        crate::query::IntoMode::Replace => {
                            // Full rewrite
                            guard.rewrite_table_df(dest, df.clone())?;
                        }
                        crate::query::IntoMode::Append => {
                            // Read existing if present and vstack, else just write new
                            let combined = match guard.read_df(dest) {
                                Ok(existing) => { existing.vstack(&df)? }
                                Err(_) => df.clone(),
                            };
                            guard.rewrite_table_df(dest, combined)?;
                        }
                    }
                }
            }
            Ok(dataframe_to_json(&df))
        }
        Command::SelectUnion { queries, all } => {
            // Execute each query and collect DataFrames
            let mut dfs: Vec<DataFrame> = Vec::new();
            for q in queries {
                let df = run_select(store, &q)?;
                dfs.push(df);
            }
            // Align schemas (union of columns)
            let mut all_cols: Vec<String> = Vec::new();
            for df in &dfs {
                for n in df.get_column_names().iter().map(|s| s.to_string()) {
                    if !all_cols.contains(&n) { all_cols.push(n); }
                }
            }
            let mut aligned: Vec<DataFrame> = Vec::new();
            // First, determine the dtype for each column from the first DF that has it
            let mut col_types: std::collections::HashMap<String, DataType> = std::collections::HashMap::new();
            for df in &dfs {
                for col_name in df.get_column_names() {
                    let col_name_str = col_name.to_string();
                    if !col_types.contains_key(&col_name_str) {
                        if let Ok(col) = df.column(col_name.as_str()) {
                            col_types.insert(col_name_str, col.dtype().clone());
                        }
                    }
                }
            }
            for mut df in dfs {
                let df_cols: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
                for c in &all_cols {
                    if !df_cols.iter().any(|n| n == c) {
                        // Create a null series with the correct dtype
                        let dtype = col_types.get(c).cloned().unwrap_or(DataType::Null);
                        let s = Series::new_null(c.as_str().into(), df.height()).cast(&dtype)?;
                        df.with_column(s)?;
                    }
                }
                // Reorder columns to all_cols order
                let cols: Vec<Column> = all_cols.iter().map(|n| df.column(n.as_str()).unwrap().clone()).collect();
                aligned.push(DataFrame::new(cols)?);
            }
            // Concatenate
            let mut out = if aligned.is_empty() { DataFrame::new(Vec::<Column>::new())? } else {
                let mut acc = aligned[0].clone();
                for df in aligned.iter().skip(1) { acc.vstack_mut(df)?; }
                acc
            };
            if !all {
                // DISTINCT rows
                out = out.lazy().unique(None, UniqueKeepStrategy::First).collect()?;
            }
            Ok(dataframe_to_json(&out))
        }
        Command::Calculate { target_sensor, query } => {
            // run select
            let df = run_select(store, &query)?;
            // Expect columns: _time and one value column
            let mut records = Vec::with_capacity(df.height());
            let time_col = df.column("_time").ok();
            let time = time_col.and_then(|c| c.i64().ok()).ok_or_else(|| anyhow::anyhow!("_time not in result for CALCULATE"))?;

            // pick the first non-time column for value
            let val_series_name = df.get_column_names().into_iter().find(|n| n.as_str() != "_time").ok_or_else(|| anyhow::anyhow!("No value column to save"))?;
            let val_series = df.column(val_series_name)?;

            match val_series.dtype() {
                DataType::Float64 => {
                    let vals = val_series.f64()?;
                    for i in 0..df.height() {
                        let t = time.get(i).ok_or_else(|| anyhow::anyhow!("bad time index"))?;
                        if let Some(v) = vals.get(i) {
                            let mut map = serde_json::Map::new();
                            map.insert(target_sensor.clone(), serde_json::json!(v));
                            records.push(crate::storage::Record { _time: t, sensors: map });
                        }
                    }
                }
                DataType::Int64 => {
                    let vals = val_series.i64()?;
                    for i in 0..df.height() {
                        let t = time.get(i).ok_or_else(|| anyhow::anyhow!("bad time index"))?;
                        if let Some(v) = vals.get(i) {
                            let mut map = serde_json::Map::new();
                            map.insert(target_sensor.clone(), serde_json::json!(v));
                            records.push(crate::storage::Record { _time: t, sensors: map });
                        }
                    }
                }
                DataType::String => {
                    for i in 0..df.height() {
                        let t = time.get(i).ok_or_else(|| anyhow::anyhow!("bad time index"))?;
                        let av = val_series.get(i);
                        if let Ok(AnyValue::StringOwned(s)) = av {
                            let mut map = serde_json::Map::new();
                            map.insert(target_sensor.clone(), serde_json::json!(s));
                            records.push(crate::storage::Record { _time: t, sensors: map });
                        } else if let Ok(AnyValue::String(s)) = av {
                            let mut map = serde_json::Map::new();
                            map.insert(target_sensor.clone(), serde_json::json!(s));
                            records.push(crate::storage::Record { _time: t, sensors: map });
                        }
                    }
                }
                dt => {
                    anyhow::bail!("Unsupported CALCULATE value dtype: {:?}", dt);
                }
            }
            {
                let guard = store.0.lock();
                let tbl = query.base_table.as_ref().ok_or_else(|| anyhow::anyhow!("CALCULATE requires a FROM source to persist results"))?;
                let table_name = tbl.table_name().ok_or_else(|| anyhow::anyhow!("CALCULATE requires a table, not a subquery"))?;
                guard.write_records(table_name, &records)?;
            }
            Ok(serde_json::json!({"saved": records.len()}))
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
            use std::time::Duration;
            let kv = store.kv_store(&database, &st);
            let ttl = ttl_ms.and_then(|ms| if ms > 0 { Some(Duration::from_millis(ms as u64)) } else { None });
            let vstr = value.trim();
            // Helper to try loading DF from an address
            let try_df = || -> anyhow::Result<DataFrame> {
                // Value could be a KV address or a table path
                read_df_or_kv(store, vstr)
            };
            let kind: &str;
            let kv_val = if vstr.starts_with('{') || vstr.starts_with('[') {
                // JSON literal
                let j: serde_json::Value = serde_json::from_str(vstr)?;
                kind = "json";
                KvValue::Json(j)
            } else if (vstr.starts_with('"') && vstr.ends_with('"')) || (vstr.starts_with('\'') && vstr.ends_with('\'')) {
                // Quoted string
                let un = &vstr[1..vstr.len()-1];
                kind = "string";
                KvValue::Str(un.to_string())
            } else if vstr.contains(".store.") || vstr.contains(".time") || vstr.contains('/') {
                if let Ok(df) = try_df() {
                    kind = "table";
                    KvValue::ParquetDf(df)
                } else if let Ok(n) = vstr.parse::<i64>() {
                    kind = "int";
                    KvValue::Int(n)
                } else {
                    kind = "string";
                    KvValue::Str(vstr.to_string())
                }
            } else if let Ok(n) = vstr.parse::<i64>() {
                kind = "int";
                KvValue::Int(n)
            } else {
                // Try JSON as last resort
                if let Ok(j) = serde_json::from_str::<serde_json::Value>(vstr) {
                    kind = "json"; KvValue::Json(j)
                } else {
                    kind = "string"; KvValue::Str(vstr.to_string())
                }
            };
            kv.set(&key, kv_val, ttl, reset_on_access);
            Ok(serde_json::json!({"status":"ok","written":1,"type": kind}))
        }
        Command::ReadKey { database, store: st, key } => {
            let kv = store.kv_store(&database, &st);
            if let Some(val) = kv.get(&key) {
                match val {
                    KvValue::Str(s) => Ok(serde_json::json!({"type":"string","value": s})),
                    KvValue::Int(n) => Ok(serde_json::json!({"type":"int","value": n})),
                    KvValue::Json(j) => Ok(serde_json::json!({"type":"json","value": j})),
                    KvValue::ParquetDf(df) => {
                        let cols_meta: Vec<serde_json::Value> = df.get_column_names().iter().map(|name| {
                            let dt = df.column(name.as_str()).ok().map(|c| format!("{:?}", c.dtype())).unwrap_or_else(|| "Unknown".into());
                            serde_json::json!({"name": name, "dtype": dt})
                        }).collect();
                        Ok(serde_json::json!({
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
        Command::DropKey { database, store: st, key } => {
            let kv = store.kv_store(&database, &st);
            let existed = kv.delete(&key);
            Ok(serde_json::json!({"status":"ok","dropped": existed}))
        }
        Command::RenameKey { database, store: st, from, to } => {
            let kv = store.kv_store(&database, &st);
            let moved = kv.rename_key(&from, &to);
            Ok(serde_json::json!({"status":"ok","renamed": moved}))
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
        Command::DeleteColumns { database, columns, where_clause } => {
            // Load full dataframe
            let mut df_all = read_df_or_kv(store, &database)?;
            if columns.is_empty() { return Ok(serde_json::json!({"status":"no-op"})); }
            let new_df = if let Some(w) = &where_clause {
                // Create DataContext with registry snapshot for WHERE clause evaluation
                let registry_snapshot = crate::scripts::get_script_registry()
                    .and_then(|r| r.snapshot().ok());
                let mut ctx = crate::server::data_context::DataContext::with_defaults("clarium", "public");
                if let Some(reg) = registry_snapshot {
                    ctx.script_registry = Some(reg);
                }
                // Build mask boolean (with subquery support)
                let mask = if where_contains_subquery(w) {
                    eval_where_mask(&df_all, &ctx, store, w)?
                } else {
                    let mask_df = df_all.clone().lazy().select([build_where_expr(w, &ctx).alias("__m__")]).collect()?;
                    mask_df.column("__m__")?.bool()?.clone()
                };
                // For each target column, null out where mask true
                for name in &columns {
                    if !df_all.get_column_names().iter().any(|c| c.as_str() == name) { continue; }
                    let s = df_all.column(name.as_str())?;
                    let dt = s.dtype().clone();
                    let len = s.len();
                    let mut null_idx: Vec<bool> = Vec::with_capacity(len);
                    for i in 0..len { null_idx.push(mask.get(i).unwrap_or(false)); }
                    let new_series = match dt {
                        DataType::Int64 => {
                            let ca = s.i64()?;
                            let mut out: Vec<Option<i64>> = Vec::with_capacity(len);
                            for (i, v) in ca.into_iter().enumerate() { if null_idx[i] { out.push(None); } else { out.push(v); } }
                            Series::new(name.clone().into(), out)
                        }
                        DataType::Float64 => {
                            let ca = s.f64()?;
                            let mut out: Vec<Option<f64>> = Vec::with_capacity(len);
                            for (i, v) in ca.into_iter().enumerate() { if null_idx[i] { out.push(None); } else { out.push(v); } }
                            Series::new(name.clone().into(), out)
                        }
                        DataType::String => {
                            let mut out: Vec<Option<String>> = Vec::with_capacity(len);
                            for i in 0..len {
                                if null_idx[i] { out.push(None); } else {
                                    match s.get(i) {
                                        Ok(AnyValue::StringOwned(v)) => out.push(Some(v.to_string())),
                                        Ok(AnyValue::String(v)) => out.push(Some(v.to_string())),
                                        _ => out.push(None),
                                    }
                                }
                            }
                            Series::new(name.clone().into(), out)
                        }
                        _ => {
                            // Fallback: cast to Float64
                            let s_cast = s.cast(&DataType::Float64)?;
                            let ca = s_cast.f64()?;
                            let mut out: Vec<Option<f64>> = Vec::with_capacity(len);
                            for (i, v) in ca.into_iter().enumerate() { if null_idx[i] { out.push(None); } else { out.push(v); } }
                            Series::new(name.clone().into(), out)
                        }
                    };
                    df_all.replace(name.as_str(), new_series)?;
                }
                df_all
            } else {
                // Drop columns entirely
                let to_drop: Vec<&str> = columns.iter().filter(|n| df_all.get_column_names().iter().any(|c| c.as_str() == n.as_str())).map(|s| s.as_str()).collect();
                if !to_drop.is_empty() { df_all = df_all.drop_many(to_drop); }
                df_all
            };
            let guard = store.0.lock();
            guard.rewrite_table_df(&database, new_df)?;
            Ok(serde_json::json!({"status": "ok"}))
        }
        Command::CreateScript { path, code } => {
            use std::fs;
            let root = { let g = store.0.lock(); g.root_path().clone() };
            let parts: Vec<&str> = path.split('/').collect();
            if parts.len() != 3 { anyhow::bail!("SCRIPT path must be <db>/<schema>/<name>"); }
            let dir = scripts_dir_for(std::path::Path::new(&root), parts[0], parts[1]);
            fs::create_dir_all(&dir)?;
            let mut fname = parts[2].to_string();
            if !fname.ends_with(".lua") { fname.push_str(".lua"); }
            let fpath = dir.join(&fname);
            fs::write(&fpath, code.as_bytes())?;
            if let Some(reg) = get_script_registry() {
                let name_no_ext = parts[2].split('.').next().unwrap_or(parts[2]);
                let text = code;
                let _ = reg.load_script_text(name_no_ext, &text);
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::DropScript { path } => {
            use std::fs;
            let root = { let g = store.0.lock(); g.root_path().clone() };
            let parts: Vec<&str> = path.split('/').collect();
            if parts.len() != 3 { anyhow::bail!("SCRIPT path must be <db>/<schema>/<name>"); }
            let dir = scripts_dir_for(std::path::Path::new(&root), parts[0], parts[1]);
            let mut fname = parts[2].to_string();
            if !fname.ends_with(".lua") { fname.push_str(".lua"); }
            let fpath = dir.join(&fname);
            if fpath.exists() { fs::remove_file(&fpath)?; }
            if let Some(reg) = get_script_registry() {
                let name_no_ext = parts[2].split('.').next().unwrap_or(parts[2]);
                reg.unload_function(name_no_ext);
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::RenameScript { from, to } => {
            use std::fs;
            let root = { let g = store.0.lock(); g.root_path().clone() };
            let fparts: Vec<&str> = from.split('/').collect();
            if fparts.len() != 3 { anyhow::bail!("SCRIPT path must be <db>/<schema>/<name>"); }
            let dir = scripts_dir_for(std::path::Path::new(&root), fparts[0], fparts[1]);
            fs::create_dir_all(&dir)?;
            let mut from_name = fparts[2].to_string(); if !from_name.ends_with(".lua") { from_name.push_str(".lua"); }
            let mut to_name = {
                let tparts: Vec<&str> = to.split('/').collect();
                if tparts.len() == 1 { tparts[0].to_string() } else if tparts.len() == 3 { if tparts[0]!=fparts[0] || tparts[1]!=fparts[1] { anyhow::bail!("Cannot move scripts across schemas"); } tparts[2].to_string() } else { anyhow::bail!("Invalid RENAME SCRIPT target"); }
            };
            if !to_name.ends_with(".lua") { to_name.push_str(".lua"); }
            let fp_from = dir.join(&from_name);
            let fp_to = dir.join(&to_name);
            fs::rename(&fp_from, &fp_to)?;
            if let Some(reg) = get_script_registry() {
                let oldn = fparts[2].split('.').next().unwrap_or(fparts[2]);
                let newn = to_name.trim_end_matches(".lua");
                let _ = reg.rename_function(oldn, newn);
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::LoadScript { path } => {
            use std::fs;
            let root = { let g = store.0.lock(); g.root_path().clone() };
            if let Some(p) = path {
                let parts: Vec<&str> = p.split('/').collect();
                if parts.len() != 3 { anyhow::bail!("SCRIPT path must be <db>/<schema>/<name>"); }
                let dir = scripts_dir_for(std::path::Path::new(&root), parts[0], parts[1]);
                let mut fname = parts[2].to_string(); if !fname.ends_with(".lua") { fname.push_str(".lua"); }
                let fpath = dir.join(&fname);
                let code = fs::read_to_string(&fpath)?;
                if let Some(reg) = get_script_registry() { let name_no_ext = parts[2].split('.').next().unwrap_or(parts[2]); let _ = reg.load_script_text(name_no_ext, &code); }
            } else {
                // Load all scripts from all schemas
                for dbent in fs::read_dir(&root)? {
                    let dbent = dbent?; if !dbent.file_type()?.is_dir() { continue; }
                    for schent in fs::read_dir(dbent.path())? { let schent = schent?; if !schent.file_type()?.is_dir() { continue; }
                        let sdir = scripts_dir_for(std::path::Path::new(&root), &dbent.file_name().to_string_lossy(), &schent.file_name().to_string_lossy());
                        if sdir.exists() {
                            for sf in fs::read_dir(&sdir)? { let sf = sf?; let pth = sf.path(); if pth.extension().and_then(|e| e.to_str()).unwrap_or("").eq_ignore_ascii_case("lua") { let name = pth.file_stem().and_then(|s| s.to_str()).unwrap_or(""); let code = fs::read_to_string(&pth)?; if let Some(reg) = get_script_registry() { let _ = reg.load_script_text(name, &code); } }
                            }
                        }
                    }
                }
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
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
            let dir = store.root_path().join(path.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            fs::create_dir_all(&dir)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::DropSchema { path } => {
            use std::fs;
            let dir = store.root_path().join(path.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            if dir.exists() { let _ = fs::remove_dir_all(&dir); }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::RenameSchema { from, to } => {
            use std::fs;
            let src = store.root_path().join(from.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            let dst = store.root_path().join(to.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            if !src.exists() { anyhow::bail!("Source schema not found: {}", from); }
            if let Some(parent) = dst.parent() { fs::create_dir_all(parent).ok(); }
            fs::rename(&src, &dst)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::CreateTimeTable { table } => {
            let guard = store.0.lock();
            guard.create_table(&table)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::DropTimeTable { table } => {
            let guard = store.0.lock();
            guard.delete_table(&table)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::RenameTimeTable { from, to } => {
            use std::fs;
            let src = store.root_path().join(from.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            let dst = store.root_path().join(to.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            if !src.exists() { anyhow::bail!("Source time table not found: {}", from); }
            if let Some(parent) = dst.parent() { fs::create_dir_all(parent).ok(); }
            fs::rename(&src, &dst)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::CreateTable { table, primary_key, partitions } => {
            // Regular table: no .time suffix; initialize schema.json (tableType=regular)
            debug!(target: "clarium::exec", "CreateTable: begin table='{}' pk={:?} partitions={:?}", table, primary_key, partitions);
            let guard = store.0.lock();
            if table.ends_with(".time") { anyhow::bail!("CREATE TABLE cannot target a .time table"); }
            guard.create_table(&table)?;
            debug!(target: "clarium::exec", "CreateTable: directory/schema ensured for table='{}'", table);
            if primary_key.is_some() || partitions.is_some() {
                guard.set_table_metadata(&table, primary_key, partitions)?;
                debug!(target: "clarium::exec", "CreateTable: metadata saved for table='{}'", table);
            }
            debug!(target: "clarium::exec", "CreateTable: success table='{}'", table);
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::DropTable { table, if_exists } => {
            let guard = store.0.lock();
            if table.ends_with(".time") { anyhow::bail!("DROP TABLE cannot target a .time table"); }
            // Check if table exists
            let table_path = guard.root_path().join(table.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            let exists = table_path.exists();
            // If IF EXISTS is used and table doesn't exist, return success without error
            if if_exists && !exists {
                return Ok(serde_json::json!({"status":"ok"}));
            }
            // If table doesn't exist and IF EXISTS is not used, return error
            if !exists {
                anyhow::bail!("Table not found: {}", table);
            }
            // Otherwise proceed with normal deletion
            guard.delete_table(&table)?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::RenameTable { from, to } => {
            use std::fs;
            if from.ends_with(".time") || to.ends_with(".time") { anyhow::bail!("RENAME TABLE cannot rename .time tables; use RENAME TIME TABLE"); }
            let src = store.root_path().join(from.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            let dst = store.root_path().join(to.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
            if !src.exists() { anyhow::bail!("Source table not found: {}", from); }
            if let Some(parent) = dst.parent() { fs::create_dir_all(parent).ok(); }
            fs::rename(&src, &dst)?;
            Ok(serde_json::json!({"status":"ok"}))
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








fn qualify_identifier_with_defaults(ident: &str, db: &str, schema: &str) -> String {
    let d = crate::ident::QueryDefaults::new(db.to_string(), schema.to_string());
    crate::ident::qualify_time_ident(ident, &d)
}

fn qualify_identifier_regular_table_with_defaults(ident: &str, db: &str, schema: &str) -> String {
    let d = crate::ident::QueryDefaults::new(db.to_string(), schema.to_string());
    crate::ident::qualify_regular_ident(ident, &d)
}

pub fn normalize_query_with_defaults(q: &str, db: &str, schema: &str) -> String {
    let up = q.to_uppercase();
    // Normalize unqualified regular TABLE DDL to include current db/schema
    if up.starts_with("DROP TABLE ") {
        let prefix = "DROP TABLE";
        let mut s = q[prefix.len()..].trim_start();
        let s_up = s.to_uppercase();
        // skip optional IF EXISTS
        let mut if_exists = "";
        if s_up.starts_with("IF EXISTS ") {
            if_exists = " IF EXISTS";
            s = &s["IF EXISTS ".len()..].trim_start();
        }
        if s.is_empty() { return q.to_string(); }
        let qualified = qualify_identifier_regular_table_with_defaults(s, db, schema);
        return format!("{}{} {}", prefix, if_exists, qualified);
    }
    if up.starts_with("RENAME TABLE ") {
        let prefix = "RENAME TABLE";
        let tail = &q[prefix.len()..];
        let tail_up = tail.to_uppercase();
        if let Some(i) = tail_up.find(" TO ") {
            let left = tail[..i].trim();
            let right = tail[i+4..].trim();
            if left.is_empty() || right.is_empty() { return q.to_string(); }
            let ql = qualify_identifier_regular_table_with_defaults(left, db, schema);
            let qr = qualify_identifier_regular_table_with_defaults(right, db, schema);
            return format!("{} {} TO {}", prefix, ql, qr);
        }
        return q.to_string();
    }
    // Qualify INSERT INTO targets using current db/schema and require .time
    if up.starts_with("INSERT INTO ") {
        // Preserve rest of statement, only replace target identifier
        let after = &q["INSERT INTO ".len()..];
        let mut ident = after.trim_start();
        let mut rest = "";
        // identifier ends at first whitespace or '(' whichever comes first
        for (i, ch) in ident.char_indices() {
            if ch.is_whitespace() || ch == '(' { rest = &ident[i..]; ident = &ident[..i]; break; }
        }
        if ident.is_empty() { return q.to_string(); }
        let mut qualified = qualify_identifier_with_defaults(ident, db, schema);
        if !qualified.ends_with(".time") { qualified.push_str(".time"); }
        return format!("INSERT INTO {}{}", qualified, rest);
    }
    // Do not rewrite SELECT or SLICE statements; column/table resolution is handled by Data Context at execution time
    if up.starts_with("SELECT ") || up.starts_with("SLICE") { return q.to_string(); }
    // Qualify CREATE TABLE targets using current db/schema (regular table)
    if up.starts_with("CREATE TABLE ") {
        let after = &q["CREATE TABLE ".len()..];
        let mut s = after;
        let s_up = s.to_uppercase();
        // skip optional IF NOT EXISTS
        let mut consumed = 0usize;
        if s_up.starts_with("IF NOT EXISTS ") { consumed = "IF NOT EXISTS ".len(); s = &s[consumed..]; }
        // Extract identifier up to '(' or whitespace
        let mut ident = s.trim_start();
        let mut rest = "";
        for (i, ch) in ident.char_indices() {
            if ch.is_whitespace() || ch == '(' { rest = &ident[i..]; ident = &ident[..i]; break; }
        }
        if ident.is_empty() { return q.to_string(); }
        let qualified = qualify_identifier_regular_table_with_defaults(ident, db, schema);
        let prefix = if consumed > 0 { format!("CREATE TABLE IF NOT EXISTS {}", qualified) } else { format!("CREATE TABLE {}", qualified) };
        return format!("{}{}", prefix, rest);
    }
    if up.starts_with("DELETE ") {
        if let Some(idx) = up.find(" FROM ") {
            let (head, tail) = q.split_at(idx + 6);
            let mut ident = tail.trim_start();
            let mut rest = "";
            for (i, ch) in ident.char_indices() {
                if ch.is_whitespace() { rest = &ident[i..]; ident = &ident[..i]; break; }
            }
            if ident.is_empty() { return q.to_string(); }
            let qualified = qualify_identifier_with_defaults(ident, db, schema);
            return format!("{}{}{}", head, qualified, rest);
        }
    }
    if up.starts_with("CALCULATE ") {
        if let Some(p) = up.find(" AS SELECT ") {
            let (left, right) = q.split_at(p + 4);
            let normalized = normalize_query_with_defaults(&right[1..], db, schema);
            return format!("{} {}", left, normalized);
        }
    }
    q.to_string()
}

pub fn execute_select_df(store: &SharedStore, q: &crate::query::Query) -> Result<DataFrame> {
    // Staged pipeline: use unified run_select path for all cases (ROLLING handled inside stages)
    run_select(store, q)
}

pub fn dataframe_to_tabular(df: &DataFrame) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    let cols: Vec<String> = df.get_column_names().into_iter().map(|s| s.to_string()).collect();
    let mut data: Vec<Vec<Option<String>>> = Vec::with_capacity(df.height());
    for row_idx in 0..df.height() {
        let mut row: Vec<Option<String>> = Vec::with_capacity(cols.len());
        for c in &cols {
            let s = df.column(c.as_str()).unwrap();
            let av = s.get(row_idx);
            let cell = match av {
                Ok(AnyValue::Int64(v)) => Some(v.to_string()),
                Ok(AnyValue::Int32(v)) => Some((v as i64).to_string()),
                Ok(AnyValue::Float64(v)) => Some(v.to_string()),
                Ok(AnyValue::Boolean(v)) => Some(if v {"t".into()} else {"f".into()}),
                Ok(AnyValue::String(v)) => Some(v.to_string()),
                Ok(AnyValue::StringOwned(v)) => Some(v.to_string()),
                Ok(AnyValue::Null) => None,
                _ => None,
            };
            row.push(cell);
        }
        data.push(row);
    }
    (cols, data)
}

pub async fn execute_query2(store: &SharedStore, text: &str) -> Result<serde_json::Value> {
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
        Command::Select(q) => {
            let df = run_select(store, &q)?;
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
        Command::CreateScript { path, code } => {
            use std::fs;
            let root = { let g = store.0.lock(); g.root_path().clone() };
            // Expect path in form db/schema/name[.lua]
            let parts: Vec<&str> = path.split('/').collect();
            if parts.len() != 3 { anyhow::bail!("SCRIPT path must be <db>/<schema>/<name>"); }
            let dir = scripts_dir_for(Path::new(&root), parts[0], parts[1]);
            fs::create_dir_all(&dir)?;
            let mut fname = parts[2].to_string();
            if !fname.ends_with(".lua") { fname.push_str(".lua"); }
            let fpath = dir.join(&fname);
            fs::write(&fpath, code.as_bytes())?;
            if let Some(reg) = get_script_registry() {
                let name_no_ext = parts[2].split('.').next().unwrap_or(parts[2]);
                let text = code;
                let _ = reg.load_script_text(name_no_ext, &text);
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::DropScript { path } => {
            use std::fs;
            let root = { let g = store.0.lock(); g.root_path().clone() };
            let parts: Vec<&str> = path.split('/').collect();
            if parts.len() != 3 { anyhow::bail!("SCRIPT path must be <db>/<schema>/<name>"); }
            let dir = scripts_dir_for(Path::new(&root), parts[0], parts[1]);
            let mut fname = parts[2].to_string();
            if !fname.ends_with(".lua") { fname.push_str(".lua"); }
            let fpath = dir.join(&fname);
            if fpath.exists() { fs::remove_file(&fpath)?; }
            if let Some(reg) = get_script_registry() {
                let name_no_ext = parts[2].split('.').next().unwrap_or(parts[2]);
                reg.unload_function(name_no_ext);
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::RenameScript { from, to } => {
            use std::fs;
            let root = { let g = store.0.lock(); g.root_path().clone() };
            let fparts: Vec<&str> = from.split('/').collect();
            if fparts.len() != 3 { anyhow::bail!("SCRIPT path must be <db>/<schema>/<name>"); }
            let dir = scripts_dir_for(Path::new(&root), fparts[0], fparts[1]);
            fs::create_dir_all(&dir)?;
            let mut from_name = fparts[2].to_string(); if !from_name.ends_with(".lua") { from_name.push_str(".lua"); }
            let mut to_name = {
                let tparts: Vec<&str> = to.split('/').collect();
                if tparts.len() == 1 { tparts[0].to_string() } else if tparts.len() == 3 { if tparts[0]!=fparts[0] || tparts[1]!=fparts[1] { anyhow::bail!("Cannot move scripts across schemas"); } tparts[2].to_string() } else { anyhow::bail!("Invalid RENAME SCRIPT target"); }
            };
            if !to_name.ends_with(".lua") { to_name.push_str(".lua"); }
            let fp_from = dir.join(&from_name);
            let fp_to = dir.join(&to_name);
            fs::rename(&fp_from, &fp_to)?;
            if let Some(reg) = get_script_registry() {
                let oldn = fparts[2].split('.').next().unwrap_or(fparts[2]);
                let newn = to_name.trim_end_matches(".lua");
                let _ = reg.rename_function(oldn, newn);
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::LoadScript { path } => {
            use std::fs;
            let root = { let g = store.0.lock(); g.root_path().clone() };
            if let Some(p) = path {
                let parts: Vec<&str> = p.split('/').collect();
                if parts.len() != 3 { anyhow::bail!("SCRIPT path must be <db>/<schema>/<name>"); }
                let dir = scripts_dir_for(Path::new(&root), parts[0], parts[1]);
                let mut fname = parts[2].to_string(); if !fname.ends_with(".lua") { fname.push_str(".lua"); }
                let fpath = dir.join(&fname);
                let code = fs::read_to_string(&fpath)?;
                if let Some(reg) = get_script_registry() { let name_no_ext = parts[2].split('.').next().unwrap_or(parts[2]); let _ = reg.load_script_text(name_no_ext, &code); }
            } else {
                // Load all scripts from all schemas
                for dbent in fs::read_dir(&root)? {
                    let dbent = dbent?; if !dbent.file_type()?.is_dir() { continue; }
                    for schent in fs::read_dir(dbent.path())? { let schent = schent?; if !schent.file_type()?.is_dir() { continue; }
                        let sdir = scripts_dir_for(Path::new(&root), &dbent.file_name().to_string_lossy(), &schent.file_name().to_string_lossy());
                        if sdir.exists() {
                            for sf in fs::read_dir(&sdir)? { let sf = sf?; let pth = sf.path(); if pth.extension().and_then(|e| e.to_str()).unwrap_or("").eq_ignore_ascii_case("lua") { let name = pth.file_stem().and_then(|s| s.to_str()).unwrap_or(""); let code = fs::read_to_string(&pth)?; if let Some(reg) = get_script_registry() { let _ = reg.load_script_text(name, &code); } } }
                        }
                    }
                }
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        _ => execute_query(store, text).await,
    }
}

// Convenience: normalize with defaults then execute
pub async fn execute_query_with_defaults(store: &SharedStore, text: &str, defaults: &QueryDefaults) -> Result<serde_json::Value> {
    let effective = normalize_query_with_defaults(text, &defaults.current_database, &defaults.current_schema);
    execute_query2(store, &effective).await
}

/// Parse and execute a CREATE TABLE statement with full SQL syntax (columns and types).
/// This function is used by pgwire to support CREATE TABLE commands from SQL clients.
/// 
/// Syntax: CREATE TABLE [IF NOT EXISTS] <db>/<schema>/<table> (col type, ...)
pub fn do_create_table(store: &SharedStore, q: &str) -> Result<()> {
    use anyhow::anyhow;
    // Parse: CREATE TABLE [IF NOT EXISTS] <db>/<schema>/<table> (col type, ...)
    debug!(target: "clarium::exec", "do_create_table: begin q='{}'", q);
    let mut s = q.trim();
    let up = s.to_uppercase();
    if !up.starts_with("CREATE TABLE ") { return Err(anyhow!("unsupported CREATE TABLE")); }
    s = s["CREATE TABLE ".len()..].trim();
    let s_up = s.to_uppercase();
    if s_up.starts_with("IF NOT EXISTS ") { s = s["IF NOT EXISTS ".len()..].trim(); }
    // Extract identifier up to '(' and the column list inside (...)
    let p_open = s.find('(').ok_or_else(|| anyhow!("expected ( in CREATE TABLE"))?;
    let ident = s[..p_open].trim();
    let p_close = s.rfind(')').ok_or_else(|| anyhow!("expected ) in CREATE TABLE"))?;
    let cols_str = &s[p_open+1 .. p_close];
    // Parse columns: name type [, name type ...]
    // Also detect PRIMARY KEY constraints (both inline and table-level)
    let mut cols: Vec<(String, String)> = Vec::new();
    let mut cur = String::new();
    let mut depth = 0i32;
    for ch in cols_str.chars() {
        match ch {
            '(' => { depth += 1; cur.push(ch); }
            ')' => { depth -= 1; cur.push(ch); }
            ',' if depth == 0 => { if !cur.trim().is_empty() { cols.push(split_col_def(cur.trim())); } cur.clear(); }
            _ => cur.push(ch),
        }
    }
    if !cur.trim().is_empty() { cols.push(split_col_def(cur.trim())); }
    
    // Detect PRIMARY KEY: check for inline "PRIMARY KEY" or table-level "PRIMARY KEY (col)"
    let mut has_primary_key = false;
    let cols_str_up = cols_str.to_uppercase();
    if cols_str_up.contains("PRIMARY KEY") {
        has_primary_key = true;
    }
    
    // Map SQL types to internal type keys
    let mut schema_entries: Vec<(String, String)> = Vec::new();
    for (name, ty) in cols.into_iter() {
        let n = name.trim_matches('"').to_string();
        // Skip table-level constraints (they start with keywords like PRIMARY, FOREIGN, UNIQUE, CHECK, CONSTRAINT)
        let n_up = n.to_uppercase();
        if n_up == "PRIMARY" || n_up == "FOREIGN" || n_up == "UNIQUE" || n_up == "CHECK" || n_up == "CONSTRAINT" {
            continue;
        }
        if n == "_time" { continue; }
        let t_up = ty.to_ascii_lowercase();
        let key = if t_up.contains("char") || t_up.contains("text") || t_up.contains("json") || t_up.contains("bool") { "string".to_string() }
            else if t_up.contains("int") { "int64".to_string() }
            else if t_up.contains("double") || t_up.contains("real") || t_up.contains("float") || t_up.contains("numeric") || t_up.contains("decimal") { "float64".to_string() }
            else if t_up.contains("time") || t_up.contains("date") { "int64".to_string() }
            else { "string".to_string() };
        schema_entries.push((n, key));
    }
    // Create directory and schema.json
    let ident_norm = ident.trim().trim_matches('"');
    // If normalized earlier, we expect slashes; otherwise convert dots to slashes
    let db_path = if ident_norm.contains('/') { ident_norm.to_string() } else { ident_norm.replace('.', "/") };
    let root = store.root_path();
    let dir = std::path::Path::new(&root).join(db_path.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
    debug!(target: "clarium::exec", "do_create_table: dir='{}' (db_path='{}')", dir.display(), db_path);
    std::fs::create_dir_all(&dir).with_context(|| format!("create table dir {}", dir.display()))?;
    let mut map = serde_json::Map::new();
    for (k, t) in schema_entries.into_iter() { map.insert(k, serde_json::Value::String(t)); }
    // Add PRIMARY marker column if table has primary key constraint
    if has_primary_key {
        map.insert("PRIMARY".to_string(), serde_json::Value::String("marker".to_string()));
    }
    let sj = dir.join("schema.json");
    std::fs::write(&sj, serde_json::to_string_pretty(&serde_json::Value::Object(map))?)?;
    debug!(target: "clarium::exec", "do_create_table: wrote schema.json at '{}'", sj.display());
    Ok(())
}

fn split_col_def(s: &str) -> (String, String) {
    let mut parts = s.split_whitespace();
    let name = parts.next().unwrap_or("").to_string();
    let mut ty = String::new();
    for p in parts { if !ty.is_empty() { ty.push(' '); } ty.push_str(p); }
    (name, ty)
}

#[cfg(test)]
mod tests;



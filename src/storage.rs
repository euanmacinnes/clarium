//!
//! clarium storage module
//! -----------------------
//! This module implements the on-disk store for clarium using a simple three-level
//! directory layout: `database/schema/table.time`. Each table directory contains
//! one or more Parquet chunks named `data-<min>-<max>-<ts>.parquet` and a schema.json
//! that records the logical column types for all non-_time fields. The special
//! column `_time` is always stored as epoch milliseconds (i64) and is implicitly
//! present on every table.
//!
//! Key responsibilities:
//! - Record ingestion with schema inference and safe type widening.
//! - Incremental persistence to Parquet with basic statistics enabled.
//! - Lightweight schema management including optional per-column locks.
//! - Utilities for whole-table rewrite operations from a DataFrame.
//!
//! The public API centers around the `Store` type, which is usually wrapped in a
//! thread-safe `SharedStore` (`Arc<Mutex<Store>>`) elsewhere in the codebase.

use std::{fs, path::{Path, PathBuf}};
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use parking_lot::Mutex;
use std::sync::Arc;
use polars::prelude::*;
use polars::prelude::StatisticsOptions;
use tracing::debug;

/// Core on-disk storage handle for a clarium table directory tree.
///
/// Store exposes methods to create/delete logical databases (table directories),
/// append records with schema inference, and rewrite a table from a DataFrame.
/// It operates under a configured root folder and resolves logical paths like
/// "clarium/public/demo.time" into real directories.
#[derive(Clone)]
pub struct Store {
    /// Root folder for all databases/schemas/tables.
    root: PathBuf,
}

/// A single logical row to ingest into a clarium table.
///
/// Fields other than `_time` are flattened under `sensors` and may be numeric
/// or string. During ingestion, types are inferred per-column and may be widened
/// across batches (Int64 -> Float64 -> String) unless locked in the schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Event timestamp in epoch milliseconds. Determines sort order on disk.
    pub _time: i64,
    /// Arbitrary sensor/value map for non-time columns.
    #[serde(flatten)]
    pub sensors: serde_json::Map<String, serde_json::Value>,
}

impl Store {
    /// Read primary key columns from schema.json if present.
    pub fn get_primary_key(&self, table: &str) -> Option<Vec<String>> {
        let p = self.schema_path(table);
        if !p.exists() { return None; }
        if let Ok(text) = std::fs::read_to_string(&p) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(arr) = v.get("primaryKey").and_then(|x| x.as_array()) {
                    let cols: Vec<String> = arr.iter().filter_map(|e| e.as_str().map(|s| s.to_string())).collect();
                    if !cols.is_empty() { return Some(cols); }
                }
            }
        }
        None
    }

    /// Read partitions list from schema.json if present.
    pub fn get_partitions(&self, table: &str) -> Vec<String> {
        let p = self.schema_path(table);
        if !p.exists() { return Vec::new(); }
        if let Ok(text) = std::fs::read_to_string(&p) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(arr) = v.get("partitions").and_then(|x| x.as_array()) {
                    return arr.iter().filter_map(|e| e.as_str().map(|s| s.to_string())).collect();
                }
            }
        }
        Vec::new()
    }
    /// Create a new Store rooted at the given filesystem path.
    /// The directory is created if it does not already exist.
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).ok();
        Ok(Self { root })
    }

    /// Return the configured root folder for this Store.
    pub fn root_path(&self) -> &PathBuf { &self.root }

    /// Create an empty logical database (table directory) and initialize schema.json.
    ///
    /// The `table` parameter is a logical path like "clarium/public/mytable.time".
    pub fn create_table(&self, table: &str) -> Result<()> {
        let dir = self.db_dir(table);
        debug!(target: "clarium::storage", "create_table: begin table='{}' dir='{}'", table, dir.display());
        fs::create_dir_all(&dir)?;
        debug!(target: "clarium::storage", "create_table: directory ensured for table='{}'", table);
        // Initialize empty schema.json and set tableType for non-time tables
        use std::collections::{HashMap, HashSet};
        let schema: HashMap<String, DataType> = HashMap::new();
        let locks: HashSet<String> = HashSet::new();
        // Seed metadata if needed
        let schema_path = self.schema_path(table);
        debug!(target: "clarium::storage", "create_table: schema path='{}' exists={} table='{}'", schema_path.display(), schema_path.exists(), table);
        if !schema_path.exists() {
            let mut meta = serde_json::Map::new();
            if !table.ends_with(".time") { meta.insert("tableType".into(), serde_json::json!("regular")); }
            fs::write(&schema_path, serde_json::to_string_pretty(&serde_json::Value::Object(meta))?)?;
            debug!(target: "clarium::storage", "create_table: wrote initial schema.json for table='{}'", table);
        }
        self.save_schema_with_locks(table, &schema, &locks)?;
        debug!(target: "clarium::storage", "create_table: completed table='{}'", table);
        Ok(())
    }

    /// Delete a logical table (table directory) and all its files if it exists.
    pub fn delete_table(&self, table: &str) -> Result<()> {
        let dir = self.db_dir(table);
        debug!(target: "clarium::storage", "delete_table: deleting table='{}'", dir.display());
        if dir.exists() {
            fs::remove_dir_all(&dir).ok();
        }
        Ok(())
    }

    /// Rewrite the entire logical table from the provided DataFrame.
    ///
    /// Existing parquet chunk files are removed and a single new chunk is written.
    /// The schema is regenerated from the DataFrame (excluding `_time`) and existing
    /// per-column locks are preserved for surviving columns.
    pub fn rewrite_table_df(&self, table: &str, mut df: DataFrame) -> Result<()> {
        let __t0 = std::time::Instant::now();
        // Remove existing parquet files and legacy file, then write df as a single new chunk and update schema
        let dir = self.db_dir(table);
        fs::create_dir_all(&dir).ok();
        // delete existing chunk files
        let __t_scan_rm0 = std::time::Instant::now();
        if dir.exists() {
            for entry in fs::read_dir(&dir)? {
                let p = entry?.path();
                if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                    if (name.starts_with("data-") && name.ends_with(".parquet")) || name == "data.parquet" || name == "schema.json" {}
                }
            }
        }
        crate::tprintln!("[STORAGE] rewrite_table_df: pre-scan dir='{}' took={:?}", dir.display(), __t_scan_rm0.elapsed());

        debug!(target: "clarium::storage", "rewrite_table: rewriting table='{}'", dir.display());

        // Remove all parquet files
        let __t_rm = std::time::Instant::now();
        if dir.exists() {
            let mut to_remove: Vec<PathBuf> = Vec::new();
            for entry in fs::read_dir(&dir)? {
                let p = entry?.path();
                if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                    if (name.starts_with("data-") && name.ends_with(".parquet")) || name == "data.parquet" {
                        to_remove.push(p);
                    }
                }
            }
            for p in to_remove { let _ = fs::remove_file(&p); }
        }
        crate::tprintln!("[STORAGE] rewrite_table_df: removed old parquet files took={:?}", __t_rm.elapsed());
        // Update schema.json from df (excluding _time), preserving existing locks for remaining columns
        use std::collections::{HashMap, HashSet};
        let __t_schema = std::time::Instant::now();
        let (_, existing_locks) = self.load_schema_with_locks(table).unwrap_or((HashMap::new(), HashSet::new()));
        let mut schema: HashMap<String, DataType> = HashMap::new();
        for name in df.get_column_names() {
            let n = name.to_string();
            if n.as_str() == "_time" { continue; }
            let dt = df.column(n.as_str())?.dtype().clone();
            schema.insert(n.clone(), dt);
        }
        // Intersect locks with new schema columns
        let mut locks: HashSet<String> = HashSet::new();
        for k in existing_locks { if schema.contains_key(&k) { locks.insert(k); } }
        self.save_schema_with_locks(table, &schema, &locks)?;
        crate::tprintln!("[STORAGE] rewrite_table_df: update schema took={:?}", __t_schema.elapsed());
        // For regular tables (no .time suffix): if partitions are defined, write partitioned files.
        // Otherwise write a single data.parquet and return.
        if !table.ends_with(".time") {
            // Check for partitions in schema.json
            let sp = self.schema_path(table);
            let mut wrote_partitioned = false;
            if sp.exists() {
                if let Ok(text) = fs::read_to_string(&sp) {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                        if let Some(parts_arr) = v.get("partitions").and_then(|x| x.as_array()) {
                            let partitions: Vec<String> = parts_arr
                                .iter()
                                .filter_map(|e| e.as_str().map(|s| s.to_string()))
                                .collect();
                            if !partitions.is_empty() {
                                // Group rows by partition key tuple
                                use std::collections::HashMap as Map;
                                let mut groups: Map<String, Vec<usize>> = Map::new();
                                let n = df.height();
                                let __t_group = std::time::Instant::now();
                                let val_to_string = |av: AnyValue| -> String {
                                    match av {
                                        AnyValue::String(s) => s.to_string(),
                                        AnyValue::StringOwned(s) => s.to_string(),
                                        AnyValue::Int64(i) => i.to_string(),
                                        AnyValue::Float64(f) => {
                                            let mut s = format!("{}", f);
                                            if s.contains('.') { s = s.trim_end_matches('0').trim_end_matches('.').to_string(); }
                                            s
                                        }
                                        _ => av.to_string(),
                                    }
                                };
                                for i in 0..n {
                                    let mut key_parts: Vec<String> = Vec::with_capacity(partitions.len());
                                    for pcol in &partitions {
                                        let av = df.column(pcol.as_str()).ok().and_then(|s| s.get(i).ok());
                                        let sval = av.map(|a| val_to_string(a)).unwrap_or_else(|| "NULL".to_string());
                                        key_parts.push(format!("{}={}", pcol, sval));
                                    }
                                    let key = key_parts.join(",");
                                    groups.entry(key).or_default().push(i);
                                }
                                crate::tprintln!("[STORAGE] rewrite_table_df: partition grouping rows={} parts={} took={:?}", n, partitions.len(), __t_group.elapsed());
                                // Remove any existing parquet files before rewrite
                                let dir = self.db_dir(table);
                                let __t_rm2 = std::time::Instant::now();
                                if dir.exists() {
                                    for entry in fs::read_dir(&dir)? {
                                        let p = entry?.path();
                                        if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                                            if name.starts_with("data-") && name.ends_with(".parquet") { let _ = fs::remove_file(&p); }
                                            if name == "data.parquet" { let _ = fs::remove_file(&p); }
                                        }
                                    }
                                }
                                crate::tprintln!("[STORAGE] rewrite_table_df: remove pre-existing parquet before partition write took={:?}", __t_rm2.elapsed());
                                // Write one parquet per group
                                use polars::prelude::ParquetWriter;
                                use std::time::{SystemTime, UNIX_EPOCH};
                                fs::create_dir_all(&dir).ok();
                                let sanitize = |s: &str| -> String {
                                    let mut out = String::with_capacity(s.len());
                                    for ch in s.chars() {
                                        if ch.is_ascii_alphanumeric() { out.push(ch); }
                                        else if ch == '=' || ch == '-' || ch == '_' { out.push(ch); }
                                        else if ch == ',' { out.push('_'); } else { out.push('-'); }
                                    }
                                    out
                                };
                                let __t_write_parts = std::time::Instant::now();
                                let mut parts_written = 0usize;
                                for (k, idxs) in groups.into_iter() {
                                    let idx_vec: Vec<u32> = idxs.into_iter().map(|i| i as u32).collect();
                                    let idx_u = UInt32Chunked::from_vec("idx".into(), idx_vec);
                                    let mut part_df = df.take(&idx_u)?;
                                    let now_ms: u128 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
                                    let fname = format!("data-part-{}-{}.parquet", sanitize(&k), now_ms);
                                    let path = dir.join(fname);
                                    let mut file = std::fs::File::create(&path)?;
                                    ParquetWriter::new(&mut file)
                                        .with_statistics(StatisticsOptions::default())
                                        .finish(&mut part_df)?;
                                    parts_written += 1;
                                }
                                crate::tprintln!("[STORAGE] rewrite_table_df: wrote {} partition files took={:?}", parts_written, __t_write_parts.elapsed());
                                wrote_partitioned = true;
                            }
                        }
                    }
                }
            }
            if wrote_partitioned {
                crate::tprintln!("[STORAGE] rewrite_table_df: partitioned total took={:?}", __t0.elapsed());
                return Ok(());
            } else {
                let path = self.db_file(table);
                let __t_write = std::time::Instant::now();
                let mut file = std::fs::File::create(&path)?;
                ParquetWriter::new(&mut file)
                    .with_statistics(StatisticsOptions::default())
                    .finish(&mut df)?;
                crate::tprintln!("[STORAGE] rewrite_table_df: wrote single parquet rows={} took={:?} total={:?}", df.height(), __t_write.elapsed(), __t0.elapsed());
                return Ok(());
            }
        }
        // Ensure _time is i64 for time-series tables
        if df.column("_time").map(|s| s.dtype() != &DataType::Int64).unwrap_or(true)
            && df.get_column_names().iter().any(|c| c.as_str() == "_time") {
                let s = df.column("_time")?.cast(&DataType::Int64)?;
                if let Some(ser) = s.as_series() {
                    df.replace("_time", ser.clone())?;
                }
            }
        // Write one parquet chunk
        let min_t = df.column("_time")?.i64()?.min().unwrap_or(0);
        let max_t = df.column("_time")?.i64()?.max().unwrap_or(0);
        use std::time::{SystemTime, UNIX_EPOCH};
        let now_ms: u128 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let fname = format!("data-{}-{}-{}.parquet", min_t, max_t, now_ms);
        let path = dir.join(fname);
        let __t_write_ts = std::time::Instant::now();
        let mut file = std::fs::File::create(&path)?;
        ParquetWriter::new(&mut file)
            .with_statistics(StatisticsOptions::default())
            .finish(&mut df)?;
        crate::tprintln!("[STORAGE] rewrite_table_df: wrote time-table parquet rows={} took={:?} total={:?}", df.height(), __t_write_ts.elapsed(), __t0.elapsed());
        Ok(())
    }

    fn db_dir(&self, table: &str) -> PathBuf {
        // Delegate to central identifier module for consistent resolution
        // Detect if this is a time-series table and ensure `.time` suffix on the last segment
        let d = crate::ident::QueryDefaults::from_options(Some("clarium"), Some("public"));
        // Heuristic: if the identifier explicitly contains ".time" anywhere, treat as time table
        let is_time = table.contains(".time");
        let qualified = if is_time {
            crate::ident::qualify_time_ident(table, &d)
        } else {
            crate::ident::qualify_regular_ident(table, &d)
        };
        crate::ident::to_local_path(&self.root, &qualified)
    }

    fn db_file(&self, table: &str) -> PathBuf {
        self.db_dir(table).join("data.parquet")
    }

    /// Append a batch of records to the logical table, inferring/widening schema as needed
    /// and writing them as a new parquet chunk. Rows are sorted by `_time` before persisting.
    pub fn write_records(&self, table: &str, records: &[Record]) -> Result<()> {
        debug!(target: "clarium::storage", "write_records: begin table='{}' records={} dir='{}'", table, records.len(), self.db_dir(table).display());
        use std::collections::HashMap;
        use std::time::{SystemTime, UNIX_EPOCH};

        fs::create_dir_all(self.db_dir(table))?;

        // Build list of all columns seen in this batch
        let mut col_names: Vec<String> = Vec::new();
        for r in records {
            for k in r.sensors.keys() {
                if !col_names.iter().any(|s| s == k) {
                    col_names.push(k.clone());
                }
            }
        }
        col_names.sort();

        // Load existing schema (if any) and infer from incoming records
        let (mut schema, locks) = self.load_schema_with_locks(table).unwrap_or((std::collections::HashMap::new(), std::collections::HashSet::new()));
        let inferred = Self::infer_dtypes(records, &col_names);
        // Merge: Utf8 > Float64 > Int64 precedence; honor locks
        for (k, dt) in inferred {
            let merged = match schema.get(&k) {
                None => dt,
                Some(existing) => {
                    if locks.contains(&k) { existing.clone() } else { Self::merge_dtype(existing.clone(), dt) }
                }
            };
            schema.insert(k, merged);
        }
        let locks = locks; // keep for save

        // Prepare column buffers according to merged schema
        let mut times: Vec<i64> = Vec::with_capacity(records.len());
        let mut f64_cols: HashMap<String, Vec<Option<f64>>> = HashMap::new();
        let mut i64_cols: HashMap<String, Vec<Option<i64>>> = HashMap::new();
        let mut str_cols: HashMap<String, Vec<Option<String>>> = HashMap::new();
        for name in &col_names {
            match schema.get(name) {
                Some(DataType::String) => { str_cols.insert(name.clone(), Vec::with_capacity(records.len())); },
                Some(DataType::Int64) => { i64_cols.insert(name.clone(), Vec::with_capacity(records.len())); },
                _ => { f64_cols.insert(name.clone(), Vec::with_capacity(records.len())); },
            }
        }

        for r in records {
            times.push(r._time);
            for name in &col_names {
                match schema.get(name) {
                    Some(DataType::String) => {
                        let entry = str_cols.get_mut(name).unwrap();
                        let v = r.sensors.get(name).and_then(|val| match val {
                            serde_json::Value::String(s) => Some(s.clone()),
                            serde_json::Value::Number(n) => Some(n.to_string()),
                            _ => None,
                        });
                        entry.push(v);
                    }
                    Some(DataType::Int64) => {
                        let entry = i64_cols.get_mut(name).unwrap();
                        let v = r.sensors.get(name).and_then(|val| match val {
                            serde_json::Value::Number(n) => n.as_i64(),
                            serde_json::Value::String(s) => s.parse::<i64>().ok(),
                            _ => None,
                        });
                        entry.push(v);
                    }
                    _ => {
                        let entry = f64_cols.get_mut(name).unwrap();
                        let v = r.sensors.get(name).and_then(|val| match val {
                            serde_json::Value::Number(n) => n.as_f64(),
                            serde_json::Value::String(s) => s.parse::<f64>().ok(),
                            _ => None,
                        });
                        entry.push(v);
                    }
                }
            }
        }

        // Ensure rows are sorted by _time before persisting
        let mut idx: Vec<usize> = (0..times.len()).collect();
        idx.sort_by(|a, b| times[*a].cmp(&times[*b]));
        // reorder helper
        let reorder_vec_i64 = |v: &mut Vec<Option<i64>>, idx: &Vec<usize>| {
            let mut out: Vec<Option<i64>> = Vec::with_capacity(v.len());
            for &i in idx { out.push(v[i]); }
            *v = out;
        };
        let reorder_vec_f64 = |v: &mut Vec<Option<f64>>, idx: &Vec<usize>| {
            let mut out: Vec<Option<f64>> = Vec::with_capacity(v.len());
            for &i in idx { out.push(v[i]); }
            *v = out;
        };
        let reorder_vec_str = |v: &mut Vec<Option<String>>, idx: &Vec<usize>| {
            let mut out: Vec<Option<String>> = Vec::with_capacity(v.len());
            for &i in idx { out.push(v[i].clone()); }
            *v = out;
        };
        // apply reorder to all columns
        let mut times_sorted: Vec<i64> = Vec::with_capacity(times.len());
        for &i in &idx { times_sorted.push(times[i]); }
        times = times_sorted;
        let mut i64_cols = i64_cols; // make mutable shadow
        for (_name, vals) in i64_cols.iter_mut() { reorder_vec_i64(vals, &idx); }
        let mut f64_cols = f64_cols;
        for (_name, vals) in f64_cols.iter_mut() { reorder_vec_f64(vals, &idx); }
        let mut str_cols = str_cols;
        for (_name, vals) in str_cols.iter_mut() { reorder_vec_str(vals, &idx); }

        // Build DataFrame for this batch (sorted by _time)
        let mut s_time = Series::new("_time".into(), times);
        s_time = s_time.cast(&DataType::Int64)?; // ensure i64
        let mut df = DataFrame::new(vec![s_time.into()])?;
        for (name, vals) in i64_cols.into_iter() { df.with_column(Series::new(name.into(), vals))?; }
        for (name, vals) in f64_cols.into_iter() { df.with_column(Series::new(name.into(), vals))?; }
        for (name, vals) in str_cols.into_iter() { df.with_column(Series::new(name.into(), vals))?; }

        // Align to full schema: add any missing known columns as nulls and cast dtypes
        let mut all_cols: Vec<String> = schema.keys().cloned().collect();
        all_cols.sort();
        for (name, dt) in schema.iter() {
            if name == "_time" { continue; }
            if !df.get_column_names().iter().any(|c| c.as_str() == name) {
                // add null column; avoid nested Vec<Option<Vec<_>>> constructions that are not supported uniformly across Polars
                // Prefer nullable Utf8 for most types except Int64 where we can use nullable i64
                let s = match dt {
                    DataType::Int64 => Series::new(name.clone().into(), vec![Option::<i64>::None; df.height()]),
                    _ => Series::new(name.clone().into(), vec![Option::<String>::None; df.height()]),
                };
                df.with_column(s)?;
            } else {
                // cast to expected dtype
                let s = df.column(name)?.cast(dt)?;
                if let Some(ser) = s.as_series() {
                    df.replace(name, ser.clone())?;
                }
            }
        }

        // Persist new schema, preserving locks
        self.save_schema_with_locks(table, &schema, &locks)?;

        // Partition-aware write for regular (non-.time) tables when schema.json has partitions metadata
        if !table.ends_with(".time") {
            // Try to read partitions list from schema.json
            let sp = self.schema_path(table);
            if sp.exists() {
                if let Ok(text) = fs::read_to_string(&sp) {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                        if let Some(parts_arr) = v.get("partitions").and_then(|x| x.as_array()) {
                            let partitions: Vec<String> = parts_arr.iter().filter_map(|e| e.as_str().map(|s| s.to_string())).collect();
                            if !partitions.is_empty() {
                                // Group rows by partition key tuple
                                use std::collections::HashMap as Map;
                                let mut groups: Map<String, Vec<usize>> = Map::new();
                                let n = df.height();
                                // helper to stringify AnyValue
                                let val_to_string = |av: AnyValue| -> String {
                                    match av {
                                        AnyValue::String(s) => s.to_string(),
                                        AnyValue::StringOwned(s) => s.to_string(),
                                        AnyValue::Int64(i) => i.to_string(),
                                        AnyValue::Float64(f) => {
                                            // stable float formatting
                                            let mut s = format!("{}", f);
                                            if s.contains('.') { s = s.trim_end_matches('0').trim_end_matches('.').to_string(); }
                                            s
                                        }
                                        _ => av.to_string(),
                                    }
                                };
                                // Build keys
                                for i in 0..n {
                                    let mut key_parts: Vec<String> = Vec::with_capacity(partitions.len());
                                    for pcol in &partitions {
                                        let av = df.column(pcol.as_str()).ok().and_then(|s| s.get(i).ok());
                                        let sval = av.map(|a| val_to_string(a)).unwrap_or_else(|| "NULL".to_string());
                                        key_parts.push(format!("{}={}", pcol, sval));
                                    }
                                    let key = key_parts.join(",");
                                    groups.entry(key).or_default().push(i);
                                }
                                // Write one parquet per group
                                let dir = self.db_dir(table);
                                
                                
                                use polars::prelude::ParquetWriter;
                                use std::time::{SystemTime, UNIX_EPOCH};
                                fs::create_dir_all(&dir).ok();
                                let sanitize = |s: &str| -> String {
                                    let mut out = String::with_capacity(s.len());
                                    for ch in s.chars() {
                                        if ch.is_ascii_alphanumeric() { out.push(ch); }
                                        else if ch == '=' || ch == '-' || ch == '_' { out.push(ch); }
                                        else if ch == ',' { out.push('_'); } else { out.push('-'); }
                                    }
                                    out
                                };
                                for (k, idxs) in groups.into_iter() {
                                    let idx_vec: Vec<u32> = idxs.into_iter().map(|i| i as u32).collect();
                                    let idx_u = UInt32Chunked::from_vec("idx".into(), idx_vec);
                                    let mut part_df = df.take(&idx_u)?;
                                    // Write file with partition key in name
                                    let now_ms: u128 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
                                    let fname = format!("data-part-{}-{}.parquet", sanitize(&k), now_ms);
                                    let path = dir.join(fname);
                                    let mut file = std::fs::File::create(&path)?;
                                    ParquetWriter::new(&mut file)
                                        .with_statistics(StatisticsOptions::default())
                                        .finish(&mut part_df)?;
                                }
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }

        // Determine overlap with existing chunk files and handle updates if necessary
        let new_min_t = df.column("_time")?.i64()?.min().unwrap_or(0);
        let new_max_t = df.column("_time")?.i64()?.max().unwrap_or(0);

        let dir = self.db_dir(table);
        let mut overlapping: Vec<PathBuf> = Vec::new();
        if dir.exists() {
            for entry in fs::read_dir(&dir)? {
                let p = entry?.path();
                if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                    if name.starts_with("data-") && name.ends_with(".parquet") {
                        // parse min and max from filename: data-<min>-<max>-<ts>.parquet
                        let parts: Vec<&str> = name.trim_start_matches("data-").trim_end_matches(".parquet").split('-').collect();
                        if parts.len() >= 3 {
                            if let (Ok(min_v), Ok(max_v)) = (parts[0].parse::<i64>(), parts[1].parse::<i64>()) {
                                // overlap if [min_v, max_v] intersects [new_min_t, new_max_t]
                                if !(max_v < new_min_t || min_v > new_max_t) {
                                    overlapping.push(p);
                                }
                            }
                        }
                    }
                }
            }
        }

        if overlapping.is_empty() {
            // No overlap; append as new chunk
            let now_ms: u128 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
            let fname = format!("data-{}-{}-{}.parquet", new_min_t, new_max_t, now_ms);
            let path = dir.join(fname);
            let mut file = std::fs::File::create(&path)?;
            ParquetWriter::new(&mut file)
                .with_statistics(StatisticsOptions::default())
                .finish(&mut df)?;
            return Ok(());
        }

        // There are overlapping chunks. Read them, merge with new df, favoring new rows on duplicate _time.
        let mut old_df_opt: Option<DataFrame> = None;
        for p in &overlapping {
            let file = std::fs::File::open(p)?;
            let df_old = ParquetReader::new(file).finish()?;
            match &mut old_df_opt {
                None => { old_df_opt = Some(df_old); },
                Some(acc) => { acc.vstack_mut(&df_old)?; },
            }
        }
        let mut merged = if let Some(old_df) = old_df_opt {
            // Remove rows from old_df that have _time present in new df, then stack new on top.
            let new_times_vec = df.column("_time")?.i64()?.into_no_null_iter().collect::<Vec<i64>>();
            let new_times_set: std::collections::HashSet<i64> = new_times_vec.into_iter().collect();
            let times_ca = old_df.column("_time")?.i64()?;
            let mask_keep: Vec<bool> = times_ca
                .into_iter()
                .map(|opt| opt.map(|v| !new_times_set.contains(&v)).unwrap_or(true))
                .collect();
            let mask_series = Series::new("keep".into(), mask_keep);
            let old_filtered = old_df.filter(mask_series.bool()?)?;
            // Align columns by name before vstack to avoid order/name mismatches
            let acc_cols = old_filtered.get_column_names();
            let df_cols = df.get_column_names();
            if acc_cols == df_cols {
                old_filtered.vstack(&df)?
            } else {
                // If sets are equal but order differs, reorder df to match acc
                let acc_set: std::collections::HashSet<&str> = acc_cols.iter().map(|s| s.as_str()).collect();
                let df_set: std::collections::HashSet<&str> = df_cols.iter().map(|s| s.as_str()).collect();
                if acc_set == df_set {
                    let mut reordered: Vec<Column> = Vec::with_capacity(acc_cols.len());
                    for name in acc_cols {
                        let s = df.column(name)?.clone();
                        reordered.push(s.into());
                    }
                    let df2 = DataFrame::new(reordered)?;
                    old_filtered.vstack(&df2)?
                } else {
                    // Fallback: select common columns in accumulator order
                    let mut common: Vec<Column> = Vec::new();
                    for name in old_filtered.get_column_names() {
                        if let Ok(s) = df.column(name) { common.push(s.clone().into()); }
                    }
                    let df2 = DataFrame::new(common)?;
                    old_filtered.vstack(&df2)?
                }
            }
        } else {
            df.clone()
        };

        // Remove the old overlapping chunk files
        for p in overlapping { let _ = fs::remove_file(p); }

        // Write the merged replacement chunk
        let min_t = merged.column("_time")?.i64()?.min().unwrap_or(new_min_t);
        let max_t = merged.column("_time")?.i64()?.max().unwrap_or(new_max_t);
        let now_ms: u128 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let fname = format!("data-{}-{}-{}.parquet", min_t, max_t, now_ms);
        let path = dir.join(fname);
        let mut file = std::fs::File::create(&path)?;
        ParquetWriter::new(&mut file)
            .with_statistics(StatisticsOptions::default())
            .finish(&mut merged)?;
        Ok(())
    }

    pub fn read_df(&self, table: &str) -> Result<DataFrame> {
        // Read legacy single file if present and no chunk files
        let dir = self.db_dir(table);
        let legacy = self.db_file(table);
        let mut chunk_files: Vec<PathBuf> = Vec::new();
        if dir.exists() {
            for entry in fs::read_dir(&dir)? {
                let p = entry?.path();
                if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                    if name.starts_with("data-") && name.ends_with(".parquet") {
                        chunk_files.push(p);
                    }
                }
            }
        }
        if chunk_files.is_empty() && legacy.exists() {
            let file = std::fs::File::open(&legacy)
                .with_context(|| format!("Table not found: {}", legacy.display()))?;
            let df = ParquetReader::new(file).finish()?;
            if df.width() == 0 {
                anyhow::bail!(format!("Table is empty or unreadable: {}", legacy.display()));
            }
            return Ok(df);
        }
        // Otherwise, read all chunk files and vstack
        chunk_files.sort_by_key(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .and_then(|name| name.strip_prefix("data-") )
                .and_then(|rest| rest.split('-').next())
                .and_then(|min_str| min_str.parse::<i64>().ok())
                .unwrap_or(i64::MIN)
        });
        let mut out: Option<DataFrame> = None;
        for f in chunk_files {
            let file = std::fs::File::open(&f)?;
            let df = ParquetReader::new(file).finish()?;
            match &mut out {
                None => out = Some(df),
                Some(acc) => {
                    // Ensure column order matches the accumulator to avoid vstack name mismatches
                    let acc_cols = acc.get_column_names();
                    let df_cols = df.get_column_names();
                    if acc_cols == df_cols {
                        acc.vstack_mut(&df)?;
                    } else {
                        // If sets are equal but order differs, reorder df to match acc
                        let acc_set: std::collections::HashSet<&str> = acc_cols.iter().map(|s| s.as_str()).collect();
                        let df_set: std::collections::HashSet<&str> = df_cols.iter().map(|s| s.as_str()).collect();
                        if acc_set == df_set {
                            let mut reordered: Vec<Column> = Vec::with_capacity(acc_cols.len());
                            for name in acc_cols {
                                let s = df.column(name)?.clone();
                                reordered.push(s.into());
                            }
                            let df2 = DataFrame::new(reordered)?;
                            acc.vstack_mut(&df2)?;
                        } else {
                            // Fallback: attempt to select common columns in acc order
                            let mut common: Vec<Column> = Vec::new();
                            for name in acc_cols {
                                if let Ok(s) = df.column(name) { common.push(s.clone().into()); }
                            }
                            let df2 = DataFrame::new(common)?;
                            acc.vstack_mut(&df2)?;
                        }
                    }
                }
            }
        }
        match out {
            None => Err(anyhow::anyhow!("Table not found: {}", dir.display())),
            Some(df) => {
                if df.width() == 0 {
                    Err(anyhow::anyhow!(format!("Table is empty or unreadable: {}", dir.display())))
                } else { Ok(df) }
            }
        }
    }

    pub fn filter_df(
        &self,
        table: &str,
        cols: &[String],
        min_time: Option<i64>,
        max_time: Option<i64>,
    ) -> Result<DataFrame> {
        // Build expected schema from schema.json if present (for alignment)
        let mut expected = self.load_schema(table).unwrap_or_default();
        expected.insert("_time".into(), DataType::Int64);

        // Gather files (both legacy single file and chunk files)
        let dir = self.db_dir(table);
        let legacy = self.db_file(table);
        let mut files: Vec<PathBuf> = Vec::new();
        if dir.exists() {
            for entry in fs::read_dir(&dir)? {
                let p = entry?.path();
                if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                    if name.starts_with("data-") && name.ends_with(".parquet") {
                        files.push(p);
                    }
                }
            }
        }
        if files.is_empty() && legacy.exists() { files.push(legacy); }
        files.sort_by_key(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .and_then(|name| name.strip_prefix("data-") )
                .and_then(|rest| rest.split('-').next())
                .and_then(|min_str| min_str.parse::<i64>().ok())
                .unwrap_or(i64::MIN)
        });

        let mut acc: Option<DataFrame> = None;
        for f in files {
            let file = std::fs::File::open(&f)?;
            let mut df = ParquetReader::new(file).finish()?;

            // Time filter per chunk
            if let Some(min_t) = min_time { let mask = df.column("_time")?.i64()?.gt_eq(min_t); df = df.filter(&mask)?; }
            if let Some(max_t) = max_time { let mask = df.column("_time")?.i64()?.lt_eq(max_t); df = df.filter(&mask)?; }
            if df.height() == 0 { continue; }

            // Align to expected schema (if schema.json missing, derive from first df and update expected)
            if expected.is_empty() {
                for name in df.get_column_names() {
                    if name.as_str() == "_time" { continue; }
                    let dt = df.column(name.as_str())?.dtype().clone();
                    expected.insert(name.to_string(), dt);
                }
            }
            for (name, dt) in expected.clone() { // clone to iterate
                if name == "_time" { continue; }
                if !df.get_column_names().iter().any(|c| c.as_str() == name) {
                    // add null column (prefer Utf8 for portability; use Int64 where appropriate)
                    let s = match dt {
                        DataType::Int64 => Series::new(name.clone().into(), vec![Option::<i64>::None; df.height()]),
                        _ => Series::new(name.clone().into(), vec![Option::<String>::None; df.height()]),
                    };
                    df.with_column(s)?;
                } else {
                    let s = df.column(&name)?.cast(&dt)?;
                    if let Some(ser) = s.as_series() {
                        df.replace(&name, ser.clone())?;
                    }
                }
            }

            // Select columns if requested
            if !cols.is_empty() {
                let mut select_cols: Vec<&str> = Vec::new();
                for c in cols {
                    if c == "_time" || df.get_column_names().iter().any(|n| n.as_str() == c) {
                        select_cols.push(c);
                    }
                }
                if !select_cols.is_empty() { df = df.select(select_cols)?; }
            }

            match &mut acc {
                None => acc = Some(df),
                Some(a) => { a.vstack_mut(&df)?; }
            }
        }
        acc.ok_or_else(|| anyhow::anyhow!("Table not found: {}", dir.display()))
    }

    // --- Schema management helpers ---
    fn schema_path(&self, table: &str) -> PathBuf { self.db_dir(table).join("schema.json") }

    pub(crate) fn load_schema_with_locks(&self, table: &str) -> Result<(std::collections::HashMap<String, DataType>, std::collections::HashSet<String>)> {
        use std::collections::{HashMap, HashSet};
        let p = self.schema_path(table);
        if !p.exists() { return Ok((HashMap::new(), HashSet::new())); }
        let s = fs::read_to_string(p)?;
        let json: serde_json::Value = serde_json::from_str(&s)?;
        let mut map: HashMap<String, DataType> = HashMap::new();
        let mut locks: HashSet<String> = HashSet::new();
        if let serde_json::Value::Object(obj) = json {
            for (k, v) in obj {
                if let serde_json::Value::String(t) = v {
                    map.insert(k, Self::str_to_dtype(&t));
                } else if let serde_json::Value::Object(m) = v {
                    let t = m.get("type").and_then(|x| x.as_str()).unwrap_or("");
                    let locked = m.get("locked").and_then(|x| x.as_bool()).unwrap_or(false);
                    if !t.is_empty() { map.insert(k.clone(), Self::str_to_dtype(t)); }
                    if locked { locks.insert(k.clone()); }
                }
            }
        }
        Ok((map, locks))
    }

    fn save_schema_with_locks(
        &self,
        table: &str,
        schema: &std::collections::HashMap<String, DataType>,
        locks: &std::collections::HashSet<String>,
    ) -> Result<()> {
        use std::collections::HashMap;
        let mut m: HashMap<String, serde_json::Value> = HashMap::new();
        for (k, v) in schema.iter() {
            if k == "_time" { continue; }
            let ty = Self::dtype_to_str(v);
            if locks.contains(k) {
                m.insert(k.clone(), serde_json::json!({"type": ty, "locked": true}));
            } else {
                m.insert(k.clone(), serde_json::json!(ty));
            }
        }
        // Preserve known metadata fields from existing schema.json
        let p = self.schema_path(table);
        if p.exists() {
            if let Ok(text) = fs::read_to_string(&p) {
                if let Ok(existing) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(obj) = existing.as_object() {
                        for key in ["primaryKey", "partitions", "tableType"].iter() {
                            if let Some(v) = obj.get(*key) { m.insert((*key).to_string(), v.clone()); }
                        }
                    }
                }
            }
        }
        fs::write(p, serde_json::to_string_pretty(&m)?)?;
        Ok(())
    }

    fn load_schema(&self, table: &str) -> Result<std::collections::HashMap<String, DataType>> {
        Ok(self.load_schema_with_locks(table)?.0)
    }

    fn save_schema(&self, table: &str, schema: &std::collections::HashMap<String, DataType>) -> Result<()> {        
        // Preserve existing locks if any
        let (_, locks) = self.load_schema_with_locks(table).unwrap_or((std::collections::HashMap::new(), std::collections::HashSet::new()));
        self.save_schema_with_locks(table, schema, &locks)
    }

    pub(crate) fn dtype_to_str(dt: &DataType) -> String {
        match dt {
            DataType::String => "string".into(),
            DataType::Int64 => "int64".into(),
            // Treat List(Float64) as our logical 'vector' type for schema purposes
            DataType::List(inner) => {
                if matches!(**inner, DataType::Float64) || matches!(**inner, DataType::Int64) {
                    "vector".into()
                } else {
                    // default label for other lists
                    "list".into()
                }
            }
            _ => "float64".into(),
        }
    }
    fn str_to_dtype(s: &str) -> DataType {
        match s.to_ascii_lowercase().as_str() {
            "utf8" | "string" => DataType::String,
            "int64" => DataType::Int64,
            // Map logical 'vector' to List(Float64)
            "vector" => DataType::List(Box::new(DataType::Float64)),
            _ => DataType::Float64,
        }
    }

    fn merge_dtype(a: DataType, b: DataType) -> DataType {
        use DataType::*;
        match (a, b) {
            (String, _) | (_, String) => String,
            // Do not implicitly widen to/from vectors. If any side is List, keep List if other side is numeric; else fall back to String.
            (List(a), List(b)) => {
                if *a == *b { List(a) } else { String }
            }
            (List(a), Float64) | (Float64, List(a)) => List(a),
            (List(a), Int64) | (Int64, List(a)) => List(a),
            (Float64, _) | (_, Float64) => Float64,
            _ => Int64,
        }
    }

    fn infer_dtypes(records: &[Record], names: &[String]) -> std::collections::HashMap<String, DataType> {
        use std::collections::HashMap;
        let mut map: HashMap<String, DataType> = HashMap::new();
        for name in names {
            let mut any_string_label = false;
            let mut any_float = false;
            let mut saw_value = false;
            for r in records {
                if let Some(val) = r.sensors.get(name) {
                    match val {
                        serde_json::Value::String(s) => {
                            if s.parse::<i64>().is_ok() { saw_value = true; }
                            else if s.parse::<f64>().is_ok() { any_float = true; saw_value = true; }
                            else { any_string_label = true; break; }
                        }
                        serde_json::Value::Number(n) => {
                            if n.as_i64().is_some() { saw_value = true; } else { any_float = true; saw_value = true; }
                        }
                        _ => {}
                    }
                }
            }
            let dt = if any_string_label { DataType::String } else if any_float { DataType::Float64 } else if saw_value { DataType::Int64 } else { DataType::Float64 };
            map.insert(name.clone(), dt);
        }
        map
    }
    pub fn schema_add(&self, table: &str, entries: &[(String, DataType)]) -> Result<()> {
        use std::collections::{HashMap, HashSet};
        fs::create_dir_all(self.db_dir(table))?;
        let (mut schema, mut locks) = self.load_schema_with_locks(table).unwrap_or((HashMap::new(), HashSet::new()));
        for (name, dt) in entries {
            schema.insert(name.clone(), dt.clone());
            locks.insert(name.clone());
        }
        self.save_schema_with_locks(table, &schema, &locks)
    }
}

#[derive(Clone)]
pub struct SharedStore(pub Arc<Mutex<Store>>);

// ------------------------
// In-memory KV stores API
// ------------------------
use std::time::{Duration, Instant};
use std::collections::HashMap as StdHashMap;
use std::sync::OnceLock;
use serde_json::Value as JsonValue;

/// Value variants supported by the in-memory KV store.
/// Note: Parquet values are kept in-memory as Polars DataFrame.
#[derive(Clone)]
pub enum KvValue {
    Str(String),
    Int(i64),
    Json(JsonValue),
    ParquetDf(DataFrame),
    /// Raw binary value intended for high-performance blobs (e.g., Lua bytecode)
    Bytes(Vec<u8>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StoreSettings {
    /// Arbitrary settings for the store. Extendable for future features (e.g. replication).
    pub name: String,
    /// If true, a GET will reset the TTL for keys that were inserted with a TTL.
    pub reset_on_access_default: bool,
    /// Placeholder for future replication options
    #[serde(default)]
    pub replication: Option<serde_json::Value>,
    /// Optional persistence settings loaded from `<store dir>/store.json`.
    #[serde(default)]
    pub persistence: Option<PersistenceSettings>,
}

impl Default for StoreSettings {
    fn default() -> Self {
        Self { name: String::new(), reset_on_access_default: true, replication: None, persistence: Some(PersistenceSettings::default()) }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PersistenceSettings {
    /// Enable periodic snapshotting of this KV store to disk
    #[serde(default)]
    pub enabled: bool,
    /// Interval in milliseconds between snapshots
    #[serde(default = "PersistenceSettings::default_interval_ms")]
    pub interval_ms: u64,
    /// Snapshot format: currently supports "bincode" (fast, compact)
    #[serde(default = "PersistenceSettings::default_format")]
    pub format: String,
}

impl PersistenceSettings {
    fn default_interval_ms() -> u64 { 5_000 }
    fn default_format() -> String { "bincode".to_string() }
}

impl Default for PersistenceSettings {
    fn default() -> Self {
        Self { enabled: false, interval_ms: Self::default_interval_ms(), format: Self::default_format() }
    }
}

#[derive(Clone)]
struct Entry {
    value: KvValue,
    /// Optional original TTL for resets
    ttl: Option<Duration>,
    /// Optional expiry time
    expires_at: Option<Instant>,
    /// If true for this key, accesses reset TTL
    reset_on_access: bool,
}

/// A single named in-memory KV store.
#[derive(Clone)]
pub struct KvStore {
    settings: StoreSettings,
    dir: PathBuf,
    map: Arc<parking_lot::RwLock<StdHashMap<String, Entry>>>,
    /// Guard to ensure we only spawn one persistence thread
    persist_started: Arc<parking_lot::Mutex<bool>>,
}

impl KvStore {
    fn new(dir: PathBuf, settings: StoreSettings) -> Self {
        fs::create_dir_all(&dir).ok();
        let s = Self { settings, dir, map: Arc::new(parking_lot::RwLock::new(StdHashMap::new())), persist_started: Arc::new(parking_lot::Mutex::new(false)) };
        // Start persistence loop if enabled
        s.ensure_persistence_loop();
        s
    }

    fn config_path(&self) -> PathBuf { self.dir.join("store.json") }
    fn legacy_config_path(&self) -> PathBuf { self.dir.join("config.json") }
    fn snapshot_path(&self) -> PathBuf { self.dir.join("snapshot.bin") }
    fn parquet_dir(&self) -> PathBuf { self.dir.join("parquet") }

    pub fn load_or_default(dir: PathBuf, name: &str) -> Self {
        let cfg_new = dir.join("store.json");
        let cfg_legacy = dir.join("config.json");
        let mut settings = StoreSettings::default();
        settings.name = name.to_string();
        if let Ok(bytes) = fs::read(&cfg_new) {
            if let Ok(s) = serde_json::from_slice::<StoreSettings>(&bytes) { settings = s; }
        } else if let Ok(bytes) = fs::read(&cfg_legacy) {
            if let Ok(s) = serde_json::from_slice::<StoreSettings>(&bytes) { settings = s; }
            // migrate on next save
        } else {
            // write default to new path
            let _ = fs::write(&cfg_new, serde_json::to_vec_pretty(&settings).unwrap_or_default());
        }
        let kv = Self::new(dir, settings);
        // Attempt to restore snapshot on startup
        let _ = kv.load_snapshot();
        kv
    }

    pub fn save_settings(&self) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(&self.settings)?;
        fs::write(self.config_path(), bytes)?;
        Ok(())
    }

    fn ensure_persistence_loop(&self) {
        let enabled = self.settings.persistence.as_ref().map(|p| p.enabled).unwrap_or(false);
        if !enabled { return; }
        let mut started = self.persist_started.lock();
        if *started { return; }
        *started = true;
        let interval_ms = self.settings.persistence.as_ref().map(|p| p.interval_ms).unwrap_or(5_000);
        let this = self.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(interval_ms));
                if let Err(e) = this.save_snapshot() {
                    // Best-effort: log and continue
                    crate::tprintln!("KV persistence save_snapshot error for store '{}': {}", this.settings.name, e);
                }
            }
        });
    }

    fn sanitize_key_for_file(key: &str) -> String {
        let mut out = String::with_capacity(key.len());
        for ch in key.chars() {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' { out.push(ch); } else { out.push('_'); }
        }
        out
    }

    /// Persist current in-memory map to disk using a fast binary snapshot.
    pub fn save_snapshot(&self) -> anyhow::Result<()> {
        use std::io::Write;
        fs::create_dir_all(&self.dir).ok();
        fs::create_dir_all(self.parquet_dir()).ok();
        let now = Instant::now();
        #[derive(Serialize, Deserialize)]
        enum SnapVal {
            Str(String),
            Int(i64),
            Json(Vec<u8>),
            Bytes(Vec<u8>),
            Parquet { rel_path: String },
        }
        #[derive(Serialize, Deserialize)]
        struct SnapEntry { key: String, val: SnapVal, ttl_ms: Option<u64>, remaining_ms: Option<u64>, reset_on_access: bool }
        #[derive(Serialize, Deserialize)]
        struct Snapshot { version: u32, created_ms: i64, entries: Vec<SnapEntry> }

        let mut entries: Vec<SnapEntry> = Vec::new();
        {
            let r = self.map.read();
            for (k, ent) in r.iter() {
                let val = match &ent.value {
                    KvValue::Str(s) => SnapVal::Str(s.clone()),
                    KvValue::Int(i) => SnapVal::Int(*i),
                    KvValue::Json(j) => {
                        let bytes = serde_json::to_vec(j).unwrap_or_default();
                        SnapVal::Json(bytes)
                    }
                    KvValue::Bytes(b) => SnapVal::Bytes(b.clone()),
                    KvValue::ParquetDf(df) => {
                        // Write external parquet file
                        let fname = format!("{}.parquet", Self::sanitize_key_for_file(k));
                        let path = self.parquet_dir().join(&fname);
                        let mut f = std::fs::File::create(&path)?;
                        let mut dfc = df.clone();
                        polars::prelude::ParquetWriter::new(&mut f).finish(&mut dfc).map_err(|e| anyhow::anyhow!(e.to_string()))?;
                        let rel = PathBuf::from("parquet").join(fname).to_string_lossy().to_string();
                        SnapVal::Parquet { rel_path: rel }
                    }
                };
                let ttl_ms = ent.ttl.map(|d| d.as_millis() as u64);
                let remaining_ms = ent.expires_at.and_then(|x| if x > now { Some((x - now).as_millis() as u64) } else { Some(0) });
                entries.push(SnapEntry { key: k.clone(), val, ttl_ms, remaining_ms, reset_on_access: ent.reset_on_access });
            }
        }
        let snapshot = Snapshot { version: 1, created_ms: chrono::Utc::now().timestamp_millis(), entries };
        let bytes = bincode::serialize(&snapshot)?;
        let tmp = self.snapshot_path().with_extension("tmp");
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all().ok();
        std::fs::rename(tmp, self.snapshot_path())?;
        Ok(())
    }

    /// Load snapshot from disk into memory; ignores errors to allow startup.
    pub fn load_snapshot(&self) -> anyhow::Result<()> {
        if !self.snapshot_path().exists() { return Ok(()); }
        #[derive(Serialize, Deserialize)]
        enum SnapVal { Str(String), Int(i64), Json(Vec<u8>), Bytes(Vec<u8>), Parquet { rel_path: String } }
        #[derive(Serialize, Deserialize)]
        struct SnapEntry { key: String, val: SnapVal, ttl_ms: Option<u64>, remaining_ms: Option<u64>, reset_on_access: bool }
        #[derive(Serialize, Deserialize)]
        struct Snapshot { version: u32, created_ms: i64, entries: Vec<SnapEntry> }
        let bytes = std::fs::read(self.snapshot_path())?;
        let snap: Snapshot = bincode::deserialize(&bytes)?;
        let now = Instant::now();
        let mut w = self.map.write();
        w.clear();
        for e in snap.entries.into_iter() {
            let kv = match e.val {
                SnapVal::Str(s) => KvValue::Str(s),
                SnapVal::Int(i) => KvValue::Int(i),
                SnapVal::Json(b) => {
                    let j: JsonValue = serde_json::from_slice(&b).unwrap_or(JsonValue::Null);
                    KvValue::Json(j)
                }
                SnapVal::Bytes(b) => KvValue::Bytes(b),
                SnapVal::Parquet { rel_path } => {
                    let p = self.dir.join(rel_path);
                    match polars::prelude::ParquetReader::new(std::fs::File::open(&p)?).finish() {
                        Ok(df) => KvValue::ParquetDf(df),
                        Err(_) => KvValue::Bytes(Vec::new()),
                    }
                }
            };
            let ttl = e.ttl_ms.map(|ms| Duration::from_millis(ms));
            let expires_at = match (ttl, e.remaining_ms) {
                (Some(_), Some(rem)) if rem > 0 => Some(now + Duration::from_millis(rem)),
                (Some(d), _) => Some(now + d), // fallback if missing remaining
                _ => None,
            };
            w.insert(e.key, Entry { value: kv, ttl, expires_at, reset_on_access: e.reset_on_access });
        }
        Ok(())
    }

    /// Set a key with optional TTL and per-key reset-on-access flag (defaults from store settings).
    pub fn set(&self, key: impl Into<String>, value: KvValue, ttl: Option<Duration>, reset_on_access: Option<bool>) {
        let key = key.into();
        let now = Instant::now();
        let reset = reset_on_access.unwrap_or(self.settings.reset_on_access_default);
        let expires_at = ttl.map(|d| now + d);
        let ent = Entry { value, ttl, expires_at, reset_on_access: reset };
        let mut w = self.map.write();
        w.insert(key, ent);
    }

    /// Convenience: store raw bytes without extra allocations by cloning once into the map.
    pub fn set_bytes(&self, key: impl Into<String>, bytes: &[u8], ttl: Option<Duration>, reset_on_access: Option<bool>) {
        self.set(key, KvValue::Bytes(bytes.to_vec()), ttl, reset_on_access);
    }

    /// Get a key. If expired, removes it and returns None. If reset_on_access, bumps expiry.
    pub fn get(&self, key: &str) -> Option<KvValue> {
        // First prune single key quickly
        let mut to_reset: Option<(String, Instant)> = None;
        {
            let r = self.map.read();
            if let Some(ent) = r.get(key) {
                if let Some(exp) = ent.expires_at {
                    if Instant::now() >= exp { /* expired */ }
                    else if ent.reset_on_access {
                        if let Some(ttl) = ent.ttl { to_reset = Some((key.to_string(), Instant::now() + ttl)); }
                    }
                }
            } else {
                return None;
            }
        }
        // Apply reset-on-access before checking expiry to avoid flakiness around the boundary
        let mut w = self.map.write();
        if let Some((k, new_exp)) = to_reset {
            if let Some(ent_mut) = w.get_mut(&k) { ent_mut.expires_at = Some(new_exp); }
        }
        // If expired (after potential reset), remove and return None
        if let Some(ent) = w.get(key) {
            if let Some(exp) = ent.expires_at { if Instant::now() >= exp { w.remove(key); return None; } }
        } else { return None; }
        w.get(key).map(|e| e.value.clone())
    }

    /// Convenience: get raw bytes if the stored value is binary.
    pub fn get_bytes(&self, key: &str) -> Option<Vec<u8>> {
        match self.get(key) {
            Some(KvValue::Bytes(b)) => Some(b),
            _ => None,
        }
    }

    pub fn delete(&self, key: &str) -> bool { self.map.write().remove(key).is_some() }
    pub fn clear(&self) { self.map.write().clear(); }
    pub fn len(&self) -> usize { self.map.read().len() }
    /// Return a snapshot of all keys in this store
    pub fn keys(&self) -> Vec<String> { self.map.read().keys().cloned().collect() }

    /// Delete keys that start with the provided prefix. Returns number of removed keys.
    pub fn delete_prefix(&self, prefix: &str) -> usize {
        let mut w = self.map.write();
        let to_remove: Vec<String> = w.keys().filter(|k| k.starts_with(prefix)).cloned().collect();
        let n = to_remove.len();
        for k in to_remove { w.remove(&k); }
        n
    }

    /// Remove expired keys. Returns number removed.
    pub fn sweep(&self) -> usize {
        let now = Instant::now();
        let mut removed = 0;
        let mut w = self.map.write();
        let keys: Vec<String> = w.iter()
            .filter_map(|(k, v)| v.expires_at.map(|exp| (k.clone(), exp)))
            .filter(|(_, exp)| now >= *exp)
            .map(|(k, _)| k)
            .collect();
        for k in keys { if w.remove(&k).is_some() { removed += 1; } }
        removed
    }

    /// Rename a key within this store. Returns true if the source existed and was moved.
    pub fn rename_key(&self, from: &str, to: &str) -> bool {
        if from == to { return true; }
        let mut w = self.map.write();
        if let Some(entry) = w.remove(from) {
            w.insert(to.to_string(), entry);
            true
        } else { false }
    }
}

/// Registry of KV stores per database under the root path.
#[derive(Clone)]
pub struct KvStoresRegistry {
    root: PathBuf,
    /// db_name -> (store_name -> KvStore)
    inner: Arc<parking_lot::RwLock<StdHashMap<String, StdHashMap<String, KvStore>>>>,
}

impl KvStoresRegistry {
    fn new(root: PathBuf) -> Self { Self { root, inner: Arc::new(parking_lot::RwLock::new(StdHashMap::new())) } }

    fn stores_dir_for_db(&self, db: &str) -> PathBuf { self.root.join(db).join("stores") }

    pub fn get_store(&self, database: &str, store_name: &str) -> KvStore {
        // Fast path read
        if let Some(st) = self.inner.read().get(database).and_then(|m| m.get(store_name)).cloned() { return st; }
        // Create path and load settings
        let dir = self.stores_dir_for_db(database).join(store_name);
        fs::create_dir_all(&dir).ok();
        let kv = KvStore::load_or_default(dir, store_name);
        let mut w = self.inner.write();
        let entry = w.entry(database.to_string()).or_default();
        entry.insert(store_name.to_string(), kv.clone());
        kv
    }

    /// Drop a store: remove from registry and delete its directory. Returns true if it existed.
    pub fn drop_store(&self, database: &str, store_name: &str) -> anyhow::Result<bool> {
        let dir = self.stores_dir_for_db(database).join(store_name);
        // Remove from cache first
        {
            let mut w = self.inner.write();
            if let Some(m) = w.get_mut(database) {
                m.remove(store_name);
            }
        }
        // Delete directory if exists
        if dir.exists() { std::fs::remove_dir_all(&dir).ok(); return Ok(true); }
        Ok(false)
    }

    /// Rename a store within a database: renames directory and updates registry cache.
    pub fn rename_store(&self, database: &str, from: &str, to: &str) -> anyhow::Result<()> {
        if from == to { return Ok(()); }
        let base = self.stores_dir_for_db(database);
        let src = base.join(from);
        let dst = base.join(to);
        std::fs::create_dir_all(&base).ok();
        if src.exists() {
            // If destination exists, error
            if dst.exists() { anyhow::bail!(format!("Target store already exists: {}", to)); }
            std::fs::rename(&src, &dst)?;
        } else {
            // Ensure destination exists for future use
            std::fs::create_dir_all(&dst).ok();
        }
        // Update cache: move KvStore if present, else lazy-load on next access
        let mut w = self.inner.write();
        if let Some(m) = w.get_mut(database) {
            if let Some(kv) = m.remove(from) {
                // Recreate with new dir, keeping settings but updating name
                let mut settings = kv.settings.clone();
                settings.name = to.to_string();
                let new_kv = KvStore::new(dst.clone(), settings);
                // Persist settings to config.json
                let _ = new_kv.save_settings();
                m.insert(to.to_string(), new_kv);
            }
        }
        Ok(())
    }

    /// Sweep all stores, return total removed count
    pub fn sweep_all(&self) -> usize {
        let mut total = 0;
        for (_db, m) in self.inner.read().iter() {
            for (_name, kv) in m.iter() { total += kv.sweep(); }
        }
        total
    }
}

static REGISTRIES: OnceLock<parking_lot::RwLock<StdHashMap<PathBuf, Arc<KvStoresRegistry>>>> = OnceLock::new();

fn kv_registry_for_root(root: &Path) -> Arc<KvStoresRegistry> {
    let root_key = root.to_path_buf();
    let map = REGISTRIES.get_or_init(|| parking_lot::RwLock::new(StdHashMap::new()));
    // fast path read
    if let Some(reg) = map.read().get(&root_key).cloned() { return reg; }
    // create
    let reg = Arc::new(KvStoresRegistry::new(root_key.clone()));
    map.write().insert(root_key, reg.clone());
    reg
}

impl SharedStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root_path = root.as_ref().to_path_buf();
        let s = Self(Arc::new(Mutex::new(Store::new(&root_path)?)));
        // Ensure a registry exists for this root (idempotent)
        let _ = kv_registry_for_root(&root_path);
        Ok(s)
    }
    pub fn root_path(&self) -> PathBuf {
        // Safe because we only clone; no long-lived borrow
        self.0.lock().root.clone()
    }
    /// Get a handle to a named KV store under a given logical database.
    /// This will create the store directory and default config if missing at /<database>/stores/<store>.
    pub fn kv_store(&self, database: &str, store_name: &str) -> KvStore {
        let root = self.root_path();
        let reg = kv_registry_for_root(&root);
        reg.get_store(database, store_name)
    }
    /// Access registry directly (for sweeping)
    pub fn kv_registry(&self) -> Arc<KvStoresRegistry> {
        let root = self.root_path();
        kv_registry_for_root(&root)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;

    #[test]
    fn test_dtype_string_roundtrip() {
        assert_eq!(Store::dtype_to_str(&DataType::String), "string");
        assert_eq!(Store::dtype_to_str(&DataType::Int64), "int64");
        assert_eq!(Store::dtype_to_str(&DataType::Float64), "float64");
        assert!(matches!(Store::str_to_dtype("string"), DataType::String));
        assert!(matches!(Store::str_to_dtype("utf8"), DataType::String));
        assert!(matches!(Store::str_to_dtype("int64"), DataType::Int64));
        assert!(matches!(Store::str_to_dtype("something-else"), DataType::Float64));
    }

    #[test]
    fn test_merge_dtype_precedence() {
        use polars::prelude::DataType::*;
        assert!(matches!(Store::merge_dtype(String, Int64), String));
        assert!(matches!(Store::merge_dtype(Int64, String), String));
        assert!(matches!(Store::merge_dtype(Float64, Int64), Float64));
        assert!(matches!(Store::merge_dtype(Int64, Float64), Float64));
        assert!(matches!(Store::merge_dtype(Int64, Int64), Int64));
    }

    #[test]
    fn test_infer_dtypes() {
        let records = vec![
            Record { _time: 1, sensors: serde_json::Map::from_iter(vec![
                ("a".into(), json!(1)),
                ("b".into(), json!(1.5)),
                ("c".into(), json!("str")),
                ("d".into(), json!("2")),
                ("e".into(), json!("2.5")),
            ]) },
            Record { _time: 2, sensors: serde_json::Map::from_iter(vec![
                ("a".into(), json!(2)),
                ("b".into(), json!(2.5)),
                ("c".into(), json!("str2")),
            ]) },
        ];
        let names: Vec<String> = vec!["a","b","c","d","e"].into_iter().map(|s| s.to_string()).collect();
        let map = Store::infer_dtypes(&records, &names);
        assert!(matches!(map.get("a").unwrap(), DataType::Int64));
        assert!(matches!(map.get("b").unwrap(), DataType::Float64));
        assert!(matches!(map.get("c").unwrap(), DataType::String));
        assert!(matches!(map.get("d").unwrap(), DataType::Int64));
        assert!(matches!(map.get("e").unwrap(), DataType::Float64));
    }

    #[test]
    fn test_kv_store_ttl_and_reset() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let kv = shared.kv_store("clarium", "cache1");
        // set with ttl 100ms, reset on access true
        kv.set("a", KvValue::Int(1), Some(Duration::from_millis(100)), Some(true));
        // immediate get returns value and resets ttl
        match kv.get("a").unwrap() { KvValue::Int(v) => assert_eq!(v, 1), _ => panic!("wrong type") }
        // sleep 80ms and get again; since reset_on_access, it should still be alive
        std::thread::sleep(std::time::Duration::from_millis(80));
        assert!(kv.get("a").is_some());
        // sleep 120ms without access; now it should expire
        std::thread::sleep(std::time::Duration::from_millis(120));
        assert!(kv.get("a").is_none());
    }

    #[test]
    fn test_kv_store_settings_persist() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let _kv = shared.kv_store("db1", "s1");
        // settings file should exist at /<db>/stores/<store>/config.json
        let cfg_new = shared.root_path().join("db1").join("stores").join("s1").join("store.json");
        let cfg_legacy = shared.root_path().join("db1").join("stores").join("s1").join("config.json");
        assert!(cfg_new.exists() || cfg_legacy.exists());
    }

    #[test]
    fn test_kv_store_persistence_snapshot_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let kv = shared.kv_store("dbp", "cache");
        // enable persistence in settings and save
        {
            let mut s = kv.settings.clone();
            s.persistence = Some(super::PersistenceSettings { enabled: true, interval_ms: 5_000, format: "bincode".into() });
            let kv2 = super::KvStore::new(kv.dir.clone(), s);
            let _ = kv2.save_settings();
            // populate values
            kv2.set("a", KvValue::Str("hello".into()), None, None);
            kv2.set("b", KvValue::Int(42), None, None);
            kv2.set("c", KvValue::Json(json!({"x":1})), None, None);
            // snapshot now
            kv2.save_snapshot().unwrap();
        }
        // Reload store; should restore entries
        let kv_new = shared.kv_store("dbp", "cache");
        assert!(matches!(kv_new.get("a").unwrap(), KvValue::Str(ref s) if s=="hello"));
        assert!(matches!(kv_new.get("b").unwrap(), KvValue::Int(42)));
        match kv_new.get("c").unwrap() { KvValue::Json(v) => assert_eq!(v["x"], 1), _ => panic!("json missing") }
    }

    #[test]
    fn test_kv_store_persistence_ttl_restore() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let kv = shared.kv_store("dbq", "cache");
        let mut s = kv.settings.clone();
        s.persistence = Some(super::PersistenceSettings { enabled: true, interval_ms: 5_000, format: "bincode".into() });
        let kv2 = super::KvStore::new(kv.dir.clone(), s);
        kv2.set("ttl", KvValue::Int(1), Some(Duration::from_millis(200)), Some(false));
        // snapshot immediately to capture remaining time (~200ms)
        kv2.save_snapshot().unwrap();
        // reload and ensure key exists, then after >200ms, it expires
        let kv3 = shared.kv_store("dbq", "cache");
        assert!(kv3.get("ttl").is_some());
        std::thread::sleep(Duration::from_millis(250));
        assert!(kv3.get("ttl").is_none());
    }

    #[test]
    fn test_write_and_read_roundtrip() {
        // Use a temp directory under target to avoid clutter; Windows-safe
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let mut sensors1 = serde_json::Map::new();
        sensors1.insert("v".into(), json!(1.0));
        sensors1.insert("label".into(), json!("a"));
        let mut sensors2 = serde_json::Map::new();
        sensors2.insert("v".into(), json!(2)); // int, should merge to float64 for v due to 1.0
        let recs = vec![
            Record { _time: 1_000, sensors: sensors1 },
            Record { _time: 2_000, sensors: sensors2 },
        ];
        store.write_records("db1", &recs).unwrap();
        // Read back
        let df = store.read_df("db1").unwrap();
        assert_eq!(df.height(), 2);
        assert!(df.get_column_names().iter().any(|c| c.as_str() == "_time"));
        assert!(df.get_column_names().iter().any(|c| c.as_str() == "v"));
        assert!(df.get_column_names().iter().any(|c| c.as_str() == "label"));
    }

    #[test]
    fn test_regular_table_partitioning_writes_multiple_files() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let table = "mydb/public/rtab"; // regular table (no .time)
        store.create_table(table).unwrap();
        // Set partitions metadata: partition by region
        store.set_table_metadata(table, None, Some(vec!["region".to_string()])).unwrap();
        // Write rows across two regions
        let mut recs: Vec<Record> = Vec::new();
        for i in 0..10 {
            let mut m = serde_json::Map::new();
            m.insert("region".into(), json!(if i % 2 == 0 { "north" } else { "south" }));
            m.insert("v".into(), json!(i as i64));
            recs.push(Record { _time: 1_700_000_000_000 + i as i64, sensors: m });
        }
        store.write_records(table, &recs).unwrap();
        // Count files
        let dir = store.db_dir(table);
        let mut count = 0usize;
        for e in std::fs::read_dir(&dir).unwrap() {
            let p = e.unwrap().path();
            if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                if name.starts_with("data-") && name.ends_with(".parquet") { count += 1; }
            }
        }
        assert!(count >= 2, "expected >=2 parquet files, found {}", count);
        // Read back and ensure all rows present
        let df = store.read_df(table).unwrap();
        assert_eq!(df.height(), recs.len());
    }

    #[test]
    fn test_out_of_order_insert_is_sorted_on_disk_and_read_sorted() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        // write out-of-order
        let mut s1 = serde_json::Map::new(); s1.insert("a".into(), json!(1));
        let mut s2 = serde_json::Map::new(); s2.insert("a".into(), json!(2));
        let mut s3 = serde_json::Map::new(); s3.insert("a".into(), json!(3));
        let recs = vec![
            Record { _time: 2000, sensors: s2 },
            Record { _time: 1000, sensors: s1 },
            Record { _time: 3000, sensors: s3 },
        ];
        store.write_records("db", &recs).unwrap();
        let df = store.read_df("db").unwrap();
        let times: Vec<i64> = df.column("_time").unwrap().i64().unwrap().into_iter().map(|o| o.unwrap()).collect();
        assert_eq!(times, vec![1000, 2000, 3000]);
    }
}


impl Store {
    /// Update table-level metadata in schema.json for regular tables.
    /// This preserves existing column definitions and unrelated metadata keys.
    pub fn set_table_metadata(&self, table: &str, primary_key: Option<Vec<String>>, partitions: Option<Vec<String>>) -> Result<()> {
        use serde_json::{Value, Map};
        let p = self.schema_path(table);
        let mut obj: Map<String, Value> = if p.exists() {
            if let Ok(text) = fs::read_to_string(&p) {
                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                    if let Some(m) = v.as_object() { m.clone() } else { Map::new() }
                } else { Map::new() }
            } else { Map::new() }
        } else { Map::new() };
        if let Some(pk) = primary_key { 
            obj.insert("primaryKey".into(), serde_json::json!(pk)); 
            // Add PRIMARY marker column to indicate this table has a primary key
            // This marker is used by system catalogs (pg_constraint) and DESCRIBE command
            obj.insert("PRIMARY".into(), serde_json::json!("marker"));
        }
        if let Some(parts) = partitions { obj.insert("partitions".into(), serde_json::json!(parts)); }
        fs::write(&p, serde_json::to_string_pretty(&Value::Object(obj))?)?;
        Ok(())
    }
}

impl KvStoresRegistry {
    /// List existing KV stores for a database by scanning the filesystem under <db>/stores
    pub fn list_stores(&self, database: &str) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let dir = self.stores_dir_for_db(database);
        if let Ok(rd) = std::fs::read_dir(&dir) {
            for ent in rd.flatten() {
                if ent.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                    let name = ent.file_name().to_string_lossy().to_string();
                    if !name.starts_with('.') { out.push(name); }
                }
            }
        }
        out.sort();
        out
    }
}

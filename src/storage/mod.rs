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
use anyhow::Result;
use serde::{Deserialize, Serialize};
use parking_lot::Mutex;
use std::sync::Arc;
use polars::prelude::*;
use tracing::debug;

mod paths;
pub mod kv;
pub mod schema;
mod io;

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
    pub fn get_primary_key(&self, table: &str) -> Option<Vec<String>> { schema::get_primary_key(self, table) }

    /// Read partitions list from schema.json if present.
    pub fn get_partitions(&self, table: &str) -> Vec<String> { schema::get_partitions(self, table) }
    /// Create a new Store rooted at the given filesystem path.
    /// The directory is created if it does not already exist.
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).ok();
        Ok(Self { root })
    }

    /// Return the configured root folder for this Store.
    pub fn root_path(&self) -> &PathBuf { &self.root }

    /// Determine if a logical table is a time-series table using metadata only.
    ///
    /// IMPORTANT: Do not rely on directory name heuristics like a ".time" suffix.
    /// We only trust the persisted schema metadata (and system registry when
    /// applicable). If metadata is absent or invalid, we default to `false`.
    pub fn is_time_table(&self, table: &str) -> bool {
        // Auto-upgrade legacy `.time` directories that are missing explicit tableType
        let _ = crate::storage::schema::ensure_time_tabletype_for_legacy_dir(self, table);
        let schema_path = self.schema_path(table);
        if schema_path.exists() {
            if let Ok(text) = std::fs::read_to_string(&schema_path) {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(tt) = v.get("tableType").and_then(|x| x.as_str()) {
                        let is_time = tt.eq_ignore_ascii_case("time");
                        crate::tprintln!(
                            "[storage.is_time_table] table='{}' schema='{}' tableType='{}' -> {}",
                            table,
                            schema_path.display(),
                            tt,
                            is_time
                        );
                        return is_time;
                    }
                }
            }
        }
        crate::tprintln!(
            "[storage.is_time_table] table='{}' no/invalid schema.json -> default false",
            table
        );
        false
    }

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
            // Always set explicit tableType at creation time
            if table.ends_with(".time") {
                meta.insert("tableType".into(), serde_json::json!("time"));
            } else {
                meta.insert("tableType".into(), serde_json::json!("regular"));
            }
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
    // rewrite_table_df is implemented for Store in io.rs

    // Path helpers moved to paths.rs

    // write_records and read_df are implemented for Store in io.rs

    // schema_path moved to paths.rs

    pub fn load_schema_with_locks(&self, table: &str) -> Result<(std::collections::HashMap<String, DataType>, std::collections::HashSet<String>)> { schema::load_schema_with_locks(self, table) }

    fn save_schema_with_locks(&self, table: &str, schema: &std::collections::HashMap<String, DataType>, locks: &std::collections::HashSet<String>) -> Result<()> { schema::save_schema_with_locks(self, table, schema, locks) }

    // filter_df implemented in io.rs impl Store
}

impl Store {
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
            let mut any_list = false;
            let mut saw_value = false;
            for r in records {
                if let Some(val) = r.sensors.get(name) {
                    match val {
                        serde_json::Value::Array(arr) => {
                            // Treat any array as a vector (List(Float64)) if elements are numeric or string-encoded numbers
                            if !arr.is_empty() {
                                any_list = true;
                                saw_value = true;
                            }
                        }
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
            let dt = if any_list { DataType::List(Box::new(DataType::Float64)) }
                else if any_string_label { DataType::String }
                else if any_float { DataType::Float64 }
                else if saw_value { DataType::Int64 }
                else { DataType::Float64 };
            if cfg!(debug_assertions) {
                crate::tprintln!("[storage.infer_dtypes] name='{}' any_list={} any_float={} any_string_label={} -> {:?}", name, any_list, any_float, any_string_label, dt);
            }
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

// Re-export KV submodule API (declared once at top of file)
pub use kv::{KvStore, KvStoresRegistry, KvValue, StoreSettings, PersistenceSettings};

#[cfg(test)]
#[path = "storage_tests.rs"]
mod storage_tests;

// set_table_metadata is implemented in schema.rs

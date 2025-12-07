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

mod paths;
pub mod kv;
mod schema;
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

// Re-export KV submodule API (declared once at top of file)
pub use kv::{KvStore, KvStoresRegistry, KvValue, StoreSettings, PersistenceSettings};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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

// set_table_metadata is implemented in schema.rs

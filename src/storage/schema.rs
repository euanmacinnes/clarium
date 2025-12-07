use std::collections::{HashMap, HashSet};
use polars::prelude::*;
use crate::tprintln;
use super::Store;

pub(crate) fn get_primary_key(store: &Store, table: &str) -> Option<Vec<String>> {
    let p = store.schema_path(table);
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

pub(crate) fn get_partitions(store: &Store, table: &str) -> Vec<String> {
    let p = store.schema_path(table);
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

pub(crate) fn load_schema_with_locks(store: &Store, table: &str) -> anyhow::Result<(HashMap<String, DataType>, HashSet<String>)> {
    let mut map: HashMap<String, DataType> = HashMap::new();
    let mut locks: HashSet<String> = HashSet::new();
    let p = store.schema_path(table);
    tprintln!("[SCHEMA] load_schema_with_locks: table='{}' path='{}' exists={}", table, p.display(), p.exists());
    if p.exists() {
        let text = std::fs::read_to_string(&p)?;
        tprintln!("[SCHEMA] load_schema_with_locks: raw_json='{}'", text);
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(obj) = v.as_object() {
                if let Some(cols) = obj.get("columns").and_then(|x| x.as_object()) {
                    tprintln!("[SCHEMA] load_schema_with_locks: using nested format, cols={:?}", cols.keys().collect::<Vec<_>>());
                    for (k, v) in cols.iter() {
                        if let Some(s) = v.as_str() { map.insert(k.clone(), str_to_dtype(s)); }
                    }
                } else {
                    // Legacy: flat schema map
                    tprintln!("[SCHEMA] load_schema_with_locks: using flat format, keys={:?}", obj.keys().collect::<Vec<_>>());
                    for (k, v) in obj.iter() {
                        if let Some(s) = v.as_str() { map.insert(k.clone(), str_to_dtype(s)); }
                    }
                }
                if let Some(arr) = obj.get("locks").and_then(|x| x.as_array()) {
                    for e in arr.iter() { if let Some(s) = e.as_str() { locks.insert(s.to_string()); } }
                }
            }
        }
    }
    tprintln!("[SCHEMA] load_schema_with_locks: result map_keys={:?}", map.keys().collect::<Vec<_>>());
    Ok((map, locks))
}

pub(crate) fn save_schema_with_locks(store: &Store, table: &str, schema: &HashMap<String, DataType>, locks: &HashSet<String>) -> anyhow::Result<()> {
    let p = store.schema_path(table);
    let mut root: serde_json::Map<String, serde_json::Value> = if p.exists() {
        let text = std::fs::read_to_string(&p).unwrap_or_default();
        serde_json::from_str::<serde_json::Value>(&text).ok().and_then(|v| v.as_object().cloned()).unwrap_or_default()
    } else { serde_json::Map::new() };
    // Write as nested { columns: { name: dtype }, locks: [] }
    let mut cols: HashMap<String, String> = HashMap::new();
    for (k, dt) in schema.iter() { cols.insert(k.clone(), dtype_to_str(dt)); }
    root.insert("columns".into(), serde_json::json!(cols));
    root.insert("locks".into(), serde_json::json!(locks.iter().cloned().collect::<Vec<_>>()));
    std::fs::write(&p, serde_json::to_string_pretty(&serde_json::Value::Object(root))?)?;
    Ok(())
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

pub(crate) fn str_to_dtype(s: &str) -> DataType {
    match s.to_ascii_lowercase().as_str() {
        "utf8" | "string" => DataType::String,
        "int64" => DataType::Int64,
        // Map logical 'vector' to List(Float64)
        "vector" => DataType::List(Box::new(DataType::Float64)),
        _ => DataType::Float64,
    }
}

pub(crate) fn merge_dtype(a: DataType, b: DataType) -> DataType {
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

impl Store {
    pub fn set_table_metadata(&self, table: &str, primary_key: Option<Vec<String>>, partitions: Option<Vec<String>>) -> anyhow::Result<()> {
        use serde_json::{Value, Map};
        let p = self.schema_path(table);
        let mut obj: Map<String, Value> = if p.exists() {
            if let Ok(text) = std::fs::read_to_string(&p) {
                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                    if let Some(m) = v.as_object() { m.clone() } else { Map::new() }
                } else { Map::new() }
            } else { Map::new() }
        } else { Map::new() };
        if let Some(pk) = primary_key { 
            obj.insert("primaryKey".into(), serde_json::json!(pk)); 
            // PRIMARY marker column for system catalogs
            obj.insert("PRIMARY".into(), serde_json::json!("marker"));
        }
        if let Some(parts) = partitions { obj.insert("partitions".into(), serde_json::json!(parts)); }
        std::fs::write(&p, serde_json::to_string_pretty(&Value::Object(obj))?)?;
        Ok(())
    }
}

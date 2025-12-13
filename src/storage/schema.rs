use std::collections::{HashMap, HashSet};
use std::path::Path;
use polars::prelude::*;
use crate::tprintln;
use super::Store;
use crate::system_catalog::registry as sysreg;

/// If a table directory name ends with `.time` but its `schema.json` lacks
/// `tableType: "time"`, upgrade it in place. Returns true if a change was made.
pub(crate) fn ensure_time_tabletype_for_legacy_dir(store: &Store, table: &str) -> anyhow::Result<bool> {
    // Only act on legacy `.time` directory naming
    if !table.ends_with(".time") { return Ok(false); }
    let p = store.schema_path(table);
    if !p.exists() { return Ok(false); }
    let text = match std::fs::read_to_string(&p) { Ok(t) => t, Err(_) => return Ok(false) };
    let mut changed = false;
    if let Ok(mut v) = serde_json::from_str::<serde_json::Value>(&text) {
        if let Some(obj) = v.as_object_mut() {
            let needs = match obj.get("tableType").and_then(|x| x.as_str()) {
                Some(tt) => !tt.eq_ignore_ascii_case("time"),
                None => true,
            };
            if needs {
                crate::tprintln!("[SCHEMA] auto-upgrade: setting tableType='time' for legacy dir '{}'", table);
                obj.insert("tableType".into(), serde_json::json!("time"));
                if std::fs::write(&p, serde_json::to_string_pretty(&serde_json::Value::Object(obj.clone())).unwrap_or_else(|_| text.clone())).is_ok() {
                    changed = true;
                }
            }
        }
    }
    Ok(changed)
}

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
    // First, check system catalog registry for a matching table. If present,
    // return schema derived from ColumnDef instead of reading JSON files.
    if let Some(sys) = sysreg::lookup_from_str(table) {
        tprintln!("[SCHEMA] load_schema_with_locks: using system registry for '{}.{}' (input='{}')", sys.schema(), sys.name(), table);
        let m = sysreg::schema_map_for(sys.as_ref());
        tprintln!("[SCHEMA] load_schema_with_locks: system map_keys={:?}", m.keys().collect::<Vec<_>>());
        return Ok((m, locks));
    }
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
    // Preserve existing tableType if present. Do NOT infer from table name here.
    // Creation paths are responsible for setting an explicit tableType.
    if !root.contains_key("tableType") {
        root.insert("tableType".into(), serde_json::json!("regular"));
    }
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
        // Generic list (array) type used for non-vector arrays in schemas
        // Default inner type to String for resilience; execution paths can cast as needed.
        "list" | "array" => DataType::List(Box::new(DataType::String)),
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

// --- Schema migration utilities ---

/// Migrate all `schema.json` files under the provided root to the new nested format
/// with explicit `tableType`. Returns the number of files updated.
pub(crate) fn migrate_all_schemas_for_root(root: &Path) -> anyhow::Result<usize> {
    let mut updated = 0usize;
    if !root.exists() { return Ok(0); }
    // Walk: root/db/schema/table/(schema.json)
    for db_ent in std::fs::read_dir(root).unwrap_or_else(|_| std::fs::ReadDir::from(std::fs::read_dir(root).unwrap())) {
        if let Ok(db) = db_ent { let dbp = db.path(); if !dbp.is_dir() { continue; }
            for sch_ent in std::fs::read_dir(&dbp).unwrap_or_else(|_| std::fs::read_dir(&dbp).unwrap()) {
                if let Ok(sch) = sch_ent { let schp = sch.path(); if !schp.is_dir() { continue; }
                    for tab_ent in std::fs::read_dir(&schp).unwrap_or_else(|_| std::fs::read_dir(&schp).unwrap()) {
                        if let Ok(tab) = tab_ent { let tabp = tab.path(); if !tabp.is_dir() { continue; }
                            let sj = tabp.join("schema.json");
                            if !sj.exists() { continue; }
                            if let Ok(text) = std::fs::read_to_string(&sj) {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                    let mut changed = false;
                                    let mut obj = v.as_object().cloned().unwrap_or_default();
                                    // Ensure tableType
                                    if !obj.contains_key("tableType") {
                                        let tt = if tabp.file_name().and_then(|s| s.to_str()).map(|s| s.ends_with(".time")).unwrap_or(false) { "time" } else { "regular" };
                                        obj.insert("tableType".into(), serde_json::json!(tt));
                                        changed = true;
                                    }
                                    // Ensure nested columns
                                    let has_nested = obj.get("columns").and_then(|x| x.as_object()).is_some();
                                    if !has_nested {
                                        // Derive from flat entries: collect string values except known metadata keys
                                        let mut cols: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
                                        for (k, val) in obj.clone().into_iter() {
                                            if matches!(k.as_str(), "columns" | "locks" | "PRIMARY" | "primaryKey" | "partitions" | "tableType") { continue; }
                                            if let Some(s) = val.as_str() { cols.insert(k, serde_json::json!(s)); }
                                        }
                                        obj.insert("columns".into(), serde_json::Value::Object(cols));
                                        changed = true;
                                    }
                                    if changed {
                                        if std::fs::write(&sj, serde_json::to_string_pretty(&serde_json::Value::Object(obj))?).is_ok() {
                                            updated += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if updated > 0 { tprintln!("[SCHEMA] migrate_all_schemas_for_root: updated {} schema.json files", updated); }
    Ok(updated)
}

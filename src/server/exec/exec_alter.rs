use anyhow::{anyhow, Result};
use tracing::{debug, info};

use crate::server::query::AlterOp;
use crate::storage::SharedStore;

/// Apply ALTER TABLE operations to a regular table by updating schema.json metadata.
pub fn handle_alter_table(store: &SharedStore, table: &str, ops: &[AlterOp]) -> Result<serde_json::Value> {
    use serde_json::{Value, Map, json};
    // Qualify
    let qd = crate::system::current_query_defaults();
    let tableq = crate::ident::qualify_regular_ident(table, &qd);
    // Resolve schema.json path
    let root = store.root_path().clone();
    let dir = root.join(tableq.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
    let spath = dir.join("schema.json");
    if !dir.exists() {
        return Err(anyhow!(format!("ALTER TABLE target does not exist: {}", tableq)));
    }
    // Load current schema.json as a map
    let mut obj: Map<String, Value> = if spath.exists() {
        if let Ok(text) = std::fs::read_to_string(&spath) {
            if let Ok(v) = serde_json::from_str::<Value>(&text) {
                v.as_object().cloned().unwrap_or_default()
            } else { Map::new() }
        } else { Map::new() }
    } else { Map::new() };

    // Helper to get constraints array
    let mut get_constraints = |obj: &mut Map<String, Value>| -> Vec<Map<String, Value>> {
        match obj.get("constraints").and_then(|v| v.as_array()) {
            Some(arr) => arr.iter().filter_map(|e| e.as_object().cloned()).collect(),
            None => Vec::new(),
        }
    };

    for op in ops {
        match op {
            AlterOp::AddColumn { name, type_key, .. } => {
                if name == "_time" { continue; }
                obj.insert(name.clone(), Value::String(type_key.clone()));
                info!(target: "clarium::ddl", "ALTER TABLE {}: ADD COLUMN {} {}", tableq, name, type_key);
            }
            AlterOp::RenameColumn { from, to } => {
                if let Some(v) = obj.remove(from) {
                    obj.insert(to.clone(), v);
                    info!(target: "clarium::ddl", "ALTER TABLE {}: RENAME COLUMN {} TO {}", tableq, from, to);
                } else {
                    debug!(target: "clarium::ddl", "ALTER TABLE {}: RENAME COLUMN skipped, source '{}' not found", tableq, from);
                }
            }
            AlterOp::AlterColumnType { name, type_key } => {
                // Update type if column exists
                if obj.contains_key(name) {
                    obj.insert(name.clone(), Value::String(type_key.clone()));
                    info!(target: "clarium::ddl", "ALTER TABLE {}: ALTER COLUMN {} TYPE {}", tableq, name, type_key);
                } else {
                    return Err(anyhow!(format!("column not found: {}", name)));
                }
            }
            AlterOp::AddPrimaryKey { columns } => {
                obj.insert("primaryKey".into(), json!(columns));
                obj.insert("PRIMARY".into(), json!("marker"));
                info!(target: "clarium::ddl", "ALTER TABLE {}: ADD PRIMARY KEY ({})", tableq, columns.join(", "));
            }
            AlterOp::DropPrimaryKey => {
                obj.remove("primaryKey");
                obj.remove("PRIMARY");
                info!(target: "clarium::ddl", "ALTER TABLE {}: DROP PRIMARY KEY", tableq);
            }
            AlterOp::AddConstraint { name, udf } => {
                // Validate UDF exists
                if let Some(reg) = crate::scripts::get_script_registry() {
                    if !reg.has_function(udf) {
                        return Err(anyhow!(format!("constraint UDF not found: {}", udf)));
                    }
                }
                let mut arr = get_constraints(&mut obj);
                // Replace existing by name if present
                arr.retain(|m| m.get("name").and_then(|v| v.as_str()) != Some(name.as_str()));
                let mut m = Map::new();
                m.insert("name".into(), Value::String(name.clone()));
                m.insert("udf".into(), Value::String(udf.clone()));
                arr.push(m);
                obj.insert("constraints".into(), Value::Array(arr.into_iter().map(Value::Object).collect()));
                info!(target: "clarium::ddl", "ALTER TABLE {}: ADD CONSTRAINT {} USING {}", tableq, name, udf);
            }
            AlterOp::DropConstraint { name } => {
                let mut arr = get_constraints(&mut obj);
                let before = arr.len();
                arr.retain(|m| m.get("name").and_then(|v| v.as_str()) != Some(name.as_str()));
                if arr.len() != before {
                    obj.insert("constraints".into(), Value::Array(arr.into_iter().map(Value::Object).collect()));
                }
                info!(target: "clarium::ddl", "ALTER TABLE {}: DROP CONSTRAINT {}", tableq, name);
            }
        }
    }

    // Persist
    std::fs::write(&spath, serde_json::to_string_pretty(&Value::Object(obj))?)?;
    Ok(serde_json::json!({"status":"ok"}))
}

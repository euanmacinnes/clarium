//! exec_keys
//! ---------
//! KV key operations extracted from exec.rs to keep dispatcher thin.

use anyhow::Result;
use crate::storage::{SharedStore, KvValue};
use std::time::Duration;

use crate::server::exec::df_utils::read_df_or_kv;

pub fn handle_write_key(store: &SharedStore, database: &str, st: &str, key: &str, value: &str, ttl_ms: Option<i64>, reset_on_access: Option<bool>) -> Result<serde_json::Value> {
    let kv = store.kv_store(database, st);
    let ttl: Option<Duration> = ttl_ms.and_then(|ms| if ms > 0 { Some(Duration::from_millis(ms as u64)) } else { None });
    let vstr = value.trim();
    // Try to interpret like original behavior
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
        if let Ok(df) = read_df_or_kv(store, vstr) {
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
    kv.set(key, kv_val, ttl, reset_on_access.unwrap_or(false));
    Ok(serde_json::json!({"status":"ok","written":1,"type": kind}))
}

pub fn handle_read_key(store: &SharedStore, database: &str, st: &str, key: &str) -> Result<serde_json::Value> {
    let kv = store.kv_store(database, st);
    if let Some(val) = kv.get(key) {
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

pub fn handle_drop_key(store: &SharedStore, database: &str, st: &str, key: &str) -> Result<serde_json::Value> {
    let kv = store.kv_store(database, st);
    let existed = kv.delete(key);
    Ok(serde_json::json!({"status":"ok","dropped": existed}))
}

pub fn handle_rename_key(store: &SharedStore, database: &str, st: &str, from: &str, to: &str) -> Result<serde_json::Value> {
    let kv = store.kv_store(database, st);
    let moved = kv.rename_key(from, to);
    Ok(serde_json::json!({"status":"ok","renamed": moved}))
}

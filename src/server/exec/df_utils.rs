use anyhow::Result;
use serde_json::Value;
use polars::prelude::*;
use tracing::debug;

use crate::storage::{SharedStore, KvValue};
use crate::server::query::Query;

// Helper: read a DataFrame from either a regular table path or a KV store address
pub(crate) fn read_df_or_kv(store: &SharedStore, name: &str) -> anyhow::Result<DataFrame> {
    debug!(target: "clarium::exec", "read_df_or_kv: name='{}'", name);
    // Detect pattern: <database>.store.<store>.<key>
    if name.contains(".store.") {
        let parts: Vec<&str> = name.split('.').collect();
        if parts.len() < 4 {
            anyhow::bail!(format!("Invalid store address '{}'. Expected <database>.store.<store>.<key>", name));
        }
        if parts[1].to_lowercase() != "store" {
            // e.g., allow schema like clarium.public? Not for KV. Enforce explicit 'store'
            anyhow::bail!(format!("Invalid store address '{}'. Expected literal 'store' segment", name));
        }
        let db = parts[0];
        let store_name = parts[2];
        let key = parts[3..].join(".");
        let kv = store.kv_store(db, store_name);
        if let Some(val) = kv.get(&key) {
            match val {
                KvValue::ParquetDf(df) => Ok(df),
                KvValue::Json(_) => anyhow::bail!("JSON key cannot be used in FROM yet; JSON querying is not implemented"),
                KvValue::Str(_) | KvValue::Int(_) => anyhow::bail!("Scalar key cannot be used in FROM; expected a table"),
            }
        } else {
            anyhow::bail!(format!("KV key not found: {}.store.{}.{}", db, store_name, key));
        }
    } else {
        let guard = store.0.lock();
        match guard.read_df(name) {
            Ok(df) => Ok(df),
            Err(e) => {
                // Fallback: if a time-series table exists with '.time' suffix, attempt to read that.
                if !name.ends_with(".time") {
                    let alt = format!("{}.time", name);
                    debug!(target: "clarium::exec", "read_df_or_kv fallback: trying '{}'", alt);
                    match guard.read_df(&alt) {
                        Ok(df2) => Ok(df2),
                        Err(_) => Err(e),
                    }
                } else {
                    Err(e)
                }
            }
        }
    }
}

pub(crate) fn apply_order_and_limit(mut df: DataFrame, q: &Query) -> Result<DataFrame> {
    if let Some(ob) = &q.order_by {
        if !ob.is_empty() {
            // Strict ORDER BY: all specified columns must be present in the DataFrame at this point.
            // Callers that need to sort by a non-projected column must inject it prior to calling this function.
            let existing: std::collections::HashSet<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
            let mut exprs: Vec<Expr> = Vec::new();
            let mut descending: Vec<bool> = Vec::new();
            for (name, asc) in ob.iter() {
                if !existing.contains(name.as_str()) {
                    anyhow::bail!("ORDER BY column '{}' does not exist in the result set", name);
                }
                exprs.push(col(name.as_str()));
                descending.push(!asc);
            }
            if !exprs.is_empty() {
                let nulls_last: Vec<bool> = vec![true; exprs.len()];
                let opts = polars::prelude::SortMultipleOptions { descending, nulls_last, maintain_order: true, multithreaded: true, limit: None };
                df = df.lazy().sort_by_exprs(exprs, opts).collect()?;
            }
        }
    }
    if let Some(n) = q.limit {
        let h = df.height();
        if n > 0 {
            let m = n as usize;
            if m >= h { /* no-op, return full df */ }
            else { df = df.slice(0, m); }
        } else if n < 0 {
            let m = (-n) as usize;
            if m >= h { /* no-op, return full df */ }
            else {
                let start = (h - m) as i64;
                df = df.slice(start, m);
            }
        } else {
            // n == 0 => empty
            df = df.slice(0, 0);
        }
    }
    Ok(df)
}

pub(crate) fn dataframe_to_json(df: &DataFrame) -> Value {
    // Convert to vector of maps
    let cols = df.get_column_names();
    let mut out = Vec::with_capacity(df.height());
    for row_idx in 0..df.height() {
        let mut map = serde_json::Map::with_capacity(cols.len());
        for c in &cols {
            let s = match df.column(c) {
                Ok(col) => col,
                Err(_) => { map.insert(c.to_string(), Value::Null); continue; }
            };
            let av = s.get(row_idx);
            let v = match av {
                Ok(AnyValue::Int64(v)) => serde_json::json!(v),
                Ok(AnyValue::Int32(v)) => serde_json::json!(v as i64),
                Ok(AnyValue::Float64(v)) => serde_json::json!(v),
                Ok(AnyValue::Boolean(v)) => serde_json::json!(v),
                Ok(AnyValue::String(v)) => serde_json::json!(v),
                Ok(AnyValue::StringOwned(v)) => serde_json::json!(v),
                Ok(AnyValue::Null) => Value::Null,
                _ => Value::Null,
            };
            map.insert(c.to_string(), v);
        }
        out.push(Value::Object(map));
    }
    Value::Array(out)
}

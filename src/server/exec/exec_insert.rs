//! exec_insert
//! -----------
//! INSERT command implementation extracted from exec.rs. Keep all INSERT logic
//! here so the dispatcher stays small.

use anyhow::Result;
use polars::prelude::*;

use crate::{server::query, storage::SharedStore};

pub fn handle_insert(store: &SharedStore, table: String, columns: Vec<String>, values: Vec<Vec<query::ArithTerm>>) -> Result<serde_json::Value> {
    // Convert dot notation (schema.table) to slash notation (schema/table) for storage
    // But preserve .time suffix for time tables
    let table_path = if table.ends_with(".time") {
        let base = &table[..table.len() - 5]; // Remove ".time"
        format!("{}.time", base.replace('.', "/"))
    } else {
        table.replace('.', "/")
    };

    let guard = store.0.lock();
    // Ensure table exists
    guard.create_table(&table_path).ok();

    // Time-table insert path
    if table_path.ends_with(".time") {
        // Find _time column index (accept ID or _time)
        let time_col_idx = columns.iter().position(|c| {
            let c_upper = c.to_uppercase();
            c_upper == "_TIME" || c_upper == "ID"
        }).ok_or_else(|| anyhow::anyhow!("INSERT into time table requires _time or ID column"))?;

        let mut records: Vec<crate::storage::Record> = Vec::with_capacity(values.len());
        for row in &values {
            if row.len() != columns.len() {
                anyhow::bail!("INSERT value count mismatch: expected {} columns", columns.len());
            }
            let time_val = match &row[time_col_idx] {
                query::ArithTerm::Number(n) => *n as i64,
                query::ArithTerm::Str(s) => s.parse::<i64>()?,
                query::ArithTerm::Null => anyhow::bail!("_time cannot be NULL in time table"),
                _ => anyhow::bail!("Invalid _time value"),
            };
            let mut sensors = serde_json::Map::new();
            for (i, col_name) in columns.iter().enumerate() {
                if i == time_col_idx { continue; }
                let val = match &row[i] {
                    query::ArithTerm::Number(n) => serde_json::json!(n),
                    query::ArithTerm::Str(s) => serde_json::json!(s),
                    query::ArithTerm::Null => serde_json::Value::Null,
                    _ => serde_json::Value::Null,
                };
                sensors.insert(col_name.clone(), val);
            }
            records.push(crate::storage::Record { _time: time_val, sensors });
        }
        guard.write_records(&table_path, &records)?;
        return Ok(serde_json::json!({"status":"ok", "inserted": records.len()}));
    }

    // Regular parquet table - build DataFrame and append
    // Create series for each column
    let mut series_vec: Vec<Series> = Vec::new();
    for (col_idx, col_name) in columns.iter().enumerate() {
        let mut col_values: Vec<query::ArithTerm> = Vec::with_capacity(values.len());
        for row in &values {
            if row.len() != columns.len() { anyhow::bail!("INSERT value count mismatch: expected {} columns", columns.len()); }
            col_values.push(row[col_idx].clone());
        }
        // Determine column type
        let mut all_null = true;
        let mut has_string = false;
        let mut has_float = false;
        for val in &col_values {
            match val {
                query::ArithTerm::Str(_) => { all_null = false; has_string = true; }
                query::ArithTerm::Number(_) => { all_null = false; has_float = true; }
                query::ArithTerm::Null => {}
                _ => {}
            }
        }
        let series = if all_null {
            Series::new_null(col_name.as_str().into(), col_values.len())
        } else if has_string {
            let vals: Vec<Option<String>> = col_values.iter().map(|v| match v {
                query::ArithTerm::Str(s) => Some(s.clone()),
                query::ArithTerm::Number(n) => Some(n.to_string()),
                query::ArithTerm::Null => None,
                _ => None,
            }).collect();
            Series::new(col_name.as_str().into(), vals)
        } else if has_float {
            let vals: Vec<Option<f64>> = col_values.iter().map(|v| match v {
                query::ArithTerm::Number(n) => Some(*n),
                query::ArithTerm::Str(s) => s.parse::<f64>().ok(),
                query::ArithTerm::Null => None,
                _ => None,
            }).collect();
            Series::new(col_name.as_str().into(), vals)
        } else {
            Series::new_null(col_name.as_str().into(), col_values.len())
        };
        series_vec.push(series);
    }
    let columns_vec: Vec<Column> = series_vec.into_iter().map(|s| s.into()).collect();
    let new_df = DataFrame::new(columns_vec)?;

    // Enforce primary key uniqueness if table defines a primary key
    {
        let guard = store.0.lock();
        if let Some(pk_cols) = guard.get_primary_key(&table_path) {
            if !pk_cols.is_empty() {
                // Ensure PK columns exist in new_df and are non-null; build keys
                let mut new_keys: Vec<String> = Vec::with_capacity(new_df.height());
                let n = new_df.height();
                for i in 0..n {
                    let mut parts: Vec<String> = Vec::with_capacity(pk_cols.len());
                    for c in &pk_cols {
                        if !new_df.get_column_names().iter().any(|n| n.as_str() == c) {
                            anyhow::bail!(format!("INSERT missing primary key column '{}'", c));
                        }
                        let av = new_df.column(c.as_str())?.get(i).ok();
                        // Reject NULLs in PK
                        if matches!(av, Some(AnyValue::Null) | None) {
                            anyhow::bail!("PRIMARY KEY cannot be NULL");
                        }
                        let sval = match av.unwrap() {
                            AnyValue::String(s) => s.to_string(),
                            AnyValue::StringOwned(s) => s.to_string(),
                            AnyValue::Int64(v) => v.to_string(),
                            AnyValue::Float64(f) => {
                                let mut s = format!("{}", f);
                                if s.contains('.') { s = s.trim_end_matches('0').trim_end_matches('.').to_string(); }
                                s
                            }
                            v => v.to_string(),
                        };
                        parts.push(format!("{}={}", c, sval));
                    }
                    new_keys.push(parts.join(","));
                }
                // Check duplicates within the new batch
                {
                    use std::collections::HashSet;
                    let mut seen: HashSet<String> = HashSet::with_capacity(new_keys.len());
                    for k in &new_keys {
                        if !seen.insert(k.clone()) {
                            anyhow::bail!("Duplicate PRIMARY KEY in INSERT batch");
                        }
                    }
                    // Check against existing keys
                    if let Ok(existing_df) = guard.read_df(&table_path) {
                        if existing_df.height() > 0 {
                            // Build key set from existing
                            let mut existing_set: HashSet<String> = HashSet::new();
                            let m = existing_df.height();
                            for i in 0..m {
                                let mut parts: Vec<String> = Vec::with_capacity(pk_cols.len());
                                for c in &pk_cols {
                                    if !existing_df.get_column_names().iter().any(|n| n.as_str() == c) { continue; }
                                    let av = existing_df.column(c.as_str()).ok().and_then(|s| s.get(i).ok());
                                    // If any part is null, treat as not a valid key to compare
                                    if matches!(av, Some(AnyValue::Null) | None) { parts.clear(); break; }
                                    let sval = match av.unwrap() {
                                        AnyValue::String(s) => s.to_string(),
                                        AnyValue::StringOwned(s) => s.to_string(),
                                        AnyValue::Int64(v) => v.to_string(),
                                        AnyValue::Float64(f) => {
                                            let mut s = format!("{}", f);
                                            if s.contains('.') { s = s.trim_end_matches('0').trim_end_matches('.').to_string(); }
                                            s
                                        }
                                        v => v.to_string(),
                                    };
                                    parts.push(format!("{}={}", c, sval));
                                }
                                if !parts.is_empty() { existing_set.insert(parts.join(",")); }
                            }
                            for k in &new_keys { if existing_set.contains(k) { anyhow::bail!("PRIMARY KEY violation: duplicate key exists"); } }
                        }
                    }
                }
            }
        }
    }

    // Append or create
    let combined = match guard.read_df(&table_path) {
        Ok(existing) => existing.vstack(&new_df)?,
        Err(_) => new_df.clone(),
    };
    guard.rewrite_table_df(&table_path, combined)?;
    Ok(serde_json::json!({"status":"ok", "inserted": new_df.height()}))
}

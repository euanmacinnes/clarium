//! exec_insert
//! -----------
//! INSERT command implementation extracted from exec.rs. Keep all INSERT logic
//! here so the dispatcher stays small.

use anyhow::Result;
use polars::prelude::*;

use crate::{server::query, storage::SharedStore};

pub fn handle_insert(store: &SharedStore, table: String, columns: Vec<String>, values: Vec<Vec<query::ArithTerm>>) -> Result<serde_json::Value> {
    let __t0 = std::time::Instant::now();
    // Convert dot notation (schema.table) to slash notation (schema/table) for storage
    // But preserve .time suffix for time tables
    let table_path = if table.ends_with(".time") {
        let base = &table[..table.len() - 5]; // Remove ".time"
        format!("{}.time", base.replace('.', "/"))
    } else {
        table.replace('.', "/")
    };

    // Ensure table exists (lock only for this short scope)
    {
        let guard = store.0.lock();
        guard.create_table(&table_path).ok();
        // guard dropped here
    }

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
        // Acquire lock only while writing records
        {
            let guard = store.0.lock();
            guard.write_records(&table_path, &records)?;
        }
        return Ok(serde_json::json!({"status":"ok", "inserted": records.len()}));
    }

    // Regular parquet table - build DataFrame and append
    let __t_build_df = std::time::Instant::now();
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
    crate::tprintln!("[EXEC_INSERT] build_df rows={} cols={} took={:?}", new_df.height(), new_df.width(), __t_build_df.elapsed());

    // Enforce primary key uniqueness if table defines a primary key
    {
        let __t_pk = std::time::Instant::now();
        // Lock only to read PK metadata
        let pk_cols_opt: Option<Vec<String>> = {
            let guard = store.0.lock();
            guard.get_primary_key(&table_path)
        };
        if let Some(pk_cols) = pk_cols_opt {
            if !pk_cols.is_empty() {
                // Validate PK columns exist once and cache column handles for faster row access
                let mut pk_series: Vec<&Column> = Vec::with_capacity(pk_cols.len());
                let schema_names = new_df.get_column_names();
                for c in &pk_cols {
                    if !schema_names.iter().any(|n| n.as_str() == c) {
                        anyhow::bail!(format!("INSERT missing primary key column '{}'", c));
                    }
                    pk_series.push(new_df.column(c.as_str())?);
                }

                // Build keys for new batch with minimal allocations
                let n = new_df.height();
                let mut new_keys: Vec<String> = Vec::with_capacity(n);
                let mut key_buf = String::new();
                for i in 0..n {
                    key_buf.clear();
                    let mut first = true;
                    for (idx, c) in pk_cols.iter().enumerate() {
                        let av = pk_series[idx].get(i).ok();
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
                        if !first { key_buf.push(','); }
                        first = false;
                        key_buf.push_str(c);
                        key_buf.push('=');
                        key_buf.push_str(&sval);
                    }
                    new_keys.push(key_buf.clone());
                }
                // Check duplicates within the new batch using HashSet
                {
                    use std::collections::HashSet;
                    let mut seen: HashSet<String> = HashSet::with_capacity(new_keys.len());
                    for k in &new_keys {
                        if !seen.insert(k.clone()) {
                            anyhow::bail!("Duplicate PRIMARY KEY in INSERT batch");
                        }
                    }
                    // Check against existing keys (if any rows exist)
                    let __t_read_existing = std::time::Instant::now();
                    // Lock only to read existing DF
                    let existing_df_res = {
                        let guard = store.0.lock();
                        guard.read_df(&table_path)
                    };
                    if let Ok(existing_df) = existing_df_res {
                        let m = existing_df.height();
                        if m > 0 {
                            crate::tprintln!("[EXEC_INSERT] existing_df rows={} read_time={:?}", m, __t_read_existing.elapsed());
                            // Cache existing pk columns once
                            let mut existing_pk_series: Vec<Option<Column>> = Vec::with_capacity(pk_cols.len());
                            let existing_names = existing_df.get_column_names();
                            for c in &pk_cols {
                                if existing_names.iter().any(|n| n.as_str() == c) {
                                    existing_pk_series.push(Some(existing_df.column(c.as_str())?.clone()))
                                } else {
                                    existing_pk_series.push(None);
                                }
                            }
                            // Build a set of existing keys
                            let __t_existing_set = std::time::Instant::now();
                            let mut existing_set: std::collections::HashSet<String> = std::collections::HashSet::with_capacity(m.min(1024));
                            let mut buf = String::new();
                            'ROW: for i in 0..m {
                                buf.clear();
                                let mut first = true;
                                for (idx, c) in pk_cols.iter().enumerate() {
                                    let opt_s = &existing_pk_series[idx];
                                    if opt_s.is_none() { continue 'ROW; }
                                    let sref = opt_s.as_ref().unwrap();
                                    let av = sref.get(i).ok();
                                    if matches!(av, Some(AnyValue::Null) | None) { continue 'ROW; }
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
                                    if !first { buf.push(','); }
                                    first = false;
                                    buf.push_str(c);
                                    buf.push('=');
                                    buf.push_str(&sval);
                                }
                                if !buf.is_empty() { existing_set.insert(buf.clone()); }
                            }
                            crate::tprintln!("[EXEC_INSERT] build_existing_keyset rows={} took={:?}", m, __t_existing_set.elapsed());
                            for k in &new_keys {
                                if existing_set.contains(k) {
                                    anyhow::bail!("PRIMARY KEY violation: duplicate key exists");
                                }
                            }
                        }
                    }
                }
                crate::tprintln!("[EXEC_INSERT] pk_validate rows_new={} took={:?}", new_df.height(), __t_pk.elapsed());
            }
        }
    }

    // Append or create
    let __t_rewrite = std::time::Instant::now();
    // Read existing (if any) under lock, then perform combine and rewrite with minimal lock scopes
    let existing_res = {
        let guard = store.0.lock();
        guard.read_df(&table_path)
    };
    let combined = match existing_res {
        Ok(existing) => {
            // If existing is empty with zero columns, just take new_df
            if existing.width() == 0 && existing.height() == 0 {
                new_df.clone()
            } else if existing.width() == 0 {
                // No columns in existing: skip alignment and use new_df
                new_df.clone()
            } else if new_df.width() == 0 {
                existing.clone()
            } else {
                // Align schemas by column name before vstack
                let mut left = existing.clone();
                let mut right = new_df.clone();
                let left_names = left.get_column_names();
                let right_names = right.get_column_names();
                use std::collections::HashSet;
                // Build name sets as HashSet<&str> or String; use &str borrowing for contains
                let left_set: HashSet<String> = left_names.iter().map(|n| n.as_str().to_string()).collect();
                let right_set: HashSet<String> = right_names.iter().map(|n| n.as_str().to_string()).collect();
                // Columns present in left but missing in right → add as nulls
                for name in left_names.iter() {
                    if !right_set.contains(name.as_str()) {
                        let dtype = left.column(name.as_str()).map(|c| c.dtype().clone()).unwrap_or(DataType::Null);
                        let s: Series = match dtype {
                            DataType::Int64 => Series::new(name.as_str().into(), vec![Option::<i64>::None; right.height()]),
                            DataType::Float64 => Series::new(name.as_str().into(), vec![Option::<f64>::None; right.height()]),
                            DataType::String => Series::new(name.as_str().into(), vec![Option::<String>::None; right.height()]),
                            DataType::UInt64 => Series::new(name.as_str().into(), vec![Option::<u64>::None; right.height()]),
                            DataType::UInt32 => Series::new(name.as_str().into(), vec![Option::<u32>::None; right.height()]),
                            DataType::Boolean => Series::new(name.as_str().into(), vec![Option::<bool>::None; right.height()]),
                            DataType::List(inner) => {
                                match *inner {
                                    DataType::Float64 => Series::new(name.as_str().into(), Vec::<Option<f64>>::new()),
                                    DataType::Int64 => Series::new(name.as_str().into(), Vec::<Option<i64>>::new()),
                                    _ => Series::new(name.as_str().into(), Vec::<Option<String>>::new()),
                                }
                            }
                            _ => Series::new_null(name.as_str().into(), right.height()),
                        };
                        right = right.hstack(&[s.into()])?;
                    }
                }
                // Columns present in right but missing in left → add as nulls
                for name in right.get_column_names().iter() {
                    if !left_set.contains(name.as_str()) {
                        // Infer dtype from right column to create nulls in left
                        let dtype = right.column(name.as_str()).map(|c| c.dtype().clone()).unwrap_or(DataType::Null);
                        let s: Series = match dtype {
                            DataType::Int64 => Series::new(name.as_str().into(), vec![Option::<i64>::None; left.height()]),
                            DataType::Float64 => Series::new(name.as_str().into(), vec![Option::<f64>::None; left.height()]),
                            DataType::String => Series::new(name.as_str().into(), vec![Option::<String>::None; left.height()]),
                            DataType::UInt64 => Series::new(name.as_str().into(), vec![Option::<u64>::None; left.height()]),
                            DataType::UInt32 => Series::new(name.as_str().into(), vec![Option::<u32>::None; left.height()]),
                            DataType::Boolean => Series::new(name.as_str().into(), vec![Option::<bool>::None; left.height()]),
                            DataType::List(inner) => {
                                match *inner {
                                    DataType::Float64 => Series::new(name.as_str().into(), Vec::<Option<f64>>::new()),
                                    DataType::Int64 => Series::new(name.as_str().into(), Vec::<Option<i64>>::new()),
                                    _ => Series::new(name.as_str().into(), Vec::<Option<String>>::new()),
                                }
                            }
                            _ => Series::new_null(name.as_str().into(), left.height()),
                        };
                        left = left.hstack(&[s.into()])?;
                    }
                }
                // Reorder right to match left column order for vstack
                let final_order = left.get_column_names();
                let mut reordered_right_cols: Vec<Column> = Vec::with_capacity(final_order.len());
                for name in &final_order {
                    let s = right.column(name.as_str())?.clone();
                    reordered_right_cols.push(s);
                }
                let right_reordered = DataFrame::new(reordered_right_cols)?;
                left.vstack(&right_reordered)?
            }
        }
        Err(_) => new_df.clone(),
    };
    {
        let guard = store.0.lock();
        guard.rewrite_table_df(&table_path, combined)?;
    }
    crate::tprintln!("[EXEC_INSERT] rewrite_table rows={} took={:?} total={:?}", new_df.height(), __t_rewrite.elapsed(), __t0.elapsed());
    Ok(serde_json::json!({"status":"ok", "inserted": new_df.height()}))
}

use std::path::PathBuf;
use std::fs;
use anyhow::Result;
use polars::prelude::*;
use polars::prelude::StatisticsOptions;

use super::{Record, Store};
use crate::tprintln;

pub(crate) fn parse_chunk_min_max(name: &str) -> Option<(i64, i64)> {
    // Expect: data-<min>-<max>-<ts>.parquet
    let base = name.strip_prefix("data-")?.strip_suffix(".parquet")?;
    let parts: Vec<&str> = base.split('-').collect();
    if parts.len() < 3 { return None; }
    let min_t = parts[0].parse::<i64>().ok()?;
    let max_t = parts[1].parse::<i64>().ok()?;
    Some((min_t, max_t))
}

impl Store {
    pub fn filter_df(&self, table: &str, cols: &[String], t0: Option<i64>, t1: Option<i64>) -> Result<DataFrame> {
        let dir = self.db_dir(table);
        let mut wanted: Vec<String> = cols.iter().cloned().collect();
        // Ensure _time present only for time-series tables (*.time)
        let is_time_table = table.ends_with(".time");
        if is_time_table && !wanted.iter().any(|c| c == "_time") { wanted.insert(0, "_time".into()); }
        let mut dfs: Vec<DataFrame> = Vec::new();
        if dir.exists() {
            let mut files: Vec<PathBuf> = Vec::new();
            for entry in fs::read_dir(&dir)? {
                let p = entry?.path();
                if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                    if name == "data.parquet" || (name.starts_with("data-") && name.ends_with(".parquet")) {
                        // If time filter provided and chunk is time-ranged, prune by filename
                        if name.starts_with("data-") {
                            if let Some((min_t, max_t)) = parse_chunk_min_max(name) {
                                if let Some(lo) = t0 { if max_t < lo { continue; } }
                                if let Some(hi) = t1 { if min_t > hi { continue; } }
                            }
                        }
                        files.push(p);
                    }
                }
            }
            files.sort();
            for p in files {
                // Read available columns from parquet without pre-filtering. We will project
                // and synthesize missing requested columns after stacking.
                let mut reader = ParquetReader::new(std::fs::File::open(&p)?);
                let mut df = reader.finish()?;
                if (t0.is_some() || t1.is_some()) && is_time_table {
                    if df.get_column_names().iter().any(|c| c.as_str() == "_time") {
                        let mut lf = df.lazy();
                        if let Some(lo) = t0 { lf = lf.filter(col("_time").gt_eq(lit(lo))); }
                        if let Some(hi) = t1 { lf = lf.filter(col("_time").lt_eq(lit(hi))); }
                        df = lf.collect()?;
                    }
                }
                dfs.push(df);
            }
        }
        if dfs.is_empty() {
            // Empty with requested columns
            let mut cols_out: Vec<Column> = Vec::new();
            for c in wanted {
                if c == "_time" {
                    cols_out.push(Series::new("_time".into(), Vec::<i64>::new()).into());
                } else {
                    cols_out.push(Series::new(c.into(), Vec::<Option<f64>>::new()).into());
                }
            }
            return Ok(DataFrame::new(cols_out)?);
        }
        let mut out = dfs.remove(0);
        for df in dfs.into_iter() { out.vstack_mut(&df)?; }
        // Ensure all requested columns exist; if missing in parquet, synthesize null columns based on schema
        let present: std::collections::HashSet<String> = out
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let schema = self.load_schema(table).unwrap_or_default();
        let mut to_add: Vec<Column> = Vec::new();
        for w in &wanted {
            if present.contains(w) { continue; }
            if w == "_time" { continue; }
            if let Some(dt) = schema.get(w) {
                let s: Column = match dt {
                    DataType::Int64 => Series::new(w.as_str().into(), vec![None::<i64>; out.height()]).into(),
                    DataType::Float64 => Series::new(w.as_str().into(), vec![None::<f64>; out.height()]).into(),
                    DataType::String => Series::new(w.as_str().into(), vec![None::<String>; out.height()]).into(),
                    DataType::List(inner) if matches!(**inner, DataType::Float64) => {
                        Series::full_null(w.as_str().into(), out.height(), &DataType::List(Box::new(DataType::Float64))).into()
                    }
                    DataType::List(inner) if matches!(**inner, DataType::Int64) => {
                        Series::full_null(w.as_str().into(), out.height(), &DataType::List(Box::new(DataType::Int64))).into()
                    }
                    _ => Series::new(w.as_str().into(), vec![None::<f64>; out.height()]).into(),
                };
                to_add.push(s);
            }
        }
        if !to_add.is_empty() { out.hstack_mut(&to_add)?; }
        // Finally, project and order columns to the requested list when possible.
        // Keep existing columns if some requested are missing (they may not be in schema),
        // but tests pass requested columns that we either read or synthesized.
        // Build projection list of names
        let mut project: Vec<String> = Vec::new();
        for w in &wanted {
            if out.get_column_names().iter().any(|c| c.as_str() == w.as_str()) {
                project.push(w.to_string());
            }
        }
        if !project.is_empty() {
            out = out.select(&project)?;
        }
        Ok(out)
    }

    pub fn read_df(&self, table: &str) -> Result<DataFrame> {
        let dir = self.db_dir(table);
        let mut dfs: Vec<DataFrame> = Vec::new();
        if dir.exists() {
            let mut files: Vec<PathBuf> = Vec::new();
            for entry in fs::read_dir(&dir)? {
                let p = entry?.path();
                if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                    if (name.starts_with("data-") && name.ends_with(".parquet")) || name == "data.parquet" {
                        files.push(p);
                    }
                }
            }
            files.sort();
            for p in files {
                let f = std::fs::File::open(&p)?;
                let reader = ParquetReader::new(f);
                let df = reader.finish()?;
                dfs.push(df);
            }
        }
        if dfs.is_empty() {
            // Return empty dataframe with schema from schema.json if present.
            // Only include `_time` automatically for time-series tables (*.time).
            let mut cols: Vec<Column> = Vec::new();
            if table.ends_with(".time") {
                cols.push(Series::new("_time".into(), Vec::<i64>::new()).into());
            }
            let schema = self.load_schema(table).unwrap_or_default();
            for (name, dt) in schema.into_iter() {
                let s: Column = match dt {
                    DataType::Int64 => Series::new((&name).into(), Vec::<Option<i64>>::new()).into(),
                    DataType::Float64 => Series::new((&name).into(), Vec::<Option<f64>>::new()).into(),
                    DataType::String => Series::new((&name).into(), Vec::<Option<String>>::new()).into(),
                    // For vectors (List(Float64)), create an empty List series (0 height)
                    DataType::List(inner) if matches!(*inner, DataType::Float64) => {
                        let empty: Vec<Option<Series>> = Vec::new();
                        Series::new((&name).into(), empty).into()
                    }
                    DataType::List(inner) if matches!(*inner, DataType::Int64) => {
                        let empty: Vec<Option<Series>> = Vec::new();
                        Series::new((&name).into(), empty).into()
                    }
                    _ => Series::new((&name).into(), Vec::<Option<f64>>::new()).into(),
                };
                cols.push(s.into());
            }
            return Ok(DataFrame::new(cols)?);
        }
        let mut out = dfs.remove(0);
        for df in dfs.into_iter() { out.vstack_mut(&df)?; }
        Ok(out)
    }

    pub fn rewrite_table_df(&self, table: &str, mut df: DataFrame) -> Result<()> {
        let __t0 = std::time::Instant::now();
        // Remove existing parquet files and legacy file, then write df as a single new chunk and update schema
        let dir = self.db_dir(table);
        fs::create_dir_all(&dir).ok();
        // delete existing chunk files
        let __t_scan_rm0 = std::time::Instant::now();
        if dir.exists() {
            for entry in fs::read_dir(&dir)? {
                let _p = entry?.path();
            }
        }
        tprintln!("[STORAGE] rewrite_table_df: pre-scan dir='{}' took={:?}", dir.display(), __t_scan_rm0.elapsed());

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
        tprintln!("[STORAGE] rewrite_table_df: removed old parquet files took={:?}", __t_rm.elapsed());

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
        super::schema::save_schema_with_locks(self, table, &schema, &locks)?;
        tprintln!("[STORAGE] rewrite_table_df: update schema took={:?}", __t_schema.elapsed());
        // For regular tables: if partitions are defined, write partitioned files.
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
                                            if f.fract() == 0.0 { (f as i64).to_string() } else { f.to_string() }
                                        }
                                        _ => av.to_string(),
                                    }
                                };
                                for i in 0..n {
                                    let mut key_parts: Vec<String> = Vec::with_capacity(partitions.len());
                                    for pcol in &partitions {
                                        let s = df.column(pcol).ok();
                                        let val = s
                                            .and_then(|ser| ser.get(i).ok())
                                            .map(|av| val_to_string(av))
                                            .unwrap_or_else(|| "_NULL".to_string());
                                        key_parts.push(format!("{}={}", pcol, val));
                                    }
                                    let key = key_parts.join("/");
                                    groups.entry(key).or_default().push(i);
                                }
                                tprintln!("[STORAGE] rewrite_table_df: group by partitions took={:?}", __t_group.elapsed());
                                // Write each group as a parquet file under subdir
                                let mut parts_written = 0usize;
                                let __t_write_parts = std::time::Instant::now();
                                for (_key, idxs) in groups.into_iter() {
                                    // Take subset rows
                                    let idx_vec: Vec<u32> = idxs.into_iter().map(|i| i as u32).collect();
                                    let idx_ca = UInt32Chunked::from_vec("".into(), idx_vec);
                                    let df_part = df.take(&idx_ca)?;
                                    // Regular tables may not have _time; handle gracefully
                                    let (min_t, max_t) = match df_part.column("_time") {
                                        Ok(c) => {
                                            let ci = c.i64();
                                            if let Ok(ci) = ci { (ci.min().unwrap_or(0), ci.max().unwrap_or(0)) } else { (0, 0) }
                                        }
                                        Err(_) => (0, 0),
                                    };
                                    use std::time::{SystemTime, UNIX_EPOCH};
                                    let now_ms: u128 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
                                    let fname = format!("data-{}-{}-{}.parquet", min_t, max_t, now_ms);
                                    let path = dir.join(fname);
                                    let mut file = std::fs::File::create(&path)?;
                                    ParquetWriter::new(&mut file)
                                        .with_statistics(StatisticsOptions::default())
                                        .finish(&mut df_part.clone())?;
                                    parts_written += 1;
                                }
                                tprintln!("[STORAGE] rewrite_table_df: wrote {} partition files took={:?}", parts_written, __t_write_parts.elapsed());
                                wrote_partitioned = true;
                            }
                        }
                    }
                }
            }
            if wrote_partitioned {
                tprintln!("[STORAGE] rewrite_table_df: partitioned total took={:?}", __t0.elapsed());
                return Ok(());
            } else {
                let path = self.db_file(table);
                let __t_write = std::time::Instant::now();
                let mut file = std::fs::File::create(&path)?;
                ParquetWriter::new(&mut file)
                    .with_statistics(StatisticsOptions::default())
                    .finish(&mut df)?;
                tprintln!("[STORAGE] rewrite_table_df: wrote single parquet rows={} took={:?} total={:?}", df.height(), __t_write.elapsed(), __t0.elapsed());
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
        tprintln!("[STORAGE] rewrite_table_df: wrote time-table parquet rows={} took={:?} total={:?}", df.height(), __t_write_ts.elapsed(), __t0.elapsed());
        Ok(())
    }

    pub fn write_records(&self, table: &str, records: &[Record]) -> Result<()> {
        use std::collections::HashMap;
        use std::time::UNIX_EPOCH;

        fs::create_dir_all(self.db_dir(table))?;

        // Build list of all columns seen in this batch
        let mut col_names: Vec<String> = Vec::new();
        for r in records {
            for k in r.sensors.keys() {
                if !col_names.iter().any(|s| s == k) { col_names.push(k.clone()); }
            }
        }
        col_names.sort();

        // Load existing schema (if any) and infer from incoming records
        let (mut schema, locks) = self.load_schema_with_locks(table).unwrap_or((std::collections::HashMap::new(), std::collections::HashSet::new()));
        let inferred = super::Store::infer_dtypes(records, &col_names);
        // Merge respecting locks
        for (k, dt) in inferred {
            let merged = match schema.get(&k) {
                None => dt,
                Some(existing) => {
                    if locks.contains(&k) { existing.clone() } else { super::schema::merge_dtype(existing.clone(), dt) }
                }
            };
            schema.insert(k, merged);
        }
        let locks = locks;

        // Build the set of columns to write as the union of schema keys and observed record keys
        // This ensures schema-declared columns (e.g., VECTOR) are present even if missing in incoming rows
        let mut write_names: Vec<String> = schema.keys().cloned().collect();
        for k in &col_names { if !write_names.iter().any(|w| w == k) { write_names.push(k.clone()); } }
        write_names.sort();

        // Prepare column buffers according to merged schema
        let mut times: Vec<i64> = Vec::with_capacity(records.len());
        let mut f64_cols: HashMap<String, Vec<Option<f64>>> = HashMap::new();
        let mut i64_cols: HashMap<String, Vec<Option<i64>>> = HashMap::new();
        let mut str_cols: HashMap<String, Vec<Option<String>>> = HashMap::new();
        let mut vec_cols: HashMap<String, Vec<Option<Vec<f64>>>> = HashMap::new();
        for name in &write_names {
            match schema.get(name) {
                Some(DataType::String) => { str_cols.insert(name.clone(), Vec::with_capacity(records.len())); },
                Some(DataType::Int64) => { i64_cols.insert(name.clone(), Vec::with_capacity(records.len())); },
                Some(DataType::List(inner)) if matches!(**inner, DataType::Float64) => {
                    vec_cols.insert(name.clone(), Vec::with_capacity(records.len()));
                }
                _ => { f64_cols.insert(name.clone(), Vec::with_capacity(records.len())); },
            }
        }

        for r in records {
            times.push(r._time);
            for name in &write_names {
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
                    Some(DataType::List(inner)) if matches!(**inner, DataType::Float64) => {
                        // VECTOR column: accept arrays or string-encoded arrays; push None if missing
                        let entry = vec_cols.get_mut(name).unwrap();
                        let v: Option<Vec<f64>> = r.sensors.get(name).and_then(|val| match val {
                            serde_json::Value::Array(a) => {
                                let mut out: Vec<f64> = Vec::with_capacity(a.len());
                                for e in a {
                                    if let Some(n) = e.as_f64() {
                                        out.push(n);
                                    } else if let Some(s) = e.as_str() {
                                        if let Ok(n) = s.trim().parse::<f64>() { out.push(n); } else { return None; }
                                    } else { return None; }
                                }
                                Some(out)
                            }
                            serde_json::Value::String(s) => {
                                // tolerate formats: "[1,2,3]" or "1,2,3"
                                let t = s.trim().trim_start_matches('[').trim_end_matches(']');
                                let mut out: Vec<f64> = Vec::new();
                                for part in t.split(',') {
                                    let p = part.trim();
                                    if p.is_empty() { continue; }
                                    if let Ok(n) = p.parse::<f64>() { out.push(n); } else { return None; }
                                }
                                if out.is_empty() { None } else { Some(out) }
                            }
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

        // Create series and assemble into DataFrame
        let is_time_table = table.ends_with(".time");
        // Determine frame height for constructing null/list columns
        let height: usize = if is_time_table {
            times.len()
        } else {
            if let Some((_, v)) = f64_cols.iter().next() { v.len() }
            else if let Some((_, v)) = i64_cols.iter().next() { v.len() }
            else if let Some((_, v)) = str_cols.iter().next() { v.len() }
            else if let Some((_, v)) = vec_cols.iter().next() { v.len() }
            else { 0 }
        };
        let mut cols: Vec<Column> = Vec::with_capacity(write_names.len() + 1);

        if is_time_table {
            // Only include `_time` column in stored parquet for time tables
            cols.push(Series::new("_time".into(), times).into());
            // Time tables: other columns from sensors (excluding `_time` which is authoritative)
            for (name, vals) in f64_cols.into_iter() { if name != "_time" { cols.push(Series::new(name.into(), vals).into()); } }
            for (name, vals) in i64_cols.into_iter() { if name != "_time" { cols.push(Series::new(name.into(), vals).into()); } }
            for (name, vals) in str_cols.into_iter() { if name != "_time" { cols.push(Series::new(name.into(), vals).into()); } }
            // vector cols
            for (name, vals) in vec_cols.into_iter() {
                if name == "_time" { continue; }
                if vals.iter().all(|v| v.is_none()) {
                    // Explicit full-null List(Float64) to preserve dtype in parquet
                    let s = Series::full_null((&name).into(), height, &DataType::List(Box::new(DataType::Float64)));
                    cols.push(s.into());
                } else {
                    let mut cells: Vec<Option<Series>> = Vec::with_capacity(vals.len());
                    for opt in vals {
                        if let Some(v) = opt { cells.push(Some(Series::new("".into(), v))); } else { cells.push(None); }
                    }
                    cols.push(Series::new(name.into(), cells).into());
                }
            }
        } else {
            // Regular tables: include payload columns; They DO NOT HAVE _time columns by default. Only time tables get a mandatory _time column
            for (name, vals) in f64_cols.into_iter() { cols.push(Series::new(name.into(), vals).into()); }
            for (name, vals) in i64_cols.into_iter() { cols.push(Series::new(name.into(), vals).into()); }
            for (name, vals) in str_cols.into_iter() { cols.push(Series::new(name.into(), vals).into()); }
            for (name, vals) in vec_cols.into_iter() {
                if vals.iter().all(|v| v.is_none()) {
                    // Explicit full-null List(Float64) to preserve dtype in parquet
                    let s = Series::full_null((&name).into(), height, &DataType::List(Box::new(DataType::Float64)));
                    cols.push(s.into());
                } else {
                    let mut cells: Vec<Option<Series>> = Vec::with_capacity(vals.len());
                    for opt in vals {
                        if let Some(v) = opt { cells.push(Some(Series::new("".into(), v))); } else { cells.push(None); }
                    }
                    cols.push(Series::new(name.into(), cells).into());
                }
            }
        }
        let mut df = DataFrame::new(cols)?;

        // Sort by _time ascending for time tables only
        if is_time_table {
            let opts = SortMultipleOptions { descending: vec![false], ..Default::default() };
            df = df.sort(["_time"], opts)?;
        }

        // Persist chunk for time tables or single file for regular tables without partitions
        if !is_time_table {
            // If regular table and no partitions set, write/replace single data.parquet file
            let sp = self.schema_path(table);
            let parts = if sp.exists() {
                if let Ok(text) = fs::read_to_string(&sp) {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                        v.get("partitions").and_then(|x| x.as_array()).map(|a| a.len()).unwrap_or(0)
                    } else { 0 }
                } else { 0 }
            } else { 0 };
            if parts == 0 {
                let path = self.db_file(table);
                let mut file = std::fs::File::create(&path)?;
                ParquetWriter::new(&mut file)
                    .with_statistics(StatisticsOptions::default())
                    .finish(&mut df)?;
                // Update schema.json: merge existing declared schema with columns present in this df
                // Do NOT drop previously declared columns (e.g., VECTOR) that may be missing in this write.
                use std::collections::{HashMap, HashSet};
                let (mut existing_schema, existing_locks) = self
                    .load_schema_with_locks(table)
                    .unwrap_or((HashMap::new(), HashSet::new()));
                // Start from existing schema to preserve declared columns and dtypes
                let mut new_schema: HashMap<String, DataType> = existing_schema.drain().collect();
                // Overlay actual columns present in df (excluding _time)
                for name in df.get_column_names() {
                    if name.as_str() == "_time" { continue; }
                    let dt = df.column(name.as_str())?.dtype().clone();
                    new_schema.insert(name.to_string(), dt);
                }
                // Preserve locks that still refer to columns in the merged schema
                let mut new_locks: HashSet<String> = HashSet::new();
                for k in existing_locks { if new_schema.contains_key(&k) { new_locks.insert(k); } }
                super::schema::save_schema_with_locks(self, table, &new_schema, &new_locks)?;
                return Ok(());
            }
            // Partitions are defined for a regular table: delegate to partition-aware rewrite_table_df
            // This will remove previous parquet files and write one file per partition group.
            self.rewrite_table_df(table, df.clone())?;
            return Ok(());
        }

        // Write chunked file with min/max/time suffix
        let (min_t, max_t) = if let Ok(c) = df.column("_time") {
            let ca = c.i64();
            if let Ok(ci) = ca { (ci.min().unwrap_or(0), ci.max().unwrap_or(0)) } else { (0, 0) }
        } else { (0, 0) };
        let now_ms: u128 = UNIX_EPOCH.elapsed().unwrap().as_millis();
        let fname = format!("data-{}-{}-{}.parquet", min_t, max_t, now_ms);
        let path = self.db_dir(table).join(fname);
        let mut file = std::fs::File::create(&path)?;
        ParquetWriter::new(&mut file)
            .with_statistics(StatisticsOptions::default())
            .finish(&mut df)?;

        // Save merged schema with locks preserved
        super::schema::save_schema_with_locks(self, table, &schema, &locks)?;

        Ok(())
    }
}

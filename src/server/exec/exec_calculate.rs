//! exec_calculate
//! --------------
//! CALCULATE command implementation extracted from exec.rs. Keep this logic here
//! so the main dispatcher remains thin.

use anyhow::Result;
use polars::prelude::*;
use crate::storage::SharedStore;
use crate::server::query::query_common::Query;

pub fn handle_calculate(store: &SharedStore, target_sensor: &str, q: &Query) -> Result<serde_json::Value> {
    // run select
    let df = crate::server::exec::exec_select::run_select(store, q)?;
    // Expect columns: _time and one value column
    let mut records = Vec::with_capacity(df.height());
    let time_col = df.column("_time").ok();
    let time = time_col.and_then(|c| c.i64().ok()).ok_or_else(|| anyhow::anyhow!("_time not in result for CALCULATE"))?;

    // pick the first non-time column for value
    let val_series_name = df.get_column_names().into_iter().find(|n| n.as_str() != "_time").ok_or_else(|| anyhow::anyhow!("No value column to save"))?;
    let val_series = df.column(val_series_name)?;

    match val_series.dtype() {
        DataType::Float64 => {
            let vals = val_series.f64()?;
            for i in 0..df.height() {
                let t = time.get(i).ok_or_else(|| anyhow::anyhow!("bad time index"))?;
                if let Some(v) = vals.get(i) {
                    let mut map = serde_json::Map::new();
                    map.insert(target_sensor.to_string(), serde_json::json!(v));
                    records.push(crate::storage::Record { _time: t, sensors: map });
                }
            }
        }
        DataType::Int64 => {
            let vals = val_series.i64()?;
            for i in 0..df.height() {
                let t = time.get(i).ok_or_else(|| anyhow::anyhow!("bad time index"))?;
                if let Some(v) = vals.get(i) {
                    let mut map = serde_json::Map::new();
                    map.insert(target_sensor.to_string(), serde_json::json!(v));
                    records.push(crate::storage::Record { _time: t, sensors: map });
                }
            }
        }
        DataType::String => {
            let vals = val_series.str()?;
            for i in 0..df.height() {
                let t = time.get(i).ok_or_else(|| anyhow::anyhow!("bad time index"))?;
                if let Some(v) = vals.get(i) {
                    let mut map = serde_json::Map::new();
                    map.insert(target_sensor.to_string(), serde_json::json!(v.to_string()));
                    records.push(crate::storage::Record { _time: t, sensors: map });
                }
            }
        }
        _ => {}
    }
    // Persist into the source time table referenced by the query's FROM (as in original behavior)
    let guard = store.0.lock();
    let tbl = q.base_table.as_ref().ok_or_else(|| anyhow::anyhow!("CALCULATE requires a FROM source to persist results"))?;
    let table_name = tbl.table_name().ok_or_else(|| anyhow::anyhow!("CALCULATE requires a table, not a subquery"))?;
    guard.write_records(table_name, &records)?;
    Ok(serde_json::json!({"status":"ok","saved": records.len()}))
}

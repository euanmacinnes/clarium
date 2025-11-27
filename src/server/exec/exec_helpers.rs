//! exec_helpers
//! -------------
//! Shared helpers for the exec subsystem. Keep generic utilities here and keep
//! exec.rs thin. When adding new helpers, prefer placing them here instead of
//! inflating exec.rs. This makes it easier for future LLM-driven edits to keep
//! the dispatcher small and delegate logic into focused modules.

use anyhow::Result;
use polars::prelude::*;

/// Convert a DataFrame into a serde_json::Value for HTTP/pgwire simple protocol.
pub fn dataframe_to_json(df: &DataFrame) -> serde_json::Value {
    let (cols, data) = dataframe_to_tabular(df);
    let mut out: Vec<serde_json::Value> = Vec::with_capacity(df.height());
    for row in data {
        let mut map = serde_json::Map::new();
        for (i, c) in cols.iter().enumerate() {
            match &row[i] {
                Some(v) => { map.insert(c.clone(), serde_json::Value::String(v.clone())); }
                None => { map.insert(c.clone(), serde_json::Value::Null); }
            }
        }
        out.push(serde_json::Value::Object(map));
    }
    serde_json::Value::Array(out)
}

/// Convert a DataFrame into a column header vector and rows of optional strings
/// suitable for RowDescription/DataRow emission.
pub fn dataframe_to_tabular(df: &DataFrame) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    let cols: Vec<String> = df.get_column_names().into_iter().map(|s| s.to_string()).collect();
    let mut data: Vec<Vec<Option<String>>> = Vec::with_capacity(df.height());
    for row_idx in 0..df.height() {
        let mut row: Vec<Option<String>> = Vec::with_capacity(cols.len());
        for c in &cols {
            let s = df.column(c.as_str()).unwrap();
            let av = s.get(row_idx);
            let cell = match av {
                Ok(AnyValue::Int64(v)) => Some(v.to_string()),
                Ok(AnyValue::Int32(v)) => Some((v as i64).to_string()),
                Ok(AnyValue::Float64(v)) => Some(v.to_string()),
                Ok(AnyValue::Boolean(v)) => Some(if v {"t".into()} else {"f".into()}),
                Ok(AnyValue::String(v)) => Some(v.to_string()),
                Ok(AnyValue::StringOwned(v)) => Some(v.to_string()),
                Ok(AnyValue::Null) => None,
                _ => None,
            };
            row.push(cell);
        }
        data.push(row);
    }
    (cols, data)
}

/// Convenience: staged SELECT execution returning a DataFrame.
pub fn execute_select_df(store: &crate::storage::SharedStore, q: &crate::query::Query) -> Result<DataFrame> {
    crate::server::exec::exec_select::run_select(store, q)
}

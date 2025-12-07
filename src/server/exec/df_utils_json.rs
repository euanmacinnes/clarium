use anyhow::{anyhow, Result};
use polars::prelude::*;
use serde_json::Value;
use tracing::debug;

/// Convert a serde_json::Value into a Polars DataFrame.
/// Supported roots:
/// - Array of objects: rows per element, columns = union of keys
/// - Object: single-row frame, keys as columns
/// - Array of scalars: single column "value"
/// - Empty array -> empty single-column frame with no rows
pub fn json_to_df(j: &Value) -> Result<DataFrame> {
    match j {
        Value::Array(arr) => {
            if arr.is_empty() {
                // empty -> empty df with zero columns
                return Ok(DataFrame::new(vec![])?);
            }
            // detect if array of objects
            let all_obj = arr.iter().all(|v| v.is_object());
            if all_obj {
                json_array_objects_to_df(arr)
            } else {
                // treat as array of scalars -> single column "value" as strings for robustness
                let mut vals: Vec<String> = Vec::with_capacity(arr.len());
                for v in arr {
                    vals.push(value_to_string(v));
                }
                let ser = Series::new("value".into(), vals);
                Ok(DataFrame::new(vec![ser.into()])?)
            }
        }
        Value::Object(map) => {
            // Single row: represent values as strings (null/missing -> empty string) for robustness
            let mut cols: Vec<polars::prelude::Column> = Vec::with_capacity(map.len());
            for (k, v) in map.iter() {
                let ser = Series::new(k.as_str().into(), vec![value_to_string(v)]);
                cols.push(ser.into());
            }
            Ok(DataFrame::new(cols)?)
        }
        _ => Err(anyhow!("Unsupported JSON root for FROM: expected array or object")),
    }
}

fn json_array_objects_to_df(arr: &Vec<Value>) -> Result<DataFrame> {
    // Collect union of keys
    use std::collections::BTreeSet;
    let mut keys: BTreeSet<String> = BTreeSet::new();
    for v in arr.iter() {
        if let Value::Object(m) = v {
            for k in m.keys() { keys.insert(k.clone()); }
        }
    }
    let key_list: Vec<String> = keys.into_iter().collect();
    debug!(target: "clarium::exec", keys=?key_list, "json_to_df: array-of-objects keys inferred");

    // For simplicity and robustness, build columns with string representation.
    // Nulls/missing values are represented as empty strings.
    let mut cols: Vec<polars::prelude::Column> = Vec::with_capacity(key_list.len());
    for k in &key_list {
        let mut col: Vec<String> = Vec::with_capacity(arr.len());
        for v in arr.iter() {
            match v {
                Value::Object(m) => {
                    if let Some(val) = m.get(k) {
                        if val.is_null() { col.push(String::new()); }
                        else { col.push(value_to_string(val)); }
                    } else {
                        col.push(String::new());
                    }
                }
                _ => col.push(String::new()),
            }
        }
        let s = Series::new(k.as_str().into(), col);
        cols.push(s.into());
    }
    Ok(DataFrame::new(cols)?)
}

#[inline]
fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        _ => v.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn array_of_objects() {
        let j = json!([
            {"a": 1, "b": "x"},
            {"a": 2},
            {"b": "y", "c": true}
        ]);
        let df = json_to_df(&j).unwrap();
        let cols = df.get_column_names();
        assert!(cols.contains(&"a".to_string()));
        assert!(cols.contains(&"b".to_string()));
        assert!(cols.contains(&"c".to_string()));
        assert_eq!(df.height(), 3);
    }

    #[test]
    fn object_single_row() {
        let j = json!({"x": 10, "y": "hi"});
        let df = json_to_df(&j).unwrap();
        assert_eq!(df.height(), 1);
        let cols = df.get_column_names();
        assert!(cols.contains(&"x".to_string()));
        assert!(cols.contains(&"y".to_string()));
    }

    #[test]
    fn array_of_scalars() {
        let j = json!([1, 2, 3]);
        let df = json_to_df(&j).unwrap();
        assert_eq!(df.height(), 3);
        assert_eq!(df.get_column_names(), vec!["value".to_string()]);
    }
}

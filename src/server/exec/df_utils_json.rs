use anyhow::{anyhow, Result};
use polars::prelude::*;
use serde_json::Value;
use tracing::debug;
// Avoid requiring 'static lifetimes for column names; own Strings instead.

/// Convert a serde_json::Value into a Polars DataFrame.
/// Supported roots:
/// - Array of objects: rows per element, columns = union of keys
/// - Object: single-row frame, keys as columns
/// - Array of scalars: single column "value"
/// - Empty array -> empty single-column frame with no rows
pub fn json_to_df(j: &Value) -> Result<DataFrame> {
    json_to_df_with_opts(j, &JsonReadOptions::default())
}

// ---------------- Inference + options ----------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InferType { Bool, Int64, Float64, Utf8 }

impl InferType {
    #[inline]
    fn widen(a: InferType, b: InferType) -> InferType {
        use InferType::*;
        match (a, b) {
            (Utf8, _) | (_, Utf8) => Utf8,
            (Float64, _) | (_, Float64) => Float64,
            (Int64, Int64) => Int64,
            (Int64, Bool) | (Bool, Int64) => Int64,
            (Bool, Bool) => Bool,
        }
    }
}

#[derive(Clone, Debug)]
struct JsonReadOptions { infer_sample_rows: usize }

impl Default for JsonReadOptions { fn default() -> Self { Self { infer_sample_rows: 1024 } } }

fn infer_type_of_value(v: &Value) -> Option<InferType> {
    use InferType::*;
    match v {
        Value::Bool(_) => Some(Bool),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() { Some(Int64) } else { Some(Float64) }
        }
        Value::String(_) => Some(Utf8),
        Value::Null => None,
        Value::Array(_) | Value::Object(_) => Some(Utf8),
    }
}

fn json_to_df_with_opts(j: &Value, opts: &JsonReadOptions) -> Result<DataFrame> {
    match j {
        Value::Array(arr) => array_root_to_df(arr, opts),
        Value::Object(map) => object_root_to_df(map),
        _ => Err(anyhow!("Unsupported JSON root for FROM: expected array or object")),
    }
}

fn array_root_to_df(arr: &Vec<Value>, opts: &JsonReadOptions) -> Result<DataFrame> {
    if arr.is_empty() { return Ok(DataFrame::new(vec![])?); }
    let all_obj = arr.iter().all(|v| v.is_object());
    if all_obj { return array_of_objects_to_df(arr, opts); }
    // Array of scalars: infer single column type
    let mut ty: Option<InferType> = None;
    let mut seen = 0usize;
    for v in arr.iter() {
        if seen >= opts.infer_sample_rows { break; }
        seen += 1;
        if let Some(t) = infer_type_of_value(v) {
            ty = Some(if let Some(prev) = ty { InferType::widen(prev, t) } else { t });
        }
    }
    let t = ty.unwrap_or(InferType::Utf8);
    let name: String = "value".to_string();
    use InferType::*;
    let col: polars::prelude::Column = match t {
        Bool => {
            let mut v: Vec<Option<bool>> = Vec::with_capacity(arr.len());
            for x in arr.iter() { v.push(x.as_bool()); }
            Series::new(name.clone().into(), v).into()
        }
        Int64 => {
            let mut v: Vec<Option<i64>> = Vec::with_capacity(arr.len());
            for x in arr.iter() {
                v.push(match x {
                    Value::Number(n) => if n.is_i64() { n.as_i64() } else if n.is_u64() { n.as_u64().map(|u| u as i64) } else { None },
                    _ => None,
                });
            }
            Series::new(name.clone().into(), v).into()
        }
        Float64 => {
            let mut v: Vec<Option<f64>> = Vec::with_capacity(arr.len());
            for x in arr.iter() { v.push(x.as_f64()); }
            Series::new(name.clone().into(), v).into()
        }
        Utf8 => {
            let mut v: Vec<Option<String>> = Vec::with_capacity(arr.len());
            for x in arr.iter() { v.push(value_to_opt_string(x)); }
            Series::new(name.clone().into(), v).into()
        }
    };
    Ok(DataFrame::new(vec![col])?)
}

fn object_root_to_df(map: &serde_json::Map<String, Value>) -> Result<DataFrame> {
    // Infer per-field types and materialize a single row with proper nulls
    let mut cols: Vec<polars::prelude::Column> = Vec::with_capacity(map.len());
    for (k, v) in map.iter() {
        let name: String = k.clone();
        let ty = infer_type_of_value(v).unwrap_or(InferType::Utf8);
        use InferType::*;
        let col: polars::prelude::Column = match ty {
            Bool => Series::new(name.clone().into(), vec![v.as_bool()]).into(),
            Int64 => {
                let vi = match v {
                    Value::Number(n) => if n.is_i64() { n.as_i64() } else if n.is_u64() { n.as_u64().map(|u| u as i64) } else { None },
                    _ => None,
                };
                Series::new(name.clone().into(), vec![vi]).into()
            }
            Float64 => Series::new(name.clone().into(), vec![v.as_f64()]).into(),
            Utf8 => Series::new(name.clone().into(), vec![value_to_opt_string(v)]).into(),
        };
        cols.push(col);
    }
    Ok(DataFrame::new(cols)?)
}

fn array_of_objects_to_df(arr: &Vec<Value>, opts: &JsonReadOptions) -> Result<DataFrame> {
    use std::collections::{BTreeSet, HashMap};
    // Union keys
    let mut keys: BTreeSet<String> = BTreeSet::new();
    for v in arr.iter() {
        if let Value::Object(m) = v { for k in m.keys() { keys.insert(k.clone()); } }
    }
    let key_list: Vec<String> = keys.into_iter().collect();
    debug!(target: "clarium::exec", keys=?key_list, "json_to_df: array-of-objects keys inferred");
    // First pass: infer per-key type from sample
    let mut inferred: HashMap<String, InferType> = HashMap::new();
    let mut sampled = 0usize;
    for v in arr.iter() {
        if sampled >= opts.infer_sample_rows { break; }
        sampled += 1;
        if let Value::Object(m) = v {
            for (k, val) in m.iter() {
                if let Some(t) = infer_type_of_value(val) {
                    let entry = inferred.entry(k.clone()).or_insert(t);
                    *entry = InferType::widen(*entry, t);
                }
            }
        }
    }
    // Default missing keys to Utf8
    for k in &key_list { inferred.entry(k.clone()).or_insert(InferType::Utf8); }

    // Second pass: materialize columns by inferred type
    let mut cols: Vec<polars::prelude::Column> = Vec::with_capacity(key_list.len());
    for k in &key_list {
        let ty = *inferred.get(k).unwrap_or(&InferType::Utf8);
        let name: String = k.clone();
        use InferType::*;
        let col: polars::prelude::Column = match ty {
            Bool => {
                let mut v: Vec<Option<bool>> = Vec::with_capacity(arr.len());
                for row in arr.iter() {
                    v.push(match row { Value::Object(m) => m.get(k).and_then(|x| x.as_bool()), _ => None });
                }
                Series::new(name.clone().into(), v).into()
            }
            Int64 => {
                let mut v: Vec<Option<i64>> = Vec::with_capacity(arr.len());
                for row in arr.iter() {
                    let vi = match row {
                        Value::Object(m) => m.get(k).and_then(|x| match x {
                            Value::Number(n) => if n.is_i64() { n.as_i64() } else if n.is_u64() { n.as_u64().map(|u| u as i64) } else { None },
                            _ => None,
                        }),
                        _ => None,
                    };
                    v.push(vi);
                }
                Series::new(name.clone().into(), v).into()
            }
            Float64 => {
                let mut v: Vec<Option<f64>> = Vec::with_capacity(arr.len());
                for row in arr.iter() {
                    let vf = match row { Value::Object(m) => m.get(k).and_then(|x| x.as_f64()), _ => None };
                    v.push(vf);
                }
                Series::new(name.clone().into(), v).into()
            }
            Utf8 => {
                let mut v: Vec<Option<String>> = Vec::with_capacity(arr.len());
                for row in arr.iter() {
                    let vs = match row { Value::Object(m) => m.get(k).map(|x| value_to_opt_string(x)), _ => None };
                    v.push(vs.unwrap_or(None));
                }
                Series::new(name.clone().into(), v).into()
            }
        };
        cols.push(col);
    }
    Ok(DataFrame::new(cols)?)
}

#[inline]
fn value_to_opt_string(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        _ => Some(v.to_string()),
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

    #[test]
    fn dtype_inference_scalars_ints() {
        let j = json!([1, 2, 3]);
        let df = json_to_df(&j).unwrap();
        let dt = df.column("value").unwrap().dtype().clone();
        assert!(matches!(dt, DataType::Int64));
    }

    #[test]
    fn dtype_inference_scalars_mixed_float() {
        let j = json!([1, 2.5, 3]);
        let df = json_to_df(&j).unwrap();
        let dt = df.column("value").unwrap().dtype().clone();
        assert!(matches!(dt, DataType::Float64));
    }

    #[test]
    fn dtype_inference_array_of_objects() {
        let j = json!([
            {"a": 1, "b": true, "c": "x"},
            {"a": 2, "b": null, "c": "y"}
        ]);
        let df = json_to_df(&j).unwrap();
        assert!(matches!(df.column("a").unwrap().dtype().clone(), DataType::Int64));
        assert!(matches!(df.column("b").unwrap().dtype().clone(), DataType::Boolean));
        assert!(matches!(df.column("c").unwrap().dtype().clone(), DataType::Utf8));
    }
}

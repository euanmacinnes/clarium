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
    let has = |name: &str| cols.iter().any(|c| c.as_str() == name);
    assert!(has("a"));
    assert!(has("b"));
    assert!(has("c"));
    assert_eq!(df.height(), 3);
}

#[test]
fn object_single_row() {
    let j = json!({"x": 10, "y": "hi"});
    let df = json_to_df(&j).unwrap();
    assert_eq!(df.height(), 1);
    let cols = df.get_column_names();
    let has = |name: &str| cols.iter().any(|c| c.as_str() == name);
    assert!(has("x"));
    assert!(has("y"));
}

#[test]
fn array_of_scalars() {
    let j = json!([1, 2, 3]);
    let df = json_to_df(&j).unwrap();
    assert_eq!(df.height(), 3);
    let cols = df.get_column_names();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].as_str(), "value");
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
    assert!(is_str_dtype(df.column("c").unwrap().dtype()));
}

fn is_str_dtype(dt: &DataType) -> bool {
    let s = format!("{:?}", dt);
    s.contains("Utf8") || s.contains("String")
}

#[test]
fn array_of_scalars_bools_with_nulls() {
    let j = json!([true, null, false]);
    let df = json_to_df(&j).unwrap();
    assert_eq!(df.height(), 3);
    let dt = df.column("value").unwrap().dtype().clone();
    assert!(matches!(dt, DataType::Boolean));
    // spot check null in the middle
    let s = df.column("value").unwrap();
    assert!(matches!(s.get(1).unwrap(), AnyValue::Null));
}

#[test]
fn array_of_scalars_mixed_numeric_with_null() {
    let j = json!([1, null, 2.5]);
    let df = json_to_df(&j).unwrap();
    let dt = df.column("value").unwrap().dtype().clone();
    assert!(matches!(dt, DataType::Float64));
    let s = df.column("value").unwrap();
    assert!(matches!(s.get(1).unwrap(), AnyValue::Null));
}

#[test]
fn array_of_scalars_mixed_string_and_number() {
    let j = json!( ["1", 2] );
    let df = json_to_df(&j).unwrap();
    let dt = df.column("value").unwrap().dtype().clone();
    assert!(is_str_dtype(&dt));
}

#[test]
fn array_of_objects_missing_and_nulls() {
    let j = json!([
        {"a": 1, "b": true},
        {"a": null},
        {"b": false},
    ]);
    let df = json_to_df(&j).unwrap();
    assert!(matches!(df.column("a").unwrap().dtype().clone(), DataType::Int64));
    assert!(matches!(df.column("b").unwrap().dtype().clone(), DataType::Boolean));
    // Check that missing becomes null
    let a = df.column("a").unwrap();
    let b = df.column("b").unwrap();
    assert!(matches!(a.get(1).unwrap(), AnyValue::Null));
    assert!(matches!(b.get(2).unwrap(), AnyValue::Boolean(false)));
    assert!(matches!(b.get(0).unwrap(), AnyValue::Boolean(true)));
}

#[test]
fn object_single_row_mixed_types_widening() {
    let j = json!({
        "i": 42,
        "f": 3.14,
        "b": true,
        "s": "x",
        "n": null
    });
    let df = json_to_df(&j).unwrap();
    assert!(matches!(df.column("i").unwrap().dtype().clone(), DataType::Int64));
    assert!(matches!(df.column("f").unwrap().dtype().clone(), DataType::Float64));
    assert!(matches!(df.column("b").unwrap().dtype().clone(), DataType::Boolean));
    assert!(is_str_dtype(df.column("s").unwrap().dtype()));
    assert!(is_str_dtype(df.column("n").unwrap().dtype()));
}

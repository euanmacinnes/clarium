use polars::prelude::*;

use crate::server::exec::vector_utils::{parse_vec_literal, extract_vec_f32};

#[test]
fn parse_vec_literal_accepts_various_formats() {
    assert_eq!(parse_vec_literal("[1,2,3]").unwrap(), vec![1.0,2.0,3.0]);
    assert_eq!(parse_vec_literal("1,2,3").unwrap(), vec![1.0,2.0,3.0]);
    assert_eq!(parse_vec_literal("1 2 3").unwrap(), vec![1.0,2.0,3.0]);
    assert_eq!(parse_vec_literal("(1 2 3)").unwrap(), vec![1.0,2.0,3.0]);
    // Quotes around the literal are allowed
    assert_eq!(parse_vec_literal("\"[4, 5, 6]\"").unwrap(), vec![4.0,5.0,6.0]);
}

#[test]
fn extract_vec_f32_from_list_series() {
    // Create a List(Float64) series: [[1.0, 2.0, 3.0]]
    let inner = Series::new("inner".into(), &[1.0f64, 2.0, 3.0]);
    let list = Series::new("vec".into(), &[inner]);
    let v = extract_vec_f32(&list, 0).unwrap();
    assert_eq!(v, vec![1.0f32, 2.0, 3.0]);
}

#[test]
fn extract_vec_f32_from_string_series() {
    let s = Series::new("vec".into(), &["1,2,3"]);
    let v = extract_vec_f32(&s, 0).unwrap();
    assert_eq!(v, vec![1.0f32,2.0,3.0]);
}

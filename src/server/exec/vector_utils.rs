//! Vector utilities: parsing and extraction helpers centralized for reuse
//!
//! Follows Polars 0.51+ guidelines: avoid Utf8 iterators, prefer Series::get + AnyValue.

use polars::prelude::*;

/// Parse a vector literal into f32 values.
/// Accepts forms like "[1, 2, 3]", "1,2,3", "1 2 3", or "(1 2 3)"; ignores surrounding quotes.
pub fn parse_vec_literal(s: &str) -> Option<Vec<f32>> {
    let mut txt = s.trim().trim_matches('"').trim_matches('\'').to_string();
    if txt.len() >= 2 {
        let first = txt.as_bytes()[0] as char;
        let last = txt.as_bytes()[txt.len() - 1] as char;
        if (first == '[' && last == ']') || (first == '(' && last == ')') {
            txt = txt[1..txt.len() - 1].to_string();
        }
    }
    // Replace whitespace with commas for easier split
    let txt = txt.replace(|c: char| c.is_whitespace(), ",");
    let mut out: Vec<f32> = Vec::new();
    for part in txt.split(',') {
        let p = part.trim();
        if p.is_empty() { continue; }
        match p.parse::<f32>() {
            Ok(v) => out.push(v),
            Err(_) => return None,
        }
    }
    let res = if out.is_empty() { None } else { Some(out) };
    if cfg!(debug_assertions) {
        crate::tprintln!("[vector_utils] parse_vec_literal len_in={} -> len_out={:?}", s.len(), res.as_ref().map(|v| v.len()));
    }
    res
}

/// Extract a row value from a Series as a vector of f32, supporting:
/// - Native List(Float64/Float32/Int64/Int32)
/// - String or other scalar encodings parsable by `parse_vec_literal`
///
/// Hygiene rules:
/// - Coerce numeric values f64→f32 at boundaries.
/// - If any element in a List is non-numeric, treat the entire cell as null (None) instead of fabricating values.
/// - Invalid formats never panic; return None.
pub fn extract_vec_f32(series: &Series, i: usize) -> Option<Vec<f32>> {
    match series.get(i) {
        Ok(AnyValue::List(inner)) => {
            let ser = inner;
            let mut out: Vec<f32> = Vec::with_capacity(ser.len());
            for li in 0..ser.len() {
                match ser.get(li) {
                    Ok(AnyValue::Float64(f)) => out.push(f as f32),
                    Ok(AnyValue::Float32(f)) => out.push(f),
                    Ok(AnyValue::Int64(iv)) => out.push(iv as f32),
                    Ok(AnyValue::Int32(iv)) => out.push(iv as f32),
                    // Any non-numeric inner value invalidates the whole cell (treated as null)
                    _ => return None,
                }
            }
            let r = if out.is_empty() { None } else { Some(out) };
            if cfg!(debug_assertions) { crate::tprintln!("[vector_utils] extract_vec_f32[List] len_out={:?}", r.as_ref().map(|v| v.len())); }
            r
        }
        Ok(AnyValue::String(s)) => { let r = parse_vec_literal(s); if cfg!(debug_assertions) { crate::tprintln!("[vector_utils] extract_vec_f32[String] parsed={:?}", r.as_ref().map(|v| v.len())); } r },
        Ok(AnyValue::StringOwned(s)) => { let r = parse_vec_literal(s.as_str()); if cfg!(debug_assertions) { crate::tprintln!("[vector_utils] extract_vec_f32[StringOwned] parsed={:?}", r.as_ref().map(|v| v.len())); } r },
        Ok(other) => { let r = parse_vec_literal(&other.to_string()); if cfg!(debug_assertions) { crate::tprintln!("[vector_utils] extract_vec_f32[Other] parsed={:?}", r.as_ref().map(|v| v.len())); } r },
        Err(_) => None,
    }
}

/// Same as `extract_vec_f32` but accepts a `&Column` reference, which is what `DataFrame::column` returns in Polars 0.51+.
pub fn extract_vec_f32_col(series: &Column, i: usize) -> Option<Vec<f32>> {
    match series.get(i) {
        Ok(AnyValue::List(inner)) => {
            let ser = inner;
            let mut out: Vec<f32> = Vec::with_capacity(ser.len());
            for li in 0..ser.len() {
                match ser.get(li) {
                    Ok(AnyValue::Float64(f)) => out.push(f as f32),
                    Ok(AnyValue::Float32(f)) => out.push(f),
                    Ok(AnyValue::Int64(iv)) => out.push(iv as f32),
                    Ok(AnyValue::Int32(iv)) => out.push(iv as f32),
                    _ => return None,
                }
            }
            let r = if out.is_empty() { None } else { Some(out) };
            if cfg!(debug_assertions) { crate::tprintln!("[vector_utils] extract_vec_f32_col[List] len_out={:?}", r.as_ref().map(|v| v.len())); }
            r
        }
        Ok(AnyValue::String(s)) => { let r = parse_vec_literal(s); if cfg!(debug_assertions) { crate::tprintln!("[vector_utils] extract_vec_f32_col[String] parsed={:?}", r.as_ref().map(|v| v.len())); } r },
        Ok(AnyValue::StringOwned(s)) => { let r = parse_vec_literal(s.as_str()); if cfg!(debug_assertions) { crate::tprintln!("[vector_utils] extract_vec_f32_col[StringOwned] parsed={:?}", r.as_ref().map(|v| v.len())); } r },
        Ok(other) => { let r = parse_vec_literal(&other.to_string()); if cfg!(debug_assertions) { crate::tprintln!("[vector_utils] extract_vec_f32_col[Other] parsed={:?}", r.as_ref().map(|v| v.len())); } r },
        Err(_) => None,
    }
}

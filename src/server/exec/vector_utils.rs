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
    if out.is_empty() { None } else { Some(out) }
}

/// Extract a row value from a Series as a vector of f32, supporting:
/// - Native List(Float64/Float32/Int64/Int32)
/// - String or other scalar encodings parsable by `parse_vec_literal`
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
                    Ok(other) => {
                        let s_owned = other.to_string();
                        if let Some(mut parsed) = parse_vec_literal(&s_owned) {
                            if parsed.is_empty() { out.push(0.0); } else { out.append(&mut parsed); }
                        } else {
                            out.push(0.0);
                        }
                    }
                    Err(_) => out.push(0.0),
                }
            }
            Some(out)
        }
        Ok(AnyValue::String(s)) => parse_vec_literal(s),
        Ok(AnyValue::StringOwned(s)) => parse_vec_literal(s.as_str()),
        Ok(other) => parse_vec_literal(&other.to_string()),
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
                    Ok(other) => {
                        let s_owned = other.to_string();
                        if let Some(mut parsed) = parse_vec_literal(&s_owned) {
                            if parsed.is_empty() { out.push(0.0); } else { out.append(&mut parsed); }
                        } else {
                            out.push(0.0);
                        }
                    }
                    Err(_) => out.push(0.0),
                }
            }
            Some(out)
        }
        Ok(AnyValue::String(s)) => parse_vec_literal(s),
        Ok(AnyValue::StringOwned(s)) => parse_vec_literal(s.as_str()),
        Ok(other) => parse_vec_literal(&other.to_string()),
        Err(_) => None,
    }
}

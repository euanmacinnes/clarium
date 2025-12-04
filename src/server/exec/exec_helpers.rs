//! exec_helpers
//! -------------
//! Shared helpers for the exec subsystem. Keep generic utilities here and keep
//! exec.rs thin. When adding new helpers, prefer placing them here instead of
//! inflating exec.rs. This makes it easier for future LLM-driven edits to keep
//! the dispatcher small and delegate logic into focused modules.

use anyhow::Result;
use polars::prelude::*;

use crate::server::query::Query;

/// Convert a DataFrame into a serde_json::Value for HTTP/pgwire simple protocol.
pub fn dataframe_to_json(df: &DataFrame) -> serde_json::Value {
    // Emit native JSON numbers/bools/strings/nulls to satisfy engine tests
    let cols: Vec<String> = df.get_column_names().into_iter().map(|s| s.to_string()).collect();
    let mut out: Vec<serde_json::Value> = Vec::with_capacity(df.height());
    for row_idx in 0..df.height() {
        let mut map = serde_json::Map::new();
        for c in &cols {
            let s = df.column(c.as_str()).unwrap();
            let av = s.get(row_idx);
            let jv = match av {
                Ok(AnyValue::Int64(v)) => serde_json::Value::Number(serde_json::Number::from(v)),
                Ok(AnyValue::Int32(v)) => serde_json::Value::Number(serde_json::Number::from(v as i64)),
                Ok(AnyValue::UInt32(v)) => serde_json::Value::Number(serde_json::Number::from(v as u64)),
                Ok(AnyValue::UInt64(v)) => {
                    // serde_json Numbers cannot represent full u64 safely; fall back to string if overflow
                    if let Some(n) = serde_json::Number::from_f64(v as f64) { serde_json::Value::Number(n) } else { serde_json::Value::String(v.to_string()) }
                }
                Ok(AnyValue::Float64(v)) => {
                    if let Some(n) = serde_json::Number::from_f64(v) { serde_json::Value::Number(n) } else { serde_json::Value::Null }
                }
                Ok(AnyValue::Boolean(b)) => serde_json::Value::Bool(b),
                Ok(AnyValue::String(v)) => serde_json::Value::String(v.to_string()),
                Ok(AnyValue::StringOwned(v)) => serde_json::Value::String(v.to_string()),
                Ok(AnyValue::Null) => serde_json::Value::Null,
                _ => serde_json::Value::Null,
            };
            map.insert(c.clone(), jv);
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
pub fn execute_select_df(store: &crate::storage::SharedStore, q: &Query) -> Result<DataFrame> {
    crate::server::exec::exec_select::run_select(store, q)
}


pub fn qualify_identifier_with_defaults(ident: &str, db: &str, schema: &str) -> String {
    let d = crate::ident::QueryDefaults::new(db.to_string(), schema.to_string());
    crate::ident::qualify_time_ident(ident, &d)
}

pub fn qualify_identifier_regular_table_with_defaults(ident: &str, db: &str, schema: &str) -> String {
    let d = crate::ident::QueryDefaults::new(db.to_string(), schema.to_string());
    crate::ident::qualify_regular_ident(ident, &d)
}

pub fn normalize_query_with_defaults(q: &str, db: &str, schema: &str) -> String {
    let up = q.to_uppercase();
    // Normalize unqualified regular TABLE DDL to include current db/schema
    if up.starts_with("DROP TABLE ") {
        let prefix = "DROP TABLE";
        let mut s = q[prefix.len()..].trim_start();
        let s_up = s.to_uppercase();
        // skip optional IF EXISTS
        let mut if_exists = "";
        if s_up.starts_with("IF EXISTS ") {
            if_exists = " IF EXISTS";
            s = &s["IF EXISTS ".len()..].trim_start();
        }
        if s.is_empty() { return q.to_string(); }
        let qualified = qualify_identifier_regular_table_with_defaults(s, db, schema);
        return format!("{}{} {}", prefix, if_exists, qualified);
    }
    if up.starts_with("RENAME TABLE ") {
        let prefix = "RENAME TABLE";
        let tail = &q[prefix.len()..];
        let tail_up = tail.to_uppercase();
        if let Some(i) = tail_up.find(" TO ") {
            let left = tail[..i].trim();
            let right = tail[i+4..].trim();
            if left.is_empty() || right.is_empty() { return q.to_string(); }
            let ql = qualify_identifier_regular_table_with_defaults(left, db, schema);
            let qr = qualify_identifier_regular_table_with_defaults(right, db, schema);
            return format!("{} {} TO {}", prefix, ql, qr);
        }
        return q.to_string();
    }
    // Qualify INSERT INTO targets using current db/schema.
    if up.starts_with("INSERT INTO ") {
        // Preserve rest of statement, only replace target identifier
        let after = &q["INSERT INTO ".len()..];
        let mut ident = after.trim_start();
        let mut rest = "";
        // identifier ends at first whitespace or '(' whichever comes first
        for (i, ch) in ident.char_indices() {
            if ch.is_whitespace() || ch == '(' { rest = &ident[i..]; ident = &ident[..i]; break; }
        }
        if ident.is_empty() { return q.to_string(); }

        // Don't strip quotes here - let normalize_identifier handle them to preserve case-sensitivity info
        // Support both normal and .time tables: choose qualifier based on suffix
        let normalized = crate::ident::normalize_identifier(ident);
        let qualified = if normalized.to_lowercase().ends_with(".time") {
            qualify_identifier_with_defaults(&normalized, db, schema)
        } else {
            qualify_identifier_regular_table_with_defaults(&normalized, db, schema)
        };
        return format!("INSERT INTO {}{}", qualified, rest);
    }
    // Do not rewrite SELECT or SLICE statements; column/table resolution is handled by Data Context at execution time
    if up.starts_with("SELECT ") || up.starts_with("SLICE") { return q.to_string(); }
    // Qualify CREATE TABLE targets using current db/schema (regular table)
    if up.starts_with("CREATE TABLE ") {
        let after = &q["CREATE TABLE ".len()..];
        let mut s = after;
        let s_up = s.to_uppercase();
        // skip optional IF NOT EXISTS
        let mut consumed = 0usize;
        if s_up.starts_with("IF NOT EXISTS ") { consumed = "IF NOT EXISTS ".len(); s = &s[consumed..]; }
        // Extract identifier up to '(' or whitespace
        let mut ident = s.trim_start();
        let mut rest = "";
        for (i, ch) in ident.char_indices() {
            if ch.is_whitespace() || ch == '(' { rest = &ident[i..]; ident = &ident[..i]; break; }
        }
        if ident.is_empty() { return q.to_string(); }

        // Don't strip quotes here - let normalize_identifier handle them
        let normalized = crate::ident::normalize_identifier(ident);
        let qualified = qualify_identifier_regular_table_with_defaults(&normalized, db, schema);
        let prefix = if consumed > 0 { format!("CREATE TABLE IF NOT EXISTS {}", qualified) } else { format!("CREATE TABLE {}", qualified) };
        return format!("{}{}", prefix, rest);
    }
    if up.starts_with("DELETE ") {
        if let Some(idx) = up.find(" FROM ") {
            let (head, tail) = q.split_at(idx + 6);
            let mut ident = tail.trim_start();
            let mut rest = "";
            for (i, ch) in ident.char_indices() {
                if ch.is_whitespace() { rest = &ident[i..]; ident = &ident[..i]; break; }
            }
            if ident.is_empty() { return q.to_string(); }

            // Don't strip quotes here - let normalize_identifier handle them
            let normalized = crate::ident::normalize_identifier(ident);
            let qualified = qualify_identifier_with_defaults(&normalized, db, schema);
            return format!("{}{}{}", head, qualified, rest);
        }
    }
    // Qualify UPDATE targets using current db/schema.
    if up.starts_with("UPDATE ") {
        // UPDATE <ident> SET ...
        let after = &q[7..];
        let up_after = after.to_uppercase();
        if let Some(i) = up_after.find(" SET ") {
            let ident = after[..i].trim();
            let rest = &after[i..];
            // Don't strip quotes here - let normalize_identifier handle them
            let normalized = crate::ident::normalize_identifier(ident);
            let qualified = if normalized.to_lowercase().ends_with(".time") {
                qualify_identifier_with_defaults(&normalized, db, schema)
            } else {
                qualify_identifier_regular_table_with_defaults(&normalized, db, schema)
            };
            return format!("UPDATE {}{}", qualified, rest);
        }
        return q.to_string();
    }
    if up.starts_with("CALCULATE ") {
        if let Some(p) = up.find(" AS SELECT ") {
            let (left, right) = q.split_at(p + 4);
            let normalized = normalize_query_with_defaults(&right[1..], db, schema);
            return format!("{} {}", left, normalized);
        }
    }
    q.to_string()
}
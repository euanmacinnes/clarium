//! exec_array_tvf
//! ----------------
//! Table-valued functions working with generic arrays (lists):
//! - unnest(array_literal)
//!
//! First implementation supports:
//!   FROM unnest(ARRAY[...])
//!   FROM unnest('{...}')
//! Elements are parsed into strings (dtype-agnostic). Later we can extend to typed
//! columns and lateral semantics when base tables are present.

use anyhow::{anyhow, Result};
use polars::prelude::*;

use crate::tprintln;

fn strip_quotes(x: &str) -> String {
    let t = x.trim();
    if (t.starts_with('"') && t.ends_with('"')) || (t.starts_with('\'') && t.ends_with('\'')) {
        if t.len() >= 2 { return t[1..t.len()-1].to_string(); }
    }
    t.to_string()
}

fn parse_func_args(call: &str) -> Option<(&str, String)> {
    let s = call.trim();
    let open = s.find('(')?;
    if !s.ends_with(')') { return None; }
    let fname = &s[..open].trim();
    let inside = &s[open+1..s.len()-1];
    Some((fname, inside.trim().to_string()))
}

fn split_array_inside(inner: &str) -> Vec<String> {
    // Split at top-level commas, respecting quotes and nested parens/brackets
    let mut parts: Vec<String> = Vec::new();
    let mut buf = String::new();
    let mut d_par = 0i32; let mut d_br = 0i32; let mut in_str = false; let mut qch = '\'';
    for ch in inner.chars() {
        if in_str {
            if ch == qch {
                in_str = false; buf.push(ch); continue;
            }
            // handle escaped '' in single quotes
            if qch == '\'' && ch == '\'' { buf.push('\''); continue; }
            buf.push(ch); continue;
        }
        match ch {
            '\'' | '"' => { in_str = true; qch = ch; buf.push(ch); }
            '(' => { d_par += 1; buf.push(ch); }
            ')' => { d_par -= 1; buf.push(ch); }
            '[' => { d_br += 1; buf.push(ch); }
            ']' => { d_br -= 1; buf.push(ch); }
            ',' if d_par == 0 && d_br == 0 => { parts.push(buf.trim().to_string()); buf.clear(); }
            _ => buf.push(ch),
        }
    }
    if !buf.trim().is_empty() { parts.push(buf.trim().to_string()); }
    parts
}

fn parse_brace_array_literal(txt: &str) -> Result<Vec<String>> {
    // Expect a literal like {a,b,"a,b",NULL}
    let s = txt.trim();
    if !s.starts_with('{') || !s.ends_with('}') { return Err(anyhow!("brace array must be {{...}}")); }
    let inner = &s[1..s.len()-1];
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut in_q = false; let mut qch = '"'; let mut esc = false;
    for ch in inner.chars() {
        if in_q {
            if esc { cur.push(ch); esc = false; continue; }
            if ch == '\\' { esc = true; continue; }
            if ch == qch { in_q = false; continue; }
            cur.push(ch); continue;
        } else {
            match ch {
                '"' => { in_q = true; qch = '"'; }
                ',' => { let t = cur.trim(); if t.eq_ignore_ascii_case("NULL") { out.push(String::new()); } else { out.push(t.to_string()); } cur.clear(); }
                _ => cur.push(ch),
            }
        }
    }
    if !cur.is_empty() {
        let t = cur.trim(); if t.eq_ignore_ascii_case("NULL") { out.push(String::new()); } else { out.push(t.to_string()); }
    }
    Ok(out)
}

pub fn try_array_tvf(_store: &crate::storage::SharedStore, raw: &str) -> Result<Option<DataFrame>> {
    let s = raw.trim();
    let low = s.to_ascii_lowercase();
    if !low.starts_with("unnest(") { return Ok(None); }
    let (_fname, arg_text) = match parse_func_args(s) { Some(v) => v, None => return Ok(None) };
    // Accept:
    //  - ARRAY[...]
    //  - array(...) constructor (lower/upper case)
    //  - brace literal {...}
    //  - quoted string containing any of the above (e.g., '{1,2}', 'ARRAY[1,2]', 'array(1,2)')
    let arg_trim = arg_text.trim();
    // If quoted, strip quotes first
    let arg_core = if (arg_trim.starts_with('"') && arg_trim.ends_with('"')) || (arg_trim.starts_with('\'') && arg_trim.ends_with('\'')) {
        if arg_trim.len() >= 2 { &arg_trim[1..arg_trim.len()-1] } else { arg_trim }
    } else { arg_trim };
    let elems: Vec<String> = if arg_core.to_ascii_uppercase().starts_with("ARRAY[") && arg_core.ends_with(']') {
        let inside = &arg_core[6..arg_core.len()-1];
        let parts = split_array_inside(inside);
        parts.into_iter().map(|p| strip_quotes(&p)).collect()
    } else if {
        // Support array(...) constructor emitted by the arithmetic parser for ARRAY literals
        let up = arg_core.to_ascii_lowercase();
        up.starts_with("array(") && arg_core.ends_with(')')
    } {
        let inside = &arg_core[6..arg_core.len()-1];
        let parts = split_array_inside(inside);
        parts.into_iter().map(|p| strip_quotes(&p)).collect()
    } else if arg_core.starts_with('{') && arg_core.ends_with('}') {
        parse_brace_array_literal(arg_core)?
    } else if (arg_trim.starts_with('\'') && arg_trim.ends_with('\'')) || (arg_trim.starts_with('"') && arg_trim.ends_with('"')) {
        // Fallback: a quoted, comma-separated list without braces
        // Example: unnest('a,b,c') -> ["a","b","c"]
        let inner = &arg_trim[1..arg_trim.len()-1];
        let mut out: Vec<String> = Vec::new();
        let mut cur = String::new();
        let mut in_q = false; let mut qch = '"'; let mut esc = false;
        for ch in inner.chars() {
            if in_q {
                if esc { cur.push(ch); esc = false; continue; }
                if ch == '\\' { esc = true; continue; }
                if ch == qch { in_q = false; continue; }
                cur.push(ch);
            } else {
                match ch {
                    '"' | '\'' => { in_q = true; qch = ch; }
                    ',' => { out.push(strip_quotes(cur.trim())); cur.clear(); }
                    _ => cur.push(ch),
                }
            }
        }
        if !cur.trim().is_empty() { out.push(strip_quotes(cur.trim())); }
        out
    } else {
        // Not a supported literal form
        return Err(anyhow!("unnest: only ARRAY[...] / array(...) and brace '{{...}}' literals (optionally quoted) are supported in TVF for now"));
    };
    tprintln!("[array.tvf] unnest: {} element(s)", elems.len());
    let df = DataFrame::new(vec![Series::new("unnest".into(), elems).into()])?;
    Ok(Some(df))
}

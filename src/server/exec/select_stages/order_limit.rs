//! ORDER BY / LIMIT stage

use anyhow::Result;
use polars::prelude::*;
use crate::tprintln;

use crate::query::Query;
use crate::query;
use crate::server::exec::exec_select::run_select_with_context;
use crate::server::data_context::{DataContext, SelectStage};
use crate::system; // for strict_projection flag
use tracing::debug;
use serde::Deserialize;
use std::fs;
use std::path::Path;

pub fn order_limit(mut df: DataFrame, q: &Query, ctx: &mut DataContext) -> Result<DataFrame> {
    // Resolve ORDER BY columns. When strict projection is disabled, tolerate
    // ORDER BY on columns that are not part of the final projection by
    // skipping those sort keys instead of erroring. This preserves historical
    // behavior expected by tests which allow ORDER BY on non-projected columns.
    tprintln!("[ORDER_LIMIT] DataFrame columns: {:?}", df.get_column_names());
    if let Some(ob) = &q.order_by {
        tprintln!("[ORDER_LIMIT] ORDER BY clauses: {:?}", ob);
        if !ob.is_empty() {
            let strict = system::get_strict_projection();
            let mut ann_applied = false;
            // Honor optional ANN/EXACT hint attached to ORDER BY
            if let Some(hint) = q.order_by_hint.as_deref() {
                match hint {
                    "exact" => {
                        debug!(target: "clarium::exec", "ORDER BY hint EXACT → using exact sort");
                    }
                    "ann" => {
                        // Best-effort index lookup and diagnostics; execution still falls back to exact for now
                        // If we preserved raw ORDER BY expressions, try to detect vec_l2/cosine_sim form
                        if let Some(raw_items) = &q.order_by_raw {
                            if let Some((raw0, _asc)) = raw_items.get(0) {
                                if let Some(p) = parse_ann_order_expr(raw0) {
                                    if let Some(rhs_val) = eval_scalar_expr(ctx, &p.rhs_expr) {
                                        if let Some(diag) = ann_find_index_for(ctx, &p.table, &p.column) {
                                            tprintln!("[ORDER_LIMIT] ANN hint + pattern '{}' detected with table='{}', column='{}'; metric={:?}, dim={:?}; RHS scalar evaluated (len={} chars). Using exact fallback for now.",
                                                      p.func, diag.table, diag.column, diag.metric, diag.dim, rhs_val.len());
                                        } else {
                                            tprintln!("[ORDER_LIMIT] ANN hint + pattern detected but no matching vector index for {}.{}; falling back to exact sort", p.table, p.column);
                                        }
                                    } else {
                                        tprintln!("[ORDER_LIMIT] ANN hint + pattern detected but RHS scalar expression could not be evaluated; falling back to exact sort");
                                    }
                                } else {
                                    // No ANN pattern; fall back
                                    if let Some(primary_key) = ob.get(0).map(|(n, _)| n.clone()) {
                                        if let Some(diag) = ann_diag_for_order_by(ctx, &primary_key) {
                                            tprintln!("[ORDER_LIMIT] ANN hint present; candidate vector index found: table='{}' column='{}' metric={:?} dim={:?}; proceeding with exact fallback pending ANN executor",
                                                      diag.table, diag.column, diag.metric, diag.dim);
                                        } else {
                                            tprintln!("[ORDER_LIMIT] ANN hint present but no matching vector index was found for ORDER BY key '{}'; falling back to exact sort", primary_key);
                                        }
                                    }
                                }
                            }
                        } else {
                            tprintln!("[ORDER_LIMIT] ANN hint present but ORDER BY list is empty/unavailable; falling back to exact sort");
                        }
                    }
                    _ => {
                        // Unknown hint token; ignore and continue with exact
                        debug!(target: "clarium::exec", "ORDER BY hint '{}' not recognized; using exact sort", hint);
                    }
                }
            }
            // If hint requests ANN, try ANN execution path before building regular sort expressions
            if let Some(hint) = q.order_by_hint.as_deref() {
                if hint == "ann" {
                    if let Some(raw_items) = &q.order_by_raw {
                        if let Some((raw0, asc_flag)) = raw_items.get(0) {
                            if let Some(p) = parse_ann_order_expr(raw0) {
                                if let Some(rhs_val) = eval_scalar_expr(ctx, &p.rhs_expr) {
                                    if let Some(diag) = ann_find_index_for(ctx, &p.table, &p.column) {
                                        // Attempt ANN ordering using the current DataFrame
                                        if df.get_column_names().iter().any(|c| c.eq_ignore_ascii_case(&p.column)) {
                                            // Pass LIMIT as top-k optimization to sorter
                                            let topk = q.limit.and_then(|n| if n > 0 { Some(n as usize) } else { None });
                                            let ef_search = crate::system::get_vector_ef_search();
                                            tprintln!("[ORDER_LIMIT] ANN executing with ef_search={} topk={:?}", ef_search, topk);
                                            if let Ok(sorted) = ann_order_dataframe(&df, &p.column, &p.func, diag.metric.as_deref(), diag.dim, &rhs_val, *asc_flag, topk) {
                                                df = sorted;
                                                ann_applied = true;
                                                // Skip building exact sort expressions — we already sorted via ANN
                                                // LIMIT handling continues below as usual
                                                // Register columns and proceed to LIMIT
                                                // Note: we purposely do not remove any columns; ann_order_dataframe cleans up temp columns
                                                // Continue to LIMIT without executing the exact branch
                                                // Jump to LIMIT by using a scoped block and then falling through
                                            } else {
                                                tprintln!("[ORDER_LIMIT] ANN execution failed; falling back to exact sort");
                                            }
                                        } else {
                                            tprintln!("[ORDER_LIMIT] ANN requested but column '{}' not present in projection; fallback to exact", p.column);
                                        }
                                    } else {
                                        tprintln!("[ORDER_LIMIT] ANN requested but no matching vector index for {}.{}; falling back to exact", p.table, p.column);
                                    }
                                } else {
                                    tprintln!("[ORDER_LIMIT] ANN requested but RHS scalar could not be evaluated; falling back to exact");
                                }
                            }
                        }
                    }
                }
            }
            if !ann_applied {
                // In strict mode, validate that no ORDER BY columns are temporary (not in original SELECT)
                if strict && !ctx.temp_order_by_columns.is_empty() {
                    for (name, _asc) in ob.iter() {
                        if ctx.temp_order_by_columns.contains(name) {
                            return Err(
                                crate::server::data_context::DataContext::column_not_found_error(
                                    name,
                                    "ORDER BY",
                                    &df,
                                ),
                            );
                        }
                    }
                }
                let mut exprs: Vec<Expr> = Vec::new();
                let mut descending: Vec<bool> = Vec::new();
                for (name, asc) in ob.iter() {
                    // Try to resolve the ORDER BY column against current DF/Context
                    match ctx.resolve_column_at_stage(&df, name, SelectStage::OrderLimit) {
                        Ok(resolved) => {
                            exprs.push(col(resolved.as_str()));
                            descending.push(!asc);
                        }
                        Err(_) => {
                            // Column not found by resolver. In loose (non-strict) mode, we skip
                            // unknown ORDER BY keys. In strict mode, surface a clause-specific error.
                            if strict {
                                return Err(
                                    crate::server::data_context::DataContext::column_not_found_error(
                                        name,
                                        "ORDER BY",
                                        &df,
                                    ),
                                );
                            } else {
                                // Best-effort diagnostic to help track behavior during tests
                                tprintln!(
                                    "[ORDER_LIMIT] Skipping unknown ORDER BY key '{}' (strict_projection=false)",
                                    name
                                );
                                continue;
                            }
                        }
                    }
                }
                if !exprs.is_empty() {
                    let nulls_last: Vec<bool> = vec![true; exprs.len()];
                    let opts = polars::prelude::SortMultipleOptions { descending, nulls_last, maintain_order: true, multithreaded: true, limit: None };
                    df = df.lazy().sort_by_exprs(exprs, opts).collect()?;
                }
            }
            // In loose mode, drop temporary ORDER BY columns that were added for sorting
            if !strict && !ctx.temp_order_by_columns.is_empty() {
                let cols_to_keep: Vec<String> = df.get_column_names()
                    .iter()
                    .filter(|c| !ctx.temp_order_by_columns.contains(c.as_str()))
                    .map(|c| c.to_string())
                    .collect();
                if !cols_to_keep.is_empty() {
                    df = df.select(&cols_to_keep)?;
                }
            }
        }
    }
    // Apply LIMIT locally (mirror df_utils::apply_order_and_limit)
    if let Some(n) = q.limit {
        let h = df.height();
        if n > 0 {
            let m = n as usize;
            if m < h { df = df.slice(0, m); }
        } else if n < 0 {
            let m = (-n) as usize;
            if m < h {
                let start = (h - m) as i64;
                df = df.slice(start, m);
            }
        } else {
            df = df.slice(0, 0);
        }
    }
    ctx.register_df_columns_for_stage(SelectStage::OrderLimit, &df);
    Ok(df)
}

// --- ANN diagnostics helpers (non-fatal): find a matching `.vindex` for ORDER BY key ---

#[derive(Debug, Clone, Deserialize)]
struct VIndexProbe {
    name: Option<String>,
    table: Option<String>,
    column: Option<String>,
    algo: Option<String>,
    metric: Option<String>,
    dim: Option<i32>,
}

struct AnnDiag { pub table: String, pub column: String, pub metric: Option<String>, pub dim: Option<i32> }

fn ann_diag_for_order_by(ctx: &DataContext, order_key: &str) -> Option<AnnDiag> {
    // We need a storage handle to inspect `.vindex` sidecars
    let store = ctx.store.as_ref()?;
    let root = store.0.lock().root_path().clone();
    // Walk the db/schema tree and search for any .vindex whose column matches the ORDER BY key
    // This is a heuristic when we cannot perfectly resolve the source table at this stage.
    let mut best: Option<AnnDiag> = None;
    if let Ok(dbs) = fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let dbp = db_ent.path(); if !dbp.is_dir() { continue; }
            if let Ok(schemas) = fs::read_dir(&dbp) {
                for sch_ent in schemas.flatten() {
                    let sp = sch_ent.path(); if !sp.is_dir() { continue; }
                    if let Ok(entries) = fs::read_dir(&sp) {
                        for e in entries.flatten() {
                            let p = e.path();
                            if p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("vindex") {
                                if let Ok(text) = fs::read_to_string(&p) {
                                    if let Ok(vp) = serde_json::from_str::<VIndexProbe>(&text) {
                                        let col = vp.column.clone().unwrap_or_default();
                                        if col.eq_ignore_ascii_case(order_key) {
                                            let tbl = vp.table.clone().unwrap_or_default();
                                            best = Some(AnnDiag { table: tbl, column: col, metric: vp.metric.clone(), dim: vp.dim });
                                            return best; // first match is good enough for diagnostics
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    best
}

// Try to find a specific index by matching both table and column
fn ann_find_index_for(ctx: &DataContext, table: &str, column: &str) -> Option<AnnDiag> {
    let store = ctx.store.as_ref()?;
    let root = store.0.lock().root_path().clone();
    if let Ok(dbs) = fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let dbp = db_ent.path(); if !dbp.is_dir() { continue; }
            if let Ok(schemas) = fs::read_dir(&dbp) {
                for sch_ent in schemas.flatten() {
                    let sp = sch_ent.path(); if !sp.is_dir() { continue; }
                    if let Ok(entries) = fs::read_dir(&sp) {
                        for e in entries.flatten() {
                            let p = e.path();
                            if p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("vindex") {
                                if let Ok(text) = fs::read_to_string(&p) {
                                    if let Ok(vp) = serde_json::from_str::<VIndexProbe>(&text) {
                                        let col = vp.column.clone().unwrap_or_default();
                                        let tbl = vp.table.clone().unwrap_or_default();
                                        let tbl_match = tbl.eq_ignore_ascii_case(table) || tbl.ends_with(table);
                                        if tbl_match && col.eq_ignore_ascii_case(column) {
                                            return Some(AnnDiag { table: tbl, column: col, metric: vp.metric.clone(), dim: vp.dim });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

// Parse ORDER BY raw expression of the form: vec_l2(table.col, <rhs>) or cosine_sim(table.col, <rhs>)
struct ParsedAnnOrder { func: String, table: String, column: String, rhs_expr: String }

fn parse_ann_order_expr(expr: &str) -> Option<ParsedAnnOrder> {
    let txt = expr.trim();
    let up = txt.to_ascii_lowercase();
    let funcs = ["vec_l2", "cosine_sim"]; // supported functions for ANN trigger
    let func = funcs.iter().find(|f| up.starts_with(&format!("{}(", f))).cloned()?;
    // Find outermost parentheses and split args by first comma respecting nesting
    let open_pos = txt.find('(')?;
    let inner = &txt[open_pos+1..].trim();
    // walk to matching ')'
    let mut depth = 1i32;
    let mut i = 0usize;
    let bytes: Vec<char> = inner.chars().collect();
    let mut comma_at: Option<usize> = None;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == '(' { depth += 1; }
        else if ch == ')' { depth -= 1; if depth == 0 { break; } }
        else if ch == ',' && depth == 1 && comma_at.is_none() { comma_at = Some(i); }
        i += 1;
    }
    if depth != 0 { return None; }
    let end_pos = i; // index of ')'
    let body: String = bytes[..end_pos].iter().collect();
    let (lhs, rhs) = if let Some(cpos) = comma_at { (&body[..cpos], &body[cpos+1..]) } else { return None; };
    let lhs = lhs.trim();
    let rhs = rhs.trim();
    // LHS expected as table.col (optionally qualified); extract last two segments
    let lhs_norm = lhs.trim_matches('"').trim_matches('`');
    let parts: Vec<&str> = lhs_norm.split(|c| c == '.' || c == '/').collect();
    if parts.len() < 2 { return None; }
    let column = parts.last().unwrap().to_string();
    let table = parts[parts.len()-2].to_string();
    Some(ParsedAnnOrder { func: func.to_string(), table, column, rhs_expr: rhs.to_string() })
}

// Evaluate RHS scalar expression; support two forms: (SELECT ...) subquery returning one cell, or a scalar literal string/number
fn eval_scalar_expr(ctx: &DataContext, expr: &str) -> Option<String> {
    let e = expr.trim();
    // Subquery form: starts with '(' SELECT ... ')'
    if e.starts_with('(') && e.to_ascii_uppercase().contains("SELECT") && e.ends_with(')') {
        let inner = &e[1..e.len()-1];
        if let Ok(cmd) = query::parse(inner) {
            if let query::Command::Select(q) = cmd {
                let store = ctx.store.as_ref()?;
                if let Ok(df) = run_select_with_context(store, &q, Some(ctx)) {
                    if df.height() >= 1 && !df.get_column_names().is_empty() {
                        let col_name = df.get_column_names()[0].clone();
                        if let Ok(col) = df.column(&col_name) {
                            if col.len() >= 1 {
                                let v = col.get(0).unwrap();
                                return Some(v.to_string());
                            }
                        }
                    }
                }
            }
        }
        return None;
    }
    // Strip quotes if present
    let s = e.trim_matches('\'').trim_matches('"').to_string();
    if s.is_empty() { None } else { Some(s) }
}

// --- ANN execution helpers ---

// Parse a vector literal string into Vec<f64>; accepts comma or whitespace separated, optional brackets
fn parse_vec_literal(s: &str) -> Option<Vec<f64>> {
    let mut txt = s.trim().trim_matches('"').trim_matches('\'').to_string();
    if txt.len() >= 2 {
        let first = txt.as_bytes()[0] as char;
        let last = txt.as_bytes()[txt.len()-1] as char;
        if (first == '[' && last == ']') || (first == '(' && last == ')') {
            txt = txt[1..txt.len()-1].to_string();
        }
    }
    // Replace whitespace with commas for easier split
    let txt = txt.replace(|c: char| c.is_whitespace(), ",");
    let mut out: Vec<f64> = Vec::new();
    for part in txt.split(',') {
        let p = part.trim();
        if p.is_empty() { continue; }
        match p.parse::<f64>() {
            Ok(v) => out.push(v),
            Err(_) => return None,
        }
    }
    if out.is_empty() { None } else { Some(out) }
}

// Compute ANN ordering using exact scoring on the provided column containing vector-encoded strings
fn ann_order_dataframe(
    df: &DataFrame,
    col_name: &str,
    func: &str,
    index_metric: Option<&str>,
    index_dim: Option<i32>,
    rhs_scalar: &str,
    asc_hint: bool,
    topk: Option<usize>,
) -> Result<DataFrame> {
    // Parse query vector once
    let qvec = parse_vec_literal(rhs_scalar).ok_or_else(|| anyhow::anyhow!("invalid query vector for ANN"))?;
    // Validate dimension if index declares it
    if let Some(d) = index_dim { if d as usize != qvec.len() { anyhow::bail!("ANN: query vector dim {} does not match index dim {}", qvec.len(), d); } }
    // Validate metric compatibility if specified
    if let Some(m) = index_metric {
        let m = m.to_ascii_lowercase();
        let func_key = if func.eq_ignore_ascii_case("cosine_sim") { "cosine" } else if func.eq_ignore_ascii_case("vec_l2") { "l2" } else { "unknown" };
        if func_key != "unknown" && m != func_key {
            anyhow::bail!("ANN: metric mismatch between ORDER BY function '{}' and index metric '{}'", func, m);
        }
    }
    // Ensure the column exists and is Utf8-like
    let cname = df.get_column_names().iter().find(|c| c.eq_ignore_ascii_case(&col_name)).cloned().unwrap_or(col_name.to_string());
    let col_series = df.column(&cname)?;
    // Create score series
    let mut scores: Vec<f64> = Vec::with_capacity(df.height());
    match col_series.dtype() {
        DataType::Utf8 | DataType::String => {
            let ca = col_series.utf8().map_err(|_| anyhow::anyhow!("vector column '{}' must be Utf8", cname))?;
            for opt in ca.into_iter() {
                let s = opt.unwrap_or("");
                let v = parse_vec_literal(s).unwrap_or_default();
                let score = match func {
                    f if f.eq_ignore_ascii_case("vec_l2") => l2_distance(&v, &qvec),
                    f if f.eq_ignore_ascii_case("cosine_sim") => cosine_similarity(&v, &qvec),
                    _ => l2_distance(&v, &qvec),
                };
                scores.push(score);
            }
        }
        _ => {
            // Attempt to stringify other types
            let s = col_series.to_string();
            // to_string() prints the whole column; that's not usable. Fail gracefully
            return Err(anyhow::anyhow!("vector column '{}' has unsupported dtype {:?}", cname, col_series.dtype()));
        }
    }
    // Determine final sort direction:
    // For cosine_sim: higher is better (DESC by default). For vec_l2: lower is better (ASC by default).
    let default_desc = func.eq_ignore_ascii_case("cosine_sim");
    // If user specified ASC/DESC explicitly, honor it; asc_hint=true means ASC, so descending=false
    // We combine by using the user's flag as the final direction.
    let final_desc = !asc_hint; // true means DESC
    // Attach score column
    let score_series = Series::new("__ann_score", scores);
    let mut df2 = df.clone();
    df2.with_column(score_series)?;
    let opts = polars::prelude::SortMultipleOptions { descending: vec![final_desc], nulls_last: vec![true], maintain_order: true, multithreaded: true, limit: topk };
    let orig_cols: Vec<_> = df.get_column_names().iter().map(|s| col(s)).collect();
    let df_sorted = df2.lazy().sort_by_exprs(vec![col("__ann_score")], opts).select(&orig_cols).collect()?;
    Ok(df_sorted)
}

fn l2_distance(a: &Vec<f64>, b: &Vec<f64>) -> f64 {
    let n = a.len().min(b.len());
    if n == 0 { return f64::INFINITY; }
    let mut s = 0.0;
    for i in 0..n { let d = a[i] - b[i]; s += d*d; }
    s.sqrt()
}

fn cosine_similarity(a: &Vec<f64>, b: &Vec<f64>) -> f64 {
    let n = a.len().min(b.len());
    if n == 0 { return f64::NAN; }
    let mut dot = 0.0; let mut na = 0.0; let mut nb = 0.0;
    for i in 0..n { dot += a[i]*b[i]; na += a[i]*a[i]; nb += b[i]*b[i]; }
    if na == 0.0 || nb == 0.0 { return f64::NAN; }
    dot / (na.sqrt()*nb.sqrt())
}

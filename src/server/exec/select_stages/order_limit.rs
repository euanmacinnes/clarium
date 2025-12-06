//! ORDER BY / LIMIT stage

use anyhow::Result;
use polars::prelude::*;
use crate::tprintln;

use crate::server::query::query_common::Query;
use crate::server::query;
use crate::server::exec::exec_select::run_select_with_context;
use crate::server::data_context::{DataContext, SelectStage};
use crate::system; // for strict_projection flag
use tracing::debug;
use serde::Deserialize;
use std::fs;
use std::path::Path;
use std::collections::HashMap;
use std::cmp::Reverse;

use crate::server::exec::vector_utils;
use crate::server::exec::exec_vector_index::VIndexFile;

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
                                    // Attempt ANN-style ordering using exact computation even if no index is present
                                    let diag = ann_find_index_for(ctx, &p.table, &p.column);
                                    let metric = diag.as_ref().and_then(|d| d.metric.as_deref());
                                    let dim = diag.as_ref().and_then(|d| d.dim);
                                    if let Some(d) = &diag {
                                        tprintln!("[ORDER_LIMIT] ANN path: matching index found for {}.{} metric={:?} dim={:?}", d.table, d.column, d.metric, d.dim);
                                    } else {
                                        tprintln!("[ORDER_LIMIT] ANN path: no index found for {}.{}; computing exact distances for ordering", p.table, p.column);
                                    }
                                    if df.get_column_names().iter().any(|c| c.eq_ignore_ascii_case(&p.column)) {
                                        // Pass LIMIT as top-k optimization to sorter
                                        let topk = q.limit.and_then(|n| if n > 0 { Some(n as usize) } else { None });
                                        let ef_search = crate::system::get_vector_ef_search();
                                        tprintln!("[ORDER_LIMIT] ANN executing (exact compute) with ef_search={} topk={:?}", ef_search, topk);
                                        // Secondary keys are the remaining ORDER BY keys after the primary
                                        let sec: Option<Vec<(String,bool)>> = q.order_by.as_ref().map(|v| v.iter().skip(1).cloned().collect());
                                        if let Ok(sorted) = ann_order_dataframe(ctx, &df, &p.column, &p.func, metric, dim, &rhs_val, *asc_flag, topk, sec.as_ref()) {
                                            df = sorted;
                                            ann_applied = true;
                                        } else {
                                            tprintln!("[ORDER_LIMIT] ANN execution failed; falling back to exact sort");
                                        }
                                    } else {
                                        tprintln!("[ORDER_LIMIT] ANN requested but column '{}' not present in projection; fallback to exact", p.column);
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
                // Opportunistic ANN path: even without explicit USING ANN, attempt to detect known ANN functions
                if let Some(raw_items) = &q.order_by_raw {
                    if let Some((raw0, asc_flag)) = raw_items.get(0) {
                        if let Some(p) = parse_ann_order_expr(raw0) {
                            if let Some(rhs_val) = eval_scalar_expr(ctx, &p.rhs_expr) {
                                let diag = ann_find_index_for(ctx, &p.table, &p.column);
                                let metric = diag.as_ref().and_then(|d| d.metric.as_deref());
                                let dim = diag.as_ref().and_then(|d| d.dim);
                                if df.get_column_names().iter().any(|c| c.eq_ignore_ascii_case(&p.column)) {
                                    let topk = q.limit.and_then(|n| if n > 0 { Some(n as usize) } else { None });
                                    let sec: Option<Vec<(String,bool)>> = q.order_by.as_ref().map(|v| v.iter().skip(1).cloned().collect());
                                    if let Ok(sorted) = ann_order_dataframe(ctx, &df, &p.column, &p.func, metric, dim, &rhs_val, *asc_flag, topk, sec.as_ref()) {
                                        df = sorted;
                                        ann_applied = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if !ann_applied {
                // In strict mode, validate that no ORDER BY columns are temporary (not in original SELECT)
                // If a temporary ORDER BY column has an equivalent projected column (e.g., 'id' vs 'd.id'),
                // transparently remap ORDER BY to the projected column instead of erroring.
                let mut ob_overrides: HashMap<String, String> = HashMap::new();
                if strict && !ctx.temp_order_by_columns.is_empty() {
                    for (name, _asc) in ob.iter() {
                        if ctx.temp_order_by_columns.contains(name) {
                            // Find candidates in the DataFrame whose last segment matches `name`,
                            // but are not exactly the temporary column itself.
                            let mut candidates: Vec<String> = df
                                .get_column_names()
                                .iter()
                                .filter_map(|c| {
                                    let s = c.as_str();
                                    let last = s.rsplit('.').next().unwrap_or(s);
                                    if last.eq_ignore_ascii_case(name) && s != name { Some(s.to_string()) } else { None }
                                })
                                .collect();
                            // Prefer a single qualified candidate if available
                            if candidates.len() == 1 {
                                ob_overrides.insert(name.clone(), candidates.remove(0));
                            } else if candidates.is_empty() {
                                // No equivalent projected column found → keep existing strict behavior
                                return Err(
                                    crate::server::data_context::DataContext::column_not_found_error(
                                        name,
                                        "ORDER BY",
                                        &df,
                                    ),
                                );
                            } else {
                                // Multiple candidates; if all are identical references, we could pick one.
                                // For now, be conservative and require disambiguation.
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
                }
                let mut exprs: Vec<Expr> = Vec::new();
                let mut descending: Vec<bool> = Vec::new();
                for (name, asc) in ob.iter() {
                    // Apply any override established during strict temp validation
                    let effective_name: &str = if let Some(n) = ob_overrides.get(name) { n.as_str() } else { name.as_str() };
                    // If the ORDER BY key looks like an expression (e.g., a function call),
                    // skip it here; expression-based ordering should be handled via ANN path
                    // or earlier stages. This prevents mis-resolving full expressions as columns.
                    if effective_name.contains('(') || effective_name.contains(')') {
                        tprintln!("[ORDER_LIMIT] Skipping expression ORDER BY key '{}' in exact path", effective_name);
                        continue;
                    }
                    // Try to resolve the ORDER BY column against current DF/Context
                    match ctx.resolve_column_at_stage(&df, effective_name, SelectStage::OrderLimit) {
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
                                        effective_name,
                                        "ORDER BY",
                                        &df,
                                    ),
                                );
                            } else {
                                // Best-effort diagnostic to help track behavior during tests
                                tprintln!(
                                    "[ORDER_LIMIT] Skipping unknown ORDER BY key '{}' (strict_projection=false)",
                                    effective_name
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

// Vector parsing is centralized in vector_utils; keep a small adapter to f64
fn parse_vec_literal_f64(s: &str) -> Option<Vec<f64>> {
    vector_utils::parse_vec_literal(s).map(|v| v.into_iter().map(|x| x as f64).collect())
}

// Compute ANN ordering using exact scoring on the provided column containing vector-encoded strings
fn ann_order_dataframe(
    ctx: &DataContext,
    df: &DataFrame,
    col_name: &str,
    func: &str,
    index_metric: Option<&str>,
    index_dim: Option<i32>,
    rhs_scalar: &str,
    asc_hint: bool,
    topk: Option<usize>,
    // Secondary ORDER BY keys (beyond the primary vector function), in order
    secondary_keys: Option<&Vec<(String, bool)>>,
) -> Result<DataFrame> {
    // Parse query vector once. If parsing fails, bail so caller can fallback to exact ORDER BY path.
    let qvec = parse_vec_literal_f64(rhs_scalar).ok_or_else(|| anyhow::anyhow!("invalid query vector for ANN"))?;
    // Do NOT hard fail on index metric/dimension mismatch; treat index hints as advisory.
    // This enables graceful fallback to exact scoring even when index metadata doesn't align with the request.
    // Ensure the column exists and is String-like
    let cname = df
        .get_column_names()
        .iter()
        .find(|c| c.eq_ignore_ascii_case(&col_name))
        .map(|c| c.to_string())
        .unwrap_or_else(|| col_name.to_string());
    let col_series = df.column(&cname)?;
    // Identify a stable row-id column if present (prefer namespaced __row_id.<alias>, else __row_id)
    let mut rid_col_name: Option<String> = None;
    for n in df.get_column_names() {
        let ns = n.as_str();
        if ns == "__row_id" || ns.starts_with("__row_id.") { rid_col_name = Some(ns.to_string()); break; }
    }
    // Helper: build final sort expressions combining primary score + secondary keys + stable row-id tie-break
    let build_sort_keys = |df_cols: &DataFrame, rid_name: Option<&str>, final_desc: bool| -> (Vec<Expr>, Vec<bool>) {
        let mut exprs: Vec<Expr> = vec![col("__ann_score")];
        let mut desc: Vec<bool> = vec![final_desc];
        if let Some(keys) = secondary_keys {
            for (name, asc) in keys.iter() {
                // Skip function-like expressions here; those are not supported in exact path inside this stage
                if name.contains('(') || name.contains(')') { continue; }
                // Resolve approximately by exact match or case-insensitive match in DF
                let effective: String = df_cols
                    .get_column_names()
                    .iter()
                    .find(|c| c.as_str() == name.as_str())
                    .map(|c| c.to_string())
                    .or_else(|| df_cols.get_column_names().iter().find(|c| c.eq_ignore_ascii_case(name)).map(|c| c.to_string()))
                    .unwrap_or_else(|| name.clone());
                exprs.push(col(effective.as_str()));
                desc.push(!*asc);
            }
        }
        if let Some(rn) = rid_name { exprs.push(col(rn)); desc.push(false); }
        (exprs, desc)
    };

    // Try ANN runtime search first when LIMIT is present and a matching index exists; fallback to exact path on any error
    if let Some(k) = topk {
        if k > 0 {
            if let Some(store) = ctx.store.as_ref() {
                if let Some(ann) = ann_diag_for_order_by(ctx, &cname) {
                    // Find the exact .vindex file matching table+column
                    let root = store.0.lock().root_path().clone();
                    'outer: for db_ent in std::fs::read_dir(&root).unwrap_or_else(|_| std::fs::read_dir(".").unwrap()) {
                        if let Ok(db_e) = db_ent {
                            let dbp = db_e.path(); if !dbp.is_dir() { continue; }
                            if let Ok(schemas) = std::fs::read_dir(&dbp) {
                                for sch_ent in schemas.flatten() {
                                    let sp = sch_ent.path(); if !sp.is_dir() { continue; }
                                    if let Ok(entries) = std::fs::read_dir(&sp) {
                                        for e in entries.flatten() {
                                            let p = e.path();
                                            if p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("vindex") {
                                                if let Ok(text) = std::fs::read_to_string(&p) {
                                                    if let Ok(vf) = serde_json::from_str::<VIndexFile>(&text) {
                                                        let tbl_match = vf.table.eq_ignore_ascii_case(&ann.table) || vf.table.ends_with(&ann.table);
                                                        if tbl_match && vf.column.eq_ignore_ascii_case(&ann.column) {
                                                            // Call runtime search
                                                            let qf: Vec<f32> = qvec.iter().map(|x| *x as f32).collect();
                                                            // Preselect W = alpha * k candidates using ANN engine (or flat as baseline)
                                                            let alpha = crate::system::get_vector_preselect_alpha().max(1) as usize;
                                                            let w = k.saturating_mul(alpha);
                                                            if let Ok(cands) = crate::server::exec::exec_vector_runtime::search_vector_index(store, &vf, &qf, w) {
                                                                if !cands.is_empty() {
                                                                    // If a stable __row_id column exists, map candidates by row_id; otherwise treat ids as positional indices
                                                                    if let Some(rn) = rid_col_name.as_deref() {
                                                                        if let Ok(rid_col) = df.column(rn) {
                                                                            // Build map from row_id -> row_index
                                                                            use std::collections::HashMap;
                                                                            let mut pos: HashMap<u64, u32> = HashMap::with_capacity(df.height());
                                                                            for i in 0..rid_col.len() {
                                                                                if let Ok(av) = rid_col.get(i) {
                                                                                    if let Ok(id) = av.try_extract::<u64>() {
                                                                                        pos.insert(id, i as u32);
                                                                                    } else if let Ok(id32) = av.try_extract::<u32>() {
                                                                                        pos.insert(id32 as u64, i as u32);
                                                                                    } else if let Ok(id64) = av.try_extract::<i64>() {
                                                                                        pos.insert(id64 as u64, i as u32);
                                                                                    }
                                                                                }
                                                                            }
                                                                            // Collect DF indices for preselected candidates, preserving ANN order
                                                                            let mut idx: Vec<u32> = Vec::with_capacity(cands.len());
                                                                            for (rid, _s) in cands.iter() {
                                                                                if let Some(pi) = pos.get(&(*rid as u64)) { idx.push(*pi); }
                                                                            }
                                                                            if !idx.is_empty() {
                                                                                // Slice DF to W candidates
                                                                                let idx_u = UInt32Chunked::from_slice("__take".into(), &idx);
                                                                                let mut df_w = df.take(&idx_u)?;
                                                                                // Compute exact scores on W and attach column
                                                                                let mut scores: Vec<f64> = Vec::with_capacity(df_w.height());
                                                                                let series_w = df_w.column(&cname)?;
                                                                                for i in 0..series_w.len() {
                                                                                    let v = vector_utils::extract_vec_f32_col(series_w, i).unwrap_or_default().into_iter().map(|x| x as f64).collect::<Vec<f64>>();
                                                                                    let s = match func {
                                                                                        f if f.eq_ignore_ascii_case("vec_l2") => l2_distance(&v, &qvec),
                                                                                        f if f.eq_ignore_ascii_case("cosine_sim") => cosine_similarity(&v, &qvec),
                                                                                        _ => l2_distance(&v, &qvec),
                                                                                    };
                                                                                    scores.push(s);
                                                                                }
                                                                                df_w.with_column(Series::new("__ann_score".into(), scores))?;
                                                                                // Build final sort: score + secondary keys + row-id
                                                                                let final_desc = !asc_hint;
                                                                                let (exprs, desc) = build_sort_keys(&df_w, rid_col_name.as_deref(), final_desc);
                                                                                let opts = polars::prelude::SortMultipleOptions {
                                                                                    descending: desc,
                                                                                    nulls_last: vec![true; exprs.len()],
                                                                                    maintain_order: true,
                                                                                    multithreaded: true,
                                                                                    limit: Some((k as polars::prelude::IdxSize).min(df_w.height() as polars::prelude::IdxSize)),
                                                                                };
                                                                                let orig_cols: Vec<_> = df.get_column_names().iter().map(|s| col(s.as_str())).collect();
                                                                                let df_sorted = df_w.lazy().sort_by_exprs(exprs, opts).select(&orig_cols).collect()?;
                                                                                tprintln!("[ORDER_LIMIT][ANN] LIMIT path: engine=flat metric_hint={:?} alpha={} W={} final_k={} secondary_keys={} rid={} -> took {} rows", index_metric, alpha, w, k, secondary_keys.map(|v| v.len()).unwrap_or(0), rid_col_name.as_deref().unwrap_or("<none>"), df_sorted.height());
                                                                                return Ok(df_sorted);
                                                                            }
                                                                        }
                                                                    } else {
                                                                        // Fallback: treat ids as positional row indices (only valid for ordinal ids)
                                                                        let h = df.height() as u32;
                                                                        let mut idx: Vec<u32> = Vec::with_capacity(cands.len());
                                                                        for (rid, _s) in cands.into_iter() {
                                                                            let mut ii = rid as u32;
                                                                            if ii >= h { ii = h.saturating_sub(1); }
                                                                            idx.push(ii);
                                                                        }
                                                                        let idx_u = UInt32Chunked::from_slice("__take".into(), &idx);
                                                                        let mut df_w = df.take(&idx_u)?;
                                                                        // Compute exact scores on W and sort with secondary keys
                                                                        let mut scores: Vec<f64> = Vec::with_capacity(df_w.height());
                                                                        let series_w = df_w.column(&cname)?;
                                                                        for i in 0..series_w.len() {
                                                                            let v = vector_utils::extract_vec_f32_col(series_w, i).unwrap_or_default().into_iter().map(|x| x as f64).collect::<Vec<f64>>();
                                                                            let s = match func {
                                                                                f if f.eq_ignore_ascii_case("vec_l2") => l2_distance(&v, &qvec),
                                                                                f if f.eq_ignore_ascii_case("cosine_sim") => cosine_similarity(&v, &qvec),
                                                                                _ => l2_distance(&v, &qvec),
                                                                            };
                                                                            scores.push(s);
                                                                        }
                                                                        df_w.with_column(Series::new("__ann_score".into(), scores))?;
                                                                        let final_desc = !asc_hint;
                                                                        let (exprs, desc) = build_sort_keys(&df_w, rid_col_name.as_deref(), final_desc);
                                                                        let opts = polars::prelude::SortMultipleOptions {
                                                                            descending: desc,
                                                                            nulls_last: vec![true; exprs.len()],
                                                                            maintain_order: true,
                                                                            multithreaded: true,
                                                                            limit: Some((k as polars::prelude::IdxSize).min(df_w.height() as polars::prelude::IdxSize)),
                                                                        };
                                                                        let orig_cols: Vec<_> = df.get_column_names().iter().map(|s| col(s.as_str())).collect();
                                                                        let df_sorted = df_w.lazy().sort_by_exprs(exprs, opts).select(&orig_cols).collect()?;
                                                                        tprintln!("[ORDER_LIMIT][ANN] LIMIT path (positional ids): alpha={} W={} final_k={} secondary_keys={} -> took {} rows", alpha, w, k, secondary_keys.map(|v| v.len()).unwrap_or(0), df_sorted.height());
                                                                        return Ok(df_sorted);
                                                                    }
                                                                }
                                                            }
                                                            break 'outer;
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
                }
            }
        }
    }
    // If LIMIT k present, use streaming top-k selection to avoid full sort
    let final_desc = !asc_hint; // true means DESC
    // Provide a total-ordering key for f64 so we can use BinaryHeap without requiring Ord for f64
    #[inline]
    fn f64_key(v: f64) -> u64 {
        let b = v.to_bits();
        // Map IEEE754 to lexicographically ordered bits (NaNs will be ordered but their exact placement is not critical here)
        if b & (1u64 << 63) != 0 { !b } else { b | (1u64 << 63) }
    }
    if let Some(k) = topk {
        let n = col_series.len();
        if k == 0 || n == 0 { return Ok(df.clone()); }
        if final_desc {
            // Want largest k (DESC): maintain a min-heap of (score, idx); pop when new > smallest
            let mut heap: std::collections::BinaryHeap<Reverse<(u64, u32)>> = std::collections::BinaryHeap::with_capacity(k + 1);
            for i in 0..n {
                let v = vector_utils::extract_vec_f32_col(col_series, i).unwrap_or_default().into_iter().map(|x| x as f64).collect::<Vec<f64>>();
                let score = match func {
                    f if f.eq_ignore_ascii_case("vec_l2") => l2_distance(&v, &qvec),
                    f if f.eq_ignore_ascii_case("cosine_sim") => cosine_similarity(&v, &qvec),
                    _ => l2_distance(&v, &qvec),
                };
                heap.push(Reverse((f64_key(score), i as u32)));
                if heap.len() > k { heap.pop(); }
            }
            let mut items: Vec<(u64, u32)> = heap.into_iter().map(|Reverse(t)| t).collect();
            // Sort by key DESC (largest scores first); tie-break by row index ASC for stability
            items.sort_by(|a, b| {
                match b.0.cmp(&a.0) {
                    std::cmp::Ordering::Equal => a.1.cmp(&b.1),
                    other => other,
                }
            });
            let idx: Vec<u32> = items.into_iter().map(|(_, i)| i).collect();
            let idx_u = UInt32Chunked::from_slice("__take".into(), &idx);
            let mut out = df.take(&idx_u)?;
            // Attach exact scores to selected top-k to allow secondary key ordering
            let mut scores2: Vec<f64> = Vec::with_capacity(out.height());
            let ser2 = out.column(&cname)?;
            for i in 0..ser2.len() {
                let v = vector_utils::extract_vec_f32_col(ser2, i).unwrap_or_default().into_iter().map(|x| x as f64).collect::<Vec<f64>>();
                let s = match func {
                    f if f.eq_ignore_ascii_case("vec_l2") => l2_distance(&v, &qvec),
                    f if f.eq_ignore_ascii_case("cosine_sim") => cosine_similarity(&v, &qvec),
                    _ => l2_distance(&v, &qvec),
                };
                scores2.push(s);
            }
            out.with_column(Series::new("__ann_score".into(), scores2))?;
            let (exprs, desc) = build_sort_keys(&out, rid_col_name.as_deref(), final_desc);
            let opts = polars::prelude::SortMultipleOptions { descending: desc, nulls_last: vec![true; exprs.len()], maintain_order: true, multithreaded: true, limit: Some(out.height() as polars::prelude::IdxSize) };
            let orig_cols: Vec<_> = df.get_column_names().iter().map(|s| col(s.as_str())).collect();
            let out2 = out.lazy().sort_by_exprs(exprs, opts).select(&orig_cols).collect()?;
            return Ok(out2);
        } else {
            // Want smallest k (ASC): maintain a max-heap of (score, idx); pop when new < largest
            let mut heap: std::collections::BinaryHeap<(u64, u32)> = std::collections::BinaryHeap::with_capacity(k + 1);
            for i in 0..n {
                let v = vector_utils::extract_vec_f32_col(col_series, i).unwrap_or_default().into_iter().map(|x| x as f64).collect::<Vec<f64>>();
                let score = match func {
                    f if f.eq_ignore_ascii_case("vec_l2") => l2_distance(&v, &qvec),
                    f if f.eq_ignore_ascii_case("cosine_sim") => cosine_similarity(&v, &qvec),
                    _ => l2_distance(&v, &qvec),
                };
                heap.push((f64_key(score), i as u32));
                if heap.len() > k { heap.pop(); }
            }
            let mut items: Vec<(u64, u32)> = heap.into_iter().collect();
            // Sort by key ASC (smallest scores first); tie-break by row index ASC for stability
            items.sort_by(|a, b| {
                match a.0.cmp(&b.0) {
                    std::cmp::Ordering::Equal => a.1.cmp(&b.1),
                    other => other,
                }
            });
            let idx: Vec<u32> = items.into_iter().map(|(_, i)| i).collect();
            let idx_u = UInt32Chunked::from_slice("__take".into(), &idx);
            let mut out = df.take(&idx_u)?;
            // Attach exact scores to selected top-k to allow secondary key ordering
            let mut scores2: Vec<f64> = Vec::with_capacity(out.height());
            let ser2 = out.column(&cname)?;
            for i in 0..ser2.len() {
                let v = vector_utils::extract_vec_f32_col(ser2, i).unwrap_or_default().into_iter().map(|x| x as f64).collect::<Vec<f64>>();
                let s = match func {
                    f if f.eq_ignore_ascii_case("vec_l2") => l2_distance(&v, &qvec),
                    f if f.eq_ignore_ascii_case("cosine_sim") => cosine_similarity(&v, &qvec),
                    _ => l2_distance(&v, &qvec),
                };
                scores2.push(s);
            }
            out.with_column(Series::new("__ann_score".into(), scores2))?;
            let (exprs, desc) = build_sort_keys(&out, rid_col_name.as_deref(), final_desc);
            let opts = polars::prelude::SortMultipleOptions { descending: desc, nulls_last: vec![true; exprs.len()], maintain_order: true, multithreaded: true, limit: Some(out.height() as polars::prelude::IdxSize) };
            let orig_cols: Vec<_> = df.get_column_names().iter().map(|s| col(s.as_str())).collect();
            let out2 = out.lazy().sort_by_exprs(exprs, opts).select(&orig_cols).collect()?;
            return Ok(out2);
        }
    }
    // No LIMIT k → compute scores and full sort with IdxSize limit (none)
    // Two-phase ANN preselect (diagnostic): if an index exists, preselect W = alpha * k' candidates first
    if let Some(store) = ctx.store.as_ref() {
        if let Some(ann) = ann_diag_for_order_by(ctx, &cname) {
            let n = df.height().max(1);
            let alpha = crate::system::get_vector_preselect_alpha().max(1) as usize;
            // Derive a working k' when LIMIT is absent; use min(100, n)
            let final_k = std::cmp::min(100usize, n);
            let w = std::cmp::min(n, alpha.saturating_mul(final_k));
            // Try to locate the matching .vindex and issue a preselect search
            let mut used_engine = "flat".to_string();
            let mut preselect_count: usize = 0;
            // Locate vindex matching table+column
            let root = store.0.lock().root_path().clone();
            'outer2: for db_ent in std::fs::read_dir(&root).unwrap_or_else(|_| std::fs::read_dir(".").unwrap()) {
                if let Ok(db_e) = db_ent {
                    let dbp = db_e.path(); if !dbp.is_dir() { continue; }
                    if let Ok(schemas) = std::fs::read_dir(&dbp) {
                        for sch_ent in schemas.flatten() {
                            let sp = sch_ent.path(); if !sp.is_dir() { continue; }
                            if let Ok(entries) = std::fs::read_dir(&sp) {
                                for e in entries.flatten() {
                                    let p = e.path();
                                    if p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("vindex") {
                                        if let Ok(text) = std::fs::read_to_string(&p) {
                                            if let Ok(vf) = serde_json::from_str::<VIndexFile>(&text) {
                                                let tbl_match = vf.table.eq_ignore_ascii_case(&ann.table) || vf.table.ends_with(&ann.table);
                                                if tbl_match && vf.column.eq_ignore_ascii_case(&ann.column) {
                                                    let qf: Vec<f32> = qvec.iter().map(|x| *x as f32).collect();
                                                    if let Ok(cands) = crate::server::exec::exec_vector_runtime::search_vector_index(store, &vf, &qf, w) {
                                                        preselect_count = cands.len();
                                                        #[cfg(feature = "ann_hnsw")]
                                                        { used_engine = "hnsw".to_string(); }
                                                    }
                                                    break 'outer2;
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
            tprintln!(
                "[ORDER_LIMIT][ANN] two-phase (no LIMIT): engine={}, metric_hint={:?}, alpha={}, W={}, final_k_derived={}, n={}; preselect_count={}",
                used_engine, index_metric, alpha, w, final_k, n, preselect_count
            );
        }
    }
    let mut scores: Vec<f64> = Vec::with_capacity(df.height());
    for i in 0..col_series.len() {
        let v = vector_utils::extract_vec_f32_col(col_series, i).unwrap_or_default().into_iter().map(|x| x as f64).collect::<Vec<f64>>();
        let score = match func {
            f if f.eq_ignore_ascii_case("vec_l2") => l2_distance(&v, &qvec),
            f if f.eq_ignore_ascii_case("cosine_sim") => cosine_similarity(&v, &qvec),
            _ => l2_distance(&v, &qvec),
        };
        scores.push(score);
    }
    let final_desc = !asc_hint;
    let score_series = Series::new("__ann_score".into(), scores);
    let mut df2 = df.clone();
    df2.with_column(score_series)?;
    // Build sort keys: primary by __ann_score, then by secondary ORDER BY keys, then stable row-id tie-break
    let (mut sort_exprs, mut descending) = build_sort_keys(&df2, rid_col_name.as_deref(), final_desc);
    let opts = polars::prelude::SortMultipleOptions {
        descending,
        nulls_last: vec![true; sort_exprs.len()],
        maintain_order: true,
        multithreaded: true,
        limit: None,
    };
    let orig_cols: Vec<_> = df.get_column_names().iter().map(|s| col(s.as_str())).collect();
    let df_sorted = df2.lazy().sort_by_exprs(sort_exprs, opts).select(&orig_cols).collect()?;
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

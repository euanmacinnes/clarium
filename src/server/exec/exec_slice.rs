use anyhow::Result;

use polars::prelude::*;
use crate::tprintln;


use crate::{
            server::exec::exec_common::{build_where_expr, collect_where_columns},
             storage::{ SharedStore}};

use crate::server::query::query_common::{SliceSource, SlicePlan, SliceOp};             

// Helper: compute simple stats for interval lists for logging
fn interval_stats(v: &Vec<(i64,i64)>) -> (usize, i64, i64) {
    if v.is_empty() { return (0, 0, 0); }
    let mut min_s = i64::MAX; let mut max_e = i64::MIN;
    for (s,e) in v {
        if *s < min_s { min_s = *s; }
        if *e > max_e { max_e = *e; }
    }
    (v.len(), min_s, max_e)
}
fn interval_stats_labeled(v: &Vec<(i64,i64,Vec<Option<String>>)> ) -> (usize, i64, i64) {
    if v.is_empty() { return (0, 0, 0); }
    let mut min_s = i64::MAX; let mut max_e = i64::MIN;
    for (s,e,_) in v {
        if *s < min_s { min_s = *s; }
        if *e > max_e { max_e = *e; }
    }
    (v.len(), min_s, max_e)
}

fn fmt_intervals(v: &Vec<(i64,i64)>, max_items: usize) -> String {
    let mut parts: Vec<String> = Vec::new();
    for (idx, (s,e)) in v.iter().enumerate() {
        if idx >= max_items { break; }
        parts.push(format!("[{}, {}]", s, e));
    }
    if v.len() > max_items { parts.push(format!("... ({} more)", v.len()-max_items)); }
    parts.join(", ").to_string()
}
fn fmt_intervals_labeled(v: &Vec<(i64,i64,Vec<Option<String>>)>, labels: &Vec<String>, max_items: usize) -> String {
    let mut parts: Vec<String> = Vec::new();
    for (idx, (s,e,labs)) in v.iter().enumerate() {
        if idx >= max_items { break; }
        let mut kvs: Vec<String> = Vec::new();
        for (i, name) in labels.iter().enumerate() {
            let val = labs.get(i).and_then(|o| o.as_ref()).map(|s| s.as_str()).unwrap_or("null");
            kvs.push(format!("{}:{}", name, val));
        }
        parts.push(format!("[{}, {}] {{{}}}", s, e, kvs.join(", ")));
    }
    if v.len() > max_items { parts.push(format!("... ({} more)", v.len()-max_items)); }
    parts.join(", ").to_string()
}

// --- SLICE evaluation ---
fn derive_labels_from_plan(plan: &SlicePlan) -> Option<Vec<String>> {    
    fn collect(src: &SliceSource, names: &mut Vec<String>, max_unnamed: &mut usize) {
        match src {
            SliceSource::Plan(p) => {
                if p.labels.is_some() { /* named elsewhere, skip */ }
                collect(&p.base, names, max_unnamed);
                for cl in &p.clauses { collect(&cl.source, names, max_unnamed); }
            }
            SliceSource::Manual { rows } => {
                // collect explicit names
                for r in rows {
                    let mut unnamed = 0usize;
                    for lab in &r.labels {
                        if let Some(n) = &lab.name {
                            if !names.iter().any(|x| x == n) { names.push(n.clone()); }
                        } else { unnamed += 1; }
                    }
                    if unnamed > *max_unnamed { *max_unnamed = unnamed; }
                }
            }
            SliceSource::Table { .. } => {}
        }
    }
    let mut names: Vec<String> = Vec::new();
    let mut max_unnamed: usize = 0;
    collect(&plan.base, &mut names, &mut max_unnamed);
    for cl in &plan.clauses { collect(&cl.source, &mut names, &mut max_unnamed); }
    if names.is_empty() && max_unnamed == 0 { return None; }
    // add auto names for unnamed
    for i in 0..max_unnamed { names.push(format!("label_{}", i+1)); }
    Some(names)
}

pub fn run_slice(store: &SharedStore, plan: &SlicePlan, ctx: &crate::server::data_context::DataContext) -> Result<DataFrame> {    
    // Determine label names: explicit plan labels or derive from manual sources
    let derived = derive_labels_from_plan(plan);
    if let Some(label_names) = plan.labels.as_ref().or(derived.as_ref()) {
        let mut cur = eval_slice_source_labeled(store, &plan.base, label_names, ctx)?;
        // Print base stats
        let (base_cnt, base_min, base_max) = interval_stats_labeled(&cur);
        tprintln!("BY SLICE (labeled) base: rows={} range=[{}, {}] labels={:?}", base_cnt, base_min, base_max, label_names);
        for cl in &plan.clauses {
            let (lhs_cnt, lhs_min, lhs_max) = interval_stats_labeled(&cur);
            let rhs = eval_slice_source_labeled(store, &cl.source, label_names, ctx)?;
            let (rhs_cnt, rhs_min, rhs_max) = interval_stats_labeled(&rhs);
            tprintln!("BY SLICE (labeled) {:?} input: lhs_rows={} range=[{}, {}], rhs_rows={} range=[{}, {}]", cl.op, lhs_cnt, lhs_min, lhs_max, rhs_cnt, rhs_min, rhs_max);
            cur = match cl.op { SliceOp::Intersect => intersect_labeled(&cur, &rhs), SliceOp::Union => union_labeled(&cur, &rhs) };
            let (res_cnt, res_min, res_max) = interval_stats_labeled(&cur);
            tprintln!("BY SLICE (labeled) {:?} result: rows={} range=[{}, {}]", cl.op, res_cnt, res_min, res_max);
        }
        // Build DataFrame with _start_date, _end_date and labels
        let mut starts: Vec<i64> = Vec::with_capacity(cur.len());
        let mut ends: Vec<i64> = Vec::with_capacity(cur.len());
        let mut label_cols: Vec<Vec<Option<String>>> = label_names.iter().map(|_| Vec::with_capacity(cur.len())).collect();
        for (s, e, labs) in cur {
            starts.push(s); ends.push(e);
            for (idx, _name) in label_names.iter().enumerate() {
                let v_opt = labs.get(idx).cloned().unwrap_or(None);
                label_cols[idx].push(v_opt);
            }
        }
        let mut cols: Vec<Series> = vec![Series::new("_start_date".into(), starts), Series::new("_end_date".into(), ends)];
        for (idx, name) in label_names.iter().enumerate() {
            cols.push(Series::new(name.clone().into(), label_cols[idx].clone()));
        }
        let df = DataFrame::new(cols.into_iter().map(|s| s.into()).collect())?;
        return Ok(df);
    }

    // Unlabeled path (legacy)
    let mut cur = eval_slice_source(store, &plan.base, ctx)?;
    let (base_cnt, base_min, base_max) = interval_stats(&cur);
    tprintln!("BY SLICE base: rows={} range=[{}, {}]", base_cnt, base_min, base_max);
    // Apply clauses
    for cl in &plan.clauses {
        let (lhs_cnt, lhs_min, lhs_max) = interval_stats(&cur);
        let rhs = eval_slice_source(store, &cl.source, ctx)?;
        let (rhs_cnt, rhs_min, rhs_max) = interval_stats(&rhs);
        tprintln!("BY SLICE {:?} input: lhs_rows={} range=[{}, {}], rhs_rows={} range=[{}, {}]", cl.op, lhs_cnt, lhs_min, lhs_max, rhs_cnt, rhs_min, rhs_max);
        cur = match cl.op { SliceOp::Intersect => intersect_intervals(&cur, &rhs), SliceOp::Union => union_intervals(&cur, &rhs) };
        let (res_cnt, res_min, res_max) = interval_stats(&cur);
        tprintln!("BY SLICE {:?} result: rows={} range=[{}, {}]", cl.op, res_cnt, res_min, res_max);
    }
    // Build DataFrame with _start_date and _end_date
    let mut starts: Vec<i64> = Vec::with_capacity(cur.len());
    let mut ends: Vec<i64> = Vec::with_capacity(cur.len());
    for (s, e) in cur { starts.push(s); ends.push(e); }
    let df = DataFrame::new(vec![Series::new("_start_date".into(), starts).into(), Series::new("_end_date".into(), ends).into()])?;
    Ok(df)
}

fn eval_slice_source(store: &SharedStore, src: &SliceSource, ctx: &crate::server::data_context::DataContext) -> Result<Vec<(i64,i64)>> {
    match src {
        SliceSource::Plan(p) => run_slice(store, p, ctx).map(|df| df_to_intervals(&df)),
        SliceSource::Manual { rows } => {
            let mut out: Vec<(i64,i64)> = rows.iter().filter_map(|r| if r.end >= r.start { Some((r.start, r.end)) } else { None }).collect();
            out.sort_by(|a,b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            Ok(merge_overlaps(out))
        }
        SliceSource::Table { database, start_col, end_col, where_clause, .. } => {
            let sc = start_col.as_deref().unwrap_or("_start_date");
            let ec = end_col.as_deref().unwrap_or("_end_date");
            // Load only needed columns plus where columns
            let mut cols = vec![sc.to_string(), ec.to_string()];
            if let Some(w) = where_clause { collect_where_columns(w, &mut cols); }
            cols.sort(); cols.dedup();
            let mut df = {
                let g = store.0.lock();
                if where_clause.is_some() {
                    g.read_df(database)?
                } else {
                    g.filter_df(database, &cols, None, None)?
                }
            };
            if let Some(w) = where_clause { df = df.lazy().filter(build_where_expr(w, ctx)).collect()?; }
            // Extract intervals
            let s_series = df.column(sc).map_err(|_| anyhow::anyhow!(format!("Column '{}' not found", sc)))?;
            let e_series = df.column(ec).map_err(|_| anyhow::anyhow!(format!("Column '{}' not found", ec)))?;
            let s_vals: Vec<Option<i64>> = if let Ok(ca) = s_series.i64() { ca.into_iter().collect() } else { s_series.cast(&DataType::Int64)?.i64()?.into_iter().collect() };
            let e_vals: Vec<Option<i64>> = if let Ok(ca) = e_series.i64() { ca.into_iter().collect() } else { e_series.cast(&DataType::Int64)?.i64()?.into_iter().collect() };
            let mut out: Vec<(i64,i64)> = Vec::new();
            for (os, oe) in s_vals.into_iter().zip(e_vals.into_iter()) {
                if let (Some(sv), Some(ev)) = (os, oe) { if ev >= sv { out.push((sv, ev)); } }
            }
            out.sort_by(|a,b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            Ok(merge_overlaps(out))
        }
    }
}

// Labeled SLICE source evaluation
fn eval_slice_source_labeled(store: &SharedStore, src: &SliceSource, label_names: &Vec<String>, ctx: &crate::server::data_context::DataContext) -> Result<Vec<(i64,i64,Vec<Option<String>>)>> {
    match src {
        SliceSource::Plan(p) => {
            let df = run_slice(store, p, ctx)?;
            // Expect df contains _start_date/_end_date and possibly label columns matching label_names
            let base = df_to_intervals(&df);
            let mut out: Vec<(i64,i64,Vec<Option<String>>)> = Vec::with_capacity(base.len());
            // Attempt to read labels if present; else nulls
            let mut cols_opt: Vec<Option<Series>> = Vec::new();
            for name in label_names {
                let ser_opt: Option<Series> = df.column(name).ok().and_then(|c| c.clone().as_series().cloned());
                cols_opt.push(ser_opt);
            }
            for (idx, (s,e)) in base.into_iter().enumerate() {
                let mut labs: Vec<Option<String>> = Vec::with_capacity(label_names.len());
                for (j, _nm) in label_names.iter().enumerate() {
                    let val_opt = if let Some(Some(series)) = cols_opt.get(j).map(|o| o.as_ref()) {
                        let av = series.get(idx);
                        match av {
                            Ok(AnyValue::StringOwned(vs)) => Some(vs.to_string()),
                            Ok(AnyValue::String(vs)) => Some(vs.to_string()),
                            Ok(AnyValue::Int64(vn)) => Some(vn.to_string()),
                            Ok(AnyValue::Float64(vf)) => Some(format!("{}", vf)),
                            Ok(AnyValue::Null) => None,
                            _ => None,
                        }
                    } else { None };
                    labs.push(val_opt);
                }
                out.push((s,e,labs));
            }
            Ok(out)
        }
        SliceSource::Manual { rows } => {
            use std::collections::HashMap;
            // map name -> index
            let mut name_idx: HashMap<&str, usize> = HashMap::new();
            for (i, n) in label_names.iter().enumerate() { name_idx.insert(n.as_str(), i); }
            let mut out: Vec<(i64,i64,Vec<Option<String>>)> = Vec::new();
            for r in rows {
                if r.end < r.start { continue; }
                let mut labs: Vec<Option<String>> = vec![None; label_names.len()];
                // first assign named labels
                for lab in &r.labels {
                    if let Some(name) = &lab.name {
                        if let Some(&idx) = name_idx.get(name.as_str()) {
                            labs[idx] = lab.value.clone();
                        }
                    }
                }
                // then assign unnamed in order to remaining positions
                let mut pos = 0usize;
                for lab in &r.labels {
                    if lab.name.is_none() {
                        // find next empty slot
                        while pos < labs.len() && labs[pos].is_some() { pos += 1; }
                        if pos < labs.len() { labs[pos] = lab.value.clone(); }
                        pos += 1;
                    }
                }
                out.push((r.start, r.end, labs));
            }
            // sort and merge with label-aware merging
            out.sort_by(|a,b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
            Ok(merge_overlaps_labeled(out))
        }
        SliceSource::Table { database, start_col, end_col, where_clause, label_values } => {
            let sc = start_col.as_deref().unwrap_or("_start_date");
            let ec = end_col.as_deref().unwrap_or("_end_date");
            // Collect required columns: start/end, where columns, and LABEL(...) referenced columns (non-quoted, non-NULL)
            let mut cols = vec![sc.to_string(), ec.to_string()];
            if let Some(w) = where_clause { collect_where_columns(w, &mut cols); }
            if let Some(exprs) = label_values {
                for expr in exprs {
                    let e = expr.trim();
                    let up = e.to_uppercase();
                    if (e.starts_with('\'') && e.ends_with('\'')) || (e.starts_with('"') && e.ends_with('"')) || up == "NULL" {
                        // literal or NULL, no column to load
                    } else {
                        cols.push(e.to_string());
                    }
                }
            }
            cols.sort(); cols.dedup();
            let mut df = {
                let g = store.0.lock();
                if where_clause.is_some() || label_values.is_some() {
                    // label expressions may reference arbitrary columns; load full df for simplicity
                    g.read_df(database)?
                } else {
                    g.filter_df(database, &cols, None, None)?
                }
            };
            if let Some(w) = where_clause { df = df.lazy().filter(build_where_expr(w, ctx)).collect()?; }
            // Extract intervals and labels per row
            let s_series = df.column(sc).map_err(|_| anyhow::anyhow!(format!("Column '{}' not found", sc)))?;
            let e_series = df.column(ec).map_err(|_| anyhow::anyhow!(format!("Column '{}' not found", ec)))?;
            let s_vals: Vec<Option<i64>> = if let Ok(ca) = s_series.i64() { ca.into_iter().collect() } else { s_series.cast(&DataType::Int64)?.i64()?.into_iter().collect() };
            let e_vals: Vec<Option<i64>> = if let Ok(ca) = e_series.i64() { ca.into_iter().collect() } else { e_series.cast(&DataType::Int64)?.i64()?.into_iter().collect() };
            let n = s_vals.len().min(e_vals.len());
            let mut out: Vec<(i64,i64,Vec<Option<String>>)> = Vec::new();
            for idx in 0..n {
                if let (Some(sv), Some(ev)) = (s_vals[idx], e_vals[idx]) {
                    if ev >= sv {
                        let mut labs: Vec<Option<String>> = vec![None; label_names.len()];
                        if let Some(exprs) = label_values {
                            for (pos, expr) in exprs.iter().enumerate() {
                                if pos >= label_names.len() { break; }
                                let e = expr.trim();
                                let up = e.to_uppercase();
                                let val_opt: Option<String> = if up == "NULL" { None } else if (e.starts_with('\'') && e.ends_with('\'')) || (e.starts_with('"') && e.ends_with('"')) {
                                    Some(e[1..e.len()-1].to_string())
                                } else if let Ok(s) = df.column(e) {
                                    match s.get(idx) {
                                        Ok(AnyValue::StringOwned(vs)) => Some(vs.to_string()),
                                        Ok(AnyValue::String(vs)) => Some(vs.to_string()),
                                        Ok(AnyValue::Int64(vn)) => Some(vn.to_string()),
                                        Ok(AnyValue::Float64(vf)) => Some(format!("{}", vf)),
                                        Ok(AnyValue::Null) => None,
                                        _ => None,
                                    }
                                } else { None };
                                labs[pos] = val_opt;
                            }
                        }
                        out.push((sv, ev, labs));
                    }
                }
            }
            // sort and merge only identical-label overlaps
            out.sort_by(|a,b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
            Ok(merge_overlaps_labeled(out))
        }
    }
}

fn labels_equal(a: &Vec<Option<String>>, b: &Vec<Option<String>>) -> bool { a == b }

fn merge_overlaps_labeled(mut v: Vec<(i64,i64,Vec<Option<String>>)>) -> Vec<(i64,i64,Vec<Option<String>>)> {
    if v.is_empty() { return v; }
    v.sort_by(|a,b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    let mut out: Vec<(i64,i64,Vec<Option<String>>)> = Vec::new();
    let mut cur = v[0].clone();
    for (s,e,l) in v.into_iter().skip(1) {
        if s <= cur.1 && labels_equal(&l, &cur.2) { cur.1 = cur.1.max(e); } else { out.push(cur); cur = (s,e,l); }
    }
    out.push(cur);
    out
}

fn intersect_labeled(a: &[(i64,i64,Vec<Option<String>>) ], b: &[(i64,i64,Vec<Option<String>>) ]) -> Vec<(i64,i64,Vec<Option<String>>)> {
    let mut i = 0usize; let mut j = 0usize; let mut out: Vec<(i64,i64,Vec<Option<String>>)> = Vec::new();
    while i < a.len() && j < b.len() {
        let (a1,a2,al) = (&a[i].0, &a[i].1, &a[i].2);
        let (b1,b2,bl) = (&b[j].0, &b[j].1, &b[j].2);
        let s = (*a1).max(*b1); let e = (*a2).min(*b2);
        if e >= s {
            // overlay labels: prefer RHS when it has Some(non-empty), else keep LHS
            let mut labs = al.clone();
            for k in 0..labs.len() {
                if k < bl.len() {
                    if let Some(ref v) = bl[k] {
                        if !v.is_empty() { labs[k] = Some(v.clone()); }
                    }
                }
            }
            out.push((s,e,labs));
        }
        if a2 < b2 { i += 1; } else { j += 1; }
    }
    merge_overlaps_labeled(out)
}

fn union_labeled(a: &[(i64,i64,Vec<Option<String>>) ], b: &[(i64,i64,Vec<Option<String>>) ]) -> Vec<(i64,i64,Vec<Option<String>>)> {
    // New semantics: coalesce regardless of label equality. Labels propagate left-to-right.
    // LHS labels stick; RHS labels only fill when LHS is null/empty. Ignore NULL/Empty RHS.
    fn is_empty_label(v: &Option<String>) -> bool { match v { None => true, Some(s) => s.is_empty() } }
    fn merge_labels_pref(lhs: &mut Vec<Option<String>>, rhs: &Vec<Option<String>>) {
        let n = lhs.len().max(rhs.len());
        if lhs.len() < n { lhs.resize(n, None); }
        for i in 0..n {
            let l = lhs.get(i).cloned().unwrap_or(None);
            if is_empty_label(&l) {
                if let Some(rval) = rhs.get(i).cloned().unwrap_or(None) {
                    if !rval.is_empty() { lhs[i] = Some(rval); }
                }
            }
        }
    }
    let mut v: Vec<(i64,i64,Vec<Option<String>>)> = Vec::with_capacity(a.len()+b.len());
    v.extend_from_slice(a); v.extend_from_slice(b);
    if v.is_empty() { return v; }
    v.sort_by(|x,y| x.0.cmp(&y.0).then(x.1.cmp(&y.1)));
    let mut out: Vec<(i64,i64,Vec<Option<String>>)> = Vec::new();
    let mut cur = v[0].clone();
    for (s,e,labs) in v.into_iter().skip(1) {
        if s <= cur.1 { // overlap or touch
            if e > cur.1 { cur.1 = e; }
            // Update labels with RHS filling only where LHS is null/empty
            merge_labels_pref(&mut cur.2, &labs);
        } else {
            out.push(cur);
            cur = (s,e,labs);
        }
    }
    out.push(cur);
    out
}

fn df_to_intervals(df: &DataFrame) -> Vec<(i64,i64)> {
    let s = df.column("_start_date").ok();
    let e = df.column("_end_date").ok();
    if s.is_none() || e.is_none() { return Vec::new(); }
    let s = s.unwrap(); let e = e.unwrap();
    let sv: Vec<Option<i64>> = if let Ok(ca) = s.i64() { ca.into_iter().collect() } else { Vec::new() };
    let ev: Vec<Option<i64>> = if let Ok(ca) = e.i64() { ca.into_iter().collect() } else { Vec::new() };
    let mut out = Vec::new();
    for (os, oe) in sv.into_iter().zip(ev.into_iter()) { if let (Some(a), Some(b)) = (os, oe) { out.push((a,b)); } }
    merge_overlaps(out)
}

fn merge_overlaps(mut v: Vec<(i64,i64)>) -> Vec<(i64,i64)> {
    if v.is_empty() { return v; }
    v.sort_by(|a,b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut out: Vec<(i64,i64)> = Vec::new();
    let mut cur = v[0];
    for &(s,e) in v.iter().skip(1) {
        if s <= cur.1 { cur.1 = cur.1.max(e); } else { out.push(cur); cur = (s,e); }
    }
    out.push(cur);
    out
}

fn intersect_intervals(a: &[(i64,i64)], b: &[(i64,i64)]) -> Vec<(i64,i64)> {
    let mut i = 0usize; let mut j = 0usize; let mut out = Vec::new();
    while i < a.len() && j < b.len() {
        let (a1,a2) = a[i]; let (b1,b2) = b[j];
        let s = a1.max(b1); let e = a2.min(b2);
        if e >= s { out.push((s,e)); }
        if a2 < b2 { i += 1; } else { j += 1; }
    }
    merge_overlaps(out)
}

fn union_intervals(a: &[(i64,i64)], b: &[(i64,i64)]) -> Vec<(i64,i64)> {
    let mut v: Vec<(i64,i64)> = Vec::with_capacity(a.len()+b.len());
    v.extend_from_slice(a); v.extend_from_slice(b);
    merge_overlaps(v)
}
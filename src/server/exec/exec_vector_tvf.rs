//! exec_vector_tvf
//! ----------------
//! Table-valued functions for vectors:
//! - nearest_neighbors(table, column, qvec, k [, metric, ef_search, with_ord])
//! - vector_search(index_name, qvec, k [, topk, engine])
//!
//! Returns a DataFrame with columns: row_id (UInt32), score (Float32) and optional ord (UInt32)
//! Polars 0.51+ compliant DataFrame construction.

use anyhow::{Result, anyhow};
use polars::prelude::*;

use crate::server::exec::vector_utils;
use crate::tprintln;

fn strip_quotes(x: &str) -> String {
    let t = x.trim();
    if (t.starts_with('"') && t.ends_with('"')) || (t.starts_with('\'') && t.ends_with('\'')) {
        if t.len() >= 2 { return t[1..t.len()-1].to_string(); }
    }
    t.to_string()
}

fn parse_func_args(call: &str) -> Option<(&str, Vec<String>)> {
    let s = call.trim();
    let open = s.find('(')?;
    if !s.ends_with(')') { return None; }
    let fname = &s[..open].trim();
    let inside = &s[open+1..s.len()-1];
    // Split on commas not inside quotes
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut in_sq = false; let mut in_dq = false; let mut prev_bs = false;
    for ch in inside.chars() {
        if ch == '\\' { prev_bs = !prev_bs; cur.push(ch); continue; } else { prev_bs = false; }
        if ch == '\'' && !in_dq { in_sq = !in_sq; cur.push(ch); continue; }
        if ch == '"' && !in_sq { in_dq = !in_dq; cur.push(ch); continue; }
        if ch == ',' && !in_sq && !in_dq { out.push(cur.trim().to_string()); cur.clear(); continue; }
        cur.push(ch);
    }
    if !cur.is_empty() { out.push(cur.trim().to_string()); }
    Some((fname, out))
}

pub fn try_vector_tvf(store: &crate::storage::SharedStore, raw: &str) -> Result<Option<DataFrame>> {
    let s = raw.trim();
    let low = s.to_ascii_lowercase();
    if !(low.starts_with("nearest_neighbors(") || low.starts_with("vector_search(")) {
        return Ok(None);
    }
    let (fname, args) = match parse_func_args(s) { Some(v) => v, None => return Ok(None) };
    let fname_low = fname.to_ascii_lowercase();
    match fname_low.as_str() {
        "vector_search" => {
            // vector_search(index_name, qvec, k [, topk, engine])
            if args.len() < 3 { anyhow::bail!("vector_search(index_name, qvec, k) requires 3 args"); }
            let index_name = strip_quotes(&args[0]);
            let qvec_s = strip_quotes(&args[1]);
            let k: usize = strip_quotes(&args[2]).parse::<usize>().unwrap_or(10);
            let topk_opt = args.get(3).and_then(|a| strip_quotes(a).parse::<usize>().ok());
            let engine_hint = args.get(4).map(|a| strip_quotes(a));
            let qvec = vector_utils::parse_vec_literal(&qvec_s).ok_or_else(|| anyhow!("invalid qvec for vector_search"))?;
            let qualified = crate::ident::qualify_regular_ident(&index_name, &crate::system::current_query_defaults());
            let vf = match crate::server::exec::exec_vector_index::read_vindex_file(store, &qualified)? {
                Some(v) => v,
                None => anyhow::bail!(format!("Vector index not found: {}", index_name)),
            };
            let metric = vf.metric.clone().unwrap_or_else(|| "l2".to_string());
            let ef_search = vf.params.as_ref().and_then(|p| p.get("ef_search")).and_then(|x| x.as_i64()).unwrap_or(0);
            // Detect ANN engine availability (only when feature enabled and file exists); else flat
            #[cfg(feature = "ann_hnsw")]
            let ann_path_exists = {
                let mut p = store.0.lock().root_path().clone();
                let local = qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
                p.push(local); p.set_extension("hnsw"); p.exists()
            };
            #[cfg(not(feature = "ann_hnsw"))]
            let ann_path_exists = false;
            let engine_auto = if ann_path_exists { "hnsw" } else { "flat" };
            let engine_effective = engine_hint.as_deref().unwrap_or(engine_auto);
            tprintln!("[ann.tvf] vector_search index={} engine={} metric={} ef_search={} k={} topk={:?}", qualified, engine_effective, metric, ef_search, k, topk_opt);
            let opts = crate::server::exec::exec_vector_runtime::SearchOptions {
                metric_override: None,
                ef_search: ef_search.try_into().ok(),
                engine_hint: engine_hint.clone(),
            };
            let mut res = crate::server::exec::exec_vector_runtime::search_vector_index_with_opts(store, &vf, &qvec, k, &opts)?;
            if let Some(topk) = topk_opt { if topk < res.len() { res.truncate(topk); } }
            let (mut ids, mut scores): (Vec<u32>, Vec<f32>) = (Vec::with_capacity(res.len()), Vec::with_capacity(res.len()));
            for (id, sc) in res { ids.push(id); scores.push(sc); }
            let df = DataFrame::new(vec![
                Series::new("row_id".into(), ids).into(),
                Series::new("score".into(), scores).into(),
            ])?;
            return Ok(Some(df));
        }
        "nearest_neighbors" => {
            // nearest_neighbors(table, column, qvec, k [, metric, ef_search, with_ord])
            if args.len() < 4 { anyhow::bail!("nearest_neighbors(table, column, qvec, k) requires at least 4 args"); }
            let table = strip_quotes(&args[0]);
            let column = strip_quotes(&args[1]);
            let qvec_s = strip_quotes(&args[2]);
            let k: usize = strip_quotes(&args[3]).parse::<usize>().unwrap_or(10);
            let metric = args.get(4).map(|a| strip_quotes(a).to_ascii_lowercase());
            let ef_search_opt = args.get(5).and_then(|a| strip_quotes(a).parse::<usize>().ok());
            let with_ord = args.get(6).map(|a| strip_quotes(a)).map(|s| {
                let ls = s.to_ascii_lowercase();
                ls == "1" || ls == "true" || ls == "yes"
            }).unwrap_or(false);
            let qvec = vector_utils::parse_vec_literal(&qvec_s).ok_or_else(|| anyhow!("invalid qvec for nearest_neighbors"))?;
            // Try to find matching index first; else exact scan
            let qualified_table = crate::ident::qualify_regular_ident(&table, &crate::system::current_query_defaults());
            if let Some(mut vf) = crate::server::exec::exec_vector_index::read_vindex_file(store, &format!("{}/idx_{}_{}", qualified_table, qualified_table.replace('/',"_"), column)).ok().flatten() {
                // Not reliable naming; fall back to directory scan for .vindex matching table+column
                let root = store.0.lock().root_path().clone();
                'scan: for db_ent in std::fs::read_dir(&root).unwrap_or_else(|_| std::fs::read_dir(".").unwrap()) {
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
                                                if let Ok(vvx) = serde_json::from_str::<crate::server::exec::exec_vector_index::VIndexFile>(&text) {
                                                    let tbl_match = vvx.table.eq_ignore_ascii_case(&qualified_table) || vvx.table.ends_with(&qualified_table);
                                                    if tbl_match && vvx.column.eq_ignore_ascii_case(&column) { vf = vvx; break 'scan; }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                let metric_used = vf.metric.clone().unwrap_or_else(|| metric.clone().unwrap_or_else(|| "l2".into()));
                let ef_search = vf.params.as_ref().and_then(|p| p.get("ef_search")).and_then(|x| x.as_i64()).unwrap_or(0);
                #[cfg(feature = "ann_hnsw")]
                let ann_available = {
                    let mut p = store.0.lock().root_path().clone();
                    let local = vf.qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
                    p.push(local); p.set_extension("hnsw"); p.exists()
                };
                #[cfg(not(feature = "ann_hnsw"))]
                let ann_available = false;
                let engine = if ann_available { "hnsw" } else { "flat" };
                tprintln!("[ann.tvf] nearest_neighbors table={} col={} engine={} metric={} ef_search={} k={} with_ord={}", qualified_table, column, engine, metric_used, ef_search, k, with_ord);
                let opts = crate::server::exec::exec_vector_runtime::SearchOptions {
                    metric_override: metric.clone(),
                    ef_search: ef_search_opt.or_else(|| ef_search.try_into().ok()),
                    engine_hint: None,
                };
                let res = crate::server::exec::exec_vector_runtime::search_vector_index_with_opts(store, &vf, &qvec, k, &opts)?;
                let (mut ids, mut scores): (Vec<u32>, Vec<f32>) = (Vec::with_capacity(res.len()), Vec::with_capacity(res.len()));
                for (id, sc) in res { ids.push(id); scores.push(sc); }
                let mut cols: Vec<Column> = vec![Series::new("row_id".into(), ids).into(), Series::new("score".into(), scores).into()];
                if with_ord {
                    // We cannot recover original row ordinal from the vindex without scanning source; leave absent on index path.
                    // Include an empty ord column for schema stability when requested.
                    cols.push(Series::new("ord".into(), Vec::<u32>::new()).into());
                }
                let df = DataFrame::new(cols)?;
                return Ok(Some(df));
            } else {
                // Exact scan of table
                let df = store.0.lock().read_df(&qualified_table)?;
                let cname: String = df
                    .get_column_names()
                    .iter()
                    .find(|c| c.eq_ignore_ascii_case(&column))
                    .map(|c| c.as_str().to_string())
                    .unwrap_or_else(|| column.clone());
                let col = df.column(cname.as_str())?;
                // Build heap depending on metric
                let use_desc = match metric.as_deref() { Some("ip") | Some("dot") | Some("cosine") => true, _ => false };
                #[inline] fn f32_key(v: f32) -> u32 { let b = v.to_bits(); if b & (1u32<<31) != 0 { !b } else { b | (1u32<<31) } }
                if use_desc {
                    let mut heap: std::collections::BinaryHeap<std::cmp::Reverse<(u32, u32)>> = std::collections::BinaryHeap::with_capacity(k+1);
                    for i in 0..col.len() {
                        let v = vector_utils::extract_vec_f32_col(col, i).unwrap_or_default();
                        if v.is_empty() { continue; }
                        let s = match metric.as_deref() { Some("cosine") => cosine(&v, &qvec), Some("ip")|Some("dot") => dot(&v,&qvec), _ => -l2(&v,&qvec) };
                        heap.push(std::cmp::Reverse((f32_key(s), i as u32)));
                        if heap.len()>k { heap.pop(); }
                    }
                    let mut items: Vec<(u32,u32)> = heap.into_iter().map(|std::cmp::Reverse(t)| t).collect();
                    items.sort_by(|a,b| b.0.cmp(&a.0));
                    let mut ids: Vec<u32> = Vec::with_capacity(items.len()); let mut scores: Vec<f32> = Vec::with_capacity(items.len()); let mut ords: Vec<u32> = Vec::with_capacity(items.len());
                    for (_k, i) in items { ids.push(i); let vv = vector_utils::extract_vec_f32_col(col, i as usize).unwrap_or_default(); let sc = match metric.as_deref(){Some("cosine")=>cosine(&vv,&qvec),Some("ip")|Some("dot")=>dot(&vv,&qvec), _=>l2(&vv,&qvec)}; scores.push(sc); ords.push(i);}
                    let mut cols: Vec<Column> = vec![Series::new("row_id".into(), ids).into(), Series::new("score".into(), scores).into()];
                    if with_ord { cols.push(Series::new("ord".into(), ords).into()); }
                    let df = DataFrame::new(cols)?;
                    tprintln!("[ann.tvf] nearest_neighbors table={} col={} engine=flat metric={} k={} note=exact-scan-no-index", qualified_table, column, metric.as_deref().unwrap_or("l2"), k);
                    return Ok(Some(df));
                } else {
                    let mut heap: std::collections::BinaryHeap<(u32, u32)> = std::collections::BinaryHeap::with_capacity(k+1);
                    for i in 0..col.len() {
                        let v = vector_utils::extract_vec_f32_col(col, i).unwrap_or_default();
                        if v.is_empty() { continue; }
                        let d = l2(&v, &qvec) as f32;
                        let key = f32_key(-d);
                        heap.push((key, i as u32));
                        if heap.len()>k { heap.pop(); }
                    }
                    let mut items: Vec<(u32,u32)> = heap.into_iter().collect();
                    items.sort_by(|a,b| b.0.cmp(&a.0));
                    let mut ids: Vec<u32> = Vec::with_capacity(items.len()); let mut scores: Vec<f32> = Vec::with_capacity(items.len()); let mut ords: Vec<u32> = Vec::with_capacity(items.len());
                    for (_k, i) in items { ids.push(i); let vv = vector_utils::extract_vec_f32_col(col, i as usize).unwrap_or_default(); scores.push(l2(&vv,&qvec)); ords.push(i); }
                    let mut cols: Vec<Column> = vec![Series::new("row_id".into(), ids).into(), Series::new("score".into(), scores).into()];
                    if with_ord { cols.push(Series::new("ord".into(), ords).into()); }
                    let df = DataFrame::new(cols)?;
                    tprintln!("[ann.tvf] nearest_neighbors table={} col={} engine=flat metric={} k={} note=exact-scan-no-index", qualified_table, column, metric.as_deref().unwrap_or("l2"), k);
                    return Ok(Some(df));
                }
            }
        }
        _ => {}
    }
    Ok(None)
}

// Compose minimal EXPLAIN annotation text for vector TVFs.
pub fn explain_vector_expr(store: &crate::storage::SharedStore, raw: &str) -> Option<String> {
    let s = raw.trim();
    let low = s.to_ascii_lowercase();
    if !(low.starts_with("nearest_neighbors(") || low.starts_with("vector_search(")) {
        return None;
    }
    let (fname, args) = match parse_func_args(s) { Some(v) => v, None => return None };
    let fname_low = fname.to_ascii_lowercase();
    match fname_low.as_str() {
        "vector_search" => {
            if args.len() < 3 { return Some("EXPLAIN error: vector_search requires 3 args".to_string()); }
            let index_name = strip_quotes(&args[0]);
            let qualified = crate::ident::qualify_regular_ident(&index_name, &crate::system::current_query_defaults());
            let vf = match crate::server::exec::exec_vector_index::read_vindex_file(store, &qualified).ok().flatten() { Some(v) => v, None => return Some(format!("EXPLAIN: EXACT (flat) — reason: index not found: {}", index_name)) };
            let metric = vf.metric.clone().unwrap_or_else(|| "l2".to_string());
            let ef_search = vf.params.as_ref().and_then(|p| p.get("ef_search")).and_then(|x| x.as_i64()).unwrap_or(0);
            #[cfg(feature = "ann_hnsw")]
            let ann_available = {
                let mut p = store.0.lock().root_path().clone();
                let local = vf.qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
                p.push(local); p.set_extension("hnsw"); p.exists()
            };
            #[cfg(not(feature = "ann_hnsw"))]
            let ann_available = false;
            let path = if ann_available { "ANN(HNSW)" } else { "EXACT(flat)" };
            let mut notes = Vec::new();
            if !ann_available { notes.push("hnsw_artifact_missing-or-feature-disabled".to_string()); }
            Some(format!("EXPLAIN: {} index={} metric={} ef_search={} preselect_W=- notes={}", path, vf.name, metric, ef_search, if notes.is_empty(){"-".into()} else { notes.join(";") }))
        }
        "nearest_neighbors" => {
            if args.len() < 4 { return Some("EXPLAIN error: nearest_neighbors requires at least 4 args".to_string()); }
            let table = strip_quotes(&args[0]);
            let column = strip_quotes(&args[1]);
            let qualified_table = crate::ident::qualify_regular_ident(&table, &crate::system::current_query_defaults());
            // Try to find index
            let mut found: Option<crate::server::exec::exec_vector_index::VIndexFile> = None;
            let root = store.0.lock().root_path().clone();
            if let Ok(dbs) = std::fs::read_dir(&root) {
                'outer: for db_ent in dbs.flatten() {
                    let dbp = db_ent.path(); if !dbp.is_dir() { continue; }
                    if let Ok(schemas) = std::fs::read_dir(&dbp) {
                        for sch_ent in schemas.flatten() {
                            let sp = sch_ent.path(); if !sp.is_dir() { continue; }
                            if let Ok(entries) = std::fs::read_dir(&sp) {
                                for e in entries.flatten() {
                                    let p = e.path();
                                    if p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("vindex") {
                                        if let Ok(text) = std::fs::read_to_string(&p) {
                                            if let Ok(vvx) = serde_json::from_str::<crate::server::exec::exec_vector_index::VIndexFile>(&text) {
                                                let tbl_match = vvx.table.eq_ignore_ascii_case(&qualified_table) || vvx.table.ends_with(&qualified_table);
                                                if tbl_match && vvx.column.eq_ignore_ascii_case(&column) { found = Some(vvx); break 'outer; }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if let Some(vf) = found {
                let metric = vf.metric.clone().unwrap_or_else(|| "l2".to_string());
                let ef_search = vf.params.as_ref().and_then(|p| p.get("ef_search")).and_then(|x| x.as_i64()).unwrap_or(0);
                #[cfg(feature = "ann_hnsw")]
                let ann_available = {
                    let mut p = store.0.lock().root_path().clone();
                    let local = vf.qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
                    p.push(local); p.set_extension("hnsw"); p.exists()
                };
                #[cfg(not(feature = "ann_hnsw"))]
                let ann_available = false;
                let path = if ann_available { "ANN(HNSW)" } else { "EXACT(flat)" };
                let mut notes = Vec::new();
                if !ann_available { notes.push("hnsw_artifact_missing-or-feature-disabled".to_string()); }
                Some(format!("EXPLAIN: {} index={} metric={} ef_search={} preselect_W=- notes={}", path, vf.name, metric, ef_search, if notes.is_empty(){"-".into()} else { notes.join(";") }))
            } else {
                Some("EXPLAIN: EXACT(flat) — reason: no matching index (exact table scan)".to_string())
            }
        }
        _ => None
    }
}

#[inline]
fn l2(a: &Vec<f32>, b: &Vec<f32>) -> f32 {
    let n = a.len().min(b.len());
    if n == 0 { return f32::INFINITY; }
    let mut s = 0.0f32;
    for i in 0..n { let d = a[i] - b[i]; s += d*d; }
    s.sqrt()
}
#[inline]
fn dot(a: &Vec<f32>, b: &Vec<f32>) -> f32 { a.iter().zip(b.iter()).map(|(x,y)| x*y).sum() }
#[inline]
fn cosine(a: &Vec<f32>, b: &Vec<f32>) -> f32 {
    let na = a.iter().map(|x| x*x).sum::<f32>().sqrt();
    let nb = b.iter().map(|x| x*x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 { return f32::NAN; }
    dot(a,b) / (na*nb)
}

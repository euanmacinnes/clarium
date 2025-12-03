//! exec_graph_runtime
//! ------------------
//! Runtime helpers to materialize Graph TVFs (graph_neighbors, graph_paths)
//! backed by `.graph` catalogs and regular edge tables.

use anyhow::Result;
use polars::prelude::*;
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;

use crate::storage::SharedStore;

#[derive(Debug, Clone, Deserialize)]
struct GraphNodeDef { label: String, key: String, table: Option<String>, key_column: Option<String> }

#[derive(Debug, Clone, Deserialize)]
struct GraphEdgeDef {
    #[serde(rename = "type")] r#type: String,
    from: String,
    to: String,
    table: Option<String>,
    src_column: Option<String>,
    dst_column: Option<String>,
    cost_column: Option<String>,
    time_column: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct GraphFile {
    version: i32,
    name: String,
    qualified: String,
    nodes: Vec<GraphNodeDef>,
    edges: Vec<GraphEdgeDef>,
    created_at: Option<String>,
}

fn path_for_graph(store: &SharedStore, qualified: &str) -> PathBuf {
    let mut p = store.0.lock().root_path().clone();
    let local = qualified.replace('/', std::path::MAIN_SEPARATOR_STR);
    p.push(local);
    p.set_extension("graph");
    p
}

fn read_graph_file(store: &SharedStore, qualified: &str) -> Result<GraphFile> {
    let p = path_for_graph(store, qualified);
    let text = std::fs::read_to_string(&p)?;
    let gf: GraphFile = serde_json::from_str(&text)?;
    Ok(gf)
}

fn qualify_graph_name(name: &str) -> String {
    let d = crate::ident::QueryDefaults::from_options(
        Some(&crate::system::get_current_database()),
        Some(&crate::system::get_current_schema()),
    );
    crate::ident::qualify_regular_ident(name, &d)
}

fn load_edges_df(
    store: &SharedStore,
    gf: &GraphFile,
    etype: Option<&str>,
) -> Result<(DataFrame, String, String, Option<String>, Option<String>)> {
    // Pick first edge mapping, or one matching the requested type
    let e = if let Some(t) = etype {
        let tu = t.to_ascii_lowercase();
        gf.edges
            .iter()
            .find(|e| e.r#type.to_ascii_lowercase() == tu)
            .or_else(|| gf.edges.get(0))
            .ok_or_else(|| anyhow::anyhow!("Graph has no edges defined"))?
    } else {
        gf.edges.get(0).ok_or_else(|| anyhow::anyhow!("Graph has no edges defined"))?
    };
    let table = e
        .table
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Graph edges table not bound; use USING TABLES (edges=...) when creating graph"))?;
    let src_col = e.src_column.clone().unwrap_or_else(|| "src".to_string());
    let dst_col = e.dst_column.clone().unwrap_or_else(|| "dst".to_string());
    let cost_col = e.cost_column.clone();
    // Only apply temporal filtering if a time_column is configured in the catalog
    let time_col = e.time_column.clone();
    let guard = store.0.lock();
    let df = guard.read_df(&table)?;
    Ok((df, src_col, dst_col, cost_col, time_col))
}

/// Materialize graph_neighbors(graph, start, etype, max_hops[, time_start, time_end])
/// time_start/time_end are optional ISO8601 strings or integer epoch millis; if provided and a time_column exists in the catalog,
/// only edges within [time_start, time_end] (inclusive) are considered.
pub fn graph_neighbors_df(
    store: &SharedStore,
    graph: &str,
    start: &str,
    etype: Option<&str>,
    max_hops: i64,
    time_start: Option<&str>,
    time_end: Option<&str>,
) -> Result<DataFrame> {
    let qname = qualify_graph_name(graph);
    let gf = read_graph_file(store, &qname)?;
    let (mut edges_df, src_col, dst_col, _cost, time_col_opt) = load_edges_df(store, &gf, etype)?;
    // Optional temporal filter: apply if at least one bound is supplied and time column exists
    if let Some(time_col) = time_col_opt.clone() {
        if time_start.is_some() || time_end.is_some() {
            let t0 = time_start.and_then(parse_time_to_i64);
            let t1 = time_end.and_then(parse_time_to_i64);
            if t0.is_some() || t1.is_some() {
                if let Ok(col) = edges_df.column(&time_col) {
                    let mask = col
                        .iter()
                        .map(|v| {
                            let val = match v.try_extract::<i64>() {
                                Ok(n) => Some(n),
                                Err(_) => match v.try_extract::<&str>() { Ok(s) => parse_time_to_i64(s), Err(_) => None },
                            };
                            if let Some(n) = val {
                                let ge = t0.map(|a| n >= a).unwrap_or(true);
                                let le = t1.map(|b| n <= b).unwrap_or(true);
                                Some(ge && le)
                            } else { Some(false) }
                        })
                        .collect::<Vec<Option<bool>>>();
                    let bools: Vec<bool> = mask.into_iter().map(|o| o.unwrap_or(false)).collect();
                    let s = polars::prelude::BooleanChunked::from_slice("__mask", &bools);
                    edges_df = edges_df.filter(&s)?;
                }
            }
        }
    }
    // For robustness, accept Utf8 or general string-like columns via `to_string` fallback
    let src_series = edges_df.column(&src_col)?;
    let dst_series = edges_df.column(&dst_col)?;
    let src: Vec<String> = match src_series.utf8() {
        Ok(ca) => ca.into_no_null_iter().map(|s| s.to_string()).collect(),
        Err(_) => src_series.iter().map(|v| v.to_string()).collect(),
    };
    let dst: Vec<String> = match dst_series.utf8() {
        Ok(ca) => ca.into_no_null_iter().map(|s| s.to_string()).collect(),
        Err(_) => dst_series.iter().map(|v| v.to_string()).collect(),
    };
    // Build adjacency list
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();
    for (s, d) in src.iter().zip(dst.iter()) {
        adj.entry(s.clone()).or_default().push(d.clone());
    }
    // BFS up to max_hops
    let mut out_node: Vec<String> = Vec::new();
    let mut out_prev: Vec<String> = Vec::new();
    let mut out_hop: Vec<i64> = Vec::new();
    let mut q: VecDeque<(String, Option<String>, i64)> = VecDeque::new();
    let mut seen: HashMap<String, i64> = HashMap::new();
    q.push_back((start.to_string(), None, 0));
    seen.insert(start.to_string(), 0);
    while let Some((node, prev, hop)) = q.pop_front() {
        if hop >= 1 { // exclude the start node from output
            out_node.push(node.clone());
            out_prev.push(prev.unwrap_or_default());
            out_hop.push(hop);
        }
        if hop >= max_hops { continue; }
        if let Some(neis) = adj.get(&node) {
            for n in neis {
                if !seen.contains_key(n) {
                    seen.insert(n.clone(), hop + 1);
                    q.push_back((n.clone(), Some(node.clone()), hop + 1));
                }
            }
        }
    }
    Ok(DataFrame::new(vec![
        Series::new("node_id", out_node),
        Series::new("prev_id", out_prev),
        Series::new("hop", out_hop),
    ])?)
}

/// Materialize graph_paths(graph, src, dst, max_hops[, etype[, time_start, time_end]]) –
/// returns one cheapest (by cost if available, else shortest hops) path (if any). Optional temporal window behaves like in neighbors.
pub fn graph_paths_df(
    store: &SharedStore,
    graph: &str,
    src_id: &str,
    dst_id: &str,
    max_hops: i64,
    etype: Option<&str>,
    time_start: Option<&str>,
    time_end: Option<&str>,
) -> Result<DataFrame> {
    let qname = qualify_graph_name(graph);
    let gf = read_graph_file(store, &qname)?;
    let (mut edges_df, src_col, dst_col, cost_col_opt, time_col_opt) = load_edges_df(store, &gf, etype)?;
    if let Some(time_col) = time_col_opt.clone() {
        if time_start.is_some() || time_end.is_some() {
            let t0 = time_start.and_then(parse_time_to_i64);
            let t1 = time_end.and_then(parse_time_to_i64);
            if t0.is_some() || t1.is_some() {
                if let Ok(col) = edges_df.column(&time_col) {
                    let mask = col
                        .iter()
                        .map(|v| {
                            let val = match v.try_extract::<i64>() {
                                Ok(n) => Some(n),
                                Err(_) => match v.try_extract::<&str>() { Ok(s) => parse_time_to_i64(s), Err(_) => None },
                            };
                            if let Some(n) = val {
                                let ge = t0.map(|a| n >= a).unwrap_or(true);
                                let le = t1.map(|b| n <= b).unwrap_or(true);
                                Some(ge && le)
                            } else { Some(false) }
                        })
                        .collect::<Vec<Option<bool>>>();
                    let bools: Vec<bool> = mask.into_iter().map(|o| o.unwrap_or(false)).collect();
                    let s = polars::prelude::BooleanChunked::from_slice("__mask", &bools);
                    edges_df = edges_df.filter(&s)?;
                }
            }
        }
    }
    // Extract columns as strings and optional costs
    let src_series = edges_df.column(&src_col)?;
    let dst_series = edges_df.column(&dst_col)?;
    let src: Vec<String> = match src_series.utf8() {
        Ok(ca) => ca.into_no_null_iter().map(|s| s.to_string()).collect(),
        Err(_) => src_series.iter().map(|v| v.to_string()).collect(),
    };
    let dst: Vec<String> = match dst_series.utf8() {
        Ok(ca) => ca.into_no_null_iter().map(|s| s.to_string()).collect(),
        Err(_) => dst_series.iter().map(|v| v.to_string()).collect(),
    };
    let costs: Option<Vec<f64>> = if let Some(cc) = &cost_col_opt {
        let cser = edges_df.column(cc)?;
        let mut out: Vec<f64> = Vec::with_capacity(cser.len());
        for v in cser.iter() {
            match v.try_extract::<f64>() { Ok(n) => out.push(n), Err(_) => out.push(1.0) }
        }
        Some(out)
    } else { None };
    // Build adjacency with optional cost
    let mut adj: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    for idx in 0..src.len().min(dst.len()) {
        let s = src[idx].clone();
        let d = dst[idx].clone();
        let w = costs.as_ref().and_then(|v| v.get(idx).copied()).unwrap_or(1.0);
        adj.entry(s).or_default().push((d, w));
    }
    let use_weighted = costs.is_some();
    if use_weighted {
        // Dijkstra (bounded by max_hops via a depth map) to minimize total cost
        use std::cmp::Ordering;
        #[derive(Clone)]
        struct State { node: String, cost: f64, hops: i64 }
        let mut dist: HashMap<String, f64> = HashMap::new();
        let mut hops: HashMap<String, i64> = HashMap::new();
        let mut prev: HashMap<String, String> = HashMap::new();
        let mut heap: std::collections::BinaryHeap<std::cmp::Reverse<(i64, i64, String)>> = std::collections::BinaryHeap::new();
        // (primary by cost scaled to i64 via ordering proxy, secondary by hops) — we will compare by a tuple
        dist.insert(src_id.to_string(), 0.0);
        hops.insert(src_id.to_string(), 0);
        heap.push(std::cmp::Reverse((0i64, 0i64, src_id.to_string())));
        while let Some(std::cmp::Reverse((_pcost, phops, u))) = heap.pop() {
            let cur_hops = *hops.get(&u).unwrap_or(&i64::MAX);
            if cur_hops > max_hops { continue; }
            if let Some(neis) = adj.get(&u) {
                for (v, w) in neis {
                    let next_hops = cur_hops + 1;
                    if next_hops > max_hops { continue; }
                    let alt = dist.get(&u).copied().unwrap_or(f64::INFINITY) + *w;
                    let dv = dist.get(v).copied().unwrap_or(f64::INFINITY);
                    if alt + 1e-12 < dv {
                        dist.insert(v.clone(), alt);
                        hops.insert(v.clone(), next_hops);
                        prev.insert(v.clone(), u.clone());
                        // pack cost as i64 ordering proxy; beware overflow; use scaled representation
                        let ord_cost = (alt * 1_000_000.0).round() as i64;
                        heap.push(std::cmp::Reverse((ord_cost, next_hops, v.clone())));
                    }
                }
            }
        }
        if !dist.contains_key(dst_id) { return Ok(DataFrame::new(vec![Series::new("path_id", Vec::<i64>::new()).into()])?); }
        // Reconstruct path
        let mut nodes: Vec<String> = Vec::new();
        let mut cur = dst_id.to_string();
        nodes.push(cur.clone());
        while let Some(p) = prev.get(&cur) { nodes.push(p.clone()); cur = p.clone(); }
        nodes.reverse();
        let ord: Vec<i64> = (0..nodes.len() as i64).collect();
        let path_id: Vec<i64> = vec![1; nodes.len()];
        return Ok(DataFrame::new(vec![
            Series::new("path_id", path_id),
            Series::new("node_id", nodes),
            Series::new("ord", ord),
        ])?);
    } else {
        // Unweighted BFS shortest hops
        let mut prev: HashMap<String, String> = HashMap::new();
        let mut q: VecDeque<String> = VecDeque::new();
        let mut depth: HashMap<String, i64> = HashMap::new();
        q.push_back(src_id.to_string());
        depth.insert(src_id.to_string(), 0);
        let mut found = false;
        while let Some(u) = q.pop_front() {
            let h = *depth.get(&u).unwrap_or(&0);
            if h >= max_hops { continue; }
            if let Some(neis) = adj.get(&u) {
                for (v, _w) in neis {
                    if !depth.contains_key(v) {
                        depth.insert(v.clone(), h + 1);
                        prev.insert(v.clone(), u.clone());
                        if v == dst_id { found = true; break; }
                        q.push_back(v.clone());
                    }
                }
            }
            if found { break; }
        }
        if !found { return Ok(DataFrame::new(vec![Series::new("path_id", Vec::<i64>::new()).into()])?); }
        // Reconstruct path
        let mut nodes: Vec<String> = Vec::new();
        let mut cur = dst_id.to_string();
        nodes.push(cur.clone());
        while let Some(p) = prev.get(&cur) { nodes.push(p.clone()); cur = p.clone(); }
        nodes.reverse();
        let ord: Vec<i64> = (0..nodes.len() as i64).collect();
        let path_id: Vec<i64> = vec![1; nodes.len()];
        Ok(DataFrame::new(vec![
            Series::new("path_id", path_id),
            Series::new("node_id", nodes),
            Series::new("ord", ord),
        ])?)
    }
}

/// Parse ISO8601 string or integer text into epoch milliseconds (i64)
fn parse_time_to_i64(s: &str) -> Option<i64> {
    // First try integer parse
    if let Ok(n) = s.trim().parse::<i64>() { return Some(n); }
    // Try RFC3339/ISO8601 via chrono
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s.trim()) {
        return Some(dt.timestamp_millis());
    }
    None
}

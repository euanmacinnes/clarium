//! exec_vector_runtime
//! --------------------
//! Vector index runtime: BUILD/REINDEX/STATUS and search over flat (exact) engine.
//!
//! v2 `.vdata` format adds stable row ids alongside contiguous f32 payload:
//! magic:u32 | version:u32 | flags:u32 | dim:u32 | rows:u32 | [row_ids: rows*u64 if flags&1] | data: rows*dim*f32
//! v1 compatibility: magic | version(=1) | dim | rows | data

use anyhow::{Result, bail};
use serde_json::json;
use polars::prelude::*;

use crate::server::exec::exec_vector_index::VIndexFile;
use crate::storage::SharedStore;
use crate::error::AppError;
use crate::tprintln;

#[cfg(feature = "ann_hnsw")]
mod hnsw_backend {
    // Lightweight HNSW backend façade.
    // Notes:
    // - We always persist the flat vector payload in .vdata (v2 format with row_ids).
    // - For now, we also write a tiny .hnsw sidecar JSON with build parameters to mark availability.
    // - At query time we provide an ANN-capable search path. If true graph persistence is not available,
    //   we fall back to an in‑memory build for the current query or to exact scoring if needed.
    use super::*;

    fn path_for_hnsw(store: &SharedStore, qualified: &str) -> std::path::PathBuf {
        let mut p = store.0.lock().root_path().clone();
        let local = qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
        p.push(local);
        p.set_extension("hnsw");
        p
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct HnswMeta {
        version: u32,
        metric: String,
        dim: u32,
        rows: u32,
        m: i32,
        ef_build: i32,
    }

    pub fn build_hnsw_index(store: &SharedStore, v: &VIndexFile) -> Result<()> {
        // Ensure vdata exists
        let (dim, rows, _row_ids, _data) = super::load_vdata(store, &v.qualified)?;
        let path = path_for_hnsw(store, &v.qualified);
        if let Some(parent) = path.parent() { let _ = std::fs::create_dir_all(parent); }
        // Record minimal metadata to mark HNSW availability
        let meta = HnswMeta {
            version: 1,
            metric: v.metric.clone().unwrap_or_else(|| "l2".to_string()),
            dim,
            rows,
            m: crate::system::get_vector_hnsw_m(),
            ef_build: crate::system::get_vector_hnsw_ef_build(),
        };
        let bytes = serde_json::to_vec_pretty(&meta)?;
        std::fs::write(&path, bytes)?;
        tprintln!(
            "vector.hnsw.build.ok name={} path={} rows={} dim={} m={} ef_build={}",
            v.qualified, path.display(), meta.rows, meta.dim, meta.m, meta.ef_build
        );
        Ok(())
    }

    pub fn search_hnsw_index(store: &SharedStore, v: &VIndexFile, qvec: &[f32], k: usize) -> Option<Vec<(u32, f32)>> {
        // If the .hnsw marker doesn't exist, treat as unavailable
        let path = path_for_hnsw(store, &v.qualified);
        if !path.exists() {
            tprintln!("vector.hnsw.search.fallback name={} reason=no_hnsw_marker", v.qualified);
            return None;
        }

        // Load vector payload and perform a best‑effort ANN/EXACT search depending on configuration.
        let (dim, rows, _row_ids, data) = match super::load_vdata(store, &v.qualified) {
            Ok(t) => t,
            Err(e) => {
                tprintln!("vector.hnsw.search.fallback name={} reason=load_vdata_error err={}", v.qualified, e);
                return None;
            }
        };
        if qvec.len() as u32 != dim { return None; }

        // For now, use a fast exact top‑k on the flat store under the HNSW façade. This ensures
        // engine hints and ANN path are respected without panicking, and returns consistent results.
        // Ordering direction matches metric semantics.
        let metric = v.metric.as_deref().unwrap_or("l2").to_ascii_lowercase();
        #[inline]
        fn f32_key(v: f32) -> u32 { let b = v.to_bits(); if b & (1u32 << 31) != 0 { !b } else { b | (1u32 << 31) } }
        let mut heap: std::collections::BinaryHeap<(u32, u32)> = std::collections::BinaryHeap::with_capacity(k + 1);
        for r in 0..rows as usize {
            let off = r * dim as usize;
            let slice = &data[off..off + dim as usize];
            let (key, _raw) = match metric.as_str() {
                "ip" | "dot" => { let s = super::dot(slice, qvec); (f32_key(s), s) },
                "cosine" => { let s = super::cosine(slice, qvec); (f32_key(s), s) },
                _ => { let d = super::l2(slice, qvec); let s = -d; (f32_key(s), d) },
            };
            heap.push((key, r as u32));
            if heap.len() > k { heap.pop(); }
        }
        let mut items: Vec<(u32, u32)> = heap.into_iter().collect();
        items.sort_by(|a,b| b.0.cmp(&a.0));
        let mut out: Vec<(u32, f32)> = Vec::with_capacity(items.len());
        for (_k, i) in items.into_iter() {
            let off = i as usize * dim as usize;
            let slice = &data[off..off + dim as usize];
            let s = match metric.as_str() {
                "ip" | "dot" => super::dot(slice, qvec),
                "cosine" => super::cosine(slice, qvec),
                _ => super::l2(slice, qvec),
            };
            out.push((i, s));
        }
        tprintln!("vector.hnsw.search.ok name={} path={} k={} rows={} dim={}", v.qualified, path.display(), k, rows, dim);
        Some(out)
    }
}

fn path_for_index_data(store: &SharedStore, qualified: &str) -> std::path::PathBuf {
    let mut p = store.0.lock().root_path().clone();
    let local = qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
    p.push(local);
    p.set_extension("vdata");
    p
}

pub fn build_vector_index(store: &SharedStore, v: &mut VIndexFile, _options: &Vec<(String,String)>) -> Result<serde_json::Value> {
    // Read source table and build a flat f32 vector store for now.
    let t_start = std::time::Instant::now();
    let data_path = path_for_index_data(store, &v.qualified);
    if let Some(parent) = data_path.parent() { std::fs::create_dir_all(parent).ok(); }
    // Load source table dataframe
    let df = store.0.lock().read_df(&v.table)?;
    let col = match df.get_column_names().iter().find(|c| c.eq_ignore_ascii_case(&v.column)) {
        Some(c) => c.clone(),
        None => bail!("Column not found for vector index: {}", v.column),
    };
    let series = df.column(&col)?;
    // Extract vectors and validate dimensions
    let mut buf: Vec<f32> = Vec::new();
    let mut row_ids: Vec<u64> = Vec::new();
    let mut rows: u32 = 0;
    let mut dim: u32 = 0;
    let mut total_rows: u64 = series.len() as u64;
    let mut parsed_ok: u64 = 0;
    let mut invalid_rows: u64 = 0;
    let mut dim_mismatch: u64 = 0;
    // Policy: enforce declared dim strictly or skip mismatches
    let dim_policy = v
        .params
        .as_ref()
        .and_then(|p| p.get("dim_policy"))
        .and_then(|x| x.as_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_else(|| "skip".to_string());
    // Determine row-id strategy from primary key metadata
    let pk_cols: Option<Vec<String>> = store.0.lock().get_primary_key(&v.table);
    let mut id_flags: u32 = 1; // bit0: has_rowid (we always persist row ids in v2)
    // Pre-fetch PK series if present
    let pk_series: Option<Vec<(String, Column)>> = pk_cols.as_ref().map(|cols| {
        cols.iter()
            .filter_map(|c| {
                let eff = df.get_column_names()
                    .iter()
                    .find(|n| n.as_str() == c.as_str())
                    .cloned()
                    .or_else(|| df.get_column_names().iter().find(|n| n.eq_ignore_ascii_case(c)).cloned());
                eff.and_then(|name| df.column(&name).ok().map(|s| (name.to_string(), s.clone())))
            })
            .collect::<Vec<(String, Column)>>()
    });
    // Helper to compute u64 row_id from primary key values at row i
    #[inline]
    fn hash_pk_parts(parts: &[(String, String)]) -> u64 {
        use xxhash_rust::xxh3::xxh3_64;
        // Build a single string with separators to avoid collisions
        let mut acc = String::with_capacity(parts.len() * 24);
        for (i, (k, v)) in parts.iter().enumerate() {
            if i > 0 { acc.push('|'); }
            acc.push_str(k);
            acc.push('=');
            acc.push_str(v);
        }
        xxh3_64(acc.as_bytes())
    }
    let mut used_pk_numeric = false;
    let mut used_pk_hashed = false;
    for i in 0..series.len() {
        match crate::server::exec::vector_utils::extract_vec_f32_col(series, i) {
            Some(vv) => {
                if vv.is_empty() { invalid_rows += 1; continue; }
                if dim == 0 { dim = vv.len() as u32; }
                // Check against declared dim if present
                if let Some(req_dim) = v.dim {
                    if req_dim as usize != vv.len() {
                        dim_mismatch += 1; continue;
                    }
                }
                // Enforce uniform dimension within column
                if vv.len() as u32 != dim { dim_mismatch += 1; continue; }
                parsed_ok += 1;
                buf.extend_from_slice(&vv);
                // Prefer table primary key when available; else fallback to ordinal
            if let Some(pks) = pk_series.as_ref() {
                if pks.len() == 1 {
                    let (_name, s) = &pks[0];
                    // Try numeric fast-path, else hash string representation
                    let rid_u: Option<u64> = s.get(i)
                        .ok()
                        .and_then(|av| {
                            if let Ok(v) = av.try_extract::<u64>() { return Some(v); }
                            if let Ok(v) = av.try_extract::<i64>() { return Some(v as u64); }
                            if let Ok(v) = av.try_extract::<u32>() { return Some(v as u64); }
                            if let Ok(v) = av.try_extract::<i32>() { return Some(v as u64); }
                            None
                        });
                    if let Some(vu) = rid_u {
                        used_pk_numeric = true;
                        row_ids.push(vu);
                    } else {
                        // String or other types → hash
                        let sval = s.get(i).ok().and_then(|av| av.get_str().map(|x| x.to_string())).unwrap_or_else(|| {
                            s.get(i).ok().map(|av| av.to_string()).unwrap_or_default()
                        });
                        let h = hash_pk_parts(&[(_name.clone(), sval)]);
                        used_pk_hashed = true;
                        row_ids.push(h);
                    }
                } else if !pks.is_empty() {
                    // Composite key: hash normalized tuple of "col=value"
                    let mut parts: Vec<(String, String)> = Vec::with_capacity(pks.len());
                    for (name, s) in pks.iter() {
                        let val = s.get(i).ok().and_then(|av| av.get_str().map(|x| x.to_string())).unwrap_or_else(|| {
                            s.get(i).ok().map(|av| av.to_string()).unwrap_or_default()
                        });
                        parts.push((name.clone(), val));
                    }
                    let h = hash_pk_parts(&parts);
                    used_pk_hashed = true;
                    row_ids.push(h);
                } else {
                    // No resolvable PK columns found in DF → ordinal fallback
                    row_ids.push(rows as u64);
                }
            } else {
                // No PK metadata → ordinal fallback
                row_ids.push(rows as u64);
            }
                rows += 1;
            }
            None => { invalid_rows += 1; }
        }
    }
    // If policy=error and any rows were skipped for dimension reasons, abort gracefully
    if dim_policy == "error" && (dim_mismatch > 0 || invalid_rows > 0) {
        tprintln!(
            "[vector.build] name={} policy=error dim_declared={:?} dim_inferred={} total={} ok={} invalid={} dim_mismatch={} -> abort",
            v.qualified, v.dim, dim, total_rows, parsed_ok, invalid_rows, dim_mismatch
        );
        return Err(AppError::Exec {
            code: "vector_dim_mismatch".into(),
            message: format!(
                "BUILD VECTOR INDEX '{}' aborted by policy=error: invalid_rows={} dim_mismatch={} (declared_dim={:?}, inferred_dim={})",
                v.qualified, invalid_rows, dim_mismatch, v.dim, dim
            ),
        }
        .into());
    }
    // Compose flags for id flavor
    if used_pk_numeric { id_flags |= 0x2; }
    if used_pk_hashed { id_flags |= 0x4; }
    if !used_pk_numeric && !used_pk_hashed { id_flags |= 0x8; }
    crate::tprintln!("[vector.build] {} row-id strategy={} flags=0x{:x} rows={} dim={} pk_cols={}",
        v.qualified,
        if used_pk_numeric { "pk_numeric" } else if used_pk_hashed { "pk_hashed" } else { "ordinal" },
        id_flags,
        rows,
        dim,
        pk_cols.as_ref().map(|v| v.join(",")).unwrap_or_else(|| "<none>".to_string())
    );
    // Persist v2 format with row ids
    let mut out: Vec<u8> = Vec::with_capacity(20 + row_ids.len() * 8 + buf.len() * 4);
    let magic: u32 = 0x56444346; // 'VDCF'
    let version: u32 = 2;
    let flags: u32 = id_flags; // bit0: has_rowid; bit1: pk_numeric; bit2: pk_hashed; bit3: ordinal_fallback
    out.extend_from_slice(&magic.to_le_bytes());
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&dim.to_le_bytes());
    out.extend_from_slice(&rows.to_le_bytes());
    // row ids
    for rid in &row_ids { out.extend_from_slice(&rid.to_le_bytes()); }
    // f32 payload
    let bytes = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, buf.len() * 4) };
    out.extend_from_slice(bytes);
    std::fs::write(&data_path, &out)?;
    let mut status = serde_json::Map::new();
    status.insert("state".into(), json!("built"));
    status.insert("rows_indexed".into(), json!(rows as u64));
    status.insert("bytes".into(), json!(out.len() as u64));
    status.insert("last_built_at".into(), json!(crate::server::exec::exec_vector_index::now_iso()));
    status.insert("dim".into(), json!(dim));
    status.insert("engine".into(), json!("flat"));
    status.insert("rows_total".into(), json!(total_rows));
    status.insert("rows_parsed".into(), json!(parsed_ok));
    status.insert("rows_invalid".into(), json!(invalid_rows));
    status.insert("rows_dim_mismatch".into(), json!(dim_mismatch));
    status.insert("dim_policy".into(), json!(dim_policy));
    status.insert("row_id.flags".into(), json!(flags));
    status.insert("row_id.strategy".into(), json!(if used_pk_numeric { "pk_numeric" } else if used_pk_hashed { "pk_hashed" } else { "ordinal" }));
    status.insert("build_time_ms".into(), json!(t_start.elapsed().as_millis() as u64));
    if let Some(m) = &v.metric { status.insert("metric".into(), json!(m)); }
    // Promote select params to top-level fields and also include under param.* for completeness
    if let Some(p) = &v.params {
        for (k, val) in p.iter() { status.insert(format!("param.{}", k), val.clone()); }
        if let Some(efb) = p.get("ef_build").and_then(|x| x.as_i64()) { status.insert("ef_build".into(), json!(efb)); }
        if let Some(efs) = p.get("ef_search").and_then(|x| x.as_i64()) { status.insert("ef_search".into(), json!(efs)); }
    }
    if let Some(mode) = &v.mode { status.insert("mode".into(), json!(mode)); }
    v.status = Some(status);
    // Optionally build HNSW artifact when feature enabled; ignore errors, keep flat engine as baseline
    #[cfg(feature = "ann_hnsw")]
    {
        let _ = self::hnsw_backend::build_hnsw_index(store, v);
        if let Some(st) = v.status.as_mut() {
            st.insert("engine.hnsw".into(), json!(true));
        }
    }
    tprintln!(
        "[vector.build] name={} status=ok dim={} rows_indexed={} total={} invalid={} dim_mismatch={} policy={}",
        v.qualified, dim, rows, total_rows, invalid_rows, dim_mismatch, dim_policy
    );
    Ok(json!({"status":"ok","rows_indexed":rows,"dim":dim}))
}

pub fn reindex_vector_index(store: &SharedStore, v: &mut VIndexFile) -> Result<serde_json::Value> {
    // For now, reindex just calls build again.
    build_vector_index(store, v, &Vec::new())
}

pub fn show_vector_index_status(store: &SharedStore, name: Option<&str>) -> Result<serde_json::Value> {
    // Build normalized rows per index with agreed fields
    fn normalize_row(name: &str, v: &VIndexFile) -> serde_json::Value {
        let st = v.status.as_ref();
        let get_i64 = |k: &str| st.and_then(|m| m.get(k)).and_then(|x| x.as_i64()).unwrap_or(0);
        let get_u64 = |k: &str| st.and_then(|m| m.get(k)).and_then(|x| x.as_u64()).unwrap_or(0);
        let get_str = |k: &str| st.and_then(|m| m.get(k)).and_then(|x| x.as_str()).unwrap_or("").to_string();
        let state = get_str("state");
        let rows_indexed = if rows_indexed_present(st) { get_u64("rows_indexed") } else { 0 };
        let bytes = if bytes_present(st) { get_u64("bytes") } else { 0 };
        let dim = if let Some(d) = v.dim { d as i64 } else { st.and_then(|m| m.get("dim")).and_then(|x| x.as_i64()).unwrap_or(0) };
        let metric = v.metric.clone().unwrap_or_else(|| get_str("metric"));
        // engine: prefer explicit status.engine else fallback to "flat"
        let mut engine = get_str("engine");
        if engine.is_empty() { engine = "flat".to_string(); }
        let build_time_ms = get_u64("build_time_ms");
        let ef_build = get_i64("ef_build");
        let ef_search = get_i64("ef_search");
        let mode = v.mode.clone().unwrap_or_else(|| get_str("mode"));
        serde_json::json!({
            "name": name,
            "state": state,
            "rows_indexed": rows_indexed,
            "bytes": bytes,
            "dim": dim,
            "metric": if metric.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(metric) },
            "engine": engine,
            "build_time_ms": build_time_ms,
            "ef_build": ef_build,
            "ef_search": ef_search,
            "mode": if mode.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(mode) }
        })
    }
    fn rows_indexed_present(st: Option<&serde_json::Map<String, serde_json::Value>>) -> bool { st.and_then(|m| m.get("rows_indexed")).is_some() }
    fn bytes_present(st: Option<&serde_json::Map<String, serde_json::Value>>) -> bool { st.and_then(|m| m.get("bytes")).is_some() }

    if let Some(n) = name {
        let qualified = crate::ident::qualify_regular_ident(n, &crate::system::current_query_defaults());
        if let Some(vf) = super::exec_vector_index::read_vindex_file(store, &qualified)? {
            return Ok(json!([normalize_row(&vf.name, &vf)]));
        }
        return Ok(json!([]));
    }
    let mut out_rows: Vec<serde_json::Value> = Vec::new();
    let root = store.0.lock().root_path().clone();
    if let Ok(dbs) = std::fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
            if let Ok(sd) = std::fs::read_dir(&db_path) {
                for schema_dir in sd.flatten().filter(|e| e.path().is_dir()) {
                    let sp = schema_dir.path();
                    if let Ok(td) = std::fs::read_dir(&sp) {
                        for tentry in td.flatten() {
                            let tp = tentry.path();
                            if tp.is_file() && tp.extension().and_then(|s| s.to_str()) == Some("vindex") {
                                if let Ok(text) = std::fs::read_to_string(&tp) {
                                    if let Ok(v) = serde_json::from_str::<VIndexFile>(&text) {
                                        out_rows.push(normalize_row(&v.name, &v));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(json!(out_rows))
}

/// Placeholder for incremental DML application according to index mode.
/// For now, all modes except REBUILD_ONLY are not supported and will return a friendly error.
pub fn apply_vector_dml(_store: &SharedStore, v: &VIndexFile, op: &str) -> Result<serde_json::Value> {
    let mode = v.mode.as_deref().unwrap_or("REBUILD_ONLY").to_ascii_uppercase();
    match mode.as_str() {
        "REBUILD_ONLY" => {
            Err(AppError::Exec { code: "vector_dml_rebuild_only".into(), message: format!("Vector index '{}' is REBUILD_ONLY; incremental '{}' not supported. Use BUILD/REINDEX to refresh.", v.qualified, op) }.into())
        }
        "IMMEDIATE" | "BATCHED" | "ASYNC" => {
            Err(AppError::Exec { code: "vector_dml_not_supported".into(), message: format!("Vector index mode '{}' for '{}' does not support incremental '{}' yet.", mode, v.qualified, op) }.into())
        }
        other => {
            Err(AppError::Exec { code: "vector_dml_bad_mode".into(), message: format!("Unrecognized vector index mode '{}' for '{}'.", other, v.qualified) }.into())
        }
    }
}

fn load_vdata(store: &SharedStore, qualified: &str) -> Result<(u32, u32, Option<Vec<u64>>, Vec<f32>)> {
    let p = path_for_index_data(store, qualified);
    let bytes = std::fs::read(&p)?;
    if bytes.len() < 16 { bail!("corrupt vdata: too small"); }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != 0x56444346 { bail!("corrupt vdata: bad magic"); }
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if version == 1 {
        // v1: magic|version|dim|rows|data
        let dim = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let rows = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        let expected = 16usize + (rows as usize * dim as usize * 4);
        if bytes.len() != expected { bail!("corrupt vdata(v1): size mismatch"); }
        let mut data = vec![0f32; (rows * dim) as usize];
        let src = &bytes[16..];
        let ptr = data.as_mut_ptr() as *mut u8;
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), ptr, src.len()); }
        Ok((dim, rows, None, data))
    } else {
        // v2+: magic|version|flags|dim|rows|[row_ids]|data
        if bytes.len() < 20 { bail!("corrupt vdata(v2): too small"); }
        let flags = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let dim = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        let rows = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
        let has_rowid = (flags & 1) != 0;
        let mut offset = 20usize;
        let row_ids = if has_rowid {
            let mut v = Vec::with_capacity(rows as usize);
            let need = rows as usize * 8;
            if bytes.len() < offset + need { bail!("corrupt vdata: row_ids truncated"); }
            for i in 0..rows as usize {
                let start = offset + i*8;
                v.push(u64::from_le_bytes(bytes[start..start+8].try_into().unwrap()));
            }
            offset += need;
            Some(v)
        } else { None };
        let need_data = rows as usize * dim as usize * 4;
        if bytes.len() < offset + need_data { bail!("corrupt vdata: data truncated"); }
        let mut data = vec![0f32; (rows * dim) as usize];
        let src = &bytes[offset..offset+need_data];
        let ptr = data.as_mut_ptr() as *mut u8;
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), ptr, src.len()); }
        Ok((dim, rows, row_ids, data))
    }
}

fn l2(a: &[f32], b: &[f32]) -> f32 {
    let mut s = 0f32;
    for i in 0..a.len() { let d = a[i] - b[i]; s += d*d; }
    s.sqrt()
}
fn dot(a: &[f32], b: &[f32]) -> f32 { a.iter().zip(b.iter()).map(|(x,y)| x*y).sum() }
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let na = a.iter().map(|x| x*x).sum::<f32>().sqrt();
    let nb = b.iter().map(|x| x*x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 { return f32::NAN; }
    dot(a,b) / (na*nb)
}

pub fn search_vector_index(store: &SharedStore, v: &VIndexFile, qvec: &[f32], k: usize) -> Result<Vec<(u64, f32)>> {
    // Try ANN engine if present; map back to stored row_ids
    let (dim, rows, row_ids, data) = load_vdata(store, &v.qualified)?;
    #[cfg(feature = "ann_hnsw")]
    if let Some(res) = self::hnsw_backend::search_hnsw_index(store, v, qvec, k) {
        let mut out: Vec<(u64, f32)> = Vec::with_capacity(res.len());
        for (pos, score) in res.into_iter() {
            let id = row_ids.as_ref().and_then(|v| v.get(pos as usize)).cloned().unwrap_or(pos as u64);
            out.push((id, score));
        }
        return Ok(out);
    }
    if qvec.len() as u32 != dim { bail!("query dim {} mismatch index dim {}", qvec.len(), dim); }
    let metric = v.metric.as_deref().unwrap_or("l2").to_ascii_lowercase();
    // Use ordered key to satisfy Ord: map f32 score to u32 key preserving order
    #[inline]
    fn f32_key(v: f32) -> u32 { let b = v.to_bits(); if b & (1u32 << 31) != 0 { !b } else { b | (1u32 << 31) } }
    // Maintain appropriate heap depending on metric order; for L2 store negative distance so larger is better
    let mut heap: std::collections::BinaryHeap<(u32, u32)> = std::collections::BinaryHeap::with_capacity(k + 1);
    for r in 0..rows as usize {
        let off = r * dim as usize;
        let slice = &data[off..off + dim as usize];
        let (key, _raw_score) = match metric.as_str() {
            "ip" | "dot" => { let s = dot(slice, qvec); (f32_key(s), s) },
            "cosine" => { let s = cosine(slice, qvec); (f32_key(s), s) },
            _ => { let d = l2(slice, qvec); let s = -d; (f32_key(s), d) }, // key on negative distance; raw score is positive distance
        };
        heap.push((key, r as u32));
        if heap.len() > k { heap.pop(); }
    }
    let mut items: Vec<(u32, u32)> = heap.into_iter().collect();
    // Sort descending by score key
    items.sort_by(|a,b| b.0.cmp(&a.0));
    // Recompute true score for outputs (k is small) to avoid carrying raw in heap across metrics
    let mut out: Vec<(u64, f32)> = Vec::with_capacity(items.len());
    for (_k, i) in items.into_iter() {
        let off = i as usize * dim as usize;
        let slice = &data[off..off + dim as usize];
        let s = match metric.as_str() {
            "ip" | "dot" => dot(slice, qvec),
            "cosine" => cosine(slice, qvec),
            _ => l2(slice, qvec),
        };
        // Use row_ids if present; else positional index
        let id = row_ids.as_ref().and_then(|v| v.get(i as usize)).cloned().unwrap_or(i as u64);
        out.push((id, s));
    }
    Ok(out)
}

/// Optional knobs that can influence vector search behavior.
#[derive(Debug, Clone, Default)]
pub struct SearchOptions {
    /// Override metric used by the index: "l2" | "ip" | "cosine" (case-insensitive)
    pub metric_override: Option<String>,
    /// ef_search hint for ANN engines (ignored by flat);
    /// engine backends should best-effort apply it.
    pub ef_search: Option<usize>,
    /// Engine hint: "hnsw" | "flat" (case-insensitive); best-effort.
    pub engine_hint: Option<String>,
}

/// Like `search_vector_index` but accepts optional search options.
/// The default behavior is preserved when all options are `None`.
pub fn search_vector_index_with_opts(
    store: &SharedStore,
    v: &VIndexFile,
    qvec: &[f32],
    k: usize,
    opts: &SearchOptions,
) -> Result<Vec<(u32, f32)>> {
    // Try ANN backend first when available, unless the user forces flat
    let force_flat = opts
        .engine_hint
        .as_ref()
        .map(|s| s.eq_ignore_ascii_case("flat"))
        .unwrap_or(false);

    #[cfg(feature = "ann_hnsw")]
    if !force_flat {
        // Currently only HNSW is supported.
        // If ef_search is provided and the backend supports it, apply it best-effort.
        if let Some(mut res) = self::hnsw_backend::search_hnsw_index(store, v, qvec, k) {
            // Metric override on ANN path: for now HNSW distance is L2; if override is set to ip/cosine,
            // we can only re-rank within the top-k using the requested metric for score reporting.
            if let Some(mo) = opts.metric_override.as_ref() {
                let mo_low = mo.to_ascii_lowercase();
                // Load data to recompute scores according to requested metric for the already selected top-k
                let (dim, _rows, row_ids, data) = load_vdata(store, &v.qualified)?;
                for tup in res.iter_mut() {
                    let i = tup.0 as usize; // note: tup.0 currently stores id (which may be row_id already). For ANN v2 we persist row ids; however HNSW stores internal index r.
                    // We can't map back to internal row ordinal when row_ids are persisted; so keep scores as-is for now when override mismatches.
                    // To keep behavior consistent, only adjust if metric is l2 request; otherwise leave ANN scores.
                    match mo_low.as_str() {
                        "l2" => { /* already L2 */ }
                        _ => { let _ = (dim, row_ids.as_ref(), &data); }
                    }
                }
            }
            return Ok(res);
        }
    }

    // Fallback to flat exact path (or forced by hint)
    let (dim, rows, row_ids, data) = load_vdata(store, &v.qualified)?;
    if qvec.len() as u32 != dim {
        tprintln!(
            "[vector.search] name={} warn=query_dim_mismatch qdim={} idx_dim={} action=fallback_or_error",
            v.qualified, qvec.len(), dim
        );
        return Err(AppError::Exec { code: "vector_query_dim_mismatch".into(), message: format!("query dim {} mismatch index dim {}", qvec.len(), dim) }.into());
    }
    let metric = opts
        .metric_override
        .as_ref()
        .map(|s| s.to_ascii_lowercase())
        .or_else(|| v.metric.as_ref().map(|s| s.to_ascii_lowercase()))
        .unwrap_or_else(|| "l2".to_string());
    #[inline]
    fn f32_key(v: f32) -> u32 { let b = v.to_bits(); if b & (1u32 << 31) != 0 { !b } else { b | (1u32 << 31) } }
    let mut heap: std::collections::BinaryHeap<(u32, u32)> = std::collections::BinaryHeap::with_capacity(k + 1);
    for r in 0..rows as usize {
        let off = r * dim as usize;
        let slice = &data[off..off + dim as usize];
        let (key, _raw_score) = match metric.as_str() {
            "ip" | "dot" => { let s = dot(slice, qvec); (f32_key(s), s) },
            "cosine" => { let s = cosine(slice, qvec); (f32_key(s), s) },
            _ => { let d = l2(slice, qvec); let s = -d; (f32_key(s), d) },
        };
        heap.push((key, r as u32));
        if heap.len() > k { heap.pop(); }
    }
    let mut items: Vec<(u32, u32)> = heap.into_iter().collect();
    items.sort_by(|a,b| b.0.cmp(&a.0));
    let mut out: Vec<(u32, f32)> = Vec::with_capacity(items.len());
    for (_k, i) in items.into_iter() {
        let off = i as usize * dim as usize;
        let slice = &data[off..off + dim as usize];
        let s = match metric.as_str() {
            "ip" | "dot" => dot(slice, qvec),
            "cosine" => cosine(slice, qvec),
            _ => l2(slice, qvec),
        };
        let id = row_ids.as_ref().and_then(|v| v.get(i as usize)).cloned().unwrap_or(i as u64) as u32;
        out.push((id, s));
    }
    Ok(out)
}

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

#[cfg(feature = "ann_hnsw")]
mod hnsw_backend {
    use super::*;
    use hnsw_rs::prelude::*;
    use std::fs;

    fn path_for_hnsw(store: &SharedStore, qualified: &str) -> std::path::PathBuf {
        let mut p = store.0.lock().root_path().clone();
        let local = qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
        p.push(local);
        p.set_extension("hnsw");
        p
    }

    pub fn build_hnsw_index(store: &SharedStore, v: &VIndexFile) -> Result<()> {
        // Load vdata (ensures it exists); then build HNSW in-memory and persist via bincode
        let (dim, rows, _row_ids, data) = super::load_vdata(store, &v.qualified)?;
        let m = v.params.as_ref().and_then(|p| p.get("M")).and_then(|x| x.as_i64()).unwrap_or(32) as usize;
        let ef_construction = v.params.as_ref().and_then(|p| p.get("ef_build")).and_then(|x| x.as_i64()).unwrap_or(200) as usize;
        let metric = v.metric.as_deref().unwrap_or("l2").to_ascii_lowercase();
        let mut hnsw: Hnsw<f32, DistL2> = HnswBuilder::default().m(m).ef_construct(ef_construction).num_elements(rows as usize).build();
        // Note: we currently support L2 distance; IP/cosine would require different distance types or normalization
        for r in 0..rows as usize {
            let off = r * dim as usize;
            let vec = data[off..off + dim as usize].to_vec();
            hnsw.insert((&vec, r)).map_err(|e| anyhow::anyhow!(format!("hnsw insert error: {:?}", e)))?;
        }
        hnsw.build();
        let path = path_for_hnsw(store, &v.qualified);
        if let Some(parent) = path.parent() { fs::create_dir_all(parent).ok(); }
        // Serialize
        let bytes = bincode::serialize(&hnsw).map_err(|e| anyhow::anyhow!(format!("hnsw serialize: {:?}", e)))?;
        fs::write(&path, bytes)?;
        Ok(())
    }

    pub fn search_hnsw_index(store: &SharedStore, v: &VIndexFile, qvec: &[f32], k: usize) -> Option<Vec<(u32, f32)>> {
        // Try to load .hnsw and run search; fall back to None on any error
        let path = path_for_hnsw(store, &v.qualified);
        if !path.exists() { return None; }
        let bytes = std::fs::read(&path).ok()?;
        let mut hnsw: Hnsw<f32, DistL2> = bincode::deserialize(&bytes).ok()?;
        let res = hnsw.search(qvec, k);
        // Convert to (idx, distance) with L2 distance assumption
        let mut out: Vec<(u32, f32)> = res.into_iter().map(|ne| (ne.d_id as u32, ne.distance)).collect();
        // Already in ascending distance order
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
    for i in 0..series.len() {
        if let Some(vv) = crate::server::exec::vector_utils::extract_vec_f32_col(series, i) {
            if vv.is_empty() { continue; }
            if dim == 0 { dim = vv.len() as u32; }
            if let Some(req_dim) = v.dim { if req_dim as usize != vv.len() { continue; } }
            if vv.len() as u32 != dim { continue; }
            buf.extend_from_slice(&vv);
            // For now use row ordinal as stable row id; future: prefer table primary key.
            row_ids.push(rows as u64);
            rows += 1;
        }
    }
    // Persist v2 format with row ids
    let mut out: Vec<u8> = Vec::with_capacity(20 + row_ids.len() * 8 + buf.len() * 4);
    let magic: u32 = 0x56444346; // 'VDCF'
    let version: u32 = 2;
    let flags: u32 = 1; // bit0: has_rowid
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
    if let Some(m) = &v.metric { status.insert("metric".into(), json!(m)); }
    if let Some(p) = &v.params { for (k, val) in p.iter() { status.insert(format!("param.{}", k), val.clone()); } }
    v.status = Some(status);
    // Optionally build HNSW artifact when feature enabled; ignore errors, keep flat engine as baseline
    #[cfg(feature = "ann_hnsw")]
    {
        let _ = self::hnsw_backend::build_hnsw_index(store, v);
        if let Some(st) = v.status.as_mut() {
            st.insert("engine.hnsw".into(), json!(true));
        }
    }
    Ok(json!({"status":"ok","rows_indexed":rows,"dim":dim}))
}

pub fn reindex_vector_index(store: &SharedStore, v: &mut VIndexFile) -> Result<serde_json::Value> {
    // For now, reindex just calls build again.
    build_vector_index(store, v, &Vec::new())
}

pub fn show_vector_index_status(store: &SharedStore, name: Option<&str>) -> Result<serde_json::Value> {
    // Delegate to exec_vector_index list/read utilities by reading .vindex files
    if let Some(n) = name {
        let qualified = crate::ident::qualify_regular_ident(n, &crate::system::current_query_defaults());
        if let Some(vf) = super::exec_vector_index::read_vindex_file(store, &qualified)? {
            return Ok(json!([vf.status]));
        }
        return Ok(json!([]));
    }
    // All statuses
    let mut out = Vec::new();
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
                                        out.push(v.status);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(json!(out))
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

pub fn search_vector_index(store: &SharedStore, v: &VIndexFile, qvec: &[f32], k: usize) -> Result<Vec<(u32, f32)>> {
    // Prefer HNSW when available and feature enabled
    #[cfg(feature = "ann_hnsw")]
    if let Some(res) = self::hnsw_backend::search_hnsw_index(store, v, qvec, k) {
        return Ok(res);
    }
    let (dim, rows, row_ids, data) = load_vdata(store, &v.qualified)?;
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
    let mut out: Vec<(u32, f32)> = Vec::with_capacity(items.len());
    for (_k, i) in items.into_iter() {
        let off = i as usize * dim as usize;
        let slice = &data[off..off + dim as usize];
        let s = match metric.as_str() {
            "ip" | "dot" => dot(slice, qvec),
            "cosine" => cosine(slice, qvec),
            _ => l2(slice, qvec),
        };
        // Use row_ids if present; else positional index
        let id = row_ids.as_ref().and_then(|v| v.get(i as usize)).cloned().unwrap_or(i as u64) as u32;
        out.push((id, s));
    }
    Ok(out)
}

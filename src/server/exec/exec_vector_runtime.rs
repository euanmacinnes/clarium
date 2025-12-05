//! exec_vector_runtime
//! --------------------
//! Vector index runtime lifecycle scaffolding: BUILD/REINDEX/STATUS and placeholders
//! for ANN search. This initial implementation persists status metadata only and
//! prepares file paths for future ANN engines (e.g., HNSW). It is intentionally
//! conservative to ensure compilation and incremental rollout.

use anyhow::{Result, bail};
use serde_json::json;
use polars::prelude::*;

use crate::server::exec::exec_vector_index::VIndexFile;
use crate::storage::SharedStore;

fn path_for_index_data(store: &SharedStore, qualified: &str) -> std::path::PathBuf {
    let mut p = store.0.lock().root_path().clone();
    let local = qualified.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
    p.push(local);
    p.set_extension("vdata");
    p
}

pub fn build_vector_index(store: &SharedStore, v: &mut VIndexFile, options: &Vec<(String,String)>) -> Result<serde_json::Value> {
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
    let mut rows: u32 = 0;
    let mut dim: u32 = 0;
    for i in 0..series.len() {
        if let Some(vv) = crate::server::exec::vector_utils::extract_vec_f32_col(series, i) {
            if vv.is_empty() { continue; }
            if dim == 0 { dim = vv.len() as u32; }
            if let Some(req_dim) = v.dim { if req_dim as usize != vv.len() { continue; } }
            if vv.len() as u32 != dim { continue; }
            buf.extend_from_slice(&vv);
            rows += 1;
        }
    }
    // Persist as: magic:u32, version:u32, dim:u32, rows:u32, data: rows*dim f32
    let mut out: Vec<u8> = Vec::with_capacity(16 + buf.len() * 4);
    let magic: u32 = 0x56444346; // 'VDCF'
    let version: u32 = 1;
    out.extend_from_slice(&magic.to_le_bytes());
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(&dim.to_le_bytes());
    out.extend_from_slice(&rows.to_le_bytes());
    let bytes = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, buf.len() * 4) };
    out.extend_from_slice(bytes);
    std::fs::write(&data_path, &out)?;
    let mut status = serde_json::Map::new();
    status.insert("state".into(), json!("built"));
    status.insert("rows_indexed".into(), json!(rows as u64));
    status.insert("bytes".into(), json!(out.len() as u64));
    status.insert("last_built_at".into(), json!(crate::server::exec::exec_vector_index::now_iso()));
    status.insert("dim".into(), json!(dim));
    if let Some(m) = &v.metric { status.insert("metric".into(), json!(m)); }
    if let Some(p) = &v.params { for (k, val) in p.iter() { status.insert(format!("param.{}", k), val.clone()); } }
    v.status = Some(status);
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

fn load_vdata(store: &SharedStore, qualified: &str) -> Result<(u32, u32, Vec<f32>)> {
    let p = path_for_index_data(store, qualified);
    let bytes = std::fs::read(&p)?;
    if bytes.len() < 16 { bail!("corrupt vdata: too small"); }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let _version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let dim = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let rows = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    if magic != 0x56444346 { bail!("corrupt vdata: bad magic"); }
    let expected = 16usize + (rows as usize * dim as usize * 4);
    if bytes.len() != expected { bail!("corrupt vdata: size mismatch"); }
    let mut data = vec![0f32; (rows * dim) as usize];
    let src = &bytes[16..];
    // Safe transmute copy
    let ptr = data.as_mut_ptr() as *mut u8;
    unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), ptr, src.len()); }
    Ok((dim, rows, data))
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
    let (dim, rows, data) = load_vdata(store, &v.qualified)?;
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
        let score = match metric.as_str() {
            "ip" | "dot" => dot(slice, qvec),
            "cosine" => cosine(slice, qvec),
            _ => -l2(slice, qvec), // store negative distance, so larger is better in max-heap
        };
        heap.push((f32_key(score), r as u32));
        if heap.len() > k { heap.pop(); }
    }
    let mut items: Vec<(u32, u32)> = heap.into_iter().collect();
    // Sort descending by score key
    items.sort_by(|a,b| b.0.cmp(&a.0));
    // We no longer return the raw score; return key-less (id, 0.0) placeholder score for now
    Ok(items.into_iter().map(|(_k,i)| (i, 0.0f32)).collect())
}

//! GraphStore compaction and manifest rotation (single-partition helper)
//! --------------------------------------------------------------------
//! Builds a new immutable CSR adjacency segment by merging an existing
//! immutable segment with in-memory delta indexes (adds/tombstones). Writes
//! the new segment under `edges/adj.PXXX.seg.N+1` and returns its relative
//! path. Manifest rotation (atomic swap) can then reference the new segment.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use super::segments::AdjSegment;
use super::delta::PartitionDeltaIndex;

#[inline]
fn le_bytes_u16(v: u16) -> [u8;2] { v.to_le_bytes() }
#[inline]
fn le_bytes_u32(v: u32) -> [u8;4] { v.to_le_bytes() }
#[inline]
fn le_bytes_u64(v: u64) -> [u8;8] { v.to_le_bytes() }

/// Write a new adjacency segment for `part_id` by merging `base` with `delta`.
/// Returns the relative path (under the graph root) of the new segment file.
pub fn compact_partition(
    graph_root: &Path,
    part_id: u32,
    base: &AdjSegment,
    delta: Option<&PartitionDeltaIndex>,
    next_seqno: u64,
) -> Result<String> {
    // Gather merged adjacency lists for all nodes in base.
    let nodes = base.nodes as usize;
    let mut row_ptr: Vec<u64> = Vec::with_capacity(nodes + 1);
    let mut cols: Vec<u64> = Vec::new();
    row_ptr.push(0);
    let empty_delta = PartitionDeltaIndex::default();
    let dref = delta.unwrap_or(&empty_delta);

    for u in 0..nodes as u64 {
        // Read base neighbors
        let (s, e) = base.neighbors_range(u)?;
        // Merge into a temp vec, filter tombstones
        let mut merged: Vec<u64> = Vec::with_capacity((e - s) + dref.adds.get(&u).map(|v| v.len()).unwrap_or(0));
        for idx in s..e {
            let v = base.neighbor_at(idx);
            if !dref.tombstones.contains(&(u, v)) { merged.push(v); }
        }
        if let Some(adds) = dref.adds.get(&u) {
            for &v in adds {
                if !dref.tombstones.contains(&(u, v)) { merged.push(v); }
            }
        }
        // Stable order: keep base order; delta appends after base for determinism.
        cols.extend_from_slice(&merged);
        row_ptr.push(cols.len() as u64);
    }

    // Serialize into our simple header format used by reader: 40-byte header + arrays
    let mut buf: Vec<u8> = Vec::with_capacity(40 + row_ptr.len()*8 + cols.len()*8);
    const ADJ_MAGIC: u32 = 0x4144474A; // must match reader
    buf.extend_from_slice(&le_bytes_u32(ADJ_MAGIC));
    buf.extend_from_slice(&le_bytes_u16(1)); // version
    buf.extend_from_slice(&le_bytes_u16(0)); // flags
    buf.extend_from_slice(&le_bytes_u64(base.nodes));
    buf.extend_from_slice(&le_bytes_u64(cols.len() as u64));
    let row_ptr_off = 40u64;
    let cols_off = row_ptr_off + ((base.nodes + 1) * 8);
    buf.extend_from_slice(&le_bytes_u64(row_ptr_off));
    buf.extend_from_slice(&le_bytes_u64(cols_off));
    // Append arrays
    for v in &row_ptr { buf.extend_from_slice(&le_bytes_u64(*v)); }
    for v in &cols { buf.extend_from_slice(&le_bytes_u64(*v)); }

    // Write to edges/adj.PXXX.seg.next_seqno
    let fname = format!("adj.P{:0>3}.seg.{}", part_id, next_seqno);
    let rel_path = PathBuf::from("edges").join(&fname);
    let full_path = graph_root.join(&rel_path);
    if let Some(parent) = full_path.parent() { std::fs::create_dir_all(parent).ok(); }
    let mut f = File::create(&full_path).with_context(|| format!("create {}", full_path.display()))?;
    f.write_all(&buf)?;
    f.flush()?;
    // Rely on external caller to fsync/atomic manifest rotation; std::fs::File::sync_all here for durability
    f.sync_all().ok();
    Ok(rel_path.to_string_lossy().to_string())
}

#[cfg(test)]
#[path = "compaction_tests.rs"]
mod compaction_tests;

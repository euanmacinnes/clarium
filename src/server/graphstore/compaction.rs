//! GraphStore compaction and manifest rotation (single-partition helper)
//! --------------------------------------------------------------------
//! Builds a new immutable CSR adjacency segment by merging an existing
//! immutable segment with in-memory delta indexes (adds/tombstones). Writes
//! the new segment under `edges/adj.PXXX.seg.N+1` and returns its relative
//! path. Manifest rotation (atomic swap) can then reference the new segment.

use anyhow::{anyhow, Context, Result};
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
mod tests {
    use super::*;
    use crate::server::graphstore::segments::AdjSegment;

    #[test]
    fn compact_merge_adds_and_tombstones() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // Build a tiny base segment with 3 nodes: 0->1, 1->2
        let base_path = root.join("edges"); std::fs::create_dir_all(&base_path).unwrap();
        let base_file = base_path.join("adj.P000.seg.1");

        // Manually write base
        let nodes = 3u64; let cols_base = vec![1u64, 2u64]; let row_ptr = vec![0u64,1u64,2u64,2u64];
        let mut buf: Vec<u8> = Vec::new();
        const ADJ_MAGIC: u32 = 0x4144474A;
        buf.extend_from_slice(&ADJ_MAGIC.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&nodes.to_le_bytes());
        buf.extend_from_slice(&(cols_base.len() as u64).to_le_bytes());
        let row_ptr_off = 40u64;
        let cols_off = row_ptr_off + ((nodes + 1) * 8);
        buf.extend_from_slice(&row_ptr_off.to_le_bytes());
        buf.extend_from_slice(&cols_off.to_le_bytes());
        for v in &row_ptr { buf.extend_from_slice(&v.to_le_bytes()); }
        for v in &cols_base { buf.extend_from_slice(&v.to_le_bytes()); }
        std::fs::create_dir_all(base_file.parent().unwrap()).unwrap();
        std::fs::write(&base_file, &buf).unwrap();

        let base_seg = AdjSegment::open(&base_file).unwrap();
        // Delta: add 0->2 and tombstone 1->2
        let mut d = PartitionDeltaIndex::default();
        d.add_edge(0, 2);
        d.del_edge(1, 2);
        let rel = compact_partition(root, 0, &base_seg, Some(&d), 2).unwrap();
        let new_seg = AdjSegment::open(&root.join(&rel)).unwrap();
        // Verify ranges: 0 has neighbors [1,2]; 1 has []; 2 has []
        let (s0,e0) = new_seg.neighbors_range(0).unwrap();
        assert_eq!(e0 - s0, 2);
        let v0 = new_seg.neighbor_at(s0); let v1 = new_seg.neighbor_at(s0+1);
        assert_eq!((v0,v1), (1,2));
        let (s1,e1) = new_seg.neighbors_range(1).unwrap();
        assert_eq!(e1 - s1, 0);
    }
}

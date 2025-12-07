//! GraphStore segments (read-only, ACID-ready formats)
//! --------------------------------------------------
//! Immutable adjacency segments use a CSR layout for high-performance reads.
//! Node dictionary segments map `(label,key)` pairs to dense `u64` node IDs.
//! This module provides read-only loaders with basic integrity checks. No
//! compression is used at this stage to minimize CPU overhead.

use anyhow::{anyhow, Context, Result};
use memmap2::Mmap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

// ---------------- Adjacency (CSR) segment ----------------

#[derive(Debug)]
pub struct AdjSegment {
    _file: File,
    mmap: Mmap,
    pub nodes: u64,
    pub edges: u64,
    row_ptr_off: usize,
    cols_off: usize,
}

impl AdjSegment {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open adj segment {}", path.display()))?;
        // Safety: file is not modified while mapped (immutable segment contract)
        let mmap = unsafe { Mmap::map(&file) }.with_context(|| format!("mmap {}", path.display()))?;
        // Minimal header: [magic:u32, version:u16, _flags:u16, nodes:u64, edges:u64,
        // row_ptr_off:u64, cols_off:u64]
        if mmap.len() < 4 + 2 + 2 + 8 * 4 {
            return Err(anyhow!("adj segment too small: {}", path.display()));
        }
        let magic = le_u32(&mmap[0..4]);
        const ADJ_MAGIC: u32 = 0x4144474A; // 'J','G','D','A' arbitrary stable tag
        if magic != ADJ_MAGIC {
            return Err(anyhow!("invalid adj magic in {}", path.display()));
        }
        let version = le_u16(&mmap[4..6]);
        if version != 1 {
            return Err(anyhow!("unsupported adj version {} in {}", version, path.display()));
        }
        let nodes = le_u64(&mmap[8..16]);
        let edges = le_u64(&mmap[16..24]);
        let row_ptr_off = le_u64(&mmap[24..32]) as usize;
        let cols_off = le_u64(&mmap[32..40]) as usize;
        // Basic bounds checks
        let row_ptr_len = (nodes as usize + 1) * 8;
        let cols_len = (edges as usize) * 8;
        if row_ptr_off + row_ptr_len > mmap.len() || cols_off + cols_len > mmap.len() {
            return Err(anyhow!("adj segment sections out of bounds: {}", path.display()));
        }
        // Monotonicity of row_ptr and terminal equals edges
        let rp = &mmap[row_ptr_off..row_ptr_off + row_ptr_len];
        let mut prev = 0u64;
        for i in 0..=nodes as usize {
            let v = le_u64(&rp[i * 8..i * 8 + 8]);
            if v < prev {
                return Err(anyhow!("row_ptr not monotonic at idx {} in {}", i, path.display()));
            }
            prev = v;
        }
        if prev != edges {
            return Err(anyhow!("row_ptr[last] != edges in {}", path.display()));
        }
        Ok(Self { _file: file, mmap, nodes, edges, row_ptr_off, cols_off })
    }

    #[inline]
    pub fn neighbors_range(&self, node_id: u64) -> Result<(usize, usize)> {
        if node_id >= self.nodes { return Err(anyhow!("node_id out of range")); }
        let base = self.row_ptr_off;
        let a = le_u64(&self.mmap[base + (node_id as usize) * 8 .. base + (node_id as usize + 1) * 8]) as usize;
        let b = le_u64(&self.mmap[base + (node_id as usize + 1) * 8 .. base + (node_id as usize + 2) * 8]) as usize;
        Ok((a, b))
    }

    #[inline]
    pub fn cols_slice(&self, start: usize, end: usize) -> &[u8] {
        &self.mmap[self.cols_off + start * 8 .. self.cols_off + end * 8]
    }

    #[inline]
    pub fn neighbor_at(&self, edge_idx: usize) -> u64 {
        le_u64(&self.mmap[self.cols_off + edge_idx * 8 .. self.cols_off + (edge_idx + 1) * 8])
    }
}

// ---------------- Node dictionary segment ----------------

#[derive(Debug, Default)]
pub struct NodeDict {
    // Simple in-memory map for now; segment layout to be produced by builder/compaction phase.
    // Loader supports two formats:
    // 1) Our future binary header; 2) A JSON fallback for tests.
    map: std::collections::HashMap<(String, String), u64>,
    rev: Vec<(String, String)>,
}

impl NodeDict {
    pub fn open(path: &Path) -> Result<Self> {
        // If file is empty or missing, return empty dict (graceful for tests while segments are not built).
        if !path.exists() {
            return Ok(Self::default());
        }
        let mut f = File::open(path).with_context(|| format!("open dict segment {}", path.display()))?;
        let mut head = [0u8; 16];
        let n = f.read(&mut head).unwrap_or(0);
        if n < 8 {
            // Try JSON fallback
            let mut s = String::new();
            f.seek(SeekFrom::Start(0))?;
            f.read_to_string(&mut s)?;
            return Self::from_json(&s);
        }
        let magic = le_u32(&head[0..4]);
        const DICT_MAGIC: u32 = 0x44474E44; // 'D','N','G','D' arbitrary stable tag
        if magic != DICT_MAGIC {
            // JSON fallback
            let mut s = String::new();
            f.seek(SeekFrom::Start(0))?;
            f.read_to_string(&mut s)?;
            return Self::from_json(&s);
        }
        let _version = le_u16(&head[4..6]);
        // For now, binary dict builder is not implemented; return error to avoid silent misuse.
        Err(anyhow!("binary dict.seg not yet supported by reader; use JSON fallback for tests"))
    }

    fn from_json(text: &str) -> Result<Self> {
        // Very small helper for tests: { "entries": [ {"label":"Tool","key":"planner","id":1} ] }
        #[derive(serde::Deserialize)]
        struct JEntry { label: String, key: String, id: u64 }
        #[derive(serde::Deserialize)]
        struct JRoot { entries: Vec<JEntry> }
        let jr: JRoot = serde_json::from_str(text).context("parsing dict JSON fallback")?;
        let mut map = std::collections::HashMap::with_capacity(jr.entries.len());
        let mut rev = Vec::with_capacity(jr.entries.len());
        for e in jr.entries {
            map.insert((e.label.clone(), e.key.clone()), e.id);
            let idx = e.id as usize;
            if rev.len() <= idx { rev.resize(idx + 1, (String::new(), String::new())); }
            rev[idx] = (e.label, e.key);
        }
        Ok(Self { map, rev })
    }

    #[inline]
    pub fn lookup(&self, label: &str, key: &str) -> Option<u64> {
        self.map.get(&(label.to_string(), key.to_string())).copied()
    }

    #[inline]
    pub fn lookup_any_label(&self, key: &str) -> Option<u64> {
        // Linear scan over reverse map to find first matching key; suitable for tests only.
        for (id, (_l, k)) in self.rev.iter().enumerate() {
            if !k.is_empty() && k == key { return Some(id as u64); }
        }
        None
    }

    #[inline]
    pub fn reverse_key(&self, node_id: u64) -> Option<(&str, &str)> {
        let idx = node_id as usize;
        if idx < self.rev.len() {
            let (ref l, ref k) = self.rev[idx];
            if !l.is_empty() || !k.is_empty() { return Some((l.as_str(), k.as_str())); }
        }
        None
    }

    /// Apply an upsert of a node mapping. If `node_id` is None, this call is a no-op.
    pub fn upsert(&mut self, label: &str, key: &str, node_id: Option<u64>) {
        if let Some(id) = node_id {
            let k = (label.to_string(), key.to_string());
            self.map.insert(k.clone(), id);
            let idx = id as usize;
            if self.rev.len() <= idx { self.rev.resize(idx + 1, (String::new(), String::new())); }
            self.rev[idx] = k;
        }
    }

    /// Apply a delete of a node mapping.
    pub fn delete(&mut self, label: &str, key: &str) {
        if let Some(id) = self.map.remove(&(label.to_string(), key.to_string())) {
            let idx = id as usize;
            if idx < self.rev.len() {
                self.rev[idx] = (String::new(), String::new());
            }
        }
    }
}

// ---------------- Utilities ----------------

#[inline]
fn le_u16(b: &[u8]) -> u16 { u16::from_le_bytes([b[0], b[1]]) }
#[inline]
fn le_u32(b: &[u8]) -> u32 { u32::from_le_bytes([b[0], b[1], b[2], b[3]]) }
#[inline]
fn le_u64(b: &[u8]) -> u64 { u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]) }

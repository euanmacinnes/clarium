//! GraphStore engine scaffolding
//! -----------------------------
//! This module provides a minimal runtime API surface and early read-only
//! structures for a future direct graph storage engine with ACID writes.
//! For now it exposes a `runtime_api()` that returns `None`, so existing
//! table-backed execution remains the default. We also define a manifest
//! format and loader that discovers a graph's on-disk layout under
//! `<db>/<schema>/<graph>.gstore/`.

use anyhow::{anyhow, Context, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::storage::SharedStore;
use crate::server::graphstore::segments::{AdjSegment, NodeDict};
use crate::server::graphstore::delta::{PartitionDeltaIndex, build_indexes_from_records};
use crate::server::graphstore::wal::{WalReader, WalRecord};
use crate::server::graphstore::manifest as mfutil;
use crate::server::graphstore::delta_log::{DeltaLogReader, apply_edge_deltas, NodeDeltaLogReader};
use crate::server::graphstore::metrics as gsm;

/// Runtime operations the GraphStore must support for TVFs.
pub trait GraphRuntime: Send + Sync {
    fn neighbors_bfs(
        &self,
        store: &SharedStore,
        graph_qualified: &str,
        start: &str,
        etype: Option<&str>,
        max_hops: i64,
        time_start: Option<&str>,
        time_end: Option<&str>,
    ) -> Result<DataFrame>;
}

/// Returns a runtime if the GraphStore engine is available.
///
/// For now this returns `None` so the system continues to use the
/// table-backed implementation. Future iterations will return a static
/// implementation that reads from `.gstore` manifests and segments.
pub fn runtime_api() -> Option<&'static dyn GraphRuntime> {
    // Expose a basic runtime placeholder so routing can detect availability.
    // Actual read path will be implemented in subsequent iterations.
    Some(&RUNTIME)
}

// ---------- Read-only manifest and loader (initial skeleton) ----------

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Partitioning {
    #[serde(default)]
    pub strategy: Option<String>,   // e.g., "hash_mod"
    #[serde(default)]
    pub hash_seed: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ClusterGroup {
    pub part_begin: u32,
    pub part_end: u32,
    #[serde(default)]
    pub replica_set: Vec<String>,
    #[serde(default)]
    pub leader: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ClusterMeta {
    #[serde(default)]
    pub replication_factor: Option<u32>,
    #[serde(default)]
    pub epoch_term: Option<u64>,
    #[serde(default)]
    pub placement_version: Option<u64>,
    #[serde(default)]
    pub groups: Vec<ClusterGroup>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphManifest {
    pub engine: String,           // "graphstore"
    pub epoch: Option<u64>,       // manifest epoch/version
    pub partitions: u32,          // number of edge partitions
    #[serde(default)]
    pub options: HashMap<String, String>,
    #[serde(default)]
    pub partitioning: Option<Partitioning>,
    #[serde(default)]
    pub cluster: Option<ClusterMeta>,
    pub nodes: NodesSection,
    pub edges: EdgesSection,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodesSection {
    /// Ordered list of dictionary segments (relative paths under .gstore)
    #[serde(default)]
    pub dict_segments: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EdgesSection {
    /// Edge adjacency segments per partition (relative paths)
    #[serde(default)]
    pub partitions: Vec<PartitionSegments>,
    /// Whether reverse adjacency exists; if true, partitions[].radj_segments may be present
    #[serde(default)]
    pub has_reverse: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PartitionSegments {
    pub part: u32,
    /// Immutable forward adjacency segments in ascending seal order
    #[serde(default)]
    pub adj_segments: Vec<String>,
    /// Optional reverse adjacency segments
    #[serde(default)]
    pub radj_segments: Vec<String>,
    /// Optional delta log file for recent writes (not used in read-only)
    #[serde(default)]
    pub delta_log: Option<String>,
}

/// A handle to a loaded GraphStore (read-only for now)
pub struct GraphHandle {
    pub root: PathBuf,       // .../<db>/<schema>/<graph>.gstore
    pub manifest: GraphManifest,
    // Lazily loaded read-only structures
    dict: Option<NodeDict>,
    parts: Option<Vec<PartitionState>>, // indexed by part id
}

impl GraphHandle {
    /// Open a GraphStore by reading its manifest from `<qname>.gstore/meta/manifest.json`.
    pub fn open(store: &SharedStore, graph_qualified: &str) -> Result<Self> {
        let root = gstore_root(store, graph_qualified);
        let manifest_path = root.join("meta").join("manifest.json");
        if !manifest_path.exists() {
            return Err(anyhow!(
                "GraphStore manifest not found at {}",
                manifest_path.display()
            ));
        }
        let data = std::fs::read_to_string(&manifest_path)
            .with_context(|| format!("reading manifest {}", manifest_path.display()))?;
        let mf: GraphManifest = serde_json::from_str(&data)
            .with_context(|| format!("parsing manifest {}", manifest_path.display()))?;

        if mf.engine.to_ascii_lowercase() != "graphstore" {
            return Err(anyhow!("unsupported engine '{}' in manifest", mf.engine));
        }
        if mf.partitions == 0 {
            return Err(anyhow!("manifest partitions must be > 0"));
        }
        // Validate partitioning/cluster metadata if present (forward-compatible, no-op otherwise)
        if let Some(p) = &mf.partitioning {
            if let Some(strategy) = &p.strategy {
                let s = strategy.to_ascii_lowercase();
                if s != "hash_mod" {
                    return Err(anyhow!("unsupported partitioning strategy '{}'", strategy));
                }
            }
        }
        if let Some(cluster) = &mf.cluster {
            for g in &cluster.groups {
                if g.part_begin > g.part_end { return Err(anyhow!("cluster group part_begin > part_end")); }
                if g.part_end >= mf.partitions { return Err(anyhow!("cluster group part_end out of range")); }
                if g.replica_set.is_empty() { return Err(anyhow!("cluster group replica_set must not be empty")); }
                if let Some(leader) = &g.leader {
                    if !g.replica_set.iter().any(|n| n == leader) {
                        return Err(anyhow!("cluster group leader '{}' not in replica_set", leader));
                    }
                }
            }
            if let Some(rf) = cluster.replication_factor {
                if rf == 0 { return Err(anyhow!("replication_factor must be > 0")); }
                // Best-effort check: ensure every group has at least rf replicas
                for g in &cluster.groups {
                    if (g.replica_set.len() as u32) < rf {
                        return Err(anyhow!("cluster group has fewer replicas than replication_factor"));
                    }
                }
            }
        }
        // Basic existence checks for referenced segments (best-effort; read-only skeleton)
        for seg in &mf.nodes.dict_segments {
            let p = root.join(seg);
            if !p.exists() {
                return Err(anyhow!("missing node dict segment: {}", p.display()));
            }
        }
        for ps in &mf.edges.partitions {
            if ps.part >= mf.partitions {
                return Err(anyhow!(
                    "partition index {} out of range (partitions={})",
                    ps.part, mf.partitions
                ));
            }
            for s in &ps.adj_segments {
                let p = root.join(s);
                if !p.exists() {
                    return Err(anyhow!("missing adjacency segment: {}", p.display()));
                }
            }
            for s in &ps.radj_segments {
                let p = root.join(s);
                if !p.exists() {
                    return Err(anyhow!("missing reverse adjacency segment: {}", p.display()));
                }
            }
        }

        Ok(GraphHandle { root, manifest: mf, dict: None, parts: None })
    }
}

/// Resolve `<db>/<schema>/<graph>.gstore` directory for a qualified name like `clarium/public/know`.
fn gstore_root(store: &SharedStore, qualified: &str) -> PathBuf {
    let mut p = store.0.lock().root_path().clone();
    let local = qualified.replace('/', std::path::MAIN_SEPARATOR_STR);
    p.push(local);
    p.set_extension("gstore");
    p
}

/// Utility for callers to quickly check for existence and basic validity of a graphstore.
pub fn probe_graphstore(store: &SharedStore, qualified: &str) -> Result<bool> {
    let root = gstore_root(store, qualified);
    let manifest_path = root.join("meta").join("manifest.json");
    if !manifest_path.exists() {
        return Ok(false);
    }
    // Try parsing; report false on parse/validation errors so router can fall back gracefully.
    match GraphHandle::open(store, qualified) {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

// Future work: adjacency segment mmap loader and BFS routines will live here or
// in submodules (e.g., `segments`, `runtime`). Keeping skeleton minimal for now.

// Submodule declarations for upcoming read path implementation
pub mod segments;
pub mod wal;
pub mod delta;
pub mod manifest;
pub mod delta_log;
pub mod recovery;
pub mod txn;
pub mod compaction;
pub mod metrics;

// ---------------- Basic runtime placeholder ----------------

struct BasicRuntime;

static RUNTIME: BasicRuntime = BasicRuntime;

impl GraphRuntime for BasicRuntime {
    fn neighbors_bfs(
        &self,
        _store: &SharedStore,
        graph_qualified: &str,
        _start: &str,
        _etype: Option<&str>,
        _max_hops: i64,
        _time_start: Option<&str>,
        _time_end: Option<&str>,
    ) -> Result<DataFrame> {
        gsm::inc_bfs_calls();
        // Open handle and load immutable structures.
        let mut handle = GraphHandle::open(_store, graph_qualified)?;
        handle.ensure_loaded()?;

        // Resolve start node via dictionary. We accept either "label:key" or just "key".
        let start_key = _start;
        let (label_opt, key_str) = if let Some((l,k)) = start_key.split_once(':') { (Some(l.trim()), k.trim()) } else { (None, start_key) };
        let dict = handle.dict.as_ref().ok_or_else(|| anyhow!("missing dictionary"))?;
        let start_id = if let Some(l) = label_opt {
            dict.lookup(l, key_str)
        } else {
            dict.lookup_any_label(key_str)
        }.ok_or_else(|| anyhow!("start node '{}' not found in dictionary", start_key))?;

        // BFS up to max_hops using the partition that owns start_id.
        let parts = handle.parts.as_ref().ok_or_else(|| anyhow!("no partitions loaded"))?;
        let pcount = handle.manifest.partitions as u64;
        let part_id = (start_id % pcount) as usize;
        let part = parts.get(part_id).ok_or_else(|| anyhow!("partition {} missing", part_id))?;
        let adj = part.adj.as_ref().ok_or_else(|| anyhow!("adjacency segment not available for part {}", part_id))?;

        let max_hops = _max_hops.max(0) as u64;
        if max_hops == 0 { return Ok(DataFrame::empty()); }

        use std::collections::{HashSet, VecDeque};
        let mut visited: HashSet<u64> = HashSet::with_capacity(1024);
        visited.insert(start_id);
        let mut q: VecDeque<(u64, u64, u64)> = VecDeque::new(); // (node, prev, hop)
        // seed level 1 from start (merge CSR + delta adds, filter tombstones)
        let (s, e) = adj.neighbors_range(start_id)?;
        // 1) CSR neighbors
        for idx in s..e {
            let v = adj.neighbor_at(idx);
            // filter by tombstone if present
            let tomb = parts.get(part_id).and_then(|ps| ps.delta.as_ref()).map(|d| d.tombstones.contains(&(start_id, v))).unwrap_or(false);
            if !tomb && visited.insert(v) {
                q.push_back((v, start_id, 1));
            }
        }
        // 2) Delta-added neighbors for start
        if let Some(didx) = parts.get(part_id).and_then(|ps| ps.delta.as_ref()) {
            if let Some(list) = didx.adds.get(&start_id) {
                for &v in list {
                    let tomb = didx.tombstones.contains(&(start_id, v));
                    if !tomb && visited.insert(v) {
                        q.push_back((v, start_id, 1));
                    }
                }
            }
        }

        // BFS
        let mut out_nodes: Vec<String> = Vec::new();
        let mut out_prev: Vec<String> = Vec::new();
        let mut out_hops: Vec<i64> = Vec::new();

        while let Some((u, prev, hop)) = q.pop_front() {
            // record
            let (node_label, node_key) = handle.dict.as_ref().and_then(|d| d.reverse_key(u)).unwrap_or(("", ""));
            let (prev_label, prev_key) = handle.dict.as_ref().and_then(|d| d.reverse_key(prev)).unwrap_or(("", ""));
            let node_id_txt = if !node_key.is_empty() { node_key.to_string() } else { format!("{}", u) };
            let prev_id_txt = if !prev_key.is_empty() { prev_key.to_string() } else { format!("{}", prev) };
            // Optionally include labels later; for TVF parity we output node_id/prev_id as text keys.
            out_nodes.push(node_id_txt);
            out_prev.push(prev_id_txt);
            out_hops.push(hop as i64);

            if hop >= max_hops { continue; }

            // neighbors of u live in its partition; compute and use same partition id
            let upart_id = (u % pcount) as usize;
            // Merge CSR neighbors
            if let Some(uadj) = parts.get(upart_id).and_then(|ps| ps.adj.as_ref()) {
                let (s2, e2) = uadj.neighbors_range(u)?;
                for idx2 in s2..e2 {
                    let v2 = uadj.neighbor_at(idx2);
                    let tomb = parts.get(upart_id).and_then(|ps| ps.delta.as_ref()).map(|d| d.tombstones.contains(&(u, v2))).unwrap_or(false);
                    if !tomb && visited.insert(v2) {
                        q.push_back((v2, u, hop + 1));
                    }
                }
            }
            // Merge delta-added neighbors for u
            if let Some(didx) = parts.get(upart_id).and_then(|ps| ps.delta.as_ref()) {
                if let Some(list) = didx.adds.get(&u) {
                    for &v2 in list {
                        let tomb = didx.tombstones.contains(&(u, v2));
                        if !tomb && visited.insert(v2) {
                            q.push_back((v2, u, hop + 1));
                        }
                    }
                }
            }
        }

        // Build DataFrame (align with existing constructors used in exec_graph_runtime)
        let s_node = Series::new("node_id".into(), out_nodes);
        let s_prev = Series::new("prev_id".into(), out_prev);
        let s_hop = Series::new("hop".into(), out_hops);
        let df = DataFrame::new(vec![s_node.into(), s_prev.into(), s_hop.into()])?;
        Ok(df)
    }
}

// ---------------- Internal per-partition state (read-only) ----------------

#[derive(Debug)]
struct PartitionState {
    #[allow(dead_code)]
    part_id: u32,
    // For now, maintain the latest immutable segment only (last in list);
    // delta indexes will be added later.
    adj: Option<AdjSegment>,
    delta: Option<PartitionDeltaIndex>,
}

impl GraphHandle {
    /// Load immutable segments (dictionary and latest adjacency per partition) lazily.
    /// Safe to call multiple times; subsequent calls are no-ops if already loaded.
    pub fn ensure_loaded(&mut self) -> Result<()> {
        if self.dict.is_some() && self.parts.is_some() { return Ok(()); }

        // Load dictionary (use the last segment if multiple are listed)
        let dict_path_opt = self.manifest
            .nodes
            .dict_segments
            .last()
            .map(|rel| self.root.join(rel));
        let mut dict = if let Some(p) = dict_path_opt { NodeDict::open(&p)? } else { NodeDict::default() };
        // Overlay node delta log entries (if any) to reflect recent upserts/deletes before next dict compaction
        let nlog_path = self.root.join("nodes").join("delta.log");
        if nlog_path.exists() {
            if let Ok(mut rdr) = NodeDeltaLogReader::open(&nlog_path) {
                if let Ok(recs) = rdr.read_all_nodes() {
                    // Apply in file order; idempotency ensured by future seen-set if needed
                    for r in recs {
                        match r.op {
                            0 => dict.upsert(&r.label, &r.key, r.node_id),
                            1 => dict.delete(&r.label, &r.key),
                            _ => {}
                        }
                    }
                }
            }
        }

        // Build partitions vector sized to manifest.partitions
        let mut parts: Vec<PartitionState> = (0..self.manifest.partitions)
            .map(|pid| PartitionState { part_id: pid, adj: None, delta: None })
            .collect();

        // For each partition entry in manifest, open the last adjacency segment
        for ps in &self.manifest.edges.partitions {
            if let Some(last) = ps.adj_segments.last() {
                let p = self.root.join(last);
                let adj = AdjSegment::open(&p)
                    .with_context(|| format!("opening adjacency segment for part {} at {}", ps.part, p.display()))?;
                if let Some(slot) = parts.get_mut(ps.part as usize) {
                    slot.adj = Some(adj);
                }
            }
        }

        // First, persist committed WAL records into delta logs to bound recovery.
        let _ = crate::server::graphstore::recovery::replay_wal_to_delta(&self.root).map(|_| { gsm::inc_recoveries(); () });

        // Load persisted delta logs per partition if present and merge into in-memory indexes
        let edges_dir = self.root.join("edges");
        if edges_dir.exists() {
            for pid in 0..self.manifest.partitions {
                let fname = format!("delta.P{:0>3}.log", pid);
                let p = edges_dir.join(&fname);
                if !p.exists() { continue; }
                if let Ok(mut rdr) = DeltaLogReader::open(&p) {
                    if let Ok(recs) = rdr.read_all_edges() {
                        let slot = parts.get_mut(pid as usize).unwrap();
                        if slot.delta.is_none() { slot.delta = Some(PartitionDeltaIndex::default()); }
                        let didx = slot.delta.as_mut().unwrap();
                        let mut seen: std::collections::HashSet<(u64,u32)> = std::collections::HashSet::new();
                        apply_edge_deltas(didx, &recs, &mut seen);
                    }
                }
            }
        }

        self.dict = Some(dict);
        self.parts = Some(parts);
        Ok(())
    }

    #[allow(dead_code)]
    fn dict(&self) -> Option<&NodeDict> { self.dict.as_ref() }

    #[allow(dead_code)]
    fn part(&self, p: u32) -> Option<&PartitionState> {
        self.parts.as_ref()?.get(p as usize)
    }

    /// Atomically publish a new manifest for this graph handle.
    /// Caller must ensure any referenced segments already exist on disk.
    pub fn write_and_rotate_manifest(&self, next: &GraphManifest) -> Result<()> {
        if next.engine.to_ascii_lowercase() != "graphstore" {
            return Err(anyhow!("manifest engine must be 'graphstore'"));
        }
        if next.partitions == 0 {
            return Err(anyhow!("manifest partitions must be > 0"));
        }
        let json = serde_json::to_string_pretty(next)?;
        mfutil::rotate_manifest(&self.root, &json)
    }

    /// Seal current deltas into new immutable adjacency segments for all partitions and publish a new manifest.
    /// Returns the new epoch on success.
    pub fn compact_and_publish(&mut self) -> Result<u64> {
        // Ensure current state loaded
        self.ensure_loaded()?;
        let current_epoch = self.manifest.epoch.unwrap_or(0);

        // Build next manifest by cloning and updating adj segments per partition
        let mut next = self.manifest.clone();
        let mut max_seq_per_part: Vec<u64> = vec![0; next.partitions as usize];
        // Determine next seqno by parsing last segment filename suffix
        for ps in &next.edges.partitions {
            if let Some(last) = ps.adj_segments.last() {
                if let Some(num) = last.rsplit('.').next().and_then(|s| s.parse::<u64>().ok()) {
                    max_seq_per_part[ps.part as usize] = num;
                }
            }
        }

        // For each partition, compact if we have an immutable base
        for ps in &mut next.edges.partitions {
            let pid = ps.part as usize;
            if let Some(part) = self.parts.as_ref().and_then(|v| v.get(pid)) {
                if let Some(ref adj) = part.adj {
                    let next_seq = max_seq_per_part[pid].saturating_add(1);
                    let rel = crate::server::graphstore::compaction::compact_partition(
                        &self.root, ps.part, adj, part.delta.as_ref(), next_seq,
                    )?;
                    ps.adj_segments.push(rel);
                }
            }
        }

        // Bump epoch and publish
        next.epoch = Some(current_epoch.saturating_add(1));
        self.write_and_rotate_manifest(&next)?;
        // Update in-memory manifest
        self.manifest = next;
        // Refresh in-memory parts and clear applied deltas so future reads hit compacted CSR
        self.parts = None;
        self.ensure_loaded()?;
        Ok(self.manifest.epoch.unwrap())
    }

    /// Evaluate simple GC thresholds against in-memory delta indexes and, if exceeded,
    /// compact and publish a new manifest. Returns true if a compaction occurred.
    pub fn run_gc_if_needed(&mut self) -> Result<bool> {
        self.ensure_loaded()?;
        // Read thresholds from env. Defaults are conservative and can be tuned.
        let max_delta_records: i64 = std::env::var("CLARIUM_GRAPH_GC_MAX_DELTA_RECORDS").ok()
            .and_then(|s| s.parse::<i64>().ok()).unwrap_or(10_000);
        let tombstone_ratio_ppm: i64 = std::env::var("CLARIUM_GRAPH_GC_TOMBSTONE_RATIO_PPM").ok()
            .and_then(|s| s.parse::<i64>().ok()).unwrap_or(300_000); // 30%
        // Age-based GC requires per-record timestamps; omitted in this phase.

        let mut need_compact = false;
        if let Some(parts) = &self.parts {
            for p in parts {
                if let Some(d) = &p.delta {
                    let adds = d.adds.values().map(|v| v.len() as i64).sum::<i64>();
                    let tombs = d.tombstones.len() as i64;
                    let total = adds + tombs;
                    if total >= max_delta_records { need_compact = true; break; }
                    if total > 0 {
                        let ratio_ppm = (tombs * 1_000_000) / total.max(1);
                        if ratio_ppm >= tombstone_ratio_ppm { need_compact = true; break; }
                    }
                }
            }
        }
        if need_compact {
            let _ = self.compact_and_publish()?;
            return Ok(true);
        }
        Ok(false)
    }
}

/// GraphStore status snapshot as a DataFrame for observability.
/// Columns: graph, epoch, partitions, bfs_calls, wal_commits, recoveries
pub fn graphstore_status_df(store: &SharedStore, graph: &str) -> Result<DataFrame> {
    let mut epoch_val: i64 = -1;
    let mut parts_val: i64 = 0;
    let mut delta_adds: i64 = 0;
    let mut delta_tombs: i64 = 0;
    let mut backlog: i64 = 0;
    if let Ok(mut handle) = GraphHandle::open(store, graph) {
        let _ = handle.ensure_loaded();
        epoch_val = handle.manifest.epoch.unwrap_or(0) as i64;
        parts_val = handle.manifest.partitions as i64;
        if let Some(parts) = &handle.parts {
            for p in parts {
                if let Some(d) = &p.delta {
                    delta_adds += d.adds.values().map(|v| v.len() as i64).sum::<i64>();
                    delta_tombs += d.tombstones.len() as i64;
                }
            }
        }
    }
    backlog = delta_adds + delta_tombs;
    // Config snapshot: group-commit window and GC thresholds from env (manifest options could be added similarly)
    let commit_window_ms: i64 = std::env::var("CLARIUM_GRAPH_COMMIT_WINDOW_MS").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(3);
    let gc_max_delta_records: i64 = std::env::var("CLARIUM_GRAPH_GC_MAX_DELTA_RECORDS").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(10_000);
    let gc_tombstone_ratio_ppm: i64 = std::env::var("CLARIUM_GRAPH_GC_TOMBSTONE_RATIO_PPM").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(300_000); // 30%
    let gc_max_delta_age_ms: i64 = std::env::var("CLARIUM_GRAPH_GC_MAX_DELTA_AGE_MS").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(5 * 60_000);
    let snap = gsm::snapshot();
    let df = DataFrame::new(vec![
        Series::new("graph".into(), vec![graph.to_string()]).into(),
        Series::new("epoch".into(), vec![epoch_val]).into(),
        Series::new("partitions".into(), vec![parts_val]).into(),
        Series::new("delta_adds".into(), vec![delta_adds]).into(),
        Series::new("delta_tombstones".into(), vec![delta_tombs]).into(),
        Series::new("compaction_backlog".into(), vec![backlog]).into(),
        Series::new("commit_window_ms".into(), vec![commit_window_ms]).into(),
        Series::new("gc_max_delta_records".into(), vec![gc_max_delta_records]).into(),
        Series::new("gc_tombstone_ratio_ppm".into(), vec![gc_tombstone_ratio_ppm]).into(),
        Series::new("gc_max_delta_age_ms".into(), vec![gc_max_delta_age_ms]).into(),
        Series::new("bfs_calls".into(), vec![snap.bfs_calls as i64]).into(),
        Series::new("wal_commits".into(), vec![snap.wal_commits as i64]).into(),
        Series::new("recoveries".into(), vec![snap.recoveries as i64]).into(),
    ])?;
    Ok(df)
}

/// Scan `db_root` for all `*.gstore` graphs and attempt GC on each.
pub fn gc_scan_all_graphs(store: &SharedStore) {
    // Walk db_root and find any path ending with .gstore/meta/manifest.json
    let root = store.0.lock().root_path().clone();
    let mut graphs: Vec<String> = Vec::new();
    if let Ok(dbs) = std::fs::read_dir(&root) {
        for dbe in dbs.flatten() {
            if !dbe.path().is_dir() { continue; }
            if let Ok(schemas) = std::fs::read_dir(dbe.path()) {
                for sche in schemas.flatten() {
                    if !sche.path().is_dir() { continue; }
                    if let Ok(entries) = std::fs::read_dir(sche.path()) {
                        for ent in entries.flatten() {
                            let p = ent.path();
                            if p.extension().and_then(|s| s.to_str()).map(|e| e.eq_ignore_ascii_case("gstore")).unwrap_or(false) {
                                // Rebuild qualified graph name from folder path: db/schema/name
                                if let (Some(db), Some(schema), Some(name_os)) = (
                                    dbe.file_name().to_str(),
                                    sche.file_name().to_str(),
                                    p.file_stem(),
                                ) {
                                    if let Some(name) = name_os.to_str() {
                                        graphs.push(format!("{}/{}/{}", db, schema, name));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    for g in graphs {
        if let Ok(mut handle) = GraphHandle::open(store, &g) {
            let _ = handle.run_gc_if_needed();
        }
    }
}

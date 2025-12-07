//! GraphStore delta indexes (in-memory) and WAL recovery skeleton
//! --------------------------------------------------------------
//! Builds per-partition in-memory delta indexes (adds and tombstones) by
//! scanning the WAL and applying only committed transactions. This module is
//! intentionally read-focused for now: it does not yet implement append-only
//! delta log files on disk; instead, it provides the recovery path needed to
//! materialize recent writes for read merging.

use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use super::wal::{WalReader, WalRecord, TxnData};

#[derive(Debug, Default)]
pub struct PartitionDeltaIndex {
    /// Map of src_id -> appended neighbors (dst_ids) to consider in addition to CSR
    pub adds: HashMap<u64, Vec<u64>>,
    /// Set of tombstones (src,dst) marking deletions that should be excluded
    pub tombstones: HashSet<(u64, u64)>,
}

impl PartitionDeltaIndex {
    pub fn add_edge(&mut self, src: u64, dst: u64) {
        self.adds.entry(src).or_default().push(dst);
    }
    pub fn del_edge(&mut self, src: u64, dst: u64) { self.tombstones.insert((src, dst)); }
}

/// Build per-partition delta indexes by reading a WAL file and applying
/// committed transactions only. Idempotent: if the same txn_id appears
/// twice, the resulting indexes are equivalent.
pub fn build_indexes_from_wal(wal_path: &Path) -> Result<HashMap<u32, PartitionDeltaIndex>> {
    if !wal_path.exists() { return Ok(HashMap::new()); }
    let mut reader = WalReader::open(wal_path)?;
    let recs = reader.read_all()?;
    build_indexes_from_records(&recs)
}

/// Internal helper: build indexes from an in-memory list of WAL records.
pub fn build_indexes_from_records(recs: &[WalRecord]) -> Result<HashMap<u32, PartitionDeltaIndex>> {
    // 1) Collect Data records per txn_id
    let mut by_txn: HashMap<u64, Vec<TxnData>> = HashMap::new();
    let mut committed: HashSet<u64> = HashSet::new();
    let mut aborted: HashSet<u64> = HashSet::new();
    for r in recs {
        match r {
            WalRecord::Data(d) => { by_txn.entry(d.txn_id).or_default().push(d.clone()); },
            WalRecord::Commit(c) => { committed.insert(c.txn_id); },
            WalRecord::Abort(a) => { aborted.insert(a.txn_id); },
            WalRecord::Begin(_) => {}
        }
    }
    // 2) Apply committed (not aborted) txn data in txn_id order for determinism
    let mut txns: Vec<u64> = by_txn.keys().copied().collect();
    txns.sort_unstable();
    let mut out: HashMap<u32, PartitionDeltaIndex> = HashMap::new();
    for tid in txns {
        if !committed.contains(&tid) || aborted.contains(&tid) { continue; }
        if let Some(datas) = by_txn.get(&tid) {
            for d in datas {
                for e in &d.edges {
                    let idx = out.entry(e.part).or_default();
                    match e.op { 0 => idx.add_edge(e.src, e.dst), 1 => idx.del_edge(e.src, e.dst), _ => {} }
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
#[path = "delta_tests.rs"]
mod delta_tests;

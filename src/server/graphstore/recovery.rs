//! GraphStore recovery: WAL → delta log replay (bounded startup)
//! ------------------------------------------------------------
//! On startup/open, scan WAL files and persist committed edge ops into
//! per-partition delta logs idempotently. This bounds WAL replay cost and
//! lets readers rebuild in-memory indexes from delta logs quickly.

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use super::delta_log::{DeltaLogReader, DeltaLogWriter, EdgeDeltaRec};
use super::wal::{WalReader, WalRecord, TxnData};

fn wal_files(wal_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = Vec::new();
    if wal_dir.exists() {
        for e in std::fs::read_dir(wal_dir)? {
            let p = e?.path();
            if let Some(ext) = p.extension().and_then(|s| s.to_str()) {
                if ext.eq_ignore_ascii_case("lg") { files.push(p); }
            }
        }
    }
    files.sort();
    Ok(files)
}

/// Read a partition delta log and build the seen set of `(txn_id, op_index)`.
fn build_seen_for_part(edges_dir: &Path, part: u32) -> Result<HashSet<(u64, u32)>> {
    let mut seen: HashSet<(u64,u32)> = HashSet::new();
    let p = edges_dir.join(format!("delta.P{:0>3}.log", part));
    if p.exists() {
        if let Ok(mut rdr) = DeltaLogReader::open(&p) {
            if let Ok(recs) = rdr.read_all_edges() {
                for r in recs { seen.insert((r.txn_id, r.op_index)); }
            }
        }
    }
    Ok(seen)
}

/// Persist committed WAL edge ops into per-partition delta logs idempotently.
pub fn replay_wal_to_delta(graph_root: &Path) -> Result<()> {
    let wal_dir = graph_root.join("wal");
    let edges_dir = graph_root.join("edges");
    std::fs::create_dir_all(&edges_dir).ok();

    let files = wal_files(&wal_dir)?;
    if files.is_empty() { return Ok(()); }

    // Aggregate records across WALs
    let mut recs: Vec<WalRecord> = Vec::new();
    for f in files {
        if let Ok(mut rdr) = WalReader::open(&f) {
            if let Ok(mut r) = rdr.read_all() { recs.append(&mut r); }
        }
    }

    // Build committed map txn_id -> Vec<TxnData>
    let mut by_txn: HashMap<u64, Vec<TxnData>> = HashMap::new();
    let mut committed: HashSet<u64> = HashSet::new();
    let mut aborted: HashSet<u64> = HashSet::new();
    for r in &recs {
        match r {
            WalRecord::Data(d) => { by_txn.entry(d.txn_id).or_default().push(d.clone()); },
            WalRecord::Commit(c) => { committed.insert(c.txn_id); },
            WalRecord::Abort(a) => { aborted.insert(a.txn_id); },
            WalRecord::Begin(_) => {}
        }
    }

    // Maintain per-partition writers and seen sets
    let mut writers: HashMap<u32, DeltaLogWriter> = HashMap::new();
    let mut seen_map: HashMap<u32, HashSet<(u64,u32)>> = HashMap::new();

    // Apply in txn_id order for determinism
    let mut txns: Vec<u64> = by_txn.keys().copied().collect();
    txns.sort_unstable();
    for tid in txns {
        if !committed.contains(&tid) || aborted.contains(&tid) { continue; }
        let mut op_index: u32 = 0;
        if let Some(datas) = by_txn.get(&tid) {
            for d in datas {
                for e in &d.edges {
                    // Get or create writer
                    if !writers.contains_key(&e.part) {
                        let p = edges_dir.join(format!("delta.P{:0>3}.log", e.part));
                        let w = DeltaLogWriter::open_append(&p)?; writers.insert(e.part, w);
                    }
                    // Get or build seen set for partition
                    if !seen_map.contains_key(&e.part) {
                        let seen = build_seen_for_part(&edges_dir, e.part)?; seen_map.insert(e.part, seen);
                    }
                    let key = (tid, op_index);
                    let seen = seen_map.get_mut(&e.part).unwrap();
                    if !seen.contains(&key) {
                        let rec = EdgeDeltaRec{ txn_id: tid, op_index, op: e.op, src: e.src, dst: e.dst };
                        let w = writers.get_mut(&e.part).unwrap();
                        w.append_edge(&rec)?;
                        seen.insert(key);
                    }
                    op_index = op_index.wrapping_add(1);
                }
            }
        }
    }
    Ok(())
}

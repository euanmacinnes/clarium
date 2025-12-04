//! GraphStore transactional API (skeleton)
//! --------------------------------------
//! Provides a minimal `GraphTxn` that buffers node/edge operations, writes a
//! durable WAL on commit, and then appends per-partition delta logs to make
//! writes visible to readers before compaction.

use anyhow::{anyhow, Context, Result};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::delta_log::{DeltaLogWriter, EdgeDeltaRec};
use super::wal::{WalWriter, TxnBegin, TxnCommit, TxnData, NodeOp, EdgeOp};

/// Minimal transaction handle for a single graph.
pub struct GraphTxn {
    root: PathBuf,
    snapshot_epoch: u64,
    txn_id: u64,
    nodes: Vec<NodeOp>,
    edges: Vec<EdgeOp>,
}

impl GraphTxn {
    pub fn begin(root: &Path, snapshot_epoch: u64) -> Result<Self> {
        // Use time-based txn id for now; in production use a monotonic allocator.
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64;
        Ok(Self {
            root: root.to_path_buf(),
            snapshot_epoch,
            txn_id: now,
            nodes: Vec::new(),
            edges: Vec::new(),
        })
    }

    pub fn insert_node(&mut self, label: &str, key: &str, node_id: Option<u64>) {
        self.nodes.push(NodeOp { op: 0, label: label.to_string(), key: key.to_string(), node_id });
    }

    pub fn delete_node(&mut self, label: &str, key: &str) {
        self.nodes.push(NodeOp { op: 1, label: label.to_string(), key: key.to_string(), node_id: None });
    }

    pub fn insert_edge(&mut self, part: u32, src: u64, dst: u64, etype_id: u16) {
        self.edges.push(EdgeOp { op: 0, part, src, dst, etype_id });
    }

    pub fn delete_edge(&mut self, part: u32, src: u64, dst: u64, etype_id: u16) {
        self.edges.push(EdgeOp { op: 1, part, src, dst, etype_id });
    }

    fn wal_path(&self) -> PathBuf {
        // For now, write into a single rolling file `wal/current.lg`
        self.root.join("wal").join("current.lg")
    }

    /// Commit: append Begin/Data/Commit to WAL with fsync on commit, then append
    /// to partition delta logs idempotently.
    pub fn commit(self, commit_epoch: u64) -> Result<()> {
        // 1) WAL
        let wal_p = self.wal_path();
        if let Some(parent) = wal_p.parent() { std::fs::create_dir_all(parent).ok(); }
        let mut w = WalWriter::create(&wal_p)?;
        w.append_begin(&TxnBegin { txn_id: self.txn_id, snapshot_epoch: self.snapshot_epoch })?;
        w.append_data(&TxnData { txn_id: self.txn_id, nodes: self.nodes.clone(), edges: self.edges.clone() })?;
        w.append_commit(&TxnCommit { txn_id: self.txn_id, commit_epoch })?;

        // 2) Delta logs (make visible for readers)
        self.apply_to_delta_logs()
    }

    pub fn abort(self) -> Result<()> { Ok(()) }

    fn apply_to_delta_logs(&self) -> Result<()> {
        // Append per-partition edge records with `(txn_id, op_index)`
        use std::collections::HashMap;
        let mut writers: HashMap<u32, DeltaLogWriter> = HashMap::new();
        let edges_dir = self.root.join("edges");
        std::fs::create_dir_all(&edges_dir).ok();
        let mut op_index: u32 = 0;
        for e in &self.edges {
            let w = writers.entry(e.part).or_insert_with(|| {
                let p = edges_dir.join(format!("delta.P{:0>3}.log", e.part));
                DeltaLogWriter::open_append(&p).expect("open delta log")
            });
            let rec = EdgeDeltaRec { txn_id: self.txn_id, op_index, op: e.op, src: e.src, dst: e.dst };
            w.append_edge(&rec).with_context(|| "append edge delta")?;
            op_index = op_index.wrapping_add(1);
        }
        Ok(())
    }
}

//! GraphStore transactional API (skeleton)
//! --------------------------------------
//! Provides a minimal `GraphTxn` that buffers node/edge operations, writes a
//! durable WAL on commit, and then appends per-partition delta logs to make
//! writes visible to readers before compaction.

use anyhow::{anyhow, Context, Result};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::delta_log::{DeltaLogWriter, EdgeDeltaRec, NodeDeltaLogWriter, NodeDeltaRec};
use super::wal::{WalWriter, TxnBegin, TxnCommit, TxnData, NodeOp, EdgeOp};
use super::metrics;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

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
        // Group-commit batching window (ms) from env; default 3ms. 0 disables batching.
        let window_ms: u64 = std::env::var("CLARIUM_GRAPH_COMMIT_WINDOW_MS").ok()
            .and_then(|s| s.parse::<u64>().ok()).unwrap_or(3);
        if window_ms == 0 {
            // Fallback to immediate, durable single-commit
            return self.commit_immediate(commit_epoch);
        }

        // Basic uniqueness check within this txn for node keys (label,key)
        {
            use std::collections::HashSet;
            let mut seen: HashSet<(String,String)> = HashSet::new();
            for n in &self.nodes {
                let k = (n.label.clone(), n.key.clone());
                if !seen.insert(k) {
                    return Err(anyhow!("duplicate node upsert/delete for the same (label,key) in a single txn"));
                }
            }
        }
        // Global uniqueness check against existing dictionary + node deltas could be added here later
        // (requires loading NodeDict + overlay). Skipped in this slice to avoid heavy IO in hot path.

        // Route to batching coordinator for WAL durability
        commit_via_batch(self, commit_epoch, window_ms)
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

        // Append node deltas to nodes/delta.log with independent op_index
        if !self.nodes.is_empty() {
            let nodes_log = self.root.join("nodes").join("delta.log");
            let mut nw = NodeDeltaLogWriter::open_append(&nodes_log)?;
            let mut nidx: u32 = 0;
            for n in &self.nodes {
                let rec = NodeDeltaRec{ txn_id: self.txn_id, op_index: nidx, op: n.op, label: n.label.clone(), key: n.key.clone(), node_id: n.node_id };
                nw.append_node(&rec)?;
                nidx = nidx.wrapping_add(1);
            }
        }
        Ok(())
    }
}

// ---------------- Group-commit batching coordinator ----------------

struct BatchState {
    active: bool,
    deadline: Instant,
    pending: usize,
}

static BATCH_LOCK: OnceLock<Mutex<BatchState>> = OnceLock::new();

fn batch_lock() -> &'static Mutex<BatchState> {
    BATCH_LOCK.get_or_init(|| Mutex::new(BatchState { active: false, deadline: Instant::now(), pending: 0 }))
}

fn commit_via_batch(txn: GraphTxn, commit_epoch: u64, window_ms: u64) -> Result<()> {
    let wal_p = txn.wal_path();
    if let Some(parent) = wal_p.parent() { std::fs::create_dir_all(parent).ok(); }
    let mut writer = WalWriter::create(&wal_p)?;
    // Append begin+data, and a nosync commit; fsync will be performed once per batch.
    writer.append_begin(&TxnBegin { txn_id: txn.txn_id, snapshot_epoch: txn.snapshot_epoch })?;
    writer.append_data(&TxnData { txn_id: txn.txn_id, nodes: txn.nodes.clone(), edges: txn.edges.clone() })?;
    writer.append_commit_nosync(&TxnCommit { txn_id: txn.txn_id, commit_epoch })?;

    // Enter batching window
    let lock = batch_lock();
    let mut guard = lock.lock().unwrap();
    let now = Instant::now();
    if !guard.active {
        // Start a new batch window
        guard.active = true;
        guard.pending = 1;
        guard.deadline = now + Duration::from_millis(window_ms);
        drop(guard);
        // Allow peers to join the batch until deadline
        let mut slept = 0u64;
        while Instant::now() < (now + Duration::from_millis(window_ms)) {
            std::thread::sleep(Duration::from_millis(1));
            slept += 1;
            if slept >= window_ms { break; }
        }
        // Perform durable fsync for the whole batch
        writer.append_commit(&TxnCommit { txn_id: txn.txn_id, commit_epoch })?; // commit record with fsync
        metrics::inc_wal_commits();
        // Reset batch state
        let mut guard2 = lock.lock().unwrap();
        guard2.active = false; guard2.pending = 0;
        Ok(txn.apply_to_delta_logs()?)
    } else {
        // Join existing batch; wait until it completes
        guard.pending += 1;
        let deadline = guard.deadline;
        drop(guard);
        while Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        // The first committer will have fsync’d; just append deltas and return
        Ok(txn.apply_to_delta_logs()?)
    }
}

impl GraphTxn {
    fn commit_immediate(self, commit_epoch: u64) -> Result<()> {
        let wal_p = self.wal_path();
        if let Some(parent) = wal_p.parent() { std::fs::create_dir_all(parent).ok(); }
        let mut w = WalWriter::create(&wal_p)?;
        w.append_begin(&TxnBegin { txn_id: self.txn_id, snapshot_epoch: self.snapshot_epoch })?;
        w.append_data(&TxnData { txn_id: self.txn_id, nodes: self.nodes.clone(), edges: self.edges.clone() })?;
        w.append_commit(&TxnCommit { txn_id: self.txn_id, commit_epoch })?;
        metrics::inc_wal_commits();
        self.apply_to_delta_logs()
    }
}

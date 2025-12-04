use std::sync::atomic::{AtomicU64, Ordering};

static BFS_CALLS: AtomicU64 = AtomicU64::new(0);
static WAL_COMMITS: AtomicU64 = AtomicU64::new(0);
static RECOVERIES: AtomicU64 = AtomicU64::new(0);

pub fn inc_bfs_calls() { BFS_CALLS.fetch_add(1, Ordering::Relaxed); }
pub fn inc_wal_commits() { WAL_COMMITS.fetch_add(1, Ordering::Relaxed); }
pub fn inc_recoveries() { RECOVERIES.fetch_add(1, Ordering::Relaxed); }

#[derive(Clone, Copy, Debug)]
pub struct Snapshot {
    pub bfs_calls: u64,
    pub wal_commits: u64,
    pub recoveries: u64,
}

pub fn snapshot() -> Snapshot {
    Snapshot {
        bfs_calls: BFS_CALLS.load(Ordering::Relaxed),
        wal_commits: WAL_COMMITS.load(Ordering::Relaxed),
        recoveries: RECOVERIES.load(Ordering::Relaxed),
    }
}

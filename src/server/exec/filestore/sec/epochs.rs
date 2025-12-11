//! Epoch counters for cache invalidation (scaffold).

use std::sync::atomic::{AtomicU64, Ordering};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use parking_lot::RwLock;

static EPOCH_GLOBAL: AtomicU64 = AtomicU64::new(1);
static EPOCH_FS: Lazy<RwLock<HashMap<String, u64>>> = Lazy::new(|| RwLock::new(HashMap::new()));
static EPOCH_PUB: Lazy<RwLock<HashMap<String, u64>>> = Lazy::new(|| RwLock::new(HashMap::new()));

pub fn epoch_global() -> u64 { EPOCH_GLOBAL.load(Ordering::Relaxed) }
pub fn bump_global() -> u64 { EPOCH_GLOBAL.fetch_add(1, Ordering::Relaxed) + 1 }

pub fn epoch_filestore(name: &str) -> u64 {
    *EPOCH_FS.read().get(name).unwrap_or(&1)
}
pub fn bump_filestore(name: &str) -> u64 {
    let mut m = EPOCH_FS.write();
    let v = m.get(name).copied().unwrap_or(1) + 1;
    m.insert(name.to_string(), v);
    v
}

pub fn epoch_publication(name: &str) -> u64 {
    *EPOCH_PUB.read().get(name).unwrap_or(&1)
}
pub fn bump_publication(name: &str) -> u64 {
    let mut m = EPOCH_PUB.write();
    let v = m.get(name).copied().unwrap_or(1) + 1;
    m.insert(name.to_string(), v);
    v
}

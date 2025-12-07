//! GC utilities for FILESTORE: dry-run and apply for tombstones and orphaned chunks.
//! These are conservative and operate only on the in-memory KV namespaces.

use anyhow::Result;

use crate::storage::{SharedStore, KvValue};

use super::kv::Keys;
use super::types::FileMeta;

#[derive(Debug, Clone, Default)]
pub struct GcReport {
    pub files_tombstoned: i64,
    pub files_deleted: i64,
    pub orphan_chunks: i64,
}

/// Dry-run GC: scan for tombstoned files and orphaned chunks. Does not delete.
pub fn gc_dry_run(store: &SharedStore, database: &str, filestore: &str) -> Result<GcReport> {
    let kv = store.kv_store(database, filestore);
    let mut rep = GcReport::default();
    // Count tombstoned files
    let path_prefix = Keys::path(database, filestore, "");
    for k in kv.keys() {
        if !k.starts_with(&path_prefix) { continue; }
        if let Some(KvValue::Json(j)) = kv.get(&k) {
            if let Ok(m) = serde_json::from_value::<FileMeta>(j) {
                if m.deleted { rep.files_tombstoned += 1; }
            }
        }
    }
    // Orphaned chunks detection not implemented yet (no ref indexes) — leave zero for now.
    Ok(rep)
}

/// Apply GC with conservative behavior: remove only tombstoned file metadata.
/// Chunk collection is postponed until reference tracking exists.
pub fn gc_apply(store: &SharedStore, database: &str, filestore: &str) -> Result<GcReport> {
    let kv = store.kv_store(database, filestore);
    let mut rep = GcReport::default();
    // Delete tombstoned file metas
    let path_prefix = Keys::path(database, filestore, "");
    let keys: Vec<String> = kv
        .keys()
        .into_iter()
        .filter(|k| k.starts_with(&path_prefix))
        .collect();
    for k in keys {
        if let Some(KvValue::Json(j)) = kv.get(&k) {
            if let Ok(m) = serde_json::from_value::<FileMeta>(j) {
                if m.deleted {
                    if kv.delete(&k) { rep.files_deleted += 1; }
                }
            }
        }
    }
    Ok(rep)
}

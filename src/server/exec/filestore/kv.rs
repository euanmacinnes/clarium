//! Namespaced KV key builders and ETag helpers for FILESTORE
//! Keep this module focused and small; complex logic belongs in higher layers.

use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

#[inline]
fn ns(prefix: &str, filestore: &str) -> String {
    format!("{}.store.filestore.{}", prefix, filestore)
}

/// Build keys for FILESTORE-scoped namespaces.
pub struct Keys;

impl Keys {
    // Core value spaces -----------------------------------------------------
    pub fn blob(db: &str, fs: &str, file_guid: &Uuid) -> String {
        format!("{}{}{}", ns(db, fs), ".blob::", file_guid)
    }
    #[inline]
    pub fn blob_prefix(db: &str, fs: &str) -> String { format!("{}{}", ns(db, fs), ".blob::") }
    pub fn text(db: &str, fs: &str, file_guid: &Uuid) -> String {
        format!("{}{}{}", ns(db, fs), ".text::", file_guid)
    }
    #[inline]
    pub fn text_prefix(db: &str, fs: &str) -> String { format!("{}{}", ns(db, fs), ".text::") }
    pub fn manifest(db: &str, fs: &str, manifest_guid: &Uuid) -> String {
        format!("{}{}{}", ns(db, fs), ".manifest::", manifest_guid)
    }
    #[inline]
    pub fn manifest_prefix(db: &str, fs: &str) -> String { format!("{}{}", ns(db, fs), ".manifest::") }
    pub fn chunk(db: &str, fs: &str, chunk_guid: &Uuid) -> String {
        format!("{}{}{}", ns(db, fs), ".chunk::", chunk_guid)
    }
    #[inline]
    pub fn chunk_prefix(db: &str, fs: &str) -> String { format!("{}{}", ns(db, fs), ".chunk::") }
    pub fn path(db: &str, fs: &str, logical_path_nfc: &str) -> String {
        format!("{}{}{}", ns(db, fs), ".path::", logical_path_nfc)
    }
    #[inline]
    pub fn path_prefix(db: &str, fs: &str) -> String { format!("{}{}", ns(db, fs), ".path::") }
    pub fn tree(db: &str, fs: &str, tree_guid: &Uuid) -> String {
        format!("{}{}{}", ns(db, fs), ".tree::", tree_guid)
    }
    #[inline]
    pub fn tree_prefix(db: &str, fs: &str) -> String { format!("{}{}", ns(db, fs), ".tree::") }
    pub fn commit(db: &str, fs: &str, commit_guid: &Uuid) -> String {
        format!("{}{}{}", ns(db, fs), ".commit::", commit_guid)
    }
    #[inline]
    pub fn commit_prefix(db: &str, fs: &str) -> String { format!("{}{}", ns(db, fs), ".commit::") }
    pub fn alias(db: &str, fs: &str, dest_prefix_nfc: &str) -> String {
        format!("{}{}{}", ns(db, fs), ".alias::", dest_prefix_nfc)
    }
    #[inline]
    pub fn alias_prefix(db: &str, fs: &str) -> String { format!("{}{}", ns(db, fs), ".alias::") }
    pub fn git_ref(db: &str, fs: &str, remote: &str, r#ref: &str) -> String {
        format!("{}{}{}::{}", ns(db, fs), ".git::", remote, r#ref)
    }
    #[inline]
    pub fn git_ref_prefix(db: &str, fs: &str, remote: &str) -> String {
        format!("{}{}{}::", ns(db, fs), ".git::", remote)
    }
    pub fn git_map_commit_to_sha(db: &str, fs: &str, commit_guid: &Uuid) -> String {
        format!("{}{}{}", ns(db, fs), ".map.git_sha::", commit_guid)
    }

    // Information schema / registry ---------------------------------------
    pub fn info_global(db: &str) -> String { format!("{}.info.fs.global", db) }
    pub fn info_registry_prefix(db: &str) -> String { format!("{}.info.fs.registry::", db) }
    pub fn info_registry(db: &str, fs: &str) -> String { format!("{}{}", Self::info_registry_prefix(db), fs) }
    pub fn info_folder_overrides_prefix(db: &str, fs: &str) -> String { format!("{}.info.fs.folder_overrides::{}::", db, fs) }
    pub fn info_folder_override(db: &str, fs: &str, prefix_nfc: &str) -> String { format!("{}{}", Self::info_folder_overrides_prefix(db, fs), prefix_nfc) }
}

/// Very small ETag helper based on random UUIDv4 (sufficient for optimistic concurrency here).
pub fn new_etag() -> String {
    Uuid::new_v4().to_string()
}

/// Stable ETag for a byte slice using xxh3_64; returned as fixed-width lowercase hex.
pub fn etag_for_bytes(bytes: &[u8]) -> String {
    let h = xxh3_64(bytes);
    format!("{h:016x}")
}

/// Combine multiple child etags and total size into a composite etag using xxh3_64 over bytes:
/// layout: len | etag1 | etag2 | ... | total_size_le_bytes(8)
pub fn etag_composite(child_etags: &[String], total_size: u64) -> String {
    use std::io::Write;
    let mut buf: Vec<u8> = Vec::with_capacity(8 + child_etags.len() * 16 + 8);
    // write count (LE)
    buf.extend_from_slice(&(child_etags.len() as u64).to_le_bytes());
    for e in child_etags {
        // Interpret hex string as bytes-bytes by writing ascii (simple and stable across impls)
        let _ = write!(&mut buf, "{}", e);
    }
    buf.extend_from_slice(&total_size.to_le_bytes());
    let h = xxh3_64(&buf);
    format!("{h:016x}")
}

#[cfg(test)]
mod kv_tests;

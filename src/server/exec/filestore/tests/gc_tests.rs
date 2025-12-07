use super::*;
use crate::server::exec::filestore::*;
use chrono::Utc;
use tempfile::tempdir;
use crate::storage::{SharedStore, KvValue};

#[test]
fn dry_run_counts_tombstones_only() {
    let tmp = tempdir().unwrap();
    let store = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium";
    let fs = "media";
    let kv = store.kv_store(db, fs);
    let now = Utc::now().timestamp();

    // Live file
    let live = FileMeta {
        id: uuid::Uuid::new_v4().to_string(),
        logical_path: "live.txt".to_string(),
        size: 1,
        etag: "aa".to_string(),
        version: 1,
        created_at: now,
        updated_at: now,
        content_type: None,
        deleted: false,
        description_html: None,
        custom: None,
        chunking: None,
    };
    let k1 = Keys::path(db, fs, &live.logical_path);
    kv.set(k1, KvValue::Json(serde_json::to_value(&live).unwrap()), None, None);

    // Tombstoned file
    let mut tomb = live.clone();
    tomb.logical_path = "gone.txt".to_string();
    tomb.deleted = true;
    let k2 = Keys::path(db, fs, &tomb.logical_path);
    kv.set(k2, KvValue::Json(serde_json::to_value(&tomb).unwrap()), None, None);

    let rep = gc_dry_run(&store, db, fs).unwrap();
    assert_eq!(rep.files_tombstoned, 1);
    assert_eq!(rep.files_deleted, 0);
}

#[test]
fn apply_respects_grace_period() {
    let tmp = tempdir().unwrap();
    let store = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium";
    let fs = "docs";
    let kv = store.kv_store(db, fs);

    let now = Utc::now().timestamp();
    let grace = GlobalFilestoreConfig::default().gc_grace_seconds as i64;

    // Younger tombstone (should NOT delete)
    let mut m1 = FileMeta {
        id: uuid::Uuid::new_v4().to_string(),
        logical_path: "young.txt".to_string(),
        size: 1,
        etag: "a".to_string(),
        version: 1,
        created_at: now - 10,
        updated_at: now - (grace - 10).max(0),
        content_type: None,
        deleted: true,
        description_html: None,
        custom: None,
        chunking: None,
    };
    let k1 = Keys::path(db, fs, &m1.logical_path);
    kv.set(k1.clone(), KvValue::Json(serde_json::to_value(&m1).unwrap()), None, None);

    // Older tombstone (should delete)
    let mut m2 = m1.clone();
    m2.logical_path = "old.txt".to_string();
    m2.updated_at = now - (grace + 10);
    let k2 = Keys::path(db, fs, &m2.logical_path);
    kv.set(k2.clone(), KvValue::Json(serde_json::to_value(&m2).unwrap()), None, None);

    let rep = gc_apply(&store, db, fs).unwrap();
    // Only the older one should be deleted
    assert_eq!(rep.files_deleted, 1);
    assert!(kv.get(&k1).is_some());
    assert!(kv.get(&k2).is_none());
}
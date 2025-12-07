use super::*;
use crate::server::exec::filestore::*;
use crate::storage::SharedStore;
use crate::storage::KvValue;
use tempfile::tempdir;

#[test]
fn empty_files_frame_schema() {
    let df = empty_files_df().unwrap();
    // Avoid importing DataType variants that clash with `String` type
    let cols: Vec<std::string::String> = df.get_columns().iter().map(|s| s.name().to_string()).collect();
    assert_eq!(cols, vec![
        "logical_path", "size", "etag", "version", "updated_at", "deleted", "content_type"
    ]);
    // Basic dtype checks (support either Utf8 or String by Polars version)
    use polars::prelude::DataType;
    let dts: Vec<DataType> = df.get_columns().iter().map(|s| s.dtype().clone()).collect();
    assert!(is_str_dtype(&dts[0]));
    assert!(matches!(dts[1], DataType::Int64));
    assert!(is_str_dtype(&dts[2]));
    assert!(matches!(dts[3], DataType::Int64));
    assert!(matches!(dts[4], DataType::Int64));
    assert!(matches!(dts[5], DataType::Boolean));
    assert!(is_str_dtype(&dts[6]));
    assert_eq!(df.height(), 0);
}

fn is_str_dtype(dt: &polars::prelude::DataType) -> bool {
    let s = format!("{:?}", dt);
    s.contains("Utf8") || s.contains("String")
}

#[test]
fn paging_boundaries() {
    let tmp = tempdir().unwrap();
    let store = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium";
    let fs_name = "docs";

    // Insert 5 file metas under the path namespace
    let kv = store.kv_store(db, fs_name);
    let now = chrono::Utc::now().timestamp();
    for (i, name) in ["a.txt","b.txt","c.txt","d.txt","e.txt"].iter().enumerate() {
        let meta = crate::server::exec::filestore::types::FileMeta {
            id: uuid::Uuid::new_v4().to_string(),
            logical_path: name.to_string(),
            size: (i as u64) + 1,
            etag: format!("etag{i}"),
            version: 1,
            created_at: now,
            updated_at: now,
            content_type: Some("text/plain".to_string()),
            deleted: false,
            description_html: None,
            custom: None,
            chunking: None,
        };
        let key = Keys::path(db, fs_name, &meta.logical_path);
        kv.set(key, KvValue::Json(serde_json::to_value(&meta).unwrap()), None, None);
    }

    // No limit → full
    let df = show_files_df_paged(&store, db, fs_name, None, 0, None).unwrap();
    assert_eq!(df.height(), 5);

    // limit smaller than total
    let df2 = show_files_df_paged(&store, db, fs_name, None, 0, Some(2)).unwrap();
    assert_eq!(df2.height(), 2);

    // offset within bounds + big limit clips
    let df3 = show_files_df_paged(&store, db, fs_name, None, 3, Some(10)).unwrap();
    assert_eq!(df3.height(), 2);

    // offset past end → empty
    let df4 = show_files_df_paged(&store, db, fs_name, None, 10, Some(1)).unwrap();
    assert_eq!(df4.height(), 0);
}

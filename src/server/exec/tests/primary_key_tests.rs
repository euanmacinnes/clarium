use super::super::execute_query;
use crate::storage::{Store, SharedStore};
use serde_json::json;

// Single-column primary key: valid inserts, duplicate insert fails
#[tokio::test]
async fn test_primary_key_single_column_inserts_and_duplicates() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let table = "clarium/public/pk_single";
    // Create table and add primary key on id
    execute_query(&shared, &format!("CREATE TABLE {}", table)).await.unwrap();
    execute_query(&shared, &format!("ALTER TABLE {} ADD PRIMARY KEY (id)", table)).await.unwrap();

    // Insert unique rows -> ok
    let ins_ok = format!(
        "INSERT INTO {} (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
        table
    );
    let res = execute_query(&shared, &ins_ok).await.unwrap();
    assert_eq!(res["status"], json!("ok"));
    assert_eq!(res["inserted"], json!(2));

    // Duplicate id -> should fail
    let dup = format!("INSERT INTO {} (id, name) VALUES (2, 'Dup')", table);
    let err = execute_query(&shared, &dup).await.err().expect("expected PK violation");
    let msg = err.to_string();
    assert!(msg.contains("PRIMARY KEY"), "unexpected error: {}", msg);
}

// Composite primary key: (a,b)
#[tokio::test]
async fn test_primary_key_composite_inserts_and_duplicates() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let table = "clarium/public/pk_composite";
    execute_query(&shared, &format!("CREATE TABLE {}", table)).await.unwrap();
    // Use storage API to set composite PK metadata
    store.set_table_metadata(table, Some(vec!["a".to_string(), "b".to_string()]), None).unwrap();

    // Insert unique composite keys -> ok
    let ins_ok = format!(
        "INSERT INTO {} (a, b, v) VALUES ('x', 'y', 1), ('x', 'z', 2)",
        table
    );
    execute_query(&shared, &ins_ok).await.unwrap();

    // Duplicate composite key (x,y) -> fail
    let dup = format!("INSERT INTO {} (a, b, v) VALUES ('x', 'y', 3)", table);
    let err = execute_query(&shared, &dup).await.err().expect("expected PK violation");
    assert!(err.to_string().contains("PRIMARY KEY"));
}

// UPDATE behavior: skip PK validation when not touching PK; enforce when changing PK
#[tokio::test]
async fn test_primary_key_update_validation() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let table = "clarium/public/pk_update";
    execute_query(&shared, &format!("CREATE TABLE {}", table)).await.unwrap();
    execute_query(&shared, &format!("ALTER TABLE {} ADD PRIMARY KEY (id)", table)).await.unwrap();

    // Seed
    execute_query(&shared, &format!("INSERT INTO {} (id, name) VALUES (1, 'A'), (2, 'B')", table)).await.unwrap();

    // Update non-PK column -> ok
    execute_query(&shared, &format!("UPDATE {} SET name = 'AA' WHERE id = 1", table)).await.unwrap();

    // Change PK to existing -> fail
    let err = execute_query(&shared, &format!("UPDATE {} SET id = 2 WHERE id = 1", table)).await.err().expect("expected PK violation");
    assert!(err.to_string().contains("PRIMARY KEY"));

    // Change PK to unique -> ok
    execute_query(&shared, &format!("UPDATE {} SET id = 3 WHERE id = 1", table)).await.unwrap();
}

// Partitioning respected on INSERT and UPDATE with PK including partition column
#[tokio::test]
async fn test_partitioning_with_primary_key_on_insert_and_update() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let table = "clarium/public/pk_part";
    execute_query(&shared, &format!("CREATE TABLE {}", table)).await.unwrap();
    // Set PK(a, region) and partition by region
    store.set_table_metadata(
        table,
        Some(vec!["a".to_string(), "region".to_string()]),
        Some(vec!["region".to_string()])
    ).unwrap();

    // Insert rows across two regions -> partition-aware write should create multiple files
    execute_query(&shared, &format!("INSERT INTO {} (a, region, v) VALUES (1, 'north', 10), (2, 'south', 20)", table)).await.unwrap();

    // Count files
    let dir = { let g = shared.0.lock(); g.root_path().join(table.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str())) };
    let mut files_before = 0usize;
    for e in std::fs::read_dir(&dir).unwrap() {
        let p = e.unwrap().path();
        if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
            if name.ends_with(".parquet") { files_before += 1; }
        }
    }
    assert!(files_before >= 2, "expected >=2 parquet files after INSERT, got {}", files_before);

    // Update a row to move it to another partition (region)
    execute_query(&shared, &format!("UPDATE {} SET region = 'south' WHERE a = 1", table)).await.unwrap();

    // Ensure data stays consistent and files remain present
    let df = { let g = shared.0.lock(); g.read_df(table).unwrap() };
    assert_eq!(df.height(), 2);
}

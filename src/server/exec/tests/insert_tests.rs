use super::super::execute_query;
use crate::storage::{Store, SharedStore};
use serde_json::json;

#[tokio::test]
async fn test_insert_into_regular_table() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    // Create table first
    let create_sql = "CREATE TABLE clarium/public/test_insert";
    let _ = execute_query(&shared, create_sql).await.unwrap();
    
    // Insert data
    let insert_sql = r#"INSERT INTO clarium/public/test_insert (id, name, value) VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.3)"#;
    let result = execute_query(&shared, insert_sql).await.unwrap();
    
    // Verify insert result
    assert_eq!(result["status"], "ok");
    assert_eq!(result["inserted"], 2);
    
    // Read back and verify
    let df = { let g = shared.0.lock(); g.read_df("clarium/public/test_insert").unwrap() };
    assert_eq!(df.height(), 2);
    
    // Check columns
    let id_col = df.column("id").unwrap().f64().unwrap();
    assert_eq!(id_col.get(0), Some(1.0));
    assert_eq!(id_col.get(1), Some(2.0));
    
    let name_col = df.column("name").unwrap().str().unwrap();
    assert_eq!(name_col.get(0), Some("Alice".into()));
    assert_eq!(name_col.get(1), Some("Bob".into()));
    
    let value_col = df.column("value").unwrap().f64().unwrap();
    assert_eq!(value_col.get(0), Some(10.5));
    assert_eq!(value_col.get(1), Some(20.3));
}

#[tokio::test]
async fn test_insert_into_time_table() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    // Use fully qualified path that matches where INSERT will write (with default db/schema)
    let db = "clarium/public/test_metrics.time";
    
    // Create time table directly using store
    store.create_table(db).unwrap();
    
    // Insert data into time table (unqualified name will be qualified to clarium/public)
    let base: i64 = 1_730_000_000_000;
    let insert_sql = format!(
        r#"INSERT INTO test_metrics.time (ID, VALUE, LABEL, CREATED_MS) VALUES ({}, 1.23, 'A', {}), ({}, 4.56, 'B', {}), ({}, 7.89, 'C', {})"#,
        1, base, 2, base + 5000, 3, base + 10000
    );
    let result = execute_query(&shared, &insert_sql).await.unwrap();
    
    // Verify insert result
    assert_eq!(result["status"], "ok");
    assert_eq!(result["inserted"], 3);
    
    // Read back and verify
    let df = { let g = shared.0.lock(); g.read_df(db).unwrap() };
    assert_eq!(df.height(), 3);
    
    // Check _time column
    let time_col = df.column("_time").unwrap().i64().unwrap();
    assert_eq!(time_col.get(0), Some(1));
    assert_eq!(time_col.get(1), Some(2));
    assert_eq!(time_col.get(2), Some(3));
    
    // Check VALUE column
    let value_col = df.column("VALUE").unwrap().f64().unwrap();
    assert_eq!(value_col.get(0), Some(1.23));
    assert_eq!(value_col.get(1), Some(4.56));
    assert_eq!(value_col.get(2), Some(7.89));
    
    // Check LABEL column
    let label_col = df.column("LABEL").unwrap().str().unwrap();
    assert_eq!(label_col.get(0), Some("A".into()));
    assert_eq!(label_col.get(1), Some("B".into()));
    assert_eq!(label_col.get(2), Some("C".into()));
}

#[tokio::test]
async fn test_insert_into_append() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let table = "clarium/public/append_test";
    
    // Create table
    let create_sql = format!("CREATE TABLE {}", table);
    let _ = execute_query(&shared, &create_sql).await.unwrap();
    
    // First insert
    let insert1 = format!(r#"INSERT INTO {} (id, name) VALUES (1, 'First')"#, table);
    let _ = execute_query(&shared, &insert1).await.unwrap();
    
    // Second insert (should append)
    let insert2 = format!(r#"INSERT INTO {} (id, name) VALUES (2, 'Second')"#, table);
    let _ = execute_query(&shared, &insert2).await.unwrap();
    
    // Verify both rows exist
    let df = { let g = shared.0.lock(); g.read_df(table).unwrap() };
    assert_eq!(df.height(), 2);
    
    let id_col = df.column("id").unwrap().f64().unwrap();
    assert_eq!(id_col.get(0), Some(1.0));
    assert_eq!(id_col.get(1), Some(2.0));
}

#[tokio::test]
async fn test_insert_with_null_values() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let table = "clarium/public/null_test";
    
    // Create table
    let create_sql = format!("CREATE TABLE {}", table);
    let _ = execute_query(&shared, &create_sql).await.unwrap();
    
    // Insert with NULL values
    let insert_sql = format!(r#"INSERT INTO {} (id, value, label) VALUES (1, 10.5, 'A'), (2, NULL, 'B'), (3, 30.5, NULL)"#, table);
    let result = execute_query(&shared, &insert_sql).await.unwrap();
    
    // Verify insert result
    assert_eq!(result["status"], "ok");
    assert_eq!(result["inserted"], 3);
    
    // Read back and verify
    let df = { let g = shared.0.lock(); g.read_df(table).unwrap() };
    assert_eq!(df.height(), 3);
    
    // Check value column (should have NULL at index 1)
    let value_col = df.column("value").unwrap().f64().unwrap();
    assert_eq!(value_col.get(0), Some(10.5));
    assert!(value_col.get(1).is_none());
    assert_eq!(value_col.get(2), Some(30.5));
}

#[tokio::test]
async fn test_insert_select_without_column_list() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Create source and target tables
    let src = "clarium/public/src_sel";
    let dst = "clarium/public/dst_sel";
    execute_query(&shared, &format!("CREATE TABLE {}", src)).await.unwrap();
    execute_query(&shared, &format!("CREATE TABLE {}", dst)).await.unwrap();

    // Seed source
    execute_query(&shared, &format!(
        "INSERT INTO {} (id, name, v) VALUES (1, 'A', 10.0), (2, 'B', 20.0), (3, 'C', 30.0)",
        src
    )).await.unwrap();

    // Insert into target from select, filter id >= 2, without column list (all columns assumed)
    let sql = format!("INSERT INTO {} SELECT id, name, v FROM {} WHERE id >= 2", dst, src);
    let res = execute_query(&shared, &sql).await.unwrap();
    assert_eq!(res["status"], json!("ok"));
    assert_eq!(res["inserted"], json!(2));

    let df = { let g = shared.0.lock(); g.read_df(dst).unwrap() };
    assert_eq!(df.height(), 2);
    let ids = df.column("id").unwrap();
    assert!(ids.len() == 2);
}

#[tokio::test]
async fn test_insert_select_with_column_list_and_pk_enforced() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Create source and target tables; target with PK(id)
    let src = "clarium/public/src_sel2";
    let dst = "clarium/public/dst_sel2";
    execute_query(&shared, &format!("CREATE TABLE {}", src)).await.unwrap();
    execute_query(&shared, &format!("CREATE TABLE {}", dst)).await.unwrap();
    store.set_table_metadata(dst, Some(vec!["id".to_string()]), None).unwrap();

    // Seed source with duplicate ids to test PK check later
    execute_query(&shared, &format!(
        "INSERT INTO {} (id, name, v) VALUES (1, 'A', 10.0), (2, 'B', 20.0)", src
    )).await.unwrap();

    // Insert into target selecting subset of columns and mapping order
    let sql_ok = format!("INSERT INTO {} (id, name) SELECT id, name FROM {} WHERE id = 1", dst, src);
    let res_ok = execute_query(&shared, &sql_ok).await.unwrap();
    assert_eq!(res_ok["status"], json!("ok"));
    assert_eq!(res_ok["inserted"], json!(1));

    // Attempt to insert duplicate primary key via SELECT should fail
    let sql_dup = format!("INSERT INTO {} (id, name) SELECT id, name FROM {} WHERE id = 1", dst, src);
    let err = execute_query(&shared, &sql_dup).await.err().expect("expected PK violation");
    assert!(err.to_string().contains("PRIMARY KEY"));
}

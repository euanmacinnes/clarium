use crate::{server::exec, storage::{Store, SharedStore}};

#[test]
fn test_use_affects_view_ddl() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // current-thread runtime to preserve thread-local session defaults between calls
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    // Set session defaults
    rt.block_on(exec::execute_query(&shared, "USE DATABASE mydb"))
        .expect("USE DATABASE ok");
    rt.block_on(exec::execute_query(&shared, "USE SCHEMA s1"))
        .expect("USE SCHEMA ok");

    // Create a time table using unqualified name and write a couple rows
    rt.block_on(exec::execute_query(&shared, "CREATE TIME TABLE src.time"))
        .expect("CREATE TIME TABLE ok");
    // write a couple of rows directly
    let mut recs: Vec<crate::storage::Record> = Vec::new();
    for i in 0..2 {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), serde_json::json!(i as i64));
        recs.push(crate::storage::Record { _time: 1_700_000_000_000 + i as i64, sensors: m });
    }
    store.write_records("mydb/s1/src.time", &recs).unwrap();

    // Create an unqualified view in the current db/schema
    rt.block_on(exec::execute_query(&shared, "CREATE VIEW v1 AS SELECT v FROM src.time"))
        .expect("CREATE VIEW ok");

    // SHOW VIEW should find it via current session defaults
    let show = rt.block_on(exec::execute_query(&shared, "SHOW VIEW v1")).unwrap();
    let def = show.get(0).and_then(|r| r.get("definition")).and_then(|v| v.as_str()).unwrap_or("");
    assert!(def.to_uppercase().contains("SELECT"));

    // Selecting from the unqualified view should work using session defaults
    let sel = match crate::server::query::parse("SELECT v FROM v1 ORDER BY v").unwrap() { crate::server::query::Command::Select(q) => q, _ => unreachable!() };
    let df = crate::server::exec::exec_select::run_select(&shared, &sel).unwrap();
    assert_eq!(df.height(), 2);

    // Drop view unqualified
    rt.block_on(exec::execute_query(&shared, "DROP VIEW v1")).expect("DROP VIEW ok");
}

#[test]
fn test_use_affects_table_ddl() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    rt.block_on(exec::execute_query(&shared, "USE DATABASE db2")).unwrap();
    rt.block_on(exec::execute_query(&shared, "USE SCHEMA sch2")).unwrap();

    // Create an unqualified regular table under db2/sch2
    rt.block_on(exec::execute_query(&shared, "CREATE TABLE t1")).unwrap();

    // Verify directory exists on disk
    let dir = store.root_path().join(format!("db2{}sch2{}t1", std::path::MAIN_SEPARATOR, std::path::MAIN_SEPARATOR));
    assert!(dir.exists(), "expected table directory at {}", dir.display());

    // Verify parquet can be loaded and is empty initially
    let df = { let g = shared.0.lock(); g.read_df("db2/sch2/t1").unwrap() };
    assert_eq!(df.height(), 0, "newly created table should have 0 rows");

    // Drop unqualified
    rt.block_on(exec::execute_query(&shared, "DROP TABLE t1")).unwrap();
    assert!(!dir.exists(), "expected table directory removed at {}", dir.display());
}

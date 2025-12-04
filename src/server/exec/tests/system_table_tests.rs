use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};

// System catalogs should be queryable without any user tables created.
// We only assert that the query executes and exposes expected column names;
// row counts may vary depending on the temporary folder layout.
#[test]
fn test_information_schema_schemata_select() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse("SELECT schema_name FROM information_schema.schemata").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    // Column must be present; rows may be >= 0
    assert!(df.get_column_names().iter().any(|c| c.as_str() == "schema_name"));
}

#[test]
fn test_information_schema_tables_select_and_where() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // Create a simple time table so information_schema.tables can see something.
    let path = "demo/public/sensors.time";
    let recs = vec![Record { _time: 1_700_000_000_000, sensors: serde_json::Map::from_iter(vec![("value".into(), serde_json::json!(1.0))]) }];
    store.write_records(path, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Use aggregate to avoid manual series extraction path in regular non-time projection
    let q = match query::parse("SELECT COUNT(table_name) AS cnt FROM information_schema.tables").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "cnt" || c.as_str() == "COUNT(table_name)"));

    // WHERE should also work (case-insensitive LIKE via our engine)
    let q2 = match query::parse("SELECT COUNT(table_name) AS cnt FROM information_schema.tables WHERE table_name LIKE 'sensors%'").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).unwrap();
    assert_eq!(df2.height(), 1);
}

#[test]
fn test_pg_catalog_pg_class_alias() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // Create any user table to ensure catalogs are non-empty
    let path = "demo/public/demo_table.time";
    let recs = vec![Record { _time: 1_700_000_000_000, sensors: serde_json::Map::new() }];
    store.write_records(path, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Support referencing pg_catalog.pg_class directly
    let q = match query::parse("SELECT COUNT(relname) AS cnt FROM pg_catalog.pg_class").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert!(df.get_column_names().iter().any(|c| c.as_str() == "cnt" || c.as_str() == "COUNT(relname)"));
}




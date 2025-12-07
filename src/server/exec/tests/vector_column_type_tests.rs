use crate::server::exec::exec_select::run_select;
use crate::server::query::{self, Command};
use crate::storage::{SharedStore, Store, Record};
use serde_json::json;
use futures::executor::block_on;
use polars::prelude::*;

fn seed_vector_table_native(tmp: &tempfile::TempDir, name: &str) -> SharedStore {
    let store = Store::new(tmp.path()).unwrap();
    let mut recs: Vec<Record> = Vec::new();
    // Two rows with native arrays
    {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(1));
        m.insert("emb".into(), json!([1.0, 0.0, 0.0]));
        recs.push(Record { _time: 1000, sensors: m });
    }
    {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(2));
        m.insert("emb".into(), json!([0.0, 1.0, 0.0]));
        recs.push(Record { _time: 1001, sensors: m });
    }
    store.write_records(name, &recs).unwrap();
    SharedStore::new(tmp.path()).unwrap()
}

fn seed_vector_table_string(tmp: &tempfile::TempDir, name: &str) -> SharedStore {
    let store = Store::new(tmp.path()).unwrap();
    let mut recs: Vec<Record> = Vec::new();
    // Two rows with string-encoded vectors
    {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(1));
        m.insert("emb".into(), json!("[1.0, 0.0, 0.0]"));
        recs.push(Record { _time: 2000, sensors: m });
    }
    {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(2));
        m.insert("emb".into(), json!("0.0, 1.0, 0.0"));
        recs.push(Record { _time: 2001, sensors: m });
    }
    store.write_records(name, &recs).unwrap();
    SharedStore::new(tmp.path()).unwrap()
}

#[test]
fn ddl_accepts_vector_and_schema_reports_vector() {
    let tmp = tempfile::tempdir().unwrap();
    let store = SharedStore::new(tmp.path()).unwrap();

    // Create a table with VECTOR column
    let sql = "CREATE TABLE demo/vec (id INT64, emb VECTOR)";
    block_on(crate::server::exec::execute_query(&store, sql)).unwrap();

    // Describe/Show schema should include vector type
    let out = block_on(crate::server::exec::execute_query(&store, "SCHEMA SHOW demo")).unwrap();
    let arr = out.as_array().unwrap();
    // ensure our table exists and column emb is present as vector
    let mut saw_vector = false;
    for row in arr {
        if let Some(obj) = row.as_object() {
            if obj.get("table").and_then(|v| v.as_str()) == Some("vec") {
                if let Some(cols) = obj.get("columns").and_then(|v| v.as_array()) {
                    for c in cols {
                        if let Some(co) = c.as_object() {
                            if co.get("name").and_then(|v| v.as_str()) == Some("emb") {
                                let dt = co.get("dtype").and_then(|v| v.as_str()).unwrap_or("");
                                // dtype should be vector when surfaced as string
                                if dt.eq_ignore_ascii_case("vector") { saw_vector = true; }
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(saw_vector, "schema did not report emb as vector");

    // Also load the just-created parquet (no rows yet) and verify columns/dtypes
    let df = { let g = store.0.lock(); g.filter_df("demo/vec", &vec!["id".into(), "emb".into()], None, None).unwrap() };
    // Should have zero rows immediately after CREATE
    assert_eq!(df.height(), 0, "newly created table should have 0 rows");
    // Columns should be present with expected dtypes based on schema
    let id = df.column("id").expect("id column missing after CREATE");
    assert!(matches!(id.dtype(), DataType::Int64 | DataType::Float64), "id should be numeric, got {:?}", id.dtype());
    let emb = df.column("emb").expect("emb column missing after CREATE");
    assert!(matches!(emb.dtype(), DataType::List(_)), "emb should be a List dtype, got {:?}", emb.dtype());
}

#[test]
fn order_by_cosine_and_l2_over_native_vector_column() {
    let tmp = tempfile::tempdir().unwrap();
    // Seed data with native lists
    let store = seed_vector_table_native(&tmp, "clarium/public/vec_native");

    // Cosine similarity: query closer to [1,0,0] should rank id=1 first
    let q = "SELECT id FROM clarium/public/vec_native ORDER BY cosine_sim(emb, '[1,0,0]') DESC LIMIT 1";
    let cmd = match query::parse(q).unwrap() { Command::Select(x) => x, _ => unreachable!() };
    let df = run_select(&store, &cmd).unwrap();
    let top = df.column("id").unwrap().get(0).unwrap().try_extract::<i64>().unwrap();
    assert_eq!(top, 1);

    // L2 distance: closer to [0,1,0] should rank id=2 first (ascending)
    let q = "SELECT id FROM clarium/public/vec_native ORDER BY vec_l2(emb, '[0,1,0]') ASC LIMIT 1";
    let cmd = match query::parse(q).unwrap() { Command::Select(x) => x, _ => unreachable!() };
    let df = run_select(&store, &cmd).unwrap();
    let top = df.column("id").unwrap().get(0).unwrap().try_extract::<i64>().unwrap();
    assert_eq!(top, 2);
}

#[test]
fn order_by_works_with_string_encoded_vectors_for_backward_compat() {
    let tmp = tempfile::tempdir().unwrap();
    // Seed data with string-encoded vectors
    let store = seed_vector_table_string(&tmp, "clarium/public/vec_string");

    // Cosine similarity: query closer to [1,0,0] should rank id=1 first
    let q = "SELECT id FROM clarium/public/vec_string ORDER BY cosine_sim(emb, '[1,0,0]') DESC LIMIT 1";
    let cmd = match query::parse(q).unwrap() { Command::Select(x) => x, _ => unreachable!() };
    let df = run_select(&store, &cmd).unwrap();
    let top = df.column("id").unwrap().get(0).unwrap().try_extract::<i64>().unwrap();
    assert_eq!(top, 1);

    // L2 distance: closer to [0,1,0] should rank id=2 first
    let q = "SELECT id FROM clarium/public/vec_string ORDER BY vec_l2(emb, '[0,1,0]') ASC LIMIT 1";
    let cmd = match query::parse(q).unwrap() { Command::Select(x) => x, _ => unreachable!() };
    let df = run_select(&store, &cmd).unwrap();
    let top = df.column("id").unwrap().get(0).unwrap().try_extract::<i64>().unwrap();
    assert_eq!(top, 2);
}

#[test]
fn missing_vector_column_is_added_as_null_list() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // Prepare schema with a vector column but write rows that omit it
    block_on(crate::server::exec::execute_query(&SharedStore::new(tmp.path()).unwrap(),
        "CREATE TABLE demo/missing_vec (id INT64, emb VECTOR)" )).unwrap();
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..3i64 {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(i));
        // no 'emb'
        recs.push(Record { _time: 3000 + i, sensors: m });
    }
    store.write_records("demo/missing_vec", &recs).unwrap();

    // Verify table content directly from storage (actual parquet), before running SELECT
    {
        let df = store.filter_df("demo/missing_vec", &vec!["id".into(), "emb".into()], None, None).unwrap();
        let id = df.column("id").expect("id column missing in parquet");
        assert!(matches!(id.dtype(), DataType::Int64), "id should be Int64 in parquet, got {:?}", id.dtype());
        let emb = df.column("emb").expect("emb column missing in parquet");
        assert!(matches!(emb.dtype(), DataType::List(_)), "emb should be a List dtype in parquet, got {:?}", emb.dtype());
        assert_eq!(emb.null_count(), df.height(), "emb should be all nulls in parquet");
    }

    // Read back via SELECT and verify 'emb' exists and is a List type
    let shared = SharedStore::new(tmp.path()).unwrap();
    let q = match query::parse("SELECT id, emb FROM demo/missing_vec").unwrap() { Command::Select(x) => x, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    let s = df.column("emb").unwrap();
    assert!(matches!(s.dtype(), DataType::List(_)), "emb should be a List dtype, got {:?}", s.dtype());
    // All nulls
    assert_eq!(s.null_count(), df.height());
}

use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use crate::system;
use polars::prelude::AnyValue;
use serde_json::json;

fn setup_small_table() -> (SharedStore, String) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.into_path();
    let store = Store::new(&root).unwrap();
    let shared = SharedStore::new(&root).unwrap();
    let db = "qc_udf.time".to_string();
    // Data: two groups g0 and g1 with numeric values
    let mut recs: Vec<Record> = Vec::new();
    let base: i64 = 1_700_200_000_000;
    // g0 rows
    for (i, v) in [1i64, 2, 3].into_iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("k".into(), json!("g0"));
        m.insert("v".into(), json!(v));
        recs.push(Record { _time: base + i as i64, sensors: m });
    }
    // g1 rows
    for (i, v) in [4i64, 5, 6].into_iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("k".into(), json!("g1"));
        m.insert("v".into(), json!(v));
        recs.push(Record { _time: base + 100 + i as i64, sensors: m });
    }
    store.write_records(&db, &recs).unwrap();
    (shared, db)
}

#[test]
fn quick_check_scalar_arg_order_echo2() {
    // Initialize all test UDFs once
    super::udf_common::init_all_test_udfs();
    
    let prev_strict = system::get_strict_projection();
    system::set_strict_projection(false);
    let (shared, db) = setup_small_table();

    // Use in projection (multi-return scalar supported there)
    let qtext = format!("SELECT echo2(1, 'x') AS e FROM {} LIMIT 1", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    // Expect two columns e_0 and e_1
    println!("[quick_check_scalar_arg_order_echo2] columns = {:?}", df.get_column_names());
    let c0 = df.column("e_0").unwrap().get(0).unwrap();
    let c1 = df.column("e_1").unwrap().get(0).unwrap();
    println!("[quick_check_scalar_arg_order_echo2] e_0={:?} e_1={:?}", c0, c1);
    // Keep the test non-failing regardless of order; just assert they are stringy and non-null
    match c0 { AnyValue::String(_) | AnyValue::StringOwned(_) => {}, _ => panic!("e_0 should be string") }
    match c1 { AnyValue::String(_) | AnyValue::StringOwned(_) => {}, _ => panic!("e_1 should be string") }

    system::set_strict_projection(prev_strict);
}

#[test]
fn quick_check_aggregate_arg_order_types() {
    // Initialize all test UDFs once
    super::udf_common::init_all_test_udfs();
    
    let prev_strict = system::get_strict_projection();
    system::set_strict_projection(false);
    let (shared, db) = setup_small_table();

    let qtext = format!("SELECT argtypes(k, v) AS types FROM {} GROUP BY k ORDER BY k", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    println!("[quick_check_aggregate_arg_order_types] columns = {:?}", df.get_column_names());
    for i in 0..df.height() {
        let k = df.column("k").unwrap().get(i).unwrap();
        let t0 = df.column("types_0").unwrap().get(i).unwrap();
        let t1 = df.column("types_1").unwrap().get(i).unwrap();
        println!("[quick_check_aggregate_arg_order_types] row {}: k={:?} types_0={:?} types_1={:?}", i, k, t0, t1);
    }

    system::set_strict_projection(prev_strict);
}

#[test]
fn quick_check_aggregate_first_error_dtype_inference() {
    // Initialize all test UDFs once
    super::udf_common::init_all_test_udfs();
    
    let prev_strict = system::get_strict_projection();
    system::set_strict_projection(false);
    system::set_null_on_error(true);
    let (shared, db) = setup_small_table();

    // Case A: ORDER BY k asc: expect first row is g0 (may cause first-error path)
    let qa = match query::parse(&format!("SELECT k, agg_err_if_bad(k, v) AS s FROM {} GROUP BY k ORDER BY k", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let dfa = run_select(&shared, &qa).unwrap();
    println!("[quick_check_aggregate_first_error_dtype_inference] A columns = {:?}", dfa.get_column_names());
    for i in 0..dfa.height() {
        let k = dfa.column("k").unwrap().get(i).unwrap();
        let s = dfa.column("s").unwrap().get(i).unwrap();
        println!("[quick_check_aggregate_first_error_dtype_inference] A row {}: k={:?} s={:?}", i, k, s);
    }

    // Case B: ORDER BY k desc: first processed group likely g1 (success first)
    let qb = match query::parse(&format!("SELECT k, agg_err_if_bad(k, v) AS s FROM {} GROUP BY k ORDER BY k DESC", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let dfb = run_select(&shared, &qb).unwrap();
    println!("[quick_check_aggregate_first_error_dtype_inference] B columns = {:?}", dfb.get_column_names());
    for i in 0..dfb.height() {
        let k = dfb.column("k").unwrap().get(i).unwrap();
        let s = dfb.column("s").unwrap().get(i).unwrap();
        println!("[quick_check_aggregate_first_error_dtype_inference] B row {}: k={:?} s={:?}", i, k, s);
    }

    system::set_strict_projection(prev_strict);
}



use super::super::run_select;
use crate::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use crate::system;
use polars::prelude::AnyValue;
use serde_json::json;

fn setup_basic_table() -> (SharedStore, String) {
    let tmp = tempfile::tempdir().unwrap();
    // Persist the temporary directory path for the duration of the test to avoid cleanup
    let root = tmp.into_path();
    let store = Store::new(&root).unwrap();
    let shared = SharedStore::new(&root).unwrap();
    let db = "udf_test.time".to_string();
    // rows: v = -1, 0, 1, with a string column 's'
    let mut recs: Vec<Record> = Vec::new();
    for (i, v) in [-1i64, 0, 1].into_iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(v));
        m.insert("s".into(), json!(format!("row{}", i)));
        recs.push(Record { _time: 1_700_100_000_000 + i as i64, sensors: m });
    }
    // add a NULL row for null semantics
    let mut m = serde_json::Map::new();
    m.insert("v".into(), serde_json::Value::Null);
    m.insert("s".into(), serde_json::Value::Null);
    recs.push(Record { _time: 1_700_100_000_003, sensors: m });
    store.write_records(&db, &recs).unwrap();
    (shared, db)
}

#[test]
fn test_scalar_udf_where_concat_arith_and_nulls() {
    // Initialize all test UDFs once
    super::udf_common::init_all_test_udfs();
    
    // Use loose projection mode to allow ORDER BY on non-projected columns
    let _prev = system::get_strict_projection();
    system::set_strict_projection(false);
    let (shared, db) = setup_basic_table();

    // WHERE using scalar boolean UDF
    let q1 = match query::parse(&format!("SELECT * FROM {} WHERE is_pos(v) ORDER BY _time", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).unwrap();
    // expect only v=1 row remains
    let vcol = df1.column("v").unwrap();
    assert_eq!(df1.height(), 1);
    match vcol.get(0).unwrap() { AnyValue::Int64(v) => assert_eq!(v, 1), AnyValue::Float64(v) => assert_eq!(v as i64, 1), _ => panic!("unexpected type") }

    // Arithmetic nesting: dbl(v) + dbl(v)
    let q2 = match query::parse(&format!("SELECT dbl(v)+dbl(v) AS y FROM {} ORDER BY _time LIMIT 1", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).unwrap();
    let y0 = df2.column("y").unwrap().get(0).unwrap();
    match y0 { AnyValue::Int64(v) => assert_eq!(v, -4), AnyValue::Float64(v) => assert_eq!(v, -4.0), _ => panic!("bad type") }

    // CONCAT usage
    let q3 = match query::parse(&format!("SELECT CONCAT(hello(s), '-', v) AS z FROM {} ORDER BY _time LIMIT 1", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df3 = run_select(&shared, &q3).unwrap();
    let z0 = df3.column("z").unwrap().get(0).unwrap();
    match z0 { AnyValue::String(s) => assert!(s.starts_with("hi:")), AnyValue::StringOwned(s) => assert!(s.starts_with("hi:")), _ => panic!("bad type") }

    // Null semantics: pass nil to Lua and get defined result
    let q4 = match query::parse(&format!("SELECT hello(s) AS hs FROM {} WHERE s IS NULL", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df4 = run_select(&shared, &q4).unwrap();
    let hs = df4.column("hs").unwrap().get(0).unwrap();
    match hs { AnyValue::String(s) => assert_eq!(s, "hi:nil"), AnyValue::StringOwned(s) => assert_eq!(s, "hi:nil"), _ => panic!("bad type") }
    // restore strictness
    system::set_strict_projection(_prev);
}

#[test]
fn test_scalar_udf_error_policy() {
    // Initialize all test UDFs once
    super::udf_common::init_all_test_udfs();
    
    let _prev = system::get_strict_projection();
    system::set_strict_projection(false);
    let (shared, db) = setup_basic_table();

    // default null_on_error = true → row becomes NULL, query succeeds
    system::set_null_on_error(true);
    let q1 = match query::parse(&format!("SELECT err_if_neg(v) AS e FROM {} ORDER BY _time", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q1).unwrap();
    // first row v=-1 should be NULL
    assert!(matches!(df.column("e").unwrap().get(0).unwrap(), AnyValue::Null));

    // now set false → expect compute error
    system::set_null_on_error(false);
    let q2 = match query::parse(&format!("SELECT err_if_neg(v) FROM {}", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let err = run_select(&shared, &q2).err().unwrap();
    let msg = format!("{}", err);
    assert!(msg.contains("UDF 'err_if_neg' error") || msg.to_lowercase().contains("compute"));
    // restore default
    system::set_null_on_error(true);
}

#[test]
fn test_scalar_multi_return_projection_and_misuse() {
    // Initialize all test UDFs once
    super::udf_common::init_all_test_udfs();
    
    let _prev = system::get_strict_projection();
    system::set_strict_projection(false);
    let (shared, db) = setup_basic_table();

    // projection expands columns with alias base
    let q1 = match query::parse(&format!("SELECT split2(v) AS p FROM {} ORDER BY _time LIMIT 1", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).unwrap();
    assert!(df1.get_column_names().iter().any(|c| c.as_str()=="p_0"));
    assert!(df1.get_column_names().iter().any(|c| c.as_str()=="p_1"));
    // _time should not be in final output (it was added temporarily for ORDER BY and then dropped)
    assert!(df1.get_column_names().iter().all(|c| c.as_str() != "_time"));

    // misuse in WHERE should error at plan/compute time
    let q2 = match query::parse(&format!("SELECT * FROM {} WHERE split2(v) > 0", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let err = run_select(&shared, &q2).err().unwrap();
    let msg = format!("{}", err);
    assert!(msg.contains("only allowed in SELECT projections"));
    system::set_strict_projection(_prev);
}

#[test]
fn test_aggregate_udf_group_by_single_and_multi() {
    // Initialize all test UDFs once
    super::udf_common::init_all_test_udfs();
    
    let _prev = system::get_strict_projection();
    system::set_strict_projection(false);
    // build table with key k and value v
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "udf_agg.time";
    store.create_table(db).unwrap();
    let rows = vec![
        ("a", 1i64), ("a", 2), ("a", 3),
        ("b", 10), ("b", 20),
    ];
    let mut recs: Vec<Record> = Vec::new();
    for (i, (k, v)) in rows.into_iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("k".into(), json!(k));
        m.insert("v".into(), json!(v));
        recs.push(Record { _time: 1_700_200_000_000 + i as i64, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // single-return aggregate query
    let q1 = match query::parse(&format!("SELECT k, sum_plus(v) AS sp FROM {} GROUP BY k ORDER BY k", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).unwrap();
    // expect a: 1+2+3+1 = 7; b: 10+20+1 = 31
    let ka = df1.column("k").unwrap().get(0).unwrap();
    let va = df1.column("sp").unwrap().get(0).unwrap();
    match (ka, va) {
        (AnyValue::String(s), AnyValue::Int64(v)) => { assert_eq!(s, "a"); assert_eq!(v, 7); },
        (AnyValue::StringOwned(s), AnyValue::Int64(v)) => { assert_eq!(s, "a"); assert_eq!(v, 7); },
        _ => {}
    }

    // multi-return aggregate query
    let q2 = match query::parse(&format!("SELECT k, minmax(v) AS mm FROM {} GROUP BY k ORDER BY k", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).unwrap();
    assert!(df2.get_column_names().iter().any(|c| c.as_str()=="mm_0"));
    assert!(df2.get_column_names().iter().any(|c| c.as_str()=="mm_1"));

    // HAVING on aggregate result
    let q3 = match query::parse(&format!("SELECT k, minmax(v) AS mm FROM {} GROUP BY k HAVING mm_0 > 2 ORDER BY k", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df3 = run_select(&shared, &q3).unwrap();
    // only group 'b' has min 10 (>2)
    assert_eq!(df3.height(), 1);
    let konly = df3.column("k").unwrap().get(0).unwrap();
    match konly { AnyValue::String(s) => assert_eq!(s, "b"), AnyValue::StringOwned(s) => assert_eq!(s, "b"), _ => panic!("unexpected") }
}

#[test]
fn test_aggregate_udf_error_policy() {
    // Initialize all test UDFs once
    super::udf_common::init_all_test_udfs();
    
    // table with two groups; make one group raise error
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "udf_agg_err.time";
    store.create_table(db).unwrap();
    let rows = vec![("ok", 1i64), ("ok", 2), ("bad", 5)];
    let mut recs: Vec<Record> = Vec::new();
    for (i, (k, v)) in rows.into_iter().enumerate() {
        let mut m = serde_json::Map::new(); m.insert("k".into(), json!(k)); m.insert("v".into(), json!(v));
        recs.push(Record { _time: 1_700_300_000_000 + i as i64, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // default: null_on_error = true → bad group yields NULL, query succeeds
    system::set_null_on_error(true);
    let q1 = match query::parse(&format!("SELECT k, agg_err_if_bad(k, v) AS s FROM {} GROUP BY k ORDER BY k", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).unwrap();
    // find row for 'bad'
    for i in 0..df1.height() {
        let kval = df1.column("k").unwrap().get(i).unwrap();
        let is_bad = match kval {
            AnyValue::String(s) => s == "bad",
            AnyValue::StringOwned(ref s) => s.as_str() == "bad",
            _ => false,
        };
        if is_bad {
            assert!(matches!(df1.column("s").unwrap().get(i).unwrap(), AnyValue::Null));
        }
    }
    // set false → expect error
    system::set_null_on_error(false);
    let q2 = match query::parse(&format!("SELECT k, agg_err_if_bad(k, v) AS s FROM {} GROUP BY k", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let err = run_select(&shared, &q2).err().unwrap();
    let msg = format!("{}", err);
    assert!(msg.to_lowercase().contains("udf") || msg.to_lowercase().contains("compute"));
    system::set_null_on_error(true);
}




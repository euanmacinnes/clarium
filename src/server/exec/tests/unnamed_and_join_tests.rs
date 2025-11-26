use super::super::run_select;
use crate::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

#[test]
fn test_select_unnamed_expression_and_order_by_it() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_unnamed";
    // seed regular table with column x
    let mut recs: Vec<Record> = Vec::new();
    for i in [3,1,2] {
        let mut m = serde_json::Map::new();
        m.insert("x".into(), json!(i as i64));
        recs.push(Record { _time: i as i64, sensors: m });
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // SELECT an expression without alias -> expect Unnamed_1; ORDER BY that name should work
    let qtext = format!("SELECT x+1 FROM {} ORDER BY Unnamed_1", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    // Column should be named Unnamed_1 and sorted ascending
    // Note: arithmetic operations coerce to f64
    assert!(df.get_column_names().iter().any(|c| c.as_str()=="Unnamed_1"));
    let c = df.column("Unnamed_1").unwrap().f64().unwrap();
    let vals: Vec<f64> = (0..df.height()).map(|i| c.get(i).unwrap()).collect();
    assert_eq!(vals, vec![2.0, 3.0, 4.0]);
}

#[test]
fn test_simple_inner_join_projection() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // left table a: id, la
    let mut ra: Vec<Record> = Vec::new();
    for (i, v) in [(1,10i64),(2,20)].iter() {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(*i));
        m.insert("la".into(), json!(*v));
        ra.push(Record { _time: *i as i64, sensors: m });
    }
    store.write_records("a", &ra).unwrap();
    // right table b: id, rb
    let mut rb: Vec<Record> = Vec::new();
    for (i, v) in [(1,100i64),(2,200)].iter() {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(*i));
        m.insert("rb".into(), json!(*v));
        rb.push(Record { _time: *i as i64, sensors: m });
    }
    store.write_records("b", &rb).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Join with aliases and project qualified columns
    let qtext = "SELECT a.id, b.rb FROM a AS a INNER JOIN b AS b ON a.id = b.id";
    let q = match query::parse(qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    // Expect alias-prefixed column names to be present in the result if selected as qualified
    assert!(df.get_column_names().iter().any(|c| c.as_str()=="a.id"));
    assert!(df.get_column_names().iter().any(|c| c.as_str()=="b.rb"));
    assert_eq!(df.height(), 2);
}




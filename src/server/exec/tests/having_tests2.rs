use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

fn seed_table(tmp: &tempfile::TempDir, name: &str, vals: &[f64]) -> SharedStore {
    let store = Store::new(tmp.path()).unwrap();
    let mut recs: Vec<Record> = Vec::new();
    for (i, v) in vals.iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(*v));
        recs.push(Record { _time: i as i64, sensors: m });
    }
    store.write_records(name, &recs).unwrap();
    SharedStore::new(tmp.path()).unwrap()
}

#[test]
fn test_having_accepts_select_alias() {
    let tmp = tempfile::tempdir().unwrap();
    let db = "db_having_alias";
    let shared = seed_table(&tmp, db, &[2.0, 4.0]); // AVG=3.0

    // Use SELECT alias in HAVING
    let qtext = format!("SELECT AVG(v) AS avgv FROM {} HAVING avgv > 2", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    // Single aggregated row should pass the HAVING filter
    assert_eq!(df.height(), 1);
    assert!(df.get_column_names().iter().any(|c| c.as_str()=="avgv"));
}

#[test]
fn test_having_accepts_derived_name_without_alias() {
    let tmp = tempfile::tempdir().unwrap();
    let db = "db_having_derived";
    let shared = seed_table(&tmp, db, &[0.0, 1.0, 2.0, 3.0]); // AVG=1.5

    // Reference the deterministic derived name AVG(v) directly in HAVING
    let qtext = format!("SELECT AVG(v) FROM {} HAVING AVG(v) >= 1.5", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 1);
    assert!(df.get_column_names().iter().any(|c| c.as_str()=="AVG(v)"));
}

#[test]
fn test_having_filters_out_when_condition_false() {
    let tmp = tempfile::tempdir().unwrap();
    let db = "db_having_false";
    let shared = seed_table(&tmp, db, &[0.0, 1.0, 2.0, 3.0]); // AVG=1.5

    // Condition false -> expect empty result
    let qtext = format!("SELECT AVG(v) AS av FROM {} HAVING av > 10", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 0);
}




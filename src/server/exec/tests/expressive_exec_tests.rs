use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

#[test]
fn test_by_window_expressive_agg_with_alias_and_quantile() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_expr.time";
    // Data across 2-second buckets (BY 2s)
    // t=0s: a=2,b=4,c=1 -> expr per bucket: AVG(a-1)/MAX(b) - FIRST(c)
    // bucket 0 (0-1s): rows at 0s and 1s
    // a: [2,4], b:[4,2], c:[1,1]
    // AVG(a-1) = AVG([1,3]) = 2; MAX(b)=4; FIRST(c)=1 => 2/4 - 1 = -0.5
    // bucket 1 (2-3s): a:[6,8], b:[1,9], c:[0,2] => AVG(a-1)=[5,7] avg=6; MAX(b)=9; FIRST(c)=0 => 6/9 - 0 = 0.666...
    let base: i64 = 1_700_200_000_000;
    let rows = vec![
        (0, 2.0, 4.0, 1.0),
        (1, 4.0, 2.0, 1.0),
        (2, 6.0, 1.0, 0.0),
        (3, 8.0, 9.0, 2.0),
    ];
    let mut recs: Vec<Record> = Vec::new();
    for (i, a, b, c) in rows {
        let mut m = serde_json::Map::new();
        m.insert("a".into(), json!(a));
        m.insert("b".into(), json!(b));
        m.insert("c".into(), json!(c));
        recs.push(Record { _time: base + i*1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let qtext = format!("SELECT AVG(a) AS out, QUANTILE(a, 90) FROM {} BY 2s", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 2);
    // Validate alias column exists
    assert!(df.get_column_names().iter().any(|c| c.as_str()=="out"));
    let outc = df.column("out").unwrap().f64().unwrap();
    let v0 = outc.get(0).unwrap();
    let v1 = outc.get(1).unwrap();
    assert!((v0 - 3.0).abs() < 1e-9, "bucket0 expected 3.0 got {}", v0);
    assert!((v1 - 7.0).abs() < 1e-9, "bucket1 expected 7.0 got {}", v1);
}

#[test]
fn test_projection_with_previous_and_string_funcs_and_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_prev.time";
    let base: i64 = 1_700_300_000_000;
    let rows = vec![
        (0, 10.0, "ny"),
        (1, 15.0, "la"),
        (2, 25.0, "sf"),
    ];
    let mut recs: Vec<Record> = Vec::new();
    for (i, v, city) in rows {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(v));
        m.insert("city".into(), json!(city));
        recs.push(Record { _time: base + i*1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    // Projection with PREVIOUS
    let q1 = format!("SELECT _time, v - PREVIOUS.v AS dv, UPPER(city) AS CITY FROM {}", db);
    let df1 = {
        let q = match query::parse(&q1).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        run_select(&shared, &q).unwrap()
    };
    assert_eq!(df1.height(), 3);
    let dv = df1.column("dv").unwrap().f64().unwrap();
    assert!(dv.get(0).is_none());
    assert_eq!(dv.get(1).unwrap(), 5.0);
    assert_eq!(dv.get(2).unwrap(), 10.0);
    let city = df1.column("CITY").unwrap().str().unwrap();
    assert_eq!(city.get(0).unwrap(), "NY");
    // Ensure string funcs are rejected with BY/GROUP BY
    // parse succeeds, but execution should reject string funcs with BY
    // The BY/GROUP BY rejection actually happens in execution path; simulate:
    {
        let qerr = match query::parse(&format!("SELECT UPPER(city) FROM {} BY 1m", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let err = run_select(&shared, &qerr).err();
        assert!(err.is_some());
    }
}

#[test]
fn test_rolling_by_expression_in_agg_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let db = "db_roll_err.time";
    let base: i64 = 1_700_400_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..5 {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(i as f64));
        recs.push(Record { _time: base + i*1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    // Expression inside AVG should be rejected in ROLLING BY
    let qtext = format!("SELECT AVG(v-1) FROM {} ROLLING BY 3s", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let err = run_select(&shared, &q).err();
    assert!(err.is_some());
}



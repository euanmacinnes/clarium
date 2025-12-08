use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use polars::prelude::AnyValue;
use serde_json::json;

fn make_series_55(base_ms: i64) -> Vec<Record> {
    // 55 rows, 1 second apart. Columns: v = 1..55 (i64), neg = -v
    let mut recs: Vec<Record> = Vec::with_capacity(55);
    for i in 0..55i64 {
        let mut m = serde_json::Map::new();
        let v = i + 1; // 1..55
        m.insert("v".into(), json!(v));
        m.insert("neg".into(), json!(-v));
        recs.push(Record { _time: base_ms + i * 1000, sensors: m });
    }
    recs
}

#[test]
fn test_aggregates_55_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    super::udf_common::init_all_test_udfs();
    let db = "calc/basic55.time";
    let base: i64 = 1_700_200_000_000; // divisible by 10_000 → aligns for later BY checks too
    let recs = make_series_55(base);
    store.write_records(db, &recs).unwrap();

    // COUNT, SUM, AVG, MIN, MAX, SUM(ABS(neg))
    let qtext = format!(
        "SELECT COUNT(v) AS c, SUM(v) AS s, AVG(v) AS a, MIN(v) AS mn, MAX(v) AS mx, SUM(ABS(neg)) AS sabs FROM {}",
        db
    );
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 1);
    // Known values for 1..55
    let sum_expected: i64 = 55 * 56 / 2; // 1540
    // COUNT
    let c = df.column("c").or_else(|_| df.column("COUNT(v)"))
        .unwrap().get(0).unwrap();
    match c { AnyValue::Int64(n) => assert_eq!(n, 55), AnyValue::UInt32(n) => assert_eq!(n as i64, 55), AnyValue::UInt64(n) => assert_eq!(n as i64, 55), _ => panic!("bad COUNT type") }
    // SUM
    let s = df.column("s").or_else(|_| df.column("SUM(v)"))
        .unwrap().get(0).unwrap();
    match s { AnyValue::Int64(n) => assert_eq!(n, sum_expected), AnyValue::Float64(x) => assert!((x - sum_expected as f64).abs() < 1e-9), _ => panic!("bad SUM type") }
    // AVG
    let a = df.column("a").or_else(|_| df.column("AVG(v)"))
        .unwrap().get(0).unwrap();
    match a { AnyValue::Float64(x) => assert!((x - (sum_expected as f64 / 55.0)).abs() < 1e-9), AnyValue::Int64(n) => assert_eq!(n as f64, sum_expected as f64 / 55.0), _ => panic!("bad AVG type") }
    // MIN
    let mn = df.column("mn").or_else(|_| df.column("MIN(v)"))
        .unwrap().get(0).unwrap();
    match mn { AnyValue::Int64(n) => assert_eq!(n, 1), AnyValue::Float64(x) => assert_eq!(x as i64, 1), _ => panic!("bad MIN type") }
    // MAX
    let mx = df.column("mx").or_else(|_| df.column("MAX(v)"))
        .unwrap().get(0).unwrap();
    match mx { AnyValue::Int64(n) => assert_eq!(n, 55), AnyValue::Float64(x) => assert_eq!(x as i64, 55), _ => panic!("bad MAX type") }
    // SUM(ABS(neg)) == SUM(v)
    let sabs = df.column("sabs").or_else(|_| df.column("SUM(ABS(neg))"))
        .unwrap().get(0).unwrap();
    match sabs { AnyValue::Int64(n) => assert_eq!(n, sum_expected), AnyValue::Float64(x) => assert!((x - sum_expected as f64).abs() < 1e-9), _ => panic!("bad SUM(ABS) type") }
}

#[test]
fn test_time_by_10s_aggregates_55_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    super::udf_common::init_all_test_udfs();
    let db = "calc/by10s55.time";
    let base: i64 = 1_700_210_000_000; // aligned to 10s windows
    let recs = make_series_55(base);
    store.write_records(db, &recs).unwrap();

    let qtext = format!("SELECT SUM(v) AS s FROM {} BY 10s ORDER BY _time", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    // 55 seconds, 10s windows → 6 windows
    assert_eq!(df.height(), 6);
    // Expected sums: [1..10]=55, [11..20]=155, [21..30]=255, [31..40]=355, [41..50]=455, [51..55]=265
    let expected = [55i64, 155, 255, 355, 455, 265];
    let sname = if df.get_column_names().iter().any(|c| c.as_str()=="s") { "s" } else { "SUM(v)" };
    let col = df.column(sname).unwrap();
    for i in 0..expected.len() {
        let v = col.get(i).unwrap();
        match v { AnyValue::Int64(n) => assert_eq!(n, expected[i], "win {}", i), AnyValue::Float64(x) => assert!((x - expected[i] as f64).abs() < 1e-9, "win {}", i), _ => panic!("bad type") }
    }
    // Total consistency
    let total: f64 = (0..col.len()).map(|i| match col.get(i).unwrap() { AnyValue::Int64(n) => n as f64, AnyValue::Float64(x) => x, _ => 0.0 }).sum();
    assert!((total - 1540.0).abs() < 1e-9);
}

#[test]
fn test_rolling_by_5s_avg_55_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    super::udf_common::init_all_test_udfs();
    let db = "calc/roll5s55.time";
    let base: i64 = 1_700_220_000_000; // aligned
    let recs = make_series_55(base);
    store.write_records(db, &recs).unwrap();

    let qtext = format!("SELECT AVG(v) FROM {} ROLLING BY 5s ORDER BY _time", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 55);
    let cname = if df.get_column_names().iter().any(|c| c.as_str()=="AVG(v)") { "AVG(v)" } else { "avg" };
    let col = df.column(cname).unwrap().f64().unwrap();

    // Helper to compute expected simple trailing mean of width up to 5 over 1..55
    let expect_at = |i: usize| -> f64 {
        let start = if i+1 >= 5 { i+1-5 } else { 0 };
        let count = i - start + 1;
        let sum: i64 = ((start as i64)+1..=(i as i64)+1).sum();
        sum as f64 / count as f64
    };

    for &i in &[0usize, 3, 4, 5, 10, 54] {
        let got = col.get(i).unwrap();
        let exp = expect_at(i);
        assert!((got - exp).abs() < 1e-9, "idx {} expected {} got {}", i, exp, got);
    }
}

#[test]
fn test_scalar_udf_results_55_rows() {
    // Initialize all test UDFs once (dbl, is_pos, etc.)
    super::udf_common::init_all_test_udfs();

    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "calc/scalars55.time";
    let base: i64 = 1_700_230_000_000;
    let recs = make_series_55(base);
    store.write_records(db, &recs).unwrap();

    // SUM(dbl(v)) should be 2 * SUM(v) = 3080
    let q1 = match query::parse(&format!("SELECT SUM(dbl(v)) AS sd FROM {}", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).unwrap();
    let sd = df1.column("sd").or_else(|_| df1.column("SUM(dbl(v))")).unwrap().get(0).unwrap();
    match sd { AnyValue::Int64(n) => assert_eq!(n, 3080), AnyValue::Float64(x) => assert!((x - 3080.0).abs() < 1e-9), _ => panic!("bad type") }

    // COUNT WHERE is_pos(v) should be all 55 rows
    let q2 = match query::parse(&format!("SELECT COUNT(v) AS c FROM {} WHERE is_pos(v)", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).unwrap();
    let c = df2.column("c").or_else(|_| df2.column("COUNT(v)"))
        .unwrap().get(0).unwrap();
    match c { AnyValue::Int64(n) => assert_eq!(n, 55), AnyValue::UInt32(n) => assert_eq!(n as i64, 55), AnyValue::UInt64(n) => assert_eq!(n as i64, 55), _ => panic!("bad COUNT type") }

    // Built-in ABS on neg column matches SUM(v)
    let q3 = match query::parse(&format!("SELECT SUM(ABS(neg)) AS sabs FROM {}", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df3 = run_select(&shared, &q3).unwrap();
    let sabs = df3.column("sabs").or_else(|_| df3.column("SUM(ABS(neg))")).unwrap().get(0).unwrap();
    match sabs { AnyValue::Int64(n) => assert_eq!(n, 1540), AnyValue::Float64(x) => assert!((x - 1540.0).abs() < 1e-9), _ => panic!("bad type") }
}

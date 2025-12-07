use crate::server::exec::select_stages::from_where::from_where as stage_from_where;
use crate::server::query::{self, Command};
use crate::server::data_context::DataContext;
use crate::storage::{Store, SharedStore};
use polars::prelude::*;

fn make_regular_table(store: &Store, path: &str) {
    store.create_table(path).unwrap();
    let ids = Series::new("id".into(), vec![1i64, 2, 3]);
    let names = Series::new("name".into(), vec!["a", "b", "c"]);
    let df = DataFrame::new(vec![ids.into(), names.into()]).unwrap();
    store.rewrite_table_df(path, df).unwrap();
}

#[test]
fn unqualified_regular_table_uses_current_defaults() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    make_regular_table(&store, "prod/sales/people");

    // FROM people should resolve to prod/sales/people under defaults
    let qtxt = "SELECT id FROM people";
    let q = match query::parse(qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let mut ctx = DataContext::with_defaults("prod", "sales");
    let df = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    // Column names should be prefixed with fully-resolved path
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.starts_with("prod/sales/people.")));
}

#[test]
fn partially_qualified_time_table_uses_current_db() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    // Place table under acme/metrics
    let db = "acme"; let sc = "metrics"; let name = "s1.time";
    let full = format!("{}/{}/{}", db, sc, name);
    // Write a tiny time table
    let mut recs: Vec<crate::storage::Record> = Vec::new();
    let base: i64 = 1_900_222_000_000;
    for i in 0..2i64 { 
        let mut m = serde_json::Map::new(); m.insert("v".into(), serde_json::json!(i));
        recs.push(crate::storage::Record { _time: base + i*1000, sensors: m });
    }
    store.write_records(&full, &recs).unwrap();

    // Use schema-qualified only; current db should be applied
    let qtxt = "SELECT _time FROM metrics/s1.time";
    let q = match query::parse(qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let mut ctx = DataContext::with_defaults("acme", "public");
    let df = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.starts_with("acme/metrics/s1.time.")));
}

use crate::server::data_context::{DataContext, SelectStage};
use crate::server::exec::select_stages::from_where::from_where as stage_from_where;
use crate::server::query::{self, Command};
use crate::storage::{SharedStore, Store, Record};
use polars::prelude::*;
use serde_json::json;

fn make_time_table(store: &Store, path: &str, base: i64, vals: &[i64]) {
    let mut recs: Vec<Record> = Vec::with_capacity(vals.len());
    for (i, v) in vals.iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(*v));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(path, &recs).unwrap();
}

fn make_regular_table(store: &Store, path: &str, ids: &[i64], names: &[&str]) {
    store.create_table(path).unwrap();
    let s_id = Series::new("id".into(), ids.to_vec());
    let s_name = Series::new("name".into(), names.to_vec());
    let df = DataFrame::new(vec![s_id.into(), s_name.into()]).unwrap();
    store.rewrite_table_df(path, df).unwrap();
}

#[test]
fn single_source_unqualified_resolution_time_table() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let tpath = "clarium/public/s1.time";
    make_time_table(&store, tpath, 1_900_000_000_000, &[1,2,3]);

    let qtxt = format!("SELECT _time, v FROM {}", tpath);
    let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let mut ctx = DataContext::with_defaults("clarium", "public");
    let df = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    // Register for stage visibility
    ctx.register_df_columns_for_stage(SelectStage::FromWhere, &df);

    // Unqualified resolution should work by unique suffix
    let c_time = ctx.resolve_column(&df, "_time").unwrap();
    let c_v = ctx.resolve_column(&df, "v").unwrap();
    assert!(c_time.ends_with("._time"), "{}", c_time);
    assert!(c_v.ends_with(".v"), "{}", c_v);

    // Fully-qualified column should resolve exactly
    let fq_v = format!("{}.v", tpath);
    let c_v2 = ctx.resolve_column(&df, &fq_v).unwrap();
    assert_eq!(c_v2, fq_v);

    // Partially qualified (table.col) also resolves via suffix
    let pq_v = "s1.time.v";
    let c_v3 = ctx.resolve_column(&df, pq_v).unwrap();
    assert!(c_v3.ends_with(".v"));
}

#[test]
fn alias_qualified_resolution() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let tpath = "clarium/public/a1.time";
    make_time_table(&store, tpath, 1_800_000_000_000, &[10,11]);

    let qtxt = "SELECT a._time, a.v FROM clarium.public.a1.time a";
    let q = match query::parse(qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let mut ctx = DataContext::with_defaults("clarium", "public");
    let df = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    // alias should be known and columns prefixed with 'a.'
    assert!(df.get_column_names().iter().all(|c| c.starts_with("a.")));
    let cv = ctx.resolve_column(&df, "a.v").unwrap();
    assert_eq!(cv, "a.v");
    // Unqualified should also succeed uniquely
    let cu = ctx.resolve_column(&df, "v").unwrap();
    assert_eq!(cu, "a.v");
}

#[test]
fn fully_and_partially_qualified_columns_with_alias_present() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let tpath = "clarium/public/p1.time";
    make_time_table(&store, tpath, 1_700_000_000_000, &[1,2]);
    let qtxt = "SELECT a.v FROM clarium.public.p1.time a";
    let q = match query::parse(qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let mut ctx = DataContext::with_defaults("clarium", "public");
    let df = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    // Fully qualified original path won't exist exactly because alias is used; suffix-based should pick alias
    let fq = format!("{}{}", tpath, ".v");
    let picked = ctx.resolve_column(&df, &fq).unwrap();
    assert_eq!(picked, "a.v");
    // Partially qualified
    let picked2 = ctx.resolve_column(&df, "p1.time.v").unwrap();
    assert_eq!(picked2, "a.v");
}

#[test]
fn ambiguous_unqualified_column_errors_in_join() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let left = "clarium/public/jl.time";
    let right = "clarium/public/jr.time";
    make_time_table(&store, left, 1_800_010_000_000, &[1,2]);
    make_time_table(&store, right, 1_800_010_000_000, &[10,20]);
    let qtxt = "SELECT l.v, r.v FROM clarium.public.jl.time l INNER JOIN clarium.public.jr.time r ON l._time = r._time";
    let q = match query::parse(qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let mut ctx = DataContext::with_defaults("clarium", "public");
    let df = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    // Attempting to resolve unqualified 'v' must be ambiguous
    let err = ctx.resolve_column(&df, "v").err().unwrap();
    let msg = format!("{}", err);
    assert!(msg.to_lowercase().contains("ambiguous"));
}

#[test]
fn defaults_scope_disambiguates_between_db_matches() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let t1 = "clarium/public/r1"; // regular
    let t2 = "prod/public/r1";
    make_regular_table(&store, t1, &[1,2], &["a","b"]);
    make_regular_table(&store, t2, &[3,4], &["c","d"]);
    let qtxt = "SELECT a.id, b.id FROM clarium.public.r1 a INNER JOIN prod.public.r1 b ON a.id = b.id";
    let q = match query::parse(qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    // Set defaults to clarium/public so resolution should prefer that scope
    let mut ctx = DataContext::with_defaults("clarium", "public");
    let df = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    // Both a.id and b.id exist; resolving plain 'id' should stay ambiguous at this point because alias-known branch scopes by alias.
    // But when we restrict to default scope via suffix disambiguation, resolve_column should pick the one in defaults only if one matches.
    let res = ctx.resolve_column(&df, "a.id").unwrap();
    assert_eq!(res, "a.id");
    let res2 = ctx.resolve_column(&df, "b.id").unwrap();
    assert_eq!(res2, "b.id");
    // Unqualified remains ambiguous
    assert!(ctx.resolve_column(&df, "id").is_err());
}

#[test]
fn resolve_column_at_stage_uses_single_prefix_fast_path() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let tpath = "clarium/public/sfast.time";
    make_time_table(&store, tpath, 1_700_000_100_000, &[1]);
    let qtxt = format!("SELECT _time, v FROM {}", tpath);
    let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let mut ctx = DataContext::with_defaults("clarium", "public");
    let df = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    ctx.register_df_columns_for_stage(SelectStage::FromWhere, &df);
    let r = ctx.resolve_column_at_stage(&df, "v", SelectStage::FromWhere).unwrap();
    assert!(r.ends_with(".v"));
}

#[test]
fn resolve_column_not_found_gives_helpful_message() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let tpath = "clarium/public/miss.time";
    make_time_table(&store, tpath, 1_900_100_000_000, &[5]);
    let qtxt = format!("SELECT _time FROM {}", tpath);
    let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let mut ctx = DataContext::with_defaults("clarium", "public");
    let df = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    let err = ctx.resolve_column(&df, "does_not_exist").err().unwrap();
    let msg = format!("{}", err);
    assert!(msg.to_lowercase().contains("not found"));
}

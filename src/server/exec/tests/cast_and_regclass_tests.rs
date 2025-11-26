use crate::query::{self, Command};
use crate::server::exec::run_select;
use crate::storage::{Store, SharedStore};

#[test]
fn cast_basic_types_in_select() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // '1'::int
    let q = match query::parse("SELECT '1'::int AS v").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 1);
    let v_any = df.column("v").unwrap().as_materialized_series().get(0).unwrap();
    match v_any { polars::prelude::AnyValue::Int64(i) => assert_eq!(i, 1), other => panic!("expected Int64=1, got {:?}", other) }

    // 1::text
    let q = match query::parse("SELECT 1::text AS s").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    let s_any = df.column("s").unwrap().as_materialized_series().get(0).unwrap();
    match s_any { polars::prelude::AnyValue::String(s) => assert_eq!(s, "1"), polars::prelude::AnyValue::StringOwned(s) => assert_eq!(s.as_str(), "1"), other => panic!("expected String '1', got {:?}", other) }

    // '3.14'::double precision
    let q = match query::parse("SELECT '3.14'::double precision AS f").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    let f_any = df.column("f").unwrap().as_materialized_series().get(0).unwrap();
    match f_any { polars::prelude::AnyValue::Float64(f) => assert!((f - 3.14).abs() < 1e-9), other => panic!("expected Float64 ~3.14, got {:?}", other) }
}

#[test]
fn cast_chaining_and_function_call_cast() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // chain: '1'::int::text
    let q = match query::parse("SELECT '1'::int::text AS v").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    let v_any = df.column("v").unwrap().as_materialized_series().get(0).unwrap();
    match v_any { polars::prelude::AnyValue::String(s) => assert_eq!(s, "1"), polars::prelude::AnyValue::StringOwned(s) => assert_eq!(s.as_str(), "1"), other => panic!("expected String '1', got {:?}", other) }

    // function call: version()::text (already string but should parse/execute)
    let q = match query::parse("SELECT pg_catalog.version()::text AS v").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 1);
}

#[test]
fn regclass_cast_system_and_stable_hash() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // System catalogs with fixed OIDs
    let q = match query::parse("SELECT 'pg_class'::regclass AS oid").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    let v = df.column("oid").unwrap().as_materialized_series().get(0).unwrap();
    match v { polars::prelude::AnyValue::Int32(i) => assert_eq!(i, 1259), other => panic!("expected Int32 1259, got {:?}", other) }

    let q = match query::parse("SELECT 'pg_catalog.pg_type'::regclass AS oid").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    let v = df.column("oid").unwrap().as_materialized_series().get(0).unwrap();
    match v { polars::prelude::AnyValue::Int32(i) => assert_eq!(i, 1247), other => panic!("expected Int32 1247, got {:?}", other) }

    // Arbitrary relation name: deterministic, non-zero, and consistent within row
    let q = match query::parse("SELECT 'demo.public.my_tbl'::regclass AS a, 'demo.public.my_tbl'::regclass AS b").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    let a = df.column("a").unwrap().as_materialized_series().get(0).unwrap();
    let b = df.column("b").unwrap().as_materialized_series().get(0).unwrap();
    let (ai, bi) = match (a, b) {
        (polars::prelude::AnyValue::Int32(ai), polars::prelude::AnyValue::Int32(bi)) => (ai, bi),
        other => panic!("expected Int32 pair, got {:?}", other)
    };
    assert_ne!(ai, 0);
    assert_eq!(ai, bi);
}

use crate::query::{self, Command};
use crate::server::exec::run_select;
use crate::storage::{Store, SharedStore};

// Follow-up tests for:
// - Parenthesized casts: (expr)::type with chaining
// - Casts used in WHERE / JOIN / HAVING clauses

#[test]
fn parenthesized_cast_and_chaining() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // (1+2)::int::text -> "3"
    let q = match query::parse("SELECT (1 + 2)::int::text AS s").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 1);
    let v_any = df.column("s").unwrap().as_materialized_series().get(0).unwrap();
    match v_any {
        polars::prelude::AnyValue::String(s) => assert_eq!(s, "3"),
        polars::prelude::AnyValue::StringOwned(s) => assert_eq!(s.as_str(), "3"),
        other => panic!("expected String '3', got {:?}", other),
    }
}

#[test]
fn casts_in_where_clause_true_and_false() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Use a small system table as FROM source and filter via WHERE using casts
    let q_true = match query::parse(
        "SELECT nspname FROM pg_namespace WHERE ('1'::int)::text = '1'"
    ).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df_true = run_select(&shared, &q_true).unwrap();
    assert!(df_true.height() >= 1, "expected some rows when WHERE predicate is true");

    // False predicate yields 0 rows
    let q_false = match query::parse(
        "SELECT nspname FROM pg_namespace WHERE ('0'::int) > 1"
    ).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df_false = run_select(&shared, &q_false).unwrap();
    assert_eq!(df_false.height(), 0, "expected no rows when WHERE predicate is false");
}

#[test]
fn regclass_cast_in_where_and_join() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // WHERE regclass equality should work and return at least one row from pg_type
    let qw = match query::parse(
        "SELECT typname FROM pg_type WHERE 'demo.public.my_tbl'::regclass = 'demo.public.my_tbl'::regclass"
    ).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let dfw = run_select(&shared, &qw).unwrap();
    assert!(dfw.height() >= 1);

    // JOIN using a constant regclass equality condition; should execute
    let qj = match query::parse(
        "SELECT t.typname, n.nspname FROM pg_type t JOIN pg_namespace n ON 'x.y.t'::regclass = 'x.y.t'::regclass"
    ).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let dfj = run_select(&shared, &qj).unwrap();
    assert!(dfj.height() >= 1);
}

#[test]
fn casts_in_having_clause() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Aggregate then filter in HAVING using casted expression
    let q = match query::parse(
        "SELECT COUNT(*) AS c FROM pg_type HAVING ('1'::int) = 1"
    ).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 1);
    let v_any = df.column("c").unwrap().as_materialized_series().get(0).unwrap();
    match v_any { polars::prelude::AnyValue::Int64(i) => assert!(i >= 0), other => panic!("expected Int64 count, got {:?}", other) }
}

use crate::server::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::storage::{SharedStore, Store};
use polars::prelude::*;
use serde_json::json;

// These tests focus on qualified naming resolution for tables and columns:
// - referenced by schema only
// - unqualified
// - fully qualified
// - partially qualified with quotes
// - unqualified with quotes
// and ensure the same works inside CTEs.

fn seed_people_table(store: &Store, qualified_path: &str) {
    // Create a regular (non-time) table with id/name columns
    store.create_table(qualified_path).unwrap();
    let ids = Series::new("id".into(), vec![1i64, 2, 3]);
    let names = Series::new("name".into(), vec!["alice", "bob", "carol"]);
    let df = DataFrame::new(vec![ids.into(), names.into()]).unwrap();
    store.rewrite_table_df(qualified_path, df).unwrap();
}

#[test]
fn qualified_resolution_unqualified_and_schema_only() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Create db/schema/table
    let fq = "clarium/public/people"; // no .time suffix
    seed_people_table(&store, fq);

    // 1) Unqualified reference should resolve using current defaults (db=demo, schema=public)
    let q1 = match query::parse("SELECT id, name FROM people").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).unwrap();
    let cols1 = df1.get_column_names();
    assert!(cols1.iter().any(|c| c.as_str() == "id"));
    assert!(cols1.iter().any(|c| c.as_str() == "name"));
    assert_eq!(df1.height(), 3);

    // 2) Schema-only reference should resolve against current database
    let q2 = match query::parse("SELECT id, name FROM public/people").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).unwrap();
    let cols2 = df2.get_column_names();
    assert!(cols2.iter().any(|c| c.as_str() == "id"));
    assert!(cols2.iter().any(|c| c.as_str() == "name"));
    assert_eq!(df2.height(), 3);
}

#[test]
fn qualified_resolution_fully_qualified_and_alias_projection() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let fq = "clarium/public/people";
    seed_people_table(&store, fq);

    // Fully qualified from-clause with alias and qualified column selection
    let sql = "SELECT p.id, p.name FROM clarium/public/people AS p ORDER BY p.id";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    // Expect alias-qualified column names if projected as qualified
    let names = df.get_column_names();
    assert!(names.iter().any(|c| c.as_str() == "p.id"));
    assert!(names.iter().any(|c| c.as_str() == "p.name"));
    assert_eq!(df.height(), 3);
}

#[test]
fn qualified_resolution_partially_qualified_with_quotes() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Use quoted Schema and verify quoted identifier handling.
    let fq = "clarium/public/people";
    seed_people_table(&store, fq);

    // Partially qualified with quoted schema: "public".PeOpLe
    // And quoted column name in SELECT should project with exact quoted case if supported
    let sql = "SELECT \"id\", \"name\" FROM \"public\"/people";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    let cols = df.get_column_names();
    // Depending on projection policy, columns may appear as unqualified simple names when unaliased
    assert!(cols.iter().any(|c| c.as_str() == "id"));
    assert!(cols.iter().any(|c| c.as_str() == "name"));
    assert_eq!(df.height(), 3);
}

#[test]
fn qualified_resolution_unqualified_with_quotes_in_columns() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let fq = "clarium/public/people";
    seed_people_table(&store, fq);

    // Unqualified table, quoted column references
    let sql = "SELECT \"id\" AS i, \"name\" AS n FROM people ORDER BY i";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "i"));
    assert!(cols.iter().any(|c| c.as_str() == "n"));
    assert_eq!(df.height(), 3);
}

#[test]
fn qualified_resolution_inside_cte_unqualified_and_schema_only() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let fq = "clarium/public/jobs";
    // Create jobs table: id, status
    store.create_table(fq).unwrap();
    let ids = Series::new("id".into(), vec![1i64, 2, 3, 4]);
    let status = Series::new("status".into(), vec!["new", "run", "done", "new"]);
    let df = DataFrame::new(vec![ids.into(), status.into()]).unwrap();
    store.rewrite_table_df(fq, df).unwrap();

    // Unqualified base in CTE
    let sql1 = "WITH j AS (SELECT id, status FROM jobs) SELECT id, status FROM j ORDER BY id";
    let q1 = match query::parse(sql1).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).unwrap();
    assert_eq!(df1.height(), 4);
    let cols1 = df1.get_column_names();
    assert!(cols1.iter().any(|c| c.as_str() == "id"));
    assert!(cols1.iter().any(|c| c.as_str() == "status"));

    // Schema-only base in CTE
    let sql2 = "WITH j AS (SELECT id, status FROM public/jobs) SELECT status FROM j WHERE id > 2 ORDER BY status";
    let q2 = match query::parse(sql2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).unwrap();
    assert_eq!(df2.height(), 2);
    assert!(df2.column("status").is_ok());
}

#[test]
fn qualified_resolution_inside_cte_fully_and_quoted() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let fq = "clarium/public/cases";
    store.create_table(fq).unwrap();
    let ids = Series::new("ID".into(), vec![10i64, 20, 30]);
    let names = Series::new("NaMe".into(), vec!["a", "b", "c"]);
    let df = DataFrame::new(vec![ids.into(), names.into()]).unwrap();
    store.rewrite_table_df(fq, df).unwrap();

    // Fully qualified in CTE; then select with quoted identifiers to test case sensitivity flow
    let sql = "WITH c AS (SELECT \"ID\", \"NaMe\" FROM clarium/public/cases) SELECT \"ID\" AS id, \"NaMe\" AS nm FROM c ORDER BY id";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 3);
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "id"));
    assert!(cols.iter().any(|c| c.as_str() == "nm"));
}

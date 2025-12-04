#![allow(unused_imports)]
use crate::{server::exec::exec_select::run_select, server::exec, storage::{Store, SharedStore}, server::query, server::query::Command};
use polars::prelude::*;

#[test]
fn test_create_show_select_view_and_catalogs() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Create a time table under clarium/public and insert a few rows
    let base = "clarium/public/vsrc.time";
    store.create_table(base).unwrap();
    let mut recs: Vec<crate::storage::Record> = Vec::new();
    for i in 0..3 {
        let mut m = serde_json::Map::new();
        m.insert("a".into(), serde_json::json!(format!("row{}", i)));
        m.insert("v".into(), serde_json::json!(i as i64));
        recs.push(crate::storage::Record { _time: 1_700_000_000_000 + i as i64, sensors: m });
    }
    store.write_records(base, &recs).unwrap();

    // CREATE VIEW myview AS SELECT ...
    let create_sql = "CREATE VIEW myview AS SELECT a, v FROM vsrc.time";
    // execute_query is async; run on a lightweight runtime
    let rt = tokio::runtime::Runtime::new().unwrap();
    let res = rt.block_on(exec::execute_query(&shared, create_sql));
    assert!(res.is_ok(), "CREATE VIEW should succeed: {:?}", res.err());

    // SHOW VIEW myview
    let show = rt.block_on(exec::execute_query(&shared, "SHOW VIEW myview")).unwrap();
    let def = show.as_array().and_then(|arr| arr.get(0)).and_then(|row| row.get("definition")).and_then(|v| v.as_str()).unwrap_or("");
    assert!(def.to_uppercase().contains("SELECT"), "SHOW VIEW should return definition, got: {}", def);

    // SELECT from the view
    let q1 = match query::parse("SELECT a, v FROM myview ORDER BY v").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).unwrap();
    assert_eq!(df1.height(), 3);
    let cols = df1.get_column_names();
    assert!(cols.iter().any(|c| c.as_str()=="a") && cols.iter().any(|c| c.as_str()=="v"), "Projection should include selected cols 'a' and 'v', got {:?}", cols);

    // information_schema.views lists the view
    let q2 = match query::parse("SELECT table_name FROM information_schema.views").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).unwrap();
    let names: Vec<String> = df2.column("table_name").unwrap().str().unwrap().into_iter().filter_map(|o| o.map(|s| s.to_string())).collect();
    assert!(names.iter().any(|n| n == "myview"), "information_schema.views should include myview; got: {:?}", names);

    // pg_catalog.pg_views lists the view
    let q3 = match query::parse("SELECT viewname FROM pg_catalog.pg_views").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df3 = run_select(&shared, &q3).unwrap();
    let vnames: Vec<String> = df3.column("viewname").unwrap().str().unwrap().into_iter().filter_map(|o| o.map(|s| s.to_string())).collect();
    assert!(vnames.iter().any(|n| n == "myview"), "pg_catalog.pg_views should include myview; got: {:?}", vnames);

    // pg_catalog.pg_class lists the view with relkind='v' and has a stable OID
    let q4 = match query::parse("SELECT oid, relkind FROM pg_catalog.pg_class WHERE relname='myview'").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df4 = run_select(&shared, &q4).unwrap();
    assert_eq!(df4.height(), 1, "pg_class should have one row for myview");
    let rk = df4.column("relkind").unwrap().str().unwrap().get(0).unwrap().to_string();
    assert_eq!(rk, "v");
    let oid = df4.column("oid").unwrap().i32().unwrap().get(0).unwrap();

    // pg_get_viewdef(oid) returns the same definition
    let sql_vdef = format!("SELECT pg_get_viewdef({}) AS def", oid);
    let q5 = match query::parse(&sql_vdef).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df5 = run_select(&shared, &q5).unwrap();
    let got_def = df5.column("def").unwrap().str().unwrap().get(0).unwrap().to_string();
    assert_eq!(got_def, def, "pg_get_viewdef should match SHOW VIEW definition");
}

#[test]
fn test_view_union_and_alias_and_collisions() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Two small sources
    let t1 = "clarium/public/u1.time";
    let t2 = "clarium/public/u2.time";
    store.create_table(t1).unwrap();
    store.create_table(t2).unwrap();
    let r1 = vec![crate::storage::Record { _time: 1_700_000_000_000, sensors: serde_json::json!({"x": 1}).as_object().unwrap().clone() }];
    let r2 = vec![crate::storage::Record { _time: 1_700_000_100_000, sensors: serde_json::json!({"x": 2}).as_object().unwrap().clone() }];
    store.write_records(t1, &r1).unwrap();
    store.write_records(t2, &r2).unwrap();

    // UNION view
    let create_union = "CREATE VIEW v_union AS SELECT x FROM u1.time UNION ALL SELECT x FROM u2.time";
    rt.block_on(exec::execute_query(&shared, create_union)).unwrap();

    // Use alias in FROM and JOIN self
    let q = match query::parse("SELECT a.x, b.x FROM v_union a JOIN v_union b ON a.x < b.x ORDER BY a.x, b.x").unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 1);
    let ax = df.column("a.x").unwrap().i64().unwrap().get(0).unwrap();
    let bx = df.column("b.x").unwrap().i64().unwrap().get(0).unwrap();
    assert_eq!((ax, bx), (1, 2));

    // Name collisions: table vs view
    // Create a regular table and then attempt to create a view with the same name
    rt.block_on(exec::execute_query(&shared, "CREATE TABLE clarium/public/ntbl")).unwrap();
    let err1 = rt.block_on(exec::execute_query(&shared, "CREATE VIEW ntbl AS SELECT 1 AS c")).err();
    assert!(err1.is_some(), "Expected CREATE VIEW to fail due to name collision with table");

    // Create a view and then attempt to create a regular table with the same base name
    rt.block_on(exec::execute_query(&shared, "CREATE VIEW v_only AS SELECT 1 AS c")).unwrap();
    let err2 = rt.block_on(exec::execute_query(&shared, "CREATE TABLE clarium/public/v_only")).err();
    assert!(err2.is_some(), "Expected CREATE TABLE to fail due to name collision with view");
}

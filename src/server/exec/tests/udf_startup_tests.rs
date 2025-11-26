//! Tests that permanent UDFs (nullif, format_type, pg_catalog.pg_get_expr) are loaded at startup from the global scripts folder

use crate::scripts::{ScriptRegistry, init_script_registry, load_global_default_scripts};
use crate::server::exec::run_select;
use crate::storage::{Store, SharedStore};

#[test]
fn udf_autoload_registry_contains_nullif_and_format_type() {
    // Create a fresh registry and simulate server startup loading global scripts
    let reg = ScriptRegistry::new().unwrap();
    // Simulate server startup global script autoload
    let _ = load_global_default_scripts(&reg);
    // Reuse server's startup behavior: load global defaults via server::run_with_ports normally.
    // Here, we emulate by calling into scripts loader indirectly through server init path.
    // For the test, inject as global registry so get_script_registry() works in executor paths.
    init_script_registry(reg.clone());

    // Expect the global scripts to include our permanent UDFs
    let has_nullif = reg.has_function("nullif");
    let has_format_type = reg.has_function("format_type");
    let has_pg_get_expr = reg.has_function("pg_catalog.pg_get_expr");
    assert!(has_nullif, "expected 'nullif' to be auto-loaded from global scripts");
    assert!(has_format_type, "expected 'format_type' to be auto-loaded from global scripts");
    assert!(has_pg_get_expr, "expected 'pg_catalog.pg_get_expr' to be auto-loaded from global scripts");

    // Validate metadata defaults present
    let m_nullif = reg.get_meta("nullif").expect("nullif meta present");
    assert!(m_nullif.nullable, "nullif should be nullable");
    let m_format = reg.get_meta("format_type").expect("format_type meta present");
    assert!(!m_format.nullable, "format_type should be non-nullable");
    let m_pg_get_expr = reg.get_meta("pg_catalog.pg_get_expr").expect("pg_catalog.pg_get_expr meta present");
    assert!(m_pg_get_expr.nullable, "pg_catalog.pg_get_expr should be nullable");
}

#[test]
fn udf_autoload_can_execute_both() {
    // Fresh temp store and shared store
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Prepare a fresh registry, load defaults, and init/merge into global to ensure permanence
    let reg = ScriptRegistry::new().unwrap();
    let _ = load_global_default_scripts(&reg);
    init_script_registry(reg.clone());
    // Sanity check: the merged global registry must resolve to integer
    let gl = crate::scripts::get_script_registry().expect("global registry present");
    let ft = gl
        .call_function_json(
            "format_type",
            &[serde_json::json!(23), serde_json::json!(0)],
        )
        .expect("format_type should be callable directly from merged global registry");
    assert_eq!(ft.as_str().unwrap_or(""), "integer", "format_type registry call sanity check failed after merge; snapshot=\n{}", gl.debug_snapshot());

    // Simple selects invoking UDFs; engine paths should find them via global registry
    let sql1 = "SELECT format_type(23, 0) AS t"; // integer
    let q1 = match crate::query::parse(sql1).unwrap() { crate::query::Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).expect("format_type should execute");
    assert_eq!(df1.column("t").unwrap().str().unwrap().get(0).unwrap(), "integer");

    let sql2 = "SELECT nullif('a','a') as n1, nullif('a','b') as n2";
    let q2 = match crate::query::parse(sql2).unwrap() { crate::query::Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).expect("nullif should execute");
    // n1 should be null, n2 should be 'a' (nullif returns first arg if they differ)
    // Both columns may be null-typed, cast to String to check
    let n1_casted = df2.column("n1").unwrap().cast(&polars::prelude::DataType::String).unwrap();
    assert!(n1_casted.str().unwrap().get(0).is_none());
    let n2_casted = df2.column("n2").unwrap().cast(&polars::prelude::DataType::String).unwrap();
    assert_eq!(n2_casted.str().unwrap().get(0).unwrap(), "a");

    let sql3 = "SELECT pg_catalog.pg_get_expr('CHECK (value > 0)', 12345) as expr";
    let q3 = match crate::query::parse(sql3).unwrap() { crate::query::Command::Select(q) => q, _ => unreachable!() };
    let df3 = run_select(&shared, &q3).expect("pg_catalog.pg_get_expr should execute");
    assert_eq!(df3.column("expr").unwrap().str().unwrap().get(0).unwrap(), "CHECK (value > 0)");
}

#[test]
fn test_pg_get_expr_execution() {
    // Standalone test for pg_catalog.pg_get_expr UDF
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Load global scripts
    let reg = ScriptRegistry::new().unwrap();
    let _ = load_global_default_scripts(&reg);
    init_script_registry(reg.clone());

    // Test 1: Basic expression text passthrough
    let sql1 = "SELECT pg_catalog.pg_get_expr('CHECK (value > 0)', 12345) as expr";
    let q1 = match crate::query::parse(sql1).unwrap() { crate::query::Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).expect("pg_catalog.pg_get_expr should execute");
    assert_eq!(df1.column("expr").unwrap().str().unwrap().get(0).unwrap(), "CHECK (value > 0)");

    // Test 2: NULL handling
    let sql2 = "SELECT pg_catalog.pg_get_expr(NULL, 0) as expr";
    let q2 = match crate::query::parse(sql2).unwrap() { crate::query::Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).expect("pg_catalog.pg_get_expr with NULL should execute");
    let expr_col = df2.column("expr").unwrap().cast(&polars::prelude::DataType::String).unwrap();
    assert!(expr_col.str().unwrap().get(0).is_none(), "pg_get_expr(NULL) should return NULL");
}

#[test]
fn test_pg_get_partkeydef_execution() {
    // Standalone test for pg_catalog.pg_get_partkeydef UDF
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Load global scripts
    let reg = ScriptRegistry::new().unwrap();
    let _ = load_global_default_scripts(&reg);
    init_script_registry(reg.clone());

    // Verify function is registered
    assert!(reg.has_function("pg_get_partkeydef"), "expected 'pg_get_partkeydef' to be loaded");

    // Test 1: Basic call should return NULL (clarium doesn't support PostgreSQL partitioned tables)
    let sql1 = "SELECT pg_catalog.pg_get_partkeydef(12345) as partkey";
    let q1 = match crate::query::parse(sql1).unwrap() { crate::query::Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).expect("pg_catalog.pg_get_partkeydef should execute");
    let partkey_col = df1.column("partkey").unwrap().cast(&polars::prelude::DataType::String).unwrap();
    assert!(partkey_col.str().unwrap().get(0).is_none(), "pg_get_partkeydef should return NULL for clarium tables");

    // Test 2: Unqualified version
    let sql2 = "SELECT pg_get_partkeydef(0) as partkey";
    let q2 = match crate::query::parse(sql2).unwrap() { crate::query::Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).expect("pg_get_partkeydef should execute");
    let partkey_col2 = df2.column("partkey").unwrap().cast(&polars::prelude::DataType::String).unwrap();
    assert!(partkey_col2.str().unwrap().get(0).is_none(), "pg_get_partkeydef should return NULL");
}

#[test]
fn test_pg_total_relation_size_execution() {
    // Standalone test for pg_catalog.pg_total_relation_size UDF
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Load global scripts
    let reg = ScriptRegistry::new().unwrap();
    let _ = load_global_default_scripts(&reg);
    init_script_registry(reg.clone());

    // Verify function is registered
    assert!(reg.has_function("pg_total_relation_size"), "expected 'pg_total_relation_size' to be loaded");
    assert!(reg.has_function("pg_catalog.pg_total_relation_size"), "expected 'pg_catalog.pg_total_relation_size' to be loaded");

    // Test 1: Basic call should return 0 (clarium stub implementation)
    let sql1 = "SELECT pg_catalog.pg_total_relation_size(12345) as total_size";
    let q1 = match crate::query::parse(sql1).unwrap() { crate::query::Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&shared, &q1).expect("pg_catalog.pg_total_relation_size should execute");
    let size_col = df1.column("total_size").unwrap();
    assert_eq!(size_col.i64().unwrap().get(0).unwrap(), 0, "pg_total_relation_size should return 0 for clarium tables");

    // Test 2: Unqualified version
    let sql2 = "SELECT pg_total_relation_size(0) as total_size";
    let q2 = match crate::query::parse(sql2).unwrap() { crate::query::Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).expect("pg_total_relation_size should execute");
    let size_col2 = df2.column("total_size").unwrap();
    assert_eq!(size_col2.i64().unwrap().get(0).unwrap(), 0, "pg_total_relation_size should return 0");
}

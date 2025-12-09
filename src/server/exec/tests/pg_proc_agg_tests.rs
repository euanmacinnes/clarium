//! Tests for pg_catalog.pg_proc and pg_catalog.pg_aggregate compatibility

use crate::server::exec::run_select;
use crate::server::query::{self, Command};
use crate::storage::{SharedStore, Store};

#[test]
fn execute_pg_proc_join_pg_aggregate_query_schema() {
    // Ensure UDFs are initialized (not strictly needed for this query but keeps environment consistent)
    super::udf_common::init_all_test_udfs();

    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // From the issue description
    let sql = "SELECT P.OID AS PROC_OID,P.PRONAME AS PROC_NAME,A.* FROM PG_CATALOG.PG_AGGREGATE A,PG_CATALOG.PG_PROC P WHERE P.OID=A.AGGFNOID AND P.PRONAMESPACE=('2200'::INT8) ORDER BY P.PRONAME";

    let q = match query::parse(sql).expect("query should parse") {
        Command::Select(q) => q,
        _ => unreachable!(),
    };

    let df = run_select(&shared, &q).expect("query over pg_proc and pg_aggregate should execute");

    // Schema assertions
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "PROC_OID" || c.as_str() == "proc_oid"));
    assert!(cols.iter().any(|c| c.as_str() == "PROC_NAME" || c.as_str() == "proc_name"));
    // From A.* expansion (unqualified base column names)
    for need in ["aggfnoid", "aggkind", "aggsortop", "aggtranstype", "agginitval"] {
        assert!(cols.iter().any(|c| c.as_str().eq_ignore_ascii_case(need)), "missing column from A.*: {} (cols={:?})", need, cols);
    }

    // With empty synthesized catalogs, result set may be empty but must be a valid DataFrame
    let _ = df.height();
}

//! DBeaver compatibility tests
//! Focus: parsing SELECT lists with qualified wildcards like `t.*` alongside other expressions

#[test]
fn parse_pg_type_query_with_qualified_wildcard() {
    use crate::query::{parse, Command};

    let sql = "SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name, d.description \
FROM pg_catalog.pg_type t \
LEFT OUTER JOIN pg_catalog.pg_type et ON et.oid=t.typelem \
LEFT OUTER JOIN pg_catalog.pg_class c ON c.oid=t.typrelid \
LEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid \
WHERE t.typname IS NOT NULL \
AND (c.relkind IS NULL OR c.relkind = 'c') AND (et.typcategory IS NULL OR et.typcategory <> 'C')".replace("\\n", " ").replace("  ", " ");

    let cmd = parse(&sql).expect("query should parse without treating 't.*' as multiplication");
    match cmd {
        Command::Select(q) => {
            // Ensure the SELECT list contains the qualified wildcard `t.*`
            assert!(q.select.iter().any(|it| it.func.is_none() && it.expr.is_none() && it.column == "t.*"),
                "expected qualified wildcard t.* in select list, got: {:?}", q.select);
        }
        _ => panic!("expected Command::Select for the DBeaver query"),
    }
}

#[test]
fn execute_full_pg_type_query_with_qualified_wildcard() {
    use crate::query::{parse, Command};
    use crate::server::exec::run_select;
    use crate::storage::{Store, SharedStore};
    use super::udf_common::init_all_test_udfs;

    // Set up a minimal temporary store; system tables are synthesized
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Ensure global script registry has required UDFs for the DBeaver query
    init_all_test_udfs();

    let sql = "SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name, d.description\nFROM pg_catalog.pg_type t\nLEFT OUTER JOIN pg_catalog.pg_type et ON et.oid=t.typelem\nLEFT OUTER JOIN pg_catalog.pg_class c ON c.oid=t.typrelid\nLEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid\nWHERE t.typname IS NOT NULL\nAND (c.relkind IS NULL OR c.relkind = 'c') AND (et.typcategory IS NULL OR et.typcategory <> 'C')"
        .replace("\n", " ")
        .replace("  ", " ");

    let q = match parse(&sql).expect("query should parse for execution with 't.*'") {
        Command::Select(q) => q,
        _ => panic!("expected select command"),
    };

    // Sanity: ensure FROM and aliases parsed
    assert!(q.base_table.is_some(), "parser did not set base_table for DBeaver query");
    // base alias should be 't'
    match q.base_table.as_ref().unwrap() {
        crate::query::TableRef::Table { name: _, alias } => {
            assert_eq!(alias.as_deref(), Some("t"), "expected base alias 't' for pg_catalog.pg_type");
        }
        other => panic!("expected base table, got {:?}", other),
    }
    // Joins should include aliases et, c, d
    if let Some(js) = &q.joins {
        let mut got = std::collections::HashSet::new();
        for j in js {
            match &j.right {
                crate::query::TableRef::Table { name: _, alias } => { if let Some(a) = alias { got.insert(a.clone()); } },
                _ => {}
            }
        }
        for need in ["et", "c", "d"] { assert!(got.contains(need), "expected join alias '{}' present; got {:?}", need, got); }
    } else {
        panic!("expected joins present (et, c, d)");
    }

    let df = run_select(&shared, &q).expect("full DBeaver pg_type query should execute without 't.*' column-not-found error");

    // We should get some rows (at least built-in types synthesized in pg_type)
    assert!(df.height() >= 1);

    // Validate that columns coming from t.* are expanded to base names (no 't.' prefix),
    // while explicitly selected qualified columns keep their qualifiers.
    let cols = df.get_column_names();
    // From t.* expansion
    assert!(cols.iter().any(|c| c.as_str() == "oid"), "expected 'oid' from t.*; got cols={:?}", cols);
    assert!(cols.iter().any(|c| c.as_str() == "typname"), "expected 'typname' from t.*; got cols={:?}", cols);
    // From explicit selections
    assert!(cols.iter().any(|c| c.as_str() == "c.relkind"), "expected qualified 'c.relkind' column");
    assert!(cols.iter().any(|c| c.as_str() == "base_type_name"), "expected aliased column 'base_type_name'");
    assert!(cols.iter().any(|c| c.as_str() == "d.description"), "expected qualified 'd.description' column");
}

#[test]
fn execute_pg_namespace_with_join_and_regclass_order() {
    use crate::query::{parse, Command};
    use crate::server::exec::run_select;
    use crate::storage::{Store, SharedStore};
    use super::udf_common::init_all_test_udfs;

    // Temp store; system catalog tables are synthesized on demand
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Ensure required UDFs (e.g., nullif/format_type) are available globally if referenced
    // Not strictly needed for this query, but keeps environment consistent with other DBeaver tests
    init_all_test_udfs();

    // Query from issue description (PostgreSQL-style regclass and qualified wildcard)
    let sql = "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n\nLEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass\n ORDER BY nspname"
        .replace("\n", " ")
        .replace("  ", " ");

    let q = match parse(&sql).expect("pg_namespace join with regclass should parse") {
        Command::Select(q) => q,
        _ => panic!("expected select command"),
    };

    // Execute and ensure it succeeds
    let df = run_select(&shared, &q).expect("pg_namespace join with regclass should execute");

    // Expect at least the built-in namespaces (pg_catalog, public)
    assert!(df.height() >= 2, "expected at least two namespaces");

    // Columns: n.* expands to base names (oid, nspname, ...); d.description remains qualified
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "oid"), "expected 'oid' column from n.*");
    assert!(cols.iter().any(|c| c.as_str() == "nspname"), "expected 'nspname' column from n.*");
    assert!(cols.iter().any(|c| c.as_str() == "d.description"), "expected qualified 'd.description' column from join");
}

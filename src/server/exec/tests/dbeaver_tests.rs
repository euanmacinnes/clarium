//! DBeaver compatibility tests
//! Focus: parsing SELECT lists with qualified wildcards like `t.*` alongside other expressions

use crate::server::query::query_common::TableRef;

#[test]
fn parse_pg_type_query_with_qualified_wildcard() {
    use crate::server::query::{parse, Command};

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
    use crate::server::query::{parse, Command};
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
        crate::server::query::TableRef::Table { name: _, alias } => {
            assert_eq!(alias.as_deref(), Some("t"), "expected base alias 't' for pg_catalog.pg_type");
        }
        other => panic!("expected base table, got {:?}", other),
    }
    // Joins should include aliases et, c, d
    if let Some(js) = &q.joins {
        let mut got = std::collections::HashSet::new();
        for j in js {
            match &j.right {
                TableRef::Table { name: _, alias } => { if let Some(a) = alias { got.insert(a.clone()); } },
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
    use crate::server::query::{parse, Command};
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

#[test]
fn execute_dbeaver_pg_attribute_query_no_hang() {
    use crate::server::query::{parse, Command};
    use crate::server::exec::run_select;
    use crate::storage::{Store, SharedStore};
    use super::udf_common::init_all_test_udfs;

    // Temp store; system catalog tables are synthesized on demand
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Ensure UDFs used by DBeaver metadata queries are available (e.g., pg_catalog.pg_get_expr)
    init_all_test_udfs();

    // Query reported to hang in DBeaver when reflecting table columns
    let sql = "SELECT c.relname,a.*,pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) as def_value,dsc.description,dep.objid\nFROM pg_catalog.pg_attribute a\nINNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid)\nLEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum)\nLEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)\nLEFT OUTER JOIN pg_depend dep on dep.refobjid = a.attrelid AND dep.deptype = 'i' and dep.refobjsubid = a.attnum and dep.classid = dep.refclassid\nWHERE NOT a.attisdropped AND c.relkind not in ('i','I','c') AND c.oid=('586816'::int8)\nORDER BY a.attnum"
        .replace("\n", " ")
        .replace("  ", " ");

    let q = match parse(&sql).expect("DBeaver pg_attribute query should parse") {
        Command::Select(q) => q,
        _ => panic!("expected select command"),
    };

    // Execute; success means it does not hang and returns a DataFrame (may be empty)
    let df = run_select(&shared, &q).expect("DBeaver pg_attribute query should execute without hanging");

    // No strict expectations about row count; just ensure a valid DataFrame object exists
    let _ = df.height();
}

#[test]
fn execute_pg_roles_with_join_and_order() {
    use crate::server::query::{parse, Command};
    use crate::server::exec::run_select;
    use crate::storage::{Store, SharedStore};
    use super::udf_common::init_all_test_udfs;

    // Temp store; system catalog tables are synthesized on demand
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Ensure UDFs landscape is initialized similarly to other DBeaver tests
    init_all_test_udfs();

    // Query from the issue description (DBeaver: roles and shared descriptions)
    let sql = "SELECT a.oid,a.*,pd.description FROM pg_catalog.pg_roles a \nleft join pg_catalog.pg_shdescription pd on a.oid = pd.objoid\nORDER BY a.rolname"
        .replace("\n", " ")
        .replace("  ", " ");

    let q = match parse(&sql).expect("pg_roles join with shdescription should parse") {
        Command::Select(q) => q,
        _ => panic!("expected select command"),
    };

    // Execute and ensure it succeeds
    let df = run_select(&shared, &q).expect("pg_roles join with shdescription should execute");

    // Columns: a.* expands to base names (oid, rolname, ...); pd.description remains qualified
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "oid"), "expected 'oid' column from a.*");
    assert!(cols.iter().any(|c| c.as_str() == "rolname"), "expected 'rolname' column from a.*");
    assert!(cols.iter().any(|c| c.as_str() == "pd.description"), "expected qualified 'pd.description' column from join");
}

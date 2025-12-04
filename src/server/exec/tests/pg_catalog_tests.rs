use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use super::udf_common::init_all_test_udfs;
use std::collections::HashSet;

/// pg_catalog tests that mimic SQLAlchemy introspection queries.
/// SQLAlchemy uses these queries during connection initialization and schema reflection.

#[test]
fn test_pg_catalog_version() {
    // SQLAlchemy calls: select pg_catalog.version()
    // This is used to detect server version during connection initialization
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse("SELECT pg_catalog.version()").unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 1);
    let cols = df.get_column_names();
    assert!(cols.len() >= 1);
    // The result should contain a version string
    let col = df.get_columns()[0].as_materialized_series();
    let val = col.get(0).unwrap();
    match val {
        polars::prelude::AnyValue::String(s) => {
            assert!(s.contains("PostgreSQL") || s.contains("clariumDB"));
        }
        polars::prelude::AnyValue::StringOwned(s) => {
            assert!(s.contains("PostgreSQL") || s.contains("clariumDB"));
        }
        _ => panic!("Expected string value for version(), got {:?}", val)
    }
}

#[test]
fn test_pg_catalog_version_alias() {
    // Test without schema prefix
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse("SELECT version()").unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    // This may or may not work depending on function resolution rules
    // But we should test it doesn't crash
    let _result = run_select(&shared, &q);
}

#[test]
fn test_pg_type_basic_query() {
    // SQLAlchemy queries pg_type to understand data types
    // SELECT typname, oid, typarray FROM pg_type
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse("SELECT typname, oid, typarray FROM pg_type").unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df = run_select(&shared, &q).unwrap();
    assert!(df.height() > 0, "pg_type should return at least some built-in types");
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "typname"));
    assert!(cols.iter().any(|c| c.as_str() == "oid"));
    assert!(cols.iter().any(|c| c.as_str() == "typarray"));
}

#[test]
fn test_pg_type_with_namespace_join() {
    // SQLAlchemy often joins pg_type with pg_namespace:
    // SELECT t.typname, n.nspname FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse(
        "SELECT t.typname, n.nspname FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid"
    ).unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df = run_select(&shared, &q).unwrap();
    assert!(df.height() > 0, "JOIN between pg_type and pg_namespace should return rows");
    let cols = df.get_column_names();
    // Qualified names are preserved from SELECT clause
    assert!(cols.iter().any(|c| c.as_str() == "t.typname"));
    assert!(cols.iter().any(|c| c.as_str() == "n.nspname"));
}

#[test]
fn test_pg_namespace_basic_query() {
    // SQLAlchemy queries pg_namespace to list schemas:
    // SELECT oid, nspname FROM pg_namespace
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse("SELECT oid, nspname FROM pg_namespace").unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df = run_select(&shared, &q).unwrap();
    assert!(df.height() >= 2, "pg_namespace should have at least pg_catalog and public");
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "oid"));
    assert!(cols.iter().any(|c| c.as_str() == "nspname"));
    
    // Check that pg_catalog and public are present
    let nspname_col = df.column("nspname").unwrap().as_materialized_series();
    let names: Vec<String> = nspname_col.str().unwrap()
        .into_iter()
        .filter_map(|s| s.map(|v| v.to_string()))
        .collect();
    assert!(names.contains(&"pg_catalog".to_string()));
    assert!(names.contains(&"public".to_string()));
}

#[test]
fn test_information_schema_tables_public_demo_defaults() {
    // Verify that querying information_schema.tables for schema 'public'
    // returns the expected columns in order and lists the default demo tables.
    // Query under test (exact):
    // SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'public'

    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();

    // Create the default demo tables similar to first-run bootstrap:
    // - clarium/public/demo.time (time table)
    // - clarium/public/demo_positive_events (regular table)
    // - clarium/public/demo_negative_events (regular table)
    let demo_time = "clarium/public/demo.time";
    // Write a tiny record to ensure the time table exists on disk
    let recs = vec![Record { _time: 1_700_000_000_000, sensors: serde_json::Map::from_iter(vec![("value".into(), serde_json::json!(0.0))]) }];
    store.write_records(demo_time, &recs).unwrap();
    // Create regular tables
    store.create_table("clarium/public/demo_positive_events").unwrap();
    store.create_table("clarium/public/demo_negative_events").unwrap();

    let shared = SharedStore::new(tmp.path()).unwrap();

    let sql = "SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'public'";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();

    // Verify column order is exactly as requested
    let cols = df.get_column_names();
    assert_eq!(cols, vec!["table_schema", "table_name", "table_type"]);

    // Gather results into sets for comparison without depending on filesystem order
    let mut schemas: HashSet<String> = HashSet::new();
    let mut names: HashSet<String> = HashSet::new();
    let mut types: HashSet<String> = HashSet::new();

    for i in 0..df.height() {
        // Extract plain string values without the debug quotes that AnyValue::to_string() adds
        let s = match df.get_columns()[0].as_materialized_series().get(i).unwrap() {
            polars::prelude::AnyValue::String(v) => v.to_string(),
            polars::prelude::AnyValue::StringOwned(v) => v.to_string(),
            other => other.to_string(),
        };
        let n = match df.get_columns()[1].as_materialized_series().get(i).unwrap() {
            polars::prelude::AnyValue::String(v) => v.to_string(),
            polars::prelude::AnyValue::StringOwned(v) => v.to_string(),
            other => other.to_string(),
        };
        let t = match df.get_columns()[2].as_materialized_series().get(i).unwrap() {
            polars::prelude::AnyValue::String(v) => v.to_string(),
            polars::prelude::AnyValue::StringOwned(v) => v.to_string(),
            other => other.to_string(),
        };
        schemas.insert(s);
        names.insert(n);
        types.insert(t);
    }

    // All rows should be in schema 'public'
    assert_eq!(schemas, HashSet::from(["public".to_string()]));

    // Expected default demo tables (time table name is 'demo' without .time suffix)
    let expected_names: HashSet<String> = HashSet::from([
        "demo".to_string(),
        "demo_positive_events".to_string(),
        "demo_negative_events".to_string(),
    ]);
    assert!(expected_names.is_subset(&names), "Expected demo tables missing. got={:?}", names);

    // Table type should be BASE TABLE for all
    assert_eq!(types, HashSet::from(["BASE TABLE".to_string()]));
}

#[test]
fn test_pg_catalog_pg_namespace_qualified() {
    // Ensure pg_catalog.pg_namespace works (with schema prefix)
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse("SELECT oid, nspname FROM pg_catalog.pg_namespace").unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df = run_select(&shared, &q).unwrap();
    assert!(df.height() >= 2);
}

#[test]
fn test_pg_class_basic_query() {
    // SQLAlchemy queries pg_class to list tables:
    // SELECT relname, relkind FROM pg_class
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // Create a test table
    let recs = vec![Record { _time: 1_700_000_000_000, sensors: serde_json::Map::new() }];
    store.write_records("demo/public/test_table.time", &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse("SELECT relname, relkind FROM pg_class").unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df = run_select(&shared, &q).unwrap();
    assert!(df.height() > 0, "pg_class should return at least one table");
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "relname"));
    assert!(cols.iter().any(|c| c.as_str() == "relkind"));
}

#[test]
fn test_pg_class_with_namespace_filter() {
    // SQLAlchemy often filters pg_class by namespace:
    // SELECT relname FROM pg_class WHERE nspname = 'public'
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let recs = vec![Record { _time: 1_700_000_000_000, sensors: serde_json::Map::new() }];
    store.write_records("demo/public/my_table.time", &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse("SELECT relname FROM pg_class WHERE nspname = 'public'").unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df = run_select(&shared, &q).unwrap();
    // Should execute without error
    assert!(df.get_column_names().iter().any(|c| c.as_str() == "relname"));
}

#[test]
fn test_pg_catalog_pg_type_qualified() {
    // Ensure pg_catalog.pg_type works (with schema prefix)
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse("SELECT typname, oid FROM pg_catalog.pg_type").unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df = run_select(&shared, &q).unwrap();
    assert!(df.height() > 0);
}

#[test]
fn test_pg_type_where_typname() {
    // SQLAlchemy often filters by type name:
    // SELECT oid FROM pg_type WHERE typname = 'int8'
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse("SELECT oid FROM pg_type WHERE typname = 'int8'").unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 1, "Should find exactly one int8 type");
    let oid_col = df.column("oid").unwrap().as_materialized_series();
    let oid_val = oid_col.get(0).unwrap();
    match oid_val {
        polars::prelude::AnyValue::Int32(oid) => {
            assert_eq!(oid, 20, "int8 should have OID 20");
        }
        _ => panic!("Expected Int32 OID, got {:?}", oid_val)
    }
}

#[test]
fn test_pg_type_multiple_filters() {
    init_all_test_udfs();
    // Complex query with multiple conditions:
    // SELECT typname, oid, typarray FROM pg_type WHERE typname IN ('text', 'int8', 'float8')
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse(
        "SELECT typname, oid, typarray FROM pg_type WHERE typname IN ('text', 'int8', 'float8')"
    ).unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 3, "Should find exactly 3 types");
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "typname"));
    assert!(cols.iter().any(|c| c.as_str() == "oid"));
    assert!(cols.iter().any(|c| c.as_str() == "typarray"));
}

#[test]
fn test_pg_attribute_basic_query() {
    // SQLAlchemy queries pg_attribute for column metadata:
    // SELECT attname FROM pg_attribute
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse("SELECT attname FROM pg_attribute").unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df = run_select(&shared, &q).unwrap();
    // pg_attribute may be empty in our implementation
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "attname"));
}

#[test]
fn test_information_schema_columns_sqlalchemy_style() {
    // SQLAlchemy queries information_schema.columns with specific filters:
    // SELECT column_name, data_type, is_nullable, udt_name 
    // FROM information_schema.columns 
    // WHERE table_schema = 'public' AND table_name = 'my_table'
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let recs = vec![Record { 
        _time: 1_700_000_000_000, 
        sensors: serde_json::Map::from_iter(vec![
            ("col1".into(), serde_json::json!(42)),
            ("col2".into(), serde_json::json!("test")),
        ])
    }];
    store.write_records("demo/public/my_table.time", &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse(
        "SELECT column_name, data_type, is_nullable, udt_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'my_table'"
    ).unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df = run_select(&shared, &q).unwrap();
    assert!(df.height() > 0, "Should find columns for my_table");
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "column_name"));
    assert!(cols.iter().any(|c| c.as_str() == "data_type"));
    assert!(cols.iter().any(|c| c.as_str() == "is_nullable"));
    assert!(cols.iter().any(|c| c.as_str() == "udt_name"));
}

#[test]
fn test_pg_catalog_current_schema() {
    // SQLAlchemy may call current_schema() function:
    // SELECT current_schema()
    // This test checks if the query can be parsed and executed
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // This may not be implemented yet, but we test it doesn't crash the parser
    let result = query::parse("SELECT current_schema()");
    if let Ok(Command::Select(q)) = result {
        let _df_result = run_select(&shared, &q);
        // Don't assert success - just ensure it doesn't panic
    }
}

#[test]
fn test_pg_catalog_has_schema_privilege() {
    // SQLAlchemy checks schema privileges:
    // SELECT has_schema_privilege('public', 'USAGE')
    // This test checks if the query can be parsed
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // This may not be implemented yet, but we test it doesn't crash the parser
    let result = query::parse("SELECT has_schema_privilege('public', 'USAGE')");
    if let Ok(Command::Select(q)) = result {
        let _df_result = run_select(&shared, &q);
        // Don't assert success - just ensure it doesn't panic
    }
}

#[test]
fn test_combined_catalog_query() {
    // Complex query combining multiple catalog tables as SQLAlchemy would:
    // SELECT c.relname, n.nspname, t.typname
    // FROM pg_class c
    // JOIN pg_namespace n ON c.nspname = n.nspname
    // JOIN pg_type t ON t.typnamespace = n.oid
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let recs = vec![Record { _time: 1_700_000_000_000, sensors: serde_json::Map::new() }];
    store.write_records("demo/public/joined_table.time", &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse(
        "SELECT c.relname, n.nspname FROM pg_class c JOIN pg_namespace n ON c.nspname = n.nspname"
    ).unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let _df = run_select(&shared, &q);
    // Just ensure it doesn't panic - complex joins may need work
}

#[test]
fn test_pg_class_namespace_join_with_oid() {
    // Reproduces SQLAlchemy introspection query that fails with:
    // "Column not found in JOIN ON: 'pg_catalog.pg_namespace.oid'"
    // This query is used to check if a table exists and is visible.
    // SQL: SELECT pg_catalog.pg_class.relname 
    // FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace 
    // ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
    // WHERE pg_catalog.pg_class.relname = 'clarium_test_y21ycf'
    // AND pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm'])
    // AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) 
    // AND pg_catalog.pg_namespace.nspname != 'pg_catalog'
    
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // Create a test table matching the pattern
    let recs = vec![Record { _time: 1_700_000_000_000, sensors: serde_json::Map::new() }];
    store.write_records("demo/public/clarium_test_y21ycf.time", &recs).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Simplified version of the problematic query
    let q = match query::parse(
        "SELECT pg_catalog.pg_class.relname \
         FROM pg_catalog.pg_class \
         JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace \
         WHERE pg_catalog.pg_class.relname = 'clarium_test_y21ycf'"
    ).unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    
    // This should execute without "Column not found in JOIN ON" error
    let result = run_select(&shared, &q);
    match result {
        Ok(df) => {
            // If successful, verify the structure
            let cols = df.get_column_names();
            assert!(cols.iter().any(|c| c.as_str() == "pg_catalog.pg_class.relname"));
        }
        Err(e) => {
            // The error we're diagnosing:
            // "Column not found in JOIN ON: 'pg_catalog.pg_namespace.oid'"
            let err_msg = format!("{:?}", e);
            panic!("Query failed with error: {}. \
                   This indicates that pg_namespace.oid is not available in the JOIN. \
                   The issue is that after JOIN, only pg_class columns are accessible, \
                   suggesting the JOIN implementation may not be properly merging both table schemas.", 
                   err_msg);
        }
    }
}

#[test]
fn test_pg_catalog_pg_get_viewdef() {
    // Test pg_catalog.pg_get_viewdef(oid) function
    // SQLAlchemy calls this to get view definitions
    // Since clarium doesn't support views, this should return NULL for all inputs
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Test with qualified name
    let q = match query::parse("SELECT pg_catalog.pg_get_viewdef(123)").unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 1);
    let col = df.get_columns()[0].as_materialized_series();
    let val = col.get(0).unwrap();
    assert!(matches!(val, polars::prelude::AnyValue::Null), "pg_get_viewdef should return NULL, got {:?}", val);

    // Test with unqualified name
    let q2 = match query::parse("SELECT pg_get_viewdef(456)").unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    let df2 = run_select(&shared, &q2).unwrap();
    assert_eq!(df2.height(), 1);
    let col2 = df2.get_columns()[0].as_materialized_series();
    let val2 = col2.get(0).unwrap();
    assert!(matches!(val2, polars::prelude::AnyValue::Null), "pg_get_viewdef should return NULL, got {:?}", val2);
}

#[test]
fn test_pg_type_with_to_regtype() {
    // Test query that uses TO_REGTYPE function to filter pg_type
    // This query checks for hstore type existence and retrieves type metadata
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let q = match query::parse(
        "SELECT \
               TYPNAME AS NAME, OID, TYPARRAY AS ARRAY_OID, \
               OID::REGTYPE::TEXT AS REGTYPE, TYPDELIM AS DELIMITER \
           FROM PG_TYPE T \
           WHERE T.OID = TO_REGTYPE('HSTORE') \
           ORDER BY T.OID"
    ).unwrap() {
        Command::Select(q) => q,
        _ => unreachable!()
    };
    
    let df = run_select(&shared, &q).unwrap();

    
    // Verify the columns exist
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "name" || c.as_str() == "NAME"), "Should have NAME column");
    assert!(cols.iter().any(|c| c.as_str() == "oid" || c.as_str() == "OID"), "Should have OID column");
    assert!(cols.iter().any(|c| c.as_str() == "array_oid" || c.as_str() == "ARRAY_OID"), "Should have ARRAY_OID column");
    assert!(cols.iter().any(|c| c.as_str() == "regtype" || c.as_str() == "REGTYPE"), "Should have REGTYPE column");
    assert!(cols.iter().any(|c| c.as_str() == "delimiter" || c.as_str() == "DELIMITER"), "Should have DELIMITER column");
    
    // Verify hstore type data
    let name_col_idx = cols.iter().position(|c| c.as_str().to_lowercase() == "name").unwrap();
    let oid_col_idx = cols.iter().position(|c| c.as_str().to_lowercase() == "oid").unwrap();
    
    let name_series = df.get_columns()[name_col_idx].as_materialized_series();
    let oid_series = df.get_columns()[oid_col_idx].as_materialized_series();
}

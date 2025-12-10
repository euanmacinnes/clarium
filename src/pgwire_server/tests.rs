use anyhow::{anyhow, Result, bail};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, debug, warn};
use crate::tprintln;

use crate::{storage::SharedStore, server::exec};
use crate::pgwire_server::encodedecode::*;
use crate::pgwire_server::inline::*;
use crate::pgwire_server::misc::*;
use crate::pgwire_server::oids::*;
use crate::pgwire_server::parse::*;
use crate::pgwire_server::security::*;
use crate::pgwire_server::send::*;
use crate::pgwire_server::structs::*;
use crate::server::query::{self, Command};
use crate::server::exec::exec_select::handle_select;
use polars::prelude::{AnyValue, DataFrame, DataType, TimeUnit};
use crate::ident::{DEFAULT_DB, DEFAULT_SCHEMA};
use regex::Regex;
use std::collections::HashMap;

// Unit tests focused on parameter substitution and SQL literal escaping for the pgwire extended protocol.
// These run by default and do not require starting the network server (authentication would complicate that here).

#[cfg(test)]
mod tests {
    use crate::pgwire_server::{substitute_placeholders, escape_sql_literal};

    #[test]
    fn test_escape_sql_literal_simple_and_quotes() {
        assert_eq!(escape_sql_literal("abc"), "'abc'");
        assert_eq!(escape_sql_literal("a'b"), "'a''b'");
        assert_eq!(escape_sql_literal("''"), "''''''");
        assert_eq!(escape_sql_literal(""), "''");
    }

    #[test]
    fn test_substitute_positional_basic() {
        let sql = "SELECT %s AS v, %s AS w";
        let out = substitute_placeholders(sql, &[Some("one".into()), Some("two".into())]).unwrap();
        assert_eq!(out, "SELECT 'one' AS v, 'two' AS w");
    }

    #[test]
    fn test_substitute_positional_null_and_count_errors() {
        let sql = "SELECT %s AS v";
        let out = substitute_placeholders(sql, &[None]).unwrap();
        assert_eq!(out, "SELECT NULL AS v");
        // Too few
        let err = substitute_placeholders(sql, &[]).err();
        assert!(err.is_some());
        // Too many
        let err2 = substitute_placeholders(sql, &[Some("x".into()), Some("y".into())]).err();
        assert!(err2.is_some());
    }

    #[test]
    fn test_substitute_named_single_and_duplicates() {
        let sql = "SELECT %(name)s AS v, %(name)s AS v2";
        let out = substitute_placeholders(sql, &[Some("hello".into())]).unwrap();
        assert_eq!(out, "SELECT 'hello' AS v, 'hello' AS v2");
    }

    #[test]
    fn test_substitute_named_multiple_and_null() {
        let sql = "SELECT %(a)s AS a, %(b)s AS b";
        let out = substitute_placeholders(sql, &[Some("X".into()), None]).unwrap();
        assert_eq!(out, "SELECT 'X' AS a, NULL AS b");
    }

    #[test]
    fn test_substitute_named_count_mismatch() {
        let sql = "SELECT %(a)s, %(b)s, %(c)s";
        // Expect three params (a,b,c unique), provide two -> error
        let err = substitute_placeholders(sql, &[Some("1".into()), Some("2".into())]).err();
        assert!(err.is_some());
    }
}

#[cfg(test)]
mod protocol_tests {
    use crate::pgwire_server::*;

    #[test]
    fn test_to_table_empty_array() {
        // Empty result should produce empty columns and data
        let (cols, data) = to_table(vec![]).unwrap();
        assert!(cols.is_empty());
        assert!(data.is_empty());
    }

    #[test]
    fn test_to_table_single_object() {
        // Single object row should preserve key order from JSON
        let row = serde_json::json!({"col1": "val1", "col2": 42});
        let (cols, data) = to_table(vec![row]).unwrap();
        assert_eq!(cols.len(), 2);
        assert!(cols.contains(&"col1".to_string()));
        assert!(cols.contains(&"col2".to_string()));
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].len(), 2);
    }

    #[test]
    fn test_to_table_multiple_objects_with_nulls() {
        let row1 = serde_json::json!({"a": "x", "b": null});
        let row2 = serde_json::json!({"a": null, "b": "y"});
        let (cols, data) = to_table(vec![row1, row2]).unwrap();
        assert_eq!(cols, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(data.len(), 2);
        assert_eq!(data[0], vec![Some("x".into()), None]);
        assert_eq!(data[1], vec![None, Some("y".into())]);
    }

    #[test]
    fn test_to_table_expanding_schema() {
        // First row has cols a,b; second adds col c
        let row1 = serde_json::json!({"a": 1, "b": 2});
        let row2 = serde_json::json!({"a": 3, "c": 4});
        let (cols, data) = to_table(vec![row1, row2]).unwrap();
        assert_eq!(cols, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        assert_eq!(data.len(), 2);
        // row1 should have None for c
        assert_eq!(data[0], vec![Some("1".into()), Some("2".into()), None]);
        // row2 should have None for b
        assert_eq!(data[1], vec![Some("3".into()), None, Some("4".into())]);
    }

    #[test]
    fn test_to_table_scalar_value() {
        // Non-object value should create single "value" column
        let val = serde_json::json!(123);
        let (cols, data) = to_table(vec![val]).unwrap();
        assert_eq!(cols, vec!["value".to_string()]);
        assert_eq!(data.len(), 1);
        assert_eq!(data[0], vec![Some("123".into())]);
    }

    #[test]
    fn test_parse_insert_basic() {
        let sql = "INSERT INTO clarium (id, name) VALUES (1, 'test')";
        let ins = parse_insert(sql).unwrap();
        assert_eq!(ins.database, "clarium");
        assert_eq!(ins.columns, vec!["id", "name"]);
        assert_eq!(ins.values.len(), 2);
    }

    #[test]
    fn test_parse_insert_with_qualified_table() {
        let sql = "INSERT INTO mydb (col1, col2) VALUES (10, 'value')";
        let ins = parse_insert(sql).unwrap();
        assert_eq!(ins.database, "mydb");
        assert_eq!(ins.columns.len(), 2);
    }

    #[test]
    fn test_parse_insert_null_value() {
        let sql = "INSERT INTO testdb (a, b) VALUES (NULL, 'x')";
        let ins = parse_insert(sql).unwrap();
        assert_eq!(ins.values.len(), 2);
        assert!(matches!(ins.values[0], InsertValue::Null));
        assert!(matches!(ins.values[1], InsertValue::String(_)));
    }

    #[test]
    fn test_parse_insert_invalid_syntax() {
        let sql = "INSERT INTO incomplete";
        assert!(parse_insert(sql).is_none());
    }
}


#[cfg(test)]
mod exec_like_and_rowdesc_tests {
    use crate::pgwire_server::substitute_placeholders;

    #[test]
    fn test_row_description_columns_via_df() {
        // Validate that SELECT aliases are preserved as column names, matching RowDescription logic
        let tmp = tempfile::tempdir().unwrap();
        let shared = crate::storage::SharedStore::new(tmp.path()).unwrap();
        let qtext = "SELECT 'a' AS first_col, 'b' AS second_col";
        let q = match crate::server::query::parse(qtext).unwrap() { crate::server::query::Command::Select(q) => q, _ => unreachable!() };
        let df = crate::server::exec::execute_select_df(&shared, &q).unwrap();
        let cols = df.get_column_names();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0], "first_col");
        assert_eq!(cols[1], "second_col");
    }

    #[test]
    fn test_like_with_positional_params_executes_true() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = crate::storage::SharedStore::new(tmp.path()).unwrap();
        // Build SQL via pgwire-style substitution, then execute through the regular engine
        let sql = "SELECT %s LIKE %s AS ok";
        let substituted = substitute_placeholders(sql, &[Some("New York".into()), Some("New%".into())]).unwrap();
        let q = match crate::server::query::parse(&substituted).unwrap() { crate::server::query::Command::Select(q) => q, _ => unreachable!() };
        let df = crate::server::exec::execute_select_df(&shared, &q).unwrap();
        assert_eq!(df.height(), 1);
        let col = df.column("ok").unwrap();
        let ok = col.bool().unwrap().get(0);
        assert_eq!(ok, Some(true));
    }

    #[test]
    fn test_like_with_named_params_executes_true() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = crate::storage::SharedStore::new(tmp.path()).unwrap();
        // Named placeholders: %(s)s and %(p)s should map in order of first appearance
        let sql = "SELECT %(s)s LIKE %(p)s AS ok";
        let substituted = substitute_placeholders(sql, &[Some("New York".into()), Some("New%".into())]).unwrap();
        let q = match crate::server::query::parse(&substituted).unwrap() { crate::server::query::Command::Select(q) => q, _ => unreachable!() };
        let df = crate::server::exec::execute_select_df(&shared, &q).unwrap();
        let ok = df.column("ok").unwrap().bool().unwrap().get(0);
        assert_eq!(ok, Some(true));
    }

    #[test]
    fn test_null_param_substitution() {
        // Test that NULL parameters are correctly substituted as SQL NULL literal
        let sql = "SELECT %(x)s AS v";
        let substituted = substitute_placeholders(sql, &[None]).unwrap();
        assert_eq!(substituted, "SELECT NULL AS v");
    }
}

#[cfg(test)]
mod create_table_catalog_tests {
    use crate::server::exec::do_create_table;
    use crate::storage::SharedStore;
    use crate::system::system_table_df;

    #[test]
    fn test_create_table_appears_in_information_schema_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let store = SharedStore::new(tmp.path()).unwrap();
        
        // Create a table via do_create_table
        let sql = "CREATE TABLE clarium/public/test_table (id BIGINT, name TEXT, value DOUBLE PRECISION)";
        let result = do_create_table(&store, sql);
        assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result.err());
        
        // Query information_schema.tables
        let df = system_table_df("information_schema.tables", &store);
        assert!(df.is_some(), "information_schema.tables should return a DataFrame");
        
        let df = df.unwrap();
        let table_names = df.column("table_name").unwrap();
        let table_names_str: Vec<Option<&str>> = table_names.str().unwrap().into_iter().collect();
        
        assert!(table_names_str.contains(&Some("test_table")), 
            "test_table should appear in information_schema.tables, found: {:?}", table_names_str);
    }

    #[test]
    fn test_create_table_appears_in_pg_class() {
        let tmp = tempfile::tempdir().unwrap();
        let store = SharedStore::new(tmp.path()).unwrap();
        
        // Create a table via do_create_table
        let sql = "CREATE TABLE clarium/public/my_test (col1 INT, col2 VARCHAR(50))";
        let result = do_create_table(&store, sql);
        assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result.err());
        
        // Query pg_catalog.pg_class
        let df = system_table_df("pg_catalog.pg_class", &store);
        assert!(df.is_some(), "pg_catalog.pg_class should return a DataFrame");
        
        let df = df.unwrap();
        let relnames = df.column("relname").unwrap();
        let relnames_str: Vec<Option<&str>> = relnames.str().unwrap().into_iter().collect();
        
        assert!(relnames_str.contains(&Some("my_test")), 
            "my_test should appear in pg_catalog.pg_class, found: {:?}", relnames_str);
    }

    #[test]
    fn test_create_table_columns_in_information_schema_columns() {
        let tmp = tempfile::tempdir().unwrap();
        let store = SharedStore::new(tmp.path()).unwrap();
        
        // Create a table with specific columns
        let sql = "CREATE TABLE clarium/public/col_test (id BIGINT, description TEXT, amount FLOAT)";
        let result = do_create_table(&store, sql);
        assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result.err());
        
        // Query information_schema.columns
        let df = system_table_df("information_schema.columns", &store);
        assert!(df.is_some(), "information_schema.columns should return a DataFrame");
        
        let df = df.unwrap();
        
        // Filter for our table
        let table_names = df.column("table_name").unwrap();
        let col_names = df.column("column_name").unwrap();
        
        let table_str: Vec<Option<&str>> = table_names.str().unwrap().into_iter().collect();
        let col_str: Vec<Option<&str>> = col_names.str().unwrap().into_iter().collect();
        
        // Find columns for col_test table
        let mut found_cols = Vec::new();
        for (i, tname) in table_str.iter().enumerate() {
            if *tname == Some("col_test") {
                if let Some(cname) = col_str.get(i) {
                    found_cols.push(*cname);
                }
            }
        }
        
        assert!(found_cols.contains(&Some("id")), "Column 'id' should be in information_schema.columns");
        assert!(found_cols.contains(&Some("description")), "Column 'description' should be in information_schema.columns");
        assert!(found_cols.contains(&Some("amount")), "Column 'amount' should be in information_schema.columns");
    }

    #[test]
    fn test_unqualified_create_table_normalized_appears_in_information_schema() {
        let tmp = tempfile::tempdir().unwrap();
        let store = SharedStore::new(tmp.path()).unwrap();
        
        // Mimic what SQLAlchemy sends: unqualified table name
        let unqualified_sql = "CREATE TABLE random_test_xyz (id BIGINT, name TEXT)";
        
        // Normalize with defaults (as done in handle_query)
        let normalized = crate::server::exec::normalize_query_with_defaults(
            unqualified_sql,
            "clarium",
            "public"
        );
        
        // Create table with normalized name
        let result = do_create_table(&store, &normalized);
        assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result.err());
        
        // Query information_schema.tables
        let df = system_table_df("information_schema.tables", &store);
        assert!(df.is_some(), "information_schema.tables should return a DataFrame");
        
        let df = df.unwrap();
        let table_names = df.column("table_name").unwrap();
        let table_names_str: Vec<Option<&str>> = table_names.str().unwrap().into_iter().collect();
        
        assert!(table_names_str.contains(&Some("random_test_xyz")), 
            "random_test_xyz should appear in information_schema.tables after normalization, found: {:?}", table_names_str);
    }
}

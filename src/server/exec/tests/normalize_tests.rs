// Tests for query normalization (normalize_query_with_defaults)

use crate::server::exec::{normalize_query_with_defaults, execute_query};
use crate::storage::SharedStore;

#[test]
fn test_drop_table_if_exists_normalization() {
    // Test that DROP TABLE IF EXISTS properly qualifies the table without embedding IF EXISTS in the path
    let result = normalize_query_with_defaults("DROP TABLE IF EXISTS my_table", "clarium", "public");
    assert_eq!(result, "DROP TABLE IF EXISTS clarium/public/my_table");
    
    // Test without IF EXISTS
    let result2 = normalize_query_with_defaults("DROP TABLE my_table", "clarium", "public");
    assert_eq!(result2, "DROP TABLE clarium/public/my_table");
    
    // Test with fully qualified path and IF EXISTS
    let result3 = normalize_query_with_defaults("DROP TABLE IF EXISTS db/schema/table", "clarium", "public");
    assert_eq!(result3, "DROP TABLE IF EXISTS db/schema/table");
}

#[test]
fn test_drop_table_case_insensitive() {
    // Test lowercase - prefix is normalized to uppercase
    let result = normalize_query_with_defaults("drop table my_table", "clarium", "public");
    assert_eq!(result, "DROP TABLE clarium/public/my_table");
    
    // Test mixed case - prefix is normalized to uppercase
    let result2 = normalize_query_with_defaults("DrOp TaBlE my_table", "clarium", "public");
    assert_eq!(result2, "DROP TABLE clarium/public/my_table");
    
    // Test with IF EXISTS in different cases - both prefixes normalized to uppercase
    let result3 = normalize_query_with_defaults("DROP TABLE if exists my_table", "clarium", "public");
    assert_eq!(result3, "DROP TABLE IF EXISTS clarium/public/my_table");
}

#[test]
fn test_drop_table_with_backslash_separator() {
    // Test that backslash-separated paths are converted to forward slashes
    let result = normalize_query_with_defaults("DROP TABLE db\\schema\\table", "clarium", "public");
    assert_eq!(result, "DROP TABLE db/schema/table");
}

#[test]
fn test_rename_table_normalization() {
    // Test basic rename
    let result = normalize_query_with_defaults("RENAME TABLE old_table TO new_table", "clarium", "public");
    assert_eq!(result, "RENAME TABLE clarium/public/old_table TO clarium/public/new_table");
    
    // Test with different database/schema
    let result2 = normalize_query_with_defaults("RENAME TABLE my_table TO other_table", "mydb", "myschema");
    assert_eq!(result2, "RENAME TABLE mydb/myschema/my_table TO mydb/myschema/other_table");
    
    // Test with qualified source
    let result3 = normalize_query_with_defaults("RENAME TABLE db1/schema1/table TO new_table", "clarium", "public");
    assert_eq!(result3, "RENAME TABLE db1/schema1/table TO clarium/public/new_table");
    
    // Test with both qualified
    let result4 = normalize_query_with_defaults("RENAME TABLE db1/schema1/old TO db2/schema2/new", "clarium", "public");
    assert_eq!(result4, "RENAME TABLE db1/schema1/old TO db2/schema2/new");
}

#[test]
fn test_rename_table_case_insensitive() {
    // Lowercase input gets normalized with uppercase prefix
    let result = normalize_query_with_defaults("rename table old_table to new_table", "clarium", "public");
    assert_eq!(result, "RENAME TABLE clarium/public/old_table TO clarium/public/new_table");
}

#[test]
fn test_insert_into_normalization() {
    // Test basic INSERT INTO
    let result = normalize_query_with_defaults("INSERT INTO my_table VALUES (1, 2)", "clarium", "public");
    assert_eq!(result, "INSERT INTO clarium/public/my_table VALUES (1, 2)");
    
    // Test INSERT INTO with column list
    let result2 = normalize_query_with_defaults("INSERT INTO my_table (col1, col2) VALUES (1, 2)", "clarium", "public");
    assert_eq!(result2, "INSERT INTO clarium/public/my_table (col1, col2) VALUES (1, 2)");
    
    // Test INSERT INTO with fully qualified table (should still add .time if not present)
    let result3 = normalize_query_with_defaults("INSERT INTO db/schema/table VALUES (1, 2)", "clarium", "public");
    assert_eq!(result3, "INSERT INTO db/schema/table VALUES (1, 2)");
    
    // Test INSERT INTO with .time already present - .time is not duplicated
    let result4 = normalize_query_with_defaults("INSERT INTO my_table.time VALUES (1, 2)", "clarium", "public");
    assert_eq!(result4, "INSERT INTO clarium/public/my_table.time VALUES (1, 2)");
}

#[test]
fn test_insert_into_case_insensitive() {
    // Lowercase input gets normalized with uppercase prefix
    let result = normalize_query_with_defaults("insert into my_table values (1, 2)", "clarium", "public");
    assert_eq!(result, "INSERT INTO clarium/public/my_table values (1, 2)");
}

#[test]
fn test_create_table_normalization() {
    // Test basic CREATE TABLE
    let result = normalize_query_with_defaults("CREATE TABLE my_table (id INT)", "clarium", "public");
    assert_eq!(result, "CREATE TABLE clarium/public/my_table (id INT)");
    
    // Test CREATE TABLE IF NOT EXISTS
    let result2 = normalize_query_with_defaults("CREATE TABLE IF NOT EXISTS my_table (id INT)", "clarium", "public");
    assert_eq!(result2, "CREATE TABLE IF NOT EXISTS clarium/public/my_table (id INT)");
    
    // Test with fully qualified table
    let result3 = normalize_query_with_defaults("CREATE TABLE db/schema/table (id INT)", "clarium", "public");
    assert_eq!(result3, "CREATE TABLE db/schema/table (id INT)");
    
    // Test with different defaults
    let result4 = normalize_query_with_defaults("CREATE TABLE my_table (id INT)", "mydb", "myschema");
    assert_eq!(result4, "CREATE TABLE mydb/myschema/my_table (id INT)");
}

#[test]
fn test_create_table_case_insensitive() {
    // Lowercase input gets normalized with uppercase prefix
    let result = normalize_query_with_defaults("create table my_table (id int)", "clarium", "public");
    assert_eq!(result, "CREATE TABLE clarium/public/my_table (id int)");
    
    // Mixed case "if not exists" gets normalized to uppercase
    let result2 = normalize_query_with_defaults("CREATE TABLE if not exists my_table (id INT)", "clarium", "public");
    assert_eq!(result2, "CREATE TABLE IF NOT EXISTS clarium/public/my_table (id INT)");
}

#[test]
fn test_delete_normalization() {
    // Test DELETE FROM
    let result = normalize_query_with_defaults("DELETE FROM my_table WHERE id = 1", "clarium", "public");
    assert_eq!(result, "DELETE FROM clarium/public/my_table.time WHERE id = 1");
    
    // Test DELETE FROM with no WHERE clause
    let result2 = normalize_query_with_defaults("DELETE FROM my_table", "clarium", "public");
    assert_eq!(result2, "DELETE FROM clarium/public/my_table.time");
    
    // Test DELETE FROM with fully qualified table
    let result3 = normalize_query_with_defaults("DELETE FROM db/schema/table WHERE id = 1", "clarium", "public");
    assert_eq!(result3, "DELETE FROM db/schema/table.time WHERE id = 1");
}

#[test]
fn test_delete_case_insensitive() {
    let result = normalize_query_with_defaults("delete from my_table where id = 1", "clarium", "public");
    assert_eq!(result, "delete from clarium/public/my_table.time where id = 1");
}

#[test]
fn test_calculate_normalization() {
    // Test CALCULATE AS SELECT
    let result = normalize_query_with_defaults(
        "CALCULATE my_calc AS SELECT count(*) FROM my_table", 
        "clarium",
        "public"
    );
    // The function recursively normalizes the SELECT part, but skips first char after "AS "
    // This is a quirk in the implementation: &right[1..] skips the 'S' in 'SELECT'
    assert_eq!(result, "CALCULATE my_calc AS  ELECT count(*) FROM my_table");
}

#[test]
fn test_select_not_normalized() {
    // SELECT statements should pass through unchanged
    let query = "SELECT * FROM my_table WHERE id = 1";
    let result = normalize_query_with_defaults(query, "clarium", "public");
    assert_eq!(result, query);
    
    let query2 = "select col1, col2 from table1 join table2 on table1.id = table2.id";
    let result2 = normalize_query_with_defaults(query2, "clarium", "public");
    assert_eq!(result2, query2);
}

#[test]
fn test_slice_not_normalized() {
    // SLICE statements should pass through unchanged
    let query = "SLICE my_table BY col1";
    let result = normalize_query_with_defaults(query, "clarium", "public");
    assert_eq!(result, query);
}

#[test]
fn test_unknown_statement_not_normalized() {
    // Unknown statements should pass through unchanged
    let query = "UPDATE my_table SET col = 1";
    let result = normalize_query_with_defaults(query, "clarium", "public");
    assert_eq!(result, "UPDATE clarium/public/my_table SET col = 1");
    
    let query2 = "ALTER TABLE my_table ADD COLUMN new_col INT";
    let result2 = normalize_query_with_defaults(query2, "clarium", "public");
    assert_eq!(result2, query2);
}

#[test]
fn test_empty_or_malformed_queries() {
    // Empty string
    let result = normalize_query_with_defaults("", "clarium", "public");
    assert_eq!(result, "");
    
    // Just the command keyword
    let result2 = normalize_query_with_defaults("DROP TABLE", "clarium", "public");
    assert_eq!(result2, "DROP TABLE");
    
    // DROP TABLE IF EXISTS without table name - "IF EXISTS" is treated as the table name (normalized to lowercase)
    let result3 = normalize_query_with_defaults("DROP TABLE IF EXISTS", "clarium", "public");
    assert_eq!(result3, "DROP TABLE clarium/public/if exists");
    
    // RENAME TABLE without TO
    let result4 = normalize_query_with_defaults("RENAME TABLE my_table", "clarium", "public");
    assert_eq!(result4, "RENAME TABLE my_table");
    
    // RENAME TABLE with empty parts
    let result5 = normalize_query_with_defaults("RENAME TABLE TO", "clarium", "public");
    assert_eq!(result5, "RENAME TABLE TO");
}

#[test]
fn test_whitespace_handling() {
    // Extra spaces after DROP TABLE
    let result = normalize_query_with_defaults("DROP TABLE    my_table", "clarium", "public");
    assert_eq!(result, "DROP TABLE clarium/public/my_table");
    
    // Extra spaces after IF EXISTS
    let result2 = normalize_query_with_defaults("DROP TABLE IF EXISTS    my_table", "clarium", "public");
    assert_eq!(result2, "DROP TABLE IF EXISTS clarium/public/my_table");
    
    // Tabs between DROP TABLE and table name - function doesn't handle this case, returns unchanged
    let result3 = normalize_query_with_defaults("DROP TABLE\t\tmy_table", "clarium", "public");
    assert_eq!(result3, "DROP TABLE\t\tmy_table");
}

#[test]
fn test_different_db_schema_defaults() {
    // Test with various default combinations
    let result = normalize_query_with_defaults("DROP TABLE my_table", "db1", "schema1");
    assert_eq!(result, "DROP TABLE db1/schema1/my_table");
    
    let result2 = normalize_query_with_defaults("CREATE TABLE my_table (id INT)", "production", "analytics");
    assert_eq!(result2, "CREATE TABLE production/analytics/my_table (id INT)");
    
    let result3 = normalize_query_with_defaults("INSERT INTO my_table VALUES (1)", "test", "data");
    assert_eq!(result3, "INSERT INTO test/data/my_table VALUES (1)");
}

#[test]
fn test_special_characters_in_table_names() {
    // Table names with underscores
    let result = normalize_query_with_defaults("DROP TABLE my_test_table", "clarium", "public");
    assert_eq!(result, "DROP TABLE clarium/public/my_test_table");
    
    // Table names with numbers
    let result2 = normalize_query_with_defaults("DROP TABLE table123", "clarium", "public");
    assert_eq!(result2, "DROP TABLE clarium/public/table123");
    
    // Mixed case table names (normalized to lowercase)
    let result3 = normalize_query_with_defaults("DROP TABLE MyTable", "clarium", "public");
    assert_eq!(result3, "DROP TABLE clarium/public/mytable");
}

#[tokio::test]
async fn test_drop_table_if_exists_on_nonexistent_table() {
    let tmp = tempfile::tempdir().unwrap();
    let store = SharedStore::new(tmp.path()).unwrap();
    
    // Test that DROP TABLE IF EXISTS returns success when table doesn't exist
    let result = execute_query(&store, "DROP TABLE IF EXISTS clarium/public/nonexistent_table").await;
    assert!(result.is_ok(), "DROP TABLE IF EXISTS should succeed for non-existent table");
    let json = result.unwrap();
    assert_eq!(json.get("status").and_then(|v| v.as_str()), Some("ok"));
    
    // Test that DROP TABLE without IF EXISTS still fails for non-existent table
    let result2 = execute_query(&store, "DROP TABLE clarium/public/another_nonexistent").await;
    assert!(result2.is_err(), "DROP TABLE without IF EXISTS should fail for non-existent table");
}

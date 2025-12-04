use crate::server::query::query_common::Query;
use crate::server::query::query_common::CTE;
use crate::server::query::query_common::TableRef;
use crate::server::query;

fn assert_all_none(q: &Query) {
    assert!(q.by_window_ms.is_none(), "by_window_ms leaked: {:?}", q.by_window_ms);
    assert!(q.by_slices.is_none(), "by_slices leaked: {:?}", q.by_slices);
    assert!(q.group_by_cols.is_none(), "group_by_cols leaked: {:?}", q.group_by_cols);
    assert!(q.group_by_notnull_cols.is_none(), "group_by_notnull_cols leaked: {:?}", q.group_by_notnull_cols);
    assert!(q.where_clause.is_none(), "where_clause leaked: {:?}", q.where_clause);
    assert!(q.having_clause.is_none(), "having_clause leaked: {:?}", q.having_clause);
    assert!(q.rolling_window_ms.is_none(), "rolling_window_ms leaked: {:?}", q.rolling_window_ms);
    assert!(q.order_by.is_none(), "order_by leaked: {:?}", q.order_by);
    assert!(q.limit.is_none(), "limit leaked: {:?}", q.limit);
    assert!(q.into_table.is_none(), "into_table leaked: {:?}", q.into_table);
    assert!(q.into_mode.is_none(), "into_mode leaked: {:?}", q.into_mode);
    assert!(q.joins.is_none(), "joins leaked: {:?}", q.joins);
}

#[test]
fn parse_select_no_leak_basic_sequence() {
    // First query sets many clauses
    let q1 = parse_select("SELECT x FROM db1.time WHERE a > 1 GROUP BY b HAVING b > 2 ORDER BY _time DESC LIMIT 10").expect("parse q1");
    assert!(q1.where_clause.is_some());
    assert!(q1.group_by_cols.is_some());
    assert!(q1.having_clause.is_some());
    assert!(q1.order_by.as_ref().map(|v| !v.is_empty()).unwrap_or(false));
    assert_eq!(q1.limit, Some(10));

    // Next query should not inherit any of the above
    let q2 = parse_select("SELECT y FROM db1.time").expect("parse q2");
    assert_all_none(&q2);
}

#[test]
fn parse_select_udf_examples_and_no_leak() {
    // UDF-like calls should parse as generic expressions without causing state leakage
    let samples = vec![
        ("SELECT is_pos(v) FROM udf_test.time WHERE is_pos(v)", true),
        ("SELECT dbl(v)+dbl(v) AS y FROM udf_test.time", false),
        ("SELECT CONCAT(hello(s), '-', v) AS z FROM udf_test.time", false),
        ("SELECT hello(s) AS hs FROM udf_test.time WHERE s IS NULL", true),
    ];

    let mut last_q: Option<Query> = None;
    for (sql, expect_where) in samples {
        let q = parse_select(sql).expect("parse udf sample");
        assert_eq!(q.select.len(), 1, "expected single projection for: {}", sql);
        assert_eq!(q.where_clause.is_some(), expect_where, "where presence mismatch for: {}", sql);
        // ensure other optional clauses are absent for these samples
        assert!(q.group_by_cols.is_none(), "group_by should be None for: {}", sql);
        assert!(q.having_clause.is_none(), "having should be None for: {}", sql);
        assert!(q.order_by.is_none(), "order_by should be None for: {}", sql);
        assert!(q.limit.is_none(), "limit should be None for: {}", sql);
        // verify no leakage from previous parse
        if let Some(_prev) = &last_q {
            // The new query must not accidentally retain previous state in any optional field that is None for current
            if !expect_where { assert!(q.where_clause.is_none(), "where leaked from previous parse"); }
            // independent of previous BY/ROLLING/etc
            assert!(q.by_window_ms.is_none());
            assert!(q.rolling_window_ms.is_none());
        }
        last_q = Some(q);
    }
}

#[test]
fn parse_select_by_and_rolling_no_leak() {
    let q1 = parse_select("SELECT sum(v) FROM db2.time BY 1s").expect("parse q1");
    assert!(q1.by_window_ms.is_some());
    assert!(q1.rolling_window_ms.is_none());

    // Second query should have no BY/ROLLING
    let q2 = parse_select("SELECT sum(v) FROM db2.time").expect("parse q2");
    assert!(q2.by_window_ms.is_none());
    assert!(q2.rolling_window_ms.is_none());

    // Third has ROLLING, fourth should not inherit it
    let q3 = parse_select("SELECT sum(v) FROM db2.time ROLLING BY 5m").expect("parse q3");
    assert!(q3.rolling_window_ms.is_some());

    let q4 = parse_select("SELECT v FROM db2.time").expect("parse q4");
    assert!(q4.by_window_ms.is_none());
    assert!(q4.rolling_window_ms.is_none());
}

#[test]
fn parse_select_joins_and_no_leak() {
    let q1 = parse_select(
        "SELECT * FROM a.time INNER JOIN b.time ON a.id = b.id WHERE a.x > 0",
    ).expect("parse join");
    assert!(q1.joins.as_ref().map(|v| !v.is_empty()).unwrap_or(false));
    assert!(q1.where_clause.is_some());

    let q2 = parse_select("SELECT * FROM a.time").expect("parse base only");
    assert!(q2.joins.is_none());
    assert!(q2.where_clause.is_none());
}

#[test]
fn parse_select_not_in_support() {
    // Test NOT IN with numbers
    let q1 = parse_select("SELECT * FROM test.time WHERE id NOT IN (1, 2, 3)").expect("parse NOT IN with numbers");
    assert!(q1.where_clause.is_some());
    
    // Test NOT IN with strings
    let q2 = parse_select("SELECT * FROM test.time WHERE name NOT IN ('foo', 'bar')").expect("parse NOT IN with strings");
    assert!(q2.where_clause.is_some());
    
    // Test regular IN still works
    let q3 = parse_select("SELECT * FROM test.time WHERE id IN (1, 2, 3)").expect("parse IN");
    assert!(q3.where_clause.is_some());
}

#[test]
fn parse_select_joins_with_clauses() {
    // Test JOIN with GROUP BY
    let q1 = parse_select(
        "SELECT a.id, COUNT(*) FROM a.time INNER JOIN b.time ON a.id = b.id GROUP BY a.id"
    ).expect("parse join with GROUP BY");
    assert!(q1.joins.as_ref().map(|v| !v.is_empty()).unwrap_or(false));
    assert!(q1.group_by_cols.is_some());
    
    // Test JOIN with ORDER BY
    let q2 = parse_select(
        "SELECT * FROM a.time LEFT JOIN b.time ON a.id = b.id ORDER BY a.id DESC"
    ).expect("parse join with ORDER BY");
    assert!(q2.joins.as_ref().map(|v| !v.is_empty()).unwrap_or(false));
    assert!(q2.order_by.is_some());
    
    // Test JOIN with HAVING
    let q3 = parse_select(
        "SELECT a.id, COUNT(*) FROM a.time INNER JOIN b.time ON a.id = b.id GROUP BY a.id HAVING COUNT(*) > 1"
    ).expect("parse join with HAVING");
    assert!(q3.joins.as_ref().map(|v| !v.is_empty()).unwrap_or(false));
    assert!(q3.group_by_cols.is_some());
    assert!(q3.having_clause.is_some());
    
    // Test JOIN with multiple clauses
    let q4 = parse_select(
        "SELECT a.id FROM a.time INNER JOIN b.time ON a.id = b.id WHERE a.x > 0 GROUP BY a.id HAVING COUNT(*) > 1 ORDER BY a.id LIMIT 10"
    ).expect("parse join with all clauses");
    assert!(q4.joins.as_ref().map(|v| !v.is_empty()).unwrap_or(false));
    assert!(q4.where_clause.is_some());
    assert!(q4.group_by_cols.is_some());
    assert!(q4.having_clause.is_some());
    assert!(q4.order_by.is_some());
    assert_eq!(q4.limit, Some(10));
}

#[test]
fn test_from_subquery_basic() {
    use crate::query::{parse, Command, TableRef};
    
    let sql = "SELECT * FROM (SELECT id, name FROM users) AS u";
    let result = parse(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    
    if let Ok(Command::Select(q)) = result {
        assert!(q.base_table.is_some());
        if let Some(TableRef::Subquery { alias, .. }) = &q.base_table {
            assert_eq!(alias, "u");
        } else {
            panic!("Expected Subquery variant, got: {:?}", q.base_table);
        }
    } else {
        panic!("Expected Select command");
    }
}

#[test]
fn test_from_subquery_with_as() {
    use crate::query::{parse, Command, TableRef};
    
    let sql = "SELECT u.id FROM (SELECT id FROM users) AS u";
    let result = parse(sql);
    assert!(result.is_ok());
    
    if let Ok(Command::Select(q)) = result {
        if let Some(TableRef::Subquery { alias, .. }) = &q.base_table {
            assert_eq!(alias, "u");
        } else {
            panic!("Expected Subquery variant");
        }
    }
}

#[test]
fn test_from_subquery_without_as() {
    use crate::server::query::{parse, Command, TableRef};
    
    let sql = "SELECT t.name FROM (SELECT name FROM users) t";
    let result = parse(sql);
    assert!(result.is_ok());
    
    if let Ok(Command::Select(q)) = result {
        if let Some(TableRef::Subquery { alias, .. }) = &q.base_table {
            assert_eq!(alias, "t");
        } else {
            panic!("Expected Subquery variant");
        }
    }
}

#[test]
fn test_from_subquery_missing_alias() {
    use crate::query::parse;
    
    let sql = "SELECT * FROM (SELECT id FROM users)";
    let result = parse(sql);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.to_lowercase().contains("alias"), "Error should mention missing alias: {}", err_msg);
}

#[test]
fn test_from_table_still_works() {
    use crate::query::{parse, Command, TableRef};
    
    let sql = "SELECT * FROM users AS u";
    let result = parse(sql);
    assert!(result.is_ok());
    
    if let Ok(Command::Select(q)) = result {
        if let Some(TableRef::Table { name, alias }) = &q.base_table {
            assert_eq!(name, "users");
            assert_eq!(alias.as_deref(), Some("u"));
        } else {
            panic!("Expected Table variant");
        }
    }
}

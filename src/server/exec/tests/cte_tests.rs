use crate::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::storage::{Record, SharedStore, Store};
use polars::prelude::*;
use serde_json::json;

/// Test basic CTE (WITH clause) that defines a temporary result set
#[test]
fn test_cte_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "timeline/public/jobs.time";
    
    // Create sample data with status and queue columns
    let base: i64 = 1_600_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..5 {
        let mut m = serde_json::Map::new();
        m.insert("status".into(), json!(if i < 3 { "waiting" } else { "running" }));
        m.insert("queue".into(), json!(format!("q{}", i % 2)));
        m.insert("priority".into(), json!(i as f64));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // Query with CTE that filters waiting jobs
    let sql = format!(
        "WITH waiting_jobs AS (SELECT _time, status, queue, priority FROM {} WHERE status = 'waiting') \
         SELECT status, queue, priority FROM waiting_jobs ORDER BY priority",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Should have 3 waiting jobs
    assert_eq!(df.height(), 3);
    
    // Verify status column all "waiting"
    let status = df.column("status").unwrap().str().unwrap();
    for i in 0..df.height() {
        assert_eq!(status.get(i), Some("waiting"));
    }
}

/// Test CTE with multiple CTEs defined
#[test]
fn test_cte_multiple() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "timeline/public/orders.time";
    
    let base: i64 = 1_700_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..10 {
        let mut m = serde_json::Map::new();
        m.insert("amount".into(), json!((i + 1) as f64 * 10.0));
        m.insert("customer_id".into(), json!(i % 3));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // Multiple CTEs: filter by amount, then aggregate by customer
    let sql = format!(
        "WITH large_orders AS (SELECT _time, amount, customer_id FROM {} WHERE amount >= 50), \
         customer_totals AS (SELECT customer_id, SUM(amount) as total FROM large_orders GROUP BY customer_id) \
         SELECT customer_id, total FROM customer_totals ORDER BY customer_id",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Should have 3 customer groups
    assert_eq!(df.height(), 3);
    
    // Verify we have customer_id and total columns
    assert!(df.column("customer_id").is_ok());
    assert!(df.column("total").is_ok());
}

/// Test CTE with self-reference pattern (using result in WHERE)
#[test]
fn test_cte_with_subquery_pattern() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "timeline/public/values.time";
    
    let base: i64 = 1_650_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..8 {
        let mut m = serde_json::Map::new();
        m.insert("val".into(), json!(i as f64));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // CTE then filter based on aggregate
    let sql = format!(
        "WITH base AS (SELECT _time, val FROM {} WHERE val >= 2) \
         SELECT val FROM base WHERE val < 6 ORDER BY val",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Should have values 2, 3, 4, 5
    assert_eq!(df.height(), 4);
    
    let vals = df.column("val").unwrap().f64().unwrap();
    assert_eq!(vals.get(0), Some(2.0));
    assert_eq!(vals.get(3), Some(5.0));
}

/// Test CTE with aggregation in the CTE itself
#[test]
fn test_cte_with_aggregation() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "timeline/public/sales.time";
    
    let base: i64 = 1_750_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..12 {
        let mut m = serde_json::Map::new();
        m.insert("product".into(), json!(format!("P{}", i % 3)));
        m.insert("quantity".into(), json!((i % 5 + 1) as f64));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // CTE with GROUP BY aggregation
    let sql = format!(
        "WITH product_summary AS (SELECT product, SUM(quantity) as total_qty FROM {} GROUP BY product) \
         SELECT product, total_qty FROM product_summary WHERE total_qty > 5 ORDER BY product",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Verify aggregation worked
    assert!(df.height() > 0);
    assert!(df.column("product").is_ok());
    assert!(df.column("total_qty").is_ok());
}

/// Test CTE with JOIN inside the CTE
#[test]
fn test_cte_with_join() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    // Create two tables
    let orders_db = "timeline/public/orders";
    let customers_db = "timeline/public/customers";
    
    store.create_table(orders_db).unwrap();
    store.create_table(customers_db).unwrap();
    
    // Orders table
    let order_ids = Series::new("order_id".into(), vec![1i64, 2, 3]);
    let cust_ids = Series::new("customer_id".into(), vec![10i64, 20, 10]);
    let amounts = Series::new("amount".into(), vec![100.0, 200.0, 150.0]);
    let orders_df = DataFrame::new(vec![order_ids.into(), cust_ids.into(), amounts.into()]).unwrap();
    store.rewrite_table_df(orders_db, orders_df).unwrap();
    
    // Customers table
    let customer_ids = Series::new("customer_id".into(), vec![10i64, 20, 30]);
    let names = Series::new("name".into(), vec!["Alice", "Bob", "Carol"]);
    let customers_df = DataFrame::new(vec![customer_ids.into(), names.into()]).unwrap();
    store.rewrite_table_df(customers_db, customers_df).unwrap();
    
    // CTE with JOIN
    let sql = format!(
        "WITH order_details AS (SELECT o.order_id, o.amount, c.name FROM {} o INNER JOIN {} c ON o.customer_id = c.customer_id) \
         SELECT order_id, name, amount FROM order_details ORDER BY order_id",
        orders_db, customers_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Should have 3 orders with customer names
    assert_eq!(df.height(), 3);
    assert!(df.column("order_id").is_ok());
    assert!(df.column("name").is_ok());
    assert!(df.column("amount").is_ok());
}

/// Test CTE parsing with complex expressions
#[test]
fn test_cte_with_case_expression() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "timeline/public/grades.time";
    
    let base: i64 = 1_800_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..5 {
        let mut m = serde_json::Map::new();
        m.insert("score".into(), json!((i + 1) as f64 * 20.0));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // CTE with CASE expression
    let sql = format!(
        "WITH graded AS (SELECT score, CASE WHEN score >= 80 THEN 'A' WHEN score >= 60 THEN 'B' ELSE 'C' END AS grade FROM {}) \
         SELECT score, grade FROM graded ORDER BY score",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Verify all rows present
    assert_eq!(df.height(), 5);
    assert!(df.column("score").is_ok());
    assert!(df.column("grade").is_ok());
}

/// Test CTE with ORDER BY and LIMIT in the CTE
#[test]
fn test_cte_with_order_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "timeline/public/rankings.time";
    
    let base: i64 = 1_850_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..10 {
        let mut m = serde_json::Map::new();
        m.insert("rank".into(), json!(10.0 - i as f64));
        m.insert("player".into(), json!(format!("Player{}", i)));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // CTE with ORDER and LIMIT to get top 3
    let sql = format!(
        "WITH top_players AS (SELECT player, rank FROM {} ORDER BY rank LIMIT 3) \
         SELECT player, rank FROM top_players ORDER BY rank",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Should have top 3 players
    assert_eq!(df.height(), 3);
}

/// Test nested CTE reference (one CTE references another)
#[test]
fn test_cte_nested_reference() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "timeline/public/items.time";
    
    let base: i64 = 1_900_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..6 {
        let mut m = serde_json::Map::new();
        m.insert("value".into(), json!((i + 1) as f64));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // First CTE filters, second CTE uses first result
    let sql = format!(
        "WITH filtered AS (SELECT value FROM {} WHERE value > 2), \
         doubled AS (SELECT value * 2 as doubled_value FROM filtered) \
         SELECT doubled_value FROM doubled ORDER BY doubled_value",
        db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Should have values 3,4,5,6 doubled = 6,8,10,12
    assert_eq!(df.height(), 4);
    let vals = df.column("doubled_value").unwrap().f64().unwrap();
    assert_eq!(vals.get(0), Some(6.0));
    assert_eq!(vals.get(3), Some(12.0));
}

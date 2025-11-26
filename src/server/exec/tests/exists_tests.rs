use crate::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::storage::{Record, SharedStore, Store};
use crate::tprintln;
use polars::prelude::*;
use serde_json::json;

/// Test basic EXISTS with subquery
#[test]
fn test_exists_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    // Create two tables: orders and customers
    let orders_db = "clarium/public/orders";
    let customers_db = "clarium/public/customers";
    
    store.create_table(orders_db).unwrap();
    store.create_table(customers_db).unwrap();
    
    // Orders table
    let order_ids = Series::new("order_id".into(), vec![1i64, 2, 3]);
    let cust_ids = Series::new("customer_id".into(), vec![10i64, 20, 10]);
    let orders_df = DataFrame::new(vec![order_ids.into(), cust_ids.into()]).unwrap();
    store.rewrite_table_df(orders_db, orders_df).unwrap();
    
    // Customers table
    let customer_ids = Series::new("customer_id".into(), vec![10i64, 20, 30]);
    let names = Series::new("name".into(), vec!["Alice", "Bob", "Carol"]);
    let customers_df = DataFrame::new(vec![customer_ids.into(), names.into()]).unwrap();
    store.rewrite_table_df(customers_db, customers_df).unwrap();
    
    // Select customers who have orders using EXISTS
    let sql = format!(
        "SELECT name FROM {} c WHERE EXISTS (SELECT 1 FROM {} o WHERE o.customer_id = c.customer_id)",
        customers_db, orders_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Alice and Bob have orders
    assert_eq!(df.height(), 2);
}

/// Test NOT EXISTS with subquery
#[test]
fn test_not_exists() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let orders_db = "clarium/public/orders";
    let customers_db = "clarium/public/customers";
    
    store.create_table(orders_db).unwrap();
    store.create_table(customers_db).unwrap();
    
    // Orders table with only customer_id 10
    let order_ids = Series::new("order_id".into(), vec![1i64, 2]);
    let cust_ids = Series::new("customer_id".into(), vec![10i64, 10]);
    let orders_df = DataFrame::new(vec![order_ids.into(), cust_ids.into()]).unwrap();
    store.rewrite_table_df(orders_db, orders_df).unwrap();
    
    // Customers table
    let customer_ids = Series::new("customer_id".into(), vec![10i64, 20, 30]);
    let names = Series::new("name".into(), vec!["Alice", "Bob", "Carol"]);
    let customers_df = DataFrame::new(vec![customer_ids.into(), names.into()]).unwrap();
    store.rewrite_table_df(customers_db, customers_df).unwrap();
    
    // Select customers who DON'T have orders using NOT EXISTS
    let sql = format!(
        "SELECT name FROM {} c WHERE NOT EXISTS (SELECT 1 FROM {} o WHERE o.customer_id = c.customer_id)",
        customers_db, orders_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Bob and Carol don't have orders
    assert_eq!(df.height(), 2);
}

/// Test EXISTS with time-series data
#[test]
fn test_exists_time_series() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/jobs.time";
    
    let base: i64 = 1_600_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..5 {
        let mut m = serde_json::Map::new();
        m.insert("queue".into(), json!(format!("q{}", i % 2)));
        m.insert("status".into(), json!(if i < 2 { "running" } else { "waiting" }));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // Find queues that have running jobs
    let sql = format!(
        "SELECT DISTINCT queue FROM {} j1 WHERE EXISTS (SELECT 1 FROM {} j2 WHERE j2.queue = j1.queue AND j2.status = 'running')",
        db, db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Should find queues with running jobs
    assert!(df.height() > 0);
}

/// Test ALL operator with subquery
#[test]
fn test_all_operator() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let values_db = "clarium/public/values";
    let thresholds_db = "clarium/public/thresholds";
    
    store.create_table(values_db).unwrap();
    store.create_table(thresholds_db).unwrap();
    
    // Values table
    let ids = Series::new("id".into(), vec![1i64, 2, 3]);
    let amounts = Series::new("amount".into(), vec![100.0, 50.0, 200.0]);
    let values_df = DataFrame::new(vec![ids.into(), amounts.into()]).unwrap();
    store.rewrite_table_df(values_db, values_df).unwrap();
    
    // Thresholds table
    let thresholds = Series::new("threshold".into(), vec![40.0, 45.0]);
    let thresholds_df = DataFrame::new(vec![thresholds.into()]).unwrap();
    store.rewrite_table_df(thresholds_db, thresholds_df).unwrap();
    
    // Find values greater than ALL thresholds
    let sql = format!(
        "SELECT id, amount FROM {} WHERE amount > ALL (SELECT threshold FROM {})",
        values_db, thresholds_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // All three values are > all thresholds (40, 45)
    assert_eq!(df.height(), 3);
}

/// Test ANY operator with subquery
#[test]
fn test_any_operator() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let scores_db = "clarium/public/scores";
    let targets_db = "clarium/public/targets";
    
    store.create_table(scores_db).unwrap();
    store.create_table(targets_db).unwrap();
    
    // Scores table
    let ids = Series::new("id".into(), vec![1i64, 2, 3, 4]);
    let scores = Series::new("score".into(), vec![85.0, 60.0, 95.0, 70.0]);
    let scores_df = DataFrame::new(vec![ids.into(), scores.into()]).unwrap();
    store.rewrite_table_df(scores_db, scores_df).unwrap();
    
    // Targets table
    let targets = Series::new("target".into(), vec![80.0, 90.0]);
    let targets_df = DataFrame::new(vec![targets.into()]).unwrap();
    store.rewrite_table_df(targets_db, targets_df).unwrap();
    
    // Find scores equal to ANY target
    let sql = format!(
        "SELECT id, score FROM {} WHERE score >= ANY (SELECT target FROM {})",
        scores_db, targets_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Scores 85 and 95 are >= at least one target
    assert!(df.height() >= 2);
}

/// Test ALL with equality operator
#[test]
fn test_all_with_equality() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let items_db = "clarium/public/items";
    let compare_db = "clarium/public/compare";
    
    store.create_table(items_db).unwrap();
    store.create_table(compare_db).unwrap();
    
    // Items table
    let ids = Series::new("id".into(), vec![1i64, 2, 3]);
    let values = Series::new("value".into(), vec![10.0, 20.0, 10.0]);
    let items_df = DataFrame::new(vec![ids.into(), values.into()]).unwrap();
    store.rewrite_table_df(items_db, items_df).unwrap();
    
    // Compare table with single value
    let compare_vals = Series::new("val".into(), vec![10.0]);
    let compare_df = DataFrame::new(vec![compare_vals.into()]).unwrap();
    store.rewrite_table_df(compare_db, compare_df).unwrap();
    
    // Find items equal to ALL values in compare (which is just 10)
    let sql = format!(
        "SELECT id, value FROM {} WHERE value = ALL (SELECT val FROM {})",
        items_db, compare_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Items with value 10
    assert_eq!(df.height(), 2);
}

/// Test ANY with inequality operator
#[test]
fn test_any_with_inequality() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let products_db = "clarium/public/products";
    let prices_db = "clarium/public/prices";
    
    store.create_table(products_db).unwrap();
    store.create_table(prices_db).unwrap();
    
    // Products table
    let ids = Series::new("id".into(), vec![1i64, 2, 3]);
    let costs = Series::new("cost".into(), vec![50.0, 100.0, 150.0]);
    let products_df = DataFrame::new(vec![ids.into(), costs.into()]).unwrap();
    store.rewrite_table_df(products_db, products_df).unwrap();
    
    // Prices table
    let prices = Series::new("price".into(), vec![75.0, 125.0]);
    let prices_df = DataFrame::new(vec![prices.into()]).unwrap();
    store.rewrite_table_df(prices_db, prices_df).unwrap();
    
    // Find products with cost less than ANY price
    let sql = format!(
        "SELECT id, cost FROM {} WHERE cost < ANY (SELECT price FROM {})",
        products_db, prices_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Products with cost < 125 (the max)
    assert!(df.height() >= 2);
}

/// Test EXISTS in job search pattern (queue exclusion)
#[test]
fn test_exists_job_queue_pattern() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/jobs.time";
    
    let base: i64 = 1_700_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..8 {
        let mut m = serde_json::Map::new();
        m.insert("queue".into(), json!(format!("q{}", i % 3)));
        m.insert("status".into(), json!(if i % 5 == 0 { "running" } else { "waiting" }));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    tprintln!("Jobs Table {:?} ", &recs);

    // Find waiting jobs where queue has no running jobs (simplified job search pattern)
    let sql = format!(
        "SELECT queue, status FROM {} j1 WHERE status = 'waiting' AND NOT EXISTS \
         (SELECT 1 FROM {} j2 WHERE j2.queue = j1.queue AND j2.status = 'running')",
        db, db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Should find waiting jobs in queues without running jobs
    assert!(df.height() > 0);
}

/// Test EXISTS with multiple conditions in subquery
#[test]
fn test_exists_multiple_conditions() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/events.time";
    
    let base: i64 = 1_800_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..6 {
        let mut m = serde_json::Map::new();
        m.insert("category".into(), json!(format!("cat{}", i % 2)));
        m.insert("priority".into(), json!(i as f64));
        m.insert("status".into(), json!(if i < 3 { "active" } else { "inactive" }));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // Find categories that have active high-priority events
    let sql = format!(
        "SELECT DISTINCT category FROM {} e1 WHERE EXISTS \
         (SELECT 1 FROM {} e2 WHERE e2.category = e1.category AND e2.status = 'active' AND e2.priority >= 2)",
        db, db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    assert!(df.height() > 0);
}

/// Test ALL with less-than operator
#[test]
fn test_all_less_than() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let candidates_db = "clarium/public/candidates";
    let limits_db = "clarium/public/limits";
    
    store.create_table(candidates_db).unwrap();
    store.create_table(limits_db).unwrap();
    
    // Candidates table
    let ids = Series::new("id".into(), vec![1i64, 2, 3, 4]);
    let values = Series::new("value".into(), vec![5.0, 15.0, 25.0, 8.0]);
    let candidates_df = DataFrame::new(vec![ids.into(), values.into()]).unwrap();
    store.rewrite_table_df(candidates_db, candidates_df).unwrap();
    
    // Limits table
    let limits = Series::new("test_limit".into(), vec![10.0, 20.0, 30.0]);
    let limits_df = DataFrame::new(vec![limits.into()]).unwrap();
    store.rewrite_table_df(limits_db, limits_df).unwrap();
    
    // Find candidates with value less than ALL test limits
    let sql = format!(
        "SELECT id, value FROM {} WHERE value < ALL (SELECT test_limit FROM {})",
        candidates_db, limits_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Values 5 and 8 are < all limits (10, 20, 30)
    assert_eq!(df.height(), 2);
}

/// Test ANY with not-equal operator
#[test]
fn test_any_not_equal() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let items_db = "clarium/public/items";
    let excluded_db = "clarium/public/excluded";
    
    store.create_table(items_db).unwrap();
    store.create_table(excluded_db).unwrap();
    
    // Items table
    let ids = Series::new("id".into(), vec![1i64, 2, 3]);
    let codes = Series::new("code".into(), vec![100i64, 200, 300]);
    let items_df = DataFrame::new(vec![ids.into(), codes.into()]).unwrap();
    store.rewrite_table_df(items_db, items_df).unwrap();
    
    // Excluded table
    let excluded_codes = Series::new("code".into(), vec![100i64]);
    let excluded_df = DataFrame::new(vec![excluded_codes.into()]).unwrap();
    store.rewrite_table_df(excluded_db, excluded_df).unwrap();
    
    // Find items not equal to ANY excluded code (effectively all items except those equal to all excluded)
    let sql = format!(
        "SELECT id, code FROM {} WHERE code != ANY (SELECT code FROM {})",
        items_db, excluded_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Items with code != 100
    assert_eq!(df.height(), 2);
}

/// Test EXISTS in CTE
#[test]
fn test_exists_in_cte() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let orders_db = "clarium/public/orders";
    let customers_db = "clarium/public/customers";
    
    store.create_table(orders_db).unwrap();
    store.create_table(customers_db).unwrap();
    
    // Orders table
    let order_ids = Series::new("order_id".into(), vec![1i64, 2]);
    let cust_ids = Series::new("customer_id".into(), vec![10i64, 10]);
    let orders_df = DataFrame::new(vec![order_ids.into(), cust_ids.into()]).unwrap();
    store.rewrite_table_df(orders_db, orders_df).unwrap();
    
    // Customers table
    let customer_ids = Series::new("customer_id".into(), vec![10i64, 20]);
    let names = Series::new("name".into(), vec!["Alice", "Bob"]);
    let customers_df = DataFrame::new(vec![customer_ids.into(), names.into()]).unwrap();
    store.rewrite_table_df(customers_db, customers_df).unwrap();
    
    // CTE with EXISTS filter
    let sql = format!(
        "WITH active_customers AS (SELECT name FROM {} c WHERE EXISTS (SELECT 1 FROM {} o WHERE o.customer_id = c.customer_id)) \
         SELECT name FROM active_customers",
        customers_db, orders_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Only Alice has orders
    assert_eq!(df.height(), 1);
}

/// Test complex query with EXISTS and ALL combined
#[test]
fn test_exists_and_all_combined() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let products_db = "clarium/public/products";
    let prices_db = "clarium/public/prices";
    
    store.create_table(products_db).unwrap();
    store.create_table(prices_db).unwrap();
    
    // Products table
    let ids = Series::new("id".into(), vec![1i64, 2, 3]);
    let costs = Series::new("cost".into(), vec![100.0, 200.0, 50.0]);
    let categories = Series::new("category".into(), vec!["A", "B", "A"]);
    let products_df = DataFrame::new(vec![ids.into(), costs.into(), categories.into()]).unwrap();
    store.rewrite_table_df(products_db, products_df).unwrap();
    
    // Prices table
    let min_prices = Series::new("min_price".into(), vec![40.0, 45.0]);
    let prices_df = DataFrame::new(vec![min_prices.into()]).unwrap();
    store.rewrite_table_df(prices_db, prices_df).unwrap();
    
    // Find products in category A with cost > all minimum prices
    let sql = format!(
        "SELECT id, cost FROM {} WHERE category = 'A' AND cost > ALL (SELECT min_price FROM {})",
        products_db, prices_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    
    let df = run_select(&shared, &q).unwrap();
    
    // Product 1 (cost 100) and 3 (cost 50) are in category A; both > all min prices
    assert_eq!(df.height(), 2);
}

use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore};
use polars::prelude::*;

/// Test nested EXISTS with multiple correlation levels
#[test]
fn test_nested_exists_two_levels() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    // Create three tables: departments, employees, projects
    let dept_db = "departments";
    let emp_db = "employees";
    let proj_db = "projects";
    
    store.create_table(dept_db).unwrap();
    store.create_table(emp_db).unwrap();
    store.create_table(proj_db).unwrap();
    
    // Departments: dept_id, name
    let dept_ids = Series::new("dept_id".into(), vec![1i64, 2, 3]);
    let dept_names = Series::new("name".into(), vec!["Engineering", "Sales", "Marketing"]);
    let dept_df = DataFrame::new(vec![dept_ids.into(), dept_names.into()]).unwrap();
    store.rewrite_table_df(dept_db, dept_df).unwrap();
    
    // Employees: emp_id, dept_id, name
    let emp_ids = Series::new("emp_id".into(), vec![101i64, 102, 103, 104]);
    let emp_dept_ids = Series::new("dept_id".into(), vec![1i64, 1, 2, 2]);
    let emp_names = Series::new("name".into(), vec!["Alice", "Bob", "Carol", "Dave"]);
    let emp_df = DataFrame::new(vec![emp_ids.into(), emp_dept_ids.into(), emp_names.into()]).unwrap();
    store.rewrite_table_df(emp_db, emp_df).unwrap();
    
    // Projects: proj_id, emp_id, name
    let proj_ids = Series::new("proj_id".into(), vec![1i64, 2]);
    let proj_emp_ids = Series::new("emp_id".into(), vec![101i64, 102]);
    let proj_names = Series::new("name".into(), vec!["Project A", "Project B"]);
    let proj_df = DataFrame::new(vec![proj_ids.into(), proj_emp_ids.into(), proj_names.into()]).unwrap();
    store.rewrite_table_df(proj_db, proj_df).unwrap();
    
    // Find departments that have employees with projects (two-level nested EXISTS)
    let sql = format!(
        "SELECT d.name FROM {} d WHERE EXISTS (
            SELECT 1 FROM {} e WHERE e.dept_id = d.dept_id AND EXISTS (
                SELECT 1 FROM {} p WHERE p.emp_id = e.emp_id
            )
        )",
        dept_db, emp_db, proj_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    let df = run_select(&shared, &q).unwrap();
    
    // Only Engineering department (dept_id=1) has employees with projects
    assert_eq!(df.height(), 1);
    let name = df.column("d.name").unwrap().get(0).unwrap();
    match name {
        AnyValue::String(s) => assert_eq!(s, "Engineering"),
        AnyValue::StringOwned(s) => assert_eq!(s, "Engineering"),
        _ => panic!("Unexpected type"),
    }
}

/// Test EXISTS with complex AND/OR combinations
#[test]
fn test_exists_with_complex_conditions() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let orders_db = "clarium/public/orders";
    let customers_db = "clarium/public/customers";
    
    store.create_table(orders_db).unwrap();
    store.create_table(customers_db).unwrap();
    
    // Orders: order_id, customer_id, amount
    let order_ids = Series::new("order_id".into(), vec![1i64, 2, 3, 4]);
    let cust_ids = Series::new("customer_id".into(), vec![10i64, 10, 20, 30]);
    let amounts = Series::new("amount".into(), vec![100.0, 200.0, 50.0, 300.0]);
    let orders_df = DataFrame::new(vec![order_ids.into(), cust_ids.into(), amounts.into()]).unwrap();
    store.rewrite_table_df(orders_db, orders_df).unwrap();
    
    // Customers: customer_id, name, status
    let customer_ids = Series::new("customer_id".into(), vec![10i64, 20, 30, 40]);
    let names = Series::new("name".into(), vec!["Alice", "Bob", "Carol", "Dave"]);
    let statuses = Series::new("status".into(), vec!["active", "active", "inactive", "active"]);
    let customers_df = DataFrame::new(vec![customer_ids.into(), names.into(), statuses.into()]).unwrap();
    store.rewrite_table_df(customers_db, customers_df).unwrap();
    
    // Find active customers with orders over 100
    let sql = format!(
        "SELECT name FROM {} c WHERE status = 'active' AND EXISTS (
            SELECT 1 FROM {} o WHERE o.customer_id = c.customer_id AND o.amount > 100
        )",
        customers_db, orders_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    let df = run_select(&shared, &q).unwrap();
    
    // Alice (customer 10, order 200) and Dave would match but Dave has no orders
    // Carol is inactive, Bob's order is 50
    assert_eq!(df.height(), 1);
}

/// Test NOT EXISTS with nested EXISTS
#[test]
fn test_not_exists_with_nested_exists() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let users_db = "clarium/public/users";
    let posts_db = "clarium/public/posts";
    let comments_db = "clarium/public/comments";
    
    store.create_table(users_db).unwrap();
    store.create_table(posts_db).unwrap();
    store.create_table(comments_db).unwrap();
    
    // Users
    let user_ids = Series::new("user_id".into(), vec![1i64, 2, 3]);
    let usernames = Series::new("username".into(), vec!["alice", "bob", "carol"]);
    let users_df = DataFrame::new(vec![user_ids.into(), usernames.into()]).unwrap();
    store.rewrite_table_df(users_db, users_df).unwrap();
    
    // Posts
    let post_ids = Series::new("post_id".into(), vec![101i64, 102]);
    let post_user_ids = Series::new("user_id".into(), vec![1i64, 2]);
    let posts_df = DataFrame::new(vec![post_ids.into(), post_user_ids.into()]).unwrap();
    store.rewrite_table_df(posts_db, posts_df).unwrap();
    
    // Comments (only on post 101)
    let comment_ids = Series::new("comment_id".into(), vec![1001i64]);
    let comment_post_ids = Series::new("post_id".into(), vec![101i64]);
    let comments_df = DataFrame::new(vec![comment_ids.into(), comment_post_ids.into()]).unwrap();
    store.rewrite_table_df(comments_db, comments_df).unwrap();
    
    // Find users with posts that have NO comments
    let sql = format!(
        "SELECT username FROM {} u WHERE EXISTS (
            SELECT 1 FROM {} p WHERE p.user_id = u.user_id AND NOT EXISTS (
                SELECT 1 FROM {} c WHERE c.post_id = p.post_id
            )
        )",
        users_db, posts_db, comments_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    let df = run_select(&shared, &q).unwrap();
    
    // Bob has post 102 with no comments
    assert_eq!(df.height(), 1);
    let username = df.column("username").unwrap().get(0).unwrap();
    match username {
        AnyValue::String(s) => assert_eq!(s, "bob"),
        AnyValue::StringOwned(s) => assert_eq!(s, "bob"),
        _ => panic!("Unexpected type"),
    }
}

/// Test EXISTS with multiple correlation columns
#[test]
fn test_exists_multiple_correlation_columns() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    
    let sales_db = "clarium/public/sales";
    let targets_db = "clarium/public/targets";
    
    store.create_table(sales_db).unwrap();
    store.create_table(targets_db).unwrap();
    
    // Sales: region, product, amount
    let regions = Series::new("region".into(), vec!["North", "North", "South", "South"]);
    let products = Series::new("product".into(), vec!["A", "B", "A", "B"]);
    let amounts = Series::new("amount".into(), vec![100.0, 200.0, 150.0, 250.0]);
    let sales_df = DataFrame::new(vec![regions.into(), products.into(), amounts.into()]).unwrap();
    store.rewrite_table_df(sales_db, sales_df).unwrap();
    
    // Targets: region, product, target
    let tgt_regions = Series::new("region".into(), vec!["North", "North", "South"]);
    let tgt_products = Series::new("product".into(), vec!["A", "B", "A"]);
    let targets = Series::new("target".into(), vec![90.0, 180.0, 140.0]);
    let targets_df = DataFrame::new(vec![tgt_regions.into(), tgt_products.into(), targets.into()]).unwrap();
    store.rewrite_table_df(targets_db, targets_df).unwrap();
    
    // Find region+product combinations that exceeded their target
    let sql = format!(
        "SELECT region, product FROM {} s WHERE EXISTS (
            SELECT 1 FROM {} t 
            WHERE t.region = s.region 
            AND t.product = s.product 
            AND s.amount > t.target
        )",
        sales_db, targets_db
    );
    
    let cmd = query::parse(&sql).unwrap();
    let q = match cmd { Command::Select(q) => q, _ => panic!("Expected Select") };
    let df = run_select(&shared, &q).unwrap();
    
    // North-A (100>90), North-B (200>180), South-A (150>140), South-B has no target
    assert_eq!(df.height(), 3);
}

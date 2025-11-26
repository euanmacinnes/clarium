use super::super::run_slice;
use crate::query::{self, Command};
use crate::storage::SharedStore;
use crate::server::data_context::DataContext;

#[test]
fn manual_single_row_auto_labels() {
    let tmp = tempfile::tempdir().unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let q = "SLICE USING (2025-01-01T00:00:00Z, 2025-01-01T01:00:00Z, 'A', 'B')";
    let plan = match query::parse(q).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("timeline", "public");
    let df = run_slice(&shared, &plan, &ctx).unwrap();
    assert_eq!(df.height(), 1);
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str()=="label_1"));
    assert!(cols.iter().any(|c| c.as_str()=="label_2"));
    let l1 = df.column("label_1").unwrap().str().unwrap();
    let l2 = df.column("label_2").unwrap().str().unwrap();
    assert_eq!(l1.get(0).unwrap(), "A");
    assert_eq!(l2.get(0).unwrap(), "B");
}

#[test]
fn manual_multi_rows_with_alias() {
    let tmp = tempfile::tempdir().unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let q = "SLICE USING ((2025-01-01T00:00:00Z, 2025-01-01T00:10:00Z, name:='X'), (2025-01-01T00:15:00Z, 2025-01-01T00:20:00Z, name:='Y'))";
    let plan = match query::parse(q).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("timeline", "public");
    let df = run_slice(&shared, &plan, &ctx).unwrap();
    assert_eq!(df.height(), 2);
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str()=="name"));
    let name = df.column("name").unwrap().str().unwrap();
    assert_eq!(name.get(0).unwrap(), "X");
    assert_eq!(name.get(1).unwrap(), "Y");
}

#[test]
fn manual_union_merge_and_labels() {
    let tmp = tempfile::tempdir().unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let q = "SLICE USING (0, 10000, l1:='LHS') UNION (5000, 15000, l1:='RHS')";
    let plan = match query::parse(q).unwrap() { Command::Slice(p) => p, _ => unreachable!() };
    let ctx = DataContext::with_defaults("timeline", "public");
    let df = run_slice(&shared, &plan, &ctx).unwrap();
    // Expect single merged interval and label from LHS per union semantics
    assert_eq!(df.height(), 1);
    let l1 = df.column("l1").unwrap().str().unwrap();
    assert_eq!(l1.get(0).unwrap(), "LHS");
}





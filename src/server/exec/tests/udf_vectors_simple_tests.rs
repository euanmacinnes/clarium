use crate::server::exec::exec_select::run_select;
use crate::server::query::{self, Command};
use crate::storage::SharedStore;

// Simplified, direct unit tests for Lua vector functions used by ANN/Graph features.

#[test]
fn cosine_sim_basic_and_edge_cases() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let store = SharedStore::new(tmp.path()).unwrap();

    // Identical unit vectors -> 1.0
    let q = "SELECT cosine_sim('[1,0,0]', '[1,0,0]') AS c";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    let v = df.column("c").unwrap().get(0).unwrap().try_extract::<f64>().unwrap();
    assert!((v - 1.0).abs() < 1e-12);

    // Orthogonal vectors -> 0.0
    let q = "SELECT cosine_sim('1,0', '0,1') AS c";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    let v = df.column("c").unwrap().get(0).unwrap().try_extract::<f64>().unwrap();
    assert!(v.abs() < 1e-12);

    // With whitespace and brackets
    let q = "SELECT cosine_sim('[1, 2, 3]', '(1,2,3)') AS c";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    let v = df.column("c").unwrap().get(0).unwrap().try_extract::<f64>().unwrap();
    assert!((v - 1.0).abs() < 1e-12);

    // Zero vector causes NULL (undefined cosine)
    let q = "SELECT cosine_sim('0,0,0', '1,2,3') AS c";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert_eq!(df.column("c").unwrap().is_null().get(0), Some(true));

    // Invalid tokens cause NULL
    let q = "SELECT cosine_sim('1, a, 3', '1,2,3') AS c";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert_eq!(df.column("c").unwrap().is_null().get(0), Some(true));
}

#[test]
fn vec_l2_and_ip_variants() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let store = SharedStore::new(tmp.path()).unwrap();

    // L2 distance of a classic 3-4-5 triangle
    let q = "SELECT vec_l2('0,0', '3,4') AS d";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    let v = df.column("d").unwrap().get(0).unwrap().try_extract::<f64>().unwrap();
    assert!((v - 5.0).abs() < 1e-12);

    // Negative numbers and brackets
    let q = "SELECT vec_l2('[-1, -2, -3]', '(4,5,6)') AS d";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    let v = df.column("d").unwrap().get(0).unwrap().try_extract::<f64>().unwrap();
    // sqrt((5)^2 + (7)^2 + (9)^2) = sqrt(25+49+81) = sqrt(155)
    assert!((v - 155f64.sqrt()).abs() < 1e-12);

    // Inner product simple case
    let q = "SELECT vec_ip('1,2,3', '4,5,6') AS ip";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    let v = df.column("ip").unwrap().get(0).unwrap().try_extract::<f64>().unwrap();
    assert!((v - 32.0).abs() < 1e-12);

    // Invalid input -> NULL
    let q = "SELECT vec_l2('1,2,x', '3,4,5') AS d";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert_eq!(df.column("d").unwrap().is_null().get(0), Some(true));
}

use crate::server::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::storage::SharedStore;

#[test]
fn to_vec_parsing_variants_and_invalid() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let store = SharedStore::new(tmp.path()).unwrap();

    // Bracketed → canonical
    let q = "SELECT to_vec('[1, 2, 3]') AS v";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    let s = df.column("v").unwrap();
    assert_eq!(s.get(0).unwrap().get_str().unwrap(), "1,2,3");

    // Mixed separators and whitespace
    let q = "SELECT to_vec('1; 2 |3  4') AS v";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert_eq!(df.column("v").unwrap().get(0).unwrap().get_str().unwrap(), "1,2,3,4");

    // Invalid tokens → NULL
    let q = "SELECT to_vec('1, a, 3') AS v";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    let is_null = df.column("v").unwrap().is_null().get(0) == Some(true);
    assert!(is_null);
}

#[test]
fn cosine_l2_ip_correctness_small_vectors() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let store = SharedStore::new(tmp.path()).unwrap();

    // cosine_sim([1,0],[1,0]) == 1
    let q = "SELECT cosine_sim('[1,0]', '[1,0]') AS c";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    let v = df.column("c").unwrap().get(0).unwrap().try_extract::<f64>().unwrap();
    assert!((v - 1.0).abs() < 1e-12);

    // vec_l2([0,0],[3,4]) == 5
    let q = "SELECT vec_l2('0,0', '3,4') AS d";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    let v = df.column("d").unwrap().get(0).unwrap().try_extract::<f64>().unwrap();
    assert!((v - 5.0).abs() < 1e-12);

    // vec_ip([1,2,3],[4,5,6]) == 32
    let q = "SELECT vec_ip('1,2,3','4,5,6') AS ip";
    let q = match query::parse(q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    let v = df.column("ip").unwrap().get(0).unwrap().try_extract::<f64>().unwrap();
    assert!((v - 32.0).abs() < 1e-12);
}

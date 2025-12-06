use crate::server::exec::exec_select::run_select;
use crate::server::query::{self, Command};
use crate::server::exec::tests::fixtures::*;

// Verify metric semantics and tie-breaks:
// - L2 sorts ASC (smaller distance is better)
// - cosine_sim sorts DESC (larger similarity is better)
// - vec_ip sorts DESC (larger dot product is better)
// - Deterministic tie-break via id DESC as second key
#[test]
fn metric_semantics_and_tiebreaks() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    let table = "clarium/public/docs";
    seed_docs_with_embeddings(&store, table);

    // L2 ASC with tie-break by id DESC
    let q1 = "WITH q AS (SELECT to_vec('[0.21,0,0]') v) \
              SELECT id, vec_l2(body_embed, (SELECT v FROM q)) AS d \
              FROM clarium/public/docs \
              ORDER BY d ASC, id DESC";
    let df1 = run_select(&store, match query::parse(q1).unwrap(){ Command::Select(s)=>s, _=>unreachable!() }).unwrap();
    let ids1: Vec<i64> = df1.column("id").unwrap().i64().unwrap().into_no_null_iter().collect();
    // Expected: ids closest to 0.21 along x-axis are 2, then 3 vs 1 determined by tie-break
    assert_eq!(ids1.len(), 3);
    assert_eq!(ids1[0], 2);

    // cosine_sim DESC (larger is better) with tie-break id DESC
    let q2 = "WITH q AS (SELECT to_vec('[0.21,0,0]') v) \
              SELECT id, cosine_sim(body_embed, (SELECT v FROM q)) AS c \
              FROM clarium/public/docs \
              ORDER BY c DESC, id DESC";
    let df2 = run_select(&store, match query::parse(q2).unwrap(){ Command::Select(s)=>s, _=>unreachable!() }).unwrap();
    let ids2: Vec<i64> = df2.column("id").unwrap().i64().unwrap().into_no_null_iter().collect();
    assert_eq!(ids2.len(), 3);
    assert_eq!(ids2[0], 3, "cosine should prefer the larger magnitude dot (0.3 vs 0.2 vs 0.1) with same direction");

    // vec_ip DESC (larger is better) with tie-break id DESC
    let q3 = "WITH q AS (SELECT to_vec('[0.21,0,0]') v) \
              SELECT id, vec_ip(body_embed, (SELECT v FROM q)) AS ip \
              FROM clarium/public/docs \
              ORDER BY ip DESC, id DESC";
    let df3 = run_select(&store, match query::parse(q3).unwrap(){ Command::Select(s)=>s, _=>unreachable!() }).unwrap();
    let ids3: Vec<i64> = df3.column("id").unwrap().i64().unwrap().into_no_null_iter().collect();
    assert_eq!(ids3.len(), 3);
    assert_eq!(ids3[0], 3, "inner product prefers larger projection on query vector");
}

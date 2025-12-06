use crate::server::exec::exec_select::run_select;
use crate::server::query::{self, Command};
use crate::server::exec::tests::fixtures::*;

// Two-phase ANN parity (no LIMIT) vs exact on small deterministic dataset
#[test]
fn ann_parity_no_limit_matches_exact_full_order() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    let table = "clarium/public/docs";
    seed_docs_with_embeddings(&store, table);
    // Write a ready sidecar index (HNSW) for the column
    write_vindex_sidecar(&store, "clarium/public/idx_docs_body", table, "body_embed", "l2", 3);

    // Exact baseline: compute distances in projection; ORDER BY ASC, no LIMIT
    let exact_sql = "WITH q AS (SELECT to_vec('[0.25,0,0]') v) \
                     SELECT id, vec_l2(body_embed, (SELECT v FROM q)) AS d \
                     FROM clarium/public/docs \
                     ORDER BY d ASC";
    let exact_q = match query::parse(exact_sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df_exact = run_select(&store, &exact_q).unwrap();
    let ids_exact: Vec<i64> = df_exact
        .column("id").unwrap()
        .i64().unwrap()
        .into_no_null_iter()
        .collect();

    // ANN path: ORDER BY vec_l2(...) USING ANN with no LIMIT must return parity after re-score
    let ann_sql = "WITH q AS (SELECT to_vec('[0.25,0,0]') v) \
                   SELECT id \
                   FROM clarium/public/docs \
                   ORDER BY vec_l2(clarium/public/docs.body_embed, (SELECT v FROM q)) USING ANN";
    let ann_q = match query::parse(ann_sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df_ann = run_select(&store, &ann_q).unwrap();
    let ids_ann: Vec<i64> = df_ann
        .column("id").unwrap()
        .i64().unwrap()
        .into_no_null_iter()
        .collect();

    assert_eq!(df_ann.height(), df_exact.height());
    assert_eq!(ids_exact, ids_ann, "ANN without LIMIT must match exact ordering");
}

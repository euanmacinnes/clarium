use crate::server::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::server::exec::tests::fixtures::*;
use futures::executor::block_on;

#[test]
fn nearest_neighbors_join_back_by_row_id_exact_scan_with_ord() {
    // No index present → exact scan path. Validate join back works via row_id → __row_id.<alias>
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    seed_docs_with_embeddings(&store, "clarium/public/docs");

    // Use with_ord=true to ensure ord is provided on exact path
    let sql = "SELECT d.id, nn.score \
               FROM nearest_neighbors('clarium/public/docs','body_embed','[0.21,0,0]', 2, 'l2', 64, true) AS nn \
               JOIN clarium/public/docs AS d ON nn.row_id = d.\"__row_id.d\" \
               ORDER BY nn.score";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert_eq!(df.height(), 2);
    assert!(df.column("d.id").is_ok());
    assert!(df.column("nn.score").is_ok());
}

#[test]
fn vector_search_with_topk_and_engine_hint() {
    // Create and build an index, then call vector_search with optional topk and engine hint
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    seed_docs_with_embeddings(&store, "clarium/public/docs");

    let create = "CREATE VECTOR INDEX idx_docs_body ON clarium/public/docs(body_embed) USING HNSW WITH (metric='l2', dim=3, M=16, ef_build=64)";
    block_on(crate::server::exec::execute_query(&store, create)).unwrap();
    block_on(crate::server::exec::execute_query(&store, "BUILD VECTOR INDEX clarium/public/idx_docs_body")).unwrap();

    // engine hint 'flat' should still return valid results via fallback; request topk=2 while k=5
    let sql = "SELECT * FROM vector_search('clarium/public/idx_docs_body','[0.21,0,0]', 5, 2, 'flat') AS vs";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert_eq!(df.height(), 2);
    assert!(df.column("vs.row_id").is_ok());
    assert!(df.column("vs.score").is_ok());
}

use crate::server::exec::exec_select::run_select;
use crate::server::query::{self, Command};
use crate::server::exec::tests::fixtures::*;

// Validate row-id mapping under filters and joins using __row_id.<alias>
#[test]
fn row_id_mapping_with_filter_then_join_back() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    seed_docs_with_embeddings(&store, "clarium/public/docs");

    // Filter base table to exclude id=1, then join TVF by row_id back to alias d
    let sql = "WITH d AS (SELECT * FROM clarium/public/docs WHERE id <> 1) \
               SELECT d.id, nn.score \
               FROM d \
               JOIN nearest_neighbors('clarium/public/docs','body_embed','[0.21,0,0]', 2, 'l2', 64, true) AS nn \
                 ON nn.row_id = d.'__row_id.d' \
               ORDER BY nn.score ASC";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert!(df.height() >= 1);
    // Ensure returned ids come only from filtered set {2,3}
    let ids: Vec<i64> = df.column("d.id").unwrap().i64().unwrap().into_no_null_iter().collect();
    assert!(ids.iter().all(|&id| id == 2 || id == 3));
}

#[test]
fn row_id_mapping_survives_join_then_project_alias() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    seed_docs_with_embeddings(&store, "clarium/public/docs");

    // Join first, then select using alias to ensure __row_id.<alias> resolution works
    let sql = "SELECT d.id AS did, nn.score \
               FROM clarium/public/docs AS d \
               JOIN nearest_neighbors('clarium/public/docs','body_embed','[0.29,0,0]', 2, 'l2', 64, true) AS nn \
                 ON nn.row_id = d.'__row_id.d' \
               ORDER BY nn.score ASC";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert!(df.height() >= 1);
    assert!(df.column("did").is_ok());
}

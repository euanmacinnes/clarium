use crate::server::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::server::exec::tests::fixtures::*;
use futures::executor::block_on;

#[test]
fn view_over_neighbors_and_show_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    // Seed a simple graph and write sidecar
    seed_tools_graph(&store, "clarium/public/know_nodes", "clarium/public/know_edges");
    write_graph_sidecar(&store, "clarium/public/know", "clarium/public/know_nodes", "clarium/public/know_edges");

    // Create a view over neighbors
    let create = "CREATE VIEW v_neighbors AS \
                  SELECT * FROM graph_neighbors('clarium/public/know','planner','Calls',2)";
    block_on(crate::server::exec::execute_query(&store, create)).unwrap();

    // Selecting from the view should work
    let sql = "SELECT * FROM v_neighbors ORDER BY hop, node_id";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert!(df.height() >= 1);
    assert!(df.column("node_id").is_ok());

    // SHOW VIEW returns definition
    let show = block_on(crate::server::exec::execute_query(&store, "SHOW VIEW v_neighbors")).unwrap();
    let arr = show.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    let row = arr[0].as_object().unwrap();
    assert!(row.get("definition").unwrap().as_str().unwrap().to_ascii_lowercase().contains("graph_neighbors"));
}

#[test]
fn view_with_ann_ordering_selectable() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    // Seed docs and vindex for ANN
    seed_docs_with_embeddings(&store, "clarium/public/docs");
    write_vindex_sidecar(&store, "clarium/public/idx_docs_body", "clarium/public/docs", "body_embed", "l2", 3);

    // Create a view that uses ANN ORDER BY with a literal RHS vector
    let create = "CREATE VIEW v_docs_ann AS \
                  SELECT id FROM clarium/public/docs \
                  ORDER BY vec_l2(clarium/public/docs.body_embed, '[0.21,0,0]') USING ANN";
    block_on(crate::server::exec::execute_query(&store, create)).unwrap();

    let sql = "SELECT * FROM v_docs_ann LIMIT 2";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert_eq!(df.height(), 2);
    assert!(df.column("id").is_ok());
}

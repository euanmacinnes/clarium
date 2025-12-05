use crate::server::exec::exec_select::run_select;
use crate::server::exec::tests::fixtures::*;
use crate::server::query::{self, Command};
use futures::executor::block_on;

// Validate CREATE MATCH VIEW works by rewriting MATCH to a SELECT definition
// and that selecting from the created view returns expected columns/rows.
#[test]
fn create_match_view_and_select() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    // Seed a simple graph and write sidecar
    seed_tools_graph(&store, "clarium/public/know_nodes", "clarium/public/know_edges");
    write_graph_sidecar(&store, "clarium/public/know", "clarium/public/know_nodes", "clarium/public/know_edges");

    // Use explicit USING GRAPH in the MATCH body to avoid depending on session default
    let create = "CREATE MATCH VIEW v_m AS \
                  MATCH USING GRAPH 'clarium/public/know' \
                  (s:Tool { key: 'planner' })-[:Calls*1..2]->(t:Tool) \
                  RETURN t.key AS node_id, hop ORDER BY hop, node_id";
    let res = block_on(crate::server::exec::execute_query(&store, create));
    assert!(res.is_ok(), "CREATE MATCH VIEW should succeed: {:?}", res.err());

    // SHOW VIEW should include a definition that contains graph_neighbors(...)
    let show = block_on(crate::server::exec::execute_query(&store, "SHOW VIEW v_m")).unwrap();
    let arr = show.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    let row = arr[0].as_object().unwrap();
    let def = row.get("definition").unwrap().as_str().unwrap().to_ascii_lowercase();
    assert!(def.contains("graph_neighbors"), "definition should be a rewritten SELECT over graph_neighbors: {}", def);

    // Selecting from the view should work and return at least one row
    let sql = "SELECT * FROM v_m";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert!(df.height() >= 1);
    assert!(df.column("node_id").is_ok());
    assert!(df.column("hop").is_ok());
}

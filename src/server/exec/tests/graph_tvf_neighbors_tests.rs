use crate::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::server::exec::tests::fixtures::*;
use crate::storage::SharedStore;
use polars::prelude::*;

fn setup_graph(store: &SharedStore) {
    // Seed nodes and edges, and write graph sidecar
    seed_tools_graph(store, "clarium/public/know_nodes", "clarium/public/know_edges");
    write_graph_sidecar(
        store,
        "clarium/public/know",
        "clarium/public/know_nodes",
        "clarium/public/know_edges",
    );
}

#[test]
fn neighbors_one_and_two_hops_with_etype() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    setup_graph(&store);

    // One-hop from planner should yield toolA only
    let sql1 = "SELECT * FROM graph_neighbors('clarium/public/know','planner','Calls',1) g ORDER BY hop, node_id";
    let q1 = match query::parse(sql1).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&store, &q1).unwrap();
    assert_eq!(df1.height(), 1);
    assert_eq!(df1.column("node_id").unwrap().get(0).unwrap().to_string(), "toolA");
    assert_eq!(df1.column("hop").unwrap().get(0).unwrap().try_extract::<i64>().unwrap(), 1);

    // Two hops from planner should include executor at hop=2
    let sql2 = "SELECT * FROM graph_neighbors('clarium/public/know','planner','Calls',2) g ORDER BY hop, node_id";
    let q2 = match query::parse(sql2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&store, &q2).unwrap();
    assert_eq!(df2.height(), 2);
    let hops: Vec<i64> = df2.column("hop").unwrap().i64().unwrap().into_no_null_iter().collect();
    assert_eq!(hops, vec![1,2]);
}

#[test]
fn neighbors_join_nodes_semantic_affinity() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    setup_graph(&store);

    // Compute affinity against a query vector; ensure grouping and having work
    let sql = "WITH q AS (SELECT to_vec('[0.09,0,0]') v) \
               SELECT g.node_id, avg(cosine_sim(n.embed,(SELECT v FROM q))) AS affinity \
               FROM graph_neighbors('clarium/public/know','planner','Calls',2) g \
               JOIN clarium/public/know_nodes n ON n.id = g.node_id \
               GROUP BY g.node_id HAVING affinity > 0.0 ORDER BY affinity DESC";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert!(df.height() >= 1);
    // Column names exist
    assert!(df.column("node_id").is_ok());
    assert!(df.column("affinity").is_ok());
}

#[test]
fn neighbors_temporal_window_inclusive_and_single_bound() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    // Seed time-aware edges: place one edge inside window and one outside
    // nodes
    seed_tools_graph(&store, "clarium/public/know_nodes", "clarium/public/edges_time");
    // overwrite edges_time with time values
    let mut rows: Vec<crate::storage::Record> = Vec::new();
    for (i, (s, d, cost, t)) in [
        ("planner","toolA",1.0, 1_700_000_000_000i64), // 2023-11-14 approx
        ("toolA","executor",2.0, 1_750_000_000_000i64), // 2025-07-10 approx
    ].iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("src".into(), serde_json::json!(*s));
        m.insert("dst".into(), serde_json::json!(*d));
        m.insert("cost".into(), serde_json::json!(*cost));
        m.insert("_time".into(), serde_json::json!(*t));
        rows.push(crate::storage::Record { _time: i as i64, sensors: m });
    }
    let guard = store.0.lock(); guard.write_records("clarium/public/edges_time", &rows).unwrap(); drop(guard);
    write_graph_sidecar_with_time(&store, "clarium/public/know_t", "clarium/public/know_nodes", "clarium/public/edges_time", "_time");

    // Full window that only includes first edge (upper bound before second edge)
    let sql = "SELECT * FROM graph_neighbors('clarium/public/know_t','planner','Calls',2,'2023-01-01T00:00:00Z','2024-01-01T00:00:00Z') g ORDER BY hop, node_id";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    // Only one-hop neighbor should be present
    assert_eq!(df.height(), 1);

    // Single lower bound that includes both edges
    let sql2 = "SELECT * FROM graph_neighbors('clarium/public/know_t','planner','Calls',2,'2020-01-01T00:00:00Z') g ORDER BY hop, node_id";
    let q2 = match query::parse(sql2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&store, &q2).unwrap();
    assert_eq!(df2.height(), 2);
}

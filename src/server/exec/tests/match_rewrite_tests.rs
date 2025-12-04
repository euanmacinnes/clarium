use crate::server::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::server::exec::tests::fixtures::*;
use crate::storage::{SharedStore, Record};
use serde_json::json;

#[test]
fn match_neighbors_rewrite_equivalence() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    // Seed a tiny tools graph and write .graph sidecar
    seed_tools_graph(&store, "clarium/public/nodes_m", "clarium/public/edges_m");
    write_graph_sidecar(&store, "clarium/public/g_match", "clarium/public/nodes_m", "clarium/public/edges_m");

    // MATCH neighbors up to 2 hops, omit USING GRAPH and rely on USE GRAPH default
    let _ = crate::server::exec::execute_query(&store, "USE GRAPH clarium/public/g_match");
    let sql = "MATCH (s:Tool { key: 'planner' })-[:Calls*1..2]->(t:Tool) \
               RETURN t.key AS node_id, hop ORDER BY hop, node_id";
    let q = match query::parse(sql).unwrap() { Command::MatchRewrite { sql } => {
        // Re-parse the rewritten SELECT
        match query::parse(&sql).unwrap() { Command::Select(q2) => q2, _ => panic!("expected select") }
    }, _ => panic!("expected match rewrite") };
    let df = run_select(&store, &q).unwrap();
    assert_eq!(df.height(), 2);
    let n0 = {
        let av = df.column("node_id").unwrap().get(0).unwrap();
        av.get_str().map(|s| s.to_string()).unwrap_or_else(|| av.to_string())
    };
    let h0 = df.column("hop").unwrap().get(0).unwrap().try_extract::<i64>().unwrap();
    let n1 = {
        let av = df.column("node_id").unwrap().get(1).unwrap();
        av.get_str().map(|s| s.to_string()).unwrap_or_else(|| av.to_string())
    };
    let h1 = df.column("hop").unwrap().get(1).unwrap().try_extract::<i64>().unwrap();
    assert_eq!(n0.as_str(), "toolA");
    assert_eq!(h0, 1i64);
    assert_eq!(n1.as_str(), "executor");
    assert_eq!(h1, 2i64);
}

#[test]
fn match_shortest_paths_weighted() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    // Nodes and weighted edges where the 2-hop route is cheaper than direct
    seed_tools_graph(&store, "clarium/public/nodes_ms", "clarium/public/edges_ms");
    // Overwrite edges with weights favoring planner->toolA->executor
    let mut edges = Vec::<Record>::new();
    { let mut m = serde_json::Map::new(); m.insert("src".into(), json!("planner")); m.insert("dst".into(), json!("executor")); m.insert("cost".into(), json!(10.0)); edges.push(Record { _time: 0, sensors: m }); }
    { let mut m = serde_json::Map::new(); m.insert("src".into(), json!("planner")); m.insert("dst".into(), json!("toolA")); m.insert("cost".into(), json!(1.0)); edges.push(Record { _time: 1, sensors: m }); }
    { let mut m = serde_json::Map::new(); m.insert("src".into(), json!("toolA")); m.insert("dst".into(), json!("executor")); m.insert("cost".into(), json!(2.0)); edges.push(Record { _time: 2, sensors: m }); }
    let guard = store.0.lock(); guard.write_records("clarium/public/edges_ms", &edges).unwrap(); drop(guard);
    write_graph_sidecar(&store, "clarium/public/g_match_shortest", "clarium/public/nodes_ms", "clarium/public/edges_ms");

    let sql = "MATCH SHORTEST USING GRAPH 'clarium/public/g_match_shortest' \
               (s:Tool { key: 'planner' })-[:Calls*1..3]->(t:Tool { key: 'executor' }) \
               RETURN t.key AS node_id, hop ORDER BY hop";
    // Expect planner->toolA->executor (3 rows with hops 0..2 in graph_paths ordering by ord maps to hop)
    let cmd = query::parse(sql).unwrap();
    let q = match cmd { Command::MatchRewrite { sql } => match query::parse(&sql).unwrap() { Command::Select(q2) => q2, _ => panic!("expected select") }, _ => panic!("expected match rewrite") };
    let df = run_select(&store, &q).unwrap();
    assert_eq!(df.height(), 3);
    let mid = {
        let av = df.column("node_id").unwrap().get(1).unwrap();
        av.get_str().map(|s| s.to_string()).unwrap_or_else(|| av.to_string())
    };
    assert_eq!(mid.as_str(), "toolA");
}

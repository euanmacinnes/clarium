use crate::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::server::exec::tests::fixtures::*;
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

fn write_graph_sidecar_simple(store: &SharedStore, name: &str, nodes_tbl: &str, edges_tbl: &str) {
    // Write a .graph without cost_column to force unweighted BFS
    let mut p = store.0.lock().root_path().clone();
    let local = name.replace('/', std::path::MAIN_SEPARATOR_STR);
    p.push(local);
    p.set_extension("graph");
    let gf = serde_json::json!({
        "version": 1,
        "name": name,
        "qualified": name,
        "nodes": [
            {"label": "Tool", "key": "name", "table": nodes_tbl, "key_column": "id"}
        ],
        "edges": [
            {"type": "Calls", "from":"Tool", "to":"Tool", "table": edges_tbl, "src_column":"src", "dst_column":"dst"}
        ],
        "created_at": "2025-01-01T00:00:00Z"
    });
    std::fs::create_dir_all(p.parent().unwrap()).ok();
    std::fs::write(&p, serde_json::to_string_pretty(&gf).unwrap()).unwrap();
}

#[test]
fn paths_unweighted_shortest_and_no_path() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    // Seed nodes
    let mut nodes = Vec::<Record>::new();
    for (i, id) in ["A","B","C","D"].iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(*id));
        m.insert("name".into(), json!(*id));
        m.insert("embed".into(), json!("0.0,0.0,0.0"));
        nodes.push(Record { _time: i as i64, sensors: m });
    }
    let guard = store.0.lock();
    guard.write_records("clarium/public/nodes_s", &nodes).unwrap();
    drop(guard);
    // Edges without cost (A->B, B->C, A->D)
    let mut edges = Vec::<Record>::new();
    for (i, (s,d)) in [("A","B"),("B","C"),("A","D")].iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("src".into(), json!(*s));
        m.insert("dst".into(), json!(*d));
        edges.push(Record { _time: i as i64, sensors: m });
    }
    let guard = store.0.lock();
    guard.write_records("clarium/public/edges_s", &edges).unwrap();
    drop(guard);
    write_graph_sidecar_simple(&store, "clarium/public/g_simple", "clarium/public/nodes_s", "clarium/public/edges_s");

    // Shortest path from A to C within 3 hops should be A->B->C (ord 0..2)
    let sql = "SELECT * FROM graph_paths('clarium/public/g_simple','A','C',3) ORDER BY ord";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert_eq!(df.height(), 3);
    assert_eq!(df.column("node_id").unwrap().get(0).unwrap().to_string(), "A");
    assert_eq!(df.column("node_id").unwrap().get(2).unwrap().to_string(), "C");

    // No path within 1 hop
    let sql2 = "SELECT * FROM graph_paths('clarium/public/g_simple','A','C',1)";
    let q2 = match query::parse(sql2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&store, &q2).unwrap();
    assert_eq!(df2.height(), 0);
}

#[test]
fn paths_weighted_dijkstra_prefers_cheaper_route() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    // Nodes
    seed_tools_graph(&store, "clarium/public/nodes_w", "clarium/public/edges_w");
    // Overwrite edges with two routes: direct A->C expensive, two-hop A->B->C cheaper
    let mut edges = Vec::<Record>::new();
    // A=planner, B=toolA, C=executor
    { let mut m = serde_json::Map::new(); m.insert("src".into(), json!("planner")); m.insert("dst".into(), json!("executor")); m.insert("cost".into(), json!(10.0)); edges.push(Record { _time: 0, sensors: m }); }
    { let mut m = serde_json::Map::new(); m.insert("src".into(), json!("planner")); m.insert("dst".into(), json!("toolA")); m.insert("cost".into(), json!(1.0)); edges.push(Record { _time: 1, sensors: m }); }
    { let mut m = serde_json::Map::new(); m.insert("src".into(), json!("toolA")); m.insert("dst".into(), json!("executor")); m.insert("cost".into(), json!(2.0)); edges.push(Record { _time: 2, sensors: m }); }
    let guard = store.0.lock();
    guard.write_records("clarium/public/edges_w", &edges).unwrap();
    drop(guard);
    write_graph_sidecar(&store, "clarium/public/g_weighted", "clarium/public/nodes_w", "clarium/public/edges_w");

    // With max_hops 3, Dijkstra should choose planner->toolA->executor (cost 3.0) over direct (cost 10.0)
    let sql = "SELECT * FROM graph_paths('clarium/public/g_weighted','planner','executor',3) ORDER BY ord";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    assert_eq!(df.height(), 3);
    assert_eq!(df.column("node_id").unwrap().get(1).unwrap().to_string(), "toolA");
}

#[test]
fn paths_temporal_window_changes_route() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    // Nodes
    seed_tools_graph(&store, "clarium/public/nodes_t", "clarium/public/edges_t");
    // Edges with time: two-hop path appears only after 2025; before that only direct edge exists
    let mut edges = Vec::<Record>::new();
    { let mut m = serde_json::Map::new(); m.insert("src".into(), json!("planner")); m.insert("dst".into(), json!("executor")); m.insert("cost".into(), json!(5.0)); m.insert("_time".into(), json!(1_700_000_000_000i64)); edges.push(Record { _time: 0, sensors: m }); }
    { let mut m = serde_json::Map::new(); m.insert("src".into(), json!("planner")); m.insert("dst".into(), json!("toolA")); m.insert("cost".into(), json!(1.0)); m.insert("_time".into(), json!(1_760_000_000_000i64)); edges.push(Record { _time: 1, sensors: m }); }
    { let mut m = serde_json::Map::new(); m.insert("src".into(), json!("toolA")); m.insert("dst".into(), json!("executor")); m.insert("cost".into(), json!(1.0)); m.insert("_time".into(), json!(1_760_000_000_500i64)); edges.push(Record { _time: 2, sensors: m }); }
    let guard = store.0.lock(); guard.write_records("clarium/public/edges_t", &edges).unwrap(); drop(guard);
    write_graph_sidecar_with_time(&store, "clarium/public/g_time", "clarium/public/nodes_t", "clarium/public/edges_t", "_time");

    // Early window: only direct edge is visible → path has 2 nodes
    let sql_early = "SELECT * FROM graph_paths('clarium/public/g_time','planner','executor',3,'Calls','2023-01-01T00:00:00Z','2024-01-01T00:00:00Z') ORDER BY ord";
    let q1 = match query::parse(sql_early).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df1 = run_select(&store, &q1).unwrap();
    assert_eq!(df1.height(), 2);

    // Late window: two-hop path visible → path has 3 nodes
    let sql_late = "SELECT * FROM graph_paths('clarium/public/g_time','planner','executor',3,'Calls','2025-01-01T00:00:00Z','2026-01-01T00:00:00Z') ORDER BY ord";
    let q2 = match query::parse(sql_late).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&store, &q2).unwrap();
    assert_eq!(df2.height(), 3);
}

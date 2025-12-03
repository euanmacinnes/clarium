use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

pub fn new_store(tmp: &tempfile::TempDir) -> SharedStore {
    SharedStore::new(tmp.path()).unwrap()
}

pub fn write_rows(store: &SharedStore, table: &str, rows: Vec<serde_json::Map<String, serde_json::Value>>) {
    let mut recs: Vec<Record> = Vec::new();
    for (i, m) in rows.into_iter().enumerate() {
        recs.push(Record { _time: i as i64, sensors: m });
    }
    let guard = store.0.lock();
    guard.write_records(table, &recs).unwrap();
}

pub fn seed_docs_with_embeddings(store: &SharedStore, table: &str) {
    // Small, deterministic vectors (dim=3) encoded as comma-separated strings
    let rows = vec![
        {
            let mut m = serde_json::Map::new();
            m.insert("id".into(), json!(1));
            m.insert("body".into(), json!("alpha"));
            m.insert("body_embed".into(), json!("0.1,0.0,0.0"));
            m
        },
        {
            let mut m = serde_json::Map::new();
            m.insert("id".into(), json!(2));
            m.insert("body".into(), json!("beta"));
            m.insert("body_embed".into(), json!("0.2,0.0,0.0"));
            m
        },
        {
            let mut m = serde_json::Map::new();
            m.insert("id".into(), json!(3));
            m.insert("body".into(), json!("gamma"));
            m.insert("body_embed".into(), json!("0.3,0.0,0.0"));
            m
        },
    ];
    write_rows(store, table, rows);
}

pub fn seed_tools_graph(store: &SharedStore, nodes_tbl: &str, edges_tbl: &str) {
    // Nodes: id(name==id here), embed
    let nodes = vec![
        {
            let mut m = serde_json::Map::new();
            m.insert("id".into(), json!("planner"));
            m.insert("name".into(), json!("planner"));
            m.insert("embed".into(), json!("0.1,0.0,0.0"));
            m
        },
        {
            let mut m = serde_json::Map::new();
            m.insert("id".into(), json!("executor"));
            m.insert("name".into(), json!("executor"));
            m.insert("embed".into(), json!("0.2,0.0,0.0"));
            m
        },
        {
            let mut m = serde_json::Map::new();
            m.insert("id".into(), json!("toolA"));
            m.insert("name".into(), json!("toolA"));
            m.insert("embed".into(), json!("0.3,0.0,0.0"));
            m
        },
    ];
    write_rows(store, nodes_tbl, nodes);

    // Edges with costs for Calls; Cites as type ignored at runtime but catalog keeps it
    let edges = vec![
        { let mut m = serde_json::Map::new(); m.insert("src".into(), json!("planner")); m.insert("dst".into(), json!("toolA")); m.insert("cost".into(), json!(1.0)); m },
        { let mut m = serde_json::Map::new(); m.insert("src".into(), json!("toolA")); m.insert("dst".into(), json!("executor")); m.insert("cost".into(), json!(2.5)); m },
    ];
    write_rows(store, edges_tbl, edges);
}

pub fn write_vindex_sidecar(store: &SharedStore, name: &str, table: &str, column: &str, metric: &str, dim: i32) {
    // Sidecar path: <name>.vindex under root (name is expected qualified but tests use simple name)
    let mut p = store.0.lock().root_path().clone();
    let local = name.replace('/', std::path::MAIN_SEPARATOR_STR);
    p.push(local);
    p.set_extension("vindex");
    let vf = serde_json::json!({
        "version": 1,
        "name": name,
        "qualified": name,
        "table": table,
        "column": column,
        "algo": "hnsw",
        "metric": metric,
        "dim": dim,
        "params": {"M": 32, "ef_build": 200, "ef_search": 64},
        "status": {"state": "ready"},
        "created_at": "2025-01-01T00:00:00Z"
    });
    std::fs::create_dir_all(p.parent().unwrap()).ok();
    std::fs::write(&p, serde_json::to_string_pretty(&vf).unwrap()).unwrap();
}

pub fn write_graph_sidecar(store: &SharedStore, name: &str, nodes_tbl: &str, edges_tbl: &str) {
    // Sidecar path: <name>.graph
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
            {"type": "Calls", "from":"Tool", "to":"Tool", "table": edges_tbl, "src_column":"src", "dst_column":"dst", "cost_column":"cost"}
        ],
        "created_at": "2025-01-01T00:00:00Z"
    });
    std::fs::create_dir_all(p.parent().unwrap()).ok();
    std::fs::write(&p, serde_json::to_string_pretty(&gf).unwrap()).unwrap();
}

pub fn write_graph_sidecar_with_time(store: &SharedStore, name: &str, nodes_tbl: &str, edges_tbl: &str, time_col: &str) {
    // Sidecar path: <name>.graph with time_column on edges
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
            {"type": "Calls", "from":"Tool", "to":"Tool", "table": edges_tbl, "src_column":"src", "dst_column":"dst", "cost_column":"cost", "time_column": time_col}
        ],
        "created_at": "2025-01-01T00:00:00Z"
    });
    std::fs::create_dir_all(p.parent().unwrap()).ok();
    std::fs::write(&p, serde_json::to_string_pretty(&gf).unwrap()).unwrap();
}

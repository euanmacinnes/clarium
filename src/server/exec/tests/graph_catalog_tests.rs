use crate::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::server::exec::exec_graph::GraphFile;
use crate::server::exec::tests::fixtures::*;
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

fn seed_simple_tables(tmp: &tempfile::TempDir) -> SharedStore {
    let store = Store::new(tmp.path()).unwrap();
    // nodes table
    let mut nodes: Vec<Record> = Vec::new();
    for (i, (id, emb)) in [
        ("planner", "0.1,0.0,0.0"),
        ("toolA", "0.2,0.0,0.0"),
        ("executor", "0.3,0.0,0.0"),
    ]
    .iter()
    .enumerate()
    {
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(*id));
        m.insert("name".into(), json!(*id));
        m.insert("embed".into(), json!(*emb));
        nodes.push(Record { _time: i as i64, sensors: m });
    }
    store.write_records("clarium/public/know_nodes", &nodes).unwrap();

    // edges table
    let mut edges: Vec<Record> = Vec::new();
    for (i, (s, d, c)) in [
        ("planner", "toolA", 1.0),
        ("toolA", "executor", 2.0),
    ]
    .iter()
    .enumerate()
    {
        let mut m = serde_json::Map::new();
        m.insert("src".into(), json!(*s));
        m.insert("dst".into(), json!(*d));
        m.insert("cost".into(), json!(*c));
        edges.push(Record { _time: i as i64, sensors: m });
    }
    store.write_records("clarium/public/know_edges", &edges).unwrap();
    SharedStore::new(tmp.path()).unwrap()
}

fn read_graph_sidecar(store: &SharedStore, qualified: &str) -> Option<GraphFile> {
    let mut p = store.0.lock().root_path().clone();
    let local = qualified.replace('/', std::path::MAIN_SEPARATOR_STR);
    p.push(local);
    p.set_extension("graph");
    let text = std::fs::read_to_string(&p).ok()?;
    serde_json::from_str::<GraphFile>(&text).ok()
}

#[test]
fn create_show_drop_graph_happy() {
    let tmp = tempfile::tempdir().unwrap();
    let shared = seed_simple_tables(&tmp);

    let sql = "CREATE GRAPH know \
NODES (Doc KEY(id), Tool KEY(name)) \
EDGES (Calls FROM Tool TO Tool, Cites FROM Doc TO Doc) \
USING TABLES (nodes=clarium/public/know_nodes, edges=clarium/public/know_edges)";
    crate::server::exec::execute_query(&shared, sql).unwrap();

    let show = crate::server::exec::execute_query(&shared, "SHOW GRAPH know").unwrap();
    let arr = show.as_array().cloned().unwrap();
    assert_eq!(arr.len(), 1);

    // Sidecar exists
    let gf = read_graph_sidecar(&shared, "clarium/public/know").unwrap();
    assert_eq!(gf.edges.len(), 2);

    // Drop
    crate::server::exec::execute_query(&shared, "DROP GRAPH know").unwrap();
    let gf2 = read_graph_sidecar(&shared, "clarium/public/know");
    assert!(gf2.is_none());
}

#[test]
fn create_graph_missing_tables_errors_and_malformed_definition() {
    let tmp = tempfile::tempdir().unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Missing USING TABLES bindings is allowed at create, but runtime TVFs will fail; here check malformed grammar
    // Malformed NODES block
    let bad_sql = "CREATE GRAPH g1 NODES (Doc id) EDGES (Calls FROM Tool TO Tool)";
    let err = query::parse(bad_sql).err().unwrap();
    assert!(format!("{}", err).to_lowercase().contains("invalid nodes"));

    // Proper parse but referencing missing tables should still create the graph; TVFs will fail when used
    let ok_sql = "CREATE GRAPH g2 NODES (Doc KEY(id)) EDGES (Calls FROM Doc TO Doc) USING TABLES (nodes=clarium/public/miss_nodes, edges=clarium/public/miss_edges)";
    crate::server::exec::execute_query(&shared, ok_sql).unwrap();
    let show = crate::server::exec::execute_query(&shared, "SHOW GRAPH g2").unwrap();
    assert_eq!(show.as_array().unwrap().len(), 1);
}

use crate::server::exec::tests::fixtures::new_store;
use crate::server::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::storage::SharedStore;
use polars::prelude::*;

fn write_le_u16(buf: &mut Vec<u8>, v: u16) { buf.extend_from_slice(&v.to_le_bytes()); }
fn write_le_u32(buf: &mut Vec<u8>, v: u32) { buf.extend_from_slice(&v.to_le_bytes()); }
fn write_le_u64(buf: &mut Vec<u8>, v: u64) { buf.extend_from_slice(&v.to_le_bytes()); }

fn write_gstore_fixture(store: &SharedStore, qname: &str,
                        nodes: &[(&str, &str)],
                        edges: &[(u64, u64)]) {
    // Layout: <root>/<qname>.gstore/{meta, nodes, edges}
    let mut root = store.0.lock().root_path().clone();
    let local = qname.replace('/', std::path::MAIN_SEPARATOR_STR);
    root.push(local);
    root.set_extension("gstore");

    let meta_dir = root.join("meta");
    let nodes_dir = root.join("nodes");
    let edges_dir = root.join("edges");
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&nodes_dir).unwrap();
    std::fs::create_dir_all(&edges_dir).unwrap();

    // 1) Write dict JSON fallback
    // Map provided nodes to IDs in order
    let mut entries = Vec::new();
    for (i, (label, key)) in nodes.iter().enumerate() {
        entries.push(serde_json::json!({"label": *label, "key": *key, "id": i as u64}));
    }
    let dict_json = serde_json::json!({"entries": entries});
    std::fs::write(nodes_dir.join("dict.seg.json"), serde_json::to_string_pretty(&dict_json).unwrap()).unwrap();

    // 2) Write a minimal CSR adjacency segment for partition 0 as binary
    // Header: magic:u32, version:u16=1, flags:u16=0, nodes:u64, edges:u64, row_ptr_off:u64, cols_off:u64
    // Build row_ptr for forward adjacency from provided edges (assumes nodes are densely 0..N-1)
    let n_nodes = nodes.len() as u64;
    let n_edges = edges.len() as u64;
    let mut row_ptr: Vec<u64> = vec![0; n_nodes as usize + 1];
    {
        // Count outgoing per node
        let mut counts = vec![0u64; n_nodes as usize];
        for (s, _d) in edges.iter() { counts[*s as usize] += 1; }
        // Prefix sum
        let mut acc = 0u64;
        for i in 0..n_nodes as usize {
            row_ptr[i] = acc;
            acc += counts[i];
        }
        row_ptr[n_nodes as usize] = acc;
    }
    // Build cols in node order, stable within each src
    let mut cols: Vec<u64> = vec![0; n_edges as usize];
    {
        let mut write_idx = vec![0u64; n_nodes as usize];
        for i in 0..n_nodes as usize { write_idx[i] = row_ptr[i]; }
        for (s, d) in edges.iter() {
            let pos = write_idx[*s as usize] as usize;
            cols[pos] = *d;
            write_idx[*s as usize] += 1;
        }
    }
    // Compute offsets. Place header (40 bytes), then row_ptr (8*(N+1)), then cols (8*E).
    let mut file: Vec<u8> = Vec::new();
    const ADJ_MAGIC: u32 = 0x4144474A; // must match reader
    write_le_u32(&mut file, ADJ_MAGIC);
    write_le_u16(&mut file, 1u16); // version
    write_le_u16(&mut file, 0u16); // flags
    write_le_u64(&mut file, n_nodes);
    write_le_u64(&mut file, n_edges);
    let row_ptr_off = 40u64;
    let cols_off = row_ptr_off + ((n_nodes + 1) * 8);
    write_le_u64(&mut file, row_ptr_off);
    write_le_u64(&mut file, cols_off);
    // Append row_ptr
    for v in &row_ptr { write_le_u64(&mut file, *v); }
    // Append cols
    for v in &cols { write_le_u64(&mut file, *v); }
    // Write file
    let seg_path = edges_dir.join("adj.P000.seg.1");
    std::fs::write(&seg_path, &file).unwrap();

    // 3) Write manifest.json
    let manifest = serde_json::json!({
        "engine": "graphstore",
        "epoch": 1,
        "partitions": 1,
        "nodes": { "dict_segments": ["nodes/dict.seg.json"] },
        "edges": { "has_reverse": false, "partitions": [ {"part": 0, "adj_segments": ["edges/adj.P000.seg.1"] } ] }
    });
    std::fs::write(meta_dir.join("manifest.json"), serde_json::to_string_pretty(&manifest).unwrap()).unwrap();
}

#[test]
#[ignore]
fn graphstore_neighbors_bfs_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);

    // Nodes: 0=planner, 1=toolA, 2=executor
    let nodes = [("Tool", "planner"), ("Tool", "toolA"), ("Tool", "executor")];
    // Edges: planner->toolA, toolA->executor
    let edges = [(0u64, 1u64), (1u64, 2u64)];
    write_gstore_fixture(&store, "clarium/public/know", &nodes, &edges);

    // Query neighbors up to 2 hops from planner
    let sql = "SELECT * FROM graph_neighbors('clarium/public/know','planner','Calls',2) ORDER BY hop, node_id";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    // Expect two rows: toolA (hop 1, prev planner), executor (hop 2, prev toolA)
    assert_eq!(df.height(), 2);
    let node_id0 = df.column("node_id").unwrap().get(0).unwrap().get_str().unwrap().to_string();
    let prev_id0 = df.column("prev_id").unwrap().get(0).unwrap().get_str().unwrap().to_string();
    let hop0 = df.column("hop").unwrap().get(0).unwrap().try_extract::<i64>().unwrap();
    let node_id1 = df.column("node_id").unwrap().get(1).unwrap().get_str().unwrap().to_string();
    let prev_id1 = df.column("prev_id").unwrap().get(1).unwrap().get_str().unwrap().to_string();
    let hop1 = df.column("hop").unwrap().get(1).unwrap().try_extract::<i64>().unwrap();
    assert_eq!((node_id0.as_str(), prev_id0.as_str(), hop0), ("toolA", "planner", 1));
    assert_eq!((node_id1.as_str(), prev_id1.as_str(), hop1), ("executor", "toolA", 2));
}

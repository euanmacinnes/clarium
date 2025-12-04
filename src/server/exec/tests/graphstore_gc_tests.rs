use crate::server::graphstore::delta_log::{DeltaLogWriter, DeltaLogReader, EdgeDeltaRec};
use crate::server::graphstore::txn::GraphTxn;
use crate::server::graphstore::segments::AdjSegment;
use crate::server::graphstore::{GraphHandle, graphstore_status_df};
use crate::server::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::server::exec::tests::fixtures::new_store;
use crate::storage::SharedStore;

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
    let mut entries = Vec::new();
    for (i, (label, key)) in nodes.iter().enumerate() {
        entries.push(serde_json::json!({"label": *label, "key": *key, "id": i as u64}));
    }
    let dict_json = serde_json::json!({"entries": entries});
    std::fs::write(nodes_dir.join("dict.seg.json"), serde_json::to_string_pretty(&dict_json).unwrap()).unwrap();

    // 2) Write minimal CSR adjacency segment for partition 0
    let n_nodes = nodes.len() as u64;
    let n_edges = edges.len() as u64;
    let mut row_ptr: Vec<u64> = vec![0; n_nodes as usize + 1];
    {
        let mut counts = vec![0u64; n_nodes as usize];
        for (s, _d) in edges.iter() { counts[*s as usize] += 1; }
        let mut acc = 0u64;
        for i in 0..n_nodes as usize { row_ptr[i] = acc; acc += counts[i]; }
        row_ptr[n_nodes as usize] = acc;
    }
    let mut cols: Vec<u64> = vec![0; n_edges as usize];
    {
        let mut write_idx = vec![0u64; n_nodes as usize];
        for i in 0..n_nodes as usize { write_idx[i] = row_ptr[i]; }
        for (s, d) in edges.iter() { let pos = write_idx[*s as usize] as usize; cols[pos] = *d; write_idx[*s as usize] += 1; }
    }
    let mut file: Vec<u8> = Vec::new();
    const ADJ_MAGIC: u32 = 0x4144474Au32; // 'JGDA'
    write_le_u32(&mut file, ADJ_MAGIC);
    write_le_u16(&mut file, 1u16);
    write_le_u16(&mut file, 0u16);
    write_le_u64(&mut file, n_nodes);
    write_le_u64(&mut file, n_edges);
    let row_ptr_off = 40u64; let cols_off = row_ptr_off + ((n_nodes + 1) * 8);
    write_le_u64(&mut file, row_ptr_off);
    write_le_u64(&mut file, cols_off);
    for v in &row_ptr { write_le_u64(&mut file, *v); }
    for v in &cols { write_le_u64(&mut file, *v); }
    std::fs::write(edges_dir.join("adj.P000.seg.1"), &file).unwrap();

    // 3) Manifest
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
fn gc_graph_triggers_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    let qname = "clarium/public/know";
    let nodes = [("Tool","planner"),("Tool","toolA"),("Tool","executor")];
    let edges = [(0u64,1u64),(1u64,2u64)];
    write_gstore_fixture(&store, qname, &nodes, &edges);

    // Create a delta log with one add to exceed a low threshold
    std::env::set_var("CLARIUM_GRAPH_GC_MAX_DELTA_RECORDS", "1");
    let mut root = store.0.lock().root_path().clone();
    let local = qname.replace('/', std::path::MAIN_SEPARATOR_STR); root.push(local); root.set_extension("gstore");
    let dpath = root.join("edges").join("delta.P000.log");
    { let mut w = DeltaLogWriter::open_append(&dpath).unwrap(); w.append_edge(&EdgeDeltaRec{ txn_id: 1, op_index: 0, op: 0, src: 0, dst: 2 }).unwrap(); }

    // Run GC DDL
    // execute_query is async; run it on a tokio runtime
    let rt = tokio::runtime::Runtime::new().unwrap();
    let resp = rt.block_on(crate::server::exec::execute_query(&store, "GC GRAPH clarium/public/know")).unwrap();
    assert!(resp.get("status").is_some());

    // Verify epoch bumped to 2 and new adj segment exists
    let mut handle = GraphHandle::open(&store, qname).unwrap();
    handle.ensure_loaded().unwrap();
    assert!(handle.manifest.epoch.unwrap_or(0) >= 2);
    let found_new = handle.manifest.edges.partitions[0].adj_segments.iter().any(|s| s.ends_with(".2"));
    assert!(found_new);
}

#[test]
fn show_graph_status_reports_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    let qname = "clarium/public/know";
    write_gstore_fixture(&store, qname, &[("Tool","planner")], &[]);
    let df = graphstore_status_df(&store, qname).unwrap();
    let cols = df.get_column_names();
    for must in ["graph","epoch","partitions","delta_adds","delta_tombstones","bfs_calls","wal_commits","recoveries"] {
        assert!(cols.iter().any(|c| c.as_str() == must), "missing status column: {}", must);
    }
}

#[test]
fn wal_group_commit_batching_appends_records() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);
    let qname = "clarium/public/know";
    write_gstore_fixture(&store, qname, &[("Tool","planner"),("Tool","toolA")], &[(0,1)]);
    // Graph root path
    let mut root = store.0.lock().root_path().clone();
    let local = qname.replace('/', std::path::MAIN_SEPARATOR_STR); root.push(local); root.set_extension("gstore");

    // Set a small batching window and commit two txns in parallel
    std::env::set_var("CLARIUM_GRAPH_COMMIT_WINDOW_MS", "5");
    let r1 = root.clone(); let r2 = root.clone();
    let t1 = std::thread::spawn(move || {
        let mut tx = GraphTxn::begin(&r1, 0).unwrap();
        tx.insert_edge(0, 0, 1, 1);
        tx.commit(1).unwrap();
    });
    let t2 = std::thread::spawn(move || {
        let mut tx = GraphTxn::begin(&r2, 0).unwrap();
        tx.insert_edge(0, 1, 0, 1);
        tx.commit(1).unwrap();
    });
    t1.join().unwrap(); t2.join().unwrap();

    // Read delta log and ensure both records are present
    let dpath = root.join("edges").join("delta.P000.log");
    let mut r = DeltaLogReader::open(&dpath).unwrap();
    let recs = r.read_all_edges().unwrap();
    assert!(recs.len() >= 2);
}

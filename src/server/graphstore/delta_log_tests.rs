use super::*;
use std::collections::HashSet;

#[test]
fn delta_log_roundtrip_and_apply() {
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("delta.P000.log");
    {
        let mut w = DeltaLogWriter::open_append(&p).unwrap();
        w.append_edge(&EdgeDeltaRec{ txn_id:1, op_index:0, op:0, src:0, dst:1 }).unwrap();
        w.append_edge(&EdgeDeltaRec{ txn_id:1, op_index:1, op:1, src:0, dst:2 }).unwrap();
    }
    let mut r = DeltaLogReader::open(&p).unwrap();
    let recs = r.read_all_edges().unwrap();
    assert_eq!(recs.len(), 2);
    let mut idx = PartitionDeltaIndex::default();
    let mut seen = HashSet::new();
    apply_edge_deltas(&mut idx, &recs, &mut seen);
    assert!(idx.adds.get(&0).is_some());
    assert!(idx.tombstones.contains(&(0,2)));
    // Re-apply: idempotent due to seen set
    apply_edge_deltas(&mut idx, &recs, &mut seen);
    assert_eq!(idx.adds.get(&0).unwrap().len(), 1);
}

#[test]
fn node_delta_log_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("nodes").join("delta.log");
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    {
        let mut w = NodeDeltaLogWriter::open_append(&p).unwrap();
        w.append_node(&NodeDeltaRec{ txn_id:1, op_index:0, op:0, label:"Tool".into(), key:"planner".into(), node_id: Some(0) }).unwrap();
        w.append_node(&NodeDeltaRec{ txn_id:2, op_index:0, op:1, label:"Tool".into(), key:"old".into(), node_id: None }).unwrap();
    }
    let mut r = NodeDeltaLogReader::open(&p).unwrap();
    let recs = r.read_all_nodes().unwrap();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0].label.as_str(), "Tool");
    assert_eq!(recs[0].node_id, Some(0));
}

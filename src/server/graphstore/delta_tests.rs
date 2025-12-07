use super::*;
use crate::server::graphstore::wal::{WalWriter, TxnBegin, TxnData, NodeOp, EdgeOp, TxnCommit};

#[test]
fn build_from_wal_records_committed_only() {
    // Build an in-memory record list: committed txn with two edges and one delete
    let d = TxnData{
        txn_id: 1,
        nodes: vec![NodeOp{ op:0, label:"Tool".into(), key:"planner".into(), node_id: Some(0) }],
        edges: vec![
            EdgeOp{ op:0, part:0, src:0, dst:1, etype_id:1 },
            EdgeOp{ op:1, part:0, src:0, dst:2, etype_id:1 },
        ]
    };
    let recs = vec![WalRecord::Begin(TxnBegin{ txn_id:1, snapshot_epoch:0 }), WalRecord::Data(d), WalRecord::Commit(TxnCommit{ txn_id:1, commit_epoch:1 })];
    let idxs = build_indexes_from_records(&recs).unwrap();
    let p0 = idxs.get(&0).unwrap();
    assert!(p0.adds.get(&0).is_some());
    assert_eq!(p0.adds.get(&0).unwrap().len(), 1);
    assert!(p0.tombstones.contains(&(0,2)));
}
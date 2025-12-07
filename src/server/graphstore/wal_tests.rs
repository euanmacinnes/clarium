use super::*;

#[test]
fn wal_begin_data_commit_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("waltest.lg");
    let mut w = WalWriter::create(&p).unwrap();
    let begin = TxnBegin{ txn_id: 42, snapshot_epoch: 7 };
    let data = TxnData{ txn_id: 42, nodes: vec![NodeOp{ op:0, label:"Tool".into(), key:"planner".into(), node_id: Some(0) }], edges: vec![EdgeOp{ op:0, part:0, src:0, dst:1, etype_id:1 }] };
    let commit = TxnCommit{ txn_id: 42, commit_epoch: 8 };
    w.append_begin(&begin).unwrap();
    w.append_data(&data).unwrap();
    w.append_commit(&commit).unwrap();

    let mut r = WalReader::open(&p).unwrap();
    let recs = r.read_all().unwrap();
    assert_eq!(recs.len(), 3);
    match &recs[0] { super::WalRecord::Begin(b) => { assert_eq!(b.txn_id, 42); }, _ => panic!("expected begin") }
    match &recs[1] { super::WalRecord::Data(d) => { assert_eq!(d.edges.len(), 1); }, _ => panic!("expected data") }
    match &recs[2] { super::WalRecord::Commit(c) => { assert_eq!(c.commit_epoch, 8); }, _ => panic!("expected commit") }
}

#[test]
fn wal_crc_corruption_detected() {
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("waltest_bad.lg");
    let mut w = WalWriter::create(&p).unwrap();
    let begin = TxnBegin{ txn_id: 1, snapshot_epoch: 0 };
    w.append_begin(&begin).unwrap();
    // Corrupt last byte
    {
        use std::io::{Seek, SeekFrom, Read, Write};
        let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&p).unwrap();
        let end = f.seek(SeekFrom::End(0)).unwrap();
        if end > 0 { f.seek(SeekFrom::Start(end - 1)).unwrap(); let mut b=[0u8;1]; f.read_exact(&mut b).unwrap(); b[0] ^= 0xFF; f.seek(SeekFrom::Start(end - 1)).unwrap(); f.write_all(&b).unwrap(); }
    }
    let mut r = WalReader::open(&p).unwrap();
    let res = r.read_all();
    assert!(res.is_err(), "expected CRC mismatch error");
}

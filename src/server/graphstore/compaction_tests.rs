use super::*;
use crate::server::graphstore::segments::AdjSegment;

#[test]
fn compact_merge_adds_and_tombstones() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    // Build a tiny base segment with 3 nodes: 0->1, 1->2
    let base_path = root.join("edges"); std::fs::create_dir_all(&base_path).unwrap();
    let base_file = base_path.join("adj.P000.seg.1");

    // Manually write base
    let nodes = 3u64; let cols_base = vec![1u64, 2u64]; let row_ptr = vec![0u64,1u64,2u64,2u64];
    let mut buf: Vec<u8> = Vec::new();
    const ADJ_MAGIC: u32 = 0x4144474A;
    buf.extend_from_slice(&ADJ_MAGIC.to_le_bytes());
    buf.extend_from_slice(&1u16.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes());
    buf.extend_from_slice(&nodes.to_le_bytes());
    buf.extend_from_slice(&(cols_base.len() as u64).to_le_bytes());
    let row_ptr_off = 40u64;
    let cols_off = row_ptr_off + ((nodes + 1) * 8);
    buf.extend_from_slice(&row_ptr_off.to_le_bytes());
    buf.extend_from_slice(&cols_off.to_le_bytes());
    for v in &row_ptr { buf.extend_from_slice(&v.to_le_bytes()); }
    for v in &cols_base { buf.extend_from_slice(&v.to_le_bytes()); }
    std::fs::create_dir_all(base_file.parent().unwrap()).unwrap();
    std::fs::write(&base_file, &buf).unwrap();

    let base_seg = AdjSegment::open(&base_file).unwrap();
    // Delta: add 0->2 and tombstone 1->2
    let mut d = PartitionDeltaIndex::default();
    d.add_edge(0, 2);
    d.del_edge(1, 2);
    let rel = compact_partition(root, 0, &base_seg, Some(&d), 2).unwrap();
    let new_seg = AdjSegment::open(&root.join(&rel)).unwrap();
    // Verify ranges: 0 has neighbors [1,2]; 1 has []; 2 has []
    let (s0,e0) = new_seg.neighbors_range(0).unwrap();
    assert_eq!(e0 - s0, 2);
    let v0 = new_seg.neighbor_at(s0); let v1 = new_seg.neighbor_at(s0+1);
    assert_eq!((v0,v1), (1,2));
    let (s1,e1) = new_seg.neighbors_range(1).unwrap();
    assert_eq!(e1 - s1, 0);
}

use super::*;
use serde_json::json;

#[test]
fn rotate_manifest_writes_and_replaces() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let meta = root.join("meta");
    std::fs::create_dir_all(&meta).unwrap();

    // Seed an initial manifest.json
    let m0 = json!({
        "engine": "graphstore",
        "epoch": 1,
        "partitions": 1,
        "nodes": { "dict_segments": ["nodes/dict.seg.json"] },
        "edges": { "has_reverse": false, "partitions": [ {"part":0, "adj_segments": ["edges/adj.P000.seg.1"] } ] }
    });
    let p0 = meta.join("manifest.json");
    std::fs::write(&p0, serde_json::to_string_pretty(&m0).unwrap()).unwrap();

    // Prepare next manifest with bumped epoch and different segment
    let m1 = json!({
        "engine": "graphstore",
        "epoch": 2,
        "partitions": 1,
        "nodes": { "dict_segments": ["nodes/dict.seg.json"] },
        "edges": { "has_reverse": false, "partitions": [ {"part":0, "adj_segments": ["edges/adj.P000.seg.2"] } ] }
    });
    let next_json = serde_json::to_string_pretty(&m1).unwrap();
    rotate_manifest(root, &next_json).unwrap();

    // Verify manifest.json now contains epoch 2
    let got = std::fs::read_to_string(meta.join("manifest.json")).unwrap();
    let v: serde_json::Value = serde_json::from_str(&got).unwrap();
    assert_eq!(v["epoch"].as_i64().unwrap(), 2);
    assert_eq!(v["edges"]["partitions"][0]["adj_segments"][0], "edges/adj.P000.seg.2");
}

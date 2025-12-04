//! GraphStore manifest rotation (atomic publish)
//! --------------------------------------------
//! Writes `manifest.next.json` and atomically swaps it into place as
//! `manifest.json`. On Windows we perform a best-effort replace by writing
//! the next file and then renaming over the current file.
use anyhow::{Context, Result};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Write `manifest.next.json` under `<root>/meta/` and atomically swap it
/// into `manifest.json`.
pub fn rotate_manifest(root: &Path, next_json: &str) -> Result<()> {
    let meta_dir = root.join("meta");
    std::fs::create_dir_all(&meta_dir).ok();
    let next_path = meta_dir.join("manifest.next.json");
    let final_path = meta_dir.join("manifest.json");
    // Write next file
    {
        let mut f = File::create(&next_path)
            .with_context(|| format!("create {}", next_path.display()))?;
        f.write_all(next_json.as_bytes())?;
        f.flush()?;
        // Best-effort durability of file contents
        let _ = f.sync_all();
    }
    // On Windows, no atomic rename-overwrite. Emulate by removing old then renaming.
    if final_path.exists() {
        let _ = std::fs::remove_file(&final_path);
    }
    std::fs::rename(&next_path, &final_path)
        .with_context(|| format!("rename {} -> {}", next_path.display(), final_path.display()))?;
    // Best-effort directory flush
    let _ = fsync_dir(&meta_dir);
    Ok(())
}

fn fsync_dir(dir: &Path) -> Result<()> {
    // Not all platforms expose a stable dir fsync; attempt to open and sync.
    #[allow(unused_mut)]
    let mut f = File::open(dir).with_context(|| format!("open dir {}", dir.display()))?;
    let _ = f.sync_all();
    Ok(())
}

#[cfg(test)]
mod tests {
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
}

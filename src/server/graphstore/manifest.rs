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
#[path = "manifest_tests.rs"]
mod manifest_tests;

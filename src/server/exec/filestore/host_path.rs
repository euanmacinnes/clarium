use anyhow::{bail, Result};
use path_absolutize::Absolutize;
use std::fs;
use std::path::{Path, PathBuf};

/// Normalize a host path to an absolute, canonical-like string without resolving symlinks.
/// Returns a platform-native absolute path string.
pub fn normalize_abs_path(p: &str) -> Result<String> {
    let pb = PathBuf::from(p);
    // Absolutize without touching filesystem semantics beyond normalization
    let abs = pb.absolutize()?.to_path_buf();
    Ok(abs.to_string_lossy().to_string())
}

/// Check if a given host path is allowed based on a semicolon- or comma-separated allowlist
/// of absolute path prefixes. Symlinks/junctions are denied by default.
pub fn is_host_path_allowed(candidate: &str, allowlist: &str) -> Result<bool> {
    let cand_abs = normalize_abs_path(candidate)?;
    // Deny symlinks/junctions
    if is_symlink(candidate) {
        return Ok(false);
    }
    let parts = allowlist
        .split(|c| c == ';' || c == ',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());
    for prefix in parts {
        let pref_abs = normalize_abs_path(prefix)?;
        if is_prefix_path(&cand_abs, &pref_abs) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn is_symlink(p: &str) -> bool {
    let meta = fs::symlink_metadata(p);
    match meta {
        Ok(m) => m.file_type().is_symlink(),
        Err(_) => false,
    }
}

fn is_prefix_path(path: &str, prefix: &str) -> bool {
    // Compare component-wise to avoid false positives like C:\data\x vs C:\data2
    let p = Path::new(path);
    let pre = Path::new(prefix);
    // Quick rejection if different roots on Windows
    if cfg!(windows) {
        let pr = p.components().next();
        let rr = pre.components().next();
        if pr != rr { return false; }
    }
    // Now check if `path` starts with `prefix`
    p.starts_with(pre)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlist_basic() {
        let tmp = if cfg!(windows) { "C:/" } else { "/" };
        let ok = is_host_path_allowed(tmp, tmp).unwrap();
        assert!(ok);
    }
}

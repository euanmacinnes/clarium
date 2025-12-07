use anyhow::Result;
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
    // Quick reject if allowlist is empty
    if allowlist.trim().is_empty() {
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

pub(crate) fn is_symlink(p: &str) -> bool {
    let meta = fs::symlink_metadata(p);
    match meta {
        Ok(m) => {
            let ft = m.file_type();
            if ft.is_symlink() { return true; }
            // On Windows, directory junctions and mount points present as reparse points.
            // Detect via MetadataExt attributes.
            #[cfg(windows)]
            {
                use std::os::windows::fs::MetadataExt;
                const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
                let attrs = m.file_attributes();
                return (attrs & FILE_ATTRIBUTE_REPARSE_POINT) != 0;
            }
            #[cfg(not(windows))]
            { false }
        }
        Err(_) => false,
    }
}

pub(crate) fn is_prefix_path(path: &str, prefix: &str) -> bool {
    // Compare component-wise to avoid false positives like C:\data\x vs C:\data2
    let p = Path::new(path);
    let pre = Path::new(prefix);
    // Quick rejection if different roots on Windows
    if cfg!(windows) {
        let pr = p.components().next();
        let rr = pre.components().next();
        if pr != rr { return false; }
    }
    // Primary: Path::starts_with is component-aware
    if p.starts_with(pre) { return true; }
    // Fallback: normalized, case-insensitive compare for Windows-only edge cases
    #[cfg(windows)]
    {
        let pn = normalize_for_compare(path);
        let pren = normalize_for_compare(prefix);
        return pn.starts_with(&pren);
    }
    #[cfg(not(windows))]
    { false }
}

#[cfg(windows)]
fn normalize_for_compare(s: &str) -> String {
    // Normalize slashes and lowercase for case-insensitive filesystem
    let mut out = s.replace('/', "\\");
    out.make_ascii_lowercase();
    // Ensure trailing backslash on prefix matching semantics handled by caller
    out
}


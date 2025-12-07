use anyhow::{bail, Result};
use unicode_normalization::UnicodeNormalization;

/// Normalize a UTF-8 string to NFC.
pub fn normalize_nfc(input: &str) -> String {
    input.nfc().collect::<String>()
}

/// Validate a logical path according to FILESTORE rules:
/// - UTF-8 string, segments separated by '/'
/// - NUL ("\u{0000}") not allowed
/// - '/' only used as separator; no empty segments (disallow leading/trailing or '//' sequences)
/// Returns Ok(()) if valid, Err otherwise.
pub fn validate_logical_path(path: &str) -> Result<()> {
    if path.is_empty() {
        bail!("logical path cannot be empty");
    }
    if path.chars().any(|c| c == '\u{0000}') {
        bail!("logical path cannot contain NUL characters");
    }
    // Allow a single "." as current dir? For now disallow dot-semantics entirely.
    if path == "." || path == ".." {
        bail!("'.' and '..' are not allowed as logical paths");
    }
    if path.starts_with('/') || path.ends_with('/') {
        bail!("leading or trailing '/' is not allowed in logical paths");
    }
    if path.contains("//") {
        bail!("empty segments ('//') are not allowed in logical paths");
    }
    // Disallow segments that are exactly '.' or '..'
    for seg in path.split('/') {
        if seg.is_empty() {
            bail!("empty segment in logical path");
        }
        if seg == "." || seg == ".." {
            bail!("segments '.' and '..' are not allowed");
        }
    }
    Ok(())
}

/// Split a logical path into NFC-normalized segments. Validation is performed first.
pub fn split_normalized_segments(path: &str) -> Result<Vec<String>> {
    validate_logical_path(path)?;
    let n = normalize_nfc(path);
    Ok(n.split('/').map(|s| s.to_string()).collect())
}

#[cfg(test)]
#[path = "paths_tests.rs"]
mod paths_tests;

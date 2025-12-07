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
mod tests {
    use super::*;

    #[test]
    fn test_normalize_nfc_basic() {
        // 'e' + combining acute should normalize to 'é'
        let s = "Cafe\u{0301}"; // Café
        let n = normalize_nfc(s);
        assert_eq!(n, "Café");
    }

    #[test]
    fn test_validate_and_split_segments() {
        let p = "研发/📚Docs/specs/RFC-1.md";
        validate_logical_path(p).unwrap();
        let segs = split_normalized_segments(p).unwrap();
        assert_eq!(segs, vec!["研发", "📚Docs", "specs", "RFC-1.md"]);
    }

    #[test]
    fn test_invalid_paths() {
        assert!(validate_logical_path("").is_err());
        assert!(validate_logical_path("/leading").is_err());
        assert!(validate_logical_path("trailing/").is_err());
        assert!(validate_logical_path("double//slash").is_err());
        assert!(validate_logical_path("a/./b").is_err());
        assert!(validate_logical_path("a/../b").is_err());
        let with_nul = format!("a\u{0000}b");
        assert!(validate_logical_path(&with_nul).is_err());
    }
}

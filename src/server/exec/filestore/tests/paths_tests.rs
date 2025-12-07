use super::*;
use crate::server::exec::filestore::*;

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

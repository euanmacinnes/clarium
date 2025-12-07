use super::*;

#[test]
fn validate_logical_path_ok_and_bad() {
    // Good
    assert!(validate_logical_path("a/b/c").is_ok());
    // Bad: leading slash
    assert!(validate_logical_path("/a").is_err());
    // Bad: trailing slash
    assert!(validate_logical_path("a/").is_err());
    // Bad: empty segment
    assert!(validate_logical_path("a//b").is_err());
    // Bad: dot segments
    assert!(validate_logical_path(".").is_err());
    assert!(validate_logical_path("..").is_err());
    assert!(validate_logical_path("a/../b").is_err());
}

#[test]
fn split_normalized_segments_roundtrip() {
    let segs = split_normalized_segments("foo/bar").unwrap();
    assert_eq!(segs, vec!["foo", "bar"]);
}

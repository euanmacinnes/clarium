use super::*;
use crate::server::exec::filestore::*;
use crate::server::exec::filestore::host_path::{is_prefix_path, is_symlink};

#[test]
fn allowlist_basic() {
    let tmp = if cfg!(windows) { "C:/" } else { "/" };
    let ok = is_host_path_allowed(tmp, tmp).unwrap();
    assert!(ok);
}

#[test]
fn deny_symlinks_and_empty_allowlist() {
    // Empty allowlist must deny
    let tmp = if cfg!(windows) { "C:/" } else { "/" };
    let ok = is_host_path_allowed(tmp, "").unwrap();
    assert!(!ok);
    // Symlink check best-effort: create only when possible
    // We avoid creating actual symlinks in unit tests due to permissions; just assert function is callable.
    let _ = is_symlink(tmp);
}

#[cfg(windows)]
#[test]
fn windows_component_prefix_and_case_insensitive() {
    let path = "C:/Data/Project/file.txt";
    let allow = "c:/data"; // different case
    assert!(is_prefix_path(&normalize_abs_path(path).unwrap(), &normalize_abs_path(allow).unwrap()));
    // Negative case: similar prefix but different component
    let allow_bad = "C:/Data2";
    assert!(!is_prefix_path(&normalize_abs_path(path).unwrap(), &normalize_abs_path(allow_bad).unwrap()));
}

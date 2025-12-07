use super::*;

#[test]
fn keys_are_namespaced() {
    let db = "clarium"; let fs = "docs"; let id = Uuid::nil();
    let k = Keys::blob(db, fs, &id);
    assert!(k.contains("clarium.store.filestore.docs.blob::"));
}

#[test]
fn etag_xxh3_hex_len() {
    let e = etag_for_bytes(b"hello world");
    assert_eq!(e.len(), 16);
}

#[test]
fn prefixes_are_correct() {
    let db = "clarium"; let fs = "docs";
    assert!(Keys::blob(db, fs, &Uuid::nil()).starts_with(&Keys::blob_prefix(db, fs)));
    assert!(Keys::path(db, fs, "a/b").starts_with(&Keys::path_prefix(db, fs)));
    assert!(Keys::tree(db, fs, &Uuid::nil()).starts_with(&Keys::tree_prefix(db, fs)));
    assert!(Keys::commit(db, fs, &Uuid::nil()).starts_with(&Keys::commit_prefix(db, fs)));
    assert!(Keys::alias(db, fs, "").starts_with(&Keys::alias_prefix(db, fs)));
    let gr = Keys::git_ref(db, fs, "local", "main");
    assert!(gr.starts_with(&Keys::git_ref_prefix(db, fs, "local")));
}

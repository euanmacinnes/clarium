# FILESTORE — Git backend plan: gitoxide primary, libgit2 fallback for push

Version: 1.0
Last updated: 2025-12-07

## Goal
Use gitoxide (Rust-native) for all Git plumbing in FILESTORE (objects, trees, commits, refs, ls-remote, fetch). Attempt push with gitoxide first; if unsupported, fallback to libgit2 for push only. Keep public DDL/API unchanged.

## Why gitoxide?
- Pure Rust, safer and often faster; no C toolchain required by default.
- Active ecosystem (gix-* crates). Reduces FFI and deployment complexity.
- Fallback keeps production capability where gitoxide lacks specific push auth/refspec support.

## Abstraction
- Internal trait `GitBackend` with methods: ensure_repo, write_blob, write_tree, write_commit, update_ref, ls_remote, fetch, push.
- Implementations:
  - GitoxideBackend (default for all methods; push may return Unsupported)
  - Libgit2Backend (compiled only with feature `libgit2-push`; implements push and minimal helpers)
  - CompositeGitBackend (uses gitoxide; on push Unsupported or by config, delegates to libgit2)

## Config (inherits Global → Filestore → Folder)
- git_backend = 'gitoxide' (reserved; default)
- git_push_backend = 'auto' | 'gitoxide' | 'libgit2' (default 'auto')
- Existing: git_remote, git_branch, git_mode, lfs_patterns

## Cargo features
- Default build: pure Rust (gitoxide only)
- Optional feature: `libgit2-push` to enable libgit2 dependency and push fallback

## Push flow
1) Build tree and commit via gitoxide
2) Update local ref
3) Try push via gitoxide
4) If unsupported or configured to use libgit2, and feature enabled, perform push via libgit2
5) Record commit_guid ↔ git_sha mapping; add SYNCED_TO edge

## Security & Observability
- ACL still covers commit/push/pull; dev bypass and acl_fail_open honored as per base plan
- tprintln: backend selection, fallback decision, transport start/end (no secrets logged)

## Testing
- Unit: tree assembly order, commit parents, ref updates
- Integration: local bare remote
  - gitoxide end-to-end (fetch/push when available)
  - fallback path with --features libgit2-push and git_push_backend=auto|libgit2
- UTF-8 paths in tree entries; round-trip checks

## Acceptance
- Default build performs all plumbing via gitoxide; push either succeeds via gitoxide or returns clear Unsupported
- With fallback feature, push succeeds via libgit2 when gitoxide cannot, producing identical refs/ids
- Public FILESTORE behavior unchanged

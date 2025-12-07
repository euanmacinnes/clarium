Clarium FILESTORE — Concepts and Architecture
=============================================

Overview
--------
Clarium’s FILESTORE provides immutable blob storage with logical file metadata, simple versioning via trees and commits, and optional Git plumbing for push/fetch. Storage is backed by the in‑memory KV adapter with predictable key prefixes to enable fast scans.

Key components
--------------
- Metadata (FileMeta): JSON records keyed by logical path, tracking id (UUID), size, etag, version, timestamps, content_type, deleted flag, and optional description/custom fields.
- Blobs: raw bytes keyed by the file UUID (id). Metadata and blobs are separate: renames don’t duplicate blob data.
- Trees: snapshots of logical paths to etag/size at a moment in time.
- Commits: capture a tree with author, message, tags, parents, and branch. Parents may be inferred from the current branch head.
- Refs: branch → head commit id (local namespace).
- Aliases: logical folder mappings to other stores/prefixes (for future composition).

KV key schema (prefixes)
------------------------
Keys are built using safe constructors (see src/server/exec/filestore/kv.rs — Keys::*). Important prefixes:
- path(db, fs, logical_path): metadata per logical path
- blob(db, fs, uuid): raw bytes for the file id
- chunk(db, fs, uuid): reserved for future chunk index
- tree(db, fs, uuid): tree snapshots
- commit(db, fs, uuid): commit objects
- git_ref(db, fs, scope, name): refs (scope="local" currently)
- alias(db, fs, name): alias objects
- info_registry(db, fs): filestore registry entry (also mirrored in default KV)

Logical paths
-------------
- UTF‑8 strings, NFC‑normalized.
- Slash‑separated with no empty segments; leading/trailing slashes are rejected.
- "." and ".." are disallowed as segments.

ACL and security
----------------
- All mutations (ingest/update/rename/delete/commit/push) invoke check_acl when security_check_enabled=true.
- The ACL client POSTs a request containing the user, action, logical path(s), and context (content size/type, git info, config version, request id).
- Fail‑open behavior is configurable per store. Transport errors may allow the action when acl_fail_open=true.
- Decisions are cached with TTLs; cache size is capped and evictions are logged with counters (hits/misses/evictions).

Observability and correlation IDs
---------------------------------
- A fresh CorrelationId is created at entry points; checkpoints include [corr=...] in tprintln logs.
- Secrets and payloads are redacted; logs include sizes and type lengths instead of full content.

Versioning model
----------------
- CREATE TREE captures current non‑deleted metadata, optionally filtered by a logical prefix.
- COMMIT TREE writes a commit with author metadata and optional parents/tags/branch.
- If parents are omitted, the current branch head is read and used (when present).
- Tags are normalized (trimmed, empty removed, deduplicated, sorted) for stable ordering.

SHOW/TVF outputs and Polars
----------------------------
- SHOW builders return DataFrames with stable columns and dtypes following the "Junie Polars 0.51+" guidance:
  - Avoid Utf8Chunked iterators; use Series::new and AnyValue access when needed.
  - For paging, build boolean masks or slice by index; return a typed empty frame when no rows remain.
- Temporary columns use "__" prefixes and are dropped before returning.

GC (conservative)
-----------------
- Tombstones are retained for at least GlobalFilestoreConfig.gc_grace_seconds (default 86,400s).
- gc_dry_run counts candidates; gc_apply deletes tombstoned metadata older than the grace period.
- Chunk orphan collection is deferred until reverse references are available (future work).

Git backends and push behavior
------------------------------
- GitBackend trait abstracts ensure_repo/write_tree/write_commit/update_ref/ls_remote/fetch/push.
- Selection is driven by EffectiveConfig.git_push_backend:
  - auto (default): prefer gitoxide; fallback to libgit2 push-only when compiled with feature "libgit2-push".
  - gitoxide: force gitoxide.
  - libgit2: use libgit2 push‑only backend when feature is enabled; otherwise, fallback to gitoxide.
- Unimplemented operations return clear Unsupported errors; no panics.

Host path ingestion (Windows‑first)
-----------------------------------
- Absolute path normalization and component‑wise allowlist checks are applied.
- Symlinks and junctions are denied. UNC paths are normalized and validated.
- When allowlist is empty, ingestion from host path is denied by default.

Error handling
--------------
- No panics in execution paths; errors are propagated with anyhow and reported to the client.
- DDL and ops executed via execute_query_safe are isolated in a task to convert internal panics to user‑visible errors without crashing serving threads.

Limitations and future work
---------------------------
- Chunk reference tracking is not yet implemented; ref_count remains 0 in SHOW CHUNKS.
- Rename heuristics in DIFF (detecting moves by similarity) is planned but disabled.
- Git push end‑to‑end flows are feature‑gated and may return Unsupported depending on build.

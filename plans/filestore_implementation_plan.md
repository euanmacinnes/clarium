# FILESTORE — Implementation Plan (v1.0)

Last updated: 2025-12-07

## Objective
Implement the end-to-end FILESTORE system as specified in prior plans, including:
- UTF-8 logical hierarchical paths (NFC normalized, `/` separator, NUL rejected).
- Dual namespace storage (logical paths ↔ GUID-addressed immutable content and manifests).
- Security: external POST-only ACL service with dev bypass and fail-open option; host path allowlist.
- Semantic search over chunks using existing vector/index capabilities.
- Git plumbing using gitoxide (primary), libgit2 fallback for push when enabled.
- Clones (folder aliasing) across multiple locations with live or pinned behavior.
- LLM exports and streaming; HTML description stored verbatim up to configured cap.
- Observability with permanent tprintln checkpoints.

This document breaks down components, milestones, interfaces, and test strategy to deliver the full feature set safely and incrementally.

---

## Architecture Summary
- Namespaces (per filestore `<name>`):
  - KV: `<db>.store.filestore.<name>.{blob,text,manifest,chunk,path,tree,commit,git,alias,map.git_sha}`
  - Tables: `fs_<name>_{files,chunks,trees,commits,aliases}`
  - Graph: Nodes: `LogicalPath, File, Manifest, Chunk, Tree, Commit, GitRemote, Alias` and edges `PATH_CONTAINS, HAS_MANIFEST, HAS_CHUNK, IN_TREE, PARENT_OF, SYNCED_TO, ALIAS_OF`.
  - Vector index on `fs_<name>_chunks(embedding)`.

- DDL and TVFs (parsing separated from execution):
  - Lifecycle: `CREATE/DROP/ALTER FILESTORE`, `SHOW FILESTORES`, `SHOW GLOBAL FILESTORE CONFIG`.
  - Ingest/update: `INGEST FILESTORE ... FILE/FOLDER`, `UPDATE FILE ...`, `RENAME`, `DELETE`.
  - Git: `CREATE TREE`, `COMMIT TREE`, `PUSH`, `PULL` (plumbing-only default), per-folder overrides.
  - Clones: `CLONE FOLDER ... AS ...`, `UNCLONE`, `SHOW CLONES`.
  - Search/LLM: `SEMANTIC SEARCH FILESTORE ...`, `GET FILE`, `GET CHUNKS`, `LLM_EXPORT`, `LLM_STREAM`.

- Security:
  - External ACL POST contract `{ data: 'ok'|'error', results: [{ allow, reason, effective_perms, ttl_ms }], error? }`.
  - Config precedence: Global → Filestore → Folder overrides (Git-specific subset).
  - Dev bypass: `security_check_enabled=false`; `acl_fail_open` supported.
  - Host path allowlist for `FROM HOST_PATH` ingestion.

---

## Milestones and Deliverables

1) Foundations and Configuration
   - 1.1 Information schema for global config and filestore registry (read/write). 
   - 1.2 DDL: `ALTER GLOBAL FILESTORE CONFIG`, `CREATE/DROP FILESTORE`, `SHOW FILESTORES`.
   - 1.3 Config precedence logic and resolver service (global → filestore → folder overrides).
   - 1.4 UTF-8 normalization utilities (NFC), canonical keying, and collision detection.
   - 1.5 Permanent tprintln scaffolding and correlation IDs.

2) Storage and Catalogs
   - 2.1 KV schemas: blob/text/manifest/chunk/path/tree/commit/alias; ETag/versioning.
   - 2.2 Tables: `fs_<name>_files`, `fs_<name>_chunks`, `fs_<name>_trees`, `fs_<name>_commits`, `fs_<name>_aliases`.
   - 2.3 Graph nodes/edges creation APIs (idempotent) and auditing fields.
   - 2.4 Chunking pipeline with media-type detection and text normalization.
   - 2.5 Embeddings computation and vector index build per filestore; follow Polars 0.51+ guidelines.

3) Security Layer
   - 3.1 ACL client: POST with timeouts, headers; cache allow/deny with TTL; `acl_fail_open` handling.
   - 3.2 Host path allowlist checks with absolute path normalization and symlink/junction guards.
   - 3.3 Enforcement traits integrated with DDL executors; comprehensive error types, no panics.

4) Core DDLs (Ingest/Update/Retrieve)
   - 4.1 `INGEST FILESTORE <name> FILE PATH ... FROM BYTES/HOST_PATH` → manifests, chunks, vectors.
   - 4.2 `UPDATE FILE`, `RENAME`, `DELETE` (optimistic concurrency via ETag on `.path`).
   - 4.3 `GET FILE`, `GET CHUNKS` (optionally at specific commit).
   - 4.4 Information views exposure for files/chunks.

5) Versioning and Commits
   - 5.1 `CREATE TREE FROM FOLDER` snapshots.
   - 5.2 `COMMIT TREE ...` with parents and tagging.
   - 5.3 Manifest-level diffs and `SHOW DIFF` TVF (non-blocking if deferred).

6) Git Integration (gitoxide primary)
   - 6.1 Implement `GitBackend` trait and `GitoxideBackend`.
   - 6.2 Local repo management, tree/commit assembly using UTF-8 NFC names.
   - 6.3 `ls-remote`, `fetch`, plumbing-only push via gitoxide; record mappings.
   - 6.4 Optional `libgit2` push fallback feature (`libgit2-push`), `CompositeGitBackend` selection.
   - 6.5 Per-folder overrides resolution; ACL checks for commit/push/pull.

7) Clones (Folder Aliasing)
   - 7.1 KV aliases and resolver with longest-prefix match (cycle prevention).
   - 7.2 DDLs: `CLONE`, `UNCLONE`, `SHOW CLONES` with strict/non-strict ACL modes.
   - 7.3 Execution path remapping for reads/writes, search, Git paths, and display.

8) Search and LLM Exposure
   - 8.1 `SEMANTIC SEARCH FILESTORE ...` TVF joining ANN results with file/chunk metadata.
   - 8.2 `LLM_EXPORT ... AS JSONL` and `LLM_STREAM` chunk streaming APIs.
   - 8.3 Redaction hooks (policy/attrs), size caps for output.

9) Observability, Admin, and GC
   - 9.1 `SHOW VECTOR INDEX STATUS`, `SHOW JOBS`, `SHOW GIT MAPPINGS` views.
   - 9.2 Debug `tprintln` checkpoints for ACL, Git, normalization, aliasing, vector ops.
   - 9.3 Garbage collection of unreachable blobs/manifests based on commit reachability and retention policies.

10) Testing and Hardening
   - 10.1 Unit tests per module; Polars-safe patterns per guidelines; Windows/Linux paths.
   - 10.2 Integration: ingest→search; commit→push→pull; alias round-trips; ACL matrix; dev bypass.
   - 10.3 Property tests: chunk order invariants; normalization collisions; idempotent re-ingest.
   - 10.4 Performance: embedding throughput; ANN recall/latency; Git sync over large trees.

---

## Module Layout (proposed)

- `src/server/ddl/` (parsers only)
  - `filestore/create.rs`, `filestore/alter.rs`, `filestore/drop.rs`, `filestore/show.rs`
  - `filestore/ingest.rs`, `filestore/update.rs`, `filestore/rename.rs`, `filestore/delete.rs`
  - `filestore/git/create_tree.rs`, `filestore/git/commit_tree.rs`, `filestore/git/push.rs`, `filestore/git/pull.rs`
  - `filestore/clone.rs`, `filestore/unclone.rs`, `filestore/show_clones.rs`
  - `filestore/search.rs`, `filestore/get_file.rs`, `filestore/get_chunks.rs`, `filestore/llm_export.rs`, `filestore/llm_stream.rs`

- `src/server/exec/filestore/` (executors only)
  - `config.rs` (resolver, precedence, info schema exposure)
  - `security.rs` (ACL client/cache, host-path allowlist)
  - `paths.rs` (UTF-8 normalization, NFC, collisions)
  - `kv.rs` (namespaced keys, ETag ops)
  - `manifests.rs` (create/update, diff)
  - `chunking.rs` (media detection, text extract, chunkers)
  - `embedding.rs` (vector creation, index hooks)
  - `tables.rs` (file/chunk/tree/commit/alias table writes)
  - `graph.rs` (nodes/edges updates)
  - `git/` → `backend.rs` (trait), `gitoxide.rs`, `libgit2.rs` (feature-gated), `composite.rs`, `ops.rs`
  - `clones.rs` (alias resolution, DDL impl)
  - `search.rs`, `llm.rs` (export/stream)
  - `admin.rs` (views, jobs, mappings)

- `src/server/polars_util/` (shared utilities honoring Polars 0.51+ patterns)

---

## Data Structures (selected)

- GUIDs: `file_guid`, `chunk_guid`, `manifest_guid`, `tree_guid`, `commit_guid` (UUIDv4).
- KV Values (JSON examples):
  - `manifest`: `{ "file_guid": "...", "chunks": [{"chunk_guid":"...","ord":0,"start":0,"end":1234,"len":1234,"sha256":"..."}], "media_type":"text/markdown", "encoding":"utf-8", "created_at":"...", "attrs":{} }`
  - `chunk`: `{ "file_guid":"...", "ord":0, "text":"...", "sha256":"...", "tokens":512, "size_bytes":4096, "vector_dim":1536, "etag":"..." }`
  - `path`: `{ "head_manifest_guid":"...", "tree_guid":"...", "etag":"...", "attrs":{} }`
  - `alias`: `{ "src":"/src/prefix", "mode":"live"|"pinned", "pinned_tree":null|"<tree_guid>", "strict_acl":false, "etag":"..." }`

---

## Security Details

- ACL POST request/response per finalized contract; headers from config; timeouts and retries (limited) with jitter.
- Cache keys: `(user_id, action, logical_path, filestore)`; store allow/deny with their TTLs.
- Dev bypass: if `security_check_enabled=false`, skip POST and synthesize allow.
- Fail-open: on transport/timeout, allow only if `acl_fail_open=true`.
- Host path allowlist: OS-absolute path validation; reject symlinks, `..`, and mount escapes.

---

## Git Backend (gitoxide primary, libgit2 fallback for push)

- Trait `GitBackend` with: `ensure_repo`, `write_blob`, `write_tree`, `write_commit`, `update_ref`, `ls_remote`, `fetch`, `push`.
- `GitoxideBackend` implements all; `push` may return `Unsupported` depending on auth/refspec.
- `Libgit2Backend` behind feature `libgit2-push` implements `push` (and minimal helpers).
- `CompositeGitBackend` selects gitoxide; on push unsupported or configured, delegates to libgit2.
- tprintln on backend selection/fallback; never log secrets.

---

## Polars Guidelines Compliance

- Avoid `utf8()?.iter()`; per-row access via `Series::get(i)` and `AnyValue` conversions.
- Boolean masks built via `Vec<bool>` → `Series<bool>` → `DataFrame::filter`.
- Sorting options cast `limit` to `IdxSize`.
- DataFrame construction uses `.into()` for column type consistency.
- Errors handled gracefully; no panics.

---

## Testing Strategy

- Unit tests: normalization, ACL client/cache, chunking, embedding conversions, Git tree assembly.
- Integration tests:
  - Ingest folder → manifests/chunks/vectors → search; assert path UTF-8 round-trip.
  - Commit/push/pull via gitoxide; fallback push path with `--features libgit2-push`.
  - Clone live/pinned behavior; alias resolution and ACL combinations.
  - Host path allowlist enforcement and bypass attempts.
  - LLM export streaming correctness and size limits.

- Property tests: dedup idempotency, chunk order, diff invariants, normalization collisions.

- Performance: ANN recall vs latency; ingestion throughput; Git sync on large trees.

---

## Delivery Phases and Acceptance

- Phase A (Foundations): Global/filestore config, normalization, security scaffolding; info views. Acceptance: DDLs work and configs resolve with precedence.
- Phase B (Ingest + Search): Ingest/update/GET; chunking+embeddings; ANN search. Acceptance: end-to-end ingest→search passes.
- Phase C (Versioning + Git): Trees, commits, gitoxide plumbing; optional libgit2 push. Acceptance: push/pull round-trips to local bare remote; mappings recorded.
- Phase D (Clones + LLM): Aliases implemented; LLM exports/streams. Acceptance: alias reads/writes/search reflect clone paths; LLM export correctness.
- Phase E (Hardening): Observability, GC, scale tests; security edge cases including `acl_fail_open`. Acceptance: test matrix green.

---

## Open Items and Defaults

- Defaults:
  - `html_description_max_bytes = 32768`
  - `acl_timeout_ms = 1500`, `acl_cache_ttl_allow_ms = 60000`, `acl_cache_ttl_deny_ms = 10000`, `acl_fail_open = false`
  - `git_mode = 'plumbing_only'`, `git_backend = 'gitoxide'`, `git_push_backend = 'auto'`

- Open (confirm as we implement):
  - Embedding model/dimension to use by default and how it’s configured per filestore.
  - Vector index parameters (HNSW m, ef_build, ef_search) per filestore.
  - Redaction policies for LLM exports (may be provided by middleware).

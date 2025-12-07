# FILESTORE — Logical Hierarchical File Storage (UTF‑8), Git Plumbing, Semantic Search, and LLM Exposure

Version: 1.0
Last updated: 2025‑12‑07

## 1. Goal
Design and implement a logical, hierarchical file storage system where:
- Files live in logical folders (fully UTF‑8), independent of OS constraints.
- Physical storage is GUID‑addressed and immutable for versions.
- Logical folders can be committed/pushed/pulled to Git repositories as cohesive units using plumbing APIs (no worktree required by default).
- Semantic search spans content and metadata using existing graph, vector, and key/value capabilities.
- Whole file content and chunked content are exposed for LLM use.
- Query parsing and execution remain separated; operations fail gracefully without panics.

This feature set is encapsulated as a FILESTORE object: `database.filestore.<name>` with dedicated DDL.

## 2. Naming and Hierarchy (UTF‑8)
- Logical paths are a sequence of UTF‑8 segments separated by `/` (slash reserved for hierarchy).
- Allowed: any valid UTF‑8 bytes in a segment, including spaces, punctuation, emoji, RTL scripts, combining marks.
- Disallowed in a segment: NUL (`\0`) and `/`.
- Canonicalization: store normalized NFC for canonical keys; preserve original display string.
- Collision policy: if two names collide after NFC normalization, reject the write with a clear error.

Implications for Git:
- Git trees/blobs accept UTF‑8 bytes except NUL and `/` in names; we will use plumbing (libgit2) so OS path constraints are irrelevant by default.
- Optional worktree export: apply reversible percent‑encoding for OS‑unsafe names and write a `.clarium-map.json` for round‑trip mapping.

## 3. FILESTORE Object and Configuration
Reference: `database.filestore.<name>`.

Attributes (per‑filestore; unspecified values inherit from global defaults):
- Security
  - `security_check_enabled: bool` — bypass ACL if false (dev/test only).
  - `acl_url: string` — POST‑only endpoint.
  - `acl_auth_header: string` — e.g., `Bearer <token>`.
  - `acl_timeout_ms: int` — request timeout.
  - `acl_cache_ttl_allow_ms: int`, `acl_cache_ttl_deny_ms: int` — decision caching.
  - `acl_fail_open: bool` — allow on ACL failures if true (default false).
- Host path ingestion
  - `host_path_allowlist: string` — `;`‑separated absolute path prefixes (Windows/Unix supported).
- Git
  - `git_remote: string` — default remote URL at filestore root.
  - `git_branch: string` — default branch.
  - `git_mode: 'plumbing_only' | 'worktree'` — default plumbing.
  - `lfs_patterns: string` — `;`‑separated globs to emit `.gitattributes`.
- Metadata limits
  - `html_description_max_bytes: int` — default 32768; HTML is not sanitized here.

Per‑folder overrides (subset: Git remote/branch/mode) are supported and stored in KV; precedence defined in Section 5.

## 4. Global Configuration (per Database)
Global defaults layer for ACL and Git, overridable per filestore and per folder.

Example DDL (sketch):
```
ALTER GLOBAL FILESTORE CONFIG SET (
  acl_url = 'https://acl.svc/check',
  acl_auth_header = 'Bearer ${GLOBAL_TOKEN}',
  acl_timeout_ms = 1500,
  acl_cache_ttl_allow_ms = 60000,
  acl_cache_ttl_deny_ms = 10000,
  acl_fail_open = false,

  git_remote = 'git@github.com:org/root.git',
  git_branch = 'main',
  git_mode = 'plumbing_only',
  lfs_patterns = '*.pdf;*.pptx',

  html_description_max_bytes = 32768
);

SHOW GLOBAL FILESTORE CONFIG;
```

## 5. Config Precedence
1) Folder override (most specific)
2) Filestore attributes
3) Global defaults (least specific)

## 6. DDL and APIs (Sketch)
Keep parsing and execution separate; each DDL in its own module.

- Filestore lifecycle
```
CREATE FILESTORE <name> WITH (...);
DROP FILESTORE <name>;
ALTER FILESTORE <name> SET (...);
ALTER FILESTORE <name> SET FOLDER CONFIG '<prefix>' = '{"git_remote":"...","git_branch":"..."}';
SHOW FILESTORES;
```

- Ingest/update with security
```
INGEST FILESTORE <name> FILE PATH '<logical_path_utf8>' FROM BYTES $blob MEDIA_TYPE '<mime>' ATTRS '{...}';
INGEST FILESTORE <name> FOLDER '<logical_prefix>' FROM HOST_PATH '<abs_path>' [RECURSIVE];
UPDATE FILESTORE <name> FILE PATH '<logical_path_utf8>' FROM BYTES $blob;
RENAME FILESTORE <name> PATH '<old>' TO '<new>';
DELETE FILESTORE <name> PATH '<logical_path_utf8>';
```

- Git ops (security‑restricted)
```
CREATE TREE FILESTORE <name> FROM FOLDER '<prefix>';
COMMIT TREE FILESTORE <name> <tree_guid> MESSAGE '<msg>' AUTHOR '<user>';
PUSH FILESTORE <name> FOLDER '<prefix>' TO GIT '<remote?>' BRANCH '<branch?>' [MODE plumbing_only|worktree];
PULL FILESTORE <name> FROM GIT '<remote?>' BRANCH '<branch?>' INTO FOLDER '<prefix>';
```

- Search and retrieval
```
SEMANTIC SEARCH FILESTORE <name> IN FOLDER '<prefix>' QUERY '<q>' [TOPK 50] [FILTER media_type IN (...)];
GET FILE FILESTORE <name> PATH '<logical_path_utf8>' [AT COMMIT <guid>|HEAD] FORMAT text|bytes;
GET CHUNKS FILESTORE <name> PATH '<logical_path_utf8>' [AT COMMIT <guid>|HEAD];
LLM_EXPORT FILESTORE <name> FOLDER '<prefix>' AS JSONL;
LLM_STREAM FILESTORE <name> CHUNKS FROM FOLDER '<prefix>' WHERE media_type IN ('text/markdown','text/x-rust') ORDER BY path, ord;
```

- Information schema views
  - `fs_global_config`, `fs_filestores`, `fs_folder_overrides`
  - `fs_<name>_files`, `fs_<name>_chunks`, `fs_<name>_trees`, `fs_<name>_commits`, `fs_<name>_paths`

## 7. Security Model
### 7.1 External ACL (POST‑only)
Endpoint returns `{ data: 'ok'|'error', results: [ { allow, reason, effective_perms[], ttl_ms } ], error? }`.

Request (application/json):
```
{
  "filestore": "<name>",
  "user": { "id": "<id>", "roles": ["..."], "ip": "..." },
  "action": "read"|"write"|"delete"|"move"|"copy"|"rename"|"list"|"commit"|"push"|"pull",
  "paths": { "logical": "<UTF-8 NFC path>", "old": "<UTF-8 NFC old path>" },
  "context": {
    "filestore_config_version": <int>,
    "content_meta": { "size_bytes": <int>, "media_type": "<mime>" },
    "git": { "remote": "<remote>", "branch": "<branch>" },
    "request_id": "<trace-id>"
  }
}
```

Enforcement:
- If `security_check_enabled=false` → skip POST, assume approved.
- Else if `data='ok'` and any `allow=true` → proceed; cache result for `ttl_ms` or defaults.
- Else deny. If transport/timeout/error and `acl_fail_open=true` → allow; otherwise deny.

### 7.2 Host Path Allowlist
- `INGEST ... FROM HOST_PATH` only permitted for absolute paths under configured prefixes.
- Disallow symlink traversal by default; normalize and resolve before checks.

### 7.3 Git Actions Covered by ACL
- `commit`, `push`, `pull` require allow; context includes `(remote, branch, path_prefix)`.

## 8. Data Model
### 8.1 IDs
- `file_guid` (UUIDv4): immutable content identity for a file version.
- `chunk_guid` (UUIDv4): each content chunk.
- `manifest_guid` (UUIDv4): ordered chunk list + metadata.
- `tree_guid` (UUIDv4): logical folder snapshot.
- `commit_guid` (UUIDv4): commit with parents, author, message, timestamps.

### 8.2 KV Namespaces (per filestore)
Prefix: `<db>.store.filestore.<name>.*`
- `.blob::<file_guid>` → raw bytes or external pointer.
- `.text::<file_guid>` → normalized UTF‑8 text.
- `.manifest::<manifest_guid>` → `{ file_guid, chunks[], media_type, encoding, attrs, etag }`.
- `.chunk::<chunk_guid>` → `{ file_guid, ord, text, sha256, tokens, size_bytes, vector_dim, etag }`.
- `.path::<logical_path_nfc>` → `{ head_manifest_guid, etag, attrs, perms_cache }`.
- `.tree::<tree_guid>` → snapshot entries.
- `.commit::<commit_guid>` → commit metadata.
- `.git::<remote>::<ref>` → last synced commit mapping; plus reverse maps.

### 8.3 Tables (Polars)
- `fs_<name>_files`, `fs_<name>_chunks`, `fs_<name>_trees`, `fs_<name>_commits`, `fs_<name>_paths`.

### 8.4 Graph Schema
- Nodes: `LogicalPath`, `File`, `Manifest`, `Chunk`, `Tree`, `Commit`, `GitRemote`, `Tag`, `User`.
- Edges: `PATH_CONTAINS`, `HAS_MANIFEST`, `HAS_CHUNK(ord)`, `DERIVES_FROM`, `IN_TREE`, `PARENT_OF`, `ANNOTATED_BY`, `OWNED_BY`, `SYNCED_TO`.
- Node metadata: `title`, `tags`, `owner`, `description_html` (capped), `attributes (Json)`.

## 9. Content Normalization, Chunking, Embeddings
### 9.1 Ingest Pipeline
- Media‑type detection → text extraction → normalization (newlines, whitespace, etc.).
- Chunking:
  - Text/code: paragraph/sentence boundaries; 512–1024 tokens; overlap 50–100 tokens.
  - Binary: page/granule (e.g., PDF pages); OCR when needed.
- Persist chunks and manifest; compute sizes, hashes, token counts.

### 9.2 Embeddings and Vector Indexes
- Embeddings per chunk; vectors stored as `List(Float64)`.
- Build HNSW indexes per filestore on `fs_<name>_chunks(embedding)` with metadata filters (`path_prefix`, `media_type`, `commit_guid`).
- Polars 0.51+: use `Series::get(i)` and dtype‑agnostic conversions; cast sort limits to `IdxSize`.

## 10. Git Integration
### 10.1 Plumbing‑only (default)
- Build tree/commit objects directly from manifests and UTF‑8 NFC names using libgit2.
- Map `commit_guid ↔ git_sha` in KV and graph.

### 10.2 Optional Worktree Export
- Reversible percent‑encoding for OS‑unsafe names; write `.clarium-map.json`.
- On import, decode using the mapping; verify manifest hashes.

### 10.3 Per‑folder Remotes
- Root inherits from global/filestore; subtree overrides via folder config.

## 11. LLM Exposure
- Whole file retrieval via `GET FILE` (bytes/text) with optional streaming.
- Chunk retrieval via `GET CHUNKS` ordered by `ord`.
- Bulk exports: `LLM_EXPORT ... AS JSONL` with `{ path, text | chunks[] }`.
- Safety: size caps; redaction policies can be applied by middleware; mark `attrs.redact=true` when needed.

## 12. Observability and Debuggability
- Permanent `tprintln` checkpoints:
  - ACL request/response (redact secrets), bypass usage, cache hits/misses.
  - Ingest start/end, chunking/embedding stats, vector index build/search.
  - Git push/pull steps, commit mappings, per‑folder override resolutions.
  - Unicode normalization decisions; collision detections.

## 13. Concurrency, Versioning, Recovery
- Optimistic concurrency with `etag` on `.path::<logical_path_nfc>` entries; retries with backoff.
- Background jobs keep state in KV (`fs.jobs::<id>`); resumable with checkpoints.
- Garbage collection via reachability from latest commits/tags; retention windows configurable.
- No panics: structured error returns; partial failures logged and retried.

## 14. Performance and Scalability
- Batching: bulk chunk inserts and embedding computation.
- Partitioning: chunk tables by folder/prefix or hash ranges; shard vector indexes by size.
- Caching: hot `path → manifest_guid` map; LRU for decoded text; bloom filters for dedup.
- Parallelism: rayon pools for parsing/embedding; tune HNSW `ef_build`, `m`.

## 15. Testing Strategy
- Unit tests: chunking per media type, manifest diffs, etag conflicts, KV CRUD, NFC collisions.
- Integration tests: ingest → search; Git plumbing push/pull round‑trips; folder overrides; GC safety.
- Property tests: dedup idempotency; content hashing stability; chunk order invariants.
- Performance tests: ANN recall vs latency; ingestion throughput; Git sync at scale.
- Security tests:
  - ACL allow/deny matrix; timeouts; `security_check_enabled=false` bypass; `acl_fail_open=true|false` behavior.
  - Host path allowlist bypass attempts (symlinks, junctions, `..`).

## 16. Phased Rollout Plan
1) Foundations
   - Implement KV schemas, `files/chunks/paths` tables, and manifests; basic GET FILE/CHUNKS.
   - Build embeddings and ANN index on chunks; `vector_search` TVF with filters.
2) Graph and versioning
   - Nodes/edges; `tree_guid`, `commit_guid`; diff and tagging.
3) Git integration
   - Plumbing‑only export/import; commit mappings; per‑folder remotes; optional worktree path encoding.
4) LLM exposure
   - JSONL exports; chunk streaming APIs; size caps.
5) Hardening
   - Concurrency controls; resumable jobs; GC; observability; scale tests.

## 17. Example DDL and Workflows
- Create filestore with dev bypass and host allowlist:
```
CREATE FILESTORE mydocs
WITH (
  security_check_enabled = false,
  acl_url = 'https://acl.svc/check',
  host_path_allowlist = 'C:\\ingest; /mnt/data/docs',
  git_remote = 'git@github.com:org/root.git',
  git_branch = 'main',
  git_mode = 'plumbing_only'
);
```

- Ingest a UTF‑8 path:
```
INGEST FILESTORE mydocs FILE PATH '研发/📚Docs/specs/RFC‑1.md' FROM BYTES $blob MEDIA_TYPE 'text/markdown';
```

- Enable security and push a subtree:
```
ALTER FILESTORE mydocs SET (security_check_enabled = true);
CREATE TREE FILESTORE mydocs FROM FOLDER '研发/📚Docs';
COMMIT TREE FILESTORE mydocs last_tree() MESSAGE 'Initial RFCs' AUTHOR 'euan';
PUSH FILESTORE mydocs FOLDER '研发/📚Docs' TO GIT 'git@github.com:org/root.git' BRANCH 'main';
```

- Per‑folder Git override:
```
ALTER FILESTORE mydocs SET FOLDER CONFIG '研发/📚Docs' = '{"git_remote":"git@github.com:org/docs.git","git_branch":"docs-main"}';
```

- Semantic search within a folder:
```
SELECT path, ord, score
FROM vector_search(
  table => 'fs_mydocs_chunks',
  query_text => 'how to configure hnsw index',
  topk => 50,
  filters => '{"folder":"研发/📚Docs"}'
);
```

## 18. Implementation Notes (Codebase)
- Reuse existing vector modules and TVFs; store embeddings as `List(Float64)`.
- Follow Polars 0.51+ patterns: `Series::get(i)`, `try_extract::<f64>()`; cast sort `limit` to `IdxSize`.
- Keep each new DDL in its own module; parsing in `query_parse_*`, execution in `exec_*` with graceful error paths.
- Prefer promoting shared utilities to public modules rather than duplicating; keep primary interfaces thin; avoid large functions.
- Add permanent `tprintln` diagnostics at key boundaries; they do not impact release performance.

## 19. Open Decisions (Defaults Applied)
- `acl_fail_open`: supported, default `false`.
- No additional Git permissions beyond `(remote, branch, path_prefix)` at this time.
- Default `html_description_max_bytes`: 32768.
- Default ACL timeouts and TTLs per values above; can be overridden globally or per filestore.

## 20. Acceptance Criteria
- UTF‑8 path ingest/retrieval works with normalized keys and preserved display names.
- Security enforcement integrates with external ACL (or bypassed in dev), including Git ops.
- Git plumbing push/pull round‑trips preserve names and content.
- Semantic search returns relevant chunks with metadata filters.
- LLM export/streaming supports whole files and chunked content with size caps.

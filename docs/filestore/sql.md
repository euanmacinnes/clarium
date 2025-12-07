Clarium FILESTORE — SQL Surface
===============================

This document specifies the SQL statements that operate the FILESTORE subsystem. The syntax below is the contract used by the Clarium parser and execution engine.

General notes
-------------
- Identifiers are case‑insensitive unless quoted. Filestore names and logical paths are case‑sensitive in storage.
- Errors are descriptive strings (no panics). Mutations include correlation IDs in logs as [corr=...].
- Logical paths must be UTF‑8, NFC‑normalized, with '/' separators and no empty segments.
- SHOW outputs are tabular (Polars DataFrames) and then converted to JSON.

DDL
---

1) Create a filestore

  CREATE FILESTORE `name` [WITH `json`];

Where `json` is a JSON object with any of these optional keys:
- security_check_enabled: bool
- acl_url: string|null
- acl_auth_header: string|null
- acl_timeout_ms: u64|null
- acl_cache_ttl_allow_ms: u64|null
- acl_cache_ttl_deny_ms: u64|null
- acl_fail_open: bool|null
- git_remote: string|null
- git_branch: string|null
- git_mode: string|null      -- "plumbing_only" | "worktree"
- git_backend: string|null   -- informational
- git_push_backend: string|null  -- "auto" | "gitoxide" | "libgit2"
- lfs_patterns: string|null  -- e.g., "*.pdf;*.pptx"
- html_description_max_bytes: usize|null

2) Alter filestore configuration

  ALTER FILESTORE `name` SET `json_update`;

json_update is a JSON object where each field is optional and can itself be null to clear a value.

3) Drop a filestore

  DROP FILESTORE `name` [FORCE];

When FORCE is omitted, the operation may be conservative depending on implementation; current backend uses in‑memory KV and permits drop.

Mutations
---------

1) Ingest from bytes

  INGEST FILESTORE FILE PATH 'logical_path'
  FROM BYTES 'payload' [CONTENT_TYPE 'mime'];

`payload` may be:
- 0x‑prefixed hex (e.g., 0x48656c6c6f)
- plain hex (even length)
- base64 string (fallback)

2) Ingest from host path

  INGEST FILESTORE FILE PATH 'logical_path'
  FROM HOST_PATH 'host_path' [CONTENT_TYPE 'mime'];

Host path ingestion enforces an allowlist and denies symlinks/junctions. See concepts for Windows/UNC nuances.

3) Update existing file from bytes (optimistic concurrency)

  UPDATE FILESTORE FILE PATH 'logical_path'
  IF_MATCH 'etag'
  FROM BYTES 'payload' [CONTENT_TYPE 'mime'];

4) Rename a logical path

  RENAME FILESTORE FROM PATH 'from' TO PATH 'to';

5) Delete (soft‑delete/tombstone)

  DELETE FILESTORE FILE PATH 'logical_path';

Versioning
----------

1) Create a tree snapshot

  CREATE TREE IN FILESTORE `name` [LIKE 'logical_prefix'];

2) Commit a tree

  COMMIT TREE IN FILESTORE TREE 'tree_id'
    [PARENTS 'id1,id2,...']
    [BRANCH 'branch']
    [AUTHOR_NAME 'name']
    [AUTHOR_EMAIL 'email']
    [MESSAGE 'msg']
    [TAGS 't1,t2,...'];

Behavior:
- If PARENTS omitted, the current branch head is inferred when present.
- TAGS are trimmed, empties removed, deduplicated, then sorted for stable ordering.

SHOW (information schema)
-------------------------

1) SHOW FILESTORES [IN `database`]
Columns: name, git_remote, git_branch, git_mode, git_push_backend, acl_url, acl_fail_open, lfs_patterns, config_version, created_at, updated_at

2) SHOW FILESTORE CONFIG `name` [FOLDER 'prefix']
Returns a single‑row summary of global/fs/effective values. FOLDER simulates per‑folder Git overrides.

3) SHOW FILES IN FILESTORE `name` [LIKE 'prefix'] [LIMIT n] [OFFSET k]
Columns: logical_path (String), size (Int64), etag (String), version (Int64), updated_at (Int64), deleted (Boolean), content_type (String)

4) SHOW TREES IN FILESTORE `name`
Columns: id, entries, created_at

5) SHOW COMMITS IN FILESTORE `name`
Columns: id, parents, author_name, author_email, time_unix, message, tags, branch

6) SHOW DIFF IN FILESTORE `name` FROM 'commit_a' TO 'commit_b'
Columns: path, status ("added"|"modified"|"deleted"), size_before, size_after, etag_before, etag_after

7) SHOW CHUNKS IN FILESTORE `name`
Columns: oid, size, ref_count (ref_count is 0 until reverse references are tracked)

8) SHOW ALIASES IN FILESTORE `name`
Columns: alias, folder_prefix, target_store, target_prefix

9) SHOW ADMIN IN FILESTORE `name`
Columns: files_live, files_tombstoned, chunks, trees, commits

10) SHOW HEALTH IN FILESTORE `name`
Columns: orphaned_chunks (placeholder=0), stale_refs, config_mismatches (placeholder=0)

ACL and security
----------------
- Mutations call check_acl with action (Write/Move/Delete/Commit/Push/etc). When security_check_enabled=false, actions are allowed.
- On transport/timeout errors, behavior follows acl_fail_open.
- Decisions are cached with TTLs; capacity is bounded and evictions are logged.

Errors
------
Typical error strings (subject to expansion):
- logical path validation: "logical path cannot be empty", "segments '.' and '..' are not allowed"
- ingest/update limits: "content_type_too_long", "description_html_too_large"
- update concurrency: "not_found", "gone", "precondition_failed"
- rename/delete: "not_found", "gone"
- ACL: reason from server or "acl_denied"; fail‑open reasons prefixed with "acl_fail_open_..."

Polars and JSON
---------------
- DataFrames use robust dtype patterns compatible with Polars 0.51+.
- SHOW FILES paging returns an empty but fully typed frame when OFFSET ≥ height.

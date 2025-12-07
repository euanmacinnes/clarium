# FILESTORE — Folder Clones/Aliases (Multi-Location Views of the Same Content)

Version: 1.0
Last updated: 2025-12-07

## 1. Goal
Enable logical folders to be “cloned” into multiple locations within a FILESTORE so the same underlying content appears under different logical prefixes without physical duplication. The cloned location must present a fully local path identity (i.e., all APIs, listings, Git ops, and exports reflect the clone’s full path), while content storage and versioning remain deduplicated.

This extends the FILESTORE plan (`plans/filestore_plan.md`).

## 2. Terminology
- Source folder: the original logical prefix (e.g., `A/Specs`).
- Clone (alias) folder: an additional logical prefix (e.g., `B/Team/Specs`) that presents the same content.
- Live clone: follows the current HEAD (latest manifests) of the source path subtree.
- Pinned clone: fixed to a specific snapshot (`tree_guid` or `commit_guid`).

## 3. Requirements
- Cloning a folder is O(1) metadata operation; no byte copying.
- The clone path must behave as a first-class folder:
  - Listings, `GET FILE/CHUNKS`, semantic search filters, and LLM exports reflect the clone’s full local path.
  - Git operations using the clone path operate on the same content, but report/use the clone prefix as the path root.
- Updates at the source reflect in all live clones (subject to ACL and etag semantics). Pinned clones are immutable unless re-pinned.
- Security: creating/removing a clone requires authorization (write on destination prefix and read on source). Access through a clone is governed by the destination path ACL (not the source), with an option to require both in strict mode.
- UTF-8 path model preserved; only `/` is a separator and NUL is rejected.
- Works with per-folder Git overrides; defaults apply based on the effective path (clone location) unless explicitly overridden.
- Observable, debuggable, resilient; no panics.

## 4. Design Overview
Introduce a path-aliasing layer that maps a destination prefix to either:
- `FollowHead { source_prefix }` — resolves to the source’s current `.path` entries (live clone), or
- `PinnedTree { tree_guid }` — resolves to a fixed snapshot of a tree (pinned clone), or
- `PinnedCommit { commit_guid }` — equivalent to `PinnedTree` via the commit’s tree.

Resolution is performed before `.path::<logical_path_nfc>` lookups, composing the destination prefix with the remainder of the request path and translating it to a canonical source key when needed. Returned paths in APIs use the destination (clone) prefix to preserve locality.

## 5. Data Model Additions (per FILESTORE)
-### 5.1 KV Namespaces
- `<db>.store.filestore.<name>.alias::<dest_prefix_nfc>` → JSON
  ```json
  {
    "mode": "follow_head",
    "source_prefix": "A/Specs",
    "tree_guid": null,
    "commit_guid": null,
    "created_by": "user-id",
    "created_at": "2025-12-07T11:35:00Z",
    "strict_acl": false,
    "etag": "..."
  }
  ```
  - `mode` values: `"follow_head" | "pinned_tree" | "pinned_commit"`.
  - When `mode == "pinned_tree"`, set `tree_guid` to a UUID and `source_prefix` may be omitted.
  - When `mode == "pinned_commit"`, set `commit_guid` to a UUID and `source_prefix` may be omitted.
  - When `mode == "follow_head"`, set `source_prefix` to the canonical source path and leave `tree_guid`/`commit_guid` as `null`.
- `<db>.store.filestore.<name>.alias_index` → optional materialized list/cache of all aliases for efficient prefix checks.

### 5.2 Tables / Information Schema
- `fs_<name>_aliases(path_prefix, mode, source_prefix?, tree_guid?, commit_guid?, strict_acl, created_at, created_by)`
- Add view `information_schema.fs_aliases` listing all filestores’ aliases.

### 5.3 Graph Schema
- Node: `Alias` with properties `{ dest_prefix, mode, source_prefix?, tree_guid?, commit_guid?, strict_acl }`.
- Edges:
  - `ALIAS_OF (dest_prefix) → LogicalPath(prefix=source_prefix)` for `follow_head`.
  - `ALIAS_PINS (dest_prefix) → Tree(tree_guid)` or `Commit(commit_guid)` for pinned modes.

## 6. Path Resolution Algorithm
Given a request path `P` under filestore `F`:
1) Normalize to NFC and reject NUL.
2) Find the longest matching alias `A` where `A.dest_prefix` is a prefix of `P`.
3) If none, proceed normally (lookup `.path::<P>`).
4) If `A.mode == follow_head`:
   - Compute `Remainder = P.strip_prefix(A.dest_prefix)`.
   - Compute `SourcePath = concat(A.source_prefix, Remainder)`.
   - Perform all internal lookups using `SourcePath` (e.g., `.path::<SourcePath>`), but set `DisplayPath = P` in responses.
5) If `A.mode == pinned_tree/commit`:
   - Resolve a virtual tree at `A.tree_guid` (or from `commit_guid`).
   - Materialize lookups by walking entries in the pinned tree using `Remainder`.
   - Return `DisplayPath = P`.
6) ACL evaluation: see Section 7.
7) ETag/Concurrency: for `follow_head`, use source path etags; for pinned, alias object etag can gate modifications (though pinned trees are read-only).

Notes:
- Aliases can be nested; choose the longest matching `dest_prefix` to allow fine-grained overrides.
- Prevent cycles: disallow creating an alias whose resolution (transitively) references itself; detect cycles via a bounded DFS at creation time.

## 7. Security Model for Clones
- Create alias (`CLONE FOLDER`): requires ACL `write` on destination prefix and `read` on source prefix. Request sent to external ACL with action `clone` and both paths.
- Remove alias (`UNCLONE FOLDER`): requires `write` on destination prefix.
- Access via alias:
  - Default: enforce ACL on the destination path/action only, so teams can expose a curated view with their own permissions.
  - Strict mode (`strict_acl=true`): require allow on both destination and source for the action.
- Git ops under alias: ACL includes the destination path prefix in the request; action remains `commit|push|pull` as appropriate.
- Dev bypass: if `security_check_enabled=false`, alias creation and access follow the existing bypass semantics.

ACL request extension example (POST body):
```json
{
  "action": "clone",
  "paths": { "logical": "B/Team/Specs", "old": "A/Specs" },
  "context": { "request_id": "..." }
}
```
Response contract remains `{ data, results[] }` per global plan.

## 8. DDL and APIs
- Create a live clone (follow HEAD):
  ```
  CLONE FILESTORE <name> FOLDER '<source_prefix>' AS '<dest_prefix>' [STRICT_ACL];
  ```
- Create a pinned clone (tree):
  ```
  CLONE FILESTORE <name> TREE <tree_guid> AS '<dest_prefix>' [STRICT_ACL];
  ```
- Create a pinned clone (commit):
  ```
  CLONE FILESTORE <name> COMMIT <commit_guid> AS '<dest_prefix>' [STRICT_ACL];
  ```
- Remove a clone:
  ```
  UNCLONE FILESTORE <name> FOLDER '<dest_prefix>';
  ```
- List clones:
  ```
  SHOW FILESTORE <name> CLONES;          -- tabular view of aliases
  ```

Notes:
- Destination prefix must not already exist as a concrete path subtree or another alias.
- For live clones, operations that modify content through the clone (e.g., `INGEST/UPDATE/DELETE` under the dest prefix) map to the source prefix internally and are subject to ACL rules of the destination (and optionally source in strict mode).

## 9. Git Semantics
- Plumbing-only operations resolve the effective tree based on the alias mode:
  - Live clone: construct trees from current manifests under `source_prefix` but present paths with `dest_prefix` as root.
  - Pinned clone: construct trees from `tree_guid` (or from `commit`) and present `dest_prefix` paths.
- Commit lineage: commits created from a clone still represent the underlying content; map `commit_guid ↔ git_sha` as usual.
- Per-folder Git overrides apply based on the destination (clone) prefix unless the alias metadata contains explicit overrides (optional future extension).

## 10. Search, Retrieval, and LLM Exposure
- Search filters by `folder` should accept clone prefixes; internally, expand to the effective source (live) or traverse pinned tree (pinned).
- Returned `path` fields MUST be the clone’s path when queried via clone prefix.
- `GET FILE/GET CHUNKS` honor alias resolution and return `DisplayPath = dest_prefix/...` with content from source/pinned snapshot.
- `LLM_EXPORT/LLM_STREAM` similarly reflect clone paths in outputs.

## 11. Observability & Debuggability
- Permanent `tprintln` at:
  - Alias creation/deletion, resolution decisions (selected alias, mode, mapping), and cycle detection outcomes.
  - ACL checks for `clone` and accesses via aliases (dest vs strict double-check).
  - Git operations under aliases (dest_prefix, effective source or tree).
- Add admin views:
  - `SHOW FILESTORE <name> CLONES` (DDL) and `information_schema.fs_aliases`.

## 12. Concurrency & Consistency
- Alias KV entries carry `etag`; updates (`UNCLONE`, re-pin) require matching etag.
- Live clones reflect source changes atomically with source `.path` updates due to follow-head mapping.
- Pinned clones are immutable w.r.t. source changes; re-pin is an explicit DDL updating the alias entry.

## 13. Edge Cases & Rules
- Prevent overlapping aliases that would produce ambiguous resolution under the same dest prefix; longest-prefix match resolves nested cases.
- Disallow aliasing a dest prefix that is equal to or a parent/child of its own source prefix in a way that can induce cycles.
- Moving/renaming the source prefix does not automatically update live clones; instead:
  - Option A (simple): live clones reference the original source prefix; if moved, clone becomes a “dangling alias” and returns a graceful error until updated.
  - Option B (tracked rename): detect renames via graph edges and update alias automatically. Default: Option A to keep semantics explicit.
- Deleting source paths:
  - Live clone: reflects deletion immediately.
  - Pinned clone: unaffected.

## 14. Testing Strategy (Additions)
- Create live clone; verify listings, `GET`, search, LLM export paths reflect dest prefix while content matches source.
- Modify source; verify live clone updates; verify pinned clone does not.
- ACL matrix: dest-only vs strict mode; clone creation denial; access via alias rules.
- Git plumbing push/pull from clone; verify path roots, commit mappings, and remote selection.
- Cycle/overlap prevention tests.
- Concurrency: etag conflicts on alias updates; deletion while in use.

## 15. Acceptance Criteria
- `CLONE ... AS ...` creates an alias in O(1) without data copy.
- Access via the clone shows the clone’s full local paths in all APIs.
- Live clones reflect source changes; pinned clones remain stable until re-pinned.
- ACL enforced per design (dest-only by default; optional strict).
- Git plumbing operations from clone paths succeed and produce correct trees with dest-prefix paths.
- Observability and information schema expose aliases; no panics; graceful errors for misconfigurations.

## 16. Rollout Plan (Delta to Base Plan)
1) Foundations: implement alias KV schema, resolution layer, and information views; DDL parse/exec for CLONE/UNCLONE/SHOW.
2) Integrations: wire into GET/CHUNKS/search/LLM and Git plumbing path mapping.
3) Security: add `clone` action to ACL; enforce dest-only vs strict modes.
4) Hardening: cycle detection, nested alias precedence, etag concurrency, tests.

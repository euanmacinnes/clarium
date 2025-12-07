### Clarium Vector Support — Gap Analysis and Implementation Plan

#### Current capabilities (baseline)

- Logical type and storage
  - `VECTOR` mapped to `polars::DataType::List(Float64)` across execution and storage layers.
  - Insert/scan can carry native list columns; literal parsing fallback exists where needed.

- UDFs and scalar utilities
  - Lua scalars: `cosine_sim`, `vec_l2`, `vec_ip`; helper `to_vec` for normalization.
  - Unit tests cover correctness and null/invalid cases.

- ORDER BY with ANN hint (exact compute for now)
  - Detection of ANN expressions in `ORDER BY` (e.g., `vec_l2(table.col, <expr>)`, `cosine_sim(...)`).
  - Executes exact per-row distance, then sorts; uses `LIMIT` as a top‑k hint; permanent `tprintln!` diagnostics.

- Vector index DDL (metadata only)
  - `.vindex` sidecar persisted via `CREATE/DROP/SHOW VECTOR INDEX` with `algo=hnsw`, optional `metric`, optional `dim`, `params`.
  - No build/search/runtime use yet.

- Runtime knobs
  - Thread‑local settings: `ef_search`, `hnsw.m`, `hnsw.ef_build`, settable via `SET` aliases.

---

### Addendum — v2 storage and query semantics (reference)

- Storage files
  - Metadata sidecar: `<db>/<schema>/<name>.vindex` with `version=2`, `algo`, `metric`, `dim`, `mode`, `params`, `status`.
  - Binary data: `<db>/<schema>/<name>.vdata` containing ANN graph and a compact row‑ID map.
  - Row‑ID strategy: prefer table primary key(s). If integral → store as `u64`; if string/composite → store hashed `u64`. If no PK → store ordinal. A `flags` field denotes flavor (`ordinal | pk_u64 | pk_hashed`).

- Modes (freshness policy)
  - `.vindex.mode` ∈ { IMMEDIATE, BATCHED, ASYNC, REBUILD_ONLY }.
  - Current runtime: REBUILD_ONLY is supported; others surface friendly “not supported yet” for incremental updates; never panic.

- ORDER BY ... USING ANN behavior
  - With `LIMIT k`: use ANN top‑k; optional exact re‑score for parity; apply secondary ORDER BY keys after score.
  - Without `LIMIT`: two‑phase — preselect `W = alpha·k` candidates (`vector.preselect_alpha`, default 6), exact re‑score, then final sort with secondary keys.
  - Deterministic ties broken by stable `row_id`/ordinal.

- Metrics and ordering
  - L2 ascending (smaller is better); cosine/IP descending (larger is better). Zero vectors treated as null for cosine.

- TVFs
  - `nearest_neighbors(table, column, qvec, k [, metric, ef_search])` → returns `(row_id u64, score f64 [, ord i64])`.
  - `vector_search(index_name, qvec, k [, topk, engine])` → direct index search with optional engine hint (`ann|exact`).

- Config knobs (session `SET`)
  - `vector.hnsw.M`, `vector.hnsw.ef_build`, `vector.search.ef_search`, `vector.preselect_alpha`.

- Examples
  - ANN in ORDER BY:
    ```
    WITH q AS (SELECT to_vec('[0.1,0.2,0.3]') AS v)
    SELECT id, cosine_sim(body_embed, (SELECT v FROM q)) AS score
    FROM docs
    ORDER BY cosine_sim(docs.body_embed, (SELECT v FROM q)) USING ANN
    LIMIT 10;
    ```
  - TVF join‑back by `row_id`:
    ```
    WITH q AS (SELECT to_vec('[0.1,0.2,0.3]') AS v)
    SELECT d.id, nn.score
    FROM nearest_neighbors('public.docs', 'body_embed', (SELECT v FROM q), 10, 'cosine', 96) AS nn
    JOIN docs AS d ON d.__row_id.docs = nn.row_id
    ORDER BY nn.score DESC, d.id;
    ```

### Major gaps and risks

1) ANN execution runtime is missing
- No HNSW/IVF/flat index implementation; ANN hint computes exact scores and full sort → won’t scale.

2) Index lifecycle management missing
- No build, status, persistence of index data, REINDEX/ALTER, or GC of artifacts.

3) No schema‑level dimension enforcement
- Column uses `List(Float64)` without enforced `dim`; `.vindex.dim` is advisory only.

4) Planner lacks ANN integration beyond manual hint
- No cost‑based decision or automatic use of compatible indexes.

5) Inefficient exact top‑k path
- Uses full sort; should support streaming top‑k (heap) when `LIMIT` present.

6) Dtype flexibility and memory
- Only `Float64` vectors; `f32` preferred for memory/ANN; coercion/validation is ad‑hoc.

7) Metric semantics incomplete
- Cosine normalization/zero‑vector handling, IP direction, and numeric stability need standardization.

8) Missing vector TVFs
- No `nearest_neighbors(...)` or `vector_search(index, ...)` to expose ANN outside `ORDER BY`.

9) Observability and admin
- No `SHOW VECTOR INDEX STATUS`, per‑index metrics, or `EXPLAIN` surface for ANN vs EXACT.

10) Write‑path integration
- No update hooks for indexes on DML; no policy for append‑only vs rebuild.

11) Polars 0.51+ audit
- Ensure all sorting uses `IdxSize` for `limit` and all string access follows `Series::get` + `AnyValue`.

---

### Roadmap and solutions

#### Phase 1 — Robust exact path and developer UX (short term)

- Enforce dimension at boundaries (opt‑in)
  - Add column attribute `VECTOR(dim)` in schema metadata; validate on write and during compute.
  - Centralize vector parsing/extraction in a public Rust utility to avoid drift with Lua.

- Optimize exact top‑k (no index)
  - Implement streaming heap‑based top‑k when `LIMIT k` is present (O(N log k)).
  - Keep full sort fallback; ensure `SortMultipleOptions.limit` uses `IdxSize`.

- Improve metric semantics
  - Standardize ASC/DESC: L2 ascending, IP descending, cosine descending.
  - Cosine: treat zero vector as null; document behaviors; add warnings when ambiguous.

- EXPLAIN and diagnostics
  - `EXPLAIN` to show EXACT vs ANN attempt, index match/mismatch, and parameters.

#### Phase 2 — ANN engine and index lifecycle (medium term)

- Implement HNSW runtime
  - Use a mature Rust implementation or internal HNSW over `f32` vectors.
  - Persist index data as compact binary files (e.g., `<table>.<column>.<metric>.<dim>.hnsw`), mmap on query workers.

- Build and status
  - `BUILD VECTOR INDEX name [WITH (m=?, ef_build=?, dim=?, metric=...)]` to scan and build in chunks.
  - Maintain `.vindex.status` (state, rows_indexed, last_built_at, bytes, m, ef_build, elapsed).

- Query integration
  - In `ORDER BY` ANN path with `LIMIT`, use `index.search(qvec, k, ef_search)` to return candidate ids, then assemble ordered output.
  - Without `LIMIT`, consider ANN preselect top‑W then exact score + final sort.

- DML freshness policy
  - Start with manual/periodic rebuild; plan append‑only incremental inserts; defer deletes to rebuilds.

- TVFs
  - `nearest_neighbors(table, column, qvec, k [, metric, ef_search])` and `vector_search(index_name, qvec, k)` returning `row_id`, `score` (joinable).

#### Phase 3 — Enterprise robustness (long term)

- Multi‑metric/dtype
  - Native `f32` storage path; cast from `f64` at boundaries; metric‑specific scoring with monotonic transforms.

- Planner and cost model
  - Cost‑based switch between EXACT/ANN considering table size, LIMIT, index presence, selectivity.

- Write‑path modes
  - `ALTER VECTOR INDEX ... SET mode = IMMEDIATE|BATCHED|ASYNC|REBUILD_ONLY`.

- Observability
  - `SHOW VECTOR INDEX STATUS [LIKE ...]`, JSON admin endpoints, `EXPLAIN ANALYZE` timings and ef_search used.

- Safety and validation
  - Enforce `.vindex.dim` vs column data; configurable policy: reject/null; index file checksums and versioning.

---

### Concrete tasks mapped to the codebase

- Vector utilities module
  - Promote `parse_vec_literal` to `server/exec/vector_utils.rs` with:
    - `parse_vec_literal(&str) -> Option<Vec<f32>>`.
    - `extract_vec_f32(series: &Series, i: usize) -> Option<Vec<f32>>` using `Series::get` + `AnyValue` (guidelines compliant).

- Exact top‑k improvement
  - Add heap‑based selection in `order_limit.rs` when ANN hint + `LIMIT` present; preserve full sort fallback.

- IdxSize audit
  - Ensure every `SortMultipleOptions.limit` is `Option<IdxSize>`.

- Index DDL extensions
  - Implement `BUILD VECTOR INDEX`, `REINDEX VECTOR INDEX`, and `SHOW VECTOR INDEX STATUS`.
  - New runtime module `exec_vector_runtime.rs` to scan/build/persist and update `.vindex.status`.

- Executor hook for ANN
  - In ANN branch, attempt `vector_runtime::search(index, qvec, k, ef_search)`; fallback to exact on error.

---

### Testing and validation

- Unit tests
  - Robust extraction over mixed dtypes (`List(Float64|Int64)`), malformed strings; null/zero vector handling.
  - Heap top‑k vs full sort equivalence (for small N) and complexity benefits.
  - Dimension enforcement happy/negative paths.

- Integration tests
  - ANN hint with and without index → correctness parity for small datasets; speedups at scale.
  - TVFs covering `LIMIT`, filters, and ordering.
  - DDL lifecycle: CREATE → BUILD → SHOW STATUS → REINDEX → DROP with artifact checks.

- Benchmarks (optional initially)
  - N ∈ {1e4, 1e5, 1e6}, dim ∈ {64, 384, 768}; k ∈ {10, 100}; report build time and query latency/QPS for EXACT vs ANN.

---

### Operational guidance

- Error handling: Never panic; invalid cells → null; friendly errors for metric/dim mismatch; graceful fallbacks to exact.
- Debuggability: Keep `tprintln!` breadcrumbs for detection, chosen path, ef_search, candidate sizes, and fallbacks.
- Separation of concerns: distinct modules for DDL, runtime, and utilities; separate `.vindex` metadata from index data files.
- Future compatibility: prefer `f32` internally for ANN; keep Polars interactions dtype‑agnostic via `AnyValue` conversions.

---

### Bottom line

- Strong foundation exists (dtype, UDFs, hint detection, DDL metadata, config knobs). The priority is:
  1) Implement a real ANN engine with index lifecycle.
  2) Strengthen exact path (top‑k heap), dimension enforcement, and planner selection.
  3) Add TVFs, observability, and DML freshness modes for enterprise use.

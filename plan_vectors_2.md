### Execution Plan to Continue Vector Support Work

NO STEPS STARTED.

#### 1) ORDER BY ANN parity and scalability
- Implement two-phase path when no LIMIT:
  - Preselect W = alpha·k (configurable, default 4–10×) via ANN engine → exact re-score on W → final sort.
  - Config knob: `vector.preselect_alpha` (env + session `SET`).
  - tprintln breadcrumbs: engine, metric, alpha, W, final_k, fallback reason.
- Secondary ORDER BY keys:
  - Apply secondary keys on the ANN-selected k (or W after re-score) to maintain full ORDER BY semantics.
- Row-id mapping robustness:
  - Use namespaced `__row_id.<alias>` if present; otherwise compute stable fallback (hash of PK or row ordinal) and document behavior.
- Acceptance criteria:
  - Parity vs exact on small datasets for L2/IP/cosine with and without LIMIT.
  - Deterministic tie-breaking using row_id/index to stabilize results across runs.

#### 2) Observability and EXPLAIN
- Extend SHOW VECTOR INDEX STATUS rows with:
  - Fields: `state, rows_indexed, bytes, dim, metric, engine, build_time_ms, ef_build, ef_search, mode`.
- Minimal EXPLAIN annotations:
  - Show whether EXACT or ANN path chosen, index used, metric, ef_search, preselect W, and explicit fallback notes.
- Permanent breadcrumbs:
  - Ensure consistent `tprintln!` tags for ANN planner/executor and TVFs.
- Acceptance criteria:
  - SHOW output contains the new fields; EXPLAIN text includes ANN/EXACT markers and parameters.

#### 3) DML freshness modes and lifecycle
- Respect `.vindex.mode` = IMMEDIATE | BATCHED | ASYNC | REBUILD_ONLY in runtime:
  - For now, implement graceful placeholders:
    - IMMEDIATE/BATCHED/ASYNC → return friendly “not supported yet” on incremental updates.
    - REBUILD_ONLY → current behavior.
- Surface mode in SHOW and status; validate options during CREATE/ALTER.
- Acceptance criteria:
  - DDL round-trip includes mode; unsupported paths do not panic and return user-facing errors.

#### 4) Stable row‑ID strategy in index files
- Prefer primary key(s) from table metadata as stable `row_id` in v2 `.vdata`:
  - If PK exists and is integral or string, persist it (u64 for numerics; hashed u64 for non-numeric composite).
  - If absent, keep ordinal fallback and record `flags` to denote id flavor.
- Loader/search return `(row_id, score)` unchanged, mapping via DF `__row_id.<alias>` when possible.
- Acceptance criteria:
  - Build→load round-trip preserves row IDs; mapping works after filters/reorders in DF.

#### 5) TVFs completeness and ergonomics
- `nearest_neighbors(table, column, qvec, k [, metric, ef_search])`:
  - Honor metric/ef_search; accept qualified/aliased table names; return optional `ord` for stable joins.
- `vector_search(index_name, qvec, k)`:
  - Add optional `topk` and engine hint; ensure outputs are dtype-agnostic and portable.
- Provide usage examples: joining TVF output back to source by `row_id`.
- Acceptance criteria:
  - TVFs behave correctly with options; join examples validated in tests.

#### 6) Dimension enforcement and data hygiene
- Enforce `.vindex.dim` at BUILD (skip or error based on policy knob) and at query time (warn or fallback exact).
- Coerce `f64 → f32` at boundaries; treat invalid cells as nulls; never panic.
- Acceptance criteria:
  - Negative tests show graceful error/skip behavior; positive paths succeed.

#### 7) HNSW engine enhancements (feature `ann_hnsw`)
- Extend metric coverage:
  - Cosine/IP support via normalization or alternative distance types (subject to `hnsw_rs` capabilities).
- Load performance:
  - Prefer mmap or chunked loading for large graphs (feature-gated if necessary).
- Acceptance criteria:
  - Smoke tests under the feature gate for build+search with L2 and (if feasible) cosine; compiles without the feature.

#### 8) Tests expansion (unit + integration)
- Unit tests:
  - Two-phase ANN parity (no LIMIT) vs exact.
  - Metric semantics (ASC for L2, DESC for cosine/IP) and tie-breaks.
  - Row-id mapping under filters and joins.
  - TVF options and join-back correctness.
- Integration tests:
  - DDL lifecycle with modes; SHOW/EXPLAIN fields present and formatted.
  - ANN vs EXACT parity on small datasets; limit and multi-key ordering.
- Keep `src/server/exec/tests.rs` entries in strict alphabetical order.

#### 9) Documentation updates
- Update `docs/vector-indexes.md` and `plan_vectors.md`:
  - v2 `.vdata` with row IDs and flags; ORDER BY ANN behavior; TVFs usage; metrics ordering; modes; config knobs; examples.

#### 10) Performance & hygiene
- Optional micro-benchmarks (N ∈ {1e4, 1e5}, dim ∈ {64, 384, 768}, k ∈ {10, 100}) contrasting flat vs HNSW.
- Keep files < ~600 LOC; move helpers to submodules if needed. Maintain Polars 0.51+ patterns (`Series::get` + `AnyValue`, `IdxSize` for sort limits).

#### 11) Build & verification workflow
- Compile after each major feature; do not run tests until end per earlier guidance.
- After completing all items, run full test suite; treat failures as caused by changes and fix them.
- Deliver a concise changelog and summary of diagnostics added.

---

### Milestones and order of execution
1) ORDER BY ANN two-phase + secondary keys + diagnostics.
2) Observability (SHOW extensions + EXPLAIN annotations).
3) DML freshness modes (runtime placeholders + SHOW).
4) Row‑ID PK persistence and mapping.
5) TVF enhancements and examples.
6) Dimension enforcement policy.
7) HNSW feature upgrades (optional where feasible).
8) Tests expansion → compile → full test run → fixes.
9) Docs update → final review.

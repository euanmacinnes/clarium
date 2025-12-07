### Objective
Implement remaining feature gaps identified outside filestore, with production‑grade robustness, performance, and test coverage. Prioritize minimal disruption to existing behavior while providing graceful error handling and comprehensive observability.

### Scope of Work
- General EXPLAIN coverage beyond vector TVFs
- JSON-in-FROM support for KV `Json` values
- Graphstore dictionary: binary format reader and builder (with JSON compatibility)
- Graphstore append-only delta log writing (on-disk) to complement existing WAL/recovery

### Guiding Principles
- Keep primary interfaces thin; break down large functions; cap files ~600 LOC.
- Prefer promoting/moving existing helpers over re‑implementing.
- Follow Junie Polars guidelines (0.51+) for stable APIs.
- Separation of concerns: parsing vs execution; DDL in separate files.
- Permanent debug tracing via `tracing` (debug/info) that’s no-op in release.
- Graceful errors with `anyhow`+context; no panics/bail-outs that crash threads.
- Comprehensive, multi-level tests (unit, integration, property, perf smoke).

---

### Phase 1 — EXPLAIN Expansion
1. Design
   - Define `ExplainPlan` enum covering: statement type, input sources, projections, filters, joins, aggregations, order/limit, index usage hints, and vector/graph TVFs.
   - Add `ExplainOptions { format: text|json, verbose: bool }` for future CLI compatibility.
2. Implementation
   - Parser: extend `Command::Explain` to parse optional `VERBOSE` and `FORMAT JSON` flags (keep backward compatibility).
   - Planner hooks: instrument select planning stages to populate `ExplainPlan` without executing.
   - Renderers: `explain_text(&ExplainPlan)` and `explain_json(&ExplainPlan)`.
3. Observability & errors
   - `tracing::debug!` nodes at each planner stage with stable fields.
   - Return helpful error when a stage cannot be explained yet; never panic.
4. Tests
   - Unit tests for renderer output determinism.
   - Integration tests over representative SQLs: simple SELECT, filters, joins, order/limit, vector TVF, graph TVF.
   - Golden files for text output; schema assertions for JSON output.
5. Deliverables
   - `EXPLAIN` returns structured information for non-TVF SELECTs; retains TVF explanations.

### Phase 2 — JSON-in-FROM for KV Json values
1. Design
   - Accept KV `Json` where `FROM <db>.store.<kv>.<key>`; produce a DataFrame by:
     - If JSON is array of objects → rows = elements, columns = union of object keys.
     - If JSON is single object → 1 row with keys as columns.
     - If JSON is array of scalars → single column `value`.
     - If JSON is mixed → coerce using best‑effort dtype inference per column; nulls for missing fields.
   - Dtype inference
     - Pass 1: sample N records (configurable, default 1024) to infer target dtypes per field.
     - Coercion rules with fallbacks per Polars guidelines; use `try_extract::<T>()` where feasible.
2. Implementation
   - Add `json_to_df(json: &serde_json::Value) -> DataFrame` utility in `exec/df_utils_json.rs` (new small file).
   - Integrate into existing `read_df_or_kv` paths in both `exec/df_utils.rs` and `data_context.rs` via promoted public helper, not duplicated code.
   - Preserve existing behavior for other KV types.
3. Observability & errors
   - `tracing::debug!` with sample sizes, inferred schema, coercion stats.
   - Graceful errors for invalid JSON root types (e.g., null) with clear messages.
4. Tests
   - Unit tests for object, array-of-objects, scalar lists, mixed types.
   - Property tests with randomized JSON ensuring no panics and stable column counts.
   - Integration: queries selecting and filtering on parsed columns; boolean mask patterns follow Junie guidelines.
5. Deliverables
   - `FROM <db>.store.<kv>.<key>` works for JSON with robust schema inference and stable behavior.

### Phase 3 — Graphstore Dictionary: Binary Format Reader & Builder
1. Design
   - Define stable `dict.seg` binary format v1: header (magic, version), counts, followed by compact key/value pages; include checksum.
   - Provide JSON<->binary conversion tools for tests and migration.
2. Implementation
   - Reader: implement current `open()` to parse v1 and populate in‑memory maps efficiently.
   - Builder: `NodeDictBuilder` producing v1 from input pairs; incremental append API for compaction pipelines.
   - Maintain `from_json` pathway for tests; add `to_json` for round‑trip validation.
3. Observability & errors
   - Debug logs for format detection, counts, and checksum validation.
   - Detailed error contexts; never panic on corrupt files—return descriptive errors.
4. Tests
   - Unit tests for encoder/decoder round‑trip; corruption cases; large datasets.
   - Integration: reader used by graph queries with v1 files.
   - Benchmarks: cold/warm open times and lookup throughput.
5. Deliverables
   - Binary dict support enabled with backward‑compatible JSON fallback.

### Phase 4 — Graphstore Append‑Only Delta Log Writing
1. Design
   - On-disk append-only `delta.log` per partition with record framing, checksums, and sync policy (configurable).
   - Writer API mirrors existing WAL record types; integrate with transaction boundaries.
   - Recovery merges: existing in-memory index builder consumes both WAL and delta logs coherently.
2. Implementation
   - `DeltaWriter` with buffered writes and periodic fsync; rotate by size/time.
   - Extend recovery to scan latest delta logs after WAL replay and build `PartitionDeltaIndex`.
   - Configuration surface in server settings with sensible defaults.
3. Observability & errors
   - `tracing` for append events, rotations, fsync timings; error counters.
   - Graceful degradation: if writing fails, surface error to client without crashing worker.
4. Tests
   - Unit tests for writer framing and checksums; fsync/rotation behavior.
   - Integration: run transactions, crash simulation, recovery asserts deltas reflected.
   - Performance smoke: append throughput baseline.
5. Deliverables
   - Durable append-only deltas with reliable recovery and no regressions.

### Cross-Cutting Tasks
- Refactors
  - Deduplicate `read_df_or_kv` into a single public helper module; update call sites.
  - Ensure files remain <600 LOC; split modules as needed (`exec/explain`, `exec/json_utils`, `graphstore/dict/{reader,builder}.rs`).
- Observability
  - Ensure `tracing::debug!`/`info!` across new code paths with stable targets (e.g., `clarium::exec`, `clarium::graph`).
- Documentation
  - Update `docs/README.md` and `docs/getting-started.md` covering new features and config.
  - Add developer docs for `EXPLAIN` schema and graphstore formats.
- CI & Lints
  - Add tests for new modules; run on CI with feature flags matching production.

### Milestones & Estimates (rough)
- Phase 1: EXPLAIN expansion — 3–5 days
- Phase 2: JSON-in-FROM — 4–6 days (inference + edge cases)
- Phase 3: Dict binary reader/builder — 5–8 days (incl. round‑trip and perf)
- Phase 4: Delta log writing — 6–10 days (incl. recovery + crash simulation)
- Cross‑cutting docs/CI/refactors — parallel, ~2–4 days across phases

### Acceptance Criteria
- All new features covered by unit + integration tests; no panics in happy or error paths.
- Polars 0.51+ guidelines adhered to (no `utf8()?.iter()` usage; masks built via `Vec<bool>` as needed).
- Performance: no >5% regression in existing query benchmarks; basic throughput numbers documented for new components.
- Files under ~600 LOC; no oversized functions; primary interfaces thin and modular.

### Risks & Mitigations
- JSON schema inference ambiguity → allow user override via optional hints in query or config; document behavior.
- Binary format compatibility → versioned format and round‑trip tests; JSON fallback retained for dev/testing.
- Delta log durability tradeoffs → configurable fsync strategy; defaults balanced for safety.

### Next Steps
- Confirm scope and prioritization order.
- I’ll draft interface definitions and skeletons for Phase 1 and Phase 2 (read‑only PR for review), then proceed to implementation with tests upon approval.
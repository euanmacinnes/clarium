Clarium benchmarking suite
==========================

This document describes the complete Criterion-based benchmarking suite that ships with Clarium. It covers what we measure, how the suites are organized, how to run them, how to interpret results, and how to extend the suite with new workloads.

Overview
--------
The suite measures end‑to‑end performance across five database modalities using two complementary approaches:

- Direct micro‑benchmarks (in‑memory): isolate core data‑structure and algorithmic costs without engine overheads.
- SQL‑driven benchmarks: execute the actual query engine and DDL/DML/TVFs, exercising parsing, planning, execution, and catalog/runtime integrations.

We use Criterion 0.5 with flat sampling and HTML reports for reproducible results. Synthetic datasets are generated deterministically from seeded RNGs so repeated runs are comparable.

Bench targets and sources
-------------------------
All benches live under `benches/` and are small, focused files (< 600 LOC) with helper utilities in `benches/bench_support.rs`.

Direct micro‑benchmarks (algorithmic baselines):
- benches/vector_ann.rs — exact flat L2 vs ANN HNSW micro‑benchmarks across a grid of sizes and dimensions.
- benches/tables.rs — DataFrame filtering and group‑by using Polars.
- benches/time_series.rs — temporal range filtering and rolling mean using Polars.
- benches/graph.rs — k‑hop BFS on a synthetic adjacency list.
- benches/kv.rs — HashMap put/get/delete patterns (sequential and random).

SQL‑driven, end‑to‑end benchmarks (engine path):
- benches/sql_tables.rs — regular table scan/filter and group‑by via SQL.
- benches/sql_time_series.rs — time‑series range filter and per‑key averages via SQL.
- benches/sql_vector.rs — exact vs ANN nearest‑neighbors via TVF and vector index DDL.
- benches/sql_graph.rs — graph neighbors at depth 2 and 3 over a table‑backed graph catalog.
- benches/sql_kv.rs — KV store write/read/delete via SQL DML.

Shared bench harness
--------------------
File: `benches/bench_support.rs`

- BenchCtx::new(): creates a temp store, initializes a small Tokio runtime, and sets session defaults (`USE DATABASE clarium; USE SCHEMA public`).
- exec(sql) / exec_ok(sql): execute SQL through the engine’s public async entrypoint `server::exec::execute_query`, blocking within the dedicated runtime for determinism.
- Each SQL bench uses this context to isolate state per benchmark group, avoid cross‑talk between runs, and keep setup (DDL/DML) outside the measured timing closures.

Data generation and determinism
-------------------------------
- All data is synthetic and seeded via `StdRng::seed_from_u64(seed)`.
- Inserts are batched into modest statement sizes to avoid pathological parsing costs skewing results.
- Query benchmarks perform one warm‑up execution outside the measured closure to ensure caches and just‑in‑time paths are initialized.

Vector search specifics
-----------------------
- Exact (flat) vs ANN (HNSW) are contrasted on the grid: N ∈ {1e4, 1e5}, dim ∈ {64, 384, 768}, k ∈ {10, 100}.
- SQL benches use the TVF `nearest_neighbors(table, 'vec', qvec, k, 'l2'[, ef])` to exercise planner/runtime selection.
- ANN is enabled via the `ann_hnsw` feature. When disabled, the same TVF gracefully falls back to an exact scan.
- Build DDL example used in benches:
  ```sql
  CREATE VECTOR INDEX idx_v ON clarium/public/v_ann(vec)
  USING hnsw WITH (metric='l2', M=32, ef_build=200);
  ```

Graph traversal specifics
-------------------------
- Synthetic `nodes(id)` and `edges(src,dst)` tables are created and populated deterministically.
- A logical graph is registered via:
  ```sql
  CREATE GRAPH clarium/public/bench_graph
  NODES (Node KEY(id))
  EDGES (E FROM Node TO Node)
  USING TABLES (nodes=clarium/public/bench_nodes, edges=clarium/public/bench_edges);
  ```
- Benchmarks call `graph_neighbors(graph, start, 'E', depth)` and count results for depth 2 and 3.

KV store specifics
------------------
- DML operates on `database.store.<store>` using the SQL surface:
  - WRITE KEY <k> IN <db>.store.<store> = <value> [TTL <dur>] [RESET ON ACCESS|NO RESET]
  - READ KEY <k> IN <db>.store.<store>
  - DROP KEY <k> IN <db>.store.<store>
- Benches measure sequential and random writes, random reads, and random deletes for N ∈ {1e5, 1e6}.

How to run
----------
Criterion discovers benches registered in Cargo.toml as `[[bench]]` targets. Run individual suites or all of them.

Examples (PowerShell on Windows):
- Run a single SQL bench:
  - `cargo bench --bench sql_tables`
- Run vector ANN suite (ANN enabled by default):
  - `cargo bench --bench sql_vector`
- Disable ANN feature to measure only the exact flat path:
  - `cargo bench --no-default-features --features pgwire --bench sql_vector`
- Run all registered benches:
  - `cargo bench`

Reports and artifacts
---------------------
- Criterion HTML reports are written under `target/criterion/report` and per‑bench directories under `target/criterion/<group>/report`.
- Open `target/criterion/report/index.html` in a browser to compare runs. Criterion keeps baselines; changing code invalidates and regenerates comparisons.

Interpreting results
--------------------
- Throughput annotations: benches set `Throughput::Elements(...)` to give context (e.g., elements processed or k results per query). Use this to normalize across sizes.
- Warm‑ups: each measured query has a prior warm‑up execution; reported times reflect steady‑state.
- For vector ANN, expect indexed searches to be faster at higher N and dim; flat may be competitive for small N or tiny k. Use the HNSW build benchmark to factor index build cost into planning.

Feature flags and environment
-----------------------------
- `ann_hnsw` (default on): enables HNSW ANN engine paths and vector index DDL behavior. Disable to force exact scanning paths in vector benches.
- `pgwire` (default on): unrelated to benches, but enabled by default in the project feature set.

Guidelines and hygiene
----------------------
- Files are intentionally kept short; helpers are centralized.
- SQL benches time only the engine path; setup (DDL/DML/data load) is outside measurement.
- Polars usage in direct benches follows our Junie Polars Guidelines (0.51+):
  - Avoid `utf8()?.iter()`; use `Series::get` + `AnyValue` conversions.
  - Boolean masks via `Vec<bool>` → `Series<bool>` → `DataFrame::filter`.
  - Sort limits cast to `IdxSize` where applicable.
- Error handling is defensive (no panics on malformed rows); treat extraction failures as nulls.

Extending the suite
-------------------
1) Choose the style:
   - For end‑to‑end measurement, add a new `benches/sql_<name>.rs` using `BenchCtx` and drive the workload via SQL.
   - For algorithmic baselines, add `benches/<name>.rs` measuring the primitive you care about.

2) Keep functions small and file size under ~600 lines; move shared helpers into `benches/bench_support.rs`.

3) Determinism: seed RNG and keep setup out of timing. Prefer COUNT or small result sets to avoid materialization overhead distorting measurements.

4) Register the new bench in `Cargo.toml`:
   ```toml
   [[bench]]
   name = "sql_newworkload"
   harness = false
   ```

5) Run locally, inspect Criterion reports, and add a brief description of the workload to this document.

Troubleshooting
---------------
- Build errors on ANN types: ensure the `ann_hnsw` feature is enabled (default) or skip ANN benches with `--no-default-features --features pgwire`.
- SQL bench fails with an unsupported command: confirm the statement’s surface matches the engine’s current SQL. Use `EXPLAIN <sql>` or consult docs for vector/graph TVFs.
- Criterion report missing or empty: ensure the bench actually ran (Cargo target appears in `Cargo.toml`). Delete `target/criterion` to reset baselines if needed.

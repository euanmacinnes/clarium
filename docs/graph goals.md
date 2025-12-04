### Summary
You currently materialize graph TVFs (`graph_neighbors`, `graph_paths`) from regular tables using a small `.graph` catalog. This is simple and works well for read-only use, but can’t deliver the best possible performance, nor ACID writes, because it depends on general-purpose row stores. Below is a proposal for a purpose-built graph store that preserves your SQL/TVF surface area while adding:

- Direct, high-performance graph storage (memory-mapped, cache-aware)
- ACID transactional writes with snapshot isolation (MVCC) and WAL-based recovery
- Low-latency traversals (k-hop, shortest path, constrained searches) at scale
- Online rebuild/compaction, online migration, and dual-write cutovers

Where useful, this ties back to your current code paths in `src/server/exec/exec_graph_runtime.rs` and `docs/graph-catalog.md`.

---

### Goals and non-goals
- Goals
  - Maximal read performance for traversals used by `graph_neighbors` and `graph_paths` (BFS, bounded hops, Dijkstra/A*).
  - ACID writes: atomicity and durability (WAL), consistency (schema validation), isolation (MVCC snapshot), crash-safe recovery.
  - Keep the same user-facing SQL/TVFs and `CREATE GRAPH`/`SHOW GRAPH` UX. The planner keeps seeing TVFs; the runtime picks the new engine automatically when available.
  - Operational: online import, online compaction, metrics, integrity checks, versioned side-by-side deployment.
- Non-goals
  - Full open-cypher-like MATCH language in this iteration.
  - Global transactions across independent databases beyond the existing transactional scope.

---

### Storage engine design (GraphStore)
A dedicated on-disk engine per graph that resides alongside your current DB layout: `<db>/<schema>/<graph>/.gstore/`.

- Layout
  - `nodes/`
    - `dict.seg.*` immutable segments: mapping `external_key -> internal_node_id` (packed string table + hash index), memory-mapped.
    - `delta.log` append-only WAL of node mutations (insert/upsert/delete), periodically sealed into a new `dict.seg.N`.
  - `edges/`
    - Partitioned adjacency segments (immutable) using hashed `src_id % P` to control fan-out and parallelism: `adj.PART.seg.N`.
    - Each adjacency segment is a CSR-like structure:
      - `row_ptr` (u64 offsets, length = nodes_in_partition + 1)
      - `cols` (dst_ids, tightly packed, varint or fixed u32/u64)
      - `weights`/`costs` (optional, packed f32/f64), `time` (optional start/end epochs), `etype_id` (optional small int)
    - Reverse adjacency segments are optional but recommended for algorithms needing in-edges: `radj.PART.seg.N`.
    - `delta.log.PART` append-only per partition for new edges and tombstones; periodically compacted into the next immutable `seg.N+1`.
  - `meta/`
    - `manifest.json` (current epochs, segment list, partitioning P, etype map, feature flags)
    - `wal/` (write-ahead log files with checksums + index)

- Encoding
  - Big-endian fixed-width for numeric IDs; varint for sparse lists; optional Zstd on large vectors; page-aligned blocks (e.g., 4–16 KiB) with per-page checksums.
  - Memory mapping via `memmap2` for immutable segments; direct I/O optional for sequential compactions.

- IDs and dictionaries
  - Internally use dense `u64` node IDs for adjacency; maintain a node dictionary keyed by the logical key from `.graph` (`label + key`) → `node_id`.
  - Edge encodes `src_id`, `dst_id`, `etype_id`, `cost` (optional), and validity `[ts_begin, ts_end)` for temporal queries.

- Temporal/versioned data
  - MVCC at the edge level: each logical edge can have multiple versions. Compaction merges deltas into new immutable CSR snapshots per partition with version slicing. Readers choose a snapshot epoch.

---

### Write path (ACID)
- Transaction begin produces a `TxnId` and a read snapshot (epoch S).
- Writes buffer in-memory per partition (node upserts, edge upserts/deletes).
- Prepare phase: append a single WAL record with:
  - `TxnId`, snapshot S, intent list (node/edge mutations), and a content checksum.
- Commit phase:
  - Fsync the WAL record; ack commit to client after durable WAL append (group commit enabled for batching).
  - Apply to `delta.log` files (append-only) for affected node/edge partitions.
  - Optionally trigger background compaction if thresholds exceeded.
- Abort: append an abort record; buffers discarded. On recovery, uncommitted intents are ignored.

- Isolation model: snapshot isolation (readers never block writers)
  - Readers bind to a stable manifest epoch E. They read immutable segments plus in-memory per-partition delta indexes filtered by `ts_begin <= E < ts_end`.
  - Writers only become visible when the manifest advances (post-commit publish) or when deltas are consulted under the same epoch rules.

- Concurrency control
  - Per-(graph, partition) mutex for applying deltas and compaction state; fine-grained latches during manifest rotation.
  - Optimistic concurrency for node/edge upserts; key-level conflicts resolved by last-writer-wins on `ts_begin` or by constraint checks (e.g., unique node key).

- Recovery
  - On startup, replay WAL to rebuild any missing `delta.log` tail, verify checksums per record/page.
  - Validate manifest consistency (all referenced `seg.N` exist and have matching hashes) before serving traffic.

---

### Read path and algorithms
- Adjacency resolution
  - For a node `u`, locate its partition `p`, map `row_ptr[u]..row_ptr[u+1]` to a contiguous slice of neighbors.
  - Merge-iterate immutable slice with in-memory delta index (tiny skiplist or SIMD-accelerated dedup) to materialize the current view.

- Operators
  - `graph_neighbors(graph, start, etype, max_hops[, time_start, time_end])`
    - BFS level by level using ring buffers; per-hop frontier dedup via roaring bitmap or radix hash set.
    - Optional etype filter is just a mask over adjacency entries (store `etype_id` in packed side array or compress if singular).
    - Optional time window filters edges by validity `[ts_begin, ts_end)`.
  - `graph_paths(graph, src, dst, max_hops)`
    - Bounded BFS; for weighted edges, switch to Dijkstra with a pairing heap or bucketed radix queue (Dial’s algorithm) when weights are small integers.

- Caching
  - Hot frontier pages pinned (clock-pro or TinyLFU) at the block level; decoded neighbor lists cached as small structs to avoid repeated varint decode.
  - Optional prefetch (sequential within partition) to exploit spatial locality.

- Hybrid vector + graph
  - Keep your vector index flow; join results with graph frontiers. Co-locate node embeddings in node dictionary segments for zero-copy joins.

---

### SQL and DDL extensions (backward compatible)
- Extend `CREATE GRAPH` with an engine clause and more options:
  - `CREATE GRAPH know ... USING TABLES (...)` (legacy)
  - `CREATE GRAPH know USING ENGINE GRAPHSTORE PARTITIONS 32 OPTIONS (temporal = true, reverse_adjacency = true);`

- Mutations
  - `INSERT NODE INTO know(label, key, props...) VALUES ('Tool','planner', ...);`
  - `UPSERT NODE INTO know(label, key, props...) ...;`
  - `INSERT EDGE INTO know(type, from_label, from_key, to_label, to_key, cost, valid_from, valid_to) VALUES (...);`
  - `DELETE EDGE FROM know WHERE type='Calls' AND from=('Tool','planner') AND to=('Tool','executor');`

- TVFs unchanged; planner binds them to GraphStore when `manifest.json` says `engine = graphstore`, otherwise to table-backed runtime (your current `exec_graph_runtime.rs`).

---

### Integration plan with current code
- Runtime router
  - In `exec_graph_runtime.rs`, at TVF entry (`graph_neighbors_df` / future `graph_paths_df`), resolve graph metadata.
  - If `engine = graphstore`, use the GraphStore API; else fallback to current table-based loader.

- API surface (Rust)
  - `GraphStore::open(db, schema, name) -> GraphHandle`
  - `GraphHandle::begin_txn() -> GraphTxn`
  - `GraphTxn::{insert_node, upsert_node, insert_edge, upsert_edge, delete_edge} -> Result<()>`
  - `GraphTxn::commit()` / `abort()`
  - `GraphHandle::{neighbors_bfs, neighbors_khop, shortest_path, iter_edges}
  - `GraphHandle::snapshot(epoch) -> Snapshot`

- Catalog
  - Keep `.graph` JSON but add `engine`, `partitions`, `options` fields. Persisted in `manifest.json` as the source of truth.

---

### Import, migration, and cutover
- Offline import
  - `BUILD GRAPH know FROM TABLES (nodes=..., edges=...) INTO ENGINE GRAPHSTORE PARTITIONS 32;` constructs segments in a staging directory, then atomically swaps `manifest.json`.
- Online import (dual-write)
  - Enable dual write from table-backed mutations to GraphStore WAL while continuing reads from tables; backfill historical edges by partition; when caught up, flip the runtime to GraphStore.
- Rollback
  - Keep the table source of truth until validation passes; feature flag to revert TVF routing.

---

### Compaction and maintenance
- Triggers
  - Compaction per partition based on delta size, tombstone ratio, or time since last seal.
- Process
  - Merge `delta.log.PART` into `adj.PART.seg.N+1` and optional `radj` in one pass; write new segment with checksummed pages; atomically update manifest to epoch E+1.
- Time-travel retention
  - Retain historical edge versions for a configurable window; older versions pruned during compaction.

---

### ACID details and correctness
- Atomicity: WAL records are all-or-nothing; on crash, incomplete record is discarded via checksum mismatch.
- Consistency: node/edge schema constraints validated before WAL append; etype/labels checked against catalog.
- Isolation: snapshot isolation (readers see a consistent graph state); optional read-committed mode for lower latency.
- Durability: `fdatasync`/FlushFileBuffers at group commit; knobs for `sync_policy = always | batch | relaxed`.

---

### Performance principles
- Immutable segments + memory mapping → zero-copy sequential access for BFS and path scans.
- CSR layout gives contiguous neighbor lists; partitioning gives parallel frontiers and NUMA locality.
- Delta+compaction avoids random in-place updates, turning writes into sequential appends.
- Small, cache-friendly indexes and optional reverse adjacency for algorithms that need in-edges.

Expected wins over table-backed evaluation:
- 3–10x faster k-hop neighbor enumeration on medium/large graphs; more on high-degree nodes due to contiguous memory.
- Predictable latency and lower GC/memory churn.

---

### Observability and safety
- Expose per-graph/partition metrics: frontier expansions/sec, compaction backlogs, segment sizes, cache hit rates, WAL lag, queueing delay.
- Commands:
  - `SHOW GRAPH STATUS know;`
  - `CHECK GRAPH know [DEEP];` (verifies row_ptr monotonicity, checksums, dictionary coverage, orphan edges)

---

### Benchmark plan
- Datasets: synthetic power-law graphs and real workloads similar to your `seed_tools_graph` scale; vary degree distributions.
- Queries: `graph_neighbors` at 1–4 hops; `graph_paths` with/without weights; hybrid vector+graph joins as in `end_to_end_planning_tests.rs`.
- Metrics: p50/p95 latency, throughput, CPU/IO, memory, compaction overhead.
- Success criteria: ≥3x speedup over table-backed TVFs, with stable p95 under write load (dual-write) and after compactions.

---

### Implementation milestones
1) Engine skeleton and manifest; read-only CSR loader; route TVFs when `engine=graphstore` (read-only).
2) WAL + transactional API; delta indexes; snapshot reads; test crash/recovery.
3) Compaction path and manifest rotation; reverse adjacency.
4) Temporal fields; time-slice traversal.
5) DDL plumbing; dual-write import; migration tooling; status/metrics.
6) Optimizations: roaring dedup, SIMD decode, prefetch, group commit tuning.

---

### Example end-to-end
- Define graph using new engine:
```
CREATE GRAPH clarium/public/know
USING ENGINE GRAPHSTORE PARTITIONS 32 OPTIONS (temporal = true, reverse_adjacency = true);
```
- Write edges transactionally:
```
BEGIN;
INSERT NODE INTO clarium/public/know(label, key) VALUES ('Tool','planner');
INSERT NODE INTO clarium/public/know(label, key) VALUES ('Tool','executor');
INSERT EDGE INTO clarium/public/know(type, from_label, from_key, to_label, to_key, cost, valid_from)
VALUES ('Calls','Tool','planner','Tool','executor', 1.0, NOW());
COMMIT;
```
- Query unchanged:
```
SELECT *
FROM graph_neighbors('clarium/public/know','planner','Calls',2) g
ORDER BY hop, node_id;
```

---

### Closing
This design retains your current SQL surface while replacing the table-backed runtime with a purpose-built GraphStore that offers ACID writes and high read performance. If you’d like, I can map this to concrete Rust modules/interfaces under `src/server/exec/exec_graph_runtime.rs` and a new `src/server/storage/graphstore/` skeleton, or adapt the plan to stricter constraints (e.g., no memory-mapping, or different isolation).
Graph catalogs and TVFs
=======================

Clarium supports a lightweight Graph catalog with sidecar JSON (`.graph`) and two table-valued functions to traverse graphs using standard SELECT syntax.

Create a graph
--------------
```
CREATE GRAPH know
NODES (Doc KEY(id), Tool KEY(name))
EDGES (Calls FROM Tool TO Tool, Cites FROM Doc TO Doc)
USING TABLES (nodes=clarium/public/know_nodes, edges=clarium/public/know_edges);
```

Inspect and drop
----------------
```
SHOW GRAPH know;
SHOW GRAPHS;
DROP GRAPH know;
```

On-disk catalog
---------------
Graphs live at `<db>/<schema>/<name>.graph` with lists of node labels and edge types and their table bindings.

GraphStore option
-----------------
You can opt into the GraphStore engine during creation and seed its configuration. This initializes a GraphStore manifest under `<db>/<schema>/<name>.gstore/meta/manifest.json` so traversal functions can use the GraphStore runtime immediately (data population still occurs via ingestion tools).

Examples
```
-- Create a GraphStore-backed graph with 4 partitions and a GC window option
CREATE GRAPH know
NODES (Doc KEY(id), Tool KEY(name))
EDGES (Calls FROM Tool TO Tool, Cites FROM Doc TO Doc)
USING GRAPHSTORE WITH (partitions=4, gc_window='10m');

-- Or reference a named GraphStore configuration (if your environment supports named configs)
CREATE GRAPH know
NODES (User KEY(id))
EDGES (Follows FROM User TO User)
USING GRAPHSTORE CONFIG default_cluster;
```

Write data into a graph
-----------------------
There are two ingestion paths depending on how the graph was created:

- Table-backed graphs (USING TABLES): write rows into the bound node/edge tables using standard SQL DML.
- GraphStore-backed graphs (USING GRAPHSTORE): append node/edge mutations via the GraphStore write API or tooling; the engine persists a WAL and partitioned delta logs under the graph’s `.gstore` directory.

Table-backed ingestion (SQL)
----------------------------
When a graph is created with `USING TABLES (nodes=..., edges=...)`, the catalog records which physical tables store nodes and edges. You write data by inserting into those tables. The traversal TVFs will read from them.

Required and optional columns
- Nodes table: must include a primary key column per label’s `KEY(...)`; commonly `id` (text or integer). You may include additional attributes like `label`, `name`, `embed`.
- Edges table: must include `src` and `dst` columns referencing node keys. Optional columns:
  - `etype` (edge type name; when omitted, the first edge type in the catalog is used by TVFs today)
  - `cost` (numeric weight)
  - `_time` (ISO8601 or epoch for time-sliced traversals)

Examples
```
-- Example schema (you may adapt types). If the tables already exist, skip DDL.
CREATE TABLE clarium/public/know_nodes (
  id TEXT PRIMARY KEY,
  label TEXT,
  name TEXT,
  embed VECTOR(384)
);

CREATE TABLE clarium/public/know_edges (
  src TEXT NOT NULL,
  dst TEXT NOT NULL,
  etype TEXT DEFAULT 'Calls',
  cost DOUBLE PRECISION,
  _time TIMESTAMP
);

-- Insert nodes
INSERT INTO clarium/public/know_nodes (id, label, name) VALUES
  ('planner','Tool','Planner'),
  ('executor','Tool','Executor');

-- Insert edges
INSERT INTO clarium/public/know_edges (src, dst, etype, cost)
VALUES ('planner','executor','Calls', 1.0);
```

Notes
- Use `INSERT ... ON CONFLICT (...) DO UPDATE` or `MERGE` to upsert nodes in systems that support it.
- Keep node keys stable; edges refer to node keys, not internal numeric IDs.
- Time-aware queries can be supported by populating `_time` and using time-window variants of TVFs where available.

GraphStore-backed ingestion (engine API)
----------------------------------------
When a graph is created with `USING GRAPHSTORE [...]`, writes go to the GraphStore engine under `<db>/<schema>/<name>.gstore/`.

Storage layout (relevant for ingestion)
- WAL: `<graph>.gstore/wal/current.lg` — durable transaction log (append-only; fsync on commit).
- Edge delta logs (per partition): `<graph>.gstore/edges/delta.PNNN.log` — recent edge mutations for fast visibility.
- Node delta log: `<graph>.gstore/nodes/delta.log` — node upserts/deletes.

Write semantics
- Writes are grouped into transactions. On commit, the engine appends `Begin/Data/Commit` records to the WAL, then appends partitioned delta records. Recovery and GC consolidate these into compact segments later.
- Partitioning for edges is controlled by `WITH (partitions=..., partitioning.strategy=..., partitioning.hash_seed=...)` or the referenced `CONFIG`. If `has_reverse=true`, the engine also maintains reverse adjacency during compaction.

Programmatic API (Rust)
```
use clarium::server::graphstore::txn::GraphTxn;
use std::path::Path;

// Resolve the graph root, e.g., dbs/clarium/public/know.gstore
let root = Path::new("dbs/clarium").join("public").join("know.gstore");

// Start a transaction (snapshot_epoch can be 0 for now)
let mut tx = GraphTxn::begin(&root, 0)?;

// Upsert a node (label, key[, optional assigned node_id])
tx.insert_node("Tool", "planner", None);
tx.insert_node("Tool", "executor", None);

// Route an edge to a partition (e.g., hash_mod by src with seed)
let part: u32 = 0; // compute using your partition function if partitions>1
tx.insert_edge(part, /*src_id*/ 1, /*dst_id*/ 2, /*etype_id*/ 0);

// Commit (durable WAL + delta logs)
tx.commit(/*commit_epoch*/ 1)?;
```

CLI/operational notes
- The engine exposes append-only logs; use the API (or compatible tooling) to batch writes and ensure idempotence via `(txn_id, op_index)` tracking.
- Set `CLARIUM_GRAPH_COMMIT_WINDOW_MS` to control WAL group-commit latency/throughput trade-offs (default 3ms; 0 disables batching).
- Recovery replays WAL into delta logs at startup if needed; background GC compacts deltas into segments based on `gc_*` options.

Mapping from external keys to internal IDs
- For GraphStore, node identity is `(label, key)`. The runtime maintains a node dictionary that maps these to numeric `node_id`s. During initial phases, you may supply a `node_id` when inserting a node; otherwise the engine will assign one during compaction.
- Edges in deltas reference numeric `src`/`dst` IDs. If you’re ingesting from application-level keys, resolve or upsert nodes first, then emit edges with their IDs.

Consistency and deduplication
- Within a single transaction, duplicate node operations on the same `(label,key)` are rejected.
- Delta application is idempotent by `(txn_id, op_index)`; replays won’t double-apply.

Which path should I use?
- If you already store nodes/edges in SQL tables and need simple batch loads: use table-backed graphs and `INSERT`/`MERGE`.
- If you need high-throughput streaming writes, partitioned adjacency, and WAL-based durability optimized for graph workloads: prefer GraphStore.

USING GRAPHSTORE — full WITH options
------------------------------------
The `WITH (...)` clause accepts key=value pairs (values may be quoted with single quotes). All keys are case-insensitive; unknown keys are preserved in the engine `options` map for forward compatibility. The following options are recognized today:

- partitions: u32 — total number of edge partitions to initialize; default: 1.
- has_reverse: bool — whether the store will also materialize reverse adjacency; default: false.
- partitioning.strategy: string — partitioning method. Supported: 'hash_mod'; default: 'hash_mod'.
- partitioning.hash_seed: u64 — seed used by hash-based partitioning; default: 0.
- gc_window: duration — time window to aggregate small deltas before GC; examples: '10m', '2h'; default: '10m'.
- gc_max_delta_records: integer — maximum un-compacted delta records before triggering GC; default: 100000.
- gc_tombstone_ratio_ppm: integer — tombstone ratio threshold (parts per million) to trigger compaction; default: 50000 (5%).
- wal_enabled: bool — enable write-ahead log recording; default: true.
- wal_segment_bytes: integer — maximum WAL segment size in bytes, e.g., 134217728; default: 134217728 (128 MiB).
- wal_retention: duration — minimum time to retain closed WAL segments, e.g., '1h'; default: '1h'.
- compaction.min_segments: integer — minimum number of small segments per partition before compaction runs; default: 4.
- compaction.max_segments: integer — soft limit of segments per partition after compaction; default: 16.
- cluster.replication_factor: u32 — planned replication factor for clustered deployments; default: 1.

Notes
- You may use dotted keys (e.g., `partitioning.strategy`, `compaction.min_segments`); they are stored as flat `options` keys in the manifest.
- `WITH` values are strings in the manifest; the runtime parser coerces them into expected types. Invalid values skip with warnings and fallback to defaults.
- `WITH` can be combined with `CONFIG <name>`; explicit `WITH` entries override values from the named config (see below).

GRAPHSTORE CONFIG files
-----------------------
Instead of (or in addition to) inline options, you can reference a named configuration: `USING GRAPHSTORE CONFIG <name>`.

Location
- Config files are JSON documents stored under the database root:
  - `<db>/.graphstore/configs/<name>.json`
  - Example for the default database `clarium`: `dbs/clarium/.graphstore/configs/default_cluster.json`

Format
- A JSON object containing the same keys accepted by `WITH`, as strings. Dotted keys are allowed and map 1:1 to manifest `options` keys.

Example
```
// dbs/clarium/.graphstore/configs/default_cluster.json
{
  "partitions": "4",
  "partitioning.strategy": "hash_mod",
  "partitioning.hash_seed": "42",
  "gc_window": "10m",
  "gc_max_delta_records": "200000",
  "gc_tombstone_ratio_ppm": "40000",
  "wal_enabled": "true",
  "wal_segment_bytes": "134217728",
  "wal_retention": "1h",
  "compaction.min_segments": "4",
  "compaction.max_segments": "16",
  "cluster.replication_factor": "1"
}
```

Usage and precedence
- `USING GRAPHSTORE CONFIG <name>` loads the JSON above (if present) and applies it when seeding the manifest on creation.
- You may add `WITH (...)` after `CONFIG <name>` to override selected values inline, for example:
  - `USING GRAPHSTORE CONFIG default_cluster WITH (partitions=8, gc_window='20m')`
- If the config file does not exist, creation will still proceed by seeding the manifest with defaults and any `WITH` overrides.

TVFs: neighbors and paths
-------------------------
You can traverse graphs using TVF-style sources in `FROM`:

- `graph_neighbors(graph, start, etype, max_hops)` → columns `(node_id, prev_id, hop)`
- `graph_paths(graph, src, dst, max_hops)` → columns `(path_id, node_id, ord)`

Examples
--------
```
-- 2-hop tool neighbors filtered by semantic affinity
WITH q AS (SELECT to_vec(:intent) AS v)
SELECT g.node_id, avg(cosine_sim(n.embed, (SELECT v FROM q))) AS affinity
FROM graph_neighbors('know', 'planner', 'Calls', 2) g
JOIN know_nodes n ON n.id = g.node_id
GROUP BY g.node_id
HAVING affinity > 0.55
ORDER BY affinity DESC
LIMIT 10;

-- Shortest path (up to 3 hops) between two tools
SELECT *
FROM graph_paths('know', 'planner', 'executor', 3)
ORDER BY ord;
```

Notes
-----
- For now, `graph_neighbors`/`graph_paths` use the first edge mapping from the `.graph` file; future versions may filter by `etype` precisely.
- Edge tables are expected to have `src` and `dst` columns; optional `cost` and `_time` can be added to the catalog for later use.
- No `MATCH` grammar is introduced; these TVFs integrate with joins and filters in standard SELECT queries.

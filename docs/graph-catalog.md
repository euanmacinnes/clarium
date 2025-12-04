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

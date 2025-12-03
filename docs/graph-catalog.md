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

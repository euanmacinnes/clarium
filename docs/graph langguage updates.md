### Proposal: session-level USE GRAPH to simplify MATCH (and optional TVFs)

You asked to add a session variable similar to `USE DATABASE`/`USE SCHEMA` so `MATCH` can omit `USING GRAPH`. Below is a concrete, backward-compatible design.

#### 1) Syntax and behavior
- Set default graph for the current session:
```
USE GRAPH db/schema/graph;
```
- Inspect current graph default:
```
SHOW CURRENT GRAPH;  -- returns fully qualified name or NULL
```
- Clear the default:
```
UNSET GRAPH;  -- or: USE GRAPH NONE;
```

Semantics:
- The value is a fully qualified graph name `db/schema/name` (same canonicalization as tables).
- It affects parsing/planning of `MATCH` and (optionally) simplified TVF forms. It does not change the current database or schema defaults for regular tables.

Precedence for graph resolution (highest to lowest):
1) Explicit `USING GRAPH 'db/schema/graph'` inside a `MATCH` statement.
2) Session’s `USE GRAPH` setting.
3) Error if neither provided.

#### 2) MATCH without USING GRAPH
With `USE GRAPH` set, `MATCH` can omit the `USING GRAPH` clause:
```
USE GRAPH clarium/public/know;

MATCH (s:Tool { key: 'planner' })-[:Calls*1..2]->(t:Tool)
RETURN t.key AS node_id, hop
ORDER BY hop, node_id;
```
- Planner binds to the session graph and expands to the same low-level operators (e.g., `graph_neighbors`).
- If a `MATCH` still includes `USING GRAPH`, that explicit graph overrides the session default for that statement only.

#### 3) Optional TVF simplification (strictly opt-in)
For backward compatibility, keep existing TVFs unchanged. Optionally, we can support convenience overloads that use the session graph when the first argument is omitted:
- Current (unchanged):
```
SELECT * FROM graph_neighbors('db/schema/graph','planner','Calls',2);
```
- Optional convenience (only if session graph is set):
```
USE GRAPH clarium/public/know;
SELECT * FROM graph_neighbors('planner','Calls',2);
```
If no session graph is set, the convenience forms produce a clear error: “graph not specified and no session graph set; use `USING GRAPH` or `USE GRAPH`.”

#### 4) Parser and planner changes
- Commands:
  - `USE GRAPH <qualified_ident>` → sets the session’s graph string after normalization/validation.
  - `UNSET GRAPH` or `USE GRAPH NONE` → clears.
  - `SHOW CURRENT GRAPH` → returns a one-row result set with a `graph` column (TEXT) or NULL.
- MATCH grammar updates:
  - `MatchStmt := 'MATCH' ['USING' 'GRAPH' String] Pattern 'RETURN' ...`
  - If omitted, planner queries the current session graph; if missing, error.
- TVF binding (if convenience overloads enabled): detect arity and interpret the first argument; when arity doesn’t match, try resolving via session graph.

#### 5) Session model and API integration
- State storage:
  - Extend existing per-session defaults map to include `default_graph: Option<String>` (fully qualified, normalized):
    - In `AppState { session_defaults: Arc<RwLock<HashMap<session_id, (db, schema)>>> }` today, add a companion map (or extend the tuple) to hold the default graph.
  - HTTP/WS handlers update/read this value similar to `USE DATABASE`/`USE SCHEMA`.
- Validation on `USE GRAPH`:
  - Normalize via the same qualifier logic used for tables.
  - Optionally check existence by probing `.graph` sidecar or `.gstore` manifest (`server::graphstore::probe_graphstore` or the legacy `.graph` file) and return a friendly error if not found.
  - Permission checks (if applicable) should mirror `SHOW GRAPH` visibility rules.

#### 6) Error handling
- `MATCH` without graph source and no session default:
  - Error: `No graph specified. Use "USING GRAPH 'db/schema/graph'" or set a session default with "USE GRAPH".`
- `USE GRAPH` to a non-existent graph:
  - Error: `Graph "db/schema/graph" not found` (include a hint to `SHOW GRAPHS;`).
- Convenience TVF without session default:
  - Error: `graph_neighbors: missing graph; either pass a graph name or set a session default (USE GRAPH).`

#### 7) SHOW/Introspection
- `SHOW CURRENT GRAPH;` → returns one row, e.g.:
```
+-------------------------+
| graph                   |
+-------------------------+
| clarium/public/know     |
+-------------------------+
```
- `SHOW GRAPHS;` remains as-is.

#### 8) Interactions with clustering and manifests
- The session default is only a string identifier; partition placement, replication, and manifest epoch resolution continue to be handled at execution time by the GraphStore runtime.
- On a cluster, the coordinator uses the resolved graph name to route to the correct owners per partition as before.

#### 9) Security and multi-tenant guidance
- `USE GRAPH` should be allowed only for graphs visible/accessible to the authenticated user/context.
- Clearing the default on session end avoids cross-tenant leakage.

#### 10) Examples
- Using MATCH with session default:
```
USE GRAPH clarium/public/know;
MATCH (s:Tool { key: 'planner' })-[:Calls*1..2]->(t:Tool)
RETURN t.key AS node_id, hop
ORDER BY hop, node_id;
```
- Overriding session default explicitly:
```
USE GRAPH clarium/public/know;
MATCH USING GRAPH 'other_db/public/g' (s:Tool { key: 'planner' })-[:Calls*1..2]->(t:Tool)
RETURN t.key, hop;
```
- TVF convenience form (optional feature):
```
USE GRAPH clarium/public/know;
SELECT * FROM graph_neighbors('planner','Calls',2) ORDER BY hop, node_id;
```

If you approve, I’ll wire the parser and session state to support `USE GRAPH`, `UNSET GRAPH`, and `SHOW CURRENT GRAPH`, and make the MATCH planner consult the session default when `USING GRAPH` is omitted. The TVF convenience overload can be gated behind a feature flag to keep the surface conservative by default.
# Graph language updates

This document describes the SQL-integrated MATCH grammar, USE GRAPH session defaults, and related commands.

## USE GRAPH session default

- Set default graph for the session: `USE GRAPH db/schema/graph;`
- Clear default: `UNSET GRAPH;`
- Inspect: `SHOW CURRENT GRAPH;`

The default graph is used when a statement (e.g., MATCH, SHOW GRAPH STATUS) omits an explicit graph.

## MATCH — linear patterns with expressions

Supported forms (first round):

- Neighbors (bounded hops):
```
MATCH [USING GRAPH 'db/schema/graph']
  (s:Label { key: <expr> })-[:Type*L..U]->(t:Label)
[WHERE <boolean_expr>]
RETURN <projection_expr_list>
[ORDER BY <expr> [ASC|DESC], ...]
[LIMIT <n>]
```
Rewrites to `SELECT ... FROM graph_neighbors(graph, start, etype, U)` and applies WHERE/ORDER/LIMIT.

- Shortest path (weighted/unweighted):
```
MATCH SHORTEST [USING GRAPH 'db/schema/graph']
  (s:Label { key: <expr> })-[:Type*L..U]->(t:Label { key: <expr> })
RETURN <projection_expr_list>
```
Rewrites to `SELECT ... FROM graph_paths(graph, src, dst, U, etype)`.

### Identifier mapping in expressions

- `t.key` → `node_id`
- `prev.key` → `prev_id`
- `s.key` → substituted literal of the start key expression
- `hop` → `hop`

These mappings apply in `RETURN`, `WHERE`, and `ORDER BY`.

### Subquery support

`MATCH` compiles to a regular `SELECT`, so it can be used:
- As a table source: `FROM (MATCH ...) AS m`
- In CTEs: `WITH m AS (MATCH ...) SELECT ... FROM m`
- In LATERAL joins

## SHOW GRAPH STATUS

`SHOW GRAPH STATUS [<graph>]` returns a single-row table with:
- `epoch`, `partitions`, `delta_adds`, `delta_tombstones`, `compaction_backlog`
- `commit_window_ms`, `gc_max_delta_records`, `gc_tombstone_ratio_ppm`, `gc_max_delta_age_ms`
- runtime metrics: `bfs_calls`, `wal_commits`, `recoveries`

## GC DDL

Trigger graphstore compaction based on GC thresholds:

- Per graph: `GC GRAPH db/schema/graph;`
- Session default: `USE GRAPH db/schema/g; GC GRAPH;`
- All graphs: `GC GRAPH;` (when no default is set)

Thresholds can be tuned via environment variables:
- `CLARIUM_GRAPH_GC_MAX_DELTA_RECORDS` (default 10_000)
- `CLARIUM_GRAPH_GC_TOMBSTONE_RATIO_PPM` (default 300_000 → 30%)
- `CLARIUM_GRAPH_GC_MAX_DELTA_AGE_MS` (reserved)

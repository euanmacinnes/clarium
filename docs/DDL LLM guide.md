### Clarium DDL Syntax — Guide for LLM Assistants

This guide describes the full set of Data Definition Language (DDL) statements supported by Clarium, with precise syntax, parameter semantics, constraints, normalization rules, and examples. Use this as the reference for generating correct DDL.

Notes
- Keywords are case-insensitive. Identifiers and string literals are case-sensitive unless quoted/explicitly specified.
- Paths can be provided with either `/` separators (`db/schema/table`) or dots (`db.schema.table`) where noted. Internally Clarium normalizes to `/`.
- Session defaults: Unqualified names are resolved using the current session’s default `database` and `schema` (see system/session configuration). The engine applies `crate::ident::normalize_identifier` and qualification helpers at execution time.
- “Regular tables” vs “time tables”: time tables must end with `.time` and have dedicated DDL.
- Error handling: DDL commands validate inputs and return structured errors; they do not panic. Use `IF EXISTS`/`IF NOT EXISTS` where supported to avoid errors when absent/present.

Identifier and path rules
- Qualified names: `database/schema/object` is preferred. Dotted form `database.schema.object` is accepted for some commands and normalized.
- View name uniqueness: A regular table cannot be created if a view with the same base name exists in the same path.
- `.time` suffix is mandatory for time tables in CREATE/DROP/RENAME TIME TABLE.

---

### CREATE statements

1) CREATE DATABASE
Syntax
```
CREATE DATABASE <database_name>
```
Semantics
- Creates a top-level database directory if missing.

2) CREATE SCHEMA
Syntax
```
CREATE SCHEMA <database>/<schema>
CREATE SCHEMA <schema>                  -- uses current database default
```
Semantics
- Creates a schema directory within a database. Names are normalized and qualified using session defaults if unqualified.

3) CREATE STORE
Syntax
```
CREATE STORE <database>.store.<store_name>
```
Semantics
- Ensures the on-disk store directory and default config exist for `<store_name>` under `/<database>/stores/`.

4) CREATE TIME TABLE
Syntax
```
CREATE TIME TABLE <database>/<schema>/<table>.time
```
Constraints
- Target must end with `.time`.

5) CREATE TABLE (simple path + options)
Syntax
```
CREATE TABLE <database>/<schema>/<table>
  [PRIMARY KEY (<col1>[, <col2> ...])]
  [PARTITION BY (<col1>[, <col2> ...])]
```
Semantics
- Creates a regular (non-time) table directory and a `schema.json`. If `PRIMARY KEY` or `PARTITION BY` are provided, they are stored in table metadata.
Constraints
- Must NOT end with `.time` (use CREATE TIME TABLE instead).
- Fails if a view with the same name exists in the same path.

6) CREATE TABLE (pgwire/SQL column definition form)
Syntax
```
-- Accepted via SQL clients over pgwire
CREATE TABLE [IF NOT EXISTS] <db>/<schema>/<table> (
  <col_name> <sql_type>[, ...]
  -- Table-level constraints permitted in input are parsed leniently and mostly ignored,
  -- except detection of a PRIMARY KEY marker which is stored.
);
```
Type mapping
- String-like: `char`, `varchar`, `text`, `json`, `bool` → stored as `string` in `schema.json`.
- Integers: any `int*` → `int64`.
- Floats/decimal: `double`, `real`, `float`, `numeric`, `decimal` → `float64`.
- Date/time types: mapped to `int64` (epoch representation for storage).
Notes
- Column named `_time` is skipped from schema emission.
- If the column list contains `PRIMARY KEY`, the table metadata records a PRIMARY marker.

7) CREATE VIEW / CREATE OR ALTER VIEW
Syntax
```
CREATE VIEW <view_name> AS <SELECT ...>
CREATE OR ALTER VIEW <view_name> AS <SELECT ...>
```
Semantics
- Stores the view definition SQL. `OR ALTER` updates an existing view or creates if missing.
Constraints
- The select text after `AS` is taken verbatim; may contain `UNION` etc.

8) CREATE SCRIPT
Syntax
```
CREATE SCRIPT <path> AS '<code>'
```
Semantics
- Registers a script (e.g., Lua). Code must be provided; single quotes around code are accepted and stripped.

9) CREATE VECTOR INDEX
Syntax
```
CREATE VECTOR INDEX <index_name>
ON <table_path>(<column_name>)
USING <algo>
[WITH (<opt_key>=<opt_val>[, ...])]
```
Semantics
- Declares a vector index over a vector column. Algorithm `<algo>` (e.g., `hnsw`) is required.
- `WITH` options are parsed as `k=v` pairs; values may be quoted with single quotes.
Constraints
- `<table_path>` can be qualified (`db/schema/table`).
Common options
- `m` (graph degree), `ef_construction`, `ef_search`, `distance` (e.g., `'cosine' | 'l2' | 'dot'`).
Notes
- Names are normalized. Column should store fixed‑length numeric vectors per row. Engine validates presence at query/index build time.

10) CREATE GRAPH
Syntax
```
CREATE GRAPH <name>
  NODES (
    <Label1> KEY(<key_col1>),
    <Label2> KEY(<key_col2>)
  )
  EDGES (
    <EdgeType1> FROM <FromLabel> TO <ToLabel>,
    <EdgeType2> FROM <FromLabel> TO <ToLabel>
  )
  [USING TABLES (nodes=<nodes_table_path>, edges=<edges_table_path>)]
  [USING GRAPHSTORE [CONFIG <config_name>] [WITH (<opt_key>=<opt_val>[, ...])]]
```
Semantics
- Defines a graph schema: node labels with their key columns and edge types with endpoints.
- Optionally binds existing tables for nodes/edges storage via `USING TABLES`.
- Alternatively, select the GraphStore engine and seed its configuration with `USING GRAPHSTORE`. When this clause is present, the engine writes the logical `.graph` definition and initializes an empty GraphStore manifest under `<db>/<schema>/<name>.gstore/meta/manifest.json`.
Constraints
- The `NODES(...)` and `EDGES(...)` blocks must be comma-separated entries following the formats `Label KEY(col)` and `Type FROM A TO B` respectively.
Resolution and storage
- If `USING TABLES` is omitted, the engine uses default physical locations under the current database/schema.
- When provided, `nodes=` and `edges=` should be fully qualified paths of existing tables that hold graph data.
- If `USING GRAPHSTORE` is specified, the GraphStore directory `<name>.gstore` is created (if needed) with a minimal `manifest.json` marking `engine: "graphstore"` and any provided options.
Validation
- Labels and types are parsed but not deeply validated against data until query time.
- Keys are column names on the bound nodes table; edge endpoints must reference existing labels.

GraphStore engine variant (manifest-backed)
Overview
- Clarium supports two backing implementations for graphs:
  - Table-backed graphs, where edges/nodes are stored in regular tables (the default implied by `USING TABLES` or by omission).
  - GraphStore-backed graphs, a high-performance, partitioned on-disk format under a graph directory with a manifest. This variant can be explicitly selected at DDL time with `USING GRAPHSTORE`, which seeds the graph’s manifest.

How GraphStore is detected/selected
- The planner/executor reads the graph definition file (`<db>/<schema>/<name>.graph`) and, if present, the GraphStore manifest at `<db>/<schema>/<name>.gstore/meta/manifest.json`.
- If the manifest declares `engine: "graphstore"`, traversal TVFs delegate to the GraphStore runtime; otherwise, they fall back to the table-backed implementation.
- Precedence: explicit manifest `engine` → GraphStore; otherwise table-backed.

Storage layout and metadata
- Directory structure:
  - `<db>/<schema>/<name>.gstore/meta/manifest.json` — manifest file
  - `<db>/<schema>/<name>.gstore/parts/*` — implementation-specific partition/segment files
- Manifest fields (representative — subject to evolution):
  - `engine: "graphstore"`
  - `partitions: <u32>` — number of physical partitions
  - `options: { k: v }` — engine options (e.g., GC thresholds, commit windows)
  - Logical schema is still described in the `.graph` file (`nodes` with keys; `edges` with `from`/`to`; optional `cost_column`, `time_column`).

DDL and lifecycle
- `CREATE GRAPH` defines the logical graph schema (labels, edge types). It does not itself write GraphStore data.
- With `USING GRAPHSTORE`, an empty GraphStore manifest is created so the runtime immediately recognizes the engine; population/build of actual GraphStore data is handled by ingestion/build tools that append segments and rotate the manifest.
- You may also configure a `STORE` (see `CREATE STORE`) that points to the filesystem/root used by GraphStore. The runtime resolves paths using the active store.

Query behavior and compatibility
- `MATCH`, `graph_neighbors`, and `graph_paths` work the same against GraphStore-backed graphs. The executor auto-detects the engine and uses the optimal runtime.
- Column schemas of TVFs are consistent:
  - `graph_neighbors`: `node_id`, `prev_id`, `hop`
  - `graph_paths`: `path_id`, `node_id`, `prev_id`, `hop`
- Time-bounded traversals are supported when `time_column` is set in the graph definition; pass `time_start`/`time_end` as extra TVF arguments as supported by the engine.

Operations for GraphStore
- SHOW current status (from the GraphStore runtime):
  ```
  SHOW GRAPH STATUS <db>/<schema>/<name>;
  -- or, with a session default graph set (see USE GRAPH), simply:
  SHOW GRAPH STATUS;
  ```
  Returns engine/manifest state including partitions, epoch, GC backlog, and runtime metrics.
- Trigger compaction/garbage collection:
  ```
  GC GRAPH <db>/<schema>/<name>;
  -- or, with a session default graph:
  GC GRAPH;
  ```
  GC thresholds can be tuned via environment variables (examples):
  - `CLARIUM_GRAPH_GC_MAX_DELTA_RECORDS`
  - `CLARIUM_GRAPH_GC_TOMBSTONE_RATIO_PPM`

Session default and resolution
- You can set a default graph for the session to avoid repeating `USING GRAPH` in `MATCH` or passing the graph name to TVFs:
  ```
  USE GRAPH db/schema/graph;
  MATCH (s:User { key: 'a' })-[:Calls*1..2]->(t:User)
  RETURN t.key, hop;
  ```
  The default applies equally to GraphStore-backed and table-backed graphs. An explicit `USING GRAPH` in a statement overrides the session default.

Examples
```sql
-- Logical definition (later materialized by GraphStore builder)
CREATE GRAPH public/know
  NODES (User KEY(id))
  EDGES (Calls FROM User TO User);

-- GraphStore-backed definition with inline options
CREATE GRAPH public/know
  NODES (User KEY(id))
  EDGES (Calls FROM User TO User)
  USING GRAPHSTORE WITH (partitions=4, gc_window='10m');

-- After GraphStore build creates public/know.gstore with engine: "graphstore",
-- MATCH and TVFs automatically use the GraphStore runtime
USE GRAPH clarium/public/know;
MATCH (s:User { key: 'planner' })-[:Calls*1..2]->(t:User)
RETURN t.key, prev.key, hop
ORDER BY hop, t.key;

-- Status and GC
SHOW GRAPH STATUS;   -- session default
GC GRAPH;            -- compaction over the GraphStore files
```

---

### DROP statements

1) DROP DATABASE
```
DROP DATABASE <database_name>
```

2) DROP SCHEMA
```
DROP SCHEMA <database>/<schema>
```

3) DROP STORE
```
DROP STORE <database>.store.<store_name>
```

4) DROP TIME TABLE
```
DROP TIME TABLE <database>/<schema>/<table>.time
```
Constraints: Target must end with `.time`.

5) DROP TABLE
```
DROP TABLE [IF EXISTS] <database>/<schema>/<table>
```
Constraints
- Must NOT end with `.time` (use DROP TIME TABLE instead).
- With `IF EXISTS`, succeeds without error if the table is absent.

6) DROP VIEW
```
DROP VIEW [IF EXISTS] <view_name>
```

7) DROP VECTOR INDEX
```
DROP VECTOR INDEX <index_name>
```

8) DROP GRAPH
```
DROP GRAPH <name>
```

9) DROP SCRIPT
```
DROP SCRIPT <path>
```

---

### RENAME statements

1) RENAME DATABASE
```
RENAME DATABASE <old_name> TO <new_name>
```

2) RENAME SCHEMA
```
RENAME SCHEMA <database>/<old_schema> TO <new_schema>
```

3) RENAME TIME TABLE
```
RENAME TIME TABLE <database>/<schema>/<old>.time TO <database>/<schema>/<new>.time
```
Constraints: Both names must end with `.time`.

4) RENAME TABLE
```
RENAME TABLE <database>/<schema>/<old_table> TO <database>/<schema>/<new_table>
```
Constraints: Regular tables only; `.time` not allowed here.

5) RENAME STORE
```
RENAME STORE <database>.store.<old> TO <new>
```

6) RENAME SCRIPT
```
RENAME SCRIPT <old_path> TO <new_path>
```

---

### Qualification, normalization, and defaults

- Normalization: Unquoted identifiers may be case-normalized by `normalize_identifier`; dotted paths may be converted to slashes.
- Qualification: When a table name is not fully qualified, the engine qualifies it using the session’s current database and schema.
- Filesystem mapping: Qualified paths map to directories under the configured root. For example, `clarium/public/metrics` → `<root>/clarium/public/metrics/`.

---

### Constraints and validations (summary)

- `.time` suffix: required in TIME TABLE DDL; disallowed in regular TABLE DDL and RENAME TABLE.
- View/table name conflict: CREATE TABLE fails if a `.view` with the same base path exists.
- DROP TABLE `IF EXISTS`: supported; silently succeeds when missing. `IF NOT EXISTS` is supported only in the pgwire column-definition variant of CREATE TABLE.
- Vector index: requires `USING <algo>`; options parsed from `WITH (k=v, ...)`.
- Graph: requires both `NODES (...)` and `EDGES (...)` blocks; optional `USING TABLES (...)`.

---

### Examples

Databases and schemas
```
CREATE DATABASE clarium;
CREATE SCHEMA clarium/public;
```

Regular and time tables
```
CREATE TABLE clarium/public/metrics PRIMARY KEY (id) PARTITION BY (bucket);
CREATE TIME TABLE clarium/public/events.time;
```

CREATE TABLE via pgwire (SQL clients)
```
CREATE TABLE IF NOT EXISTS clarium/public/metrics (
  id BIGINT,
  label TEXT,
  score DOUBLE PRECISION,
  created_at TIMESTAMP,
  PRIMARY KEY (id)
);
```

Views
```
CREATE VIEW top_scores AS SELECT id, label, score FROM clarium/public/metrics ORDER BY score DESC;
CREATE OR ALTER VIEW top_scores AS SELECT id, label, score FROM clarium/public/metrics WHERE score > 0.9;
```

Scripts
```
CREATE SCRIPT util/clean AS 'function clean(x) return x end';
RENAME SCRIPT util/clean TO util/cleanup;
DROP SCRIPT util/cleanup;
```

Vector index
```
CREATE VECTOR INDEX ix_vec ON clarium/public/items(embedding)
USING hnsw WITH (m=16, ef_construction=200, distance='cosine');
DROP VECTOR INDEX ix_vec;
```

Graph
```
CREATE GRAPH social
  NODES (User KEY(id), Post KEY(id))
  EDGES (AUTHORED FROM User TO Post)
  USING TABLES (nodes=clarium/public/graph_nodes, edges=clarium/public/graph_edges);

DROP GRAPH social;
```

Rename and drop
```
RENAME TABLE clarium/public/metrics TO clarium/public/metrics_v2;
RENAME TIME TABLE clarium/public/events.time TO clarium/public/events_archive.time;
DROP TABLE IF EXISTS clarium/public/unused;
```

---

### LLM prompting tips

- Always emit fully qualified paths for reliability: `database/schema/name`.
- Use `.time` suffix only with TIME TABLE DDL.
- Prefer `IF EXISTS`/`IF NOT EXISTS` where available to make operations idempotent for scripts.
- For vector indexes, include `USING <algo>` and consider common `WITH` options like `m`, `ef_construction`, and `distance`.
- For CREATE TABLE via pgwire, supply a reasonable SQL type per column; Clarium will map to internal storage types.

---

### SELECT and Query Syntax — Guide for LLM Assistants

This section documents the full SELECT/query surface supported by Clarium. It is derived from the query parser and staged executor under `src/server/query/query_parse_select*.rs` and `src/server/exec/select_stages/`. It includes CTEs, joins, grouping, slicing windows, ordering, limits, unions, and INSERT‑like `SELECT ... INTO` modes.

General notes
- Keywords are case-insensitive. Identifiers are normalized similarly to DDL (see earlier rules).
- Unqualified table names are resolved using current session defaults for database and schema.
- SELECT without FROM (sourceless) is supported for scalar expressions, useful for testing or constants.
- DISTINCT is supported at the SELECT level and with UNION (distinct vs ALL). OFFSET is not currently supported; use LIMIT with ORDER BY and/or WHERE for pagination strategies.

Top-level SELECT (with optional CTE)
Syntax
```
[WITH <cte_name> AS (<subquery>)[, <cte_name> AS (<subquery>) ...]]
SELECT [DISTINCT] <select_list>
[FROM <from_source> [<joins> ...]]
[BY <window_spec> | ROLLING BY <window_spec> | BY SLICE(<slice_plan>)]
[GROUP BY <group_list>]
[WHERE <predicate>]
[HAVING <predicate>]
[ORDER BY <order_list>]
[LIMIT <n>]
[INTO <table_path> [APPEND|REPLACE]]

-- Set operations
[UNION [ALL] <second_select> [UNION [ALL] <third_select> ...]]
```

Components
- CTEs: `WITH name AS (SELECT ...) [, ...] SELECT ...`. Multiple CTEs are allowed; each `AS` block must be parenthesized. CTE names can be referenced in FROM.
- Sourceless SELECT: `SELECT 1`, `SELECT 'ok' AS status`, `SELECT 2 + 2 AS four`.
- Set operations: `UNION` (distinct) and `UNION ALL` at the top level (not inside parentheses unless the subquery itself is parenthesized). The parser splits on top-level UNION/ALL respecting parentheses and quotes.
- INTO modes: `SELECT ... INTO <table> [APPEND|REPLACE]` materializes results into a table. If the mode is omitted, implementation default applies. Modes:
  - APPEND: add rows to existing destination.
  - REPLACE: replace existing data (semantics defined by executor).

Select list
Syntax
```
<select_list> := <item>[, <item> ...]
<item> :=
    *
  | <expression>
  | <expression> AS <alias>
```
Notes
- `*` selects all columns from the resolved source(s) (after joins/grouping rules).
- Expressions can include arithmetic, function calls, case/conditional constructs, and subqueries where supported.
- Aliases: `AS alias` or bare alias immediately after the expression.
- Aggregates supported include at least: `COUNT(*)`, `COUNT(col)`, `AVG(col)`, `SUM(col)`, `MIN(col)`, `MAX(col)`; see group-by stage. Use explicit aliases for clarity.

FROM and table sources
Syntax
```
FROM <table_or_subquery> [<alias> | AS <alias>]

<table_or_subquery> :=
    <table_path>
  | (<subquery>)
  | <cte_name>
```
Notes
- `<table_path>` prefers `db/schema/table`; dotted is normalized where supported.
- Subqueries in parentheses are supported as FROM sources and can be aliased.
- CTEs defined in WITH can be referenced as tables.
- Time tables (`.time`) can be used where applicable; windowing (BY/ROLLING/BY SLICE) operates on `_time` semantics where required.

JOINs
Syntax
```
<joins> :=
  [INNER] JOIN <right_source> [AS <alias>|<alias>] ON <predicate>
| LEFT [OUTER] JOIN <right_source> [AS <alias>|<alias>] ON <predicate>
| RIGHT [OUTER] JOIN <right_source> [AS <alias>|<alias>] ON <predicate>
| FULL [OUTER] JOIN <right_source> [AS <alias>|<alias>] ON <predicate>
```
Notes
- Join type keywords are optional for INNER (`JOIN ... ON ...`).
- `OUTER` keyword is accepted for LEFT/RIGHT/FULL but not required.
- An alias for the right source can be given with or without `AS`.
- `ON` predicate is required and parsed until the next `JOIN` or a global clause (`WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`).

WHERE and HAVING
Syntax
```
WHERE <predicate>
HAVING <predicate>
```
Notes
- Predicates support standard comparison and boolean operators. Scalar subqueries such as `EXISTS (SELECT ...)` are supported (see tests `exists_tests.rs`).
- `WHERE` filters rows before grouping. `HAVING` filters post-aggregation groups.

Grouping: GROUP BY vs BY vs ROLLING BY
Clarium supports two grouping paradigms, which are mutually exclusive:
1) GROUP BY — value-based grouping
Syntax
```
GROUP BY <col_or_expr>[ [NOTNULL]] [, ...]
```
Notes
- Columns/expressions listed determine group keys.
- Optional `NOTNULL` modifier can be applied per column token to force dropping null groups for that column; parsed into `group_by_notnull_cols`.
- Use aggregates in the select list with GROUP BY.

2) BY — time/window-based bucketing
Two primary forms are recognized by the parser and executor:
- Fixed windows: `BY <window_spec>` or `ROLLING BY <window_spec>`
  - `window_spec` parses common forms like `10s`, `5m`, `1h`, etc., mapping to milliseconds; see `parse_window` in `query_parse_misc.rs`.
  - `BY <window>` produces windowed aggregations over `_time` buckets.
  - `ROLLING BY <window>` uses a rolling window semantics over time.
- BY SLICE — manual or table-driven slices
  - Syntax (high-level):
    ```
    BY SLICE(
      USING [LABELS(name[, ...])]
        (<start>, <end>[, <label_assignments>])
        [UNION|INTERSECT (<start>, <end>[, <label_assignments>]) ...]
      [LABEL(<values>)]
      [UNION SLICE( ... ) | INTERSECT SLICE( ... )] ...
    )
    ```
  - Slices may be specified directly as ranges, composed with `UNION`/`INTERSECT`, and can carry labels with `LABELS(...)` and `LABEL(...)` forms. Nested `SLICE(...)` plans are supported and can be combined.
  - See parser `query_parse_slice.rs` and executor `exec_slice.rs` for exact operators and labeling. Tests under `exec/tests.rs` and `raw_tests.rs` exercise these forms.

Important constraint: BY/ROLLING BY/BY SLICE cannot be used together with GROUP BY in the same query.

ORDER BY
Syntax
```
ORDER BY <expr> [ASC|DESC][, <expr> [ASC|DESC] ...]
```
Notes
- Expressions are accepted; ASC/DESC per item. Implementation uses stable options. For complex expressions, the raw text is preserved downstream.
- When sorting with limits, the engine uses explicit options compatible with Polars 0.51+ (see codebase for `SortMultipleOptions` and `IdxSize`).

LIMIT
Syntax
```
LIMIT <n>
```
Notes
- Positive integer limit. Duplicate LIMITs are rejected. OFFSET is not currently supported.

UNION and UNION ALL
Syntax
```
<select> UNION <select>
<select> UNION ALL <select>
```
Notes
- `UNION` removes duplicates; `UNION ALL` retains all rows. The parser splits at the top level only.

DISTINCT
Syntax
```
SELECT DISTINCT <select_list> [FROM ...]
```
Notes
- Distinct applies to the full row of the select list after projection. See `exists_tests.rs` and `union_select_tests.rs` for examples.

SELECT ... INTO
Syntax
```
SELECT ... INTO <db>/<schema>/<table> [APPEND|REPLACE]
```
Notes
- Materializes the query result into a table. `APPEND` adds rows; `REPLACE` overwrites. Destination qualification follows the same normalization/qualification rules as DDL. If a view with the same name exists, usual name-conflict rules apply at execution.

Examples
CTE with join and aggregation
```
WITH recent AS (
  SELECT id, score, _time FROM clarium/public/metrics WHERE _time > 1700000000
)
SELECT m.id, AVG(m.score) AS avg_score
FROM recent m
LEFT JOIN clarium/public/metrics mm ON mm.id = m.id
GROUP BY m.id
HAVING AVG(m.score) > 0.8
ORDER BY avg_score DESC
LIMIT 100;
```

Sourceless and scalar
```
SELECT 1 AS one;
SELECT 'ok' AS status, 2 + 2 AS four;
```

Windowed aggregation with BY
```
SELECT COUNT(*) AS c, AVG(value) AS av
FROM clarium/public/events.time
BY 5m
ORDER BY c DESC
LIMIT 10;
```

Rolling window
```
SELECT MAX(v) AS peak
FROM clarium/public/sensors.time
ROLLING BY 30s;
```

BY SLICE with labels and composition
```
SELECT AVG(v) AS av, COUNT(v) AS cnt
FROM clarium/public/telemetry.time
BY SLICE(
  USING LABELS(region)
    (1700s, 2100s, region:='EU')
    UNION (2200s, 2600s, region:='US')
)
ORDER BY av DESC;
```

UNION and DISTINCT
```
SELECT DISTINCT category
FROM clarium/public/items i1
WHERE EXISTS (
  SELECT 1 FROM clarium/public/items i2
  WHERE i2.category = i1.category AND i2.score > 0.9
)
UNION
SELECT DISTINCT category FROM clarium/public/archive;
```

SELECT INTO
```
SELECT id, label, score
FROM clarium/public/metrics
WHERE score >= 0.95
ORDER BY score DESC
LIMIT 100
INTO clarium/public/top_metrics REPLACE;
```

Implementation references
- Parser: `src/server/query/query_parse_select.rs`, `query_parse_select_list.rs`, `query_parse_slice.rs`, `query_parse_misc.rs` (window parsing).
- Executor stages: `src/server/exec/select_stages/` including `order_limit.rs` and `by_or_groupby.rs`; slice execution in `exec_slice.rs`.
- Tests exercising SELECT: `src/server/exec/tests/union_select_tests.rs`, `exists_tests.rs`, `raw_tests.rs`, `end_to_end_planning_tests.rs`, and others under `exec/tests`.

---

### MATCH — Graph pattern queries

MATCH provides a concise graph pattern syntax that rewrites to supported graph table‑valued functions (TVFs). It can be used as a standalone statement, inside CTEs, or as a FROM source in SELECT.

Syntax
```
-- Neighborhood expansion (upper bound hops)
MATCH [USING GRAPH <graph_name>]
  (s:StartLabel { key: <start_expr> })-[:<EdgeType>*<L>[.. <U>]]->(t:EndLabel)
  [WHERE <predicate>]
  RETURN <projection_list>
  [ORDER BY <expr_list>]
  [LIMIT <n>]

-- Shortest paths (requires destination key)
MATCH SHORTEST [USING GRAPH <graph_name>]
  (s:StartLabel { key: <start_expr> })-[:<EdgeType>*<L>[.. <U>]]->(t:EndLabel { key: <dst_expr> })
  RETURN <projection_list>
  [ORDER BY <expr_list>]
  [LIMIT <n>]
```

Semantics
- USING GRAPH is optional; when omitted, the session’s current graph is used (if configured). See `crate::system::get_current_graph_opt()`.
- Pattern focuses on: start node key expression, edge type, and hop bounds. Labels are accepted for readability but not deeply validated at parse time.
- Variables and columns in `RETURN`/`WHERE`/`ORDER BY` map as:
  - `t.key` → `node_id`
  - `s.key` → the literal start key value
  - `prev.key` → `prev_id` (the previous hop’s node id)
- Rewrites:
  - `MATCH ... RETURN ...` → `SELECT <projection> FROM graph_neighbors(<graph>, <start>, <etype>, <U>) g [WHERE ...] [ORDER BY ...] [LIMIT ...]`
  - `MATCH SHORTEST ...` → `SELECT <projection> FROM graph_paths(<graph>, <start>, <dst>, <U>, <etype>) g ...`

Graph TVFs (used by MATCH and usable directly in FROM)
```
-- Columns: node_id (string), prev_id (string or null), hop (int)
graph_neighbors(graph, start, etype, max_hops[, time_start, time_end])

-- Columns: path_id (int), node_id (string), prev_id (string or null), hop (int)
graph_paths(graph, start, dst, max_hops, etype[, time_start, time_end])
```
Notes
- Both TVFs can be referenced directly in FROM and participate in joins, filters, ordering, and limits.
- Time‑bounded variants accept optional `time_start`/`time_end` arguments when the underlying graph is backed by time tables.

Using MATCH inside SELECT (CTE and FROM)
```
-- As a CTE
WITH hops AS (
  MATCH USING GRAPH clarium/public/know
    (s:User { key: 'planner' })-[:Calls*1..2]->(t:User)
    RETURN t.key, prev.key, hop
)
SELECT t.key AS node, hop
FROM hops
WHERE hop = 2
ORDER BY node;

-- Directly in FROM (parenthesized subquery)
SELECT g.node_id, g.prev_id, g.hop
FROM (
  MATCH (s:User { key: 'planner' })-[:Calls*1..2]->(t:User)
  RETURN t.key, prev.key, hop
) AS g
WHERE g.hop >= 1
ORDER BY g.hop, g.node_id;

-- Join MATCH results to a table
WITH neigh AS (
  MATCH (s:User { key: 'planner' })-[:Calls*2]->(t:User)
  RETURN t.key, hop
)
SELECT u.id, u.label, n.hop
FROM clarium/public/users u
JOIN neigh n ON n."t.key" = u.id;
```

Standalone MATCH examples
```
-- Neighborhood exploration
MATCH USING GRAPH clarium/public/know
  (s:User { key: 'planner' })-[:Calls*1..2]->(t:User)
  RETURN t.key, prev.key, hop
  ORDER BY hop, t.key;

-- Shortest path (requires destination key)
MATCH SHORTEST USING GRAPH clarium/public/know
  (s:User { key: 'planner' })-[:Calls*1..3]->(t:User { key: 'target' })
  RETURN t.key, prev.key, hop
  LIMIT 100;
```

Implementation references
- Parser/rewriter: `src/server/query/query_parse_match.rs` (rewrites to SELECT over TVFs).
- TVF execution: `src/server/exec/exec_graph_runtime.rs` (`graph_neighbors`/`graph_paths`). When the manifest declares `engine: "graphstore"`, execution delegates to the GraphStore runtime under `src/server/graphstore`.
- Operational APIs and tests: `src/server/graphstore.rs`, `src/server/graphstore/**`, and tests `graphstore_neighbors_tests.rs`, `graphstore_gc_tests.rs`.
- MATCH/TVF rewrite tests: `src/server/exec/tests/graph_tvf_neighbors_tests.rs`, `match_rewrite_tests.rs`, and usage in `end_to_end_planning_tests.rs`.

Getting Started
===============

This quick start shows how to create a database, ingest some rows, query them,
and try a few distinctive features (time-series tables, views, UDFs, vectors, graphs, filestore).

Prerequisites
-------------
- Rust toolchain (for building from source) or use the provided Dockerfile.
- Clarium binary or server build.

Create a workspace
------------------
We'll use a temporary folder for storage in examples below. The logical object
paths use the form `database/schema/table` and time-series tables end with
`.time`.

Create a time-series table and insert data
------------------------------------------
```
-- Create a time table
CREATE TIME TABLE clarium/public/sensors.time;

-- Insert a few rows via HTTP or the embedded API; using SQL SELECT ... INTO is also supported (see below)
-- Each row must have a `_time` (epoch milliseconds) and any number of fields.
```

Query the table
---------------
```
SELECT _time, temperature, humidity
FROM clarium/public/sensors.time
WHERE temperature > 20
ORDER BY _time
LIMIT 10;
```

Set session defaults and use unqualified names
----------------------------------------------
```
USE DATABASE clarium;
USE SCHEMA public;

SELECT COUNT(*) AS n FROM sensors.time;
```

Create and use a view
---------------------
```
CREATE VIEW hot_reads AS
  SELECT _time, temperature
  FROM sensors.time
  WHERE temperature >= 30;

-- Query the view like a table
SELECT * FROM hot_reads ORDER BY _time LIMIT 5;

-- See the stored definition
SHOW VIEW hot_reads;
-- Or via pg function
SELECT pg_get_viewdef(oid)
FROM pg_catalog.pg_class
WHERE relkind='v' AND relname='hot_reads';
```

Use windows and rolling analytics
---------------------------------
```
-- Fixed-duration windowing with BY
SELECT AVG(temperature) AS avg_temp
FROM sensors.time
BY 5m
ORDER BY _time;

-- Rolling window
SELECT MAX(temperature) AS max15
FROM sensors.time
ROLLING 15m
ORDER BY _time;
```

Write query results back to storage
-----------------------------------
```
-- Into a regular table
SELECT device_id, AVG(temperature) AS avg_temp
FROM sensors.time
GROUP BY device_id
INTO clarium/public/device_avgs REPLACE;

-- Into a time table (must project exactly one _time column)
SELECT _time, temperature AS temp_c
FROM sensors.time
INTO clarium/public/cleaned.time APPEND;
```

Register and call a Lua UDF (scalar)
------------------------------------
```
-- Create a scalar UDF `is_pos(x)` that returns bool
CREATE SCRIPT udf/is_pos AS 'function is_pos(x) if x==nil then return false end return x>0 end';

-- Tell the engine about return types (e.g., through API or configuration)
-- Then use in SQL
SELECT _time, temperature, is_pos(temperature) AS is_hot
FROM sensors.time
LIMIT 3;
```

Get started with vectors (embeddings, ANN search)
-------------------------------------------------
```
-- Create a simple documents table with a vector column (embedding)
CREATE TABLE clarium/public/docs (
  id TEXT PRIMARY KEY,
  title TEXT,
  body_embed VECTOR(384)
);

-- Insert a few rows; you can pass vectors as JSON-like literals via to_vec(...)
INSERT INTO clarium/public/docs (id, title, body_embed) VALUES
  ('d1','Intro', to_vec('[0.10, 0.00, 0.05, 0.01]')),
  ('d2','Guide', to_vec('[0.09, 0.02, 0.06, 0.00]')),
  ('d3','Reference', to_vec('[0.50, 0.40, 0.20, 0.10]'));

-- Build an HNSW vector index on the embedding column
CREATE VECTOR INDEX idx_docs_embed
ON docs(body_embed)
USING hnsw WITH (
  metric='cosine',
  dim=384
);

-- Search using ORDER BY ... USING ANN (top‑k)
WITH q AS (SELECT to_vec('[0.09, 0.01, 0.05, 0.00]') AS v)
SELECT id,
       cosine_sim(body_embed, (SELECT v FROM q)) AS score
FROM docs
ORDER BY vec_l2(body_embed, (SELECT v FROM q)) USING ANN
LIMIT 3;

-- Or use the TVF and join back by row id for rich selections
WITH q AS (SELECT to_vec('[0.09, 0.01, 0.05, 0.00]') AS v)
SELECT d.id, nn.score
FROM nearest_neighbors('public.docs','body_embed', (SELECT v FROM q), 3, 'cosine', 96) AS nn
JOIN docs AS d ON d.__row_id.docs = nn.row_id
ORDER BY nn.score DESC;

-- Inspect or drop the index
SHOW VECTOR INDEX idx_docs_embed;
SHOW VECTOR INDEXES;
-- DROP VECTOR INDEX idx_docs_embed;
```

Notes:
- For more depth (ANN vs EXACT, options, TVFs, diagnostics), see docs/vector-indexes.md.
- Vector column type uses the declared dimension for validation; `to_vec(text)` parses JSON-like arrays safely.

Get started with graphs (catalog + traversal TVFs)
-------------------------------------------------
```
-- Create backing tables (skip if you have existing ones)
CREATE TABLE clarium/public/know_nodes (
  id TEXT PRIMARY KEY,
  label TEXT,
  name TEXT
);

CREATE TABLE clarium/public/know_edges (
  src TEXT NOT NULL,
  dst TEXT NOT NULL,
  etype TEXT DEFAULT 'Calls'
);

-- Bind them into a named graph catalog entry
CREATE GRAPH know
NODES (Tool KEY(id))
EDGES (Calls FROM Tool TO Tool)
USING TABLES (nodes=clarium/public/know_nodes, edges=clarium/public/know_edges);

-- Seed a tiny graph
INSERT INTO clarium/public/know_nodes (id,label,name) VALUES
  ('planner','Tool','Planner'),
  ('executor','Tool','Executor');
INSERT INTO clarium/public/know_edges (src,dst,etype) VALUES ('planner','executor','Calls');

-- Traverse neighbors up to 2 hops
SELECT *
FROM graph_neighbors('clarium/public/know','planner','Calls',2) g
ORDER BY hop, node
LIMIT 10;

-- Introspect or drop
SHOW GRAPH know;
SHOW GRAPHS;
-- DROP GRAPH know;
```

Notes:
- Time-sliced traversals are supported when edge rows include a `_time` column; pass start/end timestamps as optional 5th/6th args.
- See docs/graph-catalog.md for full details and GraphStore options.

Get started with the filestore (blobs, versioned trees)
------------------------------------------------------
```
-- Create a filestore (defaults are fine for local use)
CREATE FILESTORE docs;

-- Ingest a small text file from raw bytes
INGEST FILESTORE FILE PATH 'handbook/intro.txt'
FROM BYTES '0x48656c6c6f2c20576f726c64210a'
CONTENT_TYPE 'text/plain';

-- List files (optionally filter and page)
SHOW FILES IN FILESTORE docs;
SHOW FILES IN FILESTORE docs LIKE 'handbook/' LIMIT 20 OFFSET 0;

-- Peek effective configuration
SHOW FILESTORE CONFIG docs;

-- Update content with If‑Match (first get the etag via SHOW FILES)
UPDATE FILESTORE FILE PATH 'handbook/intro.txt'
IF_MATCH 'paste-current-etag-here'
FROM BYTES 'SGVsbG8sIENsYXJpdW0h\n'
CONTENT_TYPE 'text/plain';

-- Create a snapshot tree and commit it
CREATE TREE IN FILESTORE docs LIKE 'handbook/';
-- Use the returned TREE id in the COMMIT call below
COMMIT TREE IN FILESTORE TREE '<tree_uuid>'
  BRANCH 'main'
  AUTHOR_NAME 'Docs Bot'
  AUTHOR_EMAIL 'docs@example'
  MESSAGE 'Publish handbook v1';
```

Notes:
- Full walkthrough, admin/health views, and Git/LFS options live in docs/filestore/getting-started.md.

Next steps
----------
- Read concepts.md to understand objects and naming.
- See sql-reference.md for the query surface with examples.
- Explore time-series and views in depth in time-series.md and views.md.
- Explore vectors (indexes, TVFs, ANN vs EXACT) in docs/vector-indexes.md.
- Explore graphs (catalogs, traversal TVFs, GraphStore) in docs/graph-catalog.md.
- Explore filestore end-to-end in docs/filestore/getting-started.md.
- If you’re launching Clarium from the terminal, see cli.md for server and CLI usage with copy‑pasteable examples.
 - Contributors: see dev/junie-polars-guidelines.md for Polars coding guidelines (avoid deprecated APIs, version‑resilient patterns).

Testing the pgwire interface
----------------------------
Clarium includes an in-process PostgreSQL wire (pgwire) server used by integration tests and for local experimentation.

- TRUST auth for tests: set the environment variable `CLARIUM_PGWIRE_TRUST=1` so no password is required.
- Start an ephemeral pgwire server in tests by calling `start_pgwire(shared, "127.0.0.1:0")` or using the provided test helpers in `tests/`.
- Connect with any Postgres client/driver (e.g., `tokio-postgres`) to `127.0.0.1:<port>`.
- For verbose protocol tracing during development, set `CLARIUM_PGWIRE_TRACE=1`.

Type fidelity and binary formats
- The server announces accurate PostgreSQL OIDs in `RowDescription` (bool, int2/4/8, float4/8, text, bytea, date, time, timestamp, timestamptz, interval, arrays of common types, and record/composite).
- Result format codes from `Bind` are honored per-column. Binary encoding is implemented for common scalars (bool, int2, int4, int8, float4, float8, bytea) and temporal types (date, time, timestamp, timestamptz). Other complex types fall back to text payloads while still reporting accurate OIDs.
- Extended protocol supports both text and binary parameters for common scalar types; text parameters with explicit casts are recommended for portability.

More details for developers
- See `docs/dev/pgwire.md` for an in-depth description of supported OIDs, binary encodings (including interval, numeric, and arrays), and parameter/result format handling.

Running the tests
- From the workspace root you can run the pgwire test suites:
  - `cargo test --test pgwire_mock_tests`
  - `cargo test --test pgwire_ddl_from_exec_tests`
  - `cargo test --test pgwire_binary_manual_tests`
- On Windows, ensure no external process holds the built binaries to avoid file lock errors during rebuilds.

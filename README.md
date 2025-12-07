

A minimal Rust service for storing and querying time‑series data backed by Parquet. It exposes an HTTP+WebSocket API and an optional PostgreSQL wire (pgwire) endpoint, and ships with a small SQL‑like query language for aggregations. Storage is filesystem‑based using a three‑level layout: database/schema/table.time.

Full documentation
------------------
- See docs/README.md for the full docs set, including Getting Started, SQL reference, Time‑series, Views, UDFs, System catalogs, Storage format, Administration, Compatibility, and End‑to‑end Examples.
- Pgwire error handling: for contributor guidelines on graceful error handling over the PostgreSQL wire protocol, see docs/pgwire-error-handling.md.
 - Unified error handling across HTTP/WS/pgwire: see docs/error-handling.md for the common error model, mappings, and frontend behaviors.
 - Developer note (Polars): see docs/dev/junie-polars-guidelines.md for guidance on avoiding deprecated Polars APIs and writing version‑resilient DataFrame code.
 - Benchmarks: see docs/benchmarks.md for the complete Criterion‑based benchmarking suite (direct micro‑benchmarks and SQL‑driven end‑to‑end benches) and how to run them.

Quick start

- Prerequisites: Rust stable (1.75+), Internet access for crates
- Start the server (HTTP on 7878 by default):
  - cargo run --release --bin clarium_server
- Enable the pgwire endpoint (on 5433) by building with the feature and turning it on:
  - cargo run --release --features pgwire --bin clarium_server -- --pgwire
- Configure ports and the database root via flags or environment (flags win):
  - Flags: --http-port <N> [--pg-port <N>] [--db-folder <path>] [--pgwire|--no-pgwire]
  - Env: CLARIUM_HTTP_PORT, CLARIUM_PG_PORT, CLARIUM_DB_FOLDER, CLARIUM_PGWIRE
  - Examples (PowerShell):
    - cargo run --release --bin clarium_server -- --http-port 8080 --db-folder dbs
    - $env:CLARIUM_HTTP_PORT=8080; $env:CLARIUM_PG_PORT=6432; $env:CLARIUM_DB_FOLDER='dbs'; cargo run --release --features pgwire --bin clarium_server -- --pgwire

Benchmarks
----------

- The project ships with a comprehensive Criterion benchmarking suite covering regular tables, time series, graph traversal, vector search (flat vs ANN/HNSW), and key/value stores.
- See docs/benchmarks.md for details.
- Common commands (PowerShell):
  - Run a specific bench: `cargo bench --bench sql_tables`
  - Run vector ANN benches: `cargo bench --bench sql_vector`
  - Disable ANN to force exact path: `cargo bench --no-default-features --features pgwire --bench sql_vector`
  - Open `target/criterion/report/index.html` for HTML reports.

Python tests (SQLAlchemy)

- The repository includes simple Python tests that connect to the pgwire endpoint via SQLAlchemy, inspect schemas/tables, optionally create a table, insert rows, and run queries.
- Requirements are listed in tests\python\requirements.txt.
- Default connection URL is postgresql+psycopg://localhost:5433/clarium, overridable via CLARIUM_SQLA_URL.
- How to run:
  1. Create and activate a virtual environment, then install deps and run pytest:
     - python -m venv .venv
     - .venv\Scripts\Activate.ps1
     - pip install -r tests\python\requirements.txt
     - # Override the URL if needed (example for a custom port):
     - $env:CLARIUM_SQLA_URL = "postgresql+psycopg://localhost:6432/clarium"
     - pytest -q tests/python
  3. Notes:
     - Auto-start: If a server is not already listening on the URL’s host/port, pytest will auto-start `clarium_server` via `cargo run --release --features pgwire --bin clarium_server -- --pgwire`, wait until the pgwire port is ready, and then run tests. It will attempt to stop the server when the test session finishes.
     - Opt-out: set `CLARIUM_SKIP_AUTOSTART=1` to disable auto-start (use this if you prefer to run the server yourself).
     - Overrides (optional):
       - `CLARIUM_CARGO` — path to cargo (default `cargo`)
       - `CLARIUM_SERVER_BIN` — server bin name (default `clarium_server`)
       - `CLARIUM_ARGS` — extra CLI args appended after `--pgwire`
       - `CLARIUM_STARTUP_TIMEOUT` — seconds to wait for readiness (default 45)
     - If CREATE TABLE isn’t supported by your build/path, the DDL test will be marked xfail and the rest will still run.
     - On a fresh store, a demo table public.demo may exist; tests will query it when present.

First run behavior

- On a completely empty store, the server creates a demo table at clarium/public/demo.time with 1 week of per‑second data, a sine wave cycling every 2 hours.
- At startup the server prints an inventory of installed databases and schemas discovered under the db root.
- Default session context uses database "clarium" and schema "public".

Data layout

- Three‑level directory structure rooted at the configured db root (default: dbs):
  - <db_root>\<database>\<schema>\<table>.time\
  - Example: dbs\clarium\public\demo.time\
- Each table directory contains:
  - Parquet chunks named data-<min>-<max>-<ts>.parquet
  - schema.json describing non‑_time column dtypes ("string" | "int64" | "float64")
- _time is always epoch milliseconds (Int64) and is implicit (not stored in schema.json).
- Legacy note: if no chunk files exist but data.parquet is present, it will still be read.

Chunking strategy and mid‑clarium inserts

- What is a "chunk"?
  - Each call that writes a batch of records (HTTP /write or internal APIs) produces one new Parquet file inside the table directory.
  - The file name encodes the time span covered by that batch: data-<min_time>-<max_time>-<created_ts>.parquet.
  - Before writing, the batch is sorted by _time, so every individual chunk is internally time‑sorted.
- Append‑only persistence
  - Writes are append‑only: a new chunk is added; existing chunks are not modified in place.
  - The logical table schema (schema.json) is updated on each write with type inference and safe widening (Int64 → Float64 → String). _time is always Int64 and not stored in schema.json.
  - There is no WAL or transactional rollback; a successful write results in one additional Parquet file.
- Reading and time pruning
  - Readers enumerate all chunk files and sort them by their encoded min_time (ascending).
  - When a time filter is provided, the reader prunes per‑chunk by applying the min/max time predicate within each file to avoid loading irrelevant rows.
  - After per‑chunk filtering, chunks are vertically stacked. There is no final global re‑sort step across chunks.
- Inserts in the middle of the clarium (backfills)
  - Backfilling earlier data is supported. A backfill simply creates a new chunk whose <min_time>…<max_time> falls inside (or overlaps) the existing clarium.
  - Because stacking is performed by chunk min_time and there is no cross‑chunk re‑sort, the global row order across chunk boundaries is not strictly guaranteed when time ranges overlap. Within each chunk, rows remain sorted.
  - Duplicates are not automatically deduplicated. If you insert rows with the same _time multiple times, all copies will exist. Downstream queries should aggregate or deduplicate as needed.
  - Best practice for large historical inserts is to batch them into reasonably sized, non‑overlapping windows (for example, daily files) to improve scan efficiency and maintain near‑sorted chunk order.
- Compaction and reorganization
  - If you need a perfectly ordered single file (or to eliminate overlaps/duplicates), you can rewrite the table using the rewrite_database_df API, which removes existing chunks and writes one fresh chunk from a DataFrame.
  - External compaction strategies can also be implemented by reading a range, sorting/deduplicating externally, and rewriting. There is no background compactor built in.
- Impact on queries and aggregations
  - Aggregations (BY window or GROUP BY) do not require globally sorted input; they operate correctly with stacked chunks.
  - If you require sorted output for client display, request ordering at the client level (e.g., ORDER BY _time where supported) or sort the resulting data structure.
  - Time filters (WHERE _time BETWEEN …) are efficient due to per‑chunk pruning, but overlapping chunks may still be scanned if they intersect the filter window.

HTTP API (session + CSRF)

- Authenticate: POST /login with {"username":"...","password":"..."}. On success a session cookie is set.
- CSRF: GET /csrf returns {"csrf":"..."}. Send X-CSRF-Token on subsequent POSTs and WebSocket upgrades.
- Logout: POST /logout clears the session.
- Endpoints:
  - POST /write/{database}
    - Body: {"records": [{"_time": 1697040000000, "temp": 12.3, "status": "ok"}, ...]}
    - Appends a new Parquet chunk; schema is inferred and widened as needed.
  - POST /query
    - Body: {"query": "SELECT AVG(temp), _time FROM clarium/public/demo.time BY 1m"}
    - Returns JSON with status and rows.
  - POST /use/database {"name":"clarium"}
  - POST /use/schema {"name":"public"}
  - GET /ws (WebSocket). Send the query text, receive one JSON result per message.
- Production note: run behind HTTPS in production; cookies are HttpOnly. CSRF is required for state‑changing requests.

Query language (brief)

- SELECT <expr_list> FROM <db/schema/table[.time]> [BY <window> | GROUP BY <cols> | ROLLING BY <window>] [WHERE <expr>] [HAVING <expr>] [ORDER BY <col> [ASC|DESC], ...] [LIMIT <n>]
  - Aggregates: AVG, MAX, MIN, SUM, COUNT, FIRST, LAST, STDEV, QUANTILE(<cutoff>)
  - Window examples: 1s, 5m, 1h, 1d (BY buckets by floor(_time/window_ms))
  - WHERE supports comparisons, arithmetic, BETWEEN, and PREVIOUS.<col>
  - HAVING filters after aggregation (can reference aggregate names or aliases)
  - Wildcard projection: SELECT * expands to all columns when not using BY/GROUP BY/ROLLING. Works for time tables and regular (non-.time) tables. Mixing * with other projections is allowed; duplicates are de-duplicated. Using * together with BY/GROUP BY/ROLLING is rejected.
  - ORDER BY: optional, supports multiple columns with ASC/DESC. Columns must exist in the projection (aliases allowed). For time-series tables, ORDER BY is not required for LIMIT to return the first/last rows in natural time order.
    - BY SLICE: The BY clause can accept a full SLICE plan to define custom aggregation windows. Example: SELECT AVG(v) FROM clarium/public/demo.time BY SLICE( USING (2025-01-01T00:00:00Z, 2025-01-01T01:00:00Z) UNION (2025-01-01T03:00:00Z, 2025-01-01T04:00:00Z) ). Disallows mixing with GROUP BY; string functions in SELECT are not supported with BY SLICE. When labels are produced by the SLICE plan (via USING LABELS or auto-derived), those label columns are automatically surfaced in the SELECT result alongside _time and aggregates.
  - LIMIT: positive n returns the first n rows; zero returns an empty result; negative n returns the last n rows. LIMIT can be used without ORDER BY on time tables (natural order by _time).
  - Examples:
    - SELECT _time, v FROM clarium/public/demo.time WHERE _time BETWEEN 2025-01-01T00:00:00Z AND 2025-01-01T01:00:00Z LIMIT 10
    - SELECT * FROM clarium/public/demo.time WHERE _time BETWEEN 0 AND 1000
    - SELECT id, name FROM regular/ns/users ORDER BY name DESC LIMIT 5
    - SELECT _time, v FROM clarium/public/demo.time LIMIT -100  -- last 100 rows
- CALCULATE <new_name>, _time AS SELECT ...
  - Saves the first non‑_time value column from the SELECT into the same table under <new_name> (dtype inferred).

SELECT ... INTO (persist SELECT results)

- Syntax: append to a SELECT
  - SELECT ... FROM <src> [clauses...] INTO <dest_table> [APPEND|REPLACE]
- Destination kinds
  - Time tables: destination ends with .time. The SELECT output must contain _time.
  - Regular tables: destination without .time suffix.
- Modes
  - APPEND (default):
    - Time tables: converts rows to records and appends via the chunked writer (existing chunks are merged logically by time spans; rows within chunks remain sorted by _time).
    - Regular tables: reads existing destination (if any), vertical-stacks the new result, and rewrites the table.
  - REPLACE:
    - Regular tables only: completely rewrites the destination with the SELECT result.
- Auto-create: the destination table is created automatically if it doesn’t exist.
- Examples:
  - SELECT _time, a, b FROM clarium/public/sensors.time WHERE a > 0 INTO clarium/public/output.time
  - SELECT a, b FROM clarium/public/regular_src INTO clarium/public/regular_dest
  - SELECT a, b FROM clarium/public/regular_src INTO clarium/public/regular_dest REPLACE
- Notes:
  - For time-table INTO, the _time column must be present in the SELECT output.
  - For APPEND into regular tables, schemas should be compatible; otherwise stacking may fail. Schema-alignment/union can be added in future.

pgwire (optional PostgreSQL wire)

- Build with --features pgwire and run with --pgwire to enable a Postgres‑compatible port on 5433.
- Authentication is required (default admin is clarium/clarium). Use sslmode=disable for local testing.
- Current database and schema default to clarium/public. You can run CREATE TABLE and INSERT; SELECT streams results. All columns are returned as text for simplicity.
- System catalogs are emulated enough for common clients and SQLAlchemy to introspect metadata via information_schema/pg_catalog. SQLAlchemy can list schemas/tables/columns and can CREATE TABLE and INSERT via pgwire.
- Examples (psql):
  - psql "host=127.0.0.1 port=5433 dbname=clarium user=clarium sslmode=disable"
  - CREATE TABLE clarium.public.metrics (value float, label text);
  - INSERT INTO clarium.public.metrics (_time, value, label) VALUES (extract(epoch from now())*1000, 1.23, 'a');
  - SELECT COUNT(*) FROM clarium.public.metrics;

clarium_test_suite (server + pgwire connectivity helper)

- A convenience binary that starts the clarium server (HTTP + pgwire) and optionally runs a quick connectivity check over postgres:// using tokio-postgres.
- Default credentials are clarium/clarium; default database is clarium.
- Usage examples:
  - cargo run --bin clarium_test_suite -- --db-folder dbs --pg-port 5433 --http-port 7878
  - cargo run --bin clarium_test_suite -- --check --exit-after-check
  - cargo run --bin clarium_test_suite -- --pg-port 5440 --http-port 7888 --check --sql "SELECT COUNT(_time) FROM clarium/public/demo.time"
- On startup it prints a DSN you can paste into psql/psycopg/SQLAlchemy, e.g.:
  - postgres://clarium:clarium@127.0.0.1:5433/clarium?application_name=clarium_test_suite&sslmode=disable

- Dev-only SeaORM demo:
  - Feature-gated with `seaorm_dev` to keep ORM client code out of normal builds.
  - Build and run a CRUD smoke test via SeaORM using the printed DSN:
    - cargo run --bin clarium_test_suite --features seaorm_dev -- \
      --seaorm-demo --exit-after-seaorm \
      --db-folder dbs --pg-port 5433 --http-port 7878
  - Flags:
    - `--seaorm-demo`: run a small SeaORM flow (create table if not exists, insert, count, update, select, delete)
    - `--exit-after-seaorm`: exit after the demo completes (handy for CI)
  - Note: pgwire protocol coverage is evolving; if your client expects unsupported messages, this demo may fail until pgwire is extended accordingly.

Container and deployment notes

- Container base and runtime deps:
  - The runtime image is based on debian:bookworm-slim and installs libssl3 and ca-certificates so the Rust binary can find libssl.so.3.
- Default working dir and ports:
  - WORKDIR is /etc/clarama; default ports are 7878 (HTTP) and 5433 (pgwire).
- Environment configuration (also used by clarama.deploy.yaml):
  - CLARIUM_HTTP_PORT (default 7878)
  - CLARIUM_PG_PORT (default 5433)
  - CLARIUM_PGWIRE (true/false; only effective if built with --features pgwire)
  - CLARIUM_DB_FOLDER (default dbs when running locally; in k8s typically /etc/clarama/data/{instance}/clarium)
- Kubernetes deployment integration:
  - clarama.deploy.yaml defines templated variables for Clarium and injects the above environment variables into the Clarium pod.
  - Ensure a persistent volume is mounted at /etc/clarama/data so the DB folder exists and is writable. Example path: /etc/clarama/data/{clarama_instance}/clarium.
- Startup diagnostics for permissions:
  - On startup the server logs the folder configuration (cwd, exe, user, home, PWD, db_root, CLARIUM_DB_FOLDER) and whether key paths exist.
  - If a filesystem operation fails (e.g., creating db_root), the error includes the exact path to help diagnose a Permission denied inside containers.

CLI (interactive and local)

- The clarium_cli binary can execute queries locally or act as an interpreter for a remote server.
- One‑shot local queries:
  - cargo run --bin clarium_cli -- --query "SELECT COUNT(_time) FROM clarium/public/demo.time" [--root dbs]
  - echo "SCHEMA SHOW clarium/public/demo.time" | cargo run --bin clarium_cli --
- REPL mode and remote connect:
  - cargo run --bin clarium_cli -- --repl
    - connect http://127.0.0.1:7878 clarium clarium
    - use database clarium
    - use schema public
    - SELECT _time, value FROM public.demo ORDER BY _time LIMIT 5

Table schema and typing

- Non‑_time columns are one of: String, Int64, Float64.
- Type inference per write batch promotes types as needed with precedence String > Float64 > Int64 (no downcasts). Missing columns in a batch are added as nulls.
- schema.json stores only non‑_time columns; locks may be applied by higher‑level commands (if present in your setup).

Startup inventory and demo data

- On startup the server logs and prints discovered databases and their schemas (from the three‑level layout).
- On empty stores a demo dataset is created at clarium/public/demo.time (sine wave, 7 days, 1s cadence).

Notes and limitations

- pgwire implements the simple query flow; extended protocol/SSL/transactions are not supported.
- Results over pgwire are text‑encoded. Time literals like 2025-01-01T00:00:00Z are accepted by the HTTP/WS engine.
- Large datasets may require tuning Parquet chunking and read strategies. There is no WAL/transactions; writes are append‑only chunks with occasional rewrites for some commands.


SLICE time-slice queries

- Syntax:
  SLICE
  USING [LABELS(<label1>, <label2>, ...)] <table | (start,end[, labels...]) | ((row1),(row2),...)>
       [ON <start_col> <end_col>] [WHERE <filter>] [LABEL(<expr1>, <expr2>, ...)]
  { INTERSECT | UNION } <table | (start,end[, labels...]) | ((row1),(row2),...)>
       [ON <start_col> <end_col>] [WHERE <filter>] [LABEL(<expr1>, <expr2>, ...)]
  ...
  { INTERSECT | UNION } SLICE( <nested SLICE plan> )
  
  Where:
  - A manual row has the form (start, end, [labels...]). Start/end accept ISO‑8601 strings like 2025-01-01T00:00:00Z or integer epoch ms. Labels are optional and may be:
    - Plain values: 'A', 123, NULL, "abc"
    - Aliased values: name:=value (e.g., machine:='M1')
  - Multiple manual rows can be specified as ((row1), (row2), ...).

- Behavior:
  - Produces a series of non-overlapping time slices as two columns: _start_date and _end_date (epoch milliseconds). When USING LABELS(...) is provided, those label columns are also returned in the specified order.
  - USING establishes the initial set of slices from the specified source and optional ON columns. If ON is omitted for table sources, _start_date and _end_date are assumed.
  - WHERE, when present after USING/INTERSECT/UNION, filters rows from that source before slice extraction.
  - Labels:
    - With USING LABELS(name1, name2, ...): Each source may add LABEL(expr1, expr2, ...) to provide per-interval label values positionally. Each expr can be a quoted string, a column name, or NULL. Missing/extra values are permitted; missing become null and extras are ignored.
    - Without USING LABELS: Label names are derived automatically from the plan. Explicit aliases from manual rows (name:=value) are included; any remaining unnamed slots are auto-created as label_1..label_N based on the widest row across manual sources.
    - Manual row label assignment: named values go into their named columns; unnamed values fill remaining positions left-to-right.
    - Precedence when combining:
      - INTERSECT: for overlapping ranges, RHS non-null and non-empty labels overwrite LHS labels.
      - UNION: overlapping ranges coalesce; labels prefer LHS values, with RHS only filling LHS null/empty slots.
  - INTERSECT computes the strict intersection of the current slices with the slices from the given source (table, manual, or nested SLICE).
  - UNION merges the current slices with the slices from the given source, coalescing any overlapping ranges (label values do not affect coalescing).
  - Multiple INTERSECT/UNION clauses may appear. A clause may specify SLICE( ... ) to provide a nested plan which is evaluated and then combined with the current slices.

- Notes:
  - Source tables should contain two Int64 columns representing epoch-millisecond start/end values. If your columns have other names, provide them via ON start end.
  - Identifiers may be unqualified; over pgwire/HTTP the current database and schema defaults are used to qualify them.

- Examples:
  - SLICE USING clarium/public/maintenance.time ON start_ms end_ms
  - SLICE USING public.downtime.time WHERE reason = 'power' INTERSECT public.maintenance.time
  - SLICE USING public.a.time UNION SLICE( USING public.b.time INTERSECT public.c.time )
  - SLICE USING (2025-01-01T00:00:00Z, 2025-01-01T01:00:00Z, 'A', 'B')  -- single manual row (auto labels label_1, label_2)
  - SLICE USING ((0, 10000, name:='LHS'), (5000, 15000, name:='RHS'))  -- multiple manual rows with alias
  - SLICE USING LABELS(machine, kind) public.slices.time LABEL('M1', kind)
  - SLICE USING LABELS(machine, kind) public.a.time LABEL('T1','X') UNION (1000, 2000, machine:='M2', kind:='Y')



Logging and debugging

- The Clarium server uses the `tracing` crate with `tracing_subscriber::EnvFilter` initialized in `src/main.rs`. Log verbosity is controlled via the `RUST_LOG` environment variable.
- By default (when `RUST_LOG` is not set), the server runs at `info` level.
- You can enable module‑specific debug logs to troubleshoot parsing, pgwire, and system‑catalog routing.

Common filter strings

- Development (verbose on core modules):
  - `clarium=debug,clarium::pgwire=debug,clarium::exec=debug,clarium::system=debug,clarium::parser=debug,info`
- Production (concise):
  - `info` or `clarium=info`

Enable logging (Windows PowerShell)

- One‑shot in current shell, then run the server with cargo:
  - `$env:RUST_LOG = "clarium=debug,clarium::pgwire=debug,clarium::exec=debug,clarium::system=debug,clarium::parser=debug,info"`
  - `cargo run --release --bin clarium_server`
- When using pgwire feature:
  - `$env:RUST_LOG = "clarium=debug,clarium::pgwire=debug,clarium::exec=debug,clarium::system=debug,clarium::parser=debug,info"`
  - `cargo run --release --features pgwire --bin clarium_server -- --pgwire`
- If you run the built binary directly, set `RUST_LOG` in the same shell before launching it.

Enable logging (Linux/macOS bash/zsh)

- One‑shot in current shell, then run the server with cargo:
  - `export RUST_LOG="clarium=debug,clarium::pgwire=debug,clarium::exec=debug,clarium::system=debug,clarium::parser=debug,info"`
  - `cargo run --release --bin clarium_server`
- With pgwire:
  - `export RUST_LOG="clarium=debug,clarium::pgwire=debug,clarium::exec=debug,clarium::system=debug,clarium::parser=debug,info"`
  - `cargo run --release --features pgwire --bin clarium_server -- --pgwire`

What you will see (examples)

When diagnosing query handling around system catalogs and the pgwire path, look for lines like:

- `clarium::pgwire`:
  - `incoming sql: raw='SELECT schema_name FROM information_schema.schemata'`
  - `effective sql after defaults: 'SELECT ...'`
- `clarium::system`:
  - `try_system_catalog_response: letting core handle full SQL on system table (guard matched). up_norm='...'`
  - `system_table_df: input='information_schema.schemata'`
  - `system_table_df: normalized base='information_schema.schemata' dotted='information_schema.schemata' last1='schemata' last2='information_schema.schemata'`
  - `system_table_df: matched information_schema.schemata rows=...`
- `clarium::parser`:
  - `parse SELECT: FROM found?=true (sql starts with='SELECT ...')`
- `clarium::exec`:
  - `run_select: regular table path; from='information_schema.schemata'`
  - `run_select: system_table_df matched for 'information_schema.schemata'`
  - `read_df_or_kv: name='...'`

Tips

- If you don't see any logs, confirm `RUST_LOG` is exported in the same shell where the process starts.
- Use module filters to reduce noise while keeping the area of interest at `debug`:
  - Example: `clarium::system=trace,clarium::pgwire=debug,info`
- Keep production at `info` or higher to avoid verbose output and performance overhead.
- Logs can be redirected to a file using your shell (e.g., `... 2>&1 | tee clarium.log`).

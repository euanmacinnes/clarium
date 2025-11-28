Command-Line (CLI) Guide
========================

This guide shows how to run Clarium from the command line, both the server and the CLI client, with copy‑pasteable examples for Windows (PowerShell) and Linux/macOS (bash/zsh).

What you can run
----------------
- clarium_server — the HTTP API server (and optional pgwire port) serving queries and writes against a local data folder
- clarium_cli — a lightweight CLI to run one‑off queries or use an interactive REPL, locally or against a remote server

Build the binaries
------------------
```
# Release builds recommended for normal use
cargo build --release

# With PostgreSQL wire protocol (pgwire) support
cargo build --release --features pgwire
```

Running the server (clarium_server)
-----------------------------------
Flags (CLI)
```
--http-port <N>        # HTTP JSON API port (default 7878)
--pg-port <N>          # pgwire port (default 5433) — requires --features pgwire and --pgwire flag
--db-folder <path>     # root folder for databases/schemas/tables (default ./dbs)
--pgwire               # enable pgwire listener (disabled by default)
--no-pgwire            # force-disable pgwire if enabled elsewhere
```

Environment variables (read at startup; flags take precedence)
```
CLARIUM_HTTP_PORT=<N>
CLARIUM_PG_PORT=<N>
CLARIUM_DB_FOLDER=<path>
CLARIUM_PGWIRE=true|false
RUST_LOG="clarium=debug,info"   # enable logs (see Logging section)
```

Windows PowerShell examples
```
# Start HTTP only on port 8080 using ./dbs
cargo run --release --bin clarium_server -- --http-port 8080 --db-folder dbs

# Start with pgwire on 5433 and HTTP on 7878
cargo run --release --features pgwire --bin clarium_server -- --pgwire

# Configure via environment and override pg port via flag
$env:CLARIUM_HTTP_PORT=8080
$env:CLARIUM_DB_FOLDER='dbs'
cargo run --release --features pgwire --bin clarium_server -- --pgwire --pg-port 6432
```

Linux/macOS bash examples
```
# HTTP only
export CLARIUM_DB_FOLDER=./dbs
cargo run --release --bin clarium_server -- --http-port 8080

# With pgwire enabled
export CLARIUM_PGWIRE=true
cargo run --release --features pgwire --bin clarium_server -- --pgwire
```

Quick sanity checks (HTTP)
```
# Basic SELECT over HTTP
curl -s -X POST http://127.0.0.1:7878/query \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT 1 AS one"}'

# Create a time table then list schemas/tables (no auth paths may vary by build)
curl -s -X POST http://127.0.0.1:7878/query -H 'Content-Type: application/json' \
  -d '{"query":"CREATE TIME TABLE clarium/public/demo_cli.time"}'
curl -s -X POST http://127.0.0.1:7878/query -H 'Content-Type: application/json' \
  -d '{"query":"SELECT schema_name FROM information_schema.schemata"}'
```

Connect with psql (pgwire)
--------------------------
Prerequisites: build server with `--features pgwire` and run with `--pgwire`.

psql examples
```
# Windows (PowerShell)
psql "host=127.0.0.1 port=5433 dbname=clarium user=clarium sslmode=disable"

# Linux/macOS
psql "postgres://clarium:clarium@127.0.0.1:5433/clarium?sslmode=disable"

-- Inside psql
SHOW SERVER_VERSION;      -- supported
SELECT viewname FROM pg_catalog.pg_views;
```

Using the CLI client (clarium_cli)
----------------------------------
The CLI can execute queries locally (direct filesystem access) or interact with a running server.

One‑shot local queries
```
# Query a local store under ./dbs (default); override with --root
cargo run --release --bin clarium_cli -- --query "SELECT 1 AS ok"
cargo run --release --bin clarium_cli -- --query "SELECT COUNT(*) FROM clarium/public/demo.time" --root dbs

# Pipe a script
echo "SHOW TABLES; SELECT 1" | cargo run --release --bin clarium_cli --
```

REPL mode and remote connect
```
cargo run --release --bin clarium_cli -- --repl

# In the REPL, connect to a server (HTTP)
connect http://127.0.0.1:7878 clarium clarium
use database clarium
use schema public

# Run a query via REPL
SELECT _time, value FROM clarium/public/demo.time LIMIT 5;
```

Pointing CLI to a server directly (non‑REPL one‑shot)
```
# Using environment (if supported by your build of clarium_cli)
export CLARIUM_HTTP_URL=http://127.0.0.1:7878
cargo run --release --bin clarium_cli -- --query "SHOW ALL"
```

Data folder layout and quick start data
--------------------------------------
- By default the server uses ./dbs as the root; override with `--db-folder` or `CLARIUM_DB_FOLDER`.
- On an empty root, a demo dataset may be created at `clarium/public/demo.time` for quick testing.

Docker usage (optional)
-----------------------
A simple Dockerfile is included. Example commands:
```
# Build image
docker build -t clarium:latest .

# Run with local volume for data
docker run --rm -p 7878:7878 -p 5433:5433 \
  -e CLARIUM_PGWIRE=true \
  -e CLARIUM_HTTP_PORT=7878 \
  -e CLARIUM_PG_PORT=5433 \
  -e CLARIUM_DB_FOLDER=/var/lib/clarium/dbs \
  -v ${PWD}/dbs:/var/lib/clarium/dbs \
  clarium:latest
```

Logging
-------
Enable structured logs via `RUST_LOG`.
```
# Windows PowerShell
$env:RUST_LOG = "clarium=debug,clarium::pgwire=debug,clarium::exec=debug,clarium::system=debug,clarium::parser=debug,info"
cargo run --release --bin clarium_server

# Linux/macOS
export RUST_LOG="clarium=debug,clarium::pgwire=debug,clarium::exec=debug,clarium::system=debug,clarium::parser=debug,info"
cargo run --release --bin clarium_server
```

Security notes (server)
-----------------------
- The HTTP API uses cookie sessions and CSRF tokens for state‑changing operations; see `docs/README.md` and server docs for details.
- Run behind HTTPS in production and ensure the DB folder is on persistent storage.

Handy snippets
--------------
```
# Create objects via CLI (pgwire client) after connecting with psql
CREATE TIME TABLE clarium/public/readings.time;
CREATE VIEW hot_reads AS SELECT _time, temp FROM readings.time WHERE temp>=30;
SELECT COUNT(*) FROM hot_reads;

# Fetch view definition by OID (compatibility)
SELECT pg_get_viewdef(oid) FROM pg_catalog.pg_class WHERE relkind='v' AND relname='hot_reads';
```

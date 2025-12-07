Clarium Database Documentation
==============================

Welcome to the Clarium database documentation. These docs introduce the
database, outline key features, and provide practical, copy‑pasteable examples
for common and advanced scenarios.

Start here if you’re new, or jump to a topic below.

Contents
--------
- Getting started: quick tour and first query — see getting-started.md
- Command-line (server + CLI): running clarium_server, clarium_cli, psql/pgwire — see cli.md
- Core concepts: databases, schemas, tables, time tables, views — see concepts.md
- SQL reference: SELECT, FROM, JOIN, WHERE, GROUP BY, BY windows, ROLLING, ORDER/LIMIT, INTO, CTEs, UNION — see sql-reference.md
- Views: CREATE/DROP/SHOW VIEW, selecting from views, pg_get_viewdef, collisions — see views.md
- Time series: .time tables, ingestion, windows, rolling analytics — see time-series.md
- UDFs: Lua scalar and aggregate functions, metadata, context, usage — see udf.md
- System catalogs: information_schema and pg_catalog compatibility (pg_class, pg_type, pg_views, etc.) — see system-catalogs.md
- Storage format: on-disk layout, schema.json, parquet chunks, .view JSON — see storage-format.md
- Administration: USE DATABASE/SCHEMA; create/drop/rename DB/Schema/Table/Time Table/View — see administration.md
- PostgreSQL compatibility: SQL surface, functions, pgwire hints, limitations — see compatibility.md
- End-to-end examples: scenarios you can reproduce quickly — see examples.md
 - Benchmarks: Criterion micro- and SQL-driven suites, how to run and extend — see benchmarks.md

FILESTORE documentation
-----------------------
- Getting started with FILESTORE — docs/filestore/getting-started.md
- FILESTORE SQL reference — docs/filestore/sql.md
- FILESTORE concepts and architecture — docs/filestore/concepts.md

If you find an issue or a gap in the docs, please open an issue or PR.

PostgreSQL Compatibility
========================

Clarium aims for practical compatibility with common PostgreSQL clients and
tooling. This page summarizes what works, where behavior differs, and tips for
connecting tools.

Wire protocol and clients
-------------------------
- Clarium speaks a simplified pgwire sufficient for many clients.
- Standard SHOW commands are supported (e.g., `SHOW SERVER_VERSION`, `SHOW ALL`).

Catalog compatibility
---------------------
- `information_schema.*` tables provide schema, table, column, and view listings.
- `pg_catalog` includes a subset: `pg_class`, `pg_type`, `pg_namespace`,
  `pg_attribute`, `pg_constraint`, `pg_constraint_columns`, `pg_description`,
  and `pg_views`.
- Stable OIDs are generated and persisted for tables and views, enabling
  `regclass`-style lookups in many client paths.

Functions
---------
- `pg_get_viewdef(oid)` returns the stored definition of a view; NULL otherwise.
- Basic string functions: `UPPER`, `LOWER`.
- Window functions: `ROW_NUMBER()` with `OVER (...)`.

Data definition and naming
--------------------------
- Unqualified names in DDL honor `USE DATABASE` and `USE SCHEMA` session
  defaults (as many clients expect).
- Time tables must end with `.time` in the final identifier segment.

Limitations and differences
---------------------------
- Clarium’s type system is simplified (string/int64/float64/bool; timestamps as
  Int64 epoch ms in time tables). Types in catalogs are mapped to a practical subset.
- Transaction semantics are simplified; operations are applied directly to the
  filesystem. There is no MVCC or concurrent transaction isolation.
- Indexes are not currently modeled separately; primary keys are metadata only.

Tips for tools
--------------
- SQLAlchemy/DBeaver/psql: metadata reflection should work via `information_schema`
  and `pg_catalog`. If a tool insists on system schemas, prepend `pg_catalog.`.
- For time‑series workloads, prefer windowed or rolling queries to limit data
  volume when exploring interactively.

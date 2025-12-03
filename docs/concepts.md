Core Concepts
=============

This page introduces Clarium’s object model and naming rules so you can read and
write queries with confidence.

Objects
-------
- Database: top-level folder under the storage root (e.g., `clarium`).
- Schema: sub-folder under a database (e.g., `public`).
- Regular table: a folder with `schema.json` (e.g., `clarium/public/orders`).
- Time table: a folder ending with `.time` (e.g., `clarium/public/metrics.time`).
  - Every time table implicitly has a `_time` column (epoch ms, 64-bit integer).
- View: a `.view` JSON file stored next to tables (e.g., `clarium/public/sales.view`).
  - The file contains the projected columns and original SELECT (or SELECT UNION) SQL.
- KV store: special addresses using `<db>.store.<store>` for key-value utilities.

Qualification and naming
------------------------
- Canonical table identifier: `database/schema/name` (slashes).
- Dotted form `db.schema.name` and bare names are accepted by many SQL commands.
- Time tables must end with `.time` in their last segment.
- Views are referenced by name without extension; on disk they are saved as `.view`.
- Session defaults: `USE DATABASE <db>` and `USE SCHEMA <schema>` set defaults for
  unqualified object names in DDL and SELECT. Example:
  ```
  USE DATABASE analytics;
  USE SCHEMA raw;
  SELECT * FROM events.time LIMIT 1; -- resolves to analytics/raw/events.time
  CREATE VIEW hourly AS SELECT COUNT(*) AS n FROM events.time BY 1h; -- saved under analytics/raw/hourly.view
  ```

Uniqueness and collisions
-------------------------
- Object names must be unique across tables and views within a schema.
- The base name is used for uniqueness; e.g., `foo` (table) conflicts with view `foo`.
- A time table `foo.time` conflicts with a view `foo` and vice versa.

Storage model
-------------
- Regular tables keep a `schema.json` and Parquet chunks (`data-*.parquet`).
- Time tables share the same layout but are conceptually ordered by `_time`.
- Views are saved as `<name>.view` JSON with fields `name`, `columns`, `definition_sql`.

System catalogs
---------------
- `information_schema` and `pg_catalog` compatibility tables expose metadata
  (schemas, tables, columns, views, types, class/namespace, constraints).
- `pg_catalog.pg_class` lists tables (`relkind='r'`) and views (`relkind='v'`) with stable OIDs.
- `pg_catalog.pg_views` lists all views with `schemaname`, `viewname`, `definition`.

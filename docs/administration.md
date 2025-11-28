Administration
==============

This guide covers session defaults, object management (create/drop/rename), and
name qualification rules.

Session defaults
----------------
Use `USE` to set current database and schema for unqualified names:
```
USE DATABASE mydb;
USE SCHEMA s1;

-- Affects DDL and SELECT resolution
CREATE TIME TABLE src.time;
CREATE VIEW v1 AS SELECT v FROM src.time;
SELECT * FROM v1 LIMIT 10;  -- resolves to mydb/s1/v1
```

Databases
---------
```
CREATE DATABASE mydb;
DROP DATABASE mydb;
RENAME DATABASE old_name TO new_name;
```

Schemas
-------
```
-- If unqualified, the current database is prepended
CREATE SCHEMA s1;
DROP SCHEMA s1;
RENAME SCHEMA s1 TO s2;
```

Tables (regular)
----------------
```
CREATE TABLE orders;            -- under current db/schema
DROP TABLE orders;              -- supports IF EXISTS
RENAME TABLE orders TO orders2;

-- With explicit schema and/or database
CREATE TABLE mydb/public/orders;
```

Time tables
-----------
```
CREATE TIME TABLE metrics.time;  -- must end with .time
DROP TIME TABLE metrics.time;
RENAME TIME TABLE metrics.time TO metrics_clean.time;
```

Views
-----
```
CREATE VIEW v_sales AS SELECT * FROM sales ORDER BY id;
CREATE OR ALTER VIEW v_sales AS SELECT * FROM sales ORDER BY id DESC;
DROP VIEW v_sales;              -- supports IF EXISTS
SHOW VIEW v_sales;              -- returns name and stored definition
```

Uniqueness and collisions
-------------------------
- A view name conflicts with a table directory of the same base name (and vice versa).
- A view name also conflicts with a time table of the same base name (without `.time`).
```
-- Examples of disallowed creations
CREATE TABLE t1;
CREATE VIEW t1 AS SELECT 1 AS c;            -- error: conflicts with table

CREATE VIEW v1 AS SELECT 1 AS c;
CREATE TABLE v1;                             -- error: conflicts with view

CREATE TIME TABLE src.time;
CREATE VIEW src AS SELECT 1 AS c;           -- error: conflicts with src.time
```

Security and permissions (server)
---------------------------------
- HTTP server endpoints authorize commands by kind (Select, Insert, Database, Schema, etc.).
- CSRF tokens are enforced for mutating endpoints; see server.rs for details.
